//! Defines functionality for a `LocalDataFrame`
use crate::dataframe::{Row, Rower, Schema};
use crate::error::LiquidError;
use crossbeam_utils::thread;
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};
use sorer::dataframe::{from_file, Column, Data};
use sorer::schema::{infer_schema, DataType};
use std::cmp::Ordering;
use std::convert::TryInto;

/// Represents a local data frame which contains data stored in a columnar
/// format and a well-defined `Schema`. Is useful for data sets that fit into
/// memory or for testing/debugging purposes.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, DeepSizeOf)]
pub struct LocalDataFrame {
    /// The `Schema` of this data frame
    pub schema: Schema,
    /// The data of this data frame, in columnar format
    pub data: Vec<Column>,
    /// Number of threads for this computer
    pub n_threads: usize,
    /// Current row index for implementing the `Iterator` trait
    cur_row_idx: usize,
}

macro_rules! setter {
    ($func_name:ident, $type:ty, $sorer_type:ident) => {
        /// Mutates the value in this `DataFrame` at the given `col_idx, row_idx`
        /// to be changed to the given `data`.
        pub fn $func_name(
            &mut self,
            col_idx: usize,
            row_idx: usize,
            data: $type,
        ) -> Result<(), LiquidError> {
            match self.schema.schema.get(col_idx) {
                Some(DataType::$sorer_type) => {
                    match self.data.get_mut(col_idx) {
                        Some(Column::$sorer_type(col)) => {
                            match col.get_mut(row_idx) {
                                Some(d) => {
                                    *d = Some(data);
                                    Ok(())
                                }
                                None => Err(LiquidError::RowIndexOutOfBounds),
                            }
                        }
                        None => Err(LiquidError::ColIndexOutOfBounds),
                        _ => panic!("Something is horribly wrong"),
                    }
                }
                _ => Err(LiquidError::TypeMismatch),
            }
        }
    };
}

/// An implementation for a `LocalDataFrame`, inspired by the data frames used
/// in `pandas` and `R`.
impl LocalDataFrame {
    /// Creates a new `LocalDataFrame` from the given file by reading it in
    /// parallel using the number of cores available on this machine. Only
    /// reads `len` bytes of the file, starting at the given byte offset
    /// `from`.
    pub fn from_sor(file_name: &str, from: usize, len: usize) -> Self {
        let schema = Schema::from(infer_schema(file_name).expect("Could not infer schema for {file_name:?}"));
        let n_threads = num_cpus::get();
        let data =
            from_file(file_name, schema.schema.clone(), from, len, n_threads);
        LocalDataFrame {
            schema,
            data,
            n_threads,
            cur_row_idx: 0,
        }
    }

    /// Creates an empty `LocalDataFrame` from the given `Schema`. The
    /// `LocalDataFrame` is created with no rows, but the names of the columns
    /// in the given `schema` are cloned.
    pub fn new(schema: &Schema) -> Self {
        let mut data = Vec::new();
        for data_type in &schema.schema {
            match data_type {
                DataType::Bool => data.push(Column::Bool(Vec::new())),
                DataType::Int => data.push(Column::Int(Vec::new())),
                DataType::Float => data.push(Column::Float(Vec::new())),
                DataType::String => data.push(Column::String(Vec::new())),
            }
        }
        let schema = Schema {
            schema: schema.schema.clone(),
            col_names: schema.col_names.clone(),
        };

        LocalDataFrame {
            schema,
            data,
            n_threads: num_cpus::get(),
            cur_row_idx: 0,
        }
    }

    /// Obtains a reference to the schema of this `LocalDataFrame`
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    /// Adds a `Column` to this `LocalDataFrame` with an optional name. Returns
    /// a `LiquidError::NameAlreadyExists` if the given `name` is not unique.
    pub fn add_column(
        &mut self,
        col: Column,
        name: Option<String>,
    ) -> Result<(), LiquidError> {
        match &col {
            Column::Int(_) => self.schema.add_column(DataType::Int, name),
            Column::Bool(_) => self.schema.add_column(DataType::Bool, name),
            Column::Float(_) => self.schema.add_column(DataType::Float, name),
            Column::String(_) => self.schema.add_column(DataType::String, name),
        }?;

        match self.n_rows().cmp(&col.len()) {
            Ordering::Equal => self.data.push(col),
            Ordering::Less => {
                // our data is shorter than `col`, must add Data::Null to
                // all of our columns until they are equal length w/`col`
                for j in 0..self.n_cols() - 1 {
                    let c = self.data.get_mut(j).unwrap();
                    for _ in 0..col.len() - c.len() {
                        match c {
                            Column::Bool(x) => x.push(None),
                            Column::Int(x) => x.push(None),
                            Column::Float(x) => x.push(None),
                            Column::String(x) => x.push(None),
                        }
                    }
                }
                self.data.push(col)
            }
            Ordering::Greater => {
                // our data is longer than `col`, we must add Data::Null to
                // `col` until it matches the len of our data
                let diff = self.n_rows() - col.len();
                // note that vec![] must be done inside match so types are
                // correct. a for loop also doesn't work, i tried
                // Also I know this is ugly but trust me i tried a lot of shit
                // and this is the only thing that worked
                match col {
                    Column::Bool(mut x) => {
                        let nones = vec![None; diff];
                        x.extend(nones.into_iter());
                        self.data.push(Column::Bool(x))
                    }
                    Column::Int(mut x) => {
                        let nones = vec![None; diff];
                        x.extend(nones.into_iter());
                        self.data.push(Column::Int(x))
                    }
                    Column::Float(mut x) => {
                        let nones = vec![None; diff];
                        x.extend(nones.into_iter());
                        self.data.push(Column::Float(x))
                    }
                    Column::String(mut x) => {
                        let nones = vec![None; diff];
                        x.extend(nones.into_iter());
                        self.data.push(Column::String(x))
                    }
                }
            }
        }

        Ok(())
    }

    /// Get the `Data` at the given `col_idx`, `row_idx` offsets.
    pub fn get(
        &self,
        col_idx: usize,
        row_idx: usize,
    ) -> Result<Data, LiquidError> {
        // Note that yes this is really ugly, but no it can't be abstracted
        // without macros (must match on the types) and it is for performance
        // so that we don't have to box/unbox values when constructing the
        // DataFrame and mapping over it
        match self.data.get(col_idx) {
            Some(Column::Int(col)) => match col.get(row_idx) {
                Some(optional_data) => match optional_data {
                    Some(data) => Ok(Data::Int(*data)),
                    None => Ok(Data::Null),
                },
                None => Err(LiquidError::RowIndexOutOfBounds),
            },
            Some(Column::Bool(col)) => match col.get(row_idx) {
                Some(optional_data) => match optional_data {
                    Some(data) => Ok(Data::Bool(*data)),
                    None => Ok(Data::Null),
                },
                None => Err(LiquidError::RowIndexOutOfBounds),
            },
            Some(Column::Float(col)) => match col.get(row_idx) {
                Some(optional_data) => match optional_data {
                    Some(data) => Ok(Data::Float(*data)),
                    None => Ok(Data::Null),
                },
                None => Err(LiquidError::RowIndexOutOfBounds),
            },
            Some(Column::String(col)) => match col.get(row_idx) {
                Some(optional_data) => match optional_data {
                    Some(data) => Ok(Data::String(data.clone())),
                    None => Ok(Data::Null),
                },
                None => Err(LiquidError::RowIndexOutOfBounds),
            },
            None => Err(LiquidError::ColIndexOutOfBounds),
        }
    }

    /// Get the index of the `Column` with the given `col_name`. Returns `Some`
    /// if a `Column` with the given name exists, or `None` otherwise.
    pub fn get_col_idx(&self, col_name: &str) -> Option<usize> {
        self.schema.col_idx(col_name)
    }

    /// Given a column index, returns its name
    pub fn col_name(
        &self,
        col_idx: usize,
    ) -> Result<Option<&str>, LiquidError> {
        self.schema.col_name(col_idx)
    }

    setter!(set_string, String, String);
    setter!(set_bool, bool, Bool);
    setter!(set_float, f64, Float);
    setter!(set_int, i64, Int);

    /// Set the fields of the given `Row` struct with values from this
    /// `DataFrame` at the given `row_index`.
    ///
    /// If the `row` does not have the same schema as this `DataFrame`, a
    /// `LiquidError::TypeMismatch` error will be returned.
    pub fn fill_row(
        &self,
        row_index: usize,
        row: &mut Row,
    ) -> Result<(), LiquidError> {
        for (c_idx, col) in self.data.iter().enumerate() {
            match col {
                Column::Int(c) => match c.get(row_index).unwrap() {
                    Some(x) => row.set_int(c_idx, *x)?,
                    None => row.set_null(c_idx)?,
                },
                Column::Float(c) => match c.get(row_index).unwrap() {
                    Some(x) => row.set_float(c_idx, *x)?,
                    None => row.set_null(c_idx)?,
                },
                Column::Bool(c) => match c.get(row_index).unwrap() {
                    Some(x) => row.set_bool(c_idx, *x)?,
                    None => row.set_null(c_idx)?,
                },
                Column::String(c) => match c.get(row_index).unwrap() {
                    Some(x) => row.set_string(c_idx, x.clone())?,
                    None => row.set_null(c_idx)?,
                },
            };
        }
        row.set_idx(row_index);
        Ok(())
    }

    /// Add a `Row` at the end of this `DataFrame`.
    ///
    /// If the `row` does not have the same schema as this `DataFrame`, a
    /// `LiquidError::TypeMismatch` error will be returned.
    pub fn add_row(&mut self, row: &Row) -> Result<(), LiquidError> {
        if row.schema != self.schema {
            return Err(LiquidError::TypeMismatch);
        }

        for (data, column) in row.data.iter().zip(self.data.iter_mut()) {
            match (data, column) {
                (Data::Int(n), Column::Int(l)) => l.push(Some(*n)),
                (Data::Float(n), Column::Float(l)) => l.push(Some(*n)),
                (Data::Bool(n), Column::Bool(l)) => l.push(Some(*n)),
                (Data::String(n), Column::String(l)) => l.push(Some(n.clone())),
                (Data::Null, Column::Int(l)) => l.push(None),
                (Data::Null, Column::Float(l)) => l.push(None),
                (Data::Null, Column::Bool(l)) => l.push(None),
                (Data::Null, Column::String(l)) => l.push(None),
                (_, _) => unreachable!("Something is horribly wrong"),
            };
        }

        Ok(())
    }

    /// Applies the given `rower` synchronously to every row in this
    /// `LocalDataFrame`
    ///
    /// Since `map` takes an immutable reference to `self`, the `rower` can
    /// not mutate this `DataFrame`. If mutation is desired, the `rower` must
    /// create its own `DataFrame` internally, clone each `Row` from this
    /// `DataFrame` as it visits them, and mutate the cloned row during each
    /// visit.
    pub fn map<T: Rower>(&self, rower: T) -> T {
        map_helper(self, rower, 0, self.n_rows())
    }

    /// Applies the given `rower` to every row sequentially in this `DataFrame`
    /// The `rower` is cloned `n_threads` times, according to the value of
    /// `n_threads` for this `LocalDataFrame`. Each `rower` gets operates on a
    /// chunk of this `LocalDataFrame` and are run in parallel.
    ///
    /// Since `pmap` takes an immutable reference to `self`, the `rower` can
    /// not mutate this `LocalDataFrame`. If mutation is desired, the `rower`
    /// must create its own `LocalDataFrame` internally by building one up
    /// as it visit rows, and mutates that.
    ///
    /// `n_threads` defaults to the number of cores available on this machine.
    pub fn pmap<T: Rower + Clone + Send>(&self, rower: T) -> T {
        let rowers = vec![rower; self.n_threads];
        let mut new_rowers = Vec::new();
        let step = self.n_rows() / self.n_threads;
        let mut from = 0;
        thread::scope(|s| {
            let mut threads = Vec::new();
            let mut i = 0;
            for r in rowers {
                i += 1;
                let to = if i == self.n_threads {
                    self.n_rows()
                } else {
                    from + step
                };
                threads.push(s.spawn(move |_| map_helper(&self, r, from, to)));
                from += step;
            }
            for thread in threads {
                new_rowers.push(thread.join().unwrap());
            }
        })
        .unwrap();
        let acc = new_rowers.pop().unwrap();
        new_rowers
            .into_iter()
            .rev()
            .fold(acc, |prev, x| x.join(prev))
    }

    /// Creates a new `LocalDataFrame` by applying the given `rower` to every
    /// row sequentially in this `LocalDataFrame` and cloning rows for which
    /// the given `rower` returns true from its `accept` method. Is run
    /// synchronously.
    pub fn filter<T: Rower>(&self, rower: &mut T) -> Self {
        filter_helper(self, rower, 0, self.n_rows())
    }

    /// Creates a new `LocalDataFrame` by applying the given `rower` to every
    /// row in this data frame sequentially, and cloning rows for which the
    /// given `rower` returns true from its `accept` method. The `rower` is
    /// cloned `n_threads` times, according to the value of `n_threads` for
    /// this `LocalDataFrame`. Each `rower` gets operates on a chunk of this
    /// `LocalDataFrame` and are run in parallel.
    ///
    /// `n_threads` defaults to the number of cores available on this machine.
    pub fn pfilter<T: Rower + Clone + Send>(&self, rower: &mut T) -> Self {
        let mut rowers = Vec::new();
        for _ in 0..self.n_threads {
            rowers.push(rower.clone());
        }
        // ok.... the below syntax doesn't work, not sure why
        //    let rowers = vec![*rower; self.n_threads];
        let mut new_dfs = Vec::new();
        let step = self.n_rows() / self.n_threads;
        let mut from = 0;
        thread::scope(|s| {
            let mut threads = Vec::new();
            let mut i = 0;
            for mut r in rowers {
                i += 1;
                let to = if i == self.n_threads {
                    self.n_rows()
                } else {
                    from + step
                };
                threads.push(
                    s.spawn(move |_| filter_helper(&self, &mut r, from, to)),
                );
                from += step;
            }
            for thread in threads {
                new_dfs.push(thread.join().unwrap());
            }
        })
        .unwrap();
        let acc = new_dfs.pop().unwrap();
        new_dfs
            .into_iter()
            .rev()
            .fold(acc, |prev, x| x.combine(prev).unwrap())
    }

    /// Consumes this `LocalDataFrame` and the other given `LocalDataFrame`,
    /// returning a combined `LocalDataFrame` if successful.
    ///
    /// - The columns names and the number of threads for the resulting
    ///   `LocalDataFrame` are from this `LocalDataFrame` and the column names
    ///   and `n_threads` in `other` are ignored
    /// - The data of `other` is appended to the data of this `LocalDataFrame`
    ///
    /// # Errors
    /// If the schema of this `LocalDataFrame` and `other` have different
    /// `DataType`s
    pub fn combine(mut self, other: Self) -> Result<Self, LiquidError> {
        if self.get_schema().schema != other.get_schema().schema {
            return Err(LiquidError::TypeMismatch);
        }

        for (col_idx, col) in other.data.into_iter().enumerate() {
            match self.data.get_mut(col_idx).unwrap() {
                Column::Bool(result_col) => {
                    let x: Vec<Option<bool>> = col.try_into().unwrap();
                    result_col.extend(x.into_iter())
                }
                Column::Int(result_col) => {
                    let x: Vec<Option<i64>> = col.try_into().unwrap();
                    result_col.extend(x.into_iter())
                }
                Column::Float(result_col) => {
                    let x: Vec<Option<f64>> = col.try_into().unwrap();
                    result_col.extend(x.into_iter())
                }
                Column::String(result_col) => {
                    let x: Vec<Option<String>> = col.try_into().unwrap();
                    result_col.extend(x.into_iter())
                }
            }
        }

        Ok(self)
    }

    /// Return the number of rows in this `DataFrame`.
    pub fn n_rows(&self) -> usize {
        if self.data.is_empty() {
            0
        } else {
            self.data[0].len()
        }
    }

    /// Return the number of columns in this `DataFrame`.
    pub fn n_cols(&self) -> usize {
        self.schema.width()
    }
}

fn filter_helper<T: Rower>(
    df: &LocalDataFrame,
    r: &mut T,
    start: usize,
    end: usize,
) -> LocalDataFrame {
    let mut df2 = LocalDataFrame::new(&df.schema);
    let mut row = Row::new(&df.schema);

    for i in start..end {
        df.fill_row(i, &mut row).unwrap();
        if r.visit(&row) {
            df2.add_row(&row).unwrap();
        }
    }

    df2
}

fn map_helper<T: Rower>(
    df: &LocalDataFrame,
    mut rower: T,
    start: usize,
    end: usize,
) -> T {
    let mut row = Row::new(&df.schema);
    // NOTE: IS THIS THE ~10% slower way to do counted loop???? @tom
    for i in start..end {
        df.fill_row(i, &mut row).unwrap();
        rower.visit(&row);
    }
    rower
}

impl From<Column> for LocalDataFrame {
    /// Construct a new `DataFrame` with the given `column`.
    fn from(column: Column) -> Self {
        LocalDataFrame::from(vec![column])
    }
}

impl From<Vec<Column>> for LocalDataFrame {
    /// Construct a new `DataFrame` with the given `columns`.
    fn from(data: Vec<Column>) -> Self {
        let mut schema = Schema::new();
        for column in &data {
            match &column {
                Column::Bool(_) => {
                    schema.add_column(DataType::Bool, None).unwrap()
                }
                Column::Int(_) => {
                    schema.add_column(DataType::Int, None).unwrap()
                }
                Column::Float(_) => {
                    schema.add_column(DataType::Float, None).unwrap()
                }
                Column::String(_) => {
                    schema.add_column(DataType::String, None).unwrap()
                }
            };
        }
        let n_threads = num_cpus::get();
        LocalDataFrame {
            schema,
            n_threads,
            data,
            cur_row_idx: 0,
        }
    }
}

impl From<Data> for LocalDataFrame {
    /// Construct a new `DataFrame` with the given `scalar` value.
    fn from(scalar: Data) -> Self {
        let c = match scalar {
            Data::Bool(x) => Column::Bool(vec![Some(x)]),
            Data::Int(x) => Column::Int(vec![Some(x)]),
            Data::Float(x) => Column::Float(vec![Some(x)]),
            Data::String(x) => Column::String(vec![Some(x)]),
            Data::Null => panic!("Can't make a DataFrame from a null value"),
        };
        LocalDataFrame::from(c)
    }
}

impl std::fmt::Display for LocalDataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in 0..self.n_rows() {
            for j in 0..self.n_cols() {
                write!(f, "<{}>", self.get(j, i).unwrap())?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataframe::{Row, Rower};

    #[derive(Clone)]
    struct PosIntSummer {
        sum: i64,
    }

    impl Rower for PosIntSummer {
        fn visit(&mut self, r: &Row) -> bool {
            let i = r.get(0).unwrap();
            match i {
                Data::Int(val) => {
                    if *val < 0 {
                        return false;
                    }
                    self.sum += *val;
                    true
                }
                _ => panic!(),
            }
        }

        fn join(mut self, other: Self) -> Self {
            self.sum += other.sum;
            self
        }
    }

    fn init() -> LocalDataFrame {
        let s = Schema::from(vec![DataType::Int]);
        let mut r = Row::new(&s);
        let mut df = LocalDataFrame::new(&s);

        for i in 0..1000 {
            if i % 2 == 0 {
                r.set_int(0, i * -1).unwrap();
            } else {
                r.set_int(0, i).unwrap();
            }
            df.add_row(&r).unwrap();
        }

        df
    }

    #[test]
    fn test_combine_err_case() {
        let s = Schema::from(vec![DataType::Int]);
        let df1 = LocalDataFrame::new(&s);
        let s = Schema::from(vec![DataType::Bool]);
        let df2 = LocalDataFrame::new(&s);
        assert!(df1.combine(df2).is_err());
    }

    #[test]
    fn test_combine() {
        let s = Schema::from(vec![]);
        let mut df1 = LocalDataFrame::new(&s);
        let mut df2 = LocalDataFrame::new(&s);
        let col1 = Column::Int(vec![Some(1), Some(2), Some(3)]);
        let col2 = Column::Bool(vec![Some(false), Some(false), Some(false)]);
        df1.add_column(col1, Some("col1".to_string())).unwrap();
        df1.add_column(col2, None).unwrap();
        let col3 = Column::Int(vec![Some(4), Some(5), Some(6)]);
        let col4 = Column::Bool(vec![Some(true), Some(true), Some(true)]);
        df2.add_column(col3, None).unwrap();
        df2.add_column(col4, None).unwrap();
        let res = df1.combine(df2);
        assert!(res.is_ok());
        let combined = res.unwrap();
        let mut res_schema = Schema::from(vec![DataType::Int, DataType::Bool]);
        res_schema.col_names.insert("col1".to_string(), 0);
        assert_eq!(combined.get_schema(), &res_schema);
        let r = PosIntSummer { sum: 0 };
        assert_eq!(combined.map(r).sum, 21);
    }

    #[test]
    fn test_map() {
        let df = init();
        let mut rower = PosIntSummer { sum: 0 };
        rower = df.map(rower);
        assert_eq!(1000 * 1000 / 4, rower.sum);
        assert_eq!(1000, df.n_rows());
    }

    #[test]
    fn test_pmap() {
        let df = init();
        let mut rower = PosIntSummer { sum: 0 };
        rower = df.pmap(rower);
        assert_eq!(1000 * 1000 / 4, rower.sum);
        assert_eq!(1000, df.n_rows());
    }

    #[test]
    fn test_pmap_w_1_thread() {
        let mut df = init();
        df.n_threads = 1;
        let mut rower = PosIntSummer { sum: 0 };
        rower = df.pmap(rower);
        assert_eq!(1000 * 1000 / 4, rower.sum);
        assert_eq!(1000, df.n_rows());
    }

    #[test]
    fn test_filter() {
        let df = init();
        let mut rower = PosIntSummer { sum: 0 };
        let df2 = df.filter(&mut rower);
        assert_eq!(df2.n_rows(), 501);
        assert_eq!(df2.n_cols(), 1);
        assert_eq!(df2.get(0, 10).unwrap(), Data::Int(19));
    }

    #[test]
    fn test_pfilter() {
        let df = init();
        let mut rower = PosIntSummer { sum: 0 };
        let df2 = df.pfilter(&mut rower);
        assert_eq!(df2.n_rows(), 501);
        assert_eq!(df2.n_cols(), 1);
        assert_eq!(df2.get(0, 10).unwrap(), Data::Int(19));
    }
}
