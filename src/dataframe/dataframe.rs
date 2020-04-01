//! Defines functionality for the `DataFrame`
use crate::dataframe::{DataFrame, Row, Rower, Schema};
use crate::error::LiquidError;
use num_cpus;
use sorer::dataframe::{from_file, Column, Data};
use sorer::schema::{infer_schema_from_file, DataType};
use std::cmp::Ordering;

use crossbeam_utils::thread;

/// An interface for a `DataFrame`, inspired by those used in `pandas` and `R`.
impl DataFrame {
    /// Creates a new `DataFrame` from the given file, only reads `len` bytes
    /// starting at the given byte offset `from`.
    pub fn from_sor(file_name: String, from: usize, len: usize) -> Self {
        let schema = Schema::from(infer_schema_from_file(file_name.clone()));
        let n_threads = num_cpus::get();
        let data =
            from_file(file_name, schema.schema.clone(), from, len, n_threads);
        DataFrame {
            schema,
            data,
            n_threads,
        }
    }

    /// Creates an empty `DataFrame` from the given `Schema`. The `DataFrame`
    /// is created with no rows, but the names of the columns in the given
    /// `schema` are cloned.
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

        DataFrame {
            schema,
            data,
            n_threads: num_cpus::get(),
        }
    }

    /// Obtains a reference to this `DataFrame`s schema.
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    /// Adds a `Column` to this `DataFrame` with an optional `name`. Returns
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
                        x.extend_from_slice(&nones);
                        self.data.push(Column::Bool(x))
                    }
                    Column::Int(mut x) => {
                        let nones = vec![None; diff];
                        x.extend_from_slice(&nones);
                        self.data.push(Column::Int(x))
                    }
                    Column::Float(mut x) => {
                        let nones = vec![None; diff];
                        x.extend_from_slice(&nones);
                        self.data.push(Column::Float(x))
                    }
                    Column::String(mut x) => {
                        let nones = vec![None; diff];
                        x.extend_from_slice(&nones);
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
        // (must match on the types) and it is for performance so that we don't
        // have to box/unbox values when constructing the DataFrame and mapping
        // over it
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
    pub fn get_col(&self, col_name: &str) -> Option<usize> {
        self.schema.col_idx(col_name)
    }

    /// Mutates the value in this `DataFrame` at the given `col_idx, row_idx`
    /// to be changed to the given `data`.
    pub fn set_int(
        &mut self,
        col_idx: usize,
        row_idx: usize,
        data: i64,
    ) -> Result<(), LiquidError> {
        match self.schema.schema.get(col_idx) {
            Some(DataType::Int) => match self.data.get_mut(col_idx) {
                Some(Column::Int(col)) => match col.get_mut(row_idx) {
                    Some(d) => {
                        *d = Some(data);
                        Ok(())
                    }
                    None => Err(LiquidError::RowIndexOutOfBounds),
                },
                None => Err(LiquidError::ColIndexOutOfBounds),
                _ => panic!("Something is horribly wrong"),
            },
            _ => Err(LiquidError::TypeMismatch),
        }
    }

    /// Mutates the value in this `DataFrame` at the given `col_idx, row_idx`
    /// to be changed to the given `data`.
    pub fn set_float(
        &mut self,
        col_idx: usize,
        row_idx: usize,
        data: f64,
    ) -> Result<(), LiquidError> {
        match self.schema.schema.get(col_idx) {
            Some(DataType::Float) => match self.data.get_mut(col_idx) {
                Some(Column::Float(col)) => match col.get_mut(row_idx) {
                    Some(d) => {
                        *d = Some(data);
                        Ok(())
                    }
                    None => Err(LiquidError::RowIndexOutOfBounds),
                },
                None => Err(LiquidError::ColIndexOutOfBounds),
                _ => panic!("Something is horribly wrong"),
            },
            _ => Err(LiquidError::TypeMismatch),
        }
    }

    /// Mutates the value in this `DataFrame` at the given `col_idx, row_idx`
    /// to be changed to the given `data`.
    pub fn set_bool(
        &mut self,
        col_idx: usize,
        row_idx: usize,
        data: bool,
    ) -> Result<(), LiquidError> {
        match self.schema.schema.get(col_idx) {
            Some(DataType::Bool) => match self.data.get_mut(col_idx) {
                Some(Column::Bool(col)) => match col.get_mut(row_idx) {
                    Some(d) => {
                        *d = Some(data);
                        Ok(())
                    }
                    None => Err(LiquidError::RowIndexOutOfBounds),
                },
                None => Err(LiquidError::ColIndexOutOfBounds),
                _ => panic!("Something is horribly wrong"),
            },
            _ => Err(LiquidError::TypeMismatch),
        }
    }

    /// Mutates the value in this `DataFrame` at the given `col_idx, row_idx`
    /// to be changed to the given `data`.
    pub fn set_string(
        &mut self,
        col_idx: usize,
        row_idx: usize,
        data: String,
    ) -> Result<(), LiquidError> {
        match self.schema.schema.get(col_idx) {
            Some(DataType::String) => match self.data.get_mut(col_idx) {
                Some(Column::String(col)) => match col.get_mut(row_idx) {
                    Some(d) => {
                        *d = Some(data);
                        Ok(())
                    }
                    None => Err(LiquidError::RowIndexOutOfBounds),
                },
                None => Err(LiquidError::ColIndexOutOfBounds),
                _ => panic!("Something is horribly wrong"),
            },
            _ => Err(LiquidError::TypeMismatch),
        }
    }

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
        Ok(())
    }

    /// Add a `Row` at the end of this `DataFrame`.
    ///
    /// If the `row` does not have the same schema as this `DataFrame`, a
    /// `LiquidError::TypeMismatch` error will be returned.
    pub fn add_row(&mut self, row: &Row) -> Result<(), LiquidError> {
        if row.schema != self.schema.schema {
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

    /// Applies the given `rower` sequentially to every row in this `DataFrame`.
    pub fn map<T: Rower>(&self, rower: T) -> T {
        map_helper(self, rower, 0, self.n_rows())
    }

    /// Applies the given `rower` to every row in this `DataFrame` in parallel
    /// using `self.n_threads` (which by default is set to the number of
    /// threads available on the machine this `DataFrame` runs on).
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
            .iter_mut()
            .rev()
            .fold(acc, |prev, x| x.join(&prev))
    }

    /// Create a new `DataFrame`, constructed from rows for which the given
    /// `Rower` returned true from its `accept` method.
    pub fn filter<T: Rower>(&self, r: &mut T) -> Self {
        let mut df = DataFrame::new(&self.schema);
        let mut row = Row::new(&self.schema);

        for i in 0..self.n_rows() {
            self.fill_row(i, &mut row).unwrap();
            if r.visit(&row) {
                df.add_row(&row).unwrap();
            }
        }

        df
    }

    /// Return the number of rows in this `DataFrame`.
    pub fn n_rows(&self) -> usize {
        if self.data.len() == 0 {
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

fn map_helper<T: Rower>(
    df: &DataFrame,
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

impl From<Column> for DataFrame {
    /// Construct a new `DataFrame` with the given `column`.
    fn from(column: Column) -> Self {
        DataFrame::from(vec![column])
    }
}

impl From<Vec<Column>> for DataFrame {
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
        DataFrame {
            schema,
            n_threads,
            data,
        }
    }
}

impl From<Data> for DataFrame {
    /// Construct a new `DataFrame` with the given `scalar` value.
    fn from(scalar: Data) -> Self {
        let c = match scalar {
            Data::Bool(x) => Column::Bool(vec![Some(x)]),
            Data::Int(x) => Column::Int(vec![Some(x)]),
            Data::Float(x) => Column::Float(vec![Some(x)]),
            Data::String(x) => Column::String(vec![Some(x)]),
            Data::Null => panic!("Can't make a DataFrame from a null value"),
        };
        DataFrame::from(c)
    }
}

impl std::fmt::Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in 0..self.n_rows() {
            for j in 0..self.n_cols() {
                write!(f, "<{}>", self.get(j, i).unwrap())?;
            }
            write!(f, "\n")?;
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

        fn join(&mut self, other: &Self) -> Self {
            self.sum += other.sum;
            self.clone()
        }
    }

    fn init() -> DataFrame {
        let s = Schema::from(vec![DataType::Int]);
        let mut r = Row::new(&s);
        let mut df = DataFrame::new(&s);

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
    fn test_filter() {
        let df = init();
        let mut rower = PosIntSummer { sum: 0 };
        let df2 = df.filter(&mut rower);
        assert_eq!(df2.n_rows(), 501);
        assert_eq!(df2.n_cols(), 1);
        assert_eq!(df2.get(0, 10).unwrap(), Data::Int(19));
    }
}
