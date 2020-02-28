use crate::fielder::Fielder;
use crate::schema::Schema;
use sorer::dataframe::Data;
use sorer::schema::DataType;

/// Represents a single row in a [`DataFrame`](sorer::dataframe::DataFrame)
pub struct Row<'a> {
    /// A reference to the [`Schema`](::crate::schema::Schema) of this
    /// [`DataFrame`](sorer::dataframe::DataFrame)
    pub(crate) schema: &'a Schema,
    /// The data of this `Row` as boxed values
    pub(crate) data: Vec<Data>,
    /// The offset of this `Row` in the `DataFrame`
    idx: Option<usize>,
}

fn out_of_bounds_or_bad_type(data_type: &str) -> String {
    format!(
        "Error: index out of bounds or the column's datatype was not {}",
        data_type
    )
}

impl<'a> Row<'a> {
    /// Constructs a new row with the given `Schema`
    pub fn new(schema: &'a Schema) -> Self {
        let mut data: Vec<Data> = Vec::new();
        for _ in &schema.schema {
            data.push(Data::Null);
        }

        Row {
            schema,
            data,
            idx: None,
        }
    }

    pub fn set_int(&mut self, col_idx: usize, data: i64) {
        if let Some(DataType::Int) = self.schema.schema.get(col_idx) {
            match self.data.get(col_idx).unwrap() {
                Data::Null | Data::Int(_) => *self.data.get_mut(col_idx).unwrap() = Data::Int(data),
                _ => unreachable!("Something is horribly wrong"),
            }
        } else {
            panic!(out_of_bounds_or_bad_type("int"))
        }
    }

    pub fn set_float(&mut self, col_idx: usize, data: f64) {
        if let Some(DataType::Float) = self.schema.schema.get(col_idx) {
            match self.data.get(col_idx).unwrap() {
                Data::Null | Data::Float(_) => {
                    *self.data.get_mut(col_idx).unwrap() = Data::Float(data)
                }
                _ => unreachable!("Something is horribly wrong"),
            }
        } else {
            panic!(out_of_bounds_or_bad_type("float"))
        }
    }

    pub fn set_bool(&mut self, col_idx: usize, data: bool) {
        if let Some(DataType::Bool) = self.schema.schema.get(col_idx) {
            match self.data.get(col_idx).unwrap() {
                Data::Null | Data::Bool(_) => {
                    *self.data.get_mut(col_idx).unwrap() = Data::Bool(data)
                }
                _ => unreachable!("Something is horribly wrong"),
            }
        } else {
            panic!(out_of_bounds_or_bad_type("bool"))
        }
    }

    pub fn set_string(&mut self, col_idx: usize, data: String) {
        if let Some(DataType::String) = self.schema.schema.get(col_idx) {
            match self.data.get(col_idx).unwrap() {
                Data::Null | Data::String(_) => {
                    *self.data.get_mut(col_idx).unwrap() = Data::String(data)
                }
                _ => unreachable!("Something is horribly wrong"),
            }
        } else {
            panic!(out_of_bounds_or_bad_type("String"))
        }
    }

    pub fn set_null(&mut self, col_idx: usize) {
        match self.data.get(col_idx) {
            Some(_) => *self.data.get_mut(col_idx).unwrap() = Data::Null,
            _ => panic!("Index out of bounds error"),
        }
    }

    pub fn set_idx(&mut self, idx: usize) {
        self.idx = Some(idx);
    }

    pub fn get_idx(&self) -> usize {
        match self.idx {
            Some(index) => index,
            None => panic!("Error: the index of this row has not been set"),
        }
    }

    pub fn get(&self, idx: usize) -> &Data {
        self.data
            .get(idx)
            .unwrap_or_else(|| panic!("Error: index out of bounds"))
    }

    pub fn width(&self) -> usize {
        self.data.len()
    }

    pub fn col_type(&self, idx: usize) -> &DataType {
        // NOTE: official api returns a char lol
        self.schema
            .schema
            .get(idx)
            .unwrap_or_else(|| panic!("Error: index out of bounds"))
    }

    pub fn accept<T: Fielder>(&self, f: &mut T) {
        let idx = self.get_idx();
        f.start(idx);

        for data in &self.data {
            match data {
                Data::Int(d) => f.visit_int(*d),
                Data::Bool(d) => f.visit_bool(*d),
                Data::Float(d) => f.visit_float(*d),
                Data::String(d) => f.visit_string(&d),
                Data::Null => f.visit_null(),
            }
        }

        f.done();
    }
}
