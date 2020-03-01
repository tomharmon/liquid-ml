//! Structs and functions for working with rows of data in a `DataFrame`.

use crate::error::DFError;
use crate::fielder::Fielder;
use crate::schema::Schema;
use sorer::dataframe::Data;
use sorer::schema::DataType;

/// Represents a single row in a [`DataFrame`](::crate::dataframe::DataFrame)
pub struct Row {
    /// A clone of the [`Schema`](::crate::schema::Schema) of the
    /// [`DataFrame`](::crate::dataframe::DataFrame) this `Row` is from.
    pub(crate) schema: Vec<DataType>,
    /// The data of this `Row` as boxed values.
    pub(crate) data: Vec<Data>,
    /// The offset of this `Row` in the `DataFrame`, should ideally be set for the row.
    idx: Option<usize>,
}

/// Functions for creating, mutating, and getting data from `Row`s.
impl Row {
    /// Constructs a new `Row` with the given `Schema` and fills it with
    /// [`Null`](sorer::dataframe::Data) values.
    pub fn new(schema: &Schema) -> Self {
        let mut data: Vec<Data> = Vec::new();
        for _ in &schema.schema {
            data.push(Data::Null);
        }

        Row {
            schema: schema.schema.clone(),
            data,
            idx: None,
        }
    }

    /// Set a field in the row to have the given `data`. The `DataType` at the
    /// `col_idx` must be an `Int` or will return an `TypeMismatch` error.
    pub fn set_int(
        &mut self,
        col_idx: usize,
        data: i64,
    ) -> Result<(), DFError> {
        match self.schema.get(col_idx) {
            Some(DataType::Int) => Ok(match self.data.get(col_idx).unwrap() {
                Data::Null | Data::Int(_) => {
                    *self.data.get_mut(col_idx).unwrap() = Data::Int(data)
                }
                _ => panic!("Something is horribly wrong"),
            }),
            None => Err(DFError::ColIndexOutOfBounds),
            _ => Err(DFError::TypeMismatch),
        }
    }

    /// Set a field in the row to have the given `data`. The `DataType` at the
    /// `col_idx` must be a `Float` or will return an `TypeMismatch` error.
    pub fn set_float(
        &mut self,
        col_idx: usize,
        data: f64,
    ) -> Result<(), DFError> {
        match self.schema.get(col_idx) {
            Some(DataType::Float) => {
                Ok(match self.data.get(col_idx).unwrap() {
                    Data::Null | Data::Float(_) => {
                        *self.data.get_mut(col_idx).unwrap() = Data::Float(data)
                    }
                    _ => panic!("Something is horribly wrong"),
                })
            }
            None => Err(DFError::ColIndexOutOfBounds),
            _ => Err(DFError::TypeMismatch),
        }
    }

    /// Set a field in the row to have the given `data`. The `DataType` at the
    /// `col_idx` must be a `Bool` or will return an `TypeMismatch` error.
    pub fn set_bool(
        &mut self,
        col_idx: usize,
        data: bool,
    ) -> Result<(), DFError> {
        match self.schema.get(col_idx) {
            Some(DataType::Bool) => Ok(match self.data.get(col_idx).unwrap() {
                Data::Null | Data::Bool(_) => {
                    *self.data.get_mut(col_idx).unwrap() = Data::Bool(data)
                }
                _ => panic!("Something is horribly wrong"),
            }),
            None => Err(DFError::ColIndexOutOfBounds),
            _ => Err(DFError::TypeMismatch),
        }
    }

    /// Set a field in the row to have the given `data`. The `DataType` at the
    /// `col_idx` must be a `String` or will return an `TypeMismatch` error.
    pub fn set_string(
        &mut self,
        col_idx: usize,
        data: String,
    ) -> Result<(), DFError> {
        match self.schema.get(col_idx) {
            Some(DataType::String) => {
                Ok(match self.data.get(col_idx).unwrap() {
                    Data::Null | Data::String(_) => {
                        *self.data.get_mut(col_idx).unwrap() =
                            Data::String(data)
                    }
                    _ => panic!("Something is horribly wrong"),
                })
            }
            None => Err(DFError::ColIndexOutOfBounds),
            _ => Err(DFError::TypeMismatch),
        }
    }

    /// Set an field in the row to `Null`.
    pub fn set_null(&mut self, col_idx: usize) -> Result<(), DFError> {
        match self.data.get(col_idx) {
            Some(_) => {
                *self.data.get_mut(col_idx).unwrap() = Data::Null;
                Ok(())
            }
            _ => Err(DFError::ColIndexOutOfBounds),
        }
    }

    /// Set the row offset in the daaframe for this `Row`.
    pub fn set_idx(&mut self, idx: usize) {
        self.idx = Some(idx);
    }

    /// Get the current index of this `Row`.
    pub fn get_idx(&self) -> Result<usize, DFError> {
        match self.idx {
            Some(index) => Ok(index),
            None => Err(DFError::NotSet),
        }
    }

    /// Get a reference of the boxed value at the given `idx`.
    pub fn get(&self, idx: usize) -> Result<&Data, DFError> {
        match self.data.get(idx) {
            Some(d) => Ok(d),
            None => Err(DFError::ColIndexOutOfBounds),
        }
    }

    /// Get the number of columns in this `Row`.
    pub fn width(&self) -> usize {
        self.data.len()
    }

    /// Get the `DataType` of the `Column` at the given `idx`.
    pub fn col_type(&self, idx: usize) -> Result<&DataType, DFError> {
        match self.schema.get(idx) {
            Some(d) => Ok(d),
            None => Err(DFError::ColIndexOutOfBounds),
        }
    }

    /// Accept a visitor for this row that vists all the elements in this `Row`.
    pub fn accept<T: Fielder>(&self, f: &mut T) -> Result<(), DFError> {
        let idx = match self.get_idx() {
            Ok(i) => i,
            Err(e) => return Err(e),
        };
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
        Ok(())
    }
}
