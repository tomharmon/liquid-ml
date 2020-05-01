//! A Schema module for managing the data types and row/column names of a
//! DataFrame.
use crate::error::LiquidError;
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};
use sorer::{dataframe::Column, schema::DataType};
use std::collections::HashMap;

/// Represents a `Schema` of a data frame
#[derive(
    Serialize, Deserialize, PartialEq, Clone, Debug, Default, DeepSizeOf,
)]
pub struct Schema {
    /// The `DataType`s of this `Schema`
    pub schema: Vec<DataType>,
    /// A reverse column name to column index map for all the named columns.
    /// Helps getting the index by column name faster.
    pub col_names: HashMap<String, usize>,
}

/// The implementation of the `Schema` interface, which manages data types and
/// row/column names of local and distributed data frames.
impl Schema {
    /// Constructs an empty `Schema`.
    pub fn new() -> Self {
        Schema {
            ..Default::default()
        }
    }

    /// Add a column with the given `data_type`, with an optional column name,
    /// to this `Schema`. Column names must be unique. If `col_name` is `Some`
    /// and the name already exists in this `Schema`, the column will not
    /// be added to this `Schema` and a `LiquidError::NameAlreadyExists` error
    /// will be returned.
    pub fn add_column(
        &mut self,
        data_type: DataType,
        col_name: Option<String>,
    ) -> Result<(), LiquidError> {
        if let Some(name) = col_name {
            if !self.col_names.contains_key(&name) {
                self.col_names.insert(name, self.schema.len());
            } else {
                return Err(LiquidError::NameAlreadyExists);
            }
        }
        self.schema.push(data_type);
        Ok(())
    }

    /// Get the data type of the column at the given `idx`
    /// Returns a result that will `Error` if the `idx` is out of bounds.
    pub fn col_type(&self, idx: usize) -> Result<&DataType, LiquidError> {
        match self.schema.get(idx) {
            Some(data_type) => Ok(data_type),
            None => Err(LiquidError::ColIndexOutOfBounds),
        }
    }

    /// Given a column name, returns its index
    pub fn col_idx(&self, col_name: &str) -> Option<usize> {
        match self.col_names.get(col_name) {
            Some(x) => Some(*x),
            None => None,
        }
    }

    /// Given a column index, returns its name
    pub fn col_name(
        &self,
        col_idx: usize,
    ) -> Result<Option<&str>, LiquidError> {
        if col_idx >= self.width() {
            return Err(LiquidError::ColIndexOutOfBounds);
        }
        match self.col_names.iter().find(|(_, &v)| v == col_idx) {
            Some((col_name, _)) => Ok(Some(col_name)),
            None => Ok(None),
        }
    }

    /// The number of columns in this Schema.
    pub fn width(&self) -> usize {
        self.schema.len()
    }

    fn char_to_data_type(c: char) -> DataType {
        match c {
            'B' => DataType::Bool,
            'I' => DataType::Int,
            'F' => DataType::Float,
            'S' => DataType::String,
            _ => panic!("Tried to make a bad Schema"),
        }
    }
}

impl From<&str> for Schema {
    /// Create a `Schema` from a `&str` of types. A string that contains
    /// characters other that `B`, `I`, `F`, or `S` will panic. Initializes
    /// the Column names to be `None`.
    ///
    /// | Character | DataType |
    /// |-----------|----------|
    /// | 'B'       | Bool     |
    /// | 'I'       | Int      |
    /// | 'F'       | Float    |
    /// | 'S'       | String   |
    fn from(types: &str) -> Self {
        let mut schema = Vec::new();
        for c in types.chars() {
            schema.push(Schema::char_to_data_type(c));
        }
        Schema {
            schema,
            col_names: HashMap::new(),
        }
    }
}

impl From<Vec<DataType>> for Schema {
    /// Create a `Schema` from a `Vec<DataType>`
    fn from(types: Vec<DataType>) -> Self {
        Schema {
            schema: types,
            col_names: HashMap::new(),
        }
    }
}

impl From<&Vec<Column>> for Schema {
    /// Create a `Schema` from a `Vec<Column>`
    fn from(columns: &Vec<Column>) -> Self {
        let mut schema = Vec::new();
        for c in columns {
            match c {
                Column::Bool(_) => schema.push(DataType::Bool),
                Column::Int(_) => schema.push(DataType::Int),
                Column::Float(_) => schema.push(DataType::Float),
                Column::String(_) => schema.push(DataType::String),
            };
        }
        Schema {
            schema,
            col_names: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_data_types() {
        let mut data_types = vec![];
        let mut s = Schema::from(data_types.clone());
        assert_eq!(s.width(), 0);
        data_types = vec![
            DataType::Int,
            DataType::Int,
            DataType::Float,
            DataType::Bool,
            DataType::String,
        ];
        s = Schema::from(data_types.clone());
        for (idx, data_type) in data_types.iter().enumerate() {
            assert_eq!(data_type, s.col_type(idx).unwrap());
        }
        assert_eq!(s.width(), data_types.len());
    }

    #[test]
    fn test_from_str() {
        let mut types_str = "";
        let mut s = Schema::from(types_str);
        assert_eq!(s.width(), 0);
        types_str = "IIFBS";
        s = Schema::from(types_str);
        let data_types = vec![
            DataType::Int,
            DataType::Int,
            DataType::Float,
            DataType::Bool,
            DataType::String,
        ];
        for (idx, data_type) in data_types.iter().enumerate() {
            assert_eq!(data_type, s.col_type(idx).unwrap());
        }
        assert_eq!(s.width(), data_types.len());
    }

    // add_column
    #[test]
    fn test_col_getters_setters() {
        // colummn getters/setters
        let mut s = Schema::new();
        assert_eq!(s.width(), 0);
        s.add_column(DataType::String, None).unwrap();
        assert_eq!(s.width(), 1);
        s.add_column(DataType::Int, Some(String::from("foo")))
            .unwrap();
        assert_eq!(s.width(), 2);
        assert_eq!(s.col_idx("foo"), Some(1));
    }
}
