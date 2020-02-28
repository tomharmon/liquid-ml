//! A Schema module for managing the data types and row/column names of a
//! DataFrame.

use sorer::schema::DataType;

/// Represents a [`Schema`](::crate::schema::Schema) of a
/// [`DataFrame`](::crate::dataframe::DataFrame)
pub struct Schema {
    pub schema: Vec<DataType>,
    pub col_names: Vec<Option<String>>,
    pub row_names: Vec<Option<String>>,
}

impl Schema {
    /// Constructs an empty Schema.
    pub fn new() -> Self {
        Schema {
            schema: Vec::new(),
            col_names: Vec::new(),
            row_names: Vec::new(),
        }
    }

    /// Add a column with the given `data_type`, with an optional column name,
    /// to this Schema. Column names must be unique. If `col_name` is `Some`
    /// and the name already exists in this `Schema`, the column will not
    /// be added to this Schema.
    ///
    /// | Character | DataType |
    /// |-----------|----------|
    /// | 'B'       | Bool     |
    /// | 'I'       | Int      |
    /// | 'F'       | Float    |
    /// | 'S'       | String   |
    pub fn add_column(&mut self, data_type: DataType, col_name: Option<String>) {
        match &col_name {
            Some(_name) => {
                if !self.col_names.contains(&col_name) {
                    self.schema.push(data_type);
                    self.col_names.push(col_name);
                }
            }
            None => {
                self.schema.push(data_type);
                self.col_names.push(None);
            }
        }
    }

    /// Add a row to this `Schema`. If `row_name` is `Some` and the name
    /// already exists in this `Schema`, the row will not be added.
    pub fn add_row(&mut self, row_name: Option<String>) {
        match &row_name {
            Some(_name) => {
                if !self.row_names.contains(&row_name) {
                    self.row_names.push(row_name);
                }
            }
            None => self.row_names.push(None),
        }
    }

    /// Gets the (optional) name of the row at the given `idx`.
    ///
    /// # Safety
    /// Panics if `idx` is out of bounds.
    pub fn row_name(&self, idx: usize) -> &Option<String> {
        self.row_names.get(idx).unwrap()
    }

    /// Gets the (optional) name of the column at the given `idx`.
    ///
    /// # Safety
    /// Panics if `idx` is out of bounds.
    pub fn col_name(&self, idx: usize) -> &Option<String> {
        self.col_names.get(idx).unwrap()
    }

    /// Get the data type of the column at the given `idx`
    ///
    /// # Safety
    /// Panics if `idx` is out of bounds.
    pub fn col_type(&self, idx: usize) -> &DataType {
        // NOTE: Official API returns a char, do we have to have that?
        self.schema.get(idx).unwrap()
    }

    /// Given a column name, returns its index
    pub fn col_idx(&self, col_name: &str) -> Option<usize> {
        Schema::get_idx_of_optional_name(&self.col_names, col_name)
    }

    /// Given a row name, returns its index
    pub fn row_idx(&self, row_name: &str) -> Option<usize> {
        Schema::get_idx_of_optional_name(&self.row_names, row_name)
    }

    /// The number of columns in this Schema.
    pub fn width(&self) -> usize {
        self.col_names.len()
    }

    /// The number of rows in this Schema.
    pub fn length(&self) -> usize {
        self.row_names.len()
    }

    fn get_idx_of_optional_name(names: &Vec<Option<String>>, name: &str) -> Option<usize> {
        names.iter().position(|n| match n {
            Some(col_name) => col_name == name,
            None => false,
        })
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
    /// Create a Schema from a `&str` of types. A string that contains
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
        let mut col_names = Vec::new();
        for c in types.chars() {
            schema.push(Schema::char_to_data_type(c));
            col_names.push(None);
        }
        Schema {
            schema,
            col_names,
            row_names: Vec::new(),
        }
    }
}

impl From<Vec<DataType>> for Schema {
    /// Create a Schema from a `Vec<DataType`
    fn from(types: Vec<DataType>) -> Self {
        let mut schema = Vec::new();
        let mut col_names = Vec::new();
        for t in types {
            schema.push(t);
            col_names.push(None);
        }
        Schema {
            schema,
            col_names,
            row_names: Vec::new(),
        }
    }
}
