//! A module for creating and manipulating `DataFrame`s. A `DataFrame` can be
//! created from a [`SoR`](https://docs.rs/sorer/0.1.0/sorer/) file,
//! or by adding `Column`s or `Row`s manually.
//!
//! The `DataFrame` is lightly inpsired by those found in `R` or `pandas`, and
//! supports optionally named columns and rows. You may analyze the data in a
//! `DataFrame` in a horizontally scalable manner across many machines by
//! implementing the `Rower` trait to perform `map` or `filter` operations on a
//! `DataFrame`.

use serde::{Deserialize, Serialize};
use sorer::dataframe::{Column, Data};
use sorer::schema::DataType;

pub mod dataframe;
pub mod fielder;
pub mod row;
pub mod rower;
pub mod schema;

/// Represents a DataFrame which contains `Data` stored in a columnar format
/// and a well-defined `Schema`
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct DataFrame {
    /// The `Schema` of this `DataFrame`
    pub schema: Schema,
    /// The data of this DataFrame, in columnar format
    pub data: Vec<Column>,
    /// Number of threads for this computer
    pub n_threads: usize,
}

/// Represents a `Schema` of a `DataFrame`
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Schema {
    /// The `DataType`s of this `Schema`
    pub schema: Vec<DataType>,
    /// The optional names of each `Column`, which must be unique if they are
    /// `Some`
    pub col_names: Vec<Option<String>>,
    /// The optional names of each `Row`, which must be unique if they are
    /// `Some`
    pub row_names: Vec<Option<String>>,
}

/// Represents a single row in a `DataFrame`
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Row {
    /// A clone of the `Schema` of the `DataFrame` this `Row` is from.
    pub(crate) schema: Vec<DataType>,
    /// The data of this `Row` as boxed values.
    pub(crate) data: Vec<Data>,
    /// The offset of this `Row` in the `DataFrame`
    idx: Option<usize>,
}
