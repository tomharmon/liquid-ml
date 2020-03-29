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
pub use sorer::dataframe::{Column, Data};
use sorer::schema::DataType;

// hey, inception was a great movie, come on now
#[allow(clippy::module_inception)]
pub mod dataframe;
pub mod row;
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
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Default)]
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

/// A field visitor invoked by a `Row`.
pub trait Fielder {
    /// Must be called before visiting a row
    fn start(&mut self, starting_row_index: usize);

    /// Called for fields of type `bool` with the value of the field
    fn visit_bool(&mut self, b: bool);

    /// Called for fields of type `float` with the value of the field
    fn visit_float(&mut self, f: f64);

    /// Called for fields of type `int` with the value of the field
    fn visit_int(&mut self, i: i64);

    /// Called for fields of type `String` with the value of the field
    fn visit_string(&mut self, s: &str);

    /// Called for fields where the value of the field is missing
    fn visit_null(&mut self);

    /// Called when all fields have been seen
    fn done(&mut self);
}

/// A trait for vistors who iterate through and process each row of a
/// `DataFrame`. Rowers are cloned for parallel execution in `DataFrame::pmap`.
pub trait Rower {
    /// This function is called once per row. The `Row` object is on loan and
    /// should not be retained as it is going to be reused in the next
    /// call. The return value is used in filters to indicate that a row
    /// should be kept.
    fn visit(&mut self, r: &Row) -> bool;

    /// Once traversal of the `DataFrame` is complete the rowers that were
    /// split off will be joined.  There will be one join per split. The
    /// original object will be the last to be called join on.
    fn join(&mut self, other: &Self) -> Self;
}
