//! A module for creating and manipulating `DataFrame`s. A `DataFrame` can be
//! created from a [`SoR`](https://docs.rs/sorer/0.1.0/sorer/) file,
//! or by adding `Column`s or `Row`s manually.
//!
//! The `DataFrame` is lightly inspired by those found in `R` or `pandas`, and
//! supports optionally named columns. You may analyze the data in a
//! `DataFrame` across many distributed machines in a horizontally scalable
//! manner by implementing the `Rower` trait to perform `map` or `filter`
//! operations on a `DataFrame`.

use serde::{Deserialize, Serialize};
pub use sorer::{
    dataframe::{Column, Data},
    schema::DataType,
};

// hey, inception was a great movie, come on now
#[allow(clippy::module_inception)]
mod dataframe;
mod row;
mod schema;

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
}

/// Represents a single row in a `DataFrame`. Has a clone of the `DataFrame`s
/// `Schema` and holds data as a `Vec<Data>`.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Row {
    /// A clone of the `Schema` of the `DataFrame` this `Row` is from.
    pub(crate) schema: Vec<DataType>,
    /// The data of this `Row` as boxed values.
    pub(crate) data: Vec<Data>,
    /// The offset of this `Row` in the `DataFrame`
    idx: Option<usize>,
}

/// A field visitor that may be implemented to iterate and visit all the
/// elements of a `Row`.
pub trait Fielder {
    /// Called for fields of type `bool` with the value of the field
    fn visit_bool(&mut self, b: bool);

    /// Called for fields of type `float` with the value of the field
    fn visit_float(&mut self, f: f64);

    /// Called for fields of type `int` with the value of the field
    fn visit_int(&mut self, i: i64);

    /// Called for fields of type `String` with the value of the field
    fn visit_string(&mut self, s: &str);

    /// Called for fields where the value of the field is missing. This method
    /// may be as simple as doing nothing but there are use cases where
    /// some operations are required.
    fn visit_null(&mut self);
}

/// A trait for visitors who iterate through and process each row of a
/// `DataFrame`. In `DataFrame::pmap`, `Rower`s are cloned for parallel
/// execution.
pub trait Rower {
    /// This function is called once per row.  The return value is used in
    /// `DataFrame::filter` to indicate whether a row should be kept.
    fn visit(&mut self, r: &Row) -> bool;

    // TODO: join should take `self` to avoid unnecessary clones
    /// Once traversal of the `DataFrame` is complete the rowers that were
    /// cloned for parallel execution for `DataFrame::pmap` will be joined to
    /// obtain the final result.  There will be one join for each cloned
    /// `Rower`. The original `Rower` will be the last to be called join on,
    /// and that `Rower` will contain the final results.
    fn join(&mut self, other: &Self) -> Self;
}
