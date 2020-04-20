//! A module for creating and manipulating data frame`s. A data frame can be
//! created from a [`SoR`](https://docs.rs/sorer/0.1.0/sorer/) file,
//! or by adding `Column`s or `Row`s programmatically.
//!
//! A data frame in `liquid_ml` is lightly inspired by those found in `R` or
//! `pandas`, and supports optionally named columns. You may analyze the data
//! in a data frame across many distributed machines in a horizontally scalable
//! manner by implementing the `Rower` trait to perform `map` or `filter`
//! operations.
//!
//! The `dataframe` module provides 2 implementations for a data frame:
//!
//! # [`LocalDataFrame`](crate::dataframe::local_dataframe)
//!
//! A [`LocalDataFrame`](crate::dataframe::local_dataframe) which can be
//! used to analyze data on a node locally for data that fits in memory.
//!
//! # [`DistributedDataFrame`](crate::dataframe::distributed_dataframe)
//!
//! A [`DistributedDataFrame`](crate::dataframe::distributed_dataframe) is
//! an abstraction over a distributed system of nodes that run `KVStore`s which
//! contain chunks of `LocalDataFrame`s. Therefore each `DistributedDataFrame`
//! simply holds a pointer to a `KVStore` and a map of ranges of row indices
//! to the `Key`s for the chunks of data with that range of row indices.
//!
//! Upon creation node 1 of a `DistributedDataFrame` will distribute data
//! across multiple nodes from `SoR` files, iterators, and other conveient ways
//! of adding data. Each chunk that node 1 distributes is as large as possible
//! while distributing the data evenly between nodes since experimental testing
//! found this was optimal for performance of `map` and `filter`.
//! Methods for distributed versions of these `map` and `filter` operations
//! are provided.
//!
//! **Note**: If you need these features of a `DistributedDataFrame`, it is
//! highly recommended that you check out the `LiquidML` struct since that
//! provides many convenient helper functions for working with
//! `DistributedDataFrame`s.  Using a `DistributedDataFrame` directly is only
//! recommended if you really know what you are doing.
//!
//! Data frames use these supplementary data structures and can be useful in
//! understanding DataFrames:
//!  - `Row` : A single row of `Data` from the data frame and provides a
//!     useful API to help implement the `Rower` trait
//!  - `Schema` : This can be especially useful when a `SoR` File is read and
//!     different things need to be done based on the inferred schema
//!
//! The `dataframe` module also declares the `Rower` and `Fielder` visitor
//! traits that can be used to build visitors that iterate over the elements of
//! a row or data frame.
//!
//! NOTE: RFC to add iterators along with the current visitors, since iterators
//! are more idiomatic to write in rust
pub use sorer::{
    dataframe::{Column, Data},
    schema::DataType,
};

mod distributed_dataframe;
pub use distributed_dataframe::DistributedDataFrame;

mod local_dataframe;
pub use local_dataframe::LocalDataFrame;

mod row;
pub use row::Row;

mod schema;
pub use schema::Schema;

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
    /// This function is called once per row. When used in conjunction with
    /// `pmap`, the row index of `r` is correctly set, meaning that the rower
    /// may make a copy of the DF it's mapping and mutate that copy, since
    /// the DF is not easily mutable. The return value is used in
    /// `DataFrame::filter` to indicate whether a row should be kept.
    fn visit(&mut self, r: &Row) -> bool;

    /// Once traversal of the `DataFrame` is complete the rowers that were
    /// cloned for parallel execution for `DataFrame::pmap` will be joined to
    /// obtain the final result.  There will be one join for each cloned
    /// `Rower`. The original `Rower` will be the last to be called join on,
    /// and that `Rower` will contain the final results.
    fn join(self, other: Self) -> Self;
}
