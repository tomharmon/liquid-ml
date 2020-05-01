//! A module for creating and manipulating data frames. A data frame can be
//! created from a [`SoR`] file, or by adding [`Column`]s or [`Row`]s
//! programmatically.
//!
//! A data frame in `liquid_ml` is lightly inspired by those found in `R` or
//! `pandas`, and supports optionally named columns. You may analyze the data
//! in a data frame by implementing the [`Rower`] trait to perform `map` or
//! `filter` operations. These operations can be easily performed on either
//! [`LocalDataFrame`]s for data that fits in memory or
//! [`DistributedDataFrame`]s for data that is too large to fit in one machine.
//!
//! **Note**: If you need a [`DistributedDataFrame`], it is highly recommended
//! that you check out the [`LiquidML`] struct since that provides many
//! convenient helper functions for working with [`DistributedDataFrame`]s.
//! Using a [`DistributedDataFrame`] directly is only recommended if you really
//! know what you are doing. There are also helpful examples of `map` and
//! `filter` in the [`LiquidML`] documentation
//!
//! This `dataframe` module provides 2 implementations for a data frame:
//!
//! # [`LocalDataFrame`]
//!
//! A [`LocalDataFrame`] can be used to analyze data on a node locally for data
//! that fits in memory. Is very easy to work with and get up and running
//! initially when developing. We recommend that when testing and developing
//! your [`Rower`], that you do so with a [`LocalDataFrame`].
//!
//! # [`DistributedDataFrame`]
//!
//! A [`DistributedDataFrame`] is an abstraction over a distributed system of
//! nodes that run [`KVStore`]s which contain chunks of [`LocalDataFrame`]s.
//! Therefore each [`DistributedDataFrame`] simply holds a pointer to a
//! [`KVStore`] and a map of ranges of row indices to the [`Key`]s for the
//! chunks of data with that range of row indices. A [`DistributedDataFrame`]
//! is immutable to make it trivial for the global state of the data frame to
//! be consistent.
//!
//! Because of this the [`DistributedDataFrame`] implementation is mainly
//! concerned with networking and getting and putting chunks of different
//! [`KVStore`]s. One of the main concerns are that creating a new
//! [`DistributedDataFrame`] means distributing the [`Key`]s of all the chunks
//! to all nodes and the chunks to their respective owner.
//!
//! Upon creation, node 1 of a [`DistributedDataFrame`] will distribute chunks
//! of data across multiple nodes from [`SoR`] files, iterators, and other
//! convenient ways of adding data. Note that our experimental testing found
//! that using the largest chunks possible to fit on each node increased
//! performance by over `2x`. Our [`from_sor`] constructor optimizes for large
//! chunks, but we have no control over the iterators passed in to
//! [`from_iter`], so if you are using this function yourself and care about
//! the performance of `map` and `filter`, then you should also optimize your
//! iterators this way.
//!
//! Data frames use these supplementary data structures and can be useful in
//! understanding DataFrames:
//!  - [`Row`] : A single row of [`Data`] from the data frame and provides a
//!     useful API to help implement the [`Rower`] trait
//!  - [`Schema`] : This can be especially useful when a [`SoR`] File is read and
//!     different things need to be done based on the inferred schema
//!
//! The `dataframe` module also declares the [`Rower`] and [`Fielder`] visitor
//! traits that can be used to build visitors that iterate over the elements of
//! a row or data frame.
//!
//! NOTE: We are likely to add iterators to replace the current visitors, since
//! iterators are more idiomatic to write in rust
//!
//! [`Column`]: struct.Column.html
//! [`Row`]: struct.Row.html
//! [`Rower`]: trait.Rower.html
//! [`Fielder`]: trait.Fielder.html
//! [`Schema`]: struct.Schema.html
//! [`Data`]: struct.Data.html
//! [`LocalDataFrame`]: struct.LocalDataFrame.html
//! [`DistributedDataFrame`]: struct.DistributedDataFrame.html
//! [`LiquidML`]: ../struct.LiquidML.html
//! [`KVStore`]: ../kv/struct.KVStore.html
//! [`Key`]: ../kv/struct.Key.html
//! [`SoR`]: https://docs.rs/sorer
//! [`from_sor`]: struct.DistributedDataFrame.html#method.from_sor
//! [`from_iter`]: struct.DistributedDataFrame.html#method.from_iter
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
/// elements of a [`Row`].
///
/// [`Row`]: struct.Row.html
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
/// data frame.
pub trait Rower {
    /// This function is called once per row of a data frame.  The return value
    /// is used in `filter` methods to indicate whether a row should be kept,
    /// and is meaningless when using `map`.
    ///
    /// # Data Frame Mutability
    /// Since the `row` that is visited is only an immutable reference, it is
    /// impossible to mutate a data frame via `map`/`filter` since you can't
    /// mutate each [`Row`] when visiting them. If you wish to get around this
    /// (purposeful) limitation, you may define a [`Rower`] that has a
    /// [`LocalDataFrame`] for one of its fields. Then, in your `visit`
    /// implementation, you may clone each [`Row`] as you visits them, mutate
    /// them, then adds it to your [`Rower`]'s copy of the [`LocalDataFrame`].
    /// This way you will have the original and the mutated copy after
    /// `map`/`filter`.
    ///
    /// [`Row`]: struct.Row.html
    /// [`Rower`]: trait.Rower.html
    /// [`LocalDataFrame`]: struct.LocalDataFrame.html
    fn visit(&mut self, row: &Row) -> bool;

    /// In all cases, except when using single-threaded `map` with a
    /// [`LocalDataFrame`], the [`Rower`]s being executed in separate threads
    /// or machines will need to be joined and combined to obtain the final
    /// result. This may be as simple as adding up each [`Rower`]s sum to get
    /// a total sum or may be much more complicated. In most cases, it is
    /// usually trivial. The returned [`Rower`] will contain the final results.
    fn join(self, other: Self) -> Self;
}
