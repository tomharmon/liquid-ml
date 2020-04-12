//! A module for creating and manipulating `DataFrame`s. A `DataFrame` can be
//! created from a [`SoR`](https://docs.rs/sorer/0.1.0/sorer/) file,
//! or by adding `Column`s or `Row`s programmatically.
//!
//! The `DataFrame` is lightly inspired by those found in `R` or `pandas`, and
//! supports optionally named columns. You may analyze the data in a
//! `DataFrame` across many distributed machines in a horizontally scalable
//! manner by implementing the `Rower` trait to perform `map` or `filter`
//! operations on a `DataFrame`. A DataFrame is stored in columnar format to 
//! support high performance data  analytics fast. 
//!
//! The DataFrame module provides 2 implementations for a DataFrame:
//!  - a [`Local DataFrame`](crate::dataframe::local_dataframe) which can be 
//!  used to analyze data on a node locally and removes network latency
//!  - a [`Distributed DataFrame`](crate::dataframe::distributed_dataframe) which
//!  upon creation will distribute its data across multiple nodes and provides 
//!  distributed versions of map and filter operations. 
//!
//! DataFrames use these supplementary data structures and can be useful in 
//! analyzing DataFrames:
//!  - `Row` : Which is a single row of Data from the dataframe and provides a 
//!  useful API to help write `Rowers`
//!  - `Schema` : This can be especially useful whena SoR File is read and 
//!  different things need to be done based on the inferred schema
//!
//! The DataFrame also declares the rower and fielder visitor traits that can be 
//! used to build visitors that iterate over the elements of a row or dataframe.
//!
//! NOTE: RFC to add iterators along with the current visitors, since these are 
//! cleaner to write in rust
use crate::kv::{KVStore, Key};
use crate::network::Client;
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};
pub use sorer::{
    dataframe::{Column, Data},
    schema::DataType,
};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex, Notify, RwLock};

mod distributed_dataframe;
mod local_dataframe;
mod row;
mod schema;

/// Represents a local `DataFrame` which contains `Data` stored in a columnar
/// format and a well-defined `Schema`
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, DeepSizeOf)]
pub struct LocalDataFrame {
    /// The `Schema` of this `DataFrame`
    pub schema: Schema,
    /// The data of this data frame, in columnar format
    pub data: Vec<Column>,
    /// Number of threads for this computer
    pub n_threads: usize,
}

/// Represents a distributed data frame which uses a local `KVStore` and a
/// collection of `Key`s for all the chunks in this data frame. Provides
/// convenient `map` and `filter` methods that operate on the entire
/// distributed data frame with a given `Rower`.
#[derive(Debug)]
pub struct DistributedDataFrame {
    /// The `Schema` of this `DistributedDataFrame`
    pub schema: Schema,
    /// The name of this `DistributedDataFrame`
    pub df_name: String,
    /// Keys that point to data in different nodes.
    pub df_chunk_map: HashMap<Range<usize>, Key>,
    /// cached, ddf is immutable
    pub num_rows: usize,
    /// The id of the node this `DistributedDataFrame` is running on
    pub node_id: usize,
    /// How many nodes are there in this DDF?
    pub num_nodes: usize,
    /// What's the address of the `Server`?
    pub server_addr: String,
    /// What's my IP address?
    pub my_ip: String,
    /// Used for communication with other nodes in this DDF
    network: Arc<RwLock<Client<DistributedDFMsg>>>,
    /// The `KVStore`
    kv: Arc<KVStore<LocalDataFrame>>,
    /// Used for processing messages: TODO better explanation
    internal_notifier: Arc<Notify>,
    /// Used for sending rows back and forth TODO: better explanation
    row: Arc<RwLock<Row>>,
    /// A notifier that gets notified when the `Server` has sent a `Kill`
    /// message to this `DistributedDataFrame`'s network `Client`
    kill_notifier: Arc<Notify>,
    /// Used for lower level messages TODO: better explanation
    blob_receiver: Mutex<Receiver<Vec<u8>>>,
    /// Used for processing filter results TODO: maybe a better way to do this
    filter_results: Mutex<Receiver<DistributedDFMsg>>,
}

/// Represents the kinds of messages sent between `DistributedDataFrame`s
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum DistributedDFMsg {
    /// A messaged used to request a `Row` with the given index from another
    /// node in a `DistributedDataFrame`
    GetRow(usize),
    /// A message used to respond to `GetRow` messages with the requested row
    Row(Row),
    /// A message used to tell the 1st node the results from using the `filter`
    /// method. If there were no rows after filtering, then `filtered_df_key`
    /// is `None` and `num_rows` is `0`.
    FilterResult {
        num_rows: usize,
        filtered_df_key: Option<Key>,
    },
    /// A message used to share random blobs of data with other nodes. This
    /// provides a lower level interface to facilitate other kinds of messages,
    /// for example sending rowers when performing `map`/`filter`.
    Blob(Vec<u8>),
    /// Used to inform other nodes in a `DistributedDataFrame` the required
    /// information for other nodes to construct a new `DistributedDataFrame`
    /// struct that is consistent across all nodes.
    Initialization {
        schema: Schema,
        df_chunk_map: HashMap<Range<usize>, Key>,
    },
    /// Used by nodes to co-ordinate connection to the `Server` so nodes
    /// preserve the correct ids, and to notify node 1 when to send the
    /// `Initialization` message
    Ready,
}

/// Represents a `Schema` of a `DataFrame`
#[derive(
    Serialize, Deserialize, PartialEq, Clone, Debug, Default, DeepSizeOf,
)]
pub struct Schema {
    /// The `DataType`s of this `Schema`
    pub schema: Vec<DataType>,
    /// The optional names of each `Column`, which must be unique if they are
    /// `Some`
    /// pub col_names: Vec<Option<String>>,
    /// A reverse col_name map for all the named columns to make getting
    /// index by column name faster
    pub col_names : HashMap<String, usize>,
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
