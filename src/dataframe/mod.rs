//! A module for creating and manipulating `DataFrame`s. A `DataFrame` can be
//! created from a [`SoR`](https://docs.rs/sorer/0.1.0/sorer/) file,
//! or by adding `Column`s or `Row`s manually.
//!
//! The `DataFrame` is lightly inspired by those found in `R` or `pandas`, and
//! supports optionally named columns. You may analyze the data in a
//! `DataFrame` across many distributed machines in a horizontally scalable
//! manner by implementing the `Rower` trait to perform `map` or `filter`
//! operations on a `DataFrame`.
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
    /// The data of this DataFrame, in columnar format
    pub data: Vec<Column>,
    /// Number of threads for this computer
    pub n_threads: usize,
}

/// Represents a distributed `DataFrame` which uses a local `KVStore` and a
/// `Vec<Key>` of all `Key`s referring to `DataFrame`s which live on other
/// nodes in order to abstract over the networking and provide the same
/// functionality from the `DataFrame` trait as a local `DataFrame` struct.
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
    /// A message used to tell the 1st node what ranges DistributedDataFrame
    /// nodes have after filtering
    FilterResult {
        num_rows: usize,
        filtered_df_key: Option<Key>,
    },
    /// A message used to share random blobs of data with other nodes. This
    /// provides a lower level interface to facilitate other kinds of messages
    Blob(Vec<u8>),
    /// To tell other DDFs whats up TODO:
    Initialization {
        schema: Schema,
        df_chunk_map: HashMap<Range<usize>, Key>,
    },
    /// Used by the last node to tell the first node that they are ready for
    /// the `Initialization` message
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
