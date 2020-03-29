//! A module for distributed `Key`, `Value` stores. Utilizes the `network`
//! module to communicate between nodes.

use crate::dataframe::DataFrame;
use crate::network::client::Client;
use associative_cache::indices::HashDirectMapped;
use associative_cache::replacement::lru::LruReplacement;
use associative_cache::{AssociativeCache, Capacity, WithLruTimestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Notify, RwLock};

/// A struct used in the `associative_cache` crate
pub(crate) struct Capacity1GB;
/// Determines the maximum capacity for the cache from the `associative_cache`
/// crate. This is implementation is for a maximum size of 1GB. The
/// capacity is eagerly allocated once and never resized.
impl Capacity for Capacity1GB {
    /// The number of bytes in 1GB
    const CAPACITY: usize = 1_073_741_824;
}

pub mod kv_store;

/// A `Key` defines where in a `KVStore` a `Value` is stored, as well as
/// which node (and thus which `KVStore`) 'owns' the `Value`
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Debug, Clone)]
pub struct Key {
    /// Defines where in a `KVStore` a value is stored
    name: String,
    /// Defines which node 'owns' the associated `Value`
    home: usize,
}

/// A serialized blob of data. Is associated with a `Key` which defines where
/// this `Value` is stored in a `KVStore`, as well as its 'owner'
pub type Value = Vec<u8>;

/// Defines methods for a `Key`
impl Key {
    /// Creates a new `Key` that is owned by the `KVStore` running on the id
    /// equal to `home`. The given `name` defines where in the `KVStore` the
    /// value is stored.
    pub fn new(name: &str, home: usize) -> Self {
        Key {
            name: String::from(name),
            home,
        }
    }
}

/// A `Key`, `Value` store. `Key`s know which node the values 'belong' to. A
/// `KVStore` has methods to get and put data on other `KVStore`s. Internally
/// `KVStore`s store their data in memory as serialized blobs (`Vec<u8>`). The
/// `KVStore`s also cache deserialized `DataFrame`s on a least-recently used
/// basis.
pub struct KVStore {
    /// The data owned by this `KVStore`
    data: RwLock<HashMap<Key, Value>>,
    /// An `LRU` cache of deserialized `DataFrame`s, not all of which are owned
    /// by this `KVStore`.
    cache: RwLock<
        AssociativeCache<
            Key,
            WithLruTimestamp<DataFrame>,
            Capacity1GB,
            HashDirectMapped,
            LruReplacement,
        >,
    >,
    /// The `network` layer, used to send and receive messages and data with
    /// other `KVStore`s
    network: Arc<RwLock<Client<KVMessage>>>,
    /// Used internally for processing data and messages
    internal_notifier: Notify,
    /// Used internally for processing network messages
    network_notifier: Arc<Notify>,
    /// The `id` of the node this `KVStore` is running on
    id: usize,
    /// A channel to send blobs of data to a higher level
    blob_sender: Sender<Value>,
}

/// Represents the kind of messages that can be sent between distributed
/// `KVStore`s
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KVMessage {
    /// A message used to tell other clients to put the provided data in their local store
    Put(Key, Value),
    /// A message used to request the data for the given `Key` from other nodes
    Get(Key),
    /// A message used to send a `Key` and its `Value`, in response to `Get` messages
    Data(Key, Value),
    /// A message used to share random blobs of data with other nodes. This provides a lower level
    /// interface to facilitate other kinds of messages
    Blob(Value),
}
