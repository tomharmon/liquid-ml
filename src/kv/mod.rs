//! A module for distributed `Key`, `Value` stores. Utilizes the `network`
//! module to communicate between nodes.

use crate::dataframe::DataFrame;
use crate::network::client::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

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
    pub fn new(name: String, home: usize) -> Self {
        Key { name, home }
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
    // TODO: Change to an LRU
    /// An `LRU` cache of deserialized `DataFrame`s, not all of which are owned
    /// by this `KVStore`.
    cache: RwLock<HashMap<Key, DataFrame>>,
    /// The `network` layer, used to send and receive messages and data with
    /// other `KVStore`s
    network: Arc<RwLock<Client<KVMessage>>>,
    /// Used internally for processing data and messages
    internal_notifier: Notify,
    /// Used internally for processing network messages
    network_notifier: Arc<Notify>,
    /// The `id` of the node this `KVStore` is running on
    id: usize,
}

/// Represents the kind of messages that can be sent between distributed
/// `KVStore`s
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KVMessage {
    // TODO: difference between `Put` and `Data`? I think we can safely delete
    //      one of them
    Put(Key, Value),
    /// A message used to request the data for the given `Key` from other nodes
    Get(Key),
    /// A message used to send a `Key` and its `Value`, usually in response to
    /// `Get` messages
    Data(Key, Value),
}
