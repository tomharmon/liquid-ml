//! A module for distributed `Key`, `Value` stores that hold their data in
//! memory and are generic for values of type `T`.
//!
//! A `KVStore` is essentially a very simple in-memory distributed database
//! (with no persistence) that stores data as a collection of key-value pairs
//! where a `Key` is a unique identifier to a `Value`. The `KVStore` utilizes
//! the `network` module to communicate between nodes using `KVMessage`s.
//!
//! Internally `KVStore`s store their data in memory as serialized blobs
//! (a `Value` aka a `Vec<u8>`). The `KVStore` caches deserialized `Value`s
//! into their type `T` on a least-recently used basis.
//!
//! ## Provided `KVStore` Functionality
//! - `get`: Retrieve data stored locally on a `KVStore` or in its cache
//! - `wait_and_get`: Retrieve data either locally or over the network
//! - `put`: Store a `Key`, `Value` pair either locally on a `KVStore` or send
//!    it over the network to store it on another `KVStore`
//! - `send_blob`: a lower level interface to facilitate sending any serialized
//!    data without storing it on a `KVStore`. In `liquid_ml`, used for
//!    sending `Rower`s when doing distributed functions at the `Application`
//!    level
//!
//! ## Message Processing
//!
//! The `KVStore` processes messages from the network by doing the following:
//! 1. Asynchronously await new messages from the `Client` that are
//!    sent over an `mpsc` channel.
//! 2. Spawn an asynchronous `tokio::task` to respond to the newly
//!    received message so as to not block further message processing.
//! 3. Based on the message type, do the following:
//!    - `Get` message: call `wait_and_get` to get the data, either
//!       internally from this `KVStore` or externally over the network
//!       from another one. Once we have the data, respond with a `Data`
//!       message containing the requested data.
//!    - `Data` message: Deserialize the data and put it into our cache
//!    - `Put` message: add the given data to our internal store
//!    - `Blob` message: send the data up a higher level similar to how
//!       the `Client` processes messages
use rand::{self, Rng};
use serde::{Deserialize, Serialize};

mod kv_store;
pub use crate::kv::kv_store::KVStore;

/// A `Key` defines where in a `KVStore` a `Value` is stored, as well as
/// which node (and thus which `KVStore`) 'owns' the `Value`
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Debug, Clone)]
pub struct Key {
    /// Defines where in a `KVStore` a value is stored
    pub name: String,
    /// Defines which node 'owns' the associated `Value`
    pub home: usize,
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

    /// Make a key with an automatically generated name
    pub(crate) fn generate(name: &str, home: usize) -> Self {
        let mut rng = rand::thread_rng();
        Key {
            name: format!("{}-{}-{}", name, home, rng.gen::<i16>()),
            home,
        }
    }
}

/// Represents the kind of messages that can be sent between distributed
/// `KVStore`s
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KVMessage {
    /// A message used to kindly tell other `KVStore`s to put the provided
    /// `Key` and `Value` in their local store
    Put(Key, Value),
    /// A message used to request the `Value` for the given `Key` from other
    /// `KVStore`s
    Get(Key),
    /// A message used to send a `Key` and its `Value` in response to `Get`
    /// messages
    Data(Key, Value),
    /// A message used to share random blobs of data with other nodes. This
    /// provides a lower level interface to facilitate other kinds of messages
    Blob(Value),
}
