//! A module for distributed [`Key`], [`Value`] stores that hold their data in
//! memory and are generic for (deserialized) [`Value`]s of type `T`.
//!
//! A [`KVStore`] is essentially a very simple in-memory distributed database
//! (with no persistence) that stores data as a collection of key-value pairs
//! where a [`Key`] is a unique identifier to a [`Value`]. The [`KVStore`]
//! utilizes the [`network`] module to communicate between nodes using
//! [`KVMessage`]s.
//!
//! Internally [`KVStore`]s store their data in memory as serialized blobs
//! (a [`Value`] aka a `Vec<u8>`). The [`KVStore`] caches deserialized
//! [`Value`]s into their type `T` on a least-recently used basis. A hard limit
//! for the cache size is set to be `1/3` the amount of total memory on the
//! machine, though this will be changed to be configurable.
//!
//! # Provided [`KVStore`] Functionality
//! - [`get`]: Retrieve data stored locally on a [`KVStore`] or in its cache
//! - [`wait_and_get`]: Retrieve data either locally or over the network
//! - [`put`]: Store a [`Key`], [`Value`] pair either locally on a [`KVStore`]
//!    or send it over the network to store it on another [`KVStore`]
//! - [`send_blob`]: a lower level interface to facilitate sending any
//!    serialized data. In `liquid_ml`, this is used for sending
//!    [`Rower`](../dataframe/trait.Rower.html)s
//!    (and other use cases) in a
//!    [`DistributedDataFrame`](../dataframe/struct.DistributedDataFrame.html)
//!
//!
//!
//! [`Key`]: struct.Key.html
//! [`Value`]: type.Key.html
//! [`KVStore`]: struct.KVStore.html
//! [`get`]: struct.KVStore.html#method.get
//! [`wait_and_get`]: struct.KVStore.html#method.wait_and_get
//! [`put`]: struct.KVStore.html#method.put
//! [`send_blob`]: struct.KVStore.html#method.send_blob
//! [`KVMessage`]: enum.KVMessage.html
//! [`Data`]: enum.KVMessage.html#variant.Data
//! [`Put`]: enum.KVMessage.html#variant.Put
//! [`Blob`]: enum.KVMessage.html#variant.Blob
use rand::{self, Rng};
use serde::{Deserialize, Serialize};

mod kv_store;
pub use crate::kv::kv_store::{KVMessage, KVStore};

/// A `Key` defines where in a [`KVStore`] a [`Value`] is stored, as well as
/// which node (and thus which [`KVStore`]) 'owns' the [`Value`]
///
/// [`KVStore`]: struct.KVStore.html
/// [`Value`]: type.Value.html
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Debug, Clone)]
pub struct Key {
    /// Defines where in a [`KVStore`] a value is stored
    ///
    /// [`KVStore`]: struct.KVStore.html
    pub name: String,
    /// Defines which node 'owns' the associated [`Value`]
    ///
    /// [`Value`]: type.Key.html
    pub home: usize,
}

/// A serialized blob of data. Is associated with a [`Key`] which defines where
/// this `Value` is stored in a [`KVStore`], as well as its 'owner'
///
/// [`Key`]: struct.Key.html
/// [`KVStore`]: struct.KVStore.html
pub type Value = Vec<u8>;

impl Key {
    /// Creates a new [`Key`] that is owned by the [`KVStore`] running on the
    /// node with id equal to `home`. The given `name` defines where in the
    /// [`KVStore`] the value is stored.
    ///
    /// [`Key`]: struct.Key.html
    /// [`KVStore`]: struct.KVStore.html
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
