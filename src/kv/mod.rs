use crate::dataframe::DataFrame;
use crate::network::client::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

pub mod kv_store;

#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Debug, Clone)]
pub struct Key {
    name: String,
    home: usize,
}

pub type Value = Vec<u8>;

impl Key {
    pub fn new(name: String, home: usize) -> Self {
        Key { name, home }
    }
}

pub struct KVStore {
    data: RwLock<HashMap<Key, Value>>,
    // TODO: Change to an LRU
    cache: RwLock<HashMap<Key, DataFrame>>,
    network: Arc<RwLock<Client<KVMessage>>>,
    internal_notifier: Notify,
    network_notifier: Arc<Notify>,
}

/// Represents the kind of messages that can be sent in a KVStore
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KVMessage {
    Put(Key, Vec<u8>),
    Get(Key),
    Data(Key, Vec<u8>),
}
