use crate::kv::Key;
use serde::{Deserialize, Serialize};

/// Represents the kind of messages that can be sent in a KVStore
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KVMessage {
    Put(Key, Vec<u8>),
    Get(Key),
    Data(Key, Vec<u8>),
}
