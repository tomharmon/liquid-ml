use crate::error::LiquidError;
use crate::kv_message::KVMessage;
use crate::network::client::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    data: HashMap<Key, Value>,
    cache: HashMap<Key, Value>,
    network: Client<KVMessage>,
}

impl KVStore {
    pub fn new(network: Client<KVMessage>) -> Self {
        KVStore {
            data: HashMap::new(),
            cache: HashMap::new(),
            network,
        }
    }

    pub fn get(&self, k: &Key) -> Result<Value, LiquidError> {
        if k.home == self.network.id {
            Ok(self.data.get(k).unwrap().clone())
        } else {
            Err(LiquidError::NotPresent)
        }
    }

    /// Get the data for the give key, if the key is on a different node then make a network call
    /// and wait until the data has been added to the cache
    pub async fn wait_and_get(
        &mut self,
        k: &Key,
    ) -> Result<Value, LiquidError> {
        if k.home == self.network.id {
            while self.data.get(k) == None {
                self.process_message().await?;
            }
            Ok(self.data.get(k).unwrap().clone())
        } else {
            self.network
                .send_msg(k.home, &KVMessage::Get(k.clone()))
                .await?;
            while self.cache.get(k) == None {
                self.process_message().await?;
            }
            Ok(self.cache.get(k).unwrap().clone())
        }
    }

    pub async fn put(&mut self, k: &Key, v: Value) -> Result<(), LiquidError> {
        if k.home == self.network.id {
            self.data.insert(k.clone(), v);
            Ok(())
        } else {
            self.network
                .send_msg(k.home, &KVMessage::Put(k.clone(), v))
                .await
        }
    }

    /// Internal helper to process messages from the queue
    async fn process_message(&mut self) -> Result<(), LiquidError> {
        let msg = self.network.process_message();
        unimplemented!()
        // This function will read and process a message from the recieve queue
        //
        // If the message is a get request it will read the sender of the messsage and reply with a
        // KVResponse with the data from its local store (We might have to spawn an async tokio
        // task that waits until a relavant put rquest has occured)
        //
        // If the message is a put it will add the provided data to it's local store.
        //
        // If the message is a KVResponse the provided data will be placed in the cache
    }
}
