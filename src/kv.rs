use crate::error::LiquidError;
use crate::kv_message::KVMessage;
use crate::network::client::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{RwLock, Notify};

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
    cache: RwLock<HashMap<Key, Value>>,
    network: RwLock<Client<KVMessage>>,
    notifier: Notify
}

impl KVStore {
    pub fn new(network: Client<KVMessage>) -> Self {
        let res = KVStore {
            data: RwLock::new(HashMap::new()),
            cache: RwLock::new(HashMap::new()),
            network: RwLock::new(network),
            notifier: Notify::new(),
        };
        res
    }

    pub async fn get(&self, k: &Key) -> Result<Value, LiquidError> {
        if k.home == self.network.read().await.id {
            // should not unwrap here it might panic
            Ok(self.data.read().await.get(k).unwrap().clone())
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
        if k.home == self.network.read().await.id {
            while self.data.read().await.get(k) == None {
                self.notifier.notified().await;
            }
            Ok(self.data.read().await.get(k).unwrap().clone())
        } else {
            self.network.write().await
                .send_msg(k.home, &KVMessage::Get(k.clone()))
                .await?;
            while self.cache.read().await.get(k) == None {
                self.notifier.notified().await;
            }
            Ok(self.cache.read().await.get(k).unwrap().clone())
        }
    }

    pub async fn put(&mut self, k: &Key, v: Value) -> Result<(), LiquidError> {
        if k.home == self.network.read().await.id {
            self.data.write().await.insert(k.clone(), v);
            Ok(())
        } else {
            self.network.write().await
                .send_msg(k.home, &KVMessage::Put(k.clone(), v))
                .await
        }
    }

    /// Internal helper to process messages from the queue
    pub async fn process_messages(&self) -> Result<(), LiquidError> {
        loop {
            let msg = self.network.write().await.next_msg().await;
            match &msg.msg {
                KVMessage::Get(k) => {
                    // This should wait until it has the data to respond
                    let x : Value = self.data.read().await.get(k).unwrap().clone();
                    self.network.write().await.send_msg(msg.sender_id ,&KVMessage::Data(k.clone(), x)).await?;
                },
                KVMessage::Data(k, v) => {
                    self.cache.write().await.insert(k.clone(), v.clone()); 
                    self.notifier.notify();
                },
                KVMessage::Put(k, v) => {
                    // Note is the home id actually my id should we check?
                    self.data.write().await.insert(k.clone(), v.clone());
                    self.notifier.notify();
                }
            }
        }
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




