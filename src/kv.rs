use crate::network::client::Client;
use crate::error::LiquidError;
use std::collections::HashMap;
use crate::network::message::KVResponse;
use serde::{Serialize, Deserialize};

#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Debug, Clone)]
pub struct Key {
    name: String,
    home: usize,
}

pub type Value = Vec<u8>;

impl Key {
    pub fn new(name : String, home: usize) -> Self {
        Key { name, home }
    }
}

pub struct KVStore {
    data: HashMap<Key, Value>,
    cache: HashMap<Key, Value>,
    network: Client<KVResponse>,
}

impl KVStore {
   pub fn new(network: Client<KVResponse>) -> Self {
        KVStore {
            data: HashMap::new(),
            cache: HashMap::new(),
            network
        }
   }
    
   pub fn get(&self, k: &Key) -> Result<Value, LiquidError> {
        if k.home == self.network.id {
            Ok(self.data.get(k).unwrap().clone())
        } else {
            Err(LiquidError::NotPresent)
        }
   }

   pub async fn wait_and_get(&mut self, k: &Key) -> Result<Value, LiquidError> {
       if k.home == self.network.id {
            Ok(self.data.get(k).unwrap().clone())
        } else {
           self.network.send_msg(k.home, &"Hello i want data".to_string()).await?;
           loop {
               let msg = self.network.process_next();
               // TODO remove dumb clones
               self.cache.insert(msg.msg.key.clone(), msg.msg.data.clone());
               if &msg.msg.key == k {
                    return Ok(msg.msg.data);
               }
           }
        }
   }

   pub async fn put(&mut self, k: &Key, v: Value) -> Result<(), LiquidError> {
       if k.home == self.network.id {
           self.data.insert(k.clone(), v);
           Ok(())
       } else {
           self.network.send_msg(k.home, &"send kv to node here".to_string()).await
       }
   }
}
