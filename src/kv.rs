use crate::network::client::Client;
use crate::error::LiquidError;
use std::collections::HashMap;
use crate::network::message::KVResponse;

#[derive(PartialEq, Eq, Hash)]
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
    
   pub fn get(&self, k: &Key) -> Result<Vec<u8>, LiquidError> {
        if k.home == self.network.id {
            Ok(self.data.get(k).unwrap().clone())
        } else {
            Err(LiquidError::NotPresent)
        }
   }

   pub async fn wait_and_get(&mut self, k: Key) -> Result<Vec<u8>, LiquidError> {
//       if k.home == self.network.id {
//            Ok(data.get(k))
//        } else {
//           self.network.send_msg(k.home, "Hello i want data").await?;
//        }
       unimplemented!() 
   }

   pub async fn put(&mut self, k: Key, v: Value) -> Result<(), LiquidError> {
       unimplemented!() 

   }
}
