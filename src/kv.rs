use crate::network::client::Client;
use crate::error::LiquidError;
use std::collections::HashMap;

pub struct Key {
    name: String,
    home: usize,
}

impl Key {
    pub fn new(name : String, home: usize) -> Self {
        Key { name, home }
    }
}

pub struct KVStore {
    data: Map<Key, Vec<u8>>,
    cache: Map<Key, Vec<u8>>,
    network: Client,
}

impl KVStore {
   pub fn new(network: Client) {
        KVStore {
            data: HashMap::new(),
            cache: HashMap::new(),
            network
        }
   }
    
   pub fn get(k: Key) -> Result<Vec<u8>, LiquidError> {
        if k.home == self.network.id {
            Ok(data.get(k))
        } else {
            Err(LiquidError::NotPresent)
        }
   }

   pub async fn wait_and_get(k: Key) -> Result<Vec<u8>, LiquidError> {
       unimplemented!() 
   }

   pub async fn put(k: Key, v: Value) -> Result<(), LiquidError> {
       unimplemented!() 

   }
}
