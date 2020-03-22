use crate::error::LiquidError;
use crate::kv::kv_message::KVMessage;
use crate::network::client::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use crate::dataframe::DataFrame;

impl KVStore {
    pub fn new(
        network: Arc<RwLock<Client<KVMessage>>>,
        network_notifier: Arc<Notify>,
    ) -> Self {
        let res = KVStore {
            data: RwLock::new(HashMap::new()),
            cache: RwLock::new(HashMap::new()),
            network,
            internal_notifier: Notify::new(),
            network_notifier,
        };
        res
    }

    pub async fn get(&self, k: &Key) -> Result<DataFrame, LiquidError> {
        if k.home == self.network.read().await.id {
            // should not unwrap here it might panic
            Ok(self.data.read().await.get(k).unwrap().clone())
        } else {
            Err(LiquidError::NotPresent)
        }
    }

    /// Get the data for the give key, if the key is on a different node then make a network call
    /// and wait until the data has been added to the cache
    pub async fn wait_and_get(&self, k: &Key) -> Result<Value, LiquidError> {
        if k.home == self.network.read().await.id {
            while self.data.read().await.get(k) == None {
                self.internal_notifier.notified().await;
            }
            Ok(self.data.read().await.get(k).unwrap().clone())
        } else {
            {
                self.network
                    .write()
                    .await
                    .send_msg(k.home, KVMessage::Get(k.clone()))
                    .await?;
            }
            while { self.cache.read().await.get(k) } == None {
                self.internal_notifier.notified().await;
            }
            Ok(self.cache.read().await.get(k).unwrap().clone())
        }
    }

    pub async fn put(&self, k: &Key, v: Value) -> Result<(), LiquidError> {
        if k.home == { self.network.read().await.id } {
            {
                self.data.write().await.insert(k.clone(), v);
            }
            Ok(())
        } else {
            self.network
                .write()
                .await
                .send_msg(k.home, KVMessage::Put(k.clone(), v))
                .await
        }
    }

    /// Internal helper to process messages from the queue
    pub async fn process_messages(&self) -> Result<(), LiquidError> {
        loop {
            println!("attempting to get write lock to process a message");
            let msg;
            loop {
                self.network_notifier.notified().await;
                match self.network.write().await.receiver.try_recv() {
                    Ok(v) => {
                        msg = v;
                        break;
                    }
                    Err(_) => ()
                }
            }
            println!("Got a message {:?}", msg);
            match &msg.msg {
                KVMessage::Get(k) => {
                    // This should wait until it has the data to respond
                    println!("in the correct match block");

                    let x: Value =
                        { self.data.read().await.get(k).unwrap().clone() };
                    println!("going to send over the following data: {:?}", x);
                    {
                        self.network
                            .write()
                            .await
                            .send_msg(
                                msg.sender_id,
                                KVMessage::Data(k.clone(), x),
                            )
                            .await?;
                    }
                }
                KVMessage::Data(k, v) => {
                    self.cache.write().await.insert(k.clone(), v.clone());
                    self.internal_notifier.notify();
                }
                KVMessage::Put(k, v) => {
                    // Note is the home id actually my id should we check?
                    self.data.write().await.insert(k.clone(), v.clone());
                    self.internal_notifier.notify();
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
