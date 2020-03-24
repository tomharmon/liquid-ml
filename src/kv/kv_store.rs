use crate::dataframe::DataFrame;
use crate::error::LiquidError;
use crate::kv::KVMessage;
use crate::kv::*;
use crate::network::client::Client;
use bincode::{deserialize, serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

// TODO: Add private get_raw helpers
impl KVStore {
    pub fn new(
        network: Arc<RwLock<Client<KVMessage>>>,
        network_notifier: Arc<Notify>,
    ) -> Self {
        KVStore {
            data: RwLock::new(HashMap::new()),
            cache: RwLock::new(HashMap::new()),
            network,
            internal_notifier: Notify::new(),
            network_notifier,
        }
    }

    pub async fn get(&self, k: &Key) -> Result<DataFrame, LiquidError> {
        if k.home == { self.network.read().await.id } {
            // should not unwrap here it might panic
            match { self.data.read().await.get(k) } {
                Some(x) => Ok(deserialize(&x[..])?),
                None => Err(LiquidError::NotPresent),
            }
        } else {
            Err(LiquidError::NotPresent)
        }
    }

    /// Get the data for the give key, if the key is on a different node then make a network call
    /// and wait until the data has been added to the cache
    pub async fn wait_and_get(
        &self,
        k: &Key,
    ) -> Result<DataFrame, LiquidError> {
        if { k.home == self.network.read().await.id } {
            while { self.data.read().await.get(k) } == None {
                self.internal_notifier.notified().await;
            }
            Ok(deserialize({
                &self.data.read().await.get(k).unwrap()[..]
            })?)
        } else {
            {
                self.network
                    .write()
                    .await
                    .send_msg(k.home, KVMessage::Get(k.clone()))
                    .await?;
            }
            dbg!("Sent the request to get data");
            let mut x = 0;
            while { self.cache.read().await.get(k) } == None {
                self.internal_notifier.notified().await;
                println!("while iter {}", x);
                x += 1;
            }
            dbg!("Have the data");
            // TODO: remove this clone if possible
            Ok({ self.cache.read().await.get(k).unwrap().clone() })
        }

        //Ok(deserialize(&self.wait_and_get_raw(k).await?[..])?)
    }

    pub async fn put(&self, k: &Key, v: DataFrame) -> Result<(), LiquidError> {
        let serial = serialize(&v)?;
        if k.home == { self.network.read().await.id } {
            {
                self.data.write().await.insert(k.clone(), serial);
            }
            Ok(())
        } else {
            {
                self.network
                    .write()
                    .await
                    .send_msg(k.home, KVMessage::Put(k.clone(), serial))
                    .await
            }
        }
    }

    /// Internal helper to process messages from the queue
    pub async fn process_messages(kv: Arc<KVStore>) -> Result<(), LiquidError> {
        loop {
            println!("attempting to get write lock to process a message");
            let msg;
            loop {
                kv.network_notifier.notified().await;
                match { kv.network.write().await.receiver.try_recv() } {
                    Ok(v) => {
                        msg = v;
                        break;
                    }
                    Err(_) => (),
                }
            }
            let kv_ptr_clone = kv.clone();
            println!("About to spawn a task to process a message");
            tokio::spawn(async move {
                println!("msg processing task is running");
                match &msg.msg {
                    KVMessage::Get(k) => {
                        // This should wait until it has the data to respond
                        let x: Value =
                            kv_ptr_clone.wait_and_get_raw(k).await.unwrap();
                        dbg!("got data to respond with");
                        {
                            kv_ptr_clone
                                .network
                                .write()
                                .await
                                .send_msg(
                                    msg.sender_id,
                                    KVMessage::Data(k.clone(), x),
                                )
                                .await
                                .unwrap();
                        }
                    }
                    KVMessage::Data(k, v) => {
                        {
                            kv_ptr_clone
                                .cache
                                .write()
                                .await
                                .insert(k.clone(), deserialize(v).unwrap());
                        }
                        kv_ptr_clone.internal_notifier.notify();
                    }
                    KVMessage::Put(k, v) => {
                        // Note is the home id actually my id should we check?
                        {
                            kv_ptr_clone
                                .data
                                .write()
                                .await
                                .insert(k.clone(), v.clone());
                        }
                        kv_ptr_clone.internal_notifier.notify();
                    }
                }
            });
        }
    }

    async fn get_raw(&self, k: &Key) -> Result<Value, LiquidError> {
        if k.home == { self.network.read().await.id } {
            // should not unwrap here it might panic
            match { self.data.read().await.get(k) } {
                Some(x) => Ok(x.clone()), // TODO: maybe remove clone
                None => Err(LiquidError::NotPresent),
            }
        } else {
            Err(LiquidError::NotPresent)
        }
    }

    async fn wait_and_get_raw(&self, k: &Key) -> Result<Value, LiquidError> {
        if { k.home == self.network.read().await.id } {
            while { self.data.read().await.get(k) } == None {
                self.internal_notifier.notified().await;
            }
            Ok({ self.data.read().await.get(k).unwrap().clone() })
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
            // TODO: remove this clone if possible
            Ok(serialize(&{
                self.cache.read().await.get(k).unwrap().clone()
            })?)
        }
    }
}
