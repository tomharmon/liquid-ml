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
/// Defines methods for a `KVStore`
impl KVStore {
    /// Creates a new `KVStore`. The `network` is used to communicate between
    /// distributed `KVStore`s. The `network_notifier` comes from the `network`
    /// and is used to notify this `KVStore` when a new message comes from the
    /// `network` so that this `KVStore` may process and respond to messages.
    /// The `network_notifier` is passed in separately so that `new` is not
    /// `async`.
    pub fn new(
        network: Arc<RwLock<Client<KVMessage>>>,
        network_notifier: Arc<Notify>,
        id: usize,
    ) -> Self {
        KVStore {
            data: RwLock::new(HashMap::new()),
            cache: RwLock::new(HashMap::new()),
            network,
            internal_notifier: Notify::new(),
            network_notifier,
            id,
        }
    }

    /// `get` is used to retrieve the `Value` associated with the given `key`.
    /// The difference between `get` and `wait_and_get` is that `get` returns
    /// an error if the data is not owned by this `KVStore` or is not in its
    /// cache.
    ///
    /// ## Errors
    /// If `key` is not in this `KVStore`s cache or is not owned by this
    /// `KVStore`, then the error `Err(LiquidError::NotPresent)` is returned
    pub async fn get(&self, key: &Key) -> Result<DataFrame, LiquidError> {
        // TODO: also check the cache
        if key.home == self.id {
            // TODO: should not unwrap here it might panic
            match { self.data.read().await.get(key) } {
                Some(x) => Ok(deserialize(&x[..])?),
                None => Err(LiquidError::NotPresent),
            }
        } else {
            Err(LiquidError::NotPresent)
        }
    }

    /// Get the data for the given `key`. If the key is on a different node
    /// then request the data from the node that owns the `key` and wait until
    /// that node responds with the data, then return the deserialized data.
    pub async fn wait_and_get(
        &self,
        key: &Key,
    ) -> Result<DataFrame, LiquidError> {
        if key.home == self.id {
            while { self.data.read().await.get(key) } == None {
                self.internal_notifier.notified().await;
            }
            Ok(deserialize({
                &self.data.read().await.get(key).unwrap()[..]
            })?)
        } else {
            {
                self.network
                    .write()
                    .await
                    .send_msg(key.home, KVMessage::Get(key.clone()))
                    .await?;
            }
            dbg!("Sent the request to get data");
            let mut x = 0;
            while { self.cache.read().await.get(key) } == None {
                self.internal_notifier.notified().await;
                println!("while iter {}", x);
                x += 1;
            }
            dbg!("Have the data");
            // TODO: remove this clone if possible
            Ok({ self.cache.read().await.get(key).unwrap().clone() })
        }

        //Ok(deserialize(&self.wait_and_get_raw(k).await?[..])?)
    }

    // TODO: should return an `Result<Option<Value>, LiquidError` that is `Some`
    // if the given `key` already exists in this `KVStore`
    /// Put the data held in `value` to the `KVStore` with the `id` in
    /// `key.home`
    pub async fn put(
        &self,
        key: &Key,
        value: DataFrame,
    ) -> Result<(), LiquidError> {
        let serial = serialize(&value)?;
        if key.home == self.id {
            {
                self.data.write().await.insert(key.clone(), serial);
            }
            Ok(())
        } else {
            {
                self.network
                    .write()
                    .await
                    .send_msg(key.home, KVMessage::Put(key.clone(), serial))
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
