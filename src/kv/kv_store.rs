use crate::dataframe::DataFrame;
use crate::error::LiquidError;
use crate::kv::KVMessage;
use crate::kv::*;
use crate::network::client::Client;
use associative_cache::indices::HashDirectMapped;
use associative_cache::replacement::lru::LruReplacement;
use associative_cache::{AssociativeCache, WithLruTimestamp};
use bincode::{deserialize, serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, Notify, RwLock};

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
        blob_sender: Sender<Value>,
    ) -> Self {
        KVStore {
            data: RwLock::new(HashMap::new()),
            cache: Mutex::new(AssociativeCache::<
                Key,
                WithLruTimestamp<DataFrame>,
                Capacity1GB,
                HashDirectMapped,
                LruReplacement,
            >::default()),
            network,
            internal_notifier: Notify::new(),
            network_notifier,
            id,
            blob_sender,
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
        if let Some(val) = { self.cache.lock().await.get(key) } {
            return Ok(WithLruTimestamp::into_inner(val.clone()));
        }

        let serialized_val = self.get_raw(key).await?;
        let deserialized: DataFrame = deserialize(&serialized_val[..])?;
        {
            self.cache.lock().await.insert(
                key.clone(),
                WithLruTimestamp::new(deserialized.clone()),
            );
        }
        Ok(deserialized)
    }

    /// Get the data for the given `key`. If the key belongs on a different node
    /// then the data will be requested from the node that owns the `key` and
    /// this method will block until that node responds with the data.
    ///
    /// If the belongs on this `KVStore`, then make sure this function is only
    /// being used in one of these following ways:
    /// 1. You know that the data was `put` some microseconds around when
    ///    `wait_and_get` was called, but can't guarantee it happened exactly
    ///    before `wait_and_get` is called. In this case, this function can
    ///    likely be `await`ed
    /// 2. The data will be `put` on this `KVStore` sometime in the
    ///    future at an unknown time. In this case, this function should
    ///    not be `await`ed but instead given a callback closure via
    ///    calling `and_then` on the returned future
    pub async fn wait_and_get(
        &self,
        key: &Key,
    ) -> Result<DataFrame, LiquidError> {
        if let Some(val) = { self.cache.lock().await.get(key) } {
            return Ok(WithLruTimestamp::into_inner(val.clone()));
        }

        if key.home == self.id {
            while { self.data.read().await.get(key) } == None {
                self.internal_notifier.notified().await;
            }
            let serialized_val = self.get_raw(key).await?;
            let deserialized: DataFrame = deserialize(&serialized_val[..])?;
            {
                self.cache.lock().await.insert(
                    key.clone(),
                    WithLruTimestamp::new(deserialized.clone()),
                );
            }
            Ok(deserialized)
        } else {
            // The data is not supposed to be owned by this node, we must
            // request it from another `KVStore` by sending a `get` message
            {
                self.network
                    .write()
                    .await
                    .send_msg(key.home, KVMessage::Get(key.clone()))
                    .await?;
            }
            while { self.cache.lock().await.get(key) } == None {
                self.internal_notifier.notified().await;
            }
            self.get(key).await
        }
    }

    /// Put the data held in `value` to the `KVStore` with the `id` in
    /// `key.home`.
    ///
    /// ## If `key` belongs to this `KVStore`
    /// If this `KVStore` did not have this `key` present, `Ok(None)` is
    /// returned.
    ///
    /// If this `KVStore` does have this `key` present, the `Value` is updated,
    /// and `Ok(Some<Value>)` of the old `Value` is returned. The key is not
    /// updated, though; this matters for types that can be == without being
    /// identical.
    ///
    /// ## If `key` belongs to another `KVStore`
    /// `Ok(None)` is returned after the `value` was successfully sent
    pub async fn put(
        &self,
        key: &Key,
        value: DataFrame,
    ) -> Result<Option<Value>, LiquidError> {
        let serial = serialize(&value)?;
        if key.home == self.id {
            let opt_old_data =
                { self.data.write().await.insert(key.clone(), serial) };
            self.internal_notifier.notify();
            Ok(opt_old_data)
        } else {
            {
                self.network
                    .write()
                    .await
                    .send_msg(key.home, KVMessage::Put(key.clone(), serial))
                    .await?
            }
            Ok(None)
        }
    }

    /// Sends the given `blob` to the `KVStore` with the given `target_id`
    pub async fn send_blob(
        &self,
        target_id: usize,
        blob: Value,
    ) -> Result<(), LiquidError> {
        if target_id == self.id {
            Err(LiquidError::DumbUserError)
        } else {
            self.network
                .write()
                .await
                .send_msg(target_id, KVMessage::Blob(blob))
                .await
        }
    }

    /// Internal helper to process messages from the queue
    pub async fn process_messages(kv: Arc<KVStore>) -> Result<(), LiquidError> {
        loop {
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
            let mut sender_clone = kv_ptr_clone.blob_sender.clone();
            tokio::spawn(async move {
                match &msg.msg {
                    KVMessage::Get(k) => {
                        // This should wait until it has the data to respond
                        let x = kv_ptr_clone.wait_and_get_raw(k).await.unwrap();
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
                            kv_ptr_clone.cache.lock().await.insert(
                                k.clone(),
                                WithLruTimestamp::new(deserialize(v).unwrap()),
                            );
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
                    KVMessage::Blob(v) => {
                        sender_clone.send(v.clone()).await;
                    }
                }
            });
        }
    }

    async fn get_raw(&self, key: &Key) -> Result<Value, LiquidError> {
        if key.home == self.id {
            match { self.data.read().await.get(key) } {
                Some(serialized_blob) => Ok(serialized_blob.clone()),
                None => Err(LiquidError::NotPresent),
            }
        } else {
            Err(LiquidError::NotPresent)
        }
    }

    async fn wait_and_get_raw(&self, key: &Key) -> Result<Value, LiquidError> {
        if key.home == self.id {
            while { self.data.read().await.get(key) } == None {
                self.internal_notifier.notified().await;
            }
            Ok(self.get_raw(key).await?)
        } else {
            Ok(serialize(&self.wait_and_get(key).await?)?)
        }
    }
}
