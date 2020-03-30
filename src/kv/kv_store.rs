use crate::dataframe::DataFrame;
use crate::error::LiquidError;
use crate::kv::{KVMessage, KVStore, Key, Value};
use crate::network::{Client, Message};
use bincode::{deserialize, serialize};
use lru::LruCache;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, Notify, RwLock};

const MAX_NUM_CACHED_DATAFRAMES: usize = 5;

impl KVStore {
    /// Creates a new `KVStore`. The `network` is used to communicate between
    /// distributed `KVStore`s. The `network_notifier` comes from the `network`
    /// and is used to notify this `KVStore` when a new message comes from the
    /// `network` so that this `KVStore` may process and respond to messages.
    /// The `network_notifier` is passed in separately so that `new` is not
    /// `async`.
    pub async fn new(
        server_addr: &str,
        my_addr: &str,
        blob_sender: Sender<Value>,
        kill_notifier: Arc<Notify>,
    ) -> Arc<Self> {
        // the Receiver acts as our queue of messages from the network, and
        // the network uses the Sender to add messages to our queue
        let (sender, receiver) = mpsc::channel(100);
        let network = Client::new(server_addr, my_addr, sender, kill_notifier)
            .await
            .unwrap();
        let id = network.read().await.id;
        let kv = Arc::new(KVStore {
            data: RwLock::new(HashMap::new()),
            cache: Mutex::new(LruCache::new(MAX_NUM_CACHED_DATAFRAMES)),
            network,
            internal_notifier: Notify::new(),
            id,
            blob_sender,
        });
        let kv_clone = kv.clone();
        tokio::spawn(async move {
            KVStore::process_messages(kv_clone, receiver).await.unwrap();
        });
        kv
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
            return Ok(val.clone());
        }

        let serialized_val = self.get_raw(key).await?;
        let deserialized: DataFrame = deserialize(&serialized_val[..])?;
        {
            self.cache
                .lock()
                .await
                .put(key.clone(), deserialized.clone());
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
            return Ok(val.clone());
        }

        if key.home == self.id {
            while { self.data.read().await.get(key) } == None {
                self.internal_notifier.notified().await;
            }
            let serialized_val = self.get_raw(key).await?;
            let deserialized: DataFrame = deserialize(&serialized_val[..])?;
            {
                self.cache
                    .lock()
                    .await
                    .put(key.clone(), deserialized.clone());
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
    /// This provides a lower level interface to facilitate other kinds of
    /// messages
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

    /// Processes messages from the queue that is populated by the
    /// `network::Client`.
    ///
    /// This method processes the messages by doing the following:
    /// 1. Asynchronously await new notifications from the `Client` that are
    ///    sent when messages have arrived on the queue.
    /// 2. Spawn an asynchronous `tokio::task` so as to not block further
    ///    message processing.
    /// 3. Based on the message type, do the following:
    ///    - `Get` message: call `wait_and_get` to get the data, either
    ///       internally from this `KVStore` or externally over the network
    ///       from another one. Once we have the data, respond with a `Data`
    ///       message containing the requested data.
    ///    - `Data` message: Deserialize the data and put it into our cache
    ///    - `Put` message: add the given data to our internal store
    ///    - `Blob` message: send the data up a higher level similar to how
    ///       the `Client` processes messages
    pub(crate) async fn process_messages(
        kv: Arc<KVStore>,
        mut receiver: Receiver<Message<KVMessage>>,
    ) -> Result<(), LiquidError> {
        loop {
            println!("processing messages");
            let msg = receiver.recv().await.unwrap();
            let kv_ptr_clone = kv.clone();
            let mut sender_clone = kv_ptr_clone.blob_sender.clone();
            tokio::spawn(async move {
                match &msg.msg {
                    KVMessage::Get(k) => {
                        // This must wait until it has the data to respond
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
                            kv_ptr_clone
                                .cache
                                .lock()
                                .await
                                .put(k.clone(), deserialize(v).unwrap());
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
                        sender_clone.send(v.clone()).await.unwrap();
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
