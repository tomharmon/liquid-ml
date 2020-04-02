//! The `KVStore`
use crate::error::LiquidError;
use crate::kv::{KVMessage, KVStore, Key, Value};
use crate::network::{Client, Message};
use bincode::{deserialize, serialize};
use log::info;
use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, Notify, RwLock};

const MAX_NUM_CACHED_VALUES: usize = 5;

impl<
        T: Clone
            + Serialize
            + DeserializeOwned
            + Sync
            + Send
            + PartialEq
            + 'static,
    > KVStore<T>
{
    /// Creates a new distributed `KVStore`.
    ///
    /// ## Parameters
    /// - `server_addr`: the `IP:Port` of the registration `Server` used
    ///    for orchestrating the connection of all distributed nodes in the
    ///    system.
    /// - `my_addr` is the `IP:Port` of this `KVStore`.
    /// - `blob_sender`: is the sending half of an `mpsc` channel that is
    ///    passed in by the `Application` layer that uses this `KVStore`.
    ///    Whenever this `KVStore` receives serialized blobs of data from
    ///    other `KVStore`s in the distributed system, this `KVStore` will
    ///    use the `blob_sender` to forward the blob to the `Application`.
    ///    For `liquid_ml`, these blobs are used for joining `Rower`s
    ///    for distributed `pmap`, though other use cases are possible.
    /// - `kill_notifier`: is a `Notify` passed in by the `Application` layer.
    ///    This `KVStore` then passes it down to its `Client` so that when the
    ///    `Client` gets `Kill` messages from the `Server`, the `Client` can
    ///    notify the `Application` it is time to shut down in an orderly
    ///    fashion.
    /// - `num_clients`: the number of nodes in the distributed system,
    ///    including this one.
    /// - `wait_for_all_clients`: whether or not to wait for all other nodes
    ///    to connect to this one before returning this new `KVStore`.
    pub async fn new(
        server_addr: &str,
        my_addr: &str,
        blob_sender: Sender<Value>,
        kill_notifier: Arc<Notify>,
        num_clients: usize,
        wait_for_all_clients: bool,
    ) -> Arc<Self> {
        // the Receiver acts as our queue of messages from the network, and
        // the network uses the Sender to add messages to our queue
        let (sender, receiver) = mpsc::channel(100);
        let network = Client::new(
            server_addr,
            my_addr,
            sender,
            kill_notifier,
            num_clients,
            wait_for_all_clients,
        )
        .await
        .unwrap();
        let id = network.read().await.id;
        let kv = Arc::new(KVStore {
            data: RwLock::new(HashMap::new()),
            cache: Mutex::new(LruCache::new(MAX_NUM_CACHED_VALUES)),
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

    /// Used to retrieve the deserialized `Value` associated with the given
    /// `key` if the data is held locally on this node in either the cache or
    /// the store itself. The difference between `get` and `wait_and_get` is
    /// that `get` returns an error if the data is not owned by this `KVStore`
    /// or is if it is not owned by this `KVStore` **and** it is not in its
    /// cache.
    ///
    /// ## Errors
    /// If `key` is not in this `KVStore`s cache or is not owned by this
    /// `KVStore`, then the error `Err(LiquidError::NotPresent)` is returned
    pub async fn get(&self, key: &Key) -> Result<T, LiquidError> {
        if let Some(val) = { self.cache.lock().await.get(key) } {
            return Ok(val.clone());
        }

        let serialized_val = self.get_raw(key).await?;
        let deserialized: T = deserialize(&serialized_val[..])?;
        {
            self.cache
                .lock()
                .await
                .put(key.clone(), deserialized.clone());
        }
        Ok(deserialized)
    }

    /// Get the data for the given `key`. If the key belongs on a different
    /// node then the data will be requested from the node that owns the `key`
    /// and `await`ing this method will block until that node responds with the
    /// data.
    ///
    /// Make sure that you use this function in only one of the two following
    /// ways:
    /// 1. You know that the data was `put` some microseconds around when
    ///    `wait_and_get` was called, but can't guarantee it happened exactly
    ///    before `wait_and_get` is called. In this case, this function can
    ///    be `await`ed
    /// 2. The data will be `put` on this `KVStore` sometime in the
    ///    future at an unknown time. In this case, this function should
    ///    not be `await`ed but instead given a callback closure via
    ///    calling `and_then` on the returned future
    ///
    /// If you do not do that, for example you `await` the second case,
    /// then you will waste a lot of time waiting for the data to be
    /// transferred over the network.
    pub async fn wait_and_get(&self, key: &Key) -> Result<T, LiquidError> {
        if let Some(val) = { self.cache.lock().await.get(key) } {
            return Ok(val.clone());
        }

        if key.home == self.id {
            while { self.data.read().await.get(key) } == None {
                self.internal_notifier.notified().await;
            }
            let serialized_val = self.get_raw(key).await?;
            let deserialized: T = deserialize(&serialized_val[..])?;
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

    /// Puts the data held in `value` to the `KVStore` with the `id` in
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
        value: T,
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
    /// 1. Asynchronously await new messages from the `Client` that are
    ///    sent over an `mpsc` channel.
    /// 2. Spawn an asynchronous `tokio::task` to respond to the newly
    ///    received message so as to not block further message processing.
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
        kv: Arc<KVStore<T>>,
        mut receiver: Receiver<Message<KVMessage>>,
    ) -> Result<(), LiquidError> {
        loop {
            let msg = receiver.recv().await.unwrap();
            let kv_ptr_clone = kv.clone();
            let mut sender_clone = kv_ptr_clone.blob_sender.clone();
            tokio::spawn(async move {
                info!("Proccessing a message with id: {:#?}", msg.msg_id);
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
