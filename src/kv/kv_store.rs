//! The `KVStore` implementation
use crate::error::LiquidError;
use crate::kv::{Key, Value};
use crate::network::{Client, FramedStream};
use crate::{
    BYTES_PER_GB, BYTES_PER_KIB, KV_STORE_CACHE_SIZE_FRACTION,
    MAX_NUM_CACHED_VALUES,
};
use bincode::{deserialize, serialize};
use deepsize::DeepSizeOf;
use futures::stream::{SelectAll, StreamExt};
use log::{debug, error, info};
use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::sync::{mpsc::Sender, Mutex, Notify, RwLock};

/// A distributed [`Key`], [`Value`] store which is generic for type `T`. Since
/// this is a distributed [`KVStore`], [`Key`]s know which node the values
/// 'belong' to.
///
/// Internally `KVStore`s store their data in memory as serialized blobs
/// (`Vec<u8>`). The `KVStore` caches deserialized [`Value`]s into their type
/// `T` on a least-recently used basis. There is a hard limit on the cache size
/// that is set to `1/3` the total memory of the machine.
///
/// [`Key`]: struct.Key.html
/// [`Value`]: type.Key.html
#[derive(Debug)]
pub struct KVStore<T> {
    /// The data owned by this `KVStore`
    data: RwLock<HashMap<Key, Value>>,
    /// An `LRU` cache of deserialized values of type `T` with a hard maximum
    /// memory limit set on construction. Not all cached values belong to this
    /// `KVStore`, some of it may come from other distributed `KVStore`s not
    /// running on this machine.
    cache: Mutex<LruCache<Key, Arc<T>>>,
    /// The `network` layer, used to send and receive messages and data with
    /// other `KVStore`s
    pub(crate) network: Arc<Mutex<Client<KVMessage>>>,
    /// Used internally for processing data and messages
    internal_notifier: Notify,
    /// The `id` of the node this `KVStore` is running on
    pub(crate) id: usize,
    /// A channel to send blobs of data to a higher level component, in
    /// `liquid-ml` this would be the `LiquidML` struct
    ///
    /// [`LiquidML`]: ../struct.LiquidML.html
    blob_sender: Sender<Value>,
    /// The total amount of memory (in bytes) this `KVStore` is allowed
    /// to keep in its cache
    max_cache_size: u64,
}

/// Represents the kind of messages that can be sent between distributed
/// [`KVStore`]s
///
/// [`KVStore`]: struct.KVStore.html
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KVMessage {
    /// A message used to kindly tell other [`KVStore`]s to put the provided
    /// [`Key`] and [`Value`] in their local store
    ///
    /// [`KVStore`]: struct.KVStore.html
    /// [`Key`]: struct.Key.html
    /// [`Value`]: type.Key.html
    Put(Key, Value),
    /// A message used to request the [`Value`] for the given [`Key`] from
    /// other [`KVStore`]s
    ///
    /// [`KVStore`]: struct.KVStore.html
    /// [`Key`]: struct.Key.html
    /// [`Value`]: type.Key.html
    Get(Key),
    /// A message used to send a [`Key`] and its [`Value`] in response to
    /// [`Get`] messages
    ///
    /// [`KVStore`]: struct.KVStore.html
    /// [`Key`]: struct.Key.html
    /// [`Value`]: type.Key.html
    /// [`Get`]: enum.KVMessage.html#variant.Get
    Data(Key, Value),
    /// A message used to share random blobs of data with other nodes. This
    /// provides a lower level interface to facilitate other kinds of messages
    Blob(Vec<u8>),
}

// TODO: remove `DeserializeOwned + 'static`
impl<
        T: Serialize
            + DeserializeOwned
            + Sync
            + Send
            + PartialEq
            + DeepSizeOf
            + 'static,
    > KVStore<T>
{
    /// Creates a new distributed [`KVStore`]. Note that you likely do not
    /// want to use a [`KVStore`] directly, and instead would have a much
    /// easier time using the application layer directly via the [`LiquidML`]
    /// struct.
    ///
    /// # Parameters
    /// - `server_addr`: the `IP:Port` of the registration [`Server`], used
    ///    for orchestrating the connection of all distributed nodes in the
    ///    system.
    /// - `my_addr` is the `IP:Port` of this [`KVStore`].
    /// - `blob_sender`: is the sending half of an [`mpsc`] channel that is
    ///    passed in by components using this [`KVStore`] to facilitate
    ///    lower level messages. In the case of `liquid_ml`, it will use the
    ///    `blob_sender` to forward the blob to [`LiquidML`]. If you are not
    ///    using [`LiquidML`], you must store the receiving half of the
    ///    [`mpsc`] channel so you may process blobs that are received.
    /// - `kill_notifier`: When using the application layer directly, this is
    ///    passed in for you by the [`LiquidML`] struct and is used to perform
    ///    orderly shutdown when the [`Client`] receives [`Kill`] messages from
    ///    the [`Server`]
    /// - `num_clients`: the number of nodes in the distributed system,
    ///    including this one.
    /// - `wait_for_all_clients`: whether or not to wait for all other nodes
    ///    to connect to this one before returning the new [`KVStore`].
    ///
    /// [`KVStore`]: struct.KVStore.html
    /// [`Server`]: ../network/struct.Server.html
    /// [`Client`]: ../network/struct.Client.html
    /// [`LiquidML`]: ../struct.LiquidML.html
    /// [`Kill`]: ../network/enum.ControlMsg.html#variant.Kill
    /// [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
    pub async fn new(
        server_addr: String,
        my_addr: String,
        blob_sender: Sender<Value>,
        num_clients: usize,
    ) -> Arc<Self> {
        let (my_ip, my_port) = {
            let mut iter = my_addr.split(':');
            let first = iter.next().unwrap();
            let second = iter.next().unwrap();
            (first.to_string(), second.to_string())
        };
        let (network, read_streams, _kill_notifier) = Client::new(
            server_addr,
            my_ip,
            Some(my_port),
            num_clients,
            "kvstore".to_string(),
        )
        .await
        .unwrap();
        let id = { network.lock().await.id };

        let memo_info_kind = RefreshKind::new().with_memory();
        let sys = System::new_with_specifics(memo_info_kind);
        let total_memory = sys.get_total_memory() as f64;
        let max_cache_size =
            total_memory * BYTES_PER_KIB * KV_STORE_CACHE_SIZE_FRACTION;
        let max_cache_size_in_gb = max_cache_size / BYTES_PER_GB;
        info!(
            "KVStore has a max cache size of {} GB",
            max_cache_size_in_gb
        );

        let kv = Arc::new(KVStore {
            data: RwLock::new(HashMap::new()),
            cache: Mutex::new(LruCache::new(MAX_NUM_CACHED_VALUES)),
            network,
            internal_notifier: Notify::new(),
            id,
            blob_sender,
            max_cache_size: max_cache_size as u64,
        });

        let kv_clone = kv.clone();
        tokio::spawn(async move {
            KVStore::process_messages(kv_clone, read_streams)
                .await
                .unwrap();
        });

        kv
    }

    /// Used to retrieve the deserialized [`Value`] associated with the given
    /// `key` if the data is held locally on this node in either the cache or
    /// the store itself.
    ///
    /// The difference between [`get`] and [`wait_and_get`] is that [`get`]
    /// returns an error if the data is not owned by this [`KVStore`] or if it
    /// is not owned by this [`KVStore`] **and** it is not in its cache.
    ///
    /// ## Errors
    /// If `key` is not in this [`KVStore`]s cache or is not owned by this
    /// [`KVStore`], then the error [`LiquidError::NotPresent`] is returned
    ///
    /// [`Value`]: type.Key.html
    /// [`KVStore`]: struct.KVStore.html
    /// [`get`]: struct.KVStore.html#method.get
    /// [`wait_and_get`]: struct.KVStore.html#method.wait_and_get
    /// [`LiquidError::NotPresent`]: ../error/enum.LiquidError.html#variant.NotPresent
    pub async fn get(&self, key: &Key) -> Result<Arc<T>, LiquidError> {
        if let Some(val) = { self.cache.lock().await.get(key) } {
            return Ok(val.clone());
        }

        let serialized_val = self.get_raw(key).await?;
        let value: Arc<T> = Arc::new(deserialize(&serialized_val[..])?);
        let v = value.clone();
        self.add_to_cache(key.clone(), v).await?;
        Ok(value)
    }

    /// Get the data for the given `key`. If the key belongs on a different
    /// node then the data will be requested from the node that owns the `key`
    /// and `await`ing this method will block until that node responds with the
    /// data.
    ///
    /// Make sure that you use this function in only one of the two following
    /// ways:
    /// 1. You know that the data was [`put`] some microseconds around when
    ///    [`wait_and_get`] was called, but can't guarantee it happened exactly
    ///    before [`wait_and_get`] is called. In this case, this function can
    ///    be `await`ed
    /// 2. The data will be [`put`] on this [`KVStore`] sometime in the
    ///    future at an unknown time. In this case, this function should
    ///    not be `await`ed but instead given a callback closure via
    ///    calling `and_then` on the returned future
    ///
    /// If you do not do that, for example in the second case you `await`
    /// despite our warning, then you will waste a lot of time waiting for the
    /// data to be transferred over the network.
    ///
    /// [`KVStore`]: struct.KVStore.html
    /// [`wait_and_get`]: struct.KVStore.html#method.wait_and_get
    /// [`put`]: struct.KVStore.html#method.put
    pub async fn wait_and_get(&self, key: &Key) -> Result<Arc<T>, LiquidError> {
        if let Some(val) = { self.cache.lock().await.get(key) } {
            return Ok(val.clone());
        }

        if key.home == self.id {
            // key, value belong to us
            while { self.data.read().await.get(key) } == None {
                // while we don't have the data, wait for the message
                // processing task to notify us the data is there
                self.internal_notifier.notified().await;
            }
            // get the raw serialized data, its guaranteed to be there
            let serialized_val = self.get_raw(key).await?;
            let value: Arc<T> = Arc::new(deserialize(&serialized_val[..])?);
            let v = value.clone();
            // update our LRU cache
            self.add_to_cache(key.clone(), v).await?;

            Ok(value)
        } else {
            // The data is not supposed to be owned by this node, we must
            // request it from another `KVStore` by sending a `get` message
            {
                self.network
                    .lock()
                    .await
                    .send_msg(key.home, KVMessage::Get(key.clone()))
                    .await?;
            }
            while { self.cache.lock().await.get(key) } == None {
                // while the data is not yet in our cache, wait for the
                // message processing task to notify when it is there
                self.internal_notifier.notified().await;
            }
            // it's guaranteed to be in the cache, we can get it
            self.get(key).await
        }
    }

    /// Puts the data held in `value` to the [`KVStore`] with the `id` in
    /// `key.home`.
    ///
    /// ## If `key` belongs to this [`KVStore`]
    /// If this [`KVStore`] did not have this `key` present, `Ok(None)` is
    /// returned.
    ///
    /// If this [`KVStore`] does have this `key` present, the associated
    /// [`Value`] is updated, and `Ok(Some<Value>)` of the old [`Value`] is
    /// returned. The key is not updated, though; this matters for types that
    /// can be `==` without being identical.
    ///
    /// ## If `key` belongs to another [`KVStore`]
    /// `Ok(None)` is returned after the `value` was successfully sent
    ///
    /// [`KVStore`]: struct.KVStore.html
    /// [`Value`]: type.Key.html
    pub async fn put(
        &self,
        key: Key,
        value: T,
    ) -> Result<Option<Value>, LiquidError> {
        let serial = serialize(&value)?;
        if key.home == self.id {
            debug!("Put key: {:#?} into KVStore", key.clone());
            let opt_old_data =
                { self.data.write().await.insert(key.clone(), serial) };
            self.internal_notifier.notify(); // why do we need this here again
            self.add_to_cache(key, Arc::new(value)).await?;
            Ok(opt_old_data)
        } else {
            let target_id = key.home;
            let msg = KVMessage::Put(key, serial);
            self.network.lock().await.send_msg(target_id, msg).await?;
            Ok(None)
        }
    }

    /// Sends the given `blob` to the [`KVStore`] with the given `target_id`
    /// This provides a lower level interface to facilitate other kinds of
    /// messages
    ///
    /// [`KVStore`]: struct.KVStore.html
    pub async fn send_blob(
        &self,
        target_id: usize,
        blob: Value,
    ) -> Result<(), LiquidError> {
        self.network
            .lock()
            .await
            .send_msg(target_id, KVMessage::Blob(blob))
            .await
    }

    /// Processes messages from the queue that is populated by a [`Client`].
    ///
    /// This method processes the messages by doing the following:
    /// 1. Asynchronously await new messages from the [`Client`] that are
    ///    sent over an [`mpsc`] channel.
    /// 2. Spawn an asynchronous `tokio::task` to respond to the newly
    ///    received message so as to not block further message processing.
    /// 3. Based on the message type, do the following:
    ///    - [`Get`](enum.KVMessage.html#variant.Get) message: call
    ///       [`wait_and_get`] to get the data, either internally from this
    ///       [`KVStore`] or externally over the network from another one. Once
    ///       we have the data, respond with a [`Data`] message containing the
    ///       requested data.
    ///    - [`Data`] message: Deserialize the data and put it into our cache
    ///    - [`Put`] message: add the given data to our internal store
    ///    - [`Blob`] message: send the data up a higher level similar to how
    ///       the [`Client`] processes messages
    ///
    /// [`wait_and_get`]: struct.KVStore.html#method.wait_and_get
    /// [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
    /// [`Data`]: enum.KVMessage.html#variant.Data
    /// [`Put`]: enum.KVMessage.html#variant.Put
    /// [`Blob`]: enum.KVMessage.html#variant.Blob
    /// [`Client`]: ../network/struct.Client.html
    /// [`KVStore`]: struct.KVStore.html
    pub(crate) async fn process_messages(
        self: Arc<Self>,
        mut streams: SelectAll<FramedStream<KVMessage>>,
    ) -> Result<(), LiquidError> {
        while let Some(Ok(msg)) = streams.next().await {
            let mut blob_sender_clone = self.blob_sender.clone();
            let kv = self.clone();
            tokio::spawn(async move {
                debug!("Processing a message with id: {:#?}", msg.msg_id);
                match msg.msg {
                    KVMessage::Get(k) => {
                        // This must wait until it has the data to respond
                        let v = kv.wait_and_get_raw(&k).await.unwrap();
                        let response = KVMessage::Data(k, v);
                        kv.network
                            .lock()
                            .await
                            .send_msg(msg.sender_id, response)
                            .await
                            .unwrap();
                    }
                    KVMessage::Data(k, v) => {
                        let v: Arc<T> = Arc::new(deserialize(&v).unwrap());
                        kv.add_to_cache(k, v).await.unwrap();
                        kv.internal_notifier.notify();
                    }
                    KVMessage::Put(k, v) => {
                        if k.home != kv.id {
                            error!("Someone tried to `put` the key {:?} on the wrong KV", k);
                            panic!();
                        }
                        debug!("Put key: {:#?} into KVStore", k.clone());
                        {
                            kv.data.write().await.insert(k, v)
                        };
                        kv.internal_notifier.notify();
                    }
                    KVMessage::Blob(v) => {
                        blob_sender_clone.send(v).await.unwrap();
                    }
                }
            });
        }
        Ok(())
    }

    /// Gets serialized blobs out of this [`KVStore`]
    ///
    /// [`KVStore`]: struct.KVStore.html
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

    /// Requests a serialized blob over the network if we don't have the
    /// data for the given `key`
    async fn wait_and_get_raw(&self, key: &Key) -> Result<Value, LiquidError> {
        if key.home == self.id {
            while { self.data.read().await.get(key) } == None {
                self.internal_notifier.notified().await;
            }
            Ok(self.get_raw(key).await?)
        } else {
            Ok(serialize(&*self.wait_and_get(key).await?)?)
        }
    }

    /// Intelligently add to the cache by ensuring we don't go over the
    /// pre-set limit of `self.max_cache_size`. If adding the `key` and
    /// `value` to the cache will take us over that hard limit, then we will
    /// pop items off the cache on a `LRU` basis until there is enough space,
    /// or no items are left. If the size of the `value` is more than the
    /// `self.max_cache_size` by itself, will panic.
    async fn add_to_cache(
        &self,
        key: Key,
        value: Arc<T>,
    ) -> Result<(), LiquidError> {
        let v_size = value.deep_size_of() as u64;
        if v_size > self.max_cache_size {
            panic!("Download more RAM");
        }
        {
            let mut unlocked = self.cache.lock().await;
            let mut cache_size = unlocked
                .iter()
                .fold(0, |acc, (_, v)| acc + v.deep_size_of())
                as u64;
            while cache_size + v_size > self.max_cache_size {
                let opt_kv = unlocked.pop_lru();
                match opt_kv {
                    Some((_, temp)) => {
                        let temp_size = temp.deep_size_of();
                        debug!(
                            "Popped cached value of size {} bytes",
                            temp_size
                        );
                        cache_size -= temp_size as u64
                    }
                    None => {
                        error!("Tried to add a DF of size {} to an empty cache with max cache size of {}", v_size, self.max_cache_size);
                        panic!()
                    }
                }
            }
            debug!("Added value of size {} bytes to cache", v_size);
            unlocked.put(key, value);
        }
        Ok(())
    }
}
