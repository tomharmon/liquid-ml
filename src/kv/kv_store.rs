//! The `KVStore` implementation
use crate::error::LiquidError;
use crate::kv::{KVMessage, Key, Value};
use crate::network::{Client, Message};
use crate::{
    BYTES_PER_GB, BYTES_PER_KIB, KV_STORE_CACHE_SIZE_FRACTION,
    MAX_NUM_CACHED_VALUES,
};
use bincode::{deserialize, serialize};
use deepsize::DeepSizeOf;
use log::{debug, error, info};
use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex, Notify, RwLock,
};

/// A distributed `Key`, `Value` store which is generic for type `T`. Since
/// this is a distributed `KVStore`, `Key`s know which node the values 'belong'
/// to.
///
/// Internally `KVStore`s store their data in memory as serialized blobs
/// (`Vec<u8>`). The `KVStore` caches deserialized `Value`s into their type
/// `T` on a least-recently used basis.
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
    pub(crate) network: Arc<RwLock<Client<KVMessage>>>,
    /// Used internally for processing data and messages
    internal_notifier: Notify,
    /// The `id` of the node this `KVStore` is running on
    pub(crate) id: usize,
    /// A channel to send blobs of data to a higher level component, in
    /// `liquid-ml` this would be the `Application`
    blob_sender: Sender<Value>,
    /// The total amount of memory (in bytes) this `KVStore` is allowed
    /// to keep in its cache
    max_cache_size: u64,
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
    /// Creates a new distributed `KVStore`.
    ///
    /// ## Parameters
    /// - `server_addr`: the `IP:Port` of the registration `Server`, used
    ///    for orchestrating the connection of all distributed nodes in the
    ///    system.
    /// - `my_addr` is the `IP:Port` of this `KVStore`.
    /// - `blob_sender`: is the sending half of an `mpsc` channel that is
    ///    passed in by components using this `KVStore` to facilitate
    ///    lower level messages. Whenever this `KVStore` receives serialized
    ///    blobs of data from other `KVStore`s in the distributed system, it
    ///    will use the `blob_sender` to forward the blob to `LiquidML`.
    /// - `kill_notifier`: is passed in by `LiquidML`, then is passed down to
    ///    its `Client` so that when the `Client` gets `Kill` messages from
    ///    the `Server`, the `Client` can notify `LiquidML` it is time to
    ///    shut down in an orderly fashion.
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
        let (sender, receiver) = mpsc::channel(64);
        let (my_ip, my_port) = {
            let mut iter = my_addr.split(':');
            let first = iter.next().unwrap();
            let second = iter.next().unwrap();
            (first, second)
        };
        let network = Client::new(
            server_addr,
            my_ip,
            Some(my_port),
            sender,
            kill_notifier,
            num_clients,
            wait_for_all_clients,
            "kvstore",
        )
        .await
        .unwrap();
        let id = { network.read().await.id };

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
    /// 1. You know that the data was `put` some microseconds around when
    ///    `wait_and_get` was called, but can't guarantee it happened exactly
    ///    before `wait_and_get` is called. In this case, this function can
    ///    be `await`ed
    /// 2. The data will be `put` on this `KVStore` sometime in the
    ///    future at an unknown time. In this case, this function should
    ///    not be `await`ed but instead given a callback closure via
    ///    calling `and_then` on the returned future
    ///
    /// If you do not do that, for example in the second case you `await`
    /// despite our warning, then you will waste a lot of time waiting for the
    /// data to be transferred over the network.
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
                    .write()
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
            self.network.write().await.send_msg(target_id, msg).await?;
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
        self.network
            .write()
            .await
            .send_msg(target_id, KVMessage::Blob(blob))
            .await
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
        self: Arc<Self>,
        mut receiver: Receiver<Message<KVMessage>>,
    ) -> Result<(), LiquidError> {
        loop {
            let msg = receiver.recv().await.unwrap();
            let mut sender_clone = self.blob_sender.clone();
            let kv = self.clone();
            tokio::spawn(async move {
                debug!("Processing a message with id: {:#?}", msg.msg_id);
                match msg.msg {
                    KVMessage::Get(k) => {
                        // This must wait until it has the data to respond
                        let v = kv.wait_and_get_raw(&k).await.unwrap();
                        let response = KVMessage::Data(k, v);
                        kv.network
                            .write()
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
                        sender_clone.send(v).await.unwrap();
                    }
                }
            });
        }
    }

    /// Gets serialized blobs out of this `KVStore`
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
    /// `max_cache_size` by itself, will panic.
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
