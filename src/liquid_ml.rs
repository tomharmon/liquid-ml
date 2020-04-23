//! This module defines the implementation of the highest level component in
//! a `liquid_ml` system.
use crate::dataframe::{Column, DistributedDataFrame, LocalDataFrame, Rower};
use crate::error::LiquidError;
use crate::kv::KVStore;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Receiver, Mutex, Notify};

/// Represents a `liquid_ml` application, an easy way to create and operate on
/// multiple [`DistributedDataFrame`]s at the same time.
///
/// This struct is the highest level component of a distributed `liquid_ml`
/// system, the application layer. For 90% of use cases, you will not need to
/// use anything else in this crate besides a `LiquidML` struct and your own
/// implementation of a [`Rower`].
///
/// Non-trivial and trivial examples that use the application can be found in
/// the examples directory of this crate.
///
/// [`DistributedDataFrame`]: dataframe/struct.DistributedDataFrame.html
/// [`Rower`]: dataframe/trait.Rower.html
pub struct LiquidML {
    /// A pointer to the `KVStore` that stores all the data for this node of
    /// the application
    pub kv: Arc<KVStore<LocalDataFrame>>,
    /// The id of this node, assigned by the registration [`Server`]
    ///
    /// [`Server`]: network/struct.Server.html
    pub node_id: usize,
    /// A receiver for blob messages (received by the `kv`) that can be
    /// processed by the user for lower level access to the network
    pub blob_receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    /// The number of nodes in this network
    pub num_nodes: usize,
    /// A notifier that gets notified when the [`Server`] has sent a [`Kill`]
    /// message
    ///
    /// [`Server`]: network/struct.Server.html
    /// [`Kill`]: network/enum.ControlMsg.html#variant.Kill
    pub kill_notifier: Arc<Notify>,
    /// A map of a data frame's name to that `DistributedDataFrame`
    pub data_frames: HashMap<String, Arc<DistributedDataFrame>>,
    /// The `IP:Port` address of the [`Server`]
    ///
    /// [`Server`]: network/struct.Server.html
    pub server_addr: String,
    /// The `IP` of this node
    pub my_ip: String,
}

impl LiquidML {
    /// Create a new `liquid_ml` application that runs at `my_addr` and will
    /// wait to connect to `num_nodes` nodes before returning.
    pub async fn new(
        my_addr: &str,
        server_addr: &str,
        num_nodes: usize,
    ) -> Result<Self, LiquidError> {
        let (blob_sender, blob_receiver) = mpsc::channel(20);
        let kill_notifier = Arc::new(Notify::new());
        let kv = KVStore::new(
            server_addr,
            my_addr,
            blob_sender,
            kill_notifier.clone(),
            num_nodes,
            true,
        )
        .await;
        let node_id = kv.id;
        let (my_ip, _my_port) = {
            let mut iter = my_addr.split(':');
            let first = iter.next().unwrap();
            let second = iter.next().unwrap();
            (first, second)
        };

        Ok(LiquidML {
            kv,
            node_id,
            blob_receiver: Arc::new(Mutex::new(blob_receiver)),
            num_nodes,
            kill_notifier,
            data_frames: HashMap::new(),
            server_addr: server_addr.to_string(),
            my_ip: my_ip.to_string(),
        })
    }

    /// Create a new data frame with the given name. The data will be generated
    /// by calling the provided `data_generator` function on node 1, which
    /// will then distribute chunks across all of the nodes.
    ///
    /// `await`ing this function will block until the data is completely
    /// distributed on all nodes. After the data is distributed, each node
    /// of this distributed `liquid_ml` system will have their `LiquidML`
    /// struct updated with the information of the new [`DistributedDataFrame`]
    ///
    /// **NOTE**: if `df_name` is not unique, you will not be able to access
    ///           the old data frame that will be replaced by the new data
    ///           frame, and the old data frame will not be removed from all
    ///           nodes. There is a way to fix this, and may be done at a
    ///           later date.
    ///
    /// [`DistributedDataFrame`]: dataframe/struct.DistributedDataFrame.html
    pub async fn df_from_fn(
        &mut self,
        df_name: &str,
        data_generator: fn() -> Vec<Column>,
    ) -> Result<(), LiquidError> {
        let data = if self.node_id == 1 {
            Some(data_generator())
        } else {
            None
        };
        let ddf = DistributedDataFrame::new(
            &self.server_addr,
            &self.my_ip,
            data,
            self.kv.clone(),
            df_name,
            self.num_nodes,
        )
        .await?;
        self.data_frames.insert(df_name.to_string(), ddf);
        Ok(())
    }

    /// Create a new data frame with the given name. The data comes from a
    /// `SoR` file which is assumed to only exist on node 1. Node 1 will
    /// parse the file into chunks sized so that each node only has 1 or at
    /// most 2 chunks. Node 1 distributes these chunks to all the other nodes,
    /// sending up to 2 chunks concurrently so as to restrict memory usage
    /// because of the large chunk size.
    ///
    /// `await`ing this function will block until the data is completely
    /// distributed on all nodes. After the data is distributed, each node
    /// of this distributed `liquid_ml` system will have their `LiquidML`
    /// struct updated with the information of the new [`DistributedDataFrame`]
    ///
    /// **NOTE**: if `df_name` is not unique, you will not be able to access
    ///           the old data frame that will be replaced by the new data
    ///           frame, and the old data frame will not be removed from all
    ///           nodes. There is a way to fix this, and may be done at a
    ///           later date.
    ///
    /// [`DistributedDataFrame`]: dataframe/struct.DistributedDataFrame.html
    pub async fn df_from_sor(
        &mut self,
        df_name: &str,
        file_name: &str,
    ) -> Result<(), LiquidError> {
        let ddf = DistributedDataFrame::from_sor(
            &self.server_addr,
            &self.my_ip,
            file_name,
            self.kv.clone(),
            df_name,
            self.num_nodes,
        )
        .await?;
        self.data_frames.insert(df_name.to_string(), ddf);
        Ok(())
    }

    /// Create a new data frame that consists of all the chunks in `iter` until
    /// `iter` is consumed. Node 1 will call `next` on the `iter` and
    /// distributes these chunks to all the other nodes, sending up to 2 chunks
    /// concurrently so as to restrict memory usage.
    ///
    /// There is a possibility to increase the concurrency of sending the
    /// chunks, this would change the API slightly but not in a major way.
    ///
    /// `await`ing this function will block until the data is completely
    /// distributed on all nodes. After the data is distributed, each node
    /// of this distributed `liquid_ml` system will have their `LiquidML`
    /// struct updated with the information of the new [`DistributedDataFrame`]
    ///
    /// **NOTE**: if `df_name` is not unique, you will not be able to access
    ///           the old data frame that will be replaced by the new data
    ///           frame, and the old data frame will not be removed from all
    ///           nodes. There is a way to fix this, and may be done at a
    ///           later date.
    ///
    /// [`DistributedDataFrame`]: dataframe/struct.DistributedDataFrame.html
    pub async fn df_from_iter(
        &mut self,
        df_name: &str,
        iter: impl Iterator<Item = Vec<Column>>,
    ) -> Result<(), LiquidError> {
        let ddf = DistributedDataFrame::from_iter(
            &self.server_addr,
            &self.my_ip,
            Some(iter),
            self.kv.clone(),
            df_name,
            self.num_nodes,
        )
        .await?;
        self.data_frames.insert(df_name.to_string(), ddf);
        Ok(())
    }

    /// Given a function, run it on this application. This function only
    /// terminates when a kill signal from the [`Server`] has been sent.
    ///
    /// ## Examples
    /// `examples/demo_client.rs` is a good starting point to see this in
    /// action
    ///
    /// [`Server`]: network/struct.Server.html
    pub async fn run<F, Fut>(self, f: F)
    where
        Fut: Future<Output = ()>,
        F: FnOnce(Arc<KVStore<LocalDataFrame>>) -> Fut,
    {
        f(self.kv.clone()).await;
        self.kill_notifier.notified().await;
    }

    /// Perform a distributed map operation on the [`DistributedDataFrame`] with
    /// the name `df_name` and uses the given `rower`. Returns `Some(rower)`
    /// (of the joined results) if the `node_id` of this
    /// [`DistributedDataFrame`] is `1`, and `None` otherwise.
    ///
    /// A local `pmap` is used on each node to map over that nodes' chunk.
    /// By default, each node will use the number of threads available on that
    /// machine.
    ///
    ///
    /// NOTE:
    /// There is an important design decision that comes with a distinct trade
    /// off here. The trade off is:
    /// 1. Join the last node with the next one until you get to the end. This
    ///    has reduced memory requirements but a performance impact because
    ///    of the synchronous network calls
    /// 2. Join all nodes with one node by sending network messages
    ///    concurrently to the final node. This has increased memory
    ///    requirements and greater complexity but greater performance because
    ///    all nodes can asynchronously send to one node at the same time.
    ///
    /// This implementation went with option 1 for simplicity reasons
    ///
    /// [`DistributedDataFrame`]: dataframe/struct.DistributedDataFrame.html
    pub async fn map<T: Rower + Serialize + Clone + DeserializeOwned + Send>(
        &self,
        df_name: &str,
        rower: T,
    ) -> Result<Option<T>, LiquidError> {
        let df = match self.data_frames.get(df_name) {
            Some(x) => x,
            None => return Err(LiquidError::NotPresent),
        };
        df.map(rower).await
    }

    /// Perform a distributed filter operation on the [`DistributedDataFrame`]
    /// with the name `df_name` and uses the given `rower`.  This function
    /// does not mutate the [`DistributedDataFrame`] in anyway, instead, it
    /// creates a new [`DistributedDataFrame`] of the results. This
    /// [`DistributedDataFrame`] is returned to every node so that the results
    /// are consistent everywhere.
    ///
    /// A local `pfilter` is used on each node to filter over that nodes'
    /// chunks.  By default, each node will use the number of threads available
    /// on that machine.
    ///
    /// [`DistributedDataFrame`]: dataframe/struct.DistributedDataFrame.html
    pub async fn filter<
        T: Rower + Serialize + Clone + DeserializeOwned + Send,
    >(
        &mut self,
        df_name: &str,
        rower: T,
    ) -> Result<(), LiquidError> {
        let df = match self.data_frames.get(df_name) {
            Some(x) => x,
            None => return Err(LiquidError::NotPresent),
        };
        let filtered_df = df.filter(rower).await?;
        self.data_frames
            .insert(filtered_df.df_name.clone(), filtered_df);

        Ok(())
    }
}
