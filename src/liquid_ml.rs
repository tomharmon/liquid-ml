//! This module defines an application the highest level component of a liquid_ml system. The
//! application exposes a KVStore and a blob receiver that can be used to send random blocs across
//! the network. The blob receiver is designed to be used for control messages.
//!
//! A user of the liquid_ml system need only instantiate an application and provide it an async
//! function to be run. The application grants access to its node_id so different tasks can be done
//! on different nodes.
//!
//! Detailed examples that use the application can be found in the examples directory of this
//! crate.

use crate::dataframe::{Column, DistributedDataFrame, LocalDataFrame, Rower};
use crate::error::LiquidError;
use crate::kv::KVStore;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Receiver, Mutex, Notify, RwLock};

/// Represents an application
pub struct LiquidML {
    /// A pointer to the KVStore that stores all the data for the application
    pub kv: Arc<RwLock<KVStore<LocalDataFrame>>>,
    /// The id of this node, assigned by the registration server
    pub node_id: usize,
    /// A receiver for blob messages (received by the KV) that can be
    /// processed by the user for lower level access to the network
    pub blob_receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    /// The number of nodes in this network
    /// NOTE: Panics if `num_nodes` is inconsistent with this network
    pub num_nodes: usize,
    /// A notifier that gets notified when the server has sent a kill message
    pub kill_notifier: Arc<Notify>,
    /// A map of a DataFrame's name to a DistributedDataFrame
    pub df: HashMap<String, Arc<DistributedDataFrame>>,
    /// The `IP:Port` address of the `Server`
    pub server_addr: String,
    /// The `IP` of this node
    pub my_ip: String,
}

impl LiquidML {
    /// Create a new `liquid_ml` application that runs at `my_addr` and will
    /// wait to connect to `num_nodes` nodes after registering with the
    /// `Server` at the `server_addr` before returning.
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
        let node_id = { kv.read().await.id };
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
            df: HashMap::new(),
            server_addr: server_addr.to_string(),
            my_ip: my_ip.to_string(),
        })
    }

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
            self.blob_receiver.clone(),
        )
        .await?;
        self.df.insert(df_name.to_string(), ddf);
        Ok(())
    }

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
            self.blob_receiver.clone(),
        )
        .await?;
        self.df.insert(df_name.to_string(), ddf);
        Ok(())
    }

    /* Just leaving this here in case we want to add back this function
     * for convenience/faster testing?
     *
     *
    /// Create a new application and split the given SoR file across all the
    /// nodes in the network. Assigns a key with the name `df_name` to
    /// the `DataFrame` chunk for this node.
    ///
    /// Note: assumes the entire SoR file is present on all nodes
    pub async fn from_sor(
        file_name: &str,
        my_addr: &str,
        server_addr: &str,
        num_nodes: usize,
        df_name: &str,
    ) -> Result<Self, LiquidError> {
        let app = Application::new(my_addr, server_addr, num_nodes).await?;
        let file = fs::metadata(file_name).unwrap();
        let f: File = File::open(file_name).unwrap();
        let mut reader = BufReader::new(f);
        let mut size = file.len() / num_nodes as u64;
        // Note: Node ids start at 1
        let from = size * (app.node_id - 1) as u64;

        // advance the reader to this node's starting index then
        // find the next newline character
        let mut buffer = Vec::new();
        reader.seek(SeekFrom::Start(from + size)).unwrap();
        reader.read_until(b'\n', &mut buffer).unwrap();
        size += buffer.len() as u64 + 1;

        let df = DataFrame::from_sor(file_name, from as usize, size as usize);
        let key = Key::new(df_name, app.node_id);
        app.kv.put(&key, df).await?;
        Ok(app)
    }

     *
     */

    pub async fn map<T: Rower + Serialize + Clone + DeserializeOwned + Send>(
        &self,
        df_name: &str,
        rower: T,
    ) -> Result<Option<T>, LiquidError> {
        let df = match self.df.get(df_name) {
            Some(x) => x,
            None => return Err(LiquidError::NotPresent),
        };
        df.map(rower).await
    }

    pub async fn filter<
        T: Rower + Serialize + Clone + DeserializeOwned + Send,
    >(
        &mut self,
        df_name: &str,
        rower: T,
    ) -> Result<(), LiquidError> {
        let df = match self.df.get(df_name) {
            Some(x) => x,
            None => return Err(LiquidError::NotPresent),
        };
        let filtered_df = df.filter(rower, self.blob_receiver.clone()).await?;
        self.df.insert(filtered_df.df_name.clone(), filtered_df);

        Ok(())
    }
}
