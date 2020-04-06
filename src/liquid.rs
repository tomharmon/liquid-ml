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

use crate::dataframe::{LocalDataFrame, DistributedDataFrame, Rower, Column};
use crate::error::LiquidError;
use crate::kv::{KVStore, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Receiver, Notify, Mutex};
use std::collections::HashMap;

/// Represents an application
pub struct Application {
    /// A pointer to the KVStore that stores all the data for the application
    pub(crate) kv: Arc<Mutex<KVStore<LocalDataFrame>>>,
    /// The id of this node, assigned by the registration server
    pub node_id: usize,
    /// A receiver for blob messages that can b processed by the user
    pub(crate) blob_receiver: Arc<Mutex<Receiver<Value>>>,
    /// The number of nodes in this network
    /// NOTE: Panics if `num_nodes` is inconsistent with this network
    num_nodes: usize,
    /// A notifier that gets notified when the server has sent a kill message
    pub kill_notifier: Arc<Notify>,
    pub df: HashMap<String, DistributedDataFrame>
}

impl Application {
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
        let node_id = {kv.lock().await.id};
        Ok(Application {
            kv,
            node_id,
            blob_receiver: Arc::new(Mutex::new(blob_receiver)),
            num_nodes,
            kill_notifier,
            df: HashMap::new()
        })
    }

    pub async fn create_df(&mut self, name: &str, data: Vec<Column>) -> Result<(), LiquidError> {
        let ddf = DistributedDataFrame::new(data, self.kv.clone(), name.to_string(), self.num_nodes, self.blob_receiver.clone()).await?;
        self.df.insert(name.to_string(), ddf);
        Ok(())
    }
    
    pub async fn pmap<T: Rower + Serialize + Clone + DeserializeOwned + Send>(&self, name: &str, rower: T) -> Result<Option<T>, LiquidError> {
        let df = match self.df.get(name) {
            Some(x) => x,
            None => return Err(LiquidError::NotPresent),
        };
        df.map(rower).await
    }

}

