use crate::dataframe::DataFrame;
use crate::error::LiquidError;
use crate::kv::{KVMessage, KVStore, Key};
use crate::network::client::Client;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

pub struct Application {
    pub kv: Arc<KVStore>,
    pub node_id: usize,
    // TODO: maybe use a runtime here
    msg_processor: JoinHandle<()>,
    conn_processor: JoinHandle<()>,
}

impl Application {
    pub async fn new(
        my_addr: &str,
        server_addr: &str,
    ) -> Result<Self, LiquidError> {
        let notifier = Arc::new(Notify::new());
        let c = Client::<KVMessage>::new(
            server_addr.to_string(),
            my_addr.to_string(),
            notifier.clone(),
        )
        .await?;
        let node_id = c.id;
        let arc = Arc::new(RwLock::new(c));
        let kv = KVStore::new(arc.clone(), notifier, node_id);
        let fut0 = tokio::spawn(async move {
            Client::accept_new_connections(arc).await.unwrap();
        });
        let kv_arc = Arc::new(kv);
        let arc_new = kv_arc.clone();
        let fut1 = tokio::spawn(async move {
            KVStore::process_messages(arc_new).await.unwrap();
        });
        Ok(Application {
            kv: kv_arc,
            node_id,
            msg_processor: fut1,
            conn_processor: fut0,
        })
    }

    pub async fn from_sor(
        filename: &str,
        my_addr: &str,
        server_addr: &str,
        num_nodes: usize,
    ) -> Result<Self, LiquidError> {
        let app = Application::new(my_addr, server_addr).await?;
        let file = std::fs::metadata(filename).unwrap();
        // Note: Node ids start at 1
        // TODO: IMPORTANT ROUNDING ERRORS
        let size = file.len() / num_nodes as u64;
        let from = size * (app.node_id - 1) as u64;
        let df = DataFrame::from_sor(
            String::from(filename),
            from as usize,
            size as usize,
        );
        let key = Key::new(String::from("420"), app.node_id);
        app.kv.put(&key, df).await?;
        Ok(app)
    }

    pub async fn go(self) {
        self.msg_processor.await.unwrap();
        self.conn_processor.await.unwrap();
    }
}
