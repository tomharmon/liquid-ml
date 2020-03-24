use crate::error::LiquidError;
use crate::kv::{KVMessage, KVStore};
use crate::network::client::Client;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

pub struct Application {
    pub kv: Arc<KVStore>,
    pub node_id: usize,
    // Use a runtime here
    pub msg_processor: JoinHandle<()>,
    pub conn_processor: JoinHandle<()>
}

impl Application {
    pub async fn new(my_addr: &str, server_addr: &str) -> Result<Self, LiquidError> {
        let notifier = Arc::new(Notify::new());
        let c = Client::<KVMessage>::new(
            server_addr.to_string(),
            my_addr.to_string(),
            notifier.clone(),
        ) .await?;
        let node_id = c.id;
        let arc = Arc::new(RwLock::new(c));
        let kv = KVStore::new(arc.clone(), notifier);
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
            msg_processor : fut1,
            conn_processor: fut0,
        })
   }
    pub async fn go(self) {
        self.msg_processor.await.unwrap();
        self.conn_processor.await.unwrap();
    }
    /*
  pub fn run<'async>(
        &'async self,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'async>>
    where
        Self: Sync + 'async,
    {
        async fn run(_self: &AutoplayingVideo) {
            /* the original method body */
        }

        Box::pin(run(self))
    }
    pub async fn run(func: fn(KVStore, usize) -> Box<std::pin::Pin<dyn Future<Output=()>>>) {
        func().await;


    }*/

}
