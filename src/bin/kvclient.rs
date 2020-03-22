use liquid_ml::dataframe::DataFrame;
use liquid_ml::error::LiquidError;
use liquid_ml::kv::{KVMessage, KVStore, Key};
use liquid_ml::network::client::Client;
use std::env;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let args: Vec<String> = env::args().collect();
    let notifier = Arc::new(Notify::new());
    let c = Client::<KVMessage>::new(
        "127.0.0.1:9000".to_string(),
        args[1].clone(),
        notifier.clone(),
    )
    .await?;
    let my_id = c.id;
    let arc = Arc::new(RwLock::new(c));
    let kv = KVStore::new(arc.clone(), notifier);
    let fut0 = tokio::spawn(async move {
        Client::accept_new_connections(arc).await.unwrap();
    });
    let fut = kv.process_messages();
    let key = &Key::new("hello".to_string(), 1);
    let val = DataFrame::from_sor(String::from("tests/test.sor"), 0, 10000);
    if my_id == 1 {
        println!("putting val");
        kv.put(key, val.clone()).await.unwrap();
        println!("done putting val");
    } else {
        println!("getting val");
        println!("{:?}", kv.wait_and_get(&key).await.unwrap());
    }

    //c.send_msg(1, &"hello".to_string());

    fut.await.unwrap();
    fut0.await.unwrap();
    Ok(())
}
