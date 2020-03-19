use liquid_ml::error::LiquidError;
use liquid_ml::kv::{KVStore, Key};
use liquid_ml::kv_message::KVMessage;
use liquid_ml::network::client::Client;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let args: Vec<String> = env::args().collect();
    let c =
        Client::<KVMessage>::new("127.0.0.1:9000".to_string(), args[1].clone())
            .await?;
    let my_id = c.id;
    let arc = Arc::new(RwLock::new(c));
    let kv = KVStore::new(arc.clone());
    let fut0 = Client::accept_new_connections(arc);
    let fut = kv.process_messages();
    let key = &Key::new("hello".to_string(), 1);
    let val = String::from("world").into_bytes();
    if my_id == 1 {
        println!("putting val");
        kv.put(key, val.clone()).await.unwrap();
        println!("done putting val");
    } else {
        println!("getting val");
        println!("{:?}", kv.wait_and_get(&key).await.unwrap());
    }

    //c.send_msg(1, &"hello".to_string());

    fut0.await.unwrap();
    fut.await.unwrap();
    Ok(())
}
