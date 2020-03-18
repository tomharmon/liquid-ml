use liquid_ml::error::LiquidError;
use liquid_ml::network::client::Client;
use liquid_ml::kv::{KVStore, Key};
use liquid_ml::kv_message::KVMessage;
use std::env;
use tokio::task;

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let args: Vec<String> = env::args().collect();
    let mut c =
        Client::<KVMessage>::new("127.0.0.1:9000".to_string(), args[1].clone())
            .await?;

    let kv = KVStore::new(c);
    let fut0 = kv.network.accept_new_connections();
    let fut = kv.process_messages();
    kv.get(&Key::new("hello".to_string(),1)).await?;
    //tokio::spawn

    //c.send_msg(1, &"hello".to_string());

    fut0.await;
    fut.await.unwrap();
    Ok(())
}
