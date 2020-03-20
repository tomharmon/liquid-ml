use liquid_ml::error::LiquidError;
use liquid_ml::network::client::Client;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let args: Vec<String> = env::args().collect();
    let c =
        Client::<String>::new("127.0.0.1:9000".to_string(), args[1].clone())
            .await?;
    let my_id = c.id;
    let arc = Arc::new(RwLock::new(c));
    let fut0 = Client::accept_new_connections(arc.clone());
    if my_id == 2 {
        println!("sending msgs");
        //tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
        {
            arc.write()
                .await
                .send_msg(1, String::from("hello"))
                .await
                .unwrap();
        }
        {
            arc.write()
                .await
                .send_msg(1, String::from("hello222222"))
                .await
                .unwrap();
        }
        println!("done sending msg");
    }

    fut0.await.unwrap();
    Ok(())
}
