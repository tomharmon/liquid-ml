use liquid_ml::error::LiquidError;
use liquid_ml::network::client::Client;
use std::env;

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let args: Vec<String> = env::args().collect();
    let mut c =
        Client::<String>::new("127.0.0.1:9000".to_string(), args[1].clone())
            .await?;

    let jh = tokio::spawn(async move {
        c.accept_new_connections().await.unwrap();
    });

    //c.send_msg(1, &"hello".to_string());
    //tokio::spawn(async)

    jh.await.unwrap();
    Ok(())
}
