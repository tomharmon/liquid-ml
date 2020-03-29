use liquid_ml::error::LiquidError;
use liquid_ml::network::Server;

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let mut s = Server::new("127.0.0.1:9000".to_string()).await?;
    s.accept_new_connections().await?;
    Ok(())
}
