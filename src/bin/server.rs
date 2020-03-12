use rspark::error::LiquidError;
use rspark::network::server::Server;

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let mut s = Server::new("127.0.0.1:9000".to_string()).await?;
    s.accept_new_connections().await?;
    Ok(())
}
