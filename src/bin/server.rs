//! This binary is packaged with the liquid_ml application and is a default implementation of a
//! registration server. This needs to be running to facilitate connections between differnt nodes
//! in the system.  

use clap::Clap;
use liquid_ml::error::LiquidError;
use liquid_ml::network::Server;

/// This is a simple registration server for a liquid_ml system and comes packaged with the
/// liquid_ml system. Refer to docs.rs/liquid_ml for further information.   
#[derive(Clap)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct Opts {
    /// The IP at which this server must run
    #[clap(short = "i", long = "ip", default_value = "127.0.0.1:9000")]
    ip: String,
}
#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    let mut s = Server::new(opts.ip).await?;
    s.accept_new_connections().await?;
    Ok(())
}
