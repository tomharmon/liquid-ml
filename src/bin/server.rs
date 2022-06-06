//! This binary is packaged with the `liquid_ml` application and is a default
//! implementation of a registration server. This needs to be running to
//! facilitate connections between different nodes in the system. Refer to the
//! [`network`](../liquid_ml/network/index.html) module for further information.
use clap::Parser;
use liquid_ml::error::LiquidError;
use liquid_ml::network::Server;
use log::Level;

/// This is a simple registration server for a `liquid_ml` system and comes
/// packaged with the `liquid_ml` system. This needs to be running to
/// facilitate connections between different nodes in the system, and each
/// node must be given the `IP:Port` address of the server.
#[derive(Parser)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct Opts {
    /// The `IP:Port` at which this server must run
    #[clap(short = 'a', long = "address", default_value = "127.0.0.1:9000")]
    address: String,
}

/// Can be run by building the binary and running the command:
/// `cargo --run --bin server`
#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Info).unwrap();
    let mut s = Server::new(&opts.address).await?;
    s.accept_new_connections().await?;
    Ok(())
}
