use clap::Clap;
use liquid_ml::application::Application;
use liquid_ml::error::LiquidError;
use liquid_ml::kv::Key;

/// This is a simple example showing how to load a sor file from disk and
/// distribute it across nodes
#[derive(Clap)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct Opts {
    /// The IP:Port at which the registration server is running
    #[clap(
        short = "s",
        long = "server_addr",
        default_value = "127.0.0.1:9000"
    )]
    server_address: String,
    /// The IP:Port at which this application must run
    #[clap(short = "m", long = "my_addr", default_value = "127.0.0.2:9002")]
    my_address: String,
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    let app = Application::from_sor(
        "tests/distributed.sor",
        &opts.my_address,
        &opts.server_address,
        3,
    )
    .await?;

    let k = Key::new("420", app.node_id);
    let df = app.kv.get(&k).await?;
    println!("{:?}", df.n_rows());
    println!("{}", df);
    app.kill_notifier.notified().await;

    Ok(())
}
