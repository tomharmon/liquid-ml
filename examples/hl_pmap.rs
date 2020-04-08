use clap::Clap;
use liquid_ml::dataframe::{Data, LocalDataFrame, Row, Rower};
use liquid_ml::error::LiquidError;
use liquid_ml::liquid::Application;
use log::Level;
use serde::{Deserialize, Serialize};
use simple_logger;

/// This is a simple example showing how to load a sor file from disk and
/// distribute it across nodes, and perform pmap
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

#[derive(Clone, Serialize, Deserialize, Debug)]
struct IntSummer {
    sum: i64,
}

impl Rower for IntSummer {
    fn visit(&mut self, r: &Row) -> bool {
        let i = r.get(0).unwrap();
        match i {
            Data::Int(val) => {
                self.sum += *val;
                true
            }
            _ => panic!(),
        }
    }

    fn join(mut self, other: Self) -> Self {
        self.sum += other.sum;
        self
    }
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Debug).unwrap();
    let mut app =
        Application::new(&opts.my_address, &opts.server_address, 3).await?;
    app.df_from_sor("dist", "tests/distributed.sor").await?;
    println!("{:?}", app.df.get("dist").unwrap().data);
    let r = app.pmap("dist", IntSummer { sum: 0 }).await?;
    match r {
        None => println!("Done"),
        Some(x) => println!("the sum is : {}", x.sum),
    }

    let df = LocalDataFrame::from_sor("tests/distributed.sor", 0, 1000000);
    let r2 = df.pmap(IntSummer { sum: 0 });
    println!("summing locally yields: {}", r2.sum);
    let r3 = df.map(IntSummer { sum: 0 });
    println!("summing locally sequentially yields: {}", r3.sum);
    app.kill_notifier.notified().await;

    Ok(())
}
