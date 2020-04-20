use clap::Clap;
use liquid_ml::{
    dataframe::{Column, Data, LocalDataFrame},
    error::LiquidError,
    kv::{KVStore, Key},
    LiquidML,
};
use log::Level;
use simple_logger;
use std::sync::Arc;

/// This is a simple demo client running the Milestone 1 example code.
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

async fn producer(kv: Arc<KVStore<LocalDataFrame>>) {
    let main = Key::new("main", 1);
    let ck = Key::new("ck", 1);
    let vals: Vec<Option<i64>> = (0..100_000).map(|x| Some(x)).collect();
    let sum = vals.iter().fold(0, |x, y| x + y.unwrap());
    let df1 = LocalDataFrame::from(Column::Int(vals));
    let df2 = LocalDataFrame::from(Data::Int(sum));
    kv.put(main, df1).await.unwrap();
    kv.put(ck, df2).await.unwrap();
}

async fn summer(kv: Arc<KVStore<LocalDataFrame>>) {
    let verif = Key::new("verif", 1);
    let main = Key::new("main", 1);
    let df = kv.wait_and_get(&main).await.unwrap();
    let mut sum = 0;
    for i in 0..100_000 {
        if let Data::Int(x) = df.get(0, i).unwrap() {
            sum += x;
        } else {
            unreachable!()
        }
    }
    let new_df = LocalDataFrame::from(Data::Int(sum));
    kv.put(verif, new_df).await.unwrap();
}

async fn verifier(kv: Arc<KVStore<LocalDataFrame>>) {
    let ck = Key::new("ck", 1);
    let verif = Key::new("verif", 1);
    let df2 = kv.wait_and_get(&ck).await.unwrap();
    let df1 = kv.wait_and_get(&verif).await.unwrap();
    match (df1.get(0, 0).unwrap(), df2.get(0, 0).unwrap()) {
        (Data::Int(x), Data::Int(y)) => {
            if x == y {
                println!("SUCCESS")
            } else {
                println!("FAILURE")
            };
        }
        _ => unreachable!(),
    }
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Debug).unwrap();
    let app = LiquidML::new(&opts.my_address, &opts.server_address, 3).await?;

    if app.node_id == 1 {
        app.run(producer).await;
    } else if app.node_id == 2 {
        app.run(summer).await;
    } else if app.node_id == 3 {
        app.run(verifier).await;
    }
    Ok(())
}
