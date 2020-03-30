use liquid_ml::application::Application;
use liquid_ml::dataframe::DataFrame;
use liquid_ml::error::LiquidError;
use liquid_ml::kv::{KVStore, Key};
use sorer::dataframe::{Column, Data};
use std::env;
use std::sync::Arc;

async fn producer(kv: Arc<KVStore>) {
    let main = Key::new("main", 1);
    let ck = Key::new("ck", 1);
    let vals: Vec<Option<i64>> = (0..100_000).map(|x| Some(x)).collect();
    let sum = vals.iter().fold(0, |x, y| x + y.unwrap());
    let df1 = DataFrame::from(Column::Int(vals));
    let df2 = DataFrame::from(Data::Int(sum));
    kv.put(&main, df1).await.unwrap();
    kv.put(&ck, df2).await.unwrap();
}

async fn summer(kv: Arc<KVStore>) {
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
    let new_df = DataFrame::from(Data::Int(sum));
    kv.put(&verif, new_df).await.unwrap();
}

async fn verifier(kv: Arc<KVStore>) {
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
    let args: Vec<String> = env::args().collect();
    let app = Application::new(&args[1], "127.0.0.1:9000", 3).await?;

    if app.node_id == 1 {
        app.run(producer).await;
        tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;
    } else if app.node_id == 2 {
        app.run(summer).await;
        tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;
    } else if app.node_id == 3 {
        app.run(verifier).await;
        tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;
    }
    Ok(())
}
