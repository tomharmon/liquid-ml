use liquid_ml::dataframe::DataFrame;
use liquid_ml::error::LiquidError;
use liquid_ml::kv::{KVMessage, KVStore, Key};
use liquid_ml::network::client::Client;
use sorer::dataframe::{Column, Data};
use std::env;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::time::{delay_for, Duration};

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    // Boiler plate that will be abstracted out later starts here :
    let args: Vec<String> = env::args().collect();
    let notifier = Arc::new(Notify::new());
    let c = Client::<KVMessage>::new(
        "127.0.0.1:9000".to_string(),
        args[1].clone(),
        notifier.clone(),
    )
    .await?;
    let my_id = c.id;
    let arc = Arc::new(RwLock::new(c));
    let kv = KVStore::new(arc.clone(), notifier);
    let fut0 = tokio::spawn(async move {
        Client::accept_new_connections(arc).await.unwrap();
    });
    let kv_arc = Arc::new(kv);
    let arc_new = kv_arc.clone();
    let fut = tokio::spawn(async move {
        KVStore::process_messages(arc_new).await.unwrap();
    });
    // Ends Here

    let main = Key::new("main".to_string(), 1);
    let verif = Key::new("verif".to_string(), 1);
    let ck = Key::new("ck".to_string(), 1);
    delay_for(Duration::from_secs(3)).await;

    if my_id == 1 {
        let vals: Vec<Option<i64>> = (0..100_000).map(|x| Some(x)).collect();
        let sum = vals.iter().fold(0, |x, y| x + y.unwrap());
        let df1 = DataFrame::from(Column::Int(vals));
        let df2 = DataFrame::from(Data::Int(sum));
        println!("--id 1: Done summing, adding to KV");
        kv_arc.put(&main, df1).await?;
        kv_arc.put(&ck, df2).await?;
        println!("--id 1: Done");
    } else if my_id == 2 {
        println!("--id 2: waiting for data");
        let df = kv_arc.wait_and_get(&main).await?;
        println!("--id 2: got the data");
        let mut sum = 0;
        for i in 0..100_000 {
            if let Data::Int(x) = df.get(0, i)? {
                sum += x;
            } else {
                unreachable!()
            }
        }
        println!("--id 2: the sum is {}", sum);
        let new_df = DataFrame::from(Data::Int(sum));
        kv_arc.put(&verif, new_df).await?;
        println!("--id 2: done");
    } else if my_id == 3 {
        println!("--id 3: waiting for data");
        let df1 = kv_arc.wait_and_get(&verif).await?;
        let df2 = kv_arc.wait_and_get(&ck).await?;
        match (df1.get(0, 0)?, df2.get(0, 0)?) {
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

    //let val = DataFrame::from_sor(String::from("tests/test.sor"), 0, 10000);

    //c.send_msg(1, &"hello".to_string());

    fut.await.unwrap();
    fut0.await.unwrap();
    Ok(())
}
