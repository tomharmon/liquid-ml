use liquid_ml::application::Application;
use liquid_ml::dataframe::DataFrame;
use liquid_ml::error::LiquidError;
use liquid_ml::kv::Key;
use sorer::dataframe::{Column, Data};
use std::env;

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let args: Vec<String> = env::args().collect();
    let app = Application::new(&args[1], "127.0.0.1:9000", 3).await?;

    let main = Key::new("main", 1);
    let verif = Key::new("verif", 1);
    let ck = Key::new("ck", 1);

    if app.node_id == 1 {
        let vals: Vec<Option<i64>> = (0..100_000).map(|x| Some(x)).collect();
        let sum = vals.iter().fold(0, |x, y| x + y.unwrap());
        let df1 = DataFrame::from(Column::Int(vals));
        let df2 = DataFrame::from(Data::Int(sum));
        app.kv.put(&main, df1).await?;
        app.kv.put(&ck, df2).await?;
    } else if app.node_id == 2 {
        let df = app.kv.wait_and_get(&main).await?;
        let mut sum = 0;
        for i in 0..100_000 {
            if let Data::Int(x) = df.get(0, i)? {
                sum += x;
            } else {
                unreachable!()
            }
        }
        let new_df = DataFrame::from(Data::Int(sum));
        app.kv.put(&verif, new_df).await?;
    } else if app.node_id == 3 {
        let df2 = app.kv.wait_and_get(&ck).await?;
        let df1 = app.kv.wait_and_get(&verif).await?;
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
    app.go().await;
    Ok(())
}
