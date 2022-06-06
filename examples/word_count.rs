use clap::Parser;
use liquid_ml::{
    dataframe::{Column, Data, Row, Rower},
    error::LiquidError,
    LiquidML,
};
use log::Level;
use serde::{Deserialize, Serialize};
use simple_logger;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

/// This is a simple example showing how to load a sor file from disk and
/// distribute it across nodes, and perform pmap
#[derive(Parser)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct Opts {
    /// The IP:Port at which the registration server is running
    #[clap(
        short = 's',
        long = "server_addr",
        default_value = "127.0.0.1:9000"
    )]
    server_address: String,
    /// The IP:Port at which this application must run
    #[clap(short = 'm', long = "my_addr", default_value = "127.0.0.2:9002")]
    my_address: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct WordCounter {
    map: HashMap<String, usize>,
}

impl Rower for WordCounter {
    fn visit(&mut self, r: &Row) -> bool {
        let i = r.get(0).unwrap();
        match i {
            Data::String(val) => {
                match self.map.get_mut(val) {
                    Some(num_occurences) => *num_occurences += 1,
                    None => {
                        self.map.insert(val.clone(), 1);
                    }
                };
                true
            }
            _ => panic!(),
        }
    }

    fn join(mut self, other: Self) -> Self {
        for (k, v) in other.map.iter() {
            match self.map.get_mut(k) {
                Some(num_occurences) => *num_occurences += v,
                None => {
                    self.map.insert(k.clone(), *v);
                }
            }
        }
        self
    }
}

fn reader() -> Vec<Column> {
    // open the file
    let file = File::open("examples/100k.txt").unwrap();
    let reader = BufReader::new(file);
    // seek to where we should start reading for this nodes' chunk
    let mut words = Vec::new();
    for line in reader.lines() {
        for word in line.unwrap().split_whitespace() {
            words.push(Some(word.to_string()));
        }
    }
    vec![Column::String(words)]
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Error).unwrap();
    let mut app =
        LiquidML::new(&opts.my_address, &opts.server_address, 3).await?;

    app.df_from_fn("words", reader).await?;

    let rower = WordCounter {
        map: HashMap::new(),
    };

    let result = app.map("words", rower).await?;
    match result {
        Some(joined_rower) => {
            let mut as_vec: Vec<(&String, &usize)> =
                joined_rower.map.iter().collect();
            as_vec.sort_by(|(a, _), (b, _)| a.cmp(b));
            as_vec.iter().for_each(|(x, y)| println!("{}: {}", x, y));
        }
        None => println!("done"),
    }

    app.kill_notifier.notified().await;

    Ok(())
}
