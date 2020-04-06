use clap::Clap;
use liquid_ml::liquid::Application;
use liquid_ml::dataframe::*;
use liquid_ml::error::LiquidError;
use log::Level;
use serde::{Deserialize, Serialize};
use simple_logger;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

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

fn reader(file_name: &str) -> Vec<Option<String>> {
    // open the file
    let file = File::open(file_name).unwrap();
    let mut reader = BufReader::new(file);
    // seek to where we should start reading for this nodes' chunk
    let mut words = Vec::new();
    for line in reader.lines() {
        for word in line.unwrap().split_whitespace() {
            words.push(Some(word.to_string()));
        }
    }
    words
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Debug).unwrap();
    let num_nodes = 3;
    let mut app =
        Application::new(&opts.my_address, &opts.server_address, num_nodes)
            .await?;
    let file_name = "examples/100k.txt";


    let words = vec![Column::String(reader(&file_name))];
    app.create_df("words", words).await?;

    let rower = WordCounter {
        map: HashMap::new(),
    };

    let result = app.pmap("words", rower).await?;
    match result {
        Some(joined_rower) => {
            println!("{:#?}", joined_rower.map);
        }
        None => println!("done"),
    }

    app.kill_notifier.notified().await;

    Ok(())
}
