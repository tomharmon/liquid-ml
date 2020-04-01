use clap::Clap;
use liquid_ml::application::Application;
use liquid_ml::dataframe::*;
use liquid_ml::error::LiquidError;
use log::Level;
use nom::bytes::complete::take_till;
use nom::character::complete::{alpha1, multispace0};
use nom::character::is_alphabetic;
use nom::combinator::map;
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};
use simple_logger;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

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
    sum: i64,
}

impl Rower for WordCounter {
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

    fn join(&mut self, other: &Self) -> Self {
        self.sum += other.sum;
        self.clone()
    }
}

fn next_word(input: &[u8]) -> IResult<&[u8], Option<String>> {
    map(
        delimited(multispace0, alpha1, take_till(is_alphabetic)),
        |s| Some(String::from(std::str::from_utf8(s).unwrap())),
    )(input)
}

fn reader(file_name: &str, from: u64, to: u64) -> DataFrame {
    let schema = Schema::from(vec![]);
    let mut df = DataFrame::new(&schema);
    let file = File::open(file_name).unwrap();
    let mut reader = BufReader::new(file);

    reader.seek(SeekFrom::Start(from as u64)).unwrap();
    let mut reader = reader.take(to - from);
    let mut data = Vec::new();
    reader.read_to_end(&mut data).unwrap();

    // skip the first word up to ws
    let mut words = Vec::new();
    let (remaining, _skipped) = next_word(&data).unwrap();
    let mut to_process = remaining;
    loop {
        if to_process.len() == 0 {
            break;
        }
        let (remaining, word) = next_word(to_process).unwrap();
        words.push(word);
        to_process = remaining;
    }

    println!("{:?}", words.clone());
    df.add_column(Column::String(words), None).unwrap();

    df
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Debug).unwrap();
    let num_nodes = 3;
    let file_name = "file";
    /*let mut app = Application::new(
        &opts.my_address,
        &opts.server_address,
        3
    ).await?;*/
    let node_id = 1;
    let file = std::fs::metadata(file_name).unwrap();
    let f: File = File::open(file_name).unwrap();
    let mut buf_reader = BufReader::new(f);
    let mut size = file.len() / num_nodes as u64;
    // Note: Node ids start at 1
    let from = size * (node_id - 1) as u64;

    // advance the reader to this threads starting index then
    // find the next space
    let mut buffer = Vec::new();
    buf_reader.seek(SeekFrom::Start(from + size)).unwrap();
    buf_reader.read_until(b' ', &mut buffer).unwrap();
    size += buffer.len() as u64;

    let df = reader("file", from, from + size);
    println!("{}", df);
    println!("{}", df.n_cols());
    //app.kill_notifier.notified().await;
    Ok(())
}
