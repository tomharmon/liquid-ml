use bincode::{deserialize, serialize};
use bytecount;
use clap::Clap;
use futures::future::try_join_all;
use liquid_ml::dataframe::{Data, LocalDataFrame, Row, Rower};
use liquid_ml::error::LiquidError;
use liquid_ml::liquid_ml::LiquidML;
use log::Level;
use serde::{Deserialize, Serialize};
use simple_logger;
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
    /// The number of nodes for the distributed system
    #[clap(short = "n", long = "num_nodes", default_value = "3")]
    num_nodes: usize,
    /// The name of the data file
    #[clap(
        short = "d",
        long = "data",
        default_value = "/home/tom/Downloads/spy_processed.sor"
    )]
    data: String,
}

/// Purged walk-forward cross-validation: used because of the drawbacks for
/// applying k-fold cross-validation to time-series data. Further explanation
/// found here:
///
/// https://medium.com/@samuel.monnier/cross-validation-tools-for-time-series-ffa1a5a09bf9
///
/// Splits a dataset into `k` equal blocks of contiguous samples, and a
/// training set of `p` contiguous blocks. The returned splits are then:
///
/// 1. Train set: blocks 1 to p, validation set: block p+1
/// 2. Train set: blocks 2 to p+1, validation set: block p+2
/// 3. …
///
/// The returned vec is a list of (training set, validation set)
fn walk_fwd_cross_val_split(
    data: &LocalDataFrame,
    n_splits: usize,
    period: usize,
) -> Vec<(LocalDataFrame, LocalDataFrame)> {
    // calculate p = data.len() / n_splits
    let p = data.n_rows();

    let mut split_data = Vec::new();
    let mut cur_row = 0;
    for _ in 0..n_splits {
        // for each split
        let mut training_data = LocalDataFrame::new(data.get_schema());
        let mut row = Row::new(data.get_schema());
        for _ in 0..p {
            data.fill_row(cur_row, &mut row).unwrap();
            // collect rows 0..p and add to train set
            training_data.add_row(&row).unwrap();
            cur_row += 1;
        }
        // skip the training samples whose evaluation time is posterior to the
        // prediction time of validation samples
        cur_row += period;

        // collect rows 0..p and add to validation set
        let mut validation_data = LocalDataFrame::new(data.get_schema());
        for _ in 0..p {
            data.fill_row(cur_row, &mut row).unwrap();
            // collect rows 0..p and add to validation set
            validation_data.add_row(&row).unwrap();
            cur_row += 1;
        }

        cur_row += period;
        split_data.push((training_data, validation_data));
    }

    split_data
}

/// Finds all the projects that these users have ever worked on
#[derive(Clone, Serialize, Deserialize, Debug)]
struct RandomForest {
    users: Vec<u8>,
}

impl Rower for RandomForest {
    fn visit(&mut self, r: &Row) -> bool {
        true
    }

    fn join(mut self, other: Self) -> Self {
        self
    }
}

fn count_new_lines(file_name: &str) -> usize {
    let mut buf_reader = BufReader::new(File::open(file_name).unwrap());
    let mut new_lines = 0;

    loop {
        let bytes_read = buf_reader.fill_buf().unwrap();
        let len = bytes_read.len();
        if len == 0 {
            return new_lines;
        };
        new_lines += bytecount::count(bytes_read, b'\n');
        buf_reader.consume(len);
    }
}

#[tokio::main]
async fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    simple_logger::init_with_level(Level::Error).unwrap();
    let mut app =
        LiquidML::new(&opts.my_address, &opts.server_address, opts.num_nodes)
            .await?;
    app.df_from_sor("data", &opts.data).await?;

    app.kill_notifier.notified().await;

    Ok(())
}
