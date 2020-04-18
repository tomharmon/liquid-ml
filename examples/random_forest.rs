use bincode::{deserialize, serialize};
use bytecount;
use clap::Clap;
use futures::future::try_join_all;
use liquid_ml::dataframe::{Column, Data, LocalDataFrame, Row, Rower};
use liquid_ml::error::LiquidError;
use liquid_ml::liquid_ml::LiquidML;
use log::Level;
use rand::{self, Rng};
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

#[derive(Debug, Clone)]
struct Split {
    value: f64,
    feature_idx: usize,
    groups: Vec<LocalDataFrame>,
}

#[derive(Debug, Clone)]
enum DecisionTree {
    Node {
        left: Box<DecisionTree>,
        right: Box<DecisionTree>,
    },
    Leaf(bool),
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
    let p = data.n_rows() / n_splits;

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

// returns accuracy from 0-1
fn accuracy(actual: Vec<Option<bool>>, predicted: Vec<Option<bool>>) -> f64 {
    assert_eq!(actual.len(), predicted.len());
    actual
        .iter()
        .zip(predicted.iter())
        .fold(0, |acc, (actual, pred)| {
            if &actual.unwrap() == &pred.unwrap() {
                acc + 1
            } else {
                acc
            }
        }) as f64
        / actual.len() as f64
}

#[derive(Debug, Clone)]
struct TestSplit {
    left: LocalDataFrame,
    right: LocalDataFrame,
    value: f64,
    feature_idx: usize,
}

impl Rower for TestSplit {
    fn visit(&mut self, row: &Row) -> bool {
        if row.get(self.feature_idx).unwrap().unwrap_float() < self.value {
            self.left.add_row(row).unwrap();
        } else {
            self.right.add_row(row).unwrap();
        }
        true
    }

    fn join(mut self, other: Self) -> Self {
        self.left = self.left.combine(other.left).unwrap();
        self.right = self.right.combine(other.right).unwrap();
        self
    }
}

struct SplitInfo {
    index: usize,
    value: f64,
    left: Split,
    right: Split,
}

// this assumes the last column is a boolean label
fn gini_index(groups: &[LocalDataFrame], classes: &[bool]) -> f64 {
    let n_samples = groups[0].n_rows() + groups[1].n_rows();
    let mut gini = 0.0;

    for group in groups {
        if group.n_rows() == 0 {
            continue;
        }
        let mut score = 0.0;
        for class in classes {
            let p = match group.data.get(group.n_cols() - 1).unwrap() {
                Column::Bool(c) => c.iter().fold(0.0, |acc, v| {
                    if v.unwrap() == *class {
                        acc + 1.0
                    } else {
                        acc
                    }
                }),
                _ => panic!(),
            };
            score += p * p;
        }
        gini += (1.0 - score) * (group.n_rows() as f64 / n_samples as f64);
    }

    gini
}

fn get_split(data: LocalDataFrame) -> Split {
    /*let mut features = Vec::new();
    let mut rng = rand::thread_rng();
    while features.len() < n_features {
        let i = rng.gen::<u32>();
        if !features.contains(&i) {
            features.push(i);
        }
    }*/
    let class_labels = vec![true, false];

    let mut split = Split {
        feature_idx: 0,
        value: 0.0,
        groups: Vec::new()
    };
    let mut best_score = 1_000_000_000.0;
    let mut best_value = 1_000_000_000.0;
    let mut row = Row::new(data.get_schema());
    for feature_idx in 0..data.n_cols() - 1 {
        for i in 0..data.n_rows() {
            let best_value =
                data.get(feature_idx as usize, i).unwrap().unwrap_float();
            let mut test_split = TestSplit {
                feature_idx: feature_idx as usize,
                value: best_value,
                left: LocalDataFrame::new(data.get_schema()),
                right: LocalDataFrame::new(data.get_schema()),
            };

            test_split = data.pmap(test_split);
            let groups = vec![test_split.left, test_split.right];
            let gini = gini_index(&groups, &class_labels);
            if gini < best_score {
                split.feature_idx = feature_idx as usize;
                split.value = best_value;
                split.groups = groups;
                best_score = gini;
            }
        }
    }

    split
}

struct NumTrueRower {
    num_trues: usize,
}

impl Rower for NumTrueRower {
    fn visit(&mut self, row: &Row) -> bool {
        if row.get(row.width() - 1).unwrap().unwrap_bool() {
            self.num_trues += 1;
        }
        true
    }

    fn join(mut self, other: Self) -> Self {
        self.num_trues += other.num_trues;
        self
    }
}

// returns the most common output value, assumes predictions are boolean values
fn to_terminal(data: LocalDataFrame) -> bool {
    let mut r = NumTrueRower { num_trues: 0 };
    r = data.map(r);
    r.num_trues > (data.n_rows() / 2)
}


fn split(
    mut node: Split,
    max_depth: usize,
    min_size: usize,
    depth: usize,
) -> DecisionTree {

    let left = node.groups.get(0).unwrap();
    let right = node.groups.get(1).unwrap();

    if left.n_rows() == 0 || right.n_rows() == 0 {
        return DecisionTree::Leaf(to_terminal(left.combine(right.clone()).unwrap()));
    }
    if depth >= max_depth {
        return DecisionTree::Node {
            left: Box::new(DecisionTree::Leaf(to_terminal(left))),
            right: Box::new(DecisionTree::Leaf(to_terminal(right))),
        };
    }
    DecisionTree::Leaf(false)
}


/*fn split(
    node: &mut DecisionTree,
    max_depth: usize,
    min_size: usize,
    depth: usize,
) {
    match node {
        DecisionTree::Node { left, right, split } => {
            let l = split.l
            match (left, right) {
                (DecisionTree::Leaf(b1), DecisionTree::Leaf(b2) => DecisionTree::Leaf( b1 && b2 ),


            }
            // check for a no split
            if split.groups[0].n_rows() == 0 || split.groups[1].n_rows() == 0 {
                let combined =
                    split.groups[0].combine(split.groups[1]).unwrap();
                let leaf = DecisionTree::Leaf(to_terminal(combined));
                left = &mut Box::new(Some(leaf.clone()));
                right = &mut Box::new(Some(leaf));
                return;
            }
            // check for max depth
            if depth >= max_depth {
                let left_leaf =
                    DecisionTree::Leaf(to_terminal(split.groups[0]));
                let right_leaf =
                    DecisionTree::Leaf(to_terminal(split.groups[1]));
                left = &mut Box::new(Some(left_leaf.clone()));
                right = &mut Box::new(Some(right_leaf.clone()));
                return;
            }
            // process the left child
            if split.groups[0].n_rows() <= min_size {
                let left_leaf =
                    DecisionTree::Leaf(to_terminal(split.groups[0]));
                left = &mut Box::new(Some(left_leaf.clone()));
            } else {
                todo!();
            }
        }
        DecisionTree::Leaf => todo!(),
        _ => panic!(),
    }
}
*/
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
