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
    left: LocalDataFrame,
    right: LocalDataFrame
}

#[derive(Debug, Clone)]
enum DecisionTree {
    Node {
        left: Box<DecisionTree>,
        right: Box<DecisionTree>,
        feature_idx: usize,
        value: f64
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


impl Rower for Split {
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

/// WTF is this
struct SplitInfo {
    index: usize,
    value: f64,
    left: Split,
    right: Split,
}

// this assumes the last column is a boolean label
fn gini_index(groups: &[&LocalDataFrame], classes: &[bool]) -> f64 {
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

/// Finds the best split for a Local Dataframe for a single split
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

    let mut best_score = 1_000_000_000.0;
    let mut split = None;
    for feature_idx in 0..data.n_cols() - 1 {
        for i in 0..data.n_rows() {
            let new_value =
                data.get(feature_idx as usize, i).unwrap().unwrap_float();
            let mut test_split = Split {
                feature_idx: feature_idx as usize,
                value: new_value,
                left: LocalDataFrame::new(data.get_schema()),
                right: LocalDataFrame::new(data.get_schema()),
            };

            test_split = data.pmap(test_split);
            let gini = gini_index(&[&test_split.left, &test_split.right], &class_labels);
            if gini < best_score {
                split = Some(test_split); 
                best_score = gini;
            }
        }
    }
    split.unwrap()
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
    to_split: Split,
    max_depth: usize,
    min_size: usize,
    depth: usize,
) -> DecisionTree {

    let left = to_split.left;
    let right = to_split.right;

    if left.n_rows() == 0 || right.n_rows() == 0 {
        return DecisionTree::Leaf(to_terminal(left.combine(right.clone()).unwrap()));
    }

    let new_left : DecisionTree = if left.n_rows() <= min_size || depth >= max_depth { 
        DecisionTree::Leaf(to_terminal(left))
    } else {
        let split_left = get_split(left);
        split(split_left, max_depth, min_size, depth + 1)
    };
    let new_right : DecisionTree = if right.n_rows() <= min_size || depth >= max_depth { 
        DecisionTree::Leaf(to_terminal(right))
    } else {
        let split_right = get_split(right);
        split(split_right, max_depth, min_size, depth + 1)
    };

    DecisionTree::Node {
        left: Box::new(new_left),
        right: Box::new(new_right),
        value: to_split.value,
        feature_idx: to_split.feature_idx
    }
}

fn build_tree(data: LocalDataFrame, max_depth: usize, min_size: usize) -> DecisionTree {
    let root = get_split(data);
    split(root, max_depth, min_size, 1)
}

#[derive(Debug, Clone)]
struct Predictor {
    tree: DecisionTree,
    results: Vec<bool>
}

impl Rower for Predictor {
    fn visit(&mut self, row: &Row) -> bool {
        let result = predict(&self.tree, row);   
        self.results.push(result);
        result
    }
    
    fn join(mut self, other: Self) -> Self {
        self.results.extend(other.results.into_iter());
        self
    }
}

fn predict(tree: &DecisionTree, row: &Row) -> bool {
   match tree {
       DecisionTree::Node {left, right, feature_idx, value} => {
           if row.get(*feature_idx).unwrap().unwrap_float() < *value {
                predict(left, row)
           } else {
                predict(right, row)
           }
       },
       DecisionTree::Leaf(v) => *v
   }
}

fn decision_tree(train: LocalDataFrame, test: LocalDataFrame, max_depth: usize, min_size: usize) -> Vec<bool> {
    let tree = build_tree(train, max_depth, min_size);
    let predictor = Predictor {
        tree,
        results: Vec::new()
    };
    test.pmap(predictor).results
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
