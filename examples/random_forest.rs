use bincode::{deserialize, serialize};
use clap::Clap;
use liquid_ml::dataframe::{Column, LocalDataFrame, Row, Rower};
use liquid_ml::error::LiquidError;
use liquid_ml::kv::Key;
use liquid_ml::liquid_ml::LiquidML;
use log::Level;
use rand;
use serde::{Deserialize, Serialize};
use simple_logger;
use std::sync::Arc;

/// This example builds and evaluates a random forest model for binary 
/// classification
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
    #[clap(short = "n", long = "num_nodes", default_value = "12")]
    num_nodes: usize,
    /// The name of the data file
    #[clap(
        short = "d",
        long = "data",
        default_value = "examples/banknote.sor"
    )]
    data: String,
    /// The max depth of the tree
    #[clap(long = "max_depth", default_value = "10")]
    max_depth: usize,
    /// The min size of a node to split on
    #[clap(long = "min_size", default_value = "25000")]
    min_size: usize,
}

/// A struct to represent the results of splitting a decision tree based on
/// a specific feature, where a training set gets split based on the value of
/// that feature and the value of that feature for every row
#[derive(Debug, Clone)]
struct Split {
    /// The value of the feature we are splitting on for the given row we are
    /// evaluating
    value: f64,
    /// The column index of the feature we are splitting on
    feature_idx: usize,
    /// A clone of all the rows less than `value`
    left: LocalDataFrame,
    /// A clone of all the rows greater than `value`
    right: LocalDataFrame,
}

/// Represents a learned decision tree, used for making predictions once
/// training is completed
#[derive(Debug, Clone, Serialize, Deserialize)]
enum DecisionTree {
    /// Represents a node in a decision tree that is split on a feature
    Node {
        /// The left child node of this node
        left: Box<DecisionTree>,
        /// The right child node of this node
        right: Box<DecisionTree>,
        /// The column index of the feature this node is split on
        feature_idx: usize,
        /// The value of the feature that determines whether we follow the left
        /// or right child node when making predictions with a new row. `value`
        /// is compared to the value at the `feature_idx` in the row we are
        /// predicting
        value: f64,
    },
    /// Represents a classification in the decision tree
    Leaf(bool),
}

/// Split a dataframe based on a specific feature and its value
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

/// Calculate the gini index to evaluate the information gain based on a split
/// NOTE: this assumes the last column is a boolean label
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
            } / group.n_rows() as f64;
            score += p * p;
        }
        gini += (1.0 - score) * (group.n_rows() as f64 / n_samples as f64);
    }

    gini
}

/// Finds the best split for a Local Dataframe by brute-forcing splitting on
/// all features on all rows, takes `O(mn^2)` time, where `m` is the number of
/// features and `n` is the number of rows in `data`
fn get_split(data: &LocalDataFrame) -> Split {
    println!("getting a split");
    let mut rng = rand::thread_rng();
    let r = rand::seq::index::sample(
        &mut rng,
        data.n_cols() - 1,
        (data.n_cols() as f64 - 1.0).sqrt() as usize,
    );
    let class_labels = vec![true, false];

    let mut best_score = 1_000_000_000.0;
    let mut split = None;
    for feature_idx in r.iter() {
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
            let gini = gini_index(
                &[&test_split.left, &test_split.right],
                &class_labels,
            );
            if gini < best_score {
                split = Some(test_split);
                best_score = gini;
            }
        }
    }
    split.unwrap()
}

/// Counts the number of trues in the last column(labels) of the dataframe
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

/// Returns the most common output value, assumes predictions are boolean values
/// Takes `O(n) time`
fn to_terminal(data: LocalDataFrame) -> bool {
    let mut r = NumTrueRower { num_trues: 0 };
    r = data.map(r);
    r.num_trues > (data.n_rows() / 2)
}

/// Splits `to_split` and recursively builds a decision tree. Calls `get_split`
/// for non-terminal nodes and `to_terminal` for nodes that are terminal.
///
/// A node is determined to be terminal if the tree exceeds `max_depth` or if
/// the number of rows in the left or right split are less than `min_size`,
/// which are effectively hyper-parameters to the decision tree model
fn split(
    to_split: Split,
    max_depth: usize,
    min_size: usize,
    depth: usize,
) -> DecisionTree {
    let left = to_split.left;
    let right = to_split.right;
    println!("split with {}, {}", left.n_rows(), right.n_rows());

    if left.n_rows() == 0 || right.n_rows() == 0 {
        return DecisionTree::Leaf(to_terminal(
            left.combine(right.clone()).unwrap(),
        ));
    }

    let new_left: DecisionTree =
        if left.n_rows() <= min_size || depth >= max_depth {
            DecisionTree::Leaf(to_terminal(left))
        } else {
            let split_left = get_split(&left);
            split(split_left, max_depth, min_size, depth + 1)
        };
    let new_right: DecisionTree =
        if right.n_rows() <= min_size || depth >= max_depth {
            DecisionTree::Leaf(to_terminal(right))
        } else {
            let split_right = get_split(&right);
            split(split_right, max_depth, min_size, depth + 1)
        };

    DecisionTree::Node {
        left: Box::new(new_left),
        right: Box::new(new_right),
        value: to_split.value,
        feature_idx: to_split.feature_idx,
    }
}

/// Builds a decision tree recursively. Takes `O(mn^2logn)` time, where
/// `m` is the number of features and `n` is the number of rows in `data`
fn build_tree(
    data: Arc<LocalDataFrame>,
    max_depth: usize,
    min_size: usize,
) -> DecisionTree {
    let root = get_split(&data);
    split(root, max_depth, min_size, 1)
}

/// Represents a Rower that evaluates the accuracy of the Random Forest
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Evaluator {
    /// A vec of the (tree, number of correct predictions, total number of rows)
    trees: Vec<(DecisionTree, usize, usize)>,
    /// Bookkeeping to keep track of the accuracy of the aggregated predictions
    /// of the Random Forest. First element in the tuple is the number of 
    /// correct predictions, the second element is the total number of rows
    total_accuracy: (usize, usize),
}

impl Rower for Evaluator {
    fn visit(&mut self, row: &Row) -> bool {
        let mut num_trues = 0;
        let true_value = row.get(row.width() - 1).unwrap().unwrap_bool();
        for (tree, num_correct, total) in self.trees.iter_mut() {
            let prediction = predict(&tree, row);
            if prediction == true_value {
                *num_correct += 1;
                num_trues += 1;
            }
            *total += 1;
        }
        let r_forest_pred = num_trues > self.trees.len();
        if r_forest_pred == true_value {
            self.total_accuracy.0 += 1;
        }
        self.total_accuracy.1 += 1;
        num_trues > self.trees.len()
    }

    fn join(mut self, other: Self) -> Self {
        for ((_, c1, t1), (_, c2, t2)) in
            self.trees.iter_mut().zip(other.trees.iter())
        {
            *c1 += c2;
            *t1 += t2;
        }
        self.total_accuracy.0 += other.total_accuracy.0;
        self.total_accuracy.1 += other.total_accuracy.1;
        self
    }
}

/// Given a decision tree, makes a prediction. Takes `O(logn)` time
fn predict(tree: &DecisionTree, row: &Row) -> bool {
    match tree {
        DecisionTree::Node {
            left,
            right,
            feature_idx,
            value,
        } => {
            if row.get(*feature_idx).unwrap().unwrap_float() < *value {
                predict(left, row)
            } else {
                predict(right, row)
            }
        }
        DecisionTree::Leaf(v) => *v,
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
    println!("done distributing data");

    let ddf = app.data_frames.get("data").unwrap();
    // TODO: fix this
    let keys: Vec<_> = ddf
        .df_chunk_map
        .iter()
        .filter(|(_, key)| key.home == app.node_id)
        .collect();
    let mut my_local_key = Key::new("fake", 0);
    let mut largest = 0;
    for (range, key) in keys {
        if range.end - range.start > largest {
            largest = range.end - range.start;
            my_local_key = key.clone();
        }
    }

    dbg!(&my_local_key);
    let ldf = app.kv.wait_and_get(&my_local_key).await?;
    println!("got ldf");
    let tree = build_tree(ldf, opts.max_depth, opts.min_size);
    println!("built local tree");
    let trees = if app.node_id == 1 {
        let mut trees: Vec<(DecisionTree, usize, usize)> = Vec::new();
        for _ in 0..app.num_nodes - 1 {
            let blob = { app.blob_receiver.lock().await.recv().await.unwrap() };
            let tree = deserialize(&blob[..])?;
            trees.push((tree, 0, 0));
        }
        let ser_trees = serialize(&trees)?;
        for i in 2..app.num_nodes + 1 {
            app.kv.send_blob(i, ser_trees.clone()).await?;
        }
        trees
    } else {
        let t = serialize(&tree)?;
        println!("serialized our tree");
        app.kv.send_blob(1, t).await?;
        println!("sent our tree");
        let blob = { app.blob_receiver.lock().await.recv().await.unwrap() };
        println!("received final");
        deserialize(&blob[..])?
    };
    println!("have all the trees, starting evaluator map");

    let eval = Evaluator {
        trees,
        total_accuracy: (0, 0),
    };
    let r = app.map("data", eval).await?;
    match r {
        None => println!("done"),
        Some(e) => {
            e.trees.iter().for_each(|(_, c, t)| {
                println!("accuracy: {}", *c as f64 / *t as f64)
            });
            println!(
                "RF accuracy: {}",
                e.total_accuracy.0 as f64 / e.total_accuracy.1 as f64
            );
        }
    }
    app.kill_notifier.notified().await;
    Ok(())
}
