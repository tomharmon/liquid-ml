use clap::Clap;
use liquid_ml::dataframe::{Column, LocalDataFrame, Row, Rower};
use liquid_ml::error::LiquidError;
use rand;
use serde::{Deserialize, Serialize};

/// This example builds and evaluates a binary classification decision tree
#[derive(Clap)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct Opts {
    /// The name of the data file
    #[clap(
        short = "d",
        long = "data",
        default_value = "examples/banknote.sor"
    )]
    data: String,
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

/// returns accuracy from 0-1
fn accuracy(actual: Vec<Option<bool>>, predicted: Vec<bool>) -> f64 {
    assert_eq!(actual.len(), predicted.len());
    actual
        .iter()
        .zip(predicted.iter())
        .fold(0, |acc, (actual, pred)| {
            if &actual.unwrap() == pred {
                acc + 1
            } else {
                acc
            }
        }) as f64
        / actual.len() as f64
}

/// A rower that splits the data set based on a given feature
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
fn get_split(data: LocalDataFrame) -> Split {
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

/// Counts the number of labels in the last column (labels) of the dataframe
/// that match the `label` of this `NumLabelRower`
struct NumLabelRower {
    /// Number of rows whose label matches our label
    num_matching_labels: usize,
    /// The label we are searching for
    label: bool,
}

impl Rower for NumLabelRower {
    fn visit(&mut self, row: &Row) -> bool {
        if row.get(row.width() - 1).unwrap().unwrap_bool() == self.label {
            self.num_matching_labels += 1;
        }
        true
    }

    fn join(mut self, other: Self) -> Self {
        self.num_matching_labels += other.num_matching_labels;
        self
    }
}

/// Returns the most common output value, assumes predictions are boolean values
/// Takes `O(n) time`
fn to_terminal(data: LocalDataFrame) -> bool {
    let mut r = NumLabelRower {
        num_matching_labels: 0,
        label: true,
    };
    r = data.map(r);
    r.num_matching_labels > (data.n_rows() / 2)
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

    if left.n_rows() == 0 || right.n_rows() == 0 {
        return DecisionTree::Leaf(to_terminal(
            left.combine(right.clone()).unwrap(),
        ));
    }

    let new_left: DecisionTree =
        if left.n_rows() <= min_size || depth >= max_depth {
            DecisionTree::Leaf(to_terminal(left))
        } else {
            let split_left = get_split(left);
            split(split_left, max_depth, min_size, depth + 1)
        };
    let new_right: DecisionTree =
        if right.n_rows() <= min_size || depth >= max_depth {
            DecisionTree::Leaf(to_terminal(right))
        } else {
            let split_right = get_split(right);
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
    data: LocalDataFrame,
    max_depth: usize,
    min_size: usize,
) -> DecisionTree {
    let root = get_split(data);
    split(root, max_depth, min_size, 1)
}

/// A struct that represents a decision tree and its predictions from a testing
/// set
#[derive(Debug, Clone)]
struct Predictor {
    tree: DecisionTree,
    results: Vec<bool>,
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

/// Builds a decision tree with the `train` data and tests it with the `test`
/// data
fn decision_tree(
    train: LocalDataFrame,
    test: LocalDataFrame,
    max_depth: usize,
    min_size: usize,
) -> Vec<bool> {
    let tree = build_tree(train, max_depth, min_size);
    let predictor = Predictor {
        tree,
        results: Vec::new(),
    };
    test.pmap(predictor).results
}

fn cross_val_split(
    data: LocalDataFrame,
    n_folds: usize,
) -> Vec<LocalDataFrame> {
    let mut folds = Vec::new();
    let mut rng = rand::thread_rng();
    let fold_size = data.n_rows() / n_folds;
    let mut row = Row::new(data.get_schema());
    let r = rand::seq::index::sample(&mut rng, data.n_rows(), data.n_rows());
    let mut r_iter = r.iter();
    for _ in 0..n_folds {
        let mut f = LocalDataFrame::new(data.get_schema());
        while f.n_rows() < fold_size {
            let i = r_iter.next().unwrap();
            data.fill_row(i, &mut row).unwrap();
            f.add_row(&row).unwrap();
        }
        folds.push(f);
    }
    folds
}

fn evaluation(
    data: LocalDataFrame,
    n_folds: usize,
    max_depth: usize,
    min_size: usize,
) -> Vec<f64> {
    let folds = cross_val_split(data, n_folds);
    let mut scores = Vec::new();
    for i in 0..folds.len() {
        let testing = folds.get(i).unwrap().clone();
        let mut training = LocalDataFrame::new(testing.get_schema());
        for j in 0..folds.len() {
            if i != j {
                training =
                    training.combine(folds.get(j).unwrap().clone()).unwrap();
            }
        }
        let actual = match &testing.data.get(testing.n_cols() - 1).unwrap() {
            Column::Bool(b) => b.clone(),
            _ => panic!("nope"),
        };
        let predictions = decision_tree(training, testing, max_depth, min_size);
        scores.push(accuracy(actual, predictions));
        println!("fold {} done evaluating", i);
    }
    scores
}

fn main() -> Result<(), LiquidError> {
    let opts: Opts = Opts::parse();
    let data = LocalDataFrame::from_sor(&opts.data, 0, 10000000000);
    let scores = evaluation(data, 5, 5, 10);
    println!("{:?}", scores);
    Ok(())
}
