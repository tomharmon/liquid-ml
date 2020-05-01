//! # Introduction
//! LiquidML is a platform for distributed, scalable data analysis for data
//! sets too large to fit into memory on a single machine. It aims to be easy
//! and simple to use, allowing users to easily create their own `map` and
//! `filter` operations, leaving everything else to `liquid_ml`. It ships with
//! many example uses, including the decision tree and random forest machine
//! learning algorithms built in, showing the power and ease of use of the
//! platform.
//!
//! LiquidML is written in Rust for both performance and safety reasons,
//! allowing many optimizations to be made more easily without the risk of a
//! memory safety bug. This helps guarantee security around our clients' data,
//! as many memory safety bugs can be exploited by malicious hackers.
//!
//! LiquidML is currently in the state of an MVP. Tools on top of LiquidML can
//! built and several examples are included in this crate to demonstrate
//! various use cases.
//!
//! # Architecture
//! LiquidML consists of the following architectural components:
//! - SoRer, for inferring schemas and parsing `.sor` files
//! - Networking Layer, for communication over TCP
//! - KV Store, for associating ownership of data frame chunks with nodes
//! - DataFrame, for local and distributed data frames
//! - Application Layer, for convenience of using the entire system
//!
//! ## SoRer
//! The main purpose of the `SoRer` is to handle parsing files of an unknown
//! schema and with possibly malformed rows. Schemas are inferred based on the
//! first 500 rows and any malformed rows are discarded. Specifics on the
//! architecture and implementation of `SoRer` may be found
//! [here](https://docs.rs/sorer)
//!
//! ## Networking
//! The network layer is the lowest level component of a `liquid_ml` instance.
//! It provides a simple registration [`Server`] and a network of distributed
//! [`Client`]s, each with a unique ID assigned by the [`Server`]. The
//! networking layer uses `TCP` to communicate. [`Client`]s are able to send
//! messages directly to any other [`Client`] of the same client type after
//! they register with the [`Server`].
//!
//! ## KV Store
//! The [`KVStore`] stores blobs of serialized data that are associated with
//! [`Key`]s, as well as caches deserialized values in memory. In `liquid_ml`,
//! the [`KVStore`] stores [`LocalDataFrame`]s so that chunks of data frames
//! can be associated with different nodes in the system.
//!
//! ## Data frame
//! A data frame in `liquid_ml` is lightly inspired by those found in `R` or
//! `pandas`, and supports optionally named columns. There are many provided
//! constructors which make it easy to create any kind of data frame in
//! different ways. You may analyze the data in a data frame by implementing
//! the [`Rower`] trait to perform `map` or `filter` operations. These
//! operations can be easily performed on either [`LocalDataFrame`]s for data
//! that fits in memory or [`DistributedDataFrame`]s for data that is too
//! large to fit in one machine.
//!
//! The `dataframe` module provides 2 implementations for a data frame:
//! a [`LocalDataFrame`] and a [`DistributedDataFrame`], the differences are
//! further explained in the implementation section.
//!
//! **Note**: If you need a [`DistributedDataFrame`], it is highly recommended
//! that you check out the [`LiquidML`] struct since that provides many
//! convenient helper functions for working with [`DistributedDataFrame`]s.
//! Using a [`DistributedDataFrame`] directly is only recommended if you really
//! know what you are doing. There are also helpful examples of `map` and
//! `filter` in the [`LiquidML`] documentation
//!
//! ## Application Layer
//! The application layer, aka the [`LiquidML`] struct, is an even higher level
//! API for writing programs to perform data analysis on the entire distributed
//! system. It allows a user to create and analyze multiple
//! [`DistributedDataFrame`]s without worrying about [`KVStore`]s, nodes,
//! networking, or any other complications of distributed systems, making it
//! very easy for a user to run `map` or `filter` operations.
//!
//! [`LiquidML`] also provides a `run` method which takes a function and
//! executes that function. The signature of this user-implemented function
//! is `KVStore -> ()`. This allows much lower-level access for more
//! advanced users so that they may have more powerful and general usage of the
//! system beyond our provided implementations of `map`, and `filter`.
//!
//! See the use case section and the `examples` directory for illustrative
//! examples.
//!
//! # Implementation
//!
//! ## Networking
//!
//! The Networking layer consists of [`Client`] and [`Server`] structs, as well
//! as some helper functions in the `network` module.
//!
//! There is little handling of many of the complicated edge cases associated
//! with distributed systems in the networking layer. It's assumed that most
//! things happen without any errors, although some basic error checking is
//! done, such as checking for connections being closed, etc.
//!
//! ### Client
//! A [`Client`] can be used to send a message directly to any other [`Client`]
//! of the same type at any time by using the following method:
//!
//! `client.send_msg(target_id: usize, message: Serialize)`
//!
//! When a [`Client`] is first created, it must be registered with the
//! [`Server`] and connect with all other existing [`Client`]s.
//!
//! Registration process from [`Client`] perspective:
//! 1. Connect to the [`Server`]
//! 2. Send the [`Server`] a `Message<ControlMsg::Introduction>` message
//!    containing the `IP:Port` and `network_name` for this [`Client`]
//! 3. The [`Server`] will respond with the `Message<ControlMsg::Directory>`
//!    message containing the `IP:Port` of all other currently connected
//!    [`Client`]s in that network.
//! 4. The newly created [`Client`] connects to all other existing [`Client`]s.
//! 5. The `Client` waits for all other `Client`s that have not yet started to
//!    connect to it, unless we have connected to all the nodes.
//!
//! ### Server
//! The [`Server`] asynchronously registers new [`Client`]s via
//! `Server::accept_new_connections` and also allows sending any
//! `Message<ControlMsg>` type, such as `Kill` messages for orderly shutdown.
//!
//! Due to the servers fairly simple functionality, a default implementation of
//! a server comes packaged with the `LiquidML` system and can be started by
//! running the following command:
//!
//! `
//! cargo run --bin server -- --address <Optional IP Address>
//! `
//!
//! If an IP is not provided the server defaults to `127.0.0.1:9000`.
//!
//! ## KVStore
//! Internally [`KVStore`]s store their data in memory as serialized blobs
//! (aka a `Vec<u8>`). The [`KVStore`] caches deserialized values into their
//! type `T` on a least-recently used basis. A hard limit for the cache size is
//! set to be `1/3` the amount of total memory on the machine, though this will
//! be changed to be configurable.
//!
//! [`KVStore`]s have an internal asynchronous message processing task since
//! they use the network directly and need to communicate with other
//! [`KVStore`]s. The [`KVStore`] also provides a lower level interface for
//! network communication by exposing a method to directly send any serialized
//! blob of data to any other [`KVStore`].
//!
//! ## DataFrame
//! ### [`LocalDataFrame`]
//!
//! A [`LocalDataFrame`] implements the actual data storage and processing.
//! Data is held in columnar format and with a well defined schema, and the
//! [`LocalDataFrame`] defines  the actual single and multi-threaded `map` and
//! `filter` operations. It should be noted that all `map` and `filter`
//! operations are row-wise processing, but data is held in columnar format
//! to avoid boxed types and reduced memory usage.
//!
//! ### [`DistributedDataFrame`]
//!
//! A [`DistributedDataFrame`] is an abstraction over a distributed system of
//! nodes that run [`KVStore`]s which contain chunks of [`LocalDataFrame`]s.
//! Therefore each [`DistributedDataFrame`] simply holds a pointer to a
//! [`KVStore`] and a map of ranges of row indices to the [`Key`]s for the
//! chunks of data with that range of row indices. A [`DistributedDataFrame`]
//! is immutable to make it trivial for the global state of the data frame to
//! be consistent.
//!
//! Because of this the [`DistributedDataFrame`] implementation is mainly
//! concerned with networking and getting and putting chunks of different
//! [`KVStore`]s. One of the main concerns are that creating a new
//! [`DistributedDataFrame`] means distributing the [`Key`]s of all the chunks
//! to all nodes.
//!
//! Upon creation, node 1 of a [`DistributedDataFrame`] will distribute chunks
//! of data across multiple nodes from `SoR` files, iterators, and other
//! convenient ways of adding data. Note that our experimental testing found
//! that using the largest chunks possible to fit on each node increased
//! performance by over `2x`.
//!
//! Since our experimental testing found that big chunks are best for `map` and
//! `filter` performance, we can not simply use the [`KVStore`] to support the
//! API of a [`DistributedDataFrame`] by sending chunks around, since each
//! chunk will be too big to go over the network, so methods like `get`
//! won't work unless each [`DistributedDataFrame`] has a way to (meaningfully)
//! talk to other [`DistributedDataFrame`]s, which mean they need a [`Client`]
//! of their own.
//!
//! ## Application Layer aka `LiquidML`
//! The implementation of the [`LiquidML`] struct is quite simple since it
//! delegates most of the work to the [`DistributedDataFrame`]. All it does is
//! manage the state of its own node and allow creation and analysis of
//! multiple [`DistributedDataFrame`]s.
//!
//! # Examples and Use Cases
//! Please check the `examples/` directory for more fully featured examples.
//!
//!
//! ## Creating a `DistributedDataFrame` With `LiquidML` and Using a Simple `Rower`
//! This example shows a trivial implementation of a [`Rower`] and using the
//! entire [`LiquidML`] system to perform a `map` operation while being
//! unaware of the distributed internals of the system.
//!
//! ```rust,no_run
//! use liquid_ml::dataframe::{Data, Rower, Row};
//! use liquid_ml::LiquidML;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, Clone)]
//! struct MyRower {
//!     sum: i64
//! }
//!
//! impl Rower for MyRower {
//!     fn visit(&mut self, r: &Row) -> bool {
//!         let i = r.get(0).unwrap();
//!         match i {
//!             Data::Int(val) => {
//!                 if *val < 0 {
//!                     return false;
//!                 }
//!                 self.sum += *val;
//!                 true
//!             },
//!             _ => panic!(),
//!         }
//!     }
//!
//!     fn join(mut self, other: Self) -> Self {
//!         self.sum += other.sum;
//!         self
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // This main does the following:
//!     // 1. Creates a `LiquidML` struct, which registers with the `Server`
//!     //    running at the address "192.168.0.0:9000"
//!     // 2. Construct a new `DistributedDataFrame` with the name "my-df". If
//!     //    we are node 1, schema-on-read and parse the file, distributing
//!     //    chunks to all the other nodes. Afterwards, all nodes will have
//!     //    an identical `LiquidML` struct and we can call `map`
//!     // 3. We call `map` and each node performs the operation on their local
//!     //    chunk(s). Since the `Rower` trait defines how to join chunks, the
//!     //    results from each node will be joined until we have a result
//!     let mut app = LiquidML::new("192.155.22.11:9000",
//!                                 "192.168.0.0:9000",
//!                                 20)
//!                                 .await
//!                                 .unwrap();
//!     app.df_from_sor("foo.sor", "my-df").await.unwrap();
//!     let r = MyRower { sum: 0 };
//!     app.map("my-df", r);
//! }
//! ```
//!
//! ## Generic, Low Level Use Case
//!
//! ```rust,no_run
//! use liquid_ml::dataframe::LocalDataFrame;
//! use liquid_ml::LiquidML;
//! use liquid_ml::kv::KVStore;
//! use std::sync::Arc;
//! use tokio::sync::RwLock;
//!
//! async fn something_complicated(kv: Arc<KVStore<LocalDataFrame>>) {
//!     println!("Use your imagination :D");
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let app =
//!         LiquidML::new("192.15.2.1:900", "192.16.0.0:900", 20).await.unwrap();
//!     app.run(something_complicated).await;
//! }
//! ```
//!
//!
//! ## Random Forest
//!
//! A distributed random forest implementation can be found at
//! `examples/random_forest.rs` this example demonstrates a built from scratch
//! random forest on our distributed platform.
//!
//! This is currently a very rudimentary proof of concept as it assumes that
//! the last columns is a boolean label and does not support more than
//! boolean labels
//!
//! This program can be run as follows:
//! 1. Start the [`Server`] with this command: `cargo run --bin server`
//! 2. Start 3 clients, each with a different `IP:Port`, with the following
//!    command:
//!
//! `
//! cargo run --release --example random_forest -- -m <my IP:Port> -s <Sever IP:Port> -d <path to data file>
//! `
//!
//! Full description of the command line arguments available can be seen by
//! running the following command:
//!
//! `
//! cargo run --example random_forest -- --help
//! `
//!
//! ## Degrees of Linus
//!
//! This example shows how to calculate how many people have worked within
//! `n` degrees of Linus Torvalds. It needs a very large data file to be run,
//! so the data file is not included in this repository. Please reach out to us
//! if you would like the data file.
//!
//! The code can be found in `examples/seven_degrees.rs`
//!
//! This program can be run as follows:
//! 1. Start the [`Server`] with this command: `cargo run --bin server`
//! 2. Start 3 clients, each with a different `IP:Port`, with the following
//!    command:
//!
//! `
//! cargo run --release --example seven_degrees -- -m <IP:Port> -c <full path to data file>
//! `
//!
//! Full description of the command line arguments available can be seen by
//! running the following command:
//!
//! `
//! cargo run --example seven_degrees -- --help
//! `
//!
//! We found that even with using swap space, that the `seven_degrees` example
//! had peak memory usage of `18GB` and took ~5 minutes with 4 degrees of
//! Linus and using the full sized files.
//!
//! One computer was a desktop with only 8GB of RAM and an i5-4690k, the
//! other was a (plugged in) laptop with 16GB of RAM and an i7-8550u.
//!
//! ## Word Count
//! This program counts the number of times each unique word is found in a
//! given text file. The program splits words by white space and does not
//! account for punctuation (though this would be an easy change in only the
//! example itself), so "foo" and "foo," count as different words.
//!
//! The code can be found in `examples/word_count.rs`
//!
//! This program runs the word count example and can be run as follows:
//! 1. Start the [`Server`] with this command: `cargo run --bin server`
//! 2. Start 3 clients, each with a different `IP:Port`, with the following
//!    command:
//!
//! `
//! cargo run --release --example word_count -- -m <IP:Port>
//! `
//!
//! Full description of the command line arguments available can be seen by
//! running the following command:
//!
//! `
//! cargo run --example word_count -- --help
//! `
//!
//! ## Simple Demo
//! We implemented a trivial demo that places some numbers into the `kv` adds
//! them and verifies that the addition was correct.
//!
//! This program runs the demo program and can be run as follows:
//! 1. Start the `Server` with this command: `cargo run --bin server`
//! 2. Start 3 clients, each with a different `IP:Port`, with the following
//!    command:
//!
//! `
//! cargo run --example demo_client -- -m <IP:Port>
//! `
//!
//! Full description of the command line arguments available can be seen by
//! running the following command:
//!
//! `
//! cargo run --example demo_client -- --help
//! `
//!
//! The third client will print `SUCCESS` at the end.
//!
//! # Road Map
//! 0. Build robust integration tests to define how the distributed system ks.
//! 1. Fix up orderly shutdown of the network layer.
//!
//! [`Client`]: network/struct.Client.html
//! [`Server`]: network/struct.Server.html
//! [`Connection`]: network/struct.Connection.html
//! [`LocalDataFrame`]: dataframe/struct.LocalDataFrame.html
//! [`DistributedDataFrame`]: dataframe/struct.DistributedDataFrame.html
//! [`Rower`]: dataframe/trait.Rower.html
//! [`KVStore`]: kv/struct.KVStore.html
//! [`Key`]: kv/struct.Key.html
//! [`Key`]: kv/type.Value.html
//! [`LiquidML`]: struct.LiquidML.html
pub mod dataframe;
pub mod error;
pub mod kv;
pub mod network;

mod liquid_ml;
pub use crate::liquid_ml::LiquidML;

pub(crate) const MAX_NUM_CACHED_VALUES: usize = 10;
pub(crate) const BYTES_PER_KIB: f64 = 1_024.0;
pub(crate) const BYTES_PER_GB: f64 = 1_073_741_824.0;
pub(crate) const KV_STORE_CACHE_SIZE_FRACTION: f64 = 0.33;
pub(crate) const MAX_FRAME_LEN_FRACTION: f64 = 0.8;
