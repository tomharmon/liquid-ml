//! # Introduction
//! LiquidML is an in-memory platform for distributed, scalable machine
//! learning and data analytics. It ships with the random forest machine
//! learning algorithm built in, allowing users to easily classify data using
//! the power of AI.
//!
//! LiquidML is written in Rust for both performance and safety reasons,
//! allowing many optimizations to be made more easily without the risk of a
//! memory safety bug. This helps guarantee security around our clients' data,
//! as many memory safety bugs can be exploited by malicious hackers.
//!
//! LiquidML makes it fast and easy to derive insights from your data by
//! extending it to apply your own analytics tools to your data in a
//! distributed, scalable fashion.
//!
//! LiquidML is currently in the state of an MVP. Tools on top of LiquidML can 
//! built and several examples are included in this crate to demonstrate various 
//! use cases.
//!
//! # Architecture
//! LiquidML consists of the following architectural components:
//! - SoRer, for inferring schemas and parsing files
//!          this is an in house format for data representation
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
//! [here](https://docs.rs/sorer/0.1.0/sorer/)
//!
//! ## Networking
//! The network layer is the lowest level component of a `LiquidML` instance.
//! It provides a simple registration `Server` and a network of distributed
//! `Client`s, each with a unique ID assigned by the `Server`. The networking
//! layer uses `TCP` to communicate. `Client`s are able to send messages
//! directly to any other `Client` of the same client type after they register
//! with the `Server`.
//!
//! The `Client` is designed asynchronously so that it can listen for messages
//! from the `Server` or any number of `Client`s (within physical limitations)
//! concurrently. Messages from the `Server` are processed internally by the
//! `Client`, while messages from other `Client`s are sent over a `mpsc`
//! channel which is passed in during construction of the `Client`. This
//! channel acts as a queue of messages so that they may be processed in
//! whichever way a user of the `Client` wants. Because of this the networking
//! layer is not tightly coupled to any system that uses it. `Client`s are
//! constructed with a client type so that sub-networks of different client
//! types can be dynamically created. This supports the `liquid_ml`
//! distributed filter implementation.
//!
//! The `Client` trait is generic, and is parameterized by a type `T`,
//! where `T` is the type of messages that can be sent between `Client`s. A
//! `Client` implementation is provided for any message type that implements
//! the following traits: `Send + Sync` from the standard library and
//! `DeserializeOwned + Serialize` from the serialization/deserialization crate
//! [`Serde`](https://crates.io/crates/serde).
//!
//!
//! ## KV Store
//! The `KVStore` stores blobs of serialized data that are associated with
//! `Key`s, as well as caches deserialized values in memory.
//!
//! `Key`s know which node that the data for that `Key` belongs to. Cached
//! values that are likely no longer needed are smartly evicted using an LRU
//! map to avoid running out of memory since `LiquidML` is designed to be run
//! using data sets that are much larger than what would fit into memory. The
//! LRU cache has a fixed size limit that is set to 1/3 of the total memory
//! on each node.
//!
//! Caching also helps to improve performance by reducing network calls.  Each
//! `KVStore` store implements functionality to utilize the network layer to
//! get data from a different node if the data is available elsewhere and not
//! in this `KVStore`.
//!
//!
//! ## Data frame
//! A data frame in `liquid_ml` is lightly inspired by those found in `R` or
//! `pandas`, and supports optionally named columns. You may analyze the data
//! in a data frame across many distributed machines in a horizontally scalable
//! manner by implementing the `Rower` trait to perform `map` or `filter`
//! operations.
//!
//! The `dataframe` module provides 2 implementations for a data frame:
//!
//! ### `LocalDataFrame`
//! A `LocalDataFrame` should be used to analyze data sets that fit into memory
//! to avoid network latency of a `DistributedDataFrame` and to make things
//! simple. It is mutable and gives more direct control to the user. It also
//! provides convenient functions for performing `filter` or `map` operations
//! when given a user-defined `Rower`
//!
//! ### `DistributedDataFrame`
//! A [`DistributedDataFrame`](crate::dataframe::distributed_dataframe) is
//! an abstraction over a distributed system of nodes that run `KVStore`s which
//! contain chunks of `LocalDataFrame`s. Therefore each `DistributedDataFrame`
//! simply holds a pointer to a `KVStore` and a map of ranges of row indices
//! to the `Key`s for the chunks of data with that range of row indices.
//!
//! Upon creation node 1 of a `DistributedDataFrame` will distribute data
//! across multiple nodes from `SoR` files, iterators, and other conveient ways
//! of adding data. Each chunk that node 1 distributes is as large as possible
//! while distributing the data evenly between nodes since experimental testing
//! found this was optimal for performance of `map` and `filter`.
//! Methods for distributed versions of these `map` and `filter` operations
//! are provided.
//!
//! **Note**: If you need these features of a `DistributedDataFrame`, it is
//! highly recommended that you check out the `LiquidML` struct since that
//! provides many convenient helper functions for working with
//! `DistributedDataFrame`s.  Using a `DistributedDataFrame` directly is only
//! recommended if you really know what you are doing.
//!
//! ## Application Layer
//! The `Application` layer is an even higher level API for writing programs to
//! perform data analysis on the entire distributed system. It allows a user
//! to create and analyze `DistributedDataFrame`s without worrying about
//! `KVStore`s, nodes, networking, or any other complications of distributed
//! systems, making it very easy for a user to run `map` or `filter`.
//!
//! The `Application` also provides a `run` method which takes a function and
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
//! ## Library Usage
//! ### Tokio
//! The networking layer uses [`tokio`](https://docs.rs/tokio/0.2.13/tokio/) to
//! use asynchronous programming to handle connections with multiple clients
//! concurrently. `Tokio` is an asynchronous run time for Rust since there is
//! not one included in the standard library. It also includes some faster
//! synchronization primitives that are faster than those in the standard
//! library (e.g. `RwLock`). `Tokio` is open source under an MIT license.
//!
//! ### Bincode
//! `Bincode` is a crate for encoding and decoding using a binary serialization
//! strategy. It is extremely fast and is developed by Mozilla, though it is
//! open source under an MIT license.
//!
//! ### Serde
//! `Serde` is a framework for serializing and deserializing Rust data
//! structures efficiently and generally. `Serde` is open source under an MIT
//! license.
//!
//!
//! ### Miscellaneous
//! - `lru`: provides a Least Recently Used map, since one is not available
//!    in the Rust standard library anymore. `lru` is heavily based on the
//!    original standard library implementation (they're practically the same).
//! - `thiserror`: helpful for defining error types
//! - `num_cpus`: provides a function to find the number of logical cores
//!    available on a machine
//! - `futures`: used for the `SinkExt` trait (used for sending messages over
//!    TCP). `tokio` defines the `StreamExt` trait (used for reading messages
//!    over TCP). These traits are split across crates because of the split in
//!    the Rust eco-system because it took a while for `async`/`await` to
//!    become stabilized.  It is a pain to have both imports but everyone in
//!    the ecosystem has to deal with it.
//! - `crossbeam-utils`: used to get around some limitations of lifetimes when
//!    spawning threads. Threads spawned with the Rust standard library have a
//!    lifetime of static `'static`, but `crossbeam-utils` provides a way to
//!    spawn threads with a shorter lifetime.
//! - `bytes`: an efficient buffer used for networking (used to be a part of
//!   `tokio`, but got separated out due to its general usefulness)
//! - `clap`: for command line argument parsing
//!
//! ## Networking
//!
//! The Networking layer consists of `Client` and `Server` structs, as well as
//! some helper functions in the `network` module.
//!
//! There is little handling of many of the complicated edge cases associated
//! with distributed systems in the networking layer. It's assumed that most
//! things happen without any errors. We Some of the basic checking that is done
//! is checking for connections being closed and ensuring messages are of the
//! right type.
//!
//! ### Client
//! The `Client` struct represents a node and does the following tasks
//! asynchronously:
//! 1. Listen for new connections from other nodes
//!    (`Client::accept_new_connections`)
//! 2. Listen for `Kill` messages from the `Server`
//! 3. Receive messages from any other active `Client` using the read half of a
//!   `TCPStream`. Messages are forwarded one layer up to the `KVStore` via an
//!   `mpsc` channel.
//!
//! A `Client` can be used to send a message directly to any other `Client` of
//! the same type at any time by using the following method:
//!
//! `client.send_msg(target_id: usize, message: Serialize)`
//!
//! When a `Client` is first created, it must be registered with the `Server`
//! and with all other existing `Client`s.
//!
//! Registration process from `Client` perspective:
//! 1. Connect to the `Server`
//! 2. Send the `Server` a `Message<ControlMsg::Introduction>` message
//!    containing the `IP:Port` of this `Client`
//! 3. The `Server` will respond with the `Message<ControlMsg::Directory>`
//!    message containing the `IP:Port` of all other currently connected
//!    `Client`s
//! 4. The newly created `Client` connects to all other existing `Client`s.
//!    When each connection is made:
//!   - A `Message<ControlMsg::Introduction>` is sent to the other `Client`.
//!   - They are added to the `Directory` of this `Client`.
//!   - An `mpsc` channel is created that is used to send messages to whatever
//!     is using the network layer and thus needs to process and respond to
//!     messages. In `LiquidML`, this is the `KVStore`.
//!   - A green thread is spawned to receive future messages from the other
//!     `Client`. When the messages are received they are sent over the `mpsc`
//!     channel.
//!
//! ### Server
//! The `Server` asynchronously registers new `Clients` via
//! `Server::accept_new_connections` and also allows sending any
//! `Message<ControlMsg>` type, such as `Kill` messages for orderly shutdown.
//!
//! Registration process from `Server` perspective:
//! 1. Accept an incoming TCP connection from a new `Client`
//! 2. Read a `Message<ControlMsg::Introduction>` message from the new `Client`
//!    containing the `IP:Port` of the new `Client`
//! 3. Reply to the new `Client` with a `Message<ControlMsg::Directory>`
//!    message
//! 4. Add the `Connection` with the new `Client` to the `Directory` of this
//!    `Server`
//! 5. Listen for new connections, when one arrives go to #1
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
//! There currently is a working implementation of a distributed KV store,
//! with `get`, `wait_and_get`, `put`, and `send_blob`. We also implemented a
//! method for processing messages (`process_messages`). See their
//! documentation for further details.
//!
//! ## DataFrame
//! The `DataFrame` and supporting classes loosely implement the management
//! provided API with better defined error handling using custom `LiquidError`
//! enumerations. A few discrepancies with certain data types were introduced
//! due to the use of Rust. A `from_sor` function was implemented that uses our
//! `sorer` crate to ingest raw data from a SoR file to create a `DataFrame`.
//!
//! ## Application
//! The `Application` struct is used to store the state of the node currently.
//! It provides a clean simple constructor to start up the client and setup an
//! empty KVStore. We currently also provide a run function that takes an async
//! function and runs it on the KVStore until the `Server` sends a `Kill`
//! message to all nodes.
//!
//!
//! # Use Cases
//! Please check the `src/liquid_ml/examples/` directory for more fully
//! featured examples.
//!
//!
//! Creating a `DataFrame` from a SoR file:
//! ```rust
//! use liquid_ml::dataframe::LocalDataFrame;
//!
//! let df = LocalDataFrame::from_sor("tests/test.sor", 0, 1000);
//! assert_eq!(df.n_cols(), 4);
//! assert_eq!(df.n_rows(), 2);
//! ```
//!
//! Using the Application Level API after defining your own `Rower`
//! implementation.  This form of programming, where the user is unaware of
//! the distributed nature of the system, is what we want to achieve in our API.
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
//!     // Starts up a client that will:
//!     // 1. Register with the `Server` running at the address "192.168.0.0:9000"
//!     // 2. Schema-on-read a chunk of the "foo.sor" file. The chunk that is read
//!     //    is based on the number of nodes (in this case 20) and the assigned id
//!     //    given to this Client by the Server. All nodes must have the entire
//!     //    "foo.sor" file (alternatively we could send the schema around and then
//!     //    we only need each node's chunk stored on each node).
//!     // 3. Since the `Rower` trait defines how to join chunks, the Application
//!     //    layer will handle running pmap/map/filter on a local chunk and joining
//!     //    them globally
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
//! A more generic possible use case:
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
//! The required registration server, used for co-ordination of `Client`s
//! comes as a pre-packaged binary in this crate.
//! 
//! # Examples
//! 
//! ## Degrees of Linus
//!
//! The code can be found in `src/liquid_ml/examples/seven_degrees.rs`
//!
//! This program runs the Degrees of Linus program for 4 degrees and can be run 
//! as follows:
//! 1. Start the `Server` with this command: `cargo run --bin server`
//! 2. Start 3 clients, each with a different `IP:Port`, with the following
//!    command:
//!
//! `
//! cargo run --release --example seven_degrees -- -m <IP:Port> -c <full path to commits file>
//! `
//!
//! Full description of the command line arguments available can be seen by
//! running the following command:
//!
//! `
//! cargo run --example seven_degrees -- --help
//! `
//!
//! We found that even with using swap space, that the seven_degrees example
//! had peak memory usage of `18GB` and took ~5 minutes with 4 degrees of
//! Linus and using the full sized files.
//!
//! One computer was a desktop with only 8GB of RAM and an i5-4690k, the
//! other was a (plugged in) laptop with 16GB of RAM and an i7-8550u.
//!
//! ## Word Count
//! The code can be found in `src/liquid_ml/examples/word_count.rs`
//!
//! This program runs the word count example and can be run as follows:
//! 1. Start the `Server` with this command: `cargo run --bin server`
//! 2. Start 3 clients, each with a different `IP:Port`, with the following
//!    command:
//!
//! `
//! cargo run --release --example word_count -- -m <IP:Port>
//! `
//!
//! ## Simple Demo
//! We implemented a simple demo that places soe numbres into the kv adds them
//! and verifies that the addition was correct i.e. all numbers got inserted at 
//! the right place, correctly.
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
//! We also added most of the pieces for orderly shutdown, such as `Kill`
//! messages and a way for the `Client` to listen for these `Kill` messages
//! from the `Server`. Our `Application` currently `await`s a `Kill` messages
//! before it exits. It is not fully complete.
//!
//! The third client will print `SUCCESS` at the end.
//!
//!
//! ## Completed Features
//! 1. SoRer - robust schema on read file parsing with well tested and
//!    documented behavior. Performance of over 400MB/s for a 16 column file on
//!    machine with 8GB RAM, Intel i5-4690K CPU with 4 threads.
//! 2. DataFrame + relevant related APIs - implementations to construct a aFrame
//!    from a `.sor` file, map, pmap, and filter, plus all other methods
//!    provided in the original DataFrame API and related APIs (Schema etc).
//! 3. Generic networking API - Currently allows for directed communication ween
//!    distributed nodes, using one central server for registration. Is also
//!    generic across message types. Need to polish off orderly shutdown.
//! 4. KVStore implemented as required and is able to communicate with other
//!    KVStores and process different messages.
//! 5. The application layer has been completely implemented
//! 6. Distributed DataFrame layer has been completely implemented. Supports
//!    distributed filter
//! 7. All the applications, including Linus, work as desired.
//!
//! ## Road Map
//! 0. Build robust integration tests to define how the distributed system ks.
//! 1. Fix up orderly shutdown of the network layer.
//! 2. Implement random forest.
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
