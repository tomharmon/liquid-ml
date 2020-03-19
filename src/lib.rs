//! # Introduction
//! LiquidML is an in-memory platform for distributed, scalable machine learning
//! and data analytics. It ships with the random forest machine learning algorithm
//! built in, allowing users to easily classify data using the power of AI.
//!
//! LiquidML is written in Rust for both performance and safety reasons, allowing
//! many optimizations to be made more easily without the risk of a memory safety
//! bug. This helps guarantee security around our clients' data, as many memory
//! safety bugs can be exploited by malicious hackers.
//!
//! LiquidML makes it fast and easy to derive insights from your data by extending
//! it to apply your own analytics tools to your data in a distributed, scalable
//! fashion.
//!
//! LiquidML is currently in development but the finished product will be
//! implemented soon.
//!
//! # Architecture
//! LiquidML consists of the following architectural components:
//! - SoRer
//! - KV Store
//! - DataFrame
//! - Application Layer
//!
//! ## SoRer
//! The main purpose of the `SoRer` is to handle parsing files of an unknown
//! schema and with possible malformed rows. Schemas are inferred based on the
//! first 500 rows and any malformed rows are discarded. Specifics on the `SoRer`
//! may be found [here](https://docs.rs/sorer/0.1.0/sorer/)
//!
//! ## Networking
//! The network layer is the lowest level component of a `LiquidML` instance. It
//! provides a simple registration server and a network of nodes each identified by
//! a unique ID assigned by the server. The nodes are constructed with a callback
//! function that can process different messages and respond to other nodes. The
//! default implemetation is designed to accept and respond to data requests from
//! other nodes running a `KV` store instance.
//!
//! ## KV Store
//! The `KV` store keeps blobs of serialized data in memory with programatically
//! assigned keys which know the node that the data for that key exists in. Each
//! `KV` store implements functionality to utilize the network layer to get data
//! from a different node if the data is available elsewhere and not in this node.
//! The node also caches data that it has accessed over the network previously to
//! increase performance by reducing network calls. The cache smartly evicts data
//! it likely doesn't need to reduce memory usage.
//!
//! ## DataFrame
//! The `DataFrame` represents a higher level API for accessing and processing data
//! held in the `KV` stores. The `DataFrame` has convenient functions to access
//! data in a columnar or row based format. It also supports named columns and
//! rows. Row based processing is done using visitors which implement the `Rower`
//! trait and calling either `DataFrame::filter`, `DataFrame::map`,
//! or `DataFrame::pmap`, the latter of which runs on as many threads that the
//! machine it's running on has. Column based processing can be done more manually
//! by directly getting getting columns as a `Vec` of data.
//!
//! ## Application Layer
//! The application layer is an even higher level API for writing programs to
//! perform data analysis on the entire distributed system. This is done through a
//! trait where anything that implements that trait will run on the dataframe.
//!
//! # Implementation
//! The networking layer uses [`tokio`](https://docs.rs/tokio/0.2.13/tokio/) to
//! handle connections with multiple clients concurrently.
//!
//! The `Client` struct represents a node and asynchronously listens for new
//! connections from other nodes (`Client::accept_new_connections`), listens for
//! `Kill` messages from the `Server`, and maintain a read and write half of a
//! `Connection` to every other active client to both send messages of any
//! `Serializeable` type (`Client::send_msg`) and handle incoming messages with
//! any given callback (`Client::recv_msg`, though our's currently just prints the
//! message).
//!
//! The `Server` asynchronously registers new `Clients` via
//! `Server::accept_new_connections` and also allows sending any `Serializeable`
//! type.
//!
//! There currently is a basic implementation of a KV store. We will be providing
//! the functions `get`, `wait_and_get` and `put`, where `put` stores blobs in a
//! `KV` store and the 2 get methods retrieve data (`wait_and_get` being blocking).
//!
//! The `DataFrame` and supporting classes loosely implement the management
//! provided API with better defined error handling using custom `LiquidError`
//! enumerations. A few discrepancies with certain data types were introduced due
//! to the use of Rust. A `from_sor` function was implemented that uses our `sorer`
//! crate to ingest raw data from a SoR file to create a `DataFrame`. The way
//! the visitor pattern was implemented was also changed from the management
//! given API to match the idiomatic visitor pattern.
//!
//! The application layer has also not yet been implemented however consideration
//! has been given to build a trait that defines proper application code behavior.
//!
//!
//! # Use Cases
//! Creating a dataframe from a SoR file:
//! ```
//! use liquid_ml::dataframe::DataFrame;
//!
//! let df = DataFrame::from_sor("tests/test.sor".to_string(), 0, 1000);
//! assert_eq!(df.n_cols(), 4);
//! assert_eq!(df.n_rows(), 2);
//! ```
//!
//! Further uses cases will be provided when design decisions are finalized and
//! the KV Store and Application layer are better defined.
//!
//! # Open Questions
//! 1. What does the application level API look like, more examples or an actual
//!    interface would help clarify.
//! 2. (Tom) still unsure on specifics of how the KV Store and DataFrame interact.
//!
//! # Status
//! Due to the coronavirus we had a bit less time to work on our project since
//! Sam had to move, but we still made good progress and are on schedule or even
//! slightly ahead of schedule.
//!
//! ## Completed Features
//! 1. SoRer - robust schema on read file parsing with well tested and documented
//!    behavior. Performance of over 400MB/s for a 16 column file on machine with
//!    8GB RAM, Intel i5-4690K CPU with 4 threads.
//! 2. DataFrame + relevant related APIs - implementations to construct a DataFrame
//!    from a `.sor` file, map, pmap, and filter, plus all other methods provided
//!    in the original DataFrame API and related APIs (Schema etc).
//! 3. Basic Networking API - has been ported from CwC and is at parity with the
//!    CwC version implemented in assignmnent 6 except for orderly shutdown.
//!    Currently allows for directed communication between distributed nodes,
//!    using one central server for registration. Can send messages of various
//!    types and can provide a callback with which to handle messages when they are
//!    received.
//! 4. Basic KV Store - initial work on the KV Store.
//!
//! ## Road Map
//! 0. Build robust integration tests to define how the distributed system works.
//! 1. Implement the KV store.
//! 2. Improve the network layer (better error handling, better callbacks based on
//!    `KV` store use cases, and more message types).
//! 3. Integrate the KV store with the network layer.
//! 4. Modify the dataframe to access data from the KV Store.
//! 5. Define the application layer interface.
//! 6. Implement random forest.

pub mod dataframe;
pub mod error;
pub mod fielder;
pub mod kv;
pub mod kv_message;
pub mod network;
pub mod row;
pub mod rower;
pub mod schema;
