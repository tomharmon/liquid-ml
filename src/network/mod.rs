//! A module for `Client` and `Server` traits, which define their behavior as
//! nodes on a distributed system. Implementations are provided for
//! using the `Server` to orchestrate registering nodes onto a distributed
//! system as well as for sending and receiving messages in regards to the
//! use cases of `LiquidML`.

pub mod client;
pub mod message;
pub mod network;
pub mod server;
