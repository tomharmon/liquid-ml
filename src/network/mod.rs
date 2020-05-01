//! A module with methods to create, organize, and communicate with nodes in a
//! distributed system over `TCP`, as well as implementations of [`Client`] and
//! [`Server`] for `liquid_ml`.
//!
//! The [`Server`] struct acts as a simple registration server. Once
//! constructed, calling the [`accept_new_connections`] method will allow
//! [`Client`] nodes to also start up and connect to a distributed system. A
//! [`Server`] may also kindly request order graceful shutdown of the
//! system by broadcasting [`ControlMsg::Kill`] messages to nodes.
//!
//! # Pre-packaged Server Binary and Server Usage
//!
//! An pre-packaged [`Server`] binary is available in `src/bin/server.rs` and
//! provides all needed functionality for `liquid_ml`.
//!
//! The [`Server`] binary may be run using the following command:
//!
//! `cargo run --bin server -- --address <Optional 'IP:Port' Address>`
//!
//! If an address is not provided, the [`Server`] defaults to `127.0.0.1:9000`
//!
//! # [`Client`] Design
//!
//! The [`Client`] is designed with concurrency in mind and can be used to
//! send directed communication to any other node.
//!
//! [`Client`]s are `String`-ly typed so as to support dynamic network
//! generation. If you wish to create multiple networks **and** preserve the
//! `node_id`s assigned by the [`Server`], you should check out
//! [`Client::register_network`], otherwise the [`Client::new`] function should
//! suffice.
//!
//! Processing messages received by the `Client` can by done like this with
//! the [`SelectAll`] struct that is returned by [`Client::register_network`]
//! or [`Client::new`]:
//!
//! ```ignore
//! while let Some(Ok(msg)) = streams.next().await {
//!     // ... process the message here according to your use case
//! }
//! ```
//!
//!
//! [`Client`]: struct.Client.html
//! [`Server`]: struct.Server.html
//! [`ControlMsg::Kill`]: enum.ControlMsg.html#variant.Kill
//! [`accept_new_connections`]: struct.Server.html#method.accept_new_connections
//! [`Client::register_network`]: struct.Client.html#method.register_network
//! [`Client::new`]: struct.Client.html#method.new
//! [`SelectAll`]: https://docs.rs/futures/0.3.4/futures/stream/struct.SelectAll.html
use crate::error::LiquidError;
use crate::network::message::FramedSink;
use std::net::Shutdown;
use std::net::SocketAddr;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

/// A connection to another [`Client`], used for directed communication
///
/// [`Client`]: struct.Client.html
#[derive(Debug)]
pub(crate) struct Connection<T> {
    /// The address of another [`Client`] that we're connected to
    ///
    /// [`Client`]: struct.Client.html
    pub(crate) address: SocketAddr,
    /// The buffered and framed message codec used for sending messages to the
    /// other [`Client`]
    ///
    /// [`Client`]: struct.Client.html
    pub(crate) sink: FramedSink<T>,
}

pub(crate) fn existing_conn_err<T, U>(
    stream: FramedRead<ReadHalf<TcpStream>, MessageCodec<T>>,
    sink: FramedWrite<WriteHalf<TcpStream>, MessageCodec<U>>,
) -> LiquidError {
    // Already have an open connection to this client, shut
    // down the one we just created.
    let reader = stream.into_inner();
    let unsplit = reader.unsplit(sink.into_inner());
    unsplit.shutdown(Shutdown::Both).unwrap();
    LiquidError::ReconnectionError
}

pub(crate) fn increment_msg_id(cur_id: usize, id: usize) -> usize {
    std::cmp::max(cur_id, id) + 1
}

mod client;
pub use client::Client;

mod message;
pub(crate) use message::FramedStream;
pub use message::{ControlMsg, Message, MessageCodec};

mod server;
pub use server::Server;
