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
//! The [`Client`] is designed so that it can perform various networking
//! operations asynchronously and thus it can listen for messages from the
//! [`Server`] or any number of [`Client`]s (within physical limitations)
//! concurrently.
//!
//! Messages from the [`Server`] are processed internally by the [`Client`],
//! while messages from other [`Client`]s are sent over a [`mpsc`] channel
//! (which is passed in during construction) for processing. Because of this a
//! [`Client`] can be used by higher level components without being tightly
//! coupled.
//!
//! [`Client`]s are stringly typed to support dynamic client type generation.
//! [`Client`]s may only connect to other [`Client`]s of the same type.
//!
//! [`Client`]: struct.Client.html
//! [`Server`]: struct.Server.html
//! [`ControlMsg::Kill`]: enum.ControlMsg.html#variant.Kill
//! [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
//! [`accept_new_connections`]: struct.Server.html#method.accept_new_connections
use crate::error::LiquidError;
use crate::network::message::{FramedSink, FramedStream, MessageCodec};
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
) -> Result<(), LiquidError> {
    // Already have an open connection to this client, shut
    // down the one we just created.
    let reader = stream.into_inner();
    let unsplit = reader.unsplit(sink.into_inner());
    unsplit.shutdown(Shutdown::Both)?;
    Err(LiquidError::ReconnectionError)
}

pub(crate) fn increment_msg_id(cur_id: usize, id: usize) -> usize {
    std::cmp::max(cur_id, id) + 1
}

mod client;
pub use client::Client;

mod message;
pub use message::{ControlMsg, Message};

mod server;
pub use server::Server;
