//! A module with methods to create, organize, and communicate with nodes in a
//! distributed system over `TCP`, as well as implementations of [`Client`] and
//! [`Server`] for `LiquidML`.
//!
//! The [`Server`] struct acts as a simple registration server. Once
//! constructed, calling the [`accept_new_connections`] method will allow
//! [`Client`] nodes to also start up and connect to a distributed system. A
//! [`Server`] may also kindly request order graceful shutdown of the
//! system by broadcasting [`Kill`] messages to nodes.
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
//! [`Kill`]: enum.ControlMsg.html
//! [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
//! [`accept_new_connections`]: struct.Server.html#method.accept_new_connections
use crate::error::LiquidError;
use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::Shutdown;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// A connection to another [`Client`], used for directed communication
///
/// [`Client`]: struct.Client.html
#[derive(Debug)]
pub(crate) struct Connection<T> {
    /// The `IP:Port` of another [`Client`] that we're connected to
    ///
    /// [`Client`]: struct.Client.html
    pub(crate) address: String,
    /// The buffered and framed message codec used for sending messages to the
    /// other [`Client`]
    ///
    /// [`Client`]: struct.Client.html
    pub(crate) sink: FramedSink<T>,
}

/// A buffered and framed message codec for reading messages of type `T`
type FramedStream<T> = FramedRead<ReadHalf<TcpStream>, MessageCodec<T>>;
/// A buffered and framed message codec for sending messages of type `T`
type FramedSink<T> = FramedWrite<WriteHalf<TcpStream>, MessageCodec<T>>;

/// Represents a [`Client`] node in a distributed system that is generic for
/// type `T`, where `T` is the types of messages that can be sent between
/// [`Client`]s
///
/// [`Client`]: struct.Client.html
#[derive(Debug)]
pub struct Client<T> {
    /// The `id` of this [`Client`], assigned by the [`Server`] on startup
    /// to be monotonically increasing based on the order of connections
    ///
    /// [`Client`]: struct.Client.html
    /// [`Server`]: struct.Server.html
    pub id: usize,
    /// The `address` of this [`Client`] in the format `IP:Port`
    ///
    /// [`Client`]: struct.Client.html
    pub address: String,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a map of client id to the [`Connection`] with that
    /// [`Client`]
    ///
    /// [`Connection`]: struct.Connection.html
    /// [`Client`]: struct.Client.html
    pub(crate) directory: HashMap<usize, Connection<T>>,
    /// A buffered and framed message codec for sending messages to the
    /// [`Server`]
    ///
    /// [`Server`]: struct.Server.html
    _server: FramedSink<ControlMsg>,
    /// When this [`Client`] gets a message, it uses this [`mpsc`] channel to
    /// forward messages to whatever layer is using this [`Client`] for
    /// networking to avoid tight coupling. The above layer will receive the
    /// messages on the other half of this [`mpsc`] channel.
    ///
    /// [`Client`]: struct.Client.html
    /// [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
    sender: Sender<Message<T>>,
    /// the type of this [`Client`]
    ///
    /// [`Client`]: struct.Client.html
    client_type: String,
}

/// Represents a registration [`Server`] in a distributed system.
///
/// [`Server`]: struct.Server.html
#[derive(Debug)]
pub struct Server {
    /// The `address` of this [`Server`]
    ///
    /// [`Server`]: struct.Server.html
    pub(crate) address: String,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a `HashMap` of client types to a `HashMap` of
    /// `node_id` to a [`Connection`]
    ///
    /// [`Connection`]: struct.Connection.html
    pub(crate) directory:
        HashMap<String, HashMap<usize, Connection<ControlMsg>>>,
}

/// A message that can sent between nodes for communication. The message
/// is generic for type `T`
#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T> {
    /// The id of this message
    pub msg_id: usize,
    /// The id of the sender
    pub sender_id: usize,
    /// The id of the node this message is being sent to
    pub target_id: usize,
    /// The body of the message
    pub msg: T,
}

/// Control messages to facilitate communication with the registration
/// [`Server`] and other [`Client`]s
///
/// [`Server`]: struct.Server.html
/// [`Client`]: struct.Client.html
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ControlMsg {
    /// A directory message sent by the [`Server`] to new [`Client`]s once they
    /// connect so that they know which other [`Client`]s of that type are
    /// currently connected
    ///
    /// [`Server`]: struct.Server.html
    /// [`Client`]: struct.Client.html
    Directory { dir: Vec<(usize, String)> },
    /// An introduction that a new [`Client`] sends to all other existing
    /// [`Client`]s and the [`Server`]
    Introduction {
        address: String,
        client_type: String,
    },
    /// A message the [`Server`] sends to [`Client`]s to inform them to shut
    /// down
    ///
    /// [`Server`]: struct.Server.html
    /// [`Client`]: struct.Client.html
    Kill,
}

/// A message encoder/decoder to help frame messages sent over `TCP`,
/// particularly in the case of very large messages. Uses a very simple method
/// of writing the length of the serialized message at the very start of
/// a frame, followed by the serialized message. When decoding, this length
/// is used to determine if a full frame has been read.
#[derive(Debug)]
pub(crate) struct MessageCodec<T> {
    phantom: std::marker::PhantomData<T>,
    pub(crate) codec: LengthDelimitedCodec,
}

/// Asynchronously waits to read the next message from the given `reader`
pub(crate) async fn read_msg<T: DeserializeOwned>(
    reader: &mut FramedRead<ReadHalf<TcpStream>, MessageCodec<T>>,
) -> Result<Message<T>, LiquidError> {
    match reader.next().await {
        None => Err(LiquidError::StreamClosed),
        Some(x) => Ok(x?),
    }
}

/// Send the given `message` to the node with the given `target_id` using
/// the given `directory`
pub(crate) async fn send_msg<T: Serialize>(
    target_id: usize,
    message: Message<T>,
    directory: &mut HashMap<usize, Connection<T>>,
) -> Result<(), LiquidError> {
    match directory.get_mut(&target_id) {
        None => Err(LiquidError::UnknownId),
        Some(conn) => {
            conn.sink.send(message).await?;
            Ok(())
        }
    }
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
mod message;
mod server;
