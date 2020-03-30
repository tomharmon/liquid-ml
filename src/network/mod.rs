//! A module with methods to communicate with relatively generic nodes in a
//! distributed system over TCP, as well as `Client` and `Server`
//! implementations for `LiquidML`
//!
//! The `Server` struct acts as a simple registration server. Once started with
//! a given `IP:Port` via the `Server::new` method, calling the (blocking)
//! `Server::accept_new_connections` method will allow  `Client` nodes to also
//! start up and connect to a distributed system.
//!
//! When new `Client`s connect to the `Server`, they send a
//! `ControlMsg::Introduction` to tell the `Server` the `Client`'s address
//! (`IP:Port`). The `Server` then responds with a `ControlMsg::Directory` of
//! all the currently connected `Client`s so that the new `Client` may
//! connect to them.
//!
//!
//! ## Server Usage
//! ```rust,no_run
//! use liquid_ml::error::LiquidError;
//! use liquid_ml::network::Server;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), LiquidError> {
//!     let mut s = Server::new("127.0.0.1:9000".to_string()).await?;
//!     s.accept_new_connections().await?;
//!     Ok(())
//! }
//!
//! ```
//!
//! The `Client` is designed so that it can perform various networking
//! operations asynchronously. It can listen for messages from
//! the `Server` or any number of `Client`s (within physical limitations)
//! concurrently. Messages from the `Server` are processed internally by the
//! `Client`, while messages from other `Client`s are sent over a `mpsc` channel
//! and notifies the receiving end when messages are sent. Because of this a
//! `Client` and the networking layer can be used by higher level components
//! without tight coupling.
//!
//!
//! Constructing a new `Client` does these things:
//! 1. Connects to the `Server`
//! 2. Sends the server our IP:Port address
//! 3. Server responds with a `RegistrationMsg`
//! 4. Connects to all other existing `Client`s which spawns a Tokio task
//!    for each connection that will read messages from the connection,
//!    publish them to the `mpsc` channel, and notify the receiving end so
//!    that the receiving end may process them as desired
//!
//! ## Client Usage
//! For a more in-depth and useful example, it may be worthwhile to look at
//! the source code for the `KVStore`. Here is a toy example that may be
//! useful for getting started.
//!
//! Assume that the `Server` is already running at `68.2.3.4:9000`
//!
//! Client 1 starts up, all it does is read messages and print them
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use tokio::sync::Notify;
//! use liquid_ml::network::{Client, Message};
//! use liquid_ml::error::LiquidError;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), LiquidError> {
//!     let notifier = Arc::new(Notify::new());
//!     let client = Client::<String>::new("68.2.3.4:9000",
//!                                        "69.0.4.20:9000",
//!                                        notifier.clone()).await.unwrap();
//!     loop {
//!         // this notifier is notified everytime there is a message from the queue
//!         notifier.notified().await;
//!         println!(client.write().await.receiver.recv().await);
//!     }
//!     Ok(())
//! }
//! ```
//! Client 2 starts up and greets client 1
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use tokio::sync::Notify;
//! use liquid_ml::network::{Client, Message};
//! use liquid_ml::error::LiquidError;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), LiquidError> {
//!     let notifier = Arc::new(Notify::new());
//!     let client = Client::<String>::new("68.2.3.4:9000",
//!                                        "69.80.08.5:9000",
//!                                        notifier.clone()).await.unwrap();
//!     // see `Client::new` documentation for `new` returns a
//!     // `Arc<RwLock<Client>>`
//!     { client.write().await.send_msg(id + 1, "hello".to_string()).await? };
//!     Ok(())
//! }
//! ```
use crate::error::LiquidError;
use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::Shutdown;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// A connection to another `Client`, used for sending directed communication
pub(crate) struct Connection<T> {
    /// The `IP:Port` of another `Client` that we're connected to
    pub(crate) address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub(crate) sink: FramedSink<T>,
}

type FramedStream<T> = FramedRead<ReadHalf<TcpStream>, MessageCodec<T>>;
type FramedSink<T> = FramedWrite<WriteHalf<TcpStream>, MessageCodec<T>>;

/// Represents a `Client` node in a distributed system, where Type `T` is the
/// types of messages that can be sent between `Client`s
pub struct Client<T> {
    /// The `id` of this `Client`
    pub id: usize,
    /// The `address` of this `Client`
    pub address: String,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub(crate) directory: HashMap<usize, Connection<T>>,
    /// A buffered connection to the `Server`
    _server: (FramedStream<ControlMsg>, FramedSink<ControlMsg>),
    /// When this `Client` gets a message, it uses this `mpsc` channel to give
    /// messages to whatever layer is using this `Client` for networking. The
    /// above layer will receive the messages on the other half of this `mpsc`
    /// channel.
    sender: Sender<Message<T>>,
}

/// Represents a registration `Server` in a distributed system.
pub struct Server {
    /// The `address` of this `Server`
    pub(crate) _address: String,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub(crate) directory: HashMap<usize, Connection<ControlMsg>>,
    /// A `TcpListener` which listens for connections from new `Client`s
    pub(crate) listener: TcpListener,
}

/// A message that are sent between nodes for communication
#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T> {
    /// The id of this message
    pub(crate) msg_id: usize,
    /// The id of the sender
    pub(crate) sender_id: usize,
    /// The id of the node this message is being sent to
    pub(crate) target_id: usize,
    /// The body of the message
    pub(crate) msg: T,
}

/// Control messages to facilitate communication with the registration server
#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum ControlMsg {
    /// A directory message sent by the `Server` to new `Client`s once they
    /// connect to the `Server` so that they know which other `Client`s are
    /// currently connected
    Directory { dir: Vec<(usize, String)> },
    /// An introduction that a new `Client` sends to all other existing
    /// `Client`s and the server
    Introduction { address: String },
    //TODO : Add a kill message here at some point
}

/// A message encoder/decoder to help frame messages sent over TCP,
/// particularly in the case of very large messages. Uses a very simple method
/// of writing the length of the serialized message at the very start of
/// a frame, followed by the serialized message. When decoding, this length
/// is used to determine if a full frame has been read.
#[derive(Debug)]
pub(crate) struct MessageCodec<T> {
    phantom: std::marker::PhantomData<T>,
    pub(crate) codec: LengthDelimitedCodec,
}

/// Reads a message of from the given `reader` into the `buffer` and deserialize
/// it into a type `T`
pub(crate) async fn read_msg<T: DeserializeOwned + std::fmt::Debug>(
    reader: &mut FramedRead<ReadHalf<TcpStream>, MessageCodec<T>>,
) -> Result<Message<T>, LiquidError> {
    match reader.next().await {
        None => Err(LiquidError::StreamClosed),
        Some(x) => Ok(x?),
    }
}

/// Send the given `message` over the given `write_stream`
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
