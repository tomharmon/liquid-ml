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
//! ## Pre-packaged Server Binary and Server Usage
//!
//! An pre-packaged `Server` binary is available in `src/bin/server.rs` and
//! will fit the needs of `liquid_ml` and your needs if you don't need
//! to improve or extend the `network` module.
//!
//! The `Server` binary may be run using the following command:
//! `cargo run --bin server -- --address <Optional 'IP:Port' Address>`
//!
//! If an IP:Port is not provided, the server defaults to `127.0.0.1:9000`
//!
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
//! 2. Sends the server our `IP:Port` address
//! 3. Server responds with a `RegistrationMsg`
//! 4. Connects to all other existing `Client`s which spawns a Tokio task
//!    for each connection that will read messages from the connection,
//!    publish them to the `mpsc` channel, and notify the receiving end so that the receiving end may process them as desired
//!
//! ## Client Usage
//! For a more in-depth and useful example, it may be worthwhile to look at
//! the source code for the `KVStore`. Here is a toy example that may be
//! useful for getting started.
//!
//! Assume that the `Server` is already running at `68.2.3.4:9000`
//!
//! Client 1 starts up, waits for Client 2 to connect, and sends 'Hi' to
//! Client 2 then the program exits
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use tokio::sync::{Notify, mpsc};
//! use liquid_ml::network::{Client, Message};
//! use liquid_ml::error::LiquidError;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), LiquidError> {
//!     let (sender, receiver) = mpsc::channel(100);
//!     // The `kill_notifier` is used by the `Client` to tell
//!     // higher-level components that use the `Client` when a `Kill` message
//!     // is received from the `Server` so that higher-level components
//!     // can perform orderly shutdown
//!     let kill_notifier = Arc::new(Notify::new());
//!     let client = Client::<String>::new("68.2.3.4:9000",
//!                                        "69.0.4.20:9000",
//!                                        sender,
//!                                        kill_notifier,
//!                                        2, true).await.unwrap();
//!     // `Client::new` returns a `Arc<RwLock<Client>>` so that it may
//!     // be used concurrently
//!     let id = { client.read().await.id };
//!     { client.write().await.send_msg(id + 1, "Hi".to_string()).await? };
//!     Ok(())
//! }
//! ```
//!
//! Client 2 starts up, waits to connect to Client 1, and then the message
//! sent by Client 1 is available on the receiver after it's been sent. The
//! message is popped from the queue and printed.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use tokio::sync::{Notify, mpsc};
//! use liquid_ml::network::{Client, Message};
//! use liquid_ml::error::LiquidError;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), LiquidError> {
//!     let (sender, mut receiver) = mpsc::channel(100);
//!     let kill_notifier = Arc::new(Notify::new());
//!     let client = Client::<String>::new("68.2.3.4:9000",
//!                                        "69.80.08.5:9000",
//!                                        sender,
//!                                        kill_notifier,
//!                                        2, true).await.unwrap();
//!     let msg = receiver.recv().await.unwrap();
//!     println!("{}", msg.msg);
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
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// A connection to another `Client`, used for sending directed communication
pub(crate) struct Connection<T> {
    /// The `IP:Port` of another `Client` that we're connected to
    pub(crate) address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub(crate) sink: FramedSink<T>,
    /// The total amount of memory (in `KiB`) the `Client` we're connected to
    /// is allowed to receive at one time
    pub(crate) memory: u64,
}

type FramedStream<T> = FramedRead<ReadHalf<TcpStream>, MessageCodec<T>>;
type FramedSink<T> = FramedWrite<WriteHalf<TcpStream>, MessageCodec<T>>;

/// Represents a `Client` node in a distributed system that is generic for type
/// `T`, where `T` is the types of messages that can be sent between `Client`s
pub struct Client<T> {
    /// The `id` of this `Client`, assigned by the `Server` on startup
    pub id: usize,
    /// The `address` of this `Client`
    pub address: String,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub(crate) directory: HashMap<usize, Connection<T>>,
    /// A buffered sink to send messages to the `Server`
    _server: FramedSink<ControlMsg>,
    /// When this `Client` gets a message, it uses this `mpsc` channel to give
    /// messages to whatever layer is using this `Client` for networking. The
    /// above layer will receive the messages on the other half of this `mpsc`
    /// channel.
    sender: Sender<Message<T>>,
    /// The total amount of memory (in `KiB`) this `Client` can receive at
    /// one time from other `Client`s
    memory: u64,
}

/// Represents a registration `Server` in a distributed system.
pub struct Server {
    /// The `address` of this `Server`
    pub(crate) address: String,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub(crate) directory: HashMap<usize, Connection<ControlMsg>>,
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

/// Control messages to facilitate communication with the registration `Server`
/// and other `Client`s
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ControlMsg {
    /// A directory message sent by the `Server` to new `Client`s once they
    /// connect to the `Server` so that they know which other `Client`s are
    /// currently connected
    Directory { dir: Vec<(usize, String)> },
    /// An introduction that a new `Client` sends to all other existing
    /// `Client`s and the server
    Introduction { address: String, memory: u64 },
    /// A message the `Server` sends to `Client`s to inform them to shut down
    Kill,
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
