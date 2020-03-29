//! A module with methods to communicate with nodes in a distributed system
//! over TCP, as well as `Client` and `Server` implementations for `LiquidML`
use crate::error::LiquidError;
use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::Shutdown;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Notify,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// A connection to another `Client`, used for sending directed communication
pub struct Connection<T> {
    /// The `IP:Port` of another `Client` that we're connected to
    pub address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub sink: FramedWrite<WriteHalf<TcpStream>, MessageCodec<T>>,
}

type FramedStream = FramedRead<ReadHalf<TcpStream>, MessageCodec<ControlMsg>>;
type FramedSink = FramedWrite<WriteHalf<TcpStream>, MessageCodec<ControlMsg>>;

/// Represents a `Client` node in a distributed system, where Type T is the types
/// of messages that can be sent between `Client`s
pub struct Client<T> {
    /// The `id` of this `Client`
    pub id: usize,
    /// The `address` of this `Client`
    pub address: String,
    /// The id of the current message
    pub msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub directory: HashMap<usize, Connection<T>>,
    /// A buffered connection to the `Server`
    pub server: (FramedStream, FramedSink),
    /// A queue which messages from other `Client`s are added to. After a
    /// `Message<T>` is added to the `receiver`, the layer above this `Client`
    /// is notified there is a new `message` available on the `receiver` via
    /// the `notifier` so that the above layer can get the message off this
    /// `receiver` and process it how it wants to.
    pub(crate) receiver: Receiver<Message<T>>,
    /// When this `Client` gets a message, it uses this `sender` to give
    /// messages to whatever layer above this is using this `Client` for
    /// networking. The above layer will receive the messages on the
    /// `receiver`.
    sender: Sender<Message<T>>,
    /// Used to notify whatever is using this `Client` for networking that a
    /// message has been received and is available on the `receiver`.
    pub(crate) notifier: Arc<Notify>,
}

/// Represents a registration `Server` in a distributed system.
pub struct Server {
    /// The `address` of this `Server`
    pub address: String,
    /// The id of the current message
    pub msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub directory: HashMap<usize, Connection<ControlMsg>>,
    /// A `TcpListener` which listens for connections from new `Client`s
    pub listener: TcpListener,
}

/// A message for communication between nodes
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
pub enum ControlMsg {
    /// A directory message sent by the `Server` to new `Client`s once they
    /// connect to the `Server` so that they know which other `Client`s are
    /// currently connected
    Directory { dir: Vec<(usize, String)> },
    /// An introduction that a new `Client` sends to all other existing
    /// `Client`s and the server
    Introduction { address: String },
    //TODO : Add a kill message here at some point
}

#[derive(Debug)]
pub struct MessageCodec<T> {
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
