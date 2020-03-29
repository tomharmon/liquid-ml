//! A module with methods to communicate with nodes in a distributed system
//! over TCP, as well as `Client` and `Server` implementations for `LiquidML`
use crate::error::LiquidError;
use crate::network::message::{Message, MessageCodec};
use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::net::Shutdown;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

/// A connection to another `Client`, used for sending directed communication
pub struct Connection<T> {
    /// The `IP:Port` of another `Client` that we're connected to
    pub address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub sink: FramedWrite<WriteHalf<TcpStream>, MessageCodec<T>>,
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
