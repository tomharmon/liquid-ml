//! A module with methods to communicate with nodes in a distributed system
//! over TCP, as well as `Client` and `Server` implementations for `LiquidML`
use crate::error::LiquidError;
use bytes::Bytes;
use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::net::Shutdown;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

/// A connection to another `Client`, used for sending directed communication
#[derive(Debug)]
pub struct Connection {
    /// The `IP:Port` of another `Client` that we're connected to
    pub address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub sink: FramedWrite<WriteHalf<TcpStream>, BytesCodec>,
}

/// Reads a message of from the given `reader` into the `buffer` and deserialize
/// it into a type `T`
pub(crate) async fn read_msg<T: DeserializeOwned + std::fmt::Debug>(
    reader: &mut FramedRead<ReadHalf<TcpStream>, BytesCodec>,
) -> Result<T, LiquidError> {
    match reader.next().await {
        None => Err(LiquidError::StreamClosed),
        Some(x) => {
            println!("{:?}", x);
            let res = Ok(bincode::deserialize(&x?[..])?);
            println!("deserialized to {:?}", res);
            res
        }
    }
}

/// Send the given `message` over the given `write_stream`
pub(crate) async fn send_msg<T: Serialize>(
    target_id: usize,
    message: &T,
    directory: &mut HashMap<usize, Connection>,
) -> Result<(), LiquidError> {
    match directory.get_mut(&target_id) {
        None => Err(LiquidError::UnknownId),
        Some(conn) => {
            conn.sink
                .send(Bytes::from(bincode::serialize(message)?))
                .await?;
            conn.sink.flush().await?;
            Ok(())
        }
    }
}

pub(crate) fn existing_conn_err(
    stream: FramedRead<ReadHalf<TcpStream>, BytesCodec>,
    sink: FramedWrite<WriteHalf<TcpStream>, BytesCodec>,
) -> Result<(), LiquidError> {
    // Already have an open connection to this client, shut
    // down the one we just created.
    let reader = stream.into_inner();
    let unsplit = reader.unsplit(sink.into_inner());
    unsplit.shutdown(Shutdown::Both)?;
    return Err(LiquidError::ReconnectionError);
}

pub(crate) fn increment_msg_id(cur_id: usize, id: usize) -> usize {
    std::cmp::max(cur_id, id) + 1
}

pub mod client;
pub mod message;
pub mod server;
