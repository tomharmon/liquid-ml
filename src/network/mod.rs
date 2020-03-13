//! A module with methods to communicate with nodes in a distributed system
//! over TCP, as well as `Client` and `Server` implementations for `LiquidML`
use crate::error::LiquidError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use tokio::io::{
    AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf,
};
use tokio::net::TcpStream;

/// A connection to another `Client`, used for sending directed communication
#[derive(Debug)]
pub struct Connection {
    /// The `IP:Port` of another `Client` that we're connected to
    pub address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub write_stream: BufWriter<WriteHalf<TcpStream>>,
}

/// Reads a message of from the given `reader` into the `buffer` and deserialize
/// it into a type `T`
pub(crate) async fn read_msg<T: DeserializeOwned>(
    reader: &mut BufReader<ReadHalf<TcpStream>>,
    buffer: &mut Vec<u8>,
) -> Result<T, LiquidError> {
    let n = reader.read_u64().await?;
    reader.take(n).read_to_end(buffer).await?;
    let result: T = bincode::deserialize(&buffer[..])?;
    Ok(result)
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
            let msg = bincode::serialize(message)?;
            conn.write_stream.write_u64(msg.len() as u64).await?;
            conn.write_stream.write_all(&msg).await?;
            conn.write_stream.flush().await?;
            Ok(())
        }
    }
}

pub mod client;
pub mod message;
pub mod server;
