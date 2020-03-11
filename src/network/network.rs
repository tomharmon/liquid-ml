use tokio::io::{BufWriter, WriteHalf, ReadHalf, AsyncReadExt, AsyncWriteExt, BufReader};
use crate::error::LiquidError;
use tokio::net::TcpStream;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// A connection to another `Client`, used for sending directed communication
#[derive(Debug)]
pub struct Connection {
    /// The `IP:Port` of another `Client` that we're connected to
    pub address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub stream: BufWriter<WriteHalf<TcpStream>>,
}

pub(crate) async fn read_msg<T: DeserializeOwned>(reader: &mut BufReader<ReadHalf<TcpStream>>) -> Result<T, LiquidError> {
    let n = reader.read_u64().await?;
    //TODO: Maybe pass in a buffer sso a new one doesnt need to be created everytime
    let mut buff = Vec::new();
    reader.take(n).read_to_end(&mut buff).await?;
    let result : T = bincode::deserialize(&buff[..])?;
    Ok(result)
}

pub(crate) async fn send_msg<T: Serialize>(message: &T, writer: &mut BufWriter<WriteHalf<TcpStream>>) -> Result<(), LiquidError> {
    let msg = bincode::serialize(message)?;
    writer.write_u64(msg.len() as u64).await?;
    writer.write_all(&msg).await?;
    writer.flush().await?;
    Ok(())
}
