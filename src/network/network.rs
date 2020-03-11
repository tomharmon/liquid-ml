use tokio::io::{BufWriter, WriteHalf};
use tokio::net::TcpStream;

/// A connection to another `Client`, used for sending directed communication
#[derive(Debug)]
pub struct Connection {
    /// The `IP:Port` of another `Client` that we're connected to
    pub address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub stream: BufWriter<WriteHalf<TcpStream>>,
}
