use crate::error::LiquidError;
use crate::network::message::*;
use crate::network::network;
use crate::network::network::Connection;
use serde::Serialize;
use std::collections::HashMap;
use tokio::io::{split, BufReader, BufWriter};
use tokio::net::TcpListener;

/// Represents a registration `Server` in a distributed system.
#[derive(Debug)]
pub struct Server {
    /// The `address` of this `Server`
    pub address: String,
    /// The id of the current message
    pub msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub directory: HashMap<usize, Connection>,
    /// A `TcpListener` which listens for connections from new `Client`s
    pub listener: TcpListener,
}

/// Methods which allow a `Client` node to start up and connect to a distributed
/// system, listen for new connections from other new `Client`s, send
/// directed communication to other `Client`s, and respond to messages from
/// other `Client`s
#[allow(dead_code)]
impl Server {
    /// Create a new `Server` running on the given `address` in the format of
    /// `IP:Port`
    pub async fn new(address: String) -> Result<Self, LiquidError> {
        Ok(Server {
            listener: TcpListener::bind(&address).await?,
            msg_id: 0,
            directory: HashMap::new(),
            address,
        })
    }

    /// A blocking function that allows a `Server` to listen for connections
    /// from newly started `Client`s. When a new `Client` connects to this
    /// `Server`, we add the connection to this `Server.directory`, but do
    /// not listen for further messages from the `Client` since this is not
    /// required for any desired functionality.
    pub async fn accept_new_connections(&mut self) -> Result<(), LiquidError> {
        let mut buffer = Vec::new();
        loop {
            // wait on connections from new clients
            let (socket, _) = self.listener.accept().await?;
            let (reader, writer) = split(socket);
            let mut read_stream = BufReader::new(reader);
            let write_stream = BufWriter::new(writer);
            // Read the IP:Port from the client
            let address: String =
                network::read_msg(&mut read_stream, &mut buffer).await?;

            // Represents the connection from this Server to the new Client
            let conn = Connection {
                address,
                write_stream,
            };
            let assigned_id = self.directory.len();

            // TODO: Close the newly created connections in the error cases
            // Add the connection with the new client to this directory
            match self.directory.insert(assigned_id, conn) {
                Some(_) => return Err(LiquidError::ReconnectionError),
                None => {
                    let reg_msg = RegistrationMsg {
                        assigned_id,
                        msg_id: self.msg_id,
                        clients: self
                            .directory
                            .iter()
                            .map(|(k, v)| (*k, v.address.clone()))
                            .collect(),
                    };
                    self.send_msg(assigned_id, &reg_msg).await?;
                }
            };
        }
    }

    /// Send a message to a client with the given `target_id`.
    pub(crate) async fn send_msg<T: Serialize>(
        &mut self,
        target_id: usize,
        message: &T,
    ) -> Result<(), LiquidError> {
        network::send_msg(target_id, message, &mut self.directory).await?;
        self.msg_id += 1;
        Ok(())
    }
}
