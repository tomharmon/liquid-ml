use crate::error::LiquidError;
use crate::network::network::Connection;
use crate::network::message::*;
use serde::Serialize;
use std::collections::HashMap;
use tokio::io::{
    split, AsyncReadExt, BufReader, BufStream, BufWriter, 
};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
//TODO: Look at Struct std::net::SocketAddrV4 instead of storing
//      addresses as strings

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
    /// Create a new `Server` running on the given `my_addr` IP:Port address.
    pub(crate) async fn new(
        address: String,
    ) -> Result<Self, LiquidError> {
        // Bind to the given IP:Port 
        let registration_listener = TcpListener::bind(address.clone()).await?;
        let s = Server {
            address,
            msg_id: 0,
            directory: HashMap::new(),
            listener: registration_listener
        };
        //tokio::spawn(s.accept_new_connection());
        Ok(s)
    }

    pub(crate) async fn accept_new_connection(
        &mut self,
    ) -> Result<(), LiquidError> {
        loop {
            // wait on connections from new clients
            let (socket, _) = self.listener.accept().await?;
            let (reader, writer) = split(socket);
            let mut buf_reader = BufReader::new(reader);
            let buf_writer = BufWriter::new(writer);
            // Read the IP:Port from the client
            let mut buffer = Vec::new();
            buf_reader.read_to_end(&mut buffer).await?;
            let address: String = bincode::deserialize(&buffer[..])?;
            // Add the connection to the new client to this directory
            let conn = Connection {
                address,
                stream: buf_writer,
            };

            let assigned_id = self.directory.len();

            let reg_msg = RegistrationMsg {
                assigned_id,
                msg_id: self.msg_id,
                clients: self.directory.iter().map(|(k, v)| (*k, v.address.clone())).collect()
            };

            // TODO: Close the newly created connections in the error cases
            match self.directory.insert(assigned_id, conn) {
                Some(_) => return Err(LiquidError::ReconnectionError),
                None => {
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
        match self.directory.get_mut(&target_id) {
            None => Err(LiquidError::UnknownId),
            Some(conn) => {
                let msg = bincode::serialize(message)?;
                conn.stream.write(&msg[..]).await?; // should send_msg return a future?
                self.msg_id += 1;
                Ok(())
            }
        }
    }
}
