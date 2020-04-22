//! Represents a server node in a distributed system, with implementations
//! provided for `LiquidML` use cases.
use crate::error::LiquidError;
use crate::network::{message, Connection, ControlMsg, Message, MessageCodec};
use log::info;
use std::collections::HashMap;
use tokio::io::split;
use tokio::net::TcpListener;
use tokio_util::codec::{FramedRead, FramedWrite};

/// Represents a registration `Server` in a distributed system.
#[derive(Debug)]
pub struct Server {
    /// The `address` of this `Server`
    pub(crate) address: String,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a `HashMap` of client types to a `HashMap` of
    /// `node_id` to a [`Connection`]
    ///
    /// [`Connection`]: struct.Connection.html
    pub(crate) directory:
        HashMap<String, HashMap<usize, Connection<ControlMsg>>>,
}

impl Server {
    /// Create a new [`Server`] running on the given `address` in the format of
    /// `IP:Port`.
    ///
    /// [`Server`]: struct.Server.html
    pub async fn new(address: &str) -> Result<Self, LiquidError> {
        Ok(Server {
            msg_id: 0,
            directory: HashMap::new(),
            address: address.to_string(),
        })
    }

    /// A blocking function that allows a [`Server`] to listen for connections
    /// from newly started [`Client`]s. When a new [`Client`] connects to this
    /// [`Server`], we add the connection to our directory, but do
    /// not listen for further messages from the [`Client`] since this is not
    /// required for performing simple registration.
    ///
    /// [`Server`]: struct.Server.html
    /// [`Client`]: struct.Client.html
    pub async fn accept_new_connections(&mut self) -> Result<(), LiquidError> {
        let mut listener = TcpListener::bind(&self.address).await?;
        loop {
            // wait on connections from new clients
            let (socket, _) = listener.accept().await?;
            let (reader, writer) = split(socket);
            let mut stream = FramedRead::new(reader, MessageCodec::new());
            let sink = FramedWrite::new(writer, MessageCodec::new());
            // Receive the listening IP:Port address of the new client
            let address = message::read_msg(&mut stream).await?;
            let (address, client_type) = if let ControlMsg::Introduction {
                address,
                client_type,
            } = address.msg
            {
                (address, client_type)
            } else {
                return Err(LiquidError::UnexpectedMessage);
            };
            let conn = Connection {
                address: address.clone(),
                sink,
            };

            let target_id;
            let dir;
            match self.directory.get_mut(&client_type) {
                Some(d) => {
                    // there are some existing clients of this type
                    target_id = d.len() + 1; // node id's start at 1
                    dir = d
                        .iter()
                        .map(|(k, v)| (*k, v.address.clone()))
                        .collect();
                    d.insert(target_id, conn);
                }
                None => {
                    target_id = 1;
                    dir = Vec::new();
                    let mut d = HashMap::new();
                    d.insert(target_id, conn);
                    self.directory.insert(client_type.clone(), d);
                }
            };

            info!(
                "Connected to address: {:#?}, with type {:#?}, assigning id: {:#?}",
                address.clone(),
                client_type.clone(),
                target_id
            );

            // Send the new client the list of existing nodes.
            let dir_msg = ControlMsg::Directory { dir };
            self.send_msg(target_id, &client_type, dir_msg).await?;
        }
    }

    // TODO: abstract/merge with Client::send_msg, they are the same
    /// Send the given `message` to a [`Client`] with the given `target_id`.
    ///
    /// [`Client`]: struct.Client.html
    pub async fn send_msg(
        &mut self,
        target_id: usize,
        client_type: &str,
        message: ControlMsg,
    ) -> Result<(), LiquidError> {
        let m = Message {
            sender_id: 0,
            target_id,
            msg_id: self.msg_id,
            msg: message,
        };
        message::send_msg(
            target_id,
            m,
            self.directory.get_mut(client_type).unwrap(),
        )
        .await?;
        self.msg_id += 1;
        Ok(())
    }

    /// Broadcast the given `message` to all currently connected [`Clients`]
    ///
    /// [`Client`]: struct.Client.html
    pub async fn broadcast(
        &mut self,
        message: ControlMsg,
        types: &str,
    ) -> Result<(), LiquidError> {
        let d: Vec<usize> = self
            .directory
            .iter()
            .find(|(k, _)| **k == types)
            .unwrap()
            .1
            .iter()
            .map(|(k, _)| *k)
            .collect();
        for k in d {
            self.send_msg(k, types, message.clone()).await?;
        }
        Ok(())
    }
}
