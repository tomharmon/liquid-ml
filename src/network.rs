use crate::error::LiquidError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::{
    split, AsyncReadExt, BufReader, BufStream, BufWriter, WriteHalf,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
//TODO: Look at Struct std::net::SocketAddrV4 instead of storing
//      addresses as strings

#[derive(Debug)]
pub struct Client {
    pub id: usize,
    pub address: String,
    pub msg_id: usize,
    pub directory: HashMap<usize, Connection>,
    pub server: BufStream<TcpStream>,
    pub listener: TcpListener,
}

#[derive(Debug)]
pub struct Connection {
    pub address: String,
    pub stream: BufWriter<WriteHalf<TcpStream>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct RegistrationMsg {
    assigned_id: usize,
    msg_id: usize,
    clients: Vec<(usize, String)>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ConnectionMsg {
    my_id: usize,
    msg_id: usize,
    my_address: String,
}

#[allow(dead_code)]
impl Client {
    pub(crate) async fn new(
        server_addr: String,
        my_addr: String,
    ) -> Result<Self, LiquidError> {
        // Connect to the server
        let server_stream = TcpStream::connect(server_addr).await?;
        let mut buf_server = BufStream::new(server_stream);

        // Tell the server our addresss
        buf_server.write(my_addr.as_bytes()).await?;

        // The server will give us registration information
        let mut buffer = Vec::new();
        buf_server.read_to_end(&mut buffer).await?;
        let reg: RegistrationMsg = bincode::deserialize(&buffer[..])?;

        let mut c = Client {
            id: reg.assigned_id,
            address: my_addr.clone(),
            msg_id: reg.msg_id + 1,
            directory: HashMap::new(),
            server: buf_server,
            listener: TcpListener::bind(my_addr.clone()).await?,
        };

        // Connect to all the clients
        for a in reg.clients {
            c.connect(a).await?;
        }

        Ok(c)
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
            // Read the ConnectionMsg from the new client
            let mut buffer = Vec::new();
            buf_reader.read_to_end(&mut buffer).await?;
            let conn_msg: ConnectionMsg = bincode::deserialize(&buffer[..])?;
            // Add the connection to the new client to this directory
            let conn = Connection {
                address: conn_msg.my_address,
                stream: buf_writer,
            };
            // TODO: Close the newly created connections in the error cases
            match self.directory.insert(conn_msg.my_id, conn) {
                Some(_) => return Err(LiquidError::ReconnectionError),
                None => {
                    // spawn a tokio task to handle new messages from the client
                    // that we just connected to
                    self.recv_msg(buf_reader);
                    self.increment_msg_id(conn_msg.msg_id);
                }
            };
        }
    }

    pub(crate) async fn connect(
        &mut self,
        client: (usize, String),
    ) -> Result<(), LiquidError> {
        let stream = TcpStream::connect(client.1.clone()).await?;
        let (reader, writer) = split(stream);
        // spawn a tokio task that handle new messages from the client
        let buf_reader = BufReader::new(reader);
        self.recv_msg(buf_reader);

        // Make the connection struct which holds the stream for sending msgs
        let buf_writer = BufWriter::new(writer);
        let conn = Connection {
            address: client.1,
            stream: buf_writer,
        };
        // add the connection to our directory of connections to other clients
        match self.directory.insert(client.0, conn) {
            Some(_) => Err(LiquidError::ReconnectionError),
            None => {
                // send the client our id and address so they can add us to
                // their directory
                let conn_msg = ConnectionMsg {
                    my_id: self.id,
                    msg_id: self.msg_id,
                    my_address: self.address.clone(),
                };
                self.send_msg(client.0, &conn_msg).await?;
                Ok(())
            }
        }
    }

    pub(crate) async fn send_msg<T: Serialize>(
        &mut self,
        to: usize,
        message: &T,
    ) -> Result<(), LiquidError> {
        match self.directory.get_mut(&to) {
            None => Err(LiquidError::UnknownId),
            Some(conn) => {
                let msg = bincode::serialize(message)?;
                conn.stream.write(&msg[..]).await?; // should this fn return a future?
                self.msg_id += 1;
                Ok(())
            }
        }
    }

    pub(crate) fn recv_msg<T: AsyncReadExt + std::marker::Unpin + Send + 'static>(
        &mut self,
        mut reader: T,
    ) {
        tokio::spawn(async move {
            let mut buff = Vec::new();
            loop {
                reader.read_to_end(&mut buff).await.unwrap();
                let msg: ConnectionMsg = bincode::deserialize(&buff[..]).unwrap();
                println!("{:#?}", msg);
            }
        });
    }

    fn increment_msg_id(&mut self, id: usize) {
        self.id = std::cmp::max(self.id, id) + 1;
    }
}
