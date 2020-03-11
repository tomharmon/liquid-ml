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

/// Represents a Client node in a distributed system.
#[derive(Debug)]
pub struct Client {
    /// The `id` of this `Client`
    pub id: usize,
    /// The `address` of this `Client`
    pub address: String,
    /// The id of the current message
    pub msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub directory: HashMap<usize, Connection>,
    /// A buffered connection to the `Server`
    pub server: BufStream<TcpStream>,
    /// A `TcpListener` which listens for connections from new `Client`s
    pub listener: TcpListener,
}

/// A connection to another `Client`, used for sending directed communication
#[derive(Debug)]
pub struct Connection {
    /// The `IP:Port` of another `Client` that we're connected to
    pub address: String,
    /// The buffered stream used for sending messages to the other `Client`
    pub stream: BufWriter<WriteHalf<TcpStream>>,
}

/// A registration message sent by the `Server` to new `Client`s once they
/// connect to the `Server` so that they know which other `Client`s are
/// currently connected
#[derive(Serialize, Deserialize, Debug)]
struct RegistrationMsg {
    /// The id that the `Server` assigns to the new `Client`
    assigned_id: usize,
    /// The id of this `RegistrationMsg`
    msg_id: usize,
    /// A list of the currently connected clients, containing a tuple of 
    /// `(node_id, IP:Port String)`
    clients: Vec<(usize, String)>,
}

/// A connection message that a new `Client` sends to all other existing 
/// `Client`s after the new `Client` receives a `RegistrationMsg` from
/// the `Server`
#[derive(Serialize, Deserialize, Debug)]
struct ConnectionMsg {
    /// The id of the new `Client`
    my_id: usize,
    /// The id of this `ConnectionMsg`
    msg_id: usize,
    /// The IP:Port of the new `Client`
    my_address: String,
}

/// Methods which allow a `Client` node to start up and connect to a distributed
/// system, listen for new connections from other new `Client`s, send
/// directed communication to other `Client`s, and respond to messages from
/// other `Client`s
#[allow(dead_code)]
impl Client {
    /// Create a new `Client` running on the given `my_addr` IP:Port address,
    /// which connects to a server running on the given `server_addr` IP:Port.
    ///
    /// Constructing the `Client` does these things:
    /// 1. Connects to the server
    /// 2. Sends the server our IP:Port address
    /// 3. Server responds with a `RegistrationMsg`
    /// 4. Connects to all other existing `Client`s which spawns a Tokio task
    ///    for each connection that will read messages from the connection
    ///    and handle it.
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

    /// A blocking function that allows a `Client` to listen for connections
    /// from newly started `Client`s. When a new `Client` connects to this
    /// `Client`, we add the connection to them to this `Client.directory`
    /// and spawn a Tokio task to handle further communication from the new
    /// `Client`
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
                    self.recv_msg(buf_reader, |x| println!("{:?}", x));
                    self.increment_msg_id(conn_msg.msg_id);
                }
            };
        }
    }

    /// Connect to a running `Client` with the given `(id, IP:Port)` information.
    /// After connecting, add the `Connection` to the other `Client` to this
    /// `Client.directory` for sending later messages to the `Client`. Finally,
    /// spawn a Tokio task to read further messages from the `Client` and
    /// handle the message.
    pub(crate) async fn connect(
        &mut self,
        client: (usize, String),
    ) -> Result<(), LiquidError> {
        let stream = TcpStream::connect(client.1.clone()).await?;
        let (reader, writer) = split(stream);
        let buf_reader = BufReader::new(reader);

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
                // spawn a tokio task to handle new messages from the client
                // that we just connected to
                self.recv_msg(buf_reader, |x| println!("{:?}", x));
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

    /// Spawns a Tokio task to read messages from the given `reader` and
    /// handle responding to them.
    pub(crate) fn recv_msg<T: AsyncReadExt + Unpin + Send + 'static>(
        &mut self,
        mut reader: T,
        callback: fn(&[u8]) -> ()
    ) {
        // NOTE: may need to do tokio::runtime::Runtime::spawn or
        // tokio::runtime::Handle::spawn in order to actually place spawned
        // task into an executor
        tokio::spawn(async move {
            let mut buff = Vec::new();
            println!("Listening for msgs");
            loop {
                reader.read_to_end(&mut buff).await.unwrap();
                callback(&buff[..]);
            }
        });
    }

    fn increment_msg_id(&mut self, id: usize) {
        self.id = std::cmp::max(self.id, id) + 1;
    }
}
