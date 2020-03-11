use crate::error::LiquidError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::Shutdown;
use futures::io::{AsyncReadExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{BufStream, split};
use tokio::prelude::*;
use futures::stream::SelectAll;
use futures::prelude::*;
//TODO: Look at Struct std::net::SocketAddrV4 instead of storing
//      addresses as strings

pub struct Client {
    pub id: usize,
    pub address: String,
    pub msg_id: usize,
    pub directory: HashMap<usize, Connection>,
    pub server: BufStream<TcpStream>,
    pub reader: SelectAll<BufReader<ReadHalf<TcpStream>>>
}

pub struct Connection {
    address: String,
    stream: BufWriter<WriteHalf<TcpStream>>,
}

#[derive(Serialize, Deserialize)]
struct RegistrationMsg {
    assigned_id: usize,
    msg_id: usize,
    clients: Vec<(usize, String)>,
}

#[derive(Serialize, Deserialize)]
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
        let server_stream = TcpStream::connect(server_addr).await?;
        let mut buf_server = BufStream::new(server_stream);

        // Tell the master our addresss
        buf_server.write(my_addr.as_bytes()).await?;

        // The master will give us registration information
        let mut buffer = Vec::new();
        buf_server.read_to_end(&mut buffer).await?;
        let reg: RegistrationMsg = bincode::deserialize(&buffer[..])?;

        let mut c = Client {
            id: reg.assigned_id,
            address: my_addr,
            msg_id: reg.msg_id + 1,
            directory: HashMap::new(),
            server: buf_server,
            reader: SelectAll::new() 
        };
        // Connect to all the clients
        for a in reg.clients {
            c.connect(a).await?;
        }
        Ok(c)
    }

    pub(crate) async fn accept_new_connection(&mut self) -> Result<(), LiquidError> {
        let mut listener = TcpListener::bind(self.address.clone()).await?;
        loop {
            let (socket, _) = listener.accept().await?;
            let (reader, writer) = split(socket);
            let mut buf_reader = BufReader::new(reader);
            let mut buf_writer = BufWriter::new(writer);
            let mut buffer = Vec::new();
            buf_reader.read_to_end(&mut buffer).await?;
            let conn_msg: ConnectionMsg = bincode::deserialize(&buffer[..])?;
            let conn = Connection { address: conn_msg.my_address, stream: buf_writer };
            // TODO: Close the newly created connections in the error cases
            match self.directory.insert(conn_msg.my_id, conn) {
                Some(old_buf) => return Err(LiquidError::ReconnectionError), 
                None => {
                    self.reader.push(buf_reader);
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
        let mut buf_reader = BufReader::new(reader);
        let mut buf_writer = BufWriter::new(writer);
        // send them our address
        let msg = bincode::serialize(&ConnectionMsg {
            my_id: self.id,
            msg_id: self.msg_id,
            my_address: self.address.clone(),
        })?;

        buf_writer.write(&msg[..]).await?;
        self.msg_id += 1;
        let conn = Connection {
            address: client.1,
            stream: buf_writer,
        };
        match self.directory.insert(client.0, conn) {
            Some(old_buf) => Err(LiquidError::ReconnectionError), 
            None => {
                self.reader.push(buf_reader);
                Ok(())
            }
        std::
    }

    /// NOTE: Might want to split the TCP Stream for better concurrency
    pub(crate) async fn send_msg<T: Serialize> (&mut self, to: usize, message: &T) -> Result<(), LiquidError> { 
       match self.directory.get_mut(&to) {
           None => Err(LiquidError::UnknownId),
           Some(conn) =>  {
               let msg = bincode::serialize(message)?;
               conn.stream.write(&msg[..]);
               Ok(())
           }
       }
    }

    /*pub(crate) async fn recv_msg<'de, T: Deserialize<'de>>(&mut self) -> Result<T, LiquidError> {
        let mut futures = Vec::new();
        let mut buffers = Vec::new();
        for (_, conn) in self.directory.iter_mut() {
            let mut buff = Vec::new();
            futures.push(conn.stream.read_to_end(&mut buff));
            buffers.push(buff);
        }
        future::select_all(futures.iter_mut());
        Err(LiquidError::UnknownId) 
        // TODO: Not sure what this should do 
    }*/

    fn increment_msg_id(&mut self, id: usize) {
        self.id = std::cmp::max(self.id, id) + 1; 
    }

}
