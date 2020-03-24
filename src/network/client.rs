//! Represents a client node in a distributed system, with implementations
//! provided for `LiquidML` use cases.
use crate::error::LiquidError;
use crate::network;
use crate::network::message::{ControlMsg, Message, MessageCodec};
use crate::network::{existing_conn_err, increment_msg_id, Connection};
use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{split, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Notify, RwLock};
use tokio_util::codec::{FramedRead, FramedWrite};

/// Represents a `Client` node in a distributed system, where Type T is the types
/// of messages that can be sent between `Client`s
pub struct Client<T> {
    /// The `id` of this `Client`
    pub id: usize,
    /// The `address` of this `Client`
    pub address: String,
    /// The id of the current message
    pub msg_id: usize,
    /// A directory which is a map of client id to a [`Connection`](Connection)
    pub directory: HashMap<usize, Connection<T>>,
    /// A buffered connection to the `Server`
    pub server: (
        FramedRead<ReadHalf<TcpStream>, MessageCodec<ControlMsg>>,
        FramedWrite<WriteHalf<TcpStream>, MessageCodec<ControlMsg>>,
    ),
    /// A Reciever whch acts as a queue for messages
    pub(crate) receiver: Receiver<Message<T>>,
    ///
    sender: Sender<Message<T>>,
    notifier: Arc<Notify>,
}

// TODO: remove 'static
/// Methods which allow a `Client` node to start up and connect to a distributed
/// system, listen for new connections from other new `Client`s, send
/// directed communication to other `Client`s, and respond to messages from
/// other `Client`s
impl<RT: Send + DeserializeOwned + Serialize + std::fmt::Debug + 'static>
    Client<RT>
{
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
    pub async fn new(
        server_addr: String,
        my_addr: String,
        notifier: Arc<Notify>,
    ) -> Result<Self, LiquidError> {
        // Connect to the server
        let server_stream = TcpStream::connect(server_addr.clone()).await?;
        let (reader, writer) = split(server_stream);
        let mut stream = FramedRead::new(reader, MessageCodec::new());
        let mut sink = FramedWrite::new(writer, MessageCodec::new());
        // Tell the server our address
        sink.send(Message::new(
            0,
            0,
            0,
            ControlMsg::Introduction {
                address: my_addr.clone(),
            },
        ))
        .await?;
        // The Server sends the addresses of all currently connected clients
        let dir = network::read_msg(&mut stream).await?;
        if let ControlMsg::Directory { dir: d } = dir.msg {
            // Todo : Use unbounded channel here
            let (sender, receiver) = channel::<Message<RT>>(1000);
            let mut c = Client {
                id: dir.target_id,
                address: my_addr.clone(),
                msg_id: dir.msg_id + 1,
                directory: HashMap::new(),
                server: (stream, sink),
                receiver,
                sender,
                notifier,
            };

            // Connect to all the clients
            for addr in d {
                c.connect(addr).await?;
            }
            // TODO: Listen for further messages from the Server, e.g. `Kill` messages
            //self.recv_msg();;
            Ok(c)
        } else {
            Err(LiquidError::UnexpectedMessage)
        }
    }

    /// A blocking function that allows a `Client` to listen for connections
    /// from newly started `Client`s. When a new `Client` connects to this
    /// `Client`, we add the connection to this `Client.directory`
    /// and spawn a Tokio task to handle further communication from the new
    /// `Client`
    pub async fn accept_new_connections(
        client: Arc<RwLock<Client<RT>>>,
    ) -> Result<(), LiquidError> {
        let listen_address = { client.read().await.address.clone() };
        let mut listener = TcpListener::bind(listen_address).await?;
        loop {
            // wait on connections from new clients
            let (socket, _) = listener.accept().await?;
            let (reader, writer) = split(socket);
            let mut stream =
                FramedRead::new(reader, MessageCodec::<ControlMsg>::new());
            let sink = FramedWrite::new(writer, MessageCodec::<RT>::new());
            let intro = network::read_msg(&mut stream).await?;
            let addr =
                if let ControlMsg::Introduction { address: a } = intro.msg {
                    a
                } else {
                    return Err(LiquidError::UnexpectedMessage);
                };
            let old_msg_id = { client.read().await.msg_id };
            {
                client.write().await.msg_id =
                    increment_msg_id(old_msg_id, intro.msg_id);
            }
            // Make sure we don't have an existing connection to this client
            let contains_key = {
                client.read().await.directory.contains_key(&intro.sender_id)
            };
            match contains_key {
                true => return existing_conn_err(stream, sink),
                false => {
                    // Add the connection with the new client to this directory
                    let conn = Connection {
                        address: addr.clone(),
                        sink,
                    };
                    {
                        client
                            .write()
                            .await
                            .directory
                            .insert(intro.sender_id, conn);
                    }
                    // spawn a tokio task to handle new messages from the client
                    // that we just connected to
                    // NOTE: Not unsafe because message codec has no fields and
                    // can be converted to a different type without losing meaning
                    let new_stream = unsafe {
                        std::mem::transmute::<
                            FramedRead<
                                ReadHalf<TcpStream>,
                                MessageCodec<ControlMsg>,
                            >,
                            FramedRead<ReadHalf<TcpStream>, MessageCodec<RT>>,
                        >(stream)
                    };
                    {
                        let new_sender = client.read().await.sender.clone();
                        let new_notifier = client.read().await.notifier.clone();
                        Client::recv_msg(new_sender, new_stream, new_notifier);
                    }
                    println!(
                        "Connected to id: {:#?} at address: {:#?}",
                        intro.sender_id, addr
                    );
                }
            }
        }
    }

    /// Connect to a running `Client` with the given `(id, IP:Port)` informa)tion.
    /// After connecting, add the `Connection` to the other `Client` to this
    /// `Client.directory` for sending later messages to the `Client`. Finally,
    /// spawn a Tokio task to read further messages from the `Client` and
    /// handle the message.
    pub(crate) async fn connect(
        &mut self,
        client: (usize, String),
    ) -> Result<(), LiquidError> {
        // Connect to the given client
        let stream = TcpStream::connect(client.1.clone()).await?;
        //dbg!(stream.recv_buffer_size());
        //dbg!(stream.send_buffer_size());
        stream.set_recv_buffer_size(2626560).unwrap();
        let (reader, writer) = split(stream);
        let stream = FramedRead::new(reader, MessageCodec::<RT>::new());
        let mut sink =
            FramedWrite::new(writer, MessageCodec::<ControlMsg>::new());

        // Make the connection struct which holds the stream for sending msgs

        match self.directory.contains_key(&client.0) {
            true => existing_conn_err(stream, sink),
            false => {
                sink.send(Message::new(
                    self.msg_id,
                    self.id,
                    0,
                    ControlMsg::Introduction {
                        address: self.address.clone(),
                    },
                ))
                .await?;
                // NOTE: Not unsafe because message codec has no fields and
                // can be converted to a different type without losing meaning
                let sink = unsafe {
                    std::mem::transmute::<
                        FramedWrite<
                            WriteHalf<TcpStream>,
                            MessageCodec<ControlMsg>,
                        >,
                        FramedWrite<WriteHalf<TcpStream>, MessageCodec<RT>>,
                    >(sink)
                };
                let conn = Connection {
                    address: client.1.clone(),
                    sink,
                };
                // Add the connection to our directory
                self.directory.insert(client.0, conn);
                // spawn a tokio task to handle new messages from the client
                // that we just connected to
                Client::recv_msg(
                    self.sender.clone(),
                    stream,
                    self.notifier.clone(),
                );
                // send the client our id and address so they can add us to
                // their directory
                self.msg_id += 1;
                println!(
                    "Connected to id: {:#?} at address: {:#?}",
                    client.0, client.1
                );
                Ok(())
            }
        }
    }

    // TODO: abstract/merge with Server::send_msg, they are the same
    /// Send the given `message` to a client with the given `target_id`.
    pub async fn send_msg(
        &mut self,
        target_id: usize,
        message: RT,
    ) -> Result<(), LiquidError> {
        let m = Message {
            sender_id: self.id,
            target_id,
            msg_id: self.msg_id,
            msg: message,
        };
        println!("Sending a msg");
        network::send_msg(target_id, m, &mut self.directory).await?;
        println!("sent the message");
        self.msg_id += 1;
        Ok(())
    }

    /// Spawns a Tokio task to read messages from the given `reader` and
    /// handle responding to them.
    pub(crate) fn recv_msg(
        mut sender: Sender<Message<RT>>,
        mut reader: FramedRead<ReadHalf<TcpStream>, MessageCodec<RT>>,
        notifier: Arc<Notify>,
    ) {
        // TODO: make the right callback that we want for handling messages
        // after sending them thru mpsc channel
        // TODO: need to properly increment message id but that means self
        // needs to be 'static and that propagates some
        tokio::spawn(async move {
            loop {
                println!("waiting to read msg");
                let s: Message<RT> =
                    network::read_msg(&mut reader).await.unwrap();
                //        self.msg_id = increment_msg_id(self.msg_id, s.msg_id);
                println!("Got msg");
                sender.send(s).await.unwrap();
                notifier.notify();
                println!("added msg to queue");
            }
        });
    }
}
