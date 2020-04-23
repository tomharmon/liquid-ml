//! Represents a client node in a distributed system, with implementations
//! provided for `LiquidML` use cases.
use crate::error::LiquidError;
use crate::network::{
    existing_conn_err, increment_msg_id, message, Connection, ControlMsg,
    FramedSink, FramedStream, Message, MessageCodec,
};
use futures::SinkExt;
use log::{debug, info};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc::Sender, Notify, RwLock};
use tokio_util::codec::{FramedRead, FramedWrite};

/// Represents a `Client` node in a distributed system that is generic for
/// type `T`, where `T` is the types of messages that can be sent between
/// `Client`s
#[derive(Debug)]
pub struct Client<T> {
    /// The `id` of this `Client`, assigned by the [`Server`] on startup
    /// to be monotonically increasing based on the order of connections
    ///
    /// [`Server`]: struct.Server.html
    pub id: usize,
    /// The `address` of this `Client`
    pub address: SocketAddr,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a map of client id to the [`Connection`] with that
    /// `Client`
    ///
    /// [`Connection`]: struct.Connection.html
    pub(crate) directory: HashMap<usize, Connection<T>>,
    /// A buffered and framed message codec for sending messages to the
    /// [`Server`]
    ///
    /// [`Server`]: struct.Server.html
    _server: Connection<ControlMsg>,
    /// When this `Client` gets a message, it uses this [`mpsc`] channel to
    /// forward messages to whatever layer is using this `Client` for
    /// networking to avoid tight coupling. The above layer will receive the
    /// messages on the other half of this [`mpsc`] channel.
    ///
    /// [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
    sender: Sender<Message<T>>,
    /// the type of this `Client`
    client_type: String,
}

// TODO: remove `DeserializeOwned + 'static`
impl<RT: Send + Sync + DeserializeOwned + Serialize + Clone + 'static>
    Client<RT>
{
    /// Create a new [`Client`] of a given `client_type` that runs at `my_ip`
    /// and `my_port`
    ///
    /// Constructing the [`Client`] does these things:
    /// 1. Connects to the [`Server`]
    /// 2. Sends the server a [`ControlMsg::Introduction`] containing our `IP`,
    ///    `port`, and `client_type`
    /// 3. The [`Server`] responds with a [`ControlMsg::Directory`], containing
    ///    the addresses of all other currently connected [`Client`]s with a
    ///    type that matches this [`Client`]s `client_type`
    /// 4. Connects to all other existing [`Client`]s of our `client_type`,
    ///    spawning a Tokio task for each connection. The task will read
    ///    messages from the connection and and forward it over the sending
    ///    half of an [`mpsc`] channel with the given `sender`.
    ///
    /// Creating a new [`Client`] returns an `Arc<RwLock<Self>>` so that
    /// the layer above the [`Client`] can use it concurrently.
    ///
    /// ## Parameters
    /// - `server_addr`: The address of the [`Server`] in `IP:Port` format
    /// - `my_ip`: The `IP` of this [`Client`]
    /// - `my_port`: An optional port for this [`Client`] to listen for new
    ///              connections. If its `None`, uses the OS to randomly assign
    ///              a port.
    /// - `sender`: The sending half of an [`mpsc`] channel, for forwarding
    ///             messages to a higher level component that is using this
    ///             [`Client`] so that this component may process the message
    ///             however it wants to. You must create this [`mpsc`] channel
    ///             before constructing a [`Client`], pass in the sending half,
    ///             and hold onto the receiving half so you may process
    ///             messages the [`Client`] receives.
    /// - `kill_notifier`: Created outside of this [`Client`] and passed in
    ///                    during construction so that the higher level
    ///                    component using this [`Client`] can be notified when
    ///                    a [`ControlMsg::Kill`] message is received from the
    ///                    [`Server`] so that orderly shut down may be
    ///                    performed.
    /// - `wait_for_all_clients`: Whether to wait for `num_clients` to connect
    ///                           connect to the system before returning from
    ///                           this function.
    /// - `client_type`: The type of [`Client`]s for this [`Client`] to connect
    ///                  with.
    ///
    /// [`Client`]: struct.Client.html
    /// [`Server`]: struct.Server.html
    /// [`ControlMsg::Directory`]: enum.ControlMsg.html#variant.Directory
    /// [`ControlMsg::Introduction`]: enum.ControlMsg.html#variant.Introduction
    /// [`ControlMsg::Kill`]: enum.ControlMsg.html#variant.Kill
    /// [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
    pub async fn new(
        server_addr: &str,
        my_ip: &str,
        my_port: Option<&str>,
        sender: Sender<Message<RT>>,
        kill_notifier: Arc<Notify>,
        num_clients: usize,
        wait_for_all_clients: bool,
        client_type: &str,
    ) -> Result<Arc<RwLock<Self>>, LiquidError> {
        // Setup a TCPListener
        let listener;
        let my_address: SocketAddr = match my_port {
            Some(port) => {
                let addr = format!("{}:{}", my_ip, port);
                listener = TcpListener::bind(&addr).await?;
                addr.parse().unwrap()
            }
            None => {
                let addr = format!("{}:0", my_ip);
                listener = TcpListener::bind(&addr).await?;
                listener.local_addr()?.to_string().parse().unwrap()
            }
        };
        // Connect to the server
        let server_stream = TcpStream::connect(server_addr).await?;
        let server_address = server_stream.peer_addr().unwrap();
        let (reader, writer) = io::split(server_stream);
        let mut stream = FramedRead::new(reader, MessageCodec::new());
        let sink = FramedWrite::new(writer, MessageCodec::new());
        let mut _server = Connection {
            address: server_address,
            sink,
        };
        // Tell the server our address
        _server.sink.send(Message::new(
            0,
            0,
            0,
            ControlMsg::Introduction {
                address: my_address.clone(),
                client_type: client_type.to_string(),
            },
        ))
        .await?;
        // The Server sends the addresses of all currently connected clients
        let dir_msg = message::read_msg(&mut stream).await?;
        let dir = if let ControlMsg::Directory { dir } = dir_msg.msg {
            dir
        } else {
            return Err(LiquidError::UnexpectedMessage);
        };

        info!(
            "Client of type {} got id {} running at address {}",
            client_type,
            dir_msg.target_id,
            my_address.clone()
        );

        // initialize `self`
        let mut c = Client {
            id: dir_msg.target_id,
            address: my_address.clone(),
            msg_id: dir_msg.msg_id + 1,
            directory: HashMap::new(),
            _server,
            sender,
            client_type: client_type.to_string(),
        };

        // Connect to all the clients
        for addr in dir {
            c.connect(addr).await?;
        }

        // Listen for further messages from the Server, e.g. `Kill` messages
        Client::<ControlMsg>::recv_server_msg(stream, kill_notifier);

        // spawn a tokio task for accepting connections from new clients
        let concurrent_client = Arc::new(RwLock::new(c));
        let concurrent_client_cloned = concurrent_client.clone();
        if wait_for_all_clients {
            Client::accept_new_connections(
                concurrent_client_cloned,
                listener,
                num_clients,
                wait_for_all_clients,
            )
            .await?
        } else {
            tokio::spawn(async move {
                Client::accept_new_connections(
                    concurrent_client_cloned,
                    listener,
                    num_clients,
                    wait_for_all_clients,
                )
                .await
                .unwrap();
            });
        }

        Ok(concurrent_client)
    }

    /// A blocking function that allows a [`Client`] to listen for connections
    /// from newly started [`Client`]s. When a new [`Client`] connects to this
    /// [`Client`], we add the [`Connection`] to its directory and call spawn a
    /// `tokio` task to receive messages from the connection when they arrive
    /// so that messages may be forwarded over the `sender` given to us during
    /// construction.
    ///
    /// [`Client`]: struct.Client.html
    /// [`Connection`]: struct.Connection.html
    async fn accept_new_connections(
        client: Arc<RwLock<Self>>,
        mut listener: TcpListener,
        num_clients: usize,
        wait_for_all_clients: bool,
    ) -> Result<(), LiquidError> {
        let accepted_type = { client.read().await.client_type.clone() };
        // Me + All the nodes i'm connected to
        let mut curr_clients = 1 + { client.read().await.directory.len() };
        loop {
            if wait_for_all_clients && num_clients == curr_clients {
                return Ok(());
            }
            // wait on connections from new clients
            let (socket, _) = listener.accept().await?;
            let (reader, writer) = io::split(socket);
            let mut stream =
                FramedRead::new(reader, MessageCodec::<ControlMsg>::new());
            let sink = FramedWrite::new(writer, MessageCodec::<RT>::new());
            // read the introduction message from the new client
            let intro = message::read_msg(&mut stream).await?;
            let (address, client_type) = if let ControlMsg::Introduction {
                address,
                client_type,
            } = intro.msg
            {
                (address, client_type)
            } else {
                // we should only receive `ControlMsg::Introduction` msgs here
                return Err(LiquidError::UnexpectedMessage);
            };

            if accepted_type != client_type {
                // we only want to connect with other clients that are the same
                // type as us
                return Err(LiquidError::UnexpectedMessage);
            }

            // increment the message id and check if there was an existing
            // connection
            {
                let mut unlocked = client.write().await;
                unlocked.msg_id =
                    increment_msg_id(unlocked.msg_id, intro.msg_id);
            }
            let is_existing_conn = {
                let unlocked = client.read().await;
                unlocked.directory.contains_key(&intro.sender_id)
            };

            if is_existing_conn {
                return existing_conn_err(stream, sink);
            }

            // Add the connection with the new client to this directory
            let conn = Connection {
                address: address.clone(),
                sink,
            };
            {
                client.write().await.directory.insert(intro.sender_id, conn);
            }
            // NOTE: Not unsafe because message codec has no fields and
            // can be converted to a different type without losing meaning
            let new_stream = unsafe {
                std::mem::transmute::<FramedStream<ControlMsg>, FramedStream<RT>>(
                    stream,
                )
            };
            // spawn a tokio task to handle new messages from the client
            // that we just connected to
            let new_sender = { client.read().await.sender.clone() };
            Client::recv_msg(new_sender, new_stream);
            info!(
                "Connected to id: {:#?} at address: {:#?}",
                intro.sender_id, address
            );
            curr_clients += 1;
        }
    }

    /// Connects a running [`Client`] with a [`Client`] running at the given
    /// `(id, IP:Port)`. After connecting, adds the [`Connection`] to the other
    /// [`Client`] to our directory. Finally, spawns a Tokio task to read
    /// further messages from the [`Client`] and forward them via the [`mpsc`]
    /// channel.
    ///
    /// [`Client`]: struct.Client.html
    /// [`Connection`]: struct.Connection.html
    /// [`mpsc`]: https://docs.rs/tokio/0.2.18/tokio/sync/mpsc/fn.channel.html
    #[allow(clippy::map_entry)] // clippy is being dumb
    pub(crate) async fn connect(
        &mut self,
        client: (usize, SocketAddr),
    ) -> Result<(), LiquidError> {
        // Connect to the given client
        let stream = TcpStream::connect(client.1.clone()).await?;
        let (reader, writer) = io::split(stream);
        let stream = FramedRead::new(reader, MessageCodec::<RT>::new());
        let mut sink =
            FramedWrite::new(writer, MessageCodec::<ControlMsg>::new());

        // Make the connection struct which holds the stream for sending msgs
        if self.directory.contains_key(&client.0) {
            existing_conn_err(stream, sink)
        } else {
            sink.send(Message::new(
                self.msg_id,
                self.id,
                0,
                ControlMsg::Introduction {
                    address: self.address.clone(),
                    client_type: self.client_type.clone(),
                },
            ))
            .await?;
            // NOTE: Not unsafe because message codec has no fields and
            // can be converted to a different type without losing meaning
            let sink = unsafe {
                std::mem::transmute::<FramedSink<ControlMsg>, FramedSink<RT>>(
                    sink,
                )
            };
            let conn = Connection {
                address: client.1.clone(),
                sink,
            };
            // Add the connection to our directory
            self.directory.insert(client.0, conn);
            // spawn a tokio task to handle new messages from the client
            // that we just connected to
            Client::recv_msg(self.sender.clone(), stream);
            // send the client our id and address so they can add us to
            // their directory
            self.msg_id += 1;
            info!(
                "Connected to id: {:#?} at address: {:#?}",
                client.0, client.1
            );

            Ok(())
        }
    }

    // TODO: abstract/merge with Server::send_msg, they are the same
    /// Send the given `message` to a [`Client`] with the given `target_id`.
    /// Id's are automatically assigned by a [`Server`] during the registration
    /// period based on the order of connections.
    ///
    /// [`Client`]: struct.Client.html
    /// [`Server`]: struct.Server.html
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
        message::send_msg(target_id, m, &mut self.directory).await?;
        debug!("sent a message with id, {}", self.msg_id);
        self.msg_id += 1;
        Ok(())
    }

    /// Broadcast the given `message` to all currently connected clients
    pub async fn broadcast(&mut self, message: RT) -> Result<(), LiquidError> {
        let d: Vec<usize> = self.directory.iter().map(|(k, _)| *k).collect();
        for k in d {
            self.send_msg(k, message.clone()).await?;
        }
        Ok(())
    }

    /// Spawns a `Tokio` task to read messages from the given `reader` and
    /// forward messages over the given `sender` so the message may be
    /// processed.
    fn recv_msg(mut sender: Sender<Message<RT>>, mut reader: FramedStream<RT>) {
        // TODO: need to properly increment message id but that means self
        // needs to be 'static or mutex'd
        tokio::spawn(async move {
            loop {
                let msg: Message<RT> =
                    match message::read_msg(&mut reader).await {
                        Ok(x) => x,
                        Err(_) => {
                            break;
                        }
                    };
                //        self.msg_id = increment_msg_id(self.msg_id, s.msg_id);
                let id = msg.msg_id;
                sender.send(msg).await.unwrap_or_else(|_| panic!());
                debug!(
                    "Got a msg with id: {} and added it to process queue",
                    id
                );
            }
        });
    }

    /// Spawns a `tokio` task that will handle receiving [`ControlMsg::Kill`]
    /// messages from the [`Server`]
    ///
    /// [`Server`]: struct.Server.html
    /// [`ControlMsg::Kill`]: enum.ControlMsg.html#variant.Kill
    fn recv_server_msg(
        mut reader: FramedStream<ControlMsg>,
        notifier: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            let kill_msg: Message<ControlMsg> =
                message::read_msg(&mut reader).await.unwrap();
            match &kill_msg.msg {
                ControlMsg::Kill => Ok(notifier.notify()),
                _ => Err(LiquidError::UnexpectedMessage),
            }
        });
    }
}
