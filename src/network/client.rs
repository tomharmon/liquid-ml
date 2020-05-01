//! Represents a client node in a distributed system, with implementations
//! provided for `LiquidML` use cases.
use crate::error::LiquidError;
use crate::network::{
    existing_conn_err, increment_msg_id, message, Connection, ControlMsg,
    FramedSink, FramedStream, Message, MessageCodec,
};
use futures::{
    stream::{self, SelectAll},
    SinkExt,
};
use log::{debug, info};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};
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
    /// The number of `Client`s in the network
    pub num_nodes: usize,
    /// The `address` of this `Client`
    pub address: SocketAddr,
    /// The id of the current message
    pub(crate) msg_id: usize,
    /// A directory which is a map of client id to the [`Connection`] with that
    /// `Client`
    ///
    /// [`Connection`]: struct.Connection.html
    pub(crate) directory: HashMap<usize, Connection<T>>,
    /// The connection to the [`Server`](struct.Server.html)
    server: Connection<ControlMsg>,
    /// The name of the network this `Client` will connect to. This is so that,
    /// for example, two different communication networks of
    /// `Client<DistributedDFMsg>` can be created so that separate
    /// `DistributedDataFrame`s only talk to themselves.
    network_name: String,
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
    ///    spawning a Tokio task for each connection.
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
    /// - `num_nodes`: The number of nodes in the network.
    /// - `network_name`: The name of the network to connect with, will only
    ///                   connect with other `Client`s with the same
    ///                   `network_name`
    ///
    /// [`Client`]: struct.Client.html
    /// [`Server`]: struct.Server.html
    /// [`ControlMsg::Directory`]: enum.ControlMsg.html#variant.Directory
    /// [`ControlMsg::Introduction`]: enum.ControlMsg.html#variant.Introduction
    /// [`ControlMsg::Kill`]: enum.ControlMsg.html#variant.Kill
    pub async fn new(
        server_addr: String,
        my_ip: String,
        my_port: Option<String>,
        num_nodes: usize,
        network_name: String,
    ) -> Result<
        (Arc<Mutex<Self>>, SelectAll<FramedStream<RT>>, Arc<Notify>),
        LiquidError,
    > {
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
        let mut server = Connection {
            address: server_address,
            sink,
        };
        // Tell the server our address and type
        server
            .sink
            .send(Message::new(
                0,
                0,
                0,
                ControlMsg::Introduction {
                    address: my_address.clone(),
                    network_name: network_name.to_string(),
                },
            ))
            .await?;
        // Server responds with the addresses of all currently connected clients
        let dir_msg = message::read_msg(&mut stream).await?;
        let dir = if let ControlMsg::Directory { dir } = dir_msg.msg {
            dir
        } else {
            return Err(LiquidError::UnexpectedMessage);
        };

        info!(
            "Client in network {} got id {} running at address {}",
            network_name, dir_msg.target_id, &my_address
        );

        // initialize `self`
        let mut c = Client {
            id: dir_msg.target_id,
            address: my_address.clone(),
            msg_id: dir_msg.msg_id + 1,
            directory: HashMap::new(),
            num_nodes,
            server,
            network_name: network_name.to_string(),
        };

        // Connect to all the currently existing clients
        let mut existing_conns = vec![];
        // note this is done serially and could be done concurrently, but
        // it doesn't make a difference since there will only ever be a
        // (relatively) small number of nodes
        for (id, addr) in dir.into_iter() {
            existing_conns.push(c.connect(id, addr).await?);
        }

        // Listen for further messages from the Server, e.g. `Kill` messages
        let kill_notifier = Arc::new(Notify::new());
        Client::<ControlMsg>::recv_server_msg(stream, kill_notifier.clone());
        // block until all the other clients start up and connect to us
        let new_conns =
            Client::accept_new_connections(&mut c, listener, num_nodes).await?;
        let read_streams = stream::select_all(
            existing_conns.into_iter().chain(new_conns.into_iter()),
        );

        let concurrent_client = Arc::new(Mutex::new(c));
        Ok((concurrent_client, read_streams, kill_notifier))
    }

    /// Given an already connected `Client` of any type, register a new network
    /// with the given `network_name` that will create a new network of
    /// `Client`s that connect in the same order as the `parent`. The new
    /// `Client` can only talk to `Client`s with the same `network_name` as
    /// the new network is independent of the `parent`.
    pub async fn register_network<
        T: Send + Sync + DeserializeOwned + Serialize + Clone + 'static,
    >(
        parent: Arc<Mutex<Self>>,
        network_name: String,
    ) -> Result<
        (
            Arc<Mutex<Client<T>>>,
            SelectAll<FramedStream<T>>,
            Arc<Notify>,
        ),
        LiquidError,
    > {
        let (server_addr, my_ip, node_id, listen_addr, num_nodes) = {
            let unlocked = parent.lock().await;
            let node_id = unlocked.id;
            let server_addr = unlocked.server.address.to_string().clone();
            let my_ip = unlocked.address.ip().to_string();
            let num_nodes = unlocked.num_nodes;
            (server_addr, my_ip, node_id, unlocked.address, num_nodes)
        };
        if node_id == 1 {
            // connect our client right away since we want to be node 1
            let jh = tokio::spawn(async move {
                Client::<T>::new(
                    server_addr,
                    my_ip,
                    None,
                    num_nodes,
                    network_name,
                )
                .await
            });
            // Send a ready message to node 2 so that all the other nodes
            // start connecting to the Server in the correct order
            let node_2_addr = {
                let unlocked = parent.lock().await;
                unlocked.directory.get(&2).unwrap().address
            };
            let socket = TcpStream::connect(node_2_addr).await?;
            let (_, writer) = io::split(socket);
            let mut sink =
                FramedWrite::new(writer, MessageCodec::<ControlMsg>::new());
            let msg = Message::new(0, node_id, 2, ControlMsg::Ready);
            sink.send(msg).await?;
            // TODO: await join handle
            let (network, read_streams, kill_notifier) = jh.await.unwrap()?;
            assert_eq!(1, { network.lock().await.id });
            // return the newly registered network
            Ok((network, read_streams, kill_notifier))
        } else {
            // wait to receive a `Ready` message from the node before us
            // TODO: this will panic if `wait_for_all_clients` was false for
            // the `parent` passed in
            let mut listener = TcpListener::bind(listen_addr).await?;
            let (socket, _) = listener.accept().await?;
            let (reader, writer) = io::split(socket);
            let mut stream =
                FramedRead::new(reader, MessageCodec::<ControlMsg>::new());
            let mut sink =
                FramedWrite::new(writer, MessageCodec::<ControlMsg>::new());
            // wait for the ready message
            let msg = message::read_msg(&mut stream).await?;
            //assert_eq!(msg.sender_id, node_id);
            match msg.msg {
                ControlMsg::Ready => (),
                _ => return Err(LiquidError::UnexpectedMessage),
            };
            // The node before us has joined the network, it is now time
            // to connect
            // TODO: spawn tokio task, store join handle
            let client_join_handle = tokio::spawn(async move {
                Client::<T>::new(
                    server_addr,
                    my_ip,
                    None,
                    num_nodes,
                    network_name,
                )
                .await
            });

            // tell the next node we are ready
            if node_id < num_nodes {
                // There is another node after us
                let msg = Message::new(0, node_id, node_id, ControlMsg::Ready);
                sink.send(msg).await?;
                let next_node_addr = {
                    let unlocked = parent.lock().await;
                    unlocked.directory.get(&(node_id + 1)).unwrap().address
                };
                let next_node_socket =
                    TcpStream::connect(next_node_addr).await?;
                let (_, next_node_writer) = io::split(next_node_socket);
                let mut next_node_sink = FramedWrite::new(
                    next_node_writer,
                    MessageCodec::<ControlMsg>::new(),
                );
                let ready_msg =
                    Message::new(0, node_id, node_id + 1, ControlMsg::Ready);
                next_node_sink.send(ready_msg).await?;
            }
            // TODO: await join handle
            let (network, read_streams, kill_notifier) =
                client_join_handle.await.unwrap()?;
            // assert that we joined in the right order (kv node id must
            // match client node id)
            assert_eq!(node_id, { network.lock().await.id });

            // return the newly registered network
            Ok((network, read_streams, kill_notifier))
        }
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
        &mut self,
        mut listener: TcpListener,
        num_clients: usize,
    ) -> Result<Vec<FramedStream<RT>>, LiquidError> {
        let accepted_type = self.network_name.clone();
        let mut curr_clients = self.directory.len() + 1;
        let mut streams = vec![];
        loop {
            if num_clients == curr_clients {
                return Ok(streams);
            }
            // wait on connections from new clients
            let (socket, _) = listener.accept().await?;
            let (reader, writer) = io::split(socket);
            let mut stream =
                FramedRead::new(reader, MessageCodec::<ControlMsg>::new());
            let sink = FramedWrite::new(writer, MessageCodec::<RT>::new());
            // read the introduction message from the new client
            let intro = message::read_msg(&mut stream).await?;
            let (address, network_name) = if let ControlMsg::Introduction {
                address,
                network_name,
            } = intro.msg
            {
                (address, network_name)
            } else {
                // we should only receive `ControlMsg::Introduction` msgs here
                return Err(LiquidError::UnexpectedMessage);
            };

            if accepted_type != network_name {
                // we only want to connect with other clients that are the same
                // type as us
                return Err(LiquidError::UnexpectedMessage);
            }

            // increment the message id and check if there was an existing
            // connection
            self.msg_id = increment_msg_id(self.msg_id, intro.msg_id);
            let is_existing_conn =
                self.directory.contains_key(&intro.sender_id);

            if is_existing_conn {
                return Err(existing_conn_err(stream, sink));
            }

            // Add the connection with the new client to this directory
            let conn = Connection {
                address: address.clone(),
                sink,
            };
            self.directory.insert(intro.sender_id, conn);
            // NOTE: Not unsafe because message codec has no fields and
            // can be converted to a different type without losing meaning
            let new_stream = unsafe {
                std::mem::transmute::<FramedStream<ControlMsg>, FramedStream<RT>>(
                    stream,
                )
            };
            streams.push(new_stream);
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
    async fn connect(
        &mut self,
        client_id: usize,
        client_addr: SocketAddr,
    ) -> Result<FramedStream<RT>, LiquidError> {
        // Connect to the given client
        let stream = TcpStream::connect(&client_addr).await?;
        let (reader, writer) = io::split(stream);
        let stream = FramedRead::new(reader, MessageCodec::<RT>::new());
        let mut sink =
            FramedWrite::new(writer, MessageCodec::<ControlMsg>::new());

        // Make the connection struct which holds the sink for sending msgs
        if self.directory.contains_key(&client_id) {
            Err(existing_conn_err(stream, sink))
        } else {
            sink.send(Message::new(
                self.msg_id,
                self.id,
                0,
                ControlMsg::Introduction {
                    address: self.address.clone(),
                    network_name: self.network_name.clone(),
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
                address: client_addr.clone(),
                sink,
            };
            info!(
                "Connected to id: {:#?} at address: {:#?}",
                client_id, client_addr
            );
            // Add the connection to our directory
            self.directory.insert(client_id, conn);
            // send the client our id and address so they can add us to
            // their directory
            self.msg_id += 1;

            Ok(stream)
        }
    }

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
        let m = Message::new(self.msg_id, self.id, target_id, message);
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
