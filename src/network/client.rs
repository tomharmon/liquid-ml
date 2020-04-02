//! Represents a client node in a distributed system, with implementations
//! provided for `LiquidML` use cases.
use crate::error::LiquidError;
use crate::network;
use crate::network::*;
use futures::SinkExt;
use log::info;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc::Sender, Notify, RwLock};
use tokio_util::codec::{FramedRead, FramedWrite};

// TODO: remove 'static
impl<
        RT: Send + Sync + DeserializeOwned + Serialize + std::fmt::Debug + 'static,
    > Client<RT>
{
    /// Create a new `Client` running on the given `my_addr` IP:Port address,
    /// which connects to a server running on the given `server_addr` IP:Port.
    ///
    /// Constructing the `Client` does these things:
    /// 1. Connects to the `Server`
    /// 2. Sends the server our `IP:Port` address
    /// 3. Server responds with a `ControlMsg::Directory` containing the
    ///    addresses of all other currently connected `Client`s
    /// 4. Connects to all other existing `Client`s which spawns a Tokio task
    ///    for each connection that will read messages from the connection
    ///    and handle it.
    ///
    /// Creating a new `Client` returns an `Arc<RwLock<Self>>` so that
    /// the layer above the `Client` can use it concurrently.
    pub async fn new(
        server_addr: &str,
        my_addr: &str,
        sender: Sender<Message<RT>>,
        kill_notifier: Arc<Notify>,
        num_clients: usize,
        wait_for_all_clients: bool,
    ) -> Result<Arc<RwLock<Self>>, LiquidError> {
        // Connect to the server
        let server_stream = TcpStream::connect(server_addr).await?;
        let (reader, writer) = io::split(server_stream);
        let mut stream = FramedRead::new(reader, MessageCodec::new());
        let mut sink = FramedWrite::new(writer, MessageCodec::new());
        let memory =
            System::new_with_specifics(RefreshKind::new().with_memory())
                .get_total_memory()
                / 2;
        // Tell the server our address and how much data (in `KiB`) other
        // Clients are allowed to send us at one time
        sink.send(Message::new(
            0,
            0,
            0,
            ControlMsg::Introduction {
                address: my_addr.to_string(),
                memory,
            },
        ))
        .await?;
        // The Server sends the addresses of all currently connected clients
        let dir = network::read_msg(&mut stream).await?;
        if let ControlMsg::Directory { dir: d } = dir.msg {
            let mut c = Client {
                id: dir.target_id,
                address: my_addr.to_string(),
                msg_id: dir.msg_id + 1,
                directory: HashMap::new(),
                _server: sink,
                sender,
                memory,
            };

            // Connect to all the clients
            for addr in d {
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
                    num_clients,
                    wait_for_all_clients,
                )
                .await?
            } else {
                tokio::spawn(async move {
                    Client::accept_new_connections(
                        concurrent_client_cloned,
                        num_clients,
                        wait_for_all_clients,
                    )
                    .await
                    .unwrap();
                });
            }
            Ok(concurrent_client)
        } else {
            Err(LiquidError::UnexpectedMessage)
        }
    }

    /// A blocking function that allows a `Client` to listen for connections
    /// from newly started `Client`s. When a new `Client` connects to this
    /// `Client`, we add the connection to this `Client.directory`
    /// and call `Client::recv_msg`
    async fn accept_new_connections(
        client: Arc<RwLock<Client<RT>>>,
        num_clients: usize,
        wait_for_all_clients: bool,
    ) -> Result<(), LiquidError> {
        let listen_address = { client.read().await.address.clone() };
        let mut listener = TcpListener::bind(listen_address).await?;
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
            let is_existing_conn = {
                client.read().await.directory.contains_key(&intro.sender_id)
            };
            if is_existing_conn {
                return existing_conn_err(stream, sink);
            } else {
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
                // NOTE: Not unsafe because message codec has no fields and
                // can be converted to a different type without losing meaning
                let new_stream = unsafe {
                    std::mem::transmute::<
                        FramedStream<ControlMsg>,
                        FramedStream<RT>,
                    >(stream)
                };
                // spawn a tokio task to handle new messages from the client
                // that we just connected to
                let new_sender = { client.read().await.sender.clone() };
                Client::recv_msg(new_sender, new_stream);
                info!(
                    "Connected to id: {:#?} at address: {:#?}",
                    intro.sender_id, addr
                );
                curr_clients += 1;
            }
        }
    }

    /// Connect to a running `Client` with the given `(id, IP:Port)`.
    /// After connecting, add the `Connection` to the other `Client` to this
    /// `Client.directory` for sending later messages to the `Client`. Finally,
    /// spawn a Tokio task to read further messages from the `Client` and
    /// handle the message.
    ///
    /// Note that as long as a `Server` is running, you *really should not* use
    /// this method unless you want to add `Client`s to the system that is
    /// only connected to certain other `Client`s and not the entire system.
    /// Calling `Client::new` will connect the new `Client` with
    /// all other currently existing `Client`s automatically.
    #[allow(clippy::map_entry)] // clippy is being dumb
    pub async fn connect(
        &mut self,
        client: (usize, String),
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
    /// Send the given `message` to a client with the given `target_id`.
    /// Id's are automatically assigned by a `Server` during the registration
    /// period when creating a new `Client` with `Client::new`.
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
        network::send_msg(target_id, m, &mut self.directory).await?;
        info!("sent a message with id, {}", self.msg_id);
        self.msg_id += 1;
        Ok(())
    }

    /// Spawns a Tokio task to read messages from the given `reader` and
    /// handle responding to them.
    fn recv_msg(mut sender: Sender<Message<RT>>, mut reader: FramedStream<RT>) {
        // TODO: need to properly increment message id but that means self
        // needs to be 'static or mutex'd and that propagates a lot...
        tokio::spawn(async move {
            loop {
                let msg: Message<RT> =
                    match network::read_msg(&mut reader).await {
                        Ok(x) => x,
                        Err(_) => {
                            break;
                        }
                    };
                //        self.msg_id = increment_msg_id(self.msg_id, s.msg_id);
                let id = msg.msg_id;
                sender.send(msg).await.unwrap();
                info!(
                    "Got a msg with id: {} and added it to process queue",
                    id
                );
            }
        });
    }

    fn recv_server_msg(
        mut reader: FramedStream<ControlMsg>,
        notifier: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            let kill_msg: Message<ControlMsg> =
                network::read_msg(&mut reader).await.unwrap();
            match &kill_msg.msg {
                ControlMsg::Kill => Ok(notifier.notify()),
                _ => Err(LiquidError::UnexpectedMessage),
            }
        });
    }
}
