//! Defines functionality for the `DataFrame`
use crate::dataframe::{
    DistributedDFMsg, DistributedDataFrame, LocalDataFrame, Row, Rower, Schema,
};
use crate::error::LiquidError;
use crate::kv::{KVStore, Key};
use crate::network::{Client, Message};
use bincode::{deserialize, serialize};
use bytecount;
use futures::future::try_join_all;
use log::{debug, info};
use serde::{de::DeserializeOwned, Serialize};
use sorer::dataframe::{Column, Data, SorTerator};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex, Notify, RwLock,
};

/// An interface for a `DataFrame`, inspired by those used in `pandas` and `R`.
impl DistributedDataFrame {
    /// TODO: update documentation
    /// Creates a new `DataFrame` from the given file
    pub async fn from_sor(
        server_addr: &str,
        my_ip: &str,
        file_name: &str,
        kv: Arc<RwLock<KVStore<LocalDataFrame>>>,
        df_name: &str,
        num_nodes: usize,
        kv_blob_receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Arc<Self>, LiquidError> {
        // Figure out what node we are supposed to be. We must synchronize
        // the creation of this DDF on this node based on the already assigned
        // id's
        let node_id = { kv.read().await.id };
        // initialize some other required fields of self so as not to duplicate
        // code in both branches
        let (blob_sender, blob_receiver) = mpsc::channel(2);
        // used for internal messaging processing so that the asynnchronous
        // messaging task can notify other tasks when `self.row` is ready
        let internal_notifier = Arc::new(Notify::new());
        // for this DDF's network client to forward messages to this DDF
        // for processing
        let (sender, mut receiver) = mpsc::channel(64);
        // so that our network client can notify us when they get a Kill
        // signal
        let kill_notifier = Arc::new(Notify::new());
        let df_client_type = format!("ddf-{}", df_name);

        // For this constructor, we assume the file is only on node 1
        if node_id == 1 {
            // connect our client right away since we want to be node 1
            let network = Client::new(
                server_addr,
                my_ip,
                None,
                sender,
                kill_notifier.clone(),
                num_nodes,
                false,
                df_client_type.as_str(),
            )
            .await?;
            assert_eq!(1, { network.read().await.id });
            // Send a ready message to node 2 so that all the other nodes
            // start connecting to the Server in the correct order
            let ready_blob = serialize(&DistributedDFMsg::Ready)?;
            {
                kv.read().await.send_blob(node_id + 1, ready_blob).await?
            };

            // TODO: Panics if file doesn't exist
            let total_newlines = count_new_lines(file_name);
            let max_rows_per_node = total_newlines / num_nodes;
            let schema = sorer::schema::infer_schema(file_name);
            info!(
                "Total newlines: {} max rows per node: {}",
                total_newlines, max_rows_per_node
            );
            info!("Inferred schema: {:?}", schema.clone());
            // make a chunking iterator for the sor file
            let sor_terator =
                SorTerator::new(file_name, schema.clone(), max_rows_per_node);

            // Distribute the chunked sor file round-robin style
            let mut df_chunk_map = HashMap::new();
            let mut cur_num_rows = 0;
            {
                let unlocked = kv.read().await;
                let mut futs = Vec::new();
                // in each iteration, create a future sends a chunk to a node
                for (chunk_idx, chunk) in sor_terator.into_iter().enumerate() {
                    let ldf = LocalDataFrame::from(chunk);
                    let key =
                        Key::generate(df_name, (chunk_idx % num_nodes) + 1);
                    // add this chunk range and key to our <range, key> map
                    df_chunk_map.insert(
                        Range {
                            start: cur_num_rows,
                            end: cur_num_rows + ldf.n_rows(),
                        },
                        key.clone(),
                    );
                    cur_num_rows += ldf.n_rows();

                    // add the future we make to a vec for multiplexing
                    futs.push(unlocked.put(key.clone(), ldf));

                    // TODO: might need to do some tuning on when to join the
                    // futures here, possibly even dynamically figure out some
                    // value to smooth over the tradeoff between memory and
                    // speed (right now i assume it uses a lot of memory)
                    if chunk_idx % num_nodes == 0 {
                        // send all chunks concurrently once we have num_node
                        // new chunks to send
                        try_join_all(futs).await?;
                        futs = Vec::new();
                        info!(
                            "sent {} chunks successfully, total of {} chunks",
                            num_nodes, chunk_idx
                        );
                    }
                }
                // we are almost done distributing chunks
                if !futs.is_empty() {
                    // if the number of chunks is not evenly divisible by the
                    // number of nodes, we will have a few more chunks to send
                    try_join_all(futs).await?;
                }
                info!(
                    "Finished distributing {} SoR chunks",
                    cur_num_rows / max_rows_per_node
                );
            }
            // We are done distributing chunks, now we want to make sure all
            // ddfs are connected on the network, so we wait for a `Ready`
            // message before sending out the `Initialization` message
            let ready_blob =
                { kv_blob_receiver.lock().await.recv().await.unwrap() };
            let ready_msg = deserialize(&ready_blob)?;
            match ready_msg {
                DistributedDFMsg::Ready => (),
                _ => return Err(LiquidError::UnexpectedMessage),
            }
            debug!("Node 1 got the final ready message");

            // Create an Initialization message that holds all the information
            // related to this DistributedDataFrame, the Schema and the map
            // of the range of indices that each chunk holds and the `Key`
            // associated with that chunk
            let schema = Schema::from(schema);
            let intro_msg = DistributedDFMsg::Initialization {
                schema: schema.clone(),
                df_chunk_map: df_chunk_map.clone(),
            };

            // Broadcast the initialization message to all nodes
            {
                network.write().await.broadcast(intro_msg).await?
            };
            debug!("Node 1 sent the initialization message to all nodes");

            let row = Arc::new(RwLock::new(Row::new(&schema)));

            let ddf = Arc::new(DistributedDataFrame {
                schema,
                df_name: df_name.to_string(),
                df_chunk_map,
                num_rows: cur_num_rows,
                network,
                node_id,
                num_nodes,
                kv,
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        } else {
            // Connect to the network in the correct order. First,
            // wait for a `Ready` message as a blob over the KV blob
            // receiver, sent to us by the previous node
            let ready_blob =
                { kv_blob_receiver.lock().await.recv().await.unwrap() };
            let ready_msg = deserialize(&ready_blob)?;
            match ready_msg {
                DistributedDFMsg::Ready => (),
                _ => return Err(LiquidError::UnexpectedMessage),
            }
            debug!("Received a ready message");

            // The node before us has joined the network, it is now time
            // to connect
            let network = Client::new(
                server_addr,
                my_ip,
                None,
                sender,
                kill_notifier.clone(),
                num_nodes,
                false,
                df_client_type.as_str(),
            )
            .await?;
            // assert that we joined in the right order (kv node id must
            // match client node id)
            assert_eq!(node_id, { network.read().await.id });

            if node_id < num_nodes {
                // All nodes except the last node must send a ready message
                // to the node that comes after them to let the next node know
                // they may join the network
                kv.read().await.send_blob(node_id + 1, ready_blob).await?;
            } else {
                // if we are the last node we must tell the first node that
                // all the other nodes are ready
                kv.read().await.send_blob(1, ready_blob).await?;
            }

            // Node 1 will send the initialization message to our network
            // directly, not using the KV. The Client will forward the message
            // to us via the mpsc receiver
            let init_msg = receiver.recv().await.unwrap();
            // We got a message, check it was the initialization message
            let (schema, df_chunk_map) = match init_msg.msg {
                DistributedDFMsg::Initialization {
                    schema,
                    df_chunk_map,
                } => (schema, df_chunk_map),
                _ => return Err(LiquidError::UnexpectedMessage),
            };
            debug!("Got the Initialization message from Node 1");

            let row = Arc::new(RwLock::new(Row::new(&schema)));
            let num_rows = df_chunk_map.iter().fold(0, |mut acc, (k, _)| {
                if acc > k.end {
                    acc
                } else {
                    acc = k.end;
                    acc
                }
            });

            let ddf = Arc::new(DistributedDataFrame {
                schema,
                df_name: df_name.to_string(),
                df_chunk_map,
                num_rows,
                network,
                node_id,
                num_nodes,
                kv,
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        }
    }

    pub async fn new(
        _data: Option<Vec<Column>>,
        _kv: Arc<RwLock<KVStore<LocalDataFrame>>>,
        _name: &str,
        _num_nodes: usize,
        _receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Arc<Self>, LiquidError> {
        unimplemented!();
        /*
            let node_id = { kv.read().await.id };
            if node_id == 1 {
                let mut data = data.unwrap();
                let mut rows_to_process = n_rows(&data);
                let mut chunk_idx = 0;
                let mut keys = Vec::new();
                let mut schema = Schema::new();
                let rows_per_node = data.len() / num_nodes;
                let mut send_chunk_futs = Vec::new();
                while rows_to_process > 0 {
                    let mut chunked_data = Vec::new();
                    for col in data {
                        // will panic if rows_per_node is greater than i.len()
                        let new_col = match col {
                            Column::Int(i) => {
                                Column::Int(i.drain(0..rows_per_node).collect())
                            }
                            Column::Bool(i) => {
                                Column::Bool(i.drain(0..rows_per_node).collect())
                            }
                            Column::Float(i) => {
                                Column::Float(i.drain(0..rows_per_node).collect())
                            }
                            Column::String(i) => {
                                Column::String(i.drain(0..rows_per_node).collect())
                            }
                        };
                        chunked_data.push(new_col);
                    }
                    let ldf = LocalDataFrame::from(chunked_data);
                    let key = Key::generate(name, (chunk_idx % num_nodes) + 1);

                    let unlocked = kv.read().await;
                    send_chunk_futs.push(unlocked.put(key.clone(), ldf));
                    keys.push(key);

                    if chunk_idx % num_nodes == 0 {
                        // send all chunks concurrently
                        try_join_all(send_chunk_futs).await?;
                        send_chunk_futs = Vec::new();
                        println!("sent {} chunks", chunk_idx);
                    }

                    chunk_idx += 1;
                    rows_to_process = n_rows(&data);
                }

                if !send_chunk_futs.is_empty() {
                    // maybe its not evenly divisible
                    try_join_all(send_chunk_futs).await?;
                }

                let build = serialize(&(keys.clone(), schema.clone()))?;
                for i in 2..(num_nodes + 1) {
                    kv.read().await.send_blob(i, build.clone()).await?;
                }

                Ok(DistributedDataFrame {
                    schema,
                    blob_receiver,
                    data: keys,
                    kv,
                    num_nodes,
                    node_id,
                })
            } else {
                let (data, schema) =
                    { deserialize(&receiver.lock().await.recv().await.unwrap())? };
                Ok(DistributedDataFrame {
                    data,
                    schema,
                    kv,
                    num_nodes,
                    node_id,
                    receiver,
                })
            }
        */
    }
    /// Obtains a reference to this `DataFrame`s schema.
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the `Data` at the given `col_idx`, `row_idx` offsets.
    pub async fn get(
        &self,
        col_idx: usize,
        row_idx: usize,
    ) -> Result<Data, LiquidError> {
        let r = self.get_row(row_idx).await?;
        Ok(r.get(col_idx)?.clone())
    }

    /// Returns a clone of the row at the requested index
    pub async fn get_row(&self, index: usize) -> Result<Row, LiquidError> {
        match self.df_chunk_map.iter().find(|(k, _)| k.contains(&index)) {
            Some((range, key)) => {
                // key is either owned by us or another node
                if key.home == self.node_id {
                    // we own it
                    let our_local_df =
                        { self.kv.read().await.get(&key).await? };
                    let mut r = Row::new(self.get_schema());
                    our_local_df.fill_row(index - range.start, &mut r)?;
                    Ok(r)
                } else {
                    // owned by another node, must request over the network
                    let get_msg = DistributedDFMsg::GetRow(index);
                    {
                        self.network
                            .write()
                            .await
                            .send_msg(key.home, get_msg)
                            .await?;
                    }
                    // wait here until we are notified the row is set by our
                    // message processing task
                    self.internal_notifier.notified().await;
                    // self.row is now set
                    Ok({ self.row.read().await.clone() })
                }
            }
            None => Err(LiquidError::RowIndexOutOfBounds),
        }
    }

    /// Get the index of the `Column` with the given `col_name`. Returns `Some`
    /// if a `Column` with the given name exists, or `None` otherwise.
    pub fn get_col_idx(&self, col_name: &str) -> Option<usize> {
        self.schema.col_idx(col_name)
    }

    /// Perform a distributed map operation on the `DataFrame` associated with
    /// the `df_name` with the given `rower`. Returns `Some(rower)` (of the
    /// joined results) if the `node_id` of this `Application` is `1`, and
    /// `None` otherwise.
    ///
    /// A local `pmap` is used on each node to map over that nodes' chunk.
    /// By default, each node will use the number of threads available on that
    /// machine.
    ///
    ///
    /// NOTE: takes `&mut self` only because of the blob_receiver
    ///
    /// NOTE:
    /// There is an important design decision that comes with a distinct trade
    /// off here. The trade off is:
    /// 1. Join the last node with the next one until you get to the end. This
    ///    has reduced memory requirements but a performance impact because
    ///    of the synchronous network calls
    /// 2. Join all nodes with one node by sending network messages
    ///    concurrently to the final node. This has increased memory
    ///    requirements and greater complexity but greater performance because
    ///    all nodes can asynchronously send to one node at the same time.
    ///
    /// This implementation went with option 1 for simplicity reasons
    pub async fn map<T: Rower + Clone + Send + Serialize + DeserializeOwned>(
        &self,
        mut rower: T,
    ) -> Result<Option<T>, LiquidError> {
        // get the keys for our locally owned chunks
        let my_keys: Vec<&Key> = self
            .df_chunk_map
            .iter()
            .filter(|(_, key)| key.home == self.node_id)
            .map(|(_, v)| v)
            .collect();
        // map over our chunks
        {
            let unlocked_kv = self.kv.read().await;
            for key in my_keys {
                let ldf = unlocked_kv.wait_and_get(key).await?;
                rower = ldf.pmap(rower);
            }
        }
        debug!("finished mapping local chunk(s)");
        if self.node_id == self.num_nodes {
            // we are the last node
            self.send_blob(self.node_id - 1, &rower).await?;
            debug!("Last node sent its results");
            Ok(None)
        } else {
            let blob =
                { self.blob_receiver.lock().await.recv().await.unwrap() };
            let external_rower: T = deserialize(&blob[..])?;
            rower = rower.join(external_rower);
            debug!("Received a resulting rower and joined it with local rower");
            if self.node_id != 1 {
                self.send_blob(self.node_id - 1, &rower).await?;
                debug!("Forwarded the combined rower");
                Ok(None)
            } else {
                debug!("Final node completed map");
                Ok(Some(rower))
            }
        }
    }

    /// TODO: this documentation is copy+pasted documentation from `map`, update it
    ///
    /// Perform a distributed map operation on the `DataFrame` associated with
    /// the `df_name` with the given `rower`. Returns `Some(rower)` (of the
    /// joined results) if the `node_id` of this `Application` is `1`, and
    /// `None` otherwise.
    ///
    /// A local `pmap` is used on each node to map over that nodes' chunk.
    /// By default, each node will use the number of threads available on that
    /// machine.
    ///
    /// NOTE:
    /// There is an important design decision that comes with a distinct trade
    /// off here. The trade off is:
    /// 1. Join the last node with the next one until you get to the end. This
    ///    has reduced memory requirements but a performance impact because
    ///    of the synchronous network calls
    /// 2. Join all nodes with one node by sending network messages
    ///    concurrently to the final node. This has increased memory
    ///    requirements and greater complexity but greater performance because
    ///    all nodes can asynchronously send to one node at the same time.
    ///
    /// This implementation went with option 1 for simplicity reasons
    pub async fn filter<
        T: Rower + Clone + Send + Serialize + DeserializeOwned,
    >(
        &self,
        _rower: T,
    ) -> Result<Self, LiquidError> {
        let _my_key: &Key = self
            .df_chunk_map
            .iter()
            .find(|(_, v)| v.home == self.node_id)
            .map(|(_, v)| v)
            .unwrap();

        // TODO: doesn't handle cases where the new_ldf is empty
        unimplemented!();
        /*if self.node_id == 1 {
            // we are the first node
            {
                // add our filtered local df to our kv
                self.kv.lock().await.put(key, new_ldf).await?
            };
            // wait for messages from other nodes telling us what their
            // ranges are
            let mut num_recvd_msgs = 0;
            while num_recvd_msgs < self.num_nodes - 1 {
                let filter_msg = self
                num_recvd_msgs += 1;
            }
            // respond back to all nodes with the Initialization message
            Ok(None)
        } else {
            // we are not the first node and we should send our range
            // then wait for the initialization response
        }*/
    }

    /// Return the (total) number of rows across all nodes for this
    /// `DistributedDataFrame`
    pub fn n_rows(&self) -> usize {
        self.num_rows
    }

    /// Return the number of columns in this `DataFrame`.
    pub fn n_cols(&self) -> usize {
        self.schema.width()
    }

    /// Sends the given `blob` to the `DistributedDataFrame` with the given
    /// `target_id` This provides a lower level interface to facilitate other
    /// kinds of messages, such as sending deserialized Rowers
    async fn send_blob<T: Serialize>(
        &self,
        target_id: usize,
        blob: &T,
    ) -> Result<(), LiquidError> {
        let blob = serialize(blob)?;
        if target_id == self.node_id {
            Err(LiquidError::DumbUserError)
        } else {
            self.network
                .write()
                .await
                .send_msg(target_id, DistributedDFMsg::Blob(blob))
                .await
        }
    }

    async fn process_messages(
        ddf: Arc<DistributedDataFrame>,
        mut receiver: Receiver<Message<DistributedDFMsg>>,
        blob_sender: Sender<Vec<u8>>,
    ) -> Result<(), LiquidError> {
        let ddf2 = ddf.clone();
        tokio::spawn(async move {
            loop {
                let msg = receiver.recv().await.unwrap();
                let mut blob_sender_clone = blob_sender.clone();
                let ddf3 = ddf2.clone();
                tokio::spawn(async move {
                    info!("Processing a message with id: {:#?}", msg.msg_id);
                    match msg.msg {
                        DistributedDFMsg::GetRow(row_idx) => {
                            let r = ddf3.get_row(row_idx).await.unwrap();
                            {
                                ddf3.network
                                    .write()
                                    .await
                                    .send_msg(
                                        msg.sender_id,
                                        DistributedDFMsg::Row(r),
                                    )
                                    .await
                                    .unwrap();
                            }
                        },
                        DistributedDFMsg::Row(row) => {
                            {
                                *ddf3.row.write().await = row;
                            }
                            ddf3.internal_notifier.notify();
                        },
                        DistributedDFMsg::Blob(blob) => {
                            blob_sender_clone.send(blob).await.unwrap();
                        },
                        DistributedDFMsg::FilteredSize(_size) => unimplemented!(),
                        _ => panic!("Should happen before message process loop is started"),
                    }
                });
            }
        });
        Ok(())
    }
}

fn count_new_lines(file_name: &str) -> usize {
    let mut buf_reader = BufReader::new(File::open(file_name).unwrap());
    let mut new_lines = 0;

    loop {
        let bytes_read = buf_reader.fill_buf().unwrap();
        let len = bytes_read.len();
        if len == 0 {
            return new_lines;
        };
        new_lines += bytecount::count(bytes_read, b'\n');
        buf_reader.consume(len);
    }
}
