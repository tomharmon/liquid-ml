//! Defines functionality for the `DataFrame`
use crate::dataframe::{
    DistributedDFMsg, DistributedDataFrame, LocalDataFrame, Row, Rower, Schema,
};
use crate::error::LiquidError;
use crate::kv::{KVStore, Key};
use crate::network::{Client, Message};
use bincode::{deserialize, serialize};
use bytecount;
//use futures::future::try_join_all;
use log::{debug, info};
use rand::{self, Rng};
use serde::{de::DeserializeOwned, Serialize};
use sorer::dataframe::{Column, Data, SorTerator};
use std::cmp;
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
    // TODO: we should await some of the futures for sending chunks every so
    // often so that we don't end up parsing the whole file into memory
    // since parsing is faster than sending over the network
    /// TODO: update documentation
    /// Creates a new `DataFrame` from the given file
    pub async fn from_sor(
        server_addr: &str,
        my_ip: &str,
        file_name: &str,
        kv: Arc<KVStore<LocalDataFrame>>,
        df_name: &str,
        num_nodes: usize,
        kv_blob_receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Arc<Self>, LiquidError> {
        // Figure out what node we are supposed to be. We must synchronize
        // the creation of this DDF on this node based on the already assigned
        // id's
        let node_id = kv.id;
        // initialize some other required fields of self so as not to duplicate
        // code in if branches
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
        // for processing results of distributed filtering
        let (filter_results_sender, filter_results) = mpsc::channel(num_nodes);
        let filter_results = Mutex::new(filter_results);

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
            kv.send_blob(node_id + 1, ready_blob).await?;

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

                    // TODO: might need to do some tuning on when to join the
                    // futures here, possibly even dynamically figure out some
                    // value to smooth over the tradeoff between memory and
                    // speed (right now i assume it uses a lot of memory)
                    // add the future we make to a vec for multiplexing
                    let cloned = kv.clone();
                    tokio::spawn(async move {
                        cloned.put(key.clone(), ldf).await.unwrap();
                    });
                }
                // we are almost done distributing chunks
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
                server_addr: server_addr.to_string(),
                my_ip: my_ip.to_string(),
                kv,
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
                filter_results,
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                    filter_results_sender,
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
                kv.send_blob(node_id + 1, ready_blob).await?;
            } else {
                // if we are the last node we must tell the first node that
                // all the other nodes are ready
                kv.send_blob(1, ready_blob).await?;
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
                server_addr: server_addr.to_string(),
                my_ip: my_ip.to_string(),
                kv,
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
                filter_results,
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                    filter_results_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        }
    }

    // TODO: add some verification that the `data` is not jagged (all cols equal len)
    // TODO: change multiplexing of sending chunks to send chunks more often

    pub async fn new(
        server_addr: &str,
        my_ip: &str,
        data: Option<Vec<Column>>,
        kv: Arc<KVStore<LocalDataFrame>>,
        df_name: &str,
        num_nodes: usize,
        kv_blob_receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Arc<Self>, LiquidError> {
        // Figure out what node we are supposed to be. We must synchronize
        // the creation of this DDF on this node based on the already assigned
        // id's
        let node_id = kv.id;
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
        // for processing results of distributed filtering
        let (filter_results_sender, filter_results) = mpsc::channel(num_nodes);
        let filter_results = Mutex::new(filter_results);

        // `data` must be `Some` on node 1
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
            kv.send_blob(node_id + 1, ready_blob).await?;

            let mut data = data.unwrap();
            let num_rows = n_rows(&data);
            let mut rows_to_process = num_rows.clone();
            let mut rows_processed = 0;
            let mut df_chunk_map = HashMap::new();
            let schema = Schema::from(&data);
            let rows_per_chunk = num_rows / num_nodes;

            info!(
                "Total rows: {} rows per chunk: {}",
                num_rows, rows_per_chunk
            );

            // Distribute the chunked data round-robin style
            let mut chunk_idx = 0;
            while rows_processed < num_rows {
                let mut chunked_data = Vec::with_capacity(rows_per_chunk);
                let cur_chunk_size = cmp::min(rows_per_chunk, rows_to_process);
                for col in &mut data {
                    // will panic if rows_per_node is greater than i.len()
                    let new_col = match col {
                        Column::Int(i) => {
                            Column::Int(i.drain(0..cur_chunk_size).collect())
                        }
                        Column::Bool(i) => {
                            Column::Bool(i.drain(0..cur_chunk_size).collect())
                        }
                        Column::Float(i) => {
                            Column::Float(i.drain(0..cur_chunk_size).collect())
                        }
                        Column::String(i) => {
                            Column::String(i.drain(0..cur_chunk_size).collect())
                        }
                    };
                    chunked_data.push(new_col);
                }
                let ldf = LocalDataFrame::from(chunked_data);
                let key = Key::generate(df_name, (chunk_idx % num_nodes) + 1);
                // add this chunk range and key to our <range, key> map
                df_chunk_map.insert(
                    Range {
                        start: rows_processed,
                        end: rows_processed + ldf.n_rows(),
                    },
                    key.clone(),
                );

                chunk_idx += 1;
                rows_processed += ldf.n_rows();
                rows_to_process -= ldf.n_rows();

                // add the fututre to a vec for later multiplexing
                let cloned = kv.clone();
                tokio::spawn(async move {
                    cloned.put(key.clone(), ldf).await.unwrap();
                });
            }
            info!("Finished distributing {} SoR chunks", chunk_idx);

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
                num_rows,
                network,
                node_id,
                num_nodes,
                server_addr: server_addr.to_string(),
                my_ip: my_ip.to_string(),
                kv,
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
                filter_results,
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                    filter_results_sender,
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
                kv.send_blob(node_id + 1, ready_blob).await?;
            } else {
                // if we are the last node we must tell the first node that
                // all the other nodes are ready
                kv.send_blob(1, ready_blob).await?;
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
                server_addr: server_addr.to_string(),
                my_ip: my_ip.to_string(),
                kv,
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
                filter_results,
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                    filter_results_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        }
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
                    let our_local_df = self.kv.get(&key).await?;
                    let mut r = Row::new(self.get_schema());
                    // TODO: is this index for fill_row correct?
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
        for key in my_keys {
            // TODO: should not really need wait_and_get here since we own that chunk?
            let ldf = self.kv.wait_and_get(key).await?;
            rower = ldf.pmap(rower);
        }
        debug!("Finished mapping local chunk(s)");
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
    ///
    /// NOTE: sadly must take the kv_blob_receiver for now in order to start
    ///       up new nodes for the new DDF
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
        mut rower: T,
        kv_blob_receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Arc<Self>, LiquidError> {
        // get the keys for our locally owned chunks
        let my_keys: Vec<&Key> = self
            .df_chunk_map
            .iter()
            .filter(|(_, key)| key.home == self.node_id)
            .map(|(_, v)| v)
            .collect();
        // NOTE: combines all chunks into one final chunk, may want to change
        // to stay 1-1
        // filter over our locally owned chunks
        let mut filtered_ldf = LocalDataFrame::new(self.get_schema());
        for key in &my_keys {
            // TODO: should not really need wait_and_get here since we own that chunk?
            let ldf = self.kv.wait_and_get(key).await?;
            filtered_ldf = filtered_ldf.combine(ldf.pfilter(&mut rower))?;
        }
        // initialize some other required fields of self so as not to duplicate
        // code in if branches
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
        let mut rng = rand::thread_rng();
        let r = rng.gen::<i16>();
        let new_name = format!("{}-filtered-{}", &self.df_name, r);
        let df_client_type = format!("ddf-{}", new_name);
        // for processing results of distributed filtering
        let (filter_results_sender, filter_results) =
            mpsc::channel(self.num_nodes);
        let filter_results = Mutex::new(filter_results);

        let num_rows_left = filtered_ldf.n_rows();
        info!(
            "Finished filtering {} local chunk(s), have {} rows after filter",
            my_keys.len(),
            num_rows_left
        );

        // put our result in our KVStore only if its not empty
        let mut key = None;
        if num_rows_left > 0 {
            let k = Key::generate(&new_name, self.node_id);
            key = Some(k.clone());
            self.kv.put(k, filtered_ldf).await?;
        }

        if self.node_id == 1 {
            // 1. start the new network client
            // connect our client right away since we want to be node 1
            let network = Client::new(
                &self.server_addr,
                &self.my_ip,
                None,
                sender,
                kill_notifier.clone(),
                self.num_nodes,
                false,
                df_client_type.as_str(),
            )
            .await?;
            assert_eq!(1, { network.read().await.id });
            // Send a ready message to node 2 so that all the other nodes
            // start connecting to the Server in the correct order
            let ready_blob = serialize(&DistributedDFMsg::Ready)?;
            self.kv.send_blob(self.node_id + 1, ready_blob).await?;
            // 2. collect all results from other nodes (do ours first)
            let mut df_chunk_map = HashMap::new();
            let mut cur_num_rows = 0;
            match key {
                Some(k) => {
                    df_chunk_map.insert(
                        Range {
                            start: cur_num_rows,
                            end: cur_num_rows + num_rows_left,
                        },
                        k,
                    );
                    cur_num_rows += num_rows_left;
                }
                None => (),
            };

            let mut results_received = 1;
            // TODO: maybe a better way to pass around these results
            {
                let mut unlocked = self.filter_results.lock().await;
                while results_received < self.num_nodes {
                    let msg = unlocked.recv().await.unwrap();
                    match msg {
                        DistributedDFMsg::FilterResult {
                            num_rows,
                            filtered_df_key,
                        } => {
                            match filtered_df_key {
                                Some(k) => {
                                    df_chunk_map.insert(
                                        Range {
                                            start: cur_num_rows,
                                            end: cur_num_rows + num_rows,
                                        },
                                        k,
                                    );
                                    cur_num_rows += num_rows;
                                }
                                None => {
                                    assert_eq!(num_rows, 0);
                                }
                            }
                            results_received += 1;
                        }
                        _ => return Err(LiquidError::UnexpectedMessage),
                    }
                    results_received += 1;
                }
                debug!("Got all filter results from other nodes");
            }

            // 3. broadcast initialization message

            // Create an Initialization message that holds all the information
            // related to this DistributedDataFrame, the Schema and the map
            // of the range of indices that each chunk holds and the `Key`
            // associated with that chunk
            let intro_msg = DistributedDFMsg::Initialization {
                schema: self.get_schema().clone(),
                df_chunk_map: df_chunk_map.clone(),
            };

            // Broadcast the initialization message to all nodes
            {
                network.write().await.broadcast(intro_msg).await?
            };
            debug!("Node 1 sent the initialization message to all nodes");

            // 4. initialize self
            let row = Arc::new(RwLock::new(Row::new(self.get_schema())));
            let num_rows = df_chunk_map.iter().fold(0, |mut acc, (k, _)| {
                if acc > k.end {
                    acc
                } else {
                    acc = k.end;
                    acc
                }
            });

            let ddf = Arc::new(DistributedDataFrame {
                schema: self.get_schema().clone(),
                df_name: new_name,
                df_chunk_map,
                num_rows,
                network,
                node_id: self.node_id,
                num_nodes: self.num_nodes,
                server_addr: self.server_addr.clone(),
                my_ip: self.my_ip.clone(),
                kv: self.kv.clone(),
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
                filter_results,
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                    filter_results_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        } else {
            // we are not the first node
            // 1. connect to the network in the correct order by waiting
            //    for the node before us to send a 'ready' message to our
            //    kv's blob_receiver
            let ready_blob =
                { kv_blob_receiver.lock().await.recv().await.unwrap() };
            let ready_msg = deserialize(&ready_blob)?;
            match ready_msg {
                DistributedDFMsg::Ready => (),
                _ => return Err(LiquidError::UnexpectedMessage),
            }
            debug!("Received a ready message");
            // 2. The node before us has joined the network, it is now time
            //    to connect
            let network = Client::new(
                &self.server_addr,
                &self.my_ip,
                None,
                sender,
                kill_notifier.clone(),
                self.num_nodes,
                false,
                df_client_type.as_str(),
            )
            .await?;
            // assert that we joined in the right order (kv node id must
            // match client node id)
            assert_eq!(self.node_id, { network.read().await.id });

            // 2. send a ready message to the next node so they can connect, if
            //    we are not the last node
            if self.node_id < self.num_nodes {
                // All nodes except the last node must send a ready message
                // to the node that comes after them to let the next node know
                // they may join the network
                self.kv.send_blob(self.node_id + 1, ready_blob).await?;
            }

            // 3. send our filterresults to node 1
            let results = DistributedDFMsg::FilterResult {
                num_rows: num_rows_left,
                filtered_df_key: key,
            };
            {
                network.write().await.send_msg(1, results).await?
            };

            // 4. wait for node 1 to respond with the initialization message
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

            // 4. initialize self
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
                df_name: new_name,
                df_chunk_map,
                num_rows,
                network,
                node_id: self.node_id,
                num_nodes: self.num_nodes,
                server_addr: self.server_addr.clone(),
                my_ip: self.my_ip.clone(),
                kv: self.kv.clone(),
                internal_notifier,
                row,
                kill_notifier,
                blob_receiver: Mutex::new(blob_receiver),
                filter_results,
            });

            // spawn a tokio task to process messages
            let ddf_clone = ddf.clone();
            tokio::spawn(async move {
                DistributedDataFrame::process_messages(
                    ddf_clone,
                    receiver,
                    blob_sender,
                    filter_results_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        }
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
        self.network
            .write()
            .await
            .send_msg(target_id, DistributedDFMsg::Blob(blob))
            .await
    }

    async fn process_messages(
        ddf: Arc<DistributedDataFrame>,
        mut receiver: Receiver<Message<DistributedDFMsg>>,
        blob_sender: Sender<Vec<u8>>,
        filter_results_sender: Sender<DistributedDFMsg>,
    ) -> Result<(), LiquidError> {
        let ddf2 = ddf.clone();
        tokio::spawn(async move {
            loop {
                let msg = receiver.recv().await.unwrap();
                let ddf3 = ddf2.clone();
                let mut blob_sender_clone = blob_sender.clone();
                let mut filter_res_sender = filter_results_sender.clone();
                tokio::spawn(async move {
                    debug!(
                        "DDF processing a message with id: {:#?}",
                        msg.msg_id
                    );
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
                        DistributedDFMsg::FilterResult { num_rows, filtered_df_key } => {
                            filter_res_sender.send(DistributedDFMsg:: FilterResult { num_rows, filtered_df_key }).await.unwrap();
                        }
                        _ => panic!("Should always happen before message process loop is started"),
                    }
                });
            }
        });
        Ok(())
    }
}

fn n_rows(data: &Vec<Column>) -> usize {
    match data.get(0) {
        None => 0,
        Some(x) => match x {
            Column::Int(c) => c.len(),
            Column::Float(c) => c.len(),
            Column::Bool(c) => c.len(),
            Column::String(c) => c.len(),
        },
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
