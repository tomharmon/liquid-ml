//! Defines functionality for a data frame that is split across different
//! physical machines.
use crate::dataframe::{local_dataframe::LocalDataFrame, Row, Rower, Schema};
use crate::error::LiquidError;
use crate::kv::{KVStore, Key};
use crate::network::{Client, FramedStream};
use bincode::{deserialize, serialize};
use futures::stream::{SelectAll, StreamExt};
use log::{debug, info};
use rand::{self, Rng};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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

/// Represents a distributed, immutable data frame. Provides convenient `map`
/// and `filter` methods that operate on the entire distributed data frame
/// with a given [`Rower`]
///
/// [`Rower`]: trait.Rower.html
#[derive(Debug)]
pub struct DistributedDataFrame {
    /// The `Schema` of this `DistributedDataFrame`
    pub schema: Schema,
    /// The name of this `DistributedDataFrame`. Must be unique in a `LiquidML`
    /// instance
    pub df_name: String,
    /// A map of the range of row indices to the `Key`s that point to the chunk
    /// of data with those rows. Not all `Key`s in this map belong to this node
    /// of the `DistributedDataFrame`, some may belong to other nodes
    pub df_chunk_map: HashMap<Range<usize>, Key>,
    /// The number of rows in this entire `DistributedDataFrame`
    pub num_rows: usize,
    /// The id of the node this `DistributedDataFrame` is running on
    pub node_id: usize,
    /// How many nodes are there in this `DistributedDataFrame`?
    pub num_nodes: usize,
    /// What's the address of the `Server`?
    pub server_addr: String,
    /// What's my IP address?
    pub my_ip: String,
    /// Used for communication with other nodes in this `DistributedDataFrame`
    network: Arc<Mutex<Client<DistributedDFMsg>>>,
    /// The `KVStore`, which stores the serialized data owned by this
    /// `DistributedDataFrame` and deserialized cached data that may or may
    /// not belong to this node
    kv: Arc<KVStore<LocalDataFrame>>,
    /// Used for processing messages so that the asynchronous task running
    /// the `process_message` function can notify other asynchronous tasks
    /// when the `row` of this `DistributedDataFrame` is ready to use for
    /// operations (such as returning the result to the `get_row` function
    internal_notifier: Arc<Notify>,
    /// Is mutated by the asynchronous `process_message` task to be a requested
    /// row when the network responds to `GetRow` requests, to enable getter
    /// methods for data such as `get_row`
    row: Arc<RwLock<Row>>,
    /// A notifier that gets notified when the `Server` has sent a `Kill`
    /// message to this `DistributedDataFrame`'s network `Client`
    kill_notifier: Arc<Notify>,
    /// Used for lower level messages, such as sending arbitrary `Rower`s
    blob_receiver: Mutex<Receiver<Vec<u8>>>,
    /// Used for processing filter results TODO: maybe a better way to do this
    filter_results: Mutex<Receiver<DistributedDFMsg>>,
}

/// Represents the kinds of messages sent between `DistributedDataFrame`s
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum DistributedDFMsg {
    /// A messaged used to request a `Row` with the given index from another
    /// node in a `DistributedDataFrame`
    GetRow(usize),
    /// A message used to respond to `GetRow` messages with the requested row
    Row(Row),
    /// A message used to tell the 1st node the results from using the `filter`
    /// method. If there were no rows after filtering, then `filtered_df_key`
    /// is `None` and `num_rows` is `0`.
    FilterResult {
        num_rows: usize,
        filtered_df_key: Option<Key>,
    },
    /// A message used to share random blobs of data with other nodes. This
    /// provides a lower level interface to facilitate other kinds of messages,
    /// for example sending rowers when performing `map`/`filter`.
    Blob(Vec<u8>),
    /// Used to inform other nodes in a `DistributedDataFrame` the required
    /// information for other nodes to construct a new `DistributedDataFrame`
    /// struct that is consistent across all nodes.
    Initialization {
        schema: Schema,
        df_chunk_map: HashMap<Range<usize>, Key>,
    },
}

impl DistributedDataFrame {
    /// Creates a new `DistributedDataFrame` from the given file. It is
    /// assumed that node 1 contains the file with the given `file_name`.
    /// Node 1 will then parse that file and distribute chunks to other nodes
    /// over the network, so if network latency is a concern you should not
    /// use this method.
    ///
    /// You may use this function directly to create a `DistributedDataFrame`
    /// but it is recommended to use the application layer by calling
    /// [`LiquidML::df_from_sor`] instead. Doing so will pass in many of the
    /// required parameters for you, particularly the ones that are required
    /// for the distributed system, such as the `kv` and the `kv_blob_receiver`
    ///
    /// [`LiquidML::df_from_sor`]: ../struct.LiquidML.html#method.df_from_sor
    pub(crate) async fn from_sor(
        server_addr: &str,
        my_ip: &str,
        file_name: &str,
        kv: Arc<KVStore<LocalDataFrame>>,
        df_name: &str,
        num_nodes: usize,
    ) -> Result<Arc<Self>, LiquidError> {
        // make a chunking iterator for the sor file
        let sor_terator = if kv.id == 1 {
            let total_newlines = count_new_lines(file_name);
            let max_rows_per_node = total_newlines / num_nodes;
            let schema = sorer::schema::infer_schema(file_name);
            info!(
                "Total newlines: {} max rows per node: {}",
                total_newlines, max_rows_per_node
            );
            info!("Inferred schema: {:?}", &schema);
            Some(SorTerator::new(file_name, schema, max_rows_per_node))
        } else {
            None
        };
        DistributedDataFrame::from_iter(
            server_addr,
            my_ip,
            sor_terator,
            kv,
            df_name,
            num_nodes,
        )
        .await
    }

    /// Creates a new `DataFrame` from the given iterator. The iterator is
    /// used only on node 1, which calls `next` on it and distributes chunks
    /// concurrently.
    ///
    /// You may use this function directly to create a `DistributedDataFrame`
    /// but it is recommended to use the application layer by calling
    /// [`LiquidML::df_from_iter`] instead. Doing so will pass in many of the
    /// required parameters for you, particularly the ones that are required
    /// for the distributed system, such as the `kv` and the `kv_blob_receiver`
    ///
    /// [`LiquidML::df_from_iter`]: ../struct.LiquidML.html#method.df_from_iter
    pub(crate) async fn from_iter(
        server_addr: &str,
        my_ip: &str,
        iter: Option<impl Iterator<Item = Vec<Column>>>,
        kv: Arc<KVStore<LocalDataFrame>>,
        df_name: &str,
        num_nodes: usize,
    ) -> Result<Arc<Self>, LiquidError> {
        // Figure out what node we are supposed to be
        let node_id = kv.id;
        // initialize some other required fields of self so as not to duplicate
        // code in if branches
        let (blob_sender, blob_receiver) = mpsc::channel(2);
        // used for internal messaging processing so that the asynchronous
        // messaging task can notify other tasks when `self.row` is ready
        let internal_notifier = Arc::new(Notify::new());
        // so that our network client can notify us when they get a Kill
        // signal
        let kill_notifier = Arc::new(Notify::new());
        // so that our client only connects to clients for this dataframe
        let df_network_name = format!("ddf-{}", df_name);
        // for processing results when distributed filtering is performed
        // on this `DistributedDataFrame`
        let (filter_results_sender, filter_results) = mpsc::channel(num_nodes);
        let filter_results = Mutex::new(filter_results);

        let (network, mut read_streams, _kill_notifier) =
            Client::register_network(
                kv.network.clone(),
                df_network_name.to_string(),
            )
            .await?;
        assert_eq!(node_id, { network.lock().await.id });

        // Node 1 is responsible for sending out chunks
        if node_id == 1 {
            // Distribute the chunked sor file round-robin style
            let mut df_chunk_map = HashMap::new();
            let mut cur_num_rows = 0;
            let mut schema = None;
            {
                // in each iteration, create a future sends a chunk to a node
                let mut chunk_idx = 0;
                for chunk in iter.unwrap().into_iter() {
                    if chunk_idx == 0 {
                        schema = Some(Schema::from(&chunk));
                    }

                    let ldf = LocalDataFrame::from(chunk);
                    if chunk_idx > 0 {
                        // assert all chunks have the same schema
                        assert_eq!(schema.as_ref(), Some(ldf.get_schema()));
                    }

                    // make the key that will be associated with this chunk
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

                    let kv_ptr = kv.clone();
                    tokio::spawn(async move {
                        kv_ptr.put(key, ldf).await.unwrap();
                    });

                    // NOTE: might need to do some tuning on when to join the
                    // futures here, possibly even dynamically figure out some
                    // value to smooth over the tradeoff between memory and
                    // speed
                    chunk_idx += 1;
                }

                // we are almost done distributing chunks
                info!("Finished distributing {} SoR chunks", chunk_idx);
            }

            // Create an Initialization message that holds all the information
            // related to this DistributedDataFrame, the Schema and the map
            // of the range of indices that each chunk holds and the `Key`
            // associated with that chunk
            let schema = schema.unwrap();
            let intro_msg = DistributedDFMsg::Initialization {
                schema: schema.clone(),
                df_chunk_map: df_chunk_map.clone(),
            };

            // Broadcast the initialization message to all nodes
            network.lock().await.broadcast(intro_msg).await?;
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
                    read_streams,
                    blob_sender,
                    filter_results_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        } else {
            // Node 1 will send the initialization message to our network
            let init_msg = read_streams.next().await.unwrap()?;
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
                    read_streams,
                    blob_sender,
                    filter_results_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        }
    }

    // TODO: add some verification that the `data` is not jagged. A function
    //       that is a no-op if its not jagged, otherwise inserts nulls to fix
    //       it, would be nice.

    /// Creates a new `DistributedDataFrame` by chunking the given `data` into
    /// evenly sized chunks and distributing it across all nodes. Each chunk
    /// will be size of total number of rows in `data` divided by the number of
    /// nodes, since this was found to have the best performance for `map` and
    /// `filter`. Node 1 is responsible for distributing the data, and thus
    /// `data` should only be `Some` on node 1.
    ///
    /// You may use this function directly to create a `DistributedDataFrame`
    /// but it is recommended to use the application layer by calling
    /// [`LiquidML::df_from_fn`] instead. Doing so will pass in many of the
    /// required parameters for you, particularly the ones that are required
    /// for the distributed system, such as the `kv` and the `kv_blob_receiver`
    ///
    /// [`LiquidML::df_from_fn`]: ../struct.LiquidML.html#method.df_from_fn
    ///
    /// NOTE: this function currently does not verify that `data` is not
    /// jagged, which is a required invariant of the program. There is a plan
    /// to automatically fix jagged data.
    pub(crate) async fn new(
        server_addr: &str,
        my_ip: &str,
        data: Option<Vec<Column>>,
        kv: Arc<KVStore<LocalDataFrame>>,
        df_name: &str,
        num_nodes: usize,
    ) -> Result<Arc<Self>, LiquidError> {
        let num_rows = if let Some(d) = &data { n_rows(d) } else { 0 };
        let chunk_size = num_rows / num_nodes;
        let chunkerator = if data.is_some() {
            Some(DataChunkerator { chunk_size, data })
        } else {
            None
        };
        DistributedDataFrame::from_iter(
            server_addr,
            my_ip,
            chunkerator,
            kv,
            df_name,
            num_nodes,
        )
        .await
    }

    /// Obtains a reference to this `DistributedDataFrame`s schema.
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the data at the given `col_idx`, `row_idx` offsets as a boxed value
    pub async fn get(
        &self,
        col_idx: usize,
        row_idx: usize,
    ) -> Result<Data, LiquidError> {
        let r = self.get_row(row_idx).await?;
        Ok(r.get(col_idx)?.clone())
    }

    /// Returns a clone of the row at the requested `index`
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
                            .lock()
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

    /// Perform a distributed map operation on this `DistributedDataFrame` with
    /// the given `rower`. Returns `Some(rower)` (of the joined results) if the
    /// `node_id` of this `DistributedDataFrame` is `1`, and `None` otherwise.
    ///
    /// A local `pmap` is used on each node to map over that nodes' chunk.
    /// By default, each node will use the number of threads available on that
    /// machine.
    ///
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
            // TODO: shouldn't need wait_and_get here since we own that chunk..
            let ldf = self.kv.wait_and_get(key).await?;
            rower = ldf.pmap(rower);
        }
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

    // TODO: maybe abstract this into an iterator and use the from_iter
    //       function since a **lot** of code here is copy pasted from that.
    //       One issue: filter needs to generate a client-type that is unique
    //       to the filtered dataframe, but from_iter assumes the client-type
    //       is `ddf`. We could make a private from_iter_and_type method
    //       that also accepts the client-type, and then from_iter passes in
    //       "ddf" while filter passes in the generated client-type

    /// Perform a distributed filter operation on this `DistributedDataFrame`.
    /// This function does not mutate the `DistributedDataFrame` in anyway,
    /// instead, it creates a new `DistributedDataFrame` of the results. This
    /// `DistributedDataFrame` is returned to every node so that the results
    /// are consistent everywhere.
    ///
    /// A local `pfilter` is used on each node to filter over that nodes'
    /// chunks.  By default, each node will use the number of threads available
    /// on that machine.
    ///
    /// [`LiquidML::filter`]: ../struct.LiquidML.html#method.filter
    pub async fn filter<
        T: Rower + Clone + Send + Serialize + DeserializeOwned,
    >(
        &self,
        mut rower: T,
    ) -> Result<Arc<Self>, LiquidError> {
        // so that our network client can notify us when they get a Kill
        // signal
        let kill_notifier = Arc::new(Notify::new());
        let mut rng = rand::thread_rng();
        let r = rng.gen::<i16>();
        let new_name = format!("{}-filtered-{}", &self.df_name, r);
        let df_network_name = format!("ddf-{}", new_name);
        let (network, mut read_streams, _kill_notifier) =
            Client::register_network(
                self.kv.network.clone(),
                df_network_name.to_string(),
            )
            .await?;
        assert_eq!(self.node_id, { network.lock().await.id });

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
            // 2. collect all results from other nodes (insert ours first)
            let mut df_chunk_map = HashMap::new();
            let mut cur_num_rows = 0;
            if let Some(key) = key {
                df_chunk_map.insert(
                    Range {
                        start: cur_num_rows,
                        end: cur_num_rows + num_rows_left,
                    },
                    key,
                );
                cur_num_rows += num_rows_left;
            }

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
            network.lock().await.broadcast(intro_msg).await?;
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
                    read_streams,
                    blob_sender,
                    filter_results_sender,
                )
                .await
                .unwrap();
            });

            Ok(ddf)
        } else {
            // send our filterresults to node 1
            let results = DistributedDFMsg::FilterResult {
                num_rows: num_rows_left,
                filtered_df_key: key,
            };
            network.lock().await.send_msg(1, results).await?;
            // Node 1 will send the initialization message to our network
            let init_msg = read_streams.next().await.unwrap()?;
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
                    read_streams,
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

    /// Return the number of columns in this `DistributedDataFrame`.
    pub fn n_cols(&self) -> usize {
        self.schema.width()
    }

    /// Sends the given `blob` to the `DistributedDataFrame` with the given
    /// `target_id` This provides a lower level interface to facilitate other
    /// kinds of messages, such as sending deserialized `Rower`s
    async fn send_blob<T: Serialize>(
        &self,
        target_id: usize,
        blob: &T,
    ) -> Result<(), LiquidError> {
        let blob = serialize(blob)?;
        self.network
            .lock()
            .await
            .send_msg(target_id, DistributedDFMsg::Blob(blob))
            .await
    }

    /// Spawns a `tokio` task that processes `DistributedDFMsg` messages
    /// When a message is received, a new `tokio` task is spawned to
    /// handle processing of that message to reduce blocking of the message
    /// receiving task, so that new messages can be read and processed
    /// concurrently.
    async fn process_messages(
        ddf: Arc<DistributedDataFrame>,
        mut read_streams: SelectAll<FramedStream<DistributedDFMsg>>,
        blob_sender: Sender<Vec<u8>>,
        filter_results_sender: Sender<DistributedDFMsg>,
    ) -> Result<(), LiquidError> {
        while let Some(Ok(msg)) = read_streams.next().await {
            let mut blob_sender_clone = blob_sender.clone();
            let mut filter_res_sender = filter_results_sender.clone();
            let ddf2 = ddf.clone();
            tokio::spawn(async move {
                match msg.msg {
                        DistributedDFMsg::GetRow(row_idx) => {
                            let r = ddf2.get_row(row_idx).await.unwrap();
                            {
                                ddf2.network
                                    .lock()
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
                                *ddf2.row.write().await = row;
                            }
                            ddf2.internal_notifier.notify();
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

        Ok(())
    }
}

/// A simple struct to help chunk `Vec<Column>` by a given number of rows
#[derive(Debug)]
struct DataChunkerator {
    /// how many rows in each chunk
    chunk_size: usize,
    /// Optional because its assumed node 1 has the data
    data: Option<Vec<Column>>,
}

impl Iterator for DataChunkerator {
    type Item = Vec<Column>;

    /// Advances this iterator by breaking off `self.chunk_size` rows of its
    /// data until the data is empty. The last chunk may be less than
    /// `self.chunk_size`
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(data) = &mut self.data {
            // we are node 1 and have the data
            let cur_chunk_size = cmp::min(self.chunk_size, n_rows(&data));
            if cur_chunk_size == 0 {
                // the data has been consumed
                None
            } else {
                // there is more data to chunk
                let mut chunked_data = Vec::with_capacity(data.len());
                for col in data {
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
                Some(chunked_data)
            }
        } else {
            // we are not node 1, we don't have the data
            None
        }
    }
}

fn n_rows(data: &[Column]) -> usize {
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
