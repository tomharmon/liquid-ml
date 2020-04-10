//! Defines functionality for the `DataFrame`
use crate::dataframe::{
    DistributedDFMsg, DistributedDataFrame, LocalDataFrame, Row, Rower, Schema,
};
use crate::error::LiquidError;
use crate::kv::{KVStore, Key};
use bincode::{deserialize, serialize};
use bytecount;
use futures::future::try_join_all;
use log::info;
use serde::{de::DeserializeOwned, Serialize};
use sorer::dataframe::{Column, Data, SorTerator};
//use sorer::schema;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex, Notify, RwLock};

/// An interface for a `DataFrame`, inspired by those used in `pandas` and `R`.
impl DistributedDataFrame {
    /// TODO: update documentation
    /// Creates a new `DataFrame` from the given file
    pub async fn from_sor(
        file_name: &str,
        kv: Arc<RwLock<KVStore<LocalDataFrame>>>,
        df_name: &str,
        num_nodes: usize,
        blob_receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Self, LiquidError> {
        let node_id = { kv.read().await.id };
        if node_id == 1 {
            // Bug: Panics if file doesnt exist
            let total_newlines = count_new_lines(file_name);
            dbg!(total_newlines);
            let max_rows_per_node = total_newlines / num_nodes;
            let schema = sorer::schema::infer_schema(file_name);
            dbg!(max_rows_per_node);
            dbg!(schema.clone());
            // make a chunking iterator
            let sorterator =
                SorTerator::new(file_name, schema.clone(), max_rows_per_node);
            let mut df_chunk_map = HashMap::new();
            let mut cur_num_rows = 0;
            {
                let unlocked = kv.read().await;
                let mut futs = Vec::new();
                // in each iteration, create a future sends a chunk to a node
                for (chunk_idx, chunk) in sorterator.into_iter().enumerate() {
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
                if !futs.is_empty() {
                    // maybe its not evenly divisible
                    try_join_all(futs).await?;
                    info!(
                        "Finished distributing {} SoR chunks",
                        cur_num_rows / max_rows_per_node
                    );
                }
            }

            let schema = Schema::from(schema);
            let intro_msg = serialize(&DistributedDFMsg::Initialization {
                schema: schema.clone(),
                df_chunk_map: df_chunk_map.clone(),
            })?;

            // send the other nodes this `DistributedDataFrame`
            {
                let unlocked = kv.read().await;
                for i in 2..=num_nodes {
                    unlocked.send_blob(i, intro_msg.clone()).await;
                }
            }

            let row = Arc::new(RwLock::new(Row::new(&schema)));
            let internal_notifier = Arc::new(Notify::new());
            Ok(DistributedDataFrame {
                schema,
                df_name: df_name.to_string(),
                df_chunk_map,
                num_rows: cur_num_rows,
                node_id,
                num_nodes,
                kv,
                internal_notifier,
                row,
                blob_receiver,
            })
        } else {
            let blob = { blob_receiver.lock().await.recv().await.unwrap() };
            let init_msg = deserialize(&blob)?;
            let (schema, df_chunk_map) =
                if let DistributedDFMsg::Initialization {
                    schema,
                    df_chunk_map,
                } = init_msg
                {
                    (schema, df_chunk_map)
                } else {
                    return Err(LiquidError::UnexpectedMessage);
                };
            let row = Arc::new(RwLock::new(Row::new(&schema)));
            let num_rows = df_chunk_map
                .iter()
                .max_by(|(k1, _), (k2, _)| k2.end.cmp(&k1.end))
                .unwrap()
                .0
                .end;
            dbg!(num_rows);
            let internal_notifier = Arc::new(Notify::new());

            Ok(DistributedDataFrame {
                schema,
                df_name: df_name.to_string(),
                df_chunk_map,
                num_rows,
                node_id,
                num_nodes,
                kv,
                internal_notifier,
                row,
                blob_receiver,
            })
        }
    }

    pub async fn new(
        data: Option<Vec<Column>>,
        kv: Arc<RwLock<KVStore<LocalDataFrame>>>,
        name: &str,
        num_nodes: usize,
        receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Self, LiquidError> {
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
        unimplemented!()
        /*
        let key = match self.data.get(row_idx / ROW_COUNT_PER_KEY) {
            None => return Err(LiquidError::RowIndexOutOfBounds),
            Some(k) => k,
        };
        let ldf = { self.kv.read().await.wait_and_get(key).await? };
        let adjusted_row_idx = row_idx % ROW_COUNT_PER_KEY;
        ldf.get(col_idx, adjusted_row_idx)
            */
    }

    /// Get the index of the `Column` with the given `col_name`. Returns `Some`
    /// if a `Column` with the given name exists, or `None` otherwise.
    pub fn get_col_idx(&self, col_name: &str) -> Option<usize> {
        self.schema.col_idx(col_name)
    }

    /// Set the fields of the given `Row` struct with values from this
    /// `DataFrame` at the given `row_index`.
    ///
    /// If the `row` does not have the same schema as this `DataFrame`, a
    /// `LiquidError::TypeMismatch` error will be returned.
    pub async fn fill_row(
        &self,
        row_idx: usize,
        row: &mut Row,
    ) -> Result<(), LiquidError> {
        unimplemented!()
        /*
        let key = match self.data.get(row_idx / ROW_COUNT_PER_KEY) {
            None => return Err(LiquidError::RowIndexOutOfBounds),
            Some(k) => k,
        };
        let ldf = { self.kv.read().await.wait_and_get(key).await? };
        let adjusted_row_idx = row_idx % ROW_COUNT_PER_KEY;
        ldf.fill_row(adjusted_row_idx, row)
        */
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
            .filter(|(range, key)| key.home == self.node_id)
            .map(|(_, v)| v)
            .collect();
        dbg!(my_keys.len());
        // map over our chunks
        {
            let unlocked_kv = self.kv.read().await;
            for key in my_keys {
                let ldf = unlocked_kv.wait_and_get(key).await?;
                rower = ldf.pmap(rower);
            }
        }
        if self.node_id == self.num_nodes {
            let unlocked_kv = self.kv.read().await;
            // we are the last node
            let blob = serialize(&rower)?;
            unlocked_kv.send_blob(self.node_id - 1, blob).await?;
            Ok(None)
        } else {
            let mut blob =
                self.blob_receiver.lock().await.recv().await.unwrap();
            let external_rower: T = deserialize(&blob[..])?;
            rower = rower.join(external_rower);
            let unlocked_kv = self.kv.read().await;
            if self.node_id != 1 {
                blob = serialize(&rower)?;
                unlocked_kv.send_blob(self.node_id - 1, blob).await?;
                Ok(None)
            } else {
                Ok(Some(rower))
            }
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
