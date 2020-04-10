//! Defines functionality for the `DataFrame`
use crate::dataframe::{
    DistributedDataFrame, LocalDataFrame, Row, Rower, Schema,
};
use crate::error::LiquidError;
use crate::kv::{KVStore, Key};
use bincode::{deserialize, serialize};
use futures::future::try_join_all;
use serde::{de::DeserializeOwned, Serialize};
use sorer::dataframe::{Column, Data, SorTerator};
//use sorer::schema;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex, RwLock};

const ROW_COUNT_PER_KEY: usize = 100_000;

const BYTES_PER_GB: f64 = 1_073_741_824.0;

/// An interface for a `DataFrame`, inspired by those used in `pandas` and `R`.
impl DistributedDataFrame {
    /// Creates a new `DataFrame` from the given file
    pub async fn from_sor(
        file_name: &str,
        kv: Arc<RwLock<KVStore<LocalDataFrame>>>,
        name: &str,
        num_nodes: usize,
        receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Self, LiquidError> {
        let node_id = { kv.read().await.id };
        if node_id == 1 {
            // Reads the SOR File in 1 GB chunks
            // Buffered reading of the sor file
            let total_newlines = count_newlines(file_name);
            dbg!(total_newlines);
            let max_rows_per_node = total_newlines / num_nodes;
            let mut keys = Vec::new();
            let schema = sorer::schema::infer_schema(file_name);
            dbg!(max_rows_per_node);
            dbg!(schema.clone());
            // Bug: Panics if file doesnt exist
            let sorterator =
                SorTerator::new(file_name, schema.clone(), max_rows_per_node);
            {
                let unlocked = kv.read().await;
                let mut futs = Vec::new();
                let mut chunk_idx = 0;
                for data in sorterator {
                    let ldf = LocalDataFrame::from(data);
                    let key = Key::generate(name, (chunk_idx % num_nodes) + 1);

                    futs.push(unlocked.put(key.clone(), ldf));
                    keys.push(key);

                    if chunk_idx % num_nodes == 0 {
                        try_join_all(futs).await?;
                        futs = Vec::new();
                        println!("sent num node chunks");
                    }

                    chunk_idx += 1;
                }
                if !futs.is_empty() {
                    try_join_all(futs).await?;
                }
            }

            let schema = Schema::from(schema);
            let build = serialize(&(keys.clone(), schema.clone()))?;
            for i in 2..(num_nodes + 1) {
                kv.read().await.send_blob(i, build.clone()).await?;
            }

            Ok(DistributedDataFrame {
                schema,
                receiver,
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
    }

    pub async fn new(
        data: Option<Vec<Column>>,
        kv: Arc<RwLock<KVStore<LocalDataFrame>>>,
        name: &str,
        num_nodes: usize,
        receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    ) -> Result<Self, LiquidError> {
        let node_id = { kv.read().await.id };
        if node_id == 1 {
            let mut data = data.unwrap();
            let mut to_process = n_rows(&data);
            let mut chunk_idx = 0;
            let mut keys = Vec::new();
            let mut schema = Schema::new();
            while to_process > 0 {
                let mut new_data = Vec::new();
                for c in &mut data {
                    let new_c = match c {
                        Column::Int(i) => {
                            Column::Int(i.drain(0..ROW_COUNT_PER_KEY).collect())
                        }
                        Column::Bool(i) => Column::Bool(
                            i.drain(0..ROW_COUNT_PER_KEY).collect(),
                        ),
                        Column::Float(i) => Column::Float(
                            i.drain(0..ROW_COUNT_PER_KEY).collect(),
                        ),
                        Column::String(i) => Column::String(
                            i.drain(0..ROW_COUNT_PER_KEY).collect(),
                        ),
                    };
                    new_data.push(new_c);
                }
                let ldf = LocalDataFrame::from(new_data);
                let key = Key::new(
                    &format!("{}_{}", name, chunk_idx),
                    (chunk_idx % num_nodes) + 1,
                );
                schema = ldf.get_schema().clone();
                // todo multiplex?
                {
                    kv.read().await.put(key.clone(), ldf).await?;
                }
                keys.push(key);
                chunk_idx += 1;
                to_process = n_rows(&data);
            }

            let build = serialize(&(keys.clone(), schema.clone()))?;
            for i in 2..(num_nodes + 1) {
                kv.read().await.send_blob(i, build.clone()).await?;
            }

            Ok(DistributedDataFrame {
                schema,
                receiver,
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
        let key = match self.data.get(row_idx / ROW_COUNT_PER_KEY) {
            None => return Err(LiquidError::RowIndexOutOfBounds),
            Some(k) => k,
        };
        let ldf = { self.kv.read().await.wait_and_get(key).await? };
        let adjusted_row_idx = row_idx % ROW_COUNT_PER_KEY;
        ldf.get(col_idx, adjusted_row_idx)
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
        let key = match self.data.get(row_idx / ROW_COUNT_PER_KEY) {
            None => return Err(LiquidError::RowIndexOutOfBounds),
            Some(k) => k,
        };
        let ldf = { self.kv.read().await.wait_and_get(key).await? };
        let adjusted_row_idx = row_idx % ROW_COUNT_PER_KEY;
        ldf.fill_row(adjusted_row_idx, row)
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
        let my_keys: Vec<&Key> = self
            .data
            .iter()
            .filter(|k| k.home == self.node_id)
            .collect();
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
            let mut blob = self.receiver.lock().await.recv().await.unwrap();
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

#[cfg(target_pointer_width = "16")]
const USIZE_BYTES: usize = 2;
#[cfg(target_pointer_width = "32")]
const USIZE_BYTES: usize = 4;
#[cfg(target_pointer_width = "64")]
const USIZE_BYTES: usize = 8;
const LO: usize = ::std::usize::MAX / 255;
const HI: usize = LO * 128;
const REP_NEWLINE: usize = b'\n' as usize * LO;

const EVERY_OTHER_BYTE_LO: usize = 0x0001000100010001;
const EVERY_OTHER_BYTE: usize = EVERY_OTHER_BYTE_LO * 0xFF;

fn count_newlines(file_name: &str) -> usize {
    let mut buf_reader = BufReader::new(File::open(file_name).unwrap());
    let mut newlines = 0;
    let mut buffer = [0; BYTES_PER_GB as usize / 1024];

    loop {
        let bytes_read = buf_reader.read(&mut buffer).unwrap();
        if bytes_read == 0 {
            return newlines;
        } else {
            newlines += count_newlines_hyperscreaming(&buffer[..]);
            buf_reader.consume(buffer.len());
        }
    }
}

// From: https://llogiq.github.io/2016/09/24/newline.html
fn count_newlines_hyperscreaming(text: &[u8]) -> usize {
    unsafe {
        let mut ptr = text.as_ptr();
        let mut end = ptr.offset(text.len() as isize);

        let mut count = 0;

        // Align start
        while (ptr as usize) & (USIZE_BYTES - 1) != 0 {
            if ptr == end {
                return count;
            }
            count += (*ptr == b'\n') as usize;
            ptr = ptr.offset(1);
        }

        // Align end
        while (end as usize) & (USIZE_BYTES - 1) != 0 {
            end = end.offset(-1);
            count += (*end == b'\n') as usize;
        }
        if ptr == end {
            return count;
        }

        // Read in aligned blocks
        let mut ptr = ptr as *const usize;
        let end = end as *const usize;

        unsafe fn next(ptr: &mut *const usize) -> usize {
            let ret = **ptr;
            *ptr = ptr.offset(1);
            ret
        }

        fn mask_zero(x: usize) -> usize {
            (((x ^ REP_NEWLINE).wrapping_sub(LO)) & !x & HI) >> 7
        }

        unsafe fn next_4(ptr: &mut *const usize) -> [usize; 4] {
            let x = [next(ptr), next(ptr), next(ptr), next(ptr)];
            [
                mask_zero(x[0]),
                mask_zero(x[1]),
                mask_zero(x[2]),
                mask_zero(x[3]),
            ]
        };

        fn reduce_counts(counts: usize) -> usize {
            let pair_sum = (counts & EVERY_OTHER_BYTE)
                + ((counts >> 8) & EVERY_OTHER_BYTE);
            pair_sum.wrapping_mul(EVERY_OTHER_BYTE_LO)
                >> ((USIZE_BYTES - 2) * 8)
        }

        fn arr_add(xs: [usize; 4], ys: [usize; 4]) -> [usize; 4] {
            [xs[0] + ys[0], xs[1] + ys[1], xs[2] + ys[2], xs[3] + ys[3]]
        }

        // 8kB
        while ptr.offset(4 * 255) <= end {
            let mut counts = [0, 0, 0, 0];
            for _ in 0..255 {
                counts = arr_add(counts, next_4(&mut ptr));
            }
            count += reduce_counts(counts[0]);
            count += reduce_counts(counts[1]);
            count += reduce_counts(counts[2]);
            count += reduce_counts(counts[3]);
        }

        // 1kB
        while ptr.offset(4 * 32) <= end {
            let mut counts = [0, 0, 0, 0];
            for _ in 0..32 {
                counts = arr_add(counts, next_4(&mut ptr));
            }
            count +=
                reduce_counts(counts[0] + counts[1] + counts[2] + counts[3]);
        }

        // 64B
        let mut counts = [0, 0, 0, 0];
        while ptr.offset(4 * 2) <= end {
            for _ in 0..2 {
                counts = arr_add(counts, next_4(&mut ptr));
            }
        }
        count += reduce_counts(counts[0] + counts[1] + counts[2] + counts[3]);

        // 8B
        let mut counts = 0;
        while ptr < end {
            counts += mask_zero(next(&mut ptr));
        }
        count += reduce_counts(counts);

        count
    }
}
