//! Defines functionality for the `DataFrame`
use crate::dataframe::{
    DistributedDataFrame, LocalDataFrame, Row, Rower, Schema,
};
use crate::error::LiquidError;
use crate::kv::{KVStore, Key};
use bincode::{deserialize, serialize};
use crossbeam_utils::thread;
use num_cpus;
use serde::{de::DeserializeOwned, Serialize};
use sorer::dataframe::{from_file, Column, Data};
use sorer::schema::{infer_schema, DataType};
use std::cmp::Ordering;
use std::convert::TryInto;

const ROW_COUNT_PER_KEY: usize = 1000;

/// An interface for a `DataFrame`, inspired by those used in `pandas` and `R`.
impl DistributedDataFrame {
    /// Creates a new `DataFrame` from the given file, only reads `len` bytes
    /// starting at the given byte offset `from`.
    pub fn from_sor(
        file_name: &str,
        from: usize,
        len: usize,
        kv: KVStore<LocalDataFrame>,
    ) -> Self {
        unimplemented!()
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
        let ldf = { self.kv.lock().await.wait_and_get(key).await? };
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
        let ldf = { self.kv.lock().await.wait_and_get(key).await? };
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
        rower: T,
    ) -> Result<Option<T>, LiquidError> {
        let my_keys: Vec<&Key> = self
            .data
            .iter()
            .filter(|k| k.home == self.node_id)
            .collect();
        let unlocked_kv = self.kv.lock().await;
        for key in my_keys {
            let ldf = unlocked_kv.get(key).await?;
            rower = ldf.pmap(rower);
        }
        if self.node_id == self.num_nodes {
            // we are the last node
            let blob = serialize(&rower)?;
            unlocked_kv.send_blob(self.node_id - 1, blob).await?;
            Ok(None)
        } else {
            let mut blob = self.receiver.lock().await.recv().await.unwrap();
            let external_rower: T = deserialize(&blob[..])?;
            rower = rower.join(external_rower);
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

impl From<Column> for DistributedDataFrame {
    /// Construct a new `DataFrame` with the given `column`.
    fn from(column: Column) -> Self {
        unimplemented!()
    }
}

impl From<Vec<Column>> for DistributedDataFrame {
    /// Construct a new `DataFrame` with the given `columns`.
    fn from(data: Vec<Column>) -> Self {
        unimplemented!()
    }
}

impl From<Data> for DistributedDataFrame {
    /// Construct a new `DataFrame` with the given `scalar` value.
    fn from(scalar: Data) -> Self {
        unimplemented!()
    }
}
