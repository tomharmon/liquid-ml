//! Defines a trait to create visitors which can be used to perform data
//! analytics on a `DataFrame` via the `pmap`/`map`/`filter` functions.

use crate::row::Row;

/// A trait for vistors who iterate through and process each row of a
/// `DataFrame`. Rowers are cloned for parallel execution in `DataFrame::pmap`.
pub trait Rower {
    /// This function is called once per row. The `Row` object is on loan and
    /// should not be retained as it is going to be reused in the next
    /// call. The return value is used in filters to indicate that a row
    /// should be kept.
    fn visit(&mut self, r: &Row) -> bool;

    /// Once traversal of the `DataFrame` is complete the rowers that were
    /// split off will be joined.  There will be one join per split. The
    /// original object will be the last to be called join on.
    fn join(&mut self, other: &Self) -> Self;
}
