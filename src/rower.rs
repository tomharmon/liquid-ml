use crate::row::Row;

/// Rower:
/// A trait for iterating through each row of a `DataFrame`. The intent
/// is that this trait should be implemented and the accept() function be given
/// a meaningful implementation. Rowers can be cloned for parallel execution.
pub trait Rower {
    /// This function is called once per row. The row object is on loan and
    /// should not be retained as it is likely going to be reused in the next
    /// call. The return value is used in filters to indicate that a row
    /// should be kept.
    fn visit(&mut self, r: &Row) -> bool;

    /// Once traversal of the data frame is complete the rowers that were
    /// split off will be joined.  There will be one join per split. The
    /// original object will be the last to be called join on.
    fn join(&mut self, other: &Self);
}
