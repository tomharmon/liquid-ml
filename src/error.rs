//! The possible error types when using the `DataFrame` trait.
use std::io;
use thiserror::Error;
/// An enumeration of `DataFrame` errors.
#[derive(Debug, Error)]
pub enum LiquidError {
    #[error("Row index out of bounds")]
    RowIndexOutOfBounds,
    #[error("Column index out of bounds")]
    ColIndexOutOfBounds,
    #[error("Name already in use")]
    NameAlreadyExists,
    #[error("The requested operation doesn't match the schema data type")]
    TypeMismatch,
    #[error("Must set an index for the row")]
    NotSet,
    #[error("Network error")]
    NetworkError(#[from] io::Error),
    #[error("Serialization/Deserialization Error")]
    SerdeError(#[from] Box<bincode::ErrorKind>),
    #[error("Who you sending this to?")]
    UnknownId,
    #[error("Trying to connect at an Id that already exists")]
    ReconnectionError,
}
