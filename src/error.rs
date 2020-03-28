//! The possible error types when using the liquid-ml crate.
use thiserror::Error;

/// An enumeration of `LiquidML` errors.
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
    NetworkError(#[from] std::io::Error),
    #[error("Serialization/Deserialization Error")]
    SerdeError(#[from] Box<bincode::ErrorKind>),
    #[error("Who you sending this to?")]
    UnknownId,
    #[error("Trying to connect at an Id that already exists")]
    ReconnectionError,
    #[error("Key is not present at this node")]
    NotPresent,
    #[error("Unexpected Stream shutdown")]
    StreamClosed,
    #[error("Unexpected Message")]
    UnexpectedMessage,
    #[error("User is dumb")]
    DumbUserError,
}
