//! The possible error types when using the liquid-ml crate.
use thiserror::Error;

/// An enumeration of `LiquidML` errors.
#[derive(Debug, Error)]
pub enum LiquidError {
    /// Attempted to access a row in a `DataFrame` that is out of bounds
    #[error("Row index out of bounds")]
    RowIndexOutOfBounds,
    /// Attempted to access a column in a `DataFrame` that is out of bounds
    #[error("Column index out of bounds")]
    ColIndexOutOfBounds,
    /// Attempted to add or re-name a `Column` in a `DataFrame` to a name that
    /// is already in use in that `DataFrame`
    #[error("Name already in use")]
    NameAlreadyExists,
    /// Attempted to perform an operation that conflicts with a `DataFrame`s
    /// schema, e.g. attempting to use `set_int` for a column that is of type
    /// `String`
    #[error("The requested operation doesn't match the schema data type")]
    TypeMismatch,
    /// A generic error when there is an underlying error with a `TCP`
    /// connection
    #[error("Network error")]
    NetworkError(#[from] std::io::Error),
    /// An error when serializing or deserializing
    #[error("Serialization/Deserialization Error")]
    SerdeError(#[from] Box<bincode::ErrorKind>),
    /// An error when trying to send messages to nodes that are not currently
    /// connected to this node
    #[error("Who you sending this to?")]
    UnknownId,
    /// An error when multiple connections are made to the same `id`
    #[error("Trying to connect at an Id that already exists")]
    ReconnectionError,
    /// An error when trying to `get` a value from a `KVStore` that does not
    /// exist at that `KVStore`
    #[error("Key is not present at this node")]
    NotPresent,
    /// An error when a connection is closed unexpectedly
    #[error("Unexpected Stream shutdown")]
    StreamClosed,
    /// An error when messages are received unexpectedly, e.g. when a `Client`
    /// is starting up and messages other than `ControlMsg`s are received
    #[error("Unexpected Message")]
    UnexpectedMessage,
}
