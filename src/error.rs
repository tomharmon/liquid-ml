//! The possible error types when using the `DataFrame` trait.

use std::error;
use std::fmt;

/// An enumeration of `DataFrame` errors.
#[derive(Debug)]
pub enum DFError {
    RowIndexOutOfBounds,
    ColIndexOutOfBounds,
    NameAlreadyExists,
    TypeMismatch,
    NotSet,
}

impl fmt::Display for DFError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            DFError::RowIndexOutOfBounds => {
                write!(f, "Row index out of bounds")
            }
            DFError::ColIndexOutOfBounds => {
                write!(f, "Col index out of bounds")
            }
            DFError::NameAlreadyExists => write!(f, "Name already in use"),
            DFError::TypeMismatch => write!(
                f,
                "The requested operation doesn't match the schema data type"
            ),
            DFError::NotSet => write!(f, "Must set the index for the row"),
        }
    }
}

impl error::Error for DFError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}
