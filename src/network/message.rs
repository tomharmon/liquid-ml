//! Defines messages used to communicate with the network of nodes over TCP.
use serde::{Deserialize, Serialize};

/// A registration message sent by the `Server` to new `Client`s once they
/// connect to the `Server` so that they know which other `Client`s are
/// currently connected
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RegistrationMsg {
    /// A list of the currently connected clients, containing a tuple of
    /// `(node_id, IP:Port String)`
    pub(crate) clients: Vec<(usize, String)>,
}

/// A connection message that a new `Client` sends to all other existing
/// `Client`s after the new `Client` receives a `RegistrationMsg` from
/// the `Server`
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ConnectionMsg {
    /// The IP:Port of the new `Client`
    pub(crate) my_address: String,
}

/// A message for communication between nodes
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Message<T> {
    pub(crate) msg_id: usize,
    pub(crate) sender_id: usize,
    pub(crate) target_id: usize,
    pub(crate) msg: T,
}
