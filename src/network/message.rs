//! Defines messages used to communicate with the network of nodes over TCP.
use serde::{Deserialize, Serialize};

/// A registration message sent by the `Server` to new `Client`s once they
/// connect to the `Server` so that they know which other `Client`s are
/// currently connected
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RegistrationMsg {
    /// The id that the `Server` assigns to the new `Client`
    pub(crate) assigned_id: usize,
    /// The id of this `RegistrationMsg`
    pub(crate) msg_id: usize,
    /// A list of the currently connected clients, containing a tuple of
    /// `(node_id, IP:Port String)`
    pub(crate) clients: Vec<(usize, String)>,
}

/// A connection message that a new `Client` sends to all other existing
/// `Client`s after the new `Client` receives a `RegistrationMsg` from
/// the `Server`
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ConnectionMsg {
    /// The id of the new `Client`
    pub(crate) my_id: usize,
    /// The id of this `ConnectionMsg`
    pub(crate) msg_id: usize,
    /// The IP:Port of the new `Client`
    pub(crate) my_address: String,
}

/// A message for communication between nodes
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Message<T> {
    pub(crate) msg_id: usize,
    pub(crate) sender: usize,
    pub(crate) target: usize,
    pub(crate) msg: T,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct KVRequest {
    pub(crate) key: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KVResponse {
    pub(crate) key: String,
    pub(crate) data: Vec<u8>
}
