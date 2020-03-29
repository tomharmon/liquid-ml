//! Defines messages used to communicate with the network of nodes over TCP.
use crate::error::LiquidError;
use bincode::{deserialize, serialize};
use bytes::{Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// A message for communication between nodes
#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T> {
    /// The id of this message
    pub(crate) msg_id: usize,
    /// The id of the sender
    pub(crate) sender_id: usize,
    /// The id of the node this message is being sent to
    pub(crate) target_id: usize,
    /// The body of the message
    pub(crate) msg: T,
}

impl<T> Message<T> {
    /// Creates a new message.
    pub(crate) fn new(
        msg_id: usize,
        sender_id: usize,
        target_id: usize,
        msg: T,
    ) -> Self {
        Message {
            msg_id,
            sender_id,
            target_id,
            msg,
        }
    }
}

/// Control messages to facilitate communication with the registration server
#[derive(Serialize, Deserialize, Debug)]
pub enum ControlMsg {
    /// A directory message sent by the `Server` to new `Client`s once they
    /// connect to the `Server` so that they know which other `Client`s are
    /// currently connected
    Directory { dir: Vec<(usize, String)> },
    /// An introduction that a new `Client` sends to all other existing
    /// `Client`s and the server
    Introduction { address: String },
    //TODO : Add a kill message here at some point
}

#[derive(Debug)]
pub struct MessageCodec<T> {
    phantom: std::marker::PhantomData<T>,
    pub(crate) codec: LengthDelimitedCodec,
}

impl<T> MessageCodec<T> {
    pub(crate) fn new() -> Self {
        MessageCodec {
            phantom: std::marker::PhantomData,
            codec: LengthDelimitedCodec::new(),
        }
    }
}

impl<T: DeserializeOwned> Decoder for MessageCodec<T> {
    type Item = Message<T>;
    type Error = LiquidError;
    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        match self.codec.decode(src)? {
            Some(data) => Ok(Some(deserialize(&data)?)),
            None => Ok(None),
        }
    }
}

impl<T: Serialize> Encoder<Message<T>> for MessageCodec<T> {
    type Error = LiquidError;
    fn encode(
        &mut self,
        item: Message<T>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let serialized = serialize(&item)?;
        Ok(self.codec.encode(Bytes::from(serialized), dst)?)
    }
}
