//! Defines messages used to communicate with the network of nodes over TCP.
use crate::error::LiquidError;
use crate::network::{Message, MessageCodec};
use bincode::{deserialize, serialize};
use bytes::{Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

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
