//! Defines messages and codecs used to communicate with the network of nodes
//! over `TCP`.
use crate::error::LiquidError;
use crate::network::{Message, MessageCodec};
use crate::{BYTES_PER_KIB, MAX_FRAME_LEN_FRACTION};
use bincode::{deserialize, serialize};
use bytes::{Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sysinfo::{RefreshKind, System, SystemExt};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

impl<T> Message<T> {
    /// Creates a new `Message`.
    pub fn new(
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
    /// Creates a new `MessageCodec` with a maximum frame length that is 80%
    /// of the total memory on this machine.
    pub(crate) fn new() -> Self {
        let memo_info_kind = RefreshKind::new().with_memory();
        let sys = System::new_with_specifics(memo_info_kind);
        let total_memory = sys.get_total_memory() as f64;
        let max_frame_len =
            (total_memory * BYTES_PER_KIB * MAX_FRAME_LEN_FRACTION) as usize;
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(max_frame_len)
            .new_codec();
        MessageCodec {
            phantom: std::marker::PhantomData,
            codec,
        }
    }
}

impl<T: DeserializeOwned> Decoder for MessageCodec<T> {
    type Item = Message<T>;
    type Error = LiquidError;
    /// Decodes a message by reading the length of the message (at the start of
    /// a frame) and then reading that many bytes from a buffer to complete the
    /// frame.
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
    /// Encodes a message by writing the length of the serialized message at
    /// the start of a frame, and then writing that many bytes into a buffer
    /// to be sent.
    fn encode(
        &mut self,
        item: Message<T>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let serialized = serialize(&item)?;
        Ok(self.codec.encode(Bytes::from(serialized), dst)?)
    }
}
