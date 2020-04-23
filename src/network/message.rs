//! Defines messages and codecs used to communicate with the network of nodes
//! over `TCP`.
use crate::error::LiquidError;
use crate::network::Connection;
use crate::{BYTES_PER_KIB, MAX_FRAME_LEN_FRACTION};
use bincode::{deserialize, serialize};
use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio_util::codec::{
    Decoder, Encoder, FramedRead, FramedWrite, LengthDelimitedCodec,
};

/// A buffered and framed message codec for reading messages of type `T`
pub(crate) type FramedStream<T> =
    FramedRead<ReadHalf<TcpStream>, MessageCodec<T>>;
/// A buffered and framed message codec for sending messages of type `T`
pub(crate) type FramedSink<T> =
    FramedWrite<WriteHalf<TcpStream>, MessageCodec<T>>;

/// A message that can sent between nodes for communication. The message
/// is generic for type `T`
#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T> {
    /// The id of this message
    pub msg_id: usize,
    /// The id of the sender
    pub sender_id: usize,
    /// The id of the node this message is being sent to
    pub target_id: usize,
    /// The body of the message
    pub msg: T,
}

/// Control messages to facilitate communication with the registration
/// [`Server`] and other [`Client`]s
///
/// [`Server`]: struct.Server.html
/// [`Client`]: struct.Client.html
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ControlMsg {
    /// A directory message sent by the [`Server`] to new [`Client`]s once they
    /// connect so that they know which other [`Client`]s of that type are
    /// currently connected
    ///
    /// [`Server`]: struct.Server.html
    /// [`Client`]: struct.Client.html
    Directory { dir: Vec<(usize, SocketAddr)> },
    /// An introduction that a new [`Client`] sends to all other existing
    /// [`Client`]s and the [`Server`]
    Introduction {
        address: SocketAddr,
        client_type: String,
    },
    /// A message the [`Server`] sends to [`Client`]s to inform them to shut
    /// down
    ///
    /// [`Server`]: struct.Server.html
    /// [`Client`]: struct.Client.html
    Kill,
    /// A message to notify other [`Client`]s when they are ready to register
    /// a new [`Client`] type
    Ready,
}

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

/// A message encoder/decoder to help frame messages sent over `TCP`,
/// particularly in the case of very large messages. Uses a very simple method
/// of writing the length of the serialized message at the very start of
/// a frame, followed by the serialized message. When decoding, this length
/// is used to determine if a full frame has been read.
#[derive(Debug)]
pub(crate) struct MessageCodec<T> {
    phantom: std::marker::PhantomData<T>,
    pub(crate) codec: LengthDelimitedCodec,
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

/// Asynchronously waits to read the next message from the given `reader`
pub(crate) async fn read_msg<T: DeserializeOwned>(
    reader: &mut FramedStream<T>,
) -> Result<Message<T>, LiquidError> {
    match reader.next().await {
        None => Err(LiquidError::StreamClosed),
        Some(x) => Ok(x?),
    }
}

/// Send the given `message` to the node with the given `target_id` using
/// the given `directory`
pub(crate) async fn send_msg<T: Serialize>(
    target_id: usize,
    message: Message<T>,
    directory: &mut HashMap<usize, Connection<T>>,
) -> Result<(), LiquidError> {
    match directory.get_mut(&target_id) {
        None => Err(LiquidError::UnknownId),
        Some(conn) => {
            conn.sink.send(message).await?;
            Ok(())
        }
    }
}
