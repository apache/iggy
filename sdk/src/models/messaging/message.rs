use super::header::get_headers_size_bytes;
use super::message_header::{IggyMessageHeader, IGGY_MESSAGE_HEADER_SIZE};
use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::models::messaging::header::{HeaderKey, HeaderValue};
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use crate::utils::timestamp::IggyTimestamp;
use bytes::{BufMut, Bytes, BytesMut};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::str::FromStr;

/// The single message. It is exact format in which message is saved to / retrieved from the disk.
#[serde_as]
#[derive(Default, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct IggyMessage {
    pub header: IggyMessageHeader,
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

impl IggyMessage {
    /// Create a message with payload
    pub fn new(payload: Bytes) -> Self {
        Self::builder().payload(payload).build()
    }

    /// Create a message with ID and payload
    pub fn with_id(id: u128, payload: Bytes) -> Self {
        Self::builder().id(id).payload(payload).build()
    }

    /// Start a builder for more complex configuration
    pub fn builder() -> IggyMessageBuilder {
        IggyMessageBuilder::new()
    }

    /// Write message to bytes mut
    pub fn write_to_bytes_mut(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.header.to_bytes());
        buf.put_slice(&self.payload);
        if let Some(headers) = &self.headers {
            buf.put_slice(&headers.to_bytes());
        }
    }
}

impl FromStr for IggyMessage {
    type Err = IggyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let payload = Bytes::from(s.as_bytes().to_vec());
        Ok(IggyMessage::new(payload))
    }
}

impl std::fmt::Display for IggyMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.payload.len();

        if len > 40 {
            write!(
                f,
                "{}|{}...{}",
                self.header.id,
                String::from_utf8_lossy(&self.payload[..20]),
                String::from_utf8_lossy(&self.payload[len - 20..])
            )
        } else {
            write!(
                f,
                "{}|{}",
                self.header.id,
                String::from_utf8_lossy(&self.payload)
            )
        }
    }
}

impl Sizeable for IggyMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        let payload_len = IggyByteSize::from(self.payload.len() as u64);
        let headers_len = get_headers_size_bytes(&self.headers);
        let message_header_len = IggyByteSize::from(IGGY_MESSAGE_HEADER_SIZE as u64);

        payload_len + headers_len + message_header_len
    }
}

impl BytesSerializable for IggyMessage {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.get_size_bytes().as_bytes_usize());
        let message_header = self.header.to_bytes();
        bytes.put_slice(&message_header);
        bytes.put_slice(&self.payload);
        if let Some(headers) = &self.headers {
            bytes.put_slice(&headers.to_bytes());
        }
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() < IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidCommand);
        }
        let mut position = 0;
        let header =
            IggyMessageHeader::from_bytes(bytes.slice(0..IGGY_MESSAGE_HEADER_SIZE as usize))?;

        position += IGGY_MESSAGE_HEADER_SIZE as usize;
        let payload = bytes.slice(position..position + header.payload_length as usize);
        if payload.len() != header.payload_length as usize {
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        position += header.payload_length as usize;
        let headers = if header.headers_length > 0 {
            Some(HashMap::from_bytes(
                bytes.slice(position..position + header.headers_length as usize),
            )?)
        } else {
            None
        };

        Ok(IggyMessage {
            header,
            payload,
            headers,
        })
    }
}

#[derive(Debug, Default)]
pub struct IggyMessageBuilder {
    id: Option<u128>,
    payload: Option<Bytes>,
    headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

impl IggyMessageBuilder {
    pub fn new() -> Self {
        Self {
            id: None,
            payload: None,
            headers: None,
        }
    }

    pub fn id(mut self, id: u128) -> Self {
        self.id = Some(id);
        self
    }

    pub fn payload(mut self, payload: Bytes) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn header(mut self, key: HeaderKey, value: HeaderValue) -> Self {
        let headers = self.headers.get_or_insert_with(HashMap::new);
        headers.insert(key, value);
        self
    }

    pub fn headers(mut self, headers: impl Into<Option<HashMap<HeaderKey, HeaderValue>>>) -> Self {
        self.headers = headers.into();
        self
    }

    pub fn build(self) -> IggyMessage {
        let payload = self.payload.unwrap_or_default();
        let id = self.id.unwrap_or(0);
        let headers_length = get_headers_size_bytes(&self.headers).as_bytes_u64() as u32;

        let msg_header = IggyMessageHeader {
            checksum: 0, // Checksum is calculated on server side
            id,
            offset: 0,
            timestamp: 0,
            origin_timestamp: IggyTimestamp::now().as_micros(),
            headers_length,
            payload_length: payload.len() as u32,
        };

        println!(
            "created msg of user size {}, total size {}, headers size {}",
            payload.len(),
            msg_header.payload_length + msg_header.headers_length + IGGY_MESSAGE_HEADER_SIZE,
            headers_length
        );

        IggyMessage {
            header: msg_header,
            payload,
            headers: self.headers,
        }
    }
}

// Convenience impl for conversion from common types
impl From<String> for IggyMessage {
    fn from(s: String) -> Self {
        Self::new(Bytes::from(s))
    }
}

impl From<&str> for IggyMessage {
    fn from(s: &str) -> Self {
        Self::new(Bytes::from(s.to_owned()))
    }
}

impl From<Vec<u8>> for IggyMessage {
    fn from(v: Vec<u8>) -> Self {
        Self::new(Bytes::from(v))
    }
}
