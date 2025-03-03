use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;

pub const IGGY_MESSAGE_METADATA: u64 = 4 + 4 + 16; // offset_delta timestamp_delta + id

/// The single message that is polled from the partition.
/// It consists of the following fields:
/// - `id`: the identifier of the message.
/// - `offset`: offset
/// - `timestamp`: timestamp
/// - `payload`: the binary payload of the message.
#[serde_as]
#[derive(Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct IggyMessage {
    /// The identifier of the message.
    pub id: u128,
    /// The offset of the message.
    pub offset: u64,
    /// The timestamp of the message.
    pub timestamp: u64,
    /// The binary payload of the message.
    #[serde_as(as = "Base64")]
    pub payload: Vec<u8>,
    /// The optional headers of the message.
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

impl std::fmt::Display for IggyMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.payload.len();

        if len > 40 {
            write!(
                f,
                "{}|{}...{}",
                self.id,
                String::from_utf8_lossy(&self.payload[..20]),
                String::from_utf8_lossy(&self.payload[len - 20..])
            )
        } else {
            write!(f, "{}|{}", self.id, String::from_utf8_lossy(&self.payload))
        }
    }
}

impl Sizeable for IggyMessage {
    fn get_size_bytes(&self) -> IggyByteSize {
        // id + offset_delta + timestamp_delta + length + payload + headers
        let payload_len = IggyByteSize::from(self.payload.len() as u64);
        let headers_len = header::get_headers_size_bytes(&self.headers);
        let metadata_len = IggyByteSize::from(IGGY_MESSAGE_METADATA);

        payload_len + headers_len + metadata_len
    }
}
