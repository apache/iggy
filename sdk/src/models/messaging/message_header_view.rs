use super::message_header::*;
use crate::error::IggyError;

/// A read-only, typed view into a message header in a raw buffer.
///
/// This wraps a `&[u8]` slice of at least `IGGY_MESSAGE_HEADER_SIZE` bytes.
/// All accessor methods decode fields from the underlying buffer.
#[derive(Debug)]
pub struct IggyMessageHeaderView<'a> {
    data: &'a [u8],
}

impl<'a> IggyMessageHeaderView<'a> {
    /// Creates a new `IggyMessageHeaderView` over `data`.
    ///
    /// Returns an error if `data.len() < IGGY_MESSAGE_HEADER_SIZE`.
    pub fn new(data: &'a [u8]) -> Result<Self, IggyError> {
        if data.len() < IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidCommand);
        }
        Ok(Self { data })
    }

    /// The stored checksum at the start of the header
    pub fn checksum(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    /// The 128-bit ID (16 bytes)
    pub fn id(&self) -> u128 {
        let bytes = &self.data[IGGY_MESSAGE_ID_OFFSET_RANGE];
        u128::from_le_bytes(bytes.try_into().unwrap())
    }

    /// The `offset` field (8 bytes)
    pub fn offset(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_OFFSET_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    /// The `timestamp` field (8 bytes)
    pub fn timestamp(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    /// The `origin_timestamp` field (8 bytes)
    pub fn origin_timestamp(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    /// The size in bytes of the user headers
    pub fn headers_length(&self) -> u32 {
        let bytes = &self.data[IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap())
    }

    /// The size in bytes of the message payload
    pub fn payload_length(&self) -> u32 {
        let bytes = &self.data[IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap())
    }
}
