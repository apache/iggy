use crate::{
    bytes_serializable::BytesSerializable,
    error::IggyError,
    utils::{byte_size::IggyByteSize, sizeable::Sizeable},
};
use bytes::{BufMut, Bytes, BytesMut};
use std::ops::Range;

pub const IGGY_MESSAGE_HEADER_SIZE: u32 = 8 + 16 + 8 + 8 + 8 + 4 + 4;
pub const IGGY_MESSAGE_HEADER_RANGE: Range<usize> = 0..(IGGY_MESSAGE_HEADER_SIZE as usize);

pub const IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE: Range<usize> = 0..8;
pub const IGGY_MESSAGE_ID_OFFSET_RANGE: Range<usize> = 8..24;
pub const IGGY_MESSAGE_OFFSET_OFFSET_RANGE: Range<usize> = 24..32;
pub const IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE: Range<usize> = 32..40;
pub const IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE: Range<usize> = 40..48;
pub const IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE: Range<usize> = 48..52;
pub const IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE: Range<usize> = 52..56;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Default)]
pub struct IggyMessageHeader {
    pub checksum: u64,
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub headers_length: u32,
    pub payload_length: u32,
}

impl Sizeable for IggyMessageHeader {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(IGGY_MESSAGE_HEADER_SIZE as u64)
    }
}

impl BytesSerializable for IggyMessageHeader {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.get_size_bytes().as_bytes_usize());
        bytes.put_u64_le(self.checksum);
        bytes.put_u128_le(self.id);
        bytes.put_u64_le(self.offset);
        bytes.put_u64_le(self.timestamp);
        bytes.put_u64_le(self.origin_timestamp);
        bytes.put_u32_le(self.headers_length);
        bytes.put_u32_le(self.payload_length);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() != IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidCommand);
        }

        let checksum = u64::from_le_bytes(
            bytes[IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let id = u128::from_le_bytes(
            bytes[IGGY_MESSAGE_ID_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let offset = u64::from_le_bytes(
            bytes[IGGY_MESSAGE_OFFSET_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let timestamp = u64::from_le_bytes(
            bytes[IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let origin_timestamp = u64::from_le_bytes(
            bytes[IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let headers_length = u32::from_le_bytes(
            bytes[IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let payload_length = u32::from_le_bytes(
            bytes[IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        Ok(IggyMessageHeader {
            checksum,
            id,
            offset,
            timestamp,
            origin_timestamp,
            headers_length,
            payload_length,
        })
    }
}