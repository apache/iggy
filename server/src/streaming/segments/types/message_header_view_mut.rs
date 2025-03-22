use iggy::prelude::*;

/// A typed, in-place view of a raw header in a buffer
#[derive(Debug)]
pub struct IggyMessageHeaderViewMut<'a> {
    /// The header data. Must be at least IGGY_MESSAGE_HEADER_SIZE bytes.
    data: &'a mut [u8],
}

impl<'a> IggyMessageHeaderViewMut<'a> {
    /// Construct a mutable view over the header slice.
    pub fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    pub fn checksum(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn id(&self) -> u128 {
        let bytes = &self.data[IGGY_MESSAGE_ID_OFFSET_RANGE];
        u128::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn offset(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_OFFSET_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn timestamp(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn origin_timestamp(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn headers_length(&self) -> u32 {
        let bytes = &self.data[IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn payload_length(&self) -> u32 {
        let bytes = &self.data[IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn set_checksum(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_id(&mut self, value: u128) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_ID_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_offset(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_OFFSET_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_timestamp(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_origin_timestamp(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_headers_length(&mut self, value: u32) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_payload_length(&mut self, value: u32) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE].copy_from_slice(&bytes);
    }
}
