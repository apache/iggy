/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use bytes::{Bytes, BytesMut};

use crate::{
    BytesSerializable, IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE, IGGY_MESSAGE_HEADER_SIZE,
    IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_ID_OFFSET_RANGE,
    IGGY_MESSAGE_OFFSET_OFFSET_RANGE, IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE,
    IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE,
    IggyMessageHeader, error::IggyError,
};

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
    pub fn new(data: &'a [u8]) -> Self {
        debug_assert!(
            data.len() >= IGGY_MESSAGE_HEADER_SIZE,
            "Header view requires at least {IGGY_MESSAGE_HEADER_SIZE} bytes"
        );
        Self { data }
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
    pub fn user_headers_length(&self) -> usize {
        let bytes = &self.data[IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    /// The size in bytes of the message payload
    pub fn payload_length(&self) -> usize {
        let bytes = &self.data[IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    /// Convert this view to a full IggyMessageHeader struct
    pub fn to_header(&self) -> IggyMessageHeader {
        IggyMessageHeader {
            checksum: self.checksum(),
            id: self.id(),
            offset: self.offset(),
            timestamp: self.timestamp(),
            origin_timestamp: self.origin_timestamp(),
            user_headers_length: self.user_headers_length() as u32,
            payload_length: self.payload_length() as u32,
        }
    }
}

impl BytesSerializable for IggyMessageHeaderView<'_> {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used")
    }

    fn from_bytes(_bytes: Bytes) -> Result<Self, IggyError> {
        panic!("should not be used")
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(self.data);
    }

    fn get_buffer_size(&self) -> usize {
        IGGY_MESSAGE_HEADER_SIZE
    }
}
