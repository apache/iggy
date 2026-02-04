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

use crate::{BytesSerializable, Sizeable, error::IggyError, utils::byte_size::IggyByteSize};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::ops::Range;

pub const IGGY_MESSAGE_HEADER_SIZE: usize = 8 + 16 + 8 + 8 + 8 + 4 + 4 + 8;
pub const IGGY_MESSAGE_HEADER_RANGE: Range<usize> = 0..IGGY_MESSAGE_HEADER_SIZE;

pub const IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE: Range<usize> = 0..8;
pub const IGGY_MESSAGE_ID_OFFSET_RANGE: Range<usize> = 8..24;
pub const IGGY_MESSAGE_OFFSET_OFFSET_RANGE: Range<usize> = 24..32;
pub const IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE: Range<usize> = 32..40;
pub const IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE: Range<usize> = 40..48;
pub const IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE: Range<usize> = 48..52;
pub const IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE: Range<usize> = 52..56;
pub const IGGY_MESSAGE_RESERVED_OFFSET_RANGE: Range<usize> = 56..64;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct IggyMessageHeader {
    pub checksum: u64,
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub user_headers_length: u32,
    pub payload_length: u32,
    // Reserved for future use
    pub reserved: u64,
}

impl Sizeable for IggyMessageHeader {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(IGGY_MESSAGE_HEADER_SIZE as u64)
    }
}

impl IggyMessageHeader {
    pub fn from_raw_bytes(bytes: &[u8]) -> Result<Self, IggyError> {
        if bytes.len() != IGGY_MESSAGE_HEADER_SIZE {
            return Err(IggyError::InvalidCommand);
        }

        Ok(IggyMessageHeader {
            checksum: u64::from_le_bytes(
                bytes[IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            id: u128::from_le_bytes(
                bytes[IGGY_MESSAGE_ID_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            offset: u64::from_le_bytes(
                bytes[IGGY_MESSAGE_OFFSET_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            timestamp: u64::from_le_bytes(
                bytes[IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            origin_timestamp: u64::from_le_bytes(
                bytes[IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            user_headers_length: u32::from_le_bytes(
                bytes[IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            payload_length: u32::from_le_bytes(
                bytes[IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            reserved: {
                let reserved = u64::from_le_bytes(
                    bytes[IGGY_MESSAGE_RESERVED_OFFSET_RANGE]
                        .try_into()
                        .map_err(|_| IggyError::InvalidNumberEncoding)?,
                );
                if reserved != 0 {
                    return Err(IggyError::InvalidReservedField(reserved));
                }
                reserved
            },
        })
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
        bytes.put_u32_le(self.user_headers_length);
        bytes.put_u32_le(self.payload_length);
        bytes.put_u64_le(self.reserved);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() != IGGY_MESSAGE_HEADER_SIZE {
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

        let reserved = u64::from_le_bytes(
            bytes[IGGY_MESSAGE_RESERVED_OFFSET_RANGE]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        if reserved != 0 {
            return Err(IggyError::InvalidReservedField(reserved));
        }

        Ok(IggyMessageHeader {
            checksum,
            id,
            offset,
            timestamp,
            origin_timestamp,
            user_headers_length: headers_length,
            payload_length,
            reserved,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_serialize_and_deserialize_header() {
        let header = IggyMessageHeader {
            checksum: 123456789,
            id: 987654321,
            offset: 100,
            timestamp: 1000000,
            origin_timestamp: 999999,
            user_headers_length: 50,
            payload_length: 200,
            reserved: 0,
        };

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), IGGY_MESSAGE_HEADER_SIZE);

        let deserialized = IggyMessageHeader::from_bytes(bytes).unwrap();
        assert_eq!(header, deserialized);
    }

    #[test]
    fn should_serialize_header_to_correct_size() {
        let header = IggyMessageHeader::default();
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), IGGY_MESSAGE_HEADER_SIZE);
        assert_eq!(bytes.len(), 64);
    }

    #[test]
    fn should_fail_to_deserialize_invalid_size() {
        let bytes = Bytes::from(vec![0u8; 56]);
        let result = IggyMessageHeader::from_bytes(bytes);
        assert!(result.is_err());
    }

    #[test]
    fn should_deserialize_from_raw_bytes() {
        let header = IggyMessageHeader {
            checksum: 111,
            id: 222,
            offset: 333,
            timestamp: 444,
            origin_timestamp: 555,
            user_headers_length: 66,
            payload_length: 77,
            reserved: 0,
        };

        let bytes = header.to_bytes();
        let deserialized = IggyMessageHeader::from_raw_bytes(&bytes).unwrap();
        assert_eq!(header.checksum, deserialized.checksum);
        assert_eq!(header.id, deserialized.id);
        assert_eq!(header.offset, deserialized.offset);
        assert_eq!(header.timestamp, deserialized.timestamp);
        assert_eq!(header.origin_timestamp, deserialized.origin_timestamp);
        assert_eq!(header.user_headers_length, deserialized.user_headers_length);
        assert_eq!(header.payload_length, deserialized.payload_length);
    }

    #[test]
    fn should_reject_non_zero_reserved_field_from_bytes() {
        let header = IggyMessageHeader {
            checksum: 123456789,
            id: 987654321,
            offset: 100,
            timestamp: 1000000,
            origin_timestamp: 999999,
            user_headers_length: 50,
            payload_length: 200,
            reserved: 0,
        };

        let mut bytes = header.to_bytes().to_vec();
        let non_zero_reserved: u64 = 42;
        bytes[IGGY_MESSAGE_RESERVED_OFFSET_RANGE].copy_from_slice(&non_zero_reserved.to_le_bytes());

        let result = IggyMessageHeader::from_bytes(Bytes::from(bytes));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            IggyError::InvalidReservedField(non_zero_reserved)
        );
    }

    #[test]
    fn should_reject_non_zero_reserved_field_from_raw_bytes() {
        let header = IggyMessageHeader {
            checksum: 111,
            id: 222,
            offset: 333,
            timestamp: 444,
            origin_timestamp: 555,
            user_headers_length: 66,
            payload_length: 77,
            reserved: 0,
        };

        let mut bytes = header.to_bytes().to_vec();
        let non_zero_reserved: u64 = 123456789;
        bytes[IGGY_MESSAGE_RESERVED_OFFSET_RANGE].copy_from_slice(&non_zero_reserved.to_le_bytes());

        let result = IggyMessageHeader::from_raw_bytes(&bytes);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            IggyError::InvalidReservedField(non_zero_reserved)
        );
    }
}
