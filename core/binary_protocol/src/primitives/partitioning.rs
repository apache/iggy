// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_bytes, read_u8, read_u32_le};
use bytes::{BufMut, BytesMut};

const KIND_BALANCED: u8 = 1;
const KIND_PARTITION_ID: u8 = 2;
const KIND_MESSAGES_KEY: u8 = 3;

/// Maximum key length for `MessagesKey` partitioning (u8 length prefix on the wire).
pub const MAX_MESSAGES_KEY_LENGTH: usize = 255;

/// Partitioning strategy for message routing.
///
/// Wire format: `[kind:1][length:1][value:0..255]`
/// - `Balanced`:    kind=1, length=0, no value bytes
/// - `PartitionId`: kind=2, length=4, value=u32 LE
/// - `MessagesKey`: kind=3, length=1..255, value=raw bytes
///
/// Use [`WirePartitioning::messages_key`] to construct a `MessagesKey` variant
/// with validation. Direct enum construction is possible but skips the length check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WirePartitioning {
    Balanced,
    PartitionId(u32),
    MessagesKey(Vec<u8>),
}

impl WirePartitioning {
    /// Create a `MessagesKey` partitioning with key length validation.
    ///
    /// # Errors
    /// Returns `WireError::Validation` if `key` is empty or exceeds 255 bytes.
    pub fn messages_key(key: Vec<u8>) -> Result<Self, WireError> {
        if key.is_empty() {
            return Err(WireError::Validation(
                "messages_key partitioning cannot have empty key".to_string(),
            ));
        }
        if key.len() > MAX_MESSAGES_KEY_LENGTH {
            return Err(WireError::Validation(format!(
                "messages_key length {} exceeds maximum {MAX_MESSAGES_KEY_LENGTH}",
                key.len()
            )));
        }
        Ok(Self::MessagesKey(key))
    }
}

impl WireEncode for WirePartitioning {
    fn encoded_size(&self) -> usize {
        match self {
            Self::Balanced => 2,
            Self::PartitionId(_) => 2 + 4,
            Self::MessagesKey(key) => 2 + key.len(),
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Balanced => {
                buf.put_u8(KIND_BALANCED);
                buf.put_u8(0);
            }
            Self::PartitionId(id) => {
                buf.put_u8(KIND_PARTITION_ID);
                buf.put_u8(4);
                buf.put_u32_le(*id);
            }
            Self::MessagesKey(key) => {
                debug_assert!(
                    !key.is_empty() && key.len() <= MAX_MESSAGES_KEY_LENGTH,
                    "MessagesKey length {} out of valid range 1..={MAX_MESSAGES_KEY_LENGTH}; \
                     use WirePartitioning::messages_key() for validated construction",
                    key.len()
                );
                buf.put_u8(KIND_MESSAGES_KEY);
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u8(key.len() as u8);
                buf.put_slice(key);
            }
        }
    }
}

impl WireDecode for WirePartitioning {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let kind = read_u8(buf, 0)?;
        let length = read_u8(buf, 1)? as usize;

        match kind {
            KIND_BALANCED => {
                if length != 0 {
                    return Err(WireError::Validation(format!(
                        "balanced partitioning must have length 0, got {length}"
                    )));
                }
                Ok((Self::Balanced, 2))
            }
            KIND_PARTITION_ID => {
                if length != 4 {
                    return Err(WireError::Validation(format!(
                        "partition_id partitioning must have length 4, got {length}"
                    )));
                }
                let id = read_u32_le(buf, 2)?;
                Ok((Self::PartitionId(id), 6))
            }
            KIND_MESSAGES_KEY => {
                if length == 0 {
                    return Err(WireError::Validation(
                        "messages_key partitioning cannot have empty key".to_string(),
                    ));
                }
                let key = read_bytes(buf, 2, length)?;
                Ok((Self::MessagesKey(key.to_vec()), 2 + length))
            }
            _ => Err(WireError::UnknownDiscriminant {
                type_name: "WirePartitioning",
                value: kind,
                offset: 0,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_balanced() {
        let p = WirePartitioning::Balanced;
        let bytes = p.to_bytes();
        assert_eq!(bytes.len(), 2);
        let (decoded, consumed) = WirePartitioning::decode(&bytes).unwrap();
        assert_eq!(consumed, 2);
        assert_eq!(decoded, p);
    }

    #[test]
    fn roundtrip_partition_id() {
        let p = WirePartitioning::PartitionId(42);
        let bytes = p.to_bytes();
        assert_eq!(bytes.len(), 6);
        let (decoded, consumed) = WirePartitioning::decode(&bytes).unwrap();
        assert_eq!(consumed, 6);
        assert_eq!(decoded, p);
    }

    #[test]
    fn roundtrip_messages_key() {
        let p = WirePartitioning::MessagesKey(b"user-123".to_vec());
        let bytes = p.to_bytes();
        assert_eq!(bytes.len(), 2 + 8);
        let (decoded, consumed) = WirePartitioning::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, p);
    }

    #[test]
    fn messages_key_max_length() {
        let key = vec![0xAB; 255];
        let p = WirePartitioning::MessagesKey(key.clone());
        let bytes = p.to_bytes();
        let (decoded, _) = WirePartitioning::decode(&bytes).unwrap();
        assert_eq!(decoded, WirePartitioning::MessagesKey(key));
    }

    #[test]
    fn messages_key_single_byte() {
        let p = WirePartitioning::MessagesKey(vec![0x01]);
        let bytes = p.to_bytes();
        let (decoded, consumed) = WirePartitioning::decode(&bytes).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(decoded, p);
    }

    #[test]
    fn empty_messages_key_rejected() {
        let buf = [KIND_MESSAGES_KEY, 0x00];
        assert!(WirePartitioning::decode(&buf).is_err());
    }

    #[test]
    fn unknown_kind_rejected() {
        let buf = [0xFF, 0x00];
        assert!(WirePartitioning::decode(&buf).is_err());
    }

    #[test]
    fn balanced_with_nonzero_length_rejected() {
        let buf = [KIND_BALANCED, 0x01, 0x00];
        assert!(WirePartitioning::decode(&buf).is_err());
    }

    #[test]
    fn partition_id_with_wrong_length_rejected() {
        let buf = [KIND_PARTITION_ID, 0x03, 0x00, 0x00, 0x00];
        assert!(WirePartitioning::decode(&buf).is_err());
    }

    #[test]
    fn truncated_buffer() {
        let p = WirePartitioning::PartitionId(1);
        let bytes = p.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                WirePartitioning::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_compat_balanced_layout() {
        let bytes = WirePartitioning::Balanced.to_bytes();
        assert_eq!(&bytes[..], &[KIND_BALANCED, 0x00]);
    }

    #[test]
    fn wire_compat_partition_id_layout() {
        let bytes = WirePartitioning::PartitionId(1).to_bytes();
        assert_eq!(
            &bytes[..],
            &[KIND_PARTITION_ID, 0x04, 0x01, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn messages_key_constructor_validates_empty() {
        assert!(WirePartitioning::messages_key(vec![]).is_err());
    }

    #[test]
    fn messages_key_constructor_validates_too_long() {
        let key = vec![0xAB; 256];
        assert!(WirePartitioning::messages_key(key).is_err());
    }

    #[test]
    fn messages_key_constructor_accepts_max() {
        let key = vec![0xAB; 255];
        let p = WirePartitioning::messages_key(key.clone()).unwrap();
        assert_eq!(p, WirePartitioning::MessagesKey(key));
    }

    #[test]
    fn messages_key_constructor_accepts_single_byte() {
        let p = WirePartitioning::messages_key(vec![0x01]).unwrap();
        let bytes = p.to_bytes();
        let (decoded, _) = WirePartitioning::decode(&bytes).unwrap();
        assert_eq!(decoded, p);
    }
}
