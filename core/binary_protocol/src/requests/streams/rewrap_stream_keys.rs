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
use crate::codec::{WireDecode, WireEncode, read_bytes, read_u8, read_u32_le, read_u64_le};
use bytes::{BufMut, Bytes, BytesMut};
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};

/// Bytes of one wrapped stream key envelope.
pub const WRAPPED_STREAM_KEY_LENGTH: usize = 78;
/// Most envelopes one wrap set holds.
pub const MAX_WRAP_SET_ENVELOPES: usize = 3;
const KEY_FINGERPRINT_LENGTH: usize = 8;
const ENTRY_FIXED_SIZE: usize = 4 + 8 + KEY_FINGERPRINT_LENGTH + 1;

/// Server-originated: replace the wrap sets of encrypted streams after a
/// key-encryption key rotation. Apply checks each entry against the stored
/// stream and skips one that no longer matches.
///
/// Wire format:
/// ```text
/// [count:u32]([stream_id:u32][created_op:u64][key_fingerprint:8][set_len:u8][set])*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewrapStreamKeysRequest {
    pub entries: Vec<RewrapStreamKeyEntry>,
}

/// One stream incarnation and the wrap set it should store.
#[derive(Clone, PartialEq, Eq)]
pub struct RewrapStreamKeyEntry {
    pub stream_id: u32,
    pub created_op: u64,
    pub key_fingerprint: [u8; KEY_FINGERPRINT_LENGTH],
    pub wrap_set: Bytes,
}

impl Debug for RewrapStreamKeyEntry {
    /// Prints the envelope count, never the wrapped key bytes.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RewrapStreamKeyEntry")
            .field("stream_id", &self.stream_id)
            .field("created_op", &self.created_op)
            .field("key_fingerprint", &self.key_fingerprint)
            .field(
                "envelopes",
                &(self.wrap_set.len() / WRAPPED_STREAM_KEY_LENGTH),
            )
            .finish()
    }
}

impl WireEncode for RewrapStreamKeysRequest {
    fn encoded_size(&self) -> usize {
        4 + self
            .entries
            .iter()
            .map(|entry| ENTRY_FIXED_SIZE + entry.wrap_set.len())
            .sum::<usize>()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.entries.len() as u32);
        for entry in &self.entries {
            buf.put_u32_le(entry.stream_id);
            buf.put_u64_le(entry.created_op);
            buf.put_slice(&entry.key_fingerprint);
            buf.put_u8(entry.wrap_set.len() as u8);
            buf.put_slice(&entry.wrap_set);
        }
    }
}

impl WireDecode for RewrapStreamKeysRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let count = read_u32_le(buf, 0)? as usize;
        let mut position = 4;
        // Each entry takes at least its fixed part plus one envelope, so a
        // count the body cannot hold is refused before allocating for it.
        let minimum = ENTRY_FIXED_SIZE + WRAPPED_STREAM_KEY_LENGTH;
        if count > buf.len().saturating_sub(position) / minimum {
            return Err(validation("rewrap entry count exceeds the body"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let stream_id = read_u32_le(buf, position)?;
            let created_op = read_u64_le(buf, position + 4)?;
            let fingerprint = read_bytes(buf, position + 12, KEY_FINGERPRINT_LENGTH)?;
            let set_length = usize::from(read_u8(buf, position + 20)?);
            if set_length == 0
                || !set_length.is_multiple_of(WRAPPED_STREAM_KEY_LENGTH)
                || set_length > MAX_WRAP_SET_ENVELOPES * WRAPPED_STREAM_KEY_LENGTH
            {
                return Err(validation(
                    "rewrap set length is not one to three envelopes",
                ));
            }
            let wrap_set = read_bytes(buf, position + ENTRY_FIXED_SIZE, set_length)?;
            let mut key_fingerprint = [0u8; KEY_FINGERPRINT_LENGTH];
            key_fingerprint.copy_from_slice(fingerprint);
            entries.push(RewrapStreamKeyEntry {
                stream_id,
                created_op,
                key_fingerprint,
                wrap_set: Bytes::copy_from_slice(wrap_set),
            });
            position += ENTRY_FIXED_SIZE + set_length;
        }
        if position != buf.len() {
            return Err(validation("trailing bytes after the rewrap entries"));
        }
        Ok((Self { entries }, position))
    }
}

const fn validation(reason: &'static str) -> WireError {
    WireError::Validation(Cow::Borrowed(reason))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(stream_id: u32, envelopes: usize) -> RewrapStreamKeyEntry {
        RewrapStreamKeyEntry {
            stream_id,
            created_op: 7,
            key_fingerprint: [0xAB; KEY_FINGERPRINT_LENGTH],
            wrap_set: Bytes::from(vec![0x5A; envelopes * WRAPPED_STREAM_KEY_LENGTH]),
        }
    }

    #[test]
    fn given_rewrap_entries_when_roundtripping_should_match_golden_bytes() {
        let single = RewrapStreamKeysRequest {
            entries: vec![entry(3, 1)],
        };
        let bytes = single.to_bytes();
        let mut golden = vec![1, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0];
        golden.extend_from_slice(&[0xAB; KEY_FINGERPRINT_LENGTH]);
        golden.push(78);
        golden.extend_from_slice(&[0x5A; WRAPPED_STREAM_KEY_LENGTH]);
        assert_eq!(bytes.as_ref(), golden.as_slice());
        assert_eq!(single.encoded_size(), golden.len());

        let several = RewrapStreamKeysRequest {
            entries: vec![entry(1, 2), entry(2, 3), entry(9, 1)],
        };
        let bytes = several.to_bytes();
        let (decoded, consumed) = RewrapStreamKeysRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, several);
    }

    #[test]
    fn given_malformed_rewrap_body_when_decoding_should_reject_without_panicking() {
        let bytes = RewrapStreamKeysRequest {
            entries: vec![entry(1, 1), entry(2, 2)],
        }
        .to_bytes();
        for length in 0..bytes.len() {
            assert!(
                RewrapStreamKeysRequest::decode(&bytes[..length]).is_err(),
                "a body cut at {length} must be refused"
            );
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(RewrapStreamKeysRequest::decode(&trailing).is_err());

        let set_length_at = 4 + 4 + 8 + KEY_FINGERPRINT_LENGTH;
        for invalid in [0u8, 77, 255] {
            let mut body = RewrapStreamKeysRequest {
                entries: vec![entry(1, 1)],
            }
            .to_bytes()
            .to_vec();
            body[set_length_at] = invalid;
            assert!(RewrapStreamKeysRequest::decode(&body).is_err(), "{invalid}");
        }
        let oversized_count = [0xFF, 0xFF, 0xFF, 0xFF];
        assert!(RewrapStreamKeysRequest::decode(&oversized_count).is_err());
    }

    #[test]
    fn given_rewrap_request_when_debugging_should_not_print_wrapped_keys() {
        let rendered = format!(
            "{:?}",
            RewrapStreamKeysRequest {
                entries: vec![entry(1, 2)],
            }
        );
        assert!(
            !rendered.contains("ZZ") && !rendered.contains("90"),
            "{rendered}"
        );
        assert!(rendered.contains("envelopes: 2"), "{rendered}");
    }
}
