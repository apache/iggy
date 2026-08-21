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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, Bytes, BytesMut};

/// One catalog entry in a `DescribeOptions` response.
///
/// Wire format:
/// ```text
/// [key_len:u8][key:N][kind:u8]
/// [default_len:u32][default bytes][description_len:u32][description]
/// ```
///
/// `kind` is this key's canonical header kind code: what the server encodes its
/// default under, and what a value set at create is stored as, since create
/// admission re-encodes the block from its own parse (a `String` value parsed by
/// the same rules as a config file value is accepted and canonicalized). An
/// update stores the client's bytes verbatim and is the exception. `default` is
/// that default in `kind`'s encoding, empty when the key has none, and is a
/// build constant rather than a per-node value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionDescriptor {
    pub key: WireName,
    pub kind: u8,
    pub default_value: Bytes,
    pub description: String,
}

impl WireEncode for OptionDescriptor {
    fn encoded_size(&self) -> usize {
        self.key.encoded_size() + 1 + 4 + self.default_value.len() + 4 + self.description.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        buf.put_u8(self.kind);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.default_value.len() as u32);
        buf.put_slice(&self.default_value);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.description.len() as u32);
        buf.put_slice(self.description.as_bytes());
    }
}

impl WireDecode for OptionDescriptor {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (key, mut pos) = WireName::decode(buf)?;
        let kind = read_u8(buf, pos)?;
        pos += 1;
        let default_len = read_u32_le(buf, pos)? as usize;
        pos += 4;
        // `checked_add`: on a 32-bit target a wire-supplied length wraps, the
        // truncation guard below then passes, and the slice panics instead.
        let default_end = pos
            .checked_add(default_len)
            .ok_or_else(|| WireError::UnexpectedEof {
                offset: pos,
                need: default_len,
                have: buf.len().saturating_sub(pos),
            })?;
        if buf.len() < default_end {
            return Err(WireError::UnexpectedEof {
                offset: pos,
                need: default_len,
                have: buf.len().saturating_sub(pos),
            });
        }
        let default_value = Bytes::copy_from_slice(&buf[pos..default_end]);
        pos = default_end;
        let description_len = read_u32_le(buf, pos)? as usize;
        pos += 4;
        let description_end =
            pos.checked_add(description_len)
                .ok_or_else(|| WireError::UnexpectedEof {
                    offset: pos,
                    need: description_len,
                    have: buf.len().saturating_sub(pos),
                })?;
        if buf.len() < description_end {
            return Err(WireError::UnexpectedEof {
                offset: pos,
                need: description_len,
                have: buf.len().saturating_sub(pos),
            });
        }
        let description = String::from_utf8_lossy(&buf[pos..description_end]).into_owned();
        pos = description_end;

        Ok((
            Self {
                key,
                kind,
                default_value,
                description,
            },
            pos,
        ))
    }
}

/// `DescribeOptions` response: count-prefixed catalog entries.
///
/// Wire format: `[count:u32][OptionDescriptor]*`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeOptionsResponse {
    pub entries: Vec<OptionDescriptor>,
}

impl WireEncode for DescribeOptionsResponse {
    fn encoded_size(&self) -> usize {
        4 + self
            .entries
            .iter()
            .map(WireEncode::encoded_size)
            .sum::<usize>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.entries.len() as u32);
        for entry in &self.entries {
            entry.encode(buf);
        }
    }
}

/// Smallest a descriptor can encode as: a one-character `WireName` (a length
/// byte plus at least one byte of name), a kind byte, and empty
/// length-prefixed default and description.
const MIN_DESCRIPTOR_SIZE: usize = 2 + 1 + 4 + 4;

impl WireDecode for DescribeOptionsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let count = read_u32_le(buf, 0)? as usize;
        let mut pos = 4;
        let mut entries = Vec::with_capacity(crate::codec::bounded_capacity(
            count,
            buf.len().saturating_sub(pos),
            MIN_DESCRIPTOR_SIZE,
        ));
        for _ in 0..count {
            let (entry, consumed) = OptionDescriptor::decode(&buf[pos..])?;
            pos += consumed;
            entries.push(entry);
        }
        Ok((Self { entries }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DescribeOptionsResponse {
        DescribeOptionsResponse {
            entries: vec![
                OptionDescriptor {
                    key: WireName::new("partitions_count").unwrap(),
                    kind: 11,
                    default_value: Bytes::copy_from_slice(&1u32.to_le_bytes()),
                    description: "Number of partitions to create".to_string(),
                },
                OptionDescriptor {
                    key: WireName::new("max_topic_size").unwrap(),
                    kind: 12,
                    default_value: Bytes::new(),
                    description: String::new(),
                },
            ],
        }
    }

    #[test]
    fn roundtrip() {
        let resp = sample();
        let bytes = resp.to_bytes();
        let (decoded, consumed) = DescribeOptionsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_empty() {
        let resp = DescribeOptionsResponse { entries: vec![] };
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), 4);
        let (decoded, consumed) = DescribeOptionsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let bytes = sample().to_bytes();
        for i in 5..bytes.len() {
            assert!(
                DescribeOptionsResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
