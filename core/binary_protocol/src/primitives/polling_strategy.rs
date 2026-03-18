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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u64_le};
use bytes::{BufMut, BytesMut};

const KIND_OFFSET: u8 = 1;
const KIND_TIMESTAMP: u8 = 2;
const KIND_FIRST: u8 = 3;
const KIND_LAST: u8 = 4;
const KIND_NEXT: u8 = 5;

/// Polling strategy for consuming messages.
///
/// Wire format: `[kind:1][value:8]` = 9 bytes fixed.
/// - Offset(1):    resume from a specific offset
/// - Timestamp(2): resume from a specific timestamp (microseconds)
/// - First(3):     start from the beginning
/// - Last(4):      start from the end
/// - Next(5):      continue from last stored offset
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WirePollingStrategy {
    pub kind: u8,
    pub value: u64,
}

impl WirePollingStrategy {
    #[must_use]
    pub const fn offset(value: u64) -> Self {
        Self {
            kind: KIND_OFFSET,
            value,
        }
    }

    #[must_use]
    pub const fn timestamp(value: u64) -> Self {
        Self {
            kind: KIND_TIMESTAMP,
            value,
        }
    }

    #[must_use]
    pub const fn first() -> Self {
        Self {
            kind: KIND_FIRST,
            value: 0,
        }
    }

    #[must_use]
    pub const fn last() -> Self {
        Self {
            kind: KIND_LAST,
            value: 0,
        }
    }

    #[must_use]
    pub const fn next() -> Self {
        Self {
            kind: KIND_NEXT,
            value: 0,
        }
    }
}

impl WireEncode for WirePollingStrategy {
    fn encoded_size(&self) -> usize {
        9
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.kind);
        buf.put_u64_le(self.value);
    }
}

impl WireDecode for WirePollingStrategy {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let kind = read_u8(buf, 0)?;
        match kind {
            KIND_OFFSET | KIND_TIMESTAMP | KIND_FIRST | KIND_LAST | KIND_NEXT => {}
            _ => {
                return Err(WireError::UnknownDiscriminant {
                    type_name: "WirePollingStrategy",
                    value: kind,
                    offset: 0,
                });
            }
        }
        let value = read_u64_le(buf, 1)?;
        Ok((Self { kind, value }, 9))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_offset() {
        let s = WirePollingStrategy::offset(100);
        let bytes = s.to_bytes();
        assert_eq!(bytes.len(), 9);
        let (decoded, consumed) = WirePollingStrategy::decode(&bytes).unwrap();
        assert_eq!(consumed, 9);
        assert_eq!(decoded, s);
    }

    #[test]
    fn roundtrip_timestamp() {
        let s = WirePollingStrategy::timestamp(1_700_000_000_000);
        let bytes = s.to_bytes();
        let (decoded, consumed) = WirePollingStrategy::decode(&bytes).unwrap();
        assert_eq!(consumed, 9);
        assert_eq!(decoded, s);
    }

    #[test]
    fn roundtrip_first() {
        let s = WirePollingStrategy::first();
        let bytes = s.to_bytes();
        let (decoded, _) = WirePollingStrategy::decode(&bytes).unwrap();
        assert_eq!(decoded, s);
        assert_eq!(decoded.value, 0);
    }

    #[test]
    fn roundtrip_last() {
        let s = WirePollingStrategy::last();
        let bytes = s.to_bytes();
        let (decoded, _) = WirePollingStrategy::decode(&bytes).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn roundtrip_next() {
        let s = WirePollingStrategy::next();
        let bytes = s.to_bytes();
        let (decoded, _) = WirePollingStrategy::decode(&bytes).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn unknown_kind_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF);
        buf.put_u64_le(0);
        assert!(WirePollingStrategy::decode(&buf).is_err());
    }

    #[test]
    fn kind_zero_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(0);
        buf.put_u64_le(0);
        assert!(WirePollingStrategy::decode(&buf).is_err());
    }

    #[test]
    fn truncated_buffer() {
        let s = WirePollingStrategy::offset(1);
        let bytes = s.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                WirePollingStrategy::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_compat_layout() {
        let s = WirePollingStrategy::offset(1);
        let bytes = s.to_bytes();
        assert_eq!(bytes[0], KIND_OFFSET);
        assert_eq!(u64::from_le_bytes(bytes[1..9].try_into().unwrap()), 1);
    }
}
