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
use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// Stream header on the wire. Used in both single-stream and multi-stream responses.
///
/// Wire format (33 + `name_len` bytes):
/// ```text
/// [id:4][created_at:8][topics_count:4][size_bytes:8][messages_count:8][name_len:1][name:N]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamResponse {
    pub id: u32,
    pub created_at: u64,
    pub topics_count: u32,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub name: WireName,
}

impl StreamResponse {
    const FIXED_SIZE: usize = 4 + 8 + 4 + 8 + 8 + 1; // 33
}

impl WireEncode for StreamResponse {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE + self.name.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u64_le(self.created_at);
        buf.put_u32_le(self.topics_count);
        buf.put_u64_le(self.size_bytes);
        buf.put_u64_le(self.messages_count);
        self.name.encode(buf);
    }
}

impl WireDecode for StreamResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let id = read_u32_le(buf, 0)?;
        let created_at = read_u64_le(buf, 4)?;
        let topics_count = read_u32_le(buf, 12)?;
        let size_bytes = read_u64_le(buf, 16)?;
        let messages_count = read_u64_le(buf, 24)?;
        let (name, name_consumed) = WireName::decode(&buf[32..])?;
        let consumed = 32 + name_consumed;

        Ok((
            Self {
                id,
                created_at,
                topics_count,
                size_bytes,
                messages_count,
                name,
            },
            consumed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StreamResponse {
        StreamResponse {
            id: 1,
            created_at: 1_710_000_000_000,
            topics_count: 3,
            size_bytes: 1024,
            messages_count: 100,
            name: WireName::new("test-stream").unwrap(),
        }
    }

    #[test]
    fn roundtrip() {
        let resp = sample();
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), StreamResponse::FIXED_SIZE + 11);
        let (decoded, consumed) = StreamResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = sample();
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                StreamResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn multiple_streams_sequential() {
        let s1 = StreamResponse {
            id: 1,
            name: WireName::new("a").unwrap(),
            ..sample()
        };
        let s2 = StreamResponse {
            id: 2,
            name: WireName::new("bb").unwrap(),
            ..sample()
        };
        let mut buf = BytesMut::new();
        s1.encode(&mut buf);
        s2.encode(&mut buf);
        let bytes = buf.freeze();

        let (d1, pos1) = StreamResponse::decode(&bytes).unwrap();
        let (d2, pos2) = StreamResponse::decode(&bytes[pos1..]).unwrap();
        assert_eq!(d1, s1);
        assert_eq!(d2, s2);
        assert_eq!(pos1 + pos2, bytes.len());
    }
}
