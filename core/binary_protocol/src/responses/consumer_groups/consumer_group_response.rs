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
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// Consumer group header on the wire.
///
/// Wire format (12 + 1 + `name_len` bytes):
/// ```text
/// [id:4][partitions_count:4][members_count:4][name_len:1][name:N]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupResponse {
    pub id: u32,
    pub partitions_count: u32,
    pub members_count: u32,
    pub name: WireName,
}

impl ConsumerGroupResponse {
    const FIXED_SIZE: usize = 4 + 4 + 4; // 12 (name length prefix is part of WireName)
}

impl WireEncode for ConsumerGroupResponse {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE + self.name.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u32_le(self.partitions_count);
        buf.put_u32_le(self.members_count);
        self.name.encode(buf);
    }
}

impl WireDecode for ConsumerGroupResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let id = read_u32_le(buf, 0)?;
        let partitions_count = read_u32_le(buf, 4)?;
        let members_count = read_u32_le(buf, 8)?;
        let (name, name_consumed) = WireName::decode(&buf[12..])?;
        let consumed = 12 + name_consumed;

        Ok((
            Self {
                id,
                partitions_count,
                members_count,
                name,
            },
            consumed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ConsumerGroupResponse {
        ConsumerGroupResponse {
            id: 1,
            partitions_count: 4,
            members_count: 2,
            name: WireName::new("my-group").unwrap(),
        }
    }

    #[test]
    fn roundtrip() {
        let resp = sample();
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), 12 + 1 + 8);
        let (decoded, consumed) = ConsumerGroupResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = sample();
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ConsumerGroupResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn multiple_sequential() {
        let g1 = ConsumerGroupResponse {
            id: 1,
            name: WireName::new("a").unwrap(),
            ..sample()
        };
        let g2 = ConsumerGroupResponse {
            id: 2,
            name: WireName::new("bb").unwrap(),
            ..sample()
        };
        let mut buf = BytesMut::new();
        g1.encode(&mut buf);
        g2.encode(&mut buf);
        let bytes = buf.freeze();

        let (d1, pos1) = ConsumerGroupResponse::decode(&bytes).unwrap();
        let (d2, pos2) = ConsumerGroupResponse::decode(&bytes[pos1..]).unwrap();
        assert_eq!(d1, g1);
        assert_eq!(d2, g2);
        assert_eq!(pos1 + pos2, bytes.len());
    }
}
