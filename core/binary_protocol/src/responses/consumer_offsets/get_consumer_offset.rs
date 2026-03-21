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
use bytes::{BufMut, BytesMut};

/// `GetConsumerOffset` response.
///
/// Wire format (20 bytes fixed):
/// ```text
/// [partition_id:4][current_offset:8][stored_offset:8]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerOffsetResponse {
    pub partition_id: u32,
    pub current_offset: u64,
    pub stored_offset: u64,
}

impl ConsumerOffsetResponse {
    const FIXED_SIZE: usize = 4 + 8 + 8; // 20
}

impl WireEncode for ConsumerOffsetResponse {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.partition_id);
        buf.put_u64_le(self.current_offset);
        buf.put_u64_le(self.stored_offset);
    }
}

impl WireDecode for ConsumerOffsetResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let partition_id = read_u32_le(buf, 0)?;
        let current_offset = read_u64_le(buf, 4)?;
        let stored_offset = read_u64_le(buf, 12)?;

        Ok((
            Self {
                partition_id,
                current_offset,
                stored_offset,
            },
            Self::FIXED_SIZE,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ConsumerOffsetResponse {
        ConsumerOffsetResponse {
            partition_id: 1,
            current_offset: 1000,
            stored_offset: 500,
        }
    }

    #[test]
    fn roundtrip() {
        let resp = sample();
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), ConsumerOffsetResponse::FIXED_SIZE);
        let (decoded, consumed) = ConsumerOffsetResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, ConsumerOffsetResponse::FIXED_SIZE);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = sample();
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ConsumerOffsetResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn boundary_values() {
        let resp = ConsumerOffsetResponse {
            partition_id: u32::MAX,
            current_offset: u64::MAX,
            stored_offset: 0,
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = ConsumerOffsetResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, ConsumerOffsetResponse::FIXED_SIZE);
        assert_eq!(decoded, resp);
    }
}
