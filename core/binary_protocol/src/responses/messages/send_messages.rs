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

use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use crate::error::WireError;
use bytes::{BufMut, BytesMut};

/// Size of the `SendMessages` response: `base_offset(8) + partition_id(4)`.
const SEND_MESSAGES_RESPONSE_SIZE: usize = 12;

/// Response for a successful `SendMessages` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendMessagesResponse {
    pub base_offset: u64,
    pub partition_id: u32,
}

impl WireEncode for SendMessagesResponse {
    fn encoded_size(&self) -> usize {
        SEND_MESSAGES_RESPONSE_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.base_offset);
        buf.put_u32_le(self.partition_id);
    }
}

impl WireDecode for SendMessagesResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let base_offset = read_u64_le(buf, 0)?;
        let partition_id = read_u32_le(buf, 8)?;
        Ok((
            Self {
                base_offset,
                partition_id,
            },
            SEND_MESSAGES_RESPONSE_SIZE,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let resp = SendMessagesResponse {
            base_offset: 12345,
            partition_id: 7,
        };
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), SEND_MESSAGES_RESPONSE_SIZE);
        let (decoded, consumed) = SendMessagesResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, SEND_MESSAGES_RESPONSE_SIZE);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = SendMessagesResponse {
            base_offset: 1,
            partition_id: 1,
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                SendMessagesResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn encoded_size_matches_output() {
        let resp = SendMessagesResponse {
            base_offset: u64::MAX,
            partition_id: u32::MAX,
        };
        assert_eq!(resp.encoded_size(), resp.to_bytes().len());
    }
}
