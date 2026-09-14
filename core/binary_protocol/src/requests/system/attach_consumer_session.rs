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
use crate::codec::{WireDecode, WireEncode, read_u64_le, read_u128_le};
use bytes::{BufMut, BytesMut};

/// Use a coordinator's consumer-group identity on an independently authenticated
/// data connection.
///
/// The server verifies ownership and the exact parent epoch.
/// This grants neither a new group membership nor ownership of the parent session.
///
/// Wire format: `[client_id:16 LE][session:8 LE][metadata_watermark:8 LE]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttachConsumerSessionRequest {
    pub client_id: u128,
    pub session: u64,
    pub metadata_watermark: u64,
}

impl WireEncode for AttachConsumerSessionRequest {
    fn encoded_size(&self) -> usize {
        32
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u128_le(self.client_id);
        buf.put_u64_le(self.session);
        buf.put_u64_le(self.metadata_watermark);
    }
}

impl WireDecode for AttachConsumerSessionRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        Ok((
            Self {
                client_id: read_u128_le(buf, 0)?,
                session: read_u64_le(buf, 16)?,
                metadata_watermark: read_u64_le(buf, 24)?,
            },
            32,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_reject_truncation() {
        let request = AttachConsumerSessionRequest {
            client_id: u128::MAX,
            session: 7,
            metadata_watermark: 11,
        };
        let bytes = request.to_bytes();
        assert_eq!(
            AttachConsumerSessionRequest::decode(&bytes).unwrap(),
            (request, bytes.len())
        );
        for end in 0..bytes.len() {
            assert!(
                AttachConsumerSessionRequest::decode(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
    }
}
