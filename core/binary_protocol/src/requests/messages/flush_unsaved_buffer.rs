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
use crate::WireIdentifier;
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le};
use bytes::{BufMut, BytesMut};

/// `FlushUnsavedBuffer` request.
///
/// Wire format: `[stream_id][topic_id][partition_id:4 LE][fsync:1]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushUnsavedBufferRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partition_id: u32,
    pub fsync: bool,
}

impl WireEncode for FlushUnsavedBufferRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.topic_id.encoded_size() + 4 + 1
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u32_le(self.partition_id);
        buf.put_u8(u8::from(self.fsync));
    }
}

impl WireDecode for FlushUnsavedBufferRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut pos = 0;
        let (stream_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let partition_id = read_u32_le(buf, pos)?;
        pos += 4;
        let fsync = read_u8(buf, pos)? != 0;
        pos += 1;
        Ok((
            Self {
                stream_id,
                topic_id,
                partition_id,
                fsync,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = FlushUnsavedBufferRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            partition_id: 3,
            fsync: true,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = FlushUnsavedBufferRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_identifiers() {
        let req = FlushUnsavedBufferRequest {
            stream_id: WireIdentifier::named("stream-1").unwrap(),
            topic_id: WireIdentifier::named("topic-1").unwrap(),
            partition_id: 42,
            fsync: false,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = FlushUnsavedBufferRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn fsync_flag_encoding() {
        let req_true = FlushUnsavedBufferRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: 0,
            fsync: true,
        };
        let req_false = FlushUnsavedBufferRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: 0,
            fsync: false,
        };
        let bytes_true = req_true.to_bytes();
        let bytes_false = req_false.to_bytes();
        assert_eq!(*bytes_true.last().unwrap(), 1);
        assert_eq!(*bytes_false.last().unwrap(), 0);
    }

    #[test]
    fn truncated_returns_error() {
        let req = FlushUnsavedBufferRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            partition_id: 3,
            fsync: true,
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                FlushUnsavedBufferRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_compat_byte_layout() {
        let req = FlushUnsavedBufferRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            partition_id: 3,
            fsync: true,
        };
        let bytes = req.to_bytes();
        // stream_id: [1, 4, 1, 0, 0, 0] + topic_id: [1, 4, 2, 0, 0, 0]
        // + partition_id: [3, 0, 0, 0] + fsync: [1]
        assert_eq!(
            &bytes[..],
            &[1, 4, 1, 0, 0, 0, 1, 4, 2, 0, 0, 0, 3, 0, 0, 0, 1]
        );
    }
}
