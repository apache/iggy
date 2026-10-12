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

use bytes::{BufMut, BytesMut};

use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::{WireError, WireIdentifier};

/// Partition context discovery authorized by send permission, without exposing topic details.
/// The response is a `PartitionContext` captured before a new send is admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetSendContextRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partition_id: u32,
}

impl WireEncode for GetSendContextRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.topic_id.encoded_size() + size_of::<u32>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u32_le(self.partition_id);
    }
}

impl WireDecode for GetSendContextRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, stream_size) = WireIdentifier::decode(buf)?;
        let (topic_id, topic_size) = WireIdentifier::decode(&buf[stream_size..])?;
        let position = stream_size + topic_size;
        let partition_id = read_u32_le(buf, position)?;
        Ok((
            Self {
                stream_id,
                topic_id,
                partition_id,
            },
            position + size_of::<u32>(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_send_context_request_when_encoded_should_preserve_named_and_numeric_targets() {
        for (stream_id, topic_id) in [
            (WireIdentifier::numeric(3), WireIdentifier::numeric(7)),
            (
                WireIdentifier::named("events").unwrap(),
                WireIdentifier::named("orders").unwrap(),
            ),
        ] {
            let request = GetSendContextRequest {
                stream_id,
                topic_id,
                partition_id: 2,
            };
            let bytes = request.to_bytes();
            assert_eq!(bytes.len(), request.encoded_size());
            assert_eq!(GetSendContextRequest::decode_from(&bytes).unwrap(), request);
            for length in 0..bytes.len() {
                assert!(
                    GetSendContextRequest::decode_from(&bytes[..length]).is_err(),
                    "accepted a request truncated at {length} bytes"
                );
            }
        }
    }
}
