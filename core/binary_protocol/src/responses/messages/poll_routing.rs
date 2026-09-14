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
use crate::codec::{WireDecode, WireEncode};
use crate::requests::system::AttachConsumerSessionRequest;
use crate::responses::system::get_cluster_metadata::ClusterNodeResponse;
use bytes::BytesMut;

/// Routing for the partition selected by a `PollMessagesRequest`.
///
/// The primary is a hint: the receiving partition still checks its role before
/// accepting a poll. The attachment carries the coordinator's authenticated
/// identity and metadata floor, never credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollRoutingResponse {
    pub consumer_session: AttachConsumerSessionRequest,
    pub primary: ClusterNodeResponse,
}

impl WireEncode for PollRoutingResponse {
    fn encoded_size(&self) -> usize {
        self.consumer_session.encoded_size() + self.primary.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.consumer_session.encode(buf);
        self.primary.encode(buf);
    }
}

impl WireDecode for PollRoutingResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (consumer_session, consumed) = AttachConsumerSessionRequest::decode(buf)?;
        let (primary, node_size) = ClusterNodeResponse::decode(&buf[consumed..])?;
        Ok((
            Self {
                consumer_session,
                primary,
            },
            consumed + node_size,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_reject_truncation() {
        let response = PollRoutingResponse {
            consumer_session: AttachConsumerSessionRequest {
                client_id: u128::MAX,
                session: 7,
                metadata_watermark: 11,
            },
            primary: ClusterNodeResponse {
                name: "node-1".to_owned(),
                ip: "::1".to_owned(),
                tcp_port: 8090,
                quic_port: 8091,
                http_port: 0,
                websocket_port: 3000,
                role: 1,
                status: 1,
            },
        };
        let bytes = response.to_bytes();
        assert_eq!(
            PollRoutingResponse::decode(&bytes).unwrap(),
            (response, bytes.len())
        );
        for end in 0..bytes.len() {
            assert!(
                PollRoutingResponse::decode(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
    }
}
