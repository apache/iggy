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
use crate::responses::clients::client_response::{ClientResponse, ConsumerGroupInfoResponse};
use bytes::BytesMut;

/// `GetClient` response: client header followed by consumer group entries.
///
/// Wire format:
/// ```text
/// [ClientResponse][ConsumerGroupInfoResponse]*
/// ```
///
/// The number of consumer group entries equals `client.consumer_groups_count`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientDetailsResponse {
    pub client: ClientResponse,
    pub consumer_groups: Vec<ConsumerGroupInfoResponse>,
}

impl WireEncode for ClientDetailsResponse {
    fn encoded_size(&self) -> usize {
        self.client.encoded_size()
            + self
                .consumer_groups
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.client.encode(buf);
        for group in &self.consumer_groups {
            group.encode(buf);
        }
    }
}

impl WireDecode for ClientDetailsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (client, mut pos) = ClientResponse::decode(buf)?;
        let count = client.consumer_groups_count as usize;
        let mut consumer_groups = Vec::with_capacity(count);
        for _ in 0..count {
            let (group, consumed) = ConsumerGroupInfoResponse::decode(&buf[pos..])?;
            pos += consumed;
            consumer_groups.push(group);
        }

        Ok((
            Self {
                client,
                consumer_groups,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_client(groups_count: u32) -> ClientResponse {
        ClientResponse {
            client_id: 1,
            user_id: 10,
            transport: 1,
            address: "127.0.0.1:8080".to_string(),
            consumer_groups_count: groups_count,
        }
    }

    #[test]
    fn roundtrip_no_groups() {
        let resp = ClientDetailsResponse {
            client: sample_client(0),
            consumer_groups: vec![],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = ClientDetailsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_with_groups() {
        let resp = ClientDetailsResponse {
            client: sample_client(2),
            consumer_groups: vec![
                ConsumerGroupInfoResponse {
                    stream_id: 1,
                    topic_id: 2,
                    group_id: 3,
                },
                ConsumerGroupInfoResponse {
                    stream_id: 4,
                    topic_id: 5,
                    group_id: 6,
                },
            ],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = ClientDetailsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = ClientDetailsResponse {
            client: sample_client(1),
            consumer_groups: vec![ConsumerGroupInfoResponse {
                stream_id: 1,
                topic_id: 2,
                group_id: 3,
            }],
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ClientDetailsResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
