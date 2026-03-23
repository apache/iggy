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
use crate::codec::{WireDecode, WireEncode, read_str, read_u8, read_u32_le};
use bytes::{BufMut, BytesMut};

/// Consumer group membership entry.
///
/// Wire format (12 bytes):
/// ```text
/// [stream_id:4][topic_id:4][group_id:4]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupInfoResponse {
    pub stream_id: u32,
    pub topic_id: u32,
    pub group_id: u32,
}

impl ConsumerGroupInfoResponse {
    const SIZE: usize = 12;
}

impl WireEncode for ConsumerGroupInfoResponse {
    fn encoded_size(&self) -> usize {
        Self::SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.stream_id);
        buf.put_u32_le(self.topic_id);
        buf.put_u32_le(self.group_id);
    }
}

impl WireDecode for ConsumerGroupInfoResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let stream_id = read_u32_le(buf, 0)?;
        let topic_id = read_u32_le(buf, 4)?;
        let group_id = read_u32_le(buf, 8)?;

        Ok((
            Self {
                stream_id,
                topic_id,
                group_id,
            },
            Self::SIZE,
        ))
    }
}

/// Client header on the wire. Used in both single-client and multi-client responses.
///
/// Wire format:
/// ```text
/// [client_id:4][user_id:4][transport:1][address_len:4][address:N][consumer_groups_count:4]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientResponse {
    pub client_id: u32,
    pub user_id: u32,
    pub transport: u8,
    pub address: String,
    pub consumer_groups_count: u32,
}

impl ClientResponse {
    const FIXED_SIZE: usize = 4 + 4 + 1 + 4 + 4; // 17
}

impl WireEncode for ClientResponse {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE + self.address.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.client_id);
        buf.put_u32_le(self.user_id);
        buf.put_u8(self.transport);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.address.len() as u32);
        buf.put_slice(self.address.as_bytes());
        buf.put_u32_le(self.consumer_groups_count);
    }
}

impl WireDecode for ClientResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let client_id = read_u32_le(buf, 0)?;
        let user_id = read_u32_le(buf, 4)?;
        let transport = read_u8(buf, 8)?;
        let address_len = read_u32_le(buf, 9)? as usize;
        let address = read_str(buf, 13, address_len)?;
        let consumer_groups_count = read_u32_le(buf, 13 + address_len)?;
        let consumed = 17 + address_len;

        Ok((
            Self {
                client_id,
                user_id,
                transport,
                address,
                consumer_groups_count,
            },
            consumed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_client() -> ClientResponse {
        ClientResponse {
            client_id: 1,
            user_id: 10,
            transport: 1,
            address: "127.0.0.1:8080".to_string(),
            consumer_groups_count: 2,
        }
    }

    #[test]
    fn consumer_group_info_roundtrip() {
        let info = ConsumerGroupInfoResponse {
            stream_id: 1,
            topic_id: 2,
            group_id: 3,
        };
        let bytes = info.to_bytes();
        assert_eq!(bytes.len(), 12);
        let (decoded, consumed) = ConsumerGroupInfoResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 12);
        assert_eq!(decoded, info);
    }

    #[test]
    fn client_roundtrip() {
        let client = sample_client();
        let bytes = client.to_bytes();
        assert_eq!(bytes.len(), ClientResponse::FIXED_SIZE + 14);
        let (decoded, consumed) = ClientResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, client);
    }

    #[test]
    fn client_truncated_returns_error() {
        let client = sample_client();
        let bytes = client.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ClientResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn client_empty_address() {
        let client = ClientResponse {
            address: String::new(),
            ..sample_client()
        };
        let bytes = client.to_bytes();
        let (decoded, consumed) = ClientResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, client);
    }

    #[test]
    fn multiple_clients_sequential() {
        let c1 = ClientResponse {
            client_id: 1,
            address: "a".to_string(),
            ..sample_client()
        };
        let c2 = ClientResponse {
            client_id: 2,
            address: "bb".to_string(),
            ..sample_client()
        };
        let mut buf = BytesMut::new();
        c1.encode(&mut buf);
        c2.encode(&mut buf);
        let bytes = buf.freeze();

        let (d1, pos1) = ClientResponse::decode(&bytes).unwrap();
        let (d2, pos2) = ClientResponse::decode(&bytes[pos1..]).unwrap();
        assert_eq!(d1, c1);
        assert_eq!(d2, c2);
        assert_eq!(pos1 + pos2, bytes.len());
    }
}
