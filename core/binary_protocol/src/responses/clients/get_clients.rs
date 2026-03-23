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
use crate::responses::clients::client_response::ClientResponse;
use bytes::BytesMut;

/// `GetClients` response: sequential client headers.
///
/// Wire format:
/// ```text
/// [ClientResponse]*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetClientsResponse {
    pub clients: Vec<ClientResponse>,
}

impl WireEncode for GetClientsResponse {
    fn encoded_size(&self) -> usize {
        self.clients.iter().map(WireEncode::encoded_size).sum()
    }

    fn encode(&self, buf: &mut BytesMut) {
        for client in &self.clients {
            client.encode(buf);
        }
    }
}

impl WireDecode for GetClientsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut clients = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (client, consumed) = ClientResponse::decode(&buf[pos..])?;
            pos += consumed;
            clients.push(client);
        }
        Ok((Self { clients }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty() {
        let resp = GetClientsResponse { clients: vec![] };
        let bytes = resp.to_bytes();
        assert!(bytes.is_empty());
        let (decoded, consumed) = GetClientsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_multiple() {
        let resp = GetClientsResponse {
            clients: vec![
                ClientResponse {
                    client_id: 1,
                    user_id: 10,
                    transport: 1,
                    address: "127.0.0.1:5000".to_string(),
                    consumer_groups_count: 0,
                },
                ClientResponse {
                    client_id: 2,
                    user_id: 20,
                    transport: 2,
                    address: "10.0.0.1:6000".to_string(),
                    consumer_groups_count: 3,
                },
            ],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetClientsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }
}
