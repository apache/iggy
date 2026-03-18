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
use crate::responses::users::user_response::UserResponse;
use bytes::BytesMut;

/// `GetUsers` response: sequential user headers.
///
/// Wire format:
/// ```text
/// [UserResponse]*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetUsersResponse {
    pub users: Vec<UserResponse>,
}

impl WireEncode for GetUsersResponse {
    fn encoded_size(&self) -> usize {
        self.users.iter().map(WireEncode::encoded_size).sum()
    }

    fn encode(&self, buf: &mut BytesMut) {
        for user in &self.users {
            user.encode(buf);
        }
    }
}

impl WireDecode for GetUsersResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut users = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (user, consumed) = UserResponse::decode(&buf[pos..])?;
            pos += consumed;
            users.push(user);
        }
        Ok((Self { users }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;

    #[test]
    fn roundtrip_empty() {
        let resp = GetUsersResponse { users: vec![] };
        let bytes = resp.to_bytes();
        assert!(bytes.is_empty());
        let (decoded, consumed) = GetUsersResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_multiple() {
        let resp = GetUsersResponse {
            users: vec![
                UserResponse {
                    id: 1,
                    created_at: 100,
                    status: 1,
                    username: WireName::new("admin").unwrap(),
                },
                UserResponse {
                    id: 2,
                    created_at: 200,
                    status: 2,
                    username: WireName::new("alice").unwrap(),
                },
            ],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetUsersResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }
}
