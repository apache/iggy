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
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use bytes::{BufMut, BytesMut};

/// `GetClient` request. Wire format: `[client_id:u32_le]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetClientRequest {
    pub client_id: u32,
}

impl WireEncode for GetClientRequest {
    fn encoded_size(&self) -> usize {
        4
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.client_id);
    }
}

impl WireDecode for GetClientRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let client_id = read_u32_le(buf, 0)?;
        Ok((Self { client_id }, 4))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = GetClientRequest { client_id: 42 };
        let bytes = req.to_bytes();
        let (decoded, consumed) = GetClientRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_zero() {
        let req = GetClientRequest { client_id: 0 };
        let bytes = req.to_bytes();
        let (decoded, consumed) = GetClientRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, 4);
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_max() {
        let req = GetClientRequest {
            client_id: u32::MAX,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = GetClientRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, 4);
        assert_eq!(decoded, req);
    }

    #[test]
    fn wire_compat_byte_layout() {
        let req = GetClientRequest { client_id: 1 };
        let bytes = req.to_bytes();
        assert_eq!(&bytes[..], &[1, 0, 0, 0]);
    }

    #[test]
    fn truncated_returns_error() {
        let req = GetClientRequest { client_id: 1 };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                GetClientRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
