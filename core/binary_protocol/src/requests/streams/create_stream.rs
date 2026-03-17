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
use crate::identifier::WireName;
use bytes::BytesMut;

/// `CreateStream` request. Wire format: `[name_len:1][name:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamRequest {
    pub name: WireName,
}

impl WireEncode for CreateStreamRequest {
    fn encoded_size(&self) -> usize {
        self.name.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.name.encode(buf);
    }
}

impl WireDecode for CreateStreamRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (name, consumed) = WireName::decode(buf)?;
        Ok((Self { name }, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = CreateStreamRequest {
            name: WireName::new("test-stream").unwrap(),
        };
        let bytes = req.to_bytes();
        assert_eq!(bytes.len(), 1 + 11);
        let (decoded, consumed) = CreateStreamRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn empty_name_rejected() {
        let buf = [0u8];
        assert!(CreateStreamRequest::decode(&buf).is_err());
    }

    #[test]
    fn truncated_returns_error() {
        let req = CreateStreamRequest {
            name: WireName::new("test").unwrap(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                CreateStreamRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_compat_byte_layout() {
        let req = CreateStreamRequest {
            name: WireName::new("test").unwrap(),
        };
        let bytes = req.to_bytes();
        assert_eq!(&bytes[..], &[4, b't', b'e', b's', b't']);
    }

    #[test]
    fn too_long_name_rejected_at_construction() {
        let long = "a".repeat(256);
        assert!(WireName::new(long).is_err());
    }
}
