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
use bytes::{BufMut, BytesMut};

/// `GetSnapshot` response: raw snapshot data (ZIP archive).
///
/// The server produces a ZIP file containing diagnostic data.
/// The payload is the complete archive with no additional framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetSnapshotResponse {
    pub data: Vec<u8>,
}

impl WireEncode for GetSnapshotResponse {
    fn encoded_size(&self) -> usize {
        self.data.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.data);
    }
}

impl WireDecode for GetSnapshotResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        Ok((Self { data: buf.to_vec() }, buf.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let resp = GetSnapshotResponse {
            data: vec![0x50, 0x4B, 0x03, 0x04, 1, 2, 3, 4],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetSnapshotResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn empty_snapshot() {
        let resp = GetSnapshotResponse { data: vec![] };
        let bytes = resp.to_bytes();
        assert!(bytes.is_empty());
        let (decoded, consumed) = GetSnapshotResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(decoded, resp);
    }
}
