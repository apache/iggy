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
use crate::codec::{WireDecode, WireEncode, read_u8};
use bytes::{BufMut, BytesMut};

/// `GetSnapshot` request.
///
/// Wire format:
/// ```text
/// [compression:1][types_count:1][snapshot_type:1]*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetSnapshotRequest {
    /// Compression method code (1=Stored, 2=Deflated, 3=Bzip2, 4=Zstd, 5=Lzma, 6=Xz).
    pub compression: u8,
    /// Snapshot type codes (1=FilesystemOverview, 2=ProcessList, 3=ResourceUsage,
    /// 4=Test, 5=ServerLogs, 6=ServerConfig, 100=All).
    pub snapshot_types: Vec<u8>,
}

impl WireEncode for GetSnapshotRequest {
    fn encoded_size(&self) -> usize {
        1 + 1 + self.snapshot_types.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.compression);
        let count =
            u8::try_from(self.snapshot_types.len()).expect("snapshot_types count exceeds u8::MAX");
        buf.put_u8(count);
        for &code in &self.snapshot_types {
            buf.put_u8(code);
        }
    }
}

impl WireDecode for GetSnapshotRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let compression = read_u8(buf, 0)?;
        let types_count = read_u8(buf, 1)? as usize;
        let mut pos = 2;
        let mut snapshot_types = Vec::with_capacity(types_count);
        for _ in 0..types_count {
            snapshot_types.push(read_u8(buf, pos)?);
            pos += 1;
        }
        Ok((
            Self {
                compression,
                snapshot_types,
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
        let req = GetSnapshotRequest {
            compression: 2,
            snapshot_types: vec![1, 5],
        };
        let bytes = req.to_bytes();
        assert_eq!(bytes.len(), 4); // 1 + 1 + 2
        let (decoded, consumed) = GetSnapshotRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_empty_types() {
        let req = GetSnapshotRequest {
            compression: 1,
            snapshot_types: vec![],
        };
        let bytes = req.to_bytes();
        assert_eq!(bytes.len(), 2);
        let (decoded, consumed) = GetSnapshotRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, 2);
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_all_type() {
        let req = GetSnapshotRequest {
            compression: 2,
            snapshot_types: vec![100],
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = GetSnapshotRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_returns_error() {
        let req = GetSnapshotRequest {
            compression: 2,
            snapshot_types: vec![1, 2, 3],
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                GetSnapshotRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
