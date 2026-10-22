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
use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use bytes::{BufMut, BytesMut};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedPartitionAssignment {
    pub partition_id: u32,
    pub consensus_group_id: u64,
    /// Committed placement seed, sealed by the request body checksum and never
    /// restamped when a later metadata view retransmits the create.
    pub initial_view: u32,
}

impl CreatedPartitionAssignment {
    /// Fixed wire size: partition id, consensus group id and initial view.
    pub const ENCODED_SIZE: usize = 16;
}

impl WireEncode for CreatedPartitionAssignment {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.partition_id);
        buf.put_u64_le(self.consensus_group_id);
        buf.put_u32_le(self.initial_view);
    }
}

impl WireDecode for CreatedPartitionAssignment {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        if buf.len() < Self::ENCODED_SIZE {
            return Err(WireError::UnexpectedEof {
                offset: 0,
                need: Self::ENCODED_SIZE,
                have: buf.len(),
            });
        }

        Ok((
            Self {
                partition_id: read_u32_le(buf, 0)?,
                consensus_group_id: read_u64_le(buf, 4)?,
                initial_view: read_u32_le(buf, 12)?,
            },
            Self::ENCODED_SIZE,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::CreatedPartitionAssignment;
    use crate::codec::{WireDecode, WireEncode};

    #[test]
    fn roundtrip() {
        let request = CreatedPartitionAssignment {
            partition_id: 0x0102_0304,
            consensus_group_id: 0x0506_0708_090A_0B0C,
            initial_view: 0x0D0E_0F10,
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreatedPartitionAssignment::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
        assert_eq!(bytes.len(), CreatedPartitionAssignment::ENCODED_SIZE);
        assert_eq!(
            bytes.as_ref(),
            &[4, 3, 2, 1, 12, 11, 10, 9, 8, 7, 6, 5, 16, 15, 14, 13]
        );
        for truncated in 0..bytes.len() {
            assert!(CreatedPartitionAssignment::decode(&bytes[..truncated]).is_err());
        }
    }
}
