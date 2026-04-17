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
}

impl WireEncode for CreatedPartitionAssignment {
    fn encoded_size(&self) -> usize {
        12
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.partition_id);
        buf.put_u64_le(self.consensus_group_id);
    }
}

impl WireDecode for CreatedPartitionAssignment {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        if buf.len() < 12 {
            return Err(WireError::UnexpectedEof {
                offset: 0,
                need: 12,
                have: buf.len(),
            });
        }

        Ok((
            Self {
                partition_id: read_u32_le(buf, 0)?,
                consensus_group_id: read_u64_le(buf, 4)?,
            },
            12,
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
            partition_id: 7,
            consensus_group_id: 42,
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreatedPartitionAssignment::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
    }
}
