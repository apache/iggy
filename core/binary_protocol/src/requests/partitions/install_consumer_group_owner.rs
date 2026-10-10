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

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u64_le};
use crate::primitives::partition_history::ConsumerGroupOwner;

/// Server-only partition barrier. Metadata membership alone cannot authorize offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallConsumerGroupOwnerRequest {
    pub incarnation: u64,
    pub group_id: u64,
    pub owner: ConsumerGroupOwner,
    pub metadata_op: u64,
}

impl InstallConsumerGroupOwnerRequest {
    pub const ENCODED_SIZE: usize = 5 * size_of::<u64>() + size_of::<u128>();
}

impl WireEncode for InstallConsumerGroupOwnerRequest {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.incarnation);
        buf.put_u64_le(self.group_id);
        self.owner.encode(buf);
        buf.put_u64_le(self.metadata_op);
    }
}

impl WireDecode for InstallConsumerGroupOwnerRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing ownership installation bytes".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let incarnation = read_u64_le(buf, 0)?;
        let group_id = read_u64_le(buf, size_of::<u64>())?;
        let mut pos = 2 * size_of::<u64>();
        let (owner, consumed) = ConsumerGroupOwner::decode(&buf[pos..])?;
        pos += consumed;
        let metadata_op = read_u64_le(buf, pos)?;
        pos += size_of::<u64>();
        if owner.generation == 0 || metadata_op == 0 {
            return Err(WireError::Validation(
                "incomplete ownership transition identity".into(),
            ));
        }
        Ok((
            Self {
                incarnation,
                group_id,
                owner,
                metadata_op,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_installation_pins_fields_and_rejects_incomplete_frames() {
        let request = InstallConsumerGroupOwnerRequest {
            incarnation: 17,
            group_id: 3,
            owner: ConsumerGroupOwner {
                client_id: 4,
                session: 5,
                generation: 6,
            },
            metadata_op: 7,
        };
        let encoded = request.to_bytes();
        assert_eq!(
            encoded.len(),
            InstallConsumerGroupOwnerRequest::ENCODED_SIZE
        );
        assert_eq!(&encoded[0..8], &17_u64.to_le_bytes());
        assert_eq!(&encoded[8..16], &3_u64.to_le_bytes());
        assert_eq!(&encoded[16..32], &4_u128.to_le_bytes());
        assert_eq!(&encoded[32..40], &5_u64.to_le_bytes());
        assert_eq!(&encoded[40..48], &6_u64.to_le_bytes());
        assert_eq!(&encoded[48..56], &7_u64.to_le_bytes());
        assert_eq!(
            InstallConsumerGroupOwnerRequest::decode_from(&encoded).unwrap(),
            request
        );
        for length in 0..encoded.len() {
            assert!(InstallConsumerGroupOwnerRequest::decode_from(&encoded[..length]).is_err());
        }
        for field in [16..32, 32..40, 40..48, 48..56] {
            let mut invalid = encoded.to_vec();
            invalid[field].fill(0);
            assert!(InstallConsumerGroupOwnerRequest::decode_from(&invalid).is_err());
        }
        let mut no_owner = encoded.to_vec();
        no_owner[16..40].fill(0);
        assert!(
            InstallConsumerGroupOwnerRequest::decode_from(&no_owner)
                .unwrap()
                .owner
                .is_unassigned()
        );
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(InstallConsumerGroupOwnerRequest::decode_from(&trailing).is_err());
    }
}
