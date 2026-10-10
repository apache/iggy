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

/// Terminal fence of a partition incarnation that metadata is deleting. Once
/// committed, the partition refuses every later request of that incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionPartitionHistoryRequest {
    pub incarnation: u64,
    pub metadata_op: u64,
}

impl TransitionPartitionHistoryRequest {
    pub const ENCODED_SIZE: usize = 2 * size_of::<u64>();
}

impl WireEncode for TransitionPartitionHistoryRequest {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.incarnation);
        buf.put_u64_le(self.metadata_op);
    }
}

impl WireDecode for TransitionPartitionHistoryRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing history transition bytes".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let incarnation = read_u64_le(buf, 0)?;
        let metadata_op = read_u64_le(buf, size_of::<u64>())?;
        if metadata_op == 0 {
            return Err(WireError::Validation(
                "missing history transition identity".into(),
            ));
        }
        Ok((
            Self {
                incarnation,
                metadata_op,
            },
            Self::ENCODED_SIZE,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_fence_round_trip_and_malformed_frames() {
        let request = TransitionPartitionHistoryRequest {
            incarnation: 17,
            metadata_op: 52,
        };
        let encoded = request.to_bytes();
        assert_eq!(
            encoded.len(),
            TransitionPartitionHistoryRequest::ENCODED_SIZE
        );
        assert_eq!(&encoded[0..8], &17_u64.to_le_bytes());
        assert_eq!(&encoded[8..16], &52_u64.to_le_bytes());
        assert_eq!(
            TransitionPartitionHistoryRequest::decode_from(&encoded).unwrap(),
            request
        );
        for length in 0..encoded.len() {
            assert!(TransitionPartitionHistoryRequest::decode_from(&encoded[..length]).is_err());
        }
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(TransitionPartitionHistoryRequest::decode_from(&trailing).is_err());
        let mut no_metadata_op = encoded.to_vec();
        no_metadata_op[8..16].fill(0);
        assert!(TransitionPartitionHistoryRequest::decode_from(&no_metadata_op).is_err());
    }
}
