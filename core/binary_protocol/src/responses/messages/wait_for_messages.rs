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

use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::{MAX_PARTITIONS_PER_REQUEST, WireError};
use bytes::{BufMut, BytesMut};

/// Partitions that hold a message at their start point. An empty list means
/// that the wait expired.
///
/// Wire format: `[partitions_count:4 LE] partitions_count x [partition_id:4 LE]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitForMessagesResponse {
    pub partitions: Vec<u32>,
}

impl WireEncode for WaitForMessagesResponse {
    fn encoded_size(&self) -> usize {
        size_of::<u32>() * (1 + self.partitions.len())
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.partitions.len() as u32);
        for partition_id in &self.partitions {
            buf.put_u32_le(*partition_id);
        }
    }
}

impl WireDecode for WaitForMessagesResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let count = read_u32_le(buf, 0)?;
        if count > MAX_PARTITIONS_PER_REQUEST {
            return Err(WireError::Validation(
                "readiness reply names too many partitions".into(),
            ));
        }
        let partitions = (1..=count as usize)
            .map(|index| read_u32_le(buf, index * size_of::<u32>()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((Self { partitions }, size_of::<u32>() * (1 + count as usize)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_reject_truncation() {
        for partitions in [Vec::new(), vec![0, 7, u32::MAX]] {
            let response = WaitForMessagesResponse { partitions };
            let bytes = response.to_bytes();
            assert_eq!(bytes.len(), response.encoded_size());
            assert_eq!(
                WaitForMessagesResponse::decode(&bytes).unwrap(),
                (response, bytes.len())
            );
            for end in 0..bytes.len() {
                assert!(
                    WaitForMessagesResponse::decode(&bytes[..end]).is_err(),
                    "accepted truncation at {end}"
                );
            }
        }
    }

    #[test]
    fn rejects_counts_above_the_request_limit() {
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(MAX_PARTITIONS_PER_REQUEST + 1);
        assert!(WaitForMessagesResponse::decode(&bytes).is_err());
    }
}
