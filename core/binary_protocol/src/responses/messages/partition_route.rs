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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le, read_u64_le};
use crate::primitives::partition_history::PartitionContext;
use bytes::{BufMut, BytesMut};

/// One replica's route to a partition, the reply to `GetPartitionRoute` (105).
///
/// The incarnation and the partition count come from metadata at
/// `metadata_op` or later. The view and its primary come from this replica's
/// copy of the partition group, so another replica can answer a higher view.
///
/// Wire format, exactly 25 bytes, little endian:
/// ```text
/// [incarnation:8][metadata_op:8][view:4][partitions_count:4][primary_replica:1]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRouteResponse {
    pub incarnation: u64,
    pub metadata_op: u64,
    pub view: u32,
    pub partitions_count: u32,
    /// Configured replica id of the primary of `view`, as `GetClusterMetadata`
    /// reports it, never a roster position.
    pub primary_replica: u8,
}

impl PartitionRouteResponse {
    pub const ENCODED_SIZE: usize = 2 * size_of::<u64>() + 2 * size_of::<u32>() + size_of::<u8>();

    /// The context a request routed by this reply carries. A route names no
    /// consumer, so it carries owner 0: the partition primary binds a group
    /// request with owner 0 to its installed owner, or refuses it.
    #[must_use]
    pub const fn context(&self) -> PartitionContext {
        PartitionContext {
            incarnation: self.incarnation,
            owner_generation: 0,
            metadata_op: self.metadata_op,
        }
    }
}

impl WireEncode for PartitionRouteResponse {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.incarnation);
        buf.put_u64_le(self.metadata_op);
        buf.put_u32_le(self.view);
        buf.put_u32_le(self.partitions_count);
        buf.put_u8(self.primary_replica);
    }
}

impl WireDecode for PartitionRouteResponse {
    /// A reply of any other length comes from a mismatched build, so it fails
    /// instead of yielding a route read from the wrong offsets.
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (route, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing partition route bytes".into(),
            ));
        }
        Ok(route)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        const METADATA_OP: usize = size_of::<u64>();
        const VIEW: usize = METADATA_OP + size_of::<u64>();
        const PARTITIONS_COUNT: usize = VIEW + size_of::<u32>();
        const PRIMARY_REPLICA: usize = PARTITIONS_COUNT + size_of::<u32>();
        let route = Self {
            incarnation: read_u64_le(buf, 0)?,
            metadata_op: read_u64_le(buf, METADATA_OP)?,
            view: read_u32_le(buf, VIEW)?,
            partitions_count: read_u32_le(buf, PARTITIONS_COUNT)?,
            primary_replica: read_u8(buf, PRIMARY_REPLICA)?,
        };
        Ok((route, Self::ENCODED_SIZE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROUTE: PartitionRouteResponse = PartitionRouteResponse {
        incarnation: 0x0102_0304_0506_0708,
        metadata_op: 0x1112_1314_1516_1718,
        view: 0x2122_2324,
        partitions_count: 0x3132_3334,
        primary_replica: 0x41,
    };

    #[test]
    fn given_route_when_encoded_should_match_the_golden_bytes() {
        let bytes = ROUTE.to_bytes();
        assert_eq!(
            bytes.as_ref(),
            [
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, //
                0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, //
                0x24, 0x23, 0x22, 0x21, //
                0x34, 0x33, 0x32, 0x31, //
                0x41,
            ]
        );
        assert_eq!(bytes.len(), PartitionRouteResponse::ENCODED_SIZE);
        assert_eq!(PartitionRouteResponse::decode_from(&bytes).unwrap(), ROUTE);
    }

    #[test]
    fn given_route_of_another_length_when_decoded_should_fail() {
        let bytes = ROUTE.to_bytes();
        for length in 0..bytes.len() {
            assert!(
                PartitionRouteResponse::decode_from(&bytes[..length]).is_err(),
                "accepted a route truncated at {length} bytes"
            );
        }
        let mut longer = bytes.to_vec();
        longer.push(0);
        assert!(PartitionRouteResponse::decode_from(&longer).is_err());
    }

    #[test]
    fn given_route_when_taking_its_context_should_carry_owner_zero() {
        assert_eq!(
            ROUTE.context(),
            PartitionContext {
                incarnation: ROUTE.incarnation,
                owner_generation: 0,
                metadata_op: ROUTE.metadata_op,
            }
        );
    }
}
