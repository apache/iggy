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

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};

pub const CONSUMER_GROUP_OWNERS_MAX: u32 = 1 << 20;

/// Committed metadata proves IDs below the allocation boundary absent from the
/// live catalog permanently deleted. Only the newest catalog must be retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireConsumerGroupOwnersRequest {
    pub incarnation: u64,
    pub metadata_op: u64,
    pub next_group_id: u64,
    pub live_group_ids: Vec<u64>,
}

impl RetireConsumerGroupOwnersRequest {
    pub const PREFIX_SIZE: usize = 3 * size_of::<u64>() + size_of::<u32>();

    #[must_use]
    pub fn retires(&self, group_id: u64) -> bool {
        group_id < self.next_group_id && self.live_group_ids.binary_search(&group_id).is_err()
    }
}

impl WireEncode for RetireConsumerGroupOwnersRequest {
    fn encoded_size(&self) -> usize {
        Self::PREFIX_SIZE + self.live_group_ids.len() * size_of::<u64>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.incarnation);
        buf.put_u64_le(self.metadata_op);
        buf.put_u64_le(self.next_group_id);
        buf.put_u32_le(
            u32::try_from(self.live_group_ids.len()).expect("bounded live group catalog"),
        );
        for group_id in &self.live_group_ids {
            buf.put_u64_le(*group_id);
        }
    }
}

impl WireDecode for RetireConsumerGroupOwnersRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing owner retirement bytes".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let incarnation = read_u64_le(buf, 0)?;
        let metadata_op = read_u64_le(buf, size_of::<u64>())?;
        let mut position = 2 * size_of::<u64>();
        let next_group_id = read_u64_le(buf, position)?;
        position += size_of::<u64>();
        let count = read_u32_le(buf, position)?;
        position += size_of::<u32>();
        if metadata_op == 0 || count > CONSUMER_GROUP_OWNERS_MAX {
            return Err(WireError::Validation(
                "invalid owner retirement catalog".into(),
            ));
        }
        let end = position + count as usize * size_of::<u64>();
        if end > buf.len() {
            return Err(WireError::Validation(
                "truncated owner retirement catalog".into(),
            ));
        }
        let mut live_group_ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let group_id = read_u64_le(buf, position)?;
            position += size_of::<u64>();
            if group_id >= next_group_id
                || live_group_ids
                    .last()
                    .is_some_and(|previous| *previous >= group_id)
            {
                return Err(WireError::Validation(
                    "unordered owner retirement catalog".into(),
                ));
            }
            live_group_ids.push(group_id);
        }
        Ok((
            Self {
                incarnation,
                metadata_op,
                next_group_id,
                live_group_ids,
            },
            position,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_retirement_catalog_when_decoded_should_preserve_live_and_future_groups() {
        let request = RetireConsumerGroupOwnersRequest {
            incarnation: 7,
            metadata_op: 11,
            next_group_id: 6,
            live_group_ids: vec![1, 4],
        };
        let encoded = request.to_bytes();
        assert_eq!(
            RetireConsumerGroupOwnersRequest::decode_from(&encoded).unwrap(),
            request
        );
        assert!(request.retires(0));
        assert!(request.retires(5));
        assert!(!request.retires(1));
        assert!(!request.retires(6));
        for length in 0..encoded.len() {
            assert!(RetireConsumerGroupOwnersRequest::decode_from(&encoded[..length]).is_err());
        }
        let mut invalid = request;
        invalid.live_group_ids = vec![4, 1];
        assert!(RetireConsumerGroupOwnersRequest::decode_from(&invalid.to_bytes()).is_err());
        invalid.live_group_ids = vec![1, 1];
        assert!(RetireConsumerGroupOwnersRequest::decode_from(&invalid.to_bytes()).is_err());
        invalid.live_group_ids = vec![6];
        assert!(RetireConsumerGroupOwnersRequest::decode_from(&invalid.to_bytes()).is_err());
    }
}
