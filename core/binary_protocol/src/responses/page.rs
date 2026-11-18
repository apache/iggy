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
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::primitives::page::{
    MAX_PAGE_LIMIT, WirePageCursor, optional_cursor_size, put_optional_cursor, read_optional_cursor,
};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

/// One page of a list command around the item codec of that list.
///
/// Wire format:
/// ```text
/// [next_len:u8][next]   0 = last page
/// [count:u32]
/// [item] * count
/// ```
///
/// A page is a whole reply body, so a decode refuses bytes after the last item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageResponse<T> {
    pub items: Vec<T>,
    /// The position to continue after, `None` on the last page. A page can be
    /// short or empty and still carry it.
    pub next: Option<WirePageCursor>,
}

impl<T: WireEncode> WireEncode for PageResponse<T> {
    fn encoded_size(&self) -> usize {
        optional_cursor_size(self.next.as_ref())
            + 4
            + self
                .items
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        put_optional_cursor(buf, self.next.as_ref());
        buf.put_u32_le(self.items.len() as u32);
        for item in &self.items {
            item.encode(buf);
        }
    }
}

impl<T: WireDecode> WireDecode for PageResponse<T> {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (next, mut position) = read_optional_cursor(buf, 0)?;
        let count = read_u32_le(buf, position)? as usize;
        position += 4;
        // A server never sends more than `MAX_PAGE_LIMIT` items, so a corrupt
        // count cannot make the decoder reserve more than that.
        let mut items = Vec::with_capacity(count.min(MAX_PAGE_LIMIT as usize));
        for _ in 0..count {
            let (item, consumed) = T::decode(&buf[position..])?;
            position += consumed;
            items.push(item);
        }
        if position != buf.len() {
            return Err(WireError::Validation(Cow::Owned(format!(
                "{} bytes after the last item of a page",
                buf.len() - position
            ))));
        }
        Ok((Self { items, next }, position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;
    use crate::primitives::partition_history::PartitionContext;
    use crate::responses::consumer_groups::ConsumerGroupResponse;
    use crate::responses::topics::get_topic::PartitionResponse;

    fn group(id: u32, name: &str) -> ConsumerGroupResponse {
        ConsumerGroupResponse {
            id,
            partitions_count: 3,
            members_count: 1,
            name: WireName::new(name).unwrap(),
        }
    }

    #[test]
    fn given_items_and_a_cursor_when_round_tripping_should_keep_both() {
        let page = PageResponse {
            items: vec![group(1, "first"), group(4, "second")],
            next: Some(WirePageCursor::Id(4)),
        };
        let bytes = page.to_bytes();
        assert_eq!(bytes.len(), page.encoded_size());
        assert_eq!(&bytes[..15], &[9, 1, 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1]);
        assert_eq!(
            PageResponse::<ConsumerGroupResponse>::decode(&bytes).unwrap(),
            (page, bytes.len())
        );
    }

    #[test]
    fn given_an_empty_last_page_when_encoding_should_pin_the_bytes() {
        let page = PageResponse::<ConsumerGroupResponse> {
            items: Vec::new(),
            next: None,
        };
        let bytes = page.to_bytes();
        assert_eq!(&bytes[..], &[0, 0, 0, 0, 0]);
        assert_eq!(
            PageResponse::<ConsumerGroupResponse>::decode(&bytes).unwrap(),
            (page, 5)
        );
    }

    #[test]
    fn given_a_count_above_the_items_when_decoding_should_refuse_without_reserving_it() {
        let mut bytes = PageResponse {
            items: vec![group(1, "first")],
            next: None,
        }
        .to_bytes()
        .to_vec();
        bytes[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(PageResponse::<ConsumerGroupResponse>::decode(&bytes).is_err());
    }

    #[test]
    fn given_bytes_after_the_last_item_when_decoding_should_refuse() {
        let mut bytes = PageResponse {
            items: vec![group(1, "first")],
            next: Some(WirePageCursor::Id(1)),
        }
        .to_bytes()
        .to_vec();
        bytes.push(0);
        assert!(PageResponse::<ConsumerGroupResponse>::decode(&bytes).is_err());
        assert!(PageResponse::<ConsumerGroupResponse>::decode_from(&bytes).is_err());
    }

    #[test]
    fn given_a_truncated_page_when_decoding_should_refuse() {
        let bytes = PageResponse {
            items: vec![group(1, "first"), group(2, "second")],
            next: Some(WirePageCursor::Created {
                created_at: 9,
                id: 2,
            }),
        }
        .to_bytes();
        for length in 0..bytes.len() {
            assert!(
                PageResponse::<ConsumerGroupResponse>::decode(&bytes[..length]).is_err(),
                "a page cut at {length} bytes must not decode"
            );
        }
    }

    #[test]
    fn given_a_partitions_page_when_round_tripping_or_truncating_should_keep_it_or_refuse() {
        let partition = |id: u32, members: Vec<u8>| PartitionResponse {
            id,
            created_at: 7,
            segments_count: 1,
            current_offset: 0,
            size_bytes: 0,
            messages_count: 0,
            context: PartitionContext {
                incarnation: 3,
                owner_generation: 0,
                metadata_op: 11,
            },
            in_sync_members: members.clone(),
            members,
        };
        let page = PageResponse {
            items: vec![partition(0, vec![1, 2]), partition(1, Vec::new())],
            next: Some(WirePageCursor::Id(1)),
        };
        let bytes = page.to_bytes();
        assert_eq!(bytes.len(), 10 + 4 + (66 + 4) + 66);
        assert_eq!(&bytes[..14], &[9, 1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0]);
        assert_eq!(
            PageResponse::<PartitionResponse>::decode(&bytes).unwrap(),
            (page, bytes.len())
        );
        for length in 0..bytes.len() {
            assert!(
                PageResponse::<PartitionResponse>::decode(&bytes[..length]).is_err(),
                "a page cut at {length} bytes must not decode"
            );
        }
    }
}
