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

/// One partition as its primary last reported it: the view it leads, the op
/// it had committed, and its counters. The body of a metadata probe or reply
/// is a run of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionStatusEntry {
    pub namespace: u64,
    pub created_revision: u64,
    pub view: u32,
    pub commit: u64,
    pub current_offset: u64,
    pub messages_count: u64,
    pub size_bytes: u64,
    pub segments_count: u32,
    /// Nodes the primary counts in sync, bit `n` for node `n`, itself included.
    pub in_sync_nodes: u32,
}

impl PartitionStatusEntry {
    pub const ENCODED_SIZE: usize = 60;
}

impl WireEncode for PartitionStatusEntry {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.namespace);
        buf.put_u64_le(self.created_revision);
        buf.put_u32_le(self.view);
        buf.put_u64_le(self.commit);
        buf.put_u64_le(self.current_offset);
        buf.put_u64_le(self.messages_count);
        buf.put_u64_le(self.size_bytes);
        buf.put_u32_le(self.segments_count);
        buf.put_u32_le(self.in_sync_nodes);
    }
}

impl WireDecode for PartitionStatusEntry {
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
                namespace: read_u64_le(buf, 0)?,
                created_revision: read_u64_le(buf, 8)?,
                view: read_u32_le(buf, 16)?,
                commit: read_u64_le(buf, 20)?,
                current_offset: read_u64_le(buf, 28)?,
                messages_count: read_u64_le(buf, 36)?,
                size_bytes: read_u64_le(buf, 44)?,
                segments_count: read_u32_le(buf, 52)?,
                in_sync_nodes: read_u32_le(buf, 56)?,
            },
            Self::ENCODED_SIZE,
        ))
    }
}

/// Encode `entries` as one body, in one allocation.
#[must_use]
pub fn encode_status_entries(entries: &[PartitionStatusEntry]) -> bytes::Bytes {
    let mut body = BytesMut::with_capacity(entries.len() * PartitionStatusEntry::ENCODED_SIZE);
    for entry in entries {
        entry.encode(&mut body);
    }
    body.freeze()
}

/// Decode a body that is nothing but entries.
///
/// # Errors
/// A body whose length is not a whole number of entries.
pub fn decode_status_entries(body: &[u8]) -> Result<Vec<PartitionStatusEntry>, WireError> {
    if !body
        .len()
        .is_multiple_of(PartitionStatusEntry::ENCODED_SIZE)
    {
        return Err(WireError::Validation(
            "partition status body is not a whole number of entries".into(),
        ));
    }
    body.as_chunks::<{ PartitionStatusEntry::ENCODED_SIZE }>()
        .0
        .iter()
        .map(|chunk| PartitionStatusEntry::decode(chunk).map(|(entry, _)| entry))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_entries_when_encoding_should_roundtrip_and_refuse_partial_ones() {
        let entries = [
            PartitionStatusEntry {
                namespace: 0x0102_0304_0506_0708,
                created_revision: 9,
                view: 10,
                commit: 15,
                current_offset: 11,
                messages_count: 12,
                size_bytes: 13,
                segments_count: 14,
                in_sync_nodes: 0b101,
            },
            PartitionStatusEntry {
                namespace: 1,
                created_revision: u64::MAX,
                view: u32::MAX,
                commit: u64::MAX,
                current_offset: u64::MAX,
                messages_count: 0,
                size_bytes: 0,
                segments_count: u32::MAX,
                in_sync_nodes: u32::MAX,
            },
        ];
        let body = encode_status_entries(&entries);
        assert_eq!(body.len(), 2 * PartitionStatusEntry::ENCODED_SIZE);
        assert_eq!(decode_status_entries(&body).unwrap(), entries);
        assert!(decode_status_entries(&body[..body.len() - 1]).is_err());
        assert_eq!(decode_status_entries(&[]).unwrap(), []);
    }
}
