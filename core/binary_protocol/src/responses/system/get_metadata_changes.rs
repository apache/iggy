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
use crate::codec::{
    WireDecode, WireEncode, bounded_capacity, read_bytes, read_u8, read_u16_le, read_u32_le,
    read_u64_le, read_u128_le,
};
use crate::primitives::metadata_changes::{
    METADATA_CHANGE_CURSOR_SIZE, WireMetadataChangeCursor, WireMetadataChangePhase,
    WireMetadataChangeStatus, WireMetadataCurrentPageToken,
};
use crate::primitives::metadata_changes::{
    put_flag, put_optional_u32, read_flag, read_optional_u32,
};
use bytes::{BufMut, Bytes, BytesMut};
use std::borrow::Cow;

/// One change record: the envelope stamped from the prepare header and the
/// body the handler encoded at apply, kept as raw bytes so a client can hold a
/// kind it does not know.
///
/// Wire format:
/// ```text
/// [op:u64][ordinal:u32][checksum:u128][timestamp:u64]
/// [has_actor:u8][actor_user_id:u32 if 1]
/// [group:u8][kind:u8][body_len:u32][body]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMetadataChangeRecord {
    pub op: u64,
    pub ordinal: u32,
    pub checksum: u128,
    pub timestamp: u64,
    pub actor_user_id: Option<u32>,
    pub group: u8,
    pub kind: u8,
    pub body: Bytes,
}

/// One current state item.
///
/// Wire format: `[group:u8][body_len:u32][body]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMetadataItem {
    pub group: u8,
    pub body: Bytes,
}

/// `GetMetadataChanges` response.
///
/// Wire format:
/// ```text
/// [status:u8][phase:u8]
/// [has_log_id:u8][log_id:u128 if 1]
/// [oldest_op:u64][retained_since:u64][head_op:u64]
/// [has_next:u8][cursor:44 if 1]
/// [has_more:u8]
/// [records_count:u32][record]*
/// [items_count:u32][item]*
/// [token_len:u16][token]
/// ```
///
/// A changes page carries records and no items or token, a current page
/// carries items and a token and no records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMetadataChangesResponse {
    pub status: WireMetadataChangeStatus,
    pub phase: WireMetadataChangePhase,
    pub log_id: Option<u128>,
    pub oldest_op: u64,
    pub retained_since: u64,
    pub head_op: u64,
    pub next: Option<WireMetadataChangeCursor>,
    pub has_more: bool,
    pub records: Vec<WireMetadataChangeRecord>,
    pub items: Vec<WireMetadataItem>,
    pub current_page_token: Option<WireMetadataCurrentPageToken>,
}

impl WireMetadataChangeRecord {
    const MIN_SIZE: usize = 8 + 4 + 16 + 8 + 1 + 1 + 1 + 4;
}

impl WireEncode for WireMetadataChangeRecord {
    fn encoded_size(&self) -> usize {
        Self::MIN_SIZE + if self.actor_user_id.is_some() { 4 } else { 0 } + self.body.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.op);
        buf.put_u32_le(self.ordinal);
        buf.put_u128_le(self.checksum);
        buf.put_u64_le(self.timestamp);
        put_optional_u32(buf, self.actor_user_id);
        buf.put_u8(self.group);
        buf.put_u8(self.kind);
        // Bodies are bounded by the options budget, far below `u32::MAX`.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.body.len() as u32);
        buf.put_slice(&self.body);
    }
}

impl WireDecode for WireMetadataChangeRecord {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let op = read_u64_le(buf, 0)?;
        let ordinal = read_u32_le(buf, 8)?;
        let checksum = read_u128_le(buf, 12)?;
        let timestamp = read_u64_le(buf, 28)?;
        let (actor_user_id, consumed) = read_optional_u32(buf, 36)?;
        let mut position = 36 + consumed;
        let group = read_u8(buf, position)?;
        let kind = read_u8(buf, position + 1)?;
        let body_length = read_u32_le(buf, position + 2)? as usize;
        position += 6;
        let body = Bytes::copy_from_slice(read_bytes(buf, position, body_length)?);
        position += body_length;
        Ok((
            Self {
                op,
                ordinal,
                checksum,
                timestamp,
                actor_user_id,
                group,
                kind,
                body,
            },
            position,
        ))
    }
}

impl WireEncode for WireMetadataItem {
    fn encoded_size(&self) -> usize {
        1 + 4 + self.body.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.group);
        // Item bodies are bounded by the options budget.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.body.len() as u32);
        buf.put_slice(&self.body);
    }
}

impl WireDecode for WireMetadataItem {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let group = read_u8(buf, 0)?;
        let body_length = read_u32_le(buf, 1)? as usize;
        let body = Bytes::copy_from_slice(read_bytes(buf, 5, body_length)?);
        Ok((Self { group, body }, 5 + body_length))
    }
}

impl WireEncode for GetMetadataChangesResponse {
    fn encoded_size(&self) -> usize {
        2 + if self.log_id.is_some() { 17 } else { 1 }
            + 24
            + if self.next.is_some() {
                1 + METADATA_CHANGE_CURSOR_SIZE
            } else {
                1
            }
            + 1
            + 4
            + self
                .records
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
            + 4
            + self
                .items
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
            + 2
            + self
                .current_page_token
                .as_ref()
                .map_or(0, WireEncode::encoded_size)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.status.as_u8());
        buf.put_u8(self.phase.as_u8());
        put_flag(buf, self.log_id.is_some());
        if let Some(log_id) = self.log_id {
            buf.put_u128_le(log_id);
        }
        buf.put_u64_le(self.oldest_op);
        buf.put_u64_le(self.retained_since);
        buf.put_u64_le(self.head_op);
        put_flag(buf, self.next.is_some());
        if let Some(next) = &self.next {
            next.encode(buf);
        }
        put_flag(buf, self.has_more);
        // A page holds at most 1000 records or items.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.records.len() as u32);
        for record in &self.records {
            record.encode(buf);
        }
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.items.len() as u32);
        for item in &self.items {
            item.encode(buf);
        }
        match &self.current_page_token {
            Some(token) => {
                // A valid token is at most `MAX_PAGE_TOKEN_SIZE` bytes.
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u16_le(token.encoded_size() as u16);
                token.encode(buf);
            }
            None => buf.put_u16_le(0),
        }
    }
}

impl WireDecode for GetMetadataChangesResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let status = WireMetadataChangeStatus::from_code(read_u8(buf, 0)?)?;
        let phase = WireMetadataChangePhase::from_code(read_u8(buf, 1)?)?;
        let mut position = 2;
        let log_id = if read_flag(buf, position)? {
            position += 1;
            let log_id = read_u128_le(buf, position)?;
            position += 16;
            Some(log_id)
        } else {
            position += 1;
            None
        };
        let oldest_op = read_u64_le(buf, position)?;
        let retained_since = read_u64_le(buf, position + 8)?;
        let head_op = read_u64_le(buf, position + 16)?;
        position += 24;
        let next = if read_flag(buf, position)? {
            position += 1;
            let rest = buf.get(position..).unwrap_or_default();
            let (cursor, consumed) = WireMetadataChangeCursor::decode(rest)?;
            position += consumed;
            Some(cursor)
        } else {
            position += 1;
            None
        };
        let has_more = read_flag(buf, position)?;
        position += 1;

        let records_count = read_u32_le(buf, position)? as usize;
        position += 4;
        let mut records = Vec::with_capacity(bounded_capacity(
            records_count,
            buf.len().saturating_sub(position),
            WireMetadataChangeRecord::MIN_SIZE,
        ));
        for _ in 0..records_count {
            let rest = buf.get(position..).unwrap_or_default();
            let (record, consumed) = WireMetadataChangeRecord::decode(rest)?;
            records.push(record);
            position += consumed;
        }

        let items_count = read_u32_le(buf, position)? as usize;
        position += 4;
        let mut items = Vec::with_capacity(bounded_capacity(
            items_count,
            buf.len().saturating_sub(position),
            5,
        ));
        for _ in 0..items_count {
            let rest = buf.get(position..).unwrap_or_default();
            let (item, consumed) = WireMetadataItem::decode(rest)?;
            items.push(item);
            position += consumed;
        }

        let token_length = usize::from(read_u16_le(buf, position)?);
        position += 2;
        let current_page_token = if token_length == 0 {
            None
        } else {
            let token_bytes = read_bytes(buf, position, token_length)?;
            let (token, consumed) = WireMetadataCurrentPageToken::decode(token_bytes)?;
            if consumed != token_length {
                return Err(WireError::Validation(Cow::Borrowed(
                    "trailing bytes after a page token",
                )));
            }
            position += token_length;
            Some(token)
        };

        let phase_holds_other_content = match phase {
            WireMetadataChangePhase::Changes => !items.is_empty() || current_page_token.is_some(),
            WireMetadataChangePhase::Current => !records.is_empty(),
        };
        if phase_holds_other_content {
            return Err(WireError::Validation(Cow::Borrowed(
                "a changes page carries only records and a current page only items",
            )));
        }
        Ok((
            Self {
                status,
                phase,
                log_id,
                oldest_op,
                retained_since,
                head_op,
                next,
                has_more,
                records,
                items,
                current_page_token,
            },
            position,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::metadata_changes::WireMetadataChangeGroup;

    fn cursor(op: u64, ordinal: u32) -> WireMetadataChangeCursor {
        WireMetadataChangeCursor {
            log_id: 0x0d4b_8f2a_6e1c_93d7,
            op,
            checksum: 0xa5c1_f703_8b9e_2d4c,
            ordinal,
        }
    }

    fn record(actor_user_id: Option<u32>) -> WireMetadataChangeRecord {
        WireMetadataChangeRecord {
            op: 4121,
            ordinal: 0,
            checksum: 0xa5c1_f703_8b9e_2d4c,
            timestamp: 1_760_356_800_123_456,
            actor_user_id,
            group: 1,
            kind: 1,
            body: Bytes::from_static(&[1, 2, 3, 4]),
        }
    }

    fn changes_page() -> GetMetadataChangesResponse {
        GetMetadataChangesResponse {
            status: WireMetadataChangeStatus::Ok,
            phase: WireMetadataChangePhase::Changes,
            log_id: Some(0x0d4b_8f2a_6e1c_93d7),
            oldest_op: 3871,
            retained_since: 1_760_352_000_000_000,
            head_op: 4187,
            next: Some(cursor(4187, 1)),
            has_more: false,
            records: vec![record(Some(4)), record(None)],
            items: vec![],
            current_page_token: None,
        }
    }

    fn current_page() -> GetMetadataChangesResponse {
        GetMetadataChangesResponse {
            status: WireMetadataChangeStatus::Ok,
            phase: WireMetadataChangePhase::Current,
            log_id: Some(1),
            oldest_op: 1,
            retained_since: 0,
            head_op: 40,
            next: Some(cursor(30, 0)),
            has_more: true,
            records: vec![],
            items: vec![WireMetadataItem {
                group: 1,
                body: Bytes::from_static(&[9; 12]),
            }],
            current_page_token: Some(WireMetadataCurrentPageToken {
                anchor: cursor(30, 0),
                required_head: 40,
                groups_mask: WireMetadataChangeGroup::Streams.mask_bit(),
                group: WireMetadataChangeGroup::Streams,
                last_key: Bytes::from_static(&[3, 0, 0, 0]),
            }),
        }
    }

    #[test]
    fn given_changes_page_when_round_tripping_should_preserve_every_field() {
        let original = changes_page();
        let bytes = original.to_bytes();
        assert_eq!(bytes.len(), original.encoded_size());
        let (decoded, consumed) = GetMetadataChangesResponse::decode(&bytes).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn given_current_page_when_round_tripping_should_preserve_every_field() {
        let original = current_page();
        let bytes = original.to_bytes();
        let (decoded, consumed) = GetMetadataChangesResponse::decode(&bytes).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn given_behind_without_log_id_or_next_when_round_tripping_should_preserve_absence() {
        let original = GetMetadataChangesResponse {
            status: WireMetadataChangeStatus::Behind,
            log_id: None,
            next: None,
            records: vec![],
            ..changes_page()
        };
        let bytes = original.to_bytes();
        assert_eq!(
            GetMetadataChangesResponse::decode(&bytes).unwrap().0,
            original
        );
    }

    #[test]
    fn given_items_in_changes_phase_when_decoding_should_refuse() {
        let mut page = current_page();
        page.phase = WireMetadataChangePhase::Changes;
        assert!(GetMetadataChangesResponse::decode(&page.to_bytes()).is_err());
    }

    #[test]
    fn given_records_in_current_phase_when_decoding_should_refuse() {
        let mut page = changes_page();
        page.phase = WireMetadataChangePhase::Current;
        assert!(GetMetadataChangesResponse::decode(&page.to_bytes()).is_err());
    }

    #[test]
    fn given_unknown_status_when_decoding_should_refuse() {
        let mut bytes = changes_page().to_bytes().to_vec();
        bytes[0] = 4;
        assert!(GetMetadataChangesResponse::decode(&bytes).is_err());
    }

    #[test]
    fn given_truncated_response_when_decoding_should_refuse_every_prefix() {
        for page in [changes_page(), current_page()] {
            let bytes = page.to_bytes();
            for length in 0..bytes.len() {
                assert!(
                    GetMetadataChangesResponse::decode(&bytes[..length]).is_err(),
                    "accepted a response truncated to {length} bytes"
                );
            }
        }
    }

    #[test]
    fn given_record_count_beyond_the_frame_when_decoding_should_fail_without_allocating_it() {
        let mut page = changes_page();
        page.records.clear();
        let mut bytes = page.to_bytes().to_vec();
        let count_offset = 2 + 17 + 24 + 1 + METADATA_CHANGE_CURSOR_SIZE + 1;
        bytes[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(GetMetadataChangesResponse::decode(&bytes).is_err());
    }
}
