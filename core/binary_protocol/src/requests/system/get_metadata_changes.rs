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
use crate::codec::{WireDecode, WireEncode, read_bytes, read_u8, read_u16_le, read_u32_le};
use crate::primitives::metadata_changes::{
    METADATA_CHANGE_CURSOR_SIZE, WireMetadataChangeCursor, WireMetadataChangeStartMode,
    WireMetadataCurrentPageToken,
};
use crate::primitives::metadata_changes::{put_optional_u64, read_optional_u64};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

/// Where a change log query starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMetadataChangeStart {
    /// The oldest operation the requested classes still hold completely.
    Earliest,
    /// No records, a cursor at the newest retained record.
    Latest,
    /// Records after a cursor.
    After(WireMetadataChangeCursor),
    /// Current state pages, the first one without a token.
    Current(Option<WireMetadataCurrentPageToken>),
}

/// `GetMetadataChanges` request.
///
/// Wire format:
/// ```text
/// [start:u8]
/// [log_id:u128][op:u64][checksum:u128][ordinal:u32]  cursor, all zero unless start = 2
/// [has_since:u8][since:u64 if 1]
/// [has_until:u8][until:u64 if 1]
/// [limit:u32]
/// [groups_count:u8][group:u8]*                       0 entries = all groups
/// [token_len:u16][token]                             0 unless start = 3 with a token
/// ```
///
/// Structural rules are enforced here. Which groups exist, the limit range
/// and the time bounds are checked where the request is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMetadataChangesRequest {
    pub start: WireMetadataChangeStart,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: u32,
    pub groups: Vec<u8>,
}

impl GetMetadataChangesRequest {
    const fn start_mode(&self) -> WireMetadataChangeStartMode {
        match self.start {
            WireMetadataChangeStart::Earliest => WireMetadataChangeStartMode::Earliest,
            WireMetadataChangeStart::Latest => WireMetadataChangeStartMode::Latest,
            WireMetadataChangeStart::After(_) => WireMetadataChangeStartMode::After,
            WireMetadataChangeStart::Current(_) => WireMetadataChangeStartMode::Current,
        }
    }

    /// Group codes the request can carry, at most `u8::MAX` of them.
    fn encoded_groups(&self) -> &[u8] {
        &self.groups[..self.groups.len().min(usize::from(u8::MAX))]
    }

    const fn token(&self) -> Option<&WireMetadataCurrentPageToken> {
        match &self.start {
            WireMetadataChangeStart::Current(Some(token)) => Some(token),
            _ => None,
        }
    }
}

impl WireEncode for GetMetadataChangesRequest {
    fn encoded_size(&self) -> usize {
        1 + METADATA_CHANGE_CURSOR_SIZE
            + if self.since.is_some() { 9 } else { 1 }
            + if self.until.is_some() { 9 } else { 1 }
            + 4
            + 1
            + self.encoded_groups().len()
            + 2
            + self.token().map_or(0, WireEncode::encoded_size)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.start_mode().as_u8());
        let cursor = match &self.start {
            WireMetadataChangeStart::After(cursor) => *cursor,
            _ => WireMetadataChangeCursor::default(),
        };
        cursor.encode(buf);
        put_optional_u64(buf, self.since);
        put_optional_u64(buf, self.until);
        buf.put_u32_le(self.limit);
        let groups = self.encoded_groups();
        // `encoded_groups` holds at most `u8::MAX` entries.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(groups.len() as u8);
        buf.put_slice(groups);
        match self.token() {
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

impl WireDecode for GetMetadataChangesRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mode = WireMetadataChangeStartMode::from_code(read_u8(buf, 0)?)?;
        let cursor_bytes = read_bytes(buf, 1, METADATA_CHANGE_CURSOR_SIZE)?;
        let mut position = 1 + METADATA_CHANGE_CURSOR_SIZE;
        let (since, consumed) = read_optional_u64(buf, position)?;
        position += consumed;
        let (until, consumed) = read_optional_u64(buf, position)?;
        position += consumed;
        let limit = read_u32_le(buf, position)?;
        position += 4;
        let groups_count = usize::from(read_u8(buf, position)?);
        position += 1;
        let groups = read_bytes(buf, position, groups_count)?.to_vec();
        position += groups_count;
        let token_length = usize::from(read_u16_le(buf, position)?);
        position += 2;
        let token_bytes = read_bytes(buf, position, token_length)?;
        position += token_length;
        if position != buf.len() {
            return Err(WireError::Validation(Cow::Owned(format!(
                "{} trailing bytes after a metadata changes request",
                buf.len() - position
            ))));
        }

        if mode != WireMetadataChangeStartMode::After && cursor_bytes.iter().any(|&byte| byte != 0)
        {
            return Err(WireError::Validation(Cow::Borrowed(
                "a cursor is allowed only when starting after it",
            )));
        }
        if mode != WireMetadataChangeStartMode::Current && token_length != 0 {
            return Err(WireError::Validation(Cow::Borrowed(
                "a page token is allowed only in current mode",
            )));
        }
        let start = match mode {
            WireMetadataChangeStartMode::Earliest => WireMetadataChangeStart::Earliest,
            WireMetadataChangeStartMode::Latest => WireMetadataChangeStart::Latest,
            WireMetadataChangeStartMode::After => {
                WireMetadataChangeStart::After(WireMetadataChangeCursor::decode(cursor_bytes)?.0)
            }
            WireMetadataChangeStartMode::Current if token_length == 0 => {
                WireMetadataChangeStart::Current(None)
            }
            WireMetadataChangeStartMode::Current => {
                let (token, consumed) = WireMetadataCurrentPageToken::decode(token_bytes)?;
                if consumed != token_length {
                    return Err(WireError::Validation(Cow::Borrowed(
                        "trailing bytes after a page token",
                    )));
                }
                WireMetadataChangeStart::Current(Some(token))
            }
        };
        Ok((
            Self {
                start,
                since,
                until,
                limit,
                groups,
            },
            position,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::metadata_changes::WireMetadataChangeGroup;
    use bytes::Bytes;

    fn cursor() -> WireMetadataChangeCursor {
        WireMetadataChangeCursor {
            log_id: 7,
            op: 9,
            checksum: 11,
            ordinal: 2,
        }
    }

    fn token() -> WireMetadataCurrentPageToken {
        WireMetadataCurrentPageToken {
            anchor: cursor(),
            required_head: 12,
            groups_mask: 0b11,
            group: WireMetadataChangeGroup::Topics,
            last_key: Bytes::from_static(&[1, 0, 0, 0, 2, 0, 0, 0]),
        }
    }

    fn request(start: WireMetadataChangeStart) -> GetMetadataChangesRequest {
        GetMetadataChangesRequest {
            start,
            since: Some(1_760_353_200_000_000),
            until: None,
            limit: 500,
            groups: vec![1, 2],
        }
    }

    #[test]
    fn given_every_start_mode_when_round_tripping_should_preserve_every_field() {
        for start in [
            WireMetadataChangeStart::Earliest,
            WireMetadataChangeStart::Latest,
            WireMetadataChangeStart::After(cursor()),
            WireMetadataChangeStart::Current(None),
            WireMetadataChangeStart::Current(Some(token())),
        ] {
            let original = request(start);
            let bytes = original.to_bytes();
            assert_eq!(bytes.len(), original.encoded_size());
            let (decoded, consumed) = GetMetadataChangesRequest::decode(&bytes).unwrap();
            assert_eq!(decoded, original);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn given_no_groups_and_no_bounds_when_round_tripping_should_preserve_them() {
        let original = GetMetadataChangesRequest {
            start: WireMetadataChangeStart::Latest,
            since: None,
            until: None,
            limit: 1,
            groups: vec![],
        };
        let bytes = original.to_bytes();
        assert_eq!(bytes.len(), 1 + 44 + 1 + 1 + 4 + 1 + 2);
        assert_eq!(
            GetMetadataChangesRequest::decode(&bytes).unwrap().0,
            original
        );
    }

    #[test]
    fn given_trailing_bytes_when_decoding_request_should_refuse() {
        let mut bytes = request(WireMetadataChangeStart::Earliest)
            .to_bytes()
            .to_vec();
        bytes.push(0);
        assert!(GetMetadataChangesRequest::decode(&bytes).is_err());
    }

    #[test]
    fn given_cursor_bytes_without_after_when_decoding_request_should_refuse() {
        let mut bytes = request(WireMetadataChangeStart::Latest).to_bytes().to_vec();
        bytes[5] = 1;
        assert!(GetMetadataChangesRequest::decode(&bytes).is_err());
    }

    #[test]
    fn given_non_canonical_cursor_when_decoding_request_should_refuse() {
        let original = request(WireMetadataChangeStart::After(WireMetadataChangeCursor {
            log_id: 1,
            op: 0,
            checksum: 3,
            ordinal: 0,
        }));
        assert!(GetMetadataChangesRequest::decode(&original.to_bytes()).is_err());
    }

    #[test]
    fn given_page_token_outside_current_mode_when_decoding_request_should_refuse() {
        let mut bytes = request(WireMetadataChangeStart::Current(Some(token())))
            .to_bytes()
            .to_vec();
        bytes[0] = WireMetadataChangeStartMode::Earliest.as_u8();
        assert!(GetMetadataChangesRequest::decode(&bytes).is_err());
    }

    #[test]
    fn given_unknown_start_or_presence_byte_when_decoding_request_should_refuse() {
        let bytes = request(WireMetadataChangeStart::Earliest)
            .to_bytes()
            .to_vec();
        let mut unknown_start = bytes.clone();
        unknown_start[0] = 4;
        assert!(GetMetadataChangesRequest::decode(&unknown_start).is_err());
        let mut bad_presence = bytes;
        bad_presence[1 + 44] = 2;
        assert!(GetMetadataChangesRequest::decode(&bad_presence).is_err());
    }

    #[test]
    fn given_truncated_request_when_decoding_should_refuse_every_prefix() {
        let bytes = request(WireMetadataChangeStart::Current(Some(token()))).to_bytes();
        for length in 0..bytes.len() {
            assert!(GetMetadataChangesRequest::decode(&bytes[..length]).is_err());
        }
    }
}
