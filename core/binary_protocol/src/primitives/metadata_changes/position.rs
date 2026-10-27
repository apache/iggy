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

use super::{WireMetadataChangeGroup, read_wire_name};
use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_bytes, read_u8, read_u16_le, read_u32_le};
use crate::codec::{read_u64_le, read_u128_le};
use bytes::{BufMut, Bytes, BytesMut};
use std::borrow::Cow;

/// Encoded size of a change cursor.
pub const METADATA_CHANGE_CURSOR_SIZE: usize = 44;

/// Longest page token key: a user id and a personal access token name.
pub const MAX_PAGE_TOKEN_KEY_SIZE: usize = 4 + 1 + 255;

/// Longest decoded page token payload.
pub const MAX_PAGE_TOKEN_SIZE: usize =
    METADATA_CHANGE_CURSOR_SIZE + 8 + 1 + 1 + 2 + MAX_PAGE_TOKEN_KEY_SIZE;

/// Position of the last record a caller consumed or scanned.
///
/// `(op, checksum, ordinal) = (0, 0, 0)` names the beginning of the history.
///
/// Wire format: `[log_id:u128][op:u64][checksum:u128][ordinal:u32]`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct WireMetadataChangeCursor {
    pub log_id: u128,
    pub op: u64,
    pub checksum: u128,
    pub ordinal: u32,
}

impl WireMetadataChangeCursor {
    /// The cursor before the first record of `log_id`.
    #[must_use]
    pub const fn zero(log_id: u128) -> Self {
        Self {
            log_id,
            op: 0,
            checksum: 0,
            ordinal: 0,
        }
    }

    /// Whether the cursor names the beginning of its history.
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.op == 0
    }

    /// Op 0 carries no checksum and no ordinal.
    #[must_use]
    pub const fn is_canonical(&self) -> bool {
        self.op != 0 || (self.checksum == 0 && self.ordinal == 0)
    }
}

impl WireEncode for WireMetadataChangeCursor {
    fn encoded_size(&self) -> usize {
        METADATA_CHANGE_CURSOR_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u128_le(self.log_id);
        buf.put_u64_le(self.op);
        buf.put_u128_le(self.checksum);
        buf.put_u32_le(self.ordinal);
    }
}

impl WireDecode for WireMetadataChangeCursor {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let cursor = Self {
            log_id: read_u128_le(buf, 0)?,
            op: read_u64_le(buf, 16)?,
            checksum: read_u128_le(buf, 24)?,
            ordinal: read_u32_le(buf, 40)?,
        };
        if !cursor.is_canonical() {
            return Err(WireError::Validation(Cow::Borrowed(
                "a cursor at op 0 carries no checksum and no ordinal",
            )));
        }
        Ok((cursor, METADATA_CHANGE_CURSOR_SIZE))
    }
}

/// Continuation of a current state scan.
///
/// The page token names the anchor captured before the first page, the
/// highest head any previous page reported, the requested groups and the key
/// of the last item returned, so any node that covers `required_head` can
/// serve the next page.
///
/// Wire format:
/// ```text
/// [anchor:44][required_head:u64][groups_mask:u8][group:u8][key_len:u16][last_key]
/// ```
///
/// Keys are little-endian ids: streams `[stream_id:u32]`, topics and
/// partitions `[stream_id:u32][topic_id:u32]`, consumer groups add
/// `[group_id:u32]`, members add `[member_id:u32]`, users `[user_id:u32]`,
/// personal access tokens `[user_id:u32][name]`, sessions `[epoch:u64]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMetadataCurrentPageToken {
    pub anchor: WireMetadataChangeCursor,
    pub required_head: u64,
    pub groups_mask: u8,
    pub group: WireMetadataChangeGroup,
    pub last_key: Bytes,
}

impl WireMetadataCurrentPageToken {
    const FIXED_SIZE: usize = METADATA_CHANGE_CURSOR_SIZE + 8 + 1 + 1 + 2;

    /// Check the token's own invariants: the group is selected, the key has
    /// the exact width of that group and the head covers the anchor.
    ///
    /// # Errors
    /// Returns `WireError::Validation` naming the first broken invariant.
    pub fn validate(&self) -> Result<(), WireError> {
        if self.groups_mask & self.group.mask_bit() == 0 {
            return Err(WireError::Validation(Cow::Borrowed(
                "page token group is not in its group mask",
            )));
        }
        if self.required_head < self.anchor.op {
            return Err(WireError::Validation(Cow::Borrowed(
                "page token head is below its anchor",
            )));
        }
        if !self.anchor.is_canonical() {
            return Err(WireError::Validation(Cow::Borrowed(
                "page token anchor is not canonical",
            )));
        }
        if self.last_key.len() > MAX_PAGE_TOKEN_KEY_SIZE {
            return Err(WireError::Validation(Cow::Borrowed(
                "page token key is too long",
            )));
        }
        if !key_fits_group(self.group, &self.last_key) {
            return Err(WireError::Validation(Cow::Owned(format!(
                "page token key of {} bytes does not fit group {}",
                self.last_key.len(),
                self.group.as_u8()
            ))));
        }
        Ok(())
    }
}

impl WireEncode for WireMetadataCurrentPageToken {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE + self.last_key.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.anchor.encode(buf);
        buf.put_u64_le(self.required_head);
        buf.put_u8(self.groups_mask);
        buf.put_u8(self.group.as_u8());
        // Keys are at most `MAX_PAGE_TOKEN_KEY_SIZE` bytes, see `validate`.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u16_le(self.last_key.len() as u16);
        buf.put_slice(&self.last_key);
    }
}

impl WireDecode for WireMetadataCurrentPageToken {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (anchor, mut position) = WireMetadataChangeCursor::decode(buf)?;
        let required_head = read_u64_le(buf, position)?;
        position += 8;
        let groups_mask = read_u8(buf, position)?;
        position += 1;
        let group_offset = position;
        let group = WireMetadataChangeGroup::from_code(read_u8(buf, position)?).map_err(|_| {
            WireError::Validation(Cow::Borrowed("page token names an unknown group"))
        })?;
        position += 1;
        let key_length = usize::from(read_u16_le(buf, position)?);
        position += 2;
        if key_length > MAX_PAGE_TOKEN_KEY_SIZE {
            return Err(WireError::Validation(Cow::Owned(format!(
                "page token key of {key_length} bytes at offset {group_offset} is too long"
            ))));
        }
        let last_key = Bytes::copy_from_slice(read_bytes(buf, position, key_length)?);
        position += key_length;
        let token = Self {
            anchor,
            required_head,
            groups_mask,
            group,
            last_key,
        };
        token.validate()?;
        Ok((token, position))
    }
}

fn key_fits_group(group: WireMetadataChangeGroup, key: &[u8]) -> bool {
    match group {
        WireMetadataChangeGroup::Streams | WireMetadataChangeGroup::Users => key.len() == 4,
        WireMetadataChangeGroup::Topics
        | WireMetadataChangeGroup::Partitions
        | WireMetadataChangeGroup::Sessions => key.len() == 8,
        WireMetadataChangeGroup::ConsumerGroups => key.len() == 12,
        WireMetadataChangeGroup::ConsumerGroupMembers => key.len() == 16,
        WireMetadataChangeGroup::PersonalAccessTokens => {
            key.len() > 4
                && read_wire_name(key, 4).is_ok_and(|(_, consumed)| 4 + consumed == key.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor() -> WireMetadataChangeCursor {
        WireMetadataChangeCursor {
            log_id: 0x0d4b_8f2a_6e1c_93d7,
            op: 4187,
            checksum: 0xf2a4_c6e8_b0d1_f359,
            ordinal: 1,
        }
    }

    fn token(group: WireMetadataChangeGroup, last_key: &[u8]) -> WireMetadataCurrentPageToken {
        WireMetadataCurrentPageToken {
            anchor: cursor(),
            required_head: 4200,
            groups_mask: u8::MAX,
            group,
            last_key: Bytes::copy_from_slice(last_key),
        }
    }

    fn token_key(user_id: u32, name: &str) -> Vec<u8> {
        let mut key = user_id.to_le_bytes().to_vec();
        key.push(u8::try_from(name.len()).unwrap());
        key.extend_from_slice(name.as_bytes());
        key
    }

    #[test]
    fn given_cursor_when_round_tripping_should_use_44_little_endian_bytes() {
        let bytes = cursor().to_bytes();
        assert_eq!(bytes.len(), METADATA_CHANGE_CURSOR_SIZE);
        assert_eq!(&bytes[16..24], &4187u64.to_le_bytes());
        let (decoded, consumed) = WireMetadataChangeCursor::decode(&bytes).unwrap();
        assert_eq!(decoded, cursor());
        assert_eq!(consumed, METADATA_CHANGE_CURSOR_SIZE);
    }

    #[test]
    fn given_zero_cursor_when_round_tripping_should_keep_its_log_id() {
        let zero = WireMetadataChangeCursor::zero(42);
        assert!(zero.is_zero());
        let (decoded, _) = WireMetadataChangeCursor::decode(&zero.to_bytes()).unwrap();
        assert_eq!(decoded, zero);
    }

    #[test]
    fn given_op_zero_with_checksum_or_ordinal_when_decoding_should_refuse() {
        for (checksum, ordinal) in [(1u128, 0u32), (0, 1)] {
            let cursor = WireMetadataChangeCursor {
                log_id: 1,
                op: 0,
                checksum,
                ordinal,
            };
            assert!(!cursor.is_canonical());
            assert!(WireMetadataChangeCursor::decode(&cursor.to_bytes()).is_err());
        }
    }

    #[test]
    fn given_truncated_cursor_when_decoding_should_refuse() {
        let bytes = cursor().to_bytes();
        assert!(WireMetadataChangeCursor::decode(&bytes[..43]).is_err());
    }

    #[test]
    fn given_every_group_key_width_when_round_tripping_page_tokens_should_preserve_them() {
        let cases = [
            (WireMetadataChangeGroup::Streams, vec![0u8; 4]),
            (WireMetadataChangeGroup::Topics, vec![1u8; 8]),
            (WireMetadataChangeGroup::Partitions, vec![2u8; 8]),
            (WireMetadataChangeGroup::ConsumerGroups, vec![3u8; 12]),
            (WireMetadataChangeGroup::ConsumerGroupMembers, vec![4u8; 16]),
            (WireMetadataChangeGroup::Users, vec![5u8; 4]),
            (
                WireMetadataChangeGroup::PersonalAccessTokens,
                token_key(9, "ci"),
            ),
            (WireMetadataChangeGroup::Sessions, vec![6u8; 8]),
        ];
        for (group, key) in cases {
            let original = token(group, &key);
            let bytes = original.to_bytes();
            let (decoded, consumed) = WireMetadataCurrentPageToken::decode(&bytes).unwrap();
            assert_eq!(decoded, original);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn given_longest_token_name_when_encoding_should_fit_the_maximum_payload() {
        let longest = "n".repeat(255);
        let original = token(
            WireMetadataChangeGroup::PersonalAccessTokens,
            &token_key(1, &longest),
        );
        assert_eq!(original.to_bytes().len(), MAX_PAGE_TOKEN_SIZE);
        assert!(WireMetadataCurrentPageToken::decode(&original.to_bytes()).is_ok());
    }

    #[test]
    fn given_wrong_key_width_when_decoding_page_token_should_refuse() {
        for (group, width) in [
            (WireMetadataChangeGroup::Streams, 8),
            (WireMetadataChangeGroup::Topics, 4),
            (WireMetadataChangeGroup::ConsumerGroups, 16),
            (WireMetadataChangeGroup::Sessions, 4),
            (WireMetadataChangeGroup::PersonalAccessTokens, 4),
        ] {
            let bytes = token(group, &vec![0u8; width]).to_bytes();
            assert!(WireMetadataCurrentPageToken::decode(&bytes).is_err());
        }
        let mut bad_name = token_key(1, "ci");
        bad_name.push(0);
        let bytes = token(WireMetadataChangeGroup::PersonalAccessTokens, &bad_name).to_bytes();
        assert!(WireMetadataCurrentPageToken::decode(&bytes).is_err());
    }

    #[test]
    fn given_group_outside_mask_when_decoding_page_token_should_refuse() {
        let mut original = token(WireMetadataChangeGroup::Topics, &[0u8; 8]);
        original.groups_mask = WireMetadataChangeGroup::Streams.mask_bit();
        assert!(WireMetadataCurrentPageToken::decode(&original.to_bytes()).is_err());
    }

    #[test]
    fn given_head_below_anchor_when_decoding_page_token_should_refuse() {
        let mut original = token(WireMetadataChangeGroup::Streams, &[0u8; 4]);
        original.required_head = original.anchor.op - 1;
        assert!(WireMetadataCurrentPageToken::decode(&original.to_bytes()).is_err());
    }

    #[test]
    fn given_unknown_group_code_when_decoding_page_token_should_refuse() {
        let mut bytes = token(WireMetadataChangeGroup::Streams, &[0u8; 4])
            .to_bytes()
            .to_vec();
        bytes[METADATA_CHANGE_CURSOR_SIZE + 9] = 9;
        assert!(WireMetadataCurrentPageToken::decode(&bytes).is_err());
    }

    #[test]
    fn given_truncated_page_token_when_decoding_should_refuse_every_prefix() {
        let bytes = token(WireMetadataChangeGroup::Users, &[1u8; 4]).to_bytes();
        for length in 0..bytes.len() {
            assert!(WireMetadataCurrentPageToken::decode(&bytes[..length]).is_err());
        }
    }
}
