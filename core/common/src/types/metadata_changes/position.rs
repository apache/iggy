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

use super::{MetadataChangeGroup, MetadataChangeGroups};
use crate::IggyError;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use iggy_binary_protocol::primitives::metadata_changes::{
    MAX_PAGE_TOKEN_SIZE, METADATA_CHANGE_CURSOR_SIZE, WireMetadataChangeCursor,
    WireMetadataCurrentPageToken,
};
use iggy_binary_protocol::{WireDecode, WireEncode};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// Text prefix of a change cursor.
pub const METADATA_CHANGE_CURSOR_PREFIX: &str = "mc1_";

/// Text prefix of a current state page token.
pub const METADATA_CURRENT_PAGE_TOKEN_PREFIX: &str = "cs1_";

/// Text length of a change cursor: the prefix and 44 bytes of unpadded
/// Base64url.
const CURSOR_TEXT_LENGTH: usize = METADATA_CHANGE_CURSOR_PREFIX.len() + 59;

/// Position of the last change record a caller consumed or scanned.
///
/// Its text form is `mc1_` followed by unpadded Base64url of
/// `[log_id:u128 LE][op:u64 LE][checksum:u128 LE][ordinal:u32 LE]`.
/// `op = 0` with a zero checksum and ordinal names the beginning of the
/// history `log_id` identifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MetadataChangeCursor {
    /// Prepare checksum of op 1, the root bootstrap of this history.
    pub log_id: u128,
    pub op: u64,
    /// Prepare checksum of `op`.
    pub checksum: u128,
    /// Position of the record within `op`.
    pub ordinal: u32,
}

/// Opaque continuation of a current state scan.
///
/// Its text form is `cs1_` followed by unpadded Base64url of the page token
/// payload. Send it back unchanged with the same groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCurrentPageToken(WireMetadataCurrentPageToken);

impl MetadataChangeCursor {
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

impl MetadataCurrentPageToken {
    /// The groups the scan was started with.
    #[must_use]
    pub const fn groups(&self) -> MetadataChangeGroups {
        MetadataChangeGroups::from_mask(self.0.groups_mask)
    }

    /// The anchor captured before the scan's first page.
    #[must_use]
    pub fn anchor(&self) -> MetadataChangeCursor {
        self.0.anchor.into()
    }

    /// The highest head any page of the scan reported so far.
    #[must_use]
    pub const fn required_head(&self) -> u64 {
        self.0.required_head
    }

    /// The group of the last item returned.
    #[must_use]
    pub fn last_group(&self) -> MetadataChangeGroup {
        self.0.group.into()
    }

    /// The key of the last item returned, in the page token key layout.
    #[must_use]
    pub fn last_key(&self) -> &[u8] {
        &self.0.last_key
    }
}

impl Display for MetadataChangeCursor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes = WireMetadataChangeCursor::from(*self).to_bytes();
        write!(
            formatter,
            "{METADATA_CHANGE_CURSOR_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(bytes)
        )
    }
}

impl FromStr for MetadataChangeCursor {
    type Err = IggyError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != CURSOR_TEXT_LENGTH {
            return Err(IggyError::InvalidCommand);
        }
        let bytes = decode_canonical(text, METADATA_CHANGE_CURSOR_PREFIX)?;
        if bytes.len() != METADATA_CHANGE_CURSOR_SIZE {
            return Err(IggyError::InvalidCommand);
        }
        let (cursor, _) =
            WireMetadataChangeCursor::decode(&bytes).map_err(|_| IggyError::InvalidCommand)?;
        Ok(cursor.into())
    }
}

impl Display for MetadataCurrentPageToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{METADATA_CURRENT_PAGE_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(self.0.to_bytes())
        )
    }
}

impl FromStr for MetadataCurrentPageToken {
    type Err = IggyError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let longest =
            METADATA_CURRENT_PAGE_TOKEN_PREFIX.len() + MAX_PAGE_TOKEN_SIZE.div_ceil(3) * 4;
        if text.len() > longest {
            return Err(IggyError::InvalidCommand);
        }
        let bytes = decode_canonical(text, METADATA_CURRENT_PAGE_TOKEN_PREFIX)?;
        let (token, consumed) =
            WireMetadataCurrentPageToken::decode(&bytes).map_err(|_| IggyError::InvalidCommand)?;
        if consumed != bytes.len() {
            return Err(IggyError::InvalidCommand);
        }
        Ok(Self(token))
    }
}

/// Strip `prefix` and decode canonical unpadded Base64url.
fn decode_canonical(text: &str, prefix: &str) -> Result<Vec<u8>, IggyError> {
    let encoded = text.strip_prefix(prefix).ok_or(IggyError::InvalidCommand)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| IggyError::InvalidCommand)?;
    if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
        return Err(IggyError::InvalidCommand);
    }
    Ok(bytes)
}

impl From<WireMetadataChangeCursor> for MetadataChangeCursor {
    fn from(cursor: WireMetadataChangeCursor) -> Self {
        Self {
            log_id: cursor.log_id,
            op: cursor.op,
            checksum: cursor.checksum,
            ordinal: cursor.ordinal,
        }
    }
}

impl From<MetadataChangeCursor> for WireMetadataChangeCursor {
    fn from(cursor: MetadataChangeCursor) -> Self {
        Self {
            log_id: cursor.log_id,
            op: cursor.op,
            checksum: cursor.checksum,
            ordinal: cursor.ordinal,
        }
    }
}

impl From<WireMetadataCurrentPageToken> for MetadataCurrentPageToken {
    fn from(token: WireMetadataCurrentPageToken) -> Self {
        Self(token)
    }
}

impl From<MetadataCurrentPageToken> for WireMetadataCurrentPageToken {
    fn from(token: MetadataCurrentPageToken) -> Self {
        token.0
    }
}

impl AsRef<WireMetadataCurrentPageToken> for MetadataCurrentPageToken {
    fn as_ref(&self) -> &WireMetadataCurrentPageToken {
        &self.0
    }
}

impl Serialize for MetadataChangeCursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MetadataChangeCursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::from_str(&text)
            .map_err(|_| D::Error::custom(format!("invalid change cursor {text:?}")))
    }
}

impl Serialize for MetadataCurrentPageToken {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MetadataCurrentPageToken {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::from_str(&text)
            .map_err(|_| D::Error::custom(format!("invalid current page token {text:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use iggy_binary_protocol::primitives::metadata_changes::WireMetadataChangeGroup;

    fn cursor() -> MetadataChangeCursor {
        MetadataChangeCursor {
            log_id: 0x0d4b_8f2a_6e1c_93d7,
            op: 4187,
            checksum: 0xf2a4_c6e8_b0d1_f359,
            ordinal: 1,
        }
    }

    fn token() -> MetadataCurrentPageToken {
        MetadataCurrentPageToken(WireMetadataCurrentPageToken {
            anchor: cursor().into(),
            required_head: 4200,
            groups_mask: 0b11,
            group: WireMetadataChangeGroup::Topics,
            last_key: Bytes::from_static(&[3, 0, 0, 0, 1, 0, 0, 0]),
        })
    }

    #[test]
    fn given_cursor_when_printing_should_use_the_63_character_mc1_form() {
        let text = cursor().to_string();
        assert!(text.starts_with("mc1_"));
        assert_eq!(text.len(), 63);
        assert_eq!(MetadataChangeCursor::from_str(&text).unwrap(), cursor());
    }

    #[test]
    fn given_rfc_sample_cursor_when_parsing_should_name_op_4187_ordinal_1() {
        let parsed = MetadataChangeCursor::from_str(
            "mc1_15McbiqPSw0AAAAAAAAAAFsQAAAAAAAAWfPRsOjGpPIAAAAAAAAAAAEAAAA",
        )
        .unwrap();
        assert_eq!(parsed.log_id, 0x0d4b_8f2a_6e1c_93d7);
        assert_eq!(parsed.op, 4187);
        assert_eq!(parsed.ordinal, 1);
    }

    #[test]
    fn given_malformed_cursor_text_when_parsing_should_refuse() {
        let valid = cursor().to_string();
        let encoded = &valid[4..];
        let cases = [
            format!("mc2_{encoded}"),
            encoded.to_owned(),
            format!("{valid}A"),
            valid[..62].to_owned(),
            format!("mc1_{}=", &encoded[..58]),
            valid.replace('_', "/"),
        ];
        for case in cases {
            assert!(MetadataChangeCursor::from_str(&case).is_err(), "{case}");
        }
    }

    #[test]
    fn given_non_canonical_trailing_bits_when_parsing_cursor_should_refuse() {
        let valid = cursor().to_string();
        let last = valid.chars().last().unwrap();
        let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let index = alphabet.find(last).unwrap();
        let sibling = alphabet.as_bytes()[index ^ 1] as char;
        let tampered = format!("{}{sibling}", &valid[..62]);
        assert!(MetadataChangeCursor::from_str(&tampered).is_err());
    }

    #[test]
    fn given_zero_cursor_with_checksum_when_parsing_should_refuse() {
        let non_canonical = WireMetadataChangeCursor {
            log_id: 1,
            op: 0,
            checksum: 2,
            ordinal: 0,
        };
        let text = format!("mc1_{}", URL_SAFE_NO_PAD.encode(non_canonical.to_bytes()));
        assert!(MetadataChangeCursor::from_str(&text).is_err());
        let zero = MetadataChangeCursor::zero(1).to_string();
        assert!(MetadataChangeCursor::from_str(&zero).unwrap().is_zero());
    }

    #[test]
    fn given_page_token_when_printing_should_round_trip_through_cs1_text() {
        let text = token().to_string();
        assert!(text.starts_with("cs1_"));
        let parsed = MetadataCurrentPageToken::from_str(&text).unwrap();
        assert_eq!(parsed, token());
        assert_eq!(parsed.groups().mask(), 0b11);
        assert_eq!(parsed.last_group(), MetadataChangeGroup::Topics);
        assert_eq!(parsed.anchor(), cursor());
        assert_eq!(parsed.required_head(), 4200);
    }

    #[test]
    fn given_malformed_page_token_text_when_parsing_should_refuse() {
        let valid = token().to_string();
        let cases = [
            valid.replacen("cs1_", "mc1_", 1),
            format!("{valid}AA"),
            format!("{valid}="),
            "cs1_".to_owned(),
            format!("cs1_{}", "A".repeat(500)),
        ];
        for case in cases {
            assert!(MetadataCurrentPageToken::from_str(&case).is_err(), "{case}");
        }
    }

    #[test]
    fn given_cursor_and_token_when_serializing_json_should_use_their_text_forms() {
        let json = serde_json::to_string(&(cursor(), token())).unwrap();
        let (decoded_cursor, decoded_token): (MetadataChangeCursor, MetadataCurrentPageToken) =
            serde_json::from_str(&json).unwrap();
        assert_eq!(decoded_cursor, cursor());
        assert_eq!(decoded_token, token());
        assert!(json.contains("mc1_") && json.contains("cs1_"));
    }
}
