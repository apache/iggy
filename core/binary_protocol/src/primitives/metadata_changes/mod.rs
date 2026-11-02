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

//! Wire codes, record and item bodies, the change cursor and the current
//! state page token of the metadata change log.
//!
//! A record body is encoded once on the server and copied verbatim into every
//! answer, so decoding is strict: unknown reason codes, a known kind under the
//! wrong group, presence bytes other than 0 and 1 and trailing bytes are all
//! refused. A kind or group this build does not know is not an error, the
//! caller keeps it with its raw body.

mod body;
mod position;

pub use body::{
    WireChangeOptions, WireConsumerGroupCreatedBody, WireConsumerGroupDeletedBody,
    WireConsumerGroupItemBody, WireMemberItemBody, WireMemberJoinedBody, WireMemberLeftBody,
    WireMetadataChange, WireMetadataCurrentItem, WirePartitionHandedOverBody,
    WirePartitionsChangedBody, WirePartitionsItemBody, WireSessionBody, WireSessionClosedBody,
    WireStreamBody, WireStreamDeletedBody, WireStreamKey, WireTokenBody, WireTokenDeletedBody,
    WireTopicBody, WireTopicDeletedBody, WireTopicItemBody, WireTopicKey, WireUserBody,
    WireUserIdentityBody, WireUserKey, WireUserPermissionsBody,
};
pub use position::{
    MAX_PAGE_TOKEN_KEY_SIZE, MAX_PAGE_TOKEN_SIZE, METADATA_CHANGE_CURSOR_SIZE,
    WireMetadataChangeCursor, WireMetadataCurrentPageToken,
};

use crate::WireDecode;
use crate::WireError;
use crate::codec::{bounded_capacity, read_u8, read_u32_le, read_u64_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

macro_rules! wire_code_enum {
    (
        $(#[$meta:meta])*
        $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident = $code:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[repr(u8)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $($(#[$variant_meta])* $variant = $code),+
        }

        impl $name {
            /// Decode from the wire code.
            ///
            /// # Errors
            /// Returns `WireError::UnknownDiscriminant` for a code this build
            /// does not know.
            pub const fn from_code(code: u8) -> Result<Self, WireError> {
                match code {
                    $($code => Ok(Self::$variant),)+
                    value => Err(WireError::UnknownDiscriminant {
                        type_name: stringify!($name),
                        value,
                        offset: 0,
                    }),
                }
            }

            /// The wire code.
            #[must_use]
            pub const fn as_u8(self) -> u8 {
                self as u8
            }
        }
    };
}

wire_code_enum! {
    /// Family of change records and the unit of filtering. Codes 9 to 127 are
    /// reserved for future groups, 128 to 255 for downstream extensions.
    ///
    /// A page token holds the groups in an 8 bit mask, so a ninth group needs
    /// a new token format.
    WireMetadataChangeGroup {
        Streams = 1,
        Topics = 2,
        Partitions = 3,
        ConsumerGroups = 4,
        ConsumerGroupMembers = 5,
        Users = 6,
        PersonalAccessTokens = 7,
        Sessions = 8,
    }
}

impl WireMetadataChangeGroup {
    /// Bit of this group in a page token's group mask.
    #[must_use]
    pub const fn mask_bit(self) -> u8 {
        1 << (self as u8 - 1)
    }
}

wire_code_enum! {
    /// Kind of a change record. Existing kind bodies never change in place, a
    /// new layout is a new kind.
    WireMetadataChangeKind {
        StreamCreated = 1,
        StreamUpdated = 2,
        StreamDeleted = 3,
        TopicCreated = 10,
        TopicUpdated = 11,
        TopicDeleted = 12,
        PartitionsChanged = 20,
        ConsumerGroupCreated = 30,
        ConsumerGroupDeleted = 31,
        ConsumerGroupMemberJoined = 40,
        ConsumerGroupMemberLeft = 41,
        ConsumerGroupPartitionHandedOver = 42,
        UserCreated = 50,
        UserUpdated = 51,
        UserDeleted = 52,
        UserPasswordChanged = 53,
        UserPermissionsUpdated = 54,
        PersonalAccessTokenCreated = 60,
        PersonalAccessTokenDeleted = 61,
        SessionRegistered = 70,
        SessionClosed = 71,
    }
}

impl WireMetadataChangeKind {
    /// The group every record of this kind belongs to.
    #[must_use]
    pub const fn group(self) -> WireMetadataChangeGroup {
        match self {
            Self::StreamCreated | Self::StreamUpdated | Self::StreamDeleted => {
                WireMetadataChangeGroup::Streams
            }
            Self::TopicCreated | Self::TopicUpdated | Self::TopicDeleted => {
                WireMetadataChangeGroup::Topics
            }
            Self::PartitionsChanged => WireMetadataChangeGroup::Partitions,
            Self::ConsumerGroupCreated | Self::ConsumerGroupDeleted => {
                WireMetadataChangeGroup::ConsumerGroups
            }
            Self::ConsumerGroupMemberJoined
            | Self::ConsumerGroupMemberLeft
            | Self::ConsumerGroupPartitionHandedOver => {
                WireMetadataChangeGroup::ConsumerGroupMembers
            }
            Self::UserCreated
            | Self::UserUpdated
            | Self::UserDeleted
            | Self::UserPasswordChanged
            | Self::UserPermissionsUpdated => WireMetadataChangeGroup::Users,
            Self::PersonalAccessTokenCreated | Self::PersonalAccessTokenDeleted => {
                WireMetadataChangeGroup::PersonalAccessTokens
            }
            Self::SessionRegistered | Self::SessionClosed => WireMetadataChangeGroup::Sessions,
        }
    }
}

wire_code_enum! {
    /// Why a consumer group member was removed.
    WireMemberLeftReason {
        Left = 0,
        SessionEnded = 1,
    }
}

wire_code_enum! {
    /// Why a personal access token was removed.
    WireTokenDeletedReason {
        Deleted = 0,
        Expired = 1,
    }
}

wire_code_enum! {
    /// Why a session was closed.
    WireSessionClosedReason {
        Logout = 0,
        Expired = 1,
        /// The user was deactivated or deleted.
        Revoked = 2,
    }
}

wire_code_enum! {
    /// Why a user's public permission set changed.
    WirePermissionsUpdatedReason {
        Updated = 0,
        ResourceDeleted = 1,
    }
}

wire_code_enum! {
    /// Outcome of a change log query.
    WireMetadataChangeStatus {
        Ok = 0,
        Behind = 1,
        Expired = 2,
        Reset = 3,
    }
}

wire_code_enum! {
    /// Whether an answer holds change records or current state items.
    WireMetadataChangePhase {
        Changes = 0,
        Current = 1,
    }
}

wire_code_enum! {
    /// Where a change log query starts.
    WireMetadataChangeStartMode {
        Earliest = 0,
        Latest = 1,
        After = 2,
        Current = 3,
    }
}

/// Read a presence or boolean byte, refusing anything but 0 and 1.
pub(crate) fn read_flag(buf: &[u8], offset: usize) -> Result<bool, WireError> {
    match read_u8(buf, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(WireError::UnknownDiscriminant {
            type_name: "flag",
            value,
            offset,
        }),
    }
}

pub(crate) fn put_flag(buf: &mut BytesMut, value: bool) {
    buf.put_u8(u8::from(value));
}

pub(crate) fn read_wire_name(buf: &[u8], offset: usize) -> Result<(WireName, usize), WireError> {
    let rest = buf.get(offset..).ok_or(WireError::UnexpectedEof {
        offset,
        need: 1,
        have: 0,
    })?;
    WireName::decode(rest)
}

pub(crate) const fn optional_u64_size(value: Option<u64>) -> usize {
    if value.is_some() { 9 } else { 1 }
}

pub(crate) fn put_optional_u64(buf: &mut BytesMut, value: Option<u64>) {
    put_flag(buf, value.is_some());
    if let Some(value) = value {
        buf.put_u64_le(value);
    }
}

pub(crate) fn read_optional_u64(
    buf: &[u8],
    offset: usize,
) -> Result<(Option<u64>, usize), WireError> {
    if read_flag(buf, offset)? {
        Ok((Some(read_u64_le(buf, offset + 1)?), 9))
    } else {
        Ok((None, 1))
    }
}

pub(crate) const fn optional_u32_size(value: Option<u32>) -> usize {
    if value.is_some() { 5 } else { 1 }
}

pub(crate) fn put_optional_u32(buf: &mut BytesMut, value: Option<u32>) {
    put_flag(buf, value.is_some());
    if let Some(value) = value {
        buf.put_u32_le(value);
    }
}

pub(crate) fn read_optional_u32(
    buf: &[u8],
    offset: usize,
) -> Result<(Option<u32>, usize), WireError> {
    if read_flag(buf, offset)? {
        Ok((Some(read_u32_le(buf, offset + 1)?), 5))
    } else {
        Ok((None, 1))
    }
}

pub(crate) const fn partition_ids_size(ids: &[u32]) -> usize {
    4 + ids.len() * 4
}

/// Encode `[count:u32][id:u32]*`. Callers hold ascending distinct ids.
pub(crate) fn put_partition_ids(buf: &mut BytesMut, ids: &[u32]) {
    // A topic holds at most `u32::MAX` partitions, so the count fits.
    #[allow(clippy::cast_possible_truncation)]
    buf.put_u32_le(ids.len() as u32);
    for &id in ids {
        buf.put_u32_le(id);
    }
}

/// Decode `[count:u32][id:u32]*`, refusing ids that are not strictly
/// ascending.
pub(crate) fn read_partition_ids(
    buf: &[u8],
    offset: usize,
) -> Result<(Vec<u32>, usize), WireError> {
    let count = read_u32_le(buf, offset)? as usize;
    let mut position = offset + 4;
    let remaining = buf.len().saturating_sub(position);
    let mut ids = Vec::with_capacity(bounded_capacity(count, remaining, 4));
    for _ in 0..count {
        let id = read_u32_le(buf, position)?;
        if ids.last().is_some_and(|&previous| previous >= id) {
            return Err(WireError::Validation(Cow::Borrowed(
                "partition ids must be strictly ascending",
            )));
        }
        ids.push(id);
        position += 4;
    }
    Ok((ids, position - offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_every_group_when_decoding_its_code_should_round_trip_and_set_one_mask_bit() {
        let mut mask = 0u8;
        for code in 1..=8u8 {
            let group = WireMetadataChangeGroup::from_code(code).unwrap();
            assert_eq!(group.as_u8(), code);
            assert_eq!(mask & group.mask_bit(), 0);
            mask |= group.mask_bit();
        }
        assert_eq!(mask, u8::MAX);
    }

    #[test]
    fn given_reserved_group_codes_when_decoding_should_refuse() {
        for code in [0u8, 9, 127, 128, 255] {
            assert!(WireMetadataChangeGroup::from_code(code).is_err());
        }
    }

    #[test]
    fn given_every_kind_when_decoding_its_code_should_round_trip() {
        let codes = [
            1u8, 2, 3, 10, 11, 12, 20, 30, 31, 40, 41, 42, 50, 51, 52, 53, 54, 60, 61, 70, 71,
        ];
        for code in codes {
            let kind = WireMetadataChangeKind::from_code(code).unwrap();
            assert_eq!(kind.as_u8(), code);
        }
        for code in 0..=u8::MAX {
            assert_eq!(
                WireMetadataChangeKind::from_code(code).is_ok(),
                codes.contains(&code)
            );
        }
    }

    #[test]
    fn given_kind_codes_when_mapping_groups_should_follow_the_catalog_decades() {
        for code in 0..=u8::MAX {
            if let Ok(kind) = WireMetadataChangeKind::from_code(code) {
                let expected = match code {
                    1..=3 => 1,
                    10..=12 => 2,
                    20 => 3,
                    30 | 31 => 4,
                    40..=42 => 5,
                    50..=54 => 6,
                    60 | 61 => 7,
                    _ => 8,
                };
                assert_eq!(kind.group().as_u8(), expected, "kind {code}");
            }
        }
    }

    #[test]
    fn given_unknown_reason_codes_when_decoding_should_refuse() {
        assert!(WireMemberLeftReason::from_code(2).is_err());
        assert!(WireTokenDeletedReason::from_code(2).is_err());
        assert!(WireSessionClosedReason::from_code(3).is_err());
        assert!(WirePermissionsUpdatedReason::from_code(2).is_err());
        assert!(WireMetadataChangeStatus::from_code(4).is_err());
        assert!(WireMetadataChangePhase::from_code(2).is_err());
        assert!(WireMetadataChangeStartMode::from_code(4).is_err());
    }

    #[test]
    fn given_flag_byte_above_one_when_reading_should_refuse() {
        assert!(!read_flag(&[0], 0).unwrap());
        assert!(read_flag(&[1], 0).unwrap());
        assert!(read_flag(&[2], 0).is_err());
        assert!(read_flag(&[], 0).is_err());
    }

    #[test]
    fn given_partition_ids_when_round_tripping_should_preserve_order() {
        let ids = [0u32, 3, 7, u32::MAX];
        let mut buf = BytesMut::new();
        put_partition_ids(&mut buf, &ids);
        assert_eq!(buf.len(), partition_ids_size(&ids));
        let (decoded, consumed) = read_partition_ids(&buf, 0).unwrap();
        assert_eq!(decoded, ids);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn given_unsorted_or_duplicate_partition_ids_when_decoding_should_refuse() {
        for ids in [[3u32, 1], [2, 2]] {
            let mut buf = BytesMut::new();
            buf.put_u32_le(2);
            for id in ids {
                buf.put_u32_le(id);
            }
            assert!(read_partition_ids(&buf, 0).is_err());
        }
    }

    #[test]
    fn given_huge_partition_count_when_decoding_should_fail_without_allocating_it() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(u32::MAX);
        assert!(read_partition_ids(&buf, 0).is_err());
    }
}
