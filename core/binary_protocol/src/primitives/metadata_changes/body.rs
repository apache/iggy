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

use super::{
    WireMemberLeftReason, WireMetadataChangeGroup, WireMetadataChangeKind,
    WirePermissionsUpdatedReason, WireSessionClosedReason, WireTokenDeletedReason,
    optional_u32_size, optional_u64_size, partition_ids_size, put_flag, put_optional_u32,
    put_optional_u64, put_partition_ids, read_flag, read_optional_u32, read_optional_u64,
    read_partition_ids, read_wire_name,
};
use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le, read_u64_le, read_u128_le};
use crate::primitives::identifier::WireName;
use crate::primitives::options::{
    WireOptions, decode_options_prefixed, encode_options_prefixed, options_prefixed_size,
};
use crate::primitives::permissions::WirePermissions;
use bytes::{BufMut, Bytes, BytesMut};
use std::borrow::Cow;

/// A change record body, typed by its kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMetadataChange {
    StreamCreated(WireStreamBody),
    StreamUpdated(WireStreamBody),
    StreamDeleted(WireStreamDeletedBody),
    TopicCreated(WireTopicBody),
    TopicUpdated(WireTopicBody),
    TopicDeleted(WireTopicDeletedBody),
    PartitionsChanged(WirePartitionsChangedBody),
    ConsumerGroupCreated(WireConsumerGroupCreatedBody),
    ConsumerGroupDeleted(WireConsumerGroupDeletedBody),
    ConsumerGroupMemberJoined(WireMemberJoinedBody),
    ConsumerGroupMemberLeft(WireMemberLeftBody),
    ConsumerGroupPartitionHandedOver(WirePartitionHandedOverBody),
    UserCreated(WireUserBody),
    UserUpdated(WireUserBody),
    UserDeleted(WireUserIdentityBody),
    UserPasswordChanged(WireUserIdentityBody),
    UserPermissionsUpdated(WireUserPermissionsBody),
    PersonalAccessTokenCreated(WireTokenBody),
    PersonalAccessTokenDeleted(WireTokenDeletedBody),
    SessionRegistered(WireSessionBody),
    SessionClosed(WireSessionClosedBody),
}

/// A current state item body, typed by its group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMetadataCurrentItem {
    Stream(WireStreamBody),
    Topic(WireTopicItemBody),
    Partitions(WirePartitionsItemBody),
    ConsumerGroup(WireConsumerGroupItemBody),
    ConsumerGroupMember(WireMemberItemBody),
    User(WireUserBody),
    PersonalAccessToken(WireTokenBody),
    Session(WireSessionBody),
}

/// Stream identity: ids are reused, the creating op is not.
///
/// Wire format: `[stream_id:u32][stream_created_op:u64]`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WireStreamKey {
    pub stream_id: u32,
    pub stream_created_op: u64,
}

/// Topic identity: the stream key plus the topic id and creating op.
///
/// Wire format: `[stream key:12][topic_id:u32][topic_created_op:u64]`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WireTopicKey {
    pub stream: WireStreamKey,
    pub topic_id: u32,
    pub topic_created_op: u64,
}

/// User identity, the one sessions and tokens bind to.
///
/// Wire format: `[user_id:u32][user_created_at:u64]`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WireUserKey {
    pub user_id: u32,
    pub user_created_at: u64,
}

/// Public options of a resource, or the marker for stored critical state this
/// build cannot read.
///
/// Wire format: `[state:u8]`, then for state 0 the explicit and derived blocks
/// as the read responses encode them, nothing for state 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireChangeOptions {
    Readable {
        explicit: WireOptions,
        derived: WireOptions,
    },
    Unreadable,
}

/// Body of `stream_created`, `stream_updated` and of a `streams` item.
///
/// Wire format: `[stream key][created_at:u64][name][options]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireStreamBody {
    pub key: WireStreamKey,
    pub created_at: u64,
    pub name: WireName,
    pub options: WireChangeOptions,
}

/// Body of `stream_deleted`. `requested_op` is present on a deferred
/// completion.
///
/// Wire format: `[stream key][created_at:u64][name][has:u8][requested_op:u64]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireStreamDeletedBody {
    pub key: WireStreamKey,
    pub created_at: u64,
    pub name: WireName,
    pub requested_op: Option<u64>,
}

/// Body of `topic_created` and `topic_updated`.
///
/// Wire format: `[topic key][created_at:u64][name][partitions_count:u32][options]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireTopicBody {
    pub key: WireTopicKey,
    pub created_at: u64,
    pub name: WireName,
    pub partitions_count: u32,
    pub options: WireChangeOptions,
}

/// Body of a `topics` item. Partition counts travel as `partitions` items.
///
/// Wire format: `[topic key][created_at:u64][name][options]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireTopicItemBody {
    pub key: WireTopicKey,
    pub created_at: u64,
    pub name: WireName,
    pub options: WireChangeOptions,
}

/// Body of `topic_deleted`.
///
/// Wire format: `[topic key][created_at:u64][name][has:u8][requested_op:u64]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireTopicDeletedBody {
    pub key: WireTopicKey,
    pub created_at: u64,
    pub name: WireName,
    pub requested_op: Option<u64>,
}

/// Body of `partitions_changed`. Removed ids are the actual deleted tail,
/// ascending, empty on create.
///
/// Wire format: `[topic key][partitions_count:u32][ids][has:u8][requested_op:u64]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WirePartitionsChangedBody {
    pub topic: WireTopicKey,
    pub partitions_count: u32,
    pub removed_partition_ids: Vec<u32>,
    pub requested_op: Option<u64>,
}

/// Body of a `partitions` item.
///
/// Wire format: `[topic key][partitions_count:u32]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WirePartitionsItemBody {
    pub topic: WireTopicKey,
    pub partitions_count: u32,
}

/// Body of `consumer_group_created`.
///
/// Wire format: `[topic key][group_id:u32][name][consumer_groups_count:u32]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireConsumerGroupCreatedBody {
    pub topic: WireTopicKey,
    pub group_id: u32,
    pub name: WireName,
    pub consumer_groups_count: u32,
}

/// Body of `consumer_group_deleted`.
///
/// Wire format: `[topic key][group_id:u32][name][consumer_groups_count:u32][has:u8][requested_op:u64]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireConsumerGroupDeletedBody {
    pub topic: WireTopicKey,
    pub group_id: u32,
    pub name: WireName,
    pub consumer_groups_count: u32,
    pub requested_op: Option<u64>,
}

/// Body of a `consumer_groups` item.
///
/// Wire format: `[topic key][group_id:u32][name]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireConsumerGroupItemBody {
    pub topic: WireTopicKey,
    pub group_id: u32,
    pub name: WireName,
}

/// Body of `consumer_group_member_joined`.
///
/// Wire format: `[topic key][group_id:u32][member_id:u32][client_id:u128][members_count:u32][ids]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMemberJoinedBody {
    pub topic: WireTopicKey,
    pub group_id: u32,
    pub member_id: u32,
    pub client_id: u128,
    pub members_count: u32,
    pub partition_ids: Vec<u32>,
}

/// Body of `consumer_group_member_left`.
///
/// Wire format: `[topic key][group_id:u32][member_id:u32][client_id:u128][members_count:u32][reason:u8]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMemberLeftBody {
    pub topic: WireTopicKey,
    pub group_id: u32,
    pub member_id: u32,
    pub client_id: u128,
    pub members_count: u32,
    pub reason: WireMemberLeftReason,
}

/// Body of `consumer_group_partition_handed_over`. The owner is absent when
/// the last member left.
///
/// Wire format: `[topic key][group_id:u32][partition_id:u32][has:u8][member_id:u32]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WirePartitionHandedOverBody {
    pub topic: WireTopicKey,
    pub group_id: u32,
    pub partition_id: u32,
    pub member_id: Option<u32>,
}

/// Body of a `consumer_group_members` item.
///
/// Wire format: `[topic key][group_id:u32][member_id:u32][client_id:u128][ids]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMemberItemBody {
    pub topic: WireTopicKey,
    pub group_id: u32,
    pub member_id: u32,
    pub client_id: u128,
    pub partition_ids: Vec<u32>,
}

/// Body of `user_created`, `user_updated` and of a `users` item.
///
/// Wire format: `[user key][username][status:u8][has:u8][permissions][options]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireUserBody {
    pub key: WireUserKey,
    pub username: WireName,
    pub status: u8,
    pub permissions: Option<WirePermissions>,
    pub options: WireChangeOptions,
}

/// Body of `user_deleted` and `user_password_changed`.
///
/// Wire format: `[user key][username]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireUserIdentityBody {
    pub key: WireUserKey,
    pub username: WireName,
}

/// Body of `user_permissions_updated`.
///
/// Wire format: `[user key][username][has:u8][permissions][reason:u8]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireUserPermissionsBody {
    pub key: WireUserKey,
    pub username: WireName,
    pub permissions: Option<WirePermissions>,
    pub reason: WirePermissionsUpdatedReason,
}

/// Body of `personal_access_token_created` and of a `personal_access_tokens`
/// item. Expiry 0 means the token never expires.
///
/// Wire format: `[user key][name][expiry:u64]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireTokenBody {
    pub user: WireUserKey,
    pub name: WireName,
    pub expiry: u64,
}

/// Body of `personal_access_token_deleted`.
///
/// Wire format: `[user key][name][reason:u8]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireTokenDeletedBody {
    pub user: WireUserKey,
    pub name: WireName,
    pub reason: WireTokenDeletedReason,
}

/// Body of `session_registered` and of a `sessions` item.
///
/// Wire format: `[user key][client_id:u128][session:u64]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireSessionBody {
    pub user: WireUserKey,
    pub client_id: u128,
    pub session: u64,
}

/// Body of `session_closed`.
///
/// Wire format: `[user key][client_id:u128][session:u64][reason:u8]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireSessionClosedBody {
    pub user: WireUserKey,
    pub client_id: u128,
    pub session: u64,
    pub reason: WireSessionClosedReason,
}

impl WireMetadataChange {
    #[must_use]
    pub const fn kind(&self) -> WireMetadataChangeKind {
        match self {
            Self::StreamCreated(_) => WireMetadataChangeKind::StreamCreated,
            Self::StreamUpdated(_) => WireMetadataChangeKind::StreamUpdated,
            Self::StreamDeleted(_) => WireMetadataChangeKind::StreamDeleted,
            Self::TopicCreated(_) => WireMetadataChangeKind::TopicCreated,
            Self::TopicUpdated(_) => WireMetadataChangeKind::TopicUpdated,
            Self::TopicDeleted(_) => WireMetadataChangeKind::TopicDeleted,
            Self::PartitionsChanged(_) => WireMetadataChangeKind::PartitionsChanged,
            Self::ConsumerGroupCreated(_) => WireMetadataChangeKind::ConsumerGroupCreated,
            Self::ConsumerGroupDeleted(_) => WireMetadataChangeKind::ConsumerGroupDeleted,
            Self::ConsumerGroupMemberJoined(_) => WireMetadataChangeKind::ConsumerGroupMemberJoined,
            Self::ConsumerGroupMemberLeft(_) => WireMetadataChangeKind::ConsumerGroupMemberLeft,
            Self::ConsumerGroupPartitionHandedOver(_) => {
                WireMetadataChangeKind::ConsumerGroupPartitionHandedOver
            }
            Self::UserCreated(_) => WireMetadataChangeKind::UserCreated,
            Self::UserUpdated(_) => WireMetadataChangeKind::UserUpdated,
            Self::UserDeleted(_) => WireMetadataChangeKind::UserDeleted,
            Self::UserPasswordChanged(_) => WireMetadataChangeKind::UserPasswordChanged,
            Self::UserPermissionsUpdated(_) => WireMetadataChangeKind::UserPermissionsUpdated,
            Self::PersonalAccessTokenCreated(_) => {
                WireMetadataChangeKind::PersonalAccessTokenCreated
            }
            Self::PersonalAccessTokenDeleted(_) => {
                WireMetadataChangeKind::PersonalAccessTokenDeleted
            }
            Self::SessionRegistered(_) => WireMetadataChangeKind::SessionRegistered,
            Self::SessionClosed(_) => WireMetadataChangeKind::SessionClosed,
        }
    }

    #[must_use]
    pub const fn group(&self) -> WireMetadataChangeGroup {
        self.kind().group()
    }

    /// Encoded body bytes, without the record envelope.
    #[must_use]
    pub fn body_bytes(&self) -> Bytes {
        match self {
            Self::StreamCreated(body) | Self::StreamUpdated(body) => body.to_bytes(),
            Self::StreamDeleted(body) => body.to_bytes(),
            Self::TopicCreated(body) | Self::TopicUpdated(body) => body.to_bytes(),
            Self::TopicDeleted(body) => body.to_bytes(),
            Self::PartitionsChanged(body) => body.to_bytes(),
            Self::ConsumerGroupCreated(body) => body.to_bytes(),
            Self::ConsumerGroupDeleted(body) => body.to_bytes(),
            Self::ConsumerGroupMemberJoined(body) => body.to_bytes(),
            Self::ConsumerGroupMemberLeft(body) => body.to_bytes(),
            Self::ConsumerGroupPartitionHandedOver(body) => body.to_bytes(),
            Self::UserCreated(body) | Self::UserUpdated(body) => body.to_bytes(),
            Self::UserDeleted(body) | Self::UserPasswordChanged(body) => body.to_bytes(),
            Self::UserPermissionsUpdated(body) => body.to_bytes(),
            Self::PersonalAccessTokenCreated(body) => body.to_bytes(),
            Self::PersonalAccessTokenDeleted(body) => body.to_bytes(),
            Self::SessionRegistered(body) => body.to_bytes(),
            Self::SessionClosed(body) => body.to_bytes(),
        }
    }

    /// Decode a record body by its group and kind codes.
    ///
    /// Returns `Ok(None)` for a group or kind this build does not know, so a
    /// caller can keep the record with its raw body.
    ///
    /// # Errors
    /// Returns `WireError` for a known kind under another group, a malformed
    /// body, an unknown reason code or trailing bytes.
    pub fn decode(group: u8, kind: u8, body: &[u8]) -> Result<Option<Self>, WireError> {
        let Ok(group) = WireMetadataChangeGroup::from_code(group) else {
            return Ok(None);
        };
        let Ok(kind) = WireMetadataChangeKind::from_code(kind) else {
            return Ok(None);
        };
        if kind.group() != group {
            return Err(WireError::Validation(Cow::Owned(format!(
                "change kind {} does not belong to group {}",
                kind.as_u8(),
                group.as_u8()
            ))));
        }
        let change = match kind {
            WireMetadataChangeKind::StreamCreated => Self::StreamCreated(decode_exact(body)?),
            WireMetadataChangeKind::StreamUpdated => Self::StreamUpdated(decode_exact(body)?),
            WireMetadataChangeKind::StreamDeleted => Self::StreamDeleted(decode_exact(body)?),
            WireMetadataChangeKind::TopicCreated => Self::TopicCreated(decode_exact(body)?),
            WireMetadataChangeKind::TopicUpdated => Self::TopicUpdated(decode_exact(body)?),
            WireMetadataChangeKind::TopicDeleted => Self::TopicDeleted(decode_exact(body)?),
            WireMetadataChangeKind::PartitionsChanged => {
                Self::PartitionsChanged(decode_exact(body)?)
            }
            WireMetadataChangeKind::ConsumerGroupCreated => {
                Self::ConsumerGroupCreated(decode_exact(body)?)
            }
            WireMetadataChangeKind::ConsumerGroupDeleted => {
                Self::ConsumerGroupDeleted(decode_exact(body)?)
            }
            WireMetadataChangeKind::ConsumerGroupMemberJoined => {
                Self::ConsumerGroupMemberJoined(decode_exact(body)?)
            }
            WireMetadataChangeKind::ConsumerGroupMemberLeft => {
                Self::ConsumerGroupMemberLeft(decode_exact(body)?)
            }
            WireMetadataChangeKind::ConsumerGroupPartitionHandedOver => {
                Self::ConsumerGroupPartitionHandedOver(decode_exact(body)?)
            }
            WireMetadataChangeKind::UserCreated => Self::UserCreated(decode_exact(body)?),
            WireMetadataChangeKind::UserUpdated => Self::UserUpdated(decode_exact(body)?),
            WireMetadataChangeKind::UserDeleted => Self::UserDeleted(decode_exact(body)?),
            WireMetadataChangeKind::UserPasswordChanged => {
                Self::UserPasswordChanged(decode_exact(body)?)
            }
            WireMetadataChangeKind::UserPermissionsUpdated => {
                Self::UserPermissionsUpdated(decode_exact(body)?)
            }
            WireMetadataChangeKind::PersonalAccessTokenCreated => {
                Self::PersonalAccessTokenCreated(decode_exact(body)?)
            }
            WireMetadataChangeKind::PersonalAccessTokenDeleted => {
                Self::PersonalAccessTokenDeleted(decode_exact(body)?)
            }
            WireMetadataChangeKind::SessionRegistered => {
                Self::SessionRegistered(decode_exact(body)?)
            }
            WireMetadataChangeKind::SessionClosed => Self::SessionClosed(decode_exact(body)?),
        };
        Ok(Some(change))
    }
}

impl WireMetadataCurrentItem {
    #[must_use]
    pub const fn group(&self) -> WireMetadataChangeGroup {
        match self {
            Self::Stream(_) => WireMetadataChangeGroup::Streams,
            Self::Topic(_) => WireMetadataChangeGroup::Topics,
            Self::Partitions(_) => WireMetadataChangeGroup::Partitions,
            Self::ConsumerGroup(_) => WireMetadataChangeGroup::ConsumerGroups,
            Self::ConsumerGroupMember(_) => WireMetadataChangeGroup::ConsumerGroupMembers,
            Self::User(_) => WireMetadataChangeGroup::Users,
            Self::PersonalAccessToken(_) => WireMetadataChangeGroup::PersonalAccessTokens,
            Self::Session(_) => WireMetadataChangeGroup::Sessions,
        }
    }

    /// Encoded body bytes, without the item envelope.
    #[must_use]
    pub fn body_bytes(&self) -> Bytes {
        match self {
            Self::Stream(body) => body.to_bytes(),
            Self::Topic(body) => body.to_bytes(),
            Self::Partitions(body) => body.to_bytes(),
            Self::ConsumerGroup(body) => body.to_bytes(),
            Self::ConsumerGroupMember(body) => body.to_bytes(),
            Self::User(body) => body.to_bytes(),
            Self::PersonalAccessToken(body) => body.to_bytes(),
            Self::Session(body) => body.to_bytes(),
        }
    }

    /// Decode an item body by its group code.
    ///
    /// Returns `Ok(None)` for a group this build does not know.
    ///
    /// # Errors
    /// Returns `WireError` for a malformed body or trailing bytes.
    pub fn decode(group: u8, body: &[u8]) -> Result<Option<Self>, WireError> {
        let Ok(group) = WireMetadataChangeGroup::from_code(group) else {
            return Ok(None);
        };
        let item = match group {
            WireMetadataChangeGroup::Streams => Self::Stream(decode_exact(body)?),
            WireMetadataChangeGroup::Topics => Self::Topic(decode_exact(body)?),
            WireMetadataChangeGroup::Partitions => Self::Partitions(decode_exact(body)?),
            WireMetadataChangeGroup::ConsumerGroups => Self::ConsumerGroup(decode_exact(body)?),
            WireMetadataChangeGroup::ConsumerGroupMembers => {
                Self::ConsumerGroupMember(decode_exact(body)?)
            }
            WireMetadataChangeGroup::Users => Self::User(decode_exact(body)?),
            WireMetadataChangeGroup::PersonalAccessTokens => {
                Self::PersonalAccessToken(decode_exact(body)?)
            }
            WireMetadataChangeGroup::Sessions => Self::Session(decode_exact(body)?),
        };
        Ok(Some(item))
    }
}

/// Decode a body that must fill `body` exactly.
fn decode_exact<T: WireDecode>(body: &[u8]) -> Result<T, WireError> {
    let (value, consumed) = T::decode(body)?;
    if consumed != body.len() {
        return Err(WireError::Validation(Cow::Owned(format!(
            "{} trailing bytes after a change body",
            body.len() - consumed
        ))));
    }
    Ok(value)
}

/// Sequential reader over one body. Every read checks bounds.
struct BodyReader<'a> {
    buf: &'a [u8],
    position: usize,
}

impl<'a> BodyReader<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, position: 0 }
    }

    const fn consumed(&self) -> usize {
        self.position
    }

    fn u8(&mut self) -> Result<u8, WireError> {
        let value = read_u8(self.buf, self.position)?;
        self.position += 1;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        let value = read_u32_le(self.buf, self.position)?;
        self.position += 4;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64, WireError> {
        let value = read_u64_le(self.buf, self.position)?;
        self.position += 8;
        Ok(value)
    }

    fn u128(&mut self) -> Result<u128, WireError> {
        let value = read_u128_le(self.buf, self.position)?;
        self.position += 16;
        Ok(value)
    }

    fn flag(&mut self) -> Result<bool, WireError> {
        let value = read_flag(self.buf, self.position)?;
        self.position += 1;
        Ok(value)
    }

    fn name(&mut self) -> Result<WireName, WireError> {
        let (name, consumed) = read_wire_name(self.buf, self.position)?;
        self.position += consumed;
        Ok(name)
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, WireError> {
        let (value, consumed) = read_optional_u64(self.buf, self.position)?;
        self.position += consumed;
        Ok(value)
    }

    fn optional_u32(&mut self) -> Result<Option<u32>, WireError> {
        let (value, consumed) = read_optional_u32(self.buf, self.position)?;
        self.position += consumed;
        Ok(value)
    }

    fn partition_ids(&mut self) -> Result<Vec<u32>, WireError> {
        let (ids, consumed) = read_partition_ids(self.buf, self.position)?;
        self.position += consumed;
        Ok(ids)
    }

    fn decode<T: WireDecode>(&mut self) -> Result<T, WireError> {
        let rest = self
            .buf
            .get(self.position..)
            .ok_or(WireError::UnexpectedEof {
                offset: self.position,
                need: 1,
                have: 0,
            })?;
        let (value, consumed) = T::decode(rest)?;
        self.position += consumed;
        Ok(value)
    }

    fn options(&mut self) -> Result<WireChangeOptions, WireError> {
        let offset = self.position;
        match self.u8()? {
            0 => {
                let (explicit, explicit_consumed) =
                    decode_options_prefixed(self.buf, self.position)?;
                self.position += explicit_consumed;
                let (derived, derived_consumed) = decode_options_prefixed(self.buf, self.position)?;
                self.position += derived_consumed;
                Ok(WireChangeOptions::Readable { explicit, derived })
            }
            1 => Ok(WireChangeOptions::Unreadable),
            value => Err(WireError::UnknownDiscriminant {
                type_name: "WireChangeOptions",
                value,
                offset,
            }),
        }
    }

    fn permissions(&mut self) -> Result<Option<WirePermissions>, WireError> {
        if self.flag()? {
            Ok(Some(self.decode()?))
        } else {
            Ok(None)
        }
    }

    fn reason<T>(&mut self, from_code: fn(u8) -> Result<T, WireError>) -> Result<T, WireError> {
        let offset = self.position;
        let code = self.u8()?;
        from_code(code).map_err(|error| match error {
            WireError::UnknownDiscriminant {
                type_name, value, ..
            } => WireError::UnknownDiscriminant {
                type_name,
                value,
                offset,
            },
            other => other,
        })
    }
}

fn options_size(options: &WireChangeOptions) -> usize {
    match options {
        WireChangeOptions::Readable { explicit, derived } => {
            1 + options_prefixed_size(explicit) + options_prefixed_size(derived)
        }
        WireChangeOptions::Unreadable => 1,
    }
}

fn put_options(buf: &mut BytesMut, options: &WireChangeOptions) {
    match options {
        WireChangeOptions::Readable { explicit, derived } => {
            buf.put_u8(0);
            encode_options_prefixed(explicit, buf);
            encode_options_prefixed(derived, buf);
        }
        WireChangeOptions::Unreadable => buf.put_u8(1),
    }
}

fn permissions_size(permissions: Option<&WirePermissions>) -> usize {
    1 + permissions.map_or(0, WireEncode::encoded_size)
}

fn put_permissions(buf: &mut BytesMut, permissions: Option<&WirePermissions>) {
    put_flag(buf, permissions.is_some());
    if let Some(permissions) = permissions {
        permissions.encode(buf);
    }
}

impl WireStreamKey {
    const SIZE: usize = 12;
}

impl WireEncode for WireStreamKey {
    fn encoded_size(&self) -> usize {
        Self::SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.stream_id);
        buf.put_u64_le(self.stream_created_op);
    }
}

impl WireDecode for WireStreamKey {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let key = Self {
            stream_id: reader.u32()?,
            stream_created_op: reader.u64()?,
        };
        Ok((key, reader.consumed()))
    }
}

impl WireTopicKey {
    const SIZE: usize = WireStreamKey::SIZE + 12;
}

impl WireEncode for WireTopicKey {
    fn encoded_size(&self) -> usize {
        Self::SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream.encode(buf);
        buf.put_u32_le(self.topic_id);
        buf.put_u64_le(self.topic_created_op);
    }
}

impl WireDecode for WireTopicKey {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let key = Self {
            stream: reader.decode()?,
            topic_id: reader.u32()?,
            topic_created_op: reader.u64()?,
        };
        Ok((key, reader.consumed()))
    }
}

impl WireUserKey {
    const SIZE: usize = 12;
}

impl WireEncode for WireUserKey {
    fn encoded_size(&self) -> usize {
        Self::SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.user_id);
        buf.put_u64_le(self.user_created_at);
    }
}

impl WireDecode for WireUserKey {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let key = Self {
            user_id: reader.u32()?,
            user_created_at: reader.u64()?,
        };
        Ok((key, reader.consumed()))
    }
}

impl WireEncode for WireStreamBody {
    fn encoded_size(&self) -> usize {
        WireStreamKey::SIZE + 8 + self.name.encoded_size() + options_size(&self.options)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        buf.put_u64_le(self.created_at);
        self.name.encode(buf);
        put_options(buf, &self.options);
    }
}

impl WireDecode for WireStreamBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            created_at: reader.u64()?,
            name: reader.name()?,
            options: reader.options()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireStreamDeletedBody {
    fn encoded_size(&self) -> usize {
        WireStreamKey::SIZE + 8 + self.name.encoded_size() + optional_u64_size(self.requested_op)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        buf.put_u64_le(self.created_at);
        self.name.encode(buf);
        put_optional_u64(buf, self.requested_op);
    }
}

impl WireDecode for WireStreamDeletedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            created_at: reader.u64()?,
            name: reader.name()?,
            requested_op: reader.optional_u64()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireTopicBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 8 + self.name.encoded_size() + 4 + options_size(&self.options)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        buf.put_u64_le(self.created_at);
        self.name.encode(buf);
        buf.put_u32_le(self.partitions_count);
        put_options(buf, &self.options);
    }
}

impl WireDecode for WireTopicBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            created_at: reader.u64()?,
            name: reader.name()?,
            partitions_count: reader.u32()?,
            options: reader.options()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireTopicItemBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 8 + self.name.encoded_size() + options_size(&self.options)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        buf.put_u64_le(self.created_at);
        self.name.encode(buf);
        put_options(buf, &self.options);
    }
}

impl WireDecode for WireTopicItemBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            created_at: reader.u64()?,
            name: reader.name()?,
            options: reader.options()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireTopicDeletedBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 8 + self.name.encoded_size() + optional_u64_size(self.requested_op)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        buf.put_u64_le(self.created_at);
        self.name.encode(buf);
        put_optional_u64(buf, self.requested_op);
    }
}

impl WireDecode for WireTopicDeletedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            created_at: reader.u64()?,
            name: reader.name()?,
            requested_op: reader.optional_u64()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WirePartitionsChangedBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE
            + 4
            + partition_ids_size(&self.removed_partition_ids)
            + optional_u64_size(self.requested_op)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.partitions_count);
        put_partition_ids(buf, &self.removed_partition_ids);
        put_optional_u64(buf, self.requested_op);
    }
}

impl WireDecode for WirePartitionsChangedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            partitions_count: reader.u32()?,
            removed_partition_ids: reader.partition_ids()?,
            requested_op: reader.optional_u64()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WirePartitionsItemBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.partitions_count);
    }
}

impl WireDecode for WirePartitionsItemBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            partitions_count: reader.u32()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireConsumerGroupCreatedBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4 + self.name.encoded_size() + 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.group_id);
        self.name.encode(buf);
        buf.put_u32_le(self.consumer_groups_count);
    }
}

impl WireDecode for WireConsumerGroupCreatedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            group_id: reader.u32()?,
            name: reader.name()?,
            consumer_groups_count: reader.u32()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireConsumerGroupDeletedBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4 + self.name.encoded_size() + 4 + optional_u64_size(self.requested_op)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.group_id);
        self.name.encode(buf);
        buf.put_u32_le(self.consumer_groups_count);
        put_optional_u64(buf, self.requested_op);
    }
}

impl WireDecode for WireConsumerGroupDeletedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            group_id: reader.u32()?,
            name: reader.name()?,
            consumer_groups_count: reader.u32()?,
            requested_op: reader.optional_u64()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireConsumerGroupItemBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4 + self.name.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.group_id);
        self.name.encode(buf);
    }
}

impl WireDecode for WireConsumerGroupItemBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            group_id: reader.u32()?,
            name: reader.name()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireMemberJoinedBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4 + 4 + 16 + 4 + partition_ids_size(&self.partition_ids)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.group_id);
        buf.put_u32_le(self.member_id);
        buf.put_u128_le(self.client_id);
        buf.put_u32_le(self.members_count);
        put_partition_ids(buf, &self.partition_ids);
    }
}

impl WireDecode for WireMemberJoinedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            group_id: reader.u32()?,
            member_id: reader.u32()?,
            client_id: reader.u128()?,
            members_count: reader.u32()?,
            partition_ids: reader.partition_ids()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireMemberLeftBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4 + 4 + 16 + 4 + 1
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.group_id);
        buf.put_u32_le(self.member_id);
        buf.put_u128_le(self.client_id);
        buf.put_u32_le(self.members_count);
        buf.put_u8(self.reason.as_u8());
    }
}

impl WireDecode for WireMemberLeftBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            group_id: reader.u32()?,
            member_id: reader.u32()?,
            client_id: reader.u128()?,
            members_count: reader.u32()?,
            reason: reader.reason(WireMemberLeftReason::from_code)?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WirePartitionHandedOverBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4 + 4 + optional_u32_size(self.member_id)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.group_id);
        buf.put_u32_le(self.partition_id);
        put_optional_u32(buf, self.member_id);
    }
}

impl WireDecode for WirePartitionHandedOverBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            group_id: reader.u32()?,
            partition_id: reader.u32()?,
            member_id: reader.optional_u32()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireMemberItemBody {
    fn encoded_size(&self) -> usize {
        WireTopicKey::SIZE + 4 + 4 + 16 + partition_ids_size(&self.partition_ids)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        buf.put_u32_le(self.group_id);
        buf.put_u32_le(self.member_id);
        buf.put_u128_le(self.client_id);
        put_partition_ids(buf, &self.partition_ids);
    }
}

impl WireDecode for WireMemberItemBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            topic: reader.decode()?,
            group_id: reader.u32()?,
            member_id: reader.u32()?,
            client_id: reader.u128()?,
            partition_ids: reader.partition_ids()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireUserBody {
    fn encoded_size(&self) -> usize {
        WireUserKey::SIZE
            + self.username.encoded_size()
            + 1
            + permissions_size(self.permissions.as_ref())
            + options_size(&self.options)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        self.username.encode(buf);
        buf.put_u8(self.status);
        put_permissions(buf, self.permissions.as_ref());
        put_options(buf, &self.options);
    }
}

impl WireDecode for WireUserBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            username: reader.name()?,
            status: reader.u8()?,
            permissions: reader.permissions()?,
            options: reader.options()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireUserIdentityBody {
    fn encoded_size(&self) -> usize {
        WireUserKey::SIZE + self.username.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        self.username.encode(buf);
    }
}

impl WireDecode for WireUserIdentityBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            username: reader.name()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireUserPermissionsBody {
    fn encoded_size(&self) -> usize {
        WireUserKey::SIZE
            + self.username.encoded_size()
            + permissions_size(self.permissions.as_ref())
            + 1
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.key.encode(buf);
        self.username.encode(buf);
        put_permissions(buf, self.permissions.as_ref());
        buf.put_u8(self.reason.as_u8());
    }
}

impl WireDecode for WireUserPermissionsBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            key: reader.decode()?,
            username: reader.name()?,
            permissions: reader.permissions()?,
            reason: reader.reason(WirePermissionsUpdatedReason::from_code)?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireTokenBody {
    fn encoded_size(&self) -> usize {
        WireUserKey::SIZE + self.name.encoded_size() + 8
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user.encode(buf);
        self.name.encode(buf);
        buf.put_u64_le(self.expiry);
    }
}

impl WireDecode for WireTokenBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            user: reader.decode()?,
            name: reader.name()?,
            expiry: reader.u64()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireTokenDeletedBody {
    fn encoded_size(&self) -> usize {
        WireUserKey::SIZE + self.name.encoded_size() + 1
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user.encode(buf);
        self.name.encode(buf);
        buf.put_u8(self.reason.as_u8());
    }
}

impl WireDecode for WireTokenDeletedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            user: reader.decode()?,
            name: reader.name()?,
            reason: reader.reason(WireTokenDeletedReason::from_code)?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireSessionBody {
    fn encoded_size(&self) -> usize {
        WireUserKey::SIZE + 16 + 8
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user.encode(buf);
        buf.put_u128_le(self.client_id);
        buf.put_u64_le(self.session);
    }
}

impl WireDecode for WireSessionBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            user: reader.decode()?,
            client_id: reader.u128()?,
            session: reader.u64()?,
        };
        Ok((body, reader.consumed()))
    }
}

impl WireEncode for WireSessionClosedBody {
    fn encoded_size(&self) -> usize {
        WireUserKey::SIZE + 16 + 8 + 1
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user.encode(buf);
        buf.put_u128_le(self.client_id);
        buf.put_u64_le(self.session);
        buf.put_u8(self.reason.as_u8());
    }
}

impl WireDecode for WireSessionClosedBody {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut reader = BodyReader::new(buf);
        let body = Self {
            user: reader.decode()?,
            client_id: reader.u128()?,
            session: reader.u64()?,
            reason: reader.reason(WireSessionClosedReason::from_code)?,
        };
        Ok((body, reader.consumed()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::permissions::{
        WireGlobalPermissions, WireStreamPermissions, WireTopicPermissions,
    };

    #[test]
    fn given_every_change_kind_when_round_tripping_should_preserve_every_field() {
        for change in sample_changes() {
            let body = change.body_bytes();
            let decoded =
                WireMetadataChange::decode(change.group().as_u8(), change.kind().as_u8(), &body)
                    .unwrap()
                    .unwrap();
            assert_eq!(decoded, change);
        }
    }

    #[test]
    fn given_every_item_group_when_round_tripping_should_preserve_every_field() {
        for item in sample_items() {
            let body = item.body_bytes();
            let decoded = WireMetadataCurrentItem::decode(item.group().as_u8(), &body)
                .unwrap()
                .unwrap();
            assert_eq!(decoded, item);
        }
    }

    #[test]
    fn given_every_change_kind_when_truncating_the_body_should_refuse_every_prefix() {
        for change in sample_changes() {
            let body = change.body_bytes();
            for length in 0..body.len() {
                assert!(
                    WireMetadataChange::decode(
                        change.group().as_u8(),
                        change.kind().as_u8(),
                        &body[..length]
                    )
                    .is_err(),
                    "{:?} accepted a body truncated to {length} bytes",
                    change.kind()
                );
            }
        }
    }

    #[test]
    fn given_trailing_bytes_when_decoding_a_change_body_should_refuse() {
        for change in sample_changes() {
            let mut body = change.body_bytes().to_vec();
            body.push(0);
            assert!(
                WireMetadataChange::decode(change.group().as_u8(), change.kind().as_u8(), &body)
                    .is_err(),
                "{:?} accepted a trailing byte",
                change.kind()
            );
        }
        for item in sample_items() {
            let mut body = item.body_bytes().to_vec();
            body.push(0);
            assert!(WireMetadataCurrentItem::decode(item.group().as_u8(), &body).is_err());
        }
    }

    #[test]
    fn given_known_kind_under_wrong_group_when_decoding_should_refuse() {
        let change = WireMetadataChange::StreamCreated(stream_body());
        let body = change.body_bytes();
        let error = WireMetadataChange::decode(
            WireMetadataChangeGroup::Topics.as_u8(),
            WireMetadataChangeKind::StreamCreated.as_u8(),
            &body,
        );
        assert!(error.is_err());
    }

    #[test]
    fn given_unknown_kind_or_group_when_decoding_should_return_none() {
        assert_eq!(WireMetadataChange::decode(1, 9, &[1, 2, 3]), Ok(None));
        assert_eq!(WireMetadataChange::decode(200, 1, &[]), Ok(None));
        assert_eq!(WireMetadataChange::decode(0, 1, &[]), Ok(None));
        assert_eq!(WireMetadataCurrentItem::decode(9, &[7]), Ok(None));
    }

    #[test]
    fn given_unknown_reason_code_when_decoding_member_left_should_refuse() {
        let change = WireMetadataChange::ConsumerGroupMemberLeft(WireMemberLeftBody {
            topic: topic_key(),
            group_id: 1,
            member_id: 2,
            client_id: 3,
            members_count: 0,
            reason: WireMemberLeftReason::Left,
        });
        let mut body = change.body_bytes().to_vec();
        *body.last_mut().unwrap() = 2;
        assert!(
            WireMetadataChange::decode(change.group().as_u8(), change.kind().as_u8(), &body)
                .is_err()
        );
    }

    #[test]
    fn given_unknown_reasons_when_decoding_token_session_and_permission_bodies_should_refuse() {
        let changes = [
            WireMetadataChange::PersonalAccessTokenDeleted(WireTokenDeletedBody {
                user: user_key(),
                name: WireName::new("token").unwrap(),
                reason: WireTokenDeletedReason::Expired,
            }),
            WireMetadataChange::SessionClosed(WireSessionClosedBody {
                user: user_key(),
                client_id: 9,
                session: 4,
                reason: WireSessionClosedReason::Expired,
            }),
            WireMetadataChange::UserPermissionsUpdated(WireUserPermissionsBody {
                key: user_key(),
                username: WireName::new("alice").unwrap(),
                permissions: None,
                reason: WirePermissionsUpdatedReason::ResourceDeleted,
            }),
        ];
        for change in changes {
            let mut body = change.body_bytes().to_vec();
            *body.last_mut().unwrap() = 7;
            assert!(
                WireMetadataChange::decode(change.group().as_u8(), change.kind().as_u8(), &body)
                    .is_err(),
                "{:?} accepted reason code 7",
                change.kind()
            );
        }
    }

    #[test]
    fn given_options_state_byte_above_one_when_decoding_should_refuse() {
        let change = WireMetadataChange::StreamCreated(stream_body());
        let mut body = change.body_bytes().to_vec();
        let state_offset = WireStreamKey::SIZE + 8 + 1 + "orders".len();
        body[state_offset] = 2;
        assert!(
            WireMetadataChange::decode(change.group().as_u8(), change.kind().as_u8(), &body)
                .is_err()
        );
    }

    #[test]
    fn given_presence_byte_above_one_when_decoding_requested_op_should_refuse() {
        let change = WireMetadataChange::StreamDeleted(WireStreamDeletedBody {
            key: stream_key(),
            created_at: 1,
            name: WireName::new("orders").unwrap(),
            requested_op: None,
        });
        let mut body = change.body_bytes().to_vec();
        *body.last_mut().unwrap() = 2;
        assert!(
            WireMetadataChange::decode(change.group().as_u8(), change.kind().as_u8(), &body)
                .is_err()
        );
    }

    #[test]
    fn given_unreadable_options_when_round_tripping_should_keep_the_marker() {
        let change = WireMetadataChange::TopicUpdated(WireTopicBody {
            options: WireChangeOptions::Unreadable,
            ..topic_body()
        });
        let body = change.body_bytes();
        let decoded =
            WireMetadataChange::decode(change.group().as_u8(), change.kind().as_u8(), &body)
                .unwrap()
                .unwrap();
        assert_eq!(decoded, change);
    }

    fn stream_key() -> WireStreamKey {
        WireStreamKey {
            stream_id: 7,
            stream_created_op: 4121,
        }
    }

    fn topic_key() -> WireTopicKey {
        WireTopicKey {
            stream: stream_key(),
            topic_id: 3,
            topic_created_op: 4125,
        }
    }

    fn user_key() -> WireUserKey {
        WireUserKey {
            user_id: 9,
            user_created_at: 1_760_300_000_000_000,
        }
    }

    fn options() -> WireChangeOptions {
        let mut buf = BytesMut::new();
        let value = 1_000u64.to_le_bytes();
        crate::primitives::user_headers::encode_user_headers(
            &[(2u8, b"message_expiry".as_slice(), 8u8, value.as_slice())],
            &mut buf,
        );
        WireChangeOptions::Readable {
            explicit: WireOptions::from_slice(&buf).unwrap(),
            derived: WireOptions::empty(),
        }
    }

    fn permissions() -> WirePermissions {
        WirePermissions {
            global: WireGlobalPermissions {
                manage_servers: false,
                read_servers: true,
                manage_users: false,
                read_users: true,
                manage_streams: false,
                read_streams: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: false,
            },
            streams: vec![WireStreamPermissions {
                stream_id: 7,
                manage_stream: false,
                read_stream: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: false,
                topics: vec![WireTopicPermissions {
                    topic_id: 3,
                    manage_topic: false,
                    read_topic: true,
                    poll_messages: true,
                    send_messages: false,
                }],
            }],
        }
    }

    fn stream_body() -> WireStreamBody {
        WireStreamBody {
            key: stream_key(),
            created_at: 1_760_356_800_123_456,
            name: WireName::new("orders").unwrap(),
            options: options(),
        }
    }

    fn topic_body() -> WireTopicBody {
        WireTopicBody {
            key: topic_key(),
            created_at: 1_760_356_801_000_000,
            name: WireName::new("events").unwrap(),
            partitions_count: 8,
            options: options(),
        }
    }

    fn user_body(permissions: Option<WirePermissions>) -> WireUserBody {
        WireUserBody {
            key: user_key(),
            username: WireName::new("alice").unwrap(),
            status: 1,
            permissions,
            options: WireChangeOptions::Readable {
                explicit: WireOptions::empty(),
                derived: WireOptions::empty(),
            },
        }
    }

    fn sample_changes() -> Vec<WireMetadataChange> {
        let name = || WireName::new("audit").unwrap();
        vec![
            WireMetadataChange::StreamCreated(stream_body()),
            WireMetadataChange::StreamUpdated(WireStreamBody {
                options: WireChangeOptions::Unreadable,
                ..stream_body()
            }),
            WireMetadataChange::StreamDeleted(WireStreamDeletedBody {
                key: stream_key(),
                created_at: 1,
                name: name(),
                requested_op: Some(4140),
            }),
            WireMetadataChange::TopicCreated(topic_body()),
            WireMetadataChange::TopicUpdated(topic_body()),
            WireMetadataChange::TopicDeleted(WireTopicDeletedBody {
                key: topic_key(),
                created_at: 2,
                name: name(),
                requested_op: None,
            }),
            WireMetadataChange::PartitionsChanged(WirePartitionsChangedBody {
                topic: topic_key(),
                partitions_count: 5,
                removed_partition_ids: vec![5, 6, 7],
                requested_op: Some(4200),
            }),
            WireMetadataChange::ConsumerGroupCreated(WireConsumerGroupCreatedBody {
                topic: topic_key(),
                group_id: 0,
                name: name(),
                consumer_groups_count: 1,
            }),
            WireMetadataChange::ConsumerGroupDeleted(WireConsumerGroupDeletedBody {
                topic: topic_key(),
                group_id: 0,
                name: name(),
                consumer_groups_count: 0,
                requested_op: Some(4300),
            }),
            WireMetadataChange::ConsumerGroupMemberJoined(WireMemberJoinedBody {
                topic: topic_key(),
                group_id: 1,
                member_id: 2,
                client_id: u128::MAX - 1,
                members_count: 2,
                partition_ids: vec![],
            }),
            WireMetadataChange::ConsumerGroupMemberLeft(WireMemberLeftBody {
                topic: topic_key(),
                group_id: 1,
                member_id: 2,
                client_id: 77,
                members_count: 1,
                reason: WireMemberLeftReason::SessionEnded,
            }),
            WireMetadataChange::ConsumerGroupPartitionHandedOver(WirePartitionHandedOverBody {
                topic: topic_key(),
                group_id: 1,
                partition_id: 4,
                member_id: None,
            }),
            WireMetadataChange::UserCreated(user_body(Some(permissions()))),
            WireMetadataChange::UserUpdated(user_body(None)),
            WireMetadataChange::UserDeleted(WireUserIdentityBody {
                key: user_key(),
                username: WireName::new("alice").unwrap(),
            }),
            WireMetadataChange::UserPasswordChanged(WireUserIdentityBody {
                key: user_key(),
                username: WireName::new("alice").unwrap(),
            }),
            WireMetadataChange::UserPermissionsUpdated(WireUserPermissionsBody {
                key: user_key(),
                username: WireName::new("alice").unwrap(),
                permissions: Some(permissions()),
                reason: WirePermissionsUpdatedReason::Updated,
            }),
            WireMetadataChange::PersonalAccessTokenCreated(WireTokenBody {
                user: user_key(),
                name: WireName::new("ci-token").unwrap(),
                expiry: 0,
            }),
            WireMetadataChange::PersonalAccessTokenDeleted(WireTokenDeletedBody {
                user: user_key(),
                name: WireName::new("ci-token").unwrap(),
                reason: WireTokenDeletedReason::Deleted,
            }),
            WireMetadataChange::SessionRegistered(WireSessionBody {
                user: user_key(),
                client_id: 1 << 100,
                session: 4,
            }),
            WireMetadataChange::SessionClosed(WireSessionClosedBody {
                user: user_key(),
                client_id: 1 << 100,
                session: 4,
                reason: WireSessionClosedReason::Logout,
            }),
        ]
    }

    fn sample_items() -> Vec<WireMetadataCurrentItem> {
        vec![
            WireMetadataCurrentItem::Stream(stream_body()),
            WireMetadataCurrentItem::Topic(WireTopicItemBody {
                key: topic_key(),
                created_at: 3,
                name: WireName::new("events").unwrap(),
                options: options(),
            }),
            WireMetadataCurrentItem::Partitions(WirePartitionsItemBody {
                topic: topic_key(),
                partitions_count: 12,
            }),
            WireMetadataCurrentItem::ConsumerGroup(WireConsumerGroupItemBody {
                topic: topic_key(),
                group_id: 4,
                name: WireName::new("readers").unwrap(),
            }),
            WireMetadataCurrentItem::ConsumerGroupMember(WireMemberItemBody {
                topic: topic_key(),
                group_id: 4,
                member_id: 1,
                client_id: 12,
                partition_ids: vec![0, 2],
            }),
            WireMetadataCurrentItem::User(user_body(Some(permissions()))),
            WireMetadataCurrentItem::PersonalAccessToken(WireTokenBody {
                user: user_key(),
                name: WireName::new("ci-token").unwrap(),
                expiry: 1_760_400_000_000_000,
            }),
            WireMetadataCurrentItem::Session(WireSessionBody {
                user: user_key(),
                client_id: 5,
                session: 6,
            }),
        ]
    }
}
