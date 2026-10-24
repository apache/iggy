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

use super::json::hex_u128;
use super::{
    MetadataChangeGroup, MetadataChangeKind, MetadataMemberLeftReason,
    MetadataPermissionsUpdatedReason, MetadataSessionClosedReason, MetadataTokenDeletedReason,
};
use crate::types::options::resource_options_json;
use crate::{Permissions, ResourceOptions, UserStatus};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use std::str::FromStr;

/// Public options of a resource after a change, as the read routes return
/// them, or a marker for stored critical state this build cannot read. A
/// mirror keeps its previous values for an unreadable resource.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataOptions {
    Readable(ResourceOptions),
    Unreadable,
}

/// A JSON object kept verbatim: a record or item of a group or kind this
/// build does not know.
#[derive(Debug, Clone)]
pub struct MetadataRawJson(Box<RawValue>);

/// One change record: the committed operation that produced it, its position
/// within that operation, and what is true after it.
///
/// Its JSON form is one flat object with the envelope fields, `group`, `kind`
/// and the body fields.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataChangeRecord {
    pub op: u64,
    pub ordinal: u32,
    /// Prepare checksum of `op`.
    pub checksum: u128,
    /// Prepare timestamp of `op` in microseconds.
    pub timestamp: u64,
    /// The acting user, absent for server actions.
    pub actor_user_id: Option<u32>,
    pub change: MetadataChange,
}

/// The typed body of a change record.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataChange {
    StreamCreated(MetadataStreamChange),
    StreamUpdated(MetadataStreamChange),
    StreamDeleted(MetadataStreamDeleted),
    TopicCreated(MetadataTopicChange),
    TopicUpdated(MetadataTopicChange),
    TopicDeleted(MetadataTopicDeleted),
    PartitionsChanged(MetadataPartitionsChanged),
    ConsumerGroupCreated(MetadataConsumerGroupCreated),
    ConsumerGroupDeleted(MetadataConsumerGroupDeleted),
    ConsumerGroupMemberJoined(MetadataMemberJoined),
    ConsumerGroupMemberLeft(MetadataMemberLeft),
    ConsumerGroupPartitionHandedOver(MetadataPartitionHandedOver),
    UserCreated(MetadataUserChange),
    UserUpdated(MetadataUserChange),
    UserDeleted(MetadataUserIdentity),
    UserPasswordChanged(MetadataUserIdentity),
    UserPermissionsUpdated(MetadataUserPermissionsUpdated),
    PersonalAccessTokenCreated(MetadataTokenChange),
    PersonalAccessTokenDeleted(MetadataTokenDeleted),
    SessionRegistered(MetadataSession),
    SessionClosed(MetadataSessionClosed),
    /// A binary record of a group or kind this build does not know.
    Unknown {
        group: u8,
        kind: u8,
        body: Bytes,
    },
    /// A JSON record of a group or kind this build does not know, kept whole.
    UnknownJson {
        group: String,
        kind: String,
        body: MetadataRawJson,
    },
}

/// One current state item.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataCurrentItem {
    Stream(MetadataStreamChange),
    Topic(MetadataTopicItem),
    Partitions(MetadataPartitionsItem),
    ConsumerGroup(MetadataConsumerGroupItem),
    ConsumerGroupMember(MetadataMemberItem),
    User(MetadataUserChange),
    PersonalAccessToken(MetadataTokenChange),
    Session(MetadataSession),
    /// A binary item of a group this build does not know.
    Unknown {
        group: u8,
        body: Bytes,
    },
    /// A JSON item of a group this build does not know, kept whole.
    UnknownJson {
        group: String,
        body: MetadataRawJson,
    },
}

/// A stream after a create or update, and a `streams` item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "StreamChangeJson", try_from = "StreamChangeJson")]
pub struct MetadataStreamChange {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub created_at: u64,
    pub name: String,
    pub options: MetadataOptions,
}

/// A removed stream. `requested_op` is the op of a deferred delete's request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataStreamDeleted {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub created_at: u64,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_op: Option<u64>,
}

/// A topic after a create or update.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "TopicChangeJson", try_from = "TopicChangeJson")]
pub struct MetadataTopicChange {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub created_at: u64,
    pub name: String,
    pub partitions_count: u32,
    pub options: MetadataOptions,
}

/// A `topics` item. Partition counts travel as `partitions` items.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "TopicItemJson", try_from = "TopicItemJson")]
pub struct MetadataTopicItem {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub created_at: u64,
    pub name: String,
    pub options: MetadataOptions,
}

/// A removed topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataTopicDeleted {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub created_at: u64,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_op: Option<u64>,
}

/// A partition count change. `removed_partition_ids` lists the deleted ids,
/// ascending, and is empty on create.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPartitionsChanged {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub partitions_count: u32,
    pub removed_partition_ids: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_op: Option<u64>,
}

/// A `partitions` item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPartitionsItem {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub partitions_count: u32,
}

/// A created consumer group with the topic's resulting group count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataConsumerGroupCreated {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub group_id: u32,
    pub name: String,
    pub consumer_groups_count: u32,
}

/// A removed consumer group with the topic's resulting group count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataConsumerGroupDeleted {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub group_id: u32,
    pub name: String,
    pub consumer_groups_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_op: Option<u64>,
}

/// A `consumer_groups` item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataConsumerGroupItem {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub group_id: u32,
    pub name: String,
}

/// A member that joined, with its installed partitions (normally none, a
/// handover installs them).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataMemberJoined {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub group_id: u32,
    pub member_id: u32,
    #[serde(with = "hex_u128")]
    pub client_id: u128,
    pub members_count: u32,
    pub partition_ids: Vec<u32>,
}

/// A member that left the group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataMemberLeft {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub group_id: u32,
    pub member_id: u32,
    #[serde(with = "hex_u128")]
    pub client_id: u128,
    pub members_count: u32,
    pub reason: MetadataMemberLeftReason,
}

/// A completed partition handover. `member_id` is absent when the last
/// member left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPartitionHandedOver {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub group_id: u32,
    pub partition_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_id: Option<u32>,
}

/// A `consumer_group_members` item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataMemberItem {
    pub stream_id: u32,
    pub stream_created_op: u64,
    pub topic_id: u32,
    pub topic_created_op: u64,
    pub group_id: u32,
    pub member_id: u32,
    #[serde(with = "hex_u128")]
    pub client_id: u128,
    pub partition_ids: Vec<u32>,
}

/// A user after a create or update, and a `users` item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "UserChangeJson", try_from = "UserChangeJson")]
pub struct MetadataUserChange {
    pub user_id: u32,
    pub user_created_at: u64,
    pub username: String,
    pub status: UserStatus,
    pub permissions: Option<Permissions>,
    pub options: MetadataOptions,
}

/// A deleted user, or one whose password changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataUserIdentity {
    pub user_id: u32,
    pub user_created_at: u64,
    pub username: String,
}

/// A user's public permission set after a change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataUserPermissionsUpdated {
    pub user_id: u32,
    pub user_created_at: u64,
    pub username: String,
    pub permissions: Option<Permissions>,
    pub reason: MetadataPermissionsUpdatedReason,
}

/// A created personal access token, and a `personal_access_tokens` item.
/// `expiry` in microseconds is absent for a token that never expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataTokenChange {
    pub user_id: u32,
    pub user_created_at: u64,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<u64>,
}

/// A removed personal access token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataTokenDeleted {
    pub user_id: u32,
    pub user_created_at: u64,
    pub name: String,
    pub reason: MetadataTokenDeletedReason,
}

/// A registered session, and a `sessions` item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSession {
    pub user_id: u32,
    pub user_created_at: u64,
    #[serde(with = "hex_u128")]
    pub client_id: u128,
    pub session: u64,
}

/// A closed session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSessionClosed {
    pub user_id: u32,
    pub user_created_at: u64,
    #[serde(with = "hex_u128")]
    pub client_id: u128,
    pub session: u64,
    pub reason: MetadataSessionClosedReason,
}

impl MetadataRawJson {
    /// The verbatim JSON text.
    #[must_use]
    pub fn get(&self) -> &str {
        self.0.get()
    }
}

impl PartialEq for MetadataRawJson {
    fn eq(&self, other: &Self) -> bool {
        self.0.get() == other.0.get()
    }
}

impl From<Box<RawValue>> for MetadataRawJson {
    fn from(raw: Box<RawValue>) -> Self {
        Self(raw)
    }
}

impl MetadataChange {
    /// The kind, absent for a record this build does not know.
    #[must_use]
    pub const fn kind(&self) -> Option<MetadataChangeKind> {
        Some(match self {
            Self::StreamCreated(_) => MetadataChangeKind::StreamCreated,
            Self::StreamUpdated(_) => MetadataChangeKind::StreamUpdated,
            Self::StreamDeleted(_) => MetadataChangeKind::StreamDeleted,
            Self::TopicCreated(_) => MetadataChangeKind::TopicCreated,
            Self::TopicUpdated(_) => MetadataChangeKind::TopicUpdated,
            Self::TopicDeleted(_) => MetadataChangeKind::TopicDeleted,
            Self::PartitionsChanged(_) => MetadataChangeKind::PartitionsChanged,
            Self::ConsumerGroupCreated(_) => MetadataChangeKind::ConsumerGroupCreated,
            Self::ConsumerGroupDeleted(_) => MetadataChangeKind::ConsumerGroupDeleted,
            Self::ConsumerGroupMemberJoined(_) => MetadataChangeKind::ConsumerGroupMemberJoined,
            Self::ConsumerGroupMemberLeft(_) => MetadataChangeKind::ConsumerGroupMemberLeft,
            Self::ConsumerGroupPartitionHandedOver(_) => {
                MetadataChangeKind::ConsumerGroupPartitionHandedOver
            }
            Self::UserCreated(_) => MetadataChangeKind::UserCreated,
            Self::UserUpdated(_) => MetadataChangeKind::UserUpdated,
            Self::UserDeleted(_) => MetadataChangeKind::UserDeleted,
            Self::UserPasswordChanged(_) => MetadataChangeKind::UserPasswordChanged,
            Self::UserPermissionsUpdated(_) => MetadataChangeKind::UserPermissionsUpdated,
            Self::PersonalAccessTokenCreated(_) => MetadataChangeKind::PersonalAccessTokenCreated,
            Self::PersonalAccessTokenDeleted(_) => MetadataChangeKind::PersonalAccessTokenDeleted,
            Self::SessionRegistered(_) => MetadataChangeKind::SessionRegistered,
            Self::SessionClosed(_) => MetadataChangeKind::SessionClosed,
            Self::Unknown { .. } | Self::UnknownJson { .. } => return None,
        })
    }

    /// The group, absent for a record this build does not know.
    #[must_use]
    pub fn group(&self) -> Option<MetadataChangeGroup> {
        self.kind().map(MetadataChangeKind::group)
    }

    fn from_json(kind: MetadataChangeKind, text: &str) -> serde_json::Result<Self> {
        Ok(match kind {
            MetadataChangeKind::StreamCreated => Self::StreamCreated(serde_json::from_str(text)?),
            MetadataChangeKind::StreamUpdated => Self::StreamUpdated(serde_json::from_str(text)?),
            MetadataChangeKind::StreamDeleted => Self::StreamDeleted(serde_json::from_str(text)?),
            MetadataChangeKind::TopicCreated => Self::TopicCreated(serde_json::from_str(text)?),
            MetadataChangeKind::TopicUpdated => Self::TopicUpdated(serde_json::from_str(text)?),
            MetadataChangeKind::TopicDeleted => Self::TopicDeleted(serde_json::from_str(text)?),
            MetadataChangeKind::PartitionsChanged => {
                Self::PartitionsChanged(serde_json::from_str(text)?)
            }
            MetadataChangeKind::ConsumerGroupCreated => {
                Self::ConsumerGroupCreated(serde_json::from_str(text)?)
            }
            MetadataChangeKind::ConsumerGroupDeleted => {
                Self::ConsumerGroupDeleted(serde_json::from_str(text)?)
            }
            MetadataChangeKind::ConsumerGroupMemberJoined => {
                Self::ConsumerGroupMemberJoined(serde_json::from_str(text)?)
            }
            MetadataChangeKind::ConsumerGroupMemberLeft => {
                Self::ConsumerGroupMemberLeft(serde_json::from_str(text)?)
            }
            MetadataChangeKind::ConsumerGroupPartitionHandedOver => {
                Self::ConsumerGroupPartitionHandedOver(serde_json::from_str(text)?)
            }
            MetadataChangeKind::UserCreated => Self::UserCreated(serde_json::from_str(text)?),
            MetadataChangeKind::UserUpdated => Self::UserUpdated(serde_json::from_str(text)?),
            MetadataChangeKind::UserDeleted => Self::UserDeleted(serde_json::from_str(text)?),
            MetadataChangeKind::UserPasswordChanged => {
                Self::UserPasswordChanged(serde_json::from_str(text)?)
            }
            MetadataChangeKind::UserPermissionsUpdated => {
                Self::UserPermissionsUpdated(serde_json::from_str(text)?)
            }
            MetadataChangeKind::PersonalAccessTokenCreated => {
                Self::PersonalAccessTokenCreated(serde_json::from_str(text)?)
            }
            MetadataChangeKind::PersonalAccessTokenDeleted => {
                Self::PersonalAccessTokenDeleted(serde_json::from_str(text)?)
            }
            MetadataChangeKind::SessionRegistered => {
                Self::SessionRegistered(serde_json::from_str(text)?)
            }
            MetadataChangeKind::SessionClosed => Self::SessionClosed(serde_json::from_str(text)?),
        })
    }
}

impl MetadataCurrentItem {
    /// The group, absent for an item this build does not know.
    #[must_use]
    pub const fn group(&self) -> Option<MetadataChangeGroup> {
        Some(match self {
            Self::Stream(_) => MetadataChangeGroup::Streams,
            Self::Topic(_) => MetadataChangeGroup::Topics,
            Self::Partitions(_) => MetadataChangeGroup::Partitions,
            Self::ConsumerGroup(_) => MetadataChangeGroup::ConsumerGroups,
            Self::ConsumerGroupMember(_) => MetadataChangeGroup::ConsumerGroupMembers,
            Self::User(_) => MetadataChangeGroup::Users,
            Self::PersonalAccessToken(_) => MetadataChangeGroup::PersonalAccessTokens,
            Self::Session(_) => MetadataChangeGroup::Sessions,
            Self::Unknown { .. } | Self::UnknownJson { .. } => return None,
        })
    }

    fn from_json(group: MetadataChangeGroup, text: &str) -> serde_json::Result<Self> {
        Ok(match group {
            MetadataChangeGroup::Streams => Self::Stream(serde_json::from_str(text)?),
            MetadataChangeGroup::Topics => Self::Topic(serde_json::from_str(text)?),
            MetadataChangeGroup::Partitions => Self::Partitions(serde_json::from_str(text)?),
            MetadataChangeGroup::ConsumerGroups => Self::ConsumerGroup(serde_json::from_str(text)?),
            MetadataChangeGroup::ConsumerGroupMembers => {
                Self::ConsumerGroupMember(serde_json::from_str(text)?)
            }
            MetadataChangeGroup::Users => Self::User(serde_json::from_str(text)?),
            MetadataChangeGroup::PersonalAccessTokens => {
                Self::PersonalAccessToken(serde_json::from_str(text)?)
            }
            MetadataChangeGroup::Sessions => Self::Session(serde_json::from_str(text)?),
        })
    }
}

/// The envelope fields of a record object, with the body fields left out.
#[derive(Deserialize)]
struct RecordEnvelopeJson {
    op: u64,
    ordinal: u32,
    #[serde(with = "hex_u128")]
    checksum: u128,
    timestamp: u64,
    #[serde(default)]
    actor_user_id: Option<u32>,
    group: String,
    kind: String,
}

#[derive(Serialize)]
struct RecordJson<'a, B: Serialize> {
    op: u64,
    ordinal: u32,
    #[serde(with = "hex_u128")]
    checksum: u128,
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor_user_id: Option<u32>,
    group: &'a str,
    kind: &'a str,
    #[serde(flatten)]
    body: &'a B,
}

/// A binary record of an unknown kind rendered as JSON: its codes as decimal
/// text and its body in base64, so a JSON reader keeps it as unknown too.
#[derive(Serialize)]
struct UnknownRecordJson {
    op: u64,
    ordinal: u32,
    #[serde(with = "hex_u128")]
    checksum: u128,
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor_user_id: Option<u32>,
    group: String,
    kind: String,
    body: String,
}

#[derive(Deserialize)]
struct ItemEnvelopeJson {
    group: String,
}

#[derive(Serialize)]
struct ItemJson<'a, B: Serialize> {
    group: &'a str,
    #[serde(flatten)]
    body: &'a B,
}

#[derive(Serialize)]
struct UnknownItemJson {
    group: String,
    body: String,
}

impl MetadataChangeRecord {
    fn envelope<'a, B: Serialize>(
        &self,
        group: &'a str,
        kind: &'a str,
        body: &'a B,
    ) -> RecordJson<'a, B> {
        RecordJson {
            op: self.op,
            ordinal: self.ordinal,
            checksum: self.checksum,
            timestamp: self.timestamp,
            actor_user_id: self.actor_user_id,
            group,
            kind,
            body,
        }
    }

    fn serialize_body<S: Serializer, B: Serialize>(
        &self,
        serializer: S,
        kind: MetadataChangeKind,
        body: &B,
    ) -> Result<S::Ok, S::Error> {
        self.envelope(kind.group().into(), kind.into(), body)
            .serialize(serializer)
    }
}

impl Serialize for MetadataChangeRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let kind = self.change.kind();
        match (&self.change, kind) {
            (MetadataChange::UnknownJson { body, .. }, _) => body.0.serialize(serializer),
            (MetadataChange::Unknown { group, kind, body }, _) => UnknownRecordJson {
                op: self.op,
                ordinal: self.ordinal,
                checksum: self.checksum,
                timestamp: self.timestamp,
                actor_user_id: self.actor_user_id,
                group: group.to_string(),
                kind: kind.to_string(),
                body: STANDARD.encode(body),
            }
            .serialize(serializer),
            (
                MetadataChange::StreamCreated(body) | MetadataChange::StreamUpdated(body),
                Some(kind),
            ) => self.serialize_body(serializer, kind, body),
            (MetadataChange::StreamDeleted(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (
                MetadataChange::TopicCreated(body) | MetadataChange::TopicUpdated(body),
                Some(kind),
            ) => self.serialize_body(serializer, kind, body),
            (MetadataChange::TopicDeleted(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::PartitionsChanged(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::ConsumerGroupCreated(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::ConsumerGroupDeleted(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::ConsumerGroupMemberJoined(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::ConsumerGroupMemberLeft(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::ConsumerGroupPartitionHandedOver(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::UserCreated(body) | MetadataChange::UserUpdated(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (
                MetadataChange::UserDeleted(body) | MetadataChange::UserPasswordChanged(body),
                Some(kind),
            ) => self.serialize_body(serializer, kind, body),
            (MetadataChange::UserPermissionsUpdated(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::PersonalAccessTokenCreated(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::PersonalAccessTokenDeleted(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::SessionRegistered(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (MetadataChange::SessionClosed(body), Some(kind)) => {
                self.serialize_body(serializer, kind, body)
            }
            (_, None) => Err(serde::ser::Error::custom("a known change has a kind")),
        }
    }
}

impl<'de> Deserialize<'de> for MetadataChangeRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let envelope: RecordEnvelopeJson =
            serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
        let group = MetadataChangeGroup::from_str(&envelope.group);
        let kind = MetadataChangeKind::from_str(&envelope.kind);
        let change = match (group, kind) {
            (Ok(group), Ok(kind)) => {
                if kind.group() != group {
                    return Err(D::Error::custom(format!(
                        "change kind {kind} does not belong to group {group}"
                    )));
                }
                MetadataChange::from_json(kind, raw.get()).map_err(D::Error::custom)?
            }
            _ => MetadataChange::UnknownJson {
                group: envelope.group,
                kind: envelope.kind,
                body: MetadataRawJson(raw),
            },
        };
        Ok(Self {
            op: envelope.op,
            ordinal: envelope.ordinal,
            checksum: envelope.checksum,
            timestamp: envelope.timestamp,
            actor_user_id: envelope.actor_user_id,
            change,
        })
    }
}

impl Serialize for MetadataCurrentItem {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fn item<S: Serializer, B: Serialize>(
            serializer: S,
            group: MetadataChangeGroup,
            body: &B,
        ) -> Result<S::Ok, S::Error> {
            ItemJson {
                group: group.into(),
                body,
            }
            .serialize(serializer)
        }
        match self {
            Self::Stream(body) => item(serializer, MetadataChangeGroup::Streams, body),
            Self::Topic(body) => item(serializer, MetadataChangeGroup::Topics, body),
            Self::Partitions(body) => item(serializer, MetadataChangeGroup::Partitions, body),
            Self::ConsumerGroup(body) => {
                item(serializer, MetadataChangeGroup::ConsumerGroups, body)
            }
            Self::ConsumerGroupMember(body) => {
                item(serializer, MetadataChangeGroup::ConsumerGroupMembers, body)
            }
            Self::User(body) => item(serializer, MetadataChangeGroup::Users, body),
            Self::PersonalAccessToken(body) => {
                item(serializer, MetadataChangeGroup::PersonalAccessTokens, body)
            }
            Self::Session(body) => item(serializer, MetadataChangeGroup::Sessions, body),
            Self::Unknown { group, body } => UnknownItemJson {
                group: group.to_string(),
                body: STANDARD.encode(body),
            }
            .serialize(serializer),
            Self::UnknownJson { body, .. } => body.0.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for MetadataCurrentItem {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let envelope: ItemEnvelopeJson =
            serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
        match MetadataChangeGroup::from_str(&envelope.group) {
            Ok(group) => Self::from_json(group, raw.get()).map_err(D::Error::custom),
            Err(_) => Ok(Self::UnknownJson {
                group: envelope.group,
                body: MetadataRawJson(raw),
            }),
        }
    }
}

/// The readable options map of a body's JSON form.
#[derive(Clone, Serialize, Deserialize)]
struct OptionsJson(#[serde(with = "resource_options_json")] ResourceOptions);

fn options_to_json(options: MetadataOptions) -> (Option<OptionsJson>, bool) {
    match options {
        MetadataOptions::Readable(options) => (Some(OptionsJson(options)), false),
        MetadataOptions::Unreadable => (None, true),
    }
}

fn options_from_json(
    options: Option<OptionsJson>,
    unreadable: bool,
) -> Result<MetadataOptions, String> {
    match (options, unreadable) {
        (Some(OptionsJson(options)), false) => Ok(MetadataOptions::Readable(options)),
        (None, true) => Ok(MetadataOptions::Unreadable),
        _ => Err("options must be an object, or null with options_unreadable true".to_owned()),
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StreamChangeJson {
    stream_id: u32,
    stream_created_op: u64,
    created_at: u64,
    name: String,
    options: Option<OptionsJson>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    options_unreadable: bool,
}

impl From<MetadataStreamChange> for StreamChangeJson {
    fn from(body: MetadataStreamChange) -> Self {
        let (options, options_unreadable) = options_to_json(body.options);
        Self {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            created_at: body.created_at,
            name: body.name,
            options,
            options_unreadable,
        }
    }
}

impl TryFrom<StreamChangeJson> for MetadataStreamChange {
    type Error = String;

    fn try_from(json: StreamChangeJson) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_id: json.stream_id,
            stream_created_op: json.stream_created_op,
            created_at: json.created_at,
            name: json.name,
            options: options_from_json(json.options, json.options_unreadable)?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct TopicChangeJson {
    stream_id: u32,
    stream_created_op: u64,
    topic_id: u32,
    topic_created_op: u64,
    created_at: u64,
    name: String,
    partitions_count: u32,
    options: Option<OptionsJson>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    options_unreadable: bool,
}

impl From<MetadataTopicChange> for TopicChangeJson {
    fn from(body: MetadataTopicChange) -> Self {
        let (options, options_unreadable) = options_to_json(body.options);
        Self {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            created_at: body.created_at,
            name: body.name,
            partitions_count: body.partitions_count,
            options,
            options_unreadable,
        }
    }
}

impl TryFrom<TopicChangeJson> for MetadataTopicChange {
    type Error = String;

    fn try_from(json: TopicChangeJson) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_id: json.stream_id,
            stream_created_op: json.stream_created_op,
            topic_id: json.topic_id,
            topic_created_op: json.topic_created_op,
            created_at: json.created_at,
            name: json.name,
            partitions_count: json.partitions_count,
            options: options_from_json(json.options, json.options_unreadable)?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct TopicItemJson {
    stream_id: u32,
    stream_created_op: u64,
    topic_id: u32,
    topic_created_op: u64,
    created_at: u64,
    name: String,
    options: Option<OptionsJson>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    options_unreadable: bool,
}

impl From<MetadataTopicItem> for TopicItemJson {
    fn from(body: MetadataTopicItem) -> Self {
        let (options, options_unreadable) = options_to_json(body.options);
        Self {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            created_at: body.created_at,
            name: body.name,
            options,
            options_unreadable,
        }
    }
}

impl TryFrom<TopicItemJson> for MetadataTopicItem {
    type Error = String;

    fn try_from(json: TopicItemJson) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_id: json.stream_id,
            stream_created_op: json.stream_created_op,
            topic_id: json.topic_id,
            topic_created_op: json.topic_created_op,
            created_at: json.created_at,
            name: json.name,
            options: options_from_json(json.options, json.options_unreadable)?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct UserChangeJson {
    user_id: u32,
    user_created_at: u64,
    username: String,
    status: UserStatus,
    permissions: Option<Permissions>,
    options: Option<OptionsJson>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    options_unreadable: bool,
}

impl From<MetadataUserChange> for UserChangeJson {
    fn from(body: MetadataUserChange) -> Self {
        let (options, options_unreadable) = options_to_json(body.options);
        Self {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            username: body.username,
            status: body.status,
            permissions: body.permissions,
            options,
            options_unreadable,
        }
    }
}

impl TryFrom<UserChangeJson> for MetadataUserChange {
    type Error = String;

    fn try_from(json: UserChangeJson) -> Result<Self, Self::Error> {
        Ok(Self {
            user_id: json.user_id,
            user_created_at: json.user_created_at,
            username: json.username,
            status: json.status,
            permissions: json.permissions,
            options: options_from_json(json.options, json.options_unreadable)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GlobalPermissions, HeaderKey, HeaderValue, OptionValue, StreamPermissions};
    use std::collections::BTreeMap;

    fn stream_change() -> MetadataStreamChange {
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from_str("encryption_key_source").unwrap(),
            OptionValue::derived(HeaderValue::try_from("generated").unwrap()),
        );
        MetadataStreamChange {
            stream_id: 3,
            stream_created_op: 4121,
            created_at: 1_760_356_800_123_456,
            name: "telemetry".to_owned(),
            options: MetadataOptions::Readable(options),
        }
    }

    fn permissions() -> Permissions {
        Permissions {
            global: GlobalPermissions::default(),
            streams: Some(BTreeMap::from([(
                7,
                StreamPermissions {
                    read_stream: true,
                    ..StreamPermissions::default()
                },
            )])),
        }
    }

    fn record(change: MetadataChange, actor_user_id: Option<u32>) -> MetadataChangeRecord {
        MetadataChangeRecord {
            op: 4121,
            ordinal: 0,
            checksum: 0xa5c1_f703_8b9e_2d4c,
            timestamp: 1_760_356_800_123_456,
            actor_user_id,
            change,
        }
    }

    #[test]
    fn given_stream_created_record_when_serializing_should_render_one_flat_object() {
        let json = serde_json::to_string(&record(
            MetadataChange::StreamCreated(stream_change()),
            Some(4),
        ))
        .unwrap();
        assert_eq!(
            json,
            concat!(
                r#"{"op":4121,"ordinal":0,"checksum":"0000000000000000a5c1f7038b9e2d4c","#,
                r#""timestamp":1760356800123456,"actor_user_id":4,"group":"streams","#,
                r#""kind":"stream_created","stream_id":3,"stream_created_op":4121,"#,
                r#""created_at":1760356800123456,"name":"telemetry","options":"#,
                r#"{"encryption_key_source":{"value":"generated","kind":"string","explicit":false}}}"#
            )
        );
    }

    #[test]
    fn given_server_action_when_serializing_should_omit_the_actor() {
        let json = serde_json::to_string(&record(
            MetadataChange::StreamUpdated(stream_change()),
            None,
        ))
        .unwrap();
        assert!(!json.contains("actor_user_id"));
        let decoded: MetadataChangeRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.actor_user_id, None);
    }

    #[test]
    fn given_every_body_shape_when_round_tripping_json_should_preserve_it() {
        let changes = vec![
            MetadataChange::StreamCreated(stream_change()),
            MetadataChange::StreamUpdated(MetadataStreamChange {
                options: MetadataOptions::Unreadable,
                ..stream_change()
            }),
            MetadataChange::StreamDeleted(MetadataStreamDeleted {
                stream_id: 3,
                stream_created_op: 4121,
                created_at: 1,
                name: "telemetry".to_owned(),
                requested_op: Some(4140),
            }),
            MetadataChange::PartitionsChanged(MetadataPartitionsChanged {
                stream_id: 3,
                stream_created_op: 4121,
                topic_id: 0,
                topic_created_op: 4125,
                partitions_count: 4,
                removed_partition_ids: vec![4, 5],
                requested_op: None,
            }),
            MetadataChange::ConsumerGroupMemberLeft(MetadataMemberLeft {
                stream_id: 3,
                stream_created_op: 4121,
                topic_id: 0,
                topic_created_op: 4125,
                group_id: 1,
                member_id: 2,
                client_id: u128::MAX,
                members_count: 0,
                reason: MetadataMemberLeftReason::SessionEnded,
            }),
            MetadataChange::UserCreated(MetadataUserChange {
                user_id: 7,
                user_created_at: 1_760_300_000_000_000,
                username: "writer".to_owned(),
                status: UserStatus::Inactive,
                permissions: Some(permissions()),
                options: MetadataOptions::Readable(ResourceOptions::new()),
            }),
            MetadataChange::UserPermissionsUpdated(MetadataUserPermissionsUpdated {
                user_id: 7,
                user_created_at: 1_760_300_000_000_000,
                username: "writer".to_owned(),
                permissions: None,
                reason: MetadataPermissionsUpdatedReason::ResourceDeleted,
            }),
            MetadataChange::PersonalAccessTokenCreated(MetadataTokenChange {
                user_id: 7,
                user_created_at: 1,
                name: "ci".to_owned(),
                expiry: None,
            }),
            MetadataChange::SessionClosed(MetadataSessionClosed {
                user_id: 7,
                user_created_at: 1,
                client_id: 1 << 100,
                session: 9,
                reason: MetadataSessionClosedReason::Expired,
            }),
        ];
        for change in changes {
            let original = record(change, Some(1));
            let json = serde_json::to_string(&original).unwrap();
            let decoded: MetadataChangeRecord = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, original, "{json}");
        }
    }

    #[test]
    fn given_unknown_kind_or_group_when_deserializing_should_keep_the_raw_object() {
        let text = concat!(
            r#"{"op":5,"ordinal":1,"checksum":"00000000000000000000000000000007","#,
            r#""timestamp":9,"group":"streams","kind":"stream_archived","stream_id":3,"extra":[1,2]}"#
        );
        let decoded: MetadataChangeRecord = serde_json::from_str(text).unwrap();
        let MetadataChange::UnknownJson { group, kind, body } = &decoded.change else {
            panic!("expected an unknown record");
        };
        assert_eq!(
            (group.as_str(), kind.as_str()),
            ("streams", "stream_archived")
        );
        assert_eq!(body.get(), text);
        assert_eq!(serde_json::to_string(&decoded).unwrap(), text);
        let future_group = text.replace(r#""group":"streams""#, r#""group":"audit""#);
        let decoded: MetadataChangeRecord = serde_json::from_str(&future_group).unwrap();
        assert!(decoded.change.kind().is_none());
    }

    #[test]
    fn given_known_kind_under_wrong_group_when_deserializing_should_refuse() {
        let json = serde_json::to_string(&record(
            MetadataChange::StreamCreated(stream_change()),
            None,
        ))
        .unwrap()
        .replace(r#""group":"streams""#, r#""group":"topics""#);
        assert!(serde_json::from_str::<MetadataChangeRecord>(&json).is_err());
    }

    #[test]
    fn given_options_and_unreadable_marker_both_set_when_deserializing_should_refuse() {
        let json = serde_json::to_string(&MetadataStreamChange {
            options: MetadataOptions::Unreadable,
            ..stream_change()
        })
        .unwrap();
        assert!(json.contains(r#""options":null,"options_unreadable":true"#));
        let both = json.replace(r#""options":null"#, r#""options":{}"#);
        assert!(serde_json::from_str::<MetadataStreamChange>(&both).is_err());
        let neither = json.replace(r#","options_unreadable":true"#, "");
        assert!(serde_json::from_str::<MetadataStreamChange>(&neither).is_err());
    }

    #[test]
    fn given_every_item_group_when_round_tripping_json_should_preserve_it() {
        let items = vec![
            MetadataCurrentItem::Stream(stream_change()),
            MetadataCurrentItem::Partitions(MetadataPartitionsItem {
                stream_id: 1,
                stream_created_op: 2,
                topic_id: 3,
                topic_created_op: 4,
                partitions_count: 5,
            }),
            MetadataCurrentItem::ConsumerGroupMember(MetadataMemberItem {
                stream_id: 1,
                stream_created_op: 2,
                topic_id: 3,
                topic_created_op: 4,
                group_id: 5,
                member_id: 6,
                client_id: 7,
                partition_ids: vec![0, 1],
            }),
            MetadataCurrentItem::Session(MetadataSession {
                user_id: 1,
                user_created_at: 2,
                client_id: 3,
                session: 4,
            }),
        ];
        for item in items {
            let json = serde_json::to_string(&item).unwrap();
            assert!(json.starts_with(r#"{"group":""#), "{json}");
            assert_eq!(
                serde_json::from_str::<MetadataCurrentItem>(&json).unwrap(),
                item
            );
        }
        let future: MetadataCurrentItem =
            serde_json::from_str(r#"{"group":"audit","id":1}"#).unwrap();
        assert_eq!(future.group(), None);
    }
}
