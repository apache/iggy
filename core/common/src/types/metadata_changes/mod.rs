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

pub(crate) mod answer;
pub(crate) mod json;
pub(crate) mod position;
pub(crate) mod query;
pub(crate) mod records;
pub(crate) mod wire;

pub use answer::MetadataChanges;
pub use position::{
    METADATA_CHANGE_CURSOR_PREFIX, METADATA_CURRENT_PAGE_TOKEN_PREFIX, MetadataChangeCursor,
    MetadataCurrentPageToken,
};
pub use query::{
    DEFAULT_METADATA_CHANGES_LIMIT, MAX_METADATA_CHANGES_LIMIT, MetadataChangeStart,
    MetadataChangesQuery,
};
pub use records::{
    MetadataChange, MetadataChangeRecord, MetadataConsumerGroupCreated,
    MetadataConsumerGroupDeleted, MetadataConsumerGroupItem, MetadataCurrentItem,
    MetadataMemberItem, MetadataMemberJoined, MetadataMemberLeft, MetadataOptions,
    MetadataPartitionHandedOver, MetadataPartitionsChanged, MetadataPartitionsItem,
    MetadataRawJson, MetadataSession, MetadataSessionClosed, MetadataStreamChange,
    MetadataStreamDeleted, MetadataTokenChange, MetadataTokenDeleted, MetadataTopicChange,
    MetadataTopicDeleted, MetadataTopicItem, MetadataUserChange, MetadataUserIdentity,
    MetadataUserPermissionsUpdated,
};

use iggy_binary_protocol::primitives::metadata_changes::{
    WireMemberLeftReason, WireMetadataChangeGroup, WireMetadataChangeKind, WireMetadataChangePhase,
    WireMetadataChangeStatus, WirePermissionsUpdatedReason, WireSessionClosedReason,
    WireTokenDeletedReason,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator, IntoStaticStr};

/// Family of change records and the unit of filtering. An empty selection
/// means every group.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataChangeGroup {
    Streams,
    Topics,
    Partitions,
    ConsumerGroups,
    ConsumerGroupMembers,
    Users,
    PersonalAccessTokens,
    Sessions,
}

/// Bounded sequence a group's records are retained in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataChangeClass {
    /// Streams, topics, partitions, consumer groups, users and tokens.
    Resource,
    /// Consumer group membership and sessions.
    Activity,
}

/// Kind of a change record.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataChangeKind {
    StreamCreated,
    StreamUpdated,
    StreamDeleted,
    TopicCreated,
    TopicUpdated,
    TopicDeleted,
    PartitionsChanged,
    ConsumerGroupCreated,
    ConsumerGroupDeleted,
    ConsumerGroupMemberJoined,
    ConsumerGroupMemberLeft,
    ConsumerGroupPartitionHandedOver,
    UserCreated,
    UserUpdated,
    UserDeleted,
    UserPasswordChanged,
    UserPermissionsUpdated,
    PersonalAccessTokenCreated,
    PersonalAccessTokenDeleted,
    SessionRegistered,
    SessionClosed,
}

/// Outcome of a change log query.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataChangeStatus {
    /// Records or items after the requested position.
    Ok,
    /// The answering node has not applied the requested position yet.
    Behind,
    /// The node no longer holds every record after the position.
    Expired,
    /// The position belongs to another history.
    Reset,
}

/// Whether an answer pages change records or current state items.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataChangePhase {
    Changes,
    Current,
}

/// Why a consumer group member was removed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataMemberLeftReason {
    Left,
    SessionEnded,
}

/// Why a personal access token was removed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataTokenDeletedReason {
    Deleted,
    Expired,
}

/// Why a session was closed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataSessionClosedReason {
    Logout,
    Expired,
    /// The user was deactivated or deleted.
    Revoked,
}

/// Why a user's public permission set changed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetadataPermissionsUpdatedReason {
    /// An `UpdatePermissions` replaced the set.
    Updated,
    /// A stream or topic delete pruned the user's grants on it.
    ResourceDeleted,
}

/// A set of groups, one bit per group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MetadataChangeGroups(u8);

impl MetadataChangeGroup {
    /// Retention class of this group's records.
    #[must_use]
    pub const fn class(self) -> MetadataChangeClass {
        match self {
            Self::ConsumerGroupMembers | Self::Sessions => MetadataChangeClass::Activity,
            Self::Streams
            | Self::Topics
            | Self::Partitions
            | Self::ConsumerGroups
            | Self::Users
            | Self::PersonalAccessTokens => MetadataChangeClass::Resource,
        }
    }

    /// Groups a current state scan of this group must include, so a mirror
    /// can apply the cascades of their deletes.
    #[must_use]
    pub const fn parents(self) -> &'static [Self] {
        match self {
            Self::Streams | Self::Users => &[],
            Self::Topics => &[Self::Streams],
            Self::Partitions | Self::ConsumerGroups => &[Self::Topics, Self::Streams],
            Self::ConsumerGroupMembers => &[
                Self::ConsumerGroups,
                Self::Partitions,
                Self::Topics,
                Self::Streams,
            ],
            Self::PersonalAccessTokens | Self::Sessions => &[Self::Users],
        }
    }

    /// Bit `code - 1` of a group mask. Variants follow the code order.
    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

impl MetadataChangeKind {
    /// The group every record of this kind belongs to.
    #[must_use]
    pub fn group(self) -> MetadataChangeGroup {
        WireMetadataChangeKind::from(self).group().into()
    }
}

impl MetadataChangeGroups {
    /// Every group.
    #[must_use]
    pub const fn all() -> Self {
        Self(u8::MAX)
    }

    /// The requested groups, with duplicates collapsed and an empty request
    /// meaning every group.
    #[must_use]
    pub fn requested(groups: &[MetadataChangeGroup]) -> Self {
        if groups.is_empty() {
            return Self::all();
        }
        Self(groups.iter().fold(0, |mask, group| mask | group.bit()))
    }

    /// The set a page token's group mask names.
    #[must_use]
    pub const fn from_mask(mask: u8) -> Self {
        Self(mask)
    }

    #[must_use]
    pub const fn mask(self) -> u8 {
        self.0
    }

    #[must_use]
    pub const fn contains(self, group: MetadataChangeGroup) -> bool {
        self.0 & group.bit() != 0
    }

    #[must_use]
    pub const fn contains_class(self, class: MetadataChangeClass) -> bool {
        let activity =
            MetadataChangeGroup::ConsumerGroupMembers.bit() | MetadataChangeGroup::Sessions.bit();
        match class {
            MetadataChangeClass::Activity => self.0 & activity != 0,
            MetadataChangeClass::Resource => self.0 & !activity != 0,
        }
    }

    /// The groups in code order.
    pub fn iter(self) -> impl Iterator<Item = MetadataChangeGroup> {
        MetadataChangeGroup::iter().filter(move |group| self.contains(*group))
    }

    /// The first selected group whose parent is missing, with that parent.
    #[must_use]
    pub fn missing_parent(self) -> Option<(MetadataChangeGroup, MetadataChangeGroup)> {
        self.iter().find_map(|group| {
            group
                .parents()
                .iter()
                .find(|parent| !self.contains(**parent))
                .map(|parent| (group, *parent))
        })
    }
}

macro_rules! mirror_wire_enum {
    ($domain:ident, $wire:ident, $($variant:ident),+) => {
        impl From<$domain> for $wire {
            fn from(value: $domain) -> Self {
                match value {
                    $($domain::$variant => Self::$variant,)+
                }
            }
        }

        impl From<$wire> for $domain {
            fn from(value: $wire) -> Self {
                match value {
                    $($wire::$variant => Self::$variant,)+
                }
            }
        }
    };
}

mirror_wire_enum!(
    MetadataChangeGroup,
    WireMetadataChangeGroup,
    Streams,
    Topics,
    Partitions,
    ConsumerGroups,
    ConsumerGroupMembers,
    Users,
    PersonalAccessTokens,
    Sessions
);
mirror_wire_enum!(
    MetadataChangeKind,
    WireMetadataChangeKind,
    StreamCreated,
    StreamUpdated,
    StreamDeleted,
    TopicCreated,
    TopicUpdated,
    TopicDeleted,
    PartitionsChanged,
    ConsumerGroupCreated,
    ConsumerGroupDeleted,
    ConsumerGroupMemberJoined,
    ConsumerGroupMemberLeft,
    ConsumerGroupPartitionHandedOver,
    UserCreated,
    UserUpdated,
    UserDeleted,
    UserPasswordChanged,
    UserPermissionsUpdated,
    PersonalAccessTokenCreated,
    PersonalAccessTokenDeleted,
    SessionRegistered,
    SessionClosed
);
mirror_wire_enum!(
    MetadataChangeStatus,
    WireMetadataChangeStatus,
    Ok,
    Behind,
    Expired,
    Reset
);
mirror_wire_enum!(
    MetadataChangePhase,
    WireMetadataChangePhase,
    Changes,
    Current
);
mirror_wire_enum!(
    MetadataMemberLeftReason,
    WireMemberLeftReason,
    Left,
    SessionEnded
);
mirror_wire_enum!(
    MetadataTokenDeletedReason,
    WireTokenDeletedReason,
    Deleted,
    Expired
);
mirror_wire_enum!(
    MetadataSessionClosedReason,
    WireSessionClosedReason,
    Logout,
    Expired,
    Revoked
);
mirror_wire_enum!(
    MetadataPermissionsUpdatedReason,
    WirePermissionsUpdatedReason,
    Updated,
    ResourceDeleted
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn given_every_group_when_mapping_to_wire_should_keep_the_catalog_code_order() {
        let codes: Vec<u8> = MetadataChangeGroup::iter()
            .map(|group| WireMetadataChangeGroup::from(group).as_u8())
            .collect();
        assert_eq!(codes, (1..=8).collect::<Vec<u8>>());
        for group in MetadataChangeGroup::iter() {
            assert_eq!(
                MetadataChangeGroup::from(WireMetadataChangeGroup::from(group)),
                group
            );
        }
    }

    #[test]
    fn given_group_names_when_parsing_should_use_snake_case() {
        assert_eq!(
            MetadataChangeGroup::from_str("consumer_group_members").unwrap(),
            MetadataChangeGroup::ConsumerGroupMembers
        );
        assert_eq!(
            MetadataChangeGroup::PersonalAccessTokens.to_string(),
            "personal_access_tokens"
        );
        assert!(MetadataChangeGroup::from_str("ConsumerGroups").is_err());
    }

    #[test]
    fn given_every_group_when_taking_its_bit_should_match_the_wire_mask_bit() {
        for group in MetadataChangeGroup::iter() {
            assert_eq!(group.bit(), WireMetadataChangeGroup::from(group).mask_bit());
        }
    }

    #[test]
    fn given_empty_or_duplicate_selection_when_building_group_set_should_normalize_it() {
        assert_eq!(
            MetadataChangeGroups::requested(&[]),
            MetadataChangeGroups::all()
        );
        let set = MetadataChangeGroups::requested(&[
            MetadataChangeGroup::Topics,
            MetadataChangeGroup::Topics,
            MetadataChangeGroup::Streams,
        ]);
        assert_eq!(set.mask(), 0b11);
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            vec![MetadataChangeGroup::Streams, MetadataChangeGroup::Topics]
        );
    }

    #[test]
    fn given_group_without_its_parents_when_checking_closure_should_name_the_missing_parent() {
        let members = MetadataChangeGroups::requested(&[
            MetadataChangeGroup::ConsumerGroupMembers,
            MetadataChangeGroup::ConsumerGroups,
            MetadataChangeGroup::Topics,
            MetadataChangeGroup::Streams,
        ]);
        assert_eq!(
            members.missing_parent(),
            Some((
                MetadataChangeGroup::ConsumerGroupMembers,
                MetadataChangeGroup::Partitions
            ))
        );
        assert_eq!(MetadataChangeGroups::all().missing_parent(), None);
        assert_eq!(
            MetadataChangeGroups::requested(&[
                MetadataChangeGroup::Sessions,
                MetadataChangeGroup::Users
            ])
            .missing_parent(),
            None
        );
    }

    #[test]
    fn given_group_sets_when_checking_classes_should_split_activity_from_resources() {
        let activity = MetadataChangeGroups::requested(&[MetadataChangeGroup::Sessions]);
        assert!(activity.contains_class(MetadataChangeClass::Activity));
        assert!(!activity.contains_class(MetadataChangeClass::Resource));
        let resource = MetadataChangeGroups::requested(&[MetadataChangeGroup::Users]);
        assert!(resource.contains_class(MetadataChangeClass::Resource));
        assert!(!resource.contains_class(MetadataChangeClass::Activity));
    }

    #[test]
    fn given_every_kind_when_mapping_its_group_should_match_the_wire_catalog() {
        assert_eq!(
            MetadataChangeKind::ConsumerGroupPartitionHandedOver.group(),
            MetadataChangeGroup::ConsumerGroupMembers
        );
        assert_eq!(
            MetadataChangeKind::SessionClosed.group(),
            MetadataChangeGroup::Sessions
        );
        assert_eq!(
            MetadataChangeKind::from_str("user_permissions_updated").unwrap(),
            MetadataChangeKind::UserPermissionsUpdated
        );
    }
}
