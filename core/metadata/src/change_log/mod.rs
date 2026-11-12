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

mod current;
mod query;
mod snapshot;
mod store;

pub use current::{CurrentState, current_page};
pub use query::{MAX_SCANNED_ENTRIES, change_page};

pub use snapshot::{ChangeEntrySnapshot, ChangeLogSnapshot, RingSnapshot};
pub use store::{
    ChangeEntry, ChangeLog, ChangeLogCapacities, ChangeLogCommand, ChangeLogError,
    DEFAULT_ACTIVITY_CAPACITY, DEFAULT_RESOURCE_CAPACITY, MAX_CAPACITY, MetadataChangeLog, Ring,
};

use crate::stm::ApplyContext;
use crate::stm::stream::{Stream, StreamsInner, Topic};
use crate::stm::user::User;
use bytes::Bytes;
use consensus::EXPIRED_SESSION_REQUEST_ID;
use iggy_binary_protocol::primitives::metadata_changes::{
    WireChangeOptions, WireMemberLeftBody, WireMetadataChange, WireMetadataChangeGroup,
    WireMetadataChangeKind, WirePartitionHandedOverBody, WirePartitionsChangedBody,
    WireSessionBody, WireSessionClosedBody, WireSessionClosedReason, WireStreamBody, WireStreamKey,
    WireTopicBody, WireTopicKey, WireUserBody, WireUserKey,
};
use iggy_binary_protocol::{PrepareHeader, WireName};
use iggy_common::wire_conversions::{permissions_to_wire, resource_options_to_wire_split};
use iggy_common::{OptionsScope, ResourceOptions, public_options};

/// One change a committed operation made to public metadata.
///
/// The body is encoded once, at apply, from the public projection, so the log
/// never holds more than a reader may see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedChange {
    pub kind: WireMetadataChangeKind,
    /// The requesting user, absent for a server action.
    pub actor_user_id: Option<u32>,
    pub body: Bytes,
}

impl AppliedChange {
    #[must_use]
    pub fn new(change: &WireMetadataChange, actor_user_id: Option<u32>) -> Self {
        Self {
            kind: change.kind(),
            actor_user_id,
            body: change.body_bytes(),
        }
    }

    #[must_use]
    pub const fn group(&self) -> WireMetadataChangeGroup {
        self.kind.group()
    }
}

impl From<&Stream> for WireStreamKey {
    #[allow(clippy::cast_possible_truncation)]
    fn from(stream: &Stream) -> Self {
        Self {
            stream_id: stream.id as u32,
            stream_created_op: stream.created_op,
        }
    }
}

impl From<&User> for WireUserKey {
    fn from(user: &User) -> Self {
        Self {
            user_id: user.id,
            user_created_at: user.created_at.as_micros(),
        }
    }
}

impl Stream {
    /// The identity a record gives `topic`, one of this stream's topics.
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn topic_key(&self, topic: &Topic) -> WireTopicKey {
        WireTopicKey {
            stream: WireStreamKey::from(self),
            topic_id: topic.id as u32,
            topic_created_op: topic.created_op,
        }
    }
}

impl StreamsInner {
    /// The identity a record gives the topic at these slab ids, if it exists.
    pub(crate) fn topic_key(&self, stream_id: usize, topic_id: usize) -> Option<WireTopicKey> {
        let stream = self.items.get(stream_id)?;
        Some(stream.topic_key(stream.topics.get(topic_id)?))
    }

    /// The topic's partition count after this op, with the ids it removed.
    pub(crate) fn partitions_changed(
        &self,
        stream_id: usize,
        topic_id: usize,
        removed_partition_ids: Vec<u32>,
        requested_op: Option<u64>,
    ) -> Option<WirePartitionsChangedBody> {
        let stream = self.items.get(stream_id)?;
        let topic = stream.topics.get(topic_id)?;
        Some(WirePartitionsChangedBody {
            topic: stream.topic_key(topic),
            partitions_count: u32::try_from(topic.partitions.len()).ok()?,
            removed_partition_ids,
            requested_op,
        })
    }

    /// `requested_op` and actor of a delete record: those of the intent when
    /// this apply finishes a deferred delete, this operation's otherwise.
    pub(crate) fn deletion_origin(&self) -> (Option<u64>, Option<u32>) {
        self.finalizing_lifecycle.as_ref().map_or_else(
            || (None, change_actor(&self.apply_context)),
            |intent| (Some(intent.metadata_op), change_actor(intent)),
        )
    }
}

/// The user a record names as its actor. Absent for the root bootstrap, the
/// expiry cleaner, rewraps and partition completions, which commit under
/// client 0 or a server-internal client.
pub(crate) fn change_actor(context: &ApplyContext) -> Option<u32> {
    (context.client_id != 0 && !message_bus::is_server_internal_client(context.client_id))
        .then_some(context.user_id)
}

/// The record of a Register that inserted its client, with the epoch the
/// table gave it.
pub(crate) fn session_registered(
    header: &PrepareHeader,
    user_created_at: u64,
    epoch: u64,
) -> AppliedChange {
    AppliedChange::new(
        &WireMetadataChange::SessionRegistered(WireSessionBody {
            user: WireUserKey {
                user_id: header.user_id,
                user_created_at,
            },
            client_id: header.client,
            session: epoch,
        }),
        change_actor(&ApplyContext::from(header)),
    )
}

/// The record of a Logout that ended its session. A lease expiry is a server
/// action, so it names no actor.
pub(crate) fn session_closed(header: &PrepareHeader, user: WireUserKey) -> AppliedChange {
    let (reason, actor) = if header.request == EXPIRED_SESSION_REQUEST_ID {
        (WireSessionClosedReason::Expired, None)
    } else {
        (
            WireSessionClosedReason::Logout,
            change_actor(&ApplyContext::from(header)),
        )
    };
    AppliedChange::new(
        &WireMetadataChange::SessionClosed(WireSessionClosedBody {
            user,
            client_id: header.client,
            session: header.session,
            reason,
        }),
        actor,
    )
}

/// The record of a session that a user's deactivation or deletion ended.
pub(crate) fn session_revoked(
    user: WireUserKey,
    client_id: u128,
    session: u64,
    actor_user_id: Option<u32>,
) -> AppliedChange {
    AppliedChange::new(
        &WireMetadataChange::SessionClosed(WireSessionClosedBody {
            user,
            client_id,
            session,
            reason: WireSessionClosedReason::Revoked,
        }),
        actor_user_id,
    )
}

/// The public projection of `stored`, or the unreadable marker for critical
/// state this build cannot interpret. Every replica reaches the same verdict.
pub(crate) fn change_options(scope: OptionsScope, stored: &ResourceOptions) -> WireChangeOptions {
    public_options(scope, stored)
        .and_then(|public| resource_options_to_wire_split(&public))
        .map_or(WireChangeOptions::Unreadable, |(explicit, derived)| {
            WireChangeOptions::Readable { explicit, derived }
        })
}

/// `None` only for a stored name no wire name can carry, which create and
/// rename refuse, so every replica skips the same record.
pub(crate) fn stream_body(stream: &Stream) -> Option<WireStreamBody> {
    Some(WireStreamBody {
        key: WireStreamKey::from(stream),
        created_at: stream.created_at.as_micros(),
        name: WireName::new(stream.name.as_ref()).ok()?,
        options: change_options(OptionsScope::Stream, &stream.options),
    })
}

pub(crate) fn topic_body(stream: &Stream, topic: &Topic) -> Option<WireTopicBody> {
    Some(WireTopicBody {
        key: stream.topic_key(topic),
        created_at: topic.created_at.as_micros(),
        name: WireName::new(topic.name.as_ref()).ok()?,
        partitions_count: u32::try_from(topic.partitions.len()).ok()?,
        options: change_options(OptionsScope::Topic, &topic.options),
    })
}

pub(crate) fn user_body(user: &User) -> Option<WireUserBody> {
    Some(WireUserBody {
        key: WireUserKey::from(user),
        username: WireName::new(user.username.as_ref()).ok()?,
        status: user.status.as_code(),
        permissions: user.permissions.as_deref().map(permissions_to_wire),
        options: change_options(OptionsScope::User, &user.options),
    })
}

/// The handovers a member's session change makes: its installed partitions
/// no longer match it, so each one it lost has no installed member owner, and
/// each one it gained has it. Ascending by partition id.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn installed_partition_changes(
    topic: WireTopicKey,
    group_id: u32,
    member_id: u32,
    before: &[usize],
    after: &[usize],
) -> Vec<WirePartitionHandedOverBody> {
    let mut changes: Vec<WirePartitionHandedOverBody> = before
        .iter()
        .filter(|partition_id| !after.contains(partition_id))
        .map(|&partition_id| (partition_id, None))
        .chain(
            after
                .iter()
                .filter(|partition_id| !before.contains(partition_id))
                .map(|&partition_id| (partition_id, Some(member_id))),
        )
        .map(|(partition_id, member_id)| WirePartitionHandedOverBody {
            topic,
            group_id,
            partition_id: partition_id as u32,
            member_id,
        })
        .collect();
    changes.sort_unstable_by_key(|change| change.partition_id);
    changes
}

/// Membership records of one operation by stream, topic, group, member and
/// client. Slot and hash map order differ per replica, the records may not.
pub(crate) fn sort_member_records(records: &mut [WireMemberLeftBody]) {
    let key = |record: &WireMemberLeftBody| {
        (
            record.topic.stream.stream_id,
            record.topic.topic_id,
            record.group_id,
            record.member_id,
            record.client_id,
        )
    };
    records.sort_unstable_by_key(key);
    debug_assert!(
        records
            .windows(2)
            .all(|pair| key(&pair[0]) != key(&pair[1])),
        "one operation removed the same member twice"
    );
}

#[cfg(test)]
mod tests;
