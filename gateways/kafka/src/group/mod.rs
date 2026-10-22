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

//! In-memory coordinator for Kafka's classic consumer group protocol.
//!
//! [`GroupCoordinator`] owns every group this gateway instance coordinates and is the only
//! module that awaits: `FindCoordinator`/`JoinGroup`/`Heartbeat`/`LeaveGroup`/`SyncGroup`/
//! `DescribeGroups`/`ListGroups`/`OffsetCommit`/`DeleteGroups`/`OffsetDelete` handlers translate
//! wire messages into the request types here, and `state` holds the synchronous state machine
//! those requests drive.
//!
//! Membership is process memory, not Iggy state. Two gateway instances fronting one Iggy cluster
//! therefore coordinate two independent groups under one name; see `docs/CONSUMER_GROUPS.md`.

mod offset_work;
mod state;

pub use offset_work::{RunningCommit, RunningDeletion};
pub use state::{MAX_GROUP_ID_BYTES, is_valid_group_id};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{
    HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, OffsetCommitRequest, SyncGroupRequest,
};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{Mutex, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::group::offset_work::OffsetWork;
use crate::group::state::{GroupState, Step};
use crate::protocol::api::{
    ERROR_COORDINATOR_LOAD_IN_PROGRESS, ERROR_NONE, ERROR_NOT_COORDINATOR, ERROR_UNKNOWN_MEMBER_ID,
};

/// `DescribeGroups` / `ListGroups` state string for a group with no joined members.
pub const GROUP_STATE_EMPTY: &str = "Empty";
/// `DescribeGroups` / `ListGroups` state string for a group preparing a rebalance.
pub const GROUP_STATE_PREPARING_REBALANCE: &str = "PreparingRebalance";
/// `DescribeGroups` / `ListGroups` state string for a group waiting on `SyncGroup`.
pub const GROUP_STATE_COMPLETING_REBALANCE: &str = "CompletingRebalance";
/// `DescribeGroups` / `ListGroups` state string for a group whose assignment is in effect.
pub const GROUP_STATE_STABLE: &str = "Stable";

/// Kafka's `ConsumerProtocol.PROTOCOL_TYPE`, the protocol type of a consumer group.
pub const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

/// Protocol type of a group that only holds offsets in Iggy.
///
/// Kafka keeps `consumer` for a group whose consumers left, and Iggy stores no protocol type to say
/// otherwise. Kafka's admin client takes a group as a consumer group with `consumer` or an empty
/// type.
pub const OFFSETS_ONLY_PROTOCOL_TYPE: &str = CONSUMER_PROTOCOL_TYPE;

/// v6 `error_message` when one `DescribeGroups` group does not fit the response budget.
///
/// `UNKNOWN_SERVER_ERROR` is not retried by `kafka-consumer-groups.sh`, so the tool prints this
/// for the groups that did not fit and still shows the ones that did. A retriable code would
/// send the same describe-all request back until the client timed out.
pub const DESCRIBE_RESPONSE_TOO_LARGE: &str = "Group description exceeds the response size limit.";

/// Kafka's own `group.min.session.timeout.ms` default.
const DEFAULT_MIN_SESSION_TIMEOUT: Duration = Duration::from_secs(6);
/// Kafka's own `group.max.session.timeout.ms` default.
const DEFAULT_MAX_SESSION_TIMEOUT: Duration = Duration::from_mins(30);

/// Kafka does not cap `rebalance.timeout.ms`, so this is a gateway resource bound rather than a
/// protocol rule. It matches `DEFAULT_MAX_SESSION_TIMEOUT` because a barrier deadline is what
/// bounds a park, and a larger value here would lengthen how long one request holds a connection
/// and its `max_connections` permit. A client asking for more is clamped, never refused.
const DEFAULT_MAX_REBALANCE_TIMEOUT: Duration = Duration::from_mins(30);
/// Kafka's own `group.initial.rebalance.delay.ms` default.
const DEFAULT_INITIAL_REBALANCE_DELAY: Duration = Duration::from_secs(3);
/// Below the 30 s after which the Java admin client gives up on a request (`request.timeout.ms`).
const DEFAULT_DELETION_HOLD: Duration = Duration::from_secs(25);

/// Bounds on the state an unauthenticated client can make this gateway retain.
///
/// Only `max_members_per_group` has a Kafka analogue (`group.max.size`, unlimited by default).
/// The rest exist because group state outlives the connection that created it - a member stays
/// until its session expires, up to `max_session_timeout` - so nothing in the frame-level bounds
/// guard bounds the total.
///
/// Worst-case retained opaque bytes is `max_total_members * 2 * max_member_blob_bytes` (one
/// subscription plus one assignment per member): ~1.3 GiB at the defaults below.
#[derive(Debug, Clone)]
pub struct GroupCoordinatorConfig {
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    /// Ceiling on what a member's `rebalance_timeout` may contribute to a barrier deadline.
    ///
    /// A client derives this from `max.poll.interval.ms`, which no broker range-checks, so it is
    /// clamped rather than rejected: a value above the ceiling is honoured up to it instead of
    /// failing the join. Without a bound it would be the only limit on how long a parked waiter
    /// holds its connection, since a parked member's session is refreshed rather than expiring.
    pub max_rebalance_timeout: Duration,
    /// How long a brand-new group waits for more members before completing its first join.
    pub initial_rebalance_delay: Duration,
    pub max_groups: usize,
    pub max_members_per_group: usize,
    /// Cap across every group, checked before a new member id is handed out.
    pub max_total_members: usize,
    /// Cap on the opaque bytes retained per member: the sum of one `JoinGroup`'s
    /// `protocols[].metadata`, and the size of one `SyncGroup` assignment blob.
    pub max_member_blob_bytes: usize,
    /// Cap on the roster one group's leader is sent in its `JoinGroup` response: every member's
    /// id, instance id and subscription metadata. `max_members_per_group * max_member_blob_bytes`
    /// is 64 MiB at the defaults, so without this a full group builds a response far larger than
    /// the default 8 MiB `max_frame_size` the gateway itself accepts.
    pub max_group_roster_bytes: usize,
    /// How long a `DeleteGroups` or `OffsetDelete` may run on a connection where no member
    /// heartbeats, as an admin client's. Such a request can list every topic, which takes longer
    /// than an offset call may hold a member's connection.
    pub deletion_hold: Duration,
}

impl Default for GroupCoordinatorConfig {
    fn default() -> Self {
        Self {
            min_session_timeout: DEFAULT_MIN_SESSION_TIMEOUT,
            max_session_timeout: DEFAULT_MAX_SESSION_TIMEOUT,
            max_rebalance_timeout: DEFAULT_MAX_REBALANCE_TIMEOUT,
            initial_rebalance_delay: DEFAULT_INITIAL_REBALANCE_DELAY,
            max_groups: 1_000,
            max_members_per_group: 1_000,
            max_total_members: 10_000,
            max_member_blob_bytes: 64 * 1024,
            max_group_roster_bytes: 4 * 1024 * 1024,
            deletion_hold: DEFAULT_DELETION_HOLD,
        }
    }
}

/// One `JoinGroup` request, normalized across wire versions.
#[derive(Debug, Clone)]
pub struct JoinRequest {
    pub group_id: StrBytes,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub member_id: StrBytes,
    pub group_instance_id: Option<StrBytes>,
    pub protocol_type: StrBytes,
    /// `(name, metadata)` in request order; the order is this member's preference vote.
    pub protocols: Vec<(StrBytes, Bytes)>,
    /// KIP-394: from v4 a member must claim an id the coordinator handed it first.
    pub require_known_member_id: bool,
    /// v9+: a static leader replaced while `Stable` is told to skip the assignment it already
    /// ran, instead of being named a follower.
    pub supports_skip_assignment: bool,
}

/// Every field is copied out of the frame rather than cloned: a clone is a refcounted view that
/// keeps the whole decoded frame, `reason` included, alive for as long as the join parks.
impl From<(i16, &JoinGroupRequest)> for JoinRequest {
    fn from((api_version, request): (i16, &JoinGroupRequest)) -> Self {
        let session_timeout = millis_to_duration(request.session_timeout_ms);
        Self {
            group_id: owned_str(&request.group_id.0),
            session_timeout,
            // v0 has no rebalance timeout and decodes to -1. A non-positive value from any other
            // version is treated the same way rather than admitting a zero-length rebalance
            // window, which would expire the moment it was set.
            rebalance_timeout: if request.rebalance_timeout_ms > 0 {
                millis_to_duration(request.rebalance_timeout_ms)
            } else {
                session_timeout
            },
            member_id: owned_str(&request.member_id),
            group_instance_id: request.group_instance_id.as_ref().map(owned_str),
            protocol_type: owned_str(&request.protocol_type),
            protocols: request
                .protocols
                .iter()
                .map(|protocol| {
                    (
                        owned_str(&protocol.name),
                        Bytes::copy_from_slice(&protocol.metadata),
                    )
                })
                .collect(),
            require_known_member_id: api_version >= 4,
            supports_skip_assignment: api_version >= 9,
        }
    }
}

/// One `SyncGroup` request, normalized across wire versions.
#[derive(Debug, Clone)]
pub struct SyncRequest {
    pub group_id: StrBytes,
    pub generation_id: i32,
    pub member_id: StrBytes,
    /// Present from v3; validated against the instance's current holder.
    pub group_instance_id: Option<StrBytes>,
    /// Present from v5 only; validated against the group when set.
    pub protocol_type: Option<StrBytes>,
    pub protocol_name: Option<StrBytes>,
    pub assignments: Vec<(StrBytes, Bytes)>,
}

/// Copied out of the frame for the same reason as [`JoinRequest`]: a sync can park.
impl From<&SyncGroupRequest> for SyncRequest {
    fn from(request: &SyncGroupRequest) -> Self {
        Self {
            group_id: owned_str(&request.group_id.0),
            generation_id: request.generation_id,
            member_id: owned_str(&request.member_id),
            group_instance_id: request.group_instance_id.as_ref().map(owned_str),
            protocol_type: request.protocol_type.as_ref().map(owned_str),
            protocol_name: request.protocol_name.as_ref().map(owned_str),
            assignments: request
                .assignments
                .iter()
                .map(|assignment| {
                    (
                        owned_str(&assignment.member_id),
                        Bytes::copy_from_slice(&assignment.assignment),
                    )
                })
                .collect(),
        }
    }
}

/// One member as the group leader sees it in its `JoinGroup` response.
#[derive(Debug, Clone)]
pub struct JoinedMember {
    pub member_id: StrBytes,
    pub group_instance_id: Option<StrBytes>,
    pub metadata: Bytes,
}

/// Everything a `JoinGroup` response carries, before the handler shapes it for a wire version.
#[derive(Debug, Clone)]
pub struct JoinResult {
    pub error: i16,
    pub generation_id: i32,
    pub protocol_type: Option<StrBytes>,
    pub protocol_name: Option<StrBytes>,
    pub leader: StrBytes,
    /// The id the member must use next. Set on `MEMBER_ID_REQUIRED` too.
    pub member_id: StrBytes,
    /// Non-empty only for the leader.
    pub members: Vec<JoinedMember>,
    /// v9+: the leader keeps the assignment already in effect. Set only for a static leader
    /// replaced while the group is `Stable`.
    pub skip_assignment: bool,
}

impl JoinResult {
    #[must_use]
    pub const fn error(error: i16, member_id: StrBytes) -> Self {
        Self {
            error,
            generation_id: -1,
            protocol_type: None,
            protocol_name: None,
            leader: StrBytes::new(),
            member_id,
            members: Vec::new(),
            skip_assignment: false,
        }
    }
}

/// Everything a `SyncGroup` response carries.
#[derive(Debug, Clone)]
pub struct SyncResult {
    pub error: i16,
    pub protocol_type: Option<StrBytes>,
    pub protocol_name: Option<StrBytes>,
    pub assignment: Bytes,
}

impl SyncResult {
    #[must_use]
    pub const fn error(error: i16) -> Self {
        Self {
            error,
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
        }
    }
}

/// One `LeaveGroup` request, normalized across wire versions.
#[derive(Debug, Clone)]
pub struct LeaveRequest {
    pub group_id: StrBytes,
    /// Request order, which is also the order the v3+ response answers in.
    pub members: Vec<LeavingMember>,
}

/// One identity a `LeaveGroup` asks to remove.
#[derive(Debug, Clone)]
pub struct LeavingMember {
    /// Empty when a v3+ request leaves by `group_instance_id` alone.
    pub member_id: StrBytes,
    pub group_instance_id: Option<StrBytes>,
}

impl From<(i16, &LeaveGroupRequest)> for LeaveRequest {
    fn from((api_version, request): (i16, &LeaveGroupRequest)) -> Self {
        let members = if api_version >= 3 {
            request
                .members
                .iter()
                .map(|member| LeavingMember {
                    member_id: member.member_id.clone(),
                    group_instance_id: member.group_instance_id.clone(),
                })
                .collect()
        } else {
            vec![LeavingMember {
                member_id: request.member_id.clone(),
                group_instance_id: None,
            }]
        };
        Self {
            group_id: request.group_id.0.clone(),
            members,
        }
    }
}

/// Everything a `LeaveGroup` response carries, before the handler shapes it for a wire version.
#[derive(Debug, Clone)]
pub struct LeaveResult {
    /// A whole-request failure. When set, `members` is empty.
    pub error: i16,
    /// One entry per request identity, in request order, echoing exactly what was sent.
    pub members: Vec<LeftMember>,
}

impl LeaveResult {
    #[must_use]
    pub const fn error(error: i16) -> Self {
        Self {
            error,
            members: Vec::new(),
        }
    }

    /// The single code a v0-v2 response carries: the request's own error, else the first
    /// member's, as Kafka's `LeaveGroupResponse.getError` folds them.
    #[must_use]
    pub fn top_level_error(&self) -> i16 {
        if self.error != ERROR_NONE {
            return self.error;
        }
        self.members
            .iter()
            .map(|member| member.error)
            .find(|error| *error != ERROR_NONE)
            .unwrap_or(ERROR_NONE)
    }
}

/// One member as `DescribeGroups` reports it.
#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub member_id: StrBytes,
    pub group_instance_id: Option<StrBytes>,
    /// Subscription metadata for the group's selected protocol. Empty until a protocol is chosen.
    pub metadata: Bytes,
    pub assignment: Bytes,
}

/// One group as `ListGroups` reports it. Member blobs stay on the coordinator.
#[derive(Debug, Clone)]
pub struct GroupListing {
    pub group_id: StrBytes,
    pub protocol_type: StrBytes,
    pub state: &'static str,
}

impl GroupListing {
    /// A group with no members here that holds offsets in Iggy. Kafka keeps a group whose
    /// consumers left, and lists it as `Empty`.
    #[must_use]
    pub const fn offsets_only(group_id: StrBytes) -> Self {
        Self {
            group_id,
            protocol_type: StrBytes::from_static_str(OFFSETS_ONLY_PROTOCOL_TYPE),
            state: GROUP_STATE_EMPTY,
        }
    }
}

/// One group the coordinator currently holds, as `DescribeGroups` reports it.
#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: StrBytes,
    /// `ERROR_NONE`, or `ERROR_UNKNOWN_SERVER_ERROR` when this group does not fit the response.
    /// A non-zero code is encoded as an empty group: the other fields are not put on the wire.
    pub error: i16,
    /// One of the `GROUP_STATE_*` strings.
    pub state: &'static str,
    pub protocol_type: StrBytes,
    /// Selected protocol name. `None` until the group is `Stable`, and `None` when the group is
    /// described without member metadata.
    pub protocol_name: Option<StrBytes>,
    pub members: Vec<MemberDescription>,
}

impl GroupDescription {
    /// [`GroupListing::offsets_only`] as `DescribeGroups` reports it: `Empty`, with no protocol
    /// and no members.
    #[must_use]
    pub const fn offsets_only(group_id: StrBytes) -> Self {
        Self {
            group_id,
            error: ERROR_NONE,
            state: GROUP_STATE_EMPTY,
            protocol_type: StrBytes::from_static_str(OFFSETS_ONLY_PROTOCOL_TYPE),
            protocol_name: None,
            members: Vec::new(),
        }
    }
}

/// What `DeleteGroups` found for one group on this coordinator, by Kafka's
/// `ClassicGroup.validateDeleteGroup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupRemoval {
    /// Not on this coordinator.
    Absent,
    /// Has joined members. Kafka refuses with `NON_EMPTY_GROUP`.
    Live,
    /// Has no joined members, only ids that wait to join. It goes once its offsets do
    /// ([`GroupCoordinator::remove_empty`]).
    Empty,
}

/// What `OffsetDelete` may do with the offsets of one group on this coordinator, by Kafka's
/// `ClassicGroup.validateOffsetDelete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetDeletion {
    /// Not on this coordinator.
    Absent,
    /// Has members of a protocol other than `consumer`. Kafka refuses with `NON_EMPTY_GROUP`.
    Refused,
    /// On this coordinator. The offsets of a topic that a member subscribes to stay.
    Allowed(Subscribed),
}

/// The topics that the members of a group subscribe to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subscribed {
    /// Empty for a group with no joined members.
    Topics(HashSet<String>),
    /// Every topic. A member's subscription does not decode, or the group has not chosen a
    /// protocol yet. Kafka's `ClassicGroup.isSubscribedToTopic` does the same.
    All,
}

impl Subscribed {
    #[must_use]
    pub fn contains(&self, topic: &str) -> bool {
        match self {
            Self::Topics(topics) => topics.contains(topic),
            Self::All => true,
        }
    }
}

/// The answer for one `LeavingMember`.
#[derive(Debug, Clone)]
pub struct LeftMember {
    pub member_id: StrBytes,
    pub group_instance_id: Option<StrBytes>,
    pub error: i16,
}

impl From<(&LeavingMember, i16)> for LeftMember {
    fn from((identity, error): (&LeavingMember, i16)) -> Self {
        Self {
            member_id: identity.member_id.clone(),
            group_instance_id: identity.group_instance_id.clone(),
            error,
        }
    }
}

/// Who an `OffsetCommit` says it is.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub group_id: StrBytes,
    /// Negative for a commit made outside the group protocol, as admin tools send.
    pub generation_id: i32,
    pub member_id: StrBytes,
    pub group_instance_id: Option<StrBytes>,
}

impl From<&OffsetCommitRequest> for CommitRequest {
    fn from(request: &OffsetCommitRequest) -> Self {
        Self {
            group_id: request.group_id.0.clone(),
            generation_id: request.generation_id_or_member_epoch,
            member_id: request.member_id.clone(),
            group_instance_id: request.group_instance_id.clone(),
        }
    }
}

/// One member of one group. Holds copies, so it pins no frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub group_id: StrBytes,
    pub member_id: StrBytes,
}

impl From<&HeartbeatRequest> for GroupMember {
    fn from(request: &HeartbeatRequest) -> Self {
        Self {
            group_id: owned_str(&request.group_id.0),
            member_id: owned_str(&request.member_id),
        }
    }
}

/// Every consumer group this gateway instance coordinates.
///
/// There is no timer task. A request that touches a group first expires whatever is overdue in
/// it, and a parked `JoinGroup`/`SyncGroup` waiter sleeps until that group's next deadline, so
/// the coroutine waiting on a barrier is also the timer that fires it.
pub struct GroupCoordinator {
    config: GroupCoordinatorConfig,
    groups: Mutex<HashMap<StrBytes, GroupState>>,
    /// The commits and the offset deletions that run for each group. Changed under `groups`, so
    /// a check of a group and its deletion's start are one step.
    offset_work: Arc<OffsetWork>,
    /// Resolves parked waiters on shutdown drain instead of holding it open for a full rebalance
    /// timeout.
    shutdown: CancellationToken,
}

impl GroupCoordinator {
    #[must_use]
    pub fn new(config: GroupCoordinatorConfig, shutdown: CancellationToken) -> Self {
        Self {
            config,
            groups: Mutex::new(HashMap::new()),
            offset_work: Arc::new(OffsetWork::new()),
            shutdown,
        }
    }

    /// Joins `request`'s member, parking until the group's join barrier completes.
    ///
    /// While a deletion of the group's offsets runs, answers `COORDINATOR_LOAD_IN_PROGRESS`,
    /// which clients retry. A member that joined then could subscribe to a topic whose offsets
    /// the deletion removes, and commit offsets that it wipes.
    pub async fn join(&self, request: &JoinRequest) -> JoinResult {
        let mut parked: Option<StrBytes> = None;
        loop {
            let outcome = {
                let mut groups = self.groups.lock().await;
                let now = Instant::now();
                let step = match parked.as_ref() {
                    None if self.offset_work.deleting(&request.group_id) => {
                        Step::Respond(JoinResult::error(
                            ERROR_COORDINATOR_LOAD_IN_PROGRESS,
                            request.member_id.clone(),
                        ))
                    }
                    None => state::join_step(&mut groups, &self.config, request, now),
                    Some(member_id) => state::join_resume_step(
                        &mut groups,
                        &request.group_id,
                        member_id,
                        request.group_instance_id.as_ref(),
                        now,
                    ),
                };
                let outcome = park_outcome(&groups, &request.group_id, step, |member_id| {
                    JoinResult::error(ERROR_UNKNOWN_MEMBER_ID, member_id)
                });
                drop(groups);
                outcome
            };
            let (member_id, wake_at, receiver) = match outcome {
                Parked::Done(result) => return result,
                Parked::Wait(member_id, wake_at, receiver) => (member_id, wake_at, receiver),
            };
            if !self.wait_until(receiver, wake_at).await {
                return JoinResult::error(ERROR_NOT_COORDINATOR, member_id);
            }
            parked = Some(member_id);
        }
    }

    /// Delivers `request`'s member its assignment, parking a follower until the leader syncs.
    pub async fn sync(&self, mut request: SyncRequest) -> SyncResult {
        let mut parked = false;
        loop {
            let outcome = {
                let mut groups = self.groups.lock().await;
                let now = Instant::now();
                let step = if parked {
                    state::sync_resume_step(
                        &mut groups,
                        &request.group_id,
                        &request.member_id,
                        request.group_instance_id.as_ref(),
                        request.generation_id,
                        now,
                    )
                } else {
                    state::sync_step(&mut groups, &self.config, &request, now)
                };
                let outcome = park_outcome(&groups, &request.group_id, step, |_| {
                    SyncResult::error(ERROR_UNKNOWN_MEMBER_ID)
                });
                drop(groups);
                outcome
            };
            let (wake_at, receiver) = match outcome {
                Parked::Done(result) => return result,
                Parked::Wait(_, wake_at, receiver) => (wake_at, receiver),
            };
            // `sync_resume_step` never reads them, so a parked member holds none of its blobs.
            request.assignments = Vec::new();
            if !self.wait_until(receiver, wake_at).await {
                return SyncResult::error(ERROR_NOT_COORDINATOR);
            }
            parked = true;
        }
    }

    /// Refreshes a member's session and reports whether it must rejoin. Never parks.
    pub async fn heartbeat(
        &self,
        group_id: &StrBytes,
        generation_id: i32,
        member_id: &StrBytes,
        group_instance_id: Option<&StrBytes>,
    ) -> i16 {
        let mut groups = self.groups.lock().await;
        state::heartbeat_step(
            &mut groups,
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            Instant::now(),
        )
    }

    /// Removes `request`'s members and releases whoever is parked on the group. Never parks.
    pub async fn leave(&self, request: &LeaveRequest) -> LeaveResult {
        let mut groups = self.groups.lock().await;
        state::leave_step(&mut groups, request, Instant::now())
    }

    /// Snapshot of each distinct group, in first-seen order. `None` means the group is not here.
    ///
    /// Ids are deduped before the lock is taken. Under the lock each remaining group is ticked
    /// once and snapshotted once. A group that does not fit is returned with
    /// `UNKNOWN_SERVER_ERROR` instead of failing the whole response. Encoding happens
    /// after this returns.
    pub async fn describe_groups(
        &self,
        group_ids: &[StrBytes],
        version: i16,
        max_frame_size: usize,
    ) -> Vec<Option<GroupDescription>> {
        let mut groups = self.groups.lock().await;
        state::describe_groups(
            &mut groups,
            group_ids,
            version,
            max_frame_size,
            Instant::now(),
        )
    }

    /// The commit of `request`, counted until it drops, when `request` may commit offsets for its
    /// group. Keep it until no Iggy call of the commit can land. A member's commit also refreshes
    /// its session. Never parks.
    ///
    /// # Errors
    ///
    /// The Kafka code that refuses the commit. `COORDINATOR_LOAD_IN_PROGRESS` while a deletion of
    /// the group's offsets runs, which clients retry.
    pub async fn validate_commit(&self, request: &CommitRequest) -> Result<RunningCommit, i16> {
        let mut groups = self.groups.lock().await;
        match state::commit_step(&mut groups, request, Instant::now()) {
            ERROR_NONE => self
                .offset_work
                .begin_commit(&request.group_id)
                .ok_or(ERROR_COORDINATOR_LOAD_IN_PROGRESS),
            error => Err(error),
        }
    }

    /// What `DeleteGroups` may do with each group of `group_ids`. One answer per id, in order. The
    /// deletion of the offsets of each group that is not [`GroupRemoval::Live`] runs until the
    /// returned value drops. Keep it until no Iggy call of the deletion can land. Removes no group,
    /// so a deletion that fails changes nothing, as in Kafka. Never parks.
    pub async fn group_deletion(
        &self,
        group_ids: &[StrBytes],
    ) -> (Vec<GroupRemoval>, RunningDeletion) {
        let mut groups = self.groups.lock().await;
        let now = Instant::now();
        let removals: Vec<GroupRemoval> = group_ids
            .iter()
            .map(|group_id| state::group_removal(&mut groups, group_id, now))
            .collect();
        let deleted = group_ids
            .iter()
            .zip(&removals)
            .filter(|(_, removal)| **removal != GroupRemoval::Live)
            .map(|(group_id, _)| group_id);
        let deletion = self.offset_work.begin_deletion(deleted);
        drop(groups);
        (removals, deletion)
    }

    /// Removes each group of `group_ids` that still has no joined members, with the ids that wait
    /// to join it. `DeleteGroups` calls it once the offsets of those groups are gone, while their
    /// deletion still runs, so no member joins in between. Never parks.
    pub async fn remove_empty(&self, group_ids: &[StrBytes]) {
        let mut groups = self.groups.lock().await;
        let now = Instant::now();
        for group_id in group_ids {
            state::remove_empty(&mut groups, group_id, now);
        }
    }

    /// What `OffsetDelete` may do with the offsets of `group_id`. The deletion runs until the
    /// returned value drops. Keep it until no Iggy call of the deletion can land. Never parks.
    pub async fn offset_deletion(&self, group_id: &StrBytes) -> (OffsetDeletion, RunningDeletion) {
        let mut groups = self.groups.lock().await;
        let allowed = state::offset_deletion(&mut groups, group_id, Instant::now());
        let deletion = self.offset_work.begin_deletion([group_id]);
        drop(groups);
        (allowed, deletion)
    }

    /// How long an `OffsetCommit` or `OffsetFetch` may hold its connection. A heartbeat there
    /// waits behind it. So it takes half the shortest session, and at most half of what is left
    /// of the session of `member`, the member that heartbeats on that connection. Never parks.
    pub async fn offset_hold(&self, member: Option<&GroupMember>) -> Duration {
        let hold = self.config.min_session_timeout / 2;
        let Some(member) = member else {
            return hold;
        };
        let groups = self.groups.lock().await;
        state::offset_hold(&groups, member, hold, Instant::now())
    }

    /// How long a `DeleteGroups` or `OffsetDelete` may hold its connection: `deletion_hold` when
    /// no member heartbeats there, else [`Self::offset_hold`]. Never parks.
    pub async fn deletion_hold(&self, member: Option<&GroupMember>) -> Duration {
        match member {
            Some(_) => self.offset_hold(member).await,
            None => self.config.deletion_hold,
        }
    }

    /// Every group currently in the map, ordered by group id.
    ///
    /// Does not tick. A list names no group, and sweeping here would open rebalances as a side
    /// effect of reading. Groups whose sessions and pending ids are already past `now` are left
    /// out, which is the same membership `DescribeGroups` would see after it did tick.
    pub async fn list_groups(&self) -> Vec<GroupListing> {
        let groups = self.groups.lock().await;
        state::list_groups(&groups, Instant::now())
    }

    /// Sleeps until the group changes or `wake_at` passes. `false` means the gateway is draining.
    async fn wait_until(&self, mut receiver: watch::Receiver<u64>, wake_at: Instant) -> bool {
        tokio::select! {
            _ = tokio::time::timeout_at(wake_at, receiver.changed()) => true,
            () = self.shutdown.cancelled() => false,
        }
    }
}

/// A step's outcome once the group's change channel has been captured under the same lock that
/// produced it: subscribing later would race the very wake-up being waited for.
enum Parked<T> {
    Done(T),
    Wait(StrBytes, Instant, watch::Receiver<u64>),
}

fn park_outcome<T>(
    groups: &HashMap<StrBytes, GroupState>,
    group_id: &StrBytes,
    step: Step<T>,
    on_missing: impl FnOnce(StrBytes) -> T,
) -> Parked<T> {
    match step {
        Step::Respond(result) => Parked::Done(result),
        Step::Wait { member_id, wake_at } => groups.get(group_id).map_or_else(
            || Parked::Done(on_missing(member_id.clone())),
            |group| Parked::Wait(member_id.clone(), wake_at, group.subscribe()),
        ),
    }
}

/// A `StrBytes` with its own allocation, detached from the frame it was decoded from.
fn owned_str(value: &StrBytes) -> StrBytes {
    StrBytes::from_string(value.as_str().to_owned())
}

fn millis_to_duration(millis: i32) -> Duration {
    u64::try_from(millis).map_or(Duration::ZERO, Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::leave_group_request::MemberIdentity;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;

    use super::*;
    use crate::protocol::api::ERROR_MEMBER_ID_REQUIRED;

    #[test]
    fn given_a_v0_join_when_normalizing_should_use_the_session_timeout_as_rebalance_window() {
        let request = JoinGroupRequest::default()
            .with_session_timeout_ms(9_000)
            .with_rebalance_timeout_ms(-1);
        let normalized = JoinRequest::from((0, &request));

        assert_eq!(normalized.session_timeout, Duration::from_millis(9_000));
        assert_eq!(normalized.rebalance_timeout, Duration::from_millis(9_000));
    }

    /// The handler drops the decoded request before parking, which frees the frame only if
    /// nothing normalized out of it still points into it.
    #[test]
    fn given_a_decoded_join_when_normalizing_should_not_point_into_the_frame() {
        let frame = Bytes::from_static(b"g m i consumer range meta");
        let text = |range| StrBytes::from_utf8(frame.slice(range)).unwrap();
        let request = JoinGroupRequest::default()
            .with_group_id(GroupId(text(0..1)))
            .with_member_id(text(2..3))
            .with_group_instance_id(Some(text(4..5)))
            .with_protocol_type(text(6..14))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(text(15..20))
                    .with_metadata(frame.slice(21..25)),
            ]);

        let normalized = JoinRequest::from((9, &request));

        let within = |ptr: *const u8| frame.as_ptr_range().contains(&ptr);
        let (name, metadata) = &normalized.protocols[0];
        let pointers = [
            normalized.group_id.as_ptr(),
            normalized.member_id.as_ptr(),
            normalized.group_instance_id.as_ref().unwrap().as_ptr(),
            normalized.protocol_type.as_ptr(),
            name.as_ptr(),
            metadata.as_ptr(),
        ];
        assert!(pointers.into_iter().all(|ptr| !within(ptr)));
        assert_eq!(normalized.group_instance_id.as_deref(), Some("i"));
        assert_eq!(metadata.as_ref(), b"meta");
    }

    #[test]
    fn given_a_join_version_when_normalizing_should_require_a_known_member_id_from_v4() {
        let request = JoinGroupRequest::default()
            .with_session_timeout_ms(9_000)
            .with_rebalance_timeout_ms(30_000);

        assert!(!JoinRequest::from((3, &request)).require_known_member_id);
        assert!(JoinRequest::from((4, &request)).require_known_member_id);
        assert_eq!(
            JoinRequest::from((4, &request)).rebalance_timeout,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn given_a_v2_leave_when_normalizing_should_use_the_top_level_member_id() {
        let request = LeaveGroupRequest::default()
            .with_group_id(StrBytes::from_static_str("g").into())
            .with_member_id(StrBytes::from_static_str("m-1"));

        let normalized = LeaveRequest::from((2, &request));

        assert_eq!(normalized.group_id.as_str(), "g");
        assert_eq!(normalized.members.len(), 1);
        assert_eq!(normalized.members[0].member_id.as_str(), "m-1");
        assert_eq!(normalized.members[0].group_instance_id, None);
    }

    #[test]
    fn given_a_v3_leave_when_normalizing_should_use_the_members_array() {
        let request = LeaveGroupRequest::default()
            .with_group_id(StrBytes::from_static_str("g").into())
            .with_member_id(StrBytes::from_static_str("ignored"))
            .with_members(vec![
                MemberIdentity::default().with_member_id(StrBytes::from_static_str("m-1")),
                MemberIdentity::default()
                    .with_member_id(StrBytes::new())
                    .with_group_instance_id(Some(StrBytes::from_static_str("i-2"))),
            ]);

        let normalized = LeaveRequest::from((3, &request));

        let identities: Vec<(&str, Option<&str>)> = normalized
            .members
            .iter()
            .map(|member| {
                (
                    member.member_id.as_str(),
                    member.group_instance_id.as_ref().map(StrBytes::as_str),
                )
            })
            .collect();
        assert_eq!(identities, vec![("m-1", None), ("", Some("i-2"))]);
    }

    /// No initial rebalance delay, so a first join completes at once.
    fn coordinator() -> GroupCoordinator {
        let config = GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::ZERO,
            ..GroupCoordinatorConfig::default()
        };
        GroupCoordinator::new(config, CancellationToken::new())
    }

    fn id(group: &str) -> StrBytes {
        StrBytes::from_string(group.to_owned())
    }

    fn join_request(group: &str) -> JoinRequest {
        JoinRequest {
            group_id: id(group),
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(10),
            member_id: StrBytes::new(),
            group_instance_id: None,
            protocol_type: StrBytes::from_static_str(CONSUMER_PROTOCOL_TYPE),
            protocols: vec![(StrBytes::from_static_str("range"), Bytes::new())],
            require_known_member_id: false,
            supports_skip_assignment: false,
        }
    }

    /// A commit made outside the group protocol, as admin tools send.
    fn standalone_commit(group: &str) -> CommitRequest {
        CommitRequest {
            group_id: id(group),
            generation_id: -1,
            member_id: StrBytes::new(),
            group_instance_id: None,
        }
    }

    /// Kafka's coordinator refuses a join or a commit of a group that `DeleteGroups` marked
    /// `Dead`. A member that joined during the deletion would see its commits wiped.
    #[tokio::test]
    async fn given_a_deletion_that_runs_should_refuse_joins_and_commits_of_its_groups() {
        let coordinator = coordinator();
        let (removals, deletion) = coordinator.group_deletion(&[id("orders")]).await;
        assert_eq!(removals, [GroupRemoval::Absent]);

        assert_eq!(
            coordinator.join(&join_request("orders")).await.error,
            ERROR_COORDINATOR_LOAD_IN_PROGRESS
        );
        assert_eq!(
            coordinator
                .validate_commit(&standalone_commit("orders"))
                .await
                .err(),
            Some(ERROR_COORDINATOR_LOAD_IN_PROGRESS)
        );
        assert!(
            coordinator
                .validate_commit(&standalone_commit("payments"))
                .await
                .is_ok(),
            "another group commits"
        );

        drop(deletion);
        assert!(
            coordinator
                .validate_commit(&standalone_commit("orders"))
                .await
                .is_ok()
        );
        assert_eq!(
            coordinator.join(&join_request("orders")).await.error,
            ERROR_NONE
        );
    }

    /// A group with members is not deleted, so its members go on committing.
    #[tokio::test]
    async fn given_a_live_group_when_deleting_should_not_refuse_its_commits() {
        let coordinator = coordinator();
        let joined = coordinator.join(&join_request("orders")).await;
        assert_eq!(joined.error, ERROR_NONE);
        let synced = coordinator
            .sync(SyncRequest {
                group_id: id("orders"),
                generation_id: joined.generation_id,
                member_id: joined.member_id.clone(),
                group_instance_id: None,
                protocol_type: None,
                protocol_name: None,
                assignments: vec![(joined.member_id.clone(), Bytes::new())],
            })
            .await;
        assert_eq!(synced.error, ERROR_NONE, "the group is stable");

        let (removals, _deletion) = coordinator.group_deletion(&[id("orders")]).await;
        assert_eq!(removals, [GroupRemoval::Live]);

        let commit = CommitRequest {
            generation_id: joined.generation_id,
            member_id: joined.member_id,
            ..standalone_commit("orders")
        };
        assert!(coordinator.validate_commit(&commit).await.is_ok());
    }

    /// The first join of a member that must claim its id first. The answer carries that id.
    async fn pending_member(coordinator: &GroupCoordinator, group: &str) -> StrBytes {
        let first = JoinRequest {
            require_known_member_id: true,
            ..join_request(group)
        };
        let pending = coordinator.join(&first).await;
        assert_eq!(pending.error, ERROR_MEMBER_ID_REQUIRED);
        pending.member_id
    }

    fn claim(group: &str, member_id: StrBytes) -> JoinRequest {
        JoinRequest {
            member_id,
            require_known_member_id: true,
            ..join_request(group)
        }
    }

    /// Kafka changes nothing on a `DeleteGroups` that fails, so an id that waits to join still
    /// joins after a deletion that removed nothing.
    #[tokio::test]
    async fn given_a_deletion_that_ends_without_removing_should_keep_the_ids_that_wait_to_join() {
        let coordinator = coordinator();
        let member_id = pending_member(&coordinator, "orders").await;

        let (removals, deletion) = coordinator.group_deletion(&[id("orders")]).await;
        assert_eq!(removals, [GroupRemoval::Empty]);
        drop(deletion);

        assert_eq!(
            coordinator.join(&claim("orders", member_id)).await.error,
            ERROR_NONE
        );
    }

    #[tokio::test]
    async fn given_a_deletion_whose_offsets_went_should_remove_the_ids_that_wait_to_join() {
        let coordinator = coordinator();
        let member_id = pending_member(&coordinator, "orders").await;

        let (_, deletion) = coordinator.group_deletion(&[id("orders")]).await;
        coordinator.remove_empty(&[id("orders")]).await;
        drop(deletion);

        assert_eq!(
            coordinator.join(&claim("orders", member_id)).await.error,
            ERROR_UNKNOWN_MEMBER_ID,
            "the client joins again from scratch"
        );
    }

    /// A member that joined during `OffsetDelete` could subscribe to a topic whose offsets go.
    #[tokio::test]
    async fn given_an_offset_deletion_that_runs_should_refuse_joins_of_the_group() {
        let coordinator = coordinator();
        let (deletable, deletion) = coordinator.offset_deletion(&id("orders")).await;
        assert_eq!(deletable, OffsetDeletion::Absent);

        assert_eq!(
            coordinator.join(&join_request("orders")).await.error,
            ERROR_COORDINATOR_LOAD_IN_PROGRESS
        );

        drop(deletion);
        assert_eq!(
            coordinator.join(&join_request("orders")).await.error,
            ERROR_NONE
        );
    }

    /// A deletion can list every topic. On an admin client's connection no member waits behind
    /// it, so it may take longer there than an offset call may on a member's connection.
    #[tokio::test]
    async fn given_a_connection_without_a_member_a_deletion_should_hold_it_longer() {
        let coordinator = coordinator();
        let joined = coordinator.join(&join_request("orders")).await;
        let member = GroupMember {
            group_id: id("orders"),
            member_id: joined.member_id,
        };

        assert_eq!(
            coordinator.deletion_hold(None).await,
            GroupCoordinatorConfig::default().deletion_hold
        );
        assert_eq!(
            coordinator.deletion_hold(Some(&member)).await,
            coordinator.offset_hold(Some(&member)).await
        );
    }

    #[test]
    fn given_a_decoded_sync_when_normalizing_should_not_point_into_the_frame() {
        let frame = Bytes::from_static(b"g m consumer range f blob");
        let text = |range| StrBytes::from_utf8(frame.slice(range)).unwrap();
        let request = SyncGroupRequest::default()
            .with_group_id(GroupId(text(0..1)))
            .with_member_id(text(2..3))
            .with_protocol_type(Some(text(4..12)))
            .with_protocol_name(Some(text(13..18)))
            .with_assignments(vec![
                SyncGroupRequestAssignment::default()
                    .with_member_id(text(19..20))
                    .with_assignment(frame.slice(21..25)),
            ]);

        let normalized = SyncRequest::from(&request);

        let within = |ptr: *const u8| frame.as_ptr_range().contains(&ptr);
        let (member_id, assignment) = &normalized.assignments[0];
        let pointers = [
            normalized.group_id.as_ptr(),
            normalized.member_id.as_ptr(),
            normalized.protocol_type.as_ref().unwrap().as_ptr(),
            normalized.protocol_name.as_ref().unwrap().as_ptr(),
            member_id.as_ptr(),
            assignment.as_ptr(),
        ];
        assert!(pointers.into_iter().all(|ptr| !within(ptr)));
    }
}
