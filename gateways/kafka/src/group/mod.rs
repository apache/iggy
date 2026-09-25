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
//! module that awaits: `FindCoordinator`/`JoinGroup`/`Heartbeat`/`LeaveGroup`/`SyncGroup` handlers
//! translate wire messages into the request types here, and `state` holds the synchronous state
//! machine those requests drive.
//!
//! Membership is process memory, not Iggy state. Two gateway instances fronting one Iggy cluster
//! therefore coordinate two independent groups under one name; see `docs/CONSUMER_GROUPS.md`.

mod state;

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{JoinGroupRequest, LeaveGroupRequest, SyncGroupRequest};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{Mutex, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::group::state::{GroupState, Step};
use crate::protocol::api::{ERROR_NONE, ERROR_NOT_COORDINATOR, ERROR_UNKNOWN_MEMBER_ID};

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
        }
    }
}

/// One `SyncGroup` request, normalized across wire versions.
#[derive(Debug, Clone)]
pub struct SyncRequest {
    pub group_id: StrBytes,
    pub generation_id: i32,
    pub member_id: StrBytes,
    /// Present from v5 only; validated against the group when set.
    pub protocol_type: Option<StrBytes>,
    pub protocol_name: Option<StrBytes>,
    pub assignments: Vec<(StrBytes, Bytes)>,
}

impl From<&SyncGroupRequest> for SyncRequest {
    fn from(request: &SyncGroupRequest) -> Self {
        Self {
            group_id: request.group_id.0.clone(),
            generation_id: request.generation_id,
            member_id: request.member_id.clone(),
            protocol_type: request.protocol_type.clone(),
            protocol_name: request.protocol_name.clone(),
            assignments: request
                .assignments
                .iter()
                .map(|assignment| (assignment.member_id.clone(), assignment.assignment.clone()))
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

/// Every consumer group this gateway instance coordinates.
///
/// There is no timer task. A request that touches a group first expires whatever is overdue in
/// it, and a parked `JoinGroup`/`SyncGroup` waiter sleeps until that group's next deadline, so
/// the coroutine waiting on a barrier is also the timer that fires it.
pub struct GroupCoordinator {
    config: GroupCoordinatorConfig,
    groups: Mutex<HashMap<StrBytes, GroupState>>,
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
            shutdown,
        }
    }

    /// Joins `request`'s member, parking until the group's join barrier completes.
    pub async fn join(&self, request: &JoinRequest) -> JoinResult {
        let mut parked: Option<StrBytes> = None;
        loop {
            let outcome = {
                let mut groups = self.groups.lock().await;
                let now = Instant::now();
                let step = match parked.as_ref() {
                    None => state::join_step(&mut groups, &self.config, request, now),
                    Some(member_id) => {
                        state::join_resume_step(&mut groups, &request.group_id, member_id, now)
                    }
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
    pub async fn sync(&self, request: &SyncRequest) -> SyncResult {
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
                        request.generation_id,
                        now,
                    )
                } else {
                    state::sync_step(&mut groups, &self.config, request, now)
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
    ) -> i16 {
        let mut groups = self.groups.lock().await;
        state::heartbeat_step(
            &mut groups,
            group_id,
            generation_id,
            member_id,
            Instant::now(),
        )
    }

    /// Removes `request`'s members and releases whoever is parked on the group. Never parks.
    pub async fn leave(&self, request: &LeaveRequest) -> LeaveResult {
        let mut groups = self.groups.lock().await;
        state::leave_step(&mut groups, request, Instant::now())
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

    use super::*;

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
}
