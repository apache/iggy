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

//! Synchronous state machine behind [`crate::group::GroupCoordinator`].
//!
//! Every entry point takes the whole group map, a config, and `now`, and returns either a
//! response or a deadline to park until. Nothing here allocates a future or touches a clock, so
//! the protocol rules are testable without a runtime and the coordinator's lock is never held
//! across an `.await`.
//!
//! Kafka's `Empty` group state is "absent from the map": offsets live in Iggy, so an empty group
//! holds nothing worth keeping and retaining it would be an unbounded-memory vector.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::protocol::StrBytes;
use tokio::sync::watch;
use tokio::time::Instant;
use uuid::Uuid;

use crate::group::{
    GroupCoordinatorConfig, JoinRequest, JoinResult, JoinedMember, SyncRequest, SyncResult,
};
use crate::protocol::api::{
    ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_GROUP_MAX_SIZE_REACHED, ERROR_ILLEGAL_GENERATION,
    ERROR_INCONSISTENT_GROUP_PROTOCOL, ERROR_INVALID_GROUP_ID, ERROR_INVALID_REQUEST,
    ERROR_INVALID_SESSION_TIMEOUT, ERROR_MEMBER_ID_REQUIRED, ERROR_NONE,
    ERROR_REBALANCE_IN_PROGRESS, ERROR_UNKNOWN_MEMBER_ID,
};

/// An Iggy name caps at 255 bytes and a Kafka group's offset key is `kafka.cg.<group>`, so a
/// group id this gateway admits must leave room for that prefix (`docs/OFFSET_STORAGE.md`).
pub const MAX_GROUP_ID_BYTES: usize = 246;

/// Prefix for a generated member id when the client sent no `group_instance_id`. Kafka uses the
/// header's `client_id`, which handlers do not receive.
const DEFAULT_MEMBER_PREFIX: &str = "member";

/// Floor on how long a parked waiter sleeps. Every deadline a tick leaves behind is strictly in
/// the future, so this only guards against a future rule that forgets to maintain that and turns
/// a park into a spin.
const MIN_PARK: Duration = Duration::from_millis(1);

pub type Groups = HashMap<StrBytes, GroupState>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

/// Outcome of one state-machine step: answer now, or park until `wake_at`.
pub enum Step<T> {
    Respond(T),
    Wait {
        member_id: StrBytes,
        wake_at: Instant,
    },
}

pub struct Member {
    group_instance_id: Option<StrBytes>,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    protocols: Vec<(StrBytes, Bytes)>,
    assignment: Bytes,
    session_deadline: Instant,
    /// Has this member rejoined in the rebalance currently being prepared?
    rejoined: bool,
    /// Has this member sent `SyncGroup` in the current generation?
    synced: bool,
    /// Taken by the member's own parked `JoinGroup` handler. Snapshotting the answer at join
    /// completion means a rebalance that starts before the waiter wakes cannot change what the
    /// member is told about the generation it just completed.
    join_response: Option<JoinResult>,
}

impl Member {
    fn new(request: &JoinRequest, now: Instant) -> Self {
        Self {
            group_instance_id: request.group_instance_id.clone(),
            session_timeout: request.session_timeout,
            rebalance_timeout: request.rebalance_timeout,
            protocols: request.protocols.clone(),
            assignment: Bytes::new(),
            session_deadline: now + request.session_timeout,
            rejoined: true,
            synced: false,
            join_response: None,
        }
    }

    fn rejoin(&mut self, request: &JoinRequest, now: Instant) {
        self.group_instance_id
            .clone_from(&request.group_instance_id);
        self.session_timeout = request.session_timeout;
        self.rebalance_timeout = request.rebalance_timeout;
        self.protocols.clone_from(&request.protocols);
        self.session_deadline = now + request.session_timeout;
        self.rejoined = true;
        // Cleared here rather than when a rebalance opens: a member that has rejoined is asking
        // for the next generation's answer, while one still parked on the previous generation
        // must keep the snapshot it is waiting to collect.
        self.join_response = None;
    }

    fn supports(&self, name: &StrBytes) -> bool {
        self.protocols
            .iter()
            .any(|(candidate, _)| candidate == name)
    }

    /// This member's vote: the first protocol it listed that the whole group can speak.
    fn vote<'a>(&self, candidates: &'a [StrBytes]) -> Option<&'a StrBytes> {
        self.protocols
            .iter()
            .find_map(|(name, _)| candidates.iter().find(|candidate| *candidate == name))
    }

    fn metadata_for(&self, name: &StrBytes) -> Bytes {
        self.protocols
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map_or_else(Bytes::new, |(_, metadata)| metadata.clone())
    }
}

pub struct GroupState {
    phase: Phase,
    /// A group that has completed one rebalance is at generation 1.
    generation_id: i32,
    protocol_type: StrBytes,
    protocol_name: Option<StrBytes>,
    leader: Option<StrBytes>,
    /// Ordered, not hashed: iteration decides leader fallback and protocol tie-breaks.
    members: BTreeMap<StrBytes, Member>,
    /// Ids handed out with `MEMBER_ID_REQUIRED` that have not rejoined yet, and their expiry.
    pending: BTreeMap<StrBytes, Instant>,
    join_deadline: Option<Instant>,
    sync_deadline: Option<Instant>,
    rebalance_started: Instant,
    /// First join of a new group: the barrier waits out the full delay even once every known
    /// member has joined, so a second consumer starting a moment later lands in generation 1.
    initial: bool,
    changed: watch::Sender<u64>,
}

impl GroupState {
    /// A new group's join window does not open here: it opens when the first member is admitted
    /// (`admit`). A `MEMBER_ID_REQUIRED` reply creates the group but adds no member, and a window
    /// opened at that moment would expire while the client was still on its way back with the id
    /// it was just given.
    fn new(protocol_type: StrBytes, now: Instant) -> Self {
        Self {
            phase: Phase::PreparingRebalance,
            generation_id: 0,
            protocol_type,
            protocol_name: None,
            leader: None,
            members: BTreeMap::new(),
            pending: BTreeMap::new(),
            join_deadline: None,
            sync_deadline: None,
            rebalance_started: now,
            initial: true,
            changed: watch::channel(0).0,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// `send_modify`, never `send`: the latter errors once the last receiver is gone, which is
    /// the normal state of a group whose members are all between requests.
    fn bump(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    fn is_empty(&self) -> bool {
        self.members.is_empty() && self.pending.is_empty()
    }

    fn max_rebalance_timeout(&self) -> Duration {
        self.members
            .values()
            .map(|member| member.rebalance_timeout)
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Expire whatever is overdue and complete whichever phase that unblocks.
    ///
    /// Expiry is judged against the recorded deadline, not against when this happens to run, so
    /// a request arriving after its own member's deadline finds that member already gone. That
    /// is stricter than a broker, whose timer thread may not have fired yet, and it is what
    /// makes eviction deterministic here.
    fn tick(&mut self, now: Instant) {
        let mut changed = false;

        let expired_pending: Vec<StrBytes> = self
            .pending
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired_pending {
            self.pending.remove(id);
        }
        changed |= !expired_pending.is_empty();

        let expired: Vec<StrBytes> = self
            .members
            .iter()
            .filter(|(_, member)| member.session_deadline <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired {
            self.remove_member(id);
        }
        if !expired.is_empty() {
            changed = true;
            if !self.members.is_empty() && self.phase != Phase::PreparingRebalance {
                self.prepare_rebalance(now, None);
            }
        }

        if self.phase == Phase::PreparingRebalance {
            self.maybe_complete_join(now);
        }

        if self.phase == Phase::CompletingRebalance
            && self.sync_deadline.is_some_and(|deadline| deadline <= now)
        {
            let unsynced: Vec<StrBytes> = self
                .members
                .iter()
                .filter(|(_, member)| !member.synced)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &unsynced {
                self.remove_member(id);
            }
            self.sync_deadline = None;
            if !self.members.is_empty() {
                self.prepare_rebalance(now, None);
            }
            changed = true;
        }

        if changed {
            self.bump();
        }
    }

    fn remove_member(&mut self, member_id: &StrBytes) {
        self.members.remove(member_id);
        if self.leader.as_ref() == Some(member_id) {
            self.leader = self.members.keys().next().cloned();
        }
    }

    /// The earliest moment any rule in this group could fire.
    fn next_deadline(&self) -> Option<Instant> {
        let phase_deadline = match self.phase {
            Phase::PreparingRebalance => self.join_deadline,
            Phase::CompletingRebalance => self.sync_deadline,
            Phase::Stable => None,
        };
        phase_deadline
            .into_iter()
            .chain(self.members.values().map(|member| member.session_deadline))
            .chain(self.pending.values().copied())
            .min()
    }

    fn wake_at(&self, now: Instant) -> Instant {
        let floor = now + MIN_PARK;
        self.next_deadline().unwrap_or(floor).max(floor)
    }

    fn prepare_rebalance(&mut self, now: Instant, trigger: Option<&StrBytes>) {
        self.phase = Phase::PreparingRebalance;
        self.initial = false;
        self.rebalance_started = now;
        self.sync_deadline = None;
        self.join_deadline = Some(now + self.max_rebalance_timeout());
        for (member_id, member) in &mut self.members {
            member.rejoined = trigger == Some(member_id);
        }
        self.bump();
    }

    fn maybe_complete_join(&mut self, now: Instant) {
        let all_joined = !self.initial
            && self.pending.is_empty()
            && !self.members.is_empty()
            && self.members.values().all(|member| member.rejoined);
        if all_joined || self.join_deadline.is_some_and(|deadline| now >= deadline) {
            self.complete_join(now);
        }
    }

    fn complete_join(&mut self, now: Instant) {
        self.members.retain(|_, member| member.rejoined);
        self.pending.clear();
        self.join_deadline = None;
        self.initial = false;
        if self.members.is_empty() {
            self.leader = None;
            self.bump();
            return;
        }

        let leader = match self.leader.clone() {
            Some(leader) if self.members.contains_key(&leader) => leader,
            _ => self.members.keys().next().cloned().unwrap_or_default(),
        };
        let protocol = self.select_protocol(&leader);
        self.leader = Some(leader.clone());
        self.protocol_name.clone_from(&protocol);
        self.generation_id = self.generation_id.wrapping_add(1);
        self.phase = Phase::CompletingRebalance;
        self.sync_deadline = Some(now + self.max_rebalance_timeout());

        let selected = protocol.unwrap_or_default();
        let roster: Vec<JoinedMember> = self
            .members
            .iter()
            .map(|(member_id, member)| JoinedMember {
                member_id: member_id.clone(),
                group_instance_id: member.group_instance_id.clone(),
                metadata: member.metadata_for(&selected),
            })
            .collect();

        let generation_id = self.generation_id;
        let protocol_type = self.protocol_type.clone();
        for (member_id, member) in &mut self.members {
            member.session_deadline = now + member.session_timeout;
            member.synced = false;
            member.rejoined = false;
            member.assignment = Bytes::new();
            member.join_response = Some(JoinResult {
                error: ERROR_NONE,
                generation_id,
                protocol_type: Some(protocol_type.clone()),
                protocol_name: Some(selected.clone()),
                leader: leader.clone(),
                member_id: member_id.clone(),
                members: if *member_id == leader {
                    roster.clone()
                } else {
                    Vec::new()
                },
            });
        }
        self.bump();
    }

    /// The protocol every member speaks, most first-preference votes winning.
    ///
    /// Candidates are walked in the leader's own listed order, which breaks a tie deterministically
    /// where Kafka breaks it by set iteration order.
    fn select_protocol(&self, leader_id: &StrBytes) -> Option<StrBytes> {
        let leader = self.members.get(leader_id)?;
        let candidates: Vec<StrBytes> = leader
            .protocols
            .iter()
            .map(|(name, _)| name.clone())
            .filter(|name| self.members.values().all(|member| member.supports(name)))
            .collect();

        let mut best: Option<(StrBytes, usize)> = None;
        for name in &candidates {
            let votes = self
                .members
                .values()
                .filter(|member| member.vote(&candidates) == Some(name))
                .count();
            if best.as_ref().is_none_or(|(_, most)| votes > *most) {
                best = Some((name.clone(), votes));
            }
        }
        best.map(|(name, _)| name)
    }

    /// Can `protocols` still leave one name every other member speaks?
    fn protocols_compatible(&self, member_id: &StrBytes, protocols: &[(StrBytes, Bytes)]) -> bool {
        protocols.iter().any(|(name, _)| {
            self.members
                .iter()
                .all(|(id, member)| id == member_id || member.supports(name))
        })
    }

    fn current_generation_result(&self, member_id: &StrBytes) -> JoinResult {
        // Only the leader runs an assignor, so only the leader is given the roster. Handing it an
        // empty one would have it assign nothing to everybody and leave the group consuming no
        // partitions, silently.
        let members = if self.leader.as_deref() == Some(member_id.as_ref()) {
            self.roster()
        } else {
            Vec::new()
        };
        JoinResult {
            error: ERROR_NONE,
            generation_id: self.generation_id,
            protocol_type: Some(self.protocol_type.clone()),
            protocol_name: self.protocol_name.clone(),
            leader: self.leader.clone().unwrap_or_default(),
            member_id: member_id.clone(),
            members,
        }
    }

    /// The member list an assignor needs, in the group's selected protocol.
    fn roster(&self) -> Vec<JoinedMember> {
        let selected = self.protocol_name.clone().unwrap_or_default();
        self.members
            .iter()
            .map(|(member_id, member)| JoinedMember {
                member_id: member_id.clone(),
                group_instance_id: member.group_instance_id.clone(),
                metadata: member.metadata_for(&selected),
            })
            .collect()
    }

    fn sync_result(&self, member_id: &StrBytes) -> SyncResult {
        SyncResult {
            error: ERROR_NONE,
            protocol_type: Some(self.protocol_type.clone()),
            protocol_name: self.protocol_name.clone(),
            assignment: self
                .members
                .get(member_id)
                .map_or_else(Bytes::new, |member| member.assignment.clone()),
        }
    }

    /// Fan the leader's blobs out to every member. A member the leader left out gets empty bytes,
    /// which is what a broker stores for it too.
    fn apply_assignments(&mut self, assignments: &[(StrBytes, Bytes)], now: Instant) {
        for (member_id, member) in &mut self.members {
            member.assignment = assignments
                .iter()
                .find(|(target, _)| target == member_id)
                .map_or_else(Bytes::new, |(_, blob)| blob.clone());
            member.session_deadline = now + member.session_timeout;
        }
        if let Some(leader) = self.leader.clone()
            && let Some(member) = self.members.get_mut(&leader)
        {
            member.synced = true;
        }
        self.phase = Phase::Stable;
        self.sync_deadline = None;
        self.bump();
    }
}

/// Tick every group and drop the ones that emptied, returning how many were reclaimed.
///
/// A group is only ever ticked by a request naming it, so a group whose consumers all died is
/// never reclaimed on its own: it holds its slot against `max_groups` and its members against
/// `max_total_members` forever. Clients that mint a fresh group id per run, which
/// `kafka-console-consumer` does, then walk the gateway into permanent rejection. This runs only
/// where a cap is about to reject, so the cost lands on the path that would otherwise wedge and
/// never on the hot path.
fn reclaim_expired(groups: &mut Groups, now: Instant, except: &StrBytes) -> usize {
    let before = groups.len();
    // `except` is the group the caller is mid-way through admitting. It is legitimately empty
    // until its first member lands, so sweeping it here would delete the group out from under
    // the request that just created it.
    groups.retain(|id, group| {
        if id == except {
            return true;
        }
        group.tick(now);
        !group.is_empty()
    });
    before - groups.len()
}

/// Expire what is overdue in `group_id`. `false` means the group is gone: either it never
/// existed, or ticking emptied it.
fn tick_group(groups: &mut Groups, group_id: &StrBytes, now: Instant) -> bool {
    let Some(group) = groups.get_mut(group_id) else {
        return false;
    };
    group.tick(now);
    if group.is_empty() {
        groups.remove(group_id);
        return false;
    }
    true
}

/// Everything a `JoinGroup` can be rejected for before any group is touched.
fn join_request_error(config: &GroupCoordinatorConfig, request: &JoinRequest) -> Option<i16> {
    if request.group_id.is_empty() || request.group_id.len() > MAX_GROUP_ID_BYTES {
        return Some(ERROR_INVALID_GROUP_ID);
    }
    if request.session_timeout < config.min_session_timeout
        || request.session_timeout > config.max_session_timeout
    {
        return Some(ERROR_INVALID_SESSION_TIMEOUT);
    }
    if request.protocol_type.is_empty() || request.protocols.is_empty() {
        return Some(ERROR_INCONSISTENT_GROUP_PROTOCOL);
    }
    let metadata_bytes: usize = request
        .protocols
        .iter()
        .map(|(_, metadata)| metadata.len())
        .sum();
    if metadata_bytes > config.max_member_blob_bytes {
        return Some(ERROR_INVALID_REQUEST);
    }
    None
}

/// Make sure `request.group_id` exists and is ticked, returning whether this call created it.
///
/// Reclamation runs only when the group cap is about to reject: walking every group on every
/// request would multiply the rebalance wakeup storm by the size of the whole map, while a group
/// that emptied only matters at the moment its slot is needed.
fn ensure_group(
    groups: &mut Groups,
    config: &GroupCoordinatorConfig,
    request: &JoinRequest,
    now: Instant,
) -> Result<bool, i16> {
    if groups.contains_key(&request.group_id) && tick_group(groups, &request.group_id, now) {
        return Ok(false);
    }
    if !request.member_id.is_empty() {
        return Err(ERROR_UNKNOWN_MEMBER_ID);
    }
    if groups.len() >= config.max_groups {
        reclaim_expired(groups, now, &request.group_id);
    }
    if groups.len() >= config.max_groups {
        tracing::warn!(
            max_groups = config.max_groups,
            "consumer group limit reached; rejecting JoinGroup"
        );
        return Err(ERROR_COORDINATOR_NOT_AVAILABLE);
    }
    groups.insert(
        request.group_id.clone(),
        GroupState::new(request.protocol_type.clone(), now),
    );
    Ok(true)
}

pub fn join_step(
    groups: &mut Groups,
    config: &GroupCoordinatorConfig,
    request: &JoinRequest,
    now: Instant,
) -> Step<JoinResult> {
    let reject = |error: i16| Step::Respond(JoinResult::error(error, request.member_id.clone()));
    // A group this call created must not outlive a rejection further down: the checks below run
    // after the insert, and a group left behind holds its `max_groups` slot with no member to
    // ever expire and release it.
    let reject_created = |groups: &mut Groups, created: bool, error: i16| {
        if created {
            groups.remove(&request.group_id);
        }
        Step::Respond(JoinResult::error(error, request.member_id.clone()))
    };

    if let Some(error) = join_request_error(config, request) {
        return reject(error);
    }

    let created = match ensure_group(groups, config, request, now) {
        Ok(fresh) => fresh,
        Err(error) => return reject(error),
    };

    if request.member_id.is_empty()
        && groups
            .values()
            .map(|group| group.members.len() + group.pending.len())
            .sum::<usize>()
            >= config.max_total_members
    {
        reclaim_expired(groups, now, &request.group_id);
    }
    let at_capacity = request.member_id.is_empty()
        && groups
            .values()
            .map(|group| group.members.len() + group.pending.len())
            .sum::<usize>()
            >= config.max_total_members;

    let Some(group) = groups.get_mut(&request.group_id) else {
        return reject(ERROR_UNKNOWN_MEMBER_ID);
    };

    if group.protocol_type != request.protocol_type
        || !group.protocols_compatible(&request.member_id, &request.protocols)
    {
        return reject_created(groups, created, ERROR_INCONSISTENT_GROUP_PROTOCOL);
    }

    if request.member_id.is_empty() {
        // Decided before touching `group` again so the borrow ends before the cleanup below.
        let capacity_error =
            if group.members.len() + group.pending.len() >= config.max_members_per_group {
                Some(ERROR_GROUP_MAX_SIZE_REACHED)
            } else if at_capacity {
                tracing::warn!(
                    max_total_members = config.max_total_members,
                    "consumer group member limit reached; rejecting JoinGroup"
                );
                Some(ERROR_COORDINATOR_NOT_AVAILABLE)
            } else {
                None
            };
        if let Some(error) = capacity_error {
            return reject_created(groups, created, error);
        }
        let Some(group) = groups.get_mut(&request.group_id) else {
            return reject(ERROR_UNKNOWN_MEMBER_ID);
        };
        let member_id = generate_member_id(request.group_instance_id.as_ref());
        if request.require_known_member_id {
            group
                .pending
                .insert(member_id.clone(), now + request.session_timeout);
            group.bump();
            return Step::Respond(JoinResult::error(ERROR_MEMBER_ID_REQUIRED, member_id));
        }
        return admit(group, config, &member_id, request, now);
    }

    if group.pending.contains_key(&request.member_id) {
        return admit(group, config, &request.member_id, request, now);
    }
    let Some(member) = group.members.get(&request.member_id) else {
        return reject(ERROR_UNKNOWN_MEMBER_ID);
    };
    let protocols_changed = member.protocols != request.protocols;
    let is_leader = group.leader.as_ref() == Some(&request.member_id);

    rejoin_step(group, request, is_leader, protocols_changed, now)
}

/// Dispatch for a member the group already knows, by phase.
fn rejoin_step(
    group: &mut GroupState,
    request: &JoinRequest,
    is_leader: bool,
    protocols_changed: bool,
    now: Instant,
) -> Step<JoinResult> {
    match group.phase {
        Phase::PreparingRebalance => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.rejoin(request, now);
            }
            group.bump();
            group.maybe_complete_join(now);
            park_or_respond(group, &request.member_id, now)
        }
        Phase::Stable if is_leader || protocols_changed => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.rejoin(request, now);
            }
            group.prepare_rebalance(now, Some(&request.member_id));
            group.maybe_complete_join(now);
            park_or_respond(group, &request.member_id, now)
        }
        Phase::CompletingRebalance if protocols_changed => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.rejoin(request, now);
            }
            group.prepare_rebalance(now, Some(&request.member_id));
            group.maybe_complete_join(now);
            park_or_respond(group, &request.member_id, now)
        }
        Phase::Stable | Phase::CompletingRebalance => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.session_timeout = request.session_timeout;
                member.session_deadline = now + request.session_timeout;
                // Answering without consuming the snapshot would leave an uncollected one on a
                // member that is not parked, which a later rejoin would serve as a stale answer.
                member.join_response = None;
            }
            Step::Respond(group.current_generation_result(&request.member_id))
        }
    }
}

/// Keep a member parked on a barrier alive across the session sweep.
///
/// A parked member is inside its own `JoinGroup` or `SyncGroup` call and cannot send a
/// heartbeat, so
/// the sweep would evict the member that did the right thing while one that never rejoined
/// survives on its heartbeats. Kafka exempts them outright
/// (`MemberMetadata.hasSatisfiedHeartbeat`: `isAwaitingJoin || isAwaitingSync`). Refreshing
/// rather than exempting keeps the member's own session deadline chained in `next_deadline`,
/// which is what stops a park from pinning a connection for the whole rebalance window.
fn refresh_parked_session(
    groups: &mut Groups,
    group_id: &StrBytes,
    member_id: &StrBytes,
    now: Instant,
) {
    if let Some(group) = groups.get_mut(group_id)
        && let Some(member) = group.members.get_mut(member_id)
    {
        member.session_deadline = now + member.session_timeout;
    }
}

pub fn join_resume_step(
    groups: &mut Groups,
    group_id: &StrBytes,
    member_id: &StrBytes,
    now: Instant,
) -> Step<JoinResult> {
    refresh_parked_session(groups, group_id, member_id, now);
    if !tick_group(groups, group_id, now) {
        return Step::Respond(JoinResult::error(
            ERROR_UNKNOWN_MEMBER_ID,
            member_id.clone(),
        ));
    }
    let Some(group) = groups.get_mut(group_id) else {
        return Step::Respond(JoinResult::error(
            ERROR_UNKNOWN_MEMBER_ID,
            member_id.clone(),
        ));
    };
    park_or_respond(group, member_id, now)
}

pub fn heartbeat_step(
    groups: &mut Groups,
    group_id: &StrBytes,
    generation_id: i32,
    member_id: &StrBytes,
    now: Instant,
) -> i16 {
    if !tick_group(groups, group_id, now) {
        return ERROR_UNKNOWN_MEMBER_ID;
    }
    let Some(group) = groups.get_mut(group_id) else {
        return ERROR_UNKNOWN_MEMBER_ID;
    };
    if !group.members.contains_key(member_id) {
        return ERROR_UNKNOWN_MEMBER_ID;
    }
    if generation_id != group.generation_id {
        return ERROR_ILLEGAL_GENERATION;
    }
    if let Some(member) = group.members.get_mut(member_id) {
        member.session_deadline = now + member.session_timeout;
    }
    if group.phase == Phase::PreparingRebalance {
        ERROR_REBALANCE_IN_PROGRESS
    } else {
        ERROR_NONE
    }
}

pub fn sync_step(
    groups: &mut Groups,
    config: &GroupCoordinatorConfig,
    request: &SyncRequest,
    now: Instant,
) -> Step<SyncResult> {
    if !tick_group(groups, &request.group_id, now) {
        return Step::Respond(SyncResult::error(ERROR_UNKNOWN_MEMBER_ID));
    }
    let oversized = request
        .assignments
        .iter()
        .any(|(_, blob)| blob.len() > config.max_member_blob_bytes);

    let Some(group) = groups.get_mut(&request.group_id) else {
        return Step::Respond(SyncResult::error(ERROR_UNKNOWN_MEMBER_ID));
    };
    if !group.members.contains_key(&request.member_id) {
        return Step::Respond(SyncResult::error(ERROR_UNKNOWN_MEMBER_ID));
    }
    if request.generation_id != group.generation_id {
        return Step::Respond(SyncResult::error(ERROR_ILLEGAL_GENERATION));
    }
    let protocol_mismatch = request
        .protocol_type
        .as_ref()
        .is_some_and(|protocol_type| *protocol_type != group.protocol_type)
        || request
            .protocol_name
            .as_ref()
            .is_some_and(|name| Some(name) != group.protocol_name.as_ref());
    if protocol_mismatch {
        return Step::Respond(SyncResult::error(ERROR_INCONSISTENT_GROUP_PROTOCOL));
    }
    if oversized {
        return Step::Respond(SyncResult::error(ERROR_INVALID_REQUEST));
    }

    match group.phase {
        Phase::PreparingRebalance => Step::Respond(SyncResult::error(ERROR_REBALANCE_IN_PROGRESS)),
        Phase::Stable => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.session_deadline = now + member.session_timeout;
            }
            Step::Respond(group.sync_result(&request.member_id))
        }
        Phase::CompletingRebalance => {
            if group.leader.as_ref() == Some(&request.member_id) {
                group.apply_assignments(&request.assignments, now);
                return Step::Respond(group.sync_result(&request.member_id));
            }
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.synced = true;
                member.session_deadline = now + member.session_timeout;
            }
            Step::Wait {
                member_id: request.member_id.clone(),
                wake_at: group.wake_at(now),
            }
        }
    }
}

pub fn sync_resume_step(
    groups: &mut Groups,
    group_id: &StrBytes,
    member_id: &StrBytes,
    generation_id: i32,
    now: Instant,
) -> Step<SyncResult> {
    refresh_parked_session(groups, group_id, member_id, now);
    if !tick_group(groups, group_id, now) {
        return Step::Respond(SyncResult::error(ERROR_UNKNOWN_MEMBER_ID));
    }
    let Some(group) = groups.get_mut(group_id) else {
        return Step::Respond(SyncResult::error(ERROR_UNKNOWN_MEMBER_ID));
    };
    if !group.members.contains_key(member_id) {
        return Step::Respond(SyncResult::error(ERROR_UNKNOWN_MEMBER_ID));
    }
    if group.generation_id != generation_id || group.phase == Phase::PreparingRebalance {
        return Step::Respond(SyncResult::error(ERROR_REBALANCE_IN_PROGRESS));
    }
    if group.phase == Phase::Stable {
        return Step::Respond(group.sync_result(member_id));
    }
    Step::Wait {
        member_id: member_id.clone(),
        wake_at: group.wake_at(now),
    }
}

fn admit(
    group: &mut GroupState,
    config: &GroupCoordinatorConfig,
    member_id: &StrBytes,
    request: &JoinRequest,
    now: Instant,
) -> Step<JoinResult> {
    group.pending.remove(member_id);
    group
        .members
        .insert(member_id.clone(), Member::new(request, now));
    if group.leader.is_none() {
        group.leader = Some(member_id.clone());
    }
    if group.phase == Phase::PreparingRebalance {
        if group.initial {
            if group.join_deadline.is_none() {
                group.rebalance_started = now;
            }
            let cap = group.rebalance_started + group.max_rebalance_timeout();
            group.join_deadline = Some((now + config.initial_rebalance_delay).min(cap));
        }
        group.bump();
    } else {
        group.prepare_rebalance(now, Some(member_id));
    }
    group.maybe_complete_join(now);
    park_or_respond(group, member_id, now)
}

fn park_or_respond(group: &mut GroupState, member_id: &StrBytes, now: Instant) -> Step<JoinResult> {
    let Some(member) = group.members.get_mut(member_id) else {
        return Step::Respond(JoinResult::error(
            ERROR_UNKNOWN_MEMBER_ID,
            member_id.clone(),
        ));
    };
    if let Some(result) = member.join_response.take() {
        return Step::Respond(result);
    }
    Step::Wait {
        member_id: member_id.clone(),
        wake_at: group.wake_at(now),
    }
}

/// Kafka shapes a member id as `clientId-UUID`. Handlers never see the header's `client_id`, so
/// the static-membership id stands in for it when present. The UUID matters on its own: a
/// counter would repeat across a gateway restart and turn a stale client's `UNKNOWN_MEMBER_ID`
/// into a silent identity theft.
fn generate_member_id(group_instance_id: Option<&StrBytes>) -> StrBytes {
    let prefix = group_instance_id.map_or(DEFAULT_MEMBER_PREFIX, StrBytes::as_str);
    StrBytes::from_string(format!("{prefix}-{}", Uuid::new_v4()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GROUP: &str = "g";

    fn config() -> GroupCoordinatorConfig {
        GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::ZERO,
            ..GroupCoordinatorConfig::default()
        }
    }

    fn group_id() -> StrBytes {
        StrBytes::from_static_str(GROUP)
    }

    fn request_for(group: &str, member_id: &str) -> JoinRequest {
        JoinRequest {
            group_id: StrBytes::from_string(group.to_owned()),
            ..request(member_id, &["range"])
        }
    }

    /// The cap must bound how many groups are LIVE, not how many have ever existed. Asserting
    /// that a full coordinator rejects passes with the bug present, because the bug is that the
    /// slot is never given back: only admitting a later group proves reclamation happened.
    #[test]
    fn given_a_group_whose_members_all_expired_when_a_new_group_joins_should_reclaim_the_slot() {
        let config = GroupCoordinatorConfig {
            max_groups: 1,
            ..config()
        };
        let mut groups = Groups::new();
        let start = Instant::now();

        let first = join_step(&mut groups, &config, &request_for("dead", ""), start);
        assert!(
            matches!(first, Step::Respond(_)),
            "first group must be admitted"
        );
        assert_eq!(groups.len(), 1);

        // Every member's session lapses. Nothing names this group again, which is exactly the
        // case a per-request tick cannot see.
        let later = start + Duration::from_secs(3600);
        let second = join_step(&mut groups, &config, &request_for("fresh", ""), later);

        let Step::Respond(result) = second else {
            panic!("a join that reclaims a dead group must answer, not park");
        };
        assert_eq!(
            result.error, ERROR_NONE,
            "a group left behind by dead consumers must not hold its slot forever"
        );
        assert!(
            !groups.contains_key(&StrBytes::from_static_str("dead")),
            "the emptied group must be gone, not merely ignored"
        );
    }

    /// A rejection must not leave the group it created behind: that group has no member whose
    /// expiry could ever release it, so it holds a `max_groups` slot permanently.
    #[test]
    fn given_a_rejected_join_when_it_created_the_group_should_not_leave_it_behind() {
        let config = GroupCoordinatorConfig {
            max_total_members: 0,
            ..config()
        };
        let mut groups = Groups::new();

        let outcome = join_step(
            &mut groups,
            &config,
            &request_for("ghost", ""),
            Instant::now(),
        );

        let Step::Respond(result) = outcome else {
            panic!("a capacity rejection must answer, not park");
        };
        assert_eq!(
            result.error, ERROR_COORDINATOR_NOT_AVAILABLE,
            "got error {}",
            result.error
        );
        assert!(
            groups.is_empty(),
            "a rejected join must not leave a group that nothing can ever reclaim"
        );
    }

    fn request(member_id: &str, protocols: &[&str]) -> JoinRequest {
        request_with(member_id, protocols, 10, 5)
    }

    fn request_with(
        member_id: &str,
        protocols: &[&str],
        session_secs: u64,
        rebalance_secs: u64,
    ) -> JoinRequest {
        JoinRequest {
            group_id: group_id(),
            session_timeout: Duration::from_secs(session_secs),
            rebalance_timeout: Duration::from_secs(rebalance_secs),
            member_id: StrBytes::from_string(member_id.to_owned()),
            group_instance_id: None,
            protocol_type: StrBytes::from_static_str("consumer"),
            protocols: protocols
                .iter()
                .map(|name| {
                    (
                        StrBytes::from_string((*name).to_owned()),
                        Bytes::from_static(b"m"),
                    )
                })
                .collect(),
            require_known_member_id: false,
        }
    }

    fn member_id_of(step: &Step<JoinResult>) -> StrBytes {
        match step {
            Step::Respond(result) => result.member_id.clone(),
            Step::Wait { member_id, .. } => member_id.clone(),
        }
    }

    fn error_of(step: &Step<JoinResult>) -> i16 {
        match step {
            Step::Respond(result) => result.error,
            Step::Wait { .. } => panic!("expected a response, not a park"),
        }
    }

    /// Two members whose protocol preferences disagree, at generation 2 with `first` leading.
    fn two_members(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        first: &[&str],
        second: &[&str],
        now: Instant,
    ) -> (StrBytes, StrBytes) {
        let leader = member_id_of(&join_step(groups, config, &request("", first), now));
        let follower = member_id_of(&join_step(groups, config, &request("", second), now));
        let _ = join_step(groups, config, &request(leader.as_str(), first), now);
        (leader, follower)
    }

    /// A member parked on a barrier cannot heartbeat, so the session sweep must not treat its
    /// silence as death. The second assertion is the one that matters: exempting parked members
    /// from `next_deadline` would also make the first pass, while removing the only bound on how
    /// long a park can hold a connection.
    #[test]
    fn given_a_parked_join_waiter_when_its_own_session_passes_should_stay_a_member() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        // Kafka's own default shape: the session is far shorter than the rebalance window.
        let leader = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 10, 300),
            now,
        ));
        let _follower = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 10, 300),
            now,
        ));

        // The leader is parked, waiting for the barrier. Its session lapses while it waits.
        let woken = now + Duration::from_secs(11);
        let step = join_resume_step(&mut groups, &group_id(), &leader, woken);

        assert!(
            groups[&group_id()].members.contains_key(&leader),
            "a member parked on the barrier must not be evicted for not heartbeating"
        );
        assert!(
            matches!(step, Step::Wait { .. }),
            "the barrier has not completed, so the refreshed member re-parks rather than being \
             told it is unknown"
        );
        let group = &groups[&group_id()];
        assert!(
            group.wake_at(woken) <= woken + Duration::from_secs(10),
            "the park must stay bounded by the session timeout, or it pins a connection for the \
             whole rebalance window"
        );
    }

    /// The inverse of the obvious test: the snapshot must SURVIVE a rebalance that opens before
    /// its waiter collects it. Pinning the clear instead would cement the bug, because the member
    /// is inside its own `JoinGroup` call and can never rejoin to get a fresh one.
    #[test]
    fn given_a_parked_waiter_when_a_rebalance_opens_should_keep_its_pending_answer() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);

        assert!(
            groups[&group_id()].members[&follower]
                .join_response
                .is_some(),
            "the completed join must leave the follower an answer to collect"
        );

        // The leader rejoins with changed protocols, opening a new rebalance while the follower
        // has not yet woken to take its answer.
        let _ = join_step(
            &mut groups,
            &config,
            &request(leader.as_str(), &["x", "y"]),
            now,
        );

        assert!(
            groups[&group_id()].members[&follower]
                .join_response
                .is_some(),
            "a rebalance must not revoke an answer its waiter has not collected yet"
        );
    }

    /// Kafka replays the generation for an unchanged leader in `CompletingRebalance` rather than
    /// starting another rebalance, and hands it the roster. An empty roster would have the leader
    /// assign nothing to anyone and the group consume no partitions, silently.
    #[test]
    fn given_a_leader_retry_while_completing_should_replay_the_generation_with_the_roster() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _follower) = two_members(&mut groups, &config, &["x"], &["x"], now);

        let generation = groups[&group_id()].generation_id;
        assert_eq!(groups[&group_id()].phase, Phase::CompletingRebalance);

        let step = join_step(&mut groups, &config, &request(leader.as_str(), &["x"]), now);

        let Step::Respond(result) = step else {
            panic!("an unchanged leader retry must be answered, not parked");
        };
        assert_eq!(result.error, ERROR_NONE);
        assert_eq!(
            result.generation_id, generation,
            "an unchanged leader retry replays its generation rather than opening a new one"
        );
        assert_eq!(
            result.members.len(),
            2,
            "the leader runs the assignor, so its replay must carry the roster"
        );
    }

    /// A follower replaying a stable generation gets the current answer, not a member reset.
    #[test]
    fn given_a_follower_rejoin_while_stable_should_return_the_current_generation() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;

        let step = join_step(
            &mut groups,
            &config,
            &request(follower.as_str(), &["x"]),
            now,
        );

        let Step::Respond(result) = step else {
            panic!("an unchanged follower rejoin must be answered, not parked");
        };
        assert_eq!(result.error, ERROR_NONE);
        assert_eq!(result.generation_id, generation);
        assert_eq!(result.leader, leader);
        assert!(
            result.members.is_empty(),
            "a follower runs no assignor and must not receive the roster"
        );
    }

    #[test]
    fn given_a_protocol_vote_majority_when_the_join_completes_should_select_that_protocol() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let leader = member_id_of(&join_step(
            &mut groups,
            &config,
            &request("", &["x", "y"]),
            now,
        ));
        let _ = join_step(&mut groups, &config, &request("", &["y", "x"]), now);
        let _ = join_step(&mut groups, &config, &request("", &["y", "x"]), now);
        let _ = join_step(
            &mut groups,
            &config,
            &request(leader.as_str(), &["x", "y"]),
            now,
        );

        let group = &groups[&group_id()];
        assert_eq!(group.members.len(), 3);
        assert_eq!(group.generation_id, 2);
        assert_eq!(
            group.protocol_name.as_ref().map(StrBytes::as_str),
            Some("y"),
            "two of three members voted for y, so the leader's own first choice must lose"
        );
    }

    #[test]
    fn given_a_tied_protocol_vote_when_the_join_completes_should_break_it_by_leader_order() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let (_, _) = two_members(&mut groups, &config, &["x", "y"], &["y", "x"], now);

        assert_eq!(
            groups[&group_id()]
                .protocol_name
                .as_ref()
                .map(StrBytes::as_str),
            Some("x"),
            "one vote each: the leader's list order decides"
        );
    }

    #[test]
    fn given_no_shared_protocol_when_joining_should_return_inconsistent_group_protocol() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let _ = join_step(&mut groups, &config, &request("", &["x"]), now);
        let step = join_step(&mut groups, &config, &request("", &["z"]), now);

        assert_eq!(error_of(&step), ERROR_INCONSISTENT_GROUP_PROTOCOL);
    }

    #[test]
    fn given_a_silent_member_when_the_join_deadline_passes_should_drop_it_and_move_the_leader() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        // Session timeouts far beyond the rebalance window, so what drops the silent member is
        // the join barrier and not its session expiring first.
        let leader = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 600, 5),
            now,
        ));
        let follower = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 600, 5),
            now,
        ));
        let _ = join_step(
            &mut groups,
            &config,
            &request_with(leader.as_str(), &["x"], 600, 5),
            now,
        );
        assert_eq!(groups[&group_id()].generation_id, 2);

        // The follower rejoins with different protocol metadata, which forces a new rebalance.
        let _ = join_step(
            &mut groups,
            &config,
            &request_with(follower.as_str(), &["x", "y"], 600, 5),
            now,
        );
        let step = join_resume_step(
            &mut groups,
            &group_id(),
            &follower,
            now + Duration::from_secs(5),
        );

        assert_eq!(error_of(&step), ERROR_NONE);
        let group = &groups[&group_id()];
        assert_eq!(group.generation_id, 3);
        assert_eq!(group.members.len(), 1);
        assert!(group.members.contains_key(&follower));
        assert_eq!(group.leader.as_ref(), Some(&follower));
    }

    #[test]
    fn given_an_expired_session_when_a_request_ticks_should_evict_and_start_a_rebalance() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let leader = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 600, 5),
            now,
        ));
        let follower = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 10, 5),
            now,
        ));
        let _ = join_step(
            &mut groups,
            &config,
            &request_with(leader.as_str(), &["x"], 600, 5),
            now,
        );

        let error = heartbeat_step(
            &mut groups,
            &group_id(),
            2,
            &leader,
            now + Duration::from_secs(11),
        );

        assert_eq!(error, ERROR_REBALANCE_IN_PROGRESS);
        let group = &groups[&group_id()];
        assert_eq!(group.members.len(), 1);
        assert!(!group.members.contains_key(&follower));
        assert_eq!(group.phase, Phase::PreparingRebalance);
    }

    #[test]
    fn given_the_last_member_expired_when_a_request_ticks_should_remove_the_group() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let member = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));
        let error = heartbeat_step(
            &mut groups,
            &group_id(),
            1,
            &member,
            now + Duration::from_secs(11),
        );

        assert_eq!(error, ERROR_UNKNOWN_MEMBER_ID);
        assert!(groups.is_empty());
    }

    #[test]
    fn given_an_unclaimed_member_id_when_it_expires_should_remove_the_group() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let mut pending = request("", &["x"]);
        pending.require_known_member_id = true;
        let step = join_step(&mut groups, &config, &pending, now);

        assert_eq!(error_of(&step), ERROR_MEMBER_ID_REQUIRED);
        assert!(!member_id_of(&step).is_empty());
        assert_eq!(groups[&group_id()].pending.len(), 1);
        assert!(groups[&group_id()].members.is_empty());

        let member = member_id_of(&step);
        let error = heartbeat_step(
            &mut groups,
            &group_id(),
            1,
            &member,
            now + Duration::from_secs(11),
        );
        assert_eq!(error, ERROR_UNKNOWN_MEMBER_ID);
        assert!(groups.is_empty());
    }

    #[test]
    fn given_deadlines_from_every_source_when_computing_the_next_one_should_pick_the_earliest() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let leader = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 600, 5),
            now,
        ));
        let _ = join_step(&mut groups, &config, &request_with("", &["x"], 30, 5), now);
        let _ = join_step(
            &mut groups,
            &config,
            &request_with(leader.as_str(), &["x"], 600, 5),
            now,
        );

        // Completing a join sets a sync deadline one rebalance timeout out, earlier than either
        // member's session.
        assert_eq!(
            groups[&group_id()].next_deadline(),
            Some(now + Duration::from_secs(5))
        );
    }

    #[test]
    fn given_an_unknown_group_when_a_member_id_is_sent_should_return_unknown_member_id() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let step = join_step(&mut groups, &config, &request("stale-member", &["x"]), now);

        assert_eq!(error_of(&step), ERROR_UNKNOWN_MEMBER_ID);
        assert!(groups.is_empty(), "a rejected join must not create a group");
    }
}
