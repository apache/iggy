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
//! A group with no joined members is reported as Kafka's `Empty`. One with neither members nor
//! pending member ids is dropped: offsets live in Iggy, so nothing here is worth keeping.
//! TODO(#3542): keep an offsets-only group and report `Empty` once those offsets are stored here.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::protocol::StrBytes;
use tokio::sync::watch;
use tokio::time::Instant;
use uuid::Uuid;

use crate::group::{
    CommitRequest, DESCRIBE_RESPONSE_TOO_LARGE, GROUP_STATE_COMPLETING_REBALANCE,
    GROUP_STATE_EMPTY, GROUP_STATE_PREPARING_REBALANCE, GROUP_STATE_STABLE, GroupCoordinatorConfig,
    GroupDescription, GroupListing, GroupMember, JoinRequest, JoinResult, JoinedMember,
    LeaveRequest, LeaveResult, LeavingMember, LeftMember, MemberDescription, SyncRequest,
    SyncResult, owned_str,
};
use crate::protocol::api::{
    API_KEY_DESCRIBE_GROUPS, ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_FENCED_INSTANCE_ID,
    ERROR_GROUP_MAX_SIZE_REACHED, ERROR_ILLEGAL_GENERATION, ERROR_INCONSISTENT_GROUP_PROTOCOL,
    ERROR_INVALID_GROUP_ID, ERROR_INVALID_REQUEST, ERROR_INVALID_SESSION_TIMEOUT,
    ERROR_MEMBER_ID_REQUIRED, ERROR_NONE, ERROR_REBALANCE_IN_PROGRESS, ERROR_UNKNOWN_MEMBER_ID,
    ERROR_UNKNOWN_SERVER_ERROR,
};
use crate::protocol::dedup::dedup_first_seen;
use crate::protocol::header::response_header_version;
use crate::protocol::wire_len::{kafka_compact_prefix, kafka_string_len};

/// An Iggy name caps at 255 bytes and a Kafka group's offset key is `kafka.cg.<group>`, so a
/// group id this gateway admits must leave room for that prefix (`docs/OFFSET_STORAGE.md`).
pub const MAX_GROUP_ID_BYTES: usize = 246;

/// Whether every group API admits `group_id`.
///
/// A longer one than [`MAX_GROUP_ID_BYTES`] cannot name its offsets in Iggy. Kafka refuses an
/// empty one only in the membership APIs, and accepts it in `OffsetCommit` and `OffsetFetch` for
/// old clients. This gateway refuses it in every API.
#[must_use]
pub const fn is_valid_group_id(group_id: &str) -> bool {
    !group_id.is_empty() && group_id.len() <= MAX_GROUP_ID_BYTES
}

/// Protocol selection is polynomial in this count under the coordinator lock. Stock clients list
/// one or two assignors.
pub const MAX_PROTOCOLS_PER_MEMBER: usize = 16;

/// Legacy Kafka strings are an `i16` length. `DescribeGroups` v0-v4 and `ListGroups` v0-v2 encode
/// `member_id` and (from v4) `group_instance_id` that way, and `kafka-protocol` refuses anything
/// longer instead of writing it.
const MAX_LEGACY_STRING_BYTES: usize = i16::MAX as usize;

/// `protocol_type` is stored on the group and echoed by every `ListGroups` row. `i16::MAX` would
/// let a few hundred pending-only joins pin 32 KiB types that sort ahead of real groups and fill
/// the list response. 255 covers `consumer` and any stock protocol name.
const MAX_PROTOCOL_TYPE_BYTES: usize = 255;

/// `generate_member_id` appends `-{uuid}`. A UUID's display form is 36 bytes, so the prefix has
/// to leave this much room or the id itself will not fit a legacy string.
const MEMBER_ID_SUFFIX_BYTES: usize = 1 + 36;

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

impl Phase {
    const fn as_kafka_state(self) -> &'static str {
        match self {
            Self::PreparingRebalance => GROUP_STATE_PREPARING_REBALANCE,
            Self::CompletingRebalance => GROUP_STATE_COMPLETING_REBALANCE,
            Self::Stable => GROUP_STATE_STABLE,
        }
    }
}

/// Outcome of one state-machine step: answer now, or park until `wake_at`.
pub enum Step<T> {
    Respond(T),
    Wait {
        member_id: StrBytes,
        wake_at: Instant,
    },
}

/// What one `LeaveGroup` removed, which decides how the group reacts afterwards.
#[derive(Default)]
struct Departures {
    members: bool,
    pending: bool,
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

/// Copy what a member keeps out of the request frame.
///
/// `StrBytes` and `Bytes` are refcounted views, so retaining them verbatim keeps the whole frame
/// alive: a member whose counted metadata is a few hundred bytes can pin megabytes. Copying costs
/// one allocation per protocol on a control-plane path, and bounds what a member actually retains
/// to what the caps actually measure.
fn retained_protocols(protocols: &[(StrBytes, Bytes)]) -> Vec<(StrBytes, Bytes)> {
    protocols
        .iter()
        .map(|(name, metadata)| (owned_str(name), Bytes::copy_from_slice(metadata)))
        .collect()
}

impl Member {
    fn new(request: &JoinRequest, now: Instant, max_rebalance_timeout: Duration) -> Self {
        Self {
            group_instance_id: request.group_instance_id.as_ref().map(owned_str),
            session_timeout: request.session_timeout,
            rebalance_timeout: request.rebalance_timeout.min(max_rebalance_timeout),
            protocols: retained_protocols(&request.protocols),
            assignment: Bytes::new(),
            session_deadline: now + request.session_timeout,
            rejoined: true,
            synced: false,
            join_response: None,
        }
    }

    /// Keeps `group_instance_id`: a rejoin that names one was already checked against it, and one
    /// that names none does not make a static member dynamic, as in Kafka.
    fn rejoin(&mut self, request: &JoinRequest, now: Instant, max_rebalance_timeout: Duration) {
        self.session_timeout = request.session_timeout;
        self.rebalance_timeout = request.rebalance_timeout.min(max_rebalance_timeout);
        self.protocols = retained_protocols(&request.protocols);
        self.session_deadline = now + request.session_timeout;
        self.rejoined = true;
        // Cleared here rather than when a rebalance opens: a member that has rejoined is asking
        // for the next generation's answer, while one still parked on the previous generation
        // must keep the snapshot it is waiting to collect.
        self.join_response = None;
    }

    fn roster_bytes(&self, member_id: &StrBytes) -> usize {
        roster_entry_bytes(member_id, self.group_instance_id.as_ref(), &self.protocols)
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

    fn metadata_len(&self, name: &StrBytes) -> usize {
        self.protocols
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map_or(0, |(_, metadata)| metadata.len())
    }

    /// Encoded `DescribedGroupMember` size for `version`, without copying metadata or assignment.
    /// `include_blobs` is false for Kafka's `summaryNoMetadata` shape: member id and instance id
    /// stay, and metadata and assignment are empty.
    fn described_len(
        &self,
        version: i16,
        member_id: &StrBytes,
        protocol_name: Option<&StrBytes>,
        include_blobs: bool,
    ) -> usize {
        let flexible = version >= 5;
        let mut total = kafka_string_len(flexible, member_id.len());
        if version >= 4 {
            total += kafka_nullable_string_len(
                flexible,
                self.group_instance_id.as_ref().map(|id| id.len()),
            );
        }
        // `client_id` and `client_host` stay empty: the coordinator does not retain them.
        total += kafka_string_len(flexible, 0);
        total += kafka_string_len(flexible, 0);
        let metadata_len = if include_blobs {
            protocol_name.map_or(0, |name| self.metadata_len(name))
        } else {
            0
        };
        let assignment_len = if include_blobs {
            self.assignment.len()
        } else {
            0
        };
        total += kafka_bytes_len(flexible, metadata_len);
        total += kafka_bytes_len(flexible, assignment_len);
        if flexible {
            total += 1;
        }
        total
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
    /// The member id holding each instance id. An entry is dropped with its holder, so the map
    /// never outgrows `members`.
    static_members: HashMap<StrBytes, StrBytes>,
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
            static_members: HashMap::new(),
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

    /// Joined members, not pending ids. A v4+ first join holds only a pending id and Kafka
    /// reports that group as `Empty`, which is what `--list --state Empty` filters on.
    fn reported_state(&self) -> &'static str {
        if self.members.is_empty() {
            GROUP_STATE_EMPTY
        } else {
            self.phase.as_kafka_state()
        }
    }

    /// Every stored deadline is already due. Pending ids count: a group still waiting on
    /// `MEMBER_ID_REQUIRED` is visible until that id expires. An empty map is vacuously due,
    /// which only matters for a group tick should already have removed.
    fn deadlines_elapsed(&self, now: Instant) -> bool {
        self.members
            .values()
            .all(|member| member.session_deadline <= now)
            && self.pending.values().all(|deadline| *deadline <= now)
    }

    /// Would admitting `request` as `member_id` grow the leader's roster past its cap? The
    /// member's own current entry is replaced, not added to.
    fn roster_overflows(
        &self,
        config: &GroupCoordinatorConfig,
        member_id: &StrBytes,
        request: &JoinRequest,
    ) -> bool {
        self.roster_overflows_replacing(config, member_id, member_id, request)
    }

    /// [`Self::roster_overflows`] for `request` joining as `member_id` in place of `replaced`.
    fn roster_overflows_replacing(
        &self,
        config: &GroupCoordinatorConfig,
        replaced: &StrBytes,
        member_id: &StrBytes,
        request: &JoinRequest,
    ) -> bool {
        let others: usize = self
            .members
            .iter()
            .filter(|(id, _)| *id != replaced)
            .map(|(id, member)| member.roster_bytes(id))
            .sum();
        let joining = roster_entry_bytes(
            member_id,
            request.group_instance_id.as_ref(),
            &request.protocols,
        );
        others + joining > config.max_group_roster_bytes
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

        // The deadline outlives the leader's sync, as in Kafka's `expirePendingSync`: a member of
        // this generation that never syncs in `Stable` is removed too.
        if self.phase != Phase::PreparingRebalance
            && self.sync_deadline.is_some_and(|deadline| deadline <= now)
        {
            let unsynced: Vec<StrBytes> = self
                .members
                .iter()
                .filter(|(_, member)| !member.synced)
                .map(|(id, _)| id.clone())
                .collect();
            self.sync_deadline = None;
            if self.phase == Phase::CompletingRebalance || !unsynced.is_empty() {
                for id in &unsynced {
                    self.remove_member(id);
                }
                if !self.members.is_empty() {
                    self.prepare_rebalance(now, None);
                }
                changed = true;
            }
        }

        if changed {
            self.bump();
        }
    }

    /// Adds a member, indexing it under its instance id when it names one.
    fn insert_member(&mut self, member_id: StrBytes, member: Member) {
        if let Some(instance_id) = &member.group_instance_id {
            self.static_members
                .insert(instance_id.clone(), member_id.clone());
        }
        self.members.insert(member_id, member);
    }

    /// Removes a member for good. Its instance id goes with it.
    fn remove_member(&mut self, member_id: &StrBytes) {
        if let Some(instance_id) = self
            .members
            .remove(member_id)
            .and_then(|member| member.group_instance_id)
        {
            self.static_members.remove(&instance_id);
        }
        if self.leader.as_ref() == Some(member_id) {
            self.leader = self.members.keys().next().cloned();
        }
    }

    /// The member holding `instance_id`.
    fn static_holder(&self, instance_id: &StrBytes) -> Option<&StrBytes> {
        self.static_members.get(instance_id)
    }

    /// Kafka's `ClassicGroup.validateMember`: a named instance must be held, and by this member.
    fn validate_member(
        &self,
        member_id: &StrBytes,
        instance_id: Option<&StrBytes>,
    ) -> Result<(), i16> {
        if let Some(instance_id) = instance_id {
            match self.static_holder(instance_id) {
                None => return Err(ERROR_UNKNOWN_MEMBER_ID),
                Some(holder) if holder != member_id => return Err(ERROR_FENCED_INSTANCE_ID),
                Some(_) => {}
            }
        }
        if self.members.contains_key(member_id) {
            Ok(())
        } else {
            Err(ERROR_UNKNOWN_MEMBER_ID)
        }
    }

    /// Removes one `LeaveGroup` identity, following Kafka's `handleLeaveGroup` order.
    fn leave_one(&mut self, identity: &LeavingMember, departed: &mut Departures) -> i16 {
        let holder = identity
            .group_instance_id
            .as_ref()
            .and_then(|instance_id| self.static_holder(instance_id))
            .cloned();

        if identity.member_id.is_empty() {
            let Some(holder) = holder else {
                return ERROR_UNKNOWN_MEMBER_ID;
            };
            self.remove_member(&holder);
            departed.members = true;
            return ERROR_NONE;
        }
        if self.pending.remove(&identity.member_id).is_some() {
            departed.pending = true;
            return ERROR_NONE;
        }
        match (identity.group_instance_id.is_some(), holder) {
            (_, Some(holder)) if holder == identity.member_id => {}
            (_, Some(_)) => return ERROR_FENCED_INSTANCE_ID,
            (true, None) => return ERROR_UNKNOWN_MEMBER_ID,
            (false, None) if !self.members.contains_key(&identity.member_id) => {
                return ERROR_UNKNOWN_MEMBER_ID;
            }
            (false, None) => {}
        }
        self.remove_member(&identity.member_id);
        departed.members = true;
        ERROR_NONE
    }

    /// React to a `LeaveGroup` the way the session sweep reacts to an expiry.
    ///
    /// The rebalance opens only after the removals, so its join window is sized from the
    /// members that remain. The final `bump` is unconditional on any removal: it is what answers
    /// a waiter the departed member still has parked on another connection.
    fn after_departure(&mut self, departed: &Departures, now: Instant) {
        if departed.members && !self.members.is_empty() && self.phase != Phase::PreparingRebalance {
            self.prepare_rebalance(now, None);
        }
        // A join window left open with nobody in it would close on a later tick and clear the
        // ids still on their way back. The next admitted member reopens the barrier.
        if self.members.is_empty() && !self.pending.is_empty() {
            self.join_deadline = None;
        }
        if self.phase == Phase::PreparingRebalance {
            self.maybe_complete_join(now);
        }
        if departed.members || departed.pending {
            self.bump();
        }
    }

    /// Records `member_id`'s sync, and drops the sync deadline once nobody is left to sync.
    fn mark_synced(&mut self, member_id: &StrBytes) {
        if let Some(member) = self.members.get_mut(member_id) {
            member.synced = true;
        }
        if self.members.values().all(|member| member.synced) {
            self.sync_deadline = None;
        }
    }

    /// The earliest moment any rule in this group could fire.
    fn next_deadline(&self) -> Option<Instant> {
        let phase_deadline = match self.phase {
            Phase::PreparingRebalance => self.join_deadline,
            Phase::CompletingRebalance | Phase::Stable => self.sync_deadline,
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

    /// Kafka's `completeClassicGroupJoin`: a dynamic member that did not rejoin is removed, a
    /// static one stays in the group and in the roster until its session expires or a sync
    /// deadline passes without it.
    fn complete_join(&mut self, now: Instant) {
        let absent_dynamic: Vec<StrBytes> = self
            .members
            .iter()
            .filter(|(_, member)| !member.rejoined && member.group_instance_id.is_none())
            .map(|(id, _)| id.clone())
            .collect();
        for id in &absent_dynamic {
            self.remove_member(id);
        }
        self.join_deadline = None;
        self.initial = false;
        // A window that closes with nobody in it forms no generation, so the ids still on their
        // way back keep their own expiry and the next admitted member reopens the barrier.
        if self.members.is_empty() {
            self.leader = None;
            self.bump();
            return;
        }
        let rejoined_leader = self
            .leader
            .as_ref()
            .filter(|leader| {
                self.members
                    .get(*leader)
                    .is_some_and(|member| member.rejoined)
            })
            .or_else(|| {
                self.members
                    .iter()
                    .find(|(_, member)| member.rejoined)
                    .map(|(id, _)| id)
            })
            .cloned();
        // Only absent static members remain. Kafka postpones the join until their sessions
        // expire or one of them returns.
        let Some(leader) = rejoined_leader else {
            self.join_deadline = Some(now + self.max_rebalance_timeout());
            return;
        };
        self.pending.clear();

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
            let absent = !member.rejoined;
            member.synced = false;
            member.rejoined = false;
            member.assignment = Bytes::new();
            // An absent member is not parked, so a snapshot would only be served stale later. Its
            // session keeps running from its last heartbeat.
            if absent {
                member.join_response = None;
                continue;
            }
            member.session_deadline = now + member.session_timeout;
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
                skip_assignment: false,
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
            skip_assignment: false,
        }
    }

    /// The answer for a static member that replaced its previous incarnation while `Stable`,
    /// keeping the generation and assignment already in effect (Kafka's
    /// `updateStaticMemberThenRebalanceOrCompleteJoin`). From v9 a replaced leader gets the roster
    /// and `skip_assignment`. Below v9 the response names the previous leader, so a replaced
    /// leader acts as a follower and does not reassign.
    fn replaced_static_result(
        &self,
        member_id: &StrBytes,
        previous_leader: &StrBytes,
        supports_skip_assignment: bool,
    ) -> JoinResult {
        let is_leader = self.leader.as_ref() == Some(member_id);
        let (leader, members, skip_assignment) = if supports_skip_assignment {
            let members = if is_leader { self.roster() } else { Vec::new() };
            (self.leader.clone().unwrap_or_default(), members, is_leader)
        } else {
            (previous_leader.clone(), Vec::new(), false)
        };
        JoinResult {
            error: ERROR_NONE,
            generation_id: self.generation_id,
            protocol_type: Some(self.protocol_type.clone()),
            protocol_name: self.protocol_name.clone(),
            leader,
            member_id: member_id.clone(),
            members,
            skip_assignment,
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
                .map_or_else(Bytes::new, |(_, blob)| Bytes::copy_from_slice(blob));
        }
        if let Some(leader) = self.leader.clone() {
            self.mark_synced(&leader);
        }
        // Kafka refreshes only the members whose sync it answers, so an absent member's session
        // keeps running toward expiry.
        for member in self.members.values_mut().filter(|member| member.synced) {
            member.session_deadline = now + member.session_timeout;
        }
        self.phase = Phase::Stable;
        self.bump();
    }

    /// `summary` forces Kafka's `summaryNoMetadata` shape even in `Stable`: empty `protocol_data`,
    /// metadata and assignment. A group that is not `Stable` always uses that shape, because the
    /// assignment still on the member is the previous generation and a rejoin has already
    /// replaced the subscription.
    fn description(&self, group_id: &StrBytes, summary: bool) -> GroupDescription {
        let summary = summary || self.phase != Phase::Stable;
        let protocol_name = if summary {
            None
        } else {
            self.protocol_name.clone()
        };
        let members = self
            .members
            .iter()
            .map(|(member_id, member)| MemberDescription {
                member_id: member_id.clone(),
                group_instance_id: member.group_instance_id.clone(),
                metadata: if summary {
                    Bytes::new()
                } else {
                    protocol_name
                        .as_ref()
                        .map_or_else(Bytes::new, |name| member.metadata_for(name))
                },
                assignment: if summary {
                    Bytes::new()
                } else {
                    member.assignment.clone()
                },
            })
            .collect();
        GroupDescription {
            group_id: group_id.clone(),
            error: ERROR_NONE,
            state: self.reported_state(),
            protocol_type: self.protocol_type.clone(),
            protocol_name,
            members,
        }
    }

    /// Encoded `DescribedGroup` size for `version`. Same fields [`Self::description`] copies,
    /// priced from their lengths so the frame check does not have to build that snapshot first.
    fn described_len(&self, version: i16, group_id: &StrBytes, summary: bool) -> usize {
        let summary = summary || self.phase != Phase::Stable;
        let flexible = version >= 5;
        let mut total = 2;
        if version >= 6 {
            total += kafka_nullable_string_len(true, None);
        }
        total += kafka_string_len(flexible, group_id.len());
        total += kafka_string_len(flexible, self.reported_state().len());
        total += kafka_string_len(flexible, self.protocol_type.len());
        let protocol_len = if summary {
            0
        } else {
            self.protocol_name.as_ref().map_or(0, |name| name.len())
        };
        total += kafka_string_len(flexible, protocol_len);
        total += kafka_array_prefix(flexible, self.members.len());
        let protocol_name = if summary {
            None
        } else {
            self.protocol_name.as_ref()
        };
        for (member_id, member) in &self.members {
            total = total.saturating_add(member.described_len(
                version,
                member_id,
                protocol_name,
                !summary,
            ));
        }
        if version >= 3 {
            total += 4;
        }
        if flexible {
            total += 1;
        }
        total
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
        // TODO(#3542): keep a group that only holds committed offsets and report Empty.
        // Offsets are not stored here yet, so nothing remains that a later join would need.
        groups.remove(group_id);
        return false;
    }
    true
}

/// An upper bound on what one member adds to the leader's roster. Only the selected protocol's
/// metadata is sent, and which protocol wins is not known until the join completes, so the
/// largest one stands in for it.
fn roster_entry_bytes(
    member_id: &StrBytes,
    group_instance_id: Option<&StrBytes>,
    protocols: &[(StrBytes, Bytes)],
) -> usize {
    member_id.len()
        + group_instance_id.map_or(0, |id| id.len())
        + protocols
            .iter()
            .map(|(_, metadata)| metadata.len())
            .max()
            .unwrap_or(0)
}

/// Everything a `JoinGroup` can be rejected for before any group is touched.
fn join_request_error(config: &GroupCoordinatorConfig, request: &JoinRequest) -> Option<i16> {
    if !is_valid_group_id(&request.group_id) {
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
    if request.protocols.len() > MAX_PROTOCOLS_PER_MEMBER || legacy_strings_too_long(request) {
        return Some(ERROR_INVALID_REQUEST);
    }
    // Names count too: they are retained alongside the metadata, and a request can carry many
    // long ones while declaring almost no metadata at all. `protocol_type` is retained on the
    // group itself and echoed for every listed group, so it counts the same way.
    let retained_bytes: usize = request.protocol_type.len()
        + request
            .protocols
            .iter()
            .map(|(name, metadata)| name.len() + metadata.len())
            .sum::<usize>()
        + request
            .group_instance_id
            .as_ref()
            .map_or(0, |id| id.as_str().len());
    if retained_bytes > config.max_member_blob_bytes {
        return Some(ERROR_INVALID_REQUEST);
    }
    None
}

/// A client-supplied `member_id`, `group_instance_id`, and each protocol `name` are echoed as
/// legacy strings. `DescribeGroups` v0-v4 writes the selected protocol name as `protocol_data`
/// with an i16 length. When the member id is generated, the instance id is the prefix and the
/// `-{uuid}` suffix has to fit in the same cap. `protocol_type` is tighter than that cap: see
/// [`MAX_PROTOCOL_TYPE_BYTES`].
fn legacy_strings_too_long(request: &JoinRequest) -> bool {
    if request.protocol_type.len() > MAX_PROTOCOL_TYPE_BYTES
        || request.member_id.len() > MAX_LEGACY_STRING_BYTES
        || request
            .protocols
            .iter()
            .any(|(name, _)| name.len() > MAX_LEGACY_STRING_BYTES)
    {
        return true;
    }
    let Some(instance_id) = &request.group_instance_id else {
        return false;
    };
    let limit = if request.member_id.is_empty() {
        MAX_LEGACY_STRING_BYTES - MEMBER_ID_SUFFIX_BYTES
    } else {
        MAX_LEGACY_STRING_BYTES
    };
    instance_id.len() > limit
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
        owned_str(&request.group_id),
        GroupState::new(owned_str(&request.protocol_type), now),
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

    let Some(group) = groups.get_mut(&request.group_id) else {
        return reject(ERROR_UNKNOWN_MEMBER_ID);
    };

    if group.protocol_type != request.protocol_type
        || !group.protocols_compatible(&request.member_id, &request.protocols)
    {
        return reject_created(groups, created, ERROR_INCONSISTENT_GROUP_PROTOCOL);
    }

    if request.member_id.is_empty()
        && let Some(holder) = request
            .group_instance_id
            .as_ref()
            .and_then(|instance_id| group.static_holder(instance_id))
            .cloned()
    {
        return replace_static(group, config, &holder, request, now);
    }

    if request.member_id.is_empty() {
        return join_new_member(groups, config, request, created, now);
    }

    if group.pending.contains_key(&request.member_id) {
        // Kafka hands pending ids to dynamic joins only and throws on a static one. 25 sends the
        // client back with an empty member id, where its instance id is resolved.
        if request.group_instance_id.is_some() {
            group.pending.remove(&request.member_id);
            let departed = Departures {
                members: false,
                pending: true,
            };
            group.after_departure(&departed, now);
            if group.is_empty() {
                groups.remove(&request.group_id);
            }
            return reject(ERROR_UNKNOWN_MEMBER_ID);
        }
        if group.roster_overflows(config, &request.member_id, request) {
            return reject(ERROR_GROUP_MAX_SIZE_REACHED);
        }
        return admit(group, config, &request.member_id, request, now);
    }
    if let Err(error) =
        group.validate_member(&request.member_id, request.group_instance_id.as_ref())
    {
        return reject(error);
    }
    if group.roster_overflows(config, &request.member_id, request) {
        return reject(ERROR_GROUP_MAX_SIZE_REACHED);
    }
    let Some(member) = group.members.get(&request.member_id) else {
        return reject(ERROR_UNKNOWN_MEMBER_ID);
    };
    let protocols_changed = member.protocols != request.protocols;
    let is_leader = group.leader.as_ref() == Some(&request.member_id);

    rejoin_step(group, config, request, is_leader, protocols_changed, now)
}

/// A join with no member id and no instance held: capacity checks, then `MEMBER_ID_REQUIRED` or
/// direct admission.
fn join_new_member(
    groups: &mut Groups,
    config: &GroupCoordinatorConfig,
    request: &JoinRequest,
    created: bool,
    now: Instant,
) -> Step<JoinResult> {
    let reject = |groups: &mut Groups, error: i16| {
        if created {
            groups.remove(&request.group_id);
        }
        Step::Respond(JoinResult::error(error, request.member_id.clone()))
    };
    let total_members = |groups: &Groups| {
        groups
            .values()
            .map(|group| group.members.len() + group.pending.len())
            .sum::<usize>()
    };
    if total_members(groups) >= config.max_total_members {
        reclaim_expired(groups, now, &request.group_id);
    }
    let at_capacity = total_members(groups) >= config.max_total_members;

    let Some(group) = groups.get_mut(&request.group_id) else {
        return reject(groups, ERROR_UNKNOWN_MEMBER_ID);
    };
    if group.members.len() + group.pending.len() >= config.max_members_per_group {
        return reject(groups, ERROR_GROUP_MAX_SIZE_REACHED);
    }
    if at_capacity {
        tracing::warn!(
            max_total_members = config.max_total_members,
            "consumer group member limit reached; rejecting JoinGroup"
        );
        return reject(groups, ERROR_COORDINATOR_NOT_AVAILABLE);
    }
    let member_id = generate_member_id(request.group_instance_id.as_ref());
    // Checked before an id is handed out too, so a member that could never be admitted is
    // not sent round a `MEMBER_ID_REQUIRED` trip first.
    if group.roster_overflows(config, &member_id, request) {
        return reject(groups, ERROR_GROUP_MAX_SIZE_REACHED);
    }
    // A static member is admitted directly, as in Kafka: its instance id already names it.
    if request.require_known_member_id && request.group_instance_id.is_none() {
        group
            .pending
            .insert(member_id.clone(), now + request.session_timeout);
        group.bump();
        return Step::Respond(JoinResult::error(ERROR_MEMBER_ID_REQUIRED, member_id));
    }
    admit(group, config, &member_id, request, now)
}

/// Dispatch for a member the group already knows, by phase.
fn rejoin_step(
    group: &mut GroupState,
    config: &GroupCoordinatorConfig,
    request: &JoinRequest,
    is_leader: bool,
    protocols_changed: bool,
    now: Instant,
) -> Step<JoinResult> {
    match group.phase {
        Phase::PreparingRebalance => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.rejoin(request, now, config.max_rebalance_timeout);
            }
            group.bump();
            group.maybe_complete_join(now);
            park_or_respond(group, &request.member_id, now)
        }
        Phase::Stable if is_leader || protocols_changed => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.rejoin(request, now, config.max_rebalance_timeout);
            }
            group.prepare_rebalance(now, Some(&request.member_id));
            group.maybe_complete_join(now);
            park_or_respond(group, &request.member_id, now)
        }
        Phase::CompletingRebalance if protocols_changed => {
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.rejoin(request, now, config.max_rebalance_timeout);
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

/// The waiter is checked against its instance id's holder again, so one whose instance was
/// taken over while it slept is answered `FENCED_INSTANCE_ID`, as Kafka's `replaceStaticMember`
/// answers it at once.
pub fn join_resume_step(
    groups: &mut Groups,
    group_id: &StrBytes,
    member_id: &StrBytes,
    group_instance_id: Option<&StrBytes>,
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
    if let Err(error) = group.validate_member(member_id, group_instance_id) {
        return Step::Respond(JoinResult::error(error, member_id.clone()));
    }
    park_or_respond(group, member_id, now)
}

pub fn heartbeat_step(
    groups: &mut Groups,
    group_id: &StrBytes,
    generation_id: i32,
    member_id: &StrBytes,
    group_instance_id: Option<&StrBytes>,
    now: Instant,
) -> i16 {
    if !tick_group(groups, group_id, now) {
        return ERROR_UNKNOWN_MEMBER_ID;
    }
    let Some(group) = groups.get_mut(group_id) else {
        return ERROR_UNKNOWN_MEMBER_ID;
    };
    if let Err(error) = group.validate_member(member_id, group_instance_id) {
        return error;
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
    if let Err(error) =
        group.validate_member(&request.member_id, request.group_instance_id.as_ref())
    {
        return Step::Respond(SyncResult::error(error));
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
            group.mark_synced(&request.member_id);
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
            group.mark_synced(&request.member_id);
            if let Some(member) = group.members.get_mut(&request.member_id) {
                member.session_deadline = now + member.session_timeout;
            }
            Step::Wait {
                member_id: request.member_id.clone(),
                wake_at: group.wake_at(now),
            }
        }
    }
}

/// Like [`join_resume_step`], the waiter is checked against its instance id's holder again.
pub fn sync_resume_step(
    groups: &mut Groups,
    group_id: &StrBytes,
    member_id: &StrBytes,
    group_instance_id: Option<&StrBytes>,
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
    if let Err(error) = group.validate_member(member_id, group_instance_id) {
        return Step::Respond(SyncResult::error(error));
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

/// Never creates a group, and removes one the leave empties rather than leaving it for
/// `reclaim_expired`.
pub fn leave_step(groups: &mut Groups, request: &LeaveRequest, now: Instant) -> LeaveResult {
    if request.group_id.is_empty() {
        return LeaveResult::error(ERROR_INVALID_GROUP_ID);
    }
    let unknown = || LeaveResult {
        error: ERROR_NONE,
        members: request
            .members
            .iter()
            .map(|identity| LeftMember::from((identity, ERROR_UNKNOWN_MEMBER_ID)))
            .collect(),
    };
    if !tick_group(groups, &request.group_id, now) {
        return unknown();
    }
    let Some(group) = groups.get_mut(&request.group_id) else {
        return unknown();
    };

    let mut departed = Departures::default();
    let mut members = Vec::with_capacity(request.members.len());
    for identity in &request.members {
        let error = group.leave_one(identity, &mut departed);
        members.push(LeftMember::from((identity, error)));
    }
    group.after_departure(&departed, now);

    if group.is_empty() {
        groups.remove(&request.group_id);
    }
    LeaveResult {
        error: ERROR_NONE,
        members,
    }
}

/// How one requested id is answered. Priced before [`GroupState::description`] copies blobs.
#[derive(Clone, Copy)]
enum DescribedChoice {
    /// Not in the map after the tick. Encoded as `Dead`.
    Missing,
    /// `summary` is Kafka's `summaryNoMetadata` shape.
    Present { summary: bool },
    /// Does not fit in the bytes still left in this response.
    TooLarge,
}

/// One entry per distinct id, first-seen order, after that group's usual expiry tick.
///
/// Ids are deduped before the tick, so a request that repeats one group does not walk it once
/// per copy. Each group is priced on its own. One that does not fit by itself is answered
/// without member metadata. One that does not fit in what the earlier groups left is answered
/// with [`ERROR_UNKNOWN_SERVER_ERROR`]. The connection is not closed: outbound frames are not
/// capped, and closing makes `kafka-consumer-groups.sh --describe --all-groups` retry the same
/// request until the client times out.
pub fn describe_groups(
    groups: &mut Groups,
    group_ids: &[StrBytes],
    version: i16,
    max_frame_size: usize,
    now: Instant,
) -> Vec<Option<GroupDescription>> {
    let group_ids = dedup_first_seen(group_ids);
    for group_id in &group_ids {
        tick_group(groups, group_id, now);
    }
    choose_described(version, groups, &group_ids, max_frame_size)
        .into_iter()
        .zip(group_ids)
        .map(|(choice, group_id)| match choice {
            DescribedChoice::Missing => None,
            DescribedChoice::TooLarge => Some(too_large_description(&group_id)),
            DescribedChoice::Present { summary } => groups
                .get(&group_id)
                .map(|group| group.description(&group_id, summary)),
        })
        .collect()
}

/// Groups currently in the map, ordered by group id.
///
/// Does not tick. Groups whose sessions and pending ids are already past `now` are skipped so
/// the list agrees with a describe of the same id, which would tick them away and answer `Dead`.
pub fn list_groups(groups: &Groups, now: Instant) -> Vec<GroupListing> {
    let mut listed: Vec<GroupListing> = groups
        .iter()
        .filter(|(_, group)| !group.deadlines_elapsed(now))
        .map(|(group_id, group)| GroupListing {
            group_id: group_id.clone(),
            protocol_type: group.protocol_type.clone(),
            state: group.reported_state(),
        })
        .collect();
    listed.sort_by(|left, right| left.group_id.cmp(&right.group_id));
    listed
}

fn too_large_description(group_id: &StrBytes) -> GroupDescription {
    GroupDescription {
        group_id: group_id.clone(),
        error: ERROR_UNKNOWN_SERVER_ERROR,
        state: "",
        protocol_type: StrBytes::from_static_str(""),
        protocol_name: None,
        members: Vec::new(),
    }
}

fn choose_described(
    version: i16,
    groups: &Groups,
    group_ids: &[StrBytes],
    max_frame_size: usize,
) -> Vec<DescribedChoice> {
    let mut used = describe_fixed_len(version, group_ids.len());
    let mut choices = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        let Some(group) = groups.get(group_id) else {
            let missing = missing_described_len(version, group_id);
            if used.saturating_add(missing) <= max_frame_size {
                used += missing;
                choices.push(DescribedChoice::Missing);
            } else {
                used = used.saturating_add(too_large_described_len(version, group_id));
                choices.push(DescribedChoice::TooLarge);
            }
            continue;
        };
        let natural_summary = group.phase != Phase::Stable;
        let natural = group.described_len(version, group_id, natural_summary);
        if used.saturating_add(natural) <= max_frame_size {
            used += natural;
            choices.push(DescribedChoice::Present {
                summary: natural_summary,
            });
            continue;
        }
        // Full metadata does not fit in what is left. Use the summary shape only when the group
        // would not fit even as the only group in the response. A group that fits alone and
        // loses the rest of the batch gets an error, so a later describe of that id still
        // returns the assignment.
        if !natural_summary {
            let alone =
                describe_fixed_len(version, 1) + group.described_len(version, group_id, false);
            if alone > max_frame_size {
                let summary = group.described_len(version, group_id, true);
                if used.saturating_add(summary) <= max_frame_size {
                    used += summary;
                    choices.push(DescribedChoice::Present { summary: true });
                    continue;
                }
            }
        }
        used = used.saturating_add(too_large_described_len(version, group_id));
        choices.push(DescribedChoice::TooLarge);
    }
    choices
}

/// Header plus body for the choices [`choose_described`] would make. The 4-byte length prefix
/// `send_response` writes is not part of that value. Test-only: production prices inside
/// [`choose_described`] and does not need the summed total.
#[cfg(test)]
fn describe_frame_len(
    version: i16,
    groups: &Groups,
    group_ids: &[StrBytes],
    max_frame_size: usize,
) -> usize {
    let group_ids = dedup_first_seen(group_ids);
    let mut total = describe_fixed_len(version, group_ids.len());
    for (group_id, choice) in group_ids.iter().zip(choose_described(
        version,
        groups,
        &group_ids,
        max_frame_size,
    )) {
        let group_bytes = match choice {
            DescribedChoice::Missing => missing_described_len(version, group_id),
            DescribedChoice::TooLarge => too_large_described_len(version, group_id),
            DescribedChoice::Present { summary } => groups
                .get(group_id)
                .map_or(0, |group| group.described_len(version, group_id, summary)),
        };
        total = total.saturating_add(group_bytes);
    }
    total
}

fn describe_fixed_len(version: i16, group_count: usize) -> usize {
    let mut total = kafka_response_header_len(API_KEY_DESCRIBE_GROUPS, version);
    if version >= 1 {
        total += 4;
    }
    total += kafka_array_prefix(version >= 5, group_count);
    if version >= 5 {
        total += 1;
    }
    total
}

/// Per-group `UNKNOWN_SERVER_ERROR`. v6 carries [`DESCRIBE_RESPONSE_TOO_LARGE`]; older versions
/// have no `error_message` field. Matches `described_group` in the handler.
fn too_large_described_len(version: i16, group_id: &StrBytes) -> usize {
    let flexible = version >= 5;
    let mut total = 2;
    if version >= 6 {
        total += kafka_string_len(true, DESCRIBE_RESPONSE_TOO_LARGE.len());
    }
    total += kafka_string_len(flexible, group_id.len());
    total += kafka_string_len(flexible, 0);
    total += kafka_string_len(flexible, 0);
    total += kafka_string_len(flexible, 0);
    total += kafka_array_prefix(flexible, 0);
    if version >= 3 {
        total += 4;
    }
    if flexible {
        total += 1;
    }
    total
}

/// A group that is not in the map. v6 carries `GROUP_ID_NOT_FOUND` and `Group {id} not found.`;
/// older versions encode error 0 and omit `error_message`. Matches `missing_group` in the handler.
fn missing_described_len(version: i16, group_id: &StrBytes) -> usize {
    let flexible = version >= 5;
    let mut total = 2;
    if version >= 6 {
        let message_len = "Group ".len() + group_id.len() + " not found.".len();
        total += kafka_string_len(true, message_len);
    }
    total += kafka_string_len(flexible, group_id.len());
    total += kafka_string_len(flexible, "Dead".len());
    total += kafka_string_len(flexible, 0);
    total += kafka_string_len(flexible, 0);
    total += kafka_array_prefix(flexible, 0);
    if version >= 3 {
        total += 4;
    }
    if flexible {
        total += 1;
    }
    total
}

fn kafka_response_header_len(api_key: i16, version: i16) -> usize {
    if response_header_version(api_key, version) >= 1 {
        5
    } else {
        4
    }
}

fn kafka_nullable_string_len(flexible: bool, bytes: Option<usize>) -> usize {
    match bytes {
        Some(bytes) => kafka_string_len(flexible, bytes),
        None if flexible => 1,
        None => 2,
    }
}

fn kafka_bytes_len(flexible: bool, bytes: usize) -> usize {
    if flexible {
        kafka_compact_prefix(bytes) + bytes
    } else {
        4 + bytes
    }
}

fn kafka_array_prefix(flexible: bool, count: usize) -> usize {
    if flexible {
        kafka_compact_prefix(count)
    } else {
        4
    }
}
/// Kafka's `ClassicGroup.validateOffsetCommit`. Never creates a group.
///
/// A group with no members is Kafka's `Empty`: a negative generation commits without a member. A
/// gateway restart empties every group, so a member's next commit gets `UNKNOWN_MEMBER_ID` and it
/// rejoins. Kafka's v9 answer, `GROUP_ID_NOT_FOUND`, is fatal in the Java consumer.
pub fn commit_step(groups: &mut Groups, request: &CommitRequest, now: Instant) -> i16 {
    if !is_valid_group_id(&request.group_id) {
        return ERROR_INVALID_GROUP_ID;
    }
    let empty = || {
        if request.generation_id < 0 {
            ERROR_NONE
        } else {
            ERROR_UNKNOWN_MEMBER_ID
        }
    };
    if !tick_group(groups, &request.group_id, now) {
        return empty();
    }
    let Some(group) = groups.get_mut(&request.group_id) else {
        return empty();
    };
    if group.members.is_empty() {
        return empty();
    }
    if request.generation_id < 0
        && request.member_id.is_empty()
        && request.group_instance_id.is_none()
    {
        return ERROR_UNKNOWN_MEMBER_ID;
    }
    if let Some(instance_id) = &request.group_instance_id {
        match group.static_holder(instance_id) {
            None => return ERROR_UNKNOWN_MEMBER_ID,
            Some(holder) if *holder != request.member_id => return ERROR_FENCED_INSTANCE_ID,
            Some(_) => {}
        }
    }
    let (generation_id, phase) = (group.generation_id, group.phase);
    let Some(member) = group.members.get_mut(&request.member_id) else {
        return ERROR_UNKNOWN_MEMBER_ID;
    };
    if request.generation_id != generation_id {
        return ERROR_ILLEGAL_GENERATION;
    }
    // A member commits right before it rejoins, so `PreparingRebalance` accepts.
    if phase == Phase::CompletingRebalance {
        return ERROR_REBALANCE_IN_PROGRESS;
    }
    member.session_deadline = now + member.session_timeout;
    ERROR_NONE
}

/// How long an offset call may hold the connection that `member` heartbeats on: `hold`, and at
/// most half of what is left of that member's session, so a heartbeat that waits behind the call
/// still lands in time. A member that is not there, or whose session has ended, leaves `hold`.
pub fn offset_hold(
    groups: &Groups,
    member: &GroupMember,
    hold: Duration,
    now: Instant,
) -> Duration {
    let left = groups
        .get(&member.group_id)
        .and_then(|group| group.members.get(&member.member_id))
        .and_then(|found| found.session_deadline.checked_duration_since(now))
        .filter(|left| !left.is_zero());
    left.map_or(hold, |left| hold.min(left / 2))
}

fn admit(
    group: &mut GroupState,
    config: &GroupCoordinatorConfig,
    member_id: &StrBytes,
    request: &JoinRequest,
    now: Instant,
) -> Step<JoinResult> {
    group.pending.remove(member_id);
    // A pending id arrives back as the client's copy of it, a view into its request frame.
    let member_id = &owned_str(member_id);
    group.insert_member(
        member_id.clone(),
        Member::new(request, now, config.max_rebalance_timeout),
    );
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

/// A static member returning without its member id takes over the one holding its instance id,
/// under a new id (Kafka's `updateStaticMemberThenRebalanceOrCompleteJoin`).
///
/// The assignment and generation carry over, so a `Stable` group does not rebalance unless the
/// returning member's protocols change the selected protocol. In `CompletingRebalance` the leader
/// may already be assigning to the old id, so a rebalance opens. Capacity caps do not apply: the
/// group does not grow.
fn replace_static(
    group: &mut GroupState,
    config: &GroupCoordinatorConfig,
    old_member_id: &StrBytes,
    request: &JoinRequest,
    now: Instant,
) -> Step<JoinResult> {
    let new_member_id = generate_member_id(request.group_instance_id.as_ref());
    if group.roster_overflows_replacing(config, old_member_id, &new_member_id, request) {
        return Step::Respond(JoinResult::error(
            ERROR_GROUP_MAX_SIZE_REACHED,
            request.member_id.clone(),
        ));
    }
    let previous_leader = group.leader.clone().unwrap_or_default();
    let holder = request
        .group_instance_id
        .as_ref()
        .and_then(|instance_id| group.static_members.get_mut(instance_id));
    let (Some(holder), Some(mut member)) = (holder, group.members.remove(old_member_id)) else {
        return Step::Respond(JoinResult::error(
            ERROR_UNKNOWN_MEMBER_ID,
            request.member_id.clone(),
        ));
    };
    *holder = new_member_id.clone();
    member.rejoin(request, now, config.max_rebalance_timeout);
    group.members.insert(new_member_id.clone(), member);
    if group.leader.as_ref() == Some(old_member_id) {
        group.leader = Some(new_member_id.clone());
    }

    match group.phase {
        Phase::Stable => {
            let leader = group.leader.clone().unwrap_or_default();
            if group.select_protocol(&leader) == group.protocol_name {
                if let Some(member) = group.members.get_mut(&new_member_id) {
                    member.rejoined = false;
                }
                // The replacement keeps the assignment in effect, so it owes no sync.
                group.mark_synced(&new_member_id);
                group.bump();
                return Step::Respond(group.replaced_static_result(
                    &new_member_id,
                    &previous_leader,
                    request.supports_skip_assignment,
                ));
            }
            group.prepare_rebalance(now, Some(&new_member_id));
        }
        Phase::CompletingRebalance => group.prepare_rebalance(now, Some(&new_member_id)),
        Phase::PreparingRebalance => group.bump(),
    }
    group.maybe_complete_join(now);
    park_or_respond(group, &new_member_id, now)
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
    // Two calls can be in flight for one member id, and only one snapshot is ever minted, so the
    // loser finds nothing here. While a barrier is open it is right to wait for the next one.
    // Once it has closed there is nothing left to wait for, and parking again would be
    // permanent: the session refresh on resume means such a waiter is never reaped, so it would
    // hold its connection, its `max_connections` permit and its member slot until the process
    // dies, and the group could never empty for reclamation either.
    if group.phase != Phase::PreparingRebalance {
        return Step::Respond(group.current_generation_result(member_id));
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
    use crate::protocol::handlers::describe_groups::{
        described_group, encode_response, missing_group,
    };

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

    #[test]
    fn given_more_protocols_than_the_cap_when_joining_should_answer_invalid_request() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let names: Vec<String> = (0..=MAX_PROTOCOLS_PER_MEMBER)
            .map(|index| format!("p{index}"))
            .collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();

        let over_cap = join_step(&mut groups, &config, &request("", &names), now);
        assert_eq!(error_of(&over_cap), ERROR_INVALID_REQUEST);
        assert!(groups.is_empty(), "a rejected join must not create a group");

        let at_cap = join_step(
            &mut groups,
            &config,
            &request("", &names[..MAX_PROTOCOLS_PER_MEMBER]),
            now,
        );
        assert_ne!(error_of_or_none(&at_cap), Some(ERROR_INVALID_REQUEST));
    }

    /// A single protocol name past `MAX_LEGACY_STRING_BYTES` fits easily under the aggregate
    /// `max_member_blob_bytes` cap (64 KiB), so only `legacy_strings_too_long` catches it -
    /// otherwise `DescribeGroups` v0-v4 would later fail to encode this name as `protocol_data`
    /// (an i16-length legacy string) and close the connection for an already-stored group.
    #[test]
    fn given_an_overlong_protocol_name_when_joining_should_answer_invalid_request() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let long_name = "p".repeat(MAX_LEGACY_STRING_BYTES + 1);

        let over_limit = join_step(&mut groups, &config, &request("", &[&long_name]), now);
        assert_eq!(error_of(&over_limit), ERROR_INVALID_REQUEST);
        assert!(groups.is_empty(), "a rejected join must not create a group");
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
            supports_skip_assignment: true,
        }
    }

    fn member_id_of(step: &Step<JoinResult>) -> StrBytes {
        match step {
            Step::Respond(result) => result.member_id.clone(),
            Step::Wait { member_id, .. } => member_id.clone(),
        }
    }

    fn error_of_or_none(step: &Step<JoinResult>) -> Option<i16> {
        match step {
            Step::Respond(result) => Some(result.error),
            Step::Wait { .. } => None,
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
        let step = join_resume_step(&mut groups, &group_id(), &leader, None, woken);

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

    /// Two calls can be in flight for one member id and only one snapshot is minted, so the
    /// loser finds nothing. Once the barrier has closed there is nothing left to wait for, and
    /// re-parking would be permanent: the resume refreshes the session, so nothing would ever
    /// reap it, and it would hold a connection, a permit and a member slot until the process
    /// died. Its group could never empty, so reclamation could not free it either.
    #[test]
    fn given_a_waiter_with_no_answer_when_the_barrier_has_closed_should_be_answered_not_parked() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(
            &mut groups,
            &config,
            &sync_request(&leader, generation),
            now,
        );
        assert_eq!(groups[&group_id()].phase, Phase::Stable);

        // The follower collects its answer, which is destructive.
        let first = join_resume_step(&mut groups, &group_id(), &follower, None, now);
        assert!(matches!(first, Step::Respond(_)));

        // A second in-flight call for the same member now finds no snapshot at all.
        let stranded = join_resume_step(&mut groups, &group_id(), &follower, None, now);

        assert!(
            matches!(stranded, Step::Respond(_)),
            "a waiter with no barrier to wait on must be answered; parking it again is permanent"
        );
    }

    /// A client derives `rebalance_timeout` from `max.poll.interval.ms`, which no broker
    /// range-checks and which slow-processing deployments raise well past any gateway ceiling.
    /// Rejecting is fatal on the Java client and an endless rejoin on librdkafka, so the value is
    /// clamped instead: the member is admitted and only what it contributes to a deadline is
    /// bounded.
    #[test]
    fn given_a_rebalance_timeout_beyond_the_ceiling_when_joining_should_be_clamped_not_rejected() {
        let config = config();
        let mut groups = Groups::new();
        let beyond = config.max_rebalance_timeout.as_secs() * 3;

        let step = join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 10, beyond),
            Instant::now(),
        );

        let member_id = member_id_of(&step);
        assert_ne!(
            error_of_or_none(&step),
            Some(ERROR_INVALID_SESSION_TIMEOUT),
            "a long max.poll.interval.ms is a legal tuning every real broker accepts"
        );
        assert_eq!(
            groups[&group_id()].members[&member_id].rebalance_timeout,
            config.max_rebalance_timeout,
            "the value must be bounded for deadline purposes even though the join succeeds"
        );
    }

    /// `protocol_type` is echoed on every list row. 255 is the cap; one byte over must not be
    /// stored, and 255 itself still joins.
    #[test]
    fn given_a_protocol_type_past_255_bytes_when_joining_should_reject_it() {
        let config = config();
        let mut groups = Groups::new();
        let mut request = request("", &["range"]);
        request.protocol_type = StrBytes::from_string("p".repeat(MAX_PROTOCOL_TYPE_BYTES + 1));

        let rejected = join_step(&mut groups, &config, &request, Instant::now());

        assert_eq!(error_of(&rejected), ERROR_INVALID_REQUEST);
        assert!(
            groups.is_empty(),
            "an overlong protocol_type must not be stored"
        );

        request.protocol_type = StrBytes::from_string("p".repeat(MAX_PROTOCOL_TYPE_BYTES));
        let admitted = join_step(&mut Groups::new(), &config, &request, Instant::now());
        assert_ne!(error_of(&admitted), ERROR_INVALID_REQUEST);
    }

    /// `protocol_type` is retained on the group, so it has to count toward `max_member_blob_bytes`.
    /// The metadata below fits that cap on its own.
    #[test]
    fn given_a_protocol_type_that_overflows_the_retention_cap_when_joining_should_reject_it() {
        let config = GroupCoordinatorConfig {
            max_member_blob_bytes: 16,
            ..config()
        };
        let mut groups = Groups::new();
        let mut request = request("", &["range"]);
        request.protocol_type = StrBytes::from_static_str("consumer");
        request.protocols = vec![(StrBytes::from_static_str("range"), Bytes::from(vec![1; 10]))];

        let step = join_step(&mut groups, &config, &request, Instant::now());

        assert_eq!(error_of(&step), ERROR_INVALID_REQUEST);
        assert!(groups.is_empty());
    }

    /// The generated member id is `{group_instance_id}-{uuid}`. The suffix has to be inside the
    /// same legacy-string cap, or `DescribeGroups` v0-v4 cannot encode `member_id`.
    #[test]
    fn given_an_instance_id_that_leaves_no_room_for_the_member_suffix_when_joining_should_reject_it()
     {
        let config = config();
        let mut groups = Groups::new();
        let prefix_len = MAX_LEGACY_STRING_BYTES - MEMBER_ID_SUFFIX_BYTES;
        let mut request = request("", &["range"]);
        request.group_instance_id = Some(StrBytes::from_string("i".repeat(prefix_len + 1)));

        let rejected = join_step(&mut groups, &config, &request, Instant::now());

        assert_eq!(error_of(&rejected), ERROR_INVALID_REQUEST);
        assert!(groups.is_empty());

        request.group_instance_id = Some(StrBytes::from_string("i".repeat(prefix_len)));
        let admitted = join_step(&mut Groups::new(), &config, &request, Instant::now());
        assert_ne!(error_of_or_none(&admitted), Some(ERROR_INVALID_REQUEST));
        assert_eq!(member_id_of(&admitted).len(), MAX_LEGACY_STRING_BYTES);
    }

    /// A client-supplied member id is not generated, so its own length is the cap. An instance
    /// id that already has a member id is capped on its own, without reserving the suffix.
    #[test]
    fn given_a_member_id_past_the_legacy_string_cap_when_joining_should_reject_it() {
        let config = config();
        let mut groups = Groups::new();
        let request = request(&"m".repeat(MAX_LEGACY_STRING_BYTES + 1), &["range"]);

        let step = join_step(&mut groups, &config, &request, Instant::now());

        assert_eq!(error_of(&step), ERROR_INVALID_REQUEST);
        assert!(groups.is_empty());
    }

    #[test]
    fn given_a_known_member_id_and_a_max_legacy_instance_id_when_joining_should_not_reserve_the_suffix()
     {
        let config = config();
        let mut groups = Groups::new();
        let mut request = request("known-member", &["range"]);
        request.group_instance_id =
            Some(StrBytes::from_string("i".repeat(MAX_LEGACY_STRING_BYTES)));

        let step = join_step(&mut groups, &config, &request, Instant::now());

        assert_eq!(error_of(&step), ERROR_UNKNOWN_MEMBER_ID);
    }

    /// Repeating an id must not build a second snapshot. A frame of 0 bytes answers that one
    /// group with an error and leaves it in the map.
    #[test]
    fn given_a_repeated_group_id_when_describing_should_tick_and_snapshot_it_once() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let _ = join_step(&mut groups, &config, &request("", &["range"]), now);
        let id = group_id();

        let described = describe_groups(&mut groups, &[id.clone(), id.clone()], 0, usize::MAX, now);
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].as_ref().unwrap().error, ERROR_NONE);

        let refused = describe_groups(&mut groups, std::slice::from_ref(&id), 0, 0, now);
        assert_eq!(
            refused[0].as_ref().unwrap().error,
            ERROR_UNKNOWN_SERVER_ERROR
        );
        assert!(
            groups.contains_key(&id),
            "a per-group size error must not drop the group"
        );
    }

    /// The frame check has to match what `DescribeGroups` actually encodes, header included.
    /// Fields are past the compact-varint boundary and the member has an instance id, which v4+
    /// writes and v0-v3 does not.
    #[test]
    fn given_a_described_group_when_pricing_the_frame_should_match_the_encoder() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let joined = join_wide(&mut groups, &config, now);
        let id = group_id();
        let missing = StrBytes::from_static_str("missing");
        assert_priced(&mut groups, &id, &missing, usize::MAX, now);

        let synced = sync_step(
            &mut groups,
            &config,
            &SyncRequest {
                group_instance_id: None,
                group_id: id.clone(),
                generation_id: joined.generation_id,
                member_id: joined.member_id.clone(),
                protocol_type: None,
                protocol_name: None,
                assignments: vec![(joined.member_id, Bytes::from(vec![b'a'; 130]))],
            },
            now,
        );
        assert_eq!(error_of_sync(&synced), ERROR_NONE);
        assert_eq!(groups[&id].phase, Phase::Stable);
        assert_priced(&mut groups, &id, &missing, usize::MAX, now);
    }

    /// One stable group that does not fit on its own is summarized. The member stays, without
    /// the blobs that made it too big.
    #[test]
    fn given_a_stable_group_one_byte_over_the_frame_when_describing_should_summarize() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        stabilize(&mut groups, &config, now, GROUP, &vec![b'x'; 400]);
        let id = group_id();
        let full = describe_frame_len(0, &groups, std::slice::from_ref(&id), usize::MAX);

        let described = describe_groups(&mut groups, std::slice::from_ref(&id), 0, full - 1, now);
        let group = described[0].as_ref().unwrap();
        assert_eq!(group.error, ERROR_NONE);
        assert_eq!(group.state, GROUP_STATE_STABLE);
        assert!(group.protocol_name.is_none());
        assert_eq!(group.members.len(), 1);
        assert!(group.members[0].metadata.is_empty());
        assert!(group.members[0].assignment.is_empty());
        assert_priced_response(0, &mut groups, &[id], full - 1, now);
    }

    /// The second group fits on its own and does not fit beside the first, so it is an error
    /// rather than a summary. Both groups stay.
    #[test]
    fn given_two_groups_past_the_batch_budget_when_describing_should_error_the_second() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let blob = vec![b'x'; 200];
        stabilize(&mut groups, &config, now, "alpha", &blob);
        stabilize(&mut groups, &config, now, "beta", &blob);
        let first = StrBytes::from_static_str("alpha");
        let second = StrBytes::from_static_str("beta");
        let one = describe_frame_len(0, &groups, std::slice::from_ref(&first), usize::MAX);

        let described = describe_groups(&mut groups, &[first.clone(), second.clone()], 0, one, now);
        assert_eq!(described[0].as_ref().unwrap().error, ERROR_NONE);
        assert!(
            !described[0].as_ref().unwrap().members[0]
                .assignment
                .is_empty()
        );
        assert_eq!(
            described[1].as_ref().unwrap().error,
            ERROR_UNKNOWN_SERVER_ERROR
        );
        assert!(groups.contains_key(&first));
        assert!(groups.contains_key(&second));
        assert_priced_response(0, &mut groups, &[first, second], one, now);
    }

    /// A v4+ join that only reserved a member id has no joined members. Kafka calls that `Empty`.
    #[test]
    fn given_only_a_pending_member_when_describing_should_report_empty() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let mut request = request("", &["range"]);
        request.require_known_member_id = true;
        let step = join_step(&mut groups, &config, &request, now);
        assert_eq!(error_of(&step), ERROR_MEMBER_ID_REQUIRED);

        let listed = list_groups(&groups, now);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].state, GROUP_STATE_EMPTY);

        let described = describe_groups(&mut groups, &[group_id()], 6, usize::MAX, now);
        let group = described[0].as_ref().unwrap();
        assert_eq!(group.error, ERROR_NONE);
        assert_eq!(group.state, GROUP_STATE_EMPTY);
        assert!(group.members.is_empty());
        assert!(group.protocol_name.is_none());
        assert!(groups.contains_key(&group_id()));
    }

    /// List skips a fully expired group and does not tick it. Describe then ticks and the group
    /// is gone, which the handler reports as `Dead`.
    #[test]
    fn given_expired_sessions_when_listing_should_skip_without_ticking() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        stabilize(&mut groups, &config, now, GROUP, b"assign");
        let id = group_id();
        assert_eq!(list_groups(&groups, now).len(), 1);

        let later = now + Duration::from_secs(11);
        assert!(list_groups(&groups, later).is_empty());
        assert_eq!(groups[&id].phase, Phase::Stable);
        assert!(!groups[&id].members.is_empty());

        let described =
            describe_groups(&mut groups, std::slice::from_ref(&id), 6, usize::MAX, later);
        assert!(described[0].is_none());
        assert!(!groups.contains_key(&id));
    }

    fn assert_priced(
        groups: &mut Groups,
        id: &StrBytes,
        missing: &StrBytes,
        max_frame_size: usize,
        now: Instant,
    ) {
        for version in 0..=6 {
            for ids in [
                vec![id.clone()],
                vec![missing.clone()],
                vec![id.clone(), missing.clone()],
            ] {
                assert_priced_response(version, groups, &ids, max_frame_size, now);
            }
        }
    }

    fn assert_priced_response(
        version: i16,
        groups: &mut Groups,
        ids: &[StrBytes],
        max_frame_size: usize,
        now: Instant,
    ) {
        let priced = describe_frame_len(version, groups, ids, max_frame_size);
        let described = describe_groups(groups, ids, version, max_frame_size, now);
        let encoded = encode_described(version, &dedup_first_seen(ids), &described);
        let header = kafka_response_header_len(API_KEY_DESCRIBE_GROUPS, version);
        assert_eq!(
            priced,
            header + encoded.len(),
            "version {version} ids {} priced {priced} encoded {}",
            ids.len(),
            header + encoded.len()
        );
    }

    fn encode_described(
        version: i16,
        ids: &[StrBytes],
        described: &[Option<GroupDescription>],
    ) -> Bytes {
        let groups = ids
            .iter()
            .zip(described)
            .map(|(group_id, group)| {
                group.clone().map_or_else(
                    || missing_group(version, group_id),
                    |group| described_group(version, group),
                )
            })
            .collect();
        encode_response(version, groups).expect("described group encodes")
    }

    fn join_wide(groups: &mut Groups, config: &GroupCoordinatorConfig, now: Instant) -> JoinResult {
        let wide = "x".repeat(130);
        let mut request = request("", &["range"]);
        request.group_instance_id = Some(StrBytes::from_string(wide.clone()));
        request.protocols = vec![(StrBytes::from_string(wide), Bytes::from(vec![b'm'; 130]))];
        let step = join_step(groups, config, &request, now);
        let Step::Respond(result) = step else {
            panic!("an immediate join must answer");
        };
        assert_eq!(result.error, ERROR_NONE);
        assert!(
            result.member_id.len() >= 127,
            "instance id plus the member suffix must cross the compact varint boundary"
        );
        result
    }

    fn stabilize(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        now: Instant,
        group: &str,
        assignment: &[u8],
    ) {
        let step = join_step(groups, config, &request_for(group, ""), now);
        let Step::Respond(joined) = step else {
            panic!("an immediate join must answer");
        };
        assert_eq!(joined.error, ERROR_NONE, "{group}");
        let synced = sync_step(
            groups,
            config,
            &SyncRequest {
                group_instance_id: None,
                group_id: StrBytes::from_string(group.to_owned()),
                generation_id: joined.generation_id,
                member_id: joined.member_id.clone(),
                protocol_type: None,
                protocol_name: None,
                assignments: vec![(joined.member_id, Bytes::copy_from_slice(assignment))],
            },
            now,
        );
        assert_eq!(error_of_sync(&synced), ERROR_NONE, "{group}");
    }

    fn error_of_sync(step: &Step<SyncResult>) -> i16 {
        match step {
            Step::Respond(result) => result.error,
            Step::Wait { .. } => panic!("expected a sync response, not a park"),
        }
    }

    /// The cap has to measure everything a member retains. Protocol names are kept alongside the
    /// metadata, so counting metadata alone lets a request declare almost none while pinning
    /// megabytes, which is what `max_member_blob_bytes` exists to prevent.
    #[test]
    fn given_long_protocol_names_when_joining_should_count_against_the_retention_cap() {
        let config = config();
        let mut groups = Groups::new();
        let long_name = "n".repeat(config.max_member_blob_bytes + 1);
        let request = JoinRequest {
            protocols: vec![(
                StrBytes::from_string(long_name),
                Bytes::from_static(b"tiny"),
            )],
            ..request("", &["x"])
        };

        let step = join_step(&mut groups, &config, &request, Instant::now());

        assert_eq!(
            error_of(&step),
            ERROR_INVALID_REQUEST,
            "a name is retained just like metadata, so it has to be counted like metadata"
        );
    }

    /// What a member keeps must be its own allocation. `StrBytes` and `Bytes` are refcounted
    /// views into the request frame, so retaining them verbatim keeps the entire frame alive for
    /// the member's lifetime, however little of it the caps actually measured.
    #[test]
    fn given_a_join_when_a_member_is_retained_should_not_hold_the_request_buffer() {
        let config = config();
        let mut groups = Groups::new();
        let frame = Bytes::from(vec![7u8; 4096]);
        let metadata = frame.slice(0..8);
        let request = JoinRequest {
            protocols: vec![(StrBytes::from_static_str("x"), metadata)],
            ..request("", &["x"])
        };

        let step = join_step(&mut groups, &config, &request, Instant::now());
        let member_id = member_id_of(&step);
        let retained = &groups[&group_id()].members[&member_id].protocols[0].1;

        assert_eq!(
            retained.as_ref(),
            &[7u8; 8],
            "the bytes themselves must survive"
        );
        assert!(
            !std::ptr::eq(retained.as_ptr(), frame.as_ptr()),
            "a retained member must own its bytes, not pin the frame they arrived in"
        );
    }

    /// Map keys are retained exactly like member payloads, and a pending id comes back as the
    /// client's copy of the one it was handed.
    #[test]
    fn given_a_join_when_a_group_and_member_are_created_should_not_key_them_by_the_request_buffer()
    {
        let config = config();
        let mut groups = Groups::new();
        let frame = Bytes::from_static(b"g consumer");
        let first = JoinRequest {
            group_id: StrBytes::from_utf8(frame.slice(0..1)).unwrap(),
            protocol_type: StrBytes::from_utf8(frame.slice(2..10)).unwrap(),
            require_known_member_id: true,
            ..request("", &["x"])
        };
        let issued = member_id_of(&join_step(&mut groups, &config, &first, Instant::now()));

        let echoed_frame = Bytes::from(issued.as_bytes().to_vec());
        let rejoin = JoinRequest {
            member_id: StrBytes::from_utf8(echoed_frame.clone()).unwrap(),
            ..first
        };
        let _ = join_step(&mut groups, &config, &rejoin, Instant::now());

        let within = |buffer: &Bytes, ptr: *const u8| buffer.as_ptr_range().contains(&ptr);
        let (group_key, group) = groups.iter().next().unwrap();
        let (member_key, _) = group.members.iter().next().unwrap();
        assert!(
            !within(&frame, group_key.as_ptr()),
            "group id pins the frame"
        );
        assert!(
            !within(&frame, group.protocol_type.as_ptr()),
            "protocol type pins the frame"
        );
        assert!(
            !within(&echoed_frame, member_key.as_ptr()),
            "an admitted pending id pins the frame it was echoed in"
        );
    }

    /// Each member fits `max_member_blob_bytes` on its own, so only a group-level bound stops
    /// the leader's roster outgrowing the frame size.
    #[test]
    fn given_a_full_roster_when_another_member_joins_should_reject_it_as_group_full() {
        let metadata = Bytes::from(vec![0u8; 100]);
        let with_metadata = |member_id: &str| JoinRequest {
            protocols: vec![(StrBytes::from_static_str("x"), metadata.clone())],
            ..request(member_id, &["x"])
        };
        let first = with_metadata("");
        let config = GroupCoordinatorConfig {
            max_group_roster_bytes: 150 + 100,
            ..config()
        };
        let mut groups = Groups::new();
        let now = Instant::now();

        let admitted = join_step(&mut groups, &config, &first, now);
        assert!(matches!(admitted, Step::Respond(ref result) if result.error == ERROR_NONE));
        let rejected = join_step(&mut groups, &config, &with_metadata(""), now);

        let Step::Respond(result) = rejected else {
            panic!("a capacity rejection must answer, not park");
        };
        assert_eq!(result.error, ERROR_GROUP_MAX_SIZE_REACHED);
        assert_eq!(groups[&group_id()].members.len(), 1);
    }

    #[test]
    fn given_an_oversized_roster_entry_when_it_creates_the_group_should_not_leave_it_behind() {
        let config = GroupCoordinatorConfig {
            max_group_roster_bytes: 1,
            ..config()
        };
        let mut groups = Groups::new();

        let outcome = join_step(&mut groups, &config, &request("", &["x"]), Instant::now());

        let Step::Respond(result) = outcome else {
            panic!("a capacity rejection must answer, not park");
        };
        assert_eq!(result.error, ERROR_GROUP_MAX_SIZE_REACHED);
        assert!(groups.is_empty());
    }

    /// The park bound has to be asserted on what the waiter is actually told, because that is
    /// what the coordinator sleeps on. Asserting `wake_at()` alone leaves every park site free to
    /// return any deadline it likes.
    #[test]
    fn given_a_parked_waiter_should_be_told_a_wake_time_within_its_session() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let _leader = join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 10, 300),
            now,
        );
        let step = join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 10, 300),
            now,
        );

        let Step::Wait { wake_at, .. } = step else {
            panic!("the second joiner waits on the barrier");
        };
        assert!(
            wake_at <= now + Duration::from_secs(10),
            "the wake time handed to the coordinator must stay inside the session timeout"
        );
    }

    /// The stranded-waiter answer has to be usable. Telling it `UNKNOWN_MEMBER_ID`, or handing it
    /// generation -1, sends a live consumer into a reset or an `ILLEGAL_GENERATION` on its next
    /// `SyncGroup`.
    #[test]
    fn given_a_stranded_waiter_when_answered_should_receive_a_usable_generation() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(
            &mut groups,
            &config,
            &sync_request(&leader, generation),
            now,
        );

        let _collected = join_resume_step(&mut groups, &group_id(), &follower, None, now);
        let stranded = join_resume_step(&mut groups, &group_id(), &follower, None, now);

        let Step::Respond(result) = stranded else {
            panic!("a waiter with no barrier to wait on must be answered");
        };
        assert_eq!(result.error, ERROR_NONE);
        assert_eq!(
            result.generation_id, generation,
            "must name a live generation"
        );
        assert_eq!(
            result.leader, leader,
            "the client needs to know who assigns"
        );
        assert_eq!(
            result.member_id, follower,
            "must answer the member that asked"
        );
        assert!(
            result.protocol_name.is_some(),
            "the client checks the protocol"
        );
    }

    /// A stranded waiter in `CompletingRebalance` must be answered too. Re-parking it there leaves
    /// it for the sync-deadline sweep, which evicts it as unsynced: it loses its slot instead of
    /// receiving its generation.
    #[test]
    fn given_a_stranded_waiter_while_completing_should_be_answered_not_reparked() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        assert_eq!(groups[&group_id()].phase, Phase::CompletingRebalance);

        let _collected = join_resume_step(&mut groups, &group_id(), &follower, None, now);
        let stranded = join_resume_step(&mut groups, &group_id(), &follower, None, now);

        assert!(
            matches!(stranded, Step::Respond(_)),
            "the barrier has closed, so there is nothing left to wait for in this phase either"
        );
    }

    /// A rejected join must not remove a group that was already there with live members.
    #[test]
    fn given_an_incompatible_join_when_the_group_exists_should_leave_it_intact() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _follower) = two_members(&mut groups, &config, &["x"], &["x"], now);

        let step = join_step(&mut groups, &config, &request("", &["z"]), now);

        assert_eq!(error_of(&step), ERROR_INCONSISTENT_GROUP_PROTOCOL);
        assert!(
            groups[&group_id()].members.contains_key(&leader),
            "rejecting a newcomer must not delete the group its members are using"
        );
    }

    /// A follower that changes its subscription must open a rebalance; replaying the generation
    /// would discard the new subscription until some unrelated member triggered one.
    #[test]
    fn given_a_follower_changing_protocols_while_stable_should_open_a_rebalance() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(
            &mut groups,
            &config,
            &sync_request(&leader, generation),
            now,
        );
        assert_eq!(groups[&group_id()].phase, Phase::Stable);

        let _ = join_step(
            &mut groups,
            &config,
            &request(follower.as_str(), &["x", "y"]),
            now,
        );

        assert_eq!(
            groups[&group_id()].phase,
            Phase::PreparingRebalance,
            "a changed subscription must reach the assignor, which needs a new generation"
        );
    }

    fn sync_request(member_id: &StrBytes, generation_id: i32) -> SyncRequest {
        SyncRequest {
            group_instance_id: None,
            group_id: group_id(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: None,
            protocol_name: None,
            assignments: Vec::new(),
        }
    }

    /// The member cap counts across every group, so a dead group holding member slots wedges it
    /// exactly as a dead group wedges the group cap. Pinning one reclaim site does not pin both.
    #[test]
    fn given_a_dead_group_holding_member_slots_when_a_new_group_joins_should_reclaim_them() {
        let config = GroupCoordinatorConfig {
            max_total_members: 1,
            max_groups: 10,
            ..config()
        };
        let mut groups = Groups::new();
        let start = Instant::now();
        assert!(matches!(
            join_step(&mut groups, &config, &request_for("dead", ""), start),
            Step::Respond(_)
        ));

        let later = start + Duration::from_secs(3600);
        let Step::Respond(result) =
            join_step(&mut groups, &config, &request_for("fresh", ""), later)
        else {
            panic!("a join that reclaims a dead group must answer, not park");
        };
        assert_eq!(
            result.error, ERROR_NONE,
            "the total-member cap must count live members, not ones no request will ever tick"
        );
        assert!(!groups.contains_key(&StrBytes::from_static_str("dead")));
    }

    /// The sync barrier has the same problem the join barrier had: a member parked inside
    /// `SyncGroup` cannot heartbeat, and Kafka's defaults put the session well inside the
    /// rebalance window.
    #[test]
    fn given_a_parked_sync_waiter_when_its_own_session_passes_should_stay_a_member() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let leader = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 600, 300),
            now,
        ));
        let follower = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 10, 300),
            now,
        ));
        let _ = join_step(
            &mut groups,
            &config,
            &request_with(leader.as_str(), &["x"], 600, 300),
            now,
        );
        let generation = groups[&group_id()].generation_id;

        let parked = sync_step(
            &mut groups,
            &config,
            &sync_request(&follower, generation),
            now,
        );
        let Step::Wait { wake_at, .. } = parked else {
            panic!("a follower waits for the leader's assignment");
        };
        assert!(
            wake_at <= now + Duration::from_secs(10),
            "the wake time handed to the coordinator must stay inside the session timeout"
        );

        let woken = now + Duration::from_secs(11);
        let step = sync_resume_step(&mut groups, &group_id(), &follower, None, generation, woken);

        assert!(
            groups[&group_id()].members.contains_key(&follower),
            "a member parked inside SyncGroup cannot heartbeat and must not be evicted for it"
        );
        let Step::Wait { wake_at, .. } = step else {
            panic!("the assignment has not arrived, so the follower waits again");
        };
        assert!(
            wake_at <= woken + Duration::from_secs(10),
            "a resumed park must also stay inside the session timeout"
        );
    }

    /// A member rejoining an open barrier wants the NEXT generation. Handing it the previous
    /// generation's uncollected answer lets it leave while the barrier still counts it.
    #[test]
    fn given_a_rejoin_into_an_open_barrier_should_drop_the_uncollected_answer() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;
        assert!(
            groups[&group_id()].members[&follower]
                .join_response
                .is_some()
        );

        let _ = join_step(&mut groups, &config, &request("", &["x"]), now);
        assert_eq!(groups[&group_id()].phase, Phase::PreparingRebalance);

        match join_step(
            &mut groups,
            &config,
            &request(follower.as_str(), &["x"]),
            now,
        ) {
            Step::Wait { .. } => {}
            Step::Respond(result) => panic!(
                "a rejoin into an open barrier must park for the next generation, not replay \
                 generation {} it never collected",
                result.generation_id
            ),
        }
        assert_eq!(groups[&group_id()].generation_id, generation);
    }

    /// A leader rejoining a stable group is how a Kafka client signals new topic metadata.
    /// Replaying the generation instead would leave new partitions unassigned, with no error.
    #[test]
    fn given_a_leader_rejoin_while_stable_should_open_a_rebalance() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(
            &mut groups,
            &config,
            &sync_request(&leader, generation),
            now,
        );
        assert_eq!(groups[&group_id()].phase, Phase::Stable);

        let _ = join_step(&mut groups, &config, &request(leader.as_str(), &["x"]), now);

        assert_eq!(
            groups[&group_id()].phase,
            Phase::PreparingRebalance,
            "a leader rejoin in Stable must open a rebalance, not replay the generation"
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
            None,
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
            None,
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
            None,
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
            None,
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

    fn leave(
        groups: &mut Groups,
        identities: &[(&str, Option<&str>)],
        now: Instant,
    ) -> LeaveResult {
        let request = LeaveRequest {
            group_id: group_id(),
            members: identities
                .iter()
                .map(|(member_id, instance_id)| LeavingMember {
                    member_id: StrBytes::from_string((*member_id).to_owned()),
                    group_instance_id: instance_id.map(|id| StrBytes::from_string(id.to_owned())),
                })
                .collect(),
        };
        leave_step(groups, &request, now)
    }

    fn codes(result: &LeaveResult) -> Vec<i16> {
        result.members.iter().map(|member| member.error).collect()
    }

    fn static_request(instance_id: &str) -> JoinRequest {
        JoinRequest {
            group_instance_id: Some(StrBytes::from_string(instance_id.to_owned())),
            ..request("", &["x"])
        }
    }

    fn pending_request() -> JoinRequest {
        JoinRequest {
            require_known_member_id: true,
            ..request("", &["x"])
        }
    }

    /// `two_members` carried through the leader's `SyncGroup`, so the group is Stable.
    fn stable_two_members(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        now: Instant,
    ) -> (StrBytes, StrBytes) {
        let (leader, follower) = two_members(groups, config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(groups, config, &sync_request(&leader, generation), now);
        let _ = sync_step(groups, config, &sync_request(&follower, generation), now);
        assert_eq!(groups[&group_id()].phase, Phase::Stable);
        (leader, follower)
    }

    /// The generation must hold: a bumped one turns the survivor's heartbeat into
    /// `ILLEGAL_GENERATION`, which a Java client handles as lost partitions instead of a rejoin.
    #[test]
    fn given_a_member_leaving_a_stable_group_should_open_a_rebalance_without_bumping_the_generation()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;

        let result = leave(&mut groups, &[(follower.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::PreparingRebalance);
        assert_eq!(group.generation_id, generation);
        assert!(!group.members.contains_key(&follower));
        assert_eq!(
            heartbeat_step(&mut groups, &group_id(), generation, &leader, None, now),
            ERROR_REBALANCE_IN_PROGRESS
        );
    }

    /// Asserted with no time advance: `reclaim_expired` would free the slot later anyway, so
    /// only an immediate check proves the leave removed the group itself.
    #[test]
    fn given_the_last_member_leaving_should_remove_the_group_immediately() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let member = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));

        let result = leave(&mut groups, &[(member.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        assert!(!groups.contains_key(&group_id()));
    }

    #[test]
    fn given_a_leave_for_an_unknown_group_should_not_create_it() {
        let mut groups = Groups::new();

        let result = leave(
            &mut groups,
            &[("ghost", None), ("", Some("instance"))],
            Instant::now(),
        );

        assert_eq!(result.error, ERROR_NONE);
        assert_eq!(
            codes(&result),
            vec![ERROR_UNKNOWN_MEMBER_ID, ERROR_UNKNOWN_MEMBER_ID]
        );
        assert!(groups.is_empty());
    }

    /// Checked straight after `leave_step`: a woken waiter's own tick would also complete the
    /// join, so only this proves the leave did.
    #[test]
    fn given_the_last_straggler_leaving_a_preparing_group_should_complete_the_join() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;
        let rejoin = join_step(&mut groups, &config, &request(leader.as_str(), &["x"]), now);
        assert!(matches!(rejoin, Step::Wait { .. }));

        let _ = leave(&mut groups, &[(follower.as_str(), None)], now);

        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::CompletingRebalance);
        assert_eq!(group.generation_id, generation + 1);
        let answer = group.members[&leader]
            .join_response
            .as_ref()
            .expect("the completed join must leave the leader its answer");
        let roster: Vec<&StrBytes> = answer.members.iter().map(|m| &m.member_id).collect();
        assert_eq!(roster, vec![&leader]);
    }

    /// Without the rebalance the survivor becomes leader of a generation whose roster it never
    /// received, and its `SyncGroup` would fan out an empty assignment.
    #[test]
    fn given_the_leader_leaving_while_completing_should_open_a_rebalance() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let generation = groups[&group_id()].generation_id;
        assert_eq!(groups[&group_id()].phase, Phase::CompletingRebalance);

        let _ = leave(&mut groups, &[(leader.as_str(), None)], now);

        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::PreparingRebalance);
        assert_eq!(group.leader.as_ref(), Some(&follower));
        let Step::Respond(sync) = sync_step(
            &mut groups,
            &config,
            &sync_request(&follower, generation),
            now,
        ) else {
            panic!("a sync during a rebalance is answered, not parked");
        };
        assert_eq!(sync.error, ERROR_REBALANCE_IN_PROGRESS);
    }

    #[test]
    fn given_a_leave_while_stable_should_size_the_join_window_from_the_remaining_members() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let survivor = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 600, 5),
            now,
        ));
        let leaver = member_id_of(&join_step(
            &mut groups,
            &config,
            &request_with("", &["x"], 600, 300),
            now,
        ));
        let _ = join_step(
            &mut groups,
            &config,
            &request_with(survivor.as_str(), &["x"], 600, 5),
            now,
        );
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(
            &mut groups,
            &config,
            &sync_request(&survivor, generation),
            now,
        );
        assert_eq!(groups[&group_id()].phase, Phase::Stable);

        let _ = leave(&mut groups, &[(leaver.as_str(), None)], now);

        assert_eq!(
            groups[&group_id()].join_deadline,
            Some(now + Duration::from_secs(5)),
            "the leaver's own rebalance timeout must not stretch the survivors' join window"
        );
    }

    /// A pending id never joined, so dropping it must not disturb a Stable group.
    #[test]
    fn given_a_pending_member_leaving_a_stable_group_should_not_open_a_rebalance() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let _ = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));
        assert!(groups[&group_id()].pending.contains_key(&pending));

        let result = leave(&mut groups, &[(pending.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::Stable);
        assert_eq!(group.generation_id, generation);
        assert!(group.pending.is_empty());
    }

    #[test]
    fn given_a_pending_member_leaving_a_preparing_group_should_unblock_the_join() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));
        let _ = join_step(&mut groups, &config, &request(leader.as_str(), &["x"]), now);
        let _ = join_step(
            &mut groups,
            &config,
            &request(follower.as_str(), &["x"]),
            now,
        );
        assert_eq!(
            groups[&group_id()].phase,
            Phase::PreparingRebalance,
            "the outstanding pending id must be what holds the barrier open"
        );

        let result = leave(&mut groups, &[(pending.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::CompletingRebalance);
        assert_eq!(group.generation_id, generation + 1);
    }

    #[test]
    fn given_a_leave_by_instance_id_without_a_member_id_should_remove_that_static_member() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let static_member = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let dynamic = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));

        let result = leave(&mut groups, &[("", Some("i-a"))], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&static_member));
        assert!(group.members.contains_key(&dynamic));
    }

    /// The admin `removeMembersFromConsumerGroup` path: removing a holder must open a rebalance
    /// and wake the survivors, or the group stays Stable with the removed member's partitions
    /// assigned to nobody.
    #[test]
    fn given_a_stable_group_when_a_holder_leaves_by_instance_id_alone_should_open_a_rebalance_and_wake_waiters()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;
        let group = groups.get_mut(&group_id()).expect("the stable group");
        let mut member = group.members.remove(&follower).expect("the follower");
        member.group_instance_id = Some(StrBytes::from_static_str("i-f"));
        group.insert_member(follower.clone(), member);
        let mut changed = group.subscribe();
        changed.mark_unchanged();

        let result = leave(&mut groups, &[("", Some("i-f"))], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        assert!(
            changed.has_changed().expect("the group is still live"),
            "survivor waiters must be woken"
        );
        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&follower));
        assert_eq!(group.phase, Phase::PreparingRebalance);
        assert_eq!(
            heartbeat_step(&mut groups, &group_id(), generation, &leader, None, now),
            ERROR_REBALANCE_IN_PROGRESS
        );
    }

    /// What Java `CloseOptions.LEAVE_GROUP` and Kafka Streams send: the member's own pair. It is
    /// the holder, so it must leave cleanly rather than be fenced.
    #[test]
    fn given_a_static_member_when_it_leaves_with_its_own_member_and_instance_id_should_leave() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let static_member = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let dynamic = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));

        let result = leave(&mut groups, &[(static_member.as_str(), Some("i-a"))], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&static_member));
        assert!(group.members.contains_key(&dynamic));
    }

    /// The returning static member replaces its old incarnation, so the instance-only leave
    /// takes the one holder and a repeat in the same request finds nobody left holding the id.
    #[test]
    fn given_a_replaced_static_member_when_one_request_leaves_its_instance_id_twice_should_answer_the_repeat_unknown_member_id()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let first = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let second = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        assert_ne!(first, second);
        assert!(!groups[&group_id()].members.contains_key(&first));

        let result = leave(&mut groups, &[("", Some("i-a")), ("", Some("i-a"))], now);

        assert_eq!(codes(&result), vec![ERROR_NONE, ERROR_UNKNOWN_MEMBER_ID]);
        assert!(!groups.contains_key(&group_id()));
    }

    /// A rebalance with no members would complete at once, clear `pending` and drop the group,
    /// so the pending client's rejoin would be refused. Session expiry in `tick` holds the same
    /// guard.
    #[test]
    fn given_a_pending_member_when_the_last_real_member_leaves_should_still_admit_the_pending_rejoin()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let member = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(
            &mut groups,
            &config,
            &sync_request(&member, generation),
            now,
        );
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));
        assert!(groups[&group_id()].pending.contains_key(&pending));

        let result = leave(&mut groups, &[(member.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        assert!(groups[&group_id()].pending.contains_key(&pending));
        let Step::Respond(rejoined) = join_step(
            &mut groups,
            &config,
            &request(pending.as_str(), &["x"]),
            now,
        ) else {
            panic!("the pending rejoin must be answered, not parked");
        };
        assert_eq!(rejoined.error, ERROR_NONE);
        assert!(groups[&group_id()].members.contains_key(&pending));
    }

    /// Kafka keeps pending join members across the round that empties the group, so a pending
    /// client that rejoins after the old join window would have closed must still be admitted.
    #[test]
    fn given_a_pending_member_when_the_last_member_leaves_mid_rebalance_should_admit_the_rejoin_after_the_join_window()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_two_members(&mut groups, &config, now);
        let _ = leave(&mut groups, &[(follower.as_str(), None)], now);
        assert_eq!(groups[&group_id()].phase, Phase::PreparingRebalance);
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));

        let result = leave(&mut groups, &[(leader.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        let later = now + Duration::from_secs(6);
        let Step::Respond(rejoined) = join_step(
            &mut groups,
            &config,
            &request(pending.as_str(), &["x"]),
            later,
        ) else {
            panic!("the pending rejoin must be answered, not parked");
        };
        assert_eq!(rejoined.error, ERROR_NONE);
        assert!(groups[&group_id()].members.contains_key(&pending));
    }

    /// The leave ticks the group before removing anyone, so a window already past its deadline
    /// closes first and drops the member that never rejoined. Its own leave then finds it gone.
    #[test]
    fn given_a_pending_member_when_the_last_member_leaves_after_the_join_window_should_admit_the_rejoin()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_two_members(&mut groups, &config, now);
        let _ = leave(&mut groups, &[(follower.as_str(), None)], now);
        let join_deadline = groups[&group_id()]
            .join_deadline
            .expect("the leave must open a join window");
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));

        let result = leave(&mut groups, &[(leader.as_str(), None)], join_deadline);

        assert_eq!(codes(&result), vec![ERROR_UNKNOWN_MEMBER_ID]);
        assert!(groups[&group_id()].pending.contains_key(&pending));
        let Step::Respond(rejoined) = join_step(
            &mut groups,
            &config,
            &request(pending.as_str(), &["x"]),
            join_deadline,
        ) else {
            panic!("the pending rejoin must be answered, not parked");
        };
        assert_eq!(rejoined.error, ERROR_NONE);
        assert!(groups[&group_id()].members.contains_key(&pending));
    }

    /// Same wipe through session expiry: the tick evicts the last member and closes the overdue
    /// window in one pass.
    #[test]
    fn given_a_pending_member_when_the_last_member_expires_after_the_join_window_should_keep_the_pending_id()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_, follower) = stable_two_members(&mut groups, &config, now);
        let _ = leave(&mut groups, &[(follower.as_str(), None)], now);
        let pending = member_id_of(&join_step(
            &mut groups,
            &config,
            &JoinRequest {
                session_timeout: Duration::from_secs(60),
                ..pending_request()
            },
            now,
        ));
        let later = now + Duration::from_secs(11);

        assert!(tick_group(&mut groups, &group_id(), later));
        assert!(groups[&group_id()].members.is_empty());
        assert!(groups[&group_id()].pending.contains_key(&pending));
    }

    #[test]
    fn given_a_stable_group_when_only_a_pending_member_leaves_should_wake_waiters() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let _ = stable_two_members(&mut groups, &config, now);
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));
        let mut changed = groups[&group_id()].subscribe();
        changed.mark_unchanged();

        let result = leave(&mut groups, &[(pending.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        assert!(changed.has_changed().expect("the group is still live"));
        assert_eq!(groups[&group_id()].phase, Phase::Stable);
    }

    #[test]
    fn given_a_leave_whose_member_id_does_not_hold_the_instance_id_should_be_fenced() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let holder = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let other = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-b"),
            now,
        ));

        let result = leave(&mut groups, &[(other.as_str(), Some("i-a"))], now);

        assert_eq!(codes(&result), vec![ERROR_FENCED_INSTANCE_ID]);
        let group = &groups[&group_id()];
        assert!(group.members.contains_key(&holder));
        assert!(group.members.contains_key(&other));
    }

    #[test]
    fn given_a_leave_by_an_instance_id_nobody_holds_should_return_unknown_member_id() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let dynamic = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));

        let result = leave(&mut groups, &[(dynamic.as_str(), Some("i-z"))], now);

        assert_eq!(codes(&result), vec![ERROR_UNKNOWN_MEMBER_ID]);
        assert!(groups[&group_id()].members.contains_key(&dynamic));
    }

    /// The Java client's `CloseOptions.LEAVE_GROUP` sends a static member's id with no instance id.
    #[test]
    fn given_a_static_member_leaving_by_member_id_alone_should_be_removed() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let static_member = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let dynamic = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));

        let result = leave(&mut groups, &[(static_member.as_str(), None)], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&static_member));
        assert!(group.members.contains_key(&dynamic));
    }

    /// A restarted static consumer replaces its old incarnation, so a leave by instance id
    /// removes the one holder and answers one entry echoing the request identity.
    #[test]
    fn given_a_replaced_static_member_when_left_by_instance_should_remove_only_its_holder() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let first = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let second = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let bystander = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));
        assert_ne!(first, second);
        assert!(!groups[&group_id()].members.contains_key(&first));

        let result = leave(&mut groups, &[("", Some("i-a"))], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        assert_eq!(result.members[0].member_id.as_str(), "");
        assert_eq!(
            result.members[0].group_instance_id.as_deref(),
            Some("i-a"),
            "the answer echoes the request identity, not the holder it resolved to"
        );
        let remaining: Vec<&StrBytes> = groups[&group_id()].members.keys().collect();
        assert_eq!(remaining, vec![&bystander]);
    }

    #[test]
    fn given_a_static_member_left_by_member_id_when_the_same_request_leaves_its_instance_should_answer_unknown_member_id()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let static_member = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-a"),
            now,
        ));
        let _dynamic = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));

        let result = leave(
            &mut groups,
            &[(static_member.as_str(), None), ("", Some("i-a"))],
            now,
        );

        assert_eq!(codes(&result), vec![ERROR_NONE, ERROR_UNKNOWN_MEMBER_ID]);
    }

    #[test]
    fn given_a_member_left_when_its_old_join_waiter_resumes_should_answer_unknown_member_id() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_leader, follower) = stable_two_members(&mut groups, &config, now);
        let parked = join_step(
            &mut groups,
            &config,
            &request(follower.as_str(), &["x", "y"]),
            now,
        );
        assert!(matches!(parked, Step::Wait { .. }));

        let _ = leave(&mut groups, &[(follower.as_str(), None)], now);
        let resumed = join_resume_step(&mut groups, &group_id(), &follower, None, now);

        assert_eq!(error_of(&resumed), ERROR_UNKNOWN_MEMBER_ID);
        assert!(!groups[&group_id()].members.contains_key(&follower));
    }

    #[test]
    fn given_a_member_that_left_when_a_new_member_joins_a_full_group_should_admit_it() {
        let config = GroupCoordinatorConfig {
            max_members_per_group: 1,
            ..config()
        };
        let mut groups = Groups::new();
        let now = Instant::now();
        let member = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));
        let _ = leave(&mut groups, &[(member.as_str(), None)], now);
        assert_eq!(
            error_of(&join_step(&mut groups, &config, &request("", &["x"]), now)),
            ERROR_NONE,
            "a member that left must give its slot back"
        );

        let mut groups = Groups::new();
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));
        assert_eq!(
            error_of(&join_step(&mut groups, &config, &request("", &["x"]), now)),
            ERROR_GROUP_MAX_SIZE_REACHED,
            "the pending id must be what fills the group"
        );
        let _ = leave(&mut groups, &[(pending.as_str(), None)], now);
        assert_eq!(
            error_of(&join_step(&mut groups, &config, &request("", &["x"]), now)),
            ERROR_NONE,
            "a pending id that left must give its slot back"
        );
    }

    #[test]
    fn given_a_leave_while_completing_should_keep_uncollected_answers_of_the_rest() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let leader = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));
        let second = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));
        let third = member_id_of(&join_step(&mut groups, &config, &request("", &["x"]), now));
        let _ = join_step(&mut groups, &config, &request(leader.as_str(), &["x"]), now);
        assert_eq!(groups[&group_id()].phase, Phase::CompletingRebalance);
        assert!(groups[&group_id()].members[&second].join_response.is_some());

        let _ = leave(&mut groups, &[(third.as_str(), None)], now);

        assert_eq!(groups[&group_id()].phase, Phase::PreparingRebalance);
        assert!(
            groups[&group_id()].members[&second].join_response.is_some(),
            "a leave must not revoke an answer another member's waiter has not collected"
        );
    }

    #[test]
    fn given_a_duplicate_identity_in_one_leave_should_answer_the_second_unknown_member_id() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_leader, follower) = stable_two_members(&mut groups, &config, now);

        let result = leave(
            &mut groups,
            &[(follower.as_str(), None), (follower.as_str(), None)],
            now,
        );

        assert_eq!(codes(&result), vec![ERROR_NONE, ERROR_UNKNOWN_MEMBER_ID]);
    }

    #[test]
    fn given_an_empty_group_id_when_leaving_should_return_invalid_group_id() {
        let mut groups = Groups::new();
        let request = LeaveRequest {
            group_id: StrBytes::new(),
            members: vec![LeavingMember {
                member_id: StrBytes::from_static_str("m"),
                group_instance_id: None,
            }],
        };

        let result = leave_step(&mut groups, &request, Instant::now());

        assert_eq!(result.error, ERROR_INVALID_GROUP_ID);
        assert!(result.members.is_empty());
    }

    fn commit(member_id: &str, generation_id: i32) -> CommitRequest {
        CommitRequest {
            group_id: group_id(),
            generation_id,
            member_id: StrBytes::from_string(member_id.to_owned()),
            group_instance_id: None,
        }
    }

    /// Admin tools and `assign()` consumers commit this way.
    #[test]
    fn given_no_group_when_committing_without_a_generation_should_accept_and_not_create_it() {
        let mut groups = Groups::new();

        let error = commit_step(&mut groups, &commit("", -1), Instant::now());

        assert_eq!(error, ERROR_NONE);
        assert!(groups.is_empty());
    }

    /// What every member meets after a gateway restart. `UNKNOWN_MEMBER_ID` makes it rejoin.
    #[test]
    fn given_no_group_when_committing_with_a_generation_should_answer_unknown_member_id() {
        let mut groups = Groups::new();

        let error = commit_step(&mut groups, &commit("m-1", 3), Instant::now());

        assert_eq!(error, ERROR_UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn given_only_pending_ids_when_committing_without_a_generation_should_accept() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let _ = join_step(&mut groups, &config, &pending_request(), now);

        assert_eq!(commit_step(&mut groups, &commit("", -1), now), ERROR_NONE);
    }

    /// Without the refresh a member that only commits, and so heartbeats less, is evicted.
    #[test]
    fn given_a_stable_member_when_committing_should_refresh_its_session() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_leader, follower) = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;

        let error = commit_step(
            &mut groups,
            &commit(follower.as_str(), generation),
            now + Duration::from_secs(9),
        );
        let heartbeat = heartbeat_step(
            &mut groups,
            &group_id(),
            generation,
            &follower,
            None,
            now + Duration::from_secs(15),
        );

        assert_eq!(error, ERROR_NONE);
        assert_ne!(heartbeat, ERROR_UNKNOWN_MEMBER_ID);
        assert!(groups[&group_id()].members.contains_key(&follower));
    }

    /// The consumer commits before it rejoins, while the old generation is still current.
    #[test]
    fn given_a_preparing_rebalance_when_a_member_commits_its_generation_should_accept() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;
        let _ = leave(&mut groups, &[(follower.as_str(), None)], now);
        assert_eq!(groups[&group_id()].phase, Phase::PreparingRebalance);

        let error = commit_step(&mut groups, &commit(leader.as_str(), generation), now);

        assert_eq!(error, ERROR_NONE);
    }

    #[test]
    fn given_a_completing_rebalance_when_a_member_commits_should_answer_rebalance_in_progress() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_leader, follower) = two_members(&mut groups, &config, &["x"], &["x"], now);
        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::CompletingRebalance);
        let generation = group.generation_id;

        let error = commit_step(&mut groups, &commit(follower.as_str(), generation), now);

        assert_eq!(error, ERROR_REBALANCE_IN_PROGRESS);
    }

    #[test]
    fn given_a_group_with_members_when_committing_should_check_member_and_generation() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _follower) = stable_two_members(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;
        let mut check = |request: &CommitRequest| commit_step(&mut groups, request, now);

        assert_eq!(check(&commit("", -1)), ERROR_UNKNOWN_MEMBER_ID);
        assert_eq!(
            check(&commit("stranger", generation)),
            ERROR_UNKNOWN_MEMBER_ID
        );
        assert_eq!(
            check(&commit(leader.as_str(), generation - 1)),
            ERROR_ILLEGAL_GENERATION
        );
        assert_eq!(
            check(&commit(leader.as_str(), -1)),
            ERROR_ILLEGAL_GENERATION
        );
        assert_eq!(check(&commit(leader.as_str(), generation)), ERROR_NONE);
    }

    #[test]
    fn given_a_static_member_when_committing_should_check_its_instance_id() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let member = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_request("i-1"),
            now,
        ));
        let generation = groups[&group_id()].generation_id;
        let _ = sync_step(
            &mut groups,
            &config,
            &sync_request(&member, generation),
            now,
        );
        let with_instance = |member_id: &str, instance_id: &str| CommitRequest {
            group_instance_id: Some(StrBytes::from_string(instance_id.to_owned())),
            ..commit(member_id, generation)
        };

        assert_eq!(
            commit_step(&mut groups, &with_instance("other", "i-1"), now),
            ERROR_FENCED_INSTANCE_ID
        );
        assert_eq!(
            commit_step(&mut groups, &with_instance(member.as_str(), "i-2"), now),
            ERROR_UNKNOWN_MEMBER_ID
        );
        assert_eq!(
            commit_step(&mut groups, &with_instance(member.as_str(), "i-1"), now),
            ERROR_NONE
        );
    }

    /// Each offset call takes at most half of what is left, so calls queued back to back still
    /// leave the heartbeat behind them room to land.
    #[test]
    fn given_a_member_near_its_session_end_the_offset_hold_should_take_half_of_what_is_left() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _follower) = stable_two_members(&mut groups, &config, now);
        let session = groups[&group_id()].members[&leader].session_timeout;
        let member = GroupMember {
            group_id: group_id(),
            member_id: leader,
        };
        let hold = Duration::from_secs(3);
        let late = now + session - Duration::from_secs(2);

        assert_eq!(offset_hold(&groups, &member, hold, now), hold);
        assert_eq!(
            offset_hold(&groups, &member, hold, late),
            Duration::from_secs(1)
        );
        assert_eq!(
            offset_hold(&groups, &member, hold, now + session),
            hold,
            "a session that has ended cannot be saved"
        );
        let gone = GroupMember {
            member_id: StrBytes::from_static_str("gone"),
            ..member
        };
        assert_eq!(offset_hold(&groups, &gone, hold, late), hold);
    }

    #[test]
    fn given_an_invalid_group_id_when_committing_should_answer_invalid_group_id() {
        let mut groups = Groups::new();
        let mut request = commit("", -1);
        request.group_id = StrBytes::new();
        assert_eq!(
            commit_step(&mut groups, &request, Instant::now()),
            ERROR_INVALID_GROUP_ID
        );

        request.group_id = StrBytes::from_string("g".repeat(MAX_GROUP_ID_BYTES + 1));
        assert_eq!(
            commit_step(&mut groups, &request, Instant::now()),
            ERROR_INVALID_GROUP_ID
        );
    }

    fn instance(id: &str) -> StrBytes {
        StrBytes::from_string(id.to_owned())
    }

    fn static_join(member_id: &str, instance_id: &str, protocols: &[&str]) -> JoinRequest {
        JoinRequest {
            group_instance_id: Some(instance(instance_id)),
            ..request(member_id, protocols)
        }
    }

    fn static_sync(member_id: &StrBytes, instance_id: &str, generation_id: i32) -> SyncRequest {
        SyncRequest {
            group_instance_id: Some(instance(instance_id)),
            ..sync_request(member_id, generation_id)
        }
    }

    fn joined(step: Step<JoinResult>) -> JoinResult {
        match step {
            Step::Respond(result) => result,
            Step::Wait { .. } => panic!("expected a JoinGroup response, not a park"),
        }
    }

    /// Static leader `i-a` and follower `i-b`, `Stable`, each holding its own assignment.
    fn stable_static_pair(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        protocols: &[&str],
        now: Instant,
    ) -> (StrBytes, StrBytes) {
        let follower = static_join("", "i-b", protocols);
        stable_static_pair_with(groups, config, protocols, &follower, now)
    }

    /// [`stable_static_pair`] with `i-b` joining by `follower_join`.
    fn stable_static_pair_with(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        protocols: &[&str],
        follower_join: &JoinRequest,
        now: Instant,
    ) -> (StrBytes, StrBytes) {
        let leader = member_id_of(&join_step(
            groups,
            config,
            &static_join("", "i-a", protocols),
            now,
        ));
        let follower = member_id_of(&join_step(groups, config, follower_join, now));
        let _ = join_step(
            groups,
            config,
            &static_join(leader.as_str(), "i-a", protocols),
            now,
        );
        let generation = groups[&group_id()].generation_id;
        let assign = SyncRequest {
            assignments: vec![
                (leader.clone(), Bytes::from_static(b"a-blob")),
                (follower.clone(), Bytes::from_static(b"b-blob")),
            ],
            ..static_sync(&leader, "i-a", generation)
        };
        assert_eq!(
            error_of_sync(&sync_step(groups, config, &assign, now)),
            ERROR_NONE
        );
        assert_eq!(
            error_of_sync(&sync_step(
                groups,
                config,
                &static_sync(&follower, "i-b", generation),
                now
            )),
            ERROR_NONE
        );
        assert_eq!(groups[&group_id()].phase, Phase::Stable);
        (leader, follower)
    }

    #[test]
    fn given_a_stable_static_group_when_the_follower_returns_should_keep_its_assignment_without_a_rebalance()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;

        let returned = joined(join_step(
            &mut groups,
            &config,
            &static_join("", "i-b", &["x"]),
            now,
        ));

        assert_eq!(returned.error, ERROR_NONE);
        assert_ne!(returned.member_id, follower);
        assert_eq!(returned.generation_id, generation, "no generation bump");
        assert_eq!(returned.leader, leader);
        assert!(returned.members.is_empty());
        assert!(!returned.skip_assignment);
        assert_eq!(groups[&group_id()].phase, Phase::Stable);
        let synced = sync_step(
            &mut groups,
            &config,
            &static_sync(&returned.member_id, "i-b", generation),
            now,
        );
        let Step::Respond(synced) = synced else {
            panic!("a Stable sync answers at once");
        };
        assert_eq!(synced.error, ERROR_NONE);
        assert_eq!(synced.assignment, Bytes::from_static(b"b-blob"));
    }

    #[test]
    fn given_a_stable_static_group_when_the_leader_returns_at_v9_should_skip_assignment_with_the_roster()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;

        let returned = joined(join_step(
            &mut groups,
            &config,
            &static_join("", "i-a", &["x"]),
            now,
        ));

        assert_eq!(returned.error, ERROR_NONE);
        assert_eq!(returned.generation_id, generation);
        assert_eq!(returned.leader, returned.member_id);
        assert_ne!(returned.member_id, leader);
        assert!(returned.skip_assignment);
        assert_eq!(
            returned.members.len(),
            2,
            "the leader still gets the roster"
        );
        assert_eq!(
            groups[&group_id()].leader.as_ref(),
            Some(&returned.member_id)
        );
        assert_eq!(groups[&group_id()].phase, Phase::Stable);
    }

    /// Below v9 there is no `skip_assignment`, so the response names the previous leader and the
    /// returning leader acts as a follower instead of reassigning.
    #[test]
    fn given_a_stable_static_group_when_the_leader_returns_below_v9_should_name_the_previous_leader()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _) = stable_static_pair(&mut groups, &config, &["x"], now);

        let returned = joined(join_step(
            &mut groups,
            &config,
            &JoinRequest {
                supports_skip_assignment: false,
                ..static_join("", "i-a", &["x"])
            },
            now,
        ));

        assert_eq!(returned.error, ERROR_NONE);
        assert_eq!(returned.leader, leader);
        assert!(returned.members.is_empty());
        assert!(!returned.skip_assignment);
        assert_eq!(
            groups[&group_id()].leader.as_ref(),
            Some(&returned.member_id)
        );
    }

    #[test]
    fn given_a_replaced_static_member_when_its_old_incarnation_calls_should_be_fenced() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_, old) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let _ = join_step(&mut groups, &config, &static_join("", "i-b", &["x"]), now);
        let i_b = instance("i-b");

        assert_eq!(
            heartbeat_step(&mut groups, &group_id(), generation, &old, Some(&i_b), now),
            ERROR_FENCED_INSTANCE_ID
        );
        assert_eq!(
            error_of_sync(&sync_step(
                &mut groups,
                &config,
                &static_sync(&old, "i-b", generation),
                now
            )),
            ERROR_FENCED_INSTANCE_ID
        );
        assert_eq!(
            error_of(&join_step(
                &mut groups,
                &config,
                &static_join(old.as_str(), "i-b", &["x"]),
                now
            )),
            ERROR_FENCED_INSTANCE_ID
        );
        assert_eq!(
            heartbeat_step(&mut groups, &group_id(), generation, &old, None, now),
            ERROR_UNKNOWN_MEMBER_ID,
            "without its instance id the old member id is simply gone"
        );
    }

    #[test]
    fn given_a_parked_static_join_when_its_instance_returns_while_preparing_should_fence_the_waiter()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, old) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let leader_rejoin = join_step(
            &mut groups,
            &config,
            &static_join(leader.as_str(), "i-a", &["x"]),
            now,
        );
        assert!(matches!(leader_rejoin, Step::Wait { .. }));
        let parked = join_step(
            &mut groups,
            &config,
            &static_join(old.as_str(), "i-b", &["x"]),
            now,
        );
        assert!(
            matches!(parked, Step::Wait { .. }) || error_of_or_none(&parked) == Some(ERROR_NONE)
        );

        let returned = join_step(&mut groups, &config, &static_join("", "i-b", &["x"]), now);
        let new_id = member_id_of(&returned);
        assert_ne!(new_id, old);
        assert_eq!(groups[&group_id()].generation_id, generation + 1);

        let resumed = joined(join_resume_step(
            &mut groups,
            &group_id(),
            &old,
            Some(&instance("i-b")),
            now,
        ));
        assert_eq!(resumed.error, ERROR_FENCED_INSTANCE_ID);
        assert_eq!(resumed.member_id, old);
    }

    /// The leader may already be assigning to the old member id, so the replacement opens a new
    /// rebalance rather than leave the returning member out of the assignment.
    #[test]
    fn given_a_completing_group_when_a_static_member_returns_should_open_a_rebalance() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let leader = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_join("", "i-a", &["x"]),
            now,
        ));
        let follower = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_join("", "i-b", &["x"]),
            now,
        ));
        let _ = join_step(
            &mut groups,
            &config,
            &static_join(leader.as_str(), "i-a", &["x"]),
            now,
        );
        assert_eq!(groups[&group_id()].phase, Phase::CompletingRebalance);

        let returned = join_step(&mut groups, &config, &static_join("", "i-b", &["x"]), now);

        assert!(matches!(returned, Step::Wait { .. }));
        assert_ne!(member_id_of(&returned), follower);
        assert_eq!(groups[&group_id()].phase, Phase::PreparingRebalance);
    }

    #[test]
    fn given_a_stable_static_group_when_a_member_returns_with_a_new_protocol_choice_should_rebalance()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let _ = stable_static_pair(&mut groups, &config, &["x", "y"], now);
        assert_eq!(groups[&group_id()].protocol_name.as_deref(), Some("x"));

        let returned = join_step(&mut groups, &config, &static_join("", "i-b", &["y"]), now);

        assert!(matches!(returned, Step::Wait { .. }));
        assert_eq!(groups[&group_id()].phase, Phase::PreparingRebalance);
    }

    #[test]
    fn given_a_new_static_member_when_joining_at_v4_should_be_admitted_without_member_id_required()
    {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();

        let step = join_step(
            &mut groups,
            &config,
            &JoinRequest {
                require_known_member_id: true,
                ..static_join("", "i-a", &["x"])
            },
            now,
        );

        assert_ne!(error_of_or_none(&step), Some(ERROR_MEMBER_ID_REQUIRED));
        let group = &groups[&group_id()];
        assert!(group.pending.is_empty());
        assert!(group.members.contains_key(&member_id_of(&step)));
    }

    #[test]
    /// A leader rejoin while `Stable` goes through the full rejoin, which must not drop the
    /// instance id a request without one leaves out.
    fn given_a_static_member_when_it_rejoins_by_member_id_without_its_instance_should_stay_static()
    {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;

        let rejoined = join_step(&mut groups, &config, &request(leader.as_str(), &["x"]), now);

        assert!(matches!(rejoined, Step::Wait { .. }));
        assert_eq!(
            heartbeat_step(
                &mut groups,
                &group_id(),
                generation,
                &leader,
                Some(&instance("i-a")),
                now
            ),
            ERROR_REBALANCE_IN_PROGRESS,
            "still the holder of its instance id"
        );
    }

    #[test]
    fn given_a_member_when_it_names_an_instance_nobody_holds_should_answer_unknown_member_id() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;

        assert_eq!(
            heartbeat_step(
                &mut groups,
                &group_id(),
                generation,
                &leader,
                Some(&instance("i-z")),
                now
            ),
            ERROR_UNKNOWN_MEMBER_ID
        );
    }

    #[test]
    fn given_a_static_member_when_its_session_expires_should_be_evicted_and_rebalance() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let later = now + Duration::from_secs(8);
        let i_a = instance("i-a");
        assert_eq!(
            heartbeat_step(
                &mut groups,
                &group_id(),
                generation,
                &leader,
                Some(&i_a),
                later
            ),
            ERROR_NONE
        );

        let expired = now + Duration::from_secs(11);
        let code = heartbeat_step(
            &mut groups,
            &group_id(),
            generation,
            &leader,
            Some(&i_a),
            expired,
        );

        assert_eq!(code, ERROR_REBALANCE_IN_PROGRESS);
        assert!(!groups[&group_id()].members.contains_key(&follower));
        assert!(
            !groups[&group_id()]
                .static_members
                .contains_key(&instance("i-b"))
        );
        let returned = join_step(
            &mut groups,
            &config,
            &static_join("", "i-b", &["x"]),
            expired,
        );
        assert_ne!(member_id_of(&returned), follower);
        assert_eq!(holders_of(&groups, "i-b"), vec![member_id_of(&returned)]);
    }

    fn holders_of(groups: &Groups, instance_id: &str) -> Vec<StrBytes> {
        groups[&group_id()]
            .members
            .iter()
            .filter(|(_, member)| member.group_instance_id.as_deref() == Some(instance_id))
            .map(|(member_id, _)| member_id.clone())
            .collect()
    }

    /// The leader of `stable_static_pair` rejoins while `Stable`, `i-b` stays away, and the join
    /// window closes. Returns the leader's answer for the new generation.
    fn close_window_without_follower(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        leader: &StrBytes,
        now: Instant,
        closed: Instant,
    ) -> JoinResult {
        let rejoin = join_step(
            groups,
            config,
            &static_join(leader.as_str(), "i-a", &["x"]),
            now,
        );
        assert!(matches!(rejoin, Step::Wait { .. }));
        joined(join_resume_step(
            groups,
            &group_id(),
            leader,
            Some(&instance("i-a")),
            closed,
        ))
    }

    #[test]
    fn given_an_absent_static_follower_when_the_join_window_closes_should_stay_in_the_roster_and_return_without_a_rebalance()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let closed = now + Duration::from_secs(6);

        let answer = close_window_without_follower(&mut groups, &config, &leader, now, closed);

        assert_eq!(answer.error, ERROR_NONE);
        assert_eq!(answer.generation_id, generation + 1);
        assert_eq!(answer.leader, leader);
        let roster: Vec<(StrBytes, Option<StrBytes>)> = answer
            .members
            .iter()
            .map(|member| (member.member_id.clone(), member.group_instance_id.clone()))
            .collect();
        assert!(roster.contains(&(follower.clone(), Some(instance("i-b")))));
        assert_eq!(roster.len(), 2);
        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::CompletingRebalance);
        assert!(group.members[&follower].join_response.is_none());
        assert_eq!(holders_of(&groups, "i-b"), vec![follower.clone()]);

        let assign = SyncRequest {
            assignments: vec![
                (leader.clone(), Bytes::from_static(b"a-2")),
                (follower.clone(), Bytes::from_static(b"b-2")),
            ],
            ..static_sync(&leader, "i-a", generation + 1)
        };
        assert_eq!(
            error_of_sync(&sync_step(&mut groups, &config, &assign, closed)),
            ERROR_NONE
        );
        let returned = joined(join_step(
            &mut groups,
            &config,
            &static_join("", "i-b", &["x"]),
            closed,
        ));

        assert_eq!(returned.error, ERROR_NONE);
        assert_ne!(returned.member_id, follower);
        assert_eq!(returned.generation_id, generation + 1);
        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::Stable);
        assert_eq!(group.generation_id, generation + 1);
        let synced = sync_step(
            &mut groups,
            &config,
            &static_sync(&returned.member_id, "i-b", generation + 1),
            closed,
        );
        let Step::Respond(synced) = synced else {
            panic!("a Stable sync answers at once");
        };
        assert_eq!(synced.assignment, Bytes::from_static(b"b-2"));
    }

    /// Closing the window must not refresh the absent member's session: it would otherwise
    /// outlive its last heartbeat by a whole session for every rebalance it misses.
    #[test]
    fn given_an_absent_static_follower_when_it_never_returns_should_be_evicted_at_its_session_deadline()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let generation = groups[&group_id()].generation_id;
        let closed = now + Duration::from_secs(6);
        let _ = close_window_without_follower(&mut groups, &config, &leader, now, closed);
        let i_a = instance("i-a");

        let before = now + Duration::from_millis(9_500);
        assert_eq!(
            heartbeat_step(
                &mut groups,
                &group_id(),
                generation + 1,
                &leader,
                Some(&i_a),
                before
            ),
            ERROR_NONE
        );
        assert!(groups[&group_id()].members.contains_key(&follower));

        let after = now + Duration::from_millis(10_500);
        assert_eq!(
            heartbeat_step(
                &mut groups,
                &group_id(),
                generation + 1,
                &leader,
                Some(&i_a),
                after
            ),
            ERROR_REBALANCE_IN_PROGRESS
        );
        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&follower));
        assert!(!group.static_members.contains_key(&instance("i-b")));
    }

    /// `stable_static_pair` whose follower's session outlasts the sync deadline, so only the
    /// sync sweep can remove it. The leader rejoins, the window closes without `i-b`, and the
    /// leader syncs. Returns the leader, the follower and when the window closed.
    fn leader_synced_without_follower(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        now: Instant,
    ) -> (StrBytes, StrBytes, Instant) {
        let long_session = JoinRequest {
            session_timeout: Duration::from_secs(60),
            ..static_join("", "i-b", &["x"])
        };
        let (leader, follower) =
            stable_static_pair_with(groups, config, &["x"], &long_session, now);
        let closed = now + Duration::from_secs(6);
        let answer = close_window_without_follower(groups, config, &leader, now, closed);
        let assign = SyncRequest {
            assignments: vec![
                (leader.clone(), Bytes::from_static(b"a-2")),
                (follower.clone(), Bytes::from_static(b"b-2")),
            ],
            ..static_sync(&leader, "i-a", answer.generation_id)
        };
        assert_eq!(
            error_of_sync(&sync_step(groups, config, &assign, closed)),
            ERROR_NONE
        );
        let group = &groups[&group_id()];
        assert_eq!(group.phase, Phase::Stable);
        assert!(group.members.contains_key(&follower));
        (leader, follower, closed)
    }

    fn leader_heartbeat(groups: &mut Groups, leader: &StrBytes, now: Instant) -> i16 {
        let generation = groups[&group_id()].generation_id;
        heartbeat_step(
            groups,
            &group_id(),
            generation,
            leader,
            Some(&instance("i-a")),
            now,
        )
    }

    #[test]
    fn given_an_absent_static_follower_when_the_leader_syncs_and_the_sync_deadline_passes_should_remove_it_and_rebalance()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower, closed) = leader_synced_without_follower(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;

        assert_eq!(
            leader_heartbeat(&mut groups, &leader, closed + Duration::from_secs(4)),
            ERROR_NONE
        );
        assert!(groups[&group_id()].members.contains_key(&follower));

        let code = leader_heartbeat(&mut groups, &leader, closed + Duration::from_secs(6));

        assert_eq!(code, ERROR_REBALANCE_IN_PROGRESS);
        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&follower));
        assert!(!group.static_members.contains_key(&instance("i-b")));
        assert_eq!(group.phase, Phase::PreparingRebalance);
        assert_eq!(group.generation_id, generation);
    }

    #[test]
    fn given_a_follower_that_syncs_after_the_leader_when_the_sync_deadline_passes_should_keep_it() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let rejoin = join_step(
            &mut groups,
            &config,
            &static_join(leader.as_str(), "i-a", &["x"]),
            now,
        );
        assert!(matches!(rejoin, Step::Wait { .. }));
        let follower_answer = joined(join_step(
            &mut groups,
            &config,
            &static_join(follower.as_str(), "i-b", &["x"]),
            now,
        ));
        let generation = follower_answer.generation_id;
        let _ = joined(join_resume_step(
            &mut groups,
            &group_id(),
            &leader,
            Some(&instance("i-a")),
            now,
        ));
        let assign = SyncRequest {
            assignments: vec![
                (leader.clone(), Bytes::from_static(b"a-2")),
                (follower.clone(), Bytes::from_static(b"b-2")),
            ],
            ..static_sync(&leader, "i-a", generation)
        };
        let synced_at = now + Duration::from_secs(1);
        assert_eq!(
            error_of_sync(&sync_step(&mut groups, &config, &assign, synced_at)),
            ERROR_NONE
        );

        let late = sync_step(
            &mut groups,
            &config,
            &static_sync(&follower, "i-b", generation),
            now + Duration::from_secs(2),
        );

        let Step::Respond(late) = late else {
            panic!("a Stable sync answers at once");
        };
        assert_eq!(late.assignment, Bytes::from_static(b"b-2"));
        assert_eq!(
            leader_heartbeat(&mut groups, &leader, now + Duration::from_secs(6)),
            ERROR_NONE
        );
        let group = &groups[&group_id()];
        assert!(group.members.contains_key(&follower));
        assert_eq!(group.phase, Phase::Stable);
        assert_eq!(group.generation_id, generation);
    }

    #[test]
    fn given_a_static_member_replaced_while_stable_when_the_sync_deadline_passes_should_keep_it() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower, closed) = leader_synced_without_follower(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;

        let returned = joined(join_step(
            &mut groups,
            &config,
            &static_join("", "i-b", &["x"]),
            closed + Duration::from_secs(1),
        ));
        assert_eq!(returned.error, ERROR_NONE);
        assert_eq!(returned.generation_id, generation);

        assert_eq!(
            leader_heartbeat(&mut groups, &leader, closed + Duration::from_secs(6)),
            ERROR_NONE
        );
        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&follower));
        assert!(group.members.contains_key(&returned.member_id));
        assert_eq!(group.phase, Phase::Stable);
        assert_eq!(group.generation_id, generation);
    }

    #[test]
    fn given_a_follower_parked_in_sync_when_the_leader_answers_it_should_survive_the_sync_deadline()
    {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let _ = join_step(
            &mut groups,
            &config,
            &static_join(leader.as_str(), "i-a", &["x"]),
            now,
        );
        let generation = joined(join_step(
            &mut groups,
            &config,
            &static_join(follower.as_str(), "i-b", &["x"]),
            now,
        ))
        .generation_id;
        let _ = joined(join_resume_step(
            &mut groups,
            &group_id(),
            &leader,
            Some(&instance("i-a")),
            now,
        ));
        let parked = sync_step(
            &mut groups,
            &config,
            &static_sync(&follower, "i-b", generation),
            now + Duration::from_secs(1),
        );
        assert!(matches!(parked, Step::Wait { .. }));
        let assign = SyncRequest {
            assignments: vec![
                (leader.clone(), Bytes::from_static(b"a-2")),
                (follower.clone(), Bytes::from_static(b"b-2")),
            ],
            ..static_sync(&leader, "i-a", generation)
        };
        let synced_at = now + Duration::from_secs(2);
        assert_eq!(
            error_of_sync(&sync_step(&mut groups, &config, &assign, synced_at)),
            ERROR_NONE
        );
        let Step::Respond(answered) = sync_resume_step(
            &mut groups,
            &group_id(),
            &follower,
            Some(&instance("i-b")),
            generation,
            synced_at,
        ) else {
            panic!("the leader's sync answers the parked follower");
        };
        assert_eq!(answered.assignment, Bytes::from_static(b"b-2"));
        assert!(groups[&group_id()].sync_deadline.is_none());

        assert_eq!(
            leader_heartbeat(&mut groups, &leader, now + Duration::from_secs(6)),
            ERROR_NONE
        );
        let group = &groups[&group_id()];
        assert!(group.members.contains_key(&follower));
        assert_eq!(group.phase, Phase::Stable);
    }

    /// `stable_static_pair` with `i-b` replaced once.
    fn replaced_follower(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        now: Instant,
    ) -> (StrBytes, StrBytes, StrBytes) {
        let (leader, old) = stable_static_pair(groups, config, &["x"], now);
        let new = member_id_of(&join_step(
            groups,
            config,
            &static_join("", "i-b", &["x"]),
            now,
        ));
        assert_eq!(
            groups[&group_id()].static_holder(&instance("i-b")),
            Some(&new)
        );
        (leader, old, new)
    }

    #[test]
    fn given_a_replaced_instance_when_its_holder_expires_should_drop_the_instance_entry() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (leader, _, new) = replaced_follower(&mut groups, &config, now);
        let generation = groups[&group_id()].generation_id;
        let _ = heartbeat_step(
            &mut groups,
            &group_id(),
            generation,
            &leader,
            None,
            now + Duration::from_secs(9),
        );

        let _ = heartbeat_step(
            &mut groups,
            &group_id(),
            generation,
            &leader,
            None,
            now + Duration::from_secs(11),
        );

        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&new));
        assert!(!group.static_members.contains_key(&instance("i-b")));
    }

    #[test]
    fn given_a_replaced_instance_when_its_holder_leaves_should_drop_the_instance_entry() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_, _, new) = replaced_follower(&mut groups, &config, now);

        let result = leave(&mut groups, &[(new.as_str(), Some("i-b"))], now);

        assert_eq!(codes(&result), vec![ERROR_NONE]);
        assert!(
            !groups[&group_id()]
                .static_members
                .contains_key(&instance("i-b"))
        );
    }

    #[test]
    fn given_a_replaced_instance_when_its_holder_misses_the_sync_deadline_should_drop_the_instance_entry()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let leader = joined(join_step(
            &mut groups,
            &config,
            &static_join("", "i-a", &["x"]),
            now,
        ))
        .member_id;
        assert_eq!(
            groups[&group_id()].static_holder(&instance("i-a")),
            Some(&leader)
        );
        let rejoin = join_step(
            &mut groups,
            &config,
            &static_join(leader.as_str(), "i-a", &["x"]),
            now,
        );
        assert!(matches!(rejoin, Step::Wait { .. }));
        let _ = joined(join_step(
            &mut groups,
            &config,
            &static_join(follower.as_str(), "i-b", &["x"]),
            now,
        ));
        let generation = groups[&group_id()].generation_id;
        let parked = sync_step(
            &mut groups,
            &config,
            &static_sync(&follower, "i-b", generation),
            now,
        );
        assert!(matches!(parked, Step::Wait { .. }));

        let _ = sync_resume_step(
            &mut groups,
            &group_id(),
            &follower,
            Some(&instance("i-b")),
            generation,
            now + Duration::from_secs(6),
        );

        let group = &groups[&group_id()];
        assert!(!group.members.contains_key(&leader));
        assert!(group.members.contains_key(&follower));
        assert!(!group.static_members.contains_key(&instance("i-a")));
    }

    /// Kafka throws on this; admitting it would give the instance a second holder.
    #[test]
    fn given_a_pending_member_id_when_it_rejoins_naming_a_held_instance_should_be_rejected() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (_, follower) = stable_static_pair(&mut groups, &config, &["x"], now);
        let pending = member_id_of(&join_step(&mut groups, &config, &pending_request(), now));
        assert!(groups[&group_id()].pending.contains_key(&pending));

        let step = join_step(
            &mut groups,
            &config,
            &JoinRequest {
                require_known_member_id: true,
                ..static_join(pending.as_str(), "i-b", &["x"])
            },
            now,
        );

        assert_eq!(error_of(&step), ERROR_UNKNOWN_MEMBER_ID);
        let group = &groups[&group_id()];
        assert!(!group.pending.contains_key(&pending));
        assert!(!group.members.contains_key(&pending));
        assert_eq!(holders_of(&groups, "i-b"), vec![follower.clone()]);
        assert_eq!(
            groups[&group_id()].static_holder(&instance("i-b")),
            Some(&follower)
        );
    }

    /// Static `i-a` leading and `i-b` parked in `SyncGroup`, `CompletingRebalance`.
    fn follower_parked_in_sync(
        groups: &mut Groups,
        config: &GroupCoordinatorConfig,
        now: Instant,
    ) -> (StrBytes, i32) {
        let leader = member_id_of(&join_step(
            groups,
            config,
            &static_join("", "i-a", &["x"]),
            now,
        ));
        let follower = member_id_of(&join_step(
            groups,
            config,
            &static_join("", "i-b", &["x"]),
            now,
        ));
        let _ = join_step(
            groups,
            config,
            &static_join(leader.as_str(), "i-a", &["x"]),
            now,
        );
        let generation = groups[&group_id()].generation_id;
        assert_eq!(groups[&group_id()].phase, Phase::CompletingRebalance);
        let parked = sync_step(
            groups,
            config,
            &static_sync(&follower, "i-b", generation),
            now,
        );
        assert!(matches!(parked, Step::Wait { .. }));
        (follower, generation)
    }

    #[test]
    fn given_a_parked_sync_when_its_instance_is_replaced_should_answer_fenced_instance_id() {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (old, generation) = follower_parked_in_sync(&mut groups, &config, now);

        let _ = join_step(&mut groups, &config, &static_join("", "i-b", &["x"]), now);
        let resumed = sync_resume_step(
            &mut groups,
            &group_id(),
            &old,
            Some(&instance("i-b")),
            generation,
            now,
        );

        assert_eq!(error_of_sync(&resumed), ERROR_FENCED_INSTANCE_ID);
    }

    /// The first incarnation's waiter may not relock until two replacements later.
    #[test]
    fn given_a_parked_sync_when_its_instance_is_replaced_twice_should_still_answer_fenced_instance_id()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (first, generation) = follower_parked_in_sync(&mut groups, &config, now);

        let second = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_join("", "i-b", &["x"]),
            now,
        ));
        let third = member_id_of(&join_step(
            &mut groups,
            &config,
            &static_join("", "i-b", &["x"]),
            now,
        ));

        assert_ne!(second, third);
        assert_eq!(
            error_of_sync(&sync_resume_step(
                &mut groups,
                &group_id(),
                &first,
                Some(&instance("i-b")),
                generation,
                now
            )),
            ERROR_FENCED_INSTANCE_ID
        );
        assert_eq!(
            error_of(&join_resume_step(
                &mut groups,
                &group_id(),
                &second,
                Some(&instance("i-b")),
                now
            )),
            ERROR_FENCED_INSTANCE_ID
        );
        assert_eq!(holders_of(&groups, "i-b"), vec![third]);
    }

    /// Nothing is kept per replaced id, so no number of replacements wears the fence off.
    #[test]
    fn given_a_parked_sync_when_its_instance_is_replaced_many_times_should_still_answer_fenced_instance_id()
     {
        let config = config();
        let mut groups = Groups::new();
        let now = Instant::now();
        let (first, generation) = follower_parked_in_sync(&mut groups, &config, now);

        let mut holder = first.clone();
        for _ in 0..16 {
            holder = member_id_of(&join_step(
                &mut groups,
                &config,
                &static_join("", "i-b", &["x"]),
                now,
            ));
        }

        assert_eq!(
            error_of_sync(&sync_resume_step(
                &mut groups,
                &group_id(),
                &first,
                Some(&instance("i-b")),
                generation,
                now
            )),
            ERROR_FENCED_INSTANCE_ID
        );
        assert_eq!(holders_of(&groups, "i-b"), vec![holder]);
    }
}
