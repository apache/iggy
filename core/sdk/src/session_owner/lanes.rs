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

//! A lane admits one mutation at a time, in arrival order. There is one lane
//! for metadata and one for each partition history. A mutation whose outcome
//! is unknown stays retained in its lane, and the lane admits nothing else
//! until a resolve or an abandon frees it. So a retained operation is always
//! the newest operation of its lane, and its replay can never age out.
//! A replay that proves the outcome records it, so a later resolve of that
//! operation returns the same outcome without a replay.
//! Offset reads, stores, deletes, auto-commit and Next polls share a separate
//! progress gate per partition. Stores and deletes take it before the mutation
//! gate, so waiting for a parked poll does not block sends. Retained offset
//! mutations also block progress until resolved or abandoned. A fetch and an
//! auto-commit poll take the progress gates of all their partitions in
//! ascending partition order and no mutation gate, so no two calls wait for
//! each other in a cycle. Unknown committing fetches and polls retain only
//! local progress blocks across sessions. Offset inspection bypasses those
//! blocks; explicit abandon accepts uncertainty.

use super::SessionOwner;
use super::pending::Reservation;
use crate::driver::PartitionKey;
use crate::links::Frame;
use crate::vsr::encode_request_header;
use bytes::Bytes;
use iggy_binary_protocol::HEADER_SIZE;
use iggy_binary_protocol::codes::{DELETE_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET_CODE};
use iggy_binary_protocol::consensus::{Operation, operation_for_code};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::IggyError;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::{Instant, timeout_at};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LaneKey {
    Metadata,
    /// The incarnation names one history of the partition for every spelling
    /// of its stream and topic.
    Partition {
        incarnation: u64,
        partition_id: u32,
    },
    Progress {
        incarnation: u64,
        partition_id: u32,
    },
}

impl LaneKey {
    /// The lane of `code`. A write to a partition takes the lane of the
    /// incarnation that its context carries. Reads, Register and Logout take
    /// no mutation lane by code alone. The driver selects the progress lane
    /// for offset operations and polls from their decoded arguments.
    pub(crate) fn of(code: u32, context: &PartitionContext, partition_id: u32) -> Option<Self> {
        match operation_for_code(code) {
            Operation::NonReplicated | Operation::Register | Operation::Logout => None,
            operation if operation.is_partition() => Some(Self::Partition {
                incarnation: context.incarnation,
                partition_id,
            }),
            _ => Some(Self::Metadata),
        }
    }

    fn blocking_operation(&self, lanes: &Lanes) -> Option<u64> {
        let retained = match *self {
            Self::Progress {
                incarnation,
                partition_id,
            } => lanes
                .get(&Self::Partition {
                    incarnation,
                    partition_id,
                })
                .and_then(|lane| lane.retained.as_ref())
                .filter(|retained| retained.changes_progress()),
            _ => lanes.get(self).and_then(|lane| lane.retained.as_ref()),
        };
        retained.map(RetainedOperation::op)
    }
}

pub(super) type Lanes = HashMap<LaneKey, Lane>;

/// The outcomes that replays proved, oldest first. A session reset keeps
/// them, because request ids never repeat within a client.
#[derive(Debug)]
pub(super) struct ProvedOutcomes {
    limit: NonZeroUsize,
    outcomes: VecDeque<(u64, Result<(), IggyError>)>,
}

impl ProvedOutcomes {
    /// Keeps the outcomes of the last `limit` proved replays.
    pub(super) fn new(limit: NonZeroUsize) -> Self {
        Self {
            limit,
            outcomes: VecDeque::new(),
        }
    }

    fn record(&mut self, op: u64, outcome: Result<(), IggyError>) {
        if self.outcomes.len() == self.limit.get() {
            self.outcomes.pop_front();
        }
        self.outcomes.push_back((op, outcome));
    }
}

#[derive(Debug, Default)]
pub(super) struct Lane {
    gate: Arc<Mutex<()>>,
    /// The tickets that wait for the gate, hold it or resume. The lane leaves
    /// the table when none is left and nothing is retained.
    tickets: usize,
    retained: Option<RetainedOperation>,
    fetch_block: Option<Arc<FetchBlock>>,
}

/// Local recovery metadata only. A fetch must never retain a replayable frame.
#[derive(Debug)]
struct FetchBlock {
    operation_id: u64,
    _reservation: Arc<Reservation>,
}

impl Lane {
    fn holds(&self, op: u64) -> bool {
        self.retained
            .as_ref()
            .is_some_and(|retained| retained.op() == op)
    }
}

/// A frozen mutation. Every attempt and every replay sends the same frame.
#[derive(Debug, Clone)]
pub(crate) struct RetainedOperation {
    pub(crate) code: u32,
    pub(crate) frame: Frame,
    /// The partition of a partition write, `None` for any other request. A
    /// replay looks up its route by this key.
    pub(crate) partition: Option<PartitionKey>,
    reservation: Arc<Reservation>,
}

impl RetainedOperation {
    pub(crate) fn op(&self) -> u64 {
        self.frame.header.request
    }

    fn changes_progress(&self) -> bool {
        matches!(
            self.code,
            STORE_CONSUMER_OFFSET_CODE | DELETE_CONSUMER_OFFSET_CODE
        )
    }
}

/// The right of one call to send in its lane. Dropping the ticket frees the
/// lane, except that a mutation which left the client without a definitive
/// outcome stays retained. A committing fetch or poll instead publishes
/// nonreplayable progress blocks before opening its gates. Other progress polls
/// retain nothing.
#[derive(Debug)]
pub(crate) struct LaneTicket {
    owner: Arc<SessionOwner>,
    /// `None` for a call that takes no lane.
    key: Option<LaneKey>,
    guard: Option<OwnedMutexGuard<()>>,
    /// The progress gates that the call holds besides its own lane. Offset
    /// mutations keep theirs even if abandoned during their replay.
    progress: Vec<LaneTicket>,
    operation: Option<RetainedOperation>,
    reservation: Option<Arc<Reservation>>,
    fetch_reservation: Option<Arc<Reservation>>,
    fetch_block: Option<Arc<FetchBlock>>,
    /// None retains all targets; a valid partial reply narrows that set.
    uncertain_targets: Option<Vec<u32>>,
    fence: u64,
    /// A resume replays an operation that its lane still retains.
    resumed: bool,
    sent: bool,
    done: bool,
    /// The outcome that the call proved. Only a replay records it.
    proved: Option<Result<(), IggyError>>,
}

impl SessionOwner {
    pub(crate) fn retains_operation(&self, op: u64) -> bool {
        self.lanes().values().any(|lane| {
            lane.holds(op)
                || lane
                    .fetch_block
                    .as_ref()
                    .is_some_and(|block| block.operation_id == op)
        })
    }

    /// Waits for the lane of `key` in arrival order. A lane that retains an
    /// operation fails at once with `LaneBlocked`, and a lane still busy at
    /// `deadline` fails with `TransientNotAccepted`. Neither failure takes a
    /// request id.
    pub(crate) async fn enter_lane(
        self: &Arc<Self>,
        key: Option<LaneKey>,
        deadline: Instant,
    ) -> Result<LaneTicket, IggyError> {
        self.enter_lane_for(key, deadline, None, false).await
    }

    /// Offset inspection must remain possible while a fetch outcome is unknown.
    pub(crate) async fn enter_progress_inspection(
        self: &Arc<Self>,
        key: LaneKey,
        deadline: Instant,
    ) -> Result<LaneTicket, IggyError> {
        self.enter_lane_for(Some(key), deadline, None, true).await
    }

    pub(crate) async fn enter_progress_lane(
        self: &Arc<Self>,
        progress_key: LaneKey,
        mutation_key: Option<LaneKey>,
        deadline: Instant,
    ) -> Result<LaneTicket, IggyError> {
        let progress = self.enter_lane(Some(progress_key), deadline).await?;
        let Some(key) = mutation_key else {
            return Ok(progress);
        };
        let mut ticket = self.enter_lane(Some(key), deadline).await?;
        ticket.progress.push(progress);
        Ok(ticket)
    }

    /// Enters the progress lane of every `(partition_id, incarnation)` in
    /// ascending partition order, and no mutation lane. A retained offset
    /// mutation fails the call with `PartitionLaneBlocked`, which names the
    /// partition of its lane.
    pub(crate) async fn enter_progress_lanes(
        self: &Arc<Self>,
        partitions: &[(u32, u64)],
        deadline: Instant,
    ) -> Result<LaneTicket, IggyError> {
        let mut partitions = partitions.to_vec();
        partitions.sort_unstable();
        let mut ticket = LaneTicket::new(Arc::clone(self), None, false);
        for (partition_id, incarnation) in partitions {
            let key = LaneKey::Progress {
                incarnation,
                partition_id,
            };
            let progress =
                self.enter_lane(Some(key), deadline)
                    .await
                    .map_err(|error| match error {
                        IggyError::LaneBlocked(op) => {
                            IggyError::PartitionLaneBlocked(partition_id, op)
                        }
                        error => error,
                    })?;
            ticket.progress.push(progress);
        }
        Ok(ticket)
    }

    async fn enter_lane_for(
        self: &Arc<Self>,
        key: Option<LaneKey>,
        deadline: Instant,
        resuming: Option<u64>,
        inspecting: bool,
    ) -> Result<LaneTicket, IggyError> {
        let Some(key) = key else {
            return Ok(LaneTicket::new(Arc::clone(self), None, false));
        };
        let (mut ticket, gate) = {
            let mut lanes = self.lanes();
            if !inspecting && let Some(error) = fetch_blocked(&lanes, key) {
                return Err(error);
            }
            if let Some(op) = key
                .blocking_operation(&lanes)
                .filter(|op| Some(*op) != resuming)
            {
                return Err(IggyError::LaneBlocked(op));
            }
            let lane = lanes.entry(key).or_default();
            lane.tickets += 1;
            (
                LaneTicket::new(Arc::clone(self), Some(key), false),
                Arc::clone(&lane.gate),
            )
        };
        let guard = timeout_at(deadline, gate.lock_owned())
            .await
            .map_err(|_| IggyError::TransientNotAccepted)?;
        ticket.guard = Some(guard);
        if !inspecting && let Some(error) = fetch_blocked(&self.lanes(), key) {
            return Err(error);
        }
        // The call that held the gate can have left its operation retained.
        let blocked = key
            .blocking_operation(&self.lanes())
            .filter(|op| Some(*op) != resuming);
        if let Some(op) = blocked {
            return Err(IggyError::LaneBlocked(op));
        }
        Ok(ticket)
    }

    /// Takes the lane of the retained operation `op` for a replay. The lane
    /// keeps `op` retained, so new calls still fail with `LaneBlocked` until
    /// the replay completes it.
    pub(crate) async fn resume(
        self: &Arc<Self>,
        op: u64,
        deadline: Instant,
    ) -> Result<(LaneTicket, RetainedOperation), IggyError> {
        let (mut ticket, gate, progress_key) = {
            let mut lanes = self.lanes();
            if lanes.values().any(|lane| {
                lane.fetch_block
                    .as_ref()
                    .is_some_and(|block| block.operation_id == op)
            }) {
                return Err(IggyError::FetchProgressUncertain(op));
            }
            let Some((key, lane)) = lanes.iter_mut().find(|(_, lane)| lane.holds(op)) else {
                return Err(IggyError::OperationNotRetained(op));
            };
            let progress_key = match *key {
                LaneKey::Partition {
                    incarnation,
                    partition_id,
                } if lane
                    .retained
                    .as_ref()
                    .is_some_and(RetainedOperation::changes_progress) =>
                {
                    Some(LaneKey::Progress {
                        incarnation,
                        partition_id,
                    })
                }
                _ => None,
            };
            lane.tickets += 1;
            (
                LaneTicket::new(Arc::clone(self), Some(*key), true),
                Arc::clone(&lane.gate),
                progress_key,
            )
        };
        if let Some(key) = progress_key {
            let progress = self
                .enter_lane_for(Some(key), deadline, Some(op), false)
                .await
                .map_err(|error| match error {
                    IggyError::TransientNotAccepted => IggyError::OperationUncertain(op),
                    IggyError::LaneBlocked(_) => IggyError::OperationNotRetained(op),
                    error => error,
                })?;
            ticket.progress.push(progress);
        }
        let guard = timeout_at(deadline, gate.lock_owned())
            .await
            .map_err(|_| IggyError::OperationUncertain(op))?;
        ticket.guard = Some(guard);
        let retained = {
            let lanes = self.lanes();
            ticket
                .key
                .and_then(|key| lanes.get(&key))
                .and_then(|lane| lane.retained.clone())
                .filter(|retained| retained.op() == op)
                // A reset clears the slots under this lock, so the retained
                // operation belongs to the current fence.
                .map(|retained| (retained, self.fence()))
        };
        let Some((retained, fence)) = retained else {
            return Err(IggyError::OperationNotRetained(op));
        };
        ticket.fence = fence;
        ticket.reservation = Some(Arc::clone(&retained.reservation));
        ticket.operation = Some(retained.clone());
        Ok((ticket, retained))
    }

    /// Frees the lane of the retained operation `op`. Its outcome stays
    /// unknown.
    pub(crate) fn abandon(&self, op: u64) -> Result<(), IggyError> {
        let mut lanes = self.lanes();
        let mut abandoned_fetch = false;
        lanes.retain(|_, lane| {
            if lane
                .fetch_block
                .as_ref()
                .is_some_and(|block| block.operation_id == op)
            {
                lane.fetch_block = None;
                abandoned_fetch = true;
            }
            lane.tickets > 0 || lane.retained.is_some() || lane.fetch_block.is_some()
        });
        if abandoned_fetch {
            return Ok(());
        }
        let Some((&key, lane)) = lanes.iter_mut().find(|(_, lane)| lane.holds(op)) else {
            return Err(IggyError::OperationNotRetained(op));
        };
        if let Some(retained) = lane.retained.take() {
            retained.reservation.release_operation();
        }
        if lane.tickets == 0 {
            lanes.remove(&key);
        }
        Ok(())
    }

    /// The outcome that a replay proved for `op`, while the client keeps it.
    pub(crate) fn proved_outcome(&self, op: u64) -> Result<(), IggyError> {
        self.proved()
            .outcomes
            .iter()
            .find(|(proved, _)| *proved == op)
            .map_or(Err(IggyError::OperationNotRetained(op)), |(_, outcome)| {
                outcome.clone()
            })
    }
}

/// Replays cannot cross a session reset. Local fetch blocks do not depend on a
/// replay receipt and remain until explicit abandonment.
pub(super) fn drop_retained(lanes: &mut Lanes) {
    lanes.retain(|_, lane| {
        lane.retained = None;
        lane.tickets > 0 || lane.fetch_block.is_some()
    });
}

fn fetch_blocked(lanes: &Lanes, key: LaneKey) -> Option<IggyError> {
    let LaneKey::Progress { partition_id, .. } = key else {
        return None;
    };
    lanes
        .get(&key)?
        .fetch_block
        .as_ref()
        .map(|block| IggyError::PartitionLaneBlocked(partition_id, block.operation_id))
}

impl LaneTicket {
    fn new(owner: Arc<SessionOwner>, key: Option<LaneKey>, resumed: bool) -> Self {
        Self {
            owner,
            key,
            guard: None,
            progress: Vec::new(),
            operation: None,
            reservation: None,
            fetch_reservation: None,
            fetch_block: None,
            uncertain_targets: None,
            fence: 0,
            resumed,
            sent: false,
            done: false,
            proved: None,
        }
    }

    /// Reserves client-wide capacity before allocating a request id. Reads and
    /// transient progress polls only use their ordinary link and lane bounds.
    pub(crate) async fn reserve(
        &mut self,
        code: u32,
        body_len: usize,
        deadline: Instant,
    ) -> Result<(), IggyError> {
        if !self.is_mutation(code) {
            return Ok(());
        }
        let bytes = HEADER_SIZE
            .checked_add(body_len)
            .ok_or(IggyError::InvalidSizeBytes)?;
        if self
            .reservation
            .as_ref()
            .is_some_and(|reservation| reservation.is_current(bytes))
        {
            return Ok(());
        }
        if (self.sent || self.resumed) && self.operation.is_some() {
            return Err(IggyError::Unauthenticated);
        }
        self.operation = None;
        self.reservation = None;
        self.reservation = Some(self.owner.pending.reserve(bytes, deadline).await?);
        Ok(())
    }

    /// Reserve the bounded local block before a committing fetch or poll can be
    /// sent.
    pub(crate) async fn reserve_fetch(&mut self, deadline: Instant) -> Result<(), IggyError> {
        if self.fetch_reservation.is_none() {
            let bytes = size_of::<FetchBlock>()
                + self.progress.len()
                    * (size_of::<LaneKey>() + size_of::<Lane>() + size_of::<Mutex<()>>());
            self.fetch_reservation =
                Some(self.owner.pending_fetches.reserve(bytes, deadline).await?);
        }
        Ok(())
    }

    pub(crate) fn retain_fetch_targets(&mut self, partitions: Vec<u32>) -> Option<u64> {
        self.uncertain_targets = Some(partitions);
        self.fetch_block.as_ref().map(|block| block.operation_id)
    }

    /// [`Self::freeze_in`] in the current lineage.
    #[cfg(test)]
    pub(crate) fn freeze(
        &mut self,
        code: u32,
        body: Bytes,
        context: PartitionContext,
        partition: Option<PartitionKey>,
    ) -> Result<Frame, IggyError> {
        let lineage = self.owner.epoch().lineage;
        self.freeze_in(lineage, code, body, context, partition)
    }

    /// Takes the next request id and freezes the frame of the call, but only
    /// while the lineage is `lineage`. So no call freezes under a session that
    /// the caller signed in to after the call started. A call that starts a
    /// new operation in the same lane freezes again.
    pub(crate) fn freeze_in(
        &mut self,
        lineage: u64,
        code: u32,
        body: Bytes,
        context: PartitionContext,
        partition: Option<PartitionKey>,
    ) -> Result<Frame, IggyError> {
        let mut session = self.owner.session()?;
        // The lineage changes only under the session lock.
        if self.owner.epoch().lineage != lineage {
            return Err(IggyError::Unauthenticated);
        }
        let reservation = if self.is_mutation(code) {
            let bytes = HEADER_SIZE
                .checked_add(body.len())
                .ok_or(IggyError::InvalidSizeBytes)?;
            Some(
                self.reservation
                    .as_ref()
                    .filter(|reservation| reservation.is_current(bytes))
                    .ok_or(IggyError::TransientNotAccepted)?,
            )
        } else {
            None
        };
        let (mut header, _) = encode_request_header(&mut session, code, &body)?;
        context.stamp(&mut header);
        // A reset raises the fence under the session lock, so this fence
        // names the session of the header.
        self.fence = self.owner.fence();
        drop(session);
        let body = match reservation {
            Some(reservation) => reservation.freeze_body(body),
            None => body,
        };
        let frame = Frame { header, body };
        if let Some(reservation) = &self.fetch_reservation {
            self.fetch_block = Some(Arc::new(FetchBlock {
                operation_id: frame.header.request,
                _reservation: Arc::clone(reservation),
            }));
        }
        self.operation = reservation.map(|reservation| RetainedOperation {
            code,
            frame: frame.clone(),
            partition,
            reservation: Arc::clone(reservation),
        });
        self.sent = false;
        Ok(frame)
    }

    pub(crate) fn revalidate(&self, lineage: u64) -> Result<bool, IggyError> {
        let _session = self.owner.session()?;
        let epoch = self.owner.epoch();
        if epoch.lineage != lineage {
            return Err(IggyError::Unauthenticated);
        }
        Ok(epoch.fence == self.fence)
    }

    pub(crate) fn fence(&self) -> u64 {
        self.fence
    }

    /// The flag that a link sets when the frame leaves the client.
    pub(crate) fn sent_flag(&mut self) -> &mut bool {
        &mut self.sent
    }

    /// Ends the call with a definitive outcome. A resumed operation leaves
    /// its lane.
    pub(crate) fn complete(mut self) {
        self.done = true;
    }

    /// [`Self::complete`] with the outcome that the call proved. The client
    /// keeps the outcome of a replay for a later resolve of its operation.
    pub(crate) fn prove(mut self, outcome: Result<(), IggyError>) {
        self.proved = Some(outcome);
        self.complete();
    }

    /// Ends the call with an unknown outcome. Mutations retain replay frames;
    /// committing fetches and polls retain only local progress blocks.
    pub(crate) fn retain(self) -> IggyError {
        if let Some(block) = &self.fetch_block {
            return IggyError::FetchProgressUncertain(block.operation_id);
        }
        self.operation
            .as_ref()
            .map_or(IggyError::TransientNotCommitted, |operation| {
                IggyError::OperationUncertain(operation.op())
            })
    }

    fn is_mutation(&self, code: u32) -> bool {
        self.key.is_some()
            && !matches!(
                operation_for_code(code),
                Operation::NonReplicated | Operation::Register | Operation::Logout
            )
    }
}

impl Drop for LaneTicket {
    fn drop(&mut self) {
        if self.sent
            && !self.done
            && let Some(block) = &self.fetch_block
        {
            let mut lanes = self.owner.lanes();
            for progress in &self.progress {
                if let Some(key @ LaneKey::Progress { partition_id, .. }) = progress.key
                    && self
                        .uncertain_targets
                        .as_ref()
                        .is_none_or(|targets| targets.contains(&partition_id))
                    && let Some(lane) = lanes.get_mut(&key)
                {
                    lane.fetch_block = Some(Arc::clone(block));
                }
            }
        }
        let Some(key) = self.key else {
            return;
        };
        let mut lanes = self.owner.lanes();
        let mut retained = false;
        if let Some(lane) = lanes.get_mut(&key) {
            let op = self.operation.as_ref().map(RetainedOperation::op);
            if let Some(operation) = self.operation.take() {
                if self.resumed {
                    // Recorded under the lanes lock, so a resolve that no
                    // longer finds the operation finds its outcome. An abandon
                    // during the replay does not discard what the replay proved.
                    if let Some(outcome) = self.proved.take() {
                        self.owner.proved().record(operation.op(), outcome);
                    }
                    if self.done && lane.holds(operation.op()) {
                        lane.retained = None;
                    }
                } else if self.sent
                    && !self.done
                    && lane.retained.is_none()
                    // An operation of a replaced session cannot replay, so it
                    // must not block the lane of the new one.
                    && self.fence == self.owner.fence()
                {
                    lane.retained = Some(operation);
                }
            }
            retained = op.is_some_and(|op| lane.holds(op));
            lane.tickets = lane.tickets.saturating_sub(1);
            if lane.tickets == 0 && lane.retained.is_none() && lane.fetch_block.is_none() {
                lanes.remove(&key);
            }
        }
        drop(lanes);
        if !retained && let Some(reservation) = &self.reservation {
            reservation.release_operation();
        }
        // The gate opens only after the slot holds the retained operation, so
        // the next call in the lane sees it.
        self.guard.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, FETCH_MESSAGES_CODE, LOGOUT_USER_CODE, PING_CODE, POLL_MESSAGES_CODE,
        SEND_MESSAGES_CODE,
    };
    use iggy_common::{AutoLogin, PendingOperationsConfig};
    use std::num::NonZeroUsize;
    use std::time::Duration;
    use tokio::time::timeout;

    const BOUND_SESSION: u64 = 3;
    const NEXT_SESSION: u64 = 4;
    const INCARNATION: u64 = 5;
    const PARTITION_ID: u32 = 1;
    const OTHER_PARTITION_ID: u32 = 2;
    const DEADLINE: Duration = Duration::from_secs(30);
    const SHORT_DEADLINE: Duration = Duration::from_secs(1);

    fn bound_owner() -> Arc<SessionOwner> {
        let owner = Arc::new(SessionOwner::new(AutoLogin::Disabled));
        owner.bind(BOUND_SESSION).unwrap();
        owner
    }

    fn owner_with_limits(operations: usize, bytes: usize) -> Arc<SessionOwner> {
        let owner = Arc::new(SessionOwner::with_pending_operations(
            AutoLogin::Disabled,
            PendingOperationsConfig {
                max_operations: NonZeroUsize::new(operations).unwrap(),
                max_bytes: NonZeroUsize::new(bytes).unwrap(),
            },
        ));
        owner.bind(BOUND_SESSION).unwrap();
        owner
    }

    fn deadline() -> Instant {
        Instant::now() + DEADLINE
    }

    fn partition_lane() -> Option<LaneKey> {
        let context = PartitionContext {
            incarnation: INCARNATION,
            ..PartitionContext::default()
        };
        LaneKey::of(SEND_MESSAGES_CODE, &context, PARTITION_ID)
    }

    fn progress_lane() -> LaneKey {
        LaneKey::Progress {
            incarnation: INCARNATION,
            partition_id: PARTITION_ID,
        }
    }

    /// Enters the metadata lane, freezes a write and drops the ticket after
    /// its send point, so the lane retains the write.
    async fn retain_write(owner: &Arc<SessionOwner>, key: Option<LaneKey>) -> u64 {
        let mut ticket = owner.enter_lane(key, deadline()).await.unwrap();
        ticket
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        let frame = ticket
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        *ticket.sent_flag() = true;
        drop(ticket);
        frame.header.request
    }

    fn lane_count(owner: &SessionOwner) -> usize {
        owner.lanes().len()
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_refused_write_when_its_session_is_replaced_should_release_its_old_byte_budget()
    {
        let owner = owner_with_limits(1, HEADER_SIZE);
        let mut ticket = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        ticket
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        let frame = ticket
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        owner.reset().unwrap();
        owner.bind(NEXT_SESSION).unwrap();
        drop(frame);

        ticket
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        let retried = ticket
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        assert_eq!(retried.header.session, NEXT_SESSION);
        ticket.complete();
    }

    #[tokio::test(start_paused = true)]
    async fn given_full_operation_budget_when_replaying_should_reuse_capacity_until_completion() {
        let owner = owner_with_limits(1, 4 * HEADER_SIZE);
        let op = retain_write(&owner, Some(LaneKey::Metadata)).await;
        let next_id = owner.session().unwrap().current_request_id();
        let mut waiting = owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap();
        assert_eq!(
            waiting
                .reserve(SEND_MESSAGES_CODE, 0, Instant::now() + SHORT_DEADLINE)
                .await,
            Err(IggyError::TransientNotAccepted)
        );
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);

        let (replay, retained) = owner.resume(op, deadline()).await.unwrap();
        assert_eq!(retained.op(), op);
        replay.complete();
        waiting
            .reserve(SEND_MESSAGES_CODE, 0, deadline())
            .await
            .unwrap();
        waiting.complete();
        assert!(!owner.retains_operation(op));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_writer_frame_after_abandon_or_reset_when_admitting_should_keep_its_bytes_charged()
     {
        for reset in [false, true] {
            let owner = owner_with_limits(2, HEADER_SIZE);
            let mut ticket = owner
                .enter_lane(Some(LaneKey::Metadata), deadline())
                .await
                .unwrap();
            ticket
                .reserve(CREATE_STREAM_CODE, 0, deadline())
                .await
                .unwrap();
            let writer_frame = ticket
                .freeze(
                    CREATE_STREAM_CODE,
                    Bytes::new(),
                    PartitionContext::default(),
                    None,
                )
                .unwrap();
            *ticket.sent_flag() = true;
            drop(ticket);
            if reset {
                owner.reset().unwrap();
                owner.bind(NEXT_SESSION).unwrap();
            } else {
                owner.abandon(writer_frame.header.request).unwrap();
            }
            let next_id = owner.session().unwrap().current_request_id();
            let mut waiting = owner
                .enter_lane(partition_lane(), deadline())
                .await
                .unwrap();
            assert_eq!(
                waiting
                    .reserve(SEND_MESSAGES_CODE, 0, Instant::now() + SHORT_DEADLINE)
                    .await,
                Err(IggyError::TransientNotAccepted)
            );
            assert_eq!(owner.session().unwrap().current_request_id(), next_id);

            drop(writer_frame);
            waiting
                .reserve(SEND_MESSAGES_CODE, 0, deadline())
                .await
                .unwrap();
            waiting.complete();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_reset_with_a_live_ticket_when_it_drops_should_not_release_new_session_capacity()
    {
        let owner = owner_with_limits(1, 2 * HEADER_SIZE);
        let mut old = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        old.reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        let frame = old
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        *old.sent_flag() = true;
        owner.reset().unwrap();
        owner.bind(NEXT_SESSION).unwrap();
        let next_id = owner.session().unwrap().current_request_id();
        assert!(matches!(
            old.freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None
            ),
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);
        let mut current = owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap();
        current
            .reserve(SEND_MESSAGES_CODE, 0, deadline())
            .await
            .unwrap();
        drop(old);
        drop(frame);

        let mut waiting = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        assert_eq!(
            waiting
                .reserve(CREATE_STREAM_CODE, 0, Instant::now() + SHORT_DEADLINE)
                .await,
            Err(IggyError::TransientNotAccepted)
        );
        current.complete();
        waiting
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        waiting.complete();
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_oversized_or_unreserved_mutation_when_admitting_should_fail_before_allocating_an_id()
     {
        const BODY_LEN: usize = 3;
        let owner = owner_with_limits(1, HEADER_SIZE + BODY_LEN);
        let mut ticket = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        let next_id = owner.session().unwrap().current_request_id();
        for body_len in [BODY_LEN + 1, usize::MAX] {
            assert_eq!(
                ticket
                    .reserve(CREATE_STREAM_CODE, body_len, deadline())
                    .await,
                Err(IggyError::InvalidSizeBytes)
            );
        }
        assert!(matches!(
            ticket.freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None
            ),
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);
        ticket
            .reserve(CREATE_STREAM_CODE, BODY_LEN, deadline())
            .await
            .unwrap();
        let body = Bytes::from_static(b"abc");
        let pointer = body.as_ptr();
        let frame = ticket
            .freeze(CREATE_STREAM_CODE, body, PartitionContext::default(), None)
            .unwrap();
        assert_eq!(
            frame.body.as_ptr(),
            pointer,
            "reservation must not copy the payload"
        );
        ticket.complete();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_cancelled_budget_waiter_when_capacity_returns_should_not_leak_admission() {
        let owner = owner_with_limits(1, HEADER_SIZE);
        let mut holder = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        holder
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        let next_id = owner.session().unwrap().current_request_id();
        let waiting = async {
            let mut ticket = owner
                .enter_lane(partition_lane(), deadline())
                .await
                .unwrap();
            ticket.reserve(SEND_MESSAGES_CODE, 0, deadline()).await
        };
        assert!(timeout(SHORT_DEADLINE, waiting).await.is_err());
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);
        holder.complete();
        let mut next = owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap();
        next.reserve(SEND_MESSAGES_CODE, 0, deadline())
            .await
            .unwrap();
        next.complete();
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_waiting_mutation_when_the_last_writer_releases_bytes_should_wake_before_deadline()
     {
        let owner = owner_with_limits(1, HEADER_SIZE);
        let mut holder = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        holder
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        let frame = holder
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        holder.complete();
        let waiting_owner = Arc::clone(&owner);
        let started = Instant::now();
        let waiting = tokio::spawn(async move {
            let mut ticket = waiting_owner
                .enter_lane(partition_lane(), deadline())
                .await
                .unwrap();
            ticket
                .reserve(SEND_MESSAGES_CODE, 0, deadline())
                .await
                .unwrap();
            ticket.complete();
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        drop(frame);
        waiting.await.unwrap();
        assert_eq!(Instant::now(), started);
        assert_eq!(lane_count(&owner), 0);
    }

    #[test]
    fn given_each_operation_kind_when_choosing_a_lane_should_split_reads_metadata_and_partitions() {
        let context = PartitionContext {
            incarnation: INCARNATION,
            ..PartitionContext::default()
        };

        assert_eq!(LaneKey::of(PING_CODE, &context, PARTITION_ID), None);
        assert_eq!(LaneKey::of(LOGOUT_USER_CODE, &context, PARTITION_ID), None);
        assert_eq!(
            LaneKey::of(CREATE_STREAM_CODE, &context, PARTITION_ID),
            Some(LaneKey::Metadata)
        );
        assert_eq!(
            LaneKey::of(SEND_MESSAGES_CODE, &context, PARTITION_ID),
            Some(LaneKey::Partition {
                incarnation: INCARNATION,
                partition_id: PARTITION_ID
            })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_op_when_a_call_enters_its_lane_should_fail_at_once_with_lane_blocked()
    {
        let owner = bound_owner();
        let op = retain_write(&owner, Some(LaneKey::Metadata)).await;
        // A replay holds the gate, so a call that waited for it would fail
        // with `TransientNotAccepted` at its deadline instead.
        let (_replay, _) = owner.resume(op, deadline()).await.unwrap();

        let entered = owner
            .enter_lane(Some(LaneKey::Metadata), Instant::now() + SHORT_DEADLINE)
            .await;

        assert!(matches!(entered, Err(IggyError::LaneBlocked(blocker)) if blocker == op));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_busy_lane_when_the_deadline_passes_should_fail_with_58_and_take_no_id() {
        let owner = bound_owner();
        let mut holder = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        holder
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        holder
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        let next_id = owner.session().unwrap().current_request_id();

        let entered = owner
            .enter_lane(Some(LaneKey::Metadata), Instant::now() + SHORT_DEADLINE)
            .await;

        assert_eq!(entered.unwrap_err(), IggyError::TransientNotAccepted);
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_ticket_dropped_after_the_send_point_when_the_next_call_enters_should_fail_with_lane_blocked()
     {
        let owner = bound_owner();
        let op = retain_write(&owner, partition_lane()).await;

        let entered = owner.enter_lane(partition_lane(), deadline()).await;

        assert!(matches!(entered, Err(IggyError::LaneBlocked(blocker)) if blocker == op));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_ticket_dropped_before_the_send_point_when_the_next_call_enters_should_proceed()
    {
        let owner = bound_owner();
        let mut ticket = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        ticket
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        ticket
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        drop(ticket);

        let entered = owner.enter_lane(Some(LaneKey::Metadata), deadline()).await;

        assert!(entered.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_progress_poll_when_waiting_for_reply_should_allow_partition_mutations() {
        let owner = bound_owner();
        let context = PartitionContext {
            incarnation: INCARNATION,
            ..PartitionContext::default()
        };
        let mut poll = owner
            .enter_lane(Some(progress_lane()), deadline())
            .await
            .unwrap();
        poll.freeze(POLL_MESSAGES_CODE, Bytes::new(), context, None)
            .unwrap();
        *poll.sent_flag() = true;
        let next_id = owner.session().unwrap().current_request_id();

        let blocked = owner
            .enter_lane(Some(progress_lane()), Instant::now() + SHORT_DEADLINE)
            .await;
        assert_eq!(blocked.unwrap_err(), IggyError::TransientNotAccepted);
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);

        let pure_read = owner
            .enter_lane(
                LaneKey::of(POLL_MESSAGES_CODE, &context, PARTITION_ID),
                deadline(),
            )
            .await
            .unwrap();
        let other_partition = owner
            .enter_lane(
                LaneKey::of(SEND_MESSAGES_CODE, &context, OTHER_PARTITION_ID),
                deadline(),
            )
            .await
            .unwrap();
        let same_partition = owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap();
        pure_read.complete();
        other_partition.complete();
        same_partition.complete();
        poll.complete();

        owner
            .enter_lane(Some(progress_lane()), deadline())
            .await
            .unwrap()
            .complete();
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sent_progress_poll_when_cancelled_or_uncertain_should_block_only_a_committing_one()
     {
        for committing in [false, true] {
            for cancelled in [true, false] {
                let owner = bound_owner();
                let mut poll = if committing {
                    let mut poll = owner
                        .enter_progress_lanes(&[(PARTITION_ID, INCARNATION)], deadline())
                        .await
                        .unwrap();
                    poll.reserve_fetch(deadline()).await.unwrap();
                    poll
                } else {
                    owner
                        .enter_lane(Some(progress_lane()), deadline())
                        .await
                        .unwrap()
                };
                let frame = poll
                    .freeze(
                        POLL_MESSAGES_CODE,
                        Bytes::new(),
                        PartitionContext {
                            incarnation: INCARNATION,
                            ..PartitionContext::default()
                        },
                        None,
                    )
                    .unwrap();
                *poll.sent_flag() = true;
                let op = frame.header.request;

                if cancelled {
                    drop(poll);
                } else {
                    let uncertain = if committing {
                        IggyError::FetchProgressUncertain(op)
                    } else {
                        IggyError::TransientNotCommitted
                    };
                    assert_eq!(poll.retain(), uncertain);
                }

                if committing {
                    assert!(matches!(
                        owner.resume(op, deadline()).await,
                        Err(IggyError::FetchProgressUncertain(blocking)) if blocking == op
                    ));
                    assert!(matches!(
                        owner.enter_lane(Some(progress_lane()), deadline()).await,
                        Err(IggyError::PartitionLaneBlocked(partition, blocking))
                        if partition == PARTITION_ID && blocking == op
                    ));
                    owner.abandon(op).unwrap();
                } else {
                    assert!(matches!(
                        owner.resume(op, deadline()).await,
                        Err(IggyError::OperationNotRetained(missing)) if missing == op
                    ));
                    assert!(matches!(
                        owner.abandon(op),
                        Err(IggyError::OperationNotRetained(missing)) if missing == op
                    ));
                }
                owner
                    .enter_lane(Some(progress_lane()), deadline())
                    .await
                    .unwrap()
                    .complete();
                assert_eq!(lane_count(&owner), 0);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sent_committing_fetch_when_dropped_across_reset_should_block_every_target_until_abandoned()
     {
        for reset_before_drop in [false, true] {
            let owner = bound_owner();
            let targets = [
                (PARTITION_ID, INCARNATION),
                (OTHER_PARTITION_ID, INCARNATION),
            ];
            let mut fetch = owner
                .enter_progress_lanes(&targets, deadline())
                .await
                .unwrap();
            fetch.reserve_fetch(deadline()).await.unwrap();
            let frame = fetch
                .freeze(
                    FETCH_MESSAGES_CODE,
                    Bytes::new(),
                    PartitionContext::default(),
                    None,
                )
                .unwrap();
            *fetch.sent_flag() = true;
            if reset_before_drop {
                owner.reset().unwrap();
                owner.bind(NEXT_SESSION).unwrap();
            }
            drop(fetch);
            if !reset_before_drop {
                owner.reset().unwrap();
                owner.bind(NEXT_SESSION).unwrap();
            }
            let operation_id = frame.header.request;
            for (partition_id, incarnation) in targets {
                let progress = LaneKey::Progress {
                    partition_id,
                    incarnation,
                };
                assert!(matches!(owner.enter_lane(Some(progress), deadline()).await,
                    Err(IggyError::PartitionLaneBlocked(partition, operation))
                    if partition == partition_id && operation == operation_id));
                owner
                    .enter_progress_inspection(progress, deadline())
                    .await
                    .unwrap()
                    .complete();
                owner
                    .enter_lane(
                        Some(LaneKey::Partition {
                            partition_id,
                            incarnation,
                        }),
                        deadline(),
                    )
                    .await
                    .unwrap()
                    .complete();
                owner
                    .enter_lane(
                        Some(LaneKey::Progress {
                            partition_id,
                            incarnation: incarnation + 1,
                        }),
                        deadline(),
                    )
                    .await
                    .unwrap()
                    .complete();
            }
            assert!(matches!(owner.resume(operation_id, deadline()).await,
                Err(IggyError::FetchProgressUncertain(operation)) if operation == operation_id));
            owner.abandon(operation_id).unwrap();
            owner
                .enter_progress_lanes(&targets, deadline())
                .await
                .unwrap()
                .complete();
            assert_eq!(lane_count(&owner), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_partial_unknown_fetch_when_retained_should_keep_only_uncertain_targets() {
        let owner = bound_owner();
        let mut fetch = owner
            .enter_progress_lanes(
                &[
                    (PARTITION_ID, INCARNATION),
                    (OTHER_PARTITION_ID, INCARNATION),
                ],
                deadline(),
            )
            .await
            .unwrap();
        fetch.reserve_fetch(deadline()).await.unwrap();
        let frame = fetch
            .freeze(
                FETCH_MESSAGES_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        *fetch.sent_flag() = true;
        assert_eq!(
            fetch.retain_fetch_targets(vec![OTHER_PARTITION_ID]),
            Some(frame.header.request)
        );
        drop(fetch);
        owner
            .enter_lane(Some(progress_lane()), deadline())
            .await
            .unwrap()
            .complete();
        assert!(
            matches!(owner.enter_progress_lanes(&[(OTHER_PARTITION_ID, INCARNATION)], deadline()).await,
            Err(IggyError::PartitionLaneBlocked(partition, operation))
            if partition == OTHER_PARTITION_ID && operation == frame.header.request)
        );
        owner.abandon(frame.header.request).unwrap();
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_full_fetch_retention_when_session_resets_should_refuse_before_send_until_abandoned()
     {
        const BYTES: usize = 4096;
        let owner = owner_with_limits(1, BYTES);
        let mut fetch = owner
            .enter_progress_lanes(&[(PARTITION_ID, INCARNATION)], deadline())
            .await
            .unwrap();
        fetch.reserve_fetch(deadline()).await.unwrap();
        let frame = fetch
            .freeze(
                FETCH_MESSAGES_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        *fetch.sent_flag() = true;
        drop(fetch);
        owner.reset().unwrap();
        owner.bind(NEXT_SESSION).unwrap();
        let next_id = owner.session().unwrap().current_request_id();
        let mut next = owner
            .enter_progress_lanes(&[(OTHER_PARTITION_ID, INCARNATION)], deadline())
            .await
            .unwrap();
        assert_eq!(
            next.reserve_fetch(Instant::now() + SHORT_DEADLINE).await,
            Err(IggyError::TransientNotAccepted)
        );
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);
        owner.abandon(frame.header.request).unwrap();
        next.reserve_fetch(deadline()).await.unwrap();
        drop(next);
        let mut cancelled = owner
            .enter_progress_lanes(&[(PARTITION_ID, INCARNATION)], deadline())
            .await
            .unwrap();
        cancelled.reserve_fetch(deadline()).await.unwrap();
        cancelled
            .freeze(
                FETCH_MESSAGES_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        drop(cancelled);
        owner
            .enter_progress_lanes(&[(PARTITION_ID, INCARNATION)], deadline())
            .await
            .unwrap()
            .complete();
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_offset_mutation_when_progress_poll_enters_should_remain_blocked() {
        for code in [STORE_CONSUMER_OFFSET_CODE, DELETE_CONSUMER_OFFSET_CODE] {
            for reset in [false, true] {
                let owner = bound_owner();
                let mut mutation = owner
                    .enter_progress_lane(progress_lane(), partition_lane(), deadline())
                    .await
                    .unwrap();
                mutation.reserve(code, 0, deadline()).await.unwrap();
                let frame = mutation
                    .freeze(
                        code,
                        Bytes::new(),
                        PartitionContext {
                            incarnation: INCARNATION,
                            ..PartitionContext::default()
                        },
                        None,
                    )
                    .unwrap();
                *mutation.sent_flag() = true;
                drop(mutation);
                let next_id = owner.session().unwrap().current_request_id();

                let progress_poll = owner.enter_lane(Some(progress_lane()), deadline()).await;

                assert!(
                    matches!(progress_poll, Err(IggyError::LaneBlocked(op)) if op == frame.header.request)
                );
                assert_eq!(owner.session().unwrap().current_request_id(), next_id);
                if reset {
                    owner.reset().unwrap();
                    owner.bind(NEXT_SESSION).unwrap();
                    owner
                        .enter_progress_lane(progress_lane(), partition_lane(), deadline())
                        .await
                        .unwrap()
                        .complete();
                    assert_eq!(lane_count(&owner), 0);
                    continue;
                }
                let (replay, retained) = owner
                    .resume(frame.header.request, deadline())
                    .await
                    .unwrap();
                assert_eq!(retained.code, code);
                owner.abandon(frame.header.request).unwrap();
                assert!(
                    timeout(
                        SHORT_DEADLINE,
                        owner.enter_lane(Some(progress_lane()), deadline())
                    )
                    .await
                    .is_err(),
                    "an abandoned replay still owns progress until it finishes"
                );
                replay.complete();
                owner
                    .enter_lane(Some(progress_lane()), deadline())
                    .await
                    .unwrap()
                    .complete();
                assert_eq!(lane_count(&owner), 0);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_fetch_of_several_partitions_when_entering_should_take_their_progress_gates_in_ascending_order()
     {
        let owner = bound_owner();
        let higher = LaneKey::Progress {
            incarnation: INCARNATION,
            partition_id: OTHER_PARTITION_ID,
        };
        let held = owner.enter_lane(Some(higher), deadline()).await.unwrap();
        let fetch = owner.enter_progress_lanes(
            &[
                (OTHER_PARTITION_ID, INCARNATION),
                (PARTITION_ID, INCARNATION),
            ],
            deadline(),
        );
        tokio::pin!(fetch);
        assert!(timeout(SHORT_DEADLINE, &mut fetch).await.is_err());

        let lower = owner
            .enter_lane(Some(progress_lane()), Instant::now() + SHORT_DEADLINE)
            .await;
        assert_eq!(
            lower.unwrap_err(),
            IggyError::TransientNotAccepted,
            "the fetch holds the lower gate while it waits for the higher one"
        );
        owner
            .enter_lane(partition_lane(), deadline())
            .await
            .expect("a fetch takes no mutation lane")
            .complete();
        held.complete();
        let fetch = fetch.await.unwrap();
        let busy = owner
            .enter_lane(Some(higher), Instant::now() + SHORT_DEADLINE)
            .await;
        assert_eq!(busy.unwrap_err(), IggyError::TransientNotAccepted);

        fetch.complete();
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_offset_mutation_of_one_partition_when_a_fetch_enters_should_fail_naming_that_partition()
     {
        let owner = bound_owner();
        let mut store = owner
            .enter_progress_lane(
                LaneKey::Progress {
                    incarnation: INCARNATION,
                    partition_id: OTHER_PARTITION_ID,
                },
                Some(LaneKey::Partition {
                    incarnation: INCARNATION,
                    partition_id: OTHER_PARTITION_ID,
                }),
                deadline(),
            )
            .await
            .unwrap();
        store
            .reserve(STORE_CONSUMER_OFFSET_CODE, 0, deadline())
            .await
            .unwrap();
        let frame = store
            .freeze(
                STORE_CONSUMER_OFFSET_CODE,
                Bytes::new(),
                PartitionContext {
                    incarnation: INCARNATION,
                    ..PartitionContext::default()
                },
                None,
            )
            .unwrap();
        *store.sent_flag() = true;
        drop(store);
        let next_id = owner.session().unwrap().current_request_id();

        let fetch = owner
            .enter_progress_lanes(
                &[
                    (OTHER_PARTITION_ID, INCARNATION),
                    (PARTITION_ID, INCARNATION),
                ],
                deadline(),
            )
            .await;

        assert!(
            matches!(
                fetch,
                Err(IggyError::PartitionLaneBlocked(partition_id, op))
                    if partition_id == OTHER_PARTITION_ID && op == frame.header.request
            ),
            "{fetch:?}"
        );
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);
        owner
            .enter_lane(Some(progress_lane()), deadline())
            .await
            .expect("a failed fetch frees the gates that it took")
            .complete();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_store_waiting_for_mutation_when_session_resets_should_keep_its_progress_gate()
    {
        let owner = bound_owner();
        let lineage = owner.epoch().lineage;
        let holder = owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap();
        let mut waiting =
            Box::pin(owner.enter_progress_lane(progress_lane(), partition_lane(), deadline()));
        assert!(futures::poll!(&mut waiting).is_pending());

        owner.reset().unwrap();
        owner.bind(NEXT_SESSION).unwrap();
        assert!(
            timeout(
                SHORT_DEADLINE,
                owner.enter_lane(Some(progress_lane()), deadline())
            )
            .await
            .is_err()
        );
        holder.complete();
        let mut store = waiting.await.unwrap();
        store
            .reserve(STORE_CONSUMER_OFFSET_CODE, 0, deadline())
            .await
            .unwrap();
        assert_eq!(
            store
                .freeze_in(
                    lineage,
                    STORE_CONSUMER_OFFSET_CODE,
                    Bytes::new(),
                    PartitionContext::default(),
                    None
                )
                .unwrap_err(),
            IggyError::Unauthenticated
        );
        drop(store);
        owner
            .enter_progress_lane(progress_lane(), partition_lane(), deadline())
            .await
            .unwrap()
            .complete();
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_retained_ops_when_the_session_resets_should_drop_them_and_report_not_retained() {
        let owner = bound_owner();
        let metadata_op = retain_write(&owner, Some(LaneKey::Metadata)).await;
        let partition_op = retain_write(&owner, partition_lane()).await;

        owner.reset().unwrap();

        for op in [metadata_op, partition_op] {
            assert!(matches!(
                owner.resume(op, deadline()).await,
                Err(IggyError::OperationNotRetained(missing)) if missing == op
            ));
            assert!(matches!(
                owner.abandon(op),
                Err(IggyError::OperationNotRetained(missing)) if missing == op
            ));
        }
        assert!(
            owner
                .enter_lane(Some(LaneKey::Metadata), deadline())
                .await
                .is_ok()
        );
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_ticket_frozen_before_a_reset_when_dropped_after_send_should_not_block_the_new_session()
     {
        let owner = bound_owner();
        let mut ticket = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        ticket
            .reserve(CREATE_STREAM_CODE, 0, deadline())
            .await
            .unwrap();
        ticket
            .freeze(
                CREATE_STREAM_CODE,
                Bytes::new(),
                PartitionContext::default(),
                None,
            )
            .unwrap();
        *ticket.sent_flag() = true;

        owner.reset().unwrap();
        drop(ticket);
        owner.bind(NEXT_SESSION).unwrap();

        assert!(
            owner
                .enter_lane(Some(LaneKey::Metadata), deadline())
                .await
                .is_ok()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_abandoned_op_when_resolving_or_abandoning_again_should_return_operation_not_retained()
     {
        let owner = bound_owner();
        let op = retain_write(&owner, Some(LaneKey::Metadata)).await;

        owner.abandon(op).unwrap();

        assert!(matches!(
            owner.abandon(op),
            Err(IggyError::OperationNotRetained(missing)) if missing == op
        ));
        assert!(matches!(
            owner.resume(op, deadline()).await,
            Err(IggyError::OperationNotRetained(missing)) if missing == op
        ));
        assert!(
            owner
                .enter_lane(Some(LaneKey::Metadata), deadline())
                .await
                .is_ok()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_resumed_write_when_reset_during_routing_should_preserve_its_unknown_outcome() {
        let owner = bound_owner();
        let op = retain_write(&owner, partition_lane()).await;
        let (mut replay, _retained) = owner.resume(op, deadline()).await.unwrap();
        owner.reset().unwrap();

        assert_eq!(
            replay.reserve(SEND_MESSAGES_CODE, 0, deadline()).await,
            Err(IggyError::Unauthenticated)
        );
        assert_eq!(replay.retain(), IggyError::OperationUncertain(op));
        assert!(matches!(
            owner.resume(op, deadline()).await,
            Err(IggyError::OperationNotRetained(missing)) if missing == op
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_resumed_op_when_its_replay_ends_should_stay_retained_until_completed() {
        let owner = bound_owner();
        let op = retain_write(&owner, Some(LaneKey::Metadata)).await;

        let (replay, retained) = owner.resume(op, deadline()).await.unwrap();
        assert_eq!(retained.op(), op);
        assert!(
            matches!(replay.retain(), IggyError::OperationUncertain(uncertain) if uncertain == op)
        );
        let (replay, _) = owner.resume(op, deadline()).await.unwrap();
        replay.complete();

        assert!(
            owner
                .enter_lane(Some(LaneKey::Metadata), deadline())
                .await
                .is_ok()
        );
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_idle_lanes_when_their_tickets_end_should_leave_the_table() {
        let owner = bound_owner();
        let holder = owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap();
        // A caller that goes away while it waits for the gate.
        let cancelled = timeout(
            SHORT_DEADLINE,
            owner.enter_lane(Some(LaneKey::Metadata), deadline()),
        )
        .await;
        assert!(cancelled.is_err());
        let partition = owner.enter_lane(partition_lane(), deadline()).await;
        assert_eq!(lane_count(&owner), 2);

        drop(partition);
        holder.complete();

        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_more_proved_replays_than_the_operation_limit_when_reading_after_a_reset_should_keep_only_the_newest()
     {
        const LIMIT: usize = 2;
        let owner = owner_with_limits(LIMIT, LIMIT * HEADER_SIZE);
        let outcomes = [Ok(()), Err(IggyError::InvalidOffset(0)), Ok(())];
        let mut proved = Vec::new();
        for outcome in outcomes {
            let op = retain_write(&owner, Some(LaneKey::Metadata)).await;
            let (replay, _) = owner.resume(op, deadline()).await.unwrap();
            replay.prove(outcome.clone());
            proved.push((op, outcome));
        }

        owner.reset().unwrap();

        let (aged_out, _) = proved[0];
        assert_eq!(
            owner.proved_outcome(aged_out),
            Err(IggyError::OperationNotRetained(aged_out))
        );
        for (op, outcome) in &proved[1..] {
            assert_eq!(&owner.proved_outcome(*op), outcome);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_abandon_during_a_replay_when_the_replay_proves_should_keep_its_outcome() {
        let owner = bound_owner();
        let op = retain_write(&owner, Some(LaneKey::Metadata)).await;
        let (replay, _) = owner.resume(op, deadline()).await.unwrap();

        owner.abandon(op).unwrap();
        replay.prove(Ok(()));

        assert_eq!(owner.proved_outcome(op), Ok(()));
        owner
            .enter_lane(Some(LaneKey::Metadata), deadline())
            .await
            .unwrap()
            .complete();
        assert_eq!(lane_count(&owner), 0);
    }
}
