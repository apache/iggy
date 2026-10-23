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
//! Auto-commit and Next polls share the partition gate while active. Their
//! progress is not a replayable mutation, so uncertainty releases that gate.

use super::SessionOwner;
use super::pending::Reservation;
use crate::driver::PartitionKey;
use crate::links::Frame;
use crate::vsr::encode_request_header;
use bytes::Bytes;
use iggy_binary_protocol::HEADER_SIZE;
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
}

impl LaneKey {
    /// The lane of `code`. A write to a partition takes the lane of the
    /// incarnation that its context carries. Reads, Register and Logout take
    /// no lane by code alone. The driver selects the partition lane for
    /// auto-commit and Next polls from their decoded arguments.
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
}

/// The right of one call to send in its lane. Dropping the ticket frees the
/// lane, except that a mutation which left the client without a definitive
/// outcome stays retained. A progress poll holds the same gate only while
/// active; replaying its frame would not resolve its earlier progress.
#[derive(Debug)]
pub(crate) struct LaneTicket {
    owner: Arc<SessionOwner>,
    /// `None` for a call that takes no lane.
    key: Option<LaneKey>,
    guard: Option<OwnedMutexGuard<()>>,
    operation: Option<RetainedOperation>,
    reservation: Option<Arc<Reservation>>,
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
        self.lanes().values().any(|lane| lane.holds(op))
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
        let Some(key) = key else {
            return Ok(LaneTicket::new(Arc::clone(self), None, false));
        };
        let (mut ticket, gate) = {
            let mut lanes = self.lanes();
            let lane = lanes.entry(key).or_default();
            if let Some(retained) = &lane.retained {
                return Err(IggyError::LaneBlocked(retained.op()));
            }
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
        // The call that held the gate can have left its operation retained.
        let blocked = self
            .lanes()
            .get(&key)
            .and_then(|lane| lane.retained.as_ref().map(RetainedOperation::op));
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
        let (mut ticket, gate) = {
            let mut lanes = self.lanes();
            let Some((key, lane)) = lanes.iter_mut().find(|(_, lane)| lane.holds(op)) else {
                return Err(IggyError::OperationNotRetained(op));
            };
            lane.tickets += 1;
            (
                LaneTicket::new(Arc::clone(self), Some(*key), true),
                Arc::clone(&lane.gate),
            )
        };
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

/// The outcome of every retained operation stays unknown after a session
/// reset, and none of them can replay under the new session.
pub(super) fn drop_retained(lanes: &mut Lanes) {
    lanes.retain(|_, lane| {
        lane.retained = None;
        lane.tickets > 0
    });
}

impl LaneTicket {
    fn new(owner: Arc<SessionOwner>, key: Option<LaneKey>, resumed: bool) -> Self {
        Self {
            owner,
            key,
            guard: None,
            operation: None,
            reservation: None,
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

    /// Ends the call with an unknown outcome. Only mutations remain retained;
    /// a progress poll's weak acknowledgement cannot be recovered by replay.
    pub(crate) fn retain(self) -> IggyError {
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
            if lane.tickets == 0 && lane.retained.is_none() {
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
        CREATE_STREAM_CODE, LOGOUT_USER_CODE, PING_CODE, POLL_MESSAGES_CODE, SEND_MESSAGES_CODE,
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
    async fn given_a_progress_poll_when_waiting_for_reply_should_block_its_partition_only() {
        let owner = bound_owner();
        let context = PartitionContext {
            incarnation: INCARNATION,
            ..PartitionContext::default()
        };
        let mut poll = owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap();
        poll.freeze(POLL_MESSAGES_CODE, Bytes::new(), context, None)
            .unwrap();
        *poll.sent_flag() = true;
        let next_id = owner.session().unwrap().current_request_id();

        let blocked = owner
            .enter_lane(partition_lane(), Instant::now() + SHORT_DEADLINE)
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
        pure_read.complete();
        other_partition.complete();
        poll.complete();

        owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap()
            .complete();
        assert_eq!(lane_count(&owner), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sent_progress_poll_when_cancelled_or_uncertain_should_release_without_retaining()
     {
        for cancelled in [true, false] {
            let owner = bound_owner();
            let mut poll = owner
                .enter_lane(partition_lane(), deadline())
                .await
                .unwrap();
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

            if cancelled {
                drop(poll);
            } else {
                assert_eq!(poll.retain(), IggyError::TransientNotCommitted);
            }

            let op = frame.header.request;
            assert!(matches!(
                owner.resume(op, deadline()).await,
                Err(IggyError::OperationNotRetained(missing)) if missing == op
            ));
            assert!(matches!(
                owner.abandon(op),
                Err(IggyError::OperationNotRetained(missing)) if missing == op
            ));
            owner
                .enter_lane(partition_lane(), deadline())
                .await
                .unwrap()
                .complete();
            assert_eq!(lane_count(&owner), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_retained_partition_mutation_when_progress_poll_enters_should_remain_blocked() {
        let owner = bound_owner();
        let mut mutation = owner
            .enter_lane(partition_lane(), deadline())
            .await
            .unwrap();
        mutation
            .reserve(SEND_MESSAGES_CODE, 0, deadline())
            .await
            .unwrap();
        let frame = mutation
            .freeze(
                SEND_MESSAGES_CODE,
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

        let progress_poll = owner.enter_lane(partition_lane(), deadline()).await;

        assert!(
            matches!(progress_poll, Err(IggyError::LaneBlocked(op)) if op == frame.header.request)
        );
        assert_eq!(owner.session().unwrap().current_request_id(), next_id);
        let (replay, retained) = owner
            .resume(frame.header.request, deadline())
            .await
            .unwrap();
        assert_eq!(retained.code, SEND_MESSAGES_CODE);
        replay.complete();
        owner
            .enter_lane(partition_lane(), deadline())
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
