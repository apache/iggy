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

use super::SessionOwner;
use crate::driver::PartitionKey;
use crate::links::Frame;
use crate::vsr::encode_request_header;
use bytes::Bytes;
use iggy_binary_protocol::consensus::{Operation, operation_for_code};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::IggyError;
use std::collections::HashMap;
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
    /// no lane. This includes auto-commit polls: their offset effect does not
    /// acquire a client mutation lane.
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
}

impl RetainedOperation {
    pub(crate) fn op(&self) -> u64 {
        self.frame.header.request
    }
}

/// The right of one call to send in its lane. Dropping the ticket frees the
/// lane, except that a frame which left the client without a definitive
/// outcome stays retained.
#[derive(Debug)]
pub(crate) struct LaneTicket {
    owner: Arc<SessionOwner>,
    /// `None` for a call that takes no lane.
    key: Option<LaneKey>,
    guard: Option<OwnedMutexGuard<()>>,
    operation: Option<RetainedOperation>,
    fence: u64,
    /// A resume replays an operation that its lane still retains.
    resumed: bool,
    sent: bool,
    done: bool,
}

impl SessionOwner {
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
        lane.retained = None;
        if lane.tickets == 0 {
            lanes.remove(&key);
        }
        Ok(())
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
            fence: 0,
            resumed,
            sent: false,
            done: false,
        }
    }

    /// Takes the next request id and freezes the frame of the call. A call
    /// that starts a new operation in the same lane freezes again.
    pub(crate) fn freeze(
        &mut self,
        code: u32,
        body: Bytes,
        context: PartitionContext,
        partition: Option<PartitionKey>,
    ) -> Result<Frame, IggyError> {
        let mut session = self.owner.session()?;
        let (mut header, _) = encode_request_header(&mut session, code, &body)?;
        context.stamp(&mut header);
        // A reset raises the fence under the session lock, so this fence
        // names the session of the header.
        self.fence = self.owner.fence();
        drop(session);
        let frame = Frame { header, body };
        self.operation = Some(RetainedOperation {
            code,
            frame: frame.clone(),
            partition,
        });
        self.sent = false;
        Ok(frame)
    }

    pub(crate) fn fence(&self) -> u64 {
        self.fence
    }

    pub(crate) fn takes_lane(&self) -> bool {
        self.key.is_some()
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

    /// Ends the call with an unknown outcome. A lane keeps the operation
    /// retained, and a call without a lane has nothing to retain.
    pub(crate) fn retain(self) -> IggyError {
        let op = self.operation.as_ref().map_or(0, RetainedOperation::op);
        if self.takes_lane() {
            IggyError::OperationUncertain(op)
        } else {
            IggyError::TransientNotCommitted
        }
    }
}

impl Drop for LaneTicket {
    fn drop(&mut self) {
        let Some(key) = self.key else {
            return;
        };
        let mut lanes = self.owner.lanes();
        if let Some(lane) = lanes.get_mut(&key) {
            if let Some(operation) = self.operation.take() {
                if self.resumed {
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
            lane.tickets = lane.tickets.saturating_sub(1);
            if lane.tickets == 0 && lane.retained.is_none() {
                lanes.remove(&key);
            }
        }
        drop(lanes);
        // The gate opens only after the slot holds the retained operation, so
        // the next call in the lane sees it.
        self.guard.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, LOGOUT_USER_CODE, PING_CODE, SEND_MESSAGES_CODE,
    };
    use iggy_common::AutoLogin;
    use std::time::Duration;
    use tokio::time::timeout;

    const BOUND_SESSION: u64 = 3;
    const NEXT_SESSION: u64 = 4;
    const INCARNATION: u64 = 5;
    const PARTITION_ID: u32 = 1;
    const DEADLINE: Duration = Duration::from_secs(30);
    const SHORT_DEADLINE: Duration = Duration::from_secs(1);

    fn bound_owner() -> Arc<SessionOwner> {
        let owner = Arc::new(SessionOwner::new(AutoLogin::Disabled));
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
}
