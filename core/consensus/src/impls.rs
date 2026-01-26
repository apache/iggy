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

use crate::vsr_timeout::{TimeoutKind, TimeoutManager};
use crate::{
    Consensus, DvcQuorumArray, Pipeline, Project, StoredDvc, dvc_count, dvc_max_commit,
    dvc_quorum_array_empty, dvc_record, dvc_reset, dvc_select_winner,
};
use bit_set::BitSet;
use iggy_common::header::{
    Command2, DoViewChangeHeader, PrepareHeader, PrepareOkHeader, RequestHeader,
    StartViewChangeHeader, StartViewHeader,
};
use iggy_common::message::Message;
use message_bus::IggyMessageBus;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

pub trait Sequencer {
    type Sequence;
    /// Get the current sequence number
    fn current_sequence(&self) -> Self::Sequence;

    /// Allocate the next sequence number.
    /// TODO Should this return a Future<Output = u64>? for async case?
    fn next_sequence(&self) -> Self::Sequence;

    /// Update the current sequence number.
    fn set_sequence(&self, sequence: Self::Sequence);
}

pub struct LocalSequencer {
    op: Cell<u64>,
}

impl LocalSequencer {
    pub fn new(initial_op: u64) -> Self {
        Self {
            op: Cell::new(initial_op),
        }
    }
}

impl Sequencer for LocalSequencer {
    type Sequence = u64;

    fn current_sequence(&self) -> Self::Sequence {
        self.op.get()
    }

    fn next_sequence(&self) -> Self::Sequence {
        let current = self.current_sequence();
        let next = current.checked_add(1).expect("sequence number overflow");
        self.set_sequence(next);
        next
    }

    fn set_sequence(&self, sequence: Self::Sequence) {
        self.op.set(sequence);
    }
}

/// TODO The below numbers need to be added a consensus config
/// TODO understand how to configure these numbers.
/// Maximum number of prepares that can be in-flight in the pipeline.
pub const PIPELINE_PREPARE_QUEUE_MAX: usize = 8;

/// Maximum number of replicas in a cluster.
pub const REPLICAS_MAX: usize = 32;

pub struct PipelineEntry {
    pub message: Message<PrepareHeader>,
    /// Bitmap of replicas that have acknowledged this prepare.
    pub ok_from_replicas: BitSet<u32>,
    /// Whether we've received a quorum of prepare_ok messages.
    pub ok_quorum_received: bool,
}

impl PipelineEntry {
    pub fn new(message: Message<PrepareHeader>) -> Self {
        Self {
            message,
            ok_from_replicas: BitSet::with_capacity(REPLICAS_MAX),
            ok_quorum_received: false,
        }
    }

    /// Record a prepare_ok from the given replica.
    /// Returns the new count of acknowledgments.
    pub fn add_ack(&mut self, replica: u8) -> usize {
        self.ok_from_replicas.insert(replica as usize);
        self.ok_from_replicas.len()
    }

    /// Check if we have an ack from the given replica.
    pub fn has_ack(&self, replica: u8) -> bool {
        self.ok_from_replicas.contains(replica as usize)
    }

    /// Get the number of acks received.
    pub fn ack_count(&self) -> usize {
        self.ok_from_replicas.len()
    }
}

/// A request message waiting to be prepared.
pub struct RequestEntry {
    pub message: Message<RequestHeader>,
    /// Timestamp when the request was received (for ordering/timeout).
    pub received_at: i64, //TODO figure the correct way to do this
}

impl RequestEntry {
    pub fn new(message: Message<RequestHeader>) -> Self {
        Self {
            message,
            received_at: 0, //TODO figure the correct way to do this
        }
    }
}

pub struct LocalPipeline {
    /// Messages being prepared (uncommitted and being replicated).
    prepare_queue: VecDeque<PipelineEntry>,
}

impl Default for LocalPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalPipeline {
    pub fn new() -> Self {
        Self {
            prepare_queue: VecDeque::with_capacity(PIPELINE_PREPARE_QUEUE_MAX),
        }
    }

    pub fn prepare_count(&self) -> usize {
        self.prepare_queue.len()
    }

    pub fn prepare_queue_full(&self) -> bool {
        self.prepare_queue.len() >= PIPELINE_PREPARE_QUEUE_MAX
    }

    /// Returns true if prepare queue is full.
    pub fn is_full(&self) -> bool {
        self.prepare_queue_full()
    }

    pub fn is_empty(&self) -> bool {
        self.prepare_queue.is_empty()
    }

    /// Push a new message to the pipeline.
    ///
    /// # Panics
    /// - If message queue is full.
    /// - If the message doesn't chain correctly to the previous entry.
    pub fn push_message(&mut self, message: Message<PrepareHeader>) {
        assert!(!self.prepare_queue_full(), "prepare queue is full");

        let header = message.header();

        // Verify hash chain if there's a previous entry
        if let Some(tail) = self.prepare_queue.back() {
            let tail_header = tail.message.header();
            assert_eq!(
                header.op,
                tail_header.op + 1,
                "sequence must be sequential: expected {}, got {}",
                tail_header.op + 1,
                header.op
            );
            assert_eq!(
                header.parent, tail_header.checksum,
                "parent must chain to previous checksum"
            );
            assert!(header.view >= tail_header.view, "view cannot go backwards");
        }

        self.prepare_queue.push_back(PipelineEntry::new(message));
    }

    /// Pop the oldest message (after it's been committed).
    ///
    pub fn pop_message(&mut self) -> Option<PipelineEntry> {
        self.prepare_queue.pop_front()
    }

    /// Get the head (oldest) prepare.
    pub fn prepare_head(&self) -> Option<&PipelineEntry> {
        self.prepare_queue.front()
    }

    pub fn prepare_head_mut(&mut self) -> Option<&mut PipelineEntry> {
        self.prepare_queue.front_mut()
    }

    /// Get the tail (newest) prepare.
    pub fn prepare_tail(&self) -> Option<&PipelineEntry> {
        self.prepare_queue.back()
    }

    /// Find a message by op number and checksum.
    pub fn message_by_op_and_checksum(
        &mut self,
        op: u64,
        checksum: u128,
    ) -> Option<&mut PipelineEntry> {
        let head_op = self.prepare_queue.front()?.message.header().op;
        let tail_op = self.prepare_queue.back()?.message.header().op;

        // Verify consecutive ops invariant
        debug_assert_eq!(
            tail_op,
            head_op + self.prepare_queue.len() as u64 - 1,
            "prepare queue ops not consecutive"
        );

        if op < head_op || op > tail_op {
            return None;
        }

        let index = (op - head_op) as usize;
        let entry = self.prepare_queue.get_mut(index)?;

        debug_assert_eq!(entry.message.header().op, op);

        if entry.message.header().checksum == checksum {
            Some(entry)
        } else {
            None
        }
    }

    /// Find a message by op number only.
    pub fn message_by_op(&self, op: u64) -> Option<&PipelineEntry> {
        let head_op = self.prepare_queue.front()?.message.header().op;

        if op < head_op {
            return None;
        }

        let index = (op - head_op) as usize;
        self.prepare_queue.get(index)
    }

    /// Get mutable reference to a message entry by op number.
    /// Returns None if op is not in the pipeline.
    pub fn message_by_op_mut(&mut self, op: u64) -> Option<&mut PipelineEntry> {
        let head_op = self.prepare_queue.front()?.message.header().op;
        if op < head_op {
            return None;
        }
        let index = (op - head_op) as usize;
        if index >= self.prepare_queue.len() {
            return None;
        }
        self.prepare_queue.get_mut(index)
    }

    /// Get the entry at the head of the prepare queue (oldest uncommitted).
    pub fn head(&self) -> Option<&PipelineEntry> {
        self.prepare_queue.front()
    }

    /// Search prepare queue for a message from the given client.
    ///
    /// If there are multiple messages (possible in prepare_queue after view change),
    /// returns the latest one.
    pub fn has_message_from_client(&self, client: u128) -> bool {
        self.prepare_queue
            .iter()
            .any(|p| p.message.header().client == client)
    }

    /// Verify pipeline invariants.
    ///
    /// # Panics
    /// If any invariant is violated.
    pub fn verify(&self) {
        // Check capacity limits
        assert!(self.prepare_queue.len() <= PIPELINE_PREPARE_QUEUE_MAX);

        // Verify prepare queue hash chain
        if let Some(head) = self.prepare_queue.front() {
            let mut expected_op = head.message.header().op;
            let mut expected_parent = head.message.header().parent;

            for entry in &self.prepare_queue {
                let header = entry.message.header();

                assert_eq!(header.op, expected_op, "ops must be sequential");
                assert_eq!(header.parent, expected_parent, "must be hash-chained");

                expected_parent = header.checksum;
                expected_op += 1;
            }
        }
    }

    /// Clear prepare queue.
    pub fn clear(&mut self) {
        self.prepare_queue.clear();
    }
}

impl Pipeline for LocalPipeline {
    type Message = Message<PrepareHeader>;
    type Entry = PipelineEntry;

    fn push_message(&mut self, message: Self::Message) {
        LocalPipeline::push_message(self, message)
    }

    fn pop_message(&mut self) -> Option<Self::Entry> {
        LocalPipeline::pop_message(self)
    }

    fn clear(&mut self) {
        LocalPipeline::clear(self)
    }

    fn message_by_op_mut(&mut self, op: u64) -> Option<&mut Self::Entry> {
        LocalPipeline::message_by_op_mut(self, op)
    }

    fn message_by_op_and_checksum(&mut self, op: u64, checksum: u128) -> Option<&mut Self::Entry> {
        LocalPipeline::message_by_op_and_checksum(self, op, checksum)
    }

    fn is_full(&self) -> bool {
        LocalPipeline::is_full(self)
    }

    fn is_empty(&self) -> bool {
        LocalPipeline::is_empty(self)
    }

    fn verify(&self) {
        LocalPipeline::verify(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

/// Actions to be taken by the caller after processing a VSR event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VsrAction {
    /// Send StartViewChange to all replicas.
    SendStartViewChange { view: u32 },
    /// Send DoViewChange to primary.
    SendDoViewChange {
        view: u32,
        target: u8,
        log_view: u32,
        op: u64,
        commit: u64,
    },
    /// Send StartView to all backups (as new primary).
    SendStartView { view: u32, op: u64, commit: u64 },
    /// Send PrepareOK to primary.
    SendPrepareOk { view: u32, op: u64, target: u8 },
}

#[allow(unused)]
pub struct VsrConsensus {
    cluster: u128,
    replica: u8,
    replica_count: u8,

    view: Cell<u32>,

    // The latest view where
    // - the replica was a primary and acquired a DVC quorum, or
    // - the replica was a backup and processed a SV message.
    // i.e. the latest view in which this replica changed its head message.
    // Initialized from the superblock's VSRState.
    // Invariants:
    // * `replica.log_view ≥ replica.log_view_durable`
    // * `replica.log_view = 0` when replica_count=1.
    log_view: Cell<u32>,
    status: Cell<Status>,
    commit: Cell<u64>,

    sequencer: LocalSequencer,

    last_timestamp: Cell<u64>,
    last_prepare_checksum: Cell<u128>,

    pipeline: RefCell<LocalPipeline>,

    message_bus: IggyMessageBus,
    // TODO: Add loopback_queue for messages to self
    /// Tracks start view change messages received from all replicas (including self)
    start_view_change_from_all_replicas: RefCell<BitSet<u32>>,

    /// Tracks DVC messages received (only used by primary candidate)
    /// Stores metadata; actual log comes from message
    do_view_change_from_all_replicas: RefCell<DvcQuorumArray>,
    /// Whether DVC quorum has been achieved in current view change
    do_view_change_quorum: Cell<bool>,
    /// Whether we've sent our own SVC for current view
    sent_own_start_view_change: Cell<bool>,
    /// Whether we've sent our own DVC for current view
    sent_own_do_view_change: Cell<bool>,

    timeouts: RefCell<TimeoutManager>,
}

impl VsrConsensus {
    pub fn new(cluster: u128, replica: u8, replica_count: u8) -> Self {
        assert!(
            replica < replica_count,
            "replica index must be < replica_count"
        );
        assert!(replica_count >= 1, "need at least 1 replica");
        Self {
            cluster,
            replica,
            replica_count,
            view: Cell::new(0),
            log_view: Cell::new(0),
            status: Cell::new(Status::Recovering),
            sequencer: LocalSequencer::new(0),
            commit: Cell::new(0),
            last_timestamp: Cell::new(0),
            last_prepare_checksum: Cell::new(0),
            pipeline: RefCell::new(LocalPipeline::new()),
            message_bus: IggyMessageBus::new(replica_count as usize, replica as u16, 0),
            start_view_change_from_all_replicas: RefCell::new(BitSet::with_capacity(REPLICAS_MAX)),
            do_view_change_from_all_replicas: RefCell::new(dvc_quorum_array_empty()),
            do_view_change_quorum: Cell::new(false),
            sent_own_start_view_change: Cell::new(false),
            sent_own_do_view_change: Cell::new(false),
            timeouts: RefCell::new(TimeoutManager::new(replica as u128)),
        }
    }

    pub fn primary_index(&self, view: u32) -> u8 {
        view as u8 % self.replica_count
    }

    pub fn is_primary(&self) -> bool {
        self.primary_index(self.view.get()) == self.replica
    }

    pub fn advance_commit_number(&self, commit: u64) {
        if commit > self.commit.get() {
            self.commit.set(commit);
        }

        assert!(self.commit.get() >= commit);
    }

    /// Maximum number of faulty replicas that can be tolerated.
    /// For a cluster of 2f+1 replicas, this returns f.
    pub fn max_faulty(&self) -> usize {
        (self.replica_count as usize - 1) / 2
    }

    /// Quorum size = f + 1 = max_faulty + 1
    pub fn quorum(&self) -> usize {
        self.max_faulty() + 1
    }

    pub fn commit(&self) -> u64 {
        self.commit.get()
    }

    pub fn is_syncing(&self) -> bool {
        // for now return false. we have to add syncing related setup to VsrConsensus to make this work.
        false
    }

    pub fn replica(&self) -> u8 {
        self.replica
    }

    pub fn sequencer(&self) -> &LocalSequencer {
        &self.sequencer
    }

    pub fn view(&self) -> u32 {
        self.view.get()
    }

    pub fn set_view(&mut self, view: u32) {
        self.view.set(view);
    }

    pub fn status(&self) -> Status {
        self.status.get()
    }

    pub fn pipeline(&self) -> &RefCell<LocalPipeline> {
        &self.pipeline
    }

    pub fn pipeline_mut(&mut self) -> &mut RefCell<LocalPipeline> {
        &mut self.pipeline
    }

    pub fn cluster(&self) -> u128 {
        self.cluster
    }

    pub fn replica_count(&self) -> u8 {
        self.replica_count
    }

    pub fn log_view(&self) -> u32 {
        self.log_view.get()
    }

    pub fn set_log_view(&self, log_view: u32) {
        self.log_view.set(log_view);
    }

    pub fn is_primary_for_view(&self, view: u32) -> bool {
        self.primary_index(view) == self.replica
    }

    /// Count SVCs from OTHER replicas (excluding self).
    fn svc_count_excluding_self(&self) -> usize {
        let svc = self.start_view_change_from_all_replicas.borrow();
        let total = svc.len();
        if svc.contains(self.replica as usize) {
            total.saturating_sub(1)
        } else {
            total
        }
    }

    /// Reset SVC quorum tracking.
    fn reset_svc_quorum(&self) {
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .clear();
    }

    /// Reset DVC quorum tracking.
    fn reset_dvc_quorum(&self) {
        dvc_reset(&mut self.do_view_change_from_all_replicas.borrow_mut());
        self.do_view_change_quorum.set(false);
    }

    /// Reset all view change state for a new view.
    fn reset_view_change_state(&self) {
        self.reset_svc_quorum();
        self.reset_dvc_quorum();
        self.sent_own_start_view_change.set(false);
        self.sent_own_do_view_change.set(false);
    }

    /// Process one tick. Call this periodically (e.g., every 10ms).
    ///
    /// Returns a list of actions to take based on fired timeouts.
    /// Empty vec means no actions needed.
    pub fn tick(&self, current_op: u64, current_commit: u64) -> Vec<VsrAction> {
        let mut actions = Vec::new();
        let mut timeouts = self.timeouts.borrow_mut();

        // Phase 1: Tick all timeouts
        timeouts.tick();

        // Phase 2: Handle fired timeouts
        if timeouts.fired(TimeoutKind::NormalHeartbeat) {
            drop(timeouts);
            actions.extend(self.handle_normal_heartbeat_timeout());
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::StartViewChangeMessage) {
            drop(timeouts);
            actions.extend(self.handle_start_view_change_message_timeout());
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::DoViewChangeMessage) {
            drop(timeouts);
            actions.extend(self.handle_do_view_change_message_timeout(current_op, current_commit));
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::ViewChangeStatus) {
            drop(timeouts);
            actions.extend(self.handle_view_change_status_timeout());
            // timeouts = self.timeouts.borrow_mut(); // Not needed if last
        }

        actions
    }

    /// Called when normal_heartbeat timeout fires.
    /// Backup hasn't heard from primary - start view change.
    fn handle_normal_heartbeat_timeout(&self) -> Vec<VsrAction> {
        // Only backups trigger view change on heartbeat timeout
        if self.is_primary() {
            return Vec::new();
        }

        // Already in view change
        if self.status.get() == Status::ViewChange {
            return Vec::new();
        }

        // Advance to new view and transition to view change
        let new_view = self.view.get() + 1;

        self.view.set(new_view);
        self.status.set(Status::ViewChange);
        self.reset_view_change_state();
        self.sent_own_start_view_change.set(true);
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .insert(self.replica as usize);

        // Update timeouts for view change status
        {
            let mut timeouts = self.timeouts.borrow_mut();
            timeouts.stop(TimeoutKind::NormalHeartbeat);
            timeouts.start(TimeoutKind::StartViewChangeMessage);
            timeouts.start(TimeoutKind::ViewChangeStatus);
        }

        vec![VsrAction::SendStartViewChange { view: new_view }]
    }

    /// Resend SVC message if we've started view change.
    fn handle_start_view_change_message_timeout(&self) -> Vec<VsrAction> {
        if !self.sent_own_start_view_change.get() {
            return Vec::new();
        }

        self.timeouts
            .borrow_mut()
            .reset(TimeoutKind::StartViewChangeMessage);

        vec![VsrAction::SendStartViewChange {
            view: self.view.get(),
        }]
    }

    /// Resend DVC message if we've sent one.
    fn handle_do_view_change_message_timeout(
        &self,
        current_op: u64,
        current_commit: u64,
    ) -> Vec<VsrAction> {
        if self.status.get() != Status::ViewChange {
            return Vec::new();
        }

        if !self.sent_own_do_view_change.get() {
            return Vec::new();
        }

        // If we're primary candidate with quorum, don't resend
        if self.is_primary() && self.do_view_change_quorum.get() {
            return Vec::new();
        }

        self.timeouts
            .borrow_mut()
            .reset(TimeoutKind::DoViewChangeMessage);

        vec![VsrAction::SendDoViewChange {
            view: self.view.get(),
            target: self.primary_index(self.view.get()),
            log_view: self.log_view.get(),
            op: current_op,
            commit: current_commit,
        }]
    }

    /// Escalate to next view if stuck in view change.
    fn handle_view_change_status_timeout(&self) -> Vec<VsrAction> {
        if self.status.get() != Status::ViewChange {
            return Vec::new();
        }

        // Escalate: try next view
        let next_view = self.view.get() + 1;

        self.view.set(next_view);
        self.reset_view_change_state();
        self.sent_own_start_view_change.set(true);
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .insert(self.replica as usize);

        self.timeouts
            .borrow_mut()
            .reset(TimeoutKind::ViewChangeStatus);

        vec![VsrAction::SendStartViewChange { view: next_view }]
    }

    /// Handle a received StartViewChange message.
    ///
    /// "When replica i receives STARTVIEWCHANGE messages for its view-number
    /// from f OTHER replicas, it sends a DOVIEWCHANGE message to the node
    /// that will be the primary in the new view."
    pub fn handle_start_view_change(&self, header: &StartViewChangeHeader) -> Vec<VsrAction> {
        let from_replica = header.replica;
        let msg_view = header.view;

        // Ignore SVCs for old views
        if msg_view < self.view.get() {
            return Vec::new();
        }

        let mut actions = Vec::new();

        // If SVC is for a higher view, advance to that view
        if msg_view > self.view.get() {
            self.view.set(msg_view);
            self.status.set(Status::ViewChange);
            self.reset_view_change_state();
            self.sent_own_start_view_change.set(true);
            self.start_view_change_from_all_replicas
                .borrow_mut()
                .insert(self.replica as usize);

            // Update timeouts
            {
                let mut timeouts = self.timeouts.borrow_mut();
                timeouts.stop(TimeoutKind::NormalHeartbeat);
                timeouts.start(TimeoutKind::StartViewChangeMessage);
                timeouts.start(TimeoutKind::ViewChangeStatus);
            }

            // Send our own SVC
            actions.push(VsrAction::SendStartViewChange { view: msg_view });
        }

        // Record the SVC from sender
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .insert(from_replica as usize);

        // Check if we have f SVCs from OTHER replicas
        // We need f SVCs from others to send DVC
        if !self.sent_own_do_view_change.get()
            && self.svc_count_excluding_self() >= self.max_faulty()
        {
            self.sent_own_do_view_change.set(true);

            let primary_candidate = self.primary_index(self.view.get());
            let current_op = self.sequencer.current_sequence();
            let current_commit = self.commit.get();

            // Start DVC timeout
            self.timeouts
                .borrow_mut()
                .start(TimeoutKind::DoViewChangeMessage);

            actions.push(VsrAction::SendDoViewChange {
                view: self.view.get(),
                target: primary_candidate,
                log_view: self.log_view.get(),
                op: current_op,
                commit: current_commit,
            });

            // If we are the primary candidate, record our own DVC
            if primary_candidate == self.replica {
                let own_dvc = StoredDvc {
                    replica: self.replica,
                    log_view: self.log_view.get(),
                    op: current_op,
                    commit: current_commit,
                };
                dvc_record(
                    &mut self.do_view_change_from_all_replicas.borrow_mut(),
                    own_dvc,
                );

                // Check if we now have quorum
                if dvc_count(&self.do_view_change_from_all_replicas.borrow()) >= self.quorum() {
                    self.do_view_change_quorum.set(true);
                    actions.extend(self.complete_view_change_as_primary());
                }
            }
        }

        actions
    }

    /// Handle a received DoViewChange message (only relevant for primary candidate).
    ///
    /// "When the new primary receives f + 1 DOVIEWCHANGE messages from different
    /// replicas (including itself), it sets its view-number to that in the messages
    /// and selects as the new log the one contained in the message with the largest v'..."
    pub fn handle_do_view_change(&self, header: &DoViewChangeHeader) -> Vec<VsrAction> {
        let from_replica = header.replica;
        let msg_view = header.view;
        let msg_log_view = header.log_view;
        let msg_op = header.op;
        let msg_commit = header.commit;

        // Ignore DVCs for old views
        if msg_view < self.view.get() {
            return Vec::new();
        }

        let mut actions = Vec::new();

        // If DVC is for a higher view, advance to that view
        if msg_view > self.view.get() {
            self.view.set(msg_view);
            self.status.set(Status::ViewChange);
            self.reset_view_change_state();
            self.sent_own_start_view_change.set(true);
            self.start_view_change_from_all_replicas
                .borrow_mut()
                .insert(self.replica as usize);

            // Update timeouts
            {
                let mut timeouts = self.timeouts.borrow_mut();
                timeouts.stop(TimeoutKind::NormalHeartbeat);
                timeouts.start(TimeoutKind::StartViewChangeMessage);
                timeouts.start(TimeoutKind::ViewChangeStatus);
            }

            // Send our own SVC
            actions.push(VsrAction::SendStartViewChange { view: msg_view });
        }

        // Only the primary candidate processes DVCs for quorum
        if !self.is_primary_for_view(self.view.get()) {
            return actions;
        }

        // Must be in view change to process DVCs
        if self.status.get() != Status::ViewChange {
            return actions;
        }

        let current_op = self.sequencer.current_sequence();
        let current_commit = self.commit.get();

        // If we haven't sent our own DVC yet, record it
        if !self.sent_own_do_view_change.get() {
            self.sent_own_do_view_change.set(true);

            let own_dvc = StoredDvc {
                replica: self.replica,
                log_view: self.log_view.get(),
                op: current_op,
                commit: current_commit,
            };
            dvc_record(
                &mut self.do_view_change_from_all_replicas.borrow_mut(),
                own_dvc,
            );
        }

        // Record the received DVC
        let dvc = StoredDvc {
            replica: from_replica,
            log_view: msg_log_view,
            op: msg_op,
            commit: msg_commit,
        };
        dvc_record(&mut self.do_view_change_from_all_replicas.borrow_mut(), dvc);

        // Check if quorum achieved
        if !self.do_view_change_quorum.get()
            && dvc_count(&self.do_view_change_from_all_replicas.borrow()) >= self.quorum()
        {
            self.do_view_change_quorum.set(true);
            actions.extend(self.complete_view_change_as_primary());
        }

        actions
    }

    /// Handle a received StartView message (backups only).
    ///
    /// "When other replicas receive the STARTVIEW message, they replace their log
    /// with the one in the message, set their op-number to that of the latest entry
    /// in the log, set their view-number to the view number in the message, change
    /// their status to normal, and send PrepareOK for any uncommitted ops."
    pub fn handle_start_view(&self, header: &StartViewHeader) -> Vec<VsrAction> {
        let from_replica = header.replica;
        let msg_view = header.view;
        let msg_op = header.op;
        let msg_commit = header.commit;

        // Verify sender is the primary for this view
        if self.primary_index(msg_view) != from_replica {
            return Vec::new();
        }

        // Ignore old views
        if msg_view < self.view.get() {
            return Vec::new();
        }

        // We shouldn't process our own StartView
        if from_replica == self.replica {
            return Vec::new();
        }

        // Accept the StartView and transition to normal
        self.view.set(msg_view);
        self.log_view.set(msg_view);
        self.status.set(Status::Normal);
        self.advance_commit_number(msg_commit);
        self.reset_view_change_state();

        // Update our op to match the new primary's log
        self.sequencer.set_sequence(msg_op);

        // Update timeouts for normal backup operation
        {
            let mut timeouts = self.timeouts.borrow_mut();
            timeouts.stop(TimeoutKind::ViewChangeStatus);
            timeouts.stop(TimeoutKind::DoViewChangeMessage);
            timeouts.stop(TimeoutKind::RequestStartViewMessage);
            timeouts.start(TimeoutKind::NormalHeartbeat);
        }

        // Send PrepareOK for uncommitted ops (commit+1 to op)
        let mut actions = Vec::new();
        for op_num in (msg_commit + 1)..=msg_op {
            actions.push(VsrAction::SendPrepareOk {
                view: msg_view,
                op: op_num,
                target: from_replica, // Send to new primary
            });
        }

        actions
    }

    /// Complete view change as the new primary after collecting DVC quorum.
    fn complete_view_change_as_primary(&self) -> Vec<VsrAction> {
        let dvc_array = self.do_view_change_from_all_replicas.borrow();

        let Some(winner) = dvc_select_winner(&dvc_array) else {
            return Vec::new();
        };

        let new_op = winner.op;
        let max_commit = dvc_max_commit(&dvc_array);

        // Update state
        self.log_view.set(self.view.get());
        self.status.set(Status::Normal);
        self.advance_commit_number(max_commit);

        // Update timeouts for normal primary operation
        {
            let mut timeouts = self.timeouts.borrow_mut();
            timeouts.stop(TimeoutKind::ViewChangeStatus);
            timeouts.stop(TimeoutKind::DoViewChangeMessage);
            timeouts.stop(TimeoutKind::StartViewChangeMessage);
            timeouts.start(TimeoutKind::CommitMessage);
        }

        vec![VsrAction::SendStartView {
            view: self.view.get(),
            op: new_op,
            commit: max_commit,
        }]
    }

    /// Handle a prepare_ok message from a follower.
    /// Called on the primary when a follower acknowledges a prepare.
    ///
    /// Returns true if quorum was just reached for this op.
    pub fn handle_prepare_ok(&self, message: Message<PrepareOkHeader>) -> bool {
        let header = message.header();

        assert_eq!(header.command, Command2::PrepareOk);
        assert!(
            header.replica < self.replica_count,
            "handle_prepare_ok: invalid replica {}",
            header.replica
        );

        // Ignore if not in normal status
        if self.status() != Status::Normal {
            return false;
        }

        // Ignore if from older view
        if header.view < self.view() {
            return false;
        }

        // Ignore if from newer view. This shouldn't happen if we're primary
        if header.view > self.view() {
            return false;
        }

        // We must be primary to process prepare_ok
        if !self.is_primary() {
            return false;
        }

        // Ignore if syncing
        if self.is_syncing() {
            return false;
        }

        // Find the prepare in our pipeline
        let mut pipeline = self.pipeline.borrow_mut();

        let Some(entry) = pipeline.message_by_op_mut(header.op) else {
            // Not in pipeline - could be old/duplicate or already committed
            return false;
        };

        // Verify checksum matches
        if entry.message.header().checksum != header.prepare_checksum {
            return false;
        }

        // Verify the prepare is for a valid op range
        let _commit = self.commit();

        // Check for duplicate ack
        if entry.has_ack(header.replica) {
            return false;
        }

        // Record the ack from this replica
        let ack_count = entry.add_ack(header.replica);
        let quorum = self.quorum();

        // Check if we've reached quorum
        if ack_count >= quorum && !entry.ok_quorum_received {
            entry.ok_quorum_received = true;

            return true;
        }

        false
    }

    pub fn message_bus(&self) -> &IggyMessageBus {
        &self.message_bus
    }
}

impl Project<Message<PrepareHeader>> for Message<RequestHeader> {
    type Consensus = VsrConsensus;

    fn project(self, consensus: &Self::Consensus) -> Message<PrepareHeader> {
        let op = consensus.sequencer.current_sequence() + 1;

        self.transmute_header(|old, new| {
            *new = PrepareHeader {
                cluster: consensus.cluster,
                size: old.size,
                epoch: 0,
                view: consensus.view.get(),
                release: old.release,
                command: Command2::Prepare,
                replica: consensus.replica,
                parent: 0, // TODO: Get parent checksum from the previous entry in the journal (figure out how to pass that ctx here)
                request_checksum: old.request_checksum,
                request: old.request,
                commit: consensus.commit.get(),
                op,
                timestamp: 0, // 0 for now. Implement correct way to get timestamp later
                operation: old.operation,
                ..Default::default()
            }
        })
    }
}

impl Project<Message<PrepareOkHeader>> for Message<PrepareHeader> {
    type Consensus = VsrConsensus;

    fn project(self, consensus: &Self::Consensus) -> Message<PrepareOkHeader> {
        self.transmute_header(|old, new| {
            *new = PrepareOkHeader {
                command: Command2::PrepareOk,
                parent: old.parent,
                prepare_checksum: old.checksum,
                request: old.request,
                cluster: consensus.cluster,
                replica: consensus.replica,
                epoch: 0, // TODO: consensus.epoch
                // It's important to use the view of the replica, not the received prepare!
                view: consensus.view.get(),
                op: old.op,
                commit: consensus.commit.get(),
                timestamp: old.timestamp,
                operation: old.operation,
                // PrepareOks are only header no body
                ..Default::default()
            };
        })
    }
}

impl Consensus for VsrConsensus {
    type MessageBus = IggyMessageBus;

    type RequestMessage = Message<RequestHeader>;
    type ReplicateMessage = Message<PrepareHeader>;
    type AckMessage = Message<PrepareOkHeader>;
    type Sequencer = LocalSequencer;
    type Pipeline = LocalPipeline;

    fn pipeline_message(&self, message: Self::ReplicateMessage) {
        assert!(self.is_primary(), "only primary can pipeline messages");

        let mut pipeline = self.pipeline.borrow_mut();
        pipeline.push_message(message);
    }

    fn verify_pipeline(&self) {
        let pipeline = self.pipeline.borrow();
        pipeline.verify();
    }

    fn post_replicate_verify(&self, message: &Self::ReplicateMessage) {
        let header = message.header();

        // verify the message belongs to our cluster
        assert_eq!(header.cluster, self.cluster, "cluster mismatch");

        // verify view is not from the future
        assert!(
            header.view <= self.view.get(),
            "prepare view {} is ahead of replica view {}",
            header.view,
            self.view.get()
        );

        // verify op is sequential
        assert_eq!(
            header.op,
            self.sequencer.current_sequence() + 1,
            "op must be sequential: expected {}, got {}",
            self.sequencer.current_sequence() + 1,
            header.op
        );

        // verify hash chain
        assert_eq!(
            header.parent,
            self.last_prepare_checksum.get(),
            "parent checksum mismatch"
        );
    }

    fn is_follower(&self) -> bool {
        !self.is_primary()
    }

    fn is_syncing(&self) -> bool {
        self.is_syncing()
    }
}
