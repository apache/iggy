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

use crate::{Consensus, Project};
use bit_set::BitSet;
use iggy_common::header::{Command2, PrepareHeader, PrepareOkHeader, RequestHeader};
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

pub struct Pipeline {
    /// Messages being prepared (uncommitted and being replicated).
    prepare_queue: VecDeque<PipelineEntry>,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
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

    /// Push a new prepare to the pipeline.
    ///
    /// # Panics
    /// - If prepare queue is full.
    /// - If the prepare doesn't chain correctly to the previous entry.
    pub fn push_prepare(&mut self, message: Message<PrepareHeader>) {
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

    /// Pop the oldest prepare (after it's been committed).
    ///
    pub fn pop_prepare(&mut self) -> Option<PipelineEntry> {
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

    /// Find a prepare by op number and checksum.
    pub fn prepare_by_op_and_checksum(
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

    /// Find a prepare by op number only.
    pub fn prepare_by_op(&self, op: u64) -> Option<&PipelineEntry> {
        let head_op = self.prepare_queue.front()?.message.header().op;

        if op < head_op {
            return None;
        }

        let index = (op - head_op) as usize;
        self.prepare_queue.get(index)
    }

    /// Get mutable reference to a prepare entry by op number.
    /// Returns None if op is not in the pipeline.
    pub fn prepare_by_op_mut(&mut self, op: u64) -> Option<&mut PipelineEntry> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

#[allow(unused)]
pub struct VsrConsensus {
    cluster: u128,
    replica: u8,
    replica_count: u8,

    view: Cell<u32>,
    log_view: Cell<u32>,
    status: Cell<Status>,
    commit: Cell<u64>,

    sequencer: LocalSequencer,

    last_timestamp: Cell<u64>,
    last_prepare_checksum: Cell<u128>,

    pipeline: RefCell<Pipeline>,

    message_bus: IggyMessageBus,
    // TODO: Add loopback_queue for messages to self
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
            pipeline: RefCell::new(Pipeline::new()),
            message_bus: IggyMessageBus::new(replica_count as usize, replica as u16, 0),
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

    pub fn quorum(&self) -> usize {
        (self.replica_count as usize / 2) + 1
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

    pub fn status(&self) -> Status {
        self.status.get()
    }

    pub fn pipeline(&self) -> &RefCell<Pipeline> {
        &self.pipeline
    }

    pub fn pipeline_mut(&mut self) -> &mut RefCell<Pipeline> {
        &mut self.pipeline
    }

    pub fn cluster(&self) -> u128 {
        self.cluster
    }

    pub fn replica_count(&self) -> u8 {
        self.replica_count
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

        let Some(entry) = pipeline.prepare_by_op_mut(header.op) else {
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

    fn pipeline_message(&self, message: Self::ReplicateMessage) {
        assert!(self.is_primary(), "only primary can pipeline messages");

        let mut pipeline = self.pipeline.borrow_mut();
        pipeline.push_prepare(message);
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
