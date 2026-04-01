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

//! Simplified VSR consensus state machine for visualization.
//!
//! This is a standalone implementation that mirrors the real protocol
//! in `core/consensus/src/impls.rs`, simplified for UI simulation purposes.

use super::types::{MessageType, ReplicaState, Role, Status, VsrMessage};

/// Maximum number of entries in the pipeline.
const MAX_PIPELINE_DEPTH: usize = 8;

/// Default heartbeat timeout in ticks.
const DEFAULT_HEARTBEAT_TIMEOUT: u64 = 500;

/// An entry in the replica's prepare pipeline.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PipelineEntry {
    pub op: u64,
    pub client_id: u8,
    pub request_id: u64,
    /// Bitset of replicas that have acknowledged this prepare.
    pub acks: Vec<bool>,
    pub quorum_reached: bool,
}

/// A single VSR replica with consensus state.
#[derive(Debug, Clone)]
pub struct Replica {
    pub id: u8,
    pub replica_count: u8,
    pub view: u32,
    pub log_view: u32,
    pub status: Status,
    pub commit: u64,
    pub op: u64,
    pub pipeline: Vec<PipelineEntry>,
    pub heartbeat_ticks: u64,
    pub heartbeat_timeout: u64,
    /// Bitset tracking which replicas have sent StartViewChange for the pending view.
    pub svc_received: Vec<bool>,
    /// Bitset tracking which replicas have sent DoViewChange for the pending view.
    pub dvc_received: Vec<bool>,
    pub alive: bool,
    pub next_msg_id: u64,
}

impl Replica {
    pub fn new(id: u8, replica_count: u8) -> Self {
        Self {
            id,
            replica_count,
            view: 0,
            log_view: 0,
            status: Status::Normal,
            commit: 0,
            op: 0,
            pipeline: Vec::new(),
            heartbeat_ticks: DEFAULT_HEARTBEAT_TIMEOUT,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
            svc_received: vec![false; replica_count as usize],
            dvc_received: vec![false; replica_count as usize],
            alive: true,
            next_msg_id: 0,
        }
    }

    /// Returns the role of this replica in the current view.
    pub fn role(&self) -> Role {
        if self.view % self.replica_count as u32 == self.id as u32 {
            Role::Primary
        } else {
            Role::Backup
        }
    }

    /// Returns the id of the primary for the current view.
    #[allow(dead_code)]
    pub fn primary_id(&self) -> u8 {
        (self.view % self.replica_count as u32) as u8
    }

    /// Returns the quorum size (majority).
    pub fn quorum(&self) -> u8 {
        (self.replica_count / 2) + 1
    }

    /// Returns the current pipeline depth.
    pub fn pipeline_depth(&self) -> usize {
        self.pipeline.len()
    }

    /// Creates a snapshot of this replica's state for the UI.
    pub fn to_state(&self) -> ReplicaState {
        ReplicaState {
            id: self.id,
            status: self.status,
            role: self.role(),
            view: self.view,
            log_view: self.log_view,
            commit: self.commit,
            op: self.op,
            pipeline_depth: self.pipeline_depth(),
            alive: self.alive,
        }
    }

    /// Handles a client request. Only the primary in Normal status processes these.
    /// Adds the request to the pipeline and broadcasts Prepare to all other replicas.
    pub fn handle_client_request(&mut self, client_id: u8, request_id: u64) -> Vec<VsrMessage> {
        if self.role() != Role::Primary || self.status != Status::Normal {
            return Vec::new();
        }

        if self.pipeline.len() >= MAX_PIPELINE_DEPTH {
            return Vec::new();
        }

        self.op += 1;
        let op = self.op;

        let mut acks = vec![false; self.replica_count as usize];
        // Primary counts itself as acknowledged.
        acks[self.id as usize] = true;

        self.pipeline.push(PipelineEntry {
            op,
            client_id,
            request_id,
            acks,
            quorum_reached: false,
        });

        // Broadcast Prepare to all other replicas.
        let mut messages = Vec::new();
        for peer in 0..self.replica_count {
            if peer != self.id {
                messages.push(self.make_message(
                    MessageType::Prepare,
                    peer,
                    self.view,
                    op,
                    self.commit,
                ));
            }
        }
        messages
    }

    /// Handles a Prepare message. Backups in Normal status add to pipeline and reply PrepareOk.
    pub fn handle_prepare(
        &mut self,
        from: u8,
        view: u32,
        op: u64,
        client_id: u8,
        request_id: u64,
    ) -> Vec<VsrMessage> {
        if self.role() != Role::Backup || self.status != Status::Normal {
            return Vec::new();
        }

        if view != self.view {
            return Vec::new();
        }

        // Reset heartbeat on receiving Prepare from primary.
        self.heartbeat_ticks = self.heartbeat_timeout;

        // Track the op.
        if op > self.op {
            self.op = op;
        }

        let acks = vec![false; self.replica_count as usize];
        if self.pipeline.len() < MAX_PIPELINE_DEPTH {
            self.pipeline.push(PipelineEntry {
                op,
                client_id,
                request_id,
                acks,
                quorum_reached: false,
            });
        }

        // Send PrepareOk back to the primary.
        vec![self.make_message(MessageType::PrepareOk, from, self.view, op, self.commit)]
    }

    /// Handles a PrepareOk message. Primary marks the ack and commits if quorum is reached.
    pub fn handle_prepare_ok(&mut self, from: u8, op: u64) -> Vec<VsrMessage> {
        if self.role() != Role::Primary || self.status != Status::Normal {
            return Vec::new();
        }

        let quorum = self.quorum();
        let mut messages = Vec::new();

        if let Some(entry) = self.pipeline.iter_mut().find(|e| e.op == op) {
            entry.acks[from as usize] = true;

            if !entry.quorum_reached {
                let ack_count = entry.acks.iter().filter(|&&a| a).count() as u8;
                if ack_count >= quorum {
                    entry.quorum_reached = true;
                    let client_id = entry.client_id;

                    // Commit this operation.
                    if op > self.commit {
                        self.commit = op;
                    }

                    // Send ClientReply.
                    messages.push(self.make_message(
                        MessageType::ClientReply,
                        client_id,
                        self.view,
                        op,
                        self.commit,
                    ));

                    // Drain committed entries from the front of the pipeline.
                    while self.pipeline.first().is_some_and(|e| e.quorum_reached) {
                        self.pipeline.remove(0);
                    }
                }
            }
        }

        messages
    }

    /// Handles a StartViewChange message from another replica.
    pub fn handle_start_view_change(&mut self, from: u8, view: u32) -> Vec<VsrMessage> {
        if view <= self.view && self.status != Status::ViewChange {
            return Vec::new();
        }

        let mut messages = Vec::new();

        // If this is a higher view, transition to ViewChange.
        if view > self.view || self.status != Status::ViewChange {
            self.status = Status::ViewChange;
            self.view = view;
            self.svc_received = vec![false; self.replica_count as usize];
            self.dvc_received = vec![false; self.replica_count as usize];

            // Send our own SVC to all replicas.
            for peer in 0..self.replica_count {
                if peer != self.id {
                    messages.push(self.make_message(
                        MessageType::StartViewChange,
                        peer,
                        view,
                        self.op,
                        self.commit,
                    ));
                }
            }
        }

        // Record that we received SVC from `from`.
        self.svc_received[from as usize] = true;

        // f = (replica_count - 1) / 2
        let f = (self.replica_count - 1) / 2;
        let svc_count = self.svc_received.iter().filter(|&&v| v).count() as u8;

        // Once we have f SVCs (not counting our own), send DVC to the new primary.
        if svc_count >= f {
            let new_primary = (view % self.replica_count as u32) as u8;
            if new_primary != self.id {
                messages.push(self.make_message(
                    MessageType::DoViewChange,
                    new_primary,
                    view,
                    self.op,
                    self.commit,
                ));
            }
        }

        messages
    }

    /// Handles a DoViewChange message. The new primary collects DVCs and starts the new view.
    pub fn handle_do_view_change(&mut self, from: u8, view: u32) -> Vec<VsrMessage> {
        let new_primary = (view % self.replica_count as u32) as u8;
        if new_primary != self.id {
            return Vec::new();
        }

        if self.status != Status::ViewChange {
            // Accept DVC and enter ViewChange if not already.
            self.status = Status::ViewChange;
            self.view = view;
            self.svc_received = vec![false; self.replica_count as usize];
            self.dvc_received = vec![false; self.replica_count as usize];
        }

        self.dvc_received[from as usize] = true;
        // Count ourselves.
        self.dvc_received[self.id as usize] = true;

        let dvc_count = self.dvc_received.iter().filter(|&&v| v).count() as u8;
        let quorum = self.quorum();

        if dvc_count >= quorum {
            // We have quorum -- become the new primary.
            self.status = Status::Normal;
            self.log_view = view;
            self.pipeline.clear();

            // Broadcast StartView to all other replicas.
            let mut messages = Vec::new();
            for peer in 0..self.replica_count {
                if peer != self.id {
                    messages.push(self.make_message(
                        MessageType::StartView,
                        peer,
                        view,
                        self.op,
                        self.commit,
                    ));
                }
            }
            messages
        } else {
            Vec::new()
        }
    }

    /// Handles a StartView message. Backups adopt the new view and become Normal.
    pub fn handle_start_view(
        &mut self,
        _from: u8,
        view: u32,
        op: u64,
        commit: u64,
    ) -> Vec<VsrMessage> {
        self.view = view;
        self.log_view = view;
        self.status = Status::Normal;
        self.op = op;
        self.commit = commit;
        self.pipeline.clear();
        self.heartbeat_ticks = self.heartbeat_timeout;
        self.svc_received = vec![false; self.replica_count as usize];
        self.dvc_received = vec![false; self.replica_count as usize];

        Vec::new()
    }

    /// Heartbeat tick. Backups detect primary failure via timeout.
    pub fn tick(&mut self) -> Vec<VsrMessage> {
        if !self.alive || self.status != Status::Normal || self.role() != Role::Backup {
            return Vec::new();
        }

        if self.heartbeat_ticks > 0 {
            self.heartbeat_ticks -= 1;
        }

        if self.heartbeat_ticks == 0 {
            // Timeout -- start view change.
            let new_view = self.view + 1;
            self.status = Status::ViewChange;
            self.view = new_view;
            self.svc_received = vec![false; self.replica_count as usize];
            self.dvc_received = vec![false; self.replica_count as usize];

            let mut messages = Vec::new();
            for peer in 0..self.replica_count {
                if peer != self.id {
                    messages.push(self.make_message(
                        MessageType::StartViewChange,
                        peer,
                        new_view,
                        self.op,
                        self.commit,
                    ));
                }
            }
            messages
        } else {
            Vec::new()
        }
    }

    /// Helper to construct a VsrMessage with an auto-incrementing id.
    fn make_message(
        &mut self,
        msg_type: MessageType,
        to: u8,
        view: u32,
        op: u64,
        commit: u64,
    ) -> VsrMessage {
        let id = self.next_msg_id;
        self.next_msg_id += 1;
        VsrMessage {
            msg_type,
            from: self.id,
            to,
            view,
            op,
            commit,
            id,
        }
    }
}
