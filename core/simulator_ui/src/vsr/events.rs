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

use strum_macros::{AsRefStr, Display};

use super::types::*;

/// Structured event produced by the simulator for UI consumption.
#[allow(dead_code)]
#[derive(Debug, Clone, Display, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum SimEvent {
    /// A client request was received by a replica.
    ClientRequest {
        tick: u64,
        replica_id: u8,
        client_id: u8,
        request_id: u64,
    },

    /// A prepare was queued in the pipeline.
    PrepareQueued {
        tick: u64,
        replica_id: u8,
        op: u64,
        pipeline_depth: usize,
    },

    /// A PrepareOk was received.
    PrepareAcked {
        tick: u64,
        replica_id: u8,
        op: u64,
        ack_from: u8,
        ack_count: u8,
        quorum: u8,
        quorum_reached: bool,
    },

    /// An operation was committed.
    OperationCommitted { tick: u64, replica_id: u8, op: u64 },

    /// A message was sent between replicas or to/from a client.
    MessageSent { tick: u64, message: VsrMessage },

    /// A message was delivered.
    MessageDelivered { tick: u64, message_id: u64 },

    /// A message was dropped by the network.
    MessageDropped { tick: u64, message_id: u64 },

    /// A view change was initiated.
    ViewChangeStarted {
        tick: u64,
        replica_id: u8,
        old_view: u32,
        new_view: u32,
        reason: ViewChangeReason,
    },

    /// A new primary was elected.
    PrimaryElected {
        tick: u64,
        replica_id: u8,
        view: u32,
    },

    /// Replica state changed (status or role).
    ReplicaStateChanged {
        tick: u64,
        replica_id: u8,
        old_status: Status,
        new_status: Status,
        role: Role,
    },

    /// A network link was partitioned.
    LinkPartitioned { tick: u64, from: u8, to: u8 },

    /// A network link was healed.
    LinkHealed { tick: u64, from: u8, to: u8 },

    /// Tick heartbeat (emitted every N ticks for timeline).
    Tick { tick: u64 },
}

impl SimEvent {
    pub fn tick(&self) -> u64 {
        match self {
            SimEvent::ClientRequest { tick, .. }
            | SimEvent::PrepareQueued { tick, .. }
            | SimEvent::PrepareAcked { tick, .. }
            | SimEvent::OperationCommitted { tick, .. }
            | SimEvent::MessageSent { tick, .. }
            | SimEvent::MessageDelivered { tick, .. }
            | SimEvent::MessageDropped { tick, .. }
            | SimEvent::ViewChangeStarted { tick, .. }
            | SimEvent::PrimaryElected { tick, .. }
            | SimEvent::ReplicaStateChanged { tick, .. }
            | SimEvent::LinkPartitioned { tick, .. }
            | SimEvent::LinkHealed { tick, .. }
            | SimEvent::Tick { tick } => *tick,
        }
    }

    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            SimEvent::ClientRequest { .. } => "Client Request",
            SimEvent::PrepareQueued { .. } => "Prepare Queued",
            SimEvent::PrepareAcked { .. } => "Prepare Acked",
            SimEvent::OperationCommitted { .. } => "Committed",
            SimEvent::MessageSent { .. } => "Message Sent",
            SimEvent::MessageDelivered { .. } => "Delivered",
            SimEvent::MessageDropped { .. } => "Dropped",
            SimEvent::ViewChangeStarted { .. } => "View Change",
            SimEvent::PrimaryElected { .. } => "Primary Elected",
            SimEvent::ReplicaStateChanged { .. } => "State Changed",
            SimEvent::LinkPartitioned { .. } => "Link Partitioned",
            SimEvent::LinkHealed { .. } => "Link Healed",
            SimEvent::Tick { .. } => "Tick",
        }
    }
}
