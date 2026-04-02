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

use crate::tracing_layer::{CapturedSimEvent, EventBuffer};
use crate::types::{ReplicaState, Role, Status};
use crate::util::recover_lock;
use iggy_common::sharding::IggyNamespace;
use simulator::Simulator;
use simulator::client::SimClient;
use simulator::packet::{PacketSimulatorOptions, ProcessId};

const MAX_PIPELINE_BEFORE_BACKPRESSURE: usize = 6;

pub struct UiSimulator {
    pub inner: Simulator,
    pub client: SimClient,
    pub event_buffer: EventBuffer,
    pub tick: u64,
    pub replica_count: u8,
    pub poisoned: bool,
    states: Vec<TrackedReplicaState>,
    client_id: u128,
    namespace: IggyNamespace,
}

struct TrackedReplicaState {
    status: Status,
    role: Role,
    view: u32,
    commit: u64,
    op: u64,
    pipeline_depth: usize,
}

impl UiSimulator {
    pub fn new(replica_count: u8, seed: u64, event_buffer: EventBuffer) -> Self {
        let client_id: u128 = 1;

        let network_opts = PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            ..PacketSimulatorOptions::default()
        };

        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );

        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);

        let mut states = Vec::with_capacity(replica_count as usize);
        for _ in 0..replica_count {
            states.push(TrackedReplicaState {
                status: Status::Normal,
                role: Role::Backup,
                view: 0,
                commit: 0,
                op: 0,
                pipeline_depth: 0,
            });
        }
        if !states.is_empty() {
            states[0].role = Role::Primary;
        }

        Self {
            inner: sim,
            client: SimClient::new(client_id),
            event_buffer,
            tick: 0,
            replica_count,
            poisoned: false,
            states,
            client_id,
            namespace,
        }
    }

    pub fn step(&mut self) -> Vec<CapturedSimEvent> {
        if self.poisoned {
            return Vec::new();
        }

        recover_lock(&self.event_buffer).clear();

        // Catch panics so the UI keeps running even if the simulator hits an assertion.
        // After a panic the simulator's internal state is corrupt, so we mark it poisoned
        // and never call step() again.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.inner.step()));
        if result.is_err() {
            self.poisoned = true;
            return Vec::new();
        }

        // Tick incremented only after successful step
        self.tick += 1;

        let events: Vec<CapturedSimEvent> = recover_lock(&self.event_buffer).drain(..).collect();

        for event in &events {
            self.apply_event(event);
        }

        events
    }

    pub fn replica_states(&self) -> Vec<ReplicaState> {
        (0..self.replica_count)
            .map(|replica_id| {
                let tracked = &self.states[replica_id as usize];
                ReplicaState {
                    id: replica_id,
                    status: tracked.status,
                    role: tracked.role,
                    pipeline_depth: tracked.pipeline_depth,
                    alive: !self.inner.is_crashed(replica_id),
                }
            })
            .collect()
    }

    pub fn inject_client_request(&mut self) {
        let Some(target) = self.find_primary() else {
            return;
        };
        if self.states[target as usize].pipeline_depth >= MAX_PIPELINE_BEFORE_BACKPRESSURE {
            return;
        }
        let msg = self.client.send_messages(self.namespace, &[b"sim-request"]);
        self.inner
            .submit_request(self.client_id, target, msg.into_generic());
    }

    pub fn kill_replica(&mut self, id: u8) {
        if !self.inner.is_crashed(id) {
            self.inner.replica_crash(id);
        }
    }

    pub fn is_crashed(&self, id: u8) -> bool {
        self.inner.is_crashed(id)
    }

    pub fn partition_link(&mut self, from: u8, to: u8) {
        let pf = ProcessId::Replica(from);
        let pt = ProcessId::Replica(to);
        self.inner.network.set_link_filter(pf, pt, false);
        self.inner.network.set_link_filter(pt, pf, false);
    }

    pub fn is_link_partitioned(&self, from: u8, to: u8) -> bool {
        !self
            .inner
            .network
            .is_link_enabled(ProcessId::Replica(from), ProcessId::Replica(to))
    }

    pub fn heal_all(&mut self) {
        for replica_id in 0..self.replica_count {
            if !self.inner.is_crashed(replica_id) {
                continue;
            }
            self.inner
                .network
                .process_enable(ProcessId::Replica(replica_id));
            self.inner.crashed.remove(&replica_id);
            // Reset tracked state conservatively; real values arrive via events
            let state = &mut self.states[replica_id as usize];
            state.role = Role::Backup;
            state.pipeline_depth = 0;
        }
        self.inner.network.clear_partition();
    }

    fn find_primary(&self) -> Option<u8> {
        (0..self.replica_count).find(|&replica_id| {
            !self.inner.is_crashed(replica_id)
                && self.states[replica_id as usize].role == Role::Primary
        })
    }

    fn apply_event(&mut self, event: &CapturedSimEvent) {
        let Some(replica_id) = event.replica_id else {
            return;
        };
        let idx = replica_id as usize;
        if idx >= self.states.len() {
            return;
        }
        let state = &mut self.states[idx];
        if let Some(ref status) = event.status {
            state.status = Status::from_str(status);
        }
        if let Some(ref role) = event.role {
            state.role = Role::from_str(role);
        }
        if let Some(view) = event.view {
            state.view = view as u32;
        }
        if let Some(commit) = event.commit {
            state.commit = commit;
        }
        if let Some(op) = event.op {
            state.op = op;
        }
        if let Some(pipeline_depth) = event.pipeline_depth {
            state.pipeline_depth = pipeline_depth as usize;
        }
    }
}
