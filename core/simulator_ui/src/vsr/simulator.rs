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

//! Main VSR simulator that ties replicas, network, and events together.

use super::events::SimEvent;
use super::network::{Network, NetworkConfig};
use super::protocol::Replica;
use super::types::{MessageType, ReplicaState, Role, VsrMessage};

/// The top-level VSR simulator coordinating replicas and the network.
pub struct VsrSimulator {
    pub replicas: Vec<Replica>,
    pub network: Network,
    pub events: Vec<SimEvent>,
    pub tick: u64,
    next_request_id: u64,
    client_id: u8,
}

impl VsrSimulator {
    /// Creates a new simulator with the given number of replicas and RNG seed.
    pub fn new(replica_count: u8, seed: u64) -> Self {
        let mut network = Network::new(NetworkConfig::default(), seed);
        let mut replicas = Vec::with_capacity(replica_count as usize);

        for id in 0..replica_count {
            network.register_replica(id);
            replicas.push(Replica::new(id, replica_count));
        }

        // Client id is one past the last replica id.
        let client_id = replica_count;

        Self {
            replicas,
            network,
            events: Vec::new(),
            tick: 0,
            next_request_id: 1,
            client_id,
        }
    }

    /// Advance the simulation by one tick.
    ///
    /// 1. Tick the network to get delivered messages.
    /// 2. Deliver each message to its target replica, collecting responses.
    /// 3. Submit response messages to the network.
    /// 4. Tick all alive replicas for heartbeat, submit any generated messages.
    /// 5. Return collected events.
    pub fn step(&mut self) -> Vec<SimEvent> {
        self.tick += 1;
        let mut step_events = Vec::new();

        // 1. Network tick -- get delivered messages.
        let delivered = self.network.tick();

        // 2. Deliver each message to the target replica.
        for msg in delivered {
            step_events.push(SimEvent::MessageDelivered {
                tick: self.tick,
                message_id: msg.id,
            });

            let to = msg.to as usize;
            if to >= self.replicas.len() {
                // Message addressed to a client or invalid target -- skip.
                continue;
            }

            if !self.replicas[to].alive {
                continue;
            }

            let responses = self.dispatch_message(&msg);

            // 3. Submit response messages to the network and emit events.
            for response in responses {
                step_events.push(SimEvent::MessageSent {
                    tick: self.tick,
                    message: response.clone(),
                });
                self.network.submit(response);
            }
        }

        // 4. Tick all alive replicas (heartbeat).
        for i in 0..self.replicas.len() {
            if !self.replicas[i].alive {
                continue;
            }

            let tick_messages = self.replicas[i].tick();
            for msg in tick_messages {
                step_events.push(SimEvent::MessageSent {
                    tick: self.tick,
                    message: msg.clone(),
                });
                self.network.submit(msg);
            }
        }

        self.events.extend(step_events.clone());
        step_events
    }

    /// Inject a client request directed at the current primary.
    pub fn inject_client_request(&mut self) {
        let request_id = self.next_request_id;
        self.next_request_id += 1;

        if let Some(primary_id) = self.find_primary() {
            let idx = primary_id as usize;
            let responses = self.replicas[idx].handle_client_request(self.client_id, request_id);

            self.events.push(SimEvent::ClientRequest {
                tick: self.tick,
                replica_id: primary_id,
                client_id: self.client_id,
                request_id,
            });

            for msg in responses {
                self.events.push(SimEvent::MessageSent {
                    tick: self.tick,
                    message: msg.clone(),
                });
                self.network.submit(msg);
            }
        }
    }

    /// Partition the network link between two replicas.
    #[allow(dead_code)]
    pub fn partition_link(&mut self, from: u8, to: u8) {
        self.network.partition(from, to);
        self.events.push(SimEvent::LinkPartitioned {
            tick: self.tick,
            from,
            to,
        });
    }

    /// Heal the network link between two replicas.
    #[allow(dead_code)]
    pub fn heal_link(&mut self, from: u8, to: u8) {
        self.network.heal(from, to);
        self.events.push(SimEvent::LinkHealed {
            tick: self.tick,
            from,
            to,
        });
    }

    /// Kill a replica, making it unresponsive to all messages.
    pub fn kill_replica(&mut self, id: u8) {
        if let Some(replica) = self.replicas.get_mut(id as usize) {
            replica.alive = false;
        }
        self.network.kill_replica(id);
    }

    /// Restart a previously killed replica.
    pub fn restart_replica(&mut self, id: u8) {
        if let Some(replica) = self.replicas.get_mut(id as usize) {
            replica.alive = true;
            replica.status = super::types::Status::Recovering;
            // Immediately transition to Normal as a backup (simplified recovery).
            replica.status = super::types::Status::Normal;
            replica.heartbeat_ticks = replica.heartbeat_timeout;
        }
        self.network.restart_replica(id);
    }

    /// Returns a snapshot of all replica states for the UI.
    pub fn replica_states(&self) -> Vec<ReplicaState> {
        self.replicas.iter().map(|r| r.to_state()).collect()
    }

    /// Finds the first alive primary replica in Normal status.
    pub fn find_primary(&self) -> Option<u8> {
        self.replicas
            .iter()
            .find(|r| {
                r.alive && r.role() == Role::Primary && r.status == super::types::Status::Normal
            })
            .map(|r| r.id)
    }

    /// Dispatch a network message to the appropriate handler on the target replica.
    fn dispatch_message(&mut self, msg: &VsrMessage) -> Vec<VsrMessage> {
        let to = msg.to as usize;

        match msg.msg_type {
            MessageType::Prepare => {
                self.replicas[to].handle_prepare(msg.from, msg.view, msg.op, 0, 0)
            }
            MessageType::PrepareOk => self.replicas[to].handle_prepare_ok(msg.from, msg.op),
            MessageType::StartViewChange => {
                self.replicas[to].handle_start_view_change(msg.from, msg.view)
            }
            MessageType::DoViewChange => {
                self.replicas[to].handle_do_view_change(msg.from, msg.view)
            }
            MessageType::StartView => {
                self.replicas[to].handle_start_view(msg.from, msg.view, msg.op, msg.commit)
            }
            MessageType::ClientRequest => self.replicas[to].handle_client_request(msg.from, msg.op),
            MessageType::Commit | MessageType::ClientReply => {
                // Commit and ClientReply are terminal -- no further handling needed.
                Vec::new()
            }
        }
    }
}
