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

pub mod bus;
pub mod client;
pub mod deps;
pub mod network;
pub mod packet;
pub mod ready_queue;
pub mod replica;

use bus::SimOutbox;
use consensus::PartitionsHandle;
use iggy_binary_protocol::{GenericHeader, Message, ReplyHeader};
use iggy_common::IggyError;
use iggy_common::sharding::IggyNamespace;
use message_bus::MessageBus;
use network::Network;
use packet::{PacketSimulatorOptions, ProcessId};
use partitions::{Partition, PartitionOffsets, PollQueryResult, PollingArgs, PollingConsumer};
use replica::{Replica, new_replica};
use std::collections::HashSet;
use std::sync::Arc;

pub struct Simulator {
    /// All replicas, indexed by replica id. Always fully populated — crashed
    /// replicas are kept alive but skipped during dispatch.
    pub replicas: Vec<Replica>,
    /// Per-replica outbox, indexed by replica id. Shared with consensus inside
    /// each replica via [`SharedSimOutbox`](bus::SharedSimOutbox).
    pub outboxes: Vec<Arc<SimOutbox>>,
    /// Set of replica ids that are currently crashed. Dispatch and outbox drain
    /// are skipped for these ids.
    pub crashed: HashSet<u8>,
    pub network: Network,
    pub replica_count: u8,
    pub client_ids: Vec<u128>,
}

impl Simulator {
    /// Create a new simulator with per-replica outboxes routed through a [`Network`].
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        replica_count: usize,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
    ) -> Self {
        let client_ids: Vec<u128> = clients.collect();
        let mut network = Network::new(network_options);

        for &cid in &client_ids {
            network.register_client(cid);
        }

        let rc = replica_count as u8;
        let mut replicas = Vec::with_capacity(replica_count);
        let mut outboxes = Vec::with_capacity(replica_count);

        for i in 0..replica_count {
            let id = i as u8;
            let mut bus = SimOutbox::new(id);
            for &cid in &client_ids {
                bus.add_client(cid, ());
            }
            for j in 0..rc {
                bus.add_replica(j);
            }
            let outbox = Arc::new(bus);
            let replica = new_replica(id, format!("replica-{i}"), &outbox, rc);
            replicas.push(replica);
            outboxes.push(outbox);
        }

        Self {
            replicas,
            outboxes,
            crashed: HashSet::new(),
            network,
            replica_count: rc,
            client_ids,
        }
    }

    /// Initialize a partition with its own consensus group on all live replicas.
    #[allow(clippy::cast_possible_truncation)]
    pub fn init_partition(&mut self, namespace: IggyNamespace) {
        for (i, replica) in self.replicas.iter_mut().enumerate() {
            if !self.crashed.contains(&(i as u8)) {
                replica.init_partition(namespace);
            }
        }
    }

    /// Advance the simulation by one tick.
    ///
    /// Returns all client replies delivered during this tick.
    ///
    /// The tick has three phases that never borrow replicas and network
    /// simultaneously:
    ///
    /// 1. **Deliver**: `network.step()` returns ready packets; each is
    ///    dispatched to its target replica (or collected as a client reply).
    /// 2. **Drain**: each live replica's outbox is drained and fed into
    ///    `network.submit()`.
    /// 3. **Tick**: `network.tick()` advances network time (partitions,
    ///    clogging, etc.).
    ///
    /// # Panics
    /// Panics if a packet addressed to a client cannot be converted to a
    /// `ReplyHeader` message.
    #[allow(clippy::cast_possible_truncation)]
    pub fn step(&mut self) -> Vec<Message<ReplyHeader>> {
        let mut client_replies = Vec::new();

        // Phase 1: Deliver ready packets from the network.
        let packets = self.network.step();
        for packet in &packets {
            match packet.to {
                ProcessId::Replica(id) => {
                    if !self.crashed.contains(&id)
                        && let Some(replica) = self.replicas.get(id as usize)
                    {
                        Self::dispatch_to_replica(replica, packet.message.deep_copy());
                    }
                    // Crashed or missing: packet silently dropped.
                }
                ProcessId::Client(_) => {
                    let reply: Message<ReplyHeader> = packet
                        .message
                        .deep_copy()
                        .try_into_typed()
                        .expect("invalid message, wrong command type for a client response");
                    client_replies.push(reply);
                }
            }
        }
        self.network.recycle_buffer(packets);

        // Phase 2: Drain each replica's outbox into the network.
        for (i, outbox) in self.outboxes.iter().enumerate() {
            let envelopes = outbox.drain();
            if self.crashed.contains(&(i as u8)) {
                // Defensive: discard any messages from a crashed node's outbox.
                continue;
            }
            for envelope in envelopes {
                let from = ProcessId::Replica(i as u8);
                let to = if let Some(rid) = envelope.to_replica {
                    ProcessId::Replica(rid)
                } else if let Some(cid) = envelope.to_client {
                    ProcessId::Client(cid)
                } else {
                    continue;
                };
                let message = match envelope.payload {
                    bus::EnvelopePayload::Replica(m) | bus::EnvelopePayload::Client(m) => m,
                };
                self.network.submit(from, to, message);
            }
        }

        // Phase 3: Advance network time.
        self.network.tick();

        client_replies
    }

    /// Submit a client request into the simulated network.
    ///
    /// This is the simulator equivalent of a client opening a TCP connection
    /// and sending a message to a replica.
    pub fn submit_request(
        &mut self,
        client_id: u128,
        target_replica: u8,
        message: Message<GenericHeader>,
    ) {
        self.network.submit(
            ProcessId::Client(client_id),
            ProcessId::Replica(target_replica),
            message,
        );
    }

    /// Crash a replica: disable its network links and discard its outbox.
    ///
    /// The replica object is kept alive but will not receive any messages or
    /// have its outbox drained until a future `replica_restart` (not yet
    /// implemented and it requires consensus durability support).
    ///
    /// # Panics
    /// Panics if the replica is already crashed.
    pub fn replica_crash(&mut self, replica_index: u8) {
        assert!(
            !self.crashed.contains(&replica_index),
            "cannot crash replica {replica_index}: already down"
        );

        // Discard any unsent messages (never reached the wire).
        self.outboxes[replica_index as usize].drain();

        // Block all network links to/from this process.
        self.network
            .process_disable(ProcessId::Replica(replica_index));

        self.crashed.insert(replica_index);
    }

    /// Returns `true` if the given replica is currently crashed.
    #[must_use]
    pub fn is_crashed(&self, replica_index: u8) -> bool {
        self.crashed.contains(&replica_index)
    }

    /// Poll messages directly from a replica's partition.
    ///
    /// # Errors
    /// Returns `IggyError::ResourceNotFound` if the namespace does not exist on this replica.
    pub fn poll_messages(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
        consumer: PollingConsumer,
        args: PollingArgs,
    ) -> Result<PollQueryResult<4096>, IggyError> {
        let replica = &self.replicas[replica_idx];
        let partition =
            replica
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .ok_or(IggyError::ResourceNotFound(format!(
                    "partition not found for namespace {namespace:?} on replica {replica_idx}"
                )))?;
        futures::executor::block_on(partition.poll_messages(consumer, args))
    }

    /// Get partition offsets from a replica.
    #[must_use]
    pub fn offsets(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
    ) -> Option<PartitionOffsets> {
        let replica = &self.replicas[replica_idx];
        let partition = replica.plane.partitions().get_by_ns(&namespace)?;
        Some(partition.offsets())
    }

    fn dispatch_to_replica(replica: &Replica, message: Message<GenericHeader>) {
        futures::executor::block_on(replica.on_message(message));

        let mut buf = Vec::new();
        futures::executor::block_on(replica.process_loopback(&mut buf));
        let loopback_count = futures::executor::block_on(replica.process_loopback(&mut buf));
        debug_assert_eq!(
            loopback_count, 0,
            "on_ack must not re-enqueue loopback messages"
        );
    }
}

// TODO(IGGY-66): Add acceptance test for per-partition consensus independence.
// Setup: 3-replica simulator, two partitions (ns_a, ns_b).
// 1. Fill ns_a's pipeline to PIPELINE_PREPARE_QUEUE_MAX without delivering acks.
// 2. Send a request to ns_b, step until ns_b reply arrives.
// 3. Assert ns_b committed while ns_a pipeline is still full.
// Requires namespace-aware stepping (filter bus by namespace) or two-phase delivery.
