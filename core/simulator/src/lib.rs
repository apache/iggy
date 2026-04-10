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
    /// The tick has four phases that never borrow replicas and network
    /// simultaneously:
    ///
    /// 0. **Consensus tick**: advance consensus timeouts on live replicas,
    ///    dispatching any resulting actions (view change messages, retransmissions).
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

        // Phase 0: Advance consensus timeouts (skip crashed replicas).
        for (i, replica) in self.replicas.iter().enumerate() {
            if !self.crashed.contains(&(i as u8)) {
                futures::executor::block_on(replica.tick_partitions());
                futures::executor::block_on(replica.tick_metadata());
            }
        }

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

    /// Advance consensus timeouts and dispatch actions on every live replica.
    ///
    /// Note: `step()` already calls this internally. This method is provided
    /// for callers that need to tick consensus without a full step cycle.
    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    pub async fn tick(&self) {
        for (i, replica) in self.replicas.iter().enumerate() {
            if !self.crashed.contains(&(i as u8)) {
                replica.tick_partitions().await;
                replica.tick_metadata().await;
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SimClient;
    use consensus::Status;
    use iggy_common::sharding::IggyNamespace;

    /// After crashing the primary in a 5-node cluster, the 4 remaining
    /// backups should detect the failure via heartbeat timeout and elect
    /// a new primary through the view change protocol.
    #[test]
    fn view_change_after_primary_crash() {
        iggy_common::MemoryPool::init_pool(&iggy_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 5;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            ..packet::PacketSimulatorOptions::default()
        };

        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);

        // Send a message through the primary (replica 0) to verify normal operation.
        let msg = client.send_messages(ns, &[b"before crash"]);
        sim.submit_request(client_id, 0, msg.into_generic());
        let mut got_reply = false;
        for _ in 0..100 {
            if !sim.step().is_empty() {
                got_reply = true;
                break;
            }
        }
        assert!(got_reply, "expected reply before crash");

        // Crash the primary.
        sim.replica_crash(0);

        // Run enough steps for the heartbeat timeout to fire
        // and the view change  to complete across 4 surviving replicas.
        for _ in 0..800 {
            sim.step();
        }

        // Verify that a new primary was elected in a higher view.
        let mut new_primary_found = false;
        for replica_idx in 1..replica_count {
            let replica = &sim.replicas[replica_idx as usize];
            let partitions = replica.plane.partitions();
            let consensus = partitions.consensus().unwrap();
            if consensus.view() > 0
                && consensus.status() == Status::Normal
                && consensus.is_primary()
            {
                new_primary_found = true;
            }
        }
        assert!(
            new_primary_found,
            "expected a new primary after crashing replica 0"
        );

        // Submit a request to the new primary and verify it commits.
        let c = sim.replicas[1].plane.partitions().consensus().unwrap();
        let new_primary_idx = c.primary_index(c.view());

        let msg2 = client.send_messages(ns, &[b"after view change"]);
        sim.submit_request(client_id, new_primary_idx, msg2.into_generic());
        let mut got_reply_after = false;
        for _ in 0..200 {
            if !sim.step().is_empty() {
                got_reply_after = true;
                break;
            }
        }
        assert!(
            got_reply_after,
            "expected reply from new primary after view change"
        );
    }

    /// Regression: a behind backup (`commit_min < commit_max`) becoming the new
    /// primary must not panic during the `CommitMessage` heartbeat timeout.
    /// Previously, `handle_commit_message_timeout` asserted `commit_min == commit_max`,
    /// which fails when the new primary hasn't caught up yet.
    #[test]
    fn view_change_behind_backup_becomes_primary() {
        iggy_common::MemoryPool::init_pool(&iggy_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            ..packet::PacketSimulatorOptions::default()
        };

        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);

        // Send several messages so the primary commits ahead of backups.
        // Backups receive prepares but may not have committed all of them
        // (commit_max lags behind the primary's commit_min because the
        // primary's commit point is only propagated via subsequent Prepare
        // headers or Commit heartbeats).
        for i in 0..3 {
            let msg = client.send_messages(ns, &[format!("msg-{i}").as_bytes()]);
            sim.submit_request(client_id, 0, msg.into_generic());
            // Only a few steps — enough for replication but not for the
            // backup to fully learn the commit point.
            for _ in 0..10 {
                sim.step();
            }
        }

        // Crash the primary immediately. Backups may have commit_min < commit_max.
        sim.replica_crash(0);

        // Run view change. This must not panic in handle_commit_message_timeout.
        for _ in 0..800 {
            sim.step();
        }

        // Verify a new primary was elected and is functional.
        let mut new_primary_found = false;
        for idx in 1..replica_count {
            let c = sim.replicas[idx as usize]
                .plane
                .partitions()
                .consensus()
                .unwrap();
            if c.view() > 0 && c.status() == Status::Normal && c.is_primary() {
                new_primary_found = true;
            }
        }
        assert!(new_primary_found, "expected a new primary");
    }
}
