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
pub mod workload;

use bus::SimOutbox;
use client::SimClient;
use consensus::PartitionsHandle;
use iggy_binary_protocol::{GenericHeader, ReplyHeader};
use iggy_common::IggyError;
use network::Network;
use packet::{PacketSimulatorOptions, ProcessId};
use partitions::{Partition, PartitionOffsets, PollFragments, PollingArgs, PollingConsumer};
use replica::{Replica, new_replica};
use server_common::Message;
use server_common::sharding::IggyNamespace;
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
    /// New simulator with per-replica outboxes routed through a [`Network`].
    ///
    /// # Panics
    /// Panics if `clients` yields duplicate `client_id`s. The auditor
    /// keys in-flight entries by `(client_id, request)` and the network
    /// indexes packet routes by `client_id`; duplicates would collide on
    /// both.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        replica_count: usize,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
    ) -> Self {
        let client_ids: Vec<u128> = clients.collect();
        {
            let mut seen = HashSet::with_capacity(client_ids.len());
            for &cid in &client_ids {
                assert!(
                    seen.insert(cid),
                    "Simulator::new: duplicate client_id {cid}; \
                     auditor and network both key on client_id"
                );
            }
        }
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
                bus.add_client(cid);
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

    /// Init a partition with its own consensus group on every live replica.
    #[allow(clippy::cast_possible_truncation)]
    pub fn init_partition(&mut self, namespace: IggyNamespace) {
        for (i, replica) in self.replicas.iter_mut().enumerate() {
            if !self.crashed.contains(&(i as u8)) {
                replica.init_partition(namespace);
            }
        }
    }

    /// Advance the simulation by one tick. Returns client replies delivered.
    ///
    /// Phases never borrow replicas and network simultaneously:
    ///
    /// 0. Tick consensus on live replicas (view change, retransmits).
    /// 1. Deliver ready packets from the network to replicas or clients.
    /// 2. Drain each live replica's outbox into `network.submit()`.
    /// 3. `network.tick()` advances network time.
    ///
    /// # Panics
    /// If a client-addressed packet cannot be decoded as `ReplyHeader`.
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

    /// Submit a client request into the simulated network. Equivalent to a
    /// client opening a TCP connection and sending a message to a replica.
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

    /// Register a client via the primary (replica 0). Sends `Register`
    /// through the metadata plane and binds the assigned session on
    /// `SimClient`.
    ///
    /// # Panics
    /// If no reply arrives within 100 steps.
    #[allow(clippy::cast_possible_truncation)]
    pub fn register_client_with_primary(&mut self, client: &SimClient) {
        let msg = client.register();
        self.submit_request(client.client_id(), 0, msg.into_generic());
        let mut session = 0u64;
        let mut got_reply = false;
        for _ in 0..100 {
            let replies = self.step();
            if !replies.is_empty() {
                let header = replies[0].header();
                debug_assert_eq!(
                    header.operation,
                    iggy_binary_protocol::Operation::Register,
                    "register_client_with_primary: first reply was not Register"
                );
                assert_eq!(
                    header.client,
                    client.client_id(),
                    "register_client_with_primary: reply client_id mismatch \
                     (expected {}, got {})",
                    client.client_id(),
                    header.client,
                );
                session = header.commit;
                got_reply = true;
                break;
            }
        }
        assert!(
            got_reply,
            "register_client_with_primary: no reply within 100 steps"
        );
        client.bind_session(session);

        // Partition has no `client_table`: at-least-once, no per-client
        // dedup. Consumers dedup via message id / content / producer-id+seq.
        // Sessions/dedup/eviction live on metadata only (IggyMetadata).
    }

    /// Crash a replica: disable its network links and discard its outbox.
    /// The replica object stays alive but receives no messages until a
    /// `replica_restart` (not yet implemented; needs consensus durability).
    ///
    /// # Panics
    /// If the replica is already crashed.
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

    /// `true` if the replica is currently crashed.
    #[must_use]
    pub fn is_crashed(&self, replica_index: u8) -> bool {
        self.crashed.contains(&replica_index)
    }

    /// Advance consensus timeouts and dispatch on every live replica.
    /// `step()` calls this internally; exposed for callers that need to
    /// tick consensus without a full step cycle.
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
    /// `IggyError::ResourceNotFound` if the namespace is not on this replica.
    pub fn poll_messages(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
        consumer: PollingConsumer,
        args: &PollingArgs,
    ) -> Result<PollFragments<4096>, IggyError> {
        let replica = &self.replicas[replica_idx];
        // Build the owned poll plan synchronously, then execute off the borrow.
        // The sim's partitions are in-memory (no `partition_dir`), so the plan
        // serves only the resident journal tier; `execute` performs no disk IO.
        let Some(plan) = replica
            .plane
            .partitions()
            .build_poll_snapshot(&namespace, consumer, args)
        else {
            return Err(IggyError::ResourceNotFound(format!(
                "partition not found for namespace {namespace:?} on replica {replica_idx}"
            )));
        };
        // The simulator drives partitions directly, so it never replicates a
        // poll's auto-commit (that is the serving shard's job in the real
        // server); the surfaced offset is discarded here.
        let (fragments, _commit_offset, _auto_commit) = futures::executor::block_on(plan.execute());
        Ok(fragments)
    }

    /// Partition offsets from a replica.
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

    /// Consensus view for a replica's partition-plane group, or `None` if the
    /// namespace is not present on that replica.
    #[must_use]
    pub(crate) fn consensus_view(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
    ) -> Option<u64> {
        let replica = &self.replicas[replica_idx];
        let partition = replica.plane.partitions().get_by_ns(&namespace)?;
        Some(u64::from(partition.consensus().view()))
    }

    /// Index of the current primary for `namespace`, as seen by the first live
    /// replica hosting it, or `None` if no live replica hosts it.
    ///
    /// Reads one replica's view, so it assumes live replicas agree on the
    /// primary. Sound only while the primary is static, which holds today: the
    /// driver spares primaries from crashes, so no crash-triggered view change
    /// runs mid-test. Once primary-crash injection lands, views can diverge and
    /// this may name a stale or crashed primary (see
    /// `workload::oracle::assert_converged` for the consequence and fix).
    #[must_use]
    pub(crate) fn primary_index(&self, namespace: IggyNamespace) -> Option<u8> {
        (0..self.replica_count)
            .filter(|replica_idx| !self.crashed.contains(replica_idx))
            .find_map(|replica_idx| {
                let partition = self.replicas[usize::from(replica_idx)]
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)?;
                let consensus = partition.consensus();
                Some(consensus.primary_index(consensus.view()))
            })
    }

    fn dispatch_to_replica(replica: &Replica, message: Message<GenericHeader>) {
        futures::executor::block_on(replica.on_message(message));

        let mut buf = Vec::new();
        let mut ns_scratch: Vec<IggyNamespace> = Vec::new();
        futures::executor::block_on(replica.process_loopback(&mut buf, &mut ns_scratch));
        let loopback_count =
            futures::executor::block_on(replica.process_loopback(&mut buf, &mut ns_scratch));
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
    use crate::workload::apply_sim_commands;
    use bytes::Bytes;
    use consensus::Status;
    use server_common::sharding::IggyNamespace;

    /// Crashing the primary in a 5-node cluster: 4 survivors detect via
    /// heartbeat timeout and elect a new primary via view change.
    #[test]
    fn view_change_after_primary_crash() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
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

        // Register the client with the consensus cluster.
        sim.register_client_with_primary(&client);

        // Send a message through the primary (replica 0) to verify normal operation.
        let msg = client.send_messages(ns, &[Bytes::from_static(b"before crash")]);
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
            let consensus = partitions
                .get_by_ns(&ns)
                .expect("partition must exist on every live replica")
                .consensus();
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
        let c = sim.replicas[1]
            .plane
            .partitions()
            .get_by_ns(&ns)
            .expect("partition must exist on replica 1")
            .consensus();
        let new_primary_idx = c.primary_index(c.view());

        let msg2 = client.send_messages(ns, &[Bytes::from_static(b"after view change")]);
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

    /// At-least-once failover: `SendMessages` retry on a new primary
    /// re-executes. Retry reply carries a HIGHER `commit` op (re-execution
    /// proof, not dedup). Duplicate payload lives at two offsets; consumers
    /// dedup if they need at-most-once-per-payload.
    #[test]
    fn failover_retry_re_executes_under_at_least_once() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
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
        sim.register_client_with_primary(&client);

        // Same `(client, session, request)` for replay; mirrors SDK's
        // connection-loss retry.
        let original_req = client.send_messages(ns, &[Bytes::from_static(b"failover-test")]);
        let replay_req = original_req.deep_copy();
        let original_request_id = original_req.header().request;

        sim.submit_request(client_id, 0, original_req.into_generic());

        let mut original_reply: Option<Message<ReplyHeader>> = None;
        for _ in 0..200 {
            let replies = sim.step();
            if !replies.is_empty() {
                original_reply = Some(replies[0].deep_copy());
                break;
            }
        }
        let original_reply = original_reply.expect("commit reply must arrive before primary crash");
        let original_commit_op = original_reply.header().commit;
        assert_eq!(
            original_reply.header().request,
            original_request_id,
            "sanity: original reply must echo the request id"
        );

        // Crash primary. Real-world: TCP buffer might have lost reply
        // before ack; same retry path.
        sim.replica_crash(0);

        // Steps for view change across 4 survivors.
        for _ in 0..800 {
            sim.step();
        }

        // Find new primary via any live replica.
        let live = &sim.replicas[1];
        let live_consensus = live
            .plane
            .partitions()
            .get_by_ns(&ns)
            .expect("partition must exist on a live replica")
            .consensus();
        assert!(
            live_consensus.view() > 0,
            "view must have advanced past the crashed primary"
        );
        let new_primary_idx = live_consensus.primary_index(live_consensus.view());
        assert_ne!(
            new_primary_idx, 0,
            "new primary must not be the crashed replica"
        );

        // Replay SAME request to new primary. No dedup -> re-execution.
        sim.submit_request(client_id, new_primary_idx, replay_req.into_generic());

        let mut retry_reply: Option<Message<ReplyHeader>> = None;
        for _ in 0..200 {
            let replies = sim.step();
            if !replies.is_empty() {
                retry_reply = Some(replies[0].deep_copy());
                break;
            }
        }
        let retry_reply = retry_reply.expect(
            "reply must arrive after retry; new primary re-commits as \
             fresh prepare (at-least-once)",
        );

        // At-least-once: same request id (correlation), HIGHER commit op
        // (re-execution). No dedup absorbs the retry.
        assert_eq!(
            retry_reply.header().request,
            original_request_id,
            "retry's reply must correlate to the request id"
        );
        assert!(
            retry_reply.header().commit > original_commit_op,
            "retry must re-execute (commit op > original={original_commit_op}, got {})",
            retry_reply.header().commit
        );
        assert_eq!(
            retry_reply.header().client,
            client_id,
            "retry must echo original client_id"
        );
    }

    /// Regression: a behind backup (`commit_min < commit_max`) becoming
    /// primary must not panic during the `CommitMessage` heartbeat timeout.
    /// `handle_commit_message_timeout` used to assert `commit_min == commit_max`.
    #[test]
    fn view_change_behind_backup_becomes_primary() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
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

        // Register the client with the consensus cluster.
        sim.register_client_with_primary(&client);

        // Send several messages so primary commits ahead of backups.
        // Backups receive prepares but may lag on commit (`commit_max` <
        // primary's `commit_min`): commit point only propagates via later
        // Prepare headers or Commit heartbeats.
        for i in 0..3 {
            let msg = client.send_messages(ns, &[Bytes::from(format!("msg-{i}"))]);
            sim.submit_request(client_id, 0, msg.into_generic());
            // Few steps: enough for replication, not enough for backups
            // to fully learn the commit point.
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
                .get_by_ns(&ns)
                .expect("partition must exist on every live replica")
                .consensus();
            if c.view() > 0 && c.status() == Status::Normal && c.is_primary() {
                new_primary_found = true;
            }
        }
        assert!(new_primary_found, "expected a new primary");
    }

    /// Determinism: fresh simulator + workload from the same seed (network
    /// and workload) produces an identical reply-header sequence.
    #[test]
    fn workload_replay_is_deterministic() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let h1 = workload_hash_for_seed(0xDEAD_BEEF);
        let h2 = workload_hash_for_seed(0xDEAD_BEEF);
        assert_eq!(
            h1, h2,
            "workload reply hash diverged across runs with the same seed"
        );

        // Sanity: a different seed should generally produce a different
        // trace. (Theoretically possible to collide, but vanishingly so.)
        let h3 = workload_hash_for_seed(0xCAFE_BABE);
        assert_ne!(
            h1, h3,
            "different seeds produced identical reply hashes; determinism collapsed"
        );

        // Fragile cross-run baseline, pinned to seed 0xDEAD_BEEF under the default
        // `ActionWeights`. Drifts whenever reply shape, partition commit values, or
        // PRNG draw order change. Draw order is sensitive to `pick_outcome`: adding
        // an outcome to an op sampled in this seed's window, or a weight bump,
        // shifts the trace. Re-lock on intentional changes; expect re-locks until
        // error discriminants and reply bodies stabilize the wire format.
        assert_eq!(
            h1, 0xFF7F_E659_B9D8_15E9,
            "workload reply hash drifted from locked baseline"
        );
    }

    /// Drive workload with uniform weights across all 25 `Action` variants.
    /// Assert it runs without panic and observes at least one reply.
    /// Per-op coverage not asserted: some ops can starve the in-flight slot
    /// at single-client / 1-slot pipeline limits.
    #[test]
    fn uniform_weights_runs_clean() {
        use crate::workload::{
            Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
        };
        use strum::{EnumCount, IntoEnumIterator};

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0xC0FF_EE00,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = client::SimClient::new(client_id);
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        let ns_b = server_common::sharding::IggyNamespace::new(1, 1, 1);
        sim.init_partition(ns_a);
        sim.init_partition(ns_b);
        sim.register_client_with_primary(&client);

        // 25 variants × 4 = 100.
        assert_eq!(Action::COUNT, 25, "Action::COUNT changed; adjust weights");
        let entries: Vec<(Action, u8)> = Action::iter().map(|a| (a, 4)).collect();
        let weights = ActionWeights::new(&entries);

        let mut options = WorkloadOptions::new(0xC0FF_EE00, replica_count, vec![ns_a, ns_b]);
        options.weights = weights;
        let mut wl = Workload::new(options);

        let mut replies_seen = 0u64;
        for _tick in 0..2_000u32 {
            if let Some((target, msg)) = wl.build_request(&client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
            for reply in sim.step() {
                let cmds = wl.on_reply(&reply);
                apply_sim_commands(&mut sim, &cmds);
                replies_seen += 1;
            }
        }
        assert!(
            replies_seen > 0,
            "uniform-weight workload produced no replies; sampling or dispatch broken"
        );
    }

    /// The cheap per-tick invariants run inside `workload::run` and
    /// stay green over a uniform 25-op single-client workload. A second pass
    /// with a fresh `Invariants` confirms the checks observed live state
    /// (non-vacuous): `commit_offset` is tracked for every (replica, namespace)
    /// pair, not silently skipped.
    #[test]
    fn uniform_weights_invariants_hold() {
        use crate::workload::{
            self, Workload,
            actions::Action,
            invariants::Invariants,
            options::{ActionWeights, WorkloadOptions},
        };
        use strum::{EnumCount, IntoEnumIterator};

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0xC0FF_EE00,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
        );
        let client = client::SimClient::new(client_id);
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        let ns_b = server_common::sharding::IggyNamespace::new(1, 1, 1);
        sim.init_partition(ns_a);
        sim.init_partition(ns_b);
        sim.register_client_with_primary(&client);

        assert_eq!(Action::COUNT, 25, "Action::COUNT changed; adjust weights");
        let entries: Vec<(Action, u8)> = Action::iter().map(|a| (a, 4)).collect();
        let mut options = WorkloadOptions::new(0xC0FF_EE00, replica_count, vec![ns_a, ns_b]);
        options.weights = ActionWeights::new(&entries);
        let mut wl = Workload::new(options);

        // `run` asserts the invariants every tick; any regression panics
        // here, replayable from the seed above.
        let clients = [client];
        let replies = workload::run(&mut sim, &mut wl, &clients, 2_000, u64::MAX);
        assert!(
            replies > 0,
            "uniform-weight workload produced no replies; invariants never exercised"
        );

        // Non-vacuity: the checks must have read live state for every pair.
        let mut probe = Invariants::new();
        probe.check(&sim, &wl);
        assert_eq!(
            probe.tracked_pairs(),
            usize::from(replica_count) * 2,
            "expected commit_offset tracked for every (replica, namespace) pair"
        );
    }

    /// With a positive crash probability
    /// the driver crashes followers (never the primary) but never below the
    /// survivor floor, while the per-tick invariants stay green and the
    /// surviving quorum keeps committing.
    #[test]
    fn crash_injection_spares_primary_and_keeps_quorum() {
        use crate::workload::{
            self, Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
        };

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 5;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0xC0FF_EE00,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
        );
        let client = client::SimClient::new(client_id);
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns_a);
        sim.register_client_with_primary(&client);

        let mut options = WorkloadOptions::new(0xC0FF_EE00, replica_count, vec![ns_a]);
        options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        options.crash_per_tick_ratio = 0.05;
        options.min_survivors = 3; // quorum of 5

        let mut wl = Workload::new(options);
        let clients = [client];
        // run() asserts the per-tick invariants every tick under injected crashes.
        let replies = workload::run(&mut sim, &mut wl, &clients, 3_000, u64::MAX);

        let crashed = sim.crashed.len();
        assert!(
            crashed >= 1,
            "expected at least one crash injected over the run"
        );
        assert!(
            !sim.is_crashed(0),
            "primary (replica 0) must never be crashed"
        );
        assert!(
            usize::from(replica_count) - crashed >= 3,
            "must keep at least min_survivors=3 live (crashed={crashed})"
        );
        assert!(
            replies > 0,
            "surviving quorum must keep committing under follower crashes"
        );
    }

    /// After a mixed metadata + partition run drains, the shadow's
    /// predicted streams equal the metadata committed on the leader (the
    /// entity-oracle payoff of the name-keyed shadow).
    ///
    /// Interleaves creates, deletes, and partition sends from a single client.
    /// A send between two metadata ops once consumed a metadata request number
    /// and gapped the next create into a permanent `RequestGap`; the `SimClient`
    /// per-plane numbering fix keeps the metadata sequence contiguous, so the
    /// mix now drains. The entity oracle still compares only against the leader:
    /// a quorum-excluded backup has no idle catch-up yet, so full cross-replica
    /// equality stays deferred (see [`oracle`]).
    #[test]
    fn quiesce_stream_entity_oracle_matches_leader() {
        use crate::workload::{
            self, Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
            oracle,
        };

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0xC0FF_EE00,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = client::SimClient::new(client_id);
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns_a);
        sim.register_client_with_primary(&client);

        let mut options = WorkloadOptions::new(0xC0FF_EE00, replica_count, vec![ns_a]);
        // Interleave metadata creates/deletes with partition sends: the send
        // between two metadata ops is the case that previously wedged the next
        // create. Sends do not touch the metadata entity sets, so the entity
        // oracle below still compares stream state cleanly.
        options.weights = ActionWeights::new(&[
            (Action::CreateStream, 50),
            (Action::DeleteStream, 25),
            (Action::SendMessages, 25),
        ]);
        let mut wl = Workload::new(options);

        let clients = [client];
        let replies = workload::run(&mut sim, &mut wl, &clients, 2_000, u64::MAX);
        assert!(replies > 0, "workload produced no replies");

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, 5_000),
            "system did not drain within the tick budget"
        );
        // Cross-replica agreement + entity oracle (single client => strict).
        oracle::assert_converged(&sim, &wl);
    }

    /// Under follower crashes the surviving quorum still drains and
    /// agrees on the partition commit offset at quiesce.
    #[test]
    fn quiesce_partition_offsets_converge_under_crashes() {
        use crate::workload::{
            self, Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
            oracle,
        };
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 5;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0xC0FF_EE00,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = client::SimClient::new(client_id);
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns_a);
        sim.register_client_with_primary(&client);

        // Partition-plane workload under crashes: SendMessages replies and the
        // partition plane converges across survivors. Stays partition-only by
        // design to isolate partition-offset convergence from metadata loss to a
        // crashed primary; the metadata entity oracle runs in the no-crash test.
        // Crash one follower (keep quorum slack: 5 replicas, floor 4, quorum 3),
        // so commits still reach quorum and the run drains.
        let mut options = WorkloadOptions::new(0xC0FF_EE00, replica_count, vec![ns_a]);
        options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        options.crash_per_tick_ratio = 0.05;
        options.min_survivors = 4;
        let mut wl = Workload::new(options);

        let clients = [client];
        let replies = workload::run(&mut sim, &mut wl, &clients, 3_000, u64::MAX);
        assert!(replies > 0, "workload produced no replies");
        assert!(
            !sim.crashed.is_empty(),
            "expected at least one follower crash"
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, 5_000),
            "surviving quorum did not drain within the tick budget"
        );
        oracle::assert_converged(&sim, &wl);
    }

    /// Drive Create-heavy then Delete-heavy workload; assert shadow tracks
    /// live streams:
    ///
    /// - At least one `CreateStream` commits.
    /// - At least one `DeleteStream` commits, proving sample picked a live
    ///   name (without shadow tracking, sample would return `None`).
    /// - Shadow stream count matches net `creates - deletes`.
    #[test]
    fn shadow_tracks_live_streams() {
        use crate::workload::{
            Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
        };

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0x5EED_0002,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = client::SimClient::new(client_id);
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns_a);
        sim.register_client_with_primary(&client);

        // Phase 1: Create-heavy to populate the shadow.
        let mut options = WorkloadOptions::new(0x5EED_0002, replica_count, vec![ns_a]);
        options.weights = ActionWeights::new(&[(Action::CreateStream, 100)]);
        let mut wl = Workload::new(options);
        for _tick in 0..3_000u32 {
            if let Some((target, msg)) = wl.build_request(&client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
            for reply in sim.step() {
                let cmds = wl.on_reply(&reply);
                apply_sim_commands(&mut sim, &cmds);
            }
        }
        let created = wl.auditor.stats().commits_per_action[Action::CreateStream as usize];
        assert!(created > 0, "Create-only workload produced no commits");
        assert_eq!(
            wl.shadow.stream_names.len() as u64,
            created,
            "shadow stream count diverged from CreateStream commits"
        );

        // Phase 2: Create/Delete mix. DeleteStream sample succeeds only if
        // shadow.pick_stream_name returns Some (the wiring under test).
        wl.options.weights =
            ActionWeights::new(&[(Action::CreateStream, 30), (Action::DeleteStream, 70)]);
        for _tick in 0..3_000u32 {
            if let Some((target, msg)) = wl.build_request(&client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
            for reply in sim.step() {
                let cmds = wl.on_reply(&reply);
                apply_sim_commands(&mut sim, &cmds);
            }
        }
        let deleted = wl.auditor.stats().commits_per_action[Action::DeleteStream as usize];
        let created_total = wl.auditor.stats().commits_per_action[Action::CreateStream as usize];
        assert!(
            deleted > 0,
            "DeleteStream never committed; shadow-driven sampling is broken \
             (sample would return None unless pick_stream_name found a live name)"
        );
        let expected_live = created_total.saturating_sub(deleted);
        assert_eq!(
            wl.shadow.stream_names.len() as u64,
            expected_live,
            "shadow.stream_names.len() ({}) != creates ({}) - deletes ({}) = {}",
            wl.shadow.stream_names.len(),
            created_total,
            deleted,
            expected_live,
        );
    }

    /// Drive workload with 4 concurrent clients over two namespaces; assert:
    ///
    /// - Every client observes at least one commit (no starvation).
    /// - Per-(client, namespace) commit-monotonic invariant holds.
    /// - Commits interleave across clients.
    #[test]
    fn multi_client_interleaves_commits() {
        use crate::workload::{
            Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
        };

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_ids: Vec<u128> = (1..=4).collect();
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: u8::try_from(client_ids.len()).expect("fits"),
            seed: 0x5EED_0005,
            ..packet::PacketSimulatorOptions::default()
        };

        let mut sim = Simulator::new(
            replica_count as usize,
            client_ids.iter().copied(),
            network_opts,
        );
        let clients: Vec<client::SimClient> = client_ids
            .iter()
            .map(|&id| client::SimClient::new(id))
            .collect();
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        let ns_b = server_common::sharding::IggyNamespace::new(1, 1, 1);
        sim.init_partition(ns_a);
        sim.init_partition(ns_b);
        for c in &clients {
            sim.register_client_with_primary(c);
        }

        let mut options = WorkloadOptions::new(0x5EED_0005, replica_count, vec![ns_a, ns_b]);
        options.client_count = u8::try_from(clients.len()).expect("fits");
        options.weights = ActionWeights::new(&[
            (Action::CreateStream, 5),
            (Action::SendMessages, 70),
            (Action::StoreConsumerOffset2, 25),
        ]);
        let mut wl = Workload::new(options);

        let mut commits_per_client: std::collections::HashMap<u128, u64> =
            std::collections::HashMap::new();
        let mut replies_seen = 0u64;
        for _tick in 0..4_000u32 {
            for c in &clients {
                if let Some((target, msg)) = wl.build_request(c) {
                    sim.submit_request(c.client_id(), target, msg.into_generic());
                }
            }
            for reply in sim.step() {
                let client_id = reply.header().client;
                let cmds = wl.on_reply(&reply);
                apply_sim_commands(&mut sim, &cmds);
                *commits_per_client.entry(client_id).or_insert(0) += 1;
                replies_seen += 1;
            }
        }

        assert!(
            replies_seen > 0,
            "multi-client workload produced no replies"
        );
        for &id in &client_ids {
            let count = commits_per_client.get(&id).copied().unwrap_or(0);
            assert!(
                count > 0,
                "client {id} observed no commits; multi-client routing is starving \
                 (counts: {commits_per_client:?})",
            );
        }
        let distinct = commits_per_client.values().filter(|&&c| c > 0).count();
        assert!(
            distinct >= 2,
            "commits concentrated on a single client ({commits_per_client:?}); \
             no interleaving observed"
        );
    }

    /// Outcome-first generation: with a single client the strict equality oracle
    /// is on, so any targeted-vs-committed mismatch panics in `on_reply`. Populate
    /// streams, then drive a Create/Delete mix targeting error outcomes
    /// (`NameAlreadyExists` by reusing a live name, `StreamNotFound` by fabricating
    /// an absent one). Assert the server committed rejections, proving the error
    /// paths are generated and verified end-to-end.
    #[test]
    fn outcome_first_generation_commits_targeted_rejections() {
        use crate::workload::{
            Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
        };

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolConfigOther {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0x5EED_0E4C,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = client::SimClient::new(client_id);
        let ns_a = server_common::sharding::IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns_a);
        sim.register_client_with_primary(&client);

        // Phase 1: populate streams so `NameAlreadyExists` has live targets.
        let mut options = WorkloadOptions::new(0x5EED_0E4C, replica_count, vec![ns_a]);
        options.weights = ActionWeights::new(&[(Action::CreateStream, 100)]);
        let mut wl = Workload::new(options);
        for _tick in 0..2_000u32 {
            if let Some((target, msg)) = wl.build_request(&client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
            for reply in sim.step() {
                let cmds = wl.on_reply(&reply);
                apply_sim_commands(&mut sim, &cmds);
            }
        }

        // Phase 2: keep creating (now hitting `NameAlreadyExists` on live names)
        // and deleting (hitting `StreamNotFound` on fabricated names).
        wl.options.weights =
            ActionWeights::new(&[(Action::CreateStream, 50), (Action::DeleteStream, 50)]);
        for _tick in 0..4_000u32 {
            if let Some((target, msg)) = wl.build_request(&client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
            for reply in sim.step() {
                let cmds = wl.on_reply(&reply);
                apply_sim_commands(&mut sim, &cmds);
            }
        }

        assert!(
            wl.auditor.stats().committed_rejections > 0,
            "outcome-first generation produced no committed rejections; error \
             outcomes are not being targeted, or the server is not rejecting them"
        );
    }

    fn workload_hash_for_seed(seed: u64) -> u64 {
        use crate::workload::{
            Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
        };
        use std::hash::{DefaultHasher, Hash, Hasher};

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            ..packet::PacketSimulatorOptions::default()
        };

        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);

        let ns_a = IggyNamespace::new(1, 1, 0);
        let ns_b = IggyNamespace::new(1, 1, 1);
        sim.init_partition(ns_a);
        sim.init_partition(ns_b);
        sim.register_client_with_primary(&client);

        let mut options = WorkloadOptions::new(seed, replica_count, vec![ns_a, ns_b]);
        options.weights = ActionWeights::new(&[
            (Action::CreateStream, 5),
            (Action::SendMessages, 70),
            (Action::StoreConsumerOffset2, 25),
        ]);

        let mut wl = Workload::new(options);
        let mut hasher = DefaultHasher::new();
        let mut replies_seen = 0u64;

        // Inline driver: hash each reply tuple so divergence is caught at
        // the first non-matching reply, not just end-of-run aggregates.
        // The reply cap lives inside the per-reply loop so a multi-reply
        // tick at replies_seen=49 cannot leak a 50th+ reply into the hash.
        'outer: for _tick in 0..5_000u32 {
            if let Some((target, msg)) = wl.build_request(&client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
            for reply in sim.step() {
                let h = reply.header();
                (
                    h.client,
                    h.request,
                    h.op,
                    h.commit,
                    h.namespace,
                    h.operation as u8,
                )
                    .hash(&mut hasher);
                let cmds = wl.on_reply(&reply);
                apply_sim_commands(&mut sim, &cmds);
                replies_seen += 1;
                if replies_seen >= 50 {
                    break 'outer;
                }
            }
        }
        assert!(
            replies_seen > 0,
            "workload produced no replies; driver / sim wiring is broken"
        );

        replies_seen.hash(&mut hasher);
        wl.shadow.sends_committed(ns_a).hash(&mut hasher);
        wl.shadow.sends_committed(ns_b).hash(&mut hasher);
        // Catches PRNG-trace shifts from `sample` returning `None`.
        // Stays 0 on the current seed mix; non-zero drifts the baseline.
        wl.samples_none().hash(&mut hasher);
        hasher.finish()
    }
}
