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
pub mod executor;
pub mod network;
pub mod packet;
pub mod ready_queue;
pub mod replica;
pub mod workload;

use bus::SimOutbox;
use client::SimClient;
use consensus::{ConsensusClock, MetadataHandle, PartitionsHandle, VsrState};
use deps::SimClock;
use deps::SimSuperblock;
use deps::{MemStorage, SimJournal};
use executor::{DetExecutor, RunOutcome, TaskId};
use iggy_binary_protocol::{GenericHeader, ReplyHeader};
use iggy_common::IggyError;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use metadata::impls::metadata::StreamsFrontend;
use network::Network;
use packet::{PacketSimulatorOptions, ProcessId};
use partitions::{Partition, PartitionOffsets, PollFragments, PollingArgs, PollingConsumer};
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;
use replica::{Replica, SIM_INBOX_CAPACITY, new_shard};
use server_common::Message;
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use shard::CONSENSUS_TICK_INTERVAL;
use shard::shards_table::{ShardsTable, calculate_shard_assignment};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;

/// Poll budget per [`DetExecutor::run_until_stalled`] call. The pumps are
/// event-driven, so hitting this means a task is spin-waking: always a bug,
/// surfaced as a panic carrying the seed.
const POLL_BUDGET: u32 = 100_000;

/// Salt for the entry-shard PRNG stream (sibling of the workload fault salt
/// and [`executor::EXECUTOR_SEED_SALT`]).
///
/// In production the shard-0 coordinator round-robins inbound connections
/// across shards, so the shard that receives a peer's bytes is unrelated to
/// the shard owning the target consensus group. The sim models that homing
/// with a seeded uniform pick per delivered packet; an independent stream
/// keeps those draws from perturbing network or workload traces.
pub const ENTRY_SHARD_SEED_SALT: u64 = 0x5A1A_F0E5_FACE_0003;

/// One simulated replica: its shards plus the executor bookkeeping needed
/// to crash it. One entry per shard in `shards`/`pump_tasks` (a single
/// shard until multi-shard lands).
pub struct SimReplica {
    /// Shards of this replica, indexed by shard id.
    pub shards: Vec<Rc<Replica>>,
    /// Shard 0's durable superblock, held here rather than inside the shard so its
    /// bytes survive the shard being dropped and rebuilt across a restart.
    pub superblock: Rc<SimSuperblock>,
    /// Shard 0's metadata WAL, held here for the same reason as the superblock: the
    /// bytes and index survive a restart, so a rebuilt replica recovers its
    /// op/commit and committed metadata from its own disk. Only shard 0 owns
    /// metadata consensus, so this is the single retained journal.
    pub metadata_journal: Rc<SimJournal<MemStorage>>,
    /// Shard 0's metadata consensus incarnation nonce. Harness-owned: a seed-derived
    /// value bumped by one on each restart, so successive incarnations are distinct
    /// yet the run stays byte-identical on replay. See
    /// `VsrConsensus::set_incarnation`.
    pub metadata_incarnation: u128,
    /// One durable superblock per partition group this replica has materialised,
    /// harness-owned for the same reason as the metadata one: the bytes survive
    /// the shards being dropped and rebuilt, so a re-materialised group recovers
    /// the `(view, log_view)` it recorded instead of re-entering view 0. Without
    /// a store the persist gate marks every view durable without writing, which
    /// leaves the gate, its write-failure fence, and view recovery all
    /// unexercised.
    pub partition_superblocks: RefCell<HashMap<IggyNamespace, Rc<SimSuperblock>>>,
    /// Keeps each pump's stop channel alive; dropping one would end that
    /// pump gracefully, which is reserved for future shutdown/restart
    /// tests (crash uses `DetExecutor::abort` instead).
    _stop_txs: Vec<shard::Sender<()>>,
    /// Pump task per shard, aborted on crash.
    pump_tasks: Vec<TaskId>,
}

impl SimReplica {
    /// The shard owning `namespace`'s partition data, by the same
    /// deterministic hash the router uses.
    ///
    /// # Panics
    /// Panics if the shard count does not fit `u32` (impossible: mesh
    /// construction caps it at `u16`).
    #[must_use]
    pub fn partition_shard(&self, namespace: IggyNamespace) -> &Rc<Replica> {
        let shard_count = u32::try_from(self.shards.len()).expect("shard count fits u32");
        let owner = calculate_shard_assignment(&namespace, shard_count);
        &self.shards[usize::from(owner)]
    }
}

pub struct Simulator {
    /// All replicas, indexed by replica id. Always fully populated — crashed
    /// replicas are kept alive but skipped during dispatch.
    pub replicas: Vec<SimReplica>,
    /// Per-replica outbox, indexed by replica id. Shared with consensus inside
    /// each replica via [`SharedSimOutbox`](bus::SharedSimOutbox).
    pub outboxes: Vec<Rc<SimOutbox>>,
    /// Set of replica ids that are currently crashed. Dispatch and outbox drain
    /// are skipped for these ids.
    pub crashed: HashSet<u8>,
    pub network: Network,
    pub replica_count: u8,
    pub client_ids: Vec<u128>,
    /// Drives every shard pump; scheduling picks and virtual time both
    /// derive from the network seed, so the schedule replays with it.
    executor: DetExecutor,
    /// Picks which shard of a replica receives each inbound packet,
    /// modeling the coordinator's connection homing (see
    /// [`ENTRY_SHARD_SEED_SALT`]).
    entry_rng: Xoshiro256Plus,
    /// Network seed, kept for livelock diagnostics.
    seed: u64,
    /// Dispatch-shell mode: when set, inbound client packets are delivered
    /// through the real `on_client_request` handler (see
    /// [`shard::IggyShard::deliver_client_request`]) instead of the raw
    /// `dispatch` routing. Chosen at construction via
    /// [`Simulator::with_shards_shell`].
    shell: bool,
}

impl Simulator {
    /// New simulator with per-replica outboxes routed through a [`Network`].
    ///
    /// # Panics
    /// Panics if `clients` yields duplicate `client_id`s. The auditor
    /// keys in-flight entries by `(client_id, request)` and the network
    /// indexes packet routes by `client_id`; duplicates would collide on
    /// both.
    pub fn new(
        replica_count: usize,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
    ) -> Self {
        Self::with_shards(replica_count, 1, clients, network_options)
    }

    /// [`Simulator::new`] with `shards_per_replica` shards on every replica,
    /// meshed exactly like the server bootstrap: metadata plane on shard 0,
    /// partitions hash-assigned, one pump task per shard.
    ///
    /// # Panics
    /// Panics on duplicate `client_id`s (see [`Simulator::new`]) or
    /// `shards_per_replica == 0`.
    pub fn with_shards(
        replica_count: usize,
        shards_per_replica: u16,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
    ) -> Self {
        Self::build(
            replica_count,
            shards_per_replica,
            clients,
            network_options,
            false,
        )
    }

    /// [`Simulator::with_shards`] with the deterministic dispatch shell on:
    /// every shard wires the server's real dispatch handlers, so a client
    /// request runs as a task the seeded executor interleaves with the
    /// pump. Off (the default) keeps the raw-`on_message` fast path.
    ///
    /// # Panics
    /// Panics on duplicate `client_id`s or `shards_per_replica == 0`.
    pub fn with_shards_shell(
        replica_count: usize,
        shards_per_replica: u16,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
    ) -> Self {
        Self::build(
            replica_count,
            shards_per_replica,
            clients,
            network_options,
            true,
        )
    }

    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    fn build(
        replica_count: usize,
        shards_per_replica: u16,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
        shell: bool,
    ) -> Self {
        assert!(
            shards_per_replica >= 1,
            "a replica needs at least one shard"
        );
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
        let seed = network_options.seed;
        let mut network = Network::new(network_options);

        for &cid in &client_ids {
            network.register_client(cid);
        }

        let mut executor = DetExecutor::new(seed);
        let timer = executor.timer();
        let spawns = executor.spawner();
        // One virtual clock for every consensus group: prepare timestamps
        // become a pure function of the seed instead of the wall clock.
        let consensus_clock = ConsensusClock::new(Rc::new(SimClock::new(timer.clone())));

        let rc = replica_count as u8;
        let mut replicas = Vec::with_capacity(replica_count);
        let mut outboxes = Vec::with_capacity(replica_count);

        for i in 0..replica_count {
            let id = i as u8;
            let mut bus = SimOutbox::new(id, timer.clone(), spawns.clone());
            for &cid in &client_ids {
                bus.add_client(cid);
            }
            for j in 0..rc {
                bus.add_replica(j);
            }
            let outbox = Rc::new(bus);
            // Harness-owned so they outlive a replica restart, which drops and
            // rebuilds the shards: the superblock's VSR state and the metadata WAL
            // both survive.
            let superblock = Rc::new(SimSuperblock::default());
            let metadata_journal = Rc::new(SimJournal::<MemStorage>::default());
            // Initial incarnation, spread across the 128-bit space per replica so the
            // restart increments in `replica_restart` never collide across replicas.
            // Non-zero.
            let metadata_incarnation = 1 + (u128::from(id) << 64);

            // One crossfire mesh per replica; every shard gets a clone of
            // the canonical senders vec and exclusively takes its inbox.
            let (senders, mut inboxes, mut reply_inboxes) = shard::shard_mesh_channels(
                shards_per_replica,
                SIM_INBOX_CAPACITY,
                SIM_INBOX_CAPACITY,
            );

            let mut shards = Vec::with_capacity(usize::from(shards_per_replica));
            let mut stop_txs = Vec::with_capacity(usize::from(shards_per_replica));
            let mut pump_tasks = Vec::with_capacity(usize::from(shards_per_replica));
            // Single-writer metadata (mirrors the server bootstrap): shard 0
            // builds the writable STM and mints a factory bundle; every peer
            // shard rebuilds a reader-mode mirror from it and sees committed
            // metadata through the shared read handle. Shards are built in index
            // order, so shard 0's bundle exists before any peer needs it.
            let mut metadata_bundle: Option<replica::SimMetadataBundle> = None;
            for shard_idx in 0..shards_per_replica {
                let inbox = inboxes[usize::from(shard_idx)]
                    .take()
                    .expect("mesh yields exactly one inbox per shard");
                let reply_inbox = reply_inboxes[usize::from(shard_idx)]
                    .take()
                    .expect("mesh yields exactly one reply inbox per shard");
                // Only shard 0 owns metadata consensus, so only it carries the
                // superblock. Peer shards persist nothing.
                let shard_superblock = if shard_idx == 0 {
                    Some(superblock.clone())
                } else {
                    None
                };
                let shard_journal = (shard_idx == 0).then(|| Rc::clone(&metadata_journal));
                let (shard, writer_bundle) = new_shard(
                    id,
                    shard_idx,
                    format!("replica-{i}-shard-{shard_idx}"),
                    &outbox,
                    rc,
                    senders.clone(),
                    inbox,
                    reply_inbox,
                    consensus_clock.clone(),
                    shell,
                    metadata_bundle.clone(),
                    shard_superblock,
                    shard_journal,
                    None, // fresh boot: no recovered VSR state
                    metadata_incarnation,
                );
                if shard_idx == 0 {
                    metadata_bundle = Some(
                        writer_bundle
                            .expect("shard 0 returns the metadata factory bundle for peers"),
                    );
                }

                // Same wiring as the server bootstrap: one pump task per
                // shard, stopped only by the (held) stop channel or a
                // crash abort.
                let (stop_tx, stop_rx) = shard::channel::<()>(1);
                let pump_shard = Rc::clone(&shard);
                pump_tasks.push(executor.spawn(async move {
                    pump_shard.run_message_pump(stop_rx).await;
                }));
                stop_txs.push(stop_tx);
                shards.push(shard);
            }

            replicas.push(SimReplica {
                shards,
                superblock,
                metadata_journal,
                metadata_incarnation,
                partition_superblocks: RefCell::new(HashMap::new()),
                _stop_txs: stop_txs,
                pump_tasks,
            });
            outboxes.push(outbox);
        }

        Self {
            replicas,
            outboxes,
            crashed: HashSet::new(),
            network,
            replica_count: rc,
            client_ids,
            executor,
            entry_rng: Xoshiro256Plus::seed_from_u64(seed ^ ENTRY_SHARD_SEED_SALT),
            seed,
            shell,
        }
    }

    /// Init a partition with its own consensus group on every live replica.
    ///
    /// Mirrors the reconciler's outcome without running it: the namespace is
    /// committed to metadata, the partition materialises only on its
    /// hash-owning shard, and every shard of the replica gets the routing row
    /// stamped with the committed `created_revision` (production seeds rows
    /// through `ReconcileOp::{InsertOwned,InsertRouted}`).
    ///
    /// # Panics
    /// Panics if a replica's shard count does not fit `u32` (impossible:
    /// mesh construction caps it at `u16`).
    // TODO(hubcio): partitions created down this path are built via
    // `IggyPartition::with_in_memory_storage` and rely on the writer-less
    // persist branch in `IggyPartition`; give them first-class in-memory
    // segment storage so that branch can be deleted.
    #[allow(clippy::cast_possible_truncation)]
    pub fn init_partition(&mut self, namespace: IggyNamespace) {
        for (i, replica) in self.replicas.iter().enumerate() {
            if self.crashed.contains(&(i as u8)) {
                continue;
            }
            materialise_partition(replica, namespace);
        }
    }

    /// Seed the metadata `Streams` STM on each live replica's shard-0 writer
    /// so a poll's namespace resolution (`resolve_partition_namespace`)
    /// succeeds for `namespace`, which the partition-plane-only
    /// [`Self::init_partition`] does not populate. Peer shards observe the
    /// seed through their shared left-right read handle, so only shard 0 is
    /// seeded (a direct seed on a reader-mode peer STM would panic). The
    /// simulator does not wire the reconciler, so this bypasses it the same
    /// way `init_partition` bypasses it for the partition plane. Pair it with
    /// `init_partition` for the same namespace on the dispatch-shell poll path.
    ///
    #[allow(clippy::cast_possible_truncation)]
    pub fn seed_stream_topic_partition(&self, namespace: IggyNamespace) {
        for (i, replica) in self.replicas.iter().enumerate() {
            if self.crashed.contains(&(i as u8)) {
                continue;
            }
            // Shard 0 is the sole metadata writer; peers share its read handle
            // and see the seed through the left-right publish, so seeding a
            // peer's reader-mode STM directly would panic.
            replica.shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .seed_namespace(namespace, namespace.inner());
        }
    }

    /// Log `client` in against the deterministic root user through the
    /// dispatch shell: the real `on_client_request` path verifies the
    /// seeded root credentials and runs the consensus `Register`, then this
    /// binds the assigned session on the client. Requires the shell on and
    /// the root user seeded (see [`new_shard`]); target is the primary
    /// (replica 0), as [`Self::register_client_with_primary`] does.
    ///
    /// # Panics
    /// If no login reply arrives within 200 steps or it carries no session.
    pub fn shell_login(&mut self, client: &SimClient) {
        // Register the client's connection metadata on every replica, as
        // `install_client_fd` does in production. `ensure_transport_connection`
        // reads it to admit the connection into the SessionManager, which the
        // login's session bind (Connected -> Authenticated -> Bound) requires.
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        for outbox in &self.outboxes {
            outbox.insert_client_meta(ClientConnMeta::new(
                client.client_id(),
                addr,
                ClientTransportKind::Tcp,
            ));
        }

        let msg = client.login(replica::SHELL_ROOT_USERNAME, replica::SHELL_ROOT_PASSWORD);
        self.submit_request(client.client_id(), 0, msg.into_generic());
        let mut session = 0u64;
        let mut got_reply = false;
        for _ in 0..200 {
            if let Some(reply) = self.step().first() {
                // The login reply carries the assigned session in `op`
                // (`build_reply_with_body` maps the session field to `op`).
                session = reply.header().op;
                got_reply = true;
                break;
            }
        }
        assert!(got_reply, "shell_login: no login reply within 200 steps");
        assert!(session > 0, "shell_login: login reply carried no session");
        client.bind_session(session);
    }

    /// Advance the simulation by one tick. Returns client replies delivered.
    ///
    /// Every shard runs its real message pump as an executor task, so a
    /// step is: fire the virtual consensus tick, let the pumps run to
    /// quiescence, feed network packets into the shard routers, let the
    /// pumps process the resulting frames, then exchange outboxes with the
    /// network. Interleaving between pumps is a seeded executor pick;
    /// everything a step produces lands on the wire before network time
    /// advances, matching the pre-executor phase semantics.
    ///
    /// # Panics
    /// If a client-addressed packet cannot be decoded as `ReplyHeader`, or
    /// if a pump livelocks (poll budget exhausted).
    #[allow(clippy::cast_possible_truncation)]
    pub fn step(&mut self) -> Vec<Message<ReplyHeader>> {
        let mut client_replies = Vec::new();

        // Phase 0: Fire the pumps' consensus-tick timers (view change,
        // retransmits) and run them to quiescence. Crashed replicas have no
        // live pump tasks, so they are skipped implicitly.
        self.executor.advance_time(CONSENSUS_TICK_INTERVAL);
        self.run_pumps();

        // Phase 1: Deliver ready packets from the network into the shard
        // routers. `dispatch` classifies and enqueues onto the owning
        // shard's inbox; the pumps drain those frames in phase 1b.
        let packets = self.network.step();
        for packet in &packets {
            match packet.to {
                ProcessId::Replica(id) => {
                    if !self.crashed.contains(&id)
                        && let Some(replica) = self.replicas.get(id as usize)
                    {
                        // Seeded homing: the receiving shard is usually NOT
                        // the owner, so the frame takes the real router hop
                        // (dispatch -> mesh -> owning pump), as it does when
                        // the coordinator homes a peer connection on an
                        // arbitrary shard.
                        let entry = self.entry_rng.random_range(0..replica.shards.len());
                        match packet.from {
                            // Shell mode: every client request enters through the
                            // real `on_client_request` handler (as the client-fd
                            // listener does in production), so it drains as a task
                            // the executor interleaves with the pump. Partition
                            // writes now use the same legacy `SendMessages` wire
                            // shape the real SDK sends, so
                            // `resolve_partition_request_namespace` decodes them
                            // on this path. Consensus frames (replica-sourced)
                            // always route raw.
                            ProcessId::Client(client_id) if self.shell => {
                                replica.shards[entry]
                                    .deliver_client_request(client_id, packet.message.deep_copy());
                            }
                            _ => replica.shards[entry].dispatch(packet.message.deep_copy()),
                        }
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

        // Phase 1b: Pumps process the delivered frames (and their loopback
        // and reconcile follow-ups) to quiescence.
        self.run_pumps();

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

    /// Rolling hash of the executor schedule (every poll and timer fire).
    /// Two runs from the same seed and inputs must agree; determinism
    /// tests assert on it alongside the reply-trace hash.
    #[must_use]
    pub const fn schedule_hash(&self) -> u64 {
        self.executor.schedule_hash()
    }

    /// Run the executor until every pump is parked again.
    ///
    /// # Panics
    /// On budget exhaustion: pumps are event-driven, so this is a
    /// spin-waking task, i.e. a livelock bug. The seed reproduces it.
    ///
    /// Also on a lost wakeup: a non-crashed pump quiescing with a non-empty
    /// inbox (see [`Self::assert_inboxes_drained`]).
    fn run_pumps(&mut self) {
        match self.executor.run_until_stalled(POLL_BUDGET) {
            RunOutcome::Quiescent { .. } => self.assert_inboxes_drained(),
            RunOutcome::BudgetExhausted { polls } => panic!(
                "simulator livelock: {polls} polls without quiescing \
                 (seed {:#x}, schedule hash {:#x})",
                self.seed,
                self.executor.schedule_hash(),
            ),
        }
    }

    /// Lost-wake tripwire. At executor quiescence every live pump must have
    /// drained its inbox: a non-empty inbox on a non-crashed replica means a
    /// frame reached the channel without waking the target pump. Because
    /// every pump holds a standing `CONSENSUS_TICK_INTERVAL` timer, the next
    /// `advance_time` would re-poll and silently drain it, masking the exact
    /// wake-loss class this harness exists to catch, so trip here instead.
    ///
    /// Incomplete by construction: it catches a lost wakeup only while the
    /// un-woken frame is still queued at quiescence. A later frame that does
    /// wake the pump drains the whole inbox (the recv loop pulls every queued
    /// frame), so a lost wake masked by a subsequent drain slips through. The
    /// direction is safe: a non-empty inbox at true quiescence is always a
    /// real lost wake, so it never false-trips.
    ///
    /// Crashed replicas are skipped: their pump tasks are aborted, so any
    /// frame stranded in their inbox has no drainer and is expected.
    #[allow(clippy::cast_possible_truncation)]
    fn assert_inboxes_drained(&self) {
        for (replica_id, replica) in self.replicas.iter().enumerate() {
            if self.crashed.contains(&(replica_id as u8)) {
                continue;
            }
            for shard in &replica.shards {
                let pending = shard.inbox_len();
                assert_eq!(
                    pending,
                    0,
                    "lost wakeup: replica {replica_id} shard {} inbox holds {pending} \
                     frame(s) at quiescence (seed {:#x}, schedule hash {:#x})",
                    shard.id,
                    self.seed,
                    self.executor.schedule_hash(),
                );
                let pending_replies = shard.reply_inbox_len();
                assert_eq!(
                    pending_replies,
                    0,
                    "lost wakeup: replica {replica_id} shard {} reply lane holds \
                     {pending_replies} frame(s) at quiescence (seed {:#x}, schedule hash {:#x})",
                    shard.id,
                    self.seed,
                    self.executor.schedule_hash(),
                );
            }
        }
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

    /// Crash a replica: abort its pump tasks, disable its network links, and discard
    /// its outbox. The replica object stays alive but receives no messages; a
    /// following [`Self::replica_restart`] drops and rebuilds it from the durable
    /// superblock, which is when volatile state is actually lost and recovered.
    ///
    /// # Panics
    /// If the replica is already crashed.
    pub fn replica_crash(&mut self, replica_index: u8) {
        assert!(
            !self.crashed.contains(&replica_index),
            "cannot crash replica {replica_index}: already down"
        );

        // Hard-stop the pumps: futures drop mid-await, destructors cancel
        // their channel and timer registrations, and the graceful inbox
        // drain never runs: a crash, not a shutdown.
        for task in &self.replicas[replica_index as usize].pump_tasks {
            self.executor.abort(*task);
        }

        // Tear down any detached dispatch tasks this replica's bus spawned
        // (off-pump poll IO, request drains), so a crash leaves no orphaned
        // tasks running against the dead replica.
        self.executor.abort_replica_spawned(replica_index);

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

    /// Restart a crashed replica: drop its shards, losing all volatile consensus
    /// state as a real restart does, and rebuild them against the retained
    /// superblock, recovering `(view, log_view)` from disk exactly as production's
    /// `restore_metadata_consensus` does. The superblock and outbox are
    /// harness-owned, so they survive the drop; a fresh inter-shard mesh and pump
    /// tasks are wired, and the network is re-enabled.
    ///
    /// # Panics
    /// If the replica is not crashed, or if its shard count does not fit `u16`,
    /// impossible since mesh construction caps it.
    pub fn replica_restart(&mut self, replica_index: u8) {
        assert!(
            self.crashed.contains(&replica_index),
            "cannot restart replica {replica_index}: not crashed"
        );
        let idx = replica_index as usize;
        let shards_per_replica =
            u16::try_from(self.replicas[idx].shards.len()).expect("shard count fits u16");
        let superblock = Rc::clone(&self.replicas[idx].superblock);
        // The metadata WAL is harness-owned too, so its bytes and index survive the
        // drop: the rebuilt shard 0 recovers op/commit and committed state from it,
        // not from an empty journal.
        let metadata_journal = Rc::clone(&self.replicas[idx].metadata_journal);
        // Bump the incarnation on restart, so a StartView addressed to the previous
        // incarnation and still in flight is ignored. Deterministic, so replay stays
        // byte-identical.
        let metadata_incarnation = self.replicas[idx].metadata_incarnation + 1;
        // Partition superblocks carry forward too: a group re-materialised after
        // the restart must recover its recorded view from the same store, exactly
        // as a rebooted server partition reads the record in its directory.
        let partition_superblocks =
            std::mem::take(&mut *self.replicas[idx].partition_superblocks.borrow_mut());

        // Recover the durable VSR state from the retained superblock before the
        // rebuild, as production reads it in restore_metadata_consensus.
        let recovered_state = superblock
            .read_latest_sync()
            .and_then(|bytes| VsrState::try_from(bytes.as_slice()).ok());

        let consensus_clock = ConsensusClock::new(Rc::new(SimClock::new(self.executor.timer())));
        let outbox = Rc::clone(&self.outboxes[idx]);
        let (senders, mut inboxes, mut reply_inboxes) =
            shard::shard_mesh_channels(shards_per_replica, SIM_INBOX_CAPACITY, SIM_INBOX_CAPACITY);

        let mut shards = Vec::with_capacity(usize::from(shards_per_replica));
        let mut stop_txs = Vec::with_capacity(usize::from(shards_per_replica));
        let mut pump_tasks = Vec::with_capacity(usize::from(shards_per_replica));
        let mut metadata_bundle: Option<replica::SimMetadataBundle> = None;
        for shard_idx in 0..shards_per_replica {
            let inbox = inboxes[usize::from(shard_idx)]
                .take()
                .expect("mesh yields exactly one inbox per shard");
            let reply_inbox = reply_inboxes[usize::from(shard_idx)]
                .take()
                .expect("mesh yields exactly one reply inbox per shard");
            let shard_superblock = if shard_idx == 0 {
                Some(superblock.clone())
            } else {
                None
            };
            let shard_journal = (shard_idx == 0).then(|| Rc::clone(&metadata_journal));
            let (shard, writer_bundle) = new_shard(
                replica_index,
                shard_idx,
                format!("replica-{replica_index}-shard-{shard_idx}"),
                &outbox,
                self.replica_count,
                senders.clone(),
                inbox,
                reply_inbox,
                consensus_clock.clone(),
                self.shell,
                metadata_bundle.clone(),
                shard_superblock,
                shard_journal,
                recovered_state,
                metadata_incarnation,
            );
            if shard_idx == 0 {
                metadata_bundle =
                    Some(writer_bundle.expect("shard 0 returns the metadata factory bundle"));
            }
            let (stop_tx, stop_rx) = shard::channel::<()>(1);
            let pump_shard = Rc::clone(&shard);
            pump_tasks.push(self.executor.spawn(async move {
                pump_shard.run_message_pump(stop_rx).await;
            }));
            stop_txs.push(stop_tx);
            shards.push(shard);
        }

        // Replacing the replica drops the old shards, losing all volatile consensus
        // state as a real restart does. The harness-owned superblock carries forward.
        self.replicas[idx] = SimReplica {
            shards,
            superblock,
            metadata_journal,
            metadata_incarnation,
            partition_superblocks: RefCell::new(partition_superblocks),
            _stop_txs: stop_txs,
            pump_tasks,
        };

        // Re-materialise every group this replica had before the crash, as a
        // rebooted the server re-opens every partition directory it owns. This
        // is what makes the carried-forward superblock load-bearing: the group
        // recovers the `(view, log_view)` it recorded instead of re-entering
        // view 0.
        // SORTED: `HashMap` iteration order is seeded per process, and
        // materialisation order is observable (shard init order, routing-row
        // stamps), so replay would stop being byte-identical.
        let mut materialised: Vec<IggyNamespace> = self.replicas[idx]
            .partition_superblocks
            .borrow()
            .keys()
            .copied()
            .collect();
        materialised.sort_unstable_by_key(IggyNamespace::inner);
        for namespace in materialised {
            materialise_partition(&self.replicas[idx], namespace);
        }

        // Reconnect to the network and mark the replica live again.
        self.network
            .process_enable(ProcessId::Replica(replica_index));
        self.crashed.remove(&replica_index);
    }

    /// Advance consensus timeouts on every live replica without a full
    /// step cycle: fires the pumps' virtual tick timers and runs the
    /// executor to quiescence.
    ///
    /// # Panics
    /// If a pump livelocks (poll budget exhausted).
    pub fn tick(&mut self) {
        self.executor.advance_time(CONSENSUS_TICK_INTERVAL);
        self.run_pumps();
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
        let shard = self.replicas[replica_idx].partition_shard(namespace);
        // Build the owned poll plan synchronously, then execute off the borrow.
        // The sim's partitions are in-memory (no `partition_dir`), so the plan
        // serves only the resident journal tier; `execute` performs no disk IO.
        //
        // This is the one `block_on` allowed to stay, and only because
        // `plan.execute()` cannot suspend here: the sim's partitions are
        // in-memory, so the plan serves the resident journal tier with no disk
        // IO and no `bus.sleep`. If it ever grew a suspending await it would
        // fail two ways -- on the virtual clock it would hang this thread
        // forever (the clock only advances through `advance_time`, which does
        // not run during `block_on`), and on the retry path it would panic on
        // the compio timer outside a compio runtime. Safe today only because
        // it runs between `run_pumps` calls, when the executor is quiescent and
        // no pump can hold the partition commit lock in a suspended frame.
        let Some(plan) = shard
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
        let shard = self.replicas[replica_idx].partition_shard(namespace);
        let partition = shard.plane.partitions().get_by_ns(&namespace)?;
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
        let shard = self.replicas[replica_idx].partition_shard(namespace);
        let partition = shard.plane.partitions().get_by_ns(&namespace)?;
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
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)?;
                let consensus = partition.consensus();
                Some(consensus.primary_index(consensus.view()))
            })
    }
}

/// Materialises `namespace` on its hash-owning shard of one replica and stamps
/// the routing row on every shard of that replica.
///
/// Shared by [`SimCluster::init_partition`] and the restart path: a rebooted
/// server re-opens every partition directory it owns, so the sim has to
/// re-materialise too, otherwise the superblock a restart carries forward is
/// never read back and the recovered-view branch is dead code.
fn materialise_partition(replica: &SimReplica, namespace: IggyNamespace) {
    let shard_count = u32::try_from(replica.shards.len()).expect("shard count fits u32");
    let owner = calculate_shard_assignment(&namespace, shard_count);
    // One store per group, minted on first materialisation and reused on every
    // later one, so the recorded view survives a replica restart.
    let superblock = Rc::clone(
        replica
            .partition_superblocks
            .borrow_mut()
            .entry(namespace)
            .or_default(),
    );
    let recovered_state = superblock
        .read_latest_sync()
        .and_then(|bytes| VsrState::try_from(bytes.as_slice()).ok());
    replica.shards[usize::from(owner)].init_partition(namespace, Some(superblock), recovered_state);
    // Commit the namespace before stamping the rows: a partition the metadata
    // plane never heard of is a shape production cannot produce, and the shard
    // refuses to serve client traffic whose routing-row epoch it cannot match
    // against a committed `created_revision`.
    let streams = replica.shards[0].plane.metadata().mux_stm.streams();
    streams.seed_namespace(namespace, namespace.inner());
    let epoch = streams
        .created_revision_for_namespace(namespace)
        .expect("namespace committed by the seed above");
    for shard in &replica.shards {
        shard.shards_table().insert(
            namespace,
            PartitionLocation::new(ShardId::new(owner), epoch),
        );
    }
}

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
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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
            let replica = &sim.replicas[replica_idx as usize].shards[0];
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
        let c = sim.replicas[1].shards[0]
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

    /// A metadata replica that advanced its view, persisted it through the superblock
    /// gate, then crashed recovers that same view from its own disk on restart, not a
    /// fresh 0. The split-brain guarantee: a replica never forgets a view it acted in.
    /// Impossible before the superblock, since a rebuilt consensus starts at view 0.
    #[test]
    fn given_advanced_view_when_metadata_replica_restarts_should_recover_view_from_superblock() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        // Metadata consensus view of a replica's shard 0, if it owns one.
        let metadata_view = |sim: &Simulator, replica: u8| -> Option<u32> {
            let consensus = sim.replicas[replica as usize].shards[0]
                .plane
                .metadata()
                .consensus
                .as_ref()?;
            Some(consensus.view())
        };

        // Crash the metadata primary (replica 0) to force a view change on the
        // survivors; each persists the new view through the gate.
        sim.replica_crash(0);
        for _ in 0..800 {
            sim.step();
        }

        // Find the new metadata primary, elected at view >= 1.
        let primary = (1..replica_count)
            .find(|&replica| {
                sim.replicas[replica as usize].shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .is_some_and(|consensus| {
                        consensus.view() > 0
                            && consensus.status() == Status::Normal
                            && consensus.is_primary()
                    })
            })
            .expect("a metadata primary elected at view >= 1 after crashing replica 0");
        let view_before = metadata_view(&sim, primary).expect("primary owns metadata consensus");
        assert!(
            view_before >= 1,
            "expected an advanced view, got {view_before}"
        );

        // Crash and restart the new primary. Restart drops its shards, losing the
        // in-memory view, and rebuilds from the retained superblock.
        sim.replica_crash(primary);
        sim.replica_restart(primary);

        // It recovered its persisted view from the superblock, not a fresh 0.
        let view_after = metadata_view(&sim, primary).expect("restarted primary owns consensus");
        assert_eq!(
            view_after, view_before,
            "restarted replica must recover its persisted view from the superblock, not reset to 0"
        );

        // This run takes zero traffic, so every WAL is empty: the recovered view came
        // from the superblock alone. Pin that, since it is what makes the restart gate
        // below load-bearing.
        assert!(
            sim.replicas[primary as usize]
                .metadata_journal
                .last_op()
                .is_none(),
            "no metadata traffic in this test, so the restarted replica's WAL must be empty"
        );

        // A recovered view is a prior life even with an empty WAL, so the replica must
        // rejoin as a probing backup. It is still primary-by-index for the recovered
        // view, so resuming primaryship here would have it act as primary in a view the
        // survivors may already have left, with no probe to correct it.
        let restarted = sim.replicas[primary as usize].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("restarted primary owns metadata consensus");
        assert!(
            restarted.is_primary(),
            "the restarted replica is still primary-by-index for the recovered view, \
             which is what makes ceding it necessary"
        );
        assert_eq!(
            restarted.status(),
            Status::Recovering,
            "a replica with a recovered view must probe for the current view, not \
             resume primaryship"
        );
        assert!(
            restarted.has_ceded_primaryship(),
            "a probing replica must be quorum-invisible until a StartView brings it \
             forward"
        );
    }

    #[test]
    fn given_committed_metadata_when_solo_replica_restarts_should_recover_from_own_wal() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: 1,
            client_count: 1,
            ..packet::PacketSimulatorOptions::default()
        };
        // Solo cluster: 1-of-1 quorum commits every metadata op the instant it is
        // journaled, giving a fully-committed WAL with no uncommitted suffix to
        // reconcile on restart.
        let mut sim = Simulator::new(1, std::iter::once(client_id), network_opts);
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);

        // Resolves the namespace of the stream and topic created below; `Some` exactly
        // when the Streams STM holds them.
        let resolve = |sim: &Simulator| {
            sim.replicas[0].shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .namespace_from_partition(
                    &iggy_binary_protocol::WireIdentifier::named("events").unwrap(),
                    &iggy_binary_protocol::WireIdentifier::named("logs").unwrap(),
                    0,
                )
        };

        // Drive committed metadata through consensus: a stream, then a topic with one
        // partition under it. Each appends a prepare to shard 0's WAL and mutates the
        // Streams STM. The topic references the stream, so the stream commits first.
        for msg in [
            client.create_stream("events"),
            client.create_topic("events", "logs", 1),
        ] {
            sim.submit_request(client_id, 0, msg.into_generic());
            for _ in 0..50 {
                sim.step();
            }
        }

        let namespace_before = resolve(&sim).expect("stream + topic must resolve after creation");
        let head_before = sim.replicas[0]
            .metadata_journal
            .last_op()
            .expect("metadata ops must have been appended to the WAL");
        let commit_before = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("solo shard 0 owns metadata consensus")
            .commit_min();
        assert_eq!(
            commit_before, head_before,
            "a solo replica commits every durable op, so commit tracks the WAL head"
        );

        // Crash and restart: the shards are dropped, losing all volatile consensus and
        // state-machine state, and rebuilt against the RETAINED WAL and superblock, so
        // recovery is from this replica's own disk with no peer.
        sim.replica_crash(0);
        sim.replica_restart(0);

        // The WAL bytes and index survived the restart.
        assert_eq!(
            sim.replicas[0].metadata_journal.last_op(),
            Some(head_before),
            "the metadata WAL head must survive a restart, bytes and index retained"
        );
        // Consensus recovered its op/commit from its own disk, not a fresh 0.
        let consensus_ref = sim.replicas[0].shards[0].plane.metadata();
        let consensus = consensus_ref
            .consensus
            .as_ref()
            .expect("restarted solo shard 0 owns metadata consensus");
        assert_eq!(
            consensus.commit_min(),
            head_before,
            "commit must be recovered from the retained WAL, not reset to 0"
        );
        // Replaying the retained WAL reconstructed the committed Streams STM, so the
        // stream and topic survive the restart from this replica's own disk.
        assert_eq!(
            resolve(&sim),
            Some(namespace_before),
            "the created stream/topic must survive the restart via WAL replay"
        );
    }

    #[test]
    fn given_registered_client_when_solo_replica_restarts_should_recover_session_from_own_wal() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: 1,
            client_count: 1,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(1, std::iter::once(client_id), network_opts);
        let client = SimClient::new(client_id);

        // Register creates the client-table session; one committed metadata op caches
        // a reply for at-most-once dedup. Both live in the client table, which is not
        // part of the state machine and would otherwise reset to empty on restart.
        sim.register_client_with_primary(&client);
        sim.submit_request(client_id, 0, client.create_stream("events").into_generic());
        for _ in 0..50 {
            sim.step();
        }

        let epoch_before = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .client_table
            .borrow()
            .get_epoch(client_id);
        assert!(
            epoch_before.is_some(),
            "client must hold a session before the crash"
        );

        // Crash and restart: the client table drops with the shard and is rebuilt by
        // replaying the retained WAL through the same commit apply path the live
        // cluster uses.
        sim.replica_crash(0);
        sim.replica_restart(0);

        let epoch_after = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .client_table
            .borrow()
            .get_epoch(client_id);
        assert_eq!(
            epoch_after, epoch_before,
            "the client session must survive a restart, reconstructed from the retained WAL, \
             so a returning client is recognized instead of hitting NoSession"
        );
    }

    #[test]
    fn given_superblock_write_fails_when_primary_crashes_should_withhold_votes_and_not_elect() {
        // The split-brain gate under test: a view-scoped send (SVC, DVC, StartView)
        // must not go out until the new view is durable. Fail one survivor's
        // superblock writes so its persist gate returns false, advancing its view
        // in-memory while withholding every view-scoped send. In a 3-replica cluster,
        // quorum 2, crashing the primary leaves one working survivor whose lone vote
        // cannot reach quorum, so NO new primary is elected. Were the gate to send
        // before persisting, the withheld votes would reach the peer and elect a
        // primary that has no durable record of the new view, reintroducing
        // split-brain on its restart. The positive control is
        // `given_advanced_view_when_metadata_replica_restarts_...`, which elects a new
        // primary from the same crash with healthy superblocks.
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        // Replica 1 survives the crash and is the primary-by-index for view 1.
        // Failing its superblock keeps it from durably taking any new view, so it
        // never emits a view-scoped vote.
        sim.replicas[1].superblock.set_fail_writes();

        sim.replica_crash(0);
        for _ in 0..800 {
            sim.step();
        }

        for replica in 1..replica_count {
            let elected = sim.replicas[replica as usize].shards[0]
                .plane
                .metadata()
                .consensus
                .as_ref()
                .is_some_and(|consensus| {
                    consensus.view() > 0
                        && consensus.status() == Status::Normal
                        && consensus.is_primary()
                });
            assert!(
                !elected,
                "replica {replica} must not be elected primary while a survivor's \
                 superblock write fails: the persist gate withholds its view-scoped \
                 votes, so quorum cannot form"
            );
        }

        // The faulted replica never durably recorded a new view, so a restart recovers
        // its old view, never one it might have voted in.
        let faulted = sim.replicas[1].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("replica 1 owns metadata consensus");
        assert!(
            faulted.view() > 0,
            "the gate must withhold the SEND, not the view advance: with the view still \
             at 0 there was nothing to persist and this test would pass vacuously"
        );
        assert_eq!(
            sim.replicas[1].superblock.read_latest_sync(),
            None,
            "a failing superblock persists nothing, so no new view is durable"
        );

        // The retry is bounded. Without a backoff the 10 ms consensus tick would run a
        // full `atomic_replace` (create, write, fsync, rename, dir fsync) on every tick
        // for as long as the disk stays broken, on the executor that also serves
        // partition traffic.
        let attempts = sim.replicas[1].shards[0]
            .plane
            .metadata()
            .superblock_write_failures();
        assert!(attempts > 0, "the gate must have attempted a persist");
        assert!(
            attempts < 40,
            "superblock writes must back off, not retry every tick (attempted \
             {attempts} times)"
        );
    }

    /// At-least-once failover: `SendMessages` retry on a new primary
    /// re-executes. Retry reply carries a HIGHER `commit` op (re-execution
    /// proof, not dedup). Duplicate payload lives at two offsets; consumers
    /// dedup if they need at-most-once-per-payload.
    #[test]
    fn failover_retry_re_executes_under_at_least_once() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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
        let live = &sim.replicas[1].shards[0];
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
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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
            let c = sim.replicas[idx as usize].shards[0]
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
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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
        //
        // Re-locked when the sim adopted METADATA_GROUP (1<<63)
        // for metadata requests and the metadata consensus group, replacing
        // the sim-only 0: reply headers and the per-group timeout-jitter seed
        // (replica_id ^ namespace) both changed. The old 0 only ever routed
        // correctly because `hash % 1 == 0` at one shard per replica.
        // Re-locked again when replies stopped echoing a group id (the
        // client wire lost its namespace field): the reply-hash tuple
        // dropped that component. Re-locked when the v1 consumer-offset ops
        // were removed and the v2 pair became the only store/delete actions:
        // `Action` lost two variants, shifting discriminants and draw order.
        assert_eq!(
            h1, 0x14BF_3C3F_41D4_F69D,
            "workload reply hash drifted from locked baseline"
        );
    }

    /// Drive workload with near-uniform weights across all 23 `Action` variants.
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

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        // 23 variants: 8 x 5 + 15 x 4 = 100 (weights must sum to 100).
        assert_eq!(Action::COUNT, 23, "Action::COUNT changed; adjust weights");
        let entries: Vec<(Action, u8)> = Action::iter()
            .enumerate()
            .map(|(index, action)| (action, if index < 8 { 5 } else { 4 }))
            .collect();
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

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        assert_eq!(Action::COUNT, 23, "Action::COUNT changed; adjust weights");
        let entries: Vec<(Action, u8)> = Action::iter()
            .enumerate()
            .map(|(index, action)| (action, if index < 8 { 5 } else { 4 }))
            .collect();
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

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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
            (Action::StoreConsumerOffset, 25),
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

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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
        workload_hash(seed, 1).0
    }

    /// Reply-trace and executor-schedule hashes for a full workload run at
    /// `shards_per_replica` shards. Shared by the single-shard locked
    /// baseline and the multi-shard replay tests.
    fn workload_hash(seed: u64, shards_per_replica: u16) -> (u64, u64) {
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

        let mut sim = Simulator::with_shards(
            replica_count as usize,
            shards_per_replica,
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
            (Action::StoreConsumerOffset, 25),
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
                (h.client, h.request, h.op, h.commit, h.operation as u8).hash(&mut hasher);
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
        (hasher.finish(), sim.schedule_hash())
    }

    /// Assert no shard of any replica dropped an inter-shard frame. Runs
    /// without injected loss must keep the counters at zero; a non-zero
    /// value means an inbox silently shed a frame (undersized capacity or
    /// a routing bug), which would otherwise hide behind VSR retransmit.
    ///
    /// `park_overflow` is deliberately NOT excluded. The simulator never wires
    /// the partition reconciler (`init_partition` mirrors its outcome directly),
    /// so nothing here ever drains the park buffer: a parked frame is never
    /// re-dispatched and never swept. A non-zero `park_overflow` in the simulator
    /// therefore means frames were shed for a namespace that will never
    /// materialise -- which is the very fault class this assert exists to catch,
    /// not the back-pressure it would be in production.
    ///
    /// For the same reason the park buffer must be empty at quiescence: a frame
    /// still parked here has no drainer, so it will neither be delivered nor
    /// answered.
    fn assert_no_frame_drops(sim: &Simulator) {
        for (replica_idx, replica) in sim.replicas.iter().enumerate() {
            for (shard_idx, shard) in replica.shards.iter().enumerate() {
                assert_eq!(
                    shard.metrics().frame_drops_value(),
                    0,
                    "replica {replica_idx} shard {shard_idx} dropped frames without injected loss"
                );
                assert!(
                    shard.parked_namespaces().is_empty(),
                    "replica {replica_idx} shard {shard_idx} left partition frames parked; the \
                     simulator wires no reconciler, so nothing will deliver or answer them"
                );
            }
        }
    }

    /// A partition materialises only on its murmur3 owner shard; every
    /// shard of a replica carries an identical routing row; metadata
    /// consensus/journal state exists only on shard 0.
    #[test]
    fn multi_shard_partition_and_metadata_placement() {
        use consensus::MetadataHandle;
        use shard::shards_table::{ShardsTable, calculate_shard_assignment};

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let shards_per_replica: u16 = 4;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: 3,
            client_count: 1,
            seed: 0x5EED_5AAD,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim =
            Simulator::with_shards(3, shards_per_replica, std::iter::once(1), network_opts);
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);

        let owner = calculate_shard_assignment(&ns, u32::from(shards_per_replica));
        for replica in &sim.replicas {
            for (shard_idx, shard) in replica.shards.iter().enumerate() {
                assert_eq!(
                    shard.plane.partitions().contains(&ns),
                    shard_idx == usize::from(owner),
                    "partition must live exactly on its hash owner (owner={owner})"
                );
                assert_eq!(
                    shard.shards_table().shard_for(ns),
                    Some(owner),
                    "every shard must carry the same routing row"
                );
                assert_eq!(
                    shard.plane.metadata().consensus.is_some(),
                    shard_idx == 0,
                    "metadata consensus must exist only on shard 0"
                );
            }
        }
    }

    /// Single-writer metadata: a peer shard resolves a namespace against shard
    /// 0's metadata through the shared left-right read handle. Before this,
    /// every shard carried an independent writable STM, so a write that reached
    /// only shard 0 (as a metadata consensus commit does) was invisible to
    /// peers and a partition op homing on a peer shard failed to resolve its
    /// namespace. Seeding only shard 0 and reading it back from every peer is
    /// the direct proof of the read-handle propagation.
    #[test]
    fn peer_shard_resolves_namespace_via_shard0_read_handle() {
        use iggy_binary_protocol::WireIdentifier;

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let network_opts = packet::PacketSimulatorOptions {
            node_count: 1,
            client_count: 1,
            seed: 0x5EED_B00C,
            ..packet::PacketSimulatorOptions::default()
        };
        // One replica, four shards: shard 0 writes metadata, shards 1..4 read it.
        let sim = Simulator::with_shards(1, 4, std::iter::once(1), network_opts);

        // Seeds only shard 0's writable STM (see `seed_stream_topic_partition`).
        let ns = IggyNamespace::new(0, 0, 0);
        sim.seed_stream_topic_partition(ns);

        let resolve = |shard: &Rc<Replica>| {
            shard
                .plane
                .metadata()
                .mux_stm
                .streams()
                .namespace_from_partition(
                    &WireIdentifier::numeric(0),
                    &WireIdentifier::numeric(0),
                    0,
                )
        };

        let shards = &sim.replicas[0].shards;
        let writer_resolved = resolve(&shards[0]);
        assert_eq!(
            writer_resolved,
            Some(ns),
            "shard 0 (metadata writer) must resolve the seeded namespace"
        );
        for (shard_idx, peer) in shards.iter().enumerate().skip(1) {
            assert_eq!(
                resolve(peer),
                writer_resolved,
                "peer shard {shard_idx} must resolve via shard 0's shared read handle, \
                 not an independent STM"
            );
        }
    }

    /// View change at 5 replicas x 3 shards: crash the primary replica,
    /// survivors elect a new primary for the partition group, and a
    /// post-change send commits through the mesh.
    #[test]
    fn multi_shard_view_change_after_primary_crash() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 5;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0x5EED_5C0C,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards(
            replica_count as usize,
            3,
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.register_client_with_primary(&client);

        let msg = client.send_messages(ns, &[Bytes::from_static(b"before crash")]);
        sim.submit_request(client_id, 0, msg.into_generic());
        let mut got_reply = false;
        for _ in 0..200 {
            if !sim.step().is_empty() {
                got_reply = true;
                break;
            }
        }
        assert!(got_reply, "expected reply before crash");

        sim.replica_crash(0);
        for _ in 0..800 {
            sim.step();
        }

        let mut new_primary_found = false;
        for replica_idx in 1..replica_count {
            let consensus = sim.replicas[replica_idx as usize]
                .partition_shard(ns)
                .plane
                .partitions()
                .get_by_ns(&ns)
                .expect("partition must exist on every live replica's owner shard")
                .consensus();
            if consensus.view() > 0
                && consensus.status() == Status::Normal
                && consensus.is_primary()
            {
                new_primary_found = true;
            }
        }
        assert!(new_primary_found, "expected a new primary after crash");

        let live = sim.replicas[1].partition_shard(ns);
        let live_consensus = live
            .plane
            .partitions()
            .get_by_ns(&ns)
            .expect("partition must exist on replica 1")
            .consensus();
        let new_primary_idx = live_consensus.primary_index(live_consensus.view());
        let msg2 = client.send_messages(ns, &[Bytes::from_static(b"after view change")]);
        sim.submit_request(client_id, new_primary_idx, msg2.into_generic());
        let mut got_reply_after = false;
        for _ in 0..200 {
            if !sim.step().is_empty() {
                got_reply_after = true;
                break;
            }
        }
        assert!(got_reply_after, "expected reply from new primary");
    }

    /// Multi-shard replay: the same seed reproduces both the reply trace
    /// and the executor schedule; a different seed diverges in both.
    #[test]
    fn multi_shard_replay_is_deterministic() {
        let (replies_a, schedule_a) = workload_hash(0xD0D0_0001, 4);
        let (replies_b, schedule_b) = workload_hash(0xD0D0_0001, 4);
        assert_eq!(replies_a, replies_b, "reply trace diverged at same seed");
        assert_eq!(schedule_a, schedule_b, "schedule diverged at same seed");

        let (replies_c, schedule_c) = workload_hash(0xD0D0_0002, 4);
        assert_ne!(replies_a, replies_c, "different seeds, identical replies");
        assert_ne!(
            schedule_a, schedule_c,
            "different seeds, identical schedule"
        );
    }

    /// Schedule hash for `seed` after stepping the consensus plane with no
    /// client traffic, with the dispatch shell on or off.
    fn consensus_schedule_hash(seed: u64, shell: bool) -> u64 {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let network_opts = packet::PacketSimulatorOptions {
            node_count: 3,
            client_count: 1,
            seed,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = if shell {
            Simulator::with_shards_shell(3, 1, std::iter::once(1u128), network_opts)
        } else {
            Simulator::with_shards(3, 1, std::iter::once(1u128), network_opts)
        };
        for _ in 0..20 {
            sim.step();
        }
        sim.schedule_hash()
    }

    /// Turning the dispatch shell on wires the server's real deferred
    /// handlers on every shard. With no client traffic none of them is
    /// reached, so the consensus plane both replays deterministically and
    /// matches the shell-off schedule: the toggle is genuinely off the
    /// consensus path. Also guards that shell construction does not panic.
    #[test]
    fn shell_on_consensus_schedule_matches_shell_off() {
        let seed = 0x5CED_0001;
        assert_eq!(
            consensus_schedule_hash(seed, true),
            consensus_schedule_hash(seed, true),
            "shell-on schedule diverged at same seed"
        );
        assert_eq!(
            consensus_schedule_hash(seed, true),
            consensus_schedule_hash(seed, false),
            "shell perturbed the consensus schedule despite no client traffic"
        );
        assert_ne!(
            consensus_schedule_hash(0x5CED_0001, true),
            consensus_schedule_hash(0x5CED_0002, true),
            "different seeds produced identical shell-on schedule"
        );
    }

    /// Drive a full dispatch-shell round-trip for `seed`: seed a partition
    /// plus its metadata, log a client in against root, produce one message,
    /// then poll. Returns the poll reply's raw bytes and the schedule hash.
    fn shell_produce_poll(seed: u64) -> (Vec<u8>, u64) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: 3,
            client_count: 1,
            seed,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards_shell(3, 1, std::iter::once(client_id), network_opts);
        let ns = IggyNamespace::new(0, 0, 0);
        sim.init_partition(ns);
        sim.seed_stream_topic_partition(ns);

        let client = SimClient::new(client_id);
        sim.shell_login(&client);

        // Produce through the shell too: `SimClient` now emits the legacy
        // `SendMessagesHeader` wire shape the real SDK sends, so
        // `resolve_partition_request_namespace` decodes it on the
        // `handle_client_request` path. Both the write and the poll below now
        // exercise the real dispatch layer.
        let payload = Bytes::from_static(b"shell-poll-payload");
        let produce = client.send_messages(ns, std::slice::from_ref(&payload));
        sim.submit_request(client_id, 0, produce.into_generic());
        for _ in 0..200 {
            sim.step();
        }

        // Poll through the dispatch shell: on_client_request -> drain ->
        // handle_poll_messages -> partition_read -> on_partition_read, running
        // as a task the executor interleaves with the pump.
        let poll = client.poll_messages(ns, 10);
        sim.submit_request(client_id, 0, poll.into_generic());
        let mut poll_reply = None;
        for _ in 0..200 {
            if let Some(reply) = sim.step().into_iter().next() {
                poll_reply = Some(reply);
                break;
            }
        }
        let poll_reply = poll_reply.expect("shell poll: no reply within 200 steps");
        (poll_reply.as_slice().to_vec(), sim.schedule_hash())
    }

    /// A `SimClient` poll returns the produced messages through the real
    /// dispatch read path (`on_client_request` -> `handle_poll_messages` ->
    /// `partition_read` -> `on_partition_read`), which runs as a task the
    /// executor interleaves with the pump, and the whole login/produce/poll
    /// round-trip replays byte-for-byte under one seed.
    #[test]
    fn shell_poll_returns_produced_messages_deterministically() {
        const PAYLOAD: &[u8] = b"shell-poll-payload";
        let (reply_a, schedule_a) = shell_produce_poll(0x5CED_0011);
        let (reply_b, schedule_b) = shell_produce_poll(0x5CED_0011);
        assert!(
            reply_a
                .windows(PAYLOAD.len())
                .any(|window| window == PAYLOAD),
            "poll reply did not carry the produced payload through the real read handler"
        );
        assert!(
            reply_b
                .windows(PAYLOAD.len())
                .any(|window| window == PAYLOAD),
            "second run's poll reply did not carry the produced payload"
        );
        // The produce stamps a random UUID per message (`random_id::get_uuid`),
        // so the reply bytes differ run-to-run; the executor schedule is seeded
        // and must replay. This mirrors the workload tests, which hash reply
        // headers rather than message bodies for the same reason.
        assert_eq!(
            schedule_a, schedule_b,
            "shell schedule diverged at same seed"
        );
    }

    /// The dispatch shell's reason to exist: detect the PR #3557
    /// async-concurrency class (a partition reference held across an `.await`
    /// while a sibling task mutates the partitions vec). Under the
    /// deterministic executor a parked read with a live borrow IS a
    /// borrow-held-across-a-suspension, so a concurrent `remove` trips the
    /// `#[cfg(debug_assertions)]` borrow tripwire. The correct `with_partition`
    /// read drops the borrow before the suspension, so the same interleaving is
    /// sound. Debug-only: the tripwire (and this detector) compile out in
    /// release, exactly like the `BorrowGuard` they ride on.
    ///
    /// The fault is injected via the synthetic `hold_borrow_across_await`, not
    /// the real read, on purpose: the production read has no borrow-holding
    /// suspension to seed. The partition journal read is synchronous (a pure
    /// memory copy that never awaits) and `with_partition` returns an owned
    /// `PollPlan` before the only awaits (disk read, offset persist) run off the
    /// borrow in `spawn_poll_io`, so the real read is sound by construction.
    ///
    /// TODO: once the simulator models storage faults, the disk-tier read
    /// (`PollPlan::execute` -> `read_disk`) becomes a real, seedable await in the
    /// read path. A regression holding a partition borrow across it, run against
    /// a concurrent reconcile `InsertOwned` reallocation, would then trip this
    /// detector end-to-end through the real handler, retiring the synthetic seam.
    #[cfg(debug_assertions)]
    #[test]
    fn shell_detects_partition_borrow_held_across_await() {
        use crate::executor::DetExecutor;
        use consensus::PartitionsHandle;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let network_opts = packet::PacketSimulatorOptions {
            node_count: 3,
            client_count: 1,
            seed: 0x5CED_0021,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(3, std::iter::once(1u128), network_opts);
        let ns_a = IggyNamespace::new(0, 0, 0);
        let ns_b = IggyNamespace::new(0, 0, 1);
        sim.init_partition(ns_a);
        sim.init_partition(ns_b);

        // BAD read: holds a partition borrow across a suspension. The mutator
        // task runs while it is parked (borrow live) -> the tripwire fires.
        // `catch_unwind` builds the executor inline so unwinding drops the
        // parked read's guard, restoring the borrow count for the next case.
        let tripped = catch_unwind(AssertUnwindSafe(|| {
            let mut executor = DetExecutor::new(7);
            let read = Rc::clone(&sim.replicas[0].shards[0]);
            executor.spawn(async move {
                read.plane
                    .partitions()
                    .hold_borrow_across_await(std::future::pending())
                    .await;
            });
            executor.run_until_stalled(POLL_BUDGET); // borrow acquired; task parks
            let mutate = Rc::clone(&sim.replicas[0].shards[0]);
            executor.spawn(async move {
                mutate.plane.partitions().remove(&ns_b);
            });
            executor.run_until_stalled(POLL_BUDGET); // mutate while borrow live
        }))
        .is_err();
        assert!(
            tripped,
            "borrow-held-across-await went undetected: the concurrent mutation \
             did not trip the #3557 borrow tripwire under the executor"
        );
        // Contiguity: the tripwire fires BEFORE `remove` touches the vec (the
        // assert is its first statement), so the detector aborts the mutation
        // that would have dangled the live borrow. Both partitions survive
        // intact -- the class is caught before it can corrupt state.
        assert!(
            sim.offsets(0, ns_a).is_some() && sim.offsets(0, ns_b).is_some(),
            "tripwire must abort the mutation before it corrupts the partitions vec"
        );

        // REAL read: `with_partition` scopes the borrow, dropping it before the
        // suspension, so the identical interleaving is sound (no tripwire).
        let mut executor = DetExecutor::new(7);
        let read = Rc::clone(&sim.replicas[0].shards[0]);
        executor.spawn(async move {
            let _ = read
                .plane
                .partitions()
                .with_partition(&ns_a, |_partition| ());
            std::future::pending::<()>().await;
        });
        executor.run_until_stalled(POLL_BUDGET);
        let mutate = Rc::clone(&sim.replicas[0].shards[0]);
        executor.spawn(async move {
            mutate.plane.partitions().remove(&ns_b);
        });
        executor.run_until_stalled(POLL_BUDGET);
        // The very mutation the bad read's tripwire aborted now applies
        // cleanly (no borrow was live across the suspension): `ns_b` is gone,
        // `ns_a` intact -- the correct read is sound under the same schedule.
        assert!(
            sim.offsets(0, ns_a).is_some() && sim.offsets(0, ns_b).is_none(),
            "correct with_partition read must leave the concurrent remove sound"
        );
    }

    /// The realloc half of the PR #3557 class, and the stronger one: a pump
    /// `insert` that grows the partitions vec MOVES every element, so a stale
    /// reference to any partition dangles, not just the one a `swap_remove`
    /// displaced. `shell_detects_partition_borrow_held_across_await` covers the
    /// remove; this covers the grow, on the same two-task deterministic
    /// interleave (reconciler-shaped reader parked with a borrow live, pump-shaped
    /// task mutating the container underneath it).
    ///
    /// The buffer address is asserted to have MOVED, so the test cannot pass on a
    /// push into spare capacity, which relocates nothing.
    ///
    /// Debug-only, like the tripwire it drives: `BorrowGuard` compiles out in
    /// release.
    #[cfg(debug_assertions)]
    #[test]
    fn shell_detects_partition_borrow_held_across_a_pump_realloc() {
        use crate::executor::DetExecutor;
        use consensus::PartitionsHandle;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let network_opts = packet::PacketSimulatorOptions {
            node_count: 3,
            client_count: 1,
            seed: 0x5CED_0022,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(3, std::iter::once(1u128), network_opts);
        let ns_a = IggyNamespace::new(0, 0, 0);
        let ns_b = IggyNamespace::new(0, 0, 1);
        // The namespace the pump grows the vec with. Never materialised up front:
        // inserting it IS the mutation under test.
        let ns_grow = IggyNamespace::new(0, 0, 2);
        sim.init_partition(ns_a);
        sim.init_partition(ns_b);

        // BAD read: the borrow is live across the suspension, so the pump's
        // growing insert lands while a stale reference to every partition is
        // outstanding. `catch_unwind` builds the executor inline so unwinding
        // drops the parked read's guard and restores the borrow count.
        let tripped = catch_unwind(AssertUnwindSafe(|| {
            let mut executor = DetExecutor::new(11);
            let read = Rc::clone(&sim.replicas[0].shards[0]);
            executor.spawn(async move {
                read.plane
                    .partitions()
                    .hold_borrow_across_await(std::future::pending())
                    .await;
            });
            executor.run_until_stalled(POLL_BUDGET); // borrow acquired; task parks
            let grow = Rc::clone(&sim.replicas[0].shards[0]);
            executor.spawn(async move {
                grow.init_partition(ns_grow, None, None);
            });
            executor.run_until_stalled(POLL_BUDGET); // grow while the borrow is live
        }))
        .is_err();
        assert!(
            tripped,
            "a pump realloc under a live partition borrow went undetected: the \
             #3557 tripwire did not fire on the growing insert"
        );
        // The tripwire asserts before `push`, so the vec is untouched: the two
        // originals survive and the grow namespace never materialised.
        let partitions = sim.replicas[0].shards[0].plane.partitions();
        assert_eq!(
            partitions.len(),
            2,
            "tripwire must abort the insert before it relocates the vec"
        );
        assert!(!partitions.contains(&ns_grow));

        // REAL read: `with_partition` drops the borrow before the suspension, so
        // the identical schedule is sound and the grow applies.
        let addr_before = partitions.buffer_addr();
        let mut executor = DetExecutor::new(11);
        let read = Rc::clone(&sim.replicas[0].shards[0]);
        executor.spawn(async move {
            let _ = read
                .plane
                .partitions()
                .with_partition(&ns_a, |_partition| ());
            std::future::pending::<()>().await;
        });
        executor.run_until_stalled(POLL_BUDGET);
        let grow = Rc::clone(&sim.replicas[0].shards[0]);
        executor.spawn(async move {
            grow.init_partition(ns_grow, None, None);
        });
        executor.run_until_stalled(POLL_BUDGET);

        let partitions = sim.replicas[0].shards[0].plane.partitions();
        assert!(
            partitions.contains(&ns_grow),
            "correct with_partition read must leave the concurrent insert sound"
        );
        assert_ne!(
            partitions.buffer_addr(),
            addr_before,
            "the insert landed in spare capacity, so nothing moved and this test \
             proves nothing about a realloc; seed more partitions before the grow"
        );
        // Every pre-existing partition is still addressable after the move, which
        // is what a stale reference would have missed.
        assert!(partitions.contains(&ns_a) && partitions.contains(&ns_b));
    }

    /// Committed metadata prepare timestamps for `seed`: register plus two
    /// stream creates, read back from replica 0's metadata journal.
    fn metadata_prepare_timestamps(seed: u64) -> Vec<u64> {
        use consensus::MetadataHandle;
        use journal::{Journal, JournalHandle};

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

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
        sim.register_client_with_primary(&client);

        for name in ["clock-a", "clock-b"] {
            let msg = client.create_stream(name);
            sim.submit_request(client_id, 0, msg.into_generic());
            let mut got_reply = false;
            for _ in 0..100 {
                if !sim.step().is_empty() {
                    got_reply = true;
                    break;
                }
            }
            assert!(got_reply, "create_stream({name}) must commit");
        }

        let shard = &sim.replicas[0].shards[0];
        let journal = shard
            .plane
            .metadata()
            .journal
            .as_ref()
            .expect("shard 0 owns the metadata journal");
        // Ops 1..=3: Register, then the two creates.
        (1..=3)
            .map(|op| {
                journal
                    .handle()
                    .header(op)
                    .expect("committed op must have a journal header")
                    .timestamp
            })
            .collect()
    }

    /// With the injected [`SimClock`], primary-stamped prepare timestamps
    /// are a pure function of the seed: identical across same-seed runs,
    /// anchored at the synthetic sim epoch (not 1970, not wall clock),
    /// and strictly monotonic per the clamp in
    /// `next_monotonic_timestamp`.
    #[test]
    fn prepare_timestamps_replay_with_seed() {
        let first = metadata_prepare_timestamps(0xC10C_0001);
        let second = metadata_prepare_timestamps(0xC10C_0001);
        assert_eq!(
            first, second,
            "prepare timestamps diverged across same-seed runs"
        );
        for timestamp in &first {
            assert!(
                *timestamp >= deps::SIM_EPOCH_MICROS,
                "timestamp {timestamp} predates the sim epoch; wall clock leaked"
            );
            // Sim runs complete in well under a simulated day; a wall-clock
            // leak would stamp 2026-07+ values far past this bound.
            assert!(
                *timestamp < deps::SIM_EPOCH_MICROS + 86_400_000_000,
                "timestamp {timestamp} beyond epoch + 1 day; wall clock leaked"
            );
        }
        assert!(
            first.windows(2).all(|pair| pair[0] < pair[1]),
            "prepare timestamps must be strictly monotonic: {first:?}"
        );
    }

    /// IGGY-66 acceptance: per-partition consensus independence. Block
    /// `ns_a`'s `PrepareOk` acks at the network layer and fill its
    /// pipeline to `PIPELINE_PREPARE_QUEUE_MAX`; a request on `ns_b`
    /// still commits while `ns_a` is wedged (no quorum without backup
    /// acks); lifting the block drains `ns_a` completely.
    #[test]
    fn per_partition_consensus_independence() {
        use consensus::PIPELINE_PREPARE_QUEUE_MAX;
        use iggy_binary_protocol::{Command, PrepareOkHeader};
        use packet::Packet;
        use std::sync::atomic::{AtomicU64, Ordering};

        // The link predicate is a plain fn pointer (no captures), so the
        // namespace under blockade travels through a static. Owned by
        // this test alone; other tests never install drop predicates.
        static BLOCKED_NS: AtomicU64 = AtomicU64::new(0);

        fn drop_blocked_prepare_ok(packet: &Packet) -> bool {
            if packet.message.header().command != Command::PrepareOk {
                return false;
            }
            let header: &PrepareOkHeader = bytemuck::checked::from_bytes(
                &packet.message.as_slice()[..std::mem::size_of::<PrepareOkHeader>()],
            );
            header.group == BLOCKED_NS.load(Ordering::Relaxed)
        }

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0x5EED_0066,
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
        BLOCKED_NS.store(ns_a.inner(), Ordering::Relaxed);

        // Block every backup's PrepareOk for ns_a toward the primary: the
        // primary's self-ack alone is 1 < quorum 2, so ns_a can prepare
        // and replicate but never commit.
        for backup in 1..replica_count {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(backup), ProcessId::Replica(0)) =
                Some(drop_blocked_prepare_ok);
        }

        // Fill ns_a's pipeline exactly to the cap (one more would be
        // rejected at preflight and generate a reply, muddying the
        // no-replies assertion below).
        for sequence in 0..PIPELINE_PREPARE_QUEUE_MAX {
            let msg = client.send_messages(ns_a, &[Bytes::from(format!("wedged-{sequence}"))]);
            sim.submit_request(client_id, 0, msg.into_generic());
        }
        for _ in 0..100 {
            assert!(
                sim.step().is_empty(),
                "ns_a must not commit while its PrepareOk acks are blocked"
            );
        }

        // ns_b shares the client, the replicas, and the shard, but has its
        // own consensus group: it must commit while ns_a stays wedged.
        let msg = client.send_messages(ns_b, &[Bytes::from_static(b"independent")]);
        // Replies no longer carry a group id; correlate by the request id
        // this send was stamped with.
        let ns_b_request = msg.header().request;
        sim.submit_request(client_id, 0, msg.into_generic());
        let mut independent_replies = 0usize;
        for _ in 0..100 {
            for reply in sim.step() {
                assert_eq!(
                    reply.header().request,
                    ns_b_request,
                    "only ns_b may commit while ns_a's acks are blocked"
                );
                independent_replies += 1;
            }
        }
        assert_eq!(
            independent_replies, 1,
            "ns_b request must commit while ns_a's pipeline is full"
        );

        // Lift the blockade: retransmitted acks land and ns_a drains.
        for backup in 1..replica_count {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(backup), ProcessId::Replica(0)) = None;
        }
        let mut drained_replies = 0usize;
        for _ in 0..800 {
            for reply in sim.step() {
                // Only ns_a replies are outstanding once ns_b committed
                // above, so every reply counts toward the drain.
                if reply.header().request != ns_b_request {
                    drained_replies += 1;
                }
            }
            if drained_replies == PIPELINE_PREPARE_QUEUE_MAX {
                break;
            }
        }
        assert_eq!(
            drained_replies, PIPELINE_PREPARE_QUEUE_MAX,
            "every wedged ns_a send must commit once acks flow again"
        );
    }

    /// `SendMessages` workload at 3 replicas x 4 shards drains, converges
    /// against the oracle, and drops no inter-shard frames.
    #[test]
    fn multi_shard_workload_converges() {
        use crate::workload::{
            self, Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
            oracle,
        };

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0xC0FF_EE04,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards(
            replica_count as usize,
            4,
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.register_client_with_primary(&client);

        let mut options = WorkloadOptions::new(0xC0FF_EE04, replica_count, vec![ns]);
        options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let mut wl = Workload::new(options);
        let clients = [client];
        let replies = workload::run(&mut sim, &mut wl, &clients, 2_000, u64::MAX);
        assert!(replies > 0, "workload produced no replies");

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, 5_000),
            "system did not drain within the tick budget"
        );
        oracle::assert_converged(&sim, &wl);
        assert_no_frame_drops(&sim);
    }
}

#[cfg(test)]
mod view_change_data_loss_tests {
    //! A committed, client-acknowledged op must survive a view change even when
    //! the replica that becomes primary is the one missing it.
    //!
    //! Without the sender's log suffix on the `DoViewChange`, the new primary adopts
    //! the winner's op NUMBER, rebuilds its pipeline from its OWN journal, hits the
    //! hole, and truncates the range as "decided lost" -- discarding an op journaled
    //! on a quorum and already replied to. The next client op then reuses the number
    //! and collides with the stale entry on the up-to-date backup.
    //!
    //! The hole here is punched at the commit point, so the assertion that catches a
    //! regression is "the op came back", not "the head did not regress": with nothing
    //! uncommitted there is no pipeline rebuild to truncate. The `dvc_merge` unit
    //! tests cover the sequencer-truncation path directly.

    use super::*;
    use crate::executor::yield_once;
    use consensus::{Sequencer, Status};
    use journal::Journal;
    use message_bus::MessageBus;

    /// Whether a replica's shard-0 metadata consensus is a settled primary in a
    /// view past the one that crashed.
    fn is_new_metadata_primary(sim: &Simulator, replica: u8) -> bool {
        sim.replicas[replica as usize].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .is_some_and(|consensus| {
                consensus.view() > 0
                    && consensus.status() == Status::Normal
                    && consensus.is_primary()
            })
    }

    /// `(head op, commit_max)` of a replica's shard-0 metadata consensus.
    fn metadata_progress(sim: &Simulator, replica: u8) -> (u64, u64) {
        let consensus = sim.replicas[replica as usize].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 owns metadata consensus");
        (
            consensus.sequencer().current_sequence(),
            consensus.commit_max(),
        )
    }

    /// Whether a replica's metadata journal holds `op`.
    fn metadata_holds(sim: &Simulator, replica: u8, op: u64) -> bool {
        let journal = sim.replicas[replica as usize].shards[0]
            .plane
            .metadata()
            .journal
            .as_ref()
            .expect("shard 0 owns the metadata journal");
        let slot = usize::try_from(op).expect("op fits usize");
        Journal::header(journal.as_ref(), slot).is_some()
    }

    /// Drop `op` from a replica's metadata journal, leaving a hole.
    fn metadata_forget(sim: &Simulator, replica: u8, op: u64) -> bool {
        sim.replicas[replica as usize].shards[0]
            .plane
            .metadata()
            .journal
            .as_ref()
            .expect("shard 0 owns the metadata journal")
            .forget_op(op)
    }

    #[test]
    fn given_committed_op_missing_on_next_primary_when_primary_crashes_should_survive_view_change()
    {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        // Commit some metadata ops so there is a log to lose. Registering binds
        // a session, and seeding a stream/topic/partition commits several more.
        sim.register_client_with_primary(&client);
        sim.seed_stream_topic_partition(IggyNamespace::new(1, 1, 0));
        for _ in 0..200 {
            sim.step();
        }

        // Replica 0 is primary for view 0, so replica 1 is primary-elect for view 1
        // (view % replica_count): the replica whose hole decides the outcome.
        let next_primary: u8 = 1;
        let (_, committed) = metadata_progress(&sim, next_primary);
        assert!(
            committed > 0,
            "the test needs committed metadata ops to be able to lose one"
        );

        // Every replica must hold the op: the point is that it IS recoverable, and
        // only the incoming primary lacks it.
        for replica in 0..replica_count {
            assert!(
                metadata_holds(&sim, replica, committed),
                "replica {replica} must hold op {committed} before the hole is punched"
            );
        }

        // Punch the hole: the incoming primary forgets an op its peers still hold.
        assert!(
            metadata_forget(&sim, next_primary, committed),
            "op {committed} must have been present to forget"
        );

        sim.replica_crash(0);
        for _ in 0..1500 {
            sim.step();
        }

        // A primary must emerge among the survivors.
        let primary = (1..replica_count)
            .find(|&replica| is_new_metadata_primary(&sim, replica))
            .expect("a metadata primary must be elected after the old one crashes");

        let (head, commit_max) = metadata_progress(&sim, primary);

        // The committed op must not have been discarded.
        assert!(
            head >= committed,
            "the new primary's head ({head}) regressed below the committed op ({committed}); \
             a committed, acknowledged op was discarded by the view change"
        );
        assert!(
            commit_max >= committed,
            "commit_max ({commit_max}) regressed below the committed op ({committed})"
        );

        // And back in the new primary's journal: the view change repaired the hole
        // from a peer that offered the body, rather than declaring the op lost.
        assert!(
            metadata_holds(&sim, primary, committed),
            "op {committed} must be repaired back into the new primary's journal"
        );
    }

    /// A client submit landing inside the new primary's view-start superblock
    /// persist must not corrupt the pipeline.
    ///
    /// `start_pending_view` flips the replica into a Normal primary
    /// synchronously and defers the rebuild of the inherited uncommitted
    /// suffix; the persist then suspends the pump. A register admitted in that
    /// window used to mint the next op into the still-empty pipeline, and the
    /// deferred rebuild panicked pushing the inherited op beneath it
    /// ("sequence must be sequential"); the same empty pipeline also blinded
    /// the register dedup, admitting an inherited in-flight register twice.
    /// The suspension is real on disk-backed stores (an fsync) and is restored
    /// here with `set_yield_writes`.
    #[test]
    fn given_a_register_inside_the_view_start_persist_when_the_pipeline_rebuilds_should_commit_once()
     {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let settled_client: u128 = 1;
        let straggler_client: u128 = 2;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 2,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            replica_count as usize,
            [settled_client, straggler_client].into_iter(),
            network_opts,
        );

        let client = SimClient::new(settled_client);
        sim.register_client_with_primary(&client);
        for _ in 0..100 {
            sim.step();
        }
        let (baseline_head, baseline_commit) = metadata_progress(&sim, 1);
        assert_eq!(
            baseline_head, baseline_commit,
            "the cluster must be quiescent before the straggler is staged"
        );

        // Stage the inherited suffix: the straggler's register reaches the
        // next primary's journal, then the old primary dies before the commit
        // makes it back.
        let straggler = SimClient::new(straggler_client);
        sim.submit_request(straggler_client, 0, straggler.register().into_generic());
        let mut staged = None;
        for _ in 0..200 {
            sim.step();
            let (head, commit_max) = metadata_progress(&sim, 1);
            if head > baseline_head && commit_max < head {
                staged = Some(head);
                break;
            }
        }
        let staged =
            staged.expect("the register must reach the next primary's journal before it commits");

        // Both survivors' next persists suspend once, opening the window a
        // real fsync has.
        sim.replicas[1].superblock.set_yield_writes();
        sim.replicas[2].superblock.set_yield_writes();
        sim.replica_crash(0);

        // The straggler's retry loop, as the server runs it: `dispatch` spawns
        // the in-process submit on its own task, which is what can interleave
        // with the parked pump. The sim's wire path processes requests inside
        // the pump itself, so the window is only reachable from a spawned
        // task. A plain once-per-step retry is never ready inside the drain
        // where the pump flips to primary and suspends on the persist, so
        // each tick wake spends a small budget of yield-separated attempts:
        // the yields land the retry between the pump's polls, one of which is
        // the suspended view-start persist.
        let registered = std::rc::Rc::new(std::cell::Cell::new(false));
        let submit_shard = std::rc::Rc::clone(&sim.replicas[1].shards[0]);
        let submit_flag = std::rc::Rc::clone(&registered);
        sim.executor.spawn(async move {
            loop {
                for _ in 0..32 {
                    match submit_shard
                        .plane
                        .metadata()
                        .submit_register_in_process(straggler_client, 0)
                        .await
                    {
                        Ok(_) => {
                            submit_flag.set(true);
                            return;
                        }
                        Err(error) if error.is_transient() => yield_once().await,
                        Err(_) => return,
                    }
                }
                submit_shard
                    .bus
                    .sleep(std::time::Duration::from_millis(10))
                    .await;
            }
        });

        for _ in 0..1500 {
            sim.step();
            if registered.get() {
                break;
            }
        }
        assert!(
            registered.get(),
            "the straggler's login must complete after the failover"
        );

        let primary = (1..replica_count)
            .find(|&replica| is_new_metadata_primary(&sim, replica))
            .expect("a metadata primary must be elected after the old one crashes");
        let (_, commit_max) = metadata_progress(&sim, primary);
        assert!(
            commit_max >= staged,
            "the inherited op ({staged}) must commit under the new primary \
             (commit_max = {commit_max})"
        );
    }
}

#[cfg(test)]
mod repair_frontier_tests {
    //! Journal repair moves durable coverage, not the head of the hash chain.
    //!
    //! A replica that rejoins behind the group adopts the primary's head and
    //! then backfills the ops it missed. Those land BELOW that head, so the
    //! pair `(sequencer, last_prepare_checksum)` has to keep describing one
    //! and the same entry: the pair is exactly what the next projected prepare
    //! stamps as `(op, parent)`. Carrying the repaired frame's own checksum
    //! instead rewinds the parent, and the next prepare then chains past the
    //! entry that actually precedes it -- every entry individually well
    //! sealed, the chain broken, and a WAL that refuses to boot as soon as a
    //! rewrite (checkpoint drain or uncommitted-suffix truncation) puts the
    //! two entries side by side.

    use super::*;
    use consensus::Sequencer;
    use journal::Journal;
    use std::collections::BTreeMap;

    /// Replica 0 is primary for view 0, so this one stays a backup for the
    /// whole run: only the repair ingest is under test, not an election.
    const LAGGING: u8 = 1;

    /// Committed ops the lagging replica misses and has to repair back.
    const OPS_MISSED: usize = 20;

    /// Steps allowed for the rejoin, the adoption, and the repair stream.
    const REPAIR_STEPS: usize = 4000;

    /// Highest op the journal probe walks. The workload stays far below it.
    const OP_PROBE_CEILING: u64 = 512;

    /// `(sequencer, last_prepare_checksum)` of a replica's metadata consensus:
    /// the `(op, parent)` its next projected prepare would stamp.
    fn metadata_chain_head(sim: &Simulator, replica: u8) -> (u64, u128) {
        let consensus = sim.replicas[replica as usize].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 owns metadata consensus");
        (
            consensus.sequencer().current_sequence(),
            consensus.last_prepare_checksum(),
        )
    }

    /// Every op a replica's metadata journal holds, with its checksum.
    fn metadata_journal_checksums(sim: &Simulator, replica: u8) -> BTreeMap<u64, u128> {
        let journal = sim.replicas[replica as usize].shards[0]
            .plane
            .metadata()
            .journal
            .as_ref()
            .expect("shard 0 owns the metadata journal");
        (1..=OP_PROBE_CEILING)
            .filter_map(|op| {
                let slot = usize::try_from(op).expect("op fits usize");
                Journal::header(journal.as_ref(), slot).map(|header| (op, header.checksum))
            })
            .collect()
    }

    #[test]
    fn given_repair_below_the_head_when_backfilling_should_not_rewind_the_parent() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
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

        sim.register_client_with_primary(&client);
        for _ in 0..50 {
            sim.step();
        }

        sim.replica_crash(LAGGING);
        for index in 0..OPS_MISSED {
            let msg = client.create_stream(&format!("gap-{index}"));
            sim.submit_request(client_id, 0, msg.into_generic());
            for _ in 0..30 {
                sim.step();
            }
        }

        sim.replica_restart(LAGGING);

        // Everything the restart recovered locally is the baseline; anything
        // that appears from here arrived over the wire.
        let mut journaled = metadata_journal_checksums(&sim, LAGGING);
        let mut backfills_below_head = 0usize;
        for _ in 0..REPAIR_STEPS {
            sim.step();
            let (head, parent) = metadata_chain_head(&sim, LAGGING);
            let current = metadata_journal_checksums(&sim, LAGGING);
            for (&op, &checksum) in &current {
                // A live replicated op IS the head, and the pair moves with it.
                // Only entries that landed below the head are repair backfill.
                if journaled.contains_key(&op) || op >= head {
                    continue;
                }
                backfills_below_head += 1;
                assert_ne!(
                    parent,
                    checksum,
                    "repairing op {op} rewound the parent of the next prepare: the \
                     sequencer sits at op {head} but last_prepare_checksum now \
                     describes op {op}, so the next projected prepare would be op \
                     {} parented past op {head}",
                    head + 1
                );
            }
            journaled = current;
        }

        // The contract itself, not just the absence of a rewind: after the
        // repair stream the pair has to describe one and the same entry, or
        // the next projected prepare parents on something that is not its
        // predecessor.
        let (head, parent) = metadata_chain_head(&sim, LAGGING);
        let journaled = metadata_journal_checksums(&sim, LAGGING);
        assert_eq!(
            journaled.get(&head).copied(),
            Some(parent),
            "the sequencer sits at op {head} but last_prepare_checksum describes \
             op {:?}, so the next projected prepare would parent past op {head}",
            journaled
                .iter()
                .find(|&(_, &checksum)| checksum == parent)
                .map(|(&op, _)| op)
        );

        assert!(
            backfills_below_head > 0,
            "the rejoined replica never repaired an op below its own head, so \
             nothing about the repair frontier was exercised"
        );
    }
}
