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
pub mod seeds;
pub mod workload;

use bus::SimOutbox;
use client::SimClient;
use consensus::{ConsensusClock, MetadataHandle, PartitionsHandle, VsrState};
use deps::SimClock;
use deps::SimSuperblock;
use deps::{MemStorage, SimJournal};
use executor::{DetExecutor, RunOutcome, TaskId};
use iggy_binary_protocol::{Command, GenericHeader, ReplyHeader};
use iggy_common::IggyError;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use metadata::impls::metadata::StreamsFrontend;
use network::Network;
use packet::{PacketSimulatorOptions, ProcessId};
use partitions::{
    Partition, PartitionOffsets, PollFragments, PollingArgs, PollingConsumer,
    RetainedPartitionState,
};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use replica::{Replica, SIM_INBOX_CAPACITY, new_shard};
use seeds::SimSeeds;
use server_common::Message;
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use shard::CONSENSUS_TICK_INTERVAL;
use shard::shards_table::{ShardsTable, calculate_shard_assignment};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;

/// Poll budget per [`DetExecutor::run_until_stalled`]. Pumps are event-driven, so
/// hitting it means a task is spin-waking: a bug, panicked with the seed.
const POLL_BUDGET: u32 = 100_000;

/// Retry interval and total budget for a setup handshake
/// (`register_client_with_primary`, `shell_login`).
///
/// Submit-once dies under injected loss. Both are metadata ops with a stable request
/// id, so the client table dedups the resend.
const SETUP_RETRY_STEPS: u32 = 50;
const SETUP_TOTAL_STEPS: u32 = 4_000;

/// One simulated replica: shards plus the executor bookkeeping to crash it. One
/// entry per shard in `shards` / `pump_tasks`.
pub struct SimReplica {
    /// Shards of this replica, indexed by shard id.
    pub shards: Vec<Rc<Replica>>,
    /// Shard 0's durable superblock. Held here, not in the shard, so its bytes
    /// survive the shard being rebuilt across a restart.
    pub superblock: Rc<SimSuperblock>,
    /// Shard 0's metadata WAL, retained across a restart like `superblock`, so a
    /// rebuilt replica recovers op/commit and committed metadata from its own disk.
    /// Shard 0 owns the only metadata consensus, so this is the only journal.
    pub metadata_journal: Rc<SimJournal<MemStorage>>,
    /// Shard 0's metadata incarnation nonce. Seed-derived, bumped by one per
    /// restart: distinct incarnations, byte-identical replay. See
    /// `VsrConsensus::set_incarnation`.
    pub metadata_incarnation: u128,
    /// One durable superblock per materialised partition group, retained like
    /// `superblock` so a re-materialised group recovers its recorded
    /// `(view, log_view)` instead of re-entering view 0. Storeless, the persist gate
    /// marks every view durable without writing, leaving the gate, its write-failure
    /// fence and view recovery unexercised.
    pub partition_superblocks: RefCell<HashMap<IggyNamespace, Rc<SimSuperblock>>>,
    /// One retained message log per partition group, plus the offsets recovered from
    /// it. A real server's messages are in segment files; the simulator has none, so
    /// without this a rebuilt partition comes back empty and the monotonicity
    /// invariant calls a discarded log a consensus regression. Populated by
    /// [`Simulator::replica_restart`], consumed by `materialise_partition`.
    partition_logs: RefCell<HashMap<IggyNamespace, RetainedPartitionState>>,
    /// This replica's data directory when checkpoints are enabled. Retained so a
    /// restart reads back the snapshot its previous incarnation wrote.
    data_dir: Option<std::path::PathBuf>,
    /// Keeps each pump's stop channel alive. Dropping one ends that pump gracefully,
    /// reserved for shutdown tests; crash uses `DetExecutor::abort`.
    _stop_txs: Vec<shard::Sender<()>>,
    /// Pump task per shard, aborted on crash.
    pump_tasks: Vec<TaskId>,
}

impl SimReplica {
    /// The shard owning `namespace`'s partition data, by the same
    /// deterministic hash the router uses.
    ///
    /// # Panics
    /// If the shard count does not fit `u32`; mesh construction caps it at `u16`.
    #[must_use]
    pub fn partition_shard(&self, namespace: IggyNamespace) -> &Rc<Replica> {
        let shard_count = u32::try_from(self.shards.len()).expect("shard count fits u32");
        let owner = calculate_shard_assignment(&namespace, shard_count);
        &self.shards[usize::from(owner)]
    }
}

/// One replica's view of a partition group's consensus. Read by the quiesce oracle;
/// see [`Simulator::partition_consensus_state`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartitionConsensusState {
    pub status: consensus::Status,
    pub view: u32,
    pub is_primary: bool,
    /// Ops committed in the group. Not `PartitionOffsets::commit_offset`, the
    /// highest durably PERSISTED offset, which counts an uncommitted suffix.
    pub commit_min: u64,
}

pub struct Simulator {
    /// All replicas, indexed by replica id. Always fully populated; crashed replicas
    /// stay alive but are skipped during dispatch.
    pub replicas: Vec<SimReplica>,
    /// Per-replica outbox, indexed by replica id. Shared with consensus via
    /// [`SharedSimOutbox`](bus::SharedSimOutbox).
    pub outboxes: Vec<Rc<SimOutbox>>,
    /// Currently-crashed replica ids. Dispatch and outbox drain skip these.
    pub crashed: HashSet<u8>,
    pub network: Network,
    pub replica_count: u8,
    pub client_ids: Vec<u128>,
    /// Drives every shard pump. Scheduling picks and virtual time both derive from
    /// the seed, so the schedule replays with it.
    executor: DetExecutor,
    /// Picks which shard receives each inbound packet, modelling the coordinator's
    /// connection homing: production round-robins inbound connections, so the shard
    /// receiving a peer's bytes is unrelated to the one owning the target group. Own
    /// stream ([`SimSeeds::entry_shard`]) so one draw per delivered packet cannot
    /// perturb the network or workload traces.
    entry_rng: Xoshiro256PlusPlus,
    /// Network seed, kept for livelock diagnostics.
    seed: u64,
    /// Clients the cluster has evicted since the last drain, in delivery order.
    ///
    /// An eviction ends a session: outstanding requests go unanswered and the client
    /// must log in again. Recorded, not acted on, because re-establishing a session
    /// steps the simulator and drops workload expectations, neither of which belongs
    /// inside packet delivery.
    evicted: Vec<u128>,
    /// Whether a rebuilt partition recovers its consensus frontier from the carried
    /// log. OFF by default: production restores the view alone (`load_partition`), so
    /// a run with it on studies a system more durable than Iggy is. See
    /// `IggyShard::init_partition`.
    restore_partition_frontier: bool,
    /// Replies a setup handshake pulled off the wire that were not its own.
    ///
    /// `await_setup_reply` steps the whole simulator, so it sees every client's
    /// replies. Dropping the rest strands their auditor expectations until a resend
    /// re-commits, so they are parked here and returned by the next [`Self::step`].
    deferred_client_replies: Vec<Message<ReplyHeader>>,
    /// Dispatch-shell mode: inbound client packets go through the real
    /// `on_client_request` handler (see
    /// [`shard::IggyShard::deliver_client_request`]) instead of raw `dispatch`
    /// routing. Set at construction by [`Simulator::with_shards_shell`].
    shell: bool,
}

impl Simulator {
    /// New simulator with per-replica outboxes routed through a [`Network`].
    ///
    /// # Panics
    /// If `clients` yields duplicate `client_id`s. The auditor keys in-flight
    /// entries by `(client_id, request)` and the network indexes routes by
    /// `client_id`; duplicates collide on both.
    pub fn new(
        replica_count: usize,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
    ) -> Self {
        Self::with_shards(replica_count, 1, clients, network_options)
    }

    /// [`Simulator::new`] with `shards_per_replica` shards per replica, meshed as
    /// the server bootstrap does: metadata plane on shard 0, partitions
    /// hash-assigned, one pump task per shard.
    ///
    /// # Panics
    /// On duplicate `client_id`s (see [`Simulator::new`]) or
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

    /// [`Simulator::with_shards`] with the dispatch shell on: every shard wires the
    /// server's real dispatch handlers, so a client request runs as a task the seeded
    /// executor interleaves with the pump. Off, the default, keeps the raw
    /// `on_message` fast path.
    ///
    /// # Panics
    /// On duplicate `client_id`s or `shards_per_replica == 0`.
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

    /// [`Simulator::new`] with checkpoints enabled, each replica rooted at
    /// `<data_dir_root>/replica-N`.
    ///
    /// A data directory arms the metadata `SnapshotCoordinator`; without one
    /// `checkpoint_if_needed` returns immediately and nothing produces the snapshot a
    /// state transfer serves.
    ///
    /// Opt-in, and separate from the other constructors, because the coordinator
    /// persists through `std::fs`: a harness that touches nothing outside memory
    /// should not start writing files by omission. Writes are synchronous and never
    /// touch the executor, so replay stays deterministic; the caller owns the
    /// directory's lifetime.
    ///
    /// Pair with [`Simulator::set_metadata_journal_slots`]: a checkpoint is forced by
    /// the journal running low on slots, and it is unbounded until told otherwise.
    ///
    /// # Panics
    /// On duplicate `client_id`s, or if the per-replica directories cannot be
    /// created.
    pub fn with_checkpoints(
        replica_count: usize,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
        shell: bool,
        data_dir_root: &std::path::Path,
    ) -> Self {
        Self::build_inner(
            replica_count,
            1,
            clients,
            network_options,
            shell,
            Some(data_dir_root),
        )
    }

    /// Bound every replica's metadata journal to `slots`, so filling it forces a
    /// checkpoint. See [`deps::SimJournal::set_slot_count`].
    pub fn set_metadata_journal_slots(&self, slots: usize) {
        for replica in &self.replicas {
            replica.metadata_journal.set_slot_count(slots);
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    fn build(
        replica_count: usize,
        shards_per_replica: u16,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
        shell: bool,
    ) -> Self {
        Self::build_inner(
            replica_count,
            shards_per_replica,
            clients,
            network_options,
            shell,
            None,
        )
    }

    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    fn build_inner(
        replica_count: usize,
        shards_per_replica: u16,
        clients: impl Iterator<Item = u128>,
        network_options: PacketSimulatorOptions,
        shell: bool,
        data_dir_root: Option<&std::path::Path>,
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
        // One virtual clock for every consensus group, so prepare timestamps are a
        // pure function of the seed rather than the wall clock.
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
            // Harness-owned so the superblock's VSR state and the metadata WAL
            // outlive a restart, which drops and rebuilds the shards.
            let superblock = Rc::new(SimSuperblock::default());
            let metadata_journal = Rc::new(SimJournal::<MemStorage>::default());
            // Spread across the 128-bit space per replica so `replica_restart`'s
            // increments never collide across replicas. Non-zero.
            let metadata_incarnation = 1 + (u128::from(id) << 64);
            // Created up front: the snapshot coordinator writes into
            // `<dir>/metadata/` and does not create it. Retained on `SimReplica` so a
            // restart reads back the snapshot its previous incarnation wrote.
            let replica_data_dir = data_dir_root.as_ref().map(|root| {
                let dir = root.join(format!("replica-{i}"));
                std::fs::create_dir_all(dir.join(metadata::impls::METADATA_DIR))
                    .expect("simulator data directory is creatable");
                dir
            });

            // One crossfire mesh per replica. Every shard clones the canonical
            // senders vec and exclusively takes its inbox.
            let (senders, mut inboxes, mut reply_inboxes) = shard::shard_mesh_channels(
                shards_per_replica,
                SIM_INBOX_CAPACITY,
                SIM_INBOX_CAPACITY,
            );

            let mut shards = Vec::with_capacity(usize::from(shards_per_replica));
            let mut stop_txs = Vec::with_capacity(usize::from(shards_per_replica));
            let mut pump_tasks = Vec::with_capacity(usize::from(shards_per_replica));
            // Single-writer metadata, as the server bootstrap does: shard 0 builds
            // the writable STM and mints a factory bundle, every peer rebuilds a
            // reader-mode mirror from it and reads committed metadata through the
            // shared handle. Built in index order, so shard 0's bundle exists first.
            let mut metadata_bundle: Option<replica::SimMetadataBundle> = None;
            for shard_idx in 0..shards_per_replica {
                let inbox = inboxes[usize::from(shard_idx)]
                    .take()
                    .expect("mesh yields exactly one inbox per shard");
                let reply_inbox = reply_inboxes[usize::from(shard_idx)]
                    .take()
                    .expect("mesh yields exactly one reply inbox per shard");
                // Shard 0 owns metadata consensus, so only it carries the
                // superblock. Peers persist nothing.
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
                    (shard_idx == 0).then(|| replica_data_dir.clone()).flatten(),
                    // Fresh boot: `init_partition` seeds later, before any workload.
                    &[],
                );
                if shard_idx == 0 {
                    metadata_bundle = Some(
                        writer_bundle
                            .expect("shard 0 returns the metadata factory bundle for peers"),
                    );
                }

                // Server-bootstrap wiring: one pump task per shard, stopped only by
                // the held stop channel or a crash abort.
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
                partition_logs: RefCell::new(HashMap::new()),
                data_dir: replica_data_dir,
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
            entry_rng: Xoshiro256PlusPlus::seed_from_u64(SimSeeds::derive(seed).entry_shard),
            evicted: Vec::new(),
            restore_partition_frontier: false,
            deferred_client_replies: Vec::new(),
            seed,
            shell,
        }
    }

    /// Init a partition with its own consensus group on every live replica.
    ///
    /// The reconciler's outcome without running it: the namespace is committed to
    /// metadata, the partition materialises only on its hash-owning shard, and every
    /// shard gets the routing row stamped with the committed `created_revision`.
    /// Production seeds rows through `ReconcileOp::{InsertOwned,InsertRouted}`.
    ///
    /// # Panics
    /// If a replica's shard count does not fit `u32`; mesh construction caps it at
    /// `u16`.
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
            materialise_partition(replica, namespace, self.restore_partition_frontier);
        }
    }

    /// Seed the metadata `Streams` STM on each live replica's shard-0 writer, so a
    /// poll's `resolve_partition_namespace` succeeds for `namespace`, which the
    /// partition-plane-only [`Self::init_partition`] does not populate.
    ///
    /// Shard 0 only: peers observe the seed through the shared left-right read
    /// handle, and seeding a reader-mode peer STM directly would panic. The
    /// reconciler is unwired here, so this bypasses it exactly as `init_partition`
    /// does for the partition plane. Pair the two for the same namespace on the
    /// dispatch-shell poll path.
    ///
    #[allow(clippy::cast_possible_truncation)]
    pub fn seed_stream_topic_partition(&self, namespace: IggyNamespace) {
        for (i, replica) in self.replicas.iter().enumerate() {
            if self.crashed.contains(&(i as u8)) {
                continue;
            }
            // Shard 0 is the sole metadata writer. Peers see the seed through the
            // left-right publish, so seeding a reader-mode peer STM would panic.
            replica.shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .seed_namespace(namespace, namespace.inner());
        }
    }

    /// Log `client` in against the deterministic root user through the dispatch
    /// shell: the real `on_client_request` path verifies the seeded root credentials
    /// and runs the consensus `Register`, then this binds the assigned session.
    /// Needs the shell on and the root user seeded (see [`new_shard`]); targets the
    /// primary, as [`Self::register_client_with_primary`] does.
    ///
    /// # Panics
    /// If no login reply arrives within `SETUP_TOTAL_STEPS`, or it carries no
    /// session.
    pub fn shell_login(&mut self, client: &SimClient) {
        self.shell_login_via(client, 0);
    }

    /// [`Self::shell_login`] against a chosen replica.
    ///
    /// Dialing a BACKUP is the only way to reach register forwarding: the backup
    /// verifies the credentials itself and sends only the consensus proposal on as
    /// `ForwardRegister`, parking the login until the matching
    /// `ForwardRegisterResult` returns, then answering on the connection it owns. A
    /// client that always dials the primary never produces those frames.
    ///
    /// # Panics
    /// If no login reply arrives within `SETUP_TOTAL_STEPS`, or it carries no
    /// session.
    pub fn shell_login_via(&mut self, client: &SimClient, target: u8) {
        // Connection metadata on every replica, as `install_client_fd` does in
        // production. `ensure_transport_connection` reads it to admit the connection
        // into the SessionManager, which the login's bind
        // (Connected, Authenticated, Bound) requires.
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        for outbox in &self.outboxes {
            outbox.insert_client_meta(ClientConnMeta::new(
                client.client_id(),
                addr,
                ClientTransportKind::Tcp,
            ));
        }

        // From here this client talks the real client protocol. See
        // `SimClient::shell_wire`.
        client.set_shell_wire();
        let msg = client
            .login(replica::SHELL_ROOT_USERNAME, replica::SHELL_ROOT_PASSWORD)
            .into_generic();
        // `build_reply_with_body` maps the session field to `op`.
        let session = self
            .await_setup_reply(
                client.client_id(),
                target,
                &msg,
                iggy_binary_protocol::Operation::Register,
                "shell_login",
            )
            .header()
            .op;
        assert!(session > 0, "shell_login: login reply carried no session");
        client.bind_session(session);
    }

    /// Submit `message` to `target` and step until a client reply arrives,
    /// resubmitting every [`SETUP_RETRY_STEPS`] steps.
    ///
    /// Resubmitted verbatim, so the request id is stable. Not free, though:
    /// `register_preflight` dispatches every `Register` past its gates, even one
    /// whose client already holds an entry, because a bind is a fencing event and
    /// absorbing it would hand back an un-bumped epoch. Each landed retry therefore
    /// commits another register and fences the previous holder, recoverable in one
    /// round trip but a reason to keep the interval well above it.
    ///
    /// # Panics
    /// If no reply arrives within `SETUP_TOTAL_STEPS`. A handshake that never
    /// completes leaves the fixture unusable, so there is no useful `None`.
    fn await_setup_reply(
        &mut self,
        client_id: u128,
        target: u8,
        message: &Message<GenericHeader>,
        expected_operation: iggy_binary_protocol::Operation,
        label: &str,
    ) -> Message<ReplyHeader> {
        let mut target = target;
        for step in 0..SETUP_TOTAL_STEPS {
            if step % SETUP_RETRY_STEPS == 0 {
                self.submit_request(client_id, target, message.deep_copy());
                // Rotate, as the workload's resend path does. Retrying one replica
                // forever suffices on a perfect network and is useless once it is
                // partitioned or crashed mid-handshake: a register has to reach the
                // metadata primary, and this one may be neither reachable nor able
                // to forward.
                target = (target + 1) % self.replica_count.max(1);
            }
            let mut answer = None;
            for reply in self.step() {
                let header = reply.header();
                // Correlate. Stepping the whole simulator surfaces every client's
                // replies, and taking the first hands a two-client run another
                // client's committed op as this answer, whose `op` is then bound as
                // a session: a fenced client that evicts, recovers and repeats.
                //
                // Nor is a result-framed transport rejection an answer. Dispatch
                // refused to place the request (not primary, transferring, queue
                // full), stamped `op` with its commit point instead of a session and
                // left `status` at 0, so it reads as a completed handshake and hands
                // back a session the cluster never granted, silently, whenever that
                // commit point is nonzero.
                if answer.is_none()
                    && header.client == client_id
                    && header.operation == expected_operation
                    && !setup_reply_is_transient(&reply)
                {
                    answer = Some(reply);
                    continue;
                }
                self.deferred_client_replies.push(reply);
            }
            if let Some(reply) = answer {
                return reply;
            }
        }
        panic!(
            "{label}: no reply for client {client_id} within {SETUP_TOTAL_STEPS} steps \
             (seed {:#x})",
            self.seed,
        );
    }

    /// Advance the simulation by one tick. Returns client replies delivered.
    ///
    /// Every shard runs its real message pump as an executor task. A step fires the
    /// virtual consensus tick, runs the pumps to quiescence, feeds network packets
    /// into the shard routers, runs the pumps over the resulting frames, then
    /// exchanges outboxes with the network. Pump interleaving is a seeded executor
    /// pick, and everything a step produces lands on the wire before network time
    /// advances.
    ///
    /// # Panics
    /// If a client-addressed packet does not decode as `ReplyHeader`, or a pump
    /// livelocks (poll budget exhausted).
    #[allow(clippy::cast_possible_truncation)]
    pub fn step(&mut self) -> Vec<Message<ReplyHeader>> {
        // Ahead of this step's own traffic, so a driver sees replies in delivery
        // order rather than the order a handshake happened to interrupt.
        let mut client_replies: Vec<Message<ReplyHeader>> =
            std::mem::take(&mut self.deferred_client_replies);

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
                        // Seeded homing: the receiving shard is usually NOT the
                        // owner, so the frame takes the real router hop (dispatch,
                        // mesh, owning pump), as when the coordinator homes a peer
                        // connection on an arbitrary shard.
                        let entry = self.entry_rng.random_range(0..replica.shards.len());
                        match packet.from {
                            // Shell mode: client requests enter through the real
                            // `on_client_request` handler, as the client-fd listener
                            // does in production, and drain as a task the executor
                            // interleaves with the pump. Partition writes carry the
                            // legacy `SendMessages` shape the real SDK sends, so
                            // `resolve_partition_request_namespace` decodes them
                            // here. Replica-sourced consensus frames route raw.
                            ProcessId::Client(client_id) if self.shell => {
                                replica.shards[entry]
                                    .deliver_client_request(client_id, packet.message.deep_copy());
                            }
                            _ => replica.shards[entry].dispatch(packet.message.deep_copy()),
                        }
                    }
                    // Crashed or missing: packet silently dropped.
                }
                ProcessId::Client(client_id) => {
                    // Not every client-addressed frame is a reply. `Eviction` tells
                    // a client its session is gone, sent once the client table drops
                    // it, which the dispatch shell reaches as soon as replicas crash
                    // and restart. Decoding it as a reply fails on the command
                    // discriminant, so classify first and record it for the driver.
                    if packet.message.header().command == Command::Eviction {
                        self.evicted.push(client_id);
                        continue;
                    }
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

    /// Stamp a metadata snapshot watermark on one replica, standing in for a
    /// checkpoint that superseded everything at or below `op`.
    ///
    /// Without a data directory there is no `SnapshotCoordinator`, so
    /// `checkpoint_if_needed` returns immediately and the watermark stays at zero.
    /// Nothing is evictable, the repair server has no compacted prefix to skip, and
    /// `RangeEvicted`, the only signal converting a repair into a state transfer,
    /// cannot occur however the cluster is driven.
    ///
    /// Stamping the number alone reaches the whole escalation without snapshot bytes
    /// to transfer, so the protocol path is coverable without the coordinator seam a
    /// real installing transfer needs.
    ///
    /// # Panics
    /// If `replica_idx` is out of range.
    pub fn stamp_metadata_snapshot(&self, replica_idx: usize, op: u64) {
        use journal::Journal;

        self.replicas[replica_idx]
            .metadata_journal
            .set_snapshot_op(op);
    }

    /// Submit this client's handshake to `target` WITHOUT stepping.
    ///
    /// The blocking helpers ([`Self::shell_login_via`],
    /// [`Self::register_client_with_primary`]) are for fixture setup, before a driver
    /// exists. Mid-run they step the simulator up to `SETUP_TOTAL_STEPS` times inside
    /// the driver's tick, with no `Workload::tick`, no fault injection and no
    /// invariant check, which is what `run_with_faults` is for. A driver recovering an
    /// evicted client submits here and picks the reply up from its normal loop.
    ///
    /// Sends the handshake this simulator's mode can answer: a login on the shell, a
    /// bare register on the raw path.
    pub fn submit_handshake(&mut self, client: &SimClient, target: u8) {
        let message = if self.shell {
            client.set_shell_wire();
            client.login(replica::SHELL_ROOT_USERNAME, replica::SHELL_ROOT_PASSWORD)
        } else {
            client.register()
        };
        self.submit_request(client.client_id(), target, message.into_generic());
    }

    /// The session a handshake reply carries, or `None` if it carries none. Not the
    /// caller's business because the field differs by path: the shell's login answers
    /// in `op`, the raw register in `commit`.
    #[must_use]
    pub fn handshake_session(&self, reply: &Message<ReplyHeader>) -> Option<u64> {
        if setup_reply_is_transient(reply) {
            return None;
        }
        let header = reply.header();
        let session = if self.shell { header.op } else { header.commit };
        (session > 0).then_some(session)
    }

    /// Have a rebuilt partition recover `(sequencer, commit, checksum)` from the log
    /// this harness carried across the restart.
    ///
    /// Off by default, deliberately: the partition journal is in-memory and segments
    /// carry no op numbers, so a real replica cannot do this and instead boots
    /// quorum-invisible and asks the view's primary. Turn it on only to look past the
    /// empty-frontier restart, which trips `advance_commit_min`'s sequential-advance
    /// assert, at something later in the run; the run then tests a durability
    /// guarantee production does not offer.
    pub const fn set_restore_partition_frontier(&mut self, restore: bool) {
        self.restore_partition_frontier = restore;
    }

    /// Take the clients evicted since the last call.
    ///
    /// A driver must consume these: the session is gone, so outstanding requests are
    /// unanswerable and the next one is refused until the client logs in again.
    /// Ignoring them looks exactly like a wedge.
    pub fn take_evictions(&mut self) -> Vec<u128> {
        std::mem::take(&mut self.evicted)
    }

    /// Rolling hash of the executor schedule: every poll and timer fire. Two runs
    /// from the same seed and inputs must agree; determinism tests assert on it
    /// alongside the reply-trace hash.
    #[must_use]
    pub const fn schedule_hash(&self) -> u64 {
        self.executor.schedule_hash()
    }

    /// Run the executor until every pump is parked again.
    ///
    /// # Panics
    /// On budget exhaustion: pumps are event-driven, so it means a spin-waking task,
    /// a livelock bug, reproducible from the seed. Also on a lost wakeup, a
    /// non-crashed pump quiescing with a non-empty inbox (see
    /// [`Self::assert_inboxes_drained`]).
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

    /// Lost-wake tripwire. At executor quiescence every live pump must have drained
    /// its inbox; a non-empty one on a non-crashed replica means a frame reached the
    /// channel without waking the target pump. Every pump holds a standing
    /// `CONSENSUS_TICK_INTERVAL` timer, so the next `advance_time` would re-poll and
    /// silently drain it, masking the exact wake-loss class this harness exists to
    /// catch. Trip here instead.
    ///
    /// Incomplete by construction: only catches a lost wakeup while the un-woken
    /// frame is still queued at quiescence, since a later frame that does wake the
    /// pump drains the whole inbox. Safe in direction, though: a non-empty inbox at
    /// true quiescence is always a real lost wake, so it never false-trips.
    ///
    /// Crashed replicas are skipped: their pump tasks are aborted, so a stranded
    /// frame has no drainer and is expected.
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

    /// Submit a client request into the simulated network: a client opening a TCP
    /// connection and sending a message to a replica.
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

    /// Whether client requests go through the real dispatch shell. A driver has to
    /// know: the paths do not share a handshake (`Register` versus a login that mints
    /// a session), so re-establishing a client mid-run has to pick the served one.
    #[must_use]
    pub const fn is_shell(&self) -> bool {
        self.shell
    }

    /// Register a client via the primary (replica 0): sends `Register` through the
    /// metadata plane and binds the assigned session on `SimClient`.
    ///
    /// # Panics
    /// If no reply arrives within `SETUP_TOTAL_STEPS`.
    pub fn register_client_with_primary(&mut self, client: &SimClient) {
        self.register_client_via(client, 0);
    }

    /// [`Self::register_client_with_primary`] against a chosen replica, the raw
    /// counterpart of [`Self::shell_login_via`]. A driver re-registering an evicted
    /// client picks a live replica, and the primary may be the one whose restart
    /// caused the eviction.
    ///
    /// # Panics
    /// If no reply arrives within `SETUP_TOTAL_STEPS`.
    #[allow(clippy::cast_possible_truncation)]
    pub fn register_client_via(&mut self, client: &SimClient, target: u8) {
        let msg = client.register().into_generic();
        let reply = self.await_setup_reply(
            client.client_id(),
            target,
            &msg,
            iggy_binary_protocol::Operation::Register,
            "register_client_with_primary",
        );
        let header = reply.header();
        debug_assert_eq!(
            header.operation,
            iggy_binary_protocol::Operation::Register,
            "register_client_with_primary: first reply was not Register"
        );
        assert_eq!(
            header.client,
            client.client_id(),
            "register_client_with_primary: reply client_id mismatch (expected {}, got {})",
            client.client_id(),
            header.client,
        );
        client.bind_session(header.commit);

        // Partitions have no `client_table`: at-least-once, no per-client dedup, so
        // consumers dedup on message id, content or producer-id+seq. Sessions,
        // dedup and eviction live on metadata only.
    }

    /// Crash a replica: abort its pump tasks, disable its network links, discard its
    /// outbox. The object stays alive but receives nothing; a following
    /// [`Self::replica_restart`] drops and rebuilds it from the durable superblock,
    /// which is where volatile state is actually lost and recovered.
    ///
    /// # Panics
    /// If the replica is already crashed.
    pub fn replica_crash(&mut self, replica_index: u8) {
        assert!(
            !self.crashed.contains(&replica_index),
            "cannot crash replica {replica_index}: already down"
        );

        // Hard stop: futures drop mid-await, destructors cancel their channel and
        // timer registrations, and the graceful inbox drain never runs. A crash, not
        // a shutdown.
        for task in &self.replicas[replica_index as usize].pump_tasks {
            self.executor.abort(*task);
        }

        // Detached dispatch tasks this replica's bus spawned (off-pump poll IO,
        // request drains), so a crash leaves none running against a dead replica.
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
    /// state as a real restart does, and rebuild against the retained superblock,
    /// recovering `(view, log_view)` from disk as production's
    /// `restore_metadata_consensus` does. Superblock and outbox are harness-owned and
    /// survive the drop; a fresh mesh and pump tasks are wired and the network
    /// re-enabled.
    ///
    /// # Panics
    /// If the replica is not crashed, or its shard count does not fit `u16`; mesh
    /// construction caps it.
    pub fn replica_restart(&mut self, replica_index: u8) {
        assert!(
            self.crashed.contains(&replica_index),
            "cannot restart replica {replica_index}: not crashed"
        );
        let idx = replica_index as usize;
        let shards_per_replica =
            u16::try_from(self.replicas[idx].shards.len()).expect("shard count fits u16");
        let superblock = Rc::clone(&self.replicas[idx].superblock);
        // The metadata WAL is harness-owned too, so the rebuilt shard 0 recovers
        // op/commit and committed state from it rather than from an empty journal.
        let metadata_journal = Rc::clone(&self.replicas[idx].metadata_journal);
        // Bumped so an in-flight StartView addressed to the previous incarnation is
        // ignored. Deterministic, so replay stays byte-identical.
        let metadata_incarnation = self.replicas[idx].metadata_incarnation + 1;
        // Partition superblocks carry forward too: a re-materialised group must
        // recover its recorded view from the same store, as a rebooted server
        // partition reads the record in its directory.
        let partition_superblocks =
            std::mem::take(&mut *self.replicas[idx].partition_superblocks.borrow_mut());
        // Take each live partition's log while its shard still stands: a real
        // server's messages are in segment files its boot recovers the offset counter
        // from, so rebuilding with nothing would model total data loss rather than a
        // restart. Before the rebuild, which drops the shards.
        let partition_logs = self.retain_partition_logs(idx, &partition_superblocks);
        // SORTED: seed order decides slab ids and `HashMap` order is per-process, so
        // an unsorted walk would stop replay being byte-identical. Also drives the
        // re-materialisation loop below, which must agree with it.
        let mut materialised: Vec<IggyNamespace> = partition_superblocks.keys().copied().collect();
        materialised.sort_unstable_by_key(IggyNamespace::inner);

        // Durable VSR state from the retained superblock, before the rebuild, as
        // production reads it in `restore_metadata_consensus`.
        let recovered_state = superblock
            .read_latest_sync()
            .and_then(|bytes| VsrState::try_from(bytes.as_slice()).ok());

        // Carried like the WAL and superblocks: the rebuilt replica has to find the
        // snapshot its previous incarnation persisted.
        let replica_data_dir = self.replicas[idx].data_dir.clone();

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
                (shard_idx == 0).then(|| replica_data_dir.clone()).flatten(),
                &materialised,
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
        // state as a real restart does. The harness-owned superblock carries over.
        self.replicas[idx] = SimReplica {
            shards,
            superblock,
            metadata_journal,
            metadata_incarnation,
            partition_superblocks: RefCell::new(partition_superblocks),
            partition_logs: RefCell::new(partition_logs),
            data_dir: replica_data_dir,
            _stop_txs: stop_txs,
            pump_tasks,
        };

        // Re-materialise every group this replica had before the crash, as a
        // rebooted server re-opens every partition directory it owns. This is what
        // makes the carried-forward superblock load-bearing: the group recovers its
        // recorded `(view, log_view)` instead of re-entering view 0. The metadata half
        // of the seed already ran inside `new_shard`, ahead of the replay.
        for namespace in materialised {
            materialise_partition(
                &self.replicas[idx],
                namespace,
                self.restore_partition_frontier,
            );
        }

        // Reconnect to the network and mark the replica live again.
        self.network
            .process_enable(ProcessId::Replica(replica_index));
        self.crashed.remove(&replica_index);
    }

    /// Take the message log out of every materialised partition, with the offsets
    /// recovered from it.
    ///
    /// Called while the outgoing shards are still alive, the last point the data can
    /// be read. `std::mem::take` leaves an empty log behind, which nothing observes:
    /// that shard is dropped moments later.
    ///
    /// Keyed off `partition_superblocks`, already the record of which groups this
    /// replica materialised and what the restart re-materialises from.
    fn retain_partition_logs(
        &self,
        replica_idx: usize,
        materialised: &HashMap<IggyNamespace, Rc<SimSuperblock>>,
    ) -> HashMap<IggyNamespace, RetainedPartitionState> {
        let replica = &self.replicas[replica_idx];
        let mut retained = HashMap::with_capacity(materialised.len());
        for &namespace in materialised.keys() {
            let partitions = replica.partition_shard(namespace).plane.partitions();
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                continue;
            };
            let offsets = partition.offsets();
            // `offset_space_used` off the RETIRING partition: an untouched one and
            // one holding a single message at offset 0 both report `(0, 0)`, and only
            // this instance still knows which it is.
            retained.insert(
                namespace,
                RetainedPartitionState {
                    log: std::mem::take(&mut partition.log),
                    durable_offset: offsets.commit_offset,
                    write_offset: offsets.write_offset,
                    offset_space_used: partition.offset_space_used(),
                },
            );
        }
        retained
    }

    /// Advance consensus timeouts on every live replica without a full step cycle:
    /// fire the pumps' virtual tick timers, run the executor to quiescence.
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
        //
        // The one `block_on` allowed to stay, and only because `plan.execute()`
        // cannot suspend here: the sim's partitions are in-memory (no
        // `partition_dir`), so the plan serves the resident journal tier with no disk
        // IO and no `bus.sleep`. A suspending await would fail two ways. On the
        // virtual clock it would hang this thread forever, the clock advancing only
        // through `advance_time`, which does not run during `block_on`. On the retry
        // path it would panic on the compio timer outside a compio runtime. Safe only
        // because it runs between `run_pumps` calls, with the executor quiescent and
        // no pump holding the partition commit lock in a suspended frame.
        let Some(plan) = shard
            .plane
            .partitions()
            .build_poll_snapshot(&namespace, consumer, args)
        else {
            return Err(IggyError::ResourceNotFound(format!(
                "partition not found for namespace {namespace:?} on replica {replica_idx}"
            )));
        };
        // Partitions are driven directly, so a poll's auto-commit is never
        // replicated (the serving shard's job in the real server). Offset discarded.
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

    /// Consensus view for a replica's partition-plane group, or `None` if that
    /// replica does not host the namespace.
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

    /// One replica's view of a partition group's consensus, or `None` when it does
    /// not host the namespace. Read by the quiesce oracle to decide whether a group
    /// has settled into one view, which its leader-relative checks depend on once
    /// partition primaries can be crashed.
    #[must_use]
    pub(crate) fn partition_consensus_state(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
    ) -> Option<PartitionConsensusState> {
        let shard = self.replicas[replica_idx].partition_shard(namespace);
        let partition = shard.plane.partitions().get_by_ns(&namespace)?;
        let consensus = partition.consensus();
        Some(PartitionConsensusState {
            status: consensus.status(),
            view: consensus.view(),
            is_primary: consensus.is_primary(),
            commit_min: consensus.commit_min(),
        })
    }

    /// Index of the current primary for `namespace`, as seen by the first live
    /// replica hosting it, or `None` if no live replica hosts it.
    ///
    /// Reads one replica's view, so it assumes live replicas agree on the primary.
    /// That no longer holds by construction: `spare_primary` is off under
    /// `--crash-primary`, so a crash-triggered view change can run mid-run and this
    /// may name a stale or crashed primary. Callers needing a real answer run
    /// `workload::oracle::settle_to_stable_view` first.
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
/// Shared by [`SimCluster::init_partition`] and the restart path: a rebooted server
/// re-opens every partition directory it owns, so the sim must re-materialise too,
/// else the superblock a restart carries forward is never read back and the
/// recovered-view branch is dead code.
fn materialise_partition(replica: &SimReplica, namespace: IggyNamespace, restore_frontier: bool) {
    let shard_count = u32::try_from(replica.shards.len()).expect("shard count fits u32");
    let owner = calculate_shard_assignment(&namespace, shard_count);
    // Commit the namespace first: a partition the metadata plane never heard of is
    // a shape production cannot produce, and the shard refuses client traffic whose
    // routing-row epoch it cannot match against a committed `created_revision`.
    let streams = replica.shards[0].plane.metadata().mux_stm.streams();
    streams.seed_namespace(namespace, namespace.inner());
    // No committed revision means the seed could not re-add the namespace, which
    // happens once a metadata workload has deleted its stream or topic: the seed's
    // `CreatePartitions` is then a committed REJECTION rather than an error, so it
    // reports nothing. Skip the group rather than build a partition no committed
    // metadata names, as a rebooted server does not re-open a deleted partition's
    // directory either. Before the build, so a skipped group leaves neither a
    // partition nor a routing row behind.
    let Some(epoch) = streams.created_revision_for_namespace(namespace) else {
        return;
    };
    // One store per group, minted on first materialisation and reused after, so the
    // recorded view survives a replica restart.
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
    // Hand back the log this group left behind, if it was materialised here before.
    // Removed rather than cloned: the rebuilt partition becomes its sole owner, and
    // a second materialisation with no restart between would otherwise resurrect a
    // log the live partition has moved past.
    let retained = replica.partition_logs.borrow_mut().remove(&namespace);
    replica.shards[usize::from(owner)].init_partition(
        namespace,
        Some(superblock),
        recovered_state,
        retained,
        restore_frontier,
    );
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

    /// A replica that advanced its view, persisted it through the superblock gate,
    /// then crashed recovers that view from its own disk, not a fresh 0. The
    /// split-brain guarantee: a replica never forgets a view it acted in. Impossible
    /// before the superblock, a rebuilt consensus starting at view 0.
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

        // Zero traffic, so every WAL is empty and the recovered view came from the
        // superblock alone. Pinned, since it is what makes the gate below
        // load-bearing.
        assert!(
            sim.replicas[primary as usize]
                .metadata_journal
                .last_op()
                .is_none(),
            "no metadata traffic in this test, so the restarted replica's WAL must be empty"
        );

        // A recovered view is a prior life even with an empty WAL, so the replica
        // rejoins as a probing backup. Still primary-by-index for that view, so
        // resuming primaryship would have it act as primary in a view the survivors
        // may already have left, with no probe to correct it.
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
    fn given_superblock_write_fails_when_primary_crashes_should_withhold_votes_and_not_elect() {
        // The gate under test: a view-scoped send (SVC, DVC, StartView) must not go
        // out until the new view is durable. Failing one survivor's superblock writes
        // makes its persist gate return false, advancing its view in-memory while
        // withholding every view-scoped send. Quorum 2 of 3, so crashing the primary
        // leaves one working survivor whose lone vote cannot reach quorum and NO new
        // primary is elected. Sending before persisting would instead elect a primary
        // with no durable record of the new view, reintroducing split-brain on its
        // restart. Positive control:
        // `given_advanced_view_when_metadata_replica_restarts_...`, same crash with
        // healthy superblocks.
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

        // Replica 1 survives and is primary-by-index for view 1. Failing its
        // superblock keeps it from durably taking any new view, so it never emits a
        // view-scoped vote.
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

        // Bounded retry. Without a backoff the 10 ms consensus tick would run a full
        // `atomic_replace` (create, write, fsync, rename, dir fsync) every tick for as
        // long as the disk stays broken, on the executor that serves partition
        // traffic too.
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

    /// At-least-once failover: a `SendMessages` retry on a new primary re-executes.
    /// The retry reply carries a HIGHER `commit` op, proof of re-execution rather
    /// than dedup, and the duplicate payload lives at two offsets. Consumers dedup
    /// if they want at-most-once-per-payload.
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

        // Replay the SAME request to the new primary. No dedup, so re-execution.
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
        // `ActionWeights`. Drifts on any change to reply shape, partition commit
        // values, or PRNG draw order. Draw order is sensitive to `pick_outcome`, so
        // adding an outcome to an op sampled in this window, or bumping a weight,
        // shifts the trace. Expect re-locks until error discriminants and reply bodies
        // stabilize the wire format.
        //
        // Re-locked for: METADATA_GROUP (1<<63) replacing the sim-only 0, moving both
        // reply headers and the `replica_id ^ namespace` jitter seed; replies dropping
        // the group id from the client wire; the v1 consumer-offset ops being removed,
        // shifting `Action` discriminants and draw order; those ops drawing the WIRE
        // consumer kind (1 / 2) instead of a bare boolean, kind 0 being no
        // `WireConsumer` discriminant so every such request was dropped unparsed (see
        // `ops::sample_consumer_kind`); and per-stream seeds moving from XOR salts to
        // [`SimSeeds`] alongside `Xoshiro256Plus` becoming `Xoshiro256PlusPlus`, which
        // together remap every stream.
        assert_eq!(
            h1, 0x1376_D480_4A3F_E6A9,
            "workload reply hash drifted from locked baseline"
        );
    }

    /// After a mixed metadata and partition run drains, the shadow's predicted
    /// streams equal the metadata committed on the leader.
    ///
    /// Interleaves creates, deletes and partition sends from one client. A send
    /// between two metadata ops once consumed a metadata request number, gapping the
    /// next create; `SimClient`'s per-plane numbering keeps the metadata sequence
    /// contiguous, so the mix drains and the auditor never misattributes a partition
    /// reply to a metadata entry. Compares
    /// against the leader only: a quorum-excluded backup has no idle catch-up, so
    /// full cross-replica equality stays deferred (see [`oracle`]).
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
        // The send between two metadata ops is the case that previously wedged the
        // next create. Sends do not touch the metadata entity sets, so the oracle
        // below still compares stream state cleanly.
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
        oracle::assert_converged(&sim, &mut wl);
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

        // Partition-only by design, isolating partition-offset convergence from
        // metadata lost to a crashed primary; the entity oracle runs in the no-crash
        // test. One follower crashed with quorum slack (5 replicas, floor 4, quorum
        // 3), so commits still reach quorum and the run drains.
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
        oracle::assert_converged(&sim, &mut wl);
    }

    /// A lossy network drains, because the client resends.
    ///
    /// Without [`workload::Workload::due_resends`] this wedges immediately and
    /// permanently: one in-flight slot per client, nothing times out, so the first
    /// dropped request or reply strands that slot for the run. At 5% loss a 3000-tick
    /// run drained a handful of replies and stopped, and `drive_to_quiesce` could
    /// never finish, waiting on a reply the network had discarded.
    ///
    /// Asserts the resend path ran rather than the seed getting lucky.
    #[test]
    fn packet_loss_resends_and_drains() {
        use crate::workload::{
            self, Workload,
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
        let seed = 0x105_5A1A;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            packet_loss_probability: 0.05,
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

        let mut options = WorkloadOptions::new(seed, replica_count, vec![ns_a]);
        options.weights = ActionWeights::partition_only();
        let mut wl = Workload::new(options);

        let clients = [client];
        let replies = workload::run(&mut sim, &mut wl, &clients, 3_000, u64::MAX);
        assert!(replies > 0, "lossy workload produced no replies");
        assert!(
            wl.resends() > 0,
            "no request timed out at 5% packet loss, so the resend path never ran; \
             raise the loss rate or lower request_timeout_ticks"
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, 20_000),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
        oracle::assert_converged(&sim, &mut wl);
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

        // Inline driver: hash each reply tuple, so divergence is caught at the first
        // non-matching reply rather than in end-of-run aggregates. The cap sits inside
        // the per-reply loop so a multi-reply tick at `replies_seen=49` cannot leak a
        // 50th into the hash.
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

    /// No shard of any replica dropped an inter-shard frame. Runs without injected
    /// loss must keep the counters at zero; non-zero means an inbox silently shed a
    /// frame, from undersized capacity or a routing bug, which would otherwise hide
    /// behind VSR retransmit.
    ///
    /// `park_overflow` is deliberately NOT excluded. The reconciler is unwired here
    /// (`init_partition` mirrors its outcome directly), so nothing drains the park
    /// buffer and a parked frame is never re-dispatched or swept. Non-zero
    /// `park_overflow` therefore means frames shed for a namespace that will never
    /// materialise, which is the fault class this catches rather than the
    /// back-pressure it would be in production.
    ///
    /// Same reason the park buffer must be empty at quiescence: a frame still parked
    /// has no drainer, so it is neither delivered nor answered.
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

        // Produce through the shell too: `SimClient` emits the legacy
        // `SendMessagesHeader` shape the real SDK sends, so
        // `resolve_partition_request_namespace` decodes it on the
        // `handle_client_request` path. Write and poll both hit real dispatch.
        let payload = Bytes::from_static(b"shell-poll-payload");
        let produce = client.send_messages(ns, std::slice::from_ref(&payload));
        sim.submit_request(client_id, 0, produce.into_generic());
        for _ in 0..200 {
            sim.step();
        }

        // Poll through the dispatch shell (`on_client_request`, drain,
        // `handle_poll_messages`, `partition_read`, `on_partition_read`), running as a
        // task the executor interleaves with the pump.
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

    /// A `SimClient` poll returns the produced messages through the real dispatch
    /// read path (`on_client_request`, `handle_poll_messages`, `partition_read`,
    /// `on_partition_read`), running as a task the executor interleaves with the pump,
    /// and the whole login/produce/poll round-trip replays byte-for-byte on one seed.
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
        // The produce stamps a random UUID per message (`random_id::get_uuid`), so
        // reply bytes differ run to run while the seeded executor schedule must
        // replay. Same reason the workload tests hash reply headers, not bodies.
        assert_eq!(
            schedule_a, schedule_b,
            "shell schedule diverged at same seed"
        );
    }

    /// The dispatch shell's reason to exist: detect the PR #3557 async-concurrency
    /// class, a partition reference held across an `.await` while a sibling task
    /// mutates the partitions vec. Under the deterministic executor a parked read with
    /// a live borrow IS a borrow held across a suspension, so a concurrent `remove`
    /// trips the `#[cfg(debug_assertions)]` tripwire. The correct `with_partition`
    /// read drops the borrow first, so the same interleaving is sound. Debug-only, as
    /// is the `BorrowGuard` it rides on.
    ///
    /// Injected through the synthetic `hold_borrow_across_await` rather than the real
    /// read, because the production read has no borrow-holding suspension to seed: the
    /// journal read is a synchronous memory copy, and `with_partition` returns an owned
    /// `PollPlan` before the only awaits (disk read, offset persist) run off the borrow
    /// in `spawn_poll_io`.
    ///
    /// TODO: once storage faults are modelled, the disk-tier read
    /// (`PollPlan::execute`, `read_disk`) becomes a real seedable await in the read
    /// path. A regression holding a borrow across it, against a concurrent reconcile
    /// `InsertOwned` reallocation, would trip this through the real handler and retire
    /// the synthetic seam.
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

        // BAD read: holds a partition borrow across a suspension. The mutator runs
        // while it is parked with the borrow live, so the tripwire fires.
        // `catch_unwind` builds the executor inline, so unwinding drops the parked
        // read's guard and restores the borrow count for the next case.
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
        // The tripwire fires BEFORE `remove` touches the vec, its assert being the
        // first statement, so the detector aborts the mutation that would have
        // dangled the live borrow. Both partitions survive intact: the class is caught
        // before it can corrupt state.
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
        // The mutation the bad read's tripwire aborted now applies cleanly, no borrow
        // being live across the suspension: `ns_b` gone, `ns_a` intact, so the correct
        // read is sound under the same schedule.
        assert!(
            sim.offsets(0, ns_a).is_some() && sim.offsets(0, ns_b).is_none(),
            "correct with_partition read must leave the concurrent remove sound"
        );
    }

    /// The realloc half of the PR #3557 class, and the stronger one: a pump `insert`
    /// that grows the partitions vec MOVES every element, so a stale reference to ANY
    /// partition dangles, not just the one a `swap_remove` displaced.
    /// `shell_detects_partition_borrow_held_across_await` covers the remove; this
    /// covers the grow, on the same two-task interleave (reader parked with a live
    /// borrow, pump-shaped task mutating the container under it).
    ///
    /// The buffer address is asserted to have MOVED, so this cannot pass on a push
    /// into spare capacity, which relocates nothing. Debug-only, like the
    /// `BorrowGuard` it drives.
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

        // BAD read: the borrow is live across the suspension, so the pump's growing
        // insert lands while a stale reference to every partition is outstanding.
        // `catch_unwind` builds the executor inline, so unwinding drops the parked
        // read's guard and restores the borrow count.
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
                grow.init_partition(ns_grow, None, None, None, false);
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
            grow.init_partition(ns_grow, None, None, None, false);
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

    /// Committed stream names on one replica, read out of the committed (left)
    /// buffer so an uncommitted write is invisible.
    fn committed_stream_names(
        sim: &Simulator,
        replica_idx: usize,
    ) -> std::collections::BTreeSet<String> {
        use metadata::impls::metadata::StreamsFrontend;
        sim.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| {
                inner
                    .items
                    .iter()
                    .map(|(_, stream)| stream.name.to_string())
                    .collect()
            })
    }

    /// How much of the WAL a checkpoint reclaimed. Non-zero is the precondition every
    /// checkpoint-recovery test needs: at zero the WAL still holds everything.
    fn snapshot_floor(sim: &Simulator, replica_idx: usize) -> u64 {
        use journal::Journal;
        sim.replicas[replica_idx].metadata_journal.snapshot_op()
    }

    /// A client's committed request watermark on one replica.
    fn client_watermark(sim: &Simulator, replica_idx: usize, client_id: u128) -> Option<u64> {
        sim.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .client_table
            .borrow()
            .get_watermark(client_id)
    }

    /// A checkpointing cluster with `streams` committed streams behind it, named
    /// `wl-{prefix}-N`. The returned `TempDir` keeps the snapshots alive.
    fn checkpointing_cluster(
        replicas: u8,
        seed: u64,
        prefix: &str,
        streams: u32,
    ) -> (Simulator, u128, tempfile::TempDir) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let root = tempfile::tempdir().expect("temp dir for the simulator's snapshots");
        let client_id: u128 = 1;
        let mut sim = Simulator::with_checkpoints(
            usize::from(replicas),
            std::iter::once(client_id),
            packet::PacketSimulatorOptions {
                node_count: replicas,
                client_count: 1,
                seed,
                ..packet::PacketSimulatorOptions::default()
            },
            false,
            root.path(),
        );
        // Small enough that the ops below cross the coordinator's margin.
        sim.set_metadata_journal_slots(80);

        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);
        for sequence in 0..streams {
            let msg = client.create_stream(&format!("wl-{prefix}-{sequence}"));
            sim.submit_request(client_id, 0, msg.into_generic());
            for _ in 0..40 {
                sim.step();
            }
        }
        (sim, client_id, root)
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

    /// A full fault run replays byte-identically from its seed.
    ///
    /// The other determinism tests drive the workload inline, so nothing covered
    /// `run_with_faults`, `FaultInjector` or `resubmit_due`: an injector drawing from
    /// `rand::random` passed the whole suite. Crash and restart counts are compared
    /// alongside the traces because a trace can match while the faults behind it
    /// differ.
    #[test]
    fn fault_runs_replay_from_their_seed() {
        use crate::workload::{
            FaultInjector, Workload,
            actions::Action,
            options::{ActionWeights, WorkloadOptions},
            run_with_faults,
        };

        // `(replies, schedule_hash, crashes, restarts)` for one run.
        fn fault_run(seed: u64) -> (u64, u64, u64, u64) {
            let replica_count: u8 = 3;
            let client_id: u128 = 1;
            let mut sim = Simulator::new(
                usize::from(replica_count),
                std::iter::once(client_id),
                packet::PacketSimulatorOptions {
                    node_count: replica_count,
                    client_count: 1,
                    seed,
                    packet_loss_probability: 0.02,
                    ..packet::PacketSimulatorOptions::default()
                },
            );
            let client = SimClient::new(client_id);
            let ns = server_common::sharding::IggyNamespace::new(1, 1, 0);
            sim.init_partition(ns);
            sim.register_client_with_primary(&client);

            let mut options = WorkloadOptions::new(seed, replica_count, vec![ns]);
            options.crash_per_tick_ratio = 0.02;
            options.restart_per_tick_ratio = 0.08;
            options.spare_primary = false;
            options.weights = ActionWeights::new(&[
                (Action::CreateStream, 30),
                (Action::SendMessages, 50),
                (Action::StoreConsumerOffset, 20),
            ]);
            let mut workload = Workload::new(options);
            let mut injector = FaultInjector::new(seed, replica_count);
            let clients = [client];
            let replies = run_with_faults(
                &mut sim,
                &mut workload,
                &clients,
                3_000,
                u64::MAX,
                &mut injector,
            );
            (
                replies,
                sim.schedule_hash(),
                injector.crashes(),
                injector.restarts(),
            )
        }

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let first = fault_run(0xFA17_0001);
        assert_eq!(first, fault_run(0xFA17_0001), "a fault run did not replay");
        assert!(first.0 > 0, "the run produced no replies");
        assert!(
            first.2 > 0 && first.3 > 0,
            "no crash or restart was injected, so this compares a fault-free run: \
             got {} crashes and {} restarts",
            first.2,
            first.3,
        );
        assert_ne!(
            first,
            fault_run(0xFA17_0002),
            "two seeds produced the same trace, so the seed is not reaching the \
             injector"
        );
    }

    /// Committed streams as `(slab id, name)` on one replica.
    fn committed_stream_slabs(sim: &Simulator, replica_idx: usize) -> Vec<(usize, String)> {
        use metadata::impls::metadata::StreamsFrontend;
        sim.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| {
                inner
                    .items
                    .iter()
                    .map(|(id, stream)| (id, stream.name.to_string()))
                    .collect()
            })
    }

    /// A restarted replica assigns the same slab ids as a peer that never restarted.
    ///
    /// `CreateStream::apply` takes `vacant_key()`, so slab ids follow insertion
    /// order, and a restart used to seed the fillers after replaying the WAL rather
    /// than before. The committed log matched either way, so nothing comparing
    /// headers noticed, but partition ops address a namespace by slab id.
    #[test]
    fn a_restarted_replica_keeps_its_peers_slab_ids() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 1,
                seed: 0xC4E0_0005,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let client = SimClient::new(client_id);
        let ns = server_common::sharding::IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.register_client_with_primary(&client);

        for sequence in 0..4u32 {
            let msg = client.create_stream(&format!("wl-slab-{sequence}"));
            sim.submit_request(client_id, 0, msg.into_generic());
            for _ in 0..40 {
                sim.step();
            }
        }

        let healthy = committed_stream_slabs(&sim, 0);
        assert!(
            healthy.iter().any(|(_, name)| name.starts_with("wl-")),
            "no workload stream committed, so the slab order below is only the seed"
        );

        let rejoining = 1u8;
        sim.replica_crash(rejoining);
        sim.replica_restart(rejoining);
        for _ in 0..2_000 {
            sim.step();
        }

        assert_eq!(
            committed_stream_slabs(&sim, usize::from(rejoining)),
            healthy,
            "the restarted replica assigned different slab ids than a peer holding \
             the same committed log, so a namespace names different streams on each"
        );
    }

    /// A solo replica that checkpointed recovers the state the checkpoint absorbed.
    ///
    /// The case with no second opinion: a clustered replica repairs a botched local
    /// recovery from a peer, so what boot reconstructs here IS the state. The client
    /// table is asserted too, being folded in separately by `persist_snapshot`.
    #[test]
    fn solo_replica_recovers_the_state_its_checkpoint_absorbed() {
        let (mut sim, client_id, _root) = checkpointing_cluster(1, 0xC4E0_0002, "solo", 40);

        let before = committed_stream_names(&sim, 0);
        let watermark_before = client_watermark(&sim, 0, client_id);
        assert!(
            snapshot_floor(&sim, 0) > 0,
            "the solo replica never checkpointed, so this proves nothing about \
             snapshot recovery"
        );

        sim.replica_crash(0);
        sim.replica_restart(0);
        for _ in 0..2_000 {
            sim.step();
        }

        assert_eq!(
            committed_stream_names(&sim, 0),
            before,
            "the restarted solo replica lost committed streams the checkpoint \
             drained out of the WAL"
        );
        assert_eq!(
            client_watermark(&sim, 0, client_id),
            watermark_before,
            "the checkpoint's folded client table did not come back, so a session \
             below the snapshot floor lost its watermark"
        );
    }

    /// A clustered replica that took its own checkpoint rejoins holding the same
    /// committed metadata as a healthy peer.
    ///
    /// Against a peer, not a recorded snapshot of itself: a replica that dropped the
    /// drained prefix still reports a plausible commit point, and only the peer
    /// comparison shows the state behind it is wrong.
    #[test]
    fn a_checkpointed_replica_rejoins_agreeing_with_a_healthy_peer() {
        let (mut sim, _client_id, _root) = checkpointing_cluster(3, 0xC4E0_0003, "peer", 40);

        // A backup, so the restart does not also trigger a view change: the subject
        // here is local recovery, not election.
        let rejoining = 1u8;
        assert!(
            snapshot_floor(&sim, usize::from(rejoining)) > 0,
            "replica {rejoining} never checkpointed, so its restart exercises no \
             snapshot recovery"
        );
        let healthy = committed_stream_names(&sim, 0);
        assert!(
            healthy.len() > 1,
            "the peer holds no workload streams to compare"
        );

        sim.replica_crash(rejoining);
        sim.replica_restart(rejoining);
        for _ in 0..8_000 {
            sim.step();
        }

        assert_eq!(
            committed_stream_names(&sim, usize::from(rejoining)),
            healthy,
            "the rejoined replica disagrees with a healthy peer on committed \
             metadata: local recovery dropped the prefix its checkpoint drained"
        );
    }

    /// A namespace still live in committed metadata but hosted by nobody is a
    /// convergence FAILURE, not a converged cluster.
    ///
    /// Settlement and the leader-relative offset check both skip such a namespace,
    /// which is right for a deleted stream and used to be the only word on the
    /// subject. Seeds the metadata half without the partition half, the state a total
    /// loss of instances leaves.
    #[test]
    #[should_panic(expected = "no live replica hosts it at quiesce")]
    fn a_live_namespace_with_no_host_fails_convergence() {
        use crate::workload::{Workload, options::WorkloadOptions, oracle};

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let seed = 0xC4E0_0004;
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 1,
                seed,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let client = SimClient::new(client_id);
        let ns = server_common::sharding::IggyNamespace::new(1, 1, 0);
        // Metadata only: the namespace is committed-visible, but no replica ever
        // materialises the group (`init_partition` is deliberately not called).
        sim.seed_stream_topic_partition(ns);
        sim.register_client_with_primary(&client);
        for _ in 0..200 {
            sim.step();
        }

        let mut workload = Workload::new(WorkloadOptions::new(seed, replica_count, vec![ns]));
        oracle::assert_converged(&sim, &mut workload);
    }

    /// A replica that checkpoints serves a real state transfer: the rejoining peer
    /// fetches the snapshot in chunks rather than stalling at the handshake.
    ///
    /// Companion to
    /// [`repair_below_the_snapshot_floor_escalates_to_state_transfer`], which stamps a
    /// watermark without snapshot bytes and so reaches only `StateTransferTarget`.
    /// Here the coordinator is armed with a data directory and the journal is bounded,
    /// so the cluster checkpoints for real. Both were needed and neither was present:
    /// no directory meant no `SnapshotCoordinator`, and an unbounded journal never
    /// fired `should_checkpoint`.
    #[test]
    fn checkpointing_cluster_serves_a_chunked_state_transfer() {
        use iggy_binary_protocol::Command;

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let root = tempfile::tempdir().expect("temp dir for the simulator's snapshots");
        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0xC4E0_0001,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_checkpoints(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
            false,
            root.path(),
        );
        // Small enough that the ops below cross the margin; the coordinator forces
        // a checkpoint once free slots fall to its margin (64 by default).
        sim.set_metadata_journal_slots(80);

        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);

        let lagging = 2u8;
        sim.replica_crash(lagging);

        // Commit past the checkpoint margin while the lagging replica is down, so
        // the survivors checkpoint and compact the prefix it is missing.
        for sequence in 0..40u32 {
            let msg = client.create_stream(&format!("wl-checkpoint-{sequence}"));
            sim.submit_request(client_id, 0, msg.into_generic());
            for _ in 0..40 {
                sim.step();
            }
        }

        let snapshot = root
            .path()
            .join("replica-0")
            .join(metadata::impls::METADATA_DIR)
            .join(metadata::impls::SNAPSHOT_FILE_NAME);
        assert!(
            snapshot.exists(),
            "the primary never checkpointed, so there is no snapshot to transfer: \
             raise the op count or lower the journal slot count"
        );

        sim.replica_restart(lagging);
        for _ in 0..8_000 {
            sim.step();
        }

        assert!(
            sim.network.delivered_any(Command::RequestStateChunk),
            "the rejoining replica never asked for a chunk, so the transfer \
             stalled at the handshake exactly as it does without a checkpoint"
        );
        assert!(
            sim.network.delivered_any(Command::StateChunk),
            "no chunk was served: the peer offered a transfer it could not fulfil"
        );

        // Traffic is not recovery: everything above passes when the chunks arrive and
        // the install then fails. Each assertion below closes one of those ways.
        let recovered = &sim.replicas[usize::from(lagging)].shards[0];
        let consensus = recovered
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 owns metadata consensus");
        assert_eq!(
            consensus.status(),
            consensus::Status::Normal,
            "the rejoining replica never returned to Normal"
        );
        let transferred_floor = snapshot_floor(&sim, 0);
        assert!(
            transferred_floor > 0,
            "the serving peer has no snapshot to transfer"
        );
        assert!(
            consensus.commit_min() >= transferred_floor,
            "the rejoining replica commits through {} but was served a snapshot \
             covering {transferred_floor}: the install did not land",
            consensus.commit_min(),
        );
        assert!(
            consensus.commit_max() >= consensus.recovery_barrier(),
            "a recovery barrier at {} still gates the rejoining replica (commit_max {})",
            consensus.recovery_barrier(),
            consensus.commit_max(),
        );

        assert_eq!(
            committed_stream_names(&sim, usize::from(lagging)),
            committed_stream_names(&sim, 0),
            "the transfer left the rejoining replica holding different committed \
             metadata than the peer that served it"
        );
        assert_eq!(
            client_watermark(&sim, usize::from(lagging), client_id),
            client_watermark(&sim, 0, client_id),
            "the transferred client table did not install: the session below the \
             snapshot floor came back without its watermark"
        );
    }

    /// A client that dials a BACKUP still gets a working session, and the login
    /// travels as a forwarded consensus proposal rather than a redirect.
    ///
    /// Register forwarding exists so a client need not find the primary itself: the
    /// backup verifies the credentials locally, sends only the proposal on as
    /// `ForwardRegister`, parks the login until `ForwardRegisterResult` returns, then
    /// answers on the connection it owns. The subsystem landed with no deterministic
    /// coverage and could have none while the harness only dialed the primary.
    ///
    /// Asserts the four frames were delivered rather than inferring them from a working
    /// session: dialing a backup would also "work" under a silent redirect, which is
    /// the design this replaced.
    #[test]
    fn login_via_backup_forwards_the_register_to_the_primary() {
        use iggy_binary_protocol::Command;

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
            seed: 0xF02D_0001,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards_shell(
            usize::from(replica_count),
            1,
            std::iter::once(client_id),
            network_opts,
        );
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.seed_stream_topic_partition(ns);

        // Replica 0 leads both planes at view 0, so replica 1 is a backup and the
        // login has to be forwarded.
        let client = SimClient::new(client_id);
        sim.shell_login_via(&client, 1);

        assert!(
            sim.network.delivered_any(Command::ForwardRegister),
            "no ForwardRegister crossed the wire: the backup answered the login \
             itself, so this covers nothing"
        );
        assert!(
            sim.network.delivered_any(Command::ForwardRegisterResult),
            "the forwarded register was never answered, so the login below \
             succeeded by some other route"
        );

        // Log out on the SAME backup: covers the other half of forwarding and proves
        // the session was real, only a bound session being torn down, with the
        // teardown replicating through the primary as the register did. A logout, not
        // a data request, because the session belongs to the connection and a backup
        // refuses a partition write for routing reasons (`TransientNotAccepted`) that
        // say nothing about the session.
        let logout = client.logout();
        let request = logout.header().request;
        sim.submit_request(client_id, 1, logout.into_generic());
        let mut answered = false;
        for _ in 0..400 {
            if let Some(reply) = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == request)
            {
                assert_eq!(
                    reply.header().status,
                    0,
                    "logout on the forwarded session was refused (status {})",
                    reply.header().status,
                );
                answered = true;
                break;
            }
        }
        assert!(answered, "no reply to the logout issued on the backup");
        assert!(
            sim.network.delivered_any(Command::ForwardLogout),
            "the backup committed the logout without asking the primary"
        );
        assert!(
            sim.network.delivered_any(Command::ForwardLogoutResult),
            "the forwarded logout was never answered"
        );
    }

    /// The workload drains and converges when every request goes through the
    /// server's real dispatch handlers rather than the raw `on_message` path.
    ///
    /// Its own test because the shell path is where authorization, session binding and
    /// the pre-commit deny replies live; the raw path has no deny site. Running the
    /// workload here is the only thing that exercises them, and it surfaced that the
    /// workload had never modelled a denial: nonzero `ReplyHeader::status` means an
    /// empty body, and the decoder read a result section off it and called the reply
    /// corrupt.
    ///
    /// Asserts denials were observed, so this cannot pass on a path where nothing is
    /// denied.
    #[test]
    fn shell_workload_drains_and_converges() {
        use crate::workload::{
            self, Workload,
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
        let seed = 0x5E11_0001;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards_shell(
            usize::from(replica_count),
            1,
            std::iter::once(client_id),
            network_opts,
        );
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        // The dispatch path resolves a partition request's namespace against
        // committed metadata, so the stream and topic have to exist too.
        sim.seed_stream_topic_partition(ns);

        let client = SimClient::new(client_id);
        // Log in rather than bare-register: dispatch admits a request only from a
        // bound session.
        sim.shell_login(&client);

        let mut options = WorkloadOptions::new(seed, replica_count, vec![ns]);
        options.weights = ActionWeights::uniform();
        let mut wl = Workload::new(options);

        let clients = [client];
        let replies = workload::run(&mut sim, &mut wl, &clients, 4_000, u64::MAX);
        assert!(replies > 0, "shell workload produced no replies");

        let stats = wl.auditor.stats();
        assert!(
            stats.commits_per_action.iter().sum::<u64>() > 0,
            "shell workload committed nothing, so the dispatch path never got past \
             admission"
        );
        assert!(
            stats.denials > 0,
            "no request was denied, so the pre-commit deny path this test exists to \
             cover never ran"
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, 20_000),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
        oracle::assert_converged(&sim, &mut wl);
    }

    /// A result-framed transport rejection is not a committed result.
    ///
    /// Dispatch refuses a request it cannot place (not the primary, transferring,
    /// queue full, a view change canceled the pending prepare) and answers with the
    /// reason in the reply's RESULT section, under the request's own operation, leaving
    /// `status` at 0. That reaches `on_reply` shaped exactly like a commit while
    /// carrying a code no op's result enum declares, so the classifier called it a
    /// server bug and every fault-injected shell run died on the first one.
    ///
    /// `TransientNotCommitted` also leaves the outcome UNKNOWN, so the request is held
    /// outstanding for a replay, and the quiesce below proves that replay settles
    /// rather than stalling the drain.
    ///
    /// Asserts a transient was seen, so this cannot pass on a path that never produces
    /// one.
    #[test]
    fn shell_workload_survives_result_framed_transient_rejections() {
        use crate::workload::{
            FaultInjector, Workload,
            options::{ActionWeights, WorkloadOptions},
            oracle, run_with_faults,
        };
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        // Hand-picked: of seeds 1..=40 under these options, ten reached a transient.
        // Crashing the primary under a lossy network is necessary but not sufficient,
        // so the seed cannot be arbitrary. Re-scan if the PRNG streams are remapped.
        let seed = 2;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            packet_loss_probability: 0.10,
            replay_probability: 0.03,
            one_way_delay_mean: 8,
            partition_probability: 0.02,
            unpartition_probability: 0.02,
            partition_stability: 50,
            unpartition_stability: 50,
            partition_mode: packet::PartitionMode::UniformSize,
            partition_symmetry: packet::PartitionSymmetry::Asymmetric,
            path_clog_probability: 0.01,
            path_clog_duration_mean: 25,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards_shell(
            usize::from(replica_count),
            1,
            std::iter::once(client_id),
            network_opts,
        );
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.seed_stream_topic_partition(ns);

        let client = SimClient::new(client_id);
        sim.shell_login(&client);

        let mut options = WorkloadOptions::new(seed, replica_count, vec![ns]);
        options.weights = ActionWeights::uniform();
        // Crashing the primary is what puts a live request on a replica that
        // cannot place it, which is where the transient comes from.
        options.crash_per_tick_ratio = 0.05;
        options.restart_per_tick_ratio = 0.08;
        options.spare_primary = false;
        let mut wl = Workload::new(options);

        let clients = [client];
        let mut injector = FaultInjector::new(seed, replica_count);
        run_with_faults(&mut sim, &mut wl, &clients, 1_500, u64::MAX, &mut injector);

        assert!(
            wl.auditor.stats().transient_rejections > 0,
            "no request was answered with a result-framed transient rejection, so the \
             path this test exists to cover never ran"
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, 50_000),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
    }

    /// The cross-replica equality check actually compares replicas against each
    /// other, and holds over a metadata workload with crashes and restarts.
    ///
    /// Non-vacuity is the point of the chain assertions. An equality oracle that never
    /// finds two replicas at the same op passes in silence, so `ops_compared` counts
    /// only ops witnessed on more than one replica, the subset that exercised the
    /// property.
    #[test]
    fn committed_metadata_agrees_across_replicas() {
        use crate::workload::{
            self, FaultInjector, Workload,
            invariants::Invariants,
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
        let seed = 0x57A7_E000;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.register_client_with_primary(&client);

        // Metadata ops, since the committed chain this checks is the metadata WAL.
        // Crash and restart so replicas rejoin and repair, which is when a
        // divergence would be introduced if one could be.
        let mut options = WorkloadOptions::new(seed, replica_count, vec![ns]);
        options.weights = ActionWeights::metadata_only();
        options.crash_per_tick_ratio = 0.01;
        options.restart_per_tick_ratio = 0.02;
        let mut wl = Workload::new(options);

        let clients = [client];
        let mut injector = FaultInjector::new(seed, replica_count);
        let mut invariants = Invariants::new();
        // Driven here rather than through `workload::run` so the accumulated
        // chain is readable afterwards; `run` builds its own `Invariants`.
        for _ in 0..4_000u32 {
            wl.tick();
            injector.step(&mut sim, &wl);
            workload::resubmit_due(&mut sim, &mut wl);
            if let Some((target, msg)) = wl.build_request(&clients[0]) {
                sim.submit_request(clients[0].client_id(), target, msg.into_generic());
            }
            for reply in sim.step() {
                let cmds = wl.on_reply(&reply);
                workload::apply_sim_commands(&mut sim, &cmds);
            }
            invariants.check(&sim, &wl);
        }

        assert!(
            injector.restarts() > 0,
            "no replica restarted, so rejoin and repair never ran"
        );
        let chain = invariants.state_checker();
        assert!(
            chain.chain_len() > 0,
            "the canonical commit chain is empty: nothing was ever recorded"
        );
        assert!(
            chain.ops_compared() > 0,
            "no committed op was witnessed on two replicas, so the equality check \
             never actually compared anything and would pass on a diverged cluster"
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, 50_000),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
        assert!(
            oracle::settle_to_stable_view(&mut sim, &mut wl, 50_000),
            "metadata views never converged after the drain"
        );
        oracle::assert_converged(&sim, &mut wl);
    }

    /// A lost `PrepareOk` does not wedge the metadata plane: once the acks flow
    /// again the primary reaches its commit quorum without client involvement.
    ///
    /// The retransmit is the whole mechanism, and it depends on the duplicate being
    /// admitted rather than dropped as a gap: `on_replicate`'s "journal already holds
    /// prepare" branch re-forwards it down the chain and re-acks, regenerating the ack
    /// the primary lost. Fails if a duplicate ever starts falling through to the gap
    /// check instead.
    ///
    /// Drops acks rather than crashing anyone, so the property is about lost acks in
    /// general, not restart recovery.
    #[test]
    fn lost_prepare_ok_is_recovered_by_retransmit() {
        use iggy_binary_protocol::Command;

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
            seed: 0x5EED_0077,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);

        let committed_before = metadata_commit(&sim, 0);

        // Drop only PrepareOk on both backup links. Everything else still flows,
        // so the backups receive and journal the prepare; only the primary's
        // evidence of that is lost.
        for backup in 1..replica_count {
            sim.network
                .link_filter_mut(ProcessId::Replica(backup), ProcessId::Replica(0))
                .remove(Command::PrepareOk);
        }

        let msg = client.create_stream("wl-lost-ack");
        sim.submit_request(client_id, 0, msg.into_generic());

        // Long enough for the prepare to reach and be journaled by both backups
        // while the primary sees no acks.
        for _ in 0..200 {
            sim.step();
        }
        assert_eq!(
            metadata_commit(&sim, 0),
            committed_before,
            "the primary must not commit while every backup ack is dropped"
        );
        for backup in 1..replica_count {
            assert!(
                metadata_op(&sim, usize::from(backup)) > committed_before,
                "backup {backup} must have journaled the prepare, else this test \
                 proves nothing about a LOST ack"
            );
        }

        // Restore the acks. From here the primary's retransmit is the only route
        // to a commit, which is exactly the mechanism under test.
        for backup in 1..replica_count {
            sim.network
                .link_filter_mut(ProcessId::Replica(backup), ProcessId::Replica(0))
                .insert(Command::PrepareOk);
        }

        for _ in 0..5_000 {
            sim.step();
            if metadata_commit(&sim, 0) > committed_before {
                return;
            }
        }
        panic!(
            "metadata commit stuck at {} after 5000 ticks with healthy links: a lost \
             PrepareOk is no longer recovered, so the backup's gap check is now \
             swallowing the primary's retransmit",
            metadata_commit(&sim, 0),
        );
    }

    /// A prepare that every backup journaled but never acked still commits after
    /// those backups restart.
    ///
    /// The rejoin path recovers it: a restarted replica comes back with `current_op`
    /// at N from its own WAL, rejoins as a probing backup (`Status::Recovering`, see
    /// `new_shard`), and its probe draws a targeted `StartView` that returns it to
    /// `Normal` and gets the tail acked. The retransmit cannot do it while the backup
    /// is still probing: `replicate_preflight` refuses any prepare outside
    /// `Status::Normal`.
    ///
    /// Pinned because the fuzzer finds runs where this does NOT happen: two live
    /// backups hold op 45 with the primary at commit 44, both logging the gap drop for
    /// the whole drain. Covering the case that works narrows where that one diverges.
    #[test]
    fn unacked_prepare_commits_after_the_backup_restarts() {
        use iggy_binary_protocol::Command;

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
            seed: 0x5EED_0078,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);
        let committed_before = metadata_commit(&sim, 0);

        // Lose every backup ack, so the prepare is journaled cluster-wide while
        // the primary stays one short of its commit quorum.
        for backup in 1..replica_count {
            sim.network
                .link_filter_mut(ProcessId::Replica(backup), ProcessId::Replica(0))
                .remove(Command::PrepareOk);
        }

        let msg = client.create_stream("wl-unacked");
        sim.submit_request(client_id, 0, msg.into_generic());
        for _ in 0..200 {
            sim.step();
        }
        for backup in 1..replica_count {
            assert!(
                metadata_op(&sim, usize::from(backup)) > committed_before,
                "backup {backup} must hold the prepare before it is restarted"
            );
        }
        assert_eq!(
            metadata_commit(&sim, 0),
            committed_before,
            "the primary must not have committed while its acks were dropped"
        );

        // Restart every backup. Each recovers the unacked op from its own WAL and
        // rejoins as a probing backup, which is the state the primary's
        // retransmit cannot get an ack out of.
        for backup in 1..replica_count {
            sim.replica_crash(backup);
            for _ in 0..50 {
                sim.tick();
            }
            sim.replica_restart(backup);
        }

        // Healthy links from here: nothing but the protocol stands between the
        // primary and its quorum.
        for backup in 1..replica_count {
            sim.network
                .link_filter_mut(ProcessId::Replica(backup), ProcessId::Replica(0))
                .insert(Command::PrepareOk);
        }

        for _ in 0..10_000 {
            sim.step();
            if metadata_commit(&sim, 0) > committed_before {
                return;
            }
        }
        panic!(
            "metadata commit stuck at {} after 10000 ticks with healthy links and \
             every replica holding op {}: the restarted backups never re-acked the \
             prepare they recovered from their own WALs",
            metadata_commit(&sim, 0),
            metadata_op(&sim, 1),
        );
    }

    /// Committed metadata op on a replica's shard 0.
    fn metadata_commit(sim: &Simulator, replica_idx: usize) -> u64 {
        sim.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 owns metadata consensus")
            .commit_min()
    }

    /// Highest metadata op a replica has journaled.
    fn metadata_op(sim: &Simulator, replica_idx: usize) -> u64 {
        sim.replicas[replica_idx]
            .metadata_journal
            .last_op()
            .unwrap_or(0)
    }

    /// IGGY-66 acceptance: per-partition consensus independence. Blocking `ns_a`'s
    /// `PrepareOk` acks and filling its pipeline to `PIPELINE_PREPARE_QUEUE_MAX`
    /// wedges it for want of quorum, `ns_b` still commits, and lifting the block
    /// drains `ns_a` completely.
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
        oracle::assert_converged(&sim, &mut wl);
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
    //! hole and truncates the range as "decided lost", discarding an op journaled on a
    //! quorum and already replied to. The next client op reuses that number and
    //! collides with the stale entry on the up-to-date backup.
    //!
    //! The hole is punched at the commit point, so the regression is caught by "the op
    //! came back" rather than "the head did not regress": with nothing uncommitted
    //! there is no pipeline rebuild to truncate. The `dvc_merge` unit tests cover the
    //! sequencer-truncation path directly.

    use super::*;
    use crate::executor::yield_once;
    use consensus::{Sequencer, Status};
    use journal::Journal;
    use message_bus::MessageBus;

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
}

/// Whether a setup-handshake reply is a result-framed transport rejection rather
/// than an answer. `build_result_rejection_reply` carries the reason in the result
/// section under the request's own operation with `status` left at 0, so nothing in
/// the header distinguishes it from a commit and the code is the only signal.
pub(crate) fn setup_reply_is_transient(reply: &Message<ReplyHeader>) -> bool {
    let header = reply.header();
    let Some(body) = reply
        .as_slice()
        .get(size_of::<ReplyHeader>()..header.size as usize)
    else {
        return false;
    };
    iggy_binary_protocol::result_code(body)
        .and_then(workload::TransientRejection::from_code)
        .is_some()
}
