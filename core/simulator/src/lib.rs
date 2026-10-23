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
pub mod storage;
pub mod workload;

mod partition_wal;

use bus::SimOutbox;
use client::SimClient;
use clock::Clock;
use consensus::{ConsensusClock, MetadataHandle, PartitionsHandle, Status, VsrState};
use deps::SimClock;
use deps::SimSuperblock;
use deps::{MemStorage, SimJournal};
use executor::{DetExecutor, RunOutcome, TaskId};
use iggy_binary_protocol::primitives::partition_history::{ConsumerGroupOwner, PartitionContext};
use iggy_binary_protocol::{
    Command, CommitHeader, DoViewChangeHeader, GenericHeader, PrepareHeader, ReplyHeader,
    RequestHeader, RoutedRequestHeader, StartViewChangeHeader, StartViewHeader,
};
use iggy_common::IggyError;
use message_bus::MessageBus;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use metadata::impls::metadata::StreamsFrontend;
use network::Network;
use packet::{PacketSimulatorOptions, ProcessId};
use partitions::{
    IggyPartition, Partition, PartitionOffsets, PollFragments, PollingArgs, PollingConsumer,
    RetainedPartitionState,
};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use replica::{Replica, SIM_INBOX_CAPACITY, new_shard};
use seeds::SimSeeds;
use server_common::Message;
use server_common::sharding::{IggyNamespace, METADATA_GROUP, PartitionLocation, ShardId};
use shard::shards_table::{ShardsTable, calculate_shard_assignment};
use shard::{CONSENSUS_TICK_INTERVAL, PartitionMaterialisation};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use storage::{Crash, SimStorage};

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
    /// Fixture metadata this replica observed before any WAL replay. Runtime
    /// allocations must be recovered from their logged commands instead.
    seeded_namespaces: RefCell<BTreeMap<IggyNamespace, u32>>,
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
    /// One durable superblock per materialised partition group, with the incarnation
    /// it belongs to, retained like `superblock` so a re-materialised group recovers
    /// its recorded `(view, log_view)` instead of re-entering view 0. Storeless, the
    /// persist gate marks every view durable without writing, leaving the gate, its
    /// write-failure fence and view recovery unexercised.
    pub partition_superblocks: RefCell<HashMap<IggyNamespace, (u64, Rc<SimSuperblock>)>>,
    /// The disk under every partition WAL of this replica, retained across a
    /// restart like `superblock`. A crash keeps only what the WAL synced.
    pub partition_storage: SimStorage,
    /// One retained message log per partition group, plus the offsets recovered from
    /// it. A real server's messages are in segment files; the simulator has none, so
    /// without this a rebuilt partition comes back empty and the monotonicity
    /// invariant calls a discarded log a consensus regression. Populated by
    /// [`Simulator::replica_restart`], consumed by `materialise_partition`. Belongs
    /// to the incarnation of the group's `partition_superblocks` entry.
    partition_logs: RefCell<HashMap<IggyNamespace, RetainedPartitionState>>,
    /// Fixture groups the last restart could not re-materialise, its replay having
    /// stopped short of their incarnation. Reconcile materialises each once the
    /// metadata catches up.
    deferred_partitions: RefCell<BTreeSet<IggyNamespace>>,
    /// This replica's data directory when checkpoints are enabled. Retained so a
    /// restart reads back the snapshot its previous incarnation wrote.
    data_dir: Option<std::path::PathBuf>,
    system_path: std::path::PathBuf,
    /// Keeps each pump's stop channel alive. Dropping one ends that pump gracefully,
    /// reserved for shutdown tests; crash uses `DetExecutor::abort`.
    _stop_txs: Vec<shard::Sender<()>>,
    /// Pump task per shard, aborted on crash.
    pump_tasks: Vec<TaskId>,
    /// Lifecycle report state per shard, indexed by shard id. A restart starts
    /// it empty, as a rebooted reconciler does.
    lifecycle_reports: Vec<server::LifecycleReports>,
    /// Group offset cleanup state per shard, indexed by shard id, reset by a
    /// restart like `lifecycle_reports`.
    group_offset_cleanups: Vec<server::GroupOffsetCleanup>,
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

/// A pipeline head the commit walk is holding on: covered by the commit frontier,
/// but not the op the state machine is next owed.
///
/// What `peek_committable_head` refuses to drain. Two
/// different faults share that refusal and need different thresholds, so
/// [`CommitPrefixHole::kind`] keeps them apart.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitPrefixHole {
    pub head_op: u64,
    pub commit_min: u64,
    pub commit_max: u64,
    pub kind: CommitHoldKind,
}

/// Why a commit walk is holding below its pipeline head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CommitHoldKind {
    /// `head_op > commit_min + 1`: the ops between never arrived. Legitimate and
    /// transient right after a promotion, while the bounded journal walk clears the
    /// apply backlog `RebuildPipeline` seeded above. Repair clears it, and
    /// `commit_min` climbing is the proof repair is working.
    MissingOps,
    /// `head_op <= commit_min`: the walk applied this op and advanced past a
    /// still-resident entry. Nothing missing, no repair owed, only a pop that can
    /// no longer happen -- so it never self-clears, and `commit_min` keeps climbing
    /// while the head stays frozen.
    AppliedHead,
}

impl CommitPrefixHole {
    fn read(head: Option<PrepareHeader>, commit_min: u64, commit_max: u64) -> Option<Self> {
        let head = head?;
        if head.op > commit_max || head.op == commit_min + 1 {
            return None;
        }
        Some(Self {
            head_op: head.op,
            commit_min,
            commit_max,
            kind: if head.op > commit_min {
                CommitHoldKind::MissingOps
            } else {
                CommitHoldKind::AppliedHead
            },
        })
    }
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
    /// Evicted clients and the refusing replicas, in delivery order. Recovery
    /// must bind that transport before retrying its unresolved requests.
    evicted: Vec<(u128, u8)>,
    /// Creation views retained across materialization and restart. Manual
    /// fixtures choose the metadata view at first seed; replicated creates use
    /// their committed per-assignment views. Later seeds reuse the same choice
    /// so replicas cannot initialize one group with different views.
    partition_created_views: HashMap<IggyNamespace, u32>,
    /// Highest view in which each replica sent a view-scoped message of a
    /// partition group, keyed with the incarnation it hosted when the message
    /// left. A restart may forget a later view only if it sent nothing in it.
    partition_sent_views: HashMap<(u8, IggyNamespace, u64), u32>,
    /// Replies a setup handshake pulled off the wire that were not its own.
    ///
    /// `await_setup_reply` steps the whole simulator, so it sees every client's
    /// replies. Dropping the rest strands their auditor expectations until a resend
    /// re-commits, so they are parked here with their senders and returned by the
    /// next [`Self::step_with_senders`].
    deferred_client_replies: Vec<(u8, Message<ReplyHeader>)>,
    /// Dispatch-shell mode: inbound client packets go through the real
    /// `on_client_request` handler (see
    /// [`shard::IggyShard::deliver_client_request`]) instead of raw `dispatch`
    /// routing. Set at construction by [`Simulator::with_shards_shell`].
    shell: bool,
    seed_root: bool,
    consumer_offsets_max: usize,
    // The liveness fence directory survives replica restarts and is removed last.
    _system_directory: Option<tempfile::TempDir>,
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
    /// Checkpoints remain opt-in independently of liveness fence files. Snapshot
    /// writes use synchronous I/O without advancing the executor; the caller owns
    /// the directory's lifetime.
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
            true,
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
            true,
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
        seed_root: bool,
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
        let system_directory = data_dir_root
            .is_none()
            .then(|| tempfile::tempdir().expect("simulator system directory is creatable"));
        let system_root = data_dir_root.unwrap_or_else(|| {
            system_directory
                .as_ref()
                .expect("a simulator without a checkpoint root owns a temporary directory")
                .path()
        });
        let mut replicas = Vec::with_capacity(replica_count);
        let mut outboxes = Vec::with_capacity(replica_count);

        for i in 0..replica_count {
            let id = i as u8;
            let mut bus = SimOutbox::new(id, timer.clone(), spawns.clone(), shell);
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
            let system_path = system_root.join(format!("replica-{i}"));
            std::fs::create_dir_all(&system_path)
                .expect("simulator replica system directory is creatable");
            let replica_data_dir = data_dir_root.map(|_| {
                let dir = system_path.clone();
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
            // One applied-metadata frontier per REPLICA, shared by its shards,
            // as the server bootstrap mints one per process. Volatile: a restart
            // below builds a fresh cell, matching a rebooted node.
            let mut metadata_applied_frontier = metadata::AppliedFrontier::default();
            if replica_count > 1 {
                metadata_applied_frontier = metadata_applied_frontier.with_partition_freshness(
                    outbox.monotonic_now(),
                    configs::server::ServerConfig::default()
                        .cluster
                        .heartbeat_timeout
                        .get_duration(),
                );
            }
            let metadata_applied_frontier = Arc::new(metadata_applied_frontier);
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
                let (shard, writer_bundle, liveness_stops) = new_shard(
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
                    seed_root,
                    metadata_bundle.clone(),
                    shard_superblock,
                    shard_journal,
                    None, // fresh boot: no recovered VSR state
                    metadata_incarnation,
                    (shard_idx == 0).then(|| replica_data_dir.clone()).flatten(),
                    &system_path,
                    // Fresh boot: `init_partition` seeds later, before any workload.
                    &[],
                    Arc::clone(&metadata_applied_frontier),
                );
                stop_txs.extend(liveness_stops);
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
                // The simulator crashes replicas explicitly, so nothing reads
                // the shutdown flag a commit fault would flip.
                let pump_shutdown_flag = Arc::new(AtomicBool::new(false));
                pump_tasks.push(executor.spawn(async move {
                    pump_shard
                        .run_message_pump(stop_rx, pump_shutdown_flag)
                        .await;
                }));
                stop_txs.push(stop_tx);
                shards.push(shard);
            }

            let lifecycle_reports = shards
                .iter()
                .map(|_| server::LifecycleReports::default())
                .collect();
            let group_offset_cleanups = shards
                .iter()
                .map(|_| server::GroupOffsetCleanup::default())
                .collect();
            replicas.push(SimReplica {
                seeded_namespaces: RefCell::new(BTreeMap::new()),
                shards,
                superblock,
                metadata_journal,
                metadata_incarnation,
                partition_superblocks: RefCell::new(HashMap::new()),
                partition_storage: SimStorage::default(),
                partition_logs: RefCell::new(HashMap::new()),
                deferred_partitions: RefCell::new(BTreeSet::new()),
                data_dir: replica_data_dir,
                system_path,
                _stop_txs: stop_txs,
                pump_tasks,
                lifecycle_reports,
                group_offset_cleanups,
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
            partition_created_views: HashMap::new(),
            partition_sent_views: HashMap::new(),
            deferred_client_replies: Vec::new(),
            seed,
            shell,
            seed_root,
            consumer_offsets_max: partitions::DEFAULT_CONSUMER_OFFSETS_MAX,
            _system_directory: system_directory,
        }
    }

    /// Configure the per-kind offset limit used by partitions materialised
    /// after this call.
    ///
    /// # Panics
    /// Panics when `consumer_offsets_max` is zero.
    pub fn set_consumer_offsets_max(&mut self, consumer_offsets_max: usize) {
        assert!(
            consumer_offsets_max > 0,
            "consumer offset limit must be nonzero"
        );
        self.consumer_offsets_max = consumer_offsets_max;
    }

    #[must_use]
    /// # Panics
    /// Panics when `replica_idx` is outside the simulated roster.
    pub fn partition_consumer_offset_counts(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
        kind: iggy_common::ConsumerKind,
    ) -> Option<(usize, usize)> {
        self.replicas[replica_idx]
            .partition_shard(namespace)
            .plane
            .partitions()
            .with_partition(&namespace, |partition| {
                (
                    partition.durable_consumer_offset_count(kind),
                    partition.consumer_offset_map_count(kind),
                )
            })
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
        let Some(created_view) = self.partition_created_view(namespace) else {
            return;
        };
        for (i, replica) in self.replicas.iter().enumerate() {
            if self.crashed.contains(&(i as u8)) {
                continue;
            }
            let streams = replica.shards[0].plane.metadata().mux_stm.streams();
            if streams.created_revision_for_namespace(namespace).is_none() {
                replica
                    .seeded_namespaces
                    .borrow_mut()
                    .insert(namespace, created_view);
                streams.seed_namespace(namespace, namespace.inner(), created_view);
            }
            materialise_partition(replica, namespace, self.consumer_offsets_max);
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
    pub fn seed_stream_topic_partition(&mut self, namespace: IggyNamespace) {
        let Some(created_view) = self.partition_created_view(namespace) else {
            return;
        };
        for (i, replica) in self.replicas.iter().enumerate() {
            if self.crashed.contains(&(i as u8)) {
                continue;
            }
            replica
                .seeded_namespaces
                .borrow_mut()
                .insert(namespace, created_view);
            // Shard 0 is the sole metadata writer. Peers see the seed through the
            // left-right publish, so seeding a reader-mode peer STM would panic.
            replica.shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .seed_namespace(namespace, namespace.inner(), created_view);
        }
    }

    /// Manual fixtures choose the metadata view at first seed and retain it
    /// for later seeds. Production create requests choose each assignment's
    /// initial view separately. `None` with no live metadata consensus to read.
    fn partition_created_view(&mut self, namespace: IggyNamespace) -> Option<u32> {
        if let Some(&created_view) = self.partition_created_views.get(&namespace) {
            return Some(created_view);
        }
        let created_view = self.metadata_view()?;
        self.partition_created_views.insert(namespace, created_view);
        Some(created_view)
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
                client,
                target,
                &msg,
                iggy_binary_protocol::Operation::Register,
                "shell_login",
            )
            .header()
            .op;
        assert!(session > 0, "shell_login: login reply carried no session");
        client.bind_session(session);
        self.discover_partition_contexts(client);
    }

    /// Submit `message` to `target` and step until a client reply arrives,
    /// resubmitting every [`SETUP_RETRY_STEPS`] steps.
    ///
    /// Resubmitted verbatim, so the request id is stable. A shell request first
    /// binds its destination: setup logins can answer from a backup while the
    /// primary's transport remains unbound. The same identity and secret recover
    /// the existing session across those bindings.
    ///
    /// # Panics
    /// If no reply arrives within `SETUP_TOTAL_STEPS`. A handshake that never
    /// completes leaves the fixture unusable, so there is no useful `None`.
    fn await_setup_reply(
        &mut self,
        client: &SimClient,
        target: u8,
        message: &Message<GenericHeader>,
        expected_operation: iggy_binary_protocol::Operation,
        label: &str,
    ) -> Message<ReplyHeader> {
        self.try_await_setup_reply(client, target, message, expected_operation)
            .unwrap_or_else(|| {
                panic!(
                    "{label}: no reply for client {} within {SETUP_TOTAL_STEPS} steps \
                     (seed {:#x})",
                    client.client_id(),
                    self.seed,
                )
            })
    }

    /// [`Self::await_setup_reply`] that returns `None` instead of panicking when
    /// no reply arrives within `SETUP_TOTAL_STEPS`.
    fn try_await_setup_reply(
        &mut self,
        client: &SimClient,
        target: u8,
        message: &Message<GenericHeader>,
        expected_operation: iggy_binary_protocol::Operation,
    ) -> Option<Message<ReplyHeader>> {
        let client_id = client.client_id();
        let request = bytemuck::checked::from_bytes::<RequestHeader>(
            &message.as_slice()[..size_of::<RequestHeader>()],
        )
        .request;
        let bind_before_request =
            self.shell && expected_operation != iggy_binary_protocol::Operation::Register;
        let mut target = target;
        for step in 0..SETUP_TOTAL_STEPS {
            if step % SETUP_RETRY_STEPS == 0 {
                if bind_before_request {
                    self.submit_handshake(client, target);
                } else {
                    self.submit_request(client_id, target, message.deep_copy());
                }
                // Rotate, as the workload's resend path does. Retrying one replica
                // forever suffices on a perfect network and is useless once it is
                // partitioned or crashed mid-handshake: a register has to reach the
                // metadata primary, and this one may be neither reachable nor able
                // to forward.
                target = (target + 1) % self.replica_count.max(1);
            }
            let mut answer = None;
            for (from, reply) in self.step_with_senders() {
                let header = reply.header();
                if bind_before_request
                    && header.client == client_id
                    && header.operation == iggy_binary_protocol::Operation::Register
                    && let Some(session) = self.handshake_session(&reply)
                {
                    client.bind_session(session);
                    self.submit_request(client_id, from, message.deep_copy());
                    continue;
                }
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
                    && header.request == request
                    && !setup_reply_is_transient(&reply)
                {
                    answer = Some(reply);
                    continue;
                }
                self.deferred_client_replies.push((from, reply));
            }
            if answer.is_some() {
                return answer;
            }
        }
        None
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
    /// If a client-addressed packet comes from a client or does not decode as
    /// `ReplyHeader`, or a pump livelocks (poll budget exhausted).
    pub fn step(&mut self) -> Vec<Message<ReplyHeader>> {
        self.step_with_senders()
            .into_iter()
            .map(|(_, reply)| reply)
            .collect()
    }

    /// [`Self::step`], with the replica that sent each reply.
    ///
    /// # Panics
    /// As [`Self::step`].
    #[allow(clippy::cast_possible_truncation)]
    pub fn step_with_senders(&mut self) -> Vec<(u8, Message<ReplyHeader>)> {
        // Ahead of this step's own traffic, so a driver sees replies in delivery
        // order rather than the order a handshake happened to interrupt.
        let mut client_replies = std::mem::take(&mut self.deferred_client_replies);

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
                            ProcessId::Client(_) => {
                                let shard = &replica.shards[entry];
                                shard.dispatch(stamp_metadata_watermark(shard, &packet.message));
                            }
                            ProcessId::Replica(_) => {
                                replica.shards[entry].dispatch(packet.message.deep_copy());
                            }
                        }
                    }
                    // Crashed or missing: packet silently dropped.
                }
                ProcessId::Client(client_id) => {
                    let ProcessId::Replica(from) = packet.from else {
                        panic!("client {client_id} got a packet from another client");
                    };
                    // An eviction cannot decode as a reply. Preserve its sender
                    // so recovery binds the transport that refused the request.
                    if packet.message.header().command == Command::Eviction {
                        self.evicted.push((client_id, from));
                        continue;
                    }
                    let reply: Message<ReplyHeader> = packet
                        .message
                        .deep_copy()
                        .try_into_typed()
                        .expect("invalid message, wrong command type for a client response");
                    client_replies.push((from, reply));
                }
            }
        }
        self.network.recycle_buffer(packets);

        // Phase 1b: Pumps process the delivered frames (and their loopback
        // and reconcile follow-ups) to quiescence.
        self.run_pumps();

        self.reconcile_transitions();
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
                if let Some((namespace, incarnation, view)) = view_scoped_partition_frame(&message)
                {
                    let sent = self
                        .partition_sent_views
                        .entry((i as u8, namespace, incarnation))
                        .or_default();
                    *sent = (*sent).max(view);
                }
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

    /// Take the evicted clients and refusing replicas since the last call.
    ///
    /// A driver must rebind their transports. The successful handshake determines
    /// whether outstanding requests still belong to the recovered retry session.
    pub fn take_evictions(&mut self) -> Vec<(u128, u8)> {
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

    /// Lost-work tripwire. At executor quiescence every live pump must have drained
    /// both inbox lanes and its parked-frame redispatch queue. A non-empty lane on a
    /// non-crashed replica means a frame reached the channel without waking the
    /// target pump. A non-empty redispatch queue means a materialisation staged work
    /// without the pump's priority drain completing. Every pump holds a standing
    /// `CONSENSUS_TICK_INTERVAL` timer, so the next `advance_time` would re-poll and
    /// silently drain either case, masking the fault. Trip here instead.
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
                // First and unconditional. A fenced pump has exited, so its frames
                // pile up exactly as a missed wake does and every lane assert below
                // would misreport the cause -- and the pump's `FatalCommit` return
                // is dropped at the spawn, so nothing else sees it. Gated on a
                // non-empty inbox, a fenced pump that happened to drain would pass
                // quiescence outright.
                assert!(
                    shard.fenced_partition_fault().is_none(),
                    "fenced pump: replica {replica_id} shard {} exited on a fatal commit ({:?}). \
                     Not a lost wakeup; fix the commit failure (seed {:#x}, schedule hash {:#x})",
                    shard.id,
                    shard.fenced_partition_fault(),
                    self.seed,
                    self.executor.schedule_hash(),
                );
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
                let pending_completions = shard.poll_completion_inbox_len();
                assert_eq!(
                    pending_completions,
                    0,
                    "lost wakeup: replica {replica_id} shard {} completion lane holds \
                     {pending_completions} result(s) at quiescence (seed {:#x}, schedule hash {:#x})",
                    shard.id,
                    self.seed,
                    self.executor.schedule_hash(),
                );
                let pending_redispatch = shard.redispatched_frame_count();
                assert_eq!(
                    pending_redispatch,
                    0,
                    "lost redispatch: replica {replica_id} shard {} redispatch queue holds \
                     {pending_redispatch} frame(s) at quiescence (seed {:#x}, schedule hash {:#x})",
                    shard.id,
                    self.seed,
                    self.executor.schedule_hash(),
                );
            }
        }
    }

    fn reconcile_transitions(&mut self) {
        let now = self.realtime_micros();
        let timeout = rebalancing_timeout_micros();
        for (index, replica) in self.replicas.iter().enumerate() {
            if self
                .crashed
                .contains(&u8::try_from(index).expect("replica id fits u8"))
            {
                continue;
            }
            let streams = replica.shards[0].plane.metadata().mux_stm.streams();
            // Whether to materialise the target when nothing hosts it. A recreated
            // fixture topic takes no lifecycle path, so its new incarnation only
            // replaces a hosted one, or comes up where a restart deferred it.
            let mut targets: BTreeMap<IggyNamespace, bool> = replica
                .seeded_namespaces
                .borrow()
                .keys()
                .map(|&namespace| (namespace, false))
                .collect();
            targets.extend(
                replica
                    .deferred_partitions
                    .borrow()
                    .iter()
                    .map(|&namespace| (namespace, true)),
            );
            for intent in streams.pending_lifecycles() {
                for target in intent.partitions {
                    targets.insert(
                        IggyNamespace::new(
                            intent.stream_id as usize,
                            target.topic_id as usize,
                            target.partition_id as usize,
                        ),
                        true,
                    );
                }
            }
            for transition in streams.consumer_group_pending_revocations() {
                targets.insert(
                    IggyNamespace::new(
                        transition.stream_id as usize,
                        transition.topic_id as usize,
                        transition.partition_id as usize,
                    ),
                    true,
                );
            }
            for (namespace, materialise_unhosted) in targets {
                let incarnation = streams.created_revision_for_namespace(namespace);
                let partitions = replica.partition_shard(namespace).plane.partitions();
                match partitions
                    .get_by_ns(&namespace)
                    .map(IggyPartition::created_revision)
                {
                    // A recreated topic reuses the deleted one's ids. The production
                    // reconciler tears the deleted incarnation down before it
                    // materializes the new one.
                    Some(local) if incarnation.is_some_and(|current| current != local) => {
                        partitions.remove(&namespace);
                    }
                    Some(_) => continue,
                    None if !materialise_unhosted => continue,
                    None => {}
                }
                if let Some(view) = streams.created_view_for_namespace(namespace) {
                    self.partition_created_views
                        .entry(namespace)
                        .or_insert(view);
                    if materialise_partition(replica, namespace, self.consumer_offsets_max) {
                        replica.deferred_partitions.borrow_mut().remove(&namespace);
                    }
                }
            }
            for ((shard, reports), cleanup) in replica
                .shards
                .iter()
                .zip(&replica.lifecycle_reports)
                .zip(&replica.group_offset_cleanups)
            {
                server::reconcile_pending_revocations(shard, now, timeout);
                server::reconcile_partition_lifecycles(shard, reports, now);
                server::reconcile_consumer_group_offsets(shard, cleanup);
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

    /// Model discovery before creating requests, never while delivering or retrying them.
    pub fn discover_partition_contexts(&self, client: &SimClient) {
        let Some((replica, _)) = self
            .replicas
            .iter()
            .zip(0..self.replica_count)
            .filter(|(_, replica_id)| !self.crashed.contains(replica_id))
            .max_by_key(|(replica, _)| replica.shards[0].plane.metadata().applied_frontier().get())
        else {
            return;
        };
        let metadata = replica.shards[0].plane.metadata();
        let metadata_op = metadata.applied_frontier().get();
        let () = metadata.mux_stm.streams().read(|inner| {
            for (stream_id, stream) in &inner.items {
                for (topic_id, topic) in &stream.topics {
                    for partition in &topic.partitions {
                        client.set_partition_context(
                            IggyNamespace::new(stream_id, topic_id, partition.id),
                            PartitionContext {
                                incarnation: partition.created_revision,
                                owner_generation: 0,
                                metadata_op,
                            },
                        );
                    }
                }
            }
        });
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
            client,
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
        self.discover_partition_contexts(client);
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

        // Power loss: the WAL keeps only what it synced. Handles the dead
        // process held go stale, as they do across a real restart.
        self.replicas[replica_index as usize]
            .partition_storage
            .crash(Crash::PowerLoss);

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
    #[allow(clippy::too_many_lines)]
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
        let partition_storage = self.replicas[idx].partition_storage.clone();
        // Take each live partition's log while its shard still stands: a real
        // server's messages are in segment files its boot recovers the offset counter
        // from, so rebuilding with nothing would model total data loss rather than a
        // restart. Before the rebuild, which drops the shards. A log the last boot
        // retained for a group its replay had not reached yet is still owed too.
        let mut partition_logs =
            std::mem::take(&mut *self.replicas[idx].partition_logs.borrow_mut());
        partition_logs.extend(self.retain_partition_logs(idx, &partition_superblocks));
        let seeded_namespaces = self.replicas[idx].seeded_namespaces.borrow().clone();
        let seed_namespaces: Vec<_> = seeded_namespaces
            .iter()
            .map(|(&namespace, &view)| (namespace, view))
            .collect();
        let mut materialized_namespaces: Vec<_> = partition_superblocks.keys().copied().collect();
        materialized_namespaces.sort_unstable();

        // Durable VSR state from the retained superblock, before the rebuild, as
        // production reads it in `restore_metadata_consensus`.
        let recovered_state = superblock
            .read_latest_sync()
            .and_then(|bytes| VsrState::try_from(bytes.as_slice()).ok());

        // Carried like the WAL and superblocks: the rebuilt replica has to find the
        // snapshot its previous incarnation persisted.
        let replica_data_dir = self.replicas[idx].data_dir.clone();
        let system_path = self.replicas[idx].system_path.clone();

        let consensus_clock = ConsensusClock::new(Rc::new(SimClock::new(self.executor.timer())));
        let outbox = Rc::clone(&self.outboxes[idx]);
        let (senders, mut inboxes, mut reply_inboxes) =
            shard::shard_mesh_channels(shards_per_replica, SIM_INBOX_CAPACITY, SIM_INBOX_CAPACITY);

        let mut shards = Vec::with_capacity(usize::from(shards_per_replica));
        let mut stop_txs = Vec::with_capacity(usize::from(shards_per_replica));
        let mut pump_tasks = Vec::with_capacity(usize::from(shards_per_replica));
        let mut metadata_bundle: Option<replica::SimMetadataBundle> = None;
        let mut metadata_applied_frontier = metadata::AppliedFrontier::default();
        if self.replica_count > 1 {
            metadata_applied_frontier = metadata_applied_frontier.with_partition_freshness(
                outbox.monotonic_now(),
                configs::server::ServerConfig::default()
                    .cluster
                    .heartbeat_timeout
                    .get_duration(),
            );
        }
        let metadata_applied_frontier = Arc::new(metadata_applied_frontier);
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
            let (shard, writer_bundle, liveness_stops) = new_shard(
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
                self.seed_root,
                metadata_bundle.clone(),
                shard_superblock,
                shard_journal,
                recovered_state.as_ref(),
                metadata_incarnation,
                (shard_idx == 0).then(|| replica_data_dir.clone()).flatten(),
                &system_path,
                &seed_namespaces,
                Arc::clone(&metadata_applied_frontier),
            );
            stop_txs.extend(liveness_stops);
            if shard_idx == 0 {
                metadata_bundle =
                    Some(writer_bundle.expect("shard 0 returns the metadata factory bundle"));
            }
            let (stop_tx, stop_rx) = shard::channel::<()>(1);
            let pump_shard = Rc::clone(&shard);
            let pump_shutdown_flag = Arc::new(AtomicBool::new(false));
            pump_tasks.push(self.executor.spawn(async move {
                pump_shard
                    .run_message_pump(stop_rx, pump_shutdown_flag)
                    .await;
            }));
            stop_txs.push(stop_tx);
            shards.push(shard);
        }

        // Replacing the replica drops the old shards, losing all volatile consensus
        // state as a real restart does. The harness-owned superblock carries over.
        let lifecycle_reports = shards
            .iter()
            .map(|_| server::LifecycleReports::default())
            .collect();
        let group_offset_cleanups = shards
            .iter()
            .map(|_| server::GroupOffsetCleanup::default())
            .collect();
        self.replicas[idx] = SimReplica {
            seeded_namespaces: RefCell::new(seeded_namespaces),
            shards,
            superblock,
            metadata_journal,
            metadata_incarnation,
            partition_superblocks: RefCell::new(partition_superblocks),
            partition_storage,
            partition_logs: RefCell::new(partition_logs),
            deferred_partitions: RefCell::new(BTreeSet::new()),
            data_dir: replica_data_dir,
            system_path,
            _stop_txs: stop_txs,
            pump_tasks,
            lifecycle_reports,
            group_offset_cleanups,
        };

        // Re-materialise every group this replica had before the crash, as a
        // rebooted server re-opens every partition directory it owns. This is what
        // makes the carried-forward superblock load-bearing: the group recovers its
        // recorded `(view, log_view)` instead of re-entering view 0. The metadata half
        // of the seed already ran inside `new_shard`, ahead of the replay.
        for namespace in materialized_namespaces {
            let replica = &self.replicas[idx];
            if !materialise_partition(replica, namespace, self.consumer_offsets_max)
                && replica.seeded_namespaces.borrow().contains_key(&namespace)
            {
                replica.deferred_partitions.borrow_mut().insert(namespace);
            }
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
        materialised: &HashMap<IggyNamespace, (u64, Rc<SimSuperblock>)>,
    ) -> HashMap<IggyNamespace, RetainedPartitionState> {
        let replica = &self.replicas[replica_idx];
        let mut retained = HashMap::with_capacity(materialised.len());
        for &namespace in materialised.keys() {
            let partitions = replica.partition_shard(namespace).plane.partitions();
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                continue;
            };
            retained.insert(namespace, partition.take_retained_state());
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

    /// Poll messages through a replica's partition owner.
    ///
    /// The returned future owns its request and shard handle, so it can run on
    /// the simulator executor while the caller advances the simulation or
    /// releases paused work. The owner accepts the read and replies before
    /// awaiting replication of an automatic commit.
    ///
    /// # Errors
    /// Returns routing and owner rejections, `IggyError::ResourceNotFound` if
    /// the owner reports a missing partition, or `IggyError::ShardCommunicationError`
    /// if a submitted request times out or loses its reply.
    ///
    /// # Panics
    /// If `replica_idx` is out of bounds or the owner replies for a different read type.
    #[allow(clippy::future_not_send)]
    pub fn poll_messages(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
        consumer: PollingConsumer,
        args: &PollingArgs,
    ) -> impl Future<Output = Result<PollFragments<4096>, IggyError>> + use<> {
        let owner = Rc::clone(self.replicas[replica_idx].partition_shard(namespace));
        let args = args.clone();
        async move {
            match owner
                .partition_read(
                    namespace,
                    shard::PartitionRead::Poll {
                        consumer,
                        args,
                        metadata: None,
                    },
                )
                .await
            {
                Some(shard::PartitionReadReply::Poll { fragments, .. }) => Ok(fragments),
                Some(shard::PartitionReadReply::Rejected(error)) => Err(error),
                Some(shard::PartitionReadReply::NotFound) => {
                    Err(IggyError::ResourceNotFound(format!(
                        "partition not found for namespace {namespace:?} on replica {replica_idx}"
                    )))
                }
                None => Err(IggyError::ShardCommunicationError),
                Some(reply) => panic!("unexpected reply to a poll request: {reply:?}"),
            }
        }
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

    /// The incarnation of the partition a replica hosts for `namespace`, or `None`
    /// when it hosts none. A recreated topic keeps the namespace and changes this.
    #[must_use]
    pub(crate) fn partition_incarnation(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
    ) -> Option<u64> {
        self.replicas[replica_idx]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .map(IggyPartition::created_revision)
    }

    /// The owner of consumer group `group_id` that a replica installed on its
    /// partition of `namespace`, or `None` when it installed none or hosts none.
    #[must_use]
    pub(crate) fn installed_group_owner(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
        group_id: u64,
    ) -> Option<ConsumerGroupOwner> {
        self.replicas[replica_idx]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)?
            .consumer_group_owner(group_id)
    }

    /// The consensus clock's realtime, in microseconds.
    #[must_use]
    pub(crate) fn realtime_micros(&self) -> u64 {
        SimClock::new(self.executor.timer()).realtime().as_micros()
    }

    /// Whether every live replica applied metadata `op` and hosts each of
    /// `namespaces` at the incarnation its own metadata names.
    #[must_use]
    pub(crate) fn incarnation_settled(&self, op: u64, namespaces: &[IggyNamespace]) -> bool {
        (0..self.replica_count)
            .filter(|replica_idx| !self.crashed.contains(replica_idx))
            .all(|replica_idx| {
                let idx = usize::from(replica_idx);
                let metadata = self.replicas[idx].shards[0].plane.metadata();
                let streams = metadata.mux_stm.streams();
                metadata.applied_frontier().get() >= op
                    && namespaces.iter().all(|&namespace| {
                        streams
                            .created_revision_for_namespace(namespace)
                            .is_some_and(|current| {
                                self.partition_incarnation(idx, namespace) == Some(current)
                            })
                    })
            })
    }

    /// A replica's journaled partition-plane prepare headers over `ops`, or `None`
    /// when it does not host the namespace.
    ///
    /// Repair headers, not resident ones. `evict_prefix` clears the resident vec as
    /// the committed prefix flushes to segments and moves those entries to the
    /// repair ring, so a resident-only read compares nothing at all once a run has
    /// flushed. The ring is capacity-bounded, which makes this the recently
    /// committed tail rather than the whole prefix -- and that tail is where a bad
    /// repair or a mis-decided view change lands.
    ///
    /// One pass per replica per namespace, not one per op: both lookups behind
    /// `repair_headers_in` are linear.
    #[must_use]
    pub(crate) fn partition_journaled_headers(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
        ops: std::ops::RangeInclusive<u64>,
    ) -> Option<BTreeMap<u64, PrepareHeader>> {
        let shard = self.replicas[replica_idx].partition_shard(namespace);
        let partition = shard.plane.partitions().get_by_ns(&namespace)?;
        Some(partition.log.journal().inner.repair_headers_in(ops))
    }

    /// Consensus view for a replica's partition-plane group, or `None` if that
    /// replica does not host the namespace.
    #[must_use]
    /// Highest view in which `replica_idx` sent a view-scoped message of
    /// `incarnation` of `namespace`, or `None` when it sent none.
    pub(crate) fn partition_sent_view(
        &self,
        replica_idx: u8,
        namespace: IggyNamespace,
        incarnation: u64,
    ) -> Option<u32> {
        self.partition_sent_views
            .get(&(replica_idx, namespace, incarnation))
            .copied()
    }

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

    /// A replica's metadata consensus handle, or `None` when it hosts no metadata
    /// plane. The one way to reach it; do not hand-walk the shard-0 / plane /
    /// metadata chain.
    #[must_use]
    pub(crate) fn metadata_consensus(
        &self,
        replica_idx: usize,
    ) -> Option<&consensus::VsrConsensus<crate::bus::SharedSimOutbox>> {
        self.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
    }

    /// Logs a replica refused for contradicting an op it applied, over its
    /// metadata consensus and every partition group it hosts. See
    /// `VsrConsensus::contradicts_applied_log`.
    #[must_use]
    pub(crate) fn applied_log_contradictions(&self, replica_idx: usize) -> u64 {
        let metadata = self
            .metadata_consensus(replica_idx)
            .map_or(0, consensus::VsrConsensus::applied_log_contradictions);
        let partitions: u64 = self.replicas[replica_idx]
            .shards
            .iter()
            .map(|shard| {
                let partitions = shard.plane.partitions();
                partitions
                    .namespaces()
                    .filter_map(|namespace| partitions.get_by_ns(namespace))
                    .map(|partition| partition.consensus().applied_log_contradictions())
                    .sum::<u64>()
            })
            .sum();
        metadata + partitions
    }

    /// The metadata pipeline head the commit walk is holding on, if any. See
    /// [`CommitPrefixHole`].
    #[must_use]
    pub(crate) fn metadata_commit_prefix_hole(
        &self,
        replica_idx: usize,
    ) -> Option<CommitPrefixHole> {
        let consensus = self.metadata_consensus(replica_idx)?;
        CommitPrefixHole::read(
            consensus.pipeline_head_header(),
            consensus.commit_min(),
            consensus.commit_max(),
        )
    }

    /// Partition-plane twin of [`Self::metadata_commit_prefix_hole`].
    #[must_use]
    pub(crate) fn partition_commit_prefix_hole(
        &self,
        replica_idx: usize,
        namespace: IggyNamespace,
    ) -> Option<CommitPrefixHole> {
        let shard = self.replicas[replica_idx].partition_shard(namespace);
        let partition = shard.plane.partitions().get_by_ns(&namespace)?;
        let consensus = partition.consensus();
        CommitPrefixHole::read(
            consensus.pipeline_head_header(),
            consensus.commit_min(),
            consensus.commit_max(),
        )
    }

    /// Index of the current primary for `namespace`, as seen by the first live
    /// replica hosting it, or `None` if no live replica hosts it.
    ///
    /// Reads one replica's view, so it assumes live replicas agree on the primary.
    /// That no longer holds by construction: `spare_primary` is off under
    /// `--crash-primary`, so a crash-triggered view change can run mid-run and this
    /// may name a stale or crashed primary. Callers needing a real answer run
    /// `workload::oracle::settle_to_stable_view` first.
    /// The replica the METADATA plane currently names primary, read from the
    /// first live replica that owns a metadata consensus.
    ///
    /// The twin of [`Self::primary_index`], which answers for a partition
    /// group. The two planes count views independently, so they agree only
    /// while their views are congruent mod the replica count, and a group
    /// materialised after a metadata election is the case where they part.
    ///
    /// Test-only. The workload oracle deliberately does NOT assert the two
    /// planes agree: that holds for a group at the view it was seeded in, and
    /// a later election on either plane parts them again with nothing to pull
    /// them back, so a live invariant would fire on correct runs.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn metadata_primary_index(&self) -> Option<u8> {
        (0..self.replica_count)
            .filter(|replica_idx| !self.crashed.contains(replica_idx))
            .find_map(|replica_idx| {
                let consensus = self.replicas[usize::from(replica_idx)].shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()?;
                Some(consensus.primary_index(consensus.view()))
            })
    }

    /// The metadata plane's view, as the first live replica owning a metadata
    /// consensus sees it.
    fn metadata_view(&self) -> Option<u32> {
        (0..self.replica_count)
            .filter(|replica_idx| !self.crashed.contains(replica_idx))
            .find_map(|replica_idx| {
                self.replicas[usize::from(replica_idx)].shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .map(consensus::VsrConsensus::view)
            })
    }

    /// The live replica whose metadata plane is a `Normal` primary: the holder of
    /// the committed metadata log, and where a client's leader redirect goes.
    #[must_use]
    pub fn metadata_leader(&self) -> Option<u8> {
        (0..self.replica_count)
            .filter(|replica_idx| !self.crashed.contains(replica_idx))
            .find(|&replica_idx| {
                self.replicas[usize::from(replica_idx)].shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .is_some_and(|consensus| {
                        consensus.is_primary() && consensus.status() == Status::Normal
                    })
            })
    }

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
///
/// Returns whether it materialised the group, which it declines while the
/// replica's metadata names no incarnation for it, or one older than its store.
fn materialise_partition(
    replica: &SimReplica,
    namespace: IggyNamespace,
    consumer_offsets_max: usize,
) -> bool {
    let shard_count = u32::try_from(replica.shards.len()).expect("shard count fits u32");
    let owner = calculate_shard_assignment(&namespace, shard_count);
    let streams = replica.shards[0].plane.metadata().mux_stm.streams();
    // Materialization and recovery must never create metadata. Seeding a vacant
    // slab here changes future allocation and makes replicas diverge after deletion.
    let Some(epoch) = streams.created_revision_for_namespace(namespace) else {
        return false;
    };
    // Production seeds from the committed partition, never from a live view.
    let created_view = streams
        .created_view_for_namespace(namespace)
        .expect("a committed partition records its creation view");
    // One store per incarnation, minted on first materialisation and reused after, so
    // the recorded view survives a replica restart. A deleted incarnation's store
    // must not reach its successor, and metadata replayed behind the store waits for
    // repair to catch it up.
    let (superblock, first_initialization) = {
        let mut stores = replica.partition_superblocks.borrow_mut();
        if let Some(stored) = stores.get(&namespace).map(|(incarnation, _)| *incarnation) {
            match stored.cmp(&epoch) {
                Ordering::Greater => return false,
                Ordering::Less => {
                    stores.remove(&namespace);
                    replica.partition_logs.borrow_mut().remove(&namespace);
                    partition_wal::remove(&replica.partition_storage, namespace, stored);
                }
                Ordering::Equal => {}
            }
        }
        // Store creation and shard initialization run synchronously, with no
        // simulated crash between them. Keep this record even if history is lost.
        let first_initialization = !stores.contains_key(&namespace);
        let superblock = Rc::clone(
            &stores
                .entry(namespace)
                .or_insert_with(|| (epoch, Rc::default()))
                .1,
        );
        (superblock, first_initialization)
    };
    let recovered_state = superblock
        .read_latest_sync()
        .and_then(|bytes| VsrState::try_from(bytes.as_slice()).ok());
    // Hand back the log this group left behind, if it was materialised here before.
    // Removed rather than cloned: the rebuilt partition becomes its sole owner, and
    // a second materialisation with no restart between would otherwise resurrect a
    // log the live partition has moved past.
    let mut retained = replica.partition_logs.borrow_mut().remove(&namespace);
    let owner_shard = &replica.shards[usize::from(owner)];
    // The live group holds this incarnation's WAL lease; a second open is refused.
    if !owner_shard.plane.partitions().contains(&namespace) {
        let persistence = partition_wal::reopen(
            &replica.partition_storage,
            namespace,
            epoch,
            retained.as_mut(),
        );
        owner_shard.init_partition(
            namespace,
            Some(superblock),
            recovered_state.as_ref(),
            retained,
            persistence,
            PartitionMaterialisation::new(epoch, created_view, first_initialization)
                .with_consumer_offsets_max(consumer_offsets_max),
        );
    }
    for shard in &replica.shards {
        shard.shards_table().insert(
            namespace,
            PartitionLocation::new(ShardId::new(owner), epoch),
        );
    }
    true
}

/// The partition group, incarnation and view of a message the persist gate
/// holds until the replica's view is durable, or `None` for any other message.
/// `PrepareOk` names no incarnation, so it is left out.
fn view_scoped_partition_frame(
    message: &Message<GenericHeader>,
) -> Option<(IggyNamespace, u64, u32)> {
    let (group, incarnation) = match message.header().command {
        Command::Prepare => {
            let header = message.try_as_typed::<PrepareHeader>().ok()?.header();
            (header.group, header.partition_incarnation)
        }
        Command::Commit => {
            let header = message.try_as_typed::<CommitHeader>().ok()?.header();
            (header.group, header.partition_incarnation)
        }
        Command::StartViewChange => {
            let header = message
                .try_as_typed::<StartViewChangeHeader>()
                .ok()?
                .header();
            (header.group, header.partition_incarnation)
        }
        Command::DoViewChange => {
            let header = message.try_as_typed::<DoViewChangeHeader>().ok()?.header();
            (header.group, header.partition_incarnation)
        }
        Command::StartView => {
            let header = message.try_as_typed::<StartViewHeader>().ok()?.header();
            (header.group, header.partition_incarnation)
        }
        _ => return None,
    };
    (group != METADATA_GROUP).then(|| {
        (
            IggyNamespace::from_raw(group),
            incarnation,
            message.header().view,
        )
    })
}

/// How long the reconciler lets a revocation wait for its drain, in microseconds.
pub(crate) fn rebalancing_timeout_micros() -> u64 {
    configs::server::ServerConfig::default()
        .consumer_group
        .rebalancing_timeout
        .as_micros()
}

/// The watermark is server-owned: production stamps it on the node that
/// dispatches a partition write, so the raw path stamps it on delivery.
fn stamp_metadata_watermark(
    shard: &Replica,
    message: &Message<GenericHeader>,
) -> Message<GenericHeader> {
    match message.try_as_typed::<RoutedRequestHeader>() {
        Ok(request) if request.header().operation.is_partition() => {
            let watermark = shard.plane.metadata().applied_frontier().get();
            request
                .deep_copy()
                .transmute_header(|header, stamped: &mut RoutedRequestHeader| {
                    *stamped = header;
                    stamped.metadata_watermark = watermark;
                })
                .into_generic()
        }
        _ => message.deep_copy(),
    }
}

#[cfg(test)]
mod tests {
    use super::partition_repair_driver_tests::{
        cluster, is_commit_for, prepare_for, sharded_cluster,
    };
    use super::*;
    use crate::client::SimClient;
    use crate::workload::apply_sim_commands;
    use bytes::Bytes;
    use consensus::{
        Consensus, PlaneKind, Project, Sequencer, Status, client_table::COMMITTED_WINDOW_BITS,
    };
    use futures::FutureExt;
    use iggy_binary_protocol::requests::partitions::InstallConsumerGroupOwnerRequest;
    use iggy_binary_protocol::requests::users::CreateUserRequest;
    use iggy_binary_protocol::{
        AckLevel, ConsensusHeader, Operation, PrepareOkHeader, RoutedRequestHeader,
        StartViewHeader, WireDecode, WireEncode, WireGlobalPermissions, WireIdentifier, WireName,
        WireOptions, WirePermissions,
    };
    use iggy_common::{ConsumerKind, IggyError};
    use journal::{Journal, JournalHandle};
    use message_bus::lifecycle::Shutdown;
    use message_bus::{AUTO_COMMIT_CLIENT_ID, ConnectionInstaller, MessageBus};
    use metadata::stm::stream::Streams;
    use server_common::MessageBag;
    use server_common::sharding::{IggyNamespace, LIST_CLIENTS_GATHER_TIMEOUT, METADATA_GROUP};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct RootInitialization {
        shutdown: Shutdown,
        result: Rc<RefCell<Option<Result<(), server::server_error::ServerError>>>>,
    }

    fn start_root_initialization(
        sim: &mut Simulator,
        node: usize,
        with_credentials: bool,
    ) -> RootInitialization {
        let shard = Rc::clone(&sim.replicas[node].shards[0]);
        let timer = sim.executor.timer();
        let (shutdown, token) = Shutdown::new();
        let result = Rc::new(RefCell::new(None));
        let completed = Rc::clone(&result);
        let task = sim.executor.spawn(async move {
            let credentials = with_credentials.then(|| CreateUserRequest {
                username: WireName::new(replica::SHELL_ROOT_USERNAME).unwrap(),
                password: server_common::crypto::hash_with_fixed_salt(replica::SHELL_ROOT_PASSWORD),
                status: iggy_common::UserStatus::Active.as_code(),
                permissions: Some(WirePermissions {
                    global: WireGlobalPermissions {
                        manage_servers: true,
                        read_servers: true,
                        manage_users: true,
                        read_users: true,
                        manage_streams: true,
                        read_streams: true,
                        manage_topics: true,
                        read_topics: true,
                        poll_messages: true,
                        send_messages: true,
                    },
                    streams: Vec::new(),
                }),
                options: WireOptions::empty(),
            });
            let outcome = server::boot::initialize_root_user(
                &shard,
                credentials.as_ref(),
                token,
                |duration| timer.sleep(duration),
            )
            .await;
            *completed.borrow_mut() = Some(outcome);
        });
        sim.replicas[node].pump_tasks.push(task);
        RootInitialization { shutdown, result }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_uncommitted_root_when_primary_crashes_should_recover_or_replace_the_prepare() {
        for replicated in [true, false] {
            let mut sim = Simulator::build_inner(
                3,
                2,
                std::iter::once(1),
                PacketSimulatorOptions {
                    node_count: 3,
                    client_count: 1,
                    seed: 4585,
                    ..Default::default()
                },
                true,
                None,
                false,
            );
            for node in &sim.replicas {
                assert!(
                    node.shards[0]
                        .plane
                        .metadata()
                        .mux_stm
                        .users()
                        .read(|users| users.items.is_empty())
                );
            }
            let client = SimClient::new(1);
            sim.outboxes[1].insert_client_meta(ClientConnMeta::new(
                client.client_id(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                ClientTransportKind::Tcp,
            ));
            let reply = submit_and_wait_for_reply(
                &mut sim,
                client.client_id(),
                1,
                client.login(replica::SHELL_ROOT_USERNAME, replica::SHELL_ROOT_PASSWORD),
            );
            assert_eq!(
                iggy_binary_protocol::result_code(reply.body()),
                Some(IggyError::TransientNotAccepted.as_code())
            );
            for backup in [1, 2] {
                if replicated {
                    sim.network
                        .link_filter_mut(ProcessId::Replica(backup), ProcessId::Replica(0))
                        .remove(Command::PrepareOk);
                } else {
                    sim.network
                        .link_filter_mut(ProcessId::Replica(0), ProcessId::Replica(backup))
                        .remove(Command::Prepare);
                }
            }
            let _primary = start_root_initialization(&mut sim, 0, true);
            let backups = [
                start_root_initialization(&mut sim, 1, !replicated),
                start_root_initialization(&mut sim, 2, !replicated),
            ];
            for _ in 0..200 {
                sim.step();
                if sim.replicas[0].metadata_journal.last_op() == Some(1)
                    && (!replicated
                        || sim
                            .replicas
                            .iter()
                            .all(|node| node.metadata_journal.last_op() == Some(1)))
                {
                    break;
                }
            }
            let original_checksum = sim.replicas[0].metadata_journal.header(1).unwrap().checksum;
            for (index, node) in sim.replicas.iter().enumerate() {
                assert_eq!(
                    node.metadata_journal.last_op(),
                    (index == 0 || replicated).then_some(1)
                );
                assert!(
                    node.shards[0]
                        .plane
                        .metadata()
                        .mux_stm
                        .users()
                        .read(|users| users.items.is_empty())
                );
            }
            sim.replica_crash(0);
            if !replicated {
                for backup in [1, 2] {
                    sim.network
                        .link_clear(ProcessId::Replica(0), ProcessId::Replica(backup));
                }
            }
            sim.network.clear_partition();
            for _ in 0..2_000 {
                sim.step();
                if backups
                    .iter()
                    .all(|initialization| initialization.result.borrow().is_some())
                {
                    break;
                }
            }
            sim.shell_login_via(&client, 1);
            assert_ne!(client.session_id(), 0);
            for initialization in &backups {
                assert!(matches!(*initialization.result.borrow(), Some(Ok(()))));
            }
            if !replicated {
                let primary = [1u8, 2]
                    .into_iter()
                    .find(|&node| {
                        sim.replicas[usize::from(node)].shards[0]
                            .plane
                            .metadata()
                            .consensus
                            .as_ref()
                            .unwrap()
                            .is_primary()
                    })
                    .unwrap();
                let repaired = sim.replicas[usize::from(primary)]
                    .metadata_journal
                    .entry_sync(client.session_id())
                    .unwrap();
                assert!(repaired.header().op > 1);
                sim.replicas[0]
                    .metadata_journal
                    .append(repaired)
                    .now_or_never()
                    .unwrap()
                    .unwrap();
            }
            sim.replica_restart(0);
            let restarted = start_root_initialization(&mut sim, 0, false);
            for _ in 0..2_000 {
                sim.step();
                if restarted.result.borrow().is_some() {
                    break;
                }
            }
            assert!(matches!(*restarted.result.borrow(), Some(Ok(()))));
            for node in &sim.replicas {
                let header = node.metadata_journal.header(1).unwrap();
                assert_eq!(header.operation, Operation::CreateUser);
                assert_eq!(header.client, consensus::RESERVED_CLIENT_ID);
                assert_eq!(header.user_id, 0);
                if !replicated {
                    assert_ne!(
                        header.checksum, original_checksum,
                        "the discarded root prepare must be replaced on every replica"
                    );
                }
                for shard in &node.shards {
                    assert!(shard.plane.metadata().mux_stm.users().read(|users| {
                        let root = users.items.get(0).unwrap();
                        root.username.as_ref() == replica::SHELL_ROOT_USERNAME
                            && server_common::crypto::verify_password(
                                replica::SHELL_ROOT_PASSWORD,
                                &root.password_hash,
                            )
                            && users.items.len() == 1
                    }));
                }
            }
        }
    }

    #[test]
    fn given_a_solo_replica_without_credentials_when_initializing_root_should_fail_without_waiting()
    {
        let mut sim = Simulator::build_inner(
            1,
            1,
            std::iter::empty(),
            PacketSimulatorOptions::default(),
            true,
            None,
            false,
        );
        let initialization = start_root_initialization(&mut sim, 0, false);
        sim.step();
        assert!(matches!(
            *initialization.result.borrow(),
            Some(Err(
                server::server_error::ServerError::RootCredentialsRequired { .. }
            ))
        ));
        assert_eq!(sim.replicas[0].metadata_journal.last_op(), None);
    }

    #[test]
    fn given_root_initialization_when_shutdown_before_commit_should_exit_cleanly() {
        for with_credentials in [false, true] {
            let mut sim = Simulator::build_inner(
                3,
                1,
                std::iter::empty(),
                PacketSimulatorOptions {
                    node_count: 3,
                    ..Default::default()
                },
                true,
                None,
                false,
            );
            for backup in [1, 2] {
                sim.network
                    .link_filter_mut(ProcessId::Replica(backup), ProcessId::Replica(0))
                    .remove(Command::PrepareOk);
            }
            let initialization = start_root_initialization(&mut sim, 0, with_credentials);
            for _ in 0..20 {
                sim.step();
            }
            assert!(initialization.result.borrow().is_none());
            assert_eq!(
                sim.replicas[0].metadata_journal.last_op(),
                with_credentials.then_some(1)
            );
            initialization.shutdown.trigger();
            sim.step();
            assert!(matches!(*initialization.result.borrow(), Some(Ok(()))));
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_journaled_root_when_a_fresh_replica_needs_state_transfer_should_recover_without_credentials()
     {
        for chunk in [2, 128] {
            let directory = tempfile::tempdir().unwrap();
            let mut sim = Simulator::build_inner(
                3,
                1,
                std::iter::once(1),
                PacketSimulatorOptions {
                    node_count: 3,
                    client_count: 1,
                    seed: 4586,
                    ..Default::default()
                },
                true,
                Some(directory.path()),
                false,
            );
            for node in &sim.replicas {
                for shard in &node.shards {
                    shard.set_repair_chunk_max(chunk);
                }
            }
            sim.replica_crash(2);
            let initializations = [
                start_root_initialization(&mut sim, 0, true),
                start_root_initialization(&mut sim, 1, false),
            ];
            for _ in 0..200 {
                sim.step();
                if initializations
                    .iter()
                    .all(|initialization| initialization.result.borrow().is_some())
                {
                    break;
                }
            }
            for initialization in &initializations {
                assert!(matches!(*initialization.result.borrow(), Some(Ok(()))));
            }
            sim.set_metadata_journal_slots(80);
            let client = SimClient::new(1);
            sim.shell_login(&client);
            for sequence in 0..40 {
                let reply = submit_and_wait_for_reply(
                    &mut sim,
                    client.client_id(),
                    0,
                    client.create_stream(&format!("root-recovery-{sequence}")),
                );
                assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
                for _ in 0..40 {
                    sim.step();
                }
            }
            assert!(snapshot_floor(&sim, 0) > 1);
            assert!(sim.replicas[0].metadata_journal.header(1).is_none());
            sim.replica_restart(2);
            for shard in &sim.replicas[2].shards {
                shard.set_repair_chunk_max(chunk);
            }
            let rejoined = start_root_initialization(&mut sim, 2, false);
            for _ in 0..8_000 {
                sim.step();
                let checkpoint = directory.path().join("replica-2/metadata/snapshot.bin");
                if checkpoint.exists() {
                    assert!(
                        metadata::impls::metadata::IggySnapshot::load(&checkpoint)
                            .unwrap()
                            .0
                            .snapshot()
                            .sequence_number
                            > 0
                    );
                }
                if rejoined.result.borrow().is_some()
                    && committed_stream_names(&sim, 2) == committed_stream_names(&sim, 0)
                {
                    break;
                }
            }
            assert!(matches!(*rejoined.result.borrow(), Some(Ok(()))));
            assert!(sim.network.delivered_any(Command::RequestStateChunk));
            assert!(sim.network.delivered_any(Command::StateChunk));
            assert_eq!(
                committed_stream_names(&sim, 2),
                committed_stream_names(&sim, 0)
            );
            let root_identity = |node: usize| {
                sim.replicas[node].shards[0]
                    .plane
                    .metadata()
                    .mux_stm
                    .users()
                    .read(|users| {
                        assert_eq!(users.items.len(), 1);
                        let root = users.items.get(0).unwrap();
                        (
                            root.username.clone(),
                            root.password_hash.clone(),
                            root.created_at,
                        )
                    })
            };
            assert_eq!(root_identity(2), root_identity(0));
            sim.shell_login_via(&client, 2);
        }
    }

    const DISCONNECT_STREAM: &str = "sim-stream-0";
    const DISCONNECT_TOPIC: &str = "sim-topic-0-0";
    const DISCONNECT_GROUP: &str = "disconnect-recovery";
    const DISCONNECT_PROGRESS_STEPS: usize = 200;

    #[test]
    fn given_admission_mode_when_reply_is_originless_should_only_allow_consensus_mode() {
        const CLIENT: u128 = 7;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        for shell in [false, true] {
            let options = packet::PacketSimulatorOptions {
                node_count: 1,
                client_count: 1,
                ..packet::PacketSimulatorOptions::default()
            };
            let mut sim = Simulator::build(1, 1, std::iter::once(CLIENT), options, shell);
            let client = SimClient::new(CLIENT);
            if shell {
                sim.shell_login(&client);
            } else {
                sim.register_client_with_primary(&client);
            }
            let reply =
                submit_and_wait_for_reply(&mut sim, CLIENT, 0, client.create_stream("reply-mode"));
            assert_eq!(reply.header().client, CLIENT);
            assert_eq!(metadata::stm::result::result_code(reply.body()), Some(0));
            let bus = crate::bus::SharedSimOutbox(Rc::clone(&sim.outboxes[0]));
            bus.drain();
            let originless = bus
                .send_to_client(CLIENT, reply.deep_copy().into_frozen())
                .now_or_never();
            if shell {
                assert!(matches!(
                    originless,
                    Some(Err(message_bus::SendError::ClientNotFound(CLIENT)))
                ));
                assert!(bus.drain().is_empty());
                let delivered = bus
                    .send_to_client_instance(
                        CLIENT,
                        bus.client_instance(CLIENT),
                        reply.into_frozen().into(),
                    )
                    .now_or_never();
                assert!(matches!(delivered, Some(Ok(()))));
            } else {
                assert!(matches!(originless, Some(Ok(()))));
            }
            assert_eq!(bus.drain().len(), 1);
        }
    }

    #[test]
    fn given_concurrent_simulators_when_one_restarts_should_keep_only_its_system_files() {
        let options = packet::PacketSimulatorOptions {
            node_count: 1,
            client_count: 0,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut first = Simulator::with_shards_shell(1, 1, std::iter::empty(), options.clone());
        let second = Simulator::with_shards_shell(1, 1, std::iter::empty(), options);
        let first_path = first.replicas[0].system_path.clone();
        let second_path = second.replicas[0].system_path.clone();
        assert_ne!(first_path, second_path);
        assert!(first.replicas[0].data_dir.is_none());
        assert!(
            first.replicas[0].shards[0]
                .plane
                .metadata()
                .coordinator
                .is_none()
        );
        let marker = first_path.join("retained-fence");
        std::fs::write(&marker, b"first simulator").unwrap();
        assert!(!second_path.join("retained-fence").exists());

        first.replica_crash(0);
        first.replica_restart(0);
        assert_eq!(first.replicas[0].system_path, first_path);
        assert_eq!(std::fs::read(&marker).unwrap(), b"first simulator");
        assert!(
            first.replicas[0].shards[0]
                .plane
                .metadata()
                .coordinator
                .is_none()
        );
        drop(first);
        assert!(!first_path.exists());
        assert!(second_path.exists());
        drop(second);
        assert!(!second_path.exists());
    }

    #[test]
    fn given_checkpoint_root_when_simulator_stops_should_retain_its_system_directory() {
        let root = tempfile::tempdir().unwrap();
        let options = packet::PacketSimulatorOptions {
            node_count: 1,
            client_count: 0,
            ..packet::PacketSimulatorOptions::default()
        };
        let sim = Simulator::with_checkpoints(1, std::iter::empty(), options, true, root.path());
        let system_path = root.path().join("replica-0");
        assert_eq!(sim.replicas[0].system_path, system_path);
        assert_eq!(sim.replicas[0].data_dir.as_ref(), Some(&system_path));
        assert!(
            sim.replicas[0].shards[0]
                .plane
                .metadata()
                .coordinator
                .is_some()
        );
        drop(sim);
        assert!(system_path.exists());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_partial_delete_when_primary_restarts_and_reply_is_lost_should_resume_the_same_intent()
    {
        const CLIENT: u128 = 1;
        const PROGRESS_STEPS: usize = 4_000;
        fn block_second_partition(packet: &packet::Packet) -> bool {
            packet.message.header().command == Command::Prepare
                && bytemuck::checked::from_bytes::<PrepareHeader>(
                    &packet.message.as_slice()[..size_of::<PrepareHeader>()],
                )
                .group
                    == IggyNamespace::new(0, 0, 1).inner()
        }
        fn drop_reply(packet: &packet::Packet) -> bool {
            packet.message.header().command == Command::Reply
        }
        let (mut sim, client) = cluster(0x5055_5247);
        let first = IggyNamespace::new(0, 0, 0);
        let second = IggyNamespace::new(0, 0, 1);
        sim.init_partition(first);
        sim.init_partition(second);
        sim.register_client_with_primary(&client);
        let old_send = client.send_messages(first, &[Bytes::from_static(b"retired")]);
        let old_retry = old_send.deep_copy();
        assert_eq!(
            submit_and_wait_for_reply(&mut sim, CLIENT, 0, old_send)
                .header()
                .status,
            0
        );
        for backup in 1..sim.replica_count {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(backup)) =
                Some(block_second_partition);
        }
        let delete = client.delete_topic(DISCONNECT_STREAM, DISCONNECT_TOPIC);
        let response = submit_and_wait_for_reply(&mut sim, CLIENT, 0, delete.deep_copy());
        assert_eq!(
            metadata::stm::result::result_code(response.body()),
            Some(IggyError::TransientNotCommitted.as_code())
        );
        let intent_op = response.header().op;
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.replicas[0].shards[0]
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .pending_lifecycles()
                    .first()
                    .is_some_and(|intent| {
                        intent.partitions[0].partition_op.is_some()
                            && intent.partitions[1].partition_op.is_none()
                    })
            }),
            "the first partition must complete while the second lacks quorum"
        );
        // Metadata still serves the topic here, so only the partition fence can refuse.
        let primary = sim.primary_index(first).unwrap();
        let response = submit_and_wait_for_reply(&mut sim, CLIENT, primary, old_retry);
        assert_eq!(
            response.header().status,
            IggyError::HistoryUnavailable.as_code()
        );
        sim.replica_crash(0);
        sim.replica_restart(0);
        let restored = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .pending_lifecycles();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].context.metadata_op, intent_op);
        assert_eq!(restored[0].context.request, delete.header().request);
        // The metadata report may need replay; the completed partition fence must survive.
        assert!(
            sim.replicas[0]
                .partition_shard(first)
                .plane
                .partitions()
                .get_by_ns(&first)
                .unwrap()
                .history_deleted()
        );
        assert!(restored[0].partitions[1].partition_op.is_none());
        for replica in 0..sim.replica_count {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(replica), ProcessId::Client(CLIENT)) =
                Some(drop_reply);
            if replica != 0 {
                *sim.network
                    .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(replica)) = None;
            }
        }
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.replicas.iter().all(|replica| {
                    let streams = replica.shards[0].plane.metadata().mux_stm.streams();
                    !streams.lifecycle_pending(intent_op)
                        && streams.created_revision_for_namespace(first).is_none()
                        && streams.created_revision_for_namespace(second).is_none()
                })
            }),
            "the surviving intent must finish every partition without the admin reply"
        );
        for replica in 0..sim.replica_count {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(replica), ProcessId::Client(CLIENT)) = None;
        }
        let primary = sim.metadata_primary_index().unwrap();
        let response = submit_and_wait_for_reply(&mut sim, CLIENT, primary, delete);
        assert_eq!(response.header().status, 0);
        assert_eq!(metadata::stm::result::result_code(response.body()), Some(0));
        assert_eq!(response.header().op, intent_op);
        for replica in &sim.replicas {
            for namespace in [first, second] {
                let partition = replica
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)
                    .unwrap();
                assert!(partition.history_deleted());
            }
        }
    }

    /// Only partition replicas can prove a lifecycle fence, so a metadata primary
    /// without one needs a backup to relay the completion.
    #[test]
    fn given_metadata_primary_without_partition_replica_when_topic_is_deleted_should_finish_the_delete()
     {
        const PROGRESS_STEPS: usize = 4_000;
        let (mut sim, client) = cluster(0x4643_4F4D);
        let namespace = IggyNamespace::new(0, 0, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        fence_partition_on_metadata_primary(&mut sim, namespace);

        let delete = client.delete_topic(DISCONNECT_STREAM, DISCONNECT_TOPIC);
        let response = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, delete);
        let code = metadata::stm::result::result_code(response.body());
        assert!(
            code == Some(0) || code == Some(IggyError::TransientNotCommitted.as_code()),
            "unexpected result code {code:?}"
        );
        let intent_op = response.header().op;
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.replicas.iter().all(|replica| {
                    let streams = replica.shards[0].plane.metadata().mux_stm.streams();
                    !streams.lifecycle_pending(intent_op)
                        && streams.created_revision_for_namespace(namespace).is_none()
                })
            }),
            "the delete must finish while the metadata primary holds no partition replica"
        );
        assert!(sim.network.delivered_any(Command::ForwardCompletion));
        assert_eq!(
            sim.metadata_primary_index(),
            Some(0),
            "the metadata primary is the replica without the partition"
        );
        assert_eq!(
            sim.metadata_view(),
            Some(0),
            "the delete must finish without a metadata view change"
        );
    }

    /// Fences `namespace` on replica 0, the metadata primary, as boot recovery
    /// does with unreadable files, and waits until a replica that serves the
    /// partition leads it.
    fn fence_partition_on_metadata_primary(sim: &mut Simulator, namespace: IggyNamespace) {
        const FAILOVER_STEPS: usize = 2_000;
        let revision = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .expect("the partition is committed");
        sim.replicas[0]
            .partition_shard(namespace)
            .plane
            .partitions()
            .fence(namespace, revision);
        assert!(
            (0..FAILOVER_STEPS).any(|_| {
                sim.step();
                matches!(sim.primary_index(namespace), Some(1 | 2))
            }),
            "the partition must fail over to a replica that serves it"
        );
    }

    /// `commit_message_ticks` no test outlives. Set after `init`, it also stops
    /// the running timer, so no `Commit` heartbeat fires again.
    pub const NO_COMMIT_HEARTBEAT: u64 = u64::MAX;
    /// One tick to emit the eager `Commit`, then the default network's one-way
    /// delay (at least 1, exponential with mean 3) with room for its tail.
    const EAGER_COMMIT_TICKS: usize = 16;

    /// A partition primary on a metadata backup acts on a metadata op only once
    /// it learns the commit, so the backups must not wait for the heartbeat.
    #[test]
    fn given_no_commit_heartbeat_when_metadata_commits_should_reach_every_backup_within_a_few_ticks()
     {
        let (mut sim, client) = cluster(0x4541_4745);
        sim.register_client_with_primary(&client);
        for replica in 0..sim.replica_count {
            sim.metadata_consensus(usize::from(replica))
                .expect("every replica runs the metadata plane")
                .set_commit_message_ticks(NO_COMMIT_HEARTBEAT);
        }

        let reply = submit_and_wait_for_reply(
            &mut sim,
            client.client_id(),
            0,
            client.create_stream("eager-commit"),
        );
        assert_eq!(metadata::stm::result::result_code(reply.body()), Some(0));
        let op = reply.header().op;
        assert!(
            (0..EAGER_COMMIT_TICKS).any(|_| {
                sim.step();
                (0..sim.replica_count).all(|replica| {
                    sim.metadata_consensus(usize::from(replica))
                        .is_some_and(|consensus| consensus.commit_max() >= op)
                })
            }),
            "every backup must learn metadata op {op} within {EAGER_COMMIT_TICKS} ticks"
        );
    }

    /// A delete completes only once its reporter commits the partition fence
    /// locally, and the metadata primary is often a backup of the partition.
    #[test]
    fn given_no_commit_heartbeat_when_a_partition_fence_commits_should_reach_every_backup_within_a_few_ticks()
     {
        const FENCE_STEPS: usize = 400;
        let (mut sim, client) = cluster(0x4645_4E43);
        let namespace = IggyNamespace::new(0, 0, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        let commit_max = |sim: &Simulator, replica: u8| {
            sim.replicas[usize::from(replica)]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .map(|partition| partition.consensus().commit_max())
        };
        for replica in &sim.replicas {
            replica
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .expect("every replica hosts the partition")
                .consensus()
                .set_commit_message_ticks(NO_COMMIT_HEARTBEAT);
        }

        let delete = client.delete_topic(DISCONNECT_STREAM, DISCONNECT_TOPIC);
        let _ = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, delete);
        let primary = sim
            .primary_index(namespace)
            .expect("the partition has a primary");
        let fenced = |sim: &Simulator| {
            sim.replicas[usize::from(primary)]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .is_some_and(IggyPartition::history_deleted)
        };
        for _ in 0..FENCE_STEPS {
            if fenced(&sim) {
                break;
            }
            sim.step();
        }
        assert!(
            fenced(&sim),
            "the partition primary must commit the delete fence"
        );
        let fence_op = commit_max(&sim, primary).expect("the primary hosts the partition");
        assert!(
            (0..EAGER_COMMIT_TICKS).any(|_| {
                sim.step();
                (0..sim.replica_count).all(|replica| {
                    commit_max(&sim, replica).is_some_and(|commit| commit >= fence_op)
                })
            }),
            "every backup must learn the fence commit {fence_op} within {EAGER_COMMIT_TICKS} ticks"
        );
    }

    /// Topic ids are reused after a delete, so the recreated topic's delete fence
    /// targets the namespace of a partition materialized for the deleted incarnation.
    #[test]
    fn given_recreated_topic_when_deleted_should_fence_the_recreated_incarnation() {
        const STREAM: &str = "recreated-stream";
        const TOPIC: &str = "recreated-topic";
        const PROGRESS_STEPS: usize = 4_000;
        fn incarnation(sim: &Simulator) -> (IggyNamespace, u64) {
            let streams = sim.replicas[0].shards[0].plane.metadata().mux_stm.streams();
            let namespace = streams
                .namespace_from_partition(
                    &WireIdentifier::named(STREAM).unwrap(),
                    &WireIdentifier::named(TOPIC).unwrap(),
                    0,
                )
                .unwrap();
            (
                namespace,
                streams.created_revision_for_namespace(namespace).unwrap(),
            )
        }
        // Two shards, so the partition can live off shard 0 and reconcile has to
        // look it up on its hash shard.
        const SHARDS_PER_REPLICA: u16 = 2;
        let (mut sim, client) = sharded_cluster(0x5245_4352, SHARDS_PER_REPLICA);
        sim.register_client_with_primary(&client);
        complete_metadata_op(&mut sim, &client, client.create_stream(STREAM));
        complete_metadata_op(&mut sim, &client, client.create_topic(STREAM, TOPIC, 1));
        let (namespace, deleted) = incarnation(&sim);
        assert_ne!(
            calculate_shard_assignment(&namespace, u32::from(SHARDS_PER_REPLICA)),
            0,
            "the partition must live off shard 0"
        );
        complete_metadata_op(&mut sim, &client, client.delete_topic(STREAM, TOPIC));
        complete_metadata_op(&mut sim, &client, client.create_topic(STREAM, TOPIC, 1));
        let (recreated_namespace, recreated) = incarnation(&sim);
        assert_eq!(recreated_namespace, namespace);
        assert_ne!(recreated, deleted);
        complete_metadata_op(&mut sim, &client, client.delete_topic(STREAM, TOPIC));
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.replicas.iter().all(|replica| {
                    replica
                        .partition_shard(namespace)
                        .plane
                        .partitions()
                        .get_by_ns(&namespace)
                        .is_some_and(|partition| {
                            partition.created_revision() == recreated && partition.history_deleted()
                        })
                })
            }),
            "every replica must fence the recreated incarnation"
        );
    }

    /// A client keeps the route it captured before a `DeleteStream`. Only a
    /// partition's primary orders its delete fence, so while that primary, a
    /// metadata backup, has not applied the intent, the route still commits into
    /// the old incarnation, ahead of the fence. The recreated stream serves
    /// neither that write nor a later one that still names the old route.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_stale_route_while_the_primary_lags_a_stream_delete_when_writing_should_commit_only_before_the_old_fence()
     {
        const STREAM: &str = "stale-route-stream";
        const TOPIC: &str = "stale-route-topic";
        const METADATA_PRIMARY: u8 = 0;
        const PROGRESS_STEPS: usize = 4_000;
        /// Room for a refused write to commit anyway before the oracle reads.
        const FOREIGN_WRITE_STEPS: usize = 200;
        const CONSUMER_ID: usize = 1;
        const POLL_COUNT: u32 = 16;
        const IN_WINDOW: &[u8] = b"stale-route-in-window";
        const AFTER_RECREATE: &[u8] = b"stale-route-after-recreate";

        /// The frames that would teach the partition primary about the delete.
        fn withhold_metadata(packet: &packet::Packet) -> bool {
            prepare_for(packet, METADATA_GROUP).is_some() || is_commit_for(packet, METADATA_GROUP)
        }
        fn namespace_of(sim: &Simulator, partition: u32) -> Option<IggyNamespace> {
            sim.replicas[usize::from(METADATA_PRIMARY)].shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .namespace_from_partition(
                    &WireIdentifier::named(STREAM).unwrap(),
                    &WireIdentifier::named(TOPIC).unwrap(),
                    partition,
                )
        }
        fn applied_metadata(sim: &Simulator, replica: u8) -> u64 {
            sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata()
                .applied_frontier()
                .get()
        }
        fn fenced(
            sim: &Simulator,
            replica: usize,
            namespace: IggyNamespace,
            incarnation: u64,
        ) -> bool {
            sim.replicas[replica]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .is_some_and(|partition| {
                    partition.created_revision() == incarnation && partition.history_deleted()
                })
        }
        fn retained_log(
            sim: &Simulator,
            replica: usize,
            namespace: IggyNamespace,
        ) -> BTreeMap<u64, PrepareHeader> {
            sim.partition_journaled_headers(replica, namespace, 0..=u64::MAX)
                .expect("the replica hosts the partition")
        }
        fn serving(sim: &Simulator, namespaces: &[IggyNamespace]) -> bool {
            sim.incarnation_settled(0, namespaces)
                && namespaces.iter().all(|&namespace| {
                    (0..usize::from(sim.replica_count)).all(|replica| {
                        sim.partition_consensus_state(replica, namespace)
                            .is_some_and(|state| state.status == Status::Normal)
                    })
                })
        }

        let (mut sim, client) = cluster(0x5354_414C);
        let replicas = usize::from(sim.replica_count);
        let partitions = u32::from(sim.replica_count);
        assert_eq!(sim.metadata_primary_index(), Some(METADATA_PRIMARY));
        sim.register_client_with_primary(&client);
        complete_metadata_op(&mut sim, &client, client.create_stream(STREAM));
        complete_metadata_op(
            &mut sim,
            &client,
            client.create_topic(STREAM, TOPIC, partitions),
        );
        let namespaces: Vec<_> = (0..partitions)
            .map(|partition| namespace_of(&sim, partition).expect("the topic has the partition"))
            .collect();
        for &namespace in &namespaces {
            sim.init_partition(namespace);
        }
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                serving(&sim, &namespaces)
            }),
            "every replica must serve the created partitions"
        );
        // Spread placement leaves some partition primary on a metadata backup.
        let (partition, namespace, lagging) = (0..partitions)
            .zip(namespaces.iter().copied())
            .find_map(|(partition, namespace)| {
                sim.primary_index(namespace)
                    .filter(|&primary| primary != METADATA_PRIMARY)
                    .map(|primary| (partition, namespace, primary))
            })
            .expect("a partition primary must sit on a metadata backup");
        let old = sim
            .partition_incarnation(usize::from(lagging), namespace)
            .expect("the partition primary hosts the partition");
        sim.discover_partition_contexts(&client);

        *sim.network.link_drop_packet_fn(
            ProcessId::Replica(METADATA_PRIMARY),
            ProcessId::Replica(lagging),
        ) = Some(withhold_metadata);
        let delete = submit_and_wait_for_reply(
            &mut sim,
            client.client_id(),
            METADATA_PRIMARY,
            client.delete_stream(STREAM),
        );
        assert_eq!(
            metadata::stm::result::result_code(delete.body()),
            Some(IggyError::TransientNotCommitted.as_code()),
            "the delete intent must commit without the lagging partition primary"
        );
        let intent_op = delete.header().op;

        let write = client.send_messages(namespace, &[Bytes::from_static(IN_WINDOW)]);
        let write_request = write.header().request;
        let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), lagging, write);
        assert_eq!(
            reply.header().status,
            0,
            "the stale route must still commit"
        );
        assert!(
            applied_metadata(&sim, lagging) < intent_op,
            "the write must commit before its primary applies the delete intent"
        );
        for replica in 0..replicas {
            assert!(
                !fenced(&sim, replica, namespace, old),
                "replica {replica} installed a fence the lagging primary never ordered"
            );
        }

        *sim.network.link_drop_packet_fn(
            ProcessId::Replica(METADATA_PRIMARY),
            ProcessId::Replica(lagging),
        ) = None;
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                (0..replicas).all(|replica| fenced(&sim, replica, namespace, old))
            }),
            "every replica must install the old incarnation's fence"
        );
        for replica in 0..replicas {
            let log = retained_log(&sim, replica, namespace);
            assert!(
                log.values()
                    .all(|header| header.partition_incarnation == old),
                "replica {replica} logged a prepare from another incarnation"
            );
            let write_op = log
                .values()
                .find(|header| {
                    header.operation == Operation::SendMessages
                        && header.client == client.client_id()
                        && header.request == write_request
                })
                .map(|header| header.op)
                .expect("the write must commit in the old incarnation");
            let fences: Vec<u64> = log
                .values()
                .filter(|header| header.operation == Operation::TransitionPartitionHistory)
                .map(|header| header.op)
                .collect();
            assert_eq!(
                fences.len(),
                1,
                "replica {replica} must log one delete fence"
            );
            assert!(
                write_op < fences[0],
                "replica {replica} ordered the write after the fence"
            );
            assert_eq!(
                log.keys().next_back(),
                Some(&fences[0]),
                "replica {replica} logged an op after the fence"
            );
        }
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.replicas.iter().all(|replica| {
                    let streams = replica.shards[0].plane.metadata().mux_stm.streams();
                    !streams.lifecycle_pending(intent_op)
                        && streams.created_revision_for_namespace(namespace).is_none()
                })
            }),
            "the delete must complete once the lagging primary fences"
        );

        // The production reconciler tears a deleted incarnation down before it
        // builds the recreated one. The simulator does that only for fixtures.
        for replica in &sim.replicas {
            replica
                .partition_shard(namespace)
                .plane
                .partitions()
                .remove(&namespace);
        }
        complete_metadata_op(&mut sim, &client, client.create_stream(STREAM));
        complete_metadata_op(
            &mut sim,
            &client,
            client.create_topic(STREAM, TOPIC, partitions),
        );
        assert_eq!(
            namespace_of(&sim, partition),
            Some(namespace),
            "the recreated stream must reuse the deleted ids"
        );
        sim.init_partition(namespace);
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                serving(&sim, &[namespace])
            }),
            "every replica must serve the recreated partition"
        );
        let recreated = sim
            .partition_incarnation(usize::from(METADATA_PRIMARY), namespace)
            .expect("the metadata primary hosts the recreated partition");
        assert_ne!(recreated, old);

        // The client never rediscovered, so this write still names `old`.
        let target = sim
            .primary_index(namespace)
            .expect("the recreated partition has a primary");
        let foreign = client.send_messages(namespace, &[Bytes::from_static(AFTER_RECREATE)]);
        let foreign_request = foreign.header().request;
        sim.submit_request(client.client_id(), target, foreign.into_generic());
        let mut foreign_status = None;
        for _ in 0..FOREIGN_WRITE_STEPS {
            if let Some(reply) = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == foreign_request)
            {
                foreign_status = Some(reply.header().status);
            }
        }
        for replica in 0..replicas {
            assert_eq!(
                sim.partition_incarnation(replica, namespace),
                Some(recreated)
            );
            assert!(
                !retained_log(&sim, replica, namespace)
                    .values()
                    .any(|header| header.operation == Operation::SendMessages),
                "replica {replica} logged a write in the recreated partition"
            );
        }
        let poll = sim.poll_messages(
            usize::from(target),
            namespace,
            PollingConsumer::Consumer(CONSUMER_ID, namespace.partition_id()),
            &PollingArgs::new(iggy_common::PollingStrategy::offset(0), POLL_COUNT, false),
        );
        let polled = complete_now(&mut sim, poll).expect("the recreated primary serves the poll");
        assert!(
            polled.is_empty(),
            "the recreated partition must read back empty"
        );
        assert_eq!(
            foreign_status,
            Some(IggyError::HistoryUnavailable.as_code()),
            "the old route must be refused by the recreated partition"
        );
    }

    /// A replica restarted while a fixture topic is deleted keeps the deleted
    /// incarnation's store, which the recreated incarnation must not adopt.
    #[test]
    fn given_a_restart_between_fixture_delete_and_recreate_when_recreated_should_start_empty() {
        const SENDS: usize = 3;
        const PROGRESS_STEPS: usize = 4_000;
        const RESTARTED: u8 = 2;
        let (mut sim, client) = cluster(0x5245_5354);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        sim.register_client_with_primary(&client);
        let stream = Streams::seeded_stream_name(fixture.stream_id());
        let topic = Streams::seeded_topic_name(fixture.stream_id(), fixture.topic_id());
        for index in 0..SENDS {
            let send = client.send_messages(fixture, &[Bytes::from(format!("deleted-{index}"))]);
            let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, send);
            assert_eq!(reply.header().status, 0, "send {index} must commit");
        }
        complete_metadata_op(&mut sim, &client, client.delete_topic(&stream, &topic));
        // A later prepare carries the completion's commit point, so the restarted
        // replica replays the topic as deleted and materializes nothing.
        complete_metadata_op(&mut sim, &client, client.create_stream("wl-after-delete"));
        sim.replica_crash(RESTARTED);
        sim.replica_restart(RESTARTED);
        assert_eq!(
            sim.partition_incarnation(usize::from(RESTARTED), fixture),
            None,
            "the restarted replica must replay the delete"
        );

        complete_metadata_op(&mut sim, &client, client.create_topic(&stream, &topic, 1));
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.incarnation_settled(0, &[fixture])
            }),
            "every replica must materialize the recreated incarnation"
        );
        assert_eq!(
            sim.offsets(usize::from(RESTARTED), fixture)
                .map(|offsets| offsets.commit_offset),
            sim.offsets(0, fixture).map(|offsets| offsets.commit_offset),
            "the restarted replica started the recreated incarnation from the deleted \
             incarnation's log"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_restarted_primary_with_discarded_send_when_next_send_arrives_should_rejoin_quorum() {
        const PROGRESS_STEPS: usize = 4_000;
        const OLD_PRIMARY: u8 = 0;
        const NEW_PRIMARY: u8 = 1;
        const OTHER_BACKUP: u8 = 2;
        const RETAINED: &[u8] = b"retained-prefix";
        const DISCARDED: &[u8] = b"discarded-suffix";
        const REPLACEMENT: &[u8] = b"replacement-send";
        for committed_prefix in [false, true] {
            let (mut sim, client) = cluster(0x5245_414E);
            let namespace = IggyNamespace::new(1, 1, 0);
            sim.init_partition(namespace);
            sim.register_client_with_primary(&client);
            let registered_op = metadata_commit(&sim, usize::from(OLD_PRIMARY));
            let committed_op = if committed_prefix {
                let send = client.send_messages(namespace, &[Bytes::from_static(RETAINED)]);
                let reply =
                    submit_and_wait_for_reply(&mut sim, client.client_id(), OLD_PRIMARY, send);
                assert_eq!(reply.header().status, 0);
                reply.header().commit
            } else {
                0
            };
            assert!(
                (0..PROGRESS_STEPS).any(|_| {
                    sim.step();
                    (0..3).all(|replica| {
                        metadata_commit(&sim, replica) >= registered_op
                            && sim
                                .partition_consensus_state(replica, namespace)
                                .is_some_and(|state| state.commit_min == committed_op)
                    })
                }),
                "every replica must commit the registration and prefix before isolating the send"
            );
            for backup in [NEW_PRIMARY, OTHER_BACKUP] {
                sim.network
                    .link_filter_mut(ProcessId::Replica(OLD_PRIMARY), ProcessId::Replica(backup))
                    .remove(Command::Prepare);
            }
            let send = client.send_messages(namespace, &[Bytes::from_static(DISCARDED)]);
            sim.submit_request(client.client_id(), OLD_PRIMARY, send.into_generic());
            assert!(
                (0..PROGRESS_STEPS).any(|_| {
                    sim.step();
                    sim.partition_journaled_headers(
                        usize::from(OLD_PRIMARY),
                        namespace,
                        committed_op + 1..=committed_op + 1,
                    )
                    .is_some_and(|headers| headers.contains_key(&(committed_op + 1)))
                }),
                "the old primary must retain the uncommitted send before crashing"
            );
            assert_eq!(
                sim.partition_consensus_state(usize::from(OLD_PRIMARY), namespace)
                    .expect("old primary hosts the partition")
                    .commit_min,
                committed_op
            );
            sim.replica_crash(OLD_PRIMARY);
            assert!(
                (0..PROGRESS_STEPS).any(|_| {
                    sim.step();
                    sim.partition_consensus_state(usize::from(NEW_PRIMARY), namespace)
                        .is_some_and(|state| {
                            state.status == Status::Normal && state.is_primary && state.view > 0
                        })
                }),
                "the remaining replicas must elect a primary without the isolated send"
            );
            for backup in [NEW_PRIMARY, OTHER_BACKUP] {
                sim.network
                    .link_filter_mut(ProcessId::Replica(OLD_PRIMARY), ProcessId::Replica(backup))
                    .insert(Command::Prepare);
            }
            sim.replica_restart(OLD_PRIMARY);
            assert!(
                (0..PROGRESS_STEPS).any(|_| {
                    sim.step();
                    sim.partition_consensus_state(usize::from(OLD_PRIMARY), namespace)
                        .is_some_and(|state| state.status == Status::Normal && state.view > 0)
                        && sim
                            .partition_journaled_headers(
                                usize::from(OLD_PRIMARY),
                                namespace,
                                committed_op + 1..=committed_op + 1,
                            )
                            .is_some_and(|headers| headers.is_empty())
                }),
                "StartView must discard the old primary's uncommitted suffix"
            );
            sim.replica_crash(OTHER_BACKUP);
            let send = client.send_messages(namespace, &[Bytes::from_static(REPLACEMENT)]);
            let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), NEW_PRIMARY, send);
            assert_eq!(
                reply.header().status,
                0,
                "the restarted replica must acknowledge the replacement"
            );
            let restored = retained_prepare(
                &sim,
                usize::from(OLD_PRIMARY),
                namespace,
                reply.header().commit,
            );
            assert!(
                restored
                    .body()
                    .windows(REPLACEMENT.len())
                    .any(|bytes| bytes == REPLACEMENT)
            );
            assert!(
                !restored
                    .body()
                    .windows(DISCARDED.len())
                    .any(|bytes| bytes == DISCARDED)
            );
        }
    }

    /// A replica that restarts again before its metadata reaches a recreated
    /// fixture topic still owns the log of the incarnation it hosted.
    #[test]
    fn given_two_restarts_before_a_recreated_fixture_is_hosted_when_hosted_should_keep_its_log() {
        const SENDS: usize = 3;
        const PROGRESS_STEPS: usize = 4_000;
        const RESTARTED: u8 = 2;
        let (mut sim, client) = cluster(0x4C4F_4753);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        sim.register_client_with_primary(&client);
        let stream = Streams::seeded_stream_name(fixture.stream_id());
        let topic = Streams::seeded_topic_name(fixture.stream_id(), fixture.topic_id());
        complete_metadata_op(&mut sim, &client, client.delete_topic(&stream, &topic));
        complete_metadata_op(&mut sim, &client, client.create_topic(&stream, &topic, 1));
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.incarnation_settled(0, &[fixture])
            }),
            "every replica must materialize the recreated incarnation"
        );
        sim.discover_partition_contexts(&client);
        for index in 0..SENDS {
            let send = client.send_messages(fixture, &[Bytes::from(format!("recreated-{index}"))]);
            let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, send);
            assert_eq!(reply.header().status, 0, "send {index} must commit");
        }
        let committed = |sim: &Simulator, replica_idx: usize| {
            sim.offsets(replica_idx, fixture)
                .map(|offsets| offsets.commit_offset)
        };
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                committed(&sim, usize::from(RESTARTED)) == committed(&sim, 0)
            }),
            "the replica to restart must hold every send first"
        );

        // No prepare carries the create's commit point yet, so each replay stops
        // short of it and the replica materializes nothing.
        for _ in 0..2 {
            sim.replica_crash(RESTARTED);
            sim.replica_restart(RESTARTED);
            assert_eq!(
                sim.partition_incarnation(usize::from(RESTARTED), fixture),
                None,
                "the replay must stop before the recreate"
            );
        }

        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.incarnation_settled(0, &[fixture])
            }),
            "the restarted replica must materialize the recreated incarnation"
        );
        assert_eq!(
            committed(&sim, usize::from(RESTARTED)),
            committed(&sim, 0),
            "the second restart dropped the log the first one retained"
        );
    }

    /// A restart can replay metadata that names an incarnation older than a
    /// group's store. Materializing waits for the metadata, keeping the store.
    #[test]
    fn given_a_store_of_a_newer_incarnation_when_metadata_names_an_older_one_should_keep_it() {
        let (mut sim, _) = cluster(0x4E45_5752);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        let replica = &sim.replicas[0];
        let named = replica.shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(fixture)
            .expect("the seed creates the fixture");
        replica
            .partition_shard(fixture)
            .plane
            .partitions()
            .remove(&fixture);
        let newer = named + 1;
        replica
            .partition_superblocks
            .borrow_mut()
            .get_mut(&fixture)
            .expect("materializing stores the group")
            .0 = newer;

        assert!(
            !materialise_partition(replica, fixture, sim.consumer_offsets_max),
            "the replica materialized an incarnation older than its store"
        );
        assert_eq!(replica.partition_superblocks.borrow()[&fixture].0, newer);
    }

    /// A recreated fixture topic starts its offsets again at its chosen
    /// creation view, which the per-tick marks must not read as a regression.
    #[test]
    fn given_a_recreated_fixture_when_checked_should_start_its_marks_again() {
        const SENDS: usize = 3;
        const PROGRESS_STEPS: usize = 4_000;
        const SEED: u64 = 0x4D41_524B;

        // Link hooks are bare `fn` pointers, so the group travels in a static.
        static GROUP: AtomicU64 = AtomicU64::new(0);

        fn starve_commits(packet: &packet::Packet) -> bool {
            is_commit_for(packet, GROUP.load(Ordering::Relaxed))
        }

        let (mut sim, client) = cluster(SEED);
        let fixture = IggyNamespace::new(1, 1, 0);
        GROUP.store(fixture.inner(), Ordering::Relaxed);
        sim.init_partition(fixture);
        sim.register_client_with_primary(&client);
        let workload = crate::workload::Workload::new(
            crate::workload::options::WorkloadOptions::new(SEED, sim.replica_count, vec![fixture]),
        );
        let mut invariants = crate::workload::invariants::Invariants::new();
        commit_sends_everywhere(&mut sim, &client, fixture, SENDS);
        // Without commits from its primary the partition alone changes view, so
        // the recreated incarnation starts below it.
        for backup in 1..sim.replica_count {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(backup)) =
                Some(starve_commits);
        }
        let replicas = 0..usize::from(sim.replica_count);
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                replicas
                    .clone()
                    .all(|replica_idx| sim.consensus_view(replica_idx, fixture) > Some(0))
            }),
            "starved commits must move the partition to a later view"
        );
        for backup in 1..sim.replica_count {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(backup)) = None;
        }
        invariants.check(&sim, &workload);
        let deleted_views: Vec<Option<u64>> = replicas
            .clone()
            .map(|replica_idx| sim.consensus_view(replica_idx, fixture))
            .collect();

        let stream = Streams::seeded_stream_name(fixture.stream_id());
        let topic = Streams::seeded_topic_name(fixture.stream_id(), fixture.topic_id());
        complete_metadata_op(&mut sim, &client, client.delete_topic(&stream, &topic));
        complete_metadata_op(&mut sim, &client, client.create_topic(&stream, &topic, 1));
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.incarnation_settled(0, &[fixture])
            }),
            "every replica must materialize the recreated incarnation"
        );
        assert!(
            replicas
                .zip(deleted_views)
                .all(|(replica_idx, deleted)| sim.consensus_view(replica_idx, fixture) < deleted),
            "the recreated incarnation must start below the deleted one's view"
        );
        invariants.check(&sim, &workload);
    }

    /// Production refuses a `StartView` that names another prepare at an applied
    /// op and keeps running; a simulated run fails on it, since a quorum decided a
    /// log without a committed op.
    #[test]
    #[should_panic(expected = "contradict an op it applied")]
    fn given_a_start_view_that_contradicts_an_applied_op_when_checked_should_fail_the_run() {
        const SEED: u64 = 0x4150_504C;
        const BACKUP: usize = 1;
        let (mut sim, client) = cluster(SEED);
        sim.register_client_with_primary(&client);
        complete_metadata_op(&mut sim, &client, client.create_stream("applied"));

        let metadata = sim.replicas[BACKUP].shards[0].plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 runs the metadata consensus");
        check_after_refusing_a_contradiction(&sim, SEED, consensus, PlaneKind::Metadata, |op| {
            metadata.applied_header(op)
        });
    }

    #[test]
    #[should_panic(expected = "contradict an op it applied")]
    fn given_a_partition_start_view_that_contradicts_an_applied_op_when_checked_should_fail_the_run()
     {
        const SEED: u64 = 0x5041_5254;
        const SENDS: usize = 2;
        let (mut sim, client) = cluster(SEED);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        sim.register_client_with_primary(&client);
        commit_sends_everywhere(&mut sim, &client, fixture, SENDS);

        let primary = sim
            .primary_index(fixture)
            .expect("the fixture has a primary");
        let backup = (0..sim.replica_count)
            .find(|replica| *replica != primary)
            .expect("the cluster has a backup");
        let partitions = sim.replicas[usize::from(backup)]
            .partition_shard(fixture)
            .plane
            .partitions();
        let partition = partitions
            .get_by_ns(&fixture)
            .expect("every replica hosts the fixture");
        check_after_refusing_a_contradiction(
            &sim,
            SEED,
            partition.consensus(),
            PlaneKind::Partitions,
            |op| partition.applied_header(op),
        );
    }

    /// Offer `consensus` a `StartView` of a view it does not lead whose log names
    /// another prepare at its commit point, assert the refusal, then check the
    /// invariants.
    fn check_after_refusing_a_contradiction(
        sim: &Simulator,
        seed: u64,
        consensus: &consensus::VsrConsensus<crate::bus::SharedSimOutbox>,
        plane: PlaneKind,
        applied_at: impl Fn(u64) -> Option<PrepareHeader>,
    ) {
        let applied_op = consensus.commit_min();
        assert_eq!(
            consensus.commit_max(),
            applied_op,
            "the backup is caught up"
        );
        let mut forged = applied_at(applied_op).expect("the backup journals the op it applied");
        forged.timestamp += 1;
        forged.checksum = forged.identity_checksum();
        let view = (consensus.view() + 1..=consensus.view() + u32::from(sim.replica_count))
            .find(|view| !consensus.is_primary_for_view(*view))
            .expect("primaryship rotates through every replica");
        let start_view = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
            .transmute_header(|_, header: &mut StartViewHeader| {
                header.command = Command::StartView;
                header.cluster = consensus.cluster();
                header.view = view;
                header.op = applied_op;
                header.commit = applied_op;
                header.replica = consensus.primary_index(view);
                header.group = consensus.group();
                header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
            });
        assert!(
            consensus
                .handle_start_view_guarded(
                    plane,
                    start_view.header(),
                    bytemuck::bytes_of(&forged),
                    applied_at,
                )
                .is_none(),
            "the backup refuses the StartView"
        );

        let workload = crate::workload::Workload::new(
            crate::workload::options::WorkloadOptions::new(seed, sim.replica_count, Vec::new()),
        );
        crate::workload::invariants::Invariants::new().check(sim, &workload);
    }

    /// The log of a deleted fixture incarnation goes when its topic comes back, so
    /// the driver compares it across replicas as soon as the delete is reached.
    #[test]
    #[should_panic(expected = "disagree on committed partition op")]
    fn given_replicas_that_disagree_on_a_deleted_fixture_log_when_the_delete_is_reached_should_fail_the_run()
     {
        const SENDS: usize = 3;
        const SEED: u64 = 0x4445_4C44;
        const DIVERGED: usize = 2;
        const DELETE_STEPS: usize = 4_000;
        let (mut sim, client) = cluster(SEED);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        sim.register_client_with_primary(&client);
        commit_sends_everywhere(&mut sim, &client, fixture, SENDS);
        let mut options =
            crate::workload::options::WorkloadOptions::new(SEED, sim.replica_count, vec![fixture]);
        options.weights = crate::workload::options::ActionWeights::new(&[(
            crate::workload::actions::Action::ReplaceFixtureTopic,
            100,
        )]);
        let mut workload = crate::workload::Workload::new(options);
        let (target, delete) = workload
            .build_request(&client)
            .expect("the fixture must take a delete");
        assert_eq!(delete.header().operation, Operation::DeleteTopic);
        sim.submit_request(client.client_id(), target, delete.into_generic());
        // The first reply comes before the partition fences install, so only a
        // resend learns that the delete was reached.
        assert!(
            (0..DELETE_STEPS).any(|_| {
                for reply in sim.step() {
                    assert!(workload.on_reply(&reply).is_empty());
                }
                workload.tick();
                for (target, resend) in workload.due_resends() {
                    sim.submit_request(client.client_id(), target, resend.into_generic());
                }
                workload.client_idle(client.client_id())
            }),
            "the fixture delete must be reached"
        );

        let partitions = sim.replicas[DIVERGED]
            .partition_shard(fixture)
            .plane
            .partitions();
        let partition = partitions
            .get_by_ns(&fixture)
            .expect("the deleted incarnation stays hosted until the topic comes back");
        let journal = &partition.log.journal().inner;
        let op = partition.consensus().commit_min();
        let mut entry = journal
            .repair_entry(op)
            .expect("the replica journaled its last committed op")
            .as_slice()
            .to_vec();
        let header_bytes = &mut entry[..size_of::<PrepareHeader>()];
        let mut header: PrepareHeader = bytemuck::checked::pod_read_unaligned(header_bytes);
        header.timestamp += 1;
        header_bytes.copy_from_slice(bytemuck::bytes_of(&header));
        // Only the evicted ring takes an entry from outside, and resident entries
        // take precedence over it.
        journal.clear_all();
        journal.restore_checkpoint_prepare(
            op,
            server_common::iobuf::Owned::copy_from_slice(&entry).into(),
        );

        workload.compare_deleted_fixture_logs(&sim);
    }

    /// A replica that was down while a fixture topic was replaced comes back on the
    /// deleted incarnation, whose log the quiesce oracle must not hold against the
    /// recreated one.
    #[test]
    fn given_a_replica_back_on_a_deleted_fixture_incarnation_when_converged_should_skip_it() {
        const SENDS: usize = 3;
        const PROGRESS_STEPS: usize = 4_000;
        const SEED: u64 = 0x5354_414C;
        const RESTARTED: u8 = 2;
        let (mut sim, client) = cluster(SEED);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        sim.register_client_with_primary(&client);
        let committed = |sim: &Simulator, replica_idx: u8| {
            sim.partition_consensus_state(usize::from(replica_idx), fixture)
                .map(|state| state.commit_min)
        };
        for index in 0..SENDS {
            let send = client.send_messages(fixture, &[Bytes::from(format!("deleted-{index}"))]);
            let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, send);
            assert_eq!(reply.header().status, 0, "send {index} must commit");
        }
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                committed(&sim, RESTARTED) == committed(&sim, 0)
            }),
            "the replica to crash must commit every send first"
        );
        sim.replica_crash(RESTARTED);
        let stream = Streams::seeded_stream_name(fixture.stream_id());
        let topic = Streams::seeded_topic_name(fixture.stream_id(), fixture.topic_id());
        complete_metadata_op(&mut sim, &client, client.delete_topic(&stream, &topic));
        complete_metadata_op(&mut sim, &client, client.create_topic(&stream, &topic, 1));
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.incarnation_settled(0, &[fixture])
            }),
            "every live replica must materialize the recreated incarnation"
        );
        sim.discover_partition_contexts(&client);
        let send = client.send_messages(fixture, &[Bytes::from_static(b"recreated")]);
        let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, send);
        assert_eq!(
            reply.header().status,
            0,
            "the recreated fixture must take a send"
        );
        sim.replica_restart(RESTARTED);
        let deleted = sim.partition_incarnation(usize::from(RESTARTED), fixture);
        assert!(
            deleted.is_some() && deleted != sim.partition_incarnation(0, fixture),
            "the restarted replica must replay the fixture as it was before the delete"
        );
        assert!(
            committed(&sim, RESTARTED) > committed(&sim, 0),
            "the deleted incarnation must hold more commits than the recreated one, or \
             no leader check could trip on it"
        );

        let mut workload = crate::workload::Workload::new(
            crate::workload::options::WorkloadOptions::new(SEED, sim.replica_count, vec![fixture]),
        );
        let convergence = crate::workload::oracle::assert_converged(&sim, &mut workload);
        assert_eq!(convergence.namespaces_checked, 1);
    }

    /// A replica that hosts an incarnation its own metadata does not name fails
    /// the quiesce check. No comparison between replicas of one incarnation can
    /// see it.
    #[test]
    #[should_panic(expected = "but its metadata names")]
    fn given_a_backup_hosting_an_unnamed_incarnation_when_converged_should_fail_the_run() {
        const FORGED: usize = 2;
        let (mut sim, _) = cluster(0x464F_5247);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        let replica = &sim.replicas[FORGED];
        let streams = replica.shards[0].plane.metadata().mux_stm.streams();
        let named = streams
            .created_revision_for_namespace(fixture)
            .expect("the fixture is in metadata");
        let created_view = streams
            .created_view_for_namespace(fixture)
            .expect("the fixture records its creation view");
        let shard = replica.partition_shard(fixture);
        shard.plane.partitions().remove(&fixture);
        shard.init_partition(
            fixture,
            None,
            None,
            None,
            crate::partition_wal::reopen(&replica.partition_storage, fixture, named + 1, None),
            PartitionMaterialisation::new(named + 1, created_view, true),
        );
        assert_eq!(sim.partition_incarnation(FORGED, fixture), Some(named + 1));

        let mut workload =
            crate::workload::Workload::new(crate::workload::options::WorkloadOptions::new(
                sim.seed,
                sim.replica_count,
                vec![fixture],
            ));
        let _ = crate::workload::oracle::assert_converged(&sim, &mut workload);
    }

    /// A caller context from a node ahead of the dispatching one can name an
    /// incarnation the dispatching node has not seen, so only that node's
    /// metadata may bound the request.
    #[test]
    fn given_raw_request_ahead_of_destination_metadata_when_delivered_should_refuse_as_not_accepted()
     {
        let (mut sim, client) = cluster(0x5741_5445);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        let primary = sim.primary_index(namespace).unwrap();
        let metadata = sim.replicas[usize::from(primary)].shards[0]
            .plane
            .metadata();
        client.set_partition_context(
            namespace,
            PartitionContext {
                incarnation: metadata
                    .mux_stm
                    .streams()
                    .created_revision_for_namespace(namespace)
                    .unwrap(),
                owner_generation: 0,
                metadata_op: metadata.applied_frontier().get() + 1,
            },
        );
        let send = client.send_messages(namespace, &[Bytes::from_static(b"ahead")]);
        let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), primary, send);
        assert_eq!(
            reply.header().status,
            IggyError::TransientNotAccepted.as_code()
        );
    }

    #[test]
    fn given_pending_metadata_when_disconnected_session_expires_should_reassign_partition() {
        let (mut sim, original, replacement) = consumer_group_disconnect_fixture(3, 2);
        let pending = original.create_stream("disconnect-in-flight");
        sim.submit_request(original.client_id(), 0, pending.into_generic());
        assert!(
            (0..DISCONNECT_PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.metadata_consensus(0)
                    .unwrap()
                    .pipeline_has_message_from_client(original.client_id())
            }),
            "disconnect must happen while the original client's metadata request is pending"
        );
        sim.outboxes[0].notify_client_connection_lost(original.client_id());
        sim.run_pumps();
        assert!(
            (0..DISCONNECT_PROGRESS_STEPS).any(|_| {
                sim.step();
                !sim.metadata_consensus(0)
                    .unwrap()
                    .pipeline_has_message_from_client(original.client_id())
            }),
            "the pending request must finish after the disconnect"
        );
        sim.shell_login(&replacement);
        let joined = submit_and_wait_for_reply(
            &mut sim,
            replacement.client_id(),
            0,
            replacement.join_consumer_group(DISCONNECT_STREAM, DISCONNECT_TOPIC, DISCONNECT_GROUP),
        );
        assert_eq!(joined.header().status, 0);
        for _ in 0..DISCONNECT_PROGRESS_STEPS {
            sim.step();
        }
        recover_disconnected_group(&mut sim, &original, &replacement);
    }

    #[test]
    fn given_idle_owner_when_replica_restarts_should_expire_via_virtual_heartbeats() {
        const PROGRESS_STEPS: usize = 20_000;
        for restart in [false, true] {
            let (mut sim, original, replacement) = consumer_group_disconnect_fixture(3, 2);
            sim.shell_login(&replacement);
            let reply = submit_and_wait_for_reply(
                &mut sim,
                replacement.client_id(),
                0,
                replacement.join_consumer_group(
                    DISCONNECT_STREAM,
                    DISCONNECT_TOPIC,
                    DISCONNECT_GROUP,
                ),
            );
            assert_eq!(reply.header().status, 0);
            if restart {
                sim.replica_crash(2);
                sim.replica_restart(2);
            }
            assert!(
                (0..PROGRESS_STEPS).any(|step| {
                    if step % DISCONNECT_PROGRESS_STEPS == 0 {
                        sim.submit_request(
                            replacement.client_id(),
                            0,
                            replacement.get_stream(DISCONNECT_STREAM).into_generic(),
                        );
                    }
                    sim.step();
                    disconnect_group_assignment(&sim, original.client_id()).is_none()
                        && disconnect_group_assignment(&sim, replacement.client_id())
                            == Some(vec![0])
                }),
                "restart={restart}: a silent original must expire while the active replacement stays live"
            );
            assert!(sim.network.delivered_any(Command::ConsumerSessionHeartbeat));
            assert!(
                (0..PROGRESS_STEPS).any(|step| {
                    if step % DISCONNECT_PROGRESS_STEPS == 0 {
                        sim.submit_request(
                            replacement.client_id(),
                            0,
                            replacement.get_stream(DISCONNECT_STREAM).into_generic(),
                        );
                    }
                    sim.step();
                    sim.network
                        .delivered_any(Command::SessionRetirementProgress)
                }),
                "restart={restart}: ended sessions must reach the production partition-retirement reports"
            );
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_ended_session_batch_when_liveness_sweeps_should_submit_bounded_real_barriers() {
        const PARTITIONS: usize = 2;
        const STEPS: usize = 20_000;
        const BASE_CLIENT: u128 = 100;
        let session_count = iggy_binary_protocol::requests::system::MAX_SESSIONS_PER_RETIREMENT + 1;
        let (mut sim, client, _) = consumer_group_disconnect_cluster(3, session_count + 2);
        for partition in 0..PARTITIONS {
            let namespace = IggyNamespace::new(0, 0, partition);
            sim.init_partition(namespace);
            sim.seed_stream_topic_partition(namespace);
        }
        sim.discover_partition_contexts(&client);
        for partition in 0..PARTITIONS {
            commit_sends_everywhere(&mut sim, &client, IggyNamespace::new(0, 0, partition), 1);
        }
        let end_op = sim.metadata_consensus(0).unwrap().commit_min();
        sim.discover_partition_contexts(&client);
        for partition_id in 0..PARTITIONS {
            let namespace = IggyNamespace::new(0, 0, partition_id);
            for index in 0..session_count {
                let client_id = BASE_CLIENT + index as u128;
                sim.submit_request(
                    client.client_id(),
                    0,
                    client.get_stream(DISCONNECT_STREAM).into_generic(),
                );
                let primary = usize::from(sim.primary_index(namespace).unwrap());
                let owner = Rc::clone(sim.replicas[primary].partition_shard(namespace));
                let previous = owner
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)
                    .unwrap()
                    .consensus()
                    .commit_min();
                let request = client
                    .store_consumer_offset(
                        namespace,
                        ConsumerKind::Consumer.as_code(),
                        0,
                        0,
                        AckLevel::Quorum,
                    )
                    .transmute_header(|old, header: &mut RoutedRequestHeader| {
                        *header = old;
                        header.client = client_id;
                        header.session = end_op;
                        header.request = 1;
                        header.metadata_watermark = end_op;
                        header.checksum = 0;
                    });
                let ticket = owner.partition_submit(namespace, request).unwrap();
                let (result_tx, result_rx) = shard::channel(1);
                sim.executor.spawn(async move {
                    let _ = result_tx.try_send(owner.await_partition_submit(ticket).await);
                });
                assert!(
                    (0..DISCONNECT_PROGRESS_STEPS).any(|_| {
                        sim.step();
                        sim.replicas.iter().all(|replica| {
                            replica
                                .partition_shard(namespace)
                                .plane
                                .partitions()
                                .get_by_ns(&namespace)
                                .unwrap()
                                .consensus()
                                .commit_min()
                                > previous
                        })
                    }),
                    "client {client_id} must hold a committed partition receipt before retirement: {:?}",
                    result_rx
                        .try_recv()
                        .map(|reply| reply.map(|reply| *reply.header()))
                );
            }
        }
        for replica in &sim.replicas {
            let mut table = replica.shards[0].plane.metadata().client_table.borrow_mut();
            for index in 0..session_count {
                let client_id = BASE_CLIENT + index as u128;
                let mut reply = Message::<ReplyHeader>::new(size_of::<ReplyHeader>());
                *bytemuck::checked::from_bytes_mut(reply.as_mut_slice()) = ReplyHeader {
                    command: Command::Reply,
                    operation: Operation::Register,
                    client: client_id,
                    op: end_op,
                    commit: end_op,
                    size: u32::try_from(size_of::<ReplyHeader>()).unwrap(),
                    ..ReplyHeader::default()
                };
                table
                    .commit_register(client_id, 0, [0x5a; 32], reply)
                    .unwrap();
            }
        }
        for replica in &sim.replicas {
            let mut table = replica.shards[0].plane.metadata().client_table.borrow_mut();
            for index in 0..session_count {
                assert!(table.end_session(BASE_CLIENT + index as u128, 0, end_op, end_op));
            }
        }
        assert!(
            (0..STEPS).any(|step| {
                if step % DISCONNECT_PROGRESS_STEPS == 0 {
                    sim.submit_request(
                        client.client_id(),
                        0,
                        client.get_stream(DISCONNECT_STREAM).into_generic(),
                    );
                }
                sim.step();
                sim.replicas.iter().all(|replica| {
                    replica.shards[0]
                        .plane
                        .metadata()
                        .client_table
                        .borrow()
                        .ended_sessions()
                        .next()
                        .is_none()
                })
            }),
            "the real sweeper must retire every ended session"
        );
        for partition_id in 0..PARTITIONS {
            let namespace = IggyNamespace::new(0, 0, partition_id);
            let primary = sim.primary_index(namespace).unwrap();
            let partition = sim.replicas[usize::from(primary)]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .unwrap();
            let journal = &partition.log.journal().inner;
            let barriers: Vec<_> = journal
                .repair_headers_in(1..=partition.consensus().commit_min())
                .into_iter()
                .filter(|(_, header)| header.operation == Operation::RetireSession)
                .map(|(op, _)| {
                    iggy_binary_protocol::requests::system::RetireSessionsRequest::decode_from(
                        &journal.repair_entry(op).unwrap().as_slice()[size_of::<PrepareHeader>()..],
                    )
                    .unwrap()
                })
                .collect();
            assert_eq!(
                barriers.len(),
                session_count
                    .div_ceil(iggy_binary_protocol::requests::system::MAX_SESSIONS_PER_RETIREMENT),
                "partition {partition_id}: production submissions must batch every ended session"
            );
            let mut retired: Vec<_> = barriers
                .iter()
                .flat_map(|batch| batch.identities.iter().map(|identity| identity.client_id))
                .collect();
            retired.sort_unstable();
            retired.dedup();
            assert_eq!(
                retired,
                (BASE_CLIENT..BASE_CLIENT + session_count as u128).collect::<Vec<_>>()
            );
            assert!(barriers.iter().all(|batch| batch.identities.len()
                <= iggy_binary_protocol::requests::system::MAX_SESSIONS_PER_RETIREMENT));
        }
    }

    #[test]
    fn given_stored_offsets_when_polling_next_should_start_after_each_committed_position() {
        let (mut sim, client, _) = consumer_group_disconnect_fixture(3, 2);
        let namespace = IggyNamespace::new(0, 0, 0);
        commit_sends_everywhere(&mut sim, &client, namespace, 4);
        sim.discover_partition_contexts(&client);
        for kind in [
            iggy_binary_protocol::KIND_CONSUMER,
            iggy_binary_protocol::KIND_CONSUMER_GROUP,
        ] {
            let stored = submit_and_wait_for_reply(
                &mut sim,
                client.client_id(),
                0,
                client.store_consumer_offset(namespace, kind, 0, 1, AckLevel::Quorum),
            );
            assert_eq!(stored.header().status, 0);
            let offset_poll = client.poll_messages_at(namespace, kind, 0, 0, 2);
            let mut body =
                iggy_binary_protocol::requests::messages::PollMessagesRequest::decode_from(
                    offset_poll.body(),
                )
                .unwrap();
            body.strategy = iggy_binary_protocol::WirePollingStrategy::next();
            let encoded = body.to_bytes();
            let mut next = Message::<RoutedRequestHeader>::new(
                size_of::<RoutedRequestHeader>() + encoded.len(),
            );
            next.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&encoded);
            let next = next.transmute_header(|_, header: &mut RoutedRequestHeader| {
                *header = *offset_poll.header();
                header.size =
                    u32::try_from(size_of::<RoutedRequestHeader>() + encoded.len()).unwrap();
            });
            let reply = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, next);
            assert_eq!(reply.header().status, 0);
            let poll = iggy_binary_protocol::responses::messages::PollMessagesResponse::decode(
                reply.body(),
            )
            .unwrap();
            assert_eq!(
                poll.messages
                    .map(|message| message.unwrap().offset)
                    .collect::<Vec<_>>(),
                [2, 3],
                "kind={kind}: Next must use the acknowledged stored position"
            );
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_consumer_handoff_when_each_replica_crashes_at_committed_boundaries_should_reassign() {
        const STEPS: usize = 10_000;
        const DOWN_STEPS: usize = 500;
        fn setup() -> (Simulator, SimClient, SimClient, u64, u64) {
            let (mut sim, original, replacement) = consumer_group_disconnect_fixture(3, 2);
            sim.shell_login(&replacement);
            let joined = submit_and_wait_for_reply(
                &mut sim,
                replacement.client_id(),
                0,
                replacement.join_consumer_group(
                    DISCONNECT_STREAM,
                    DISCONNECT_TOPIC,
                    DISCONNECT_GROUP,
                ),
            );
            assert_eq!(joined.header().status, 0);
            for _ in 0..DISCONNECT_PROGRESS_STEPS {
                sim.step();
            }
            let namespace = IggyNamespace::new(0, 0, 0);
            let metadata_start = sim.metadata_consensus(0).unwrap().commit_min();
            let partition_start = sim.replicas[0]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .unwrap()
                .consensus()
                .commit_min();
            sim.submit_request(
                original.client_id(),
                0,
                original
                    .leave_consumer_group(DISCONNECT_STREAM, DISCONNECT_TOPIC, DISCONNECT_GROUP)
                    .into_generic(),
            );
            (sim, original, replacement, metadata_start, partition_start)
        }
        fn advance(sim: &mut Simulator, replacement: &SimClient, step: usize) {
            if step.is_multiple_of(DISCONNECT_PROGRESS_STEPS) {
                let target = sim.metadata_primary_index().unwrap_or(0);
                sim.submit_request(
                    replacement.client_id(),
                    target,
                    replacement.get_stream(DISCONNECT_STREAM).into_generic(),
                );
            }
            sim.step();
        }
        let namespace = IggyNamespace::new(0, 0, 0);
        let (mut baseline, _, replacement, metadata_start, partition_start) = setup();
        let mut seen = BTreeSet::new();
        let mut boundaries = Vec::new();
        for step in 0..STEPS {
            advance(&mut baseline, &replacement, step);
            for replica in &baseline.replicas {
                let metadata = replica.shards[0].plane.metadata();
                for op in metadata_start + 1..=metadata.consensus.as_ref().unwrap().commit_min() {
                    let Some(header) = metadata
                        .journal
                        .as_ref()
                        .unwrap()
                        .header(usize::try_from(op).unwrap())
                    else {
                        continue;
                    };
                    if matches!(
                        header.operation,
                        Operation::LeaveConsumerGroup | Operation::CompleteConsumerGroupRevocation
                    ) && seen.insert((METADATA_GROUP, op))
                    {
                        boundaries.push((step, header.operation));
                    }
                }
                let partition = replica
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)
                    .unwrap();
                for (op, header) in partition
                    .log
                    .journal()
                    .inner
                    .repair_headers_in(partition_start + 1..=partition.consensus().commit_min())
                {
                    if header.operation == Operation::InstallConsumerGroupOwner
                        && seen.insert((namespace.inner(), op))
                    {
                        boundaries.push((step, header.operation));
                    }
                }
            }
            if disconnect_group_assignment(&baseline, replacement.client_id()) == Some(vec![0]) {
                break;
            }
        }
        assert!(
            boundaries
                .iter()
                .any(|(_, operation)| *operation == Operation::LeaveConsumerGroup)
        );
        assert!(
            boundaries
                .iter()
                .any(|(_, operation)| *operation == Operation::InstallConsumerGroupOwner)
        );
        assert!(
            boundaries
                .iter()
                .any(|(_, operation)| *operation == Operation::CompleteConsumerGroupRevocation)
        );
        for (cut, operation) in boundaries {
            for victim in 0..3 {
                let (mut sim, original, replacement, _, _) = setup();
                for step in 0..=cut {
                    advance(&mut sim, &replacement, step);
                }
                sim.replica_crash(victim);
                for step in 0..DOWN_STEPS {
                    advance(&mut sim, &replacement, step);
                }
                sim.replica_restart(victim);
                assert!(
                    (0..STEPS).any(|step| {
                        advance(&mut sim, &replacement, step);
                        sim.replicas.iter().all(|replica| {
                            let streams = replica.shards[0].plane.metadata().mux_stm.streams();
                            streams
                                .consumer_group_member_assignment(
                                    &WireIdentifier::numeric(0),
                                    &WireIdentifier::numeric(0),
                                    &WireIdentifier::named(DISCONNECT_GROUP).unwrap(),
                                    replacement.client_id(),
                                )
                                .is_some_and(|(_, partitions)| partitions == [0])
                                && replica
                                    .partition_shard(namespace)
                                    .plane
                                    .partitions()
                                    .get_by_ns(&namespace)
                                    .is_some_and(|partition| {
                                        partition.consumer_group_owner(0).is_some_and(|owner| {
                                            owner.client_id == replacement.client_id()
                                        })
                                    })
                        })
                    }),
                    "{operation:?} step={cut} victim={victim}: restarted replicas must complete the handoff"
                );
                assert_eq!(
                    disconnect_group_assignment(&sim, original.client_id()),
                    None
                );
                for replica in &sim.replicas {
                    let owner = replica
                        .partition_shard(namespace)
                        .plane
                        .partitions()
                        .get_by_ns(&namespace)
                        .unwrap()
                        .consumer_group_owner(0)
                        .unwrap();
                    assert_eq!(
                        owner.client_id,
                        replacement.client_id(),
                        "{operation:?} victim={victim}"
                    );
                }
            }
        }
    }

    #[test]
    fn given_full_registry_when_another_client_logs_in_should_preserve_the_live_session() {
        let (mut sim, original, replacement) = consumer_group_disconnect_fixture(1, 1);
        sim.submit_handshake(&replacement, 0);
        let refused = (0..DISCONNECT_PROGRESS_STEPS)
            .find_map(|_| {
                sim.step()
                    .into_iter()
                    .find(|reply| reply.header().client == replacement.client_id())
            })
            .expect("full registry replies with an admission refusal");
        assert_eq!(
            iggy_binary_protocol::consensus::result_code(refused.body()),
            Some(IggyError::TransientNotAccepted.as_code())
        );
        assert!(
            sim.replicas[0].shards[0]
                .plane
                .metadata()
                .client_table
                .borrow()
                .get_epoch(original.client_id())
                .is_some()
        );
        assert_eq!(
            disconnect_group_assignment(&sim, original.client_id()),
            Some(vec![0])
        );
        sim.outboxes[0].notify_client_connection_lost(original.client_id());
        sim.run_pumps();
        assert!(
            sim.replicas[0].shards[0]
                .plane
                .metadata()
                .client_table
                .borrow()
                .get_epoch(original.client_id())
                .is_some()
        );
    }

    /// Only partition replicas can prove an owner install, so a metadata primary
    /// without one needs a backup to relay the revocation completion.
    #[test]
    fn given_metadata_primary_without_partition_replica_when_member_joins_should_activate_its_partition()
     {
        let (mut sim, original, _) = consumer_group_disconnect_cluster(3, 2);
        fence_partition_on_metadata_primary(&mut sim, IggyNamespace::new(0, 0, 0));
        join_disconnect_group(&mut sim, &original);
        assert!(sim.network.delivered_any(Command::ForwardCompletion));
        assert_eq!(
            sim.metadata_primary_index(),
            Some(0),
            "the metadata primary is the replica without the partition"
        );
        assert_eq!(
            sim.metadata_view(),
            Some(0),
            "the owner must activate without a metadata view change"
        );
    }

    /// Without a quorum neither the fence of a handoff nor its completion can
    /// commit, so the revocation stays pending past any deadline.
    #[test]
    #[should_panic(expected = "pending revocation")]
    fn given_a_handoff_without_quorum_when_it_outlives_the_bound_should_fail_the_quiesce_check() {
        let mut sim = pending_leave_fixture();
        sim.replica_crash(1);
        sim.replica_crash(2);
        let deadline = sim.realtime_micros()
            + crate::workload::oracle::PENDING_REVOCATION_TIMEOUTS_MAX
                * rebalancing_timeout_micros();
        while sim.realtime_micros() <= deadline {
            sim.step();
        }
        crate::workload::oracle::assert_no_stale_revocations(&sim, 0, sim.seed);
    }

    /// The reconciler installs an owner on its own schedule, so settling waits
    /// for a pending one, and the owner oracle compares it.
    #[test]
    fn given_a_pending_owner_when_settled_should_compare_it() {
        let mut sim = pending_leave_fixture();
        assert_eq!(
            settle_and_converge(&mut sim).owners_compared,
            1,
            "the owner that the leave installs was not compared"
        );
    }

    /// Without a quorum the pending owner never installs, so settling stops at
    /// the stale revocation bound and the quiesce check fails the revocation.
    #[test]
    #[should_panic(expected = "pending revocation")]
    fn given_a_pending_owner_without_quorum_when_settled_should_fail_the_quiesce_check() {
        let mut sim = pending_leave_fixture();
        sim.replica_crash(1);
        sim.replica_crash(2);
        settle_and_converge(&mut sim);
    }

    /// [`consumer_group_disconnect_fixture`] with the revocation that its owner's
    /// leave starts still pending.
    fn pending_leave_fixture() -> Simulator {
        let (mut sim, original, _) = consumer_group_disconnect_fixture(3, 2);
        let leave =
            original.leave_consumer_group(DISCONNECT_STREAM, DISCONNECT_TOPIC, DISCONNECT_GROUP);
        sim.submit_request(original.client_id(), 0, leave.into_generic());
        assert!(
            (0..DISCONNECT_PROGRESS_STEPS).any(|_| {
                sim.step();
                !sim.replicas[0].shards[0]
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .consumer_group_pending_revocations()
                    .is_empty()
            }),
            "the leave must start a revocation"
        );
        sim
    }

    /// Settle `sim` as the fuzz driver does after its drain, then run the quiesce
    /// oracle over the namespace of [`consumer_group_disconnect_fixture`].
    fn settle_and_converge(sim: &mut Simulator) -> crate::workload::oracle::ConvergenceReport {
        const SETTLE_TICKS: u64 = 50_000;
        let mut workload =
            crate::workload::Workload::new(crate::workload::options::WorkloadOptions::new(
                sim.seed,
                sim.replica_count,
                vec![IggyNamespace::new(0, 0, 0)],
            ));
        let mut invariants = crate::workload::invariants::Invariants::new();
        assert!(
            crate::workload::oracle::settle_to_stable_view(
                sim,
                &mut workload,
                SETTLE_TICKS,
                &mut invariants
            ),
            "the views never settled"
        );
        crate::workload::oracle::assert_converged(sim, &mut workload)
    }

    fn consumer_group_disconnect_fixture(
        replica_count: u8,
        client_capacity: usize,
    ) -> (Simulator, SimClient, SimClient) {
        let (mut sim, original, replacement) =
            consumer_group_disconnect_cluster(replica_count, client_capacity);
        join_disconnect_group(&mut sim, &original);
        (sim, original, replacement)
    }

    /// The shell cluster of [`consumer_group_disconnect_fixture`], with the
    /// original client logged in and no group yet.
    fn consumer_group_disconnect_cluster(
        replica_count: u8,
        client_capacity: usize,
    ) -> (Simulator, SimClient, SimClient) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let original = SimClient::new(1);
        let replacement = SimClient::new(2);
        let mut sim = Simulator::with_shards_shell(
            usize::from(replica_count),
            1,
            [original.client_id(), replacement.client_id()].into_iter(),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 2,
                seed: 0x4273,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        for replica in &sim.replicas {
            replica.shards[0]
                .plane
                .metadata()
                .client_table
                .borrow_mut()
                .set_capacity(client_capacity);
        }
        let namespace = IggyNamespace::new(0, 0, 0);
        sim.init_partition(namespace);
        sim.seed_stream_topic_partition(namespace);
        sim.shell_login(&original);
        (sim, original, replacement)
    }

    /// `original` creates the group and joins it, and then owns partition 0.
    fn join_disconnect_group(sim: &mut Simulator, original: &SimClient) {
        let namespace = IggyNamespace::new(0, 0, 0);
        for request in [
            original.create_consumer_group(DISCONNECT_STREAM, DISCONNECT_TOPIC, DISCONNECT_GROUP),
            original.join_consumer_group(DISCONNECT_STREAM, DISCONNECT_TOPIC, DISCONNECT_GROUP),
        ] {
            let reply = submit_and_wait_for_reply(sim, original.client_id(), 0, request);
            assert_eq!(reply.header().status, 0);
        }
        for _ in 0..SETUP_TOTAL_STEPS {
            if disconnect_group_assignment(sim, original.client_id()) == Some(vec![0]) {
                break;
            }
            sim.step();
        }
        assert_eq!(
            disconnect_group_assignment(sim, original.client_id()),
            Some(vec![0]),
            "the original consumer must come to own the only partition; pending={:?}, partition={:?}",
            sim.replicas[0].shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .consumer_group_pending_revocations(),
            sim.replicas[0].shards[0]
                .plane
                .partitions()
                .with_partition(&namespace, |partition| (
                    partition.created_revision(),
                    partition.consumer_group_owner(0),
                    partition.consensus().commit_min(),
                    partition.consensus().sequencer().current_sequence(),
                    partition.fatal().is_some(),
                )),
        );
    }

    /// Metadata settles an owner only after the partition installed it, so a
    /// partition primary holding another owner fails the quiesce check.
    #[test]
    #[should_panic(expected = "but partition primary")]
    fn given_a_partition_primary_holding_another_owner_when_converged_should_fail_the_run() {
        const INSTALL_STEPS: usize = 2_000;
        let (mut sim, _, replacement) = consumer_group_disconnect_fixture(3, 2);
        let namespace = IggyNamespace::new(0, 0, 0);
        let mut workload =
            crate::workload::Workload::new(crate::workload::options::WorkloadOptions::new(
                sim.seed,
                sim.replica_count,
                vec![namespace],
            ));
        assert_eq!(
            crate::workload::oracle::assert_converged(&sim, &mut workload).owners_compared,
            1,
            "the fixture must settle its one owner"
        );
        let leader = usize::from(sim.metadata_leader().expect("the cluster has a leader"));
        let metadata = sim.replicas[leader].shards[0].plane.metadata();
        let group_id = metadata
            .mux_stm
            .streams()
            .read(|inner| {
                inner
                    .items
                    .get(namespace.stream_id())
                    .and_then(|stream| stream.topics.get(namespace.topic_id()))
                    .and_then(|topic| topic.consumer_groups.keys().next().copied())
            })
            .expect("the fixture created a group");
        let metadata_op = metadata
            .consensus
            .as_ref()
            .expect("the leader runs consensus")
            .commit_min();
        let primary = usize::from(
            sim.primary_index(namespace)
                .expect("the partition has a primary"),
        );
        let settled = sim
            .installed_group_owner(primary, namespace, group_id)
            .expect("the primary installed the settled owner");
        assert!(
            metadata_op > settled.generation,
            "the forged owner must be newer"
        );
        let installation = InstallConsumerGroupOwnerRequest {
            incarnation: sim
                .partition_incarnation(primary, namespace)
                .expect("the primary hosts the partition"),
            group_id,
            owner: ConsumerGroupOwner {
                client_id: replacement.client_id(),
                session: settled.session,
                generation: metadata_op,
            },
            metadata_op,
        };
        let body = installation.to_bytes();
        let size = size_of::<RoutedRequestHeader>() + body.len();
        let mut request = Message::<RoutedRequestHeader>::new(size);
        request.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
        let request = request.transmute_header(|_, header: &mut RoutedRequestHeader| {
            *header = RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::InstallConsumerGroupOwner,
                size: u32::try_from(size).expect("the install fits a frame"),
                client: AUTO_COMMIT_CLIENT_ID,
                session: 1,
                request: metadata_op,
                group: namespace.inner(),
                partition_incarnation: installation.incarnation,
                metadata_watermark: metadata_op,
                ..Default::default()
            };
        });
        let _ticket = sim.replicas[primary]
            .partition_shard(namespace)
            .partition_submit(namespace, request)
            .expect("the owning shard takes the install");
        assert!(
            (0..INSTALL_STEPS).any(|_| {
                sim.step();
                sim.installed_group_owner(primary, namespace, group_id) == Some(installation.owner)
            }),
            "the primary must install the forged owner"
        );

        let _ = crate::workload::oracle::assert_converged(&sim, &mut workload);
    }

    /// Commit `count` sends on `namespace` through replica 0, then step until
    /// every replica committed them all.
    pub fn commit_sends_everywhere(
        sim: &mut Simulator,
        client: &SimClient,
        namespace: IggyNamespace,
        count: usize,
    ) {
        const PROGRESS_STEPS: usize = 4_000;
        for index in 0..count {
            let send = client.send_messages(namespace, &[Bytes::from(format!("message-{index}"))]);
            let reply = submit_and_wait_for_reply(sim, client.client_id(), 0, send);
            assert_eq!(reply.header().status, 0, "send {index} must commit");
        }
        let last = u64::try_from(count - 1).expect("a send count fits u64");
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                (0..usize::from(sim.replica_count)).all(|replica_idx| {
                    sim.offsets(replica_idx, namespace)
                        .is_some_and(|offsets| offsets.commit_offset == last)
                })
            }),
            "every replica must commit every send"
        );
    }

    fn disconnect_group_assignment(sim: &Simulator, client_id: u128) -> Option<Vec<u32>> {
        sim.replicas[0].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_group_member_assignment(
                &WireIdentifier::numeric(0),
                &WireIdentifier::numeric(0),
                &WireIdentifier::named(DISCONNECT_GROUP).unwrap(),
                client_id,
            )
            .map(|(_, partitions)| partitions)
    }

    fn recover_disconnected_group(
        sim: &mut Simulator,
        original: &SimClient,
        replacement: &SimClient,
    ) {
        const EXPIRY_STEPS: usize = 20_000;
        assert_eq!(
            disconnect_group_assignment(sim, original.client_id()),
            Some(vec![0]),
            "failed disconnect cleanup must leave the original owning the only partition"
        );
        assert_eq!(
            disconnect_group_assignment(sim, replacement.client_id()),
            Some(vec![]),
            "the replacement joins successfully but cannot poll any partition"
        );
        let original_id = original.client_id();
        let replacement_id = replacement.client_id();
        assert!(
            (0..EXPIRY_STEPS).any(|step| {
                if step % DISCONNECT_PROGRESS_STEPS == 0 {
                    sim.submit_request(
                        replacement_id,
                        0,
                        replacement.get_stream(DISCONNECT_STREAM).into_generic(),
                    );
                }
                sim.step();
                disconnect_group_assignment(sim, original_id).is_none()
                    && disconnect_group_assignment(sim, replacement_id) == Some(vec![0])
            }),
            "the production liveness loop must expire the disconnected owner and reassign its partition"
        );
        assert!(sim.network.delivered_any(Command::ConsumerSessionHeartbeat));
        assert_eq!(disconnect_group_assignment(sim, original_id), None);
        assert_eq!(
            disconnect_group_assignment(sim, replacement_id),
            Some(vec![0]),
            "expiry must give the existing replacement the orphan's partition"
        );
    }

    #[test]
    fn given_a_shard_that_stops_answering_when_gathering_clients_should_report_incomplete() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let network_options = packet::PacketSimulatorOptions {
            node_count: 1,
            client_count: 1,
            seed: 0x4273,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards_shell(1, 2, std::iter::once(1), network_options);
        sim.shell_login(&SimClient::new(1));
        for missing_shard in [false, true] {
            if missing_shard {
                sim.executor.abort(sim.replicas[0].pump_tasks[1]);
            }
            let shard = Rc::clone(&sim.replicas[0].shards[0]);
            let result = Rc::new(RefCell::new(None));
            let gathered = Rc::clone(&result);
            sim.executor.spawn(async move {
                *gathered.borrow_mut() = Some(futures::join!(
                    shard.gather_clients(),
                    shard.gather_consumer_sessions(),
                    shard.count_all_clients()
                ));
            });
            assert!(matches!(
                sim.executor.run_until_stalled(POLL_BUDGET),
                RunOutcome::Quiescent { .. }
            ));
            if missing_shard {
                assert!(
                    result.borrow().is_none(),
                    "gather must wait for the missing shard until its deadline"
                );
                sim.executor.advance_time(LIST_CLIENTS_GATHER_TIMEOUT);
                assert!(matches!(
                    sim.executor.run_until_stalled(POLL_BUDGET),
                    RunOutcome::Quiescent { .. }
                ));
            }
            let (gathered, sessions, count) = result
                .borrow_mut()
                .take()
                .expect("gather must complete within its deadline");
            assert_eq!(gathered.complete, !missing_shard);
            assert_eq!(sessions.complete, !missing_shard);
            assert_eq!(
                count,
                gathered.clients.len(),
                "the count must cover the same shards as the client list"
            );
            if !missing_shard {
                assert!(
                    gathered
                        .clients
                        .iter()
                        .any(|client| client.vsr_client_id == Some(1))
                );
                assert!(
                    sessions
                        .clients
                        .iter()
                        .any(|session| session.client_id == 1)
                );
            }
        }
    }

    pub fn submit_and_wait_for_reply(
        sim: &mut Simulator,
        client_id: u128,
        target: u8,
        request: Message<RoutedRequestHeader>,
    ) -> Message<ReplyHeader> {
        let request_id = request.header().request;
        sim.submit_request(client_id, target, request.into_generic());
        for _ in 0..100 {
            if let Some(reply) = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == request_id)
            {
                return reply;
            }
        }
        panic!("request {request_id} did not receive a reply");
    }

    /// Submit a metadata op to replica 0 and step until every live replica applied
    /// it and finished its lifecycle. Deletes reply before their partition fences
    /// install.
    fn complete_metadata_op(
        sim: &mut Simulator,
        client: &SimClient,
        request: Message<RoutedRequestHeader>,
    ) {
        const PROGRESS_STEPS: usize = 4_000;
        let reply = submit_and_wait_for_reply(sim, client.client_id(), 0, request);
        let code = metadata::stm::result::result_code(reply.body());
        assert!(
            code == Some(0) || code == Some(IggyError::TransientNotCommitted.as_code()),
            "unexpected result code {code:?}"
        );
        let op = reply.header().op;
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                (0..sim.replica_count)
                    .filter(|replica_idx| !sim.is_crashed(*replica_idx))
                    .all(|replica_idx| {
                        let metadata = sim.replicas[usize::from(replica_idx)].shards[0]
                            .plane
                            .metadata();
                        metadata.applied_frontier().get() >= op
                            && !metadata.mux_stm.streams().lifecycle_pending(op)
                    })
            }),
            "metadata op {op} must complete on every live replica"
        );
    }

    /// The simulator helper must yield while its owner runs, then return the
    /// accepted resident read even if the automatic commit's replica send stalls.
    /// Releasing that send must let the same commit reach the other replicas.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_simulator_poll_when_replica_send_stalls_should_reply_and_resume_replication() {
        // 1. Publish one message and let the cluster finish the initial work.
        // The pause installed later must affect the poll's automatic commit.
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let client_id = 1u128;
        let replica_count = 3u8;
        let primary_replica = 0u8;
        let namespace = IggyNamespace::new(1, 1, 0);
        let mut simulator = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 1,
                seed: 0xC011_EC71,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        simulator.init_partition(namespace);
        let client = SimClient::new(client_id);
        simulator.register_client_with_primary(&client);
        let append_reply = submit_and_wait_for_reply(
            &mut simulator,
            client_id,
            primary_replica,
            client.send_messages(
                namespace,
                &[Bytes::from_static(b"reply-before-replication")],
            ),
        );
        assert_eq!(append_reply.header().status, 0);
        simulator.run_pumps();

        // 2. Confirm that the poll uses resident data. This scenario checks
        // reply timing after inline acceptance, without a detached disk read.
        let owner =
            Rc::clone(simulator.replicas[usize::from(primary_replica)].partition_shard(namespace));
        let consumer_id = 7;
        let partition_id = 0;
        let consumer = PollingConsumer::Consumer(consumer_id, partition_id);
        let auto_commit = true;
        let poll_args = PollingArgs::new(iggy_common::PollingStrategy::next(), 1, auto_commit);
        let resident_plan = owner
            .plane
            .partitions()
            .build_poll_snapshot(&namespace, consumer, &poll_args)
            .expect("poll snapshot");
        assert!(
            !resident_plan.needs_off_pump_io(),
            "fixture must exercise inline resident completion"
        );
        drop(resident_plan);

        // 3. Run the poll task with the next replica send paused.
        // The separate caller task lets the owner send a reply while
        // its replication continuation is still waiting for release.
        let resume_replication = owner.bus.delay_next_replica_send();
        let (poll_result_sender, poll_result_receiver) = shard::channel(1);
        let poll = simulator.poll_messages(
            usize::from(primary_replica),
            namespace,
            consumer,
            &poll_args,
        );
        simulator.executor.spawn(async move {
            let _ = poll_result_sender.try_send(poll.await);
        });
        simulator.run_pumps();

        // 4. The nonempty reply must already be available before releasing
        // replication. Waiting for it asynchronously here would hide a stall.
        let fragments = poll_result_receiver
            .recv()
            .now_or_never()
            .expect("admitted poll replies while replication remains suspended")
            .expect("reply channel is open")
            .expect("owning shard accepted the poll");
        assert!(!fragments.is_empty());

        // 5. Before release, neither backup has received the automatic commit.
        // Inspect backups because the stalled owner still borrows its partition.
        for backup in &simulator.replicas[1..] {
            let (consumer_offset, _) = backup
                .partition_shard(namespace)
                .plane
                .partitions()
                .consumer_offset_read(&namespace, consumer)
                .expect("partition exists on the backup");
            assert_eq!(consumer_offset, None);
        }
        assert!(
            resume_replication.send(()).is_ok(),
            "replication must still be waiting after the poll reply"
        );

        // 6. Drive network delivery as well as the executor. A reply alone
        // must not let the helper silently discard its replication continuation.
        let replicated = (0..100).any(|_| {
            simulator.step();
            simulator.replicas.iter().all(|replica| {
                replica
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .with_partition(&namespace, |partition| {
                        partition.durable_consumer_offset_count(iggy_common::ConsumerKind::Consumer)
                            == 1
                    })
                    == Some(true)
            })
        });
        assert!(
            replicated,
            "automatic commit reaches every replica after release"
        );
        let (consumer_offset, _partition_commit_offset) = owner
            .plane
            .partitions()
            .consumer_offset_read(&namespace, consumer)
            .expect("partition exists");
        assert_eq!(
            consumer_offset,
            Some(0),
            "admission records the served cursor"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_small_offset_limit_when_using_quorum_should_bound_every_replica() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let replica_count = 3u8;
        let client_id = 1u128;
        let namespace = IggyNamespace::new(1, 1, 0);
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 1,
                seed: 0xC0FF_EE01,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        sim.set_consumer_offsets_max(2);
        sim.init_partition(namespace);
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);

        let produced = submit_and_wait_for_reply(
            &mut sim,
            client_id,
            0,
            client.send_messages(namespace, &[Bytes::from_static(b"offset-cap")]),
        );
        assert_eq!(produced.header().status, 0);

        for (consumer_id, ack) in [(1, AckLevel::Quorum), (2, AckLevel::Quorum)] {
            let reply = submit_and_wait_for_reply(
                &mut sim,
                client_id,
                0,
                client.store_consumer_offset(namespace, 1, consumer_id, 0, ack),
            );
            assert_eq!(reply.header().status, 0);
        }

        let denied = submit_and_wait_for_reply(
            &mut sim,
            client_id,
            0,
            client.store_consumer_offset(namespace, 1, 3, 0, AckLevel::Quorum),
        );
        assert_eq!(
            denied.header().status,
            IggyError::TooManyConsumerOffsets.as_code()
        );

        let deleted = submit_and_wait_for_reply(
            &mut sim,
            client_id,
            0,
            client.delete_consumer_offset(namespace, 1, 1, AckLevel::Quorum),
        );
        assert_eq!(deleted.header().status, 0);
        let replacement = submit_and_wait_for_reply(
            &mut sim,
            client_id,
            0,
            client.store_consumer_offset(namespace, 1, 3, 0, AckLevel::Quorum),
        );
        assert_eq!(replacement.header().status, 0);

        sim.replica_crash(0);
        let new_primary = (0..800)
            .find_map(|_| {
                sim.step();
                (1..replica_count).find(|replica_id| {
                    sim.partition_consensus_state(usize::from(*replica_id), namespace)
                        .is_some_and(|state| {
                            state.is_primary
                                && state.status == Status::Normal
                                && sim
                                    .partition_consumer_offset_counts(
                                        usize::from(*replica_id),
                                        namespace,
                                        ConsumerKind::Consumer,
                                    )
                                    .is_some_and(|counts| counts.0 == 2)
                        })
                })
            })
            .expect("surviving replicas elect a new partition primary");
        let denied_after_failover = submit_and_wait_for_reply(
            &mut sim,
            client_id,
            new_primary,
            client.store_consumer_offset(namespace, 1, 4, 0, AckLevel::Quorum),
        );
        assert_eq!(
            denied_after_failover.header().status,
            IggyError::TooManyConsumerOffsets.as_code(),
            "the promoted primary must preserve the durable admission bound"
        );

        for _ in 0..100 {
            sim.step();
        }
        for replica_id in 0..sim.replicas.len() {
            let counts = sim
                .partition_consumer_offset_counts(replica_id, namespace, ConsumerKind::Consumer)
                .expect("partition is materialised");
            assert!(
                counts.0 <= 2,
                "replica {replica_id} durable count {counts:?}"
            );
            assert!(counts.1 <= 4, "replica {replica_id} map count {counts:?}");
        }

        sim.replica_restart(0);
        let recovered = sim
            .partition_consumer_offset_counts(0, namespace, ConsumerKind::Consumer)
            .expect("restarted replica rematerialises the partition");
        assert_eq!(recovered.0, 2, "restart must retain durable offset keys");
        for _ in 0..200 {
            sim.step();
        }
        for replica_id in 0..sim.replicas.len() {
            let counts = sim
                .partition_consumer_offset_counts(replica_id, namespace, ConsumerKind::Consumer)
                .expect("partition remains materialised after rejoin");
            assert!(
                counts.0 <= 2,
                "replica {replica_id} durable count {counts:?}"
            );
            assert!(counts.1 <= 4, "replica {replica_id} map count {counts:?}");
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_metadata_entries_below_a_new_view_suffix_when_adopting_should_refill_before_apply() {
        const BACKUP: usize = 1;
        const NEW_VIEW: u32 = 2;
        const LOCAL_ENTRIES: usize = 2;
        for fail_truncation in [false, true] {
            let (mut sim, client) = cluster(0x5EED_0310);
            sim.register_client_with_primary(&client);
            let registered = sim.replicas[0].shards[0]
                .plane
                .metadata()
                .consensus
                .as_ref()
                .unwrap()
                .commit_min();
            for _ in 0..SETUP_TOTAL_STEPS {
                if sim.replicas[BACKUP].shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .unwrap()
                    .commit_min()
                    == registered
                {
                    break;
                }
                sim.step();
            }
            let primary = Rc::clone(&sim.replicas[0].shards[0]);
            let shard = Rc::clone(&sim.replicas[BACKUP].shards[0]);
            let outbox = Rc::clone(&sim.outboxes[BACKUP]);
            complete_now(&mut sim, async move {
                let metadata = shard.plane.metadata();
                let consensus = metadata.consensus.as_ref().unwrap();
                let journal = metadata.journal.as_ref().unwrap();
                let applied = consensus.commit_min();
                assert_eq!(applied, registered);
                let source_metadata = primary.plane.metadata();
                let source = source_metadata.consensus.as_ref().unwrap();
                let mut prepares = Vec::new();
                for name in [
                    "retained-first",
                    "retained-second",
                    "repair-gap",
                    "repair-head",
                ] {
                    let prepare = consensus::seal_prepare_capacity(
                        client.create_stream(name).project(source),
                        source.retry_capacity(&source_metadata.client_table.borrow()),
                    );
                    source.sequencer().set_sequence(prepare.header().op);
                    source.set_last_prepare_checksum(prepare.header().checksum);
                    prepares.push(prepare);
                }
                for (index, prepare) in prepares.iter().take(LOCAL_ENTRIES).enumerate() {
                    let entry = if fail_truncation && index == 0 {
                        prepare
                            .clone()
                            .transmute_header(|mut header, next: &mut PrepareHeader| {
                                header.client += 1;
                                header.checksum = header.identity_checksum();
                                *next = header;
                            })
                    } else {
                        prepare.clone()
                    };
                    journal.handle().append(entry).await.unwrap();
                }
                let old_head = prepares[LOCAL_ENTRIES - 1].header().op;
                let suffix_head = *prepares.last().unwrap().header();
                consensus.sequencer().set_sequence(old_head);
                let canonical: Vec<_> = if fail_truncation {
                    prepares
                        .iter()
                        .rev()
                        .map(|prepare| *prepare.header())
                        .collect()
                } else {
                    vec![suffix_head]
                };
                if fail_truncation {
                    journal.fail_next_truncate();
                }
                let header_size = size_of::<iggy_binary_protocol::StartViewHeader>();
                let mut message = Message::<iggy_binary_protocol::StartViewHeader>::new(
                    header_size + std::mem::size_of_val(canonical.as_slice()),
                );
                consensus::encode_prepare_headers(
                    &canonical,
                    &mut message.as_mut_slice()[header_size..],
                );
                let checksum_body = u128::from(iggy_common::calculate_checksum(
                    &message.as_slice()[header_size..],
                ));
                let total = message.as_slice().len();
                let message = message.transmute_header(
                    |_, header: &mut iggy_binary_protocol::StartViewHeader| {
                        header.command = Command::StartView;
                        header.cluster = consensus.cluster();
                        header.group = METADATA_GROUP;
                        header.view = NEW_VIEW;
                        header.replica = u8::try_from(NEW_VIEW).unwrap();
                        header.op = suffix_head.op;
                        header.commit = suffix_head.op;
                        header.size = u32::try_from(total).unwrap();
                        header.checksum_body = checksum_body;
                        header.seal();
                    },
                );
                shard
                    .on_message(MessageBag::StartView(message.clone()))
                    .await;
                assert_eq!(consensus.commit_min(), applied);
                assert!(
                    journal
                        .handle()
                        .header(usize::try_from(applied + 1).unwrap())
                        .is_some()
                );
                assert!(
                    journal
                        .handle()
                        .header(usize::try_from(old_head).unwrap())
                        .is_some()
                );
                assert!(consensus.view_log_unproven());
                assert!(!consensus.view_log_entry_proven(applied + 1));

                outbox.drain();
                let request =
                    Message::<iggy_binary_protocol::RequestPreparesHeader>::new(size_of::<
                        iggy_binary_protocol::RequestPreparesHeader,
                    >(
                    ))
                    .transmute_header(
                        |_, header: &mut iggy_binary_protocol::RequestPreparesHeader| {
                            header.command = Command::RequestPrepares;
                            header.cluster = consensus.cluster();
                            header.group = METADATA_GROUP;
                            header.replica = 0;
                            header.view = NEW_VIEW;
                            header.from_op = applied + 1;
                            header.to_op = old_head;
                            header.size = u32::try_from(std::mem::size_of_val(header)).unwrap();
                            header.seal();
                        },
                    );
                shard.on_message(MessageBag::RequestPrepares(request)).await;
                assert!(
                    outbox.drain().is_empty(),
                    "unproved entries cannot serve peer repair"
                );

                if fail_truncation {
                    shard.on_message(MessageBag::StartView(message)).await;
                    assert_eq!(consensus.view(), NEW_VIEW);
                    assert!(
                        journal
                            .handle()
                            .header(usize::try_from(applied + 1).unwrap())
                            .is_none(),
                        "the same-view retry must remove the proven mismatch"
                    );
                    assert!(
                        journal
                            .handle()
                            .header(usize::try_from(old_head).unwrap())
                            .is_some(),
                        "removing a mismatch must preserve the canonical tail"
                    );
                }
                for prepare in prepares.iter().rev() {
                    let repaired = prepare.clone().transmute_header(
                        |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                            repaired.0 = header;
                            repaired.0.command = Command::RepairPrepare;
                        },
                    );
                    shard.on_message(MessageBag::RepairPrepare(repaired)).await;
                }
                assert!(!consensus.view_log_unproven());
                metadata.commit_journal().await;
                assert_eq!(consensus.commit_min(), suffix_head.op);
                for prepare in prepares {
                    assert_eq!(
                        journal
                            .handle()
                            .header(usize::try_from(prepare.header().op).unwrap())
                            .unwrap()
                            .checksum,
                        prepare.header().checksum
                    );
                }
            });
        }
    }
    /// Where a backup crashes while it reconciles a `StartView` whose log conflicts
    /// with the suffix it journaled in an older view.
    #[derive(Debug, Clone, Copy)]
    enum ViewLogCrash {
        /// The divergent suffix is still in the WAL: its removal failed.
        RemovalFailed,
        /// The suffix is gone and one op of the adopted log was refilled.
        RefillLanded,
        /// The whole adopted log is proven.
        Proven,
    }

    /// A metadata `StartView` from `primary`, carrying `suffix` highest op first.
    fn metadata_start_view(
        cluster: u128,
        view: u32,
        primary: u8,
        commit: u64,
        suffix: &[PrepareHeader],
    ) -> Message<iggy_binary_protocol::StartViewHeader> {
        let header_size = size_of::<iggy_binary_protocol::StartViewHeader>();
        let mut message = Message::<iggy_binary_protocol::StartViewHeader>::new(
            header_size + std::mem::size_of_val(suffix),
        );
        consensus::encode_prepare_headers(suffix, &mut message.as_mut_slice()[header_size..]);
        let checksum_body = u128::from(iggy_common::calculate_checksum(
            &message.as_slice()[header_size..],
        ));
        let total = message.as_slice().len();
        message.transmute_header(|_, header: &mut iggy_binary_protocol::StartViewHeader| {
            header.command = Command::StartView;
            header.cluster = cluster;
            header.group = METADATA_GROUP;
            header.view = view;
            header.replica = primary;
            header.op = suffix[0].op;
            header.commit = commit;
            header.size = u32::try_from(total).unwrap();
            header.checksum_body = checksum_body;
            header.seal();
        })
    }

    fn as_repair_prepare(
        prepare: &Message<PrepareHeader>,
    ) -> Message<iggy_binary_protocol::RepairPrepareHeader> {
        prepare.clone().transmute_header(
            |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                repaired.0 = header;
                repaired.0.command = Command::RepairPrepare;
            },
        )
    }

    /// A backup journals an old view's suffix, adopts a view whose log conflicts with
    /// it, records the adopted view, and crashes before the log is proven. It must
    /// recover the adopted head and exact identities independently of its older
    /// body certificate. Repaired matching bodies survive, divergent ones do not.
    /// With the primary gone the survivors must still start a later view.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_an_unproven_adopted_view_when_the_backup_restarts_should_restore_identity_and_body_proof()
     {
        const BACKUP: u8 = 1;
        const PEER: u8 = 2;
        const OLD_VIEW: u32 = 0;
        // Led by replica 0, which crashes before the backups move on.
        const ADOPTED_VIEW: u32 = 3;
        const CONVERGE_STEPS: u32 = 3 * SETUP_TOTAL_STEPS;
        for crash in [
            ViewLogCrash::RemovalFailed,
            ViewLogCrash::RefillLanded,
            ViewLogCrash::Proven,
        ] {
            let (mut sim, client) = cluster(0x5EED_0C06);
            sim.register_client_with_primary(&client);
            let commit_min = |sim: &Simulator, replica: u8| {
                sim.replicas[usize::from(replica)].shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .unwrap()
                    .commit_min()
            };
            let registered = commit_min(&sim, 0);
            for _ in 0..SETUP_TOTAL_STEPS {
                if [BACKUP, PEER]
                    .iter()
                    .all(|&replica| commit_min(&sim, replica) == registered)
                {
                    break;
                }
                sim.step();
            }
            let primary = Rc::clone(&sim.replicas[0].shards[0]);
            let backup = Rc::clone(&sim.replicas[usize::from(BACKUP)].shards[0]);
            let peer = Rc::clone(&sim.replicas[usize::from(PEER)].shards[0]);
            let adopted = complete_now(&mut sim, async move {
                let source_metadata = primary.plane.metadata();
                let source = source_metadata.consensus.as_ref().unwrap();
                let base_op = source.sequencer().current_sequence();
                let base_checksum = source.last_prepare_checksum();
                assert_eq!(base_op, registered);
                // Two logs over the same committed prefix, as two views decide them.
                let build = |names: [&str; 2]| -> Vec<Message<PrepareHeader>> {
                    source.sequencer().set_sequence(base_op);
                    source.set_last_prepare_checksum(base_checksum);
                    names
                        .into_iter()
                        .map(|name| {
                            let prepare = consensus::seal_prepare_capacity(
                                client.create_stream(name).project(source),
                                source.retry_capacity(&source_metadata.client_table.borrow()),
                            );
                            source.sequencer().set_sequence(prepare.header().op);
                            source.set_last_prepare_checksum(prepare.header().checksum);
                            prepare
                        })
                        .collect()
                };
                let old_suffix = build(["old-view-first", "old-view-second"]);
                let adopted = build(["adopted-view-first", "adopted-view-second"]);
                source.sequencer().set_sequence(base_op);
                source.set_last_prepare_checksum(base_checksum);
                let commit_point = *source_metadata
                    .journal
                    .as_ref()
                    .unwrap()
                    .handle()
                    .header(usize::try_from(base_op).unwrap())
                    .unwrap();
                let start_view = metadata_start_view(
                    source.cluster(),
                    ADOPTED_VIEW,
                    0,
                    base_op,
                    &[*adopted[1].header(), *adopted[0].header(), commit_point],
                );

                for prepare in &old_suffix {
                    backup
                        .on_message(MessageBag::Prepare(prepare.clone()))
                        .await;
                }
                let backup_metadata = backup.plane.metadata();
                let backup_consensus = backup_metadata.consensus.as_ref().unwrap();
                let backup_journal = backup_metadata.journal.as_ref().unwrap();
                for prepare in &old_suffix {
                    let slot = usize::try_from(prepare.header().op).unwrap();
                    assert_eq!(
                        backup_journal
                            .handle()
                            .header(slot)
                            .map(|held| held.checksum),
                        Some(prepare.header().checksum),
                        "the backup journals the old view's suffix"
                    );
                }

                peer.on_message(MessageBag::StartView(start_view.clone()))
                    .await;
                for prepare in adopted.iter().rev() {
                    peer.on_message(MessageBag::RepairPrepare(as_repair_prepare(prepare)))
                        .await;
                }
                let peer_metadata = peer.plane.metadata();
                let peer_consensus = peer_metadata.consensus.as_ref().unwrap();
                assert!(
                    !peer_consensus.view_log_unproven(),
                    "the peer proves the log"
                );
                assert!(
                    peer_metadata
                        .persist_superblock_if_needed(peer_consensus)
                        .await
                );

                if matches!(crash, ViewLogCrash::RemovalFailed) {
                    backup_journal.fail_next_truncate();
                }
                backup.on_message(MessageBag::StartView(start_view)).await;
                assert_eq!(backup_consensus.log_view(), ADOPTED_VIEW);
                match crash {
                    ViewLogCrash::RemovalFailed => {}
                    ViewLogCrash::RefillLanded => {
                        backup
                            .on_message(MessageBag::RepairPrepare(as_repair_prepare(&adopted[0])))
                            .await;
                    }
                    ViewLogCrash::Proven => {
                        for prepare in adopted.iter().rev() {
                            backup
                                .on_message(MessageBag::RepairPrepare(as_repair_prepare(prepare)))
                                .await;
                        }
                    }
                }
                assert_eq!(
                    backup_consensus.view_log_unproven(),
                    !matches!(crash, ViewLogCrash::Proven)
                );
                // What the tick would do next: record the adopted view.
                assert!(
                    backup_metadata
                        .persist_superblock_if_needed(backup_consensus)
                        .await
                );
                adopted
                    .iter()
                    .map(|prepare| *prepare.header())
                    .collect::<Vec<_>>()
            });

            sim.replica_crash(0);
            sim.replica_crash(BACKUP);
            sim.replica_restart(BACKUP);
            {
                let metadata = sim.replicas[usize::from(BACKUP)].shards[0].plane.metadata();
                let consensus = metadata.consensus.as_ref().unwrap();
                let held = metadata
                    .journal
                    .as_ref()
                    .unwrap()
                    .handle()
                    .header(usize::try_from(adopted[0].op).unwrap())
                    .map(|header| header.checksum);
                assert_eq!(consensus.log_view(), ADOPTED_VIEW, "{crash:?}");
                assert_eq!(consensus.log_view_claim(), ADOPTED_VIEW, "{crash:?}");
                assert_eq!(
                    consensus.adopted_view_head(),
                    Some((ADOPTED_VIEW, adopted[1].op))
                );
                let proven = matches!(crash, ViewLogCrash::Proven);
                assert_eq!(consensus.view_log_unproven(), !proven, "{crash:?}");
                assert_eq!(
                    consensus.history_certificate().unwrap().view,
                    if proven { ADOPTED_VIEW } else { OLD_VIEW },
                    "{crash:?}"
                );
                assert_eq!(
                    held,
                    (!matches!(crash, ViewLogCrash::RemovalFailed)).then_some(adopted[0].checksum),
                    "{crash:?}: only the vouched repaired body may survive",
                );
            }

            let adopted_view_holder = |sim: &Simulator, replica: u8| {
                let metadata = sim.replicas[usize::from(replica)].shards[0]
                    .plane
                    .metadata();
                let consensus = metadata.consensus.as_ref().unwrap();
                let journal = metadata.journal.as_ref().unwrap();
                let holds_log = adopted.iter().all(|header| {
                    journal
                        .handle()
                        .header(usize::try_from(header.op).unwrap())
                        .is_some_and(|held| held.checksum == header.checksum)
                });
                (consensus.status() == Status::Normal
                    && !consensus.view_log_unproven()
                    && consensus.view() > ADOPTED_VIEW
                    && holds_log)
                    .then(|| consensus.view())
            };
            let mut converged = None;
            for _ in 0..CONVERGE_STEPS {
                let views = (
                    adopted_view_holder(&sim, BACKUP),
                    adopted_view_holder(&sim, PEER),
                );
                if let (Some(backup_view), Some(peer_view)) = views
                    && backup_view == peer_view
                {
                    converged = Some(backup_view);
                    break;
                }
                sim.step();
            }
            assert!(
                converged.is_some(),
                "{crash:?}: the backup and its peer never started a view past {ADOPTED_VIEW} \
                 holding the adopted log"
            );
        }
    }

    /// A record from a build without history certificates proves nothing about the
    /// WAL beside it. The restarted replica may hold ops a quorum committed in a view
    /// it can no longer claim, so when its view probe goes unanswered it must not
    /// elect under a lower `log_view` with the one peer left. It waits for the
    /// `StartView` of an election it took no part in, proves its log against it, and
    /// records a certificate.
    #[test]
    fn given_a_record_without_a_history_certificate_when_no_primary_answers_should_wait_for_a_start_view()
     {
        const LEGACY: u8 = 1;
        const PEER: u8 = 2;
        // Several times what an unanswered probe takes to fall back to an election.
        const PROBE_STEPS: u32 = 3 * SETUP_TOTAL_STEPS;
        const CONVERGE_STEPS: u32 = 3 * SETUP_TOTAL_STEPS;
        let (mut sim, client) = cluster(0x5EED_0C07);
        sim.register_client_with_primary(&client);
        let with_consensus = |sim: &Simulator, replica: u8| {
            let metadata = sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata();
            let consensus = metadata.consensus.as_ref().unwrap();
            (
                consensus.status(),
                consensus.view_log_unproven(),
                consensus.history_certified(),
                consensus.recovery_election_allowed(),
                consensus.commit_min(),
            )
        };
        let registered = with_consensus(&sim, 0).4;
        for _ in 0..SETUP_TOTAL_STEPS {
            if [LEGACY, PEER]
                .iter()
                .all(|&replica| with_consensus(&sim, replica).4 == registered)
            {
                break;
            }
            sim.step();
        }
        let legacy_record = sim.replicas[usize::from(LEGACY)].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .unwrap()
            .vsr_state(0, 0)
            .with_history_certificate(None);

        sim.replica_crash(0);
        sim.replica_crash(LEGACY);
        let superblock = Rc::clone(&sim.replicas[usize::from(LEGACY)].superblock);
        journal::superblock::SuperblockStore::write(&*superblock, &legacy_record.to_bytes())
            .now_or_never()
            .expect("the simulated superblock writes on first poll")
            .expect("the simulated superblock accepts the record");
        sim.replica_restart(LEGACY);
        let (status, unproven, _, election_allowed, _) = with_consensus(&sim, LEGACY);
        assert_eq!(status, Status::Recovering);
        assert!(unproven, "nothing proves the restarted WAL");
        assert!(!election_allowed);

        for _ in 0..PROBE_STEPS {
            sim.step();
        }
        assert_eq!(
            with_consensus(&sim, LEGACY).0,
            Status::Recovering,
            "an unanswered probe let an unproven log vote"
        );
        assert_ne!(
            with_consensus(&sim, PEER).0,
            Status::Normal,
            "one peer started a view without a quorum"
        );

        sim.replica_restart(0);
        let mut certified = false;
        for _ in 0..CONVERGE_STEPS {
            let (status, unproven, history_certified, election_allowed, _) =
                with_consensus(&sim, LEGACY);
            let recorded = superblock
                .read_latest_sync()
                .and_then(|bytes| consensus::VsrState::try_from(&bytes[..]).ok())
                .is_some_and(|state| {
                    state
                        .history_certificate()
                        .is_some_and(|certificate| certificate.view == state.log_view)
                });
            if status == Status::Normal
                && !unproven
                && history_certified
                && election_allowed
                && recorded
            {
                certified = true;
                break;
            }
            sim.step();
        }
        assert!(
            certified,
            "the restarted replica never proved its log against the new view and recorded it"
        );
    }

    /// A restarted backup adopts the view and arms repair up to the head the
    /// `StartView` names. A second `StartView` of the same view raises the head
    /// while that session stands, and the window's terminator is lost, so the
    /// session closes one op short of what the view-log proof waits on. The walk
    /// refuses the unproven log and no commit edge comes, since the backup's
    /// commit point already matches the primary's. Only the tick can arm the rest.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_repair_window_below_the_proof_op_when_the_session_closes_should_arm_the_rest_from_the_tick()
     {
        const PRIMARY: u8 = 0;
        const BACKUP: u8 = 2;
        const MISSED_OPS: u64 = 2;
        // The stall retry that closes the short window, the gap debounce, then
        // the repair round trip, with margin.
        const HEAL_STEPS: u32 = 4 * partitions::REPAIR_RETRY_TICKS;
        // Probe answers would re-adopt the view and arm repair on their own, and a
        // lost terminator is what leaves the stall retry to close the window.
        fn starve_backup(packet: &packet::Packet) -> bool {
            matches!(
                packet.message.header().command,
                Command::StartView | Command::RepairDone
            )
        }

        let (mut sim, client) = cluster(0x5EED_0C08);
        sim.register_client_with_primary(&client);
        let consensus_at = |sim: &Simulator, replica: u8| {
            let metadata = sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata();
            let consensus = metadata.consensus.as_ref().unwrap();
            (
                consensus.status(),
                consensus.view(),
                consensus.view_log_unproven(),
                consensus.commit_min(),
                consensus.commit_max(),
            )
        };
        let base = consensus_at(&sim, PRIMARY).3;
        for _ in 0..SETUP_TOTAL_STEPS {
            if consensus_at(&sim, BACKUP).3 == base {
                break;
            }
            sim.step();
        }
        assert_eq!(consensus_at(&sim, BACKUP).3, base);

        sim.replica_crash(BACKUP);
        let head = base + MISSED_OPS;
        for index in 0..MISSED_OPS {
            let msg = client.create_stream(&format!("missed-{index}"));
            sim.submit_request(client.client_id(), PRIMARY, msg.into_generic());
            for _ in 0..SETUP_TOTAL_STEPS {
                if consensus_at(&sim, PRIMARY).3 == base + index + 1 {
                    break;
                }
                sim.step();
            }
        }
        assert_eq!(consensus_at(&sim, PRIMARY).3, head);

        let primary_metadata = sim.replicas[usize::from(PRIMARY)].shards[0]
            .plane
            .metadata();
        let cluster_id = primary_metadata.consensus.as_ref().unwrap().cluster();
        let header_at = |op: u64| {
            *primary_metadata
                .journal
                .as_ref()
                .unwrap()
                .handle()
                .header(usize::try_from(op).unwrap())
                .unwrap()
        };
        let first_head = metadata_start_view(
            cluster_id,
            0,
            PRIMARY,
            head - 1,
            &[header_at(head - 1), header_at(base)],
        );
        let raised_head = metadata_start_view(
            cluster_id,
            0,
            PRIMARY,
            head,
            &[header_at(head), header_at(head - 1), header_at(base)],
        );

        sim.replica_restart(BACKUP);
        assert_eq!(consensus_at(&sim, BACKUP).0, Status::Recovering);
        for peer in [PRIMARY, 1] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(peer), ProcessId::Replica(BACKUP)) =
                Some(starve_backup);
        }
        let backup = Rc::clone(&sim.replicas[usize::from(BACKUP)].shards[0]);
        complete_now(&mut sim, async move {
            backup.on_message(MessageBag::StartView(first_head)).await;
            backup.on_message(MessageBag::StartView(raised_head)).await;
        });
        let (status, view, unproven, _, commit_max) = consensus_at(&sim, BACKUP);
        assert_eq!((status, view, unproven), (Status::Normal, 0, true));
        assert_eq!(commit_max, head);
        assert_eq!(
            sim.replicas[usize::from(BACKUP)].shards[0].metadata_repair_window(),
            Some((head - 1, PRIMARY)),
            "the session armed for the first head must stand when the head is raised"
        );

        for _ in 0..HEAL_STEPS {
            if consensus_at(&sim, BACKUP).3 == head {
                break;
            }
            sim.step();
        }
        let (status, view, unproven, commit_min, _) = consensus_at(&sim, BACKUP);
        assert_eq!(
            (status, view),
            (Status::Normal, 0),
            "a view change healed the backup instead of the repair driver"
        );
        assert_eq!(
            (commit_min, unproven),
            (head, false),
            "the backup never proved op {head}: its repair window closed at {}, and \
             nothing armed the rest",
            head - 1
        );
    }

    /// The test above without a lag below the frontier. A lost quorum leaves
    /// every op above `base` uncommitted, so the restarted backup's commit point
    /// matches the primary's and no commit edge or walk can follow. Its window
    /// closes below the op the raised head's proof waits on, and only a probe
    /// that reads the proof's op can arm the rest.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_proof_op_above_a_caught_up_commit_point_when_the_session_closes_should_arm_the_rest_from_the_tick()
     {
        const REPLICA_COUNT: u8 = 3;
        const PRIMARY: u8 = 0;
        const DOWN: u8 = 1;
        const BACKUP: u8 = 2;
        // One op each: a client has one request in flight.
        const CLIENTS: [u128; 3] = [1, 2, 3];
        const HEAL_STEPS: u32 = 4 * partitions::REPAIR_RETRY_TICKS;
        fn drop_prepare_ok(packet: &packet::Packet) -> bool {
            packet.message.header().command == Command::PrepareOk
        }
        fn starve_backup(packet: &packet::Packet) -> bool {
            matches!(
                packet.message.header().command,
                Command::StartView | Command::RepairDone
            )
        }
        fn journal_holds(sim: &Simulator, replica: u8, op: u64) -> bool {
            sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata()
                .journal
                .as_ref()
                .unwrap()
                .handle()
                .header(usize::try_from(op).unwrap())
                .is_some()
        }
        fn submit_until_journaled(sim: &mut Simulator, client: &SimClient, replica: u8, op: u64) {
            let msg = client.create_stream(&format!("uncommitted-{op}"));
            sim.submit_request(client.client_id(), PRIMARY, msg.into_generic());
            for _ in 0..SETUP_TOTAL_STEPS {
                if journal_holds(sim, replica, op) {
                    break;
                }
                sim.step();
            }
            assert!(
                journal_holds(sim, replica, op),
                "replica {replica} never journaled op {op}"
            );
        }

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::new(
            usize::from(REPLICA_COUNT),
            CLIENTS.iter().copied(),
            packet::PacketSimulatorOptions {
                node_count: REPLICA_COUNT,
                client_count: u8::try_from(CLIENTS.len()).expect("test clients fit u8"),
                seed: 0x5EED_0C11,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let clients = CLIENTS.map(SimClient::new);
        for client in &clients {
            sim.register_client_with_primary(client);
        }
        let consensus_at = |sim: &Simulator, replica: u8| {
            let metadata = sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata();
            let consensus = metadata.consensus.as_ref().unwrap();
            (
                consensus.status(),
                consensus.view(),
                consensus.view_log_unproven(),
                consensus.commit_min(),
                consensus.commit_max(),
            )
        };
        let base = consensus_at(&sim, PRIMARY).3;
        for _ in 0..SETUP_TOTAL_STEPS {
            if consensus_at(&sim, BACKUP).3 == base {
                break;
            }
            sim.step();
        }
        assert_eq!(consensus_at(&sim, BACKUP).3, base);

        // The backup journals the first op, whose stamp makes `base` its
        // recovered commit point, but its ack is lost, so nothing above `base`
        // can commit.
        sim.replica_crash(DOWN);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(BACKUP), ProcessId::Replica(PRIMARY)) =
            Some(drop_prepare_ok);
        submit_until_journaled(&mut sim, &clients[0], BACKUP, base + 1);
        sim.replica_crash(BACKUP);
        submit_until_journaled(&mut sim, &clients[1], PRIMARY, base + 2);
        submit_until_journaled(&mut sim, &clients[2], PRIMARY, base + 3);
        let head = base + 3;
        assert_eq!(consensus_at(&sim, PRIMARY).4, base);

        let primary_metadata = sim.replicas[usize::from(PRIMARY)].shards[0]
            .plane
            .metadata();
        let cluster_id = primary_metadata.consensus.as_ref().unwrap().cluster();
        let header_at = |op: u64| {
            *primary_metadata
                .journal
                .as_ref()
                .unwrap()
                .handle()
                .header(usize::try_from(op).unwrap())
                .unwrap()
        };
        let first_head = metadata_start_view(
            cluster_id,
            0,
            PRIMARY,
            base,
            &[header_at(head - 1), header_at(base + 1), header_at(base)],
        );
        let raised_head = metadata_start_view(
            cluster_id,
            0,
            PRIMARY,
            base,
            &[
                header_at(head),
                header_at(head - 1),
                header_at(base + 1),
                header_at(base),
            ],
        );

        sim.replica_restart(BACKUP);
        let (status, _, _, commit_min, _) = consensus_at(&sim, BACKUP);
        assert_eq!((status, commit_min), (Status::Recovering, base));
        for peer in [PRIMARY, DOWN] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(peer), ProcessId::Replica(BACKUP)) =
                Some(starve_backup);
        }
        let backup = Rc::clone(&sim.replicas[usize::from(BACKUP)].shards[0]);
        complete_now(&mut sim, async move {
            backup.on_message(MessageBag::StartView(first_head)).await;
            backup.on_message(MessageBag::StartView(raised_head)).await;
        });
        assert_eq!(
            consensus_at(&sim, BACKUP),
            (Status::Normal, 0, true, base, base)
        );
        assert_eq!(
            sim.replicas[usize::from(BACKUP)].shards[0].metadata_repair_window(),
            Some((head - 1, PRIMARY)),
            "the session armed for the first head must stand when the head is raised"
        );

        for _ in 0..HEAL_STEPS {
            if !consensus_at(&sim, BACKUP).2 {
                break;
            }
            sim.step();
        }
        let (status, view, unproven, _, _) = consensus_at(&sim, BACKUP);
        assert_eq!(
            (status, view),
            (Status::Normal, 0),
            "a view change healed the backup instead of the repair driver"
        );
        assert!(
            !unproven,
            "the backup never proved op {head}: its repair window closed at {}, and \
             nothing armed the rest",
            head - 1
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_metadata_promotion_after_commit_resume_when_the_pipeline_owns_the_next_op_should_keep_its_body()
     {
        const NEW_PRIMARY: usize = 1;
        const NEW_VIEW: u32 = 1;
        const STREAM: &str = "promoted-pipeline-entry";
        let (mut sim, client) = cluster(0x5EED_0311);
        sim.register_client_with_primary(&client);
        let primary = Rc::clone(&sim.replicas[0].shards[0]);
        let candidate = Rc::clone(&sim.replicas[NEW_PRIMARY].shards[0]);
        let registered_op = primary
            .plane
            .metadata()
            .consensus
            .as_ref()
            .unwrap()
            .commit_min();
        for _ in 0..SETUP_TOTAL_STEPS {
            if candidate
                .plane
                .metadata()
                .consensus
                .as_ref()
                .unwrap()
                .commit_min()
                == registered_op
            {
                break;
            }
            sim.step();
        }
        complete_now(&mut sim, async move {
            let primary_metadata = primary.plane.metadata();
            let primary_consensus = primary_metadata.consensus.as_ref().unwrap();
            let prepare = consensus::seal_prepare_capacity(
                client.create_stream(STREAM).project(primary_consensus),
                primary_consensus.retry_capacity(&primary_metadata.client_table.borrow()),
            );
            let prepare_header = *prepare.header();
            let metadata = candidate.plane.metadata();
            let consensus = metadata.consensus.as_ref().unwrap();
            let journal = metadata.journal.as_ref().unwrap();
            let applied = consensus.commit_min();
            assert_eq!(
                applied, registered_op,
                "registration must apply on the candidate"
            );
            assert_eq!(prepare_header.op, applied + 1);
            assert_eq!(consensus.sequencer().current_sequence(), applied);

            // A commit can arrive ahead of its body. The DVC clamps that
            // frontier to the resident head, while consensus retains it.
            consensus.advance_commit_max(prepare_header.op);
            let header_size = size_of::<iggy_binary_protocol::DoViewChangeHeader>();
            let mut message = Message::<iggy_binary_protocol::DoViewChangeHeader>::new(
                header_size + size_of::<PrepareHeader>(),
            );
            consensus::encode_prepare_headers(
                &[prepare_header],
                &mut message.as_mut_slice()[header_size..],
            );
            let checksum_body = u128::from(iggy_common::calculate_checksum(
                &message.as_slice()[header_size..],
            ));
            let total = message.as_slice().len();
            let message = message.transmute_header(
                |_, header: &mut iggy_binary_protocol::DoViewChangeHeader| {
                    header.command = Command::DoViewChange;
                    header.cluster = consensus.cluster();
                    header.group = METADATA_GROUP;
                    header.view = NEW_VIEW;
                    header.replica = 0;
                    header.log_view = prepare_header.view;
                    header.op = prepare_header.op;
                    header.commit = applied;
                    header.present_bitset = 1;
                    header.size = u32::try_from(total).unwrap();
                    header.checksum_body = checksum_body;
                    header.seal();
                },
            );
            candidate
                .on_message(MessageBag::DoViewChange(message))
                .await;
            assert_eq!(consensus.status(), Status::ViewChange);
            let pending = consensus.pending_view_log().unwrap();
            assert_eq!(pending.op_head, prepare_header.op);
            assert_eq!(pending.commit_max, applied);
            candidate.tick_metadata().await;

            let repaired = prepare.transmute_header(
                |header, repaired: &mut iggy_binary_protocol::RepairPrepareHeader| {
                    repaired.0 = header;
                    repaired.0.command = Command::RepairPrepare;
                },
            );
            candidate
                .on_message(MessageBag::RepairPrepare(repaired))
                .await;
            assert!(journal.handle().entry(&prepare_header).await.is_some());

            candidate.tick_metadata().await;
            assert!(consensus.is_primary() && consensus.is_normal());
            assert_eq!(consensus.commit_min(), applied);
            assert_eq!(consensus.commit_max(), prepare_header.op);
            assert_eq!(
                consensus.pipeline_head_header().map(|header| header.op),
                Some(prepare_header.op),
                "promotion must rebuild the entry after the stranded-commit driver ran"
            );
            candidate.tick_metadata().await;
            assert_eq!(consensus.commit_min(), prepare_header.op);
            assert!(consensus.pipeline_head_header().is_none());
            assert!(
                journal.handle().entry(&prepare_header).await.is_some(),
                "the tick must not discard the body owned by the rebuilt pipeline"
            );
            assert!(metadata.mux_stm.streams().read(|streams| {
                streams
                    .items
                    .iter()
                    .any(|(_, stream)| &*stream.name == STREAM)
            }));
        });
    }

    /// Runs `future` on the simulator executor. Its output must be ready once
    /// the pumps drain.
    fn complete_now<T: Send + 'static>(
        sim: &mut Simulator,
        future: impl Future<Output = T> + 'static,
    ) -> T {
        let (sender, receiver) = shard::channel(1);
        sim.executor.spawn(async move {
            let _ = sender.try_send(future.await);
        });
        sim.run_pumps();
        receiver
            .recv()
            .now_or_never()
            .expect("the future completes once the pumps drain")
            .expect("the sender stays open until it sends")
    }

    /// Replica 0 acknowledges `write`, whose commit never reaches the backups,
    /// then crashes. The write sits above the merged commit of the next view,
    /// and the backups hold their `PrepareOk`s, so the new primary starts that
    /// view without applying it. Returns the new primary and the op of the
    /// write.
    fn fail_over_before_the_write_applies(
        sim: &mut Simulator,
        client: &SimClient,
        namespace: IggyNamespace,
        write: Message<RoutedRequestHeader>,
    ) -> (usize, u64) {
        const FAILOVER_STEPS: usize = 2_000;

        // Link hooks are bare `fn` pointers, so the group travels in a static.
        static GROUP: AtomicU64 = AtomicU64::new(0);

        fn starve_commits(packet: &packet::Packet) -> bool {
            is_commit_for(packet, GROUP.load(Ordering::Relaxed))
        }

        fn hold_prepare_oks(packet: &packet::Packet) -> bool {
            packet.message.header().command == Command::PrepareOk
                && bytemuck::checked::from_bytes::<PrepareOkHeader>(
                    &packet.message.as_slice()[..size_of::<PrepareOkHeader>()],
                )
                .group
                    == GROUP.load(Ordering::Relaxed)
        }

        GROUP.store(namespace.inner(), Ordering::Relaxed);
        for backup in [1, 2] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(backup)) =
                Some(starve_commits);
        }
        let acknowledged = submit_and_wait_for_reply(sim, client.client_id(), 0, write);
        assert_eq!(acknowledged.header().status, 0);
        let write_op = sim
            .partition_consensus_state(0, namespace)
            .expect("the old primary hosts the partition")
            .commit_min;
        sim.replica_crash(0);
        for (from, to) in [(1, 2), (2, 1)] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) =
                Some(hold_prepare_oks);
        }

        let new_primary = (0..FAILOVER_STEPS)
            .find_map(|_| {
                sim.step();
                [1usize, 2].into_iter().find(|replica| {
                    sim.partition_consensus_state(*replica, namespace)
                        .is_some_and(|state| state.is_primary && state.status == Status::Normal)
                })
            })
            .expect("the backups elect a new partition primary");
        let (commit_min, commit_max) = sim.replicas[new_primary]
            .partition_shard(namespace)
            .plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let consensus = partition.consensus();
                (consensus.commit_min(), consensus.commit_max())
            })
            .expect("the new primary hosts the partition");
        assert_eq!(
            commit_min, commit_max,
            "the new primary applied everything it knows to be committed"
        );
        assert!(
            commit_min < write_op,
            "the new primary must not have applied the acknowledged write yet"
        );
        (new_primary, write_op)
    }

    /// Lets the `PrepareOk`s that [`fail_over_before_the_write_applies`] holds
    /// through, and steps until the new primary applies the write.
    fn release_held_prepare_oks(
        sim: &mut Simulator,
        namespace: IggyNamespace,
        new_primary: usize,
        write_op: u64,
    ) {
        const RECOMMIT_STEPS: usize = 2_000;

        for (from, to) in [(1, 2), (2, 1)] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) = None;
        }
        let applied = (0..RECOMMIT_STEPS).any(|_| {
            sim.step();
            sim.partition_consensus_state(new_primary, namespace)
                .is_some_and(|state| state.commit_min >= write_op)
        });
        assert!(
            applied,
            "the new view re-commits the write once PrepareOk flows"
        );
    }

    /// The old primary acknowledges an offset store whose commit never reaches
    /// the backups, then crashes. The store sits above the merged commit of the
    /// next view, so the new primary has not applied it. Until it does, an
    /// offset read and a `Next` poll must answer `TransientNotAccepted` instead
    /// of reading the older offset.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_an_acknowledged_offset_store_when_the_primary_fails_over_should_not_read_back_the_older_offset()
     {
        const CONSUMER_ID: u32 = 7;
        const CONSUMER_KIND: u8 = 1;
        const OLDER_OFFSET: u64 = 1;
        const ACKNOWLEDGED_OFFSET: u64 = 2;
        const MESSAGES: usize = 4;
        const SETTLE_STEPS: usize = 200;

        let (mut sim, client) = cluster(0x5EED_0411);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        let consumer = PollingConsumer::Consumer(CONSUMER_ID as usize, 0);
        let next_poll = PollingArgs::new(iggy_common::PollingStrategy::next(), 1, false);
        for index in 0..MESSAGES {
            let message = Bytes::from(format!("message-{index}"));
            let reply = submit_and_wait_for_reply(
                &mut sim,
                client.client_id(),
                0,
                client.send_messages(namespace, &[message]),
            );
            assert_eq!(reply.header().status, 0);
        }
        let stored = submit_and_wait_for_reply(
            &mut sim,
            client.client_id(),
            0,
            client.store_consumer_offset(
                namespace,
                CONSUMER_KIND,
                CONSUMER_ID,
                OLDER_OFFSET,
                AckLevel::Quorum,
            ),
        );
        assert_eq!(stored.header().status, 0);
        for _ in 0..SETTLE_STEPS {
            sim.step();
        }

        let acknowledged = client.store_consumer_offset(
            namespace,
            CONSUMER_KIND,
            CONSUMER_ID,
            ACKNOWLEDGED_OFFSET,
            AckLevel::Quorum,
        );
        let (new_primary, store_op) =
            fail_over_before_the_write_applies(&mut sim, &client, namespace, acknowledged);
        let owner = Rc::clone(sim.replicas[new_primary].partition_shard(namespace));
        let (stored_offset, _) = owner
            .plane
            .partitions()
            .consumer_offset_read(&namespace, consumer)
            .expect("the new primary hosts the group");
        assert_eq!(
            stored_offset,
            Some(OLDER_OFFSET),
            "the applied state still holds the older offset"
        );

        let offset_read = Rc::clone(&owner);
        let reply = complete_now(&mut sim, async move {
            offset_read
                .partition_read(namespace, shard::PartitionRead::ConsumerOffset { consumer })
                .await
        });
        assert!(
            matches!(
                reply,
                Some(shard::PartitionReadReply::Rejected(
                    IggyError::TransientNotAccepted
                ))
            ),
            "an offset read before the view start applies must be refused, got {reply:?}"
        );
        let poll = sim.poll_messages(new_primary, namespace, consumer, &next_poll);
        let polled = complete_now(&mut sim, poll);
        assert!(
            matches!(polled, Err(IggyError::TransientNotAccepted)),
            "a Next poll before the view start applies must be refused, got {polled:?}"
        );

        release_held_prepare_oks(&mut sim, namespace, new_primary, store_op);
        let offset_read = Rc::clone(&owner);
        let reply = complete_now(&mut sim, async move {
            offset_read
                .partition_read(namespace, shard::PartitionRead::ConsumerOffset { consumer })
                .await
        });
        assert!(
            matches!(
                reply,
                Some(shard::PartitionReadReply::ConsumerOffset {
                    stored: Some(ACKNOWLEDGED_OFFSET),
                    ..
                })
            ),
            "the read after the view start applies returns the acknowledged offset, got {reply:?}"
        );
        let poll = sim.poll_messages(new_primary, namespace, consumer, &next_poll);
        let polled = complete_now(&mut sim, poll);
        assert!(
            polled.is_ok_and(|fragments| !fragments.is_empty()),
            "a Next poll after the view start applies is served"
        );
    }

    /// The old primary acknowledges a send whose commit never reaches the
    /// backups, then crashes. Until the new primary applies the send, a poll
    /// from its offset must answer `TransientNotAccepted` instead of returning
    /// no messages.
    #[test]
    fn given_an_acknowledged_send_when_the_primary_fails_over_should_not_poll_without_it() {
        const PAYLOAD: &[u8] = b"acknowledged-before-failover";
        const SEND_OFFSET: u64 = 0;
        const CONSUMER_ID: usize = 7;

        let (mut sim, client) = cluster(0x5EED_0412);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        let consumer = PollingConsumer::Consumer(CONSUMER_ID, 0);
        let offset_poll =
            PollingArgs::new(iggy_common::PollingStrategy::offset(SEND_OFFSET), 1, false);

        let acknowledged = client.send_messages(namespace, &[Bytes::from_static(PAYLOAD)]);
        let (new_primary, send_op) =
            fail_over_before_the_write_applies(&mut sim, &client, namespace, acknowledged);
        let poll = sim.poll_messages(new_primary, namespace, consumer, &offset_poll);
        let polled = complete_now(&mut sim, poll);
        assert!(
            matches!(polled, Err(IggyError::TransientNotAccepted)),
            "a poll before the view start applies must be refused, got {polled:?}"
        );

        release_held_prepare_oks(&mut sim, namespace, new_primary, send_op);
        let poll = sim.poll_messages(new_primary, namespace, consumer, &offset_poll);
        let fragments =
            complete_now(&mut sim, poll).expect("a poll after the view start applies is served");
        assert!(
            fragments.iter().any(|fragment| fragment
                .as_slice()
                .windows(PAYLOAD.len())
                .any(|bytes| bytes == PAYLOAD)),
            "the poll returns the acknowledged message"
        );
    }

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

    /// Manual fixture seeding chooses the current metadata view. Materializing
    /// that fixture must preserve the choice on every replica. This tests the
    /// creation-view handoff, not the production placement selector.
    #[test]
    fn given_a_chosen_creation_view_when_a_group_materialises_should_seed_that_view() {
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

        // Move the metadata plane off view 0 by crashing its view-0 primary.
        // Nothing has been written, so no partition group exists to move with
        // it: the group created below is genuinely fresh.
        sim.replica_crash(0);
        for _ in 0..800 {
            sim.step();
        }

        let metadata_primary = sim
            .metadata_primary_index()
            .expect("a live replica must own metadata consensus");
        assert_ne!(
            metadata_primary, 0,
            "the fixture needs a chosen view whose primary differs from the default replica 0"
        );

        // The manual fixture chooses this metadata view as its creation view.
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);

        let partition_primary = sim
            .primary_index(namespace)
            .expect("the group must exist on a live replica after materialisation");
        assert_eq!(
            partition_primary, metadata_primary,
            "materialization must preserve the primary selected by the fixture's creation view"
        );

        // Per replica, not just the aggregate: the seed is read locally on each
        // one, so a single replica left at view 0 would still elect itself
        // primary of that group while its peers disagree, and the aggregate
        // read above would not see it.
        for replica_idx in 0..replica_count {
            if sim.is_crashed(replica_idx) {
                continue;
            }
            let state = sim
                .partition_consensus_state(usize::from(replica_idx), namespace)
                .expect("a live replica must host the freshly materialised group");
            // Compared as primaries, not as view numbers: the two coincide only
            // while the view is below `replica_count`, and pinning the view
            // itself would make this fail on a second election for no reason.
            let seeded_primary = u8::try_from(state.view % u32::from(replica_count))
                .expect("a value modulo replica_count fits the u8 replica_count");
            assert_eq!(
                seeded_primary, metadata_primary,
                "replica {replica_idx} seeded its group at view {}, naming replica \
                 {seeded_primary} instead of the fixture's chosen primary {metadata_primary}",
                state.view
            );
        }
    }

    /// A replica that missed a group's creation materialises it after the
    /// metadata plane elected again. It must seed the view the group was CREATED
    /// in, not the live metadata view: seeded above the group's real view, its
    /// empty log outranks every peer in the next DVC merge and the committed ops
    /// collect a nack quorum, wedging the group for good.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_late_materialiser_when_the_metadata_view_moved_on_should_seed_the_creation_view() {
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
        let settle = |sim: &mut Simulator| {
            for _ in 0..800 {
                sim.step();
            }
        };
        let reply_within = |sim: &mut Simulator, budget: usize| {
            (0..budget).find_map(|_| {
                let replies = sim.step();
                (!replies.is_empty()).then(|| replies[0].deep_copy())
            })
        };

        // Replica 0 is down while the group is created, at a metadata view its
        // crash moved off 0.
        sim.replica_crash(0);
        settle(&mut sim);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        let created_view = sim
            .partition_consensus_state(1, namespace)
            .expect("replica 1 materialised the group")
            .view;
        assert!(
            created_view > 0,
            "crashing replica 0 must have moved the metadata plane off view 0, or a \
             creation-view seed is indistinguishable from a view-0 start"
        );

        // Commit a write, so the group holds history a wrong seed could outrank.
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);
        let primary = sim
            .primary_index(namespace)
            .expect("a live replica hosts the group");
        let request = client.send_messages(namespace, &[Bytes::from_static(b"before")]);
        sim.submit_request(client_id, primary, request.into_generic());
        reply_within(&mut sim, 200).expect("the write commits on the fresh group");

        // Replica 0 returns without the group, then the metadata plane elects
        // again, so its live view now exceeds the view the group was created in.
        sim.replica_restart(0);
        settle(&mut sim);
        let metadata_primary = sim
            .metadata_primary_index()
            .expect("a live replica owns metadata consensus");
        sim.replica_crash(metadata_primary);
        settle(&mut sim);
        let moved_view = sim
            .metadata_view()
            .expect("a live replica owns metadata consensus");
        assert!(
            moved_view > created_view,
            "the second election must move the metadata view ({moved_view}) past the \
             group's creation view ({created_view})"
        );

        // Replica 0 materialises the group now.
        sim.init_partition(namespace);
        let seeded = sim
            .partition_consensus_state(0, namespace)
            .expect("replica 0 materialised the group")
            .view;
        assert_eq!(
            seeded, created_view,
            "a late materialiser must seed the group's creation view, not the live metadata \
             view {moved_view}: above the group's real view its empty log wins the next merge"
        );

        // With replica 0 in, the group has a quorum again: it elects past its
        // dead primary and commits a new write.
        let settled_primary = (0..4)
            .find_map(|_| {
                settle(&mut sim);
                (0..replica_count)
                    .filter(|replica_idx| !sim.is_crashed(*replica_idx))
                    .find(|replica_idx| {
                        sim.partition_consensus_state(usize::from(*replica_idx), namespace)
                            .is_some_and(|state| state.status == Status::Normal && state.is_primary)
                    })
            })
            .unwrap_or_else(|| {
                panic!(
                    "the group must elect a live primary once replica 0 joins it: {:?}",
                    (0..replica_count)
                        .map(|replica| (
                            replica,
                            sim.is_crashed(replica),
                            sim.partition_consensus_state(usize::from(replica), namespace)
                        ))
                        .collect::<Vec<_>>()
                )
            });
        let request = client.send_messages(namespace, &[Bytes::from_static(b"after")]);
        sim.submit_request(client_id, settled_primary, request.into_generic());
        reply_within(&mut sim, 400).expect(
            "a write must commit after the late materialiser joined; a wedged merge \
             answers nothing",
        );
    }

    #[test]
    fn given_committed_unmaterialized_groups_when_restarted_should_complete_their_first_lifecycle()
    {
        const PROGRESS_STEPS: usize = 4_000;
        let (mut sim, client) = cluster(0x4649_5253);
        let namespace = IggyNamespace::new(0, 0, 0);
        sim.register_client_with_primary(&client);
        complete_metadata_op(&mut sim, &client, client.create_stream("late"));
        complete_metadata_op(&mut sim, &client, client.create_topic("late", "empty", 1));
        complete_metadata_op(&mut sim, &client, client.create_stream("replay-fence"));
        for replica in &sim.replicas {
            assert!(replica.partition_superblocks.borrow().is_empty());
            assert!(
                !replica
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .contains(&namespace)
            );
        }
        for replica in 0..sim.replica_count {
            sim.replica_crash(replica);
        }
        for replica in 0..sim.replica_count {
            sim.replica_restart(replica);
            let metadata = sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata();
            let epoch = metadata
                .mux_stm
                .streams()
                .created_revision_for_namespace(namespace)
                .expect("the committed create must survive metadata replay");
            assert!(epoch > 0 && epoch <= metadata.applied_frontier().recovered_revision());
        }
        sim.register_client_with_primary(&client);
        let primary = sim
            .metadata_primary_index()
            .expect("metadata has a live primary");
        let reply = submit_and_wait_for_reply(
            &mut sim,
            client.client_id(),
            primary,
            client.delete_topic("late", "empty"),
        );
        assert_eq!(
            metadata::stm::result::result_code(reply.body()),
            Some(IggyError::TransientNotCommitted.as_code())
        );
        let intent_op = reply.header().op;
        assert!(
            (0..PROGRESS_STEPS).any(|_| {
                sim.step();
                sim.replicas.iter().all(|replica| {
                    !replica.shards[0]
                        .plane
                        .metadata()
                        .mux_stm
                        .streams()
                        .lifecycle_pending(intent_op)
                })
            }),
            "a never-materialized committed group must initialize and finish its delete after restart"
        );
    }

    #[test]
    fn given_initialized_groups_when_retained_history_is_erased_should_remain_nonvoting() {
        const PROGRESS_STEPS: usize = 2_000;
        let (mut sim, client) = cluster(0x4552_4153);
        let namespace = IggyNamespace::new(0, 0, 0);
        sim.register_client_with_primary(&client);
        complete_metadata_op(&mut sim, &client, client.create_stream("initialized"));
        complete_metadata_op(
            &mut sim,
            &client,
            client.create_topic("initialized", "history", 1),
        );
        sim.init_partition(namespace);
        sim.discover_partition_contexts(&client);
        commit_sends_everywhere(&mut sim, &client, namespace, 1);
        complete_metadata_op(&mut sim, &client, client.create_stream("replay-fence"));
        for replica in 0..sim.replica_count {
            sim.replica_crash(replica);
            let replica = &sim.replicas[usize::from(replica)];
            assert!(
                replica
                    .partition_superblocks
                    .borrow()
                    .contains_key(&namespace)
            );
            replica
                .partition_shard(namespace)
                .plane
                .partitions()
                .remove(&namespace);
            replica.partition_logs.borrow_mut().remove(&namespace);
        }
        for replica in 0..sim.replica_count {
            sim.replica_restart(replica);
        }
        for _ in 0..PROGRESS_STEPS {
            sim.step();
        }
        for replica in &sim.replicas {
            let partitions = replica.partition_shard(namespace).plane.partitions();
            let partition = partitions
                .get_by_ns(&namespace)
                .expect("an initialized group's surviving store is reopened");
            assert!(
                !partition.consensus().recovery_election_allowed(),
                "missing retained history must not count as first initialization"
            );
            assert_eq!(partition.consensus().status(), Status::Recovering);
            assert_eq!(partition.consensus().commit_min(), 0);
        }
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
        // together remap every stream; and partition ops drawing from the one shared
        // request counter instead of a separate sequence based at `1<<63`, which
        // renumbers every partition request id and so every reply header in the trace.
        // NoAck explicit offsets use the primary-local path. Ordinary Replicated
        // writes remain admitted, so their policy-scoped replies contribute to
        // the deterministic trace. Ownership fencing now refuses group offset
        // writes without an installed owner, changing the partition commit trace.
        // External groups expand the consumer-kind draw from two choices to three.
        // Rebalance probes consume network randomness and change the scheduled trace.
        // So do the eager `Commit`s a primary sends after each metadata commit, and
        // a backup re-acking a retransmit of an op it already applied: on this seed one
        // `PrepareOk` for the committed `Register`, which the primary ignores. Sending
        // the eager `Commit` to a backup only once it acked the op moves those sends.
        // So does re-probing each peer once partition admission opens at boot.
        assert_eq!(
            h1, 0xECC6_9F76_2D47_8F12,
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
        let mut invariants = crate::workload::invariants::Invariants::new();
        let replies = workload::run(
            &mut sim,
            &mut wl,
            &clients,
            2_000,
            u64::MAX,
            &mut invariants,
        );
        assert!(replies > 0, "workload produced no replies");

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 5_000, &mut invariants),
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
        let mut invariants = crate::workload::invariants::Invariants::new();
        let replies = workload::run(
            &mut sim,
            &mut wl,
            &clients,
            3_000,
            u64::MAX,
            &mut invariants,
        );
        assert!(replies > 0, "workload produced no replies");
        assert!(
            !sim.crashed.is_empty(),
            "expected at least one follower crash"
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 5_000, &mut invariants),
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
        let mut invariants = crate::workload::invariants::Invariants::new();
        let replies = workload::run(
            &mut sim,
            &mut wl,
            &clients,
            3_000,
            u64::MAX,
            &mut invariants,
        );
        assert!(replies > 0, "lossy workload produced no replies");
        assert!(
            wl.resends() > 0,
            "no request timed out at 5% packet loss, so the resend path never ran; \
             raise the loss rate or lower request_timeout_ticks"
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 20_000, &mut invariants),
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
    /// and only an explicit `init_partition` mirrors its materialisation outcome and
    /// drains the matching park entry. Non-zero `park_overflow` therefore means
    /// frames shed for a namespace the driver never materialised, which is the fault
    /// class this catches rather than the back-pressure it would be in production.
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
                     simulator wires no reconciler, so only explicit materialisation can \
                     deliver or answer them"
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
        let produced = submit_and_wait_for_reply(&mut sim, client_id, 0, produce);
        assert_eq!(
            produced.header().status,
            0,
            "the produced message must commit"
        );

        // Poll through the dispatch shell (`on_client_request`, drain,
        // `handle_poll_messages`, `partition_read`, the owning shard), running as a
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
        assert_eq!(
            poll_reply.header().status,
            0,
            "poll denied: {:?}",
            poll_reply.header()
        );
        (poll_reply.as_slice().to_vec(), sim.schedule_hash())
    }

    /// A `SimClient` poll returns the produced messages through the real dispatch
    /// read path (`on_client_request`, `handle_poll_messages`, `partition_read`,
    /// the owning shard), running as a task the executor interleaves with the pump,
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

    #[test]
    fn given_aged_out_request_when_replayed_should_report_unknown_outcome() {
        for initially_committed in [false, true] {
            let (mut sim, client) = cluster(packet::PacketSimulatorOptions::default().seed);
            let namespace = IggyNamespace::new(1, 1, 0);
            sim.init_partition(namespace);
            sim.register_client_with_primary(&client);

            let delayed = client.send_messages(namespace, &[Bytes::from_static(b"aged-out")]);
            let destination = u8::from(!initially_committed);
            let first = submit_and_wait_for_reply(
                &mut sim,
                client.client_id(),
                destination,
                delayed.deep_copy(),
            );
            assert_eq!(
                first.header().status,
                if initially_committed {
                    0
                } else {
                    IggyError::NotPrimary.as_code()
                },
                "the original attempt must establish the intended admission outcome",
            );
            for _ in 0..COMMITTED_WINDOW_BITS {
                let later =
                    client.send_messages(namespace, &[Bytes::from_static(b"committed-later")]);
                let committed = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, later);
                assert_eq!(committed.header().status, 0);
            }

            let expected = sim
                .offsets(0, namespace)
                .expect("committed partition offsets");
            let replay = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, delayed);
            assert_eq!(
                replay.header().status,
                IggyError::RequestTooOld.as_code(),
                "aging out cannot prove whether request 1 committed",
            );
            assert_eq!(
                sim.offsets(0, namespace),
                Some(expected),
                "rejecting an aged-out request must not append another payload",
            );
        }
    }

    #[test]
    fn given_reordered_requests_within_dedup_window_when_retried_should_commit_once() {
        let (mut sim, client) = cluster(packet::PacketSimulatorOptions::default().seed);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        let delayed = client.send_messages(namespace, &[Bytes::from_static(b"delayed")]);
        let later = client.send_messages(namespace, &[Bytes::from_static(b"later")]);
        let later_receipt =
            submit_and_wait_for_reply(&mut sim, client.client_id(), 0, later.deep_copy());
        assert_eq!(later_receipt.header().status, 0);
        let committed =
            submit_and_wait_for_reply(&mut sim, client.client_id(), 0, delayed.deep_copy());
        assert_eq!(committed.header().status, 0);
        let expected = sim.offsets(0, namespace).unwrap();
        assert_eq!(
            expected.commit_offset, 1,
            "both reordered requests must append"
        );
        for (replay, expected_status) in [(delayed, IggyError::RequestTooOld.as_code()), (later, 0)]
        {
            let duplicate = submit_and_wait_for_reply(&mut sim, client.client_id(), 0, replay);
            assert_eq!(duplicate.header().status, expected_status);
            if expected_status == 0 {
                assert_eq!(
                    duplicate.as_slice(),
                    later_receipt.as_slice(),
                    "the lower request must preserve the higher request's original receipt"
                );
            }
            assert_eq!(
                sim.offsets(0, namespace),
                Some(expected),
                "a retained duplicate must not append again"
            );
        }
    }

    #[test]
    fn given_different_dedup_limits_when_primary_fails_should_not_append_a_replay() {
        const REPLICA_COUNT: u8 = 3;
        const CLIENTS: [u128; 2] = [1, 2];
        const DEDUP_CAPACITIES: [usize; REPLICA_COUNT as usize] = [2, 1, 1];
        const PROGRESS_STEPS: usize = 800;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::new(
            usize::from(REPLICA_COUNT),
            CLIENTS.iter().copied(),
            packet::PacketSimulatorOptions {
                node_count: REPLICA_COUNT,
                client_count: u8::try_from(CLIENTS.len()).expect("test clients fit u8"),
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        for (replica, capacity) in sim.replicas.iter().zip(DEDUP_CAPACITIES) {
            replica
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_mut_by_ns(&namespace)
                .expect("materialized partition")
                .set_dedup_clients_max(capacity);
        }
        let clients = CLIENTS.map(SimClient::new);
        for client in &clients {
            sim.register_client_with_primary(client);
        }
        let original = clients[0].send_messages(namespace, &[Bytes::from_static(b"first-client")]);
        let replay = original.deep_copy();
        let first = submit_and_wait_for_reply(&mut sim, CLIENTS[0], 0, original);
        assert_eq!(first.header().status, 0);
        let original_result = first.body().to_vec();
        let second = clients[1].send_messages(namespace, &[Bytes::from_static(b"second-client")]);
        let second = submit_and_wait_for_reply(&mut sim, CLIENTS[1], 0, second);
        assert_eq!(second.header().status, 0);
        let expected = sim.offsets(0, namespace).expect("primary offsets");
        assert!((0..PROGRESS_STEPS).any(|_| {
            sim.step();
            (0..DEDUP_CAPACITIES.len())
                .all(|replica| sim.offsets(replica, namespace) == Some(expected))
        }));

        sim.replica_crash(0);
        let primary = (0..PROGRESS_STEPS)
            .find_map(|_| {
                sim.step();
                (1..DEDUP_CAPACITIES.len()).find_map(|replica| {
                    sim.partition_consensus_state(replica, namespace)
                        .is_some_and(|state| state.status == Status::Normal && state.is_primary)
                        .then(|| u8::try_from(replica).expect("replica ID fits u8"))
                })
            })
            .expect("surviving replicas must elect a primary");
        let response = submit_and_wait_for_reply(&mut sim, CLIENTS[0], primary, replay);
        assert_eq!(response.header().status, 0);
        assert_eq!(
            sim.offsets(usize::from(primary), namespace),
            Some(expected),
            "a smaller backup dedup limit must not turn an acknowledged send into a new append"
        );
        assert_eq!(
            response.body(),
            original_result.as_slice(),
            "promotion must preserve the original append receipt"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_unpersisted_creation_view_when_replicas_restart_should_preserve_acknowledged_writes() {
        const PROGRESS_STEPS: usize = 2_000;
        const PAYLOAD: &[u8] = b"acknowledged-before-late-materialization";
        for created_view in [1, 2] {
            let (mut sim, client) = cluster(packet::PacketSimulatorOptions::default().seed);
            sim.register_client_with_primary(&client);
            let namespace = IggyNamespace::new(1, 1, 0);
            sim.partition_created_views.insert(namespace, created_view);
            sim.seed_stream_topic_partition(namespace);
            for replica in &sim.replicas[..2] {
                materialise_partition(replica, namespace, sim.consumer_offsets_max);
                assert!(
                    replica.partition_superblocks.borrow()[&namespace]
                        .1
                        .read_latest_sync()
                        .is_none(),
                    "the crash must precede the first durable view write"
                );
            }
            for replica in 0..2 {
                sim.replica_crash(replica);
            }
            for replica in 0..2 {
                sim.replica_restart(replica);
            }
            let primary = (0..PROGRESS_STEPS)
                .find_map(|_| {
                    sim.step();
                    (0..2).find(|replica| {
                        sim.partition_consensus_state(usize::from(*replica), namespace)
                            .is_some_and(|state| state.status == Status::Normal && state.is_primary)
                    })
                })
                .expect("the restarted quorum must elect a primary");
            // Discovery names the most advanced metadata, and the restarted
            // primary refuses a send until it applied that metadata too.
            let applied =
                |replica: &SimReplica| replica.shards[0].plane.metadata().applied_frontier().get();
            assert!(
                (0..PROGRESS_STEPS).any(|_| {
                    sim.step();
                    let primary_applied = applied(&sim.replicas[usize::from(primary)]);
                    sim.replicas
                        .iter()
                        .all(|replica| applied(replica) <= primary_applied)
                }),
                "the restarted primary must apply the committed metadata"
            );
            sim.discover_partition_contexts(&client);
            let request = client.send_messages(namespace, &[Bytes::from_static(PAYLOAD)]);
            let committed =
                submit_and_wait_for_reply(&mut sim, client.client_id(), primary, request);
            assert_eq!(committed.header().status, 0);
            let committed_op = committed.header().commit;
            let expected = sim
                .offsets(usize::from(primary), namespace)
                .expect("acknowledged partition offsets");

            materialise_partition(&sim.replicas[2], namespace, sim.consumer_offsets_max);
            assert!(
                (0..PROGRESS_STEPS).any(|_| {
                    sim.step();
                    (0..3).all(|replica| {
                        sim.partition_consensus_state(replica, namespace)
                            .is_some_and(|state| {
                                state.status == Status::Normal && state.commit_min >= committed_op
                            })
                            && sim.offsets(replica, namespace) == Some(expected)
                    })
                }),
                "late materialization must preserve the acknowledged prefix: states={:?}, offsets={:?}",
                (0..3)
                    .map(|replica| sim.partition_consensus_state(replica, namespace))
                    .collect::<Vec<_>>(),
                (0..3)
                    .map(|replica| sim.offsets(replica, namespace))
                    .collect::<Vec<_>>()
            );
            for replica in 0..3 {
                let prepare = retained_prepare(&sim, replica, namespace, committed_op);
                assert!(
                    prepare
                        .body()
                        .windows(PAYLOAD.len())
                        .any(|bytes| bytes == PAYLOAD),
                    "replica {replica} lost the acknowledged payload at op {committed_op}"
                );
            }
        }
    }

    fn successful_send_reply_count(replies: &[Message<ReplyHeader>]) -> usize {
        replies
            .iter()
            .filter(|reply| {
                reply.header().operation == iggy_binary_protocol::Operation::SendMessages
                    && reply.header().status == 0
            })
            .count()
    }

    pub fn retained_prepare(
        sim: &Simulator,
        replica: usize,
        namespace: IggyNamespace,
        op: u64,
    ) -> Message<iggy_binary_protocol::PrepareHeader> {
        let partitions = sim.replicas[replica]
            .partition_shard(namespace)
            .plane
            .partitions();
        let partition = partitions.get_by_ns(&namespace).expect("partition");
        let stored = partition
            .log
            .journal()
            .inner
            .repair_entry(op)
            .unwrap_or_else(|| panic!("replica {replica} retains prepare op {op} for repair"));
        Message::<iggy_binary_protocol::PrepareHeader>::try_from(server_common::iobuf::Owned::<
            { server_common::MESSAGE_ALIGN },
        >::copy_from_slice(
            stored.as_slice()
        ))
        .expect("journaled prepare remains a valid frame")
    }

    /// Leave one backup without the partition while the primary commits two
    /// sends through the other backup. The lagging replica parks both replicated
    /// prepares. A third prepare is withheld from it, then placed on its inbox
    /// after simulator materialisation stages the parked prefix. Running the pump
    /// without advancing its tick forces the inbox arm to drain the prefix first.
    #[allow(clippy::too_many_lines)]
    fn parked_prepare_redispatch_trace(seed: u64) -> (usize, Vec<PartitionOffsets>, u64) {
        const CLIENT_ID: u128 = 1;

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
        let mut sim = Simulator::with_shards_shell(3, 1, std::iter::once(CLIENT_ID), network_opts);
        let namespace = IggyNamespace::new(0, 0, 0);
        sim.seed_stream_topic_partition(namespace);
        // Replica 0 is the view-0 primary. Replica 2 supplies quorum while
        // replica 1 has committed metadata but no local partition yet.
        materialise_partition(&sim.replicas[0], namespace, sim.consumer_offsets_max);
        materialise_partition(&sim.replicas[2], namespace, sim.consumer_offsets_max);

        let client = SimClient::new(CLIENT_ID);
        sim.shell_login(&client);
        for payload in [
            Bytes::from_static(b"parked-redispatch-0"),
            Bytes::from_static(b"parked-redispatch-1"),
        ] {
            let request = client.send_messages(namespace, std::slice::from_ref(&payload));
            let reply = submit_and_wait_for_reply(&mut sim, CLIENT_ID, 0, request);
            assert_eq!(reply.header().status, 0);
        }

        let lagging_shard = Rc::clone(&sim.replicas[1].shards[0]);
        let mut successful_replies = 2usize;
        let mut parked = 0usize;
        for _ in 0..500 {
            sim.step();
            parked = lagging_shard.parked_frame_count(namespace);
            if parked >= 2 {
                break;
            }
        }
        assert!(parked >= 2, "lagging backup never parked both prepares");

        // Keep the third prepare off replica 1's network path. Replica 0 can
        // still commit it through replica 2, after which its journal supplies
        // the exact wire frame to put on the lagging backup's inbox below.
        sim.network.process_disable(ProcessId::Replica(1));
        let third = client.send_messages(namespace, &[Bytes::from_static(b"parked-redispatch-2")]);
        sim.submit_request(CLIENT_ID, 0, third.into_generic());
        for _ in 0..500 {
            successful_replies += successful_send_reply_count(&sim.step());
            if successful_replies >= 3 {
                break;
            }
        }
        assert!(
            successful_replies >= 3,
            "primary never committed the later send"
        );
        let later_prepare = retained_prepare(&sim, 0, namespace, 3);
        sim.network.process_enable(ProcessId::Replica(1));

        materialise_partition(&sim.replicas[1], namespace, sim.consumer_offsets_max);
        assert_eq!(lagging_shard.parked_frame_count(namespace), 0);
        assert_eq!(
            lagging_shard.redispatched_frame_count(),
            parked,
            "materialisation must move every parked prepare to the pump queue"
        );

        // No virtual-time advance here. The materialisation marker and inbox
        // wake poll the pump, whose ranked redispatch arm must put ops 1 and 2
        // ahead of this op 3 frame.
        lagging_shard.dispatch(later_prepare.into_generic());
        sim.run_pumps();
        let lagging_holds_later = sim.replicas[1]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .is_some_and(|partition| partition.log.journal().inner.header_by_op(3).is_some());
        assert!(
            lagging_holds_later,
            "inbox op 3 reached the gap check before the redispatched prefix"
        );

        let expected = sim
            .offsets(0, namespace)
            .expect("primary partition offsets");
        let mut converged = false;
        for _ in 0..500 {
            sim.step();
            converged = (0..3).all(|replica| sim.offsets(replica, namespace) == Some(expected));
            if converged {
                break;
            }
        }
        assert!(
            converged,
            "redispatched prepares did not converge the backup"
        );
        assert_eq!(lagging_shard.redispatched_frame_count(), 0);
        assert_no_frame_drops(&sim);

        let offsets = (0..3)
            .map(|replica| sim.offsets(replica, namespace).expect("partition offsets"))
            .collect();
        (parked, offsets, sim.schedule_hash())
    }

    #[test]
    fn parked_prepare_redispatch_is_seed_replayable() {
        let first = parked_prepare_redispatch_trace(0x5CED_4005);
        let second = parked_prepare_redispatch_trace(0x5CED_4005);
        assert_eq!(first, second, "same seed diverged on parked redispatch");
    }

    #[test]
    fn redispatched_solo_request_commits_without_another_inbox_frame() {
        const CLIENT_ID: u128 = 1;

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let network_opts = packet::PacketSimulatorOptions {
            node_count: 1,
            client_count: 1,
            seed: 0x5CED_4006,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::with_shards_shell(1, 1, std::iter::once(CLIENT_ID), network_opts);
        let namespace = IggyNamespace::new(0, 0, 0);
        sim.seed_stream_topic_partition(namespace);

        let client = SimClient::new(CLIENT_ID);
        sim.shell_login(&client);
        let request = client.send_messages(namespace, &[Bytes::from_static(b"solo-redispatch")]);
        let shard = Rc::clone(&sim.replicas[0].shards[0]);
        // Skips dispatch, so it carries the watermark dispatch would stamp.
        let request = server_common::MessageBag::try_from(stamp_metadata_watermark(
            &shard,
            &request.into_generic(),
        ))
        .expect("valid send request");
        futures::executor::block_on(shard.on_message(request));
        assert_eq!(
            shard.parked_frame_count(namespace),
            1,
            "the request must park before materialisation"
        );

        sim.init_partition(namespace);
        assert_eq!(
            shard.redispatched_frame_count(),
            1,
            "materialisation must stage and wake the parked request"
        );

        // No network or virtual-time step follows materialisation. The ranked
        // redispatch arm must deliver this request and process its self-ack in
        // the same pump iteration.
        sim.run_pumps();
        let state = sim
            .partition_consensus_state(0, namespace)
            .expect("the materialised solo partition has consensus state");
        assert_eq!(state.commit_min, 1, "the self-ack must commit the request");
        assert_eq!(shard.redispatched_frame_count(), 0);
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
    /// `PollPlan` before disk reads run off the borrow. Completion returns to the owner
    /// before consumer progress changes.
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
            let persistence =
                crate::partition_wal::reopen(&SimStorage::default(), ns_grow, 0, None);
            executor.spawn(async move {
                grow.init_partition(
                    ns_grow,
                    None,
                    None,
                    None,
                    persistence,
                    PartitionMaterialisation::new(0, 0, true),
                );
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
        let persistence = crate::partition_wal::reopen(&SimStorage::default(), ns_grow, 0, None);
        executor.spawn(async move {
            grow.init_partition(
                ns_grow,
                None,
                None,
                None,
                persistence,
                PartitionMaterialisation::new(0, 0, true),
            );
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
        let (mut sim, client, root) = checkpointing_sim(replicas, seed);
        commit_streams(&mut sim, &client, prefix, streams);
        (sim, client.client_id(), root)
    }

    /// A checkpointing cluster with one registered client and nothing else committed.
    fn checkpointing_sim(replicas: u8, seed: u64) -> (Simulator, SimClient, tempfile::TempDir) {
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
        // Small enough that a few dozen ops cross the coordinator's margin.
        sim.set_metadata_journal_slots(80);

        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);
        (sim, client, root)
    }

    /// Commit `streams` streams named `wl-{prefix}-N` through replica 0.
    fn commit_streams(sim: &mut Simulator, client: &SimClient, prefix: &str, streams: u32) {
        for sequence in 0..streams {
            let msg = client.create_stream(&format!("wl-{prefix}-{sequence}"));
            sim.submit_request(client.client_id(), 0, msg.into_generic());
            for _ in 0..40 {
                sim.step();
            }
        }
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

    /// Failover retry absorbed by the partition dedup slice: a `SendMessages`
    /// replay of an already-committed `(client, request)` on a NEW primary is
    /// answered without re-executing. The slice is folded in on every replica
    /// at commit, so the promoted primary knows the watermark its predecessor
    /// established -- that inheritance is what this test proves.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn failover_retry_absorbed_by_partition_dedup() {
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
        // Offset after exactly one committed batch: the duplicate must not
        // move it.
        let offset_after_original = sim.replicas[1].shards[0]
            .plane
            .partitions()
            .get_by_ns(&ns)
            .expect("partition must exist on a live replica")
            .stats
            .current_offset();
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

        // Replay the SAME request to the new primary: the dedup slice it
        // inherited at commit must absorb it.
        sim.submit_request(client_id, new_primary_idx, replay_req.into_generic());

        let mut retry_reply: Option<Message<ReplyHeader>> = None;
        for _ in 0..200 {
            let replies = sim.step();
            if !replies.is_empty() {
                retry_reply = Some(replies[0].deep_copy());
                break;
            }
        }
        let retry_reply = retry_reply
            .expect("reply must arrive after retry; the new primary absorbs it as a duplicate");

        assert_eq!(
            retry_reply.header().request,
            original_request_id,
            "retry's reply must correlate to the request id"
        );
        assert_eq!(
            retry_reply.header().client,
            client_id,
            "retry must echo original client_id"
        );
        assert_eq!(
            retry_reply.header().status,
            0,
            "an absorbed duplicate is a success, not an error"
        );
        // The absorbed answer is synthesized at admission, so it never earns a
        // new op. Re-execution would have committed past the original.
        assert!(
            retry_reply.header().op <= original_commit_op,
            "retry must NOT re-execute (original commit={original_commit_op}, reply op={})",
            retry_reply.header().op
        );
        // The payload committed exactly once.
        let committed = sim.replicas[usize::from(new_primary_idx)].shards[0]
            .plane
            .partitions()
            .get_by_ns(&ns)
            .expect("partition must exist on the new primary")
            .stats
            .current_offset();
        assert_eq!(
            committed, offset_after_original,
            "duplicate must not append a second copy"
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
        let mut invariants = crate::workload::invariants::Invariants::new();
        let replies = workload::run(
            &mut sim,
            &mut wl,
            &clients,
            3_000,
            u64::MAX,
            &mut invariants,
        );

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

    /// Liveness adds timed tasks even without client traffic, so each admission
    /// mode must reproduce its own schedule rather than the other mode's schedule.
    #[test]
    fn given_liveness_tasks_when_shell_replays_should_preserve_seeded_schedule() {
        const SEED: u64 = 0x5CED_0001;
        for shell in [false, true] {
            assert_eq!(
                consensus_schedule_hash(SEED, shell),
                consensus_schedule_hash(SEED, shell),
                "shell={shell}: schedule diverged at the same seed"
            );
            assert_ne!(
                consensus_schedule_hash(SEED, shell),
                consensus_schedule_hash(SEED + 1, shell),
                "shell={shell}: different seeds produced identical schedules"
            );
        }
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
            let mut invariants = crate::workload::invariants::Invariants::new();
            let replies = run_with_faults(
                &mut sim,
                &mut workload,
                &clients,
                3_000,
                u64::MAX,
                &mut injector,
                &mut invariants,
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

    #[test]
    fn given_checkpointing_cluster_when_replayed_should_preserve_the_schedule() {
        const SEED: u64 = 0xC4E0_0005;
        const REPLICAS: u8 = 3;
        const STREAMS: u32 = 40;

        let (first, first_client, _first_root) =
            checkpointing_cluster(REPLICAS, SEED, "replay", STREAMS);
        let (second, second_client, _second_root) =
            checkpointing_cluster(REPLICAS, SEED, "replay", STREAMS);

        assert_eq!(first.schedule_hash(), second.schedule_hash());
        for replica in 0..usize::from(REPLICAS) {
            assert!(snapshot_floor(&first, replica) > 0);
            assert_eq!(
                snapshot_floor(&first, replica),
                snapshot_floor(&second, replica)
            );
            assert_eq!(
                committed_stream_names(&first, replica),
                committed_stream_names(&second, replica)
            );
            assert_eq!(
                client_watermark(&first, replica, first_client),
                client_watermark(&second, replica, second_client)
            );
        }
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

    /// A checkpoint replaces the seeded baseline, so a restart from one must not
    /// seed a fixture topic again that a committed delete removed before it.
    #[test]
    fn given_a_fixture_deleted_before_a_checkpoint_when_the_replica_restarts_should_stay_deleted() {
        const STREAMS: u32 = 40;
        const STEPS_PER_STREAM: usize = 40;
        const CLIENT_ID: u128 = 1;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let root = tempfile::tempdir().expect("temp dir for the simulator's snapshots");
        let mut sim = Simulator::with_checkpoints(
            1,
            std::iter::once(CLIENT_ID),
            packet::PacketSimulatorOptions {
                node_count: 1,
                client_count: 1,
                seed: 0xC4E0_0006,
                ..packet::PacketSimulatorOptions::default()
            },
            false,
            root.path(),
        );
        sim.set_metadata_journal_slots(80);
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        let client = SimClient::new(CLIENT_ID);
        sim.register_client_with_primary(&client);
        let fixture_exists = |sim: &Simulator| {
            sim.replicas[0].shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .created_revision_for_namespace(fixture)
                .is_some()
        };

        complete_metadata_op(
            &mut sim,
            &client,
            client.delete_topic(
                &Streams::seeded_stream_name(fixture.stream_id()),
                &Streams::seeded_topic_name(fixture.stream_id(), fixture.topic_id()),
            ),
        );
        assert!(!fixture_exists(&sim), "the delete must remove the fixture");
        for sequence in 0..STREAMS {
            let msg = client.create_stream(&format!("wl-reseed-{sequence}"));
            sim.submit_request(CLIENT_ID, 0, msg.into_generic());
            for _ in 0..STEPS_PER_STREAM {
                sim.step();
            }
        }
        assert!(
            snapshot_floor(&sim, 0) > 0,
            "the replica never checkpointed, so its restart reads no snapshot"
        );

        sim.replica_crash(0);
        sim.replica_restart(0);

        assert!(
            !fixture_exists(&sim),
            "the restart seeded the fixture topic that its checkpoint holds deleted"
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

    /// A primary that recovered a WAL suffix still serves the state transfer its
    /// only live backup needs before it can ack that suffix.
    ///
    /// Replica 2 is down while the others commit past a checkpoint, so its gap sits
    /// below every peer's WAL floor. A full outage follows and replica 1 stays down.
    /// Replica 0 boots with its WAL head as a recovery barrier (each prepare carries
    /// the commit point of the one before it), wins a view, and needs replica 2's
    /// ack to re-commit that head. Replica 2 appends nothing until a transfer lifts
    /// it past the floor, so a primary that refuses transfers below its barrier
    /// wedges this two-replica quorum until replica 1 returns.
    #[test]
    fn given_a_backup_below_the_floor_when_the_primary_recovered_a_suffix_should_transfer_and_recommit()
     {
        const STREAMS: u32 = 40;
        const RECOVERY_TICKS: usize = 8_000;
        let (primary, absent, lagging) = (0u8, 1u8, 2u8);
        let (mut sim, client, _root) = checkpointing_sim(3, 0xC4E0_0005);

        sim.replica_crash(lagging);
        commit_streams(&mut sim, &client, "barrier", STREAMS);
        sim.replica_crash(absent);
        sim.replica_crash(primary);
        sim.replica_restart(primary);
        sim.replica_restart(lagging);

        let floor = snapshot_floor(&sim, usize::from(primary));
        let barrier = {
            let recovered = sim
                .metadata_consensus(usize::from(primary))
                .expect("shard 0 owns metadata consensus");
            assert!(
                recovered.recovery_barrier() > recovered.commit_max(),
                "replica {primary} recovered no suffix past its commit point, so \
                 nothing waits on the barrier"
            );
            recovered.recovery_barrier()
        };
        let lagging_commit = sim
            .metadata_consensus(usize::from(lagging))
            .expect("shard 0 owns metadata consensus")
            .commit_min();
        assert!(
            lagging_commit < floor,
            "replica {lagging} commits through {lagging_commit} and replica {primary} \
             retains ops above {floor}: journal repair alone can close this gap"
        );

        for _ in 0..RECOVERY_TICKS {
            sim.step();
        }

        for replica in [primary, lagging] {
            let consensus = sim
                .metadata_consensus(usize::from(replica))
                .expect("shard 0 owns metadata consensus");
            assert_eq!(
                consensus.status(),
                consensus::Status::Normal,
                "replica {replica} never settled into a view"
            );
            assert!(
                consensus.commit_min() >= barrier,
                "replica {replica} commits through {} but the recovered head {barrier} \
                 never re-committed: the primary withheld the transfer its only live \
                 backup needed to ack it",
                consensus.commit_min(),
            );
        }
        assert_eq!(
            committed_stream_names(&sim, usize::from(lagging)),
            committed_stream_names(&sim, usize::from(primary)),
            "the backup caught up to different committed metadata than the primary"
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
        // refuses a partition write for routing reasons (`NotPrimary`) that say
        // nothing about the session.
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
        shell_workload_with_ack_quorum_ratio(0.5);
    }

    #[test]
    fn given_serial_quorum_offsets_when_shell_workload_quiesces_should_compare_the_shadow() {
        shell_workload_with_ack_quorum_ratio(1.0);
    }

    fn shell_workload_with_ack_quorum_ratio(ack_quorum_ratio: f32) {
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
        options.ack_quorum_ratio = ack_quorum_ratio;
        let mut wl = Workload::new(options);

        let clients = [client];
        let mut invariants = crate::workload::invariants::Invariants::new();
        let replies = workload::run(
            &mut sim,
            &mut wl,
            &clients,
            4_000,
            u64::MAX,
            &mut invariants,
        );
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
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 20_000, &mut invariants),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
        let convergence = oracle::assert_converged(&sim, &mut wl);
        if ack_quorum_ratio >= 1.0 {
            assert!(
                wl.auditor
                    .stats()
                    .commits(crate::workload::actions::Action::StoreConsumerOffset)
                    > 0,
                "the strict CI cell must acknowledge an offset store"
            );
            assert!(
                convergence.offsets_compared_with_shadow > 0,
                "the strict CI cell must compare its acknowledged offset shadow"
            );
            println!(
                "strict offset proof: stores={}, offsets_compared_with_shadow={}",
                wl.auditor
                    .stats()
                    .commits(crate::workload::actions::Action::StoreConsumerOffset),
                convergence.offsets_compared_with_shadow,
            );
        }
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
    #[allow(clippy::too_many_lines)]
    fn shell_workload_survives_result_framed_transient_rejections() {
        use crate::workload::{
            FaultInjector, Workload,
            actions::Action,
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
        let seed = 2;
        {
            const SAMPLE_ATTEMPTS: usize = 16;
            let mut sim = Simulator::with_shards_shell(
                usize::from(replica_count),
                1,
                std::iter::once(client_id),
                packet::PacketSimulatorOptions {
                    node_count: replica_count,
                    client_count: 1,
                    seed,
                    ..packet::PacketSimulatorOptions::default()
                },
            );
            let client = SimClient::new(client_id);
            sim.shell_login(&client);
            let mut options = WorkloadOptions::new(seed, replica_count, vec![]);
            options.weights = ActionWeights::new(&[(Action::CreateStream, 100)]);
            let mut workload = Workload::new(options);
            let (target, request) = (0..SAMPLE_ATTEMPTS)
                .find_map(|_| workload.build_request(&client))
                .expect("a fresh stream can be created without existing metadata");
            let request_id = request.header().request;

            // Admission must refuse the request before the workload sees any reply.
            sim.metadata_consensus(usize::from(target))
                .expect("target hosts metadata")
                .begin_state_transfer_await();
            let reply = submit_and_wait_for_reply(&mut sim, client_id, target, request);
            assert_eq!(reply.header().request, request_id);
            assert_eq!(reply.header().status, 0);
            assert_eq!(
                iggy_binary_protocol::consensus::result_code(reply.body()),
                Some(iggy_common::IggyError::TransientNotAccepted.as_code()),
            );
            assert!(workload.on_reply(&reply).is_empty());
            assert_eq!(workload.auditor.stats().transient_rejections, 1);
            assert_eq!(workload.total_in_flight(), 1);

            sim.metadata_consensus(usize::from(target))
                .expect("target hosts metadata")
                .set_state_transfer_stage(consensus::StateTransferStage::Idle);
            let mut invariants = crate::workload::invariants::Invariants::new();
            assert!(
                oracle::drive_to_quiesce(
                    &mut sim,
                    &mut workload,
                    &[client],
                    1_000,
                    &mut invariants,
                ),
                "{}",
                oracle::quiesce_failure_report(&sim, &workload),
            );
            assert_eq!(workload.auditor.stats().commits(Action::CreateStream), 1);
            assert_eq!(workload.total_in_flight(), 0);
        }

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
        options.crash_per_tick_ratio = 0.05;
        options.restart_per_tick_ratio = 0.08;
        options.spare_primary = false;
        let mut wl = Workload::new(options);

        let clients = [client];
        let mut injector = FaultInjector::new(seed, replica_count);
        let mut invariants = crate::workload::invariants::Invariants::new();
        run_with_faults(
            &mut sim,
            &mut wl,
            &clients,
            1_500,
            u64::MAX,
            &mut injector,
            &mut invariants,
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 50_000, &mut invariants),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
    }

    /// A shell client that logged in to a backup follows the metadata leader.
    ///
    /// The session is bound to the replica the client logged in to, so pointing
    /// requests at the leader alone gets them evicted. Without a fresh login there
    /// every request lands on the backup and is refused with `NotPrimary`, which
    /// is how a crash and restart strands a client for the rest of a run.
    #[test]
    fn given_a_client_logged_in_to_a_backup_when_refused_should_follow_the_leader() {
        const RUN_TICKS: u64 = 1_000;
        let (mut sim, mut wl, clients) = client_logged_in_to_a_backup();
        let mut invariants = crate::workload::invariants::Invariants::new();
        crate::workload::run(
            &mut sim,
            &mut wl,
            &clients,
            RUN_TICKS,
            u64::MAX,
            &mut invariants,
        );

        assert!(
            wl.auditor.stats().commits_per_action.iter().sum::<u64>() > 0,
            "the client committed nothing, so it never reached the leader"
        );
        assert!(
            wl.redirects() > 0,
            "no request was redirected to the leader"
        );
        assert!(
            crate::workload::oracle::drive_to_quiesce(
                &mut sim,
                &mut wl,
                &clients,
                20_000,
                &mut invariants
            ),
            "{}",
            crate::workload::oracle::quiesce_failure_report(&sim, &wl),
        );
    }

    /// A lost login must be retried during the drain before its held request can go out.
    #[test]
    fn given_a_lost_leader_login_when_drained_should_retry_it_and_send_the_held_request() {
        const REDIRECT_TICKS: usize = 1_000;
        const ACTION: crate::workload::actions::Action =
            crate::workload::actions::Action::CreateStream;
        let (mut sim, mut wl, clients) = client_logged_in_to_a_backup();
        // Setup retries can leave replies that would satisfy the later handshake.
        assert!(
            (0..SETUP_TOTAL_STEPS).any(|_| {
                sim.step();
                sim.network.packets_in_flight() == 0
            }),
            "setup login traffic must settle before losing the redirect login"
        );
        wl.options.weights = crate::workload::options::ActionWeights::new(&[(ACTION, 100)]);
        let client_id = clients[0].client_id();
        let mut invariants = crate::workload::invariants::Invariants::new();
        assert!(
            (0..REDIRECT_TICKS).any(|_| {
                crate::workload::run(&mut sim, &mut wl, &clients, 1, u64::MAX, &mut invariants);
                wl.is_recovering(client_id) && wl.total_in_flight() == 1
            }),
            "no request waited for a leader login"
        );
        let leader = sim
            .metadata_leader()
            .expect("the cluster has a metadata leader");
        let before_loss = sim.network.packets_in_flight();
        sim.network
            .link_clear(ProcessId::Client(client_id), ProcessId::Replica(leader));
        assert_eq!(
            before_loss - sim.network.packets_in_flight(),
            1,
            "the pending leader login must be the lost packet"
        );
        for replica in 0..sim.replica_count {
            if replica != leader {
                sim.network.set_link_filter(
                    ProcessId::Client(client_id),
                    ProcessId::Replica(replica),
                    false,
                );
            }
        }
        let commits_before = wl.auditor.stats().commits(ACTION);

        assert!(
            crate::workload::oracle::drive_to_quiesce(
                &mut sim,
                &mut wl,
                &clients,
                20_000,
                &mut invariants
            ),
            "{}",
            crate::workload::oracle::quiesce_failure_report(&sim, &wl),
        );
        assert!(
            !wl.is_recovering(client_id),
            "the drain did not bind the leader login"
        );
        assert_eq!(
            wl.auditor.stats().commits(ACTION),
            commits_before + 1,
            "the held request must commit after the login retry"
        );
    }

    #[test]
    fn given_a_client_bound_only_to_a_backup_when_setting_up_a_fixture_group_should_bind_the_primary()
     {
        const PRIMARY: u8 = 0;
        let (mut sim, mut workload, clients) = client_logged_in_to_a_backup();
        workload.options.weights = crate::workload::options::ActionWeights::lifecycle();
        let client = &clients[0];
        let original_session = client.session_id();
        let (stream, topic) = workload.shadow.fixture_names(0);
        let request = client.create_consumer_group(
            &stream,
            &topic,
            crate::workload::shadow::FIXTURE_GROUP_NAME,
        );
        sim.submit_request(client.client_id(), PRIMARY, request.into_generic());
        let mut evicted = Vec::new();
        for _ in 0..SETUP_TOTAL_STEPS {
            sim.step();
            evicted.extend(sim.take_evictions());
            if !evicted.is_empty() {
                break;
            }
        }
        assert_eq!(
            evicted,
            vec![(client.client_id(), PRIMARY)],
            "the primary must start unbound"
        );

        crate::workload::create_fixture_groups(&mut sim, &mut workload, client);

        assert_eq!(client.session_id(), original_session);
        let metadata = sim.replicas[usize::from(PRIMARY)].shards[0]
            .plane
            .metadata();
        assert!(metadata.mux_stm.streams().read(|inner| {
            inner
                .items
                .iter()
                .flat_map(|(_, stream)| stream.topics.iter())
                .any(|(_, entry)| {
                    entry.name.as_ref() == topic
                        && entry.consumer_groups.values().any(|group| {
                            group.name.as_ref() == crate::workload::shadow::FIXTURE_GROUP_NAME
                        })
                })
        }));
        let journal = &sim.replicas[usize::from(PRIMARY)].metadata_journal;
        let creates = (1..=journal.last_op().unwrap())
            .filter_map(|op| journal.entry_sync(op))
            .filter(|entry| entry.header().operation == Operation::CreateConsumerGroup)
            .count();
        assert_eq!(creates, 1, "fixture setup must commit exactly one create");
    }

    #[test]
    fn given_a_previous_fixture_setup_reply_when_creating_another_group_should_wait_for_its_own_reply()
     {
        const PRIMARY: u8 = 0;
        let (mut sim, workload, clients) = client_logged_in_to_a_backup();
        let client = &clients[0];
        let (stream, topic) = workload.shadow.fixture_names(0);
        let request = client.create_consumer_group(
            &stream,
            &topic,
            crate::workload::shadow::FIXTURE_GROUP_NAME,
        );
        let first_reply = sim.await_setup_reply(
            client,
            PRIMARY,
            &request.into_generic(),
            Operation::CreateConsumerGroup,
            "first fixture group",
        );
        assert_eq!(
            iggy_binary_protocol::result_code(first_reply.body()),
            Some(0)
        );
        sim.deferred_client_replies.push((PRIMARY, first_reply));

        let second = IggyNamespace::new(1, 2, 0);
        sim.init_partition(second);
        sim.seed_stream_topic_partition(second);
        let mut options = crate::workload::options::WorkloadOptions::new(
            workload.options.seed,
            sim.replica_count,
            vec![second],
        );
        options.weights = crate::workload::options::ActionWeights::lifecycle();
        let mut second_workload = crate::workload::Workload::new(options);
        let (_, second_topic) = second_workload.shadow.fixture_names(0);

        crate::workload::create_fixture_groups(&mut sim, &mut second_workload, client);

        let metadata = sim.replicas[usize::from(PRIMARY)].shards[0]
            .plane
            .metadata();
        assert!(
            metadata.mux_stm.streams().read(|inner| {
                inner
                    .items
                    .iter()
                    .flat_map(|(_, stream)| stream.topics.iter())
                    .any(|(_, entry)| {
                        entry.name.as_ref() == second_topic
                            && entry.consumer_groups.values().any(|group| {
                                group.name.as_ref() == crate::workload::shadow::FIXTURE_GROUP_NAME
                            })
                    })
            }),
            "an earlier setup reply must not finish the next fixture before its group exists"
        );
    }

    /// A shell cluster whose only client logged in to a backup, and a uniform
    /// workload that sends there.
    fn client_logged_in_to_a_backup() -> (Simulator, crate::workload::Workload, [SimClient; 1]) {
        const BACKUP: u8 = 1;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let seed = 0x4C45_4144;
        let mut sim = Simulator::with_shards_shell(
            usize::from(replica_count),
            1,
            std::iter::once(client_id),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 1,
                seed,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.seed_stream_topic_partition(ns);
        let client = SimClient::new(client_id);
        sim.shell_login_via(&client, BACKUP);

        let mut options =
            crate::workload::options::WorkloadOptions::new(seed, replica_count, vec![ns]);
        options.weights = crate::workload::options::ActionWeights::uniform();
        let mut wl = crate::workload::Workload::new(options);
        assert!(
            wl.rehome(client_id, BACKUP).is_empty(),
            "nothing is outstanding before the run"
        );
        (sim, wl, [client])
    }

    /// The cross-replica equality check actually compares replicas against each
    /// other, and holds over a metadata workload with crashes and restarts.
    ///
    /// Non-vacuity is the point of the chain assertions. An equality oracle that never
    /// finds two replicas at the same op passes in silence, so `ops_compared` counts
    /// only ops witnessed on more than one replica, the subset that exercised the
    /// property.
    /// A replica prepares an op and stays down. One survivor holds its header
    /// without the body, the other never had it, and the merge must read that as
    /// proof both are outside the ack set. Otherwise it waits for the crashed
    /// replica: on this seed the cluster reached view 103 against `log_view` 3 with
    /// both survivors caught up and one request retried 266 times unanswered.
    ///
    /// Asserts the drain, so it fails as the fuzzer does with the outstanding-request
    /// report attached.
    #[test]
    fn view_change_completes_without_the_replica_that_prepared_the_head() {
        use crate::workload::{
            self, FaultInjector, Workload,
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
        // `workload-fuzz --seed 211 --replicas 3 --ticks 4000 --plane metadata
        // --journal-slots 80 --crash-prob 0.02 --restart-prob 0.08 --crash-primary`.
        let seed = 211u64;
        let root = tempfile::tempdir().expect("temp dir for the simulator's snapshots");
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            ..packet::PacketSimulatorOptions::default()
        };
        // A bounded journal is what drives the WAL drain behind the header-only sender.
        let mut sim = Simulator::with_checkpoints(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
            false,
            root.path(),
        );
        sim.set_metadata_journal_slots(80);

        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);

        let mut options = WorkloadOptions::new(seed, replica_count, vec![ns]);
        options.client_count = 1;
        options.crash_per_tick_ratio = 0.02;
        options.restart_per_tick_ratio = 0.08;
        options.spare_primary = false;
        options.weights = ActionWeights::metadata_only();
        let mut workload = Workload::new(options);

        let clients = [client];
        let mut injector = FaultInjector::new(seed, replica_count);
        let mut invariants = crate::workload::invariants::Invariants::new();
        let _ = workload::run_with_faults(
            &mut sim,
            &mut workload,
            &clients,
            4_000,
            u64::MAX,
            &mut injector,
            &mut invariants,
        );

        assert!(
            injector.crashes() > 0,
            "no replica crashed, so no view change ran and this proves nothing"
        );
        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut workload, &clients, 50_000, &mut invariants),
            "{}",
            oracle::quiesce_failure_report(&sim, &workload),
        );
    }

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
        // The checker is read afterwards, so it is declared here rather than left to
        // the driver: `chain` below is the accumulated canonical commit chain.
        let mut invariants = Invariants::new();
        workload::run_with_faults(
            &mut sim,
            &mut wl,
            &clients,
            4_000,
            u64::MAX,
            &mut injector,
            &mut invariants,
        );

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
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 50_000, &mut invariants),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
        assert!(
            oracle::settle_to_stable_view(&mut sim, &mut wl, 50_000, &mut invariants),
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
    /// recovers every `ns_a` operation, replaying its identity if leadership moves.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn per_partition_consensus_independence() {
        use consensus::PIPELINE_PREPARE_QUEUE_MAX;
        use iggy_binary_protocol::{Command, PrepareOkHeader};
        use packet::Packet;
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicU64, Ordering};

        const DRAIN_STEPS: usize = 800;
        const RESOLVE_STEPS: usize = 800;
        const RETRY_INTERVAL: usize = 100;

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
            client_count: u8::try_from(PIPELINE_PREPARE_QUEUE_MAX).unwrap(),
            seed: 0x5EED_0066,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            replica_count as usize,
            1..=PIPELINE_PREPARE_QUEUE_MAX as u128,
            network_opts,
        );
        let clients = (1..=PIPELINE_PREPARE_QUEUE_MAX as u128)
            .map(SimClient::new)
            .collect::<Vec<_>>();
        let client = &clients[0];
        let ns_a = IggyNamespace::new(1, 1, 0);
        let ns_b = IggyNamespace::new(1, 1, 1);
        sim.init_partition(ns_a);
        sim.init_partition(ns_b);
        for client in &clients {
            sim.register_client_with_primary(client);
        }
        BLOCKED_NS.store(ns_a.inner(), Ordering::Relaxed);

        // Block every backup's PrepareOk for ns_a toward the primary: the
        // primary's self-ack alone is 1 < quorum 2, so ns_a can prepare
        // and replicate but never commit. Every link, because the primary
        // rebalance may move ns_a's primary off replica 0 during the blockade.
        let replica_links = || {
            (0..replica_count).flat_map(move |from| {
                (0..replica_count)
                    .filter(move |to| *to != from)
                    .map(move |to| (ProcessId::Replica(from), ProcessId::Replica(to)))
            })
        };
        for (from, to) in replica_links() {
            *sim.network.link_drop_packet_fn(from, to) = Some(drop_blocked_prepare_ok);
        }

        // Fill ns_a's pipeline exactly to the cap (one more would be
        // rejected at preflight and generate a reply, muddying the
        // no-replies assertion below).
        let mut retained = Vec::with_capacity(clients.len());
        for (sequence, client) in clients.iter().enumerate() {
            let msg = client.send_messages(ns_a, &[Bytes::from(format!("wedged-{sequence}"))]);
            retained.push(msg.deep_copy());
            sim.submit_request(client.client_id(), 0, msg.into_generic());
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
        for (from, to) in replica_links() {
            *sim.network.link_drop_packet_fn(from, to) = None;
        }
        let mut committed = HashSet::new();
        // Rebalancing can cancel original waiters and discard uncommitted work.
        // Resolve the remaining identities at the current primary without
        // minting new requests or counting duplicate replies as progress.
        for step in 0..DRAIN_STEPS + RESOLVE_STEPS {
            if committed.len() == PIPELINE_PREPARE_QUEUE_MAX {
                break;
            }
            if step >= DRAIN_STEPS
                && step % RETRY_INTERVAL == 0
                && let Some(primary) = (0..replica_count).find(|replica| {
                    sim.partition_consensus_state(usize::from(*replica), ns_a)
                        .is_some_and(|state| state.is_primary && state.status == Status::Normal)
                })
            {
                for request in &retained {
                    let identity = (request.header().client, request.header().request);
                    if !committed.contains(&identity) {
                        sim.submit_request(identity.0, primary, request.deep_copy().into_generic());
                    }
                }
            }
            for reply in sim.step() {
                let identity = (reply.header().client, reply.header().request);
                assert!(retained.iter().any(|request| {
                    (request.header().client, request.header().request) == identity
                }));
                if reply.header().status == 0 {
                    committed.insert(identity);
                } else {
                    assert!(
                        workload::TransientRejection::from_code(reply.header().status).is_some(),
                        "unexpected recovery reply: {:?}",
                        reply.header()
                    );
                }
            }
        }
        assert_eq!(
            committed.len(),
            PIPELINE_PREPARE_QUEUE_MAX,
            "every wedged ns_a send must commit once acks flow again"
        );
        for _ in 0..100 {
            sim.step();
        }
        for replica in 0..usize::from(replica_count) {
            let offsets = sim.offsets(replica, ns_a).unwrap();
            assert_eq!(offsets.commit_offset, PIPELINE_PREPARE_QUEUE_MAX as u64 - 1);
            assert_eq!(offsets.write_offset, offsets.commit_offset);
        }
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
        let mut invariants = crate::workload::invariants::Invariants::new();
        let replies = workload::run(
            &mut sim,
            &mut wl,
            &clients,
            2_000,
            u64::MAX,
            &mut invariants,
        );
        assert!(replies > 0, "workload produced no replies");

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 5_000, &mut invariants),
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
    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest;
    use iggy_binary_protocol::requests::users::login_register::RegisterRequest;
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
        let registration = RegisterRequest::decode_from(straggler.register().body())
            .expect("registration carries a verified account and verifier");
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
                        .submit_register_in_process(straggler_client, 0, registration)
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

    thread_local! {
        /// The primary's first `StartView` to the prober. A link hook is a bare `fn`
        /// and cannot capture.
        static PROBE_ANSWER: RefCell<Option<Message<GenericHeader>>> = const { RefCell::new(None) };
    }

    /// Keep a copy of the probe answer and let the original through. No partition
    /// group exists in the test that installs it, so every `StartView` is metadata.
    fn copy_probe_answer(packet: &packet::Packet) -> bool {
        if packet.message.header().command == Command::StartView {
            PROBE_ANSWER.with_borrow_mut(|answer| {
                answer.get_or_insert_with(|| packet.message.deep_copy());
            });
        }
        false
    }

    /// A probe answer delivered twice must not take back what the prober acked in
    /// between: re-adopting the copy drops its head, and the plane truncates above
    /// it. With the other backup down, the prober holds the only copy of the op
    /// besides the primary, so the next view would lose an op the client saw commit.
    #[test]
    fn given_a_prober_that_acked_past_its_probe_answer_when_a_copy_arrives_late_should_keep_the_op()
    {
        const PRIMARY: u8 = 0;
        const OTHER_BACKUP: u8 = 1;
        const PROBER: u8 = 2;
        const STEPS_MAX: usize = 5_000;
        const LATE_DELIVERY_STEPS: usize = 50;
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
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);

        // The prober restarts and probes, and a copy of the primary's answer is kept
        // for a second, late delivery.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(PRIMARY), ProcessId::Replica(PROBER)) =
            Some(copy_probe_answer);
        sim.replica_crash(PROBER);
        sim.replica_restart(PROBER);
        for _ in 0..STEPS_MAX {
            sim.step();
            if metadata_status(&sim, PROBER) == Status::Normal {
                break;
            }
        }
        assert_eq!(
            metadata_status(&sim, PROBER),
            Status::Normal,
            "the prober must adopt the primary's answer"
        );
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(PRIMARY), ProcessId::Replica(PROBER)) = None;
        let answer = PROBE_ANSWER
            .take()
            .expect("the primary must have answered the probe");
        let (answered_head, answered_commit) = metadata_progress(&sim, PROBER);
        assert_eq!(
            answered_head, answered_commit,
            "the answer must find the cluster idle, so its head is the prober's commit point"
        );

        // The next op commits on the prober's ack alone, and with commit heartbeats
        // withheld the prober still takes the answer's head as the commit point.
        sim.replica_crash(OTHER_BACKUP);
        sim.network
            .link_filter_mut(ProcessId::Replica(PRIMARY), ProcessId::Replica(PROBER))
            .remove(Command::Commit);
        let reply = tests::submit_and_wait_for_reply(
            &mut sim,
            client_id,
            PRIMARY,
            client.create_stream("acked-by-the-prober"),
        );
        assert_eq!(reply.header().status, 0);
        let acked_op = answered_head + 1;
        assert_eq!(metadata_progress(&sim, PRIMARY).1, acked_op);
        assert_eq!(metadata_progress(&sim, PROBER), (acked_op, answered_head));

        // The kept copy, delayed in the network until now.
        sim.network.submit(
            ProcessId::Replica(PRIMARY),
            ProcessId::Replica(PROBER),
            answer,
        );
        for _ in 0..LATE_DELIVERY_STEPS {
            sim.step();
        }
        assert_eq!(
            metadata_progress(&sim, PROBER).0,
            acked_op,
            "the late answer moved the prober's head back below an op it acked"
        );
        assert!(
            metadata_holds(&sim, PROBER, acked_op),
            "the late answer truncated op {acked_op} from the prober's journal"
        );

        // The primary dies before the other backup ever saw the op.
        sim.replica_crash(PRIMARY);
        sim.replica_restart(OTHER_BACKUP);
        let mut primary = None;
        for _ in 0..STEPS_MAX {
            sim.step();
            primary = (1..replica_count).find(|&replica| is_new_metadata_primary(&sim, replica));
            if primary.is_some_and(|primary| metadata_progress(&sim, primary).1 >= acked_op) {
                break;
            }
        }
        let primary =
            primary.expect("a metadata primary must be elected after the old one crashes");
        assert!(
            metadata_progress(&sim, primary).1 >= acked_op,
            "the next view lost op {acked_op}, which the client saw commit"
        );
    }

    fn metadata_status(sim: &Simulator, replica: u8) -> Status {
        sim.replicas[usize::from(replica)].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 owns metadata consensus")
            .status()
    }

    /// The metadata primary admits a create that both backups journal, then
    /// crashes before any ack reaches it, so its client has no reply. The
    /// survivors elect a primary that inherits the create uncommitted. A create
    /// that primary admits meanwhile must count the inherited partitions once,
    /// and the inherited create must commit once, with the assignments the
    /// crashed primary persisted.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_an_inherited_create_when_the_new_primary_admits_another_should_count_and_commit_it_once()
     {
        const STREAM: &str = "inherited-placement";
        const INHERITED_TOPIC: &str = "inherited";
        const ADMITTED_TOPIC: &str = "admitted";
        const INHERITOR: u128 = 1;
        const ADMITTER: u128 = 2;
        const OLD_PRIMARY: u8 = 0;
        const INHERITED_PARTITIONS: u32 = 1;
        const ADMITTED_PARTITIONS: u32 = 3;
        // Nothing is placed before the inherited create, so it takes replica 0.
        const INHERITED_VIEWS: [u32; 1] = [0];
        // Counting the inherited partition once fills replicas 1 and 2 first.
        // Dropping it starts again at replica 0 ([0, 1, 2]), and counting it
        // twice repeats replica 1 ([1, 2, 1]).
        const ADMITTED_VIEWS: [u32; 3] = [1, 2, 0];
        const PREPARE_STEPS: usize = 200;
        const ELECTION_STEPS: usize = 5_000;
        const ADMISSION_STEPS: usize = 2_000;
        const RESUBMIT_STEPS: usize = 50;
        const COMMIT_STEPS: usize = 5_000;

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let replica_count: u8 = 3;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 2,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            usize::from(replica_count),
            [INHERITOR, ADMITTER].into_iter(),
            network_opts,
        );
        let inheritor = SimClient::new(INHERITOR);
        let admitter = SimClient::new(ADMITTER);
        sim.register_client_with_primary(&inheritor);
        sim.register_client_with_primary(&admitter);
        let created = super::tests::submit_and_wait_for_reply(
            &mut sim,
            INHERITOR,
            OLD_PRIMARY,
            inheritor.create_stream(STREAM),
        );
        assert_eq!(
            iggy_binary_protocol::result_code(created.body()),
            Some(0),
            "the stream must commit before the creates race"
        );
        let views = |assignments: &[(u64, u32)]| {
            assignments
                .iter()
                .map(|&(_, view)| view)
                .collect::<Vec<_>>()
        };

        // No ack reaches the old primary or passes between the survivors, so the
        // create stays uncommitted through the election and after it.
        for (from, to) in [(1, 0), (2, 0), (1, 2), (2, 1)] {
            set_prepare_ok_delivery(&mut sim, from, to, false);
        }
        let committed = metadata_progress(&sim, OLD_PRIMARY).1;
        let inherited_create =
            inheritor.create_topic(STREAM, INHERITED_TOPIC, INHERITED_PARTITIONS);
        let inherited_request = inherited_create.header().request;
        sim.submit_request(
            INHERITOR,
            OLD_PRIMARY,
            inherited_create.deep_copy().into_generic(),
        );
        let (inherited_op, inherited_assignments) = (0..PREPARE_STEPS)
            .find_map(|_| {
                assert!(
                    sim.step()
                        .iter()
                        .all(|reply| reply.header().client != INHERITOR),
                    "the inherited create must not be answered before it commits"
                );
                let on_backups = [1, 2]
                    .map(|backup| journaled_creates(&sim, backup, INHERITOR, inherited_request));
                (on_backups[0].len() == 1 && on_backups[0] == on_backups[1])
                    .then(|| on_backups[0][0].clone())
            })
            .expect("both backups must journal the create");
        assert_eq!(views(&inherited_assignments), INHERITED_VIEWS);
        assert_eq!(
            metadata_progress(&sim, OLD_PRIMARY).1,
            committed,
            "the old primary must not commit the create without acks"
        );

        sim.replica_crash(OLD_PRIMARY);
        let leader = (0..ELECTION_STEPS)
            .find_map(|_| {
                assert!(
                    sim.step()
                        .iter()
                        .all(|reply| reply.header().client != INHERITOR),
                    "the inherited create must not be answered before it commits"
                );
                (1..replica_count).find(|&replica| is_new_metadata_primary(&sim, replica))
            })
            .expect("the survivors must elect a metadata primary");
        let (head, commit_max) = metadata_progress(&sim, leader);
        assert!(
            head >= inherited_op && commit_max < inherited_op,
            "the new primary must inherit op {inherited_op} uncommitted \
             (head {head}, commit {commit_max})"
        );

        let admitted_create = admitter.create_topic(STREAM, ADMITTED_TOPIC, ADMITTED_PARTITIONS);
        let admitted_request = admitted_create.header().request;
        let (admitted_op, admitted_assignments) = (0..ADMISSION_STEPS)
            .find_map(|step| {
                if step % RESUBMIT_STEPS == 0 {
                    sim.submit_request(
                        ADMITTER,
                        leader,
                        admitted_create.deep_copy().into_generic(),
                    );
                }
                for reply in sim.step() {
                    assert!(
                        reply.header().client == ADMITTER && setup_reply_is_transient(&reply),
                        "only a transient refusal of the second create may be answered \
                         before the survivors ack"
                    );
                }
                journaled_creates(&sim, leader, ADMITTER, admitted_request)
                    .into_iter()
                    .next()
            })
            .expect("the new primary must admit a create while it holds the inherited one");
        assert!(
            metadata_progress(&sim, leader).1 < inherited_op,
            "the inherited create must still be pending when the second one is admitted"
        );
        assert_eq!(
            views(&admitted_assignments),
            ADMITTED_VIEWS,
            "the second create must count the inherited partition exactly once"
        );

        for (from, to) in [(1, 2), (2, 1)] {
            set_prepare_ok_delivery(&mut sim, from, to, true);
        }
        let reply = (0..COMMIT_STEPS)
            .find_map(|_| {
                sim.step().into_iter().find(|reply| {
                    reply.header().client == ADMITTER
                        && reply.header().request == admitted_request
                        && !setup_reply_is_transient(reply)
                })
            })
            .expect("the second create must commit once the survivors ack");
        assert_eq!(
            (
                reply.header().op,
                iggy_binary_protocol::result_code(reply.body())
            ),
            (admitted_op, Some(0))
        );

        // Commits are ordered, so the inherited create committed before the
        // second one. Its retry must get that commit back, not apply again.
        sim.submit_request(INHERITOR, leader, inherited_create.into_generic());
        let retried = (0..COMMIT_STEPS)
            .find_map(|_| {
                sim.step()
                    .into_iter()
                    .find(|reply| reply.header().client == INHERITOR)
            })
            .expect("the retried create must be answered");
        assert_eq!(
            (
                retried.header().op,
                iggy_binary_protocol::result_code(retried.body())
            ),
            (inherited_op, Some(0)),
            "the retry must return the inherited commit"
        );
        assert_eq!(
            journaled_creates(&sim, leader, INHERITOR, inherited_request),
            vec![(inherited_op, inherited_assignments.clone())],
            "the inherited create must be prepared once"
        );

        let expected = vec![
            (ADMITTED_TOPIC.to_string(), admitted_assignments),
            (INHERITED_TOPIC.to_string(), inherited_assignments),
        ];
        for replica in 1..replica_count {
            assert!(
                (0..COMMIT_STEPS).any(|_| {
                    sim.step();
                    sim.replicas[usize::from(replica)].shards[0]
                        .plane
                        .metadata()
                        .applied_frontier()
                        .get()
                        >= admitted_op
                }),
                "replica {replica} must apply both creates"
            );
            assert_eq!(
                committed_topics(&sim, replica, STREAM),
                expected,
                "replica {replica} must hold each create once, with its persisted assignments"
            );
        }
    }

    fn set_prepare_ok_delivery(sim: &mut Simulator, from: u8, to: u8, deliver: bool) {
        let filter = sim
            .network
            .link_filter_mut(ProcessId::Replica(from), ProcessId::Replica(to));
        if deliver {
            filter.insert(Command::PrepareOk);
        } else {
            filter.remove(Command::PrepareOk);
        }
    }

    /// Each op a replica journaled for `client`'s create `request`, with its
    /// partition assignments as `(consensus group, initial view)`.
    fn journaled_creates(
        sim: &Simulator,
        replica: u8,
        client: u128,
        request: u64,
    ) -> Vec<(u64, Vec<(u64, u32)>)> {
        let journal: &SimJournal<MemStorage> = sim.replicas[usize::from(replica)].shards[0]
            .plane
            .metadata()
            .journal
            .as_ref()
            .expect("shard 0 owns the metadata journal");
        (1..=journal.last_op().unwrap_or(0))
            .filter_map(|op| {
                let header = *Journal::header(journal, usize::try_from(op).ok()?)?;
                if header.client != client || header.request != request {
                    return None;
                }
                let prepare = futures::executor::block_on(journal.entry(&header))?;
                let create = CreateTopicWithAssignmentsRequest::decode_from(prepare.body())
                    .expect("a journaled create carries its assignments");
                Some((
                    header.op,
                    create
                        .partitions
                        .iter()
                        .map(|assignment| (assignment.consensus_group_id, assignment.initial_view))
                        .collect(),
                ))
            })
            .collect()
    }

    /// Each topic a replica committed in `stream`, sorted by name, with its
    /// partitions as `(consensus group, created view)`.
    fn committed_topics(
        sim: &Simulator,
        replica: u8,
        stream: &str,
    ) -> Vec<(String, Vec<(u64, u32)>)> {
        let mut topics = sim.replicas[usize::from(replica)].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|streams| {
                streams
                    .items
                    .iter()
                    .filter(|(_, candidate)| &*candidate.name == stream)
                    .flat_map(|(_, matched)| matched.topics.iter())
                    .map(|(_, topic)| {
                        (
                            topic.name.to_string(),
                            topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    (partition.consensus_group_id, partition.created_view)
                                })
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>()
            });
        topics.sort();
        topics
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

#[cfg(test)]
mod partition_repair_driver_tests {
    //! A backup that missed a committed partition prepare recovers in Normal
    //! status, without waiting for a view change.
    //!
    //! Every edge-triggered arming site is starvable. A follower advances
    //! `commit_max` from each prepare header in `replicate_preflight`, before
    //! the gap check drops the prepare, so under produce the primary's commit
    //! heartbeat lands as `CommitOutcome::Accepted` and the backstop that would
    //! arm repair on `Advanced` never runs. These tests starve that edge
    //! outright (no commit heartbeat for the group reaches the lagging replica)
    //! so nothing but the level-triggered detector in `tick_partitions` can
    //! close the gap.

    use super::*;
    use bytes::Bytes;
    use consensus::{Sequencer, Status};
    use iggy_binary_protocol::{
        CommitHeader, ConsensusHeader, PrepareHeader, RepairRangeReplyHeader,
        RequestPreparesHeader, StartViewHeader,
    };
    use packet::Packet;
    use server_common::MessageBag;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Chain replication runs 0 -> 1 -> 2 and stops before the primary, so
    /// replica 2 forwards to nobody: it is the only replica whose losses do not
    /// also starve its successor, and therefore the group, of quorum.
    const LAGGING: u8 = 2;

    /// Replicas in a [`cluster`]: the smallest group that keeps quorum while
    /// [`LAGGING`] misses ops.
    const CLUSTER_REPLICAS: u8 = 3;

    const CLIENT_ID: u128 = 1;

    /// Ops that replicate cleanly before the fault, so the gap opens above a
    /// committed prefix rather than at the group's first op.
    const WARMUP_SENDS: usize = 3;

    /// Ticks stepped after each produce. Keeps one send's round trip inside its
    /// own window so the workload is legible tick by tick.
    const STEPS_PER_SEND: usize = 12;

    /// Produces issued with the fault standing. `partitions::REPAIR_RETRY_TICKS`
    /// is 100, so this must carry the run past the debounce while prepares keep
    /// consuming the `commit_max` advance the heartbeat backstop needs.
    const GAP_SENDS: usize = 12;

    /// Quiet ticks after the produce stops, for the repair stream to land.
    ///
    /// Budgeted against `NORMAL_HEARTBEAT_TICKS` (500): with this group's commit
    /// heartbeats withheld, the lagging replica elects once that timer fires, and
    /// an election would heal the gap through `on_start_view` instead. Every
    /// phase after the fault is installed has to fit inside it.
    const QUIET_STEPS: usize = 160;

    /// Budget for the group to settle once the fault is lifted. Generous rather
    /// than tuned: the drain loop breaks on convergence, and the tail above the
    /// repaired window waits on whichever interval-driven site picks it up.
    const DRAIN_STEPS: usize = 600;

    /// Ticks of healthy load in the no-false-positive test, several debounce
    /// intervals' worth so the sweep gets many chances to arm.
    const LOAD_TICKS: usize = 4 * partitions::REPAIR_RETRY_TICKS as usize;

    /// Paced at a round trip rather than one submit per tick: the pipeline caps
    /// at `PIPELINE_PREPARE_QUEUE_MAX`, so submitting faster than the group
    /// commits just collects transient rejections and the group goes quiet.
    const TICKS_PER_SEND: usize = 4;

    /// Ops the healthy run must have committed for its verdict to mean anything.
    /// A produce's round trip is four one-way hops plus tick granularity, so this
    /// network commits on the order of one op per fifteen ticks however hard the
    /// client pushes; the floor only has to prove the group was live across
    /// several debounce intervals, not that it was saturated.
    const COMMITTED_MIN: u64 = 20;

    /// Produces issued with the fault standing in the walk-starvation test: few
    /// enough that the gap debounce fires only after produce stops, so the
    /// repair window closes at the last carried commit and lands fully resident.
    const STRAND_SENDS: usize = 3;

    /// Quiet budget for the walk-starvation test: debounce, repair stream, then
    /// the drain, kept under `NORMAL_HEARTBEAT_TICKS` so an election cannot be
    /// the healer.
    const STRAND_QUIET_STEPS: usize = 300;

    /// Defines this test's `withhold_one_prepare` chain hook over the statics it
    /// names: swallow the FIRST partition prepare for `$namespace`, once, and
    /// record its op in `$withheld_op`.
    ///
    /// A macro because link hooks are bare `fn` pointers, so the body cannot
    /// capture, and the statics must stay per-test: the sibling tests in this
    /// binary run in parallel and would otherwise share one fault. The statics
    /// are still declared in each test, where its fault setup is read.
    macro_rules! withhold_one_prepare {
        ($namespace:ident, $withheld_op:ident) => {
            fn withhold_one_prepare(packet: &Packet) -> bool {
                let Some(header) = prepare_for(packet, $namespace.load(Ordering::Relaxed)) else {
                    return false;
                };
                $withheld_op
                    .compare_exchange(0, header.op, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            }
        };
    }

    /// The prepare a packet carries, if it carries one for `group`. Keyed on
    /// the group, so `metadata_repair_driver_tests` reads the metadata plane's
    /// prepares with the same helper.
    ///
    /// `pub`, not `pub(super)`, in this and the helpers below: the module is
    /// private and `#[cfg(test)]`, so both spell the same reach, and
    /// `clippy::redundant_pub_crate` refuses the narrower one.
    pub fn prepare_for(packet: &Packet, group: u64) -> Option<PrepareHeader> {
        if packet.message.header().command != Command::Prepare {
            return None;
        }
        let header: &PrepareHeader =
            bytemuck::checked::from_bytes(&packet.message.as_slice()[..size_of::<PrepareHeader>()]);
        (header.group == group).then_some(*header)
    }

    /// Whether a packet is a commit heartbeat for `group`.
    pub fn is_commit_for(packet: &Packet, group: u64) -> bool {
        if packet.message.header().command != Command::Commit {
            return false;
        }
        let header: &CommitHeader =
            bytemuck::checked::from_bytes(&packet.message.as_slice()[..size_of::<CommitHeader>()]);
        header.group == group
    }

    /// Whether a packet is a repair request for `group`.
    pub fn is_request_prepares_for(packet: &Packet, group: u64) -> bool {
        if packet.message.header().command != Command::RequestPrepares {
            return false;
        }
        let header: &RequestPreparesHeader = bytemuck::checked::from_bytes(
            &packet.message.as_slice()[..size_of::<RequestPreparesHeader>()],
        );
        header.group == group
    }

    /// Whether a packet is a repair stream terminator for `group`.
    pub fn is_repair_done_for(packet: &Packet, group: u64) -> bool {
        if packet.message.header().command != Command::RepairDone {
            return false;
        }
        let header: &RepairRangeReplyHeader = bytemuck::checked::from_bytes(
            &packet.message.as_slice()[..size_of::<RepairRangeReplyHeader>()],
        );
        header.group == group
    }

    pub fn cluster(seed: u64) -> (Simulator, SimClient) {
        sharded_cluster(seed, 1)
    }

    /// [`cluster`] with `shards_per_replica` shards on each replica.
    pub fn sharded_cluster(seed: u64, shards_per_replica: u16) -> (Simulator, SimClient) {
        sized_cluster(seed, CLUSTER_REPLICAS, shards_per_replica)
    }

    /// [`sharded_cluster`] with `replica_count` replicas.
    pub fn sized_cluster(
        seed: u64,
        replica_count: u8,
        shards_per_replica: u16,
    ) -> (Simulator, SimClient) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            ..packet::PacketSimulatorOptions::default()
        };
        let sim = Simulator::with_shards(
            replica_count as usize,
            shards_per_replica,
            std::iter::once(CLIENT_ID),
            network_opts,
        );
        (sim, SimClient::new(CLIENT_ID))
    }

    /// Submit `sends` produces against `namespace`, stepping between each.
    fn produce(
        sim: &mut Simulator,
        client: &SimClient,
        namespace: IggyNamespace,
        sends: usize,
        tag: &str,
    ) {
        for index in 0..sends {
            let msg = client.send_messages(namespace, &[Bytes::from(format!("{tag}-{index}"))]);
            sim.submit_request(client.client_id(), 0, msg.into_generic());
            for _ in 0..STEPS_PER_SEND {
                sim.step();
            }
        }
    }

    /// `(status, view, commit_min, commit_max)` of one replica's partition group.
    fn group_state(
        sim: &Simulator,
        replica: u8,
        namespace: IggyNamespace,
    ) -> (Status, u32, u64, u64) {
        let shard = sim.replicas[replica as usize].partition_shard(namespace);
        let partition = shard
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("the replica hosts the group");
        let consensus = partition.consensus();
        (
            consensus.status(),
            consensus.view(),
            consensus.commit_min(),
            consensus.commit_max(),
        )
    }

    fn journal_holds(sim: &Simulator, replica: u8, namespace: IggyNamespace, op: u64) -> bool {
        let shard = sim.replicas[replica as usize].partition_shard(namespace);
        shard
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .is_some_and(|partition| partition.log.journal().inner.holds_op(op))
    }

    fn gap_drops(sim: &Simulator, replica: u8, namespace: IggyNamespace) -> u64 {
        sim.replicas[replica as usize]
            .partition_shard(namespace)
            .metrics()
            .partition_prepare_gap_drops_value()
    }

    /// Gap drops still buffered on the partition, i.e. recorded since the sweep
    /// last drained them into the shard counter.
    fn buffered_gap_drops(sim: &Simulator, replica: u8, namespace: IggyNamespace) -> u64 {
        sim.replicas[replica as usize]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .map_or(0, partitions::IggyPartition::prepare_gap_drops)
    }

    fn transfer_armed(sim: &Simulator, replica: u8, namespace: IggyNamespace) -> bool {
        let shard = sim.replicas[replica as usize].partition_shard(namespace);
        shard
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .is_some_and(|partition| {
                partition.transfer.is_some()
                    || partition.consensus().state_transfer_stage()
                        != consensus::StateTransferStage::Idle
            })
    }

    #[test]
    fn given_a_resident_repair_window_when_commit_walk_is_bounded_should_drain_without_repair() {
        const OPS: usize = partitions::COMMIT_WALK_OPS_MAX + 2;
        let namespace = IggyNamespace::new(1, 1, 0);
        let sim = resident_repair_window(namespace, OPS, OPS);
        let shard = sim.replicas[usize::from(LAGGING)].partition_shard(namespace);

        deliver_commit(shard, namespace, OPS as u64);

        let (_, _, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
        assert_eq!(commit_min, partitions::COMMIT_WALK_OPS_MAX as u64);
        assert_eq!(commit_max, OPS as u64);
        for op in commit_min + 1..=commit_max {
            assert!(journal_holds(&sim, LAGGING, namespace, op));
        }
        assert!(
            shard
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .unwrap()
                .repair
                .is_none(),
            "resident operations must not consume a repair session while waiting for the next walk"
        );
        assert_eq!(repair_requests(&sim), []);

        let mut namespace_scratch = Vec::new();
        assert!(
            futures::executor::block_on(shard.tick_partitions(&mut namespace_scratch)).is_none()
        );
        assert_eq!(
            group_state(&sim, LAGGING, namespace),
            (Status::Normal, 0, OPS as u64, OPS as u64),
            "the existing tick must finish the resident backlog without peer repair"
        );
        assert_eq!(repair_requests(&sim), []);
    }

    #[test]
    fn given_a_resident_prefix_when_a_later_op_is_missing_should_repair_and_drain() {
        const RESIDENT_OPS: usize = partitions::COMMIT_WALK_OPS_MAX + 1;
        const OPS: usize = RESIDENT_OPS + 1;
        let namespace = IggyNamespace::new(1, 1, 0);
        let sim = resident_repair_window(namespace, OPS, RESIDENT_OPS);
        let shard = sim.replicas[usize::from(LAGGING)].partition_shard(namespace);

        deliver_commit(shard, namespace, OPS as u64);

        let (_, _, commit_min, _) = group_state(&sim, LAGGING, namespace);
        assert_eq!(commit_min, partitions::COMMIT_WALK_OPS_MAX as u64);
        assert!(journal_holds(&sim, LAGGING, namespace, commit_min + 1));
        assert!(!journal_holds(&sim, LAGGING, namespace, OPS as u64));
        let requests = repair_requests(&sim);
        assert_eq!(
            requests.len(),
            1,
            "a later hole must still open peer repair"
        );
        assert_eq!(requests[0].from_op, commit_min + 1);
        assert_eq!(requests[0].to_op, OPS as u64);

        let prepare = tests::retained_prepare(&sim, 0, namespace, OPS as u64);
        let partition = shard.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        futures::executor::block_on(partition.apply_repaired_prepare(prepare));
        assert!(journal_holds(&sim, LAGGING, namespace, OPS as u64));
        let mut namespace_scratch = Vec::new();
        assert!(
            futures::executor::block_on(shard.tick_partitions(&mut namespace_scratch)).is_none()
        );
        assert_eq!(
            group_state(&sim, LAGGING, namespace),
            (Status::Normal, 0, OPS as u64, OPS as u64)
        );
    }

    #[test]
    fn given_a_resident_committed_window_when_an_adopted_suffix_is_missing_should_fetch_above_commit_max()
     {
        const COMMITTED_OPS: usize = partitions::COMMIT_WALK_OPS_MAX + 1;
        const OPS: usize = COMMITTED_OPS + 1;
        let namespace = IggyNamespace::new(1, 1, 0);
        let sim = resident_repair_window(namespace, OPS, COMMITTED_OPS);
        let shard = sim.replicas[usize::from(LAGGING)].partition_shard(namespace);
        let missing = tests::retained_prepare(&sim, 0, namespace, OPS as u64);
        let header_size = size_of::<StartViewHeader>();
        let total_size = header_size + size_of::<PrepareHeader>();
        let mut start_view = Message::<StartViewHeader>::new(total_size);
        start_view.as_mut_slice()[header_size..]
            .copy_from_slice(bytemuck::bytes_of(missing.header()));
        let body_checksum = u128::from(iggy_common::calculate_checksum(
            &start_view.as_slice()[header_size..],
        ));
        let start_view = start_view.transmute_header(|_, header: &mut StartViewHeader| {
            header.command = Command::StartView;
            header.cluster = 1;
            header.replica = 0;
            header.group = namespace.inner();
            header.op = OPS as u64;
            header.commit = COMMITTED_OPS as u64;
            header.partition_incarnation = missing.header().partition_incarnation;
            header.size = u32::try_from(total_size).unwrap();
            header.checksum_body = body_checksum;
            header.seal();
        });
        futures::executor::block_on(shard.on_message(MessageBag::StartView(start_view)));

        let (_, _, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
        assert_eq!(commit_min, partitions::COMMIT_WALK_OPS_MAX as u64);
        assert_eq!(commit_max, COMMITTED_OPS as u64);
        assert!(journal_holds(&sim, LAGGING, namespace, commit_min + 1));
        let requests = repair_requests(&sim);
        assert_eq!(
            requests.len(),
            1,
            "adopted headers do not supply the missing body"
        );
        assert_eq!(requests[0].from_op, commit_min + 1);
        assert_eq!(requests[0].to_op, OPS as u64);

        let partition = shard.plane.partitions().get_mut_by_ns(&namespace).unwrap();
        futures::executor::block_on(partition.apply_repaired_prepare(missing));
        assert!(journal_holds(&sim, LAGGING, namespace, OPS as u64));
        deliver_commit(shard, namespace, OPS as u64);
        assert_eq!(
            group_state(&sim, LAGGING, namespace),
            (Status::Normal, 0, OPS as u64, OPS as u64)
        );
    }

    fn resident_repair_window(
        namespace: IggyNamespace,
        total_ops: usize,
        resident_ops: usize,
    ) -> Simulator {
        let (mut sim, client) = cluster(0x5EED_0240);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        sim.replica_crash(LAGGING);
        for _ in 0..total_ops {
            let reply = tests::submit_and_wait_for_reply(
                &mut sim,
                CLIENT_ID,
                0,
                client.send_messages(namespace, &[Bytes::from_static(b"resident-repair")]),
            );
            assert_eq!(reply.header().status, 0);
        }
        assert_eq!(group_state(&sim, 0, namespace).2, total_ops as u64);

        // Replay real prepares without running the crashed backup's pump so its
        // resident backlog reaches the commit edge in one bounded walk.
        let shard = sim.replicas[usize::from(LAGGING)].partition_shard(namespace);
        for op in 1..=resident_ops as u64 {
            let prepare = tests::retained_prepare(&sim, 0, namespace, op);
            let partition = shard.plane.partitions().get_mut_by_ns(&namespace).unwrap();
            futures::executor::block_on(partition.on_replicate(prepare));
            assert!(journal_holds(&sim, LAGGING, namespace, op));
        }
        assert_eq!(group_state(&sim, LAGGING, namespace).2, 0);
        sim.outboxes[usize::from(LAGGING)].drain();
        sim
    }

    fn deliver_commit(shard: &Replica, namespace: IggyNamespace, commit: u64) {
        let incarnation = shard
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("the replica hosts the partition")
            .created_revision();
        let message = Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
            |_, header: &mut CommitHeader| {
                header.command = Command::Commit;
                header.cluster = 1;
                header.replica = 0;
                header.group = namespace.inner();
                header.partition_incarnation = incarnation;
                header.commit = commit;
                header.timestamp_monotonic = commit;
                header.size = u32::try_from(size_of::<CommitHeader>()).unwrap();
                header.seal();
            },
        );
        futures::executor::block_on(shard.on_message(MessageBag::Commit(message)));
    }

    fn repair_requests(sim: &Simulator) -> Vec<RequestPreparesHeader> {
        sim.outboxes[usize::from(LAGGING)]
            .drain()
            .into_iter()
            .filter_map(|envelope| match envelope.payload {
                bus::EnvelopePayload::Replica(message)
                    if message.header().command == Command::RequestPrepares =>
                {
                    let header = *bytemuck::checked::from_bytes::<RequestPreparesHeader>(
                        &message.as_slice()[..size_of::<RequestPreparesHeader>()],
                    );
                    assert_eq!(envelope.to_replica, Some(0));
                    Some(header)
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn given_a_backup_that_dropped_a_committed_prepare_when_heartbeat_advances_are_starved_should_repair_in_normal_status()
     {
        // Statics, not captures: the link hooks are bare `fn` pointers. Declared
        // inside the test because the sibling tests in this binary run in
        // parallel and would otherwise share them.
        static GAP_NS: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);

        withhold_one_prepare!(GAP_NS, WITHHELD_OP);

        /// Primary -> 2: withhold this group's commit heartbeats, so the
        /// `Advanced` backstop can never run, and withhold retransmits of the
        /// dropped op. The retransmit half stands in for production behaviour
        /// rather than adding a fault: `consensus::retransmit_targets` skips an
        /// op that already reached quorum, and this op reaches quorum on 0 and 1
        /// alone. Without it the retry timer could heal the gap and the test
        /// would pass with no repair driver at all.
        fn starve_commit_edge(packet: &Packet) -> bool {
            let group = GAP_NS.load(Ordering::Relaxed);
            if let Some(header) = prepare_for(packet, group) {
                return header.op == WITHHELD_OP.load(Ordering::Relaxed);
            }
            is_commit_for(packet, group)
        }

        let (mut sim, client) = cluster(0x5EED_0232);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);
        WITHHELD_OP.store(0, Ordering::Relaxed);

        produce(&mut sim, &client, namespace, WARMUP_SENDS, "warmup");
        let (_, _, warm_commit_min, _) = group_state(&sim, LAGGING, namespace);
        assert!(
            warm_commit_min > 0,
            "the lagging replica committed nothing before the fault, so the gap \
             below would open at the group's first op"
        );

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);

        produce(&mut sim, &client, namespace, GAP_SENDS, "gap");

        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no partition prepare crossed the chain link, so the fault never armed"
        );
        assert!(
            gap_drops(&sim, LAGGING, namespace) > 0,
            "the lagging replica never reached its backup gap check, so the \
             prepares after the withheld op were not dropped as a gap"
        );

        for _ in 0..QUIET_STEPS {
            sim.step();
        }

        // Judged with the blockade still standing, so the tick driver is the only
        // thing that can have armed the repair: no commit heartbeat for this
        // group has reached the replica since the gap opened, and `on_commit` is
        // where the `Advanced` backstop lives.
        let (status, view, commit_min, _) = group_state(&sim, LAGGING, namespace);
        assert_eq!(
            view, 0,
            "a view change healed the gap instead of the repair driver; the test \
             proves nothing about normal status"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert!(
            journal_holds(&sim, LAGGING, namespace, withheld),
            "op {withheld} was never repaired back into the lagging replica's journal"
        );
        assert!(
            commit_min >= withheld,
            "the commit walk never crossed the repaired hole: stopped at \
             {commit_min}, the withheld op is {withheld}"
        );

        // Lift the blockade and let the group settle. The commit walk is driven
        // by arriving frames, so with this group's heartbeats withheld the tail
        // above the repaired window has nothing to advance it; that is the
        // injected fault, not the gap under test.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) = None;
        for _ in 0..DRAIN_STEPS {
            sim.step();
            let (_, _, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
            if commit_min == commit_max {
                break;
            }
        }
        let (status, view, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
        assert_eq!((status, view), (Status::Normal, 0));
        assert_eq!(
            commit_min, commit_max,
            "the lagging replica is still gap-stopped: committed through \
             {commit_max} but walkable only to {commit_min}"
        );
    }

    #[test]
    fn given_a_repair_armed_by_the_tick_driver_when_the_range_is_evicted_should_convert_to_state_transfer()
     {
        static GAP_NS: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);

        withhold_one_prepare!(GAP_NS, WITHHELD_OP);

        fn starve_commit_edge(packet: &Packet) -> bool {
            let group = GAP_NS.load(Ordering::Relaxed);
            if let Some(header) = prepare_for(packet, group) {
                return header.op == WITHHELD_OP.load(Ordering::Relaxed);
            }
            is_commit_for(packet, group)
        }

        let (mut sim, client) = cluster(0x5EED_0233);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);
        WITHHELD_OP.store(0, Ordering::Relaxed);

        produce(&mut sim, &client, namespace, WARMUP_SENDS, "warmup");

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);
        // Evict before the repair debounce can heal the hole during produce.
        produce(&mut sim, &client, namespace, STRAND_SENDS, "gap");
        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no partition prepare crossed the chain link, so the fault never armed"
        );
        let (_, _, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
        assert!(
            commit_min < withheld
                && withheld <= commit_max
                && !journal_holds(&sim, LAGGING, namespace, withheld),
            "op {withheld} must still be a committed hole before eviction \
             (walkable to {commit_min} of {commit_max})"
        );

        // Compact the serving side past the gap. This plane's journal is
        // memory-only, so wiping it IS its retention floor moving: the serve
        // path reads `repair_retained_from` as `None` and reports eviction from
        // its own commit frontier, which is exactly what a peer that
        // checkpointed past the requested window answers.
        {
            let primary = sim.replicas[0].partition_shard(namespace);
            let partition = primary
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .expect("the primary hosts the group");
            partition.log.journal().inner.clear_all();
        }

        // A reordered RepairDone can precede RangeEvicted, requiring a retry.
        // This budget remains below the heartbeat timeout with the short burst.
        let mut repair_armed = false;
        for _ in 0..STRAND_QUIET_STEPS {
            sim.step();
            repair_armed |= sim.replicas[usize::from(LAGGING)]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .is_some_and(|partition| partition.repair.is_some());
            if transfer_armed(&sim, LAGGING, namespace) {
                break;
            }
        }

        let (status, view, ..) = group_state(&sim, LAGGING, namespace);
        assert!(
            repair_armed,
            "the tick driver never armed the repair session"
        );
        assert_eq!(
            view, 0,
            "a view change armed the recovery instead of the tick-armed repair session"
        );
        assert!(
            transfer_armed(&sim, LAGGING, namespace),
            "the tick-armed repair session hit an evicted range but never converted \
             to a state transfer (status {status:?})"
        );
    }

    /// The rotation used to step back to the primary after each backup, so in a
    /// group of five a backup alternated between the primary and the replica
    /// after it until it spent the ring and transferred from the primary. Neither
    /// of those answers this group's repair here, nor does replica 3: only
    /// replica 2, two after the primary, serves the withheld op.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_five_replicas_when_only_the_second_after_the_primary_serves_repair_should_repair_from_it()
     {
        const REPLICAS: u8 = 5;
        /// Last in the chain 0 -> 1 -> 2 -> 3 -> 4, so its losses cannot starve
        /// the group of quorum.
        const LAGGING_OF_FIVE: u8 = 4;
        /// Every replica but `primary + 2` drops the lagging replica's repair
        /// requests for the group.
        const SILENT: [u8; 3] = [0, 1, 3];
        /// Short sessions, so two silent peers fit in the quiet budget.
        const ROTATE_RETRY_TICKS: u32 = 10;
        /// The debounce, two silent sessions and the repair stream. With the
        /// produce before it, under `NORMAL_HEARTBEAT_TICKS`, so no election
        /// heals the gap first.
        const RING_QUIET_STEPS: usize = 400;

        static GAP_NS: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_COPIES: AtomicU64 = AtomicU64::new(0);
        static SERVING_PEER_REQUESTS: AtomicU64 = AtomicU64::new(0);

        fn withhold_selected_prepare(packet: &Packet) -> bool {
            let selected = prepare_for(packet, GAP_NS.load(Ordering::Relaxed))
                .is_some_and(|header| header.op == WITHHELD_OP.load(Ordering::Relaxed));
            if selected {
                WITHHELD_COPIES.fetch_add(1, Ordering::Relaxed);
            }
            selected
        }

        /// Primary -> 4: withhold this group's commit heartbeats, so only the
        /// tick driver arms and rotates the repair, and the dropped op's
        /// retransmits.
        fn starve_commit_edge(packet: &Packet) -> bool {
            let group = GAP_NS.load(Ordering::Relaxed);
            withhold_selected_prepare(packet) || is_commit_for(packet, group)
        }

        fn drop_repair_request(packet: &Packet) -> bool {
            is_request_prepares_for(packet, GAP_NS.load(Ordering::Relaxed))
        }

        fn count_serving_peer_request(packet: &Packet) -> bool {
            if is_request_prepares_for(packet, GAP_NS.load(Ordering::Relaxed)) {
                SERVING_PEER_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        let (mut sim, client) = sized_cluster(0x5EED_024B, REPLICAS, 1);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);
        WITHHELD_COPIES.store(0, Ordering::Relaxed);
        SERVING_PEER_REQUESTS.store(0, Ordering::Relaxed);
        sim.replicas[usize::from(LAGGING_OF_FIVE)]
            .partition_shard(namespace)
            .set_repair_retry_ticks(ROTATE_RETRY_TICKS);

        // Five replicas can outlast the three-replica fixture's fixed send pace.
        // Await replies so an in-flight request cannot discard the next send.
        for _ in 0..WARMUP_SENDS {
            let reply = tests::submit_and_wait_for_reply(
                &mut sim,
                CLIENT_ID,
                0,
                client.send_messages(namespace, &[Bytes::from_static(b"warmup")]),
            );
            assert_eq!(reply.header().status, 0);
        }
        let next_op = sim.replicas[0]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .unwrap()
            .consensus()
            .sequencer()
            .current_sequence()
            + 1;
        WITHHELD_OP.store(next_op, Ordering::Relaxed);

        *sim.network.link_drop_packet_fn(
            ProcessId::Replica(LAGGING_OF_FIVE - 1),
            ProcessId::Replica(LAGGING_OF_FIVE),
        ) = Some(withhold_selected_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING_OF_FIVE)) =
            Some(starve_commit_edge);
        for peer in SILENT {
            *sim.network.link_drop_packet_fn(
                ProcessId::Replica(LAGGING_OF_FIVE),
                ProcessId::Replica(peer),
            ) = Some(drop_repair_request);
        }

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(LAGGING_OF_FIVE), ProcessId::Replica(2)) =
            Some(count_serving_peer_request);

        for _ in 0..GAP_SENDS {
            let reply = tests::submit_and_wait_for_reply(
                &mut sim,
                CLIENT_ID,
                0,
                client.send_messages(namespace, &[Bytes::from_static(b"gap")]),
            );
            assert_eq!(reply.header().status, 0);
            if gap_drops(&sim, LAGGING_OF_FIVE, namespace) > 0 {
                break;
            }
        }
        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert!(
            WITHHELD_COPIES.load(Ordering::Relaxed) > 0,
            "no partition prepare crossed the chain link, so the fault never armed"
        );
        assert!(
            gap_drops(&sim, LAGGING_OF_FIVE, namespace) > 0,
            "the lagging replica never reached its backup gap check: withheld {withheld}, state {:?}, held {}",
            group_state(&sim, LAGGING_OF_FIVE, namespace),
            journal_holds(&sim, LAGGING_OF_FIVE, namespace, withheld)
        );

        let mut transferred = false;
        for _ in 0..RING_QUIET_STEPS {
            sim.step();
            transferred |= transfer_armed(&sim, LAGGING_OF_FIVE, namespace);
            let (_, view, commit_min, _) = group_state(&sim, LAGGING_OF_FIVE, namespace);
            if transferred || view != 0 || commit_min >= withheld {
                break;
            }
        }

        let (status, view, commit_min, commit_max) = group_state(&sim, LAGGING_OF_FIVE, namespace);
        assert_eq!(
            view, 0,
            "a view change healed the gap instead of the rotation; the test proves \
             nothing about the ring"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert!(
            !transferred,
            "the ring was spent before the rotation reached replica 2, the only one \
             that serves repair, so the replica transferred from the primary"
        );
        assert!(
            SERVING_PEER_REQUESTS.load(Ordering::Relaxed) > 0,
            "the repair rotation never requested the withheld op from replica 2"
        );
        assert!(
            journal_holds(&sim, LAGGING_OF_FIVE, namespace, withheld),
            "op {withheld} was never repaired from replica 2"
        );
        assert!(
            commit_min >= withheld,
            "the commit walk never crossed the hole: stopped at {commit_min} of \
             {commit_max}, the withheld op is {withheld}"
        );
    }

    #[test]
    fn given_healthy_pipelined_traffic_when_no_gap_exists_should_not_arm_repair() {
        static GAP_NS: AtomicU64 = AtomicU64::new(0);
        static REPAIR_REQUESTS: AtomicU64 = AtomicU64::new(0);

        /// Observer, not a fault: counts this group's repair requests and passes
        /// every packet through.
        fn count_repair_requests(packet: &Packet) -> bool {
            if is_request_prepares_for(packet, GAP_NS.load(Ordering::Relaxed)) {
                REPAIR_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        // TWO replicas, so quorum spans both: no op can commit without the
        // backup's ack, every reordering-induced gap therefore blocks quorum, and
        // `consensus::retransmit_targets` refills it. At three, the network's
        // per-tick delivery shuffle lets an op commit on the primary and its
        // first chain hop while the last replica loses it for good, which is the
        // very fault the sibling tests inject -- it would then be repaired here,
        // correctly, and this assertion would fire on a healthy driver.
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let replica_count: u8 = 2;
        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(CLIENT_ID),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 1,
                seed: 0x5EED_0234,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let client = SimClient::new(CLIENT_ID);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);
        REPAIR_REQUESTS.store(0, Ordering::Relaxed);

        for (from, to) in [(0u8, 1u8), (1, 0)] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) =
                Some(count_repair_requests);
        }

        // Sustained, not bursty: a produce every tick, for several debounce
        // intervals, so the sweep gets many chances to arm against ordinary
        // pipelining.
        for tick in 0..LOAD_TICKS {
            if tick % TICKS_PER_SEND == 0 {
                let msg =
                    client.send_messages(namespace, &[Bytes::from(format!("healthy-{tick}"))]);
                sim.submit_request(client.client_id(), 0, msg.into_generic());
            }
            sim.step();
        }
        for _ in 0..QUIET_STEPS {
            sim.step();
        }

        let committed = group_state(&sim, 1, namespace).2;
        let sends = LOAD_TICKS / TICKS_PER_SEND;
        assert!(
            committed >= COMMITTED_MIN,
            "the backup committed only {committed} ops across {sends} sends, so the \
             sweep was never driven over a loaded group"
        );
        // No per-tick lag sampling: `sim.step()` runs the pumps to quiescence, so
        // every sample lands AFTER the tick whose walk backstop drained whatever
        // the step's produce left, and a run that asserted something about those
        // samples would be asserting over an empty set. The commit lag a naive
        // detector misreads lives inside a step, and it is pinned where it can
        // be held still: exhaustively in `gap_detector_tests`, and end to end by
        // the walk-starvation run in this module, which strands a backup in
        // exactly that state and proves nothing arms repair over it.
        //
        // What this run adds is the part only a live group can show: a loaded,
        // lossless cluster produces no repair traffic at all.
        for replica in 0..replica_count {
            let (status, view, commit_min, commit_max) = group_state(&sim, replica, namespace);
            assert_eq!(
                (status, view),
                (Status::Normal, 0),
                "replica {replica} left view 0 / Normal, so a view change could \
                 account for repair traffic"
            );
            // Residual lag is transient at worst: a heartbeat carrying a commit
            // the replica already knows is `CommitOutcome::Accepted`, so a backup
            // can sit with committed-but-unwalked ops until the tick sweep's
            // walk-stalled backstop resumes the walk. Every one of them is
            // RESIDENT, which is what separates it from a gap.
            if commit_min < commit_max {
                assert!(
                    journal_holds(&sim, replica, namespace, commit_min + 1),
                    "replica {replica} lags at {commit_min} of {commit_max} with op \
                     {} missing, so a no-loss run produced a real hole",
                    commit_min + 1
                );
            }
        }
        assert_eq!(
            REPAIR_REQUESTS.load(Ordering::Relaxed),
            0,
            "the tick driver requested repair on a healthy group across {sends} sends; \
             its gap predicate is reading ordinary commit lag as a journal hole"
        );
    }

    #[test]
    fn given_a_backup_holding_resident_committed_ops_when_every_walk_edge_is_starved_should_drain_in_normal_status()
     {
        static GAP_NS: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_DONES: AtomicU64 = AtomicU64::new(0);

        withhold_one_prepare!(GAP_NS, WITHHELD_OP);

        /// Primary -> 2: withhold every direct prepare (live ones ride the
        /// chain, so this starves only retransmit heals), the group's commit
        /// heartbeats, and its repair terminators. The repaired ops themselves
        /// pass, so the window lands resident while `complete_repair`, the walk
        /// the terminator would run, never fires.
        fn starve_walk_edges(packet: &Packet) -> bool {
            let group = GAP_NS.load(Ordering::Relaxed);
            if prepare_for(packet, group).is_some() {
                return true;
            }
            if is_repair_done_for(packet, group) {
                WITHHELD_DONES.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            is_commit_for(packet, group)
        }

        let (mut sim, client) = cluster(0x5EED_0235);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        WITHHELD_DONES.store(0, Ordering::Relaxed);

        produce(&mut sim, &client, namespace, WARMUP_SENDS, "warmup");
        let (_, _, warm_commit_min, _) = group_state(&sim, LAGGING, namespace);
        assert!(
            warm_commit_min > 0,
            "the lagging replica committed nothing before the fault, so the gap \
             below would open at the group's first op"
        );

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_walk_edges);

        produce(&mut sim, &client, namespace, STRAND_SENDS, "strand");

        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no partition prepare crossed the chain link, so the fault never armed"
        );

        for _ in 0..STRAND_QUIET_STEPS {
            sim.step();
            let (_, view, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
            if view != 0 || (commit_min >= withheld && commit_min == commit_max) {
                break;
            }
        }

        assert!(
            WITHHELD_DONES.load(Ordering::Relaxed) > 0,
            "no repair terminator was withheld, so the walk was never starved and \
             a green run would not prove the tick backstop"
        );
        assert!(
            journal_holds(&sim, LAGGING, namespace, withheld),
            "op {withheld} was never repaired back into the lagging replica's journal"
        );
        let (status, view, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
        assert_eq!(
            view, 0,
            "a view change drained the walk instead of the tick backstop; the test \
             proves nothing about normal status"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        for op in commit_min + 1..=commit_max {
            assert!(
                journal_holds(&sim, LAGGING, namespace, op),
                "op {op} is not resident, so this run stranded on a repair gap, \
                 not a parked walk"
            );
        }
        assert_eq!(
            commit_min, commit_max,
            "the walk never resumed over resident committed ops: walkable to \
             {commit_min}, committed through {commit_max}, every op between resident"
        );
    }

    #[test]
    fn given_a_gap_stopped_backup_when_repair_is_armed_should_spend_its_debounce() {
        // The reset lives in `maybe_request_partition_repair`, the funnel every
        // arming site goes through, precisely because the four edge-triggered
        // sites never touch `gap_ticks` themselves. Driven here rather than
        // modelled: a saturated count hands the NEXT gap an arm on its first
        // tick, and the shape that reaches it is a repair short enough that no
        // sweep ever observes the partition with its recovery owned.
        static GAP_NS: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);

        withhold_one_prepare!(GAP_NS, WITHHELD_OP);

        /// Primary -> 2: withhold this group's commit heartbeats, so the
        /// edge-triggered backstop cannot be what arms the repair below.
        fn starve_commit_edge(packet: &Packet) -> bool {
            let group = GAP_NS.load(Ordering::Relaxed);
            if let Some(header) = prepare_for(packet, group) {
                return header.op == WITHHELD_OP.load(Ordering::Relaxed);
            }
            is_commit_for(packet, group)
        }

        let (mut sim, client) = cluster(0x5EED_0239);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);
        WITHHELD_OP.store(0, Ordering::Relaxed);

        produce(&mut sim, &client, namespace, WARMUP_SENDS, "warmup");
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);
        // Short, deliberately: long enough to open the gap, well under the
        // debounce, so the driver has not armed on its own and the sweep below
        // is the one that does it.
        produce(&mut sim, &client, namespace, STRAND_SENDS, "gap");
        assert_ne!(
            WITHHELD_OP.load(Ordering::Relaxed),
            0,
            "no partition prepare crossed the chain link, so the fault never armed"
        );
        let (_, _, commit_min, commit_max) = group_state(&sim, LAGGING, namespace);
        assert!(
            commit_min < commit_max && !journal_holds(&sim, LAGGING, namespace, commit_min + 1),
            "the lagging replica is not gap-stopped (walkable to {commit_min} of \
             {commit_max}), so the sweep has nothing to arm"
        );

        // Saturate the debounce by hand and take one sweep: the arm has to be
        // what spends it, whatever the tick counted on the way in.
        let shard = sim.replicas[LAGGING as usize].partition_shard(namespace);
        {
            let partitions = shard.plane.partitions();
            let partition = partitions
                .get_by_ns(&namespace)
                .expect("the replica hosts the group");
            assert!(
                partition.repair.is_none(),
                "a session was already open, so this sweep would arm nothing"
            );
            partition.gap_ticks.set(u32::MAX);
        }
        let mut namespace_scratch = Vec::new();
        futures::executor::block_on(shard.tick_partitions(&mut namespace_scratch));

        let partitions = shard.plane.partitions();
        let partition = partitions
            .get_by_ns(&namespace)
            .expect("the replica hosts the group");
        assert!(
            partition.repair.is_some(),
            "the sweep never opened a session, so the reset below proves nothing"
        );
        assert_eq!(
            partition.gap_ticks.get(),
            0,
            "the arm left the debounce saturated; a repair that completes before the \
             next sweep would then hand the following gap an arm on its first tick"
        );
    }

    #[test]
    fn given_a_local_commit_failure_in_the_tick_walk_when_the_partition_fences_should_return_the_fault()
     {
        static GAP_NS: AtomicU64 = AtomicU64::new(0);

        /// Primary -> 2: withhold this group's commit heartbeats, so the last
        /// produced op stays journaled-but-uncommitted here and the walk the
        /// test drives below is the first one that can reach it.
        fn starve_commit_edge(packet: &Packet) -> bool {
            is_commit_for(packet, GAP_NS.load(Ordering::Relaxed))
        }

        let (mut sim, client) = cluster(0x5EED_0236);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);

        produce(&mut sim, &client, namespace, WARMUP_SENDS, "warmup");
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);
        produce(&mut sim, &client, namespace, 1, "fence");

        let (_, _, commit_min, _) = group_state(&sim, LAGGING, namespace);
        let stranded = commit_min + 1;
        assert!(
            journal_holds(&sim, LAGGING, namespace, stranded),
            "op {stranded} is not resident, so the walk below would find nothing \
             to commit and nothing to fail on"
        );

        // The sweep is invoked DIRECTLY rather than through `sim.step()`: what
        // is under test is the verdict of the tick whose own walk raised the
        // fault, and a pump-driven tick would fence on one pass and be asked
        // for its verdict on the next, where the pre-walk read already answers.
        let shard = sim.replicas[LAGGING as usize].partition_shard(namespace);
        {
            let partitions = shard.plane.partitions();
            let partition = partitions
                .get_mut_by_ns(&namespace)
                .expect("the replica hosts the group");
            // Committed as far as this replica knows, with the body resident:
            // walk-stalled, which is the state the tick's backstop walks.
            partition.consensus().advance_commit_max(stranded);
            partition.inject_commit_failure();
        }

        let mut namespace_scratch = Vec::new();
        let fault = futures::executor::block_on(shard.tick_partitions(&mut namespace_scratch))
            .expect(
                "the tick returned clean over a partition its own walk fenced; the pump \
                 would keep serving a divergent replica on that verdict",
            );
        assert_eq!(fault.namespace_raw, namespace.inner());
        assert_eq!(
            fault.op, stranded,
            "the fault must name the op whose local commit failed"
        );
        assert!(
            sim.replicas[LAGGING as usize]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
                .is_some_and(|partition| partition.fatal().is_some()),
            "the partition must stay fenced after the tick reported the fault"
        );
    }

    #[test]
    fn given_buffered_gap_drops_when_the_namespace_is_removed_should_still_count_them() {
        static GAP_NS: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);

        // Every prepare after the withheld one reaches the backup's gap check
        // and is destroyed there, which is what the counter records.
        withhold_one_prepare!(GAP_NS, WITHHELD_OP);

        let (mut sim, client) = cluster(0x5EED_0237);
        let namespace = IggyNamespace::new(1, 1, 0);
        sim.init_partition(namespace);
        sim.register_client_with_primary(&client);
        GAP_NS.store(namespace.inner(), Ordering::Relaxed);
        WITHHELD_OP.store(0, Ordering::Relaxed);

        produce(&mut sim, &client, namespace, WARMUP_SENDS, "warmup");
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);

        // Stepped one at a time and stopped on the step that buffered a drop:
        // the sweep drains the count on the following tick, and the whole point
        // of this run is to remove the namespace while the count is still on
        // the partition.
        let mut buffered = 0;
        'produce: for index in 0..GAP_SENDS {
            let message = client.send_messages(namespace, &[Bytes::from(format!("gap-{index}"))]);
            sim.submit_request(client.client_id(), 0, message.into_generic());
            for _ in 0..STEPS_PER_SEND {
                sim.step();
                buffered = buffered_gap_drops(&sim, LAGGING, namespace);
                if buffered > 0 {
                    break 'produce;
                }
            }
        }
        assert!(
            buffered > 0,
            "no prepare reached the lagging replica's gap check, so the removal \
             below would have nothing to lose"
        );
        let counted = gap_drops(&sim, LAGGING, namespace);
        let partition_stats = sim.replicas[LAGGING as usize]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("partition exists before ConfirmRemove")
            .stats
            .clone();
        let topic_stats = partition_stats.parent();
        let stream_stats = topic_stats.parent();
        assert!(partition_stats.messages_count_inconsistent() > 0);

        // The reconciler's teardown order: the tombstone lands first and the
        // disk delete runs before `ConfirmRemove`, so from here `get_mut_by_ns`
        // answers `None` and the sweep can never drain the count again.
        {
            let shard = sim.replicas[LAGGING as usize].partition_shard(namespace);
            shard.plane.partitions().tombstone(namespace);
            shard.enqueue_reconcile_op(shard::ReconcileOp::ConfirmRemove { namespace });
        }
        sim.step();

        let shard = sim.replicas[LAGGING as usize].partition_shard(namespace);
        assert!(
            shard.plane.partitions().get_by_ns(&namespace).is_none(),
            "ConfirmRemove must have dropped the partition, or this run proves nothing"
        );
        assert_eq!(
            shard.metrics().partition_prepare_gap_drops_value(),
            counted + buffered,
            "the {buffered} prepare(s) buffered on the partition went to the floor \
             with it; the drops are the only record those frames existed"
        );
        assert_eq!(partition_stats.messages_count_inconsistent(), 0);
        assert_eq!(partition_stats.size_bytes_inconsistent(), 0);
        assert_eq!(partition_stats.segments_count_inconsistent(), 0);
        assert_eq!(topic_stats.messages_count_inconsistent(), 0);
        assert_eq!(topic_stats.size_bytes_inconsistent(), 0);
        assert_eq!(topic_stats.segments_count_inconsistent(), 0);
        assert_eq!(stream_stats.messages_count_inconsistent(), 0);
        assert_eq!(stream_stats.size_bytes_inconsistent(), 0);
        assert_eq!(stream_stats.segments_count_inconsistent(), 0);
    }
}

#[cfg(test)]
mod metadata_repair_driver_tests {
    //! A backup that missed a committed metadata prepare recovers in Normal
    //! status, without waiting for a view change.
    //!
    //! The metadata twin of `partition_repair_driver_tests`, driven by the
    //! detector in `tick_metadata`: the same preflight `commit_max` advance
    //! starves the `Advanced`-gated arm in `on_commit`, and
    //! `retry_stalled_metadata_repair` re-drives only a session that already
    //! exists. Every fault here is keyed on `METADATA_GROUP`, since both
    //! planes share `Prepare` and `Commit` on the same links.

    use super::partition_repair_driver_tests::{
        cluster, is_commit_for, is_repair_done_for, is_request_prepares_for, prepare_for,
        sized_cluster,
    };
    use super::tests::NO_COMMIT_HEARTBEAT;
    use super::*;
    use consensus::{Sequencer, Status};
    use futures::FutureExt;
    use iggy_binary_protocol::RequestStateTransferHeader;
    use journal::Journal;
    use packet::Packet;
    use server_common::sharding::METADATA_GROUP;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Chain replication runs 0 -> 1 -> 2 and stops before the primary, so
    /// replica 2 is the only one whose losses cannot starve the group of
    /// quorum (see `partition_repair_driver_tests::LAGGING`).
    const LAGGING: u8 = 2;

    const CLIENT_ID: u128 = 1;

    /// Ops committed cleanly before the fault, so the gap opens above a
    /// committed prefix rather than at the group's first op.
    const WARMUP_SENDS: usize = 3;

    /// Ticks stepped after each stream creation, one round trip's worth.
    const STEPS_PER_SEND: usize = 12;

    /// Creations issued with the fault standing in the gap test. Long enough
    /// that prepares keep consuming the `commit_max` advance the heartbeat
    /// backstop needs; the debounce may elapse mid-produce, which the verdict
    /// tolerates (the withheld heartbeats mean only the tick driver can arm).
    const GAP_SENDS: usize = 12;

    /// Creations issued with the fault standing in the eviction and
    /// walk-starvation tests: few enough (under the debounce) that it fires
    /// only after the traffic stops, so the floor stamp or the starved walk
    /// edge is in place before the arm runs.
    const SHORT_GAP_SENDS: usize = 3;

    /// Quiet ticks for the repair stream to land, kept under
    /// `NORMAL_HEARTBEAT_TICKS` (500) so no election can be the healer.
    const QUIET_STEPS: usize = 160;

    /// Budget for the group to settle once the fault is lifted; the drain
    /// loop breaks on convergence.
    const DRAIN_STEPS: usize = 600;

    /// Quiet budget for the walk-starvation test: debounce, repair stream,
    /// then the drain, still under `NORMAL_HEARTBEAT_TICKS`.
    const STRAND_QUIET_STEPS: usize = 300;

    /// Stall interval for the rotation run, shortened from
    /// `partitions::REPAIR_RETRY_TICKS` so a whole spent budget plus the gap
    /// debounce (floored at `shard::REPAIR_GAP_DEBOUNCE_TICKS_MIN`) fits under
    /// `NORMAL_HEARTBEAT_TICKS`. At the production interval the run would only
    /// prove that an election heals the gap.
    const ROTATE_RETRY_TICKS: u32 = 10;

    /// Quiet budget for the rotation run: debounce, the spent budget, then the
    /// repair stream from the replica it rotates onto.
    const ROTATE_QUIET_STEPS: usize = 250;

    /// Ticks of healthy load in the no-false-positive test, several debounce
    /// intervals' worth so the driver gets many chances to arm.
    const LOAD_TICKS: usize = 4 * partitions::REPAIR_RETRY_TICKS as usize;

    /// Paced at a fraction of a round trip; faster submission only collects
    /// transient rejections once the prepare pipeline fills.
    const TICKS_PER_SEND: usize = 4;

    /// Ops the healthy run must have committed for its verdict to mean
    /// anything: enough to prove the group was live across several debounce
    /// intervals, not that it was saturated.
    const COMMITTED_MIN: u64 = 20;

    /// Seed of the one-op-at-a-time run.
    const QUIET_SEED: u64 = 0x7A11_0001;

    /// Creations in the one-op-at-a-time run.
    const QUIET_SENDS: usize = 40;

    /// Two round trips per creation, so each op commits and its eager `Commit`
    /// lands before the next prepare could carry the commit instead.
    const QUIET_STEPS_PER_SEND: usize = 2 * STEPS_PER_SEND;

    /// Defines this test's `withhold_one_prepare` chain hook over the static it
    /// names: swallow the FIRST metadata prepare, once, and record its op in
    /// `$withheld_op`.
    ///
    /// A macro for the same reason as the partition twin: link hooks are bare
    /// `fn` pointers, so the body cannot capture, and the statics must stay
    /// per-test or the siblings in this binary would share one fault.
    macro_rules! withhold_one_metadata_prepare {
        ($withheld_op:ident) => {
            fn withhold_one_prepare(packet: &Packet) -> bool {
                let Some(header) = prepare_for(packet, METADATA_GROUP) else {
                    return false;
                };
                $withheld_op
                    .compare_exchange(0, header.op, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            }
        };
    }

    /// Defines this test's primary -> backup hook: withhold this group's commit
    /// heartbeats, so the `Advanced` backstop can never run, and withhold
    /// retransmits of the op `$withheld_op` names.
    ///
    /// The retransmit half stands in for production behaviour rather than
    /// adding a fault: `consensus::retransmit_targets` skips an op that already
    /// reached quorum, and this op reaches quorum on 0 and 1 alone.
    macro_rules! starve_commit_edge {
        ($withheld_op:ident) => {
            fn starve_commit_edge(packet: &Packet) -> bool {
                if let Some(header) = prepare_for(packet, METADATA_GROUP) {
                    return header.op == $withheld_op.load(Ordering::Relaxed);
                }
                is_commit_for(packet, METADATA_GROUP)
            }
        };
    }

    /// `(status, view, commit_min, commit_max)` of one replica's metadata group.
    fn metadata_state(sim: &Simulator, replica: u8) -> (Status, u32, u64, u64) {
        let metadata = sim.replicas[replica as usize].shards[0].plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 owns metadata consensus");
        (
            consensus.status(),
            consensus.view(),
            consensus.commit_min(),
            consensus.commit_max(),
        )
    }

    #[allow(clippy::cast_possible_truncation)]
    fn journal_holds(sim: &Simulator, replica: u8, op: u64) -> bool {
        sim.replicas[replica as usize]
            .metadata_journal
            .header(op as usize)
            .is_some()
    }

    fn gap_drops(sim: &Simulator, replica: u8) -> u64 {
        sim.replicas[replica as usize].shards[0]
            .metrics()
            .metadata_prepare_gap_drops_value()
    }

    fn transfer_armed(sim: &Simulator, replica: u8) -> bool {
        let metadata = sim.replicas[replica as usize].shards[0].plane.metadata();
        metadata.consensus.as_ref().is_some_and(|consensus| {
            consensus.state_transfer_stage() != consensus::StateTransferStage::Idle
        })
    }

    /// Whether a packet is a state transfer request for `group`.
    fn is_request_state_transfer_for(packet: &Packet, group: u64) -> bool {
        if packet.message.header().command != Command::RequestStateTransfer {
            return false;
        }
        let header: &RequestStateTransferHeader = bytemuck::checked::from_bytes(
            &packet.message.as_slice()[..size_of::<RequestStateTransferHeader>()],
        );
        header.group == group
    }

    /// Submit `sends` stream creations to the primary, stepping between each.
    fn create_streams(sim: &mut Simulator, client: &SimClient, sends: usize, tag: &str) {
        for index in 0..sends {
            let msg = client.create_stream(&format!("{tag}-{index}"));
            sim.submit_request(client.client_id(), 0, msg.into_generic());
            for _ in 0..STEPS_PER_SEND {
                sim.step();
            }
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_an_acked_prepare_above_a_hole_when_restarting_and_losing_primary_should_keep_it() {
        let (mut sim, client) = cluster(0x5EED_4585);
        sim.register_client_with_primary(&client);
        for index in 0..3 {
            let reply = tests::submit_and_wait_for_reply(
                &mut sim,
                client.client_id(),
                0,
                client.create_stream(&format!("before-hole-{index}")),
            );
            assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        }
        // The chain reaches replica 2 last, and one hop can outlast a round trip.
        for _ in 0..DRAIN_STEPS {
            if sim.replicas[2].metadata_journal.last_op()
                == sim.replicas[0].metadata_journal.last_op()
            {
                break;
            }
            sim.step();
        }
        let floor = sim.replicas[2].metadata_journal.last_op().unwrap();
        assert_eq!(sim.replicas[0].metadata_journal.last_op(), Some(floor));
        sim.network.process_disable(ProcessId::Replica(LAGGING));
        for index in 0..6 {
            let reply = tests::submit_and_wait_for_reply(
                &mut sim,
                client.client_id(),
                0,
                client.create_stream(&format!("repair-tail-{index}")),
            );
            assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        }
        let repaired_head = sim.replicas[0].metadata_journal.last_op().unwrap();
        // Backward repair can persist later entries before it fills the first hole.
        for op in floor + 2..=repaired_head {
            let repaired = sim.replicas[0].metadata_journal.entry_sync(op).unwrap();
            sim.replicas[2]
                .metadata_journal
                .append(repaired)
                .now_or_never()
                .unwrap()
                .unwrap();
        }
        sim.replica_crash(1);
        sim.network.process_enable(ProcessId::Replica(LAGGING));
        sim.network
            .link_filter_mut(ProcessId::Replica(LAGGING), ProcessId::Replica(0))
            .remove(Command::RequestPrepares);
        sim.network
            .link_filter_mut(ProcessId::Replica(0), ProcessId::Replica(LAGGING))
            .remove(Command::Commit);
        let reply = tests::submit_and_wait_for_reply(
            &mut sim,
            client.client_id(),
            0,
            client.create_stream("acked-above-hole"),
        );
        assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        let acked_op = reply.header().op;
        assert_eq!(acked_op, repaired_head + 1);
        let acked_stream_id = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|streams| {
                streams
                    .items
                    .iter()
                    .find(|(_, stream)| &*stream.name == "acked-above-hole")
                    .map(|(id, _)| id)
                    .unwrap()
            });
        let checksum = sim.replicas[2]
            .metadata_journal
            .header(usize::try_from(acked_op).unwrap())
            .unwrap()
            .checksum;
        assert!(!journal_holds(&sim, LAGGING, floor + 1));
        sim.replica_crash(LAGGING);
        sim.replica_restart(LAGGING);
        assert!(
            journal_holds(&sim, LAGGING, acked_op),
            "restart discarded an acknowledged prepare"
        );
        let consensus = sim.replicas[2].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .unwrap();
        assert_eq!(consensus.sequencer().current_sequence(), acked_op);
        sim.replica_crash(0);
        sim.replica_restart(1);
        for _ in 0..5_000 {
            sim.step();
            if [1, 2]
                .iter()
                .all(|&node| metadata_state(&sim, node).2 >= acked_op)
            {
                break;
            }
        }
        for node in [1, 2] {
            assert!(metadata_state(&sim, node).2 >= acked_op);
            assert_eq!(
                sim.replicas[usize::from(node)]
                    .metadata_journal
                    .header(usize::try_from(acked_op).unwrap())
                    .unwrap()
                    .checksum,
                checksum
            );
            assert_eq!(
                sim.replicas[usize::from(node)].shards[0]
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .read(|streams| streams
                        .items
                        .iter()
                        .find(|(_, stream)| &*stream.name == "acked-above-hole")
                        .map(|(id, _)| id)),
                Some(acked_stream_id)
            );
        }
        let primary = [1, 2]
            .into_iter()
            .find(|&node| {
                sim.replicas[usize::from(node)].shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .unwrap()
                    .is_primary()
            })
            .unwrap();
        let reply = tests::submit_and_wait_for_reply(
            &mut sim,
            client.client_id(),
            primary,
            client.create_stream("after-hole-recovery"),
        );
        assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        assert!(reply.header().op > acked_op);
        let next_stream_id = sim.replicas[usize::from(primary)].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|streams| {
                streams
                    .items
                    .iter()
                    .find(|(_, stream)| &*stream.name == "after-hole-recovery")
                    .map(|(id, _)| id)
                    .unwrap()
            });
        assert_ne!(next_stream_id, acked_stream_id);
    }

    #[test]
    fn given_a_backup_that_dropped_a_committed_metadata_prepare_when_heartbeat_advances_are_starved_should_repair_in_normal_status()
     {
        // Statics, not captures: the link hooks are bare `fn` pointers,
        // declared inside the test so parallel siblings cannot share them.
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);
        starve_commit_edge!(WITHHELD_OP);

        let (mut sim, client) = cluster(0x5EED_0240);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");
        let (_, _, warm_commit_min, _) = metadata_state(&sim, LAGGING);
        assert!(
            warm_commit_min > 0,
            "the lagging replica committed nothing before the fault, so the gap \
             below would open at the group's first op"
        );

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);

        create_streams(&mut sim, &client, GAP_SENDS, "md-gap");

        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );
        assert!(
            gap_drops(&sim, LAGGING) > 0,
            "the lagging replica never reached its backup gap check, so the \
             prepares after the withheld op were not dropped as a gap"
        );

        for _ in 0..QUIET_STEPS {
            sim.step();
        }

        // Judged with the blockade still standing: no commit heartbeat for
        // this group has reached the replica since the gap opened, so only
        // the tick driver can have armed the repair.
        let (status, view, commit_min, _) = metadata_state(&sim, LAGGING);
        assert_eq!(
            view, 0,
            "a view change healed the gap instead of the repair driver; the test \
             proves nothing about normal status"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert!(
            journal_holds(&sim, LAGGING, withheld),
            "op {withheld} was never repaired back into the lagging replica's WAL"
        );
        assert!(
            commit_min >= withheld,
            "the commit walk never crossed the repaired hole: stopped at \
             {commit_min}, the withheld op is {withheld}"
        );

        // Lift the blockade and let the group settle; the tail above the
        // repaired window waits on the heartbeats the fault withheld.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) = None;
        for _ in 0..DRAIN_STEPS {
            sim.step();
            let (_, _, commit_min, commit_max) = metadata_state(&sim, LAGGING);
            if commit_min == commit_max {
                break;
            }
        }
        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
        assert_eq!((status, view), (Status::Normal, 0));
        assert_eq!(
            commit_min, commit_max,
            "the lagging replica is still gap-stopped: committed through \
             {commit_max} but walkable only to {commit_min}"
        );
    }

    #[test]
    fn given_a_metadata_repair_armed_by_the_tick_driver_when_the_floor_is_evicted_should_convert_to_state_transfer()
     {
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);
        starve_commit_edge!(WITHHELD_OP);

        let (mut sim, client) = cluster(0x5EED_0241);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");
        assert_ne!(
            WITHHELD_OP.load(Ordering::Relaxed),
            0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );

        // Move the primary's retention floor past the whole gap window before
        // the debounce can arm (`SHORT_GAP_SENDS`): the serve path reads only
        // the snapshot watermark, so the request is answered `RangeEvicted`
        // (see `stamp_metadata_snapshot`).
        let primary_commit_min = metadata_state(&sim, 0).2;
        sim.stamp_metadata_snapshot(0, primary_commit_min);

        for _ in 0..QUIET_STEPS {
            sim.step();
            if transfer_armed(&sim, LAGGING) {
                break;
            }
        }

        let (status, view, ..) = metadata_state(&sim, LAGGING);
        assert_eq!(
            view, 0,
            "a view change armed the recovery instead of the tick-armed repair session"
        );
        assert!(
            transfer_armed(&sim, LAGGING),
            "the tick-armed repair session hit an evicted floor but never converted \
             to a state transfer (status {status:?})"
        );
    }

    #[test]
    fn given_a_backup_holding_resident_committed_metadata_ops_when_every_walk_edge_is_starved_should_drain_in_normal_status()
     {
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        static WITHHELD_DONES: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);

        /// Primary -> 2: withhold every direct prepare (live ones ride the
        /// chain, so this starves only retransmit heals), the group's commit
        /// heartbeats, and its repair terminators. The repaired ops themselves
        /// pass, so the window lands resident while the `RepairDone` that would
        /// run the walk never fires.
        fn starve_walk_edges(packet: &Packet) -> bool {
            if prepare_for(packet, METADATA_GROUP).is_some() {
                return true;
            }
            if is_repair_done_for(packet, METADATA_GROUP) {
                WITHHELD_DONES.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            is_commit_for(packet, METADATA_GROUP)
        }

        let (mut sim, client) = cluster(0x5EED_0242);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        WITHHELD_DONES.store(0, Ordering::Relaxed);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");
        let (_, _, warm_commit_min, _) = metadata_state(&sim, LAGGING);
        assert!(
            warm_commit_min > 0,
            "the lagging replica committed nothing before the fault, so the gap \
             below would open at the group's first op"
        );

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_walk_edges);

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-strand");

        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );

        for _ in 0..STRAND_QUIET_STEPS {
            sim.step();
            let (_, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
            if view != 0 || (commit_min >= withheld && commit_min == commit_max) {
                break;
            }
        }

        assert!(
            WITHHELD_DONES.load(Ordering::Relaxed) > 0,
            "no repair terminator was withheld, so the walk was never starved and \
             a green run would not prove the tick backstop"
        );
        assert!(
            journal_holds(&sim, LAGGING, withheld),
            "op {withheld} was never repaired back into the lagging replica's WAL"
        );
        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
        assert_eq!(
            view, 0,
            "a view change drained the walk instead of the tick backstop; the test \
             proves nothing about normal status"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        for op in commit_min + 1..=commit_max {
            assert!(
                journal_holds(&sim, LAGGING, op),
                "op {op} is not resident, so this run stranded on a repair gap, \
                 not a parked walk"
            );
        }
        assert_eq!(
            commit_min, commit_max,
            "the walk never resumed over resident committed ops: walkable to \
             {commit_min}, committed through {commit_max}, every op between resident"
        );
    }

    #[test]
    fn given_a_resident_header_with_no_body_when_the_walk_stops_on_it_should_drop_it_for_repair() {
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);
        starve_commit_edge!(WITHHELD_OP);

        let (mut sim, client) = cluster(0x5EED_0245);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        sim.replicas[LAGGING as usize].shards[0].set_repair_retry_ticks(ROTATE_RETRY_TICKS);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");
        assert_ne!(
            WITHHELD_OP.load(Ordering::Relaxed),
            0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );

        // Corrupt the lowest op repair has put back above the commit point,
        // before the walk has consumed it. Repair can land a later op first,
        // so the op the walk reads next may still be a hole. Injected here
        // rather than by dropping packets: the header and the body land in the
        // same append, so no link fault produces this state.
        let mut doomed = 0;
        for _ in 0..ROTATE_QUIET_STEPS {
            sim.step();
            let (_, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
            if view != 0 {
                break;
            }
            if let Some(resident) =
                (commit_min + 1..=commit_max).find(|&op| journal_holds(&sim, LAGGING, op))
            {
                assert!(
                    sim.replicas[LAGGING as usize]
                        .metadata_journal
                        .forget_body(resident),
                    "op {resident} had no body to forget"
                );
                doomed = resident;
                break;
            }
        }
        assert_ne!(
            doomed, 0,
            "the repair window never landed resident above the commit point, so \
             there was no walkable op to corrupt"
        );
        assert!(
            journal_holds(&sim, LAGGING, doomed),
            "op {doomed}'s header must stay resident, or the walk would read this \
             as an ordinary hole and repair would refill it unaided"
        );

        for _ in 0..ROTATE_QUIET_STEPS {
            sim.step();
            let (_, view, commit_min, _) = metadata_state(&sim, LAGGING);
            if view != 0 || commit_min >= doomed {
                break;
            }
        }

        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
        assert_eq!(
            view, 0,
            "a view change healed the log instead of the walk; the test proves \
             nothing about the unwalkable entry"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert!(
            commit_min >= doomed,
            "the walk never crossed op {doomed}: the repair ingest skips an op \
             whose header is resident and `append` refuses the slot under it, so \
             the header has to be dropped first. Stopped at {commit_min} of \
             {commit_max}"
        );
    }

    #[test]
    fn given_a_repair_peer_that_never_answers_when_the_budget_is_spent_should_re_arm_from_another_replica()
     {
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        static ASKED_PRIMARY: AtomicU64 = AtomicU64::new(0);
        static ASKED_SUCCESSOR: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);

        /// The peer `gap_repair_peer` names for a gap-stopped backup is the
        /// primary, so the session can only heal by rotating off it.
        fn blackhole(_packet: &Packet) -> bool {
            true
        }

        /// Observers, not faults: every packet passes.
        fn count_requests_to_primary(packet: &Packet) -> bool {
            if is_request_prepares_for(packet, METADATA_GROUP) {
                ASKED_PRIMARY.fetch_add(1, Ordering::Relaxed);
            }
            false
        }
        fn count_requests_to_successor(packet: &Packet) -> bool {
            if is_request_prepares_for(packet, METADATA_GROUP) {
                ASKED_SUCCESSOR.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        let (mut sim, client) = cluster(0x5EED_0244);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        ASKED_PRIMARY.store(0, Ordering::Relaxed);
        ASKED_SUCCESSOR.store(0, Ordering::Relaxed);
        sim.replicas[LAGGING as usize].shards[0].set_repair_retry_ticks(ROTATE_RETRY_TICKS);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");
        let (_, _, warm_commit_min, _) = metadata_state(&sim, LAGGING);
        assert!(
            warm_commit_min > 0,
            "the lagging replica committed nothing before the fault, so the gap \
             below would open at the group's first op"
        );

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        // Everything, not just this group's commit edge: the primary must be
        // unable to answer the repair it is about to be asked for, while the
        // chain (0 -> 1 -> 2) keeps carrying the prepares that advance
        // `commit_max` past the hole.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(blackhole);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(LAGGING), ProcessId::Replica(0)) =
            Some(count_requests_to_primary);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(LAGGING), ProcessId::Replica(1)) =
            Some(count_requests_to_successor);

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");
        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );

        for _ in 0..ROTATE_QUIET_STEPS {
            sim.step();
            let (_, view, commit_min, _) = metadata_state(&sim, LAGGING);
            if view != 0 || commit_min >= withheld {
                break;
            }
        }

        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
        assert_eq!(
            view, 0,
            "a view change healed the gap instead of the rotation; the test proves \
             nothing about the stall budget"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert!(
            ASKED_PRIMARY.load(Ordering::Relaxed) > 1,
            "the session was never re-requested from the silent primary, so the \
             stall budget (`partitions::REPAIR_MAX_STALL_RETRIES`) was not spent \
             and any rotation below came from somewhere else"
        );
        assert!(
            ASKED_SUCCESSOR.load(Ordering::Relaxed) > 0,
            "the budget ran out against a peer that cannot answer and the session \
             was never re-armed anywhere else; nothing re-drives it, so the plane \
             stays gap-stopped at {commit_min} of {commit_max}"
        );
        assert!(
            journal_holds(&sim, LAGGING, withheld),
            "op {withheld} was never repaired back into the lagging replica's WAL"
        );
        assert!(
            commit_min >= withheld,
            "the commit walk never crossed the repaired hole: stopped at \
             {commit_min}, the withheld op is {withheld}"
        );
    }

    /// A repair peer with a hole at the requested floor skips it and answers
    /// `RangeEvicted` above the floor, which reads as a compacted prefix and
    /// converts to a state transfer against that peer. A backup refuses every
    /// transfer (only a caught-up primary serves one), and the refusal re-arms
    /// repair against the same peer, which answers the same way. Each step is a
    /// direct reply, so no stall clock fires and no budget is charged: the
    /// replica never leaves that peer, even once another one can serve the hole.
    #[test]
    fn given_a_repair_peer_without_snapshot_when_it_answers_range_evicted_should_rotate_instead_of_transferring()
     {
        /// Where the session rotates off the primary (`next_transfer_peer`), and
        /// the replica given the hole.
        const SUCCESSOR: u8 = 1;

        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        static TRANSFER_REQUESTS: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);

        fn blackhole(_packet: &Packet) -> bool {
            true
        }

        /// Observer, not a fault: every packet passes.
        fn count_transfer_requests(packet: &Packet) -> bool {
            if is_request_state_transfer_for(packet, METADATA_GROUP) {
                TRANSFER_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        let (mut sim, client) = cluster(0x5EED_0246);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        TRANSFER_REQUESTS.store(0, Ordering::Relaxed);
        sim.replicas[LAGGING as usize].shards[0].set_repair_retry_ticks(ROTATE_RETRY_TICKS);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(SUCCESSOR), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        // Silent until the session has spent its budget on it and rotated.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(blackhole);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(LAGGING), ProcessId::Replica(SUCCESSOR)) =
            Some(count_transfer_requests);

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");
        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );

        // The hole a lost repair frame leaves (see `SimJournal::forget_op`). The
        // successor already walked past the op, so nothing ever refills it.
        let (_, _, successor_commit_min, _) = metadata_state(&sim, SUCCESSOR);
        assert!(
            successor_commit_min > withheld,
            "the successor has not walked past op {withheld} (commit_min \
             {successor_commit_min}), so it would repair the hole itself"
        );
        assert!(
            sim.replicas[SUCCESSOR as usize]
                .metadata_journal
                .forget_op(withheld),
            "the successor never journaled op {withheld}"
        );

        for _ in 0..ROTATE_QUIET_STEPS {
            sim.step();
            if TRANSFER_REQUESTS.load(Ordering::Relaxed) > 0 {
                break;
            }
        }
        assert_eq!(
            TRANSFER_REQUESTS.load(Ordering::Relaxed),
            1,
            "the session never rotated onto the successor and converted its \
             eviction notice into a state transfer"
        );

        // The primary can serve the hole from here on, and the stall clock is back
        // at the production interval, far above a round trip: a request/refusal
        // loop on the successor shows as more transfer requests long before the
        // stall sweep rotates the session off it.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) = None;
        sim.replicas[LAGGING as usize].shards[0]
            .set_repair_retry_ticks(partitions::REPAIR_RETRY_TICKS);

        for _ in 0..ROTATE_QUIET_STEPS {
            sim.step();
            let (_, view, commit_min, _) = metadata_state(&sim, LAGGING);
            if view != 0 || commit_min >= withheld {
                break;
            }
        }

        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
        assert_eq!(
            view, 0,
            "a view change healed the gap instead of the rotation; the test proves \
             nothing about the refused transfer"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert_eq!(
            TRANSFER_REQUESTS.load(Ordering::Relaxed),
            1,
            "the successor refused the transfer, and the repair re-armed against it \
             converted the same eviction notice into another request: a \
             request/refusal loop between two pumps"
        );
        assert!(
            !transfer_armed(&sim, LAGGING),
            "the replica is still waiting on a state transfer"
        );
        assert!(
            journal_holds(&sim, LAGGING, withheld),
            "op {withheld} was never repaired back into the lagging replica's WAL"
        );
        assert!(
            commit_min >= withheld,
            "the commit walk never crossed the hole: stopped at {commit_min} of \
             {commit_max}, the withheld op is {withheld}"
        );
    }

    /// The primary is the peer `gap_repair_peer` names, so when it holds the hole
    /// and refuses the transfer, the refused session's `RangeEvicted` takes the
    /// unspent-budget branch: drop the session and let the level trigger re-arm,
    /// which picks the primary again with a fresh session. Only the round each
    /// trip charges moves the replica off it, onto the backup that holds the op.
    #[test]
    fn given_a_primary_without_snapshot_holding_a_hole_when_it_refuses_the_transfer_should_rotate_after_the_budget()
     {
        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        static TRANSFER_REQUESTS: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);
        starve_commit_edge!(WITHHELD_OP);

        /// Observer, not a fault: every packet passes.
        fn count_transfer_requests(packet: &Packet) -> bool {
            if is_request_state_transfer_for(packet, METADATA_GROUP) {
                TRANSFER_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        let (mut sim, client) = cluster(0x5EED_0248);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        TRANSFER_REQUESTS.store(0, Ordering::Relaxed);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        // Starved so no heartbeat arms the repair before the primary's hole exists.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(LAGGING), ProcessId::Replica(0)) =
            Some(count_transfer_requests);

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");
        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );
        let (_, _, primary_commit_min, _) = metadata_state(&sim, 0);
        assert!(
            primary_commit_min > withheld,
            "the primary has not walked past op {withheld} (commit_min \
             {primary_commit_min}), so it would repair the hole itself"
        );
        assert!(
            sim.replicas[0].metadata_journal.forget_op(withheld),
            "the primary never journaled op {withheld}"
        );
        // Heartbeats again, so no election heals the gap first.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) = None;

        for _ in 0..DRAIN_STEPS {
            sim.step();
            let (_, view, commit_min, _) = metadata_state(&sim, LAGGING);
            if view != 0 || commit_min >= withheld {
                break;
            }
        }

        let requests = TRANSFER_REQUESTS.load(Ordering::Relaxed);
        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
        assert_eq!(
            view, 0,
            "a view change healed the gap instead of the rotation; the test proves \
             nothing about the refused transfer"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert!(
            requests > 0,
            "the primary's eviction notice never converted to a transfer, so the \
             refusal was never exercised"
        );
        assert!(
            requests <= u64::from(partitions::REPAIR_MAX_STALL_RETRIES) + 1,
            "the primary refused {requests} transfers: each refusal re-armed repair \
             against it without charging a round, so the replica never left it"
        );
        assert!(
            journal_holds(&sim, LAGGING, withheld),
            "op {withheld} was never repaired back into the lagging replica's WAL"
        );
        assert!(
            commit_min >= withheld,
            "the commit walk never crossed the hole: stopped at {commit_min} of \
             {commit_max}, the withheld op is {withheld}"
        );
    }

    /// Every peer compacted past the gap and none can serve a transfer: the
    /// primary has no snapshot coordinator, and a backup is not a caught-up
    /// primary. Each visit converts the eviction notice once, and the refused
    /// session's `RangeEvicted` charges a round. Once the budget is spent, a
    /// rotation straight off that reply cycles the ring at network rate, so the
    /// session must first sit out a stall round, as it does against a silent peer.
    #[test]
    fn given_no_peer_able_to_serve_when_the_repair_budget_is_spent_should_pace_the_rotation() {
        /// Ticks counted once the budget is spent, several retry intervals long.
        const PACE_WINDOW_TICKS: u32 = 5 * partitions::REPAIR_RETRY_TICKS;
        /// Room for the debounce before each round the budget charges.
        const SPEND_STEPS: usize = 1000;

        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        static TRANSFER_REQUESTS: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);
        starve_commit_edge!(WITHHELD_OP);

        /// Observer, not a fault: every packet passes.
        fn count_transfer_requests(packet: &Packet) -> bool {
            if is_request_state_transfer_for(packet, METADATA_GROUP) {
                TRANSFER_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        let (mut sim, client) = cluster(0x5EED_0249);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        TRANSFER_REQUESTS.store(0, Ordering::Relaxed);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");

        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(1), ProcessId::Replica(LAGGING)) =
            Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) =
            Some(starve_commit_edge);
        for peer in [0, 1] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(LAGGING), ProcessId::Replica(peer)) =
                Some(count_transfer_requests);
        }

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");
        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );
        for peer in [0, 1] {
            let (_, _, peer_commit_min, _) = metadata_state(&sim, peer);
            assert!(
                peer_commit_min > withheld,
                "replica {peer} has not walked past op {withheld} (commit_min \
                 {peer_commit_min}), so the stamp would not evict it"
            );
            sim.stamp_metadata_snapshot(usize::from(peer), peer_commit_min);
        }
        // Heartbeats again, so no election ends the run.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING)) = None;

        let spent_after = u64::from(partitions::REPAIR_MAX_STALL_RETRIES) + 1;
        for _ in 0..SPEND_STEPS {
            sim.step();
            if TRANSFER_REQUESTS.load(Ordering::Relaxed) >= spent_after {
                break;
            }
        }
        let at_spend = TRANSFER_REQUESTS.load(Ordering::Relaxed);
        assert!(
            at_spend >= spent_after,
            "the replica converted {at_spend} eviction notices, too few to spend its \
             repair budget, so the rotation was never exercised"
        );
        for _ in 0..PACE_WINDOW_TICKS {
            sim.step();
        }

        let in_window = TRANSFER_REQUESTS.load(Ordering::Relaxed) - at_spend;
        let paced_max = u64::from(PACE_WINDOW_TICKS / partitions::REPAIR_RETRY_TICKS) + 1;
        let (status, view, commit_min, _) = metadata_state(&sim, LAGGING);
        assert_eq!(
            (status, view),
            (Status::Normal, 0),
            "an election interrupted the run"
        );
        assert!(
            commit_min < withheld,
            "a peer served op {withheld}, so nothing was left to rotate over"
        );
        assert!(
            in_window > 0,
            "the replica stopped asking once its budget was spent, though no peer \
             has served the gap"
        );
        assert!(
            in_window <= paced_max,
            "{in_window} transfer requests in {PACE_WINDOW_TICKS} ticks after the \
             budget was spent, more than one per retry interval: the rotation runs \
             at network rate"
        );
    }

    /// The rotation used to step back to the primary after each backup, so in a
    /// group of five a backup alternated between the primary and the replica
    /// after it. Neither of those holds the withheld op here, nor does replica 3:
    /// only replica 2, two after the primary, can serve it.
    #[test]
    fn given_five_replicas_when_only_the_second_after_the_primary_holds_the_op_should_repair_from_it()
     {
        const REPLICAS: u8 = 5;
        /// Last in the chain 0 -> 1 -> 2 -> 3 -> 4, so its losses cannot starve
        /// the group of quorum.
        const LAGGING_OF_FIVE: u8 = 4;
        /// Every replica but `primary + 2` forgets the withheld op.
        const HOLED: [u8; 3] = [0, 1, 3];
        /// The debounce before each round charged against the primary, then two
        /// held rotations and the repair stream.
        const FIVE_QUIET_STEPS: usize = 1000;

        static WITHHELD_OP: AtomicU64 = AtomicU64::new(0);
        withhold_one_metadata_prepare!(WITHHELD_OP);
        starve_commit_edge!(WITHHELD_OP);

        let (mut sim, client) = sized_cluster(0x5EED_024A, REPLICAS, 1);
        sim.register_client_with_primary(&client);
        WITHHELD_OP.store(0, Ordering::Relaxed);
        sim.replicas[usize::from(LAGGING_OF_FIVE)].shards[0]
            .set_repair_retry_ticks(ROTATE_RETRY_TICKS);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");

        *sim.network.link_drop_packet_fn(
            ProcessId::Replica(LAGGING_OF_FIVE - 1),
            ProcessId::Replica(LAGGING_OF_FIVE),
        ) = Some(withhold_one_prepare);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING_OF_FIVE)) =
            Some(starve_commit_edge);

        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");
        let withheld = WITHHELD_OP.load(Ordering::Relaxed);
        assert_ne!(
            withheld, 0,
            "no metadata prepare crossed the chain link, so the fault never armed"
        );
        // A peer can learn the last gap commit from the primary's next `Commit`,
        // after the sends end. Waiting one more send interval keeps the gap phase
        // under the repair debounce, so the lagging replica cannot fetch the op.
        for _ in 0..STEPS_PER_SEND {
            if HOLED
                .iter()
                .all(|&peer| metadata_state(&sim, peer).2 > withheld)
            {
                break;
            }
            sim.step();
        }
        for peer in HOLED {
            let (_, _, peer_commit_min, _) = metadata_state(&sim, peer);
            assert!(
                peer_commit_min > withheld,
                "replica {peer} has not walked past op {withheld} (commit_min \
                 {peer_commit_min}), so it would repair the hole itself"
            );
            assert!(
                sim.replicas[usize::from(peer)]
                    .metadata_journal
                    .forget_op(withheld),
                "replica {peer} never journaled op {withheld}"
            );
        }
        // Heartbeats again, so no election heals the gap first.
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(0), ProcessId::Replica(LAGGING_OF_FIVE)) = None;

        for _ in 0..FIVE_QUIET_STEPS {
            sim.step();
            let (_, view, commit_min, _) = metadata_state(&sim, LAGGING_OF_FIVE);
            if view != 0 || commit_min >= withheld {
                break;
            }
        }

        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING_OF_FIVE);
        assert_eq!(
            view, 0,
            "a view change healed the gap instead of the rotation; the test proves \
             nothing about the ring"
        );
        assert_eq!(status, Status::Normal, "the replica left Normal status");
        assert!(
            journal_holds(&sim, LAGGING_OF_FIVE, withheld),
            "op {withheld} was never repaired: the rotation never reached replica 2, \
             the only one that holds it"
        );
        assert!(
            commit_min >= withheld,
            "the commit walk never crossed the hole: stopped at {commit_min} of \
             {commit_max}, the withheld op is {withheld}"
        );
    }

    /// A rejoining replica awaits a state transfer from the view's primary, as
    /// production's `JoinMode::ProbeAsBackup { await_state_transfer: true }`
    /// arms it; the harness restart does not, so this test does. A primary whose
    /// checkpoint never finishes refuses each descriptor request as transiently
    /// busy. If that refusal refills the transfer's stall budget, the replica
    /// waits in `AwaitingTarget` for good, with its commit walk and journal
    /// repair both deferred, though the primary's WAL still covers its gap.
    #[test]
    fn given_a_transfer_peer_that_stays_checkpointing_when_the_budget_is_spent_should_fall_back_to_journal_repair()
     {
        static TRANSFER_REQUESTS: AtomicU64 = AtomicU64::new(0);

        /// Observer, not a fault: every packet passes.
        fn count_transfer_requests(packet: &Packet) -> bool {
            if is_request_state_transfer_for(packet, METADATA_GROUP) {
                TRANSFER_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        let (mut sim, client) = cluster(0x5EED_0247);
        sim.register_client_with_primary(&client);
        TRANSFER_REQUESTS.store(0, Ordering::Relaxed);

        create_streams(&mut sim, &client, WARMUP_SENDS, "md-warm");
        sim.replica_crash(LAGGING);
        create_streams(&mut sim, &client, SHORT_GAP_SENDS, "md-gap");

        let primary = Rc::clone(&sim.replicas[0].shards[0]);
        let _checkpoint = primary
            .plane
            .metadata()
            .hold_checkpoint_gate()
            .expect("nothing checkpoints without a snapshot coordinator");

        sim.replica_restart(LAGGING);
        sim.metadata_consensus(usize::from(LAGGING))
            .expect("shard 0 owns metadata consensus")
            .begin_state_transfer_await();
        // The transfer stall shares this interval; shortened as in the rotation run.
        sim.replicas[LAGGING as usize].shards[0].set_repair_retry_ticks(ROTATE_RETRY_TICKS);
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(LAGGING), ProcessId::Replica(0)) =
            Some(count_transfer_requests);

        let (_, _, primary_commit_min, _) = metadata_state(&sim, 0);
        for _ in 0..ROTATE_QUIET_STEPS {
            sim.step();
            let (_, _, commit_min, _) = metadata_state(&sim, LAGGING);
            if !transfer_armed(&sim, LAGGING) && commit_min >= primary_commit_min {
                break;
            }
        }

        let requests = TRANSFER_REQUESTS.load(Ordering::Relaxed);
        let (status, view, commit_min, commit_max) = metadata_state(&sim, LAGGING);
        assert!(
            requests > 0,
            "the rejoining replica never asked the primary for a transfer, so the \
             refusal was never exercised"
        );
        assert!(
            requests <= u64::from(consensus::STATE_TRANSFER_MAX_STALL_RETRIES) + 1,
            "the primary refused {requests} descriptor requests as busy and every \
             refusal refilled the stall budget, so the replica never gave up on it"
        );
        assert!(
            !transfer_armed(&sim, LAGGING),
            "the replica is still awaiting a transfer the primary keeps refusing"
        );
        assert_eq!((status, view), (Status::Normal, 0));
        assert!(
            commit_min >= primary_commit_min,
            "journal repair never caught the replica up: commit_min {commit_min} of \
             {commit_max}, the primary committed through {primary_commit_min}"
        );
    }

    /// Regression canary, expected green even without the tick driver: a
    /// healthy metadata backup walks at every accepted prepare's tail
    /// (`on_replicate`), so per-tick lag never survives to quiescence and the
    /// journal-hole half of the predicate is pinned by `gap_detector_tests`
    /// instead. What this run pins is that the driver stays silent under
    /// sustained pipelined load.
    #[test]
    fn given_healthy_metadata_traffic_when_no_gap_exists_should_not_arm_repair() {
        static REPAIR_REQUESTS: AtomicU64 = AtomicU64::new(0);

        /// Observer, not a fault: counts this group's repair requests and
        /// passes every packet through.
        fn count_repair_requests(packet: &Packet) -> bool {
            if is_request_prepares_for(packet, METADATA_GROUP) {
                REPAIR_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        // TWO replicas, as in the partition twin: quorum spans both, so no op
        // can commit while the backup misses it, and every reordering-induced
        // gap blocks quorum until retransmit refills it.
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let replica_count: u8 = 2;
        let mut sim = Simulator::new(
            replica_count as usize,
            std::iter::once(CLIENT_ID),
            packet::PacketSimulatorOptions {
                node_count: replica_count,
                client_count: 1,
                seed: 0x5EED_0243,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let client = SimClient::new(CLIENT_ID);
        sim.register_client_with_primary(&client);
        REPAIR_REQUESTS.store(0, Ordering::Relaxed);

        for (from, to) in [(0u8, 1u8), (1, 0)] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) =
                Some(count_repair_requests);
        }

        // Sustained, not bursty, and the run asserts the load went somewhere,
        // so a green result cannot come from a workload that never loaded the
        // group.
        let mut lag_run = 0u32;
        let mut longest_lag_run = 0u32;
        let sample = |sim: &Simulator, lag_run: &mut u32, longest: &mut u32| {
            let (_, _, commit_min, commit_max) = metadata_state(sim, 1);
            if commit_min < commit_max {
                *lag_run += 1;
                *longest = (*longest).max(*lag_run);
            } else {
                *lag_run = 0;
            }
        };
        for tick in 0..LOAD_TICKS {
            if tick % TICKS_PER_SEND == 0 {
                let msg = client.create_stream(&format!("healthy-{tick}"));
                sim.submit_request(client.client_id(), 0, msg.into_generic());
            }
            sim.step();
            sample(&sim, &mut lag_run, &mut longest_lag_run);
        }
        for _ in 0..QUIET_STEPS {
            sim.step();
            sample(&sim, &mut lag_run, &mut longest_lag_run);
        }

        let committed = metadata_state(&sim, 1).2;
        let sends = LOAD_TICKS / TICKS_PER_SEND;
        assert!(
            committed >= COMMITTED_MIN,
            "the backup committed only {committed} ops across {sends} sends, so the \
             driver was never ticked over a loaded group"
        );
        // Asserted, not merely recorded: on this plane the tail walk in
        // `on_replicate` leaves a healthy backup caught up at every quiescence
        // point, so any lag at all is a change in that behaviour rather than
        // ordinary pipelining. The predicate itself is pinned by
        // `gap_detector_tests`; what this holds is the premise the repair check
        // below rests on.
        assert_eq!(
            longest_lag_run, 0,
            "healthy two-replica metadata traffic left the backup lagging for \
             {longest_lag_run} consecutive ticks; the run below then proves nothing \
             about false positives, and this test should sample residency the way \
             the partition twin's walk-starvation run does"
        );
        for replica in 0..replica_count {
            let (status, view, commit_min, commit_max) = metadata_state(&sim, replica);
            assert_eq!(
                (status, view),
                (Status::Normal, 0),
                "replica {replica} left view 0 / Normal, so a view change could \
                 account for repair traffic"
            );
            if commit_min < commit_max {
                assert!(
                    journal_holds(&sim, replica, commit_min + 1),
                    "replica {replica} lags at {commit_min} of {commit_max} with op \
                     {} missing, so a no-loss run produced a real hole",
                    commit_min + 1
                );
            }
        }
        assert_eq!(
            REPAIR_REQUESTS.load(Ordering::Relaxed),
            0,
            "the tick driver requested repair on a healthy group; its gap predicate \
             is reading ordinary commit lag as a journal hole"
        );
    }

    /// A backup outside the replication quorum gets each prepare from the chain
    /// after the primary committed it. An eager `Commit` that overtakes the
    /// prepare makes the backup repair an op that is still on its way.
    #[test]
    fn given_one_metadata_op_at_a_time_when_each_commits_should_not_repair_a_backup_outside_the_quorum()
     {
        static REPAIR_REQUESTS: AtomicU64 = AtomicU64::new(0);

        /// Observer, not a fault: counts this group's repair requests and
        /// passes every packet through.
        fn count_repair_requests(packet: &Packet) -> bool {
            if is_request_prepares_for(packet, METADATA_GROUP) {
                REPAIR_REQUESTS.fetch_add(1, Ordering::Relaxed);
            }
            false
        }

        // `(replicas, repair requests, ops the primary applied, whether every
        // replica applied them in view 0)` per run.
        let runs: Vec<(u8, u64, u64, bool)> = [3, 5]
            .into_iter()
            .map(|replica_count| {
                let (mut sim, client) = sized_cluster(QUIET_SEED, replica_count, 1);
                sim.register_client_with_primary(&client);
                // The heartbeat still reaches every backup on its own schedule, so
                // it can overtake a prepare too. Without it the backups learn each
                // commit from the eager `Commit` alone.
                for replica in 0..replica_count {
                    sim.metadata_consensus(usize::from(replica))
                        .expect("every replica runs the metadata plane")
                        .set_commit_message_ticks(NO_COMMIT_HEARTBEAT);
                }
                REPAIR_REQUESTS.store(0, Ordering::Relaxed);
                for from in 0..replica_count {
                    for to in (0..replica_count).filter(|to| *to != from) {
                        *sim.network.link_drop_packet_fn(
                            ProcessId::Replica(from),
                            ProcessId::Replica(to),
                        ) = Some(count_repair_requests);
                    }
                }

                for index in 0..QUIET_SENDS {
                    let msg = client.create_stream(&format!("quiet-{index}"));
                    sim.submit_request(client.client_id(), 0, msg.into_generic());
                    for _ in 0..QUIET_STEPS_PER_SEND {
                        sim.step();
                    }
                }

                let (_, _, primary_commit, _) = metadata_state(&sim, 0);
                let settled = |sim: &Simulator| {
                    (0..replica_count).all(|replica| {
                        metadata_state(sim, replica)
                            == (Status::Normal, 0, primary_commit, primary_commit)
                    })
                };
                for _ in 0..DRAIN_STEPS {
                    if settled(&sim) {
                        break;
                    }
                    sim.step();
                }
                (
                    replica_count,
                    REPAIR_REQUESTS.load(Ordering::Relaxed),
                    primary_commit,
                    settled(&sim),
                )
            })
            .collect();
        assert_eq!(
            runs.iter()
                .map(|&(replica_count, repairs, ..)| (replica_count, repairs))
                .collect::<Vec<_>>(),
            [(3, 0), (5, 0)],
            "(replicas, metadata repair requests): an eager Commit overtook the \
             prepare the chain was still forwarding"
        );
        for (replica_count, _, primary_commit, settled) in runs {
            assert!(
                primary_commit >= COMMITTED_MIN,
                "{replica_count} replicas committed only {primary_commit} metadata ops, \
                 too few for a clean run to mean anything"
            );
            assert!(
                settled,
                "{replica_count} replicas did not all apply op {primary_commit} in view 0, \
                 so a view change or a lost op could account for repair traffic"
            );
        }
    }
}

#[cfg(test)]
mod metadata_read_frontier_tests {
    //! A client that committed a metadata write and then re-homed onto a
    //! lagging backup must never be served the pre-write state.
    //!
    //! The window is not peer shards on one node: a committed reply is only
    //! produced after `gated_apply` published, and every shard reads the same
    //! left-right buffers. It is a node whose commit walk trails the epoch the
    //! client already holds. Register forwarding is the supported way to get
    //! there -- a backup verifies the credentials itself, forwards only the
    //! consensus proposal, and binds the committed session while its own
    //! `commit_journal` is still behind that op (see
    //! `server::dispatch::session_ops`).
    //!
    //! Blocking replication INTO one backup while the quorum commits without
    //! it produces the lag deterministically, and the login still completes
    //! because `ForwardRegister` / `ForwardRegisterResult` are left flowing.
    //!
    //! One shard per replica for the end-to-end read, deliberately. The harness
    //! homes each inbound client packet on a seeded-random shard while every
    //! shard owns its own `SessionManager`, so on a multi-shard replica a bound
    //! session cannot reliably receive its own follow-up requests -- and the lag
    //! under test is the node's, not a shard's.
    //!
    //! The second test is the other half: peer shards are not the WINDOW, but
    //! they are how a peer-homed read learns the node's position at all, and
    //! sharing one frontier cell across a replica's shards is what makes that
    //! work. It runs multi-shard and drives the cell directly, so it needs no
    //! session and dodges the homing problem entirely.

    use super::*;
    use crate::client::SimClient;
    use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
    use iggy_binary_protocol::{Command, PrimaryRebalanceHeader, RoutedRequestHeader, WireDecode};

    /// Replica 0 leads the metadata plane at view 0, so this one is a backup
    /// for the whole run and is the node the client re-homes onto.
    const LAGGING: u8 = 1;

    /// The gate's own budget, in `sim.step()`s: one step advances the virtual
    /// clock by one consensus tick, so the tick count of the budget IS the step
    /// count a held read survives.
    ///
    /// The simulator's replicas carry the frontier's built-in default, since
    /// they are built without a `[cluster]` config to size it from.
    #[allow(clippy::cast_possible_truncation)]
    const BUDGET_STEPS: u32 = (metadata::AppliedFrontier::DEFAULT_READ_BUDGET.as_millis()
        / shard::CONSENSUS_TICK_INTERVAL.as_millis()) as u32;

    /// Steps the read is given while the backup is still cut off. A server that
    /// answers a metadata read from an unconverged state answers within a
    /// couple of these; the gate must hold the read past all of them.
    ///
    /// A fifth of the budget, so expiry cannot masquerade as a held read and
    /// the convergence phase below still has most of the budget left.
    const STALE_WINDOW_STEPS: u32 = BUDGET_STEPS / 5;

    /// Steps the convergence phase spends waiting for the held read.
    ///
    /// Deliberately PAST the budget rather than exactly up to it: repair that
    /// lands one tick late would otherwise flip this test onto the expiry path
    /// and fail on the status assertion, which reads as "the gate is broken"
    /// when it means "convergence was slow". Overshooting instead lets the
    /// expired read be reported as what it is. Expiry has its own case, which
    /// never restores replication at all.
    const CONVERGE_STEPS: u32 = BUDGET_STEPS - STALE_WINDOW_STEPS + BUDGET_STEPS / 2;

    /// The frames that would let the backup learn the committed writes. Journal
    /// repair and `StartView` adoption are cut with the same knife as live
    /// replication: any one of them left open closes the lag this exercises.
    const REPLICATION_FRAMES: [Command; 5] = [
        Command::Prepare,
        Command::Commit,
        Command::RepairPrepare,
        Command::RepairDone,
        Command::StartView,
    ];

    /// Both cases below need the same shape: a stream created everywhere, then
    /// deleted on a quorum that excludes `LAGGING` while the client re-homes
    /// onto it, so the client holds a committed epoch above a delete that
    /// backup has not applied. Returns the sim, the re-homed client, and the
    /// op the delete committed at.
    ///
    /// Replication into `LAGGING` is left CUT: each case decides whether to
    /// restore it.
    fn backup_behind_a_deleted_stream(
        seed: u64,
        stream_name: &str,
        client_id: u128,
    ) -> (Simulator, SimClient, u64) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
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

        let client = SimClient::new(client_id);
        sim.shell_login(&client);

        // The create lands on every replica: the backup has to HOLD the stream
        // for the read to be able to serve a stale one.
        let created = commit_write(&mut sim, client_id, 0, client.create_stream(stream_name));
        step_until_applied(&mut sim, LAGGING, created);

        // Cut replication into the backup, so the delete commits on the quorum
        // formed by the primary and the remaining replica and never reaches it.
        // Journal repair and `StartView` adoption go with it: any one of them
        // left open closes the lag this exercises.
        set_replication(&mut sim, LAGGING, false);

        let deleted = commit_write(&mut sim, client_id, 0, client.delete_stream(stream_name));
        assert!(
            deleted > created,
            "the delete must commit above the create, else the read cannot \
             distinguish the two states"
        );

        let lagging_commit = metadata_commit(&sim, usize::from(LAGGING));
        assert!(
            (created..deleted).contains(&lagging_commit),
            "the backup applied up to op {lagging_commit}, outside the window \
             [{created}, {deleted}) these tests need: it must hold the create and \
             miss the delete"
        );
        assert_eq!(
            read_stream_name_on(&sim, LAGGING, stream_name),
            Some(stream_name.to_string()),
            "the backup no longer holds the deleted stream, so a read cannot \
             serve a stale one and the assertions prove nothing"
        );
        (sim, client, deleted)
    }

    /// A stream deleted before the client re-homed must not come back on the
    /// backup that has not applied the delete yet.
    #[test]
    fn given_backup_behind_the_client_epoch_when_reading_a_deleted_stream_should_not_serve_it() {
        let stream_name = "read-your-writes";
        let client_id: u128 = 1;
        let (mut sim, client, deleted) =
            backup_behind_a_deleted_stream(0x1A7E_0F31, stream_name, client_id);

        let read = client.bind_session_request(deleted);
        let request_id = read.header().request;
        sim.submit_request(client_id, LAGGING, read.into_generic());

        // Phase 1: still cut off. Any answer here is served from state that
        // predates the delete the client already saw committed.
        let mut early = None;
        for _ in 0..STALE_WINDOW_STEPS {
            if let Some(reply) = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == request_id)
            {
                early = Some(reply);
                break;
            }
        }
        if let Some(reply) = early {
            panic!(
                "the backup answered a metadata read while its applied frontier \
                 ({}) was below the client's committed epoch ({deleted}): status={}",
                metadata_commit(&sim, usize::from(LAGGING)),
                reply.header().status,
            );
        }

        // Phase 2: restore replication. The held read must answer from the
        // converged state, which no longer holds the stream.
        set_replication(&mut sim, LAGGING, true);
        for step in 0..CONVERGE_STEPS {
            if let Some(reply) = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == request_id)
            {
                assert_eq!(
                    reply.header().status,
                    0,
                    "the read was refused rather than answered {step} steps into a \
                     restored link: the gate expired at its {BUDGET_STEPS}-step budget, \
                     of which phase 1 spent {STALE_WINDOW_STEPS}, so repair was slower \
                     than the budget rather than the gate being wrong"
                );
                let read = super::tests::submit_and_wait_for_reply(
                    &mut sim,
                    client_id,
                    LAGGING,
                    client.get_stream(stream_name),
                );
                assert_eq!(
                    read_stream_name(&read),
                    None,
                    "the converged backup still serves the deleted stream"
                );
                return;
            }
        }
        panic!(
            "no answer to the held metadata read within {CONVERGE_STEPS} steps of \
             restored replication; backup applied frontier {}, client epoch {deleted}",
            metadata_commit(&sim, usize::from(LAGGING)),
        );
    }

    /// The other half of the bound: a backup that NEVER catches up must refuse
    /// the held read rather than serve the state the client already saw
    /// replaced, and it must do so inside the budget rather than hanging.
    ///
    /// Same setup as the test above with replication left cut, so the only
    /// possible outcomes are the refusal this asserts or a stale answer.
    #[test]
    fn given_a_backup_that_never_converges_when_reading_should_refuse_inside_the_budget() {
        let stream_name = "read-your-writes-expiry";
        let client_id: u128 = 1;
        let (mut sim, client, deleted) =
            backup_behind_a_deleted_stream(0x1A7E_0F32, stream_name, client_id);

        let read = client.bind_session_request(deleted);
        let request_id = read.header().request;
        sim.submit_request(client_id, LAGGING, read.into_generic());

        // Overshoot the budget: the refusal must land inside it, and a read
        // still unanswered after it is a hang, which the panic below names.
        for _ in 0..(BUDGET_STEPS + BUDGET_STEPS / 2) {
            if let Some(reply) = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == request_id)
            {
                assert_ne!(
                    reply.header().status,
                    0,
                    "the cut-off backup accepted a binding below the required metadata op {deleted}",
                );
                assert_eq!(read_stream_name(&reply), None, "a refusal carries no body");
                return;
            }
        }
        panic!(
            "the held read neither answered nor expired within {} steps; backup applied \
             frontier {}, client epoch {deleted}",
            BUDGET_STEPS + BUDGET_STEPS / 2,
            metadata_commit(&sim, usize::from(LAGGING)),
        );
    }

    /// Shards per replica for the sharing test below. Two is the whole
    /// population that matters: shard 0 and one peer.
    const SHARED_FRONTIER_SHARDS: u16 = 2;

    /// Advance applied to shard 0, chosen above whatever recovery seeded so a
    /// peer reading the pre-advance value cannot pass by accident.
    const SHARED_FRONTIER_ADVANCE: u64 = 7;

    /// A peer shard owns no metadata consensus, so `commit_min` -- the number
    /// the pre-existing read barrier gates on -- does not exist there at all.
    /// The applied frontier is the only thing its read gate can consult, and it
    /// arrives by being ONE cell per process rather than one per shard.
    ///
    /// Nothing else in the system observes that. A private cell per shard still
    /// compiles, still serves every read, and its only symptom is that each
    /// metadata read homed on a peer shard parks for the whole deadline and
    /// then fails retryable -- a latency cliff behind a warning, not an error.
    /// So the invariant is asserted directly rather than through a request: a
    /// peer must see an advance it did not make.
    #[test]
    fn given_a_peer_shard_when_shard_zero_advances_the_frontier_should_observe_it() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed: 0x5EED_5A11,
            ..packet::PacketSimulatorOptions::default()
        };
        let sim = Simulator::with_shards(
            usize::from(replica_count),
            SHARED_FRONTIER_SHARDS,
            std::iter::once(1u128),
            network_opts,
        );

        let shards = &sim.replicas[0].shards;
        assert_eq!(
            shards.len(),
            usize::from(SHARED_FRONTIER_SHARDS),
            "the replica did not build the peer shard this test needs"
        );
        for (shard_idx, shard) in shards.iter().enumerate().skip(1) {
            assert!(
                shard.plane.metadata().consensus.is_none(),
                "shard {shard_idx} owns consensus, so it is not the peer whose \
                 only source for the frontier is shard 0's cell"
            );
        }

        let advanced =
            shards[0].plane.metadata().applied_frontier().get() + SHARED_FRONTIER_ADVANCE;
        shards[0]
            .plane
            .metadata()
            .advance_applied_frontier(advanced);

        for (shard_idx, shard) in shards.iter().enumerate() {
            assert_eq!(
                shard.plane.metadata().applied_frontier().get(),
                advanced,
                "shard {shard_idx} did not observe shard 0's advance: the \
                 applied-frontier cell is per shard, not per process, so every \
                 metadata read homed here parks until its deadline"
            );
        }
    }

    #[test]
    fn given_an_isolated_metadata_primary_when_partition_requests_exceed_the_bound_should_refuse_before_resolution()
     {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            3,
            1,
            std::iter::once(1),
            packet::PacketSimulatorOptions {
                node_count: 3,
                client_count: 1,
                seed: 0xF2E5_0001,
                ..packet::PacketSimulatorOptions::default()
            },
        );
        let client = SimClient::new(1);
        sim.shell_login(&client);
        for peer in 1..3 {
            sim.network
                .set_link_filter(ProcessId::Replica(0), ProcessId::Replica(peer), false);
            sim.network
                .set_link_filter(ProcessId::Replica(peer), ProcessId::Replica(0), false);
        }
        let bound = std::time::Duration::from_secs(9);
        for _ in 0..(bound.as_millis() / shard::CONSENSUS_TICK_INTERVAL.as_millis()) {
            sim.step();
        }
        let missing = IggyNamespace::new(99, 99, 0);
        let poll = client.poll_messages(missing, 1);
        let send = client.send_messages(missing, &[bytes::Bytes::from_static(b"expired")]);
        for request in [poll, send] {
            let reply = super::tests::submit_and_wait_for_reply(&mut sim, 1, 0, request);
            assert_eq!(
                reply.header().status,
                iggy_common::IggyError::TransientNotAccepted.as_code()
            );
        }
    }

    fn partition_freshness_cluster(
        replicas: u8,
        seed: u64,
    ) -> (Simulator, SimClient, IggyNamespace) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            usize::from(replicas),
            1,
            std::iter::once(1),
            packet::PacketSimulatorOptions {
                node_count: replicas,
                client_count: 1,
                seed,
                ..Default::default()
            },
        );
        let client = SimClient::new(1);
        sim.shell_login(&client);
        commit_write(&mut sim, 1, 0, client.create_stream("freshness"));
        let created = commit_write(
            &mut sim,
            1,
            0,
            client.create_topic("freshness", "partition", 1),
        );
        let namespace = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .namespace_from_partition(
                &iggy_binary_protocol::WireIdentifier::named("freshness").unwrap(),
                &iggy_binary_protocol::WireIdentifier::named("partition").unwrap(),
                0,
            )
            .unwrap();
        for replica in 0..replicas {
            step_until_applied(&mut sim, replica, created);
        }
        sim.init_partition(namespace);
        assert!(
            (0..replicas).all(|replica| {
                sim.partition_consensus_state(usize::from(replica), namespace)
                    .is_some()
            }),
            "freshness fixture must materialise its committed partition"
        );
        sim.discover_partition_contexts(&client);
        for replica in 1..replicas {
            let reply = super::tests::submit_and_wait_for_reply(
                &mut sim,
                1,
                replica,
                client.bind_session_request(created),
            );
            assert_eq!(reply.header().status, 0, "bind replica{replica}");
        }
        for _ in 0..SETUP_TOTAL_STEPS {
            if (0..replicas).all(|replica| partition_fresh(&sim, replica)) {
                return (sim, client, namespace);
            }
            sim.step();
        }
        panic!("healthy cluster never credited every node");
    }

    fn partition_fresh(sim: &Simulator, replica: u8) -> bool {
        let shard = &sim.replicas[usize::from(replica)].shards[0];
        shard
            .plane
            .metadata()
            .applied_frontier()
            .partition_requests_allowed(shard.bus.monotonic_now())
    }

    fn advance_freshness_time(sim: &mut Simulator, duration: std::time::Duration) {
        for _ in 0..(duration.as_millis() / shard::CONSENSUS_TICK_INTERVAL.as_millis()) {
            sim.step();
        }
    }

    #[test]
    fn given_healthy_idle_three_and_five_node_clusters_when_partition_requests_arrive_should_stay_admitted()
     {
        for replicas in [3, 5] {
            let (mut sim, client, namespace) =
                partition_freshness_cluster(replicas, 0xF2E5_0010 + u64::from(replicas));
            for _ in 0..3 {
                advance_freshness_time(&mut sim, std::time::Duration::from_secs(8));
                assert!(
                    (0..replicas).all(|replica| partition_fresh(&sim, replica)),
                    "idle{replicas}-node cluster lost quorum evidence"
                );
            }
            let mut served = false;
            for replica in 0..replicas {
                let reply = super::tests::submit_and_wait_for_reply(
                    &mut sim,
                    1,
                    replica,
                    client.poll_messages(namespace, 1),
                );
                assert_ne!(
                    reply.header().status,
                    IggyError::TransientNotAccepted.as_code(),
                    "healthy node{replica}: partition {:?}, fresh {}",
                    sim.partition_consensus_state(usize::from(replica), namespace),
                    partition_fresh(&sim, replica)
                );
                served |= reply.header().status == 0;
            }
            assert!(served, "a healthy partition primary must serve the poll");
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_five_node_split_when_the_bound_expires_should_fence_the_minority_and_keep_the_majority_available()
     {
        let (mut sim, client, namespace) = partition_freshness_cluster(5, 0xF2E5_0020);
        let split_tick = sim.network.current_tick();
        eprintln!("split starts at network tick {split_tick}");
        for from in [2, 3, 4] {
            for to in [2, 3, 4] {
                *sim.network
                    .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) = Some(
                    |packet| {
                        if packet.message.header().command == Command::PrimaryRebalance {
                            let message: Message<PrimaryRebalanceHeader> =
                                packet.message.deep_copy().try_into_typed().unwrap();
                            let header = message.header();
                            if header.view > 0 {
                                eprintln!(
                                    "freshness packet {:?}->{:?}, deliverable tick {}, view {}, {:?}, quorum age ns {}",
                                    packet.from,
                                    packet.to,
                                    packet.ready_at,
                                    header.view,
                                    header.kind,
                                    header.quorum_age_ns,
                                );
                            }
                        }
                        false
                    },
                );
            }
        }
        let mut previous = None;
        let mut step = |sim: &mut Simulator| {
            sim.step();
            let state = [2, 3, 4].map(|node| {
                let consensus = sim.metadata_consensus(node).unwrap();
                (
                    consensus.view(),
                    consensus.status(),
                    partition_fresh(sim, u8::try_from(node).unwrap()),
                )
            });
            if previous != Some(state) {
                eprintln!(
                    "majority state at split tick +{}: {state:?}",
                    sim.network.current_tick() - split_tick,
                );
                previous = Some(state);
            }
        };
        for minority in [0, 1] {
            for majority in [2, 3, 4] {
                sim.network.set_link_filter(
                    ProcessId::Replica(minority),
                    ProcessId::Replica(majority),
                    false,
                );
                sim.network.set_link_filter(
                    ProcessId::Replica(majority),
                    ProcessId::Replica(minority),
                    false,
                );
            }
        }
        for _ in 0..(std::time::Duration::from_secs(6).as_millis()
            / shard::CONSENSUS_TICK_INTERVAL.as_millis())
        {
            step(&mut sim);
        }
        assert!(
            partition_fresh(&sim, 0) && partition_fresh(&sim, 1),
            "a sub-K election must not clear existing credit"
        );
        let before = super::tests::submit_and_wait_for_reply(
            &mut sim,
            1,
            0,
            client.poll_messages(namespace, 1),
        );
        assert_eq!(
            before.header().status,
            0,
            "the old primary may serve until its evidence expires: partition {:?}, fresh {}",
            sim.partition_consensus_state(0, namespace),
            partition_fresh(&sim, 0)
        );
        for _ in 0..(std::time::Duration::from_secs(4).as_millis()
            / shard::CONSENSUS_TICK_INTERVAL.as_millis())
        {
            step(&mut sim);
        }
        for replica in [0, 1] {
            assert!(
                !partition_fresh(&sim, replica),
                "minority node{replica} credited without a quorum"
            );
            let reply = super::tests::submit_and_wait_for_reply(
                &mut sim,
                1,
                replica,
                client.poll_messages(namespace, 1),
            );
            assert_eq!(
                reply.header().status,
                IggyError::TransientNotAccepted.as_code()
            );
        }
        let old = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .unwrap();
        assert!(
            old.is_primary(),
            "the isolated node still believes it is primary"
        );
        assert_eq!(old.view(), 0);
        let convergence_start = sim.network.current_tick();
        for _ in 0..SETUP_TOTAL_STEPS {
            if [2, 3, 4]
                .into_iter()
                .all(|replica| partition_fresh(&sim, replica))
            {
                break;
            }
            step(&mut sim);
            assert!(
                !partition_fresh(&sim, 0) && !partition_fresh(&sim, 1),
                "minority credit must remain expired during majority convergence",
            );
        }
        eprintln!(
            "majority qualification took {} ticks after expiry checks",
            sim.network.current_tick() - convergence_start,
        );
        let mut served = false;
        for replica in [2, 3, 4] {
            assert!(
                partition_fresh(&sim, replica),
                "majority node{replica} lost credit: {:?}",
                [2, 3, 4].map(|node| {
                    let consensus = sim.metadata_consensus(node).unwrap();
                    (
                        node,
                        consensus.view(),
                        consensus.status(),
                        consensus.commit_min(),
                        consensus.commit_max(),
                        consensus.adopted_view_head(),
                        partition_fresh(&sim, u8::try_from(node).unwrap()),
                    )
                })
            );
            let consensus = sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata()
                .consensus
                .as_ref()
                .unwrap();
            assert!(consensus.view() > 0, "the majority must depose replica0");
            let reply = super::tests::submit_and_wait_for_reply(
                &mut sim,
                1,
                replica,
                client.poll_messages(namespace, 1),
            );
            assert_ne!(
                reply.header().status,
                IggyError::TransientNotAccepted.as_code()
            );
            served |= reply.header().status == 0;
        }
        assert!(served, "the majority's partition primary must serve a read");
    }

    #[test]
    fn given_an_empty_same_view_restart_when_a_partition_is_created_should_advance_metadata_before_serving_it()
     {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            3,
            1,
            std::iter::once(1),
            packet::PacketSimulatorOptions {
                node_count: 3,
                client_count: 1,
                seed: 0xF2E5_0030,
                ..Default::default()
            },
        );
        assert!(sim.replicas[0].superblock.read_latest_sync().is_none());
        assert_eq!(metadata_commit(&sim, 0), 0);
        sim.replica_crash(0);
        sim.replica_restart(0);
        let metadata = sim.replicas[0].shards[0].plane.metadata();
        assert_eq!(metadata.consensus.as_ref().unwrap().view(), 0);
        assert!(
            metadata
                .mux_stm
                .streams()
                .read(|streams| streams.items.is_empty())
        );
        assert!(!partition_fresh(&sim, 0));
        let client = SimClient::new(1);
        sim.shell_login(&client);
        commit_write(&mut sim, 1, 0, client.create_stream("after-empty-restart"));
        let created = commit_write(
            &mut sim,
            1,
            0,
            client.create_topic("after-empty-restart", "partition", 1),
        );
        assert!(
            created > 0,
            "a partition cannot appear at the ambiguous commit0 boot"
        );
        for replica in 0..3 {
            step_until_applied(&mut sim, replica, created);
        }
        for _ in 0..SETUP_TOTAL_STEPS {
            if (0..3).all(|replica| partition_fresh(&sim, replica)) {
                return;
            }
            sim.step();
        }
        panic!("first committed metadata must restore backup credit after an empty restart");
    }

    #[test]
    fn given_a_durable_primary_restart_when_rejoining_should_start_uncredited_and_adopt_a_new_view()
    {
        let (mut sim, _client, _namespace) = partition_freshness_cluster(3, 0xF2E5_0040);
        let committed = metadata_commit(&sim, 0);
        assert!(committed > 0);
        sim.replica_crash(0);
        sim.replica_restart(0);
        assert!(
            !partition_fresh(&sim, 0),
            "durable metadata is not quorum evidence after reboot"
        );
        for _ in 0..SETUP_TOTAL_STEPS {
            sim.step();
            let consensus = sim.replicas[0].shards[0]
                .plane
                .metadata()
                .consensus
                .as_ref()
                .unwrap();
            if partition_fresh(&sim, 0) {
                assert!(consensus.view() > 0);
                assert!(
                    consensus
                        .adopted_view_head()
                        .is_some_and(|(view, head)| view == consensus.view() && head >= committed)
                );
                assert!(metadata_commit(&sim, 0) >= committed);
                return;
            }
        }
        panic!("durable primary did not regain credit after adopting the current view");
    }

    /// Open or close every replication route from the primary into `replica`.
    fn set_replication(sim: &mut Simulator, replica: u8, open: bool) {
        for frame in REPLICATION_FRAMES {
            let filter = sim
                .network
                .link_filter_mut(ProcessId::Replica(0), ProcessId::Replica(replica));
            if open {
                filter.insert(frame);
            } else {
                filter.remove(frame);
            }
        }
    }

    /// Step until `replica` has applied `op`, so the state the read sees is the
    /// one this test set up rather than whatever the last reply happened to
    /// leave behind.
    fn step_until_applied(sim: &mut Simulator, replica: u8, op: u64) {
        for _ in 0..SETUP_TOTAL_STEPS {
            if metadata_commit(sim, usize::from(replica)) >= op {
                return;
            }
            sim.step();
        }
        panic!(
            "replica {replica} never applied op {op} (stuck at {})",
            metadata_commit(sim, usize::from(replica)),
        );
    }

    /// Read `name` straight out of a replica's committed metadata, bypassing
    /// the read path under test.
    fn read_stream_name_on(sim: &Simulator, replica: u8, name: &str) -> Option<String> {
        sim.replicas[usize::from(replica)].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| {
                inner
                    .items
                    .iter()
                    .find(|(_, stream)| &*stream.name == name)
                    .map(|(_, stream)| stream.name.to_string())
            })
    }

    /// Submit one replicated metadata write to `target`, step until its reply
    /// lands, and return the op it committed at.
    fn commit_write(
        sim: &mut Simulator,
        client_id: u128,
        target: u8,
        request: Message<RoutedRequestHeader>,
    ) -> u64 {
        let request_id = request.header().request;
        sim.submit_request(client_id, target, request.into_generic());
        for _ in 0..SETUP_TOTAL_STEPS {
            if let Some(reply) = sim
                .step()
                .into_iter()
                .find(|reply| reply.header().request == request_id)
            {
                assert_eq!(
                    reply.header().status,
                    0,
                    "metadata write {request_id} was refused"
                );
                assert!(
                    !setup_reply_is_transient(&reply),
                    "metadata write {request_id} was rejected in transit, so it never \
                     committed and cannot anchor the read below"
                );
                return reply.header().commit;
            }
        }
        panic!("metadata write {request_id} never committed");
    }

    /// The stream name a `GetStream` reply carries, or `None` for the
    /// empty-body not-found answer.
    fn read_stream_name(reply: &Message<ReplyHeader>) -> Option<String> {
        let body = reply
            .as_slice()
            .get(size_of::<ReplyHeader>()..reply.header().size as usize)
            .unwrap_or_default();
        if body.is_empty() {
            return None;
        }
        let (response, _) = GetStreamResponse::decode(body)
            .expect("a non-empty GetStream reply must decode as GetStreamResponse");
        Some(response.stream.name.as_str().to_string())
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
}

#[cfg(test)]
mod review_4092_dst_tests {
    //! The deterministic simulator is this repo's strongest correctness tool and
    //! it is hard-asserted out of the feature this PR adds: `init_partition`
    //! (`core/shard/src/lib.rs:4193-4198`) panics for any topic whose durability
    //! or consumer-offset durability is persisted. So `PrepareOk` gating, log-view
    //! certification, commit ordering and quorum durability have no fault
    //! coverage at any granularity.

    use super::*;

    #[test]
    fn given_the_cluster_simulator_when_seeding_a_topic_then_a_persisted_policy_should_be_expressible()
     {
        let replica_count = 3u8;
        let client: u128 = 1;
        let network_opts = packet::PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            ..packet::PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(replica_count as usize, [client].into_iter(), network_opts);
        for _ in 0..100 {
            sim.step();
        }

        let namespace = IggyNamespace::new(1, 1, 0);
        sim.seed_stream_topic_partition(namespace);

        let options = sim.replicas[0].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| {
                inner
                    .items
                    .get(namespace.stream_id())
                    .and_then(|stream| stream.topics.get(namespace.topic_id()))
                    .map(|topic| {
                        iggy_common::TopicRuntimeOptions::from_resource_options(&topic.options)
                    })
                    .unwrap_or_default()
            });

        assert!(
            options.durability.is_persisted() && options.consumer_offset_durability.is_persisted(),
            "ideal storage must support the durable retry policy for messages and offsets"
        );
    }
}

#[cfg(test)]
mod probe_answer_divergence_tests {
    //! End-to-end seeds for the probe-answer `StartView`. The mechanism itself is
    //! pinned by `consensus::impls::probe_answer_tests`; these replay the runs that
    //! found it.

    use super::*;

    /// Seed 144 of the uniform swarm lane: replica 0 prepared op 7 in view 0
    /// without acks, view 1 truncated it and prepared a different op 7, and
    /// replica 0 then adopted view 1 through a probe answer that carried no
    /// canonical headers, so it kept its own op 7 and committed that instead.
    ///
    /// Network faults only, no crash needed, which is why it lands at op 7 and
    /// replays fast. The mechanism is pinned separately by
    /// `consensus::impls::probe_answer_tests`; this is the end-to-end seed.
    #[test]
    fn given_a_probe_adopted_view_when_the_head_diverges_should_not_commit_the_stale_entry() {
        use crate::workload::{
            self, FaultInjector, Workload,
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
        let seed = 144;
        // Swarm, not a fixed profile: the asymmetric partitions and clogs this
        // seed draws are what let a primary prepare an op it cannot get acked.
        let mut network_opts = packet::PacketSimulatorOptions::swarm(seed);
        network_opts.node_count = replica_count;
        network_opts.client_count = 1;
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
        );
        let client = SimClient::new(client_id);
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);
        sim.register_client_with_primary(&client);

        let mut options = WorkloadOptions::new(seed, replica_count, vec![ns]);
        options.weights = ActionWeights::uniform();
        options.crash_per_tick_ratio = 0.01;
        options.restart_per_tick_ratio = 0.05;
        let mut wl = Workload::new(options);

        let mut injector = FaultInjector::new(seed, replica_count);
        let mut invariants = crate::workload::invariants::Invariants::new();
        let clients = [client];
        // `run_with_faults` runs the per-tick invariants and the live state
        // checker, which is where the divergence fired.
        workload::run_with_faults(
            &mut sim,
            &mut wl,
            &clients,
            6_000,
            u64::MAX,
            &mut injector,
            &mut invariants,
        );

        assert!(
            oracle::drive_to_quiesce(&mut sim, &mut wl, &clients, 50_000, &mut invariants),
            "{}",
            oracle::quiesce_failure_report(&sim, &wl),
        );
        assert!(
            oracle::settle_to_stable_view(&mut sim, &mut wl, 50_000, &mut invariants),
            "metadata views never converged after the drain"
        );
        let report = oracle::assert_converged(&sim, &mut wl);
        assert!(
            report.ops_compared > 0,
            "no committed metadata op was witnessed on two replicas, so this seed \
             would pass on a diverged cluster"
        );
    }
}

#[cfg(test)]
mod primary_transfer_crash_tests {
    //! Crash each replica at each handoff message of a planned primary transfer.
    //!
    //! Spread placement moves a partition primary with an ordinary view change,
    //! after the metadata primary confirmed the source frontier and the target's
    //! applied history. Every run of a sweep replays one seed to the tick that
    //! delivers one handoff message, crashes one replica there and restarts it
    //! later, while writes and offset stores keep flowing. The run must converge
    //! with every acknowledged effect, and the primaries must spread again.

    use super::partition_repair_driver_tests::cluster;
    use super::*;
    use crate::workload::{
        self, Workload,
        invariants::Invariants,
        options::{ActionWeights, WorkloadOptions},
        oracle,
    };
    use iggy_binary_protocol::{
        DoViewChangeHeader, PrimaryRebalanceHeader, PrimaryRebalanceKind, StartViewChangeHeader,
        StartViewHeader,
    };
    use packet::Packet;
    use server_common::sharding::METADATA_GROUP;

    const SEED: u64 = 0x5052_494D;
    /// Fails and returns ahead of the transfer that [`Sweep::Return`] crashes into.
    const RETURNING: u8 = 1;
    /// Ticks for the swept transfer to finish in the baseline.
    const TRANSFER_TICKS: u64 = 5_000;
    /// Ticks a crashed replica stays down: past a heartbeat timeout, so the
    /// groups it led fail over.
    const DOWN_TICKS: u64 = 800;
    /// Ticks for the primaries to spread again once every replica is up.
    const SPREAD_TICKS: u64 = 10_000;
    const QUIESCE_TICKS: u64 = 20_000;

    thread_local! {
        /// Handoff messages in delivery order. A link hook is a bare `fn` and
        /// cannot capture.
        static DELIVERED: RefCell<Vec<Handoff>> = const { RefCell::new(Vec::new()) };
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Kind {
        SourceProbe,
        SourceReply,
        PartitionProbe,
        PartitionReply,
        PrimaryTransfer,
        StartViewChange,
        DoViewChange,
        StartView,
    }

    /// A delivered handoff message. `view` is the probed view of a rebalance
    /// frame and the proposed view of a view-change frame.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Handoff {
        tick: u64,
        kind: Kind,
        group: u64,
        view: u32,
        ready: bool,
        from: u8,
        to: u8,
    }

    #[derive(Clone, Copy, Debug)]
    enum Sweep {
        /// The first spread transfer. The metadata primary also leads the group,
        /// so it drains its own accepted work and the source frames stay local.
        Spread,
        /// The transfer back to [`RETURNING`]. Coordinator, source and target are
        /// three replicas, so every handoff crosses the network.
        Return,
    }

    impl Sweep {
        const fn handoffs(self) -> &'static [Kind] {
            match self {
                Self::Spread => &[
                    Kind::PartitionProbe,
                    Kind::PartitionReply,
                    Kind::StartViewChange,
                    Kind::DoViewChange,
                    Kind::StartView,
                ],
                Self::Return => &[
                    Kind::SourceProbe,
                    Kind::SourceReply,
                    Kind::PartitionProbe,
                    Kind::PartitionReply,
                    Kind::PrimaryTransfer,
                    Kind::StartViewChange,
                    Kind::DoViewChange,
                    Kind::StartView,
                ],
            }
        }
    }

    fn record_handoff(packet: &Packet) -> bool {
        let (ProcessId::Replica(from), ProcessId::Replica(to)) = (packet.from, packet.to) else {
            return false;
        };
        let bytes = packet.message.as_slice();
        let (kind, group, view, ready) = match packet.message.header().command {
            Command::PrimaryRebalance => {
                let header: &PrimaryRebalanceHeader =
                    bytemuck::checked::from_bytes(&bytes[..size_of::<PrimaryRebalanceHeader>()]);
                let kind = match header.kind {
                    PrimaryRebalanceKind::MetadataProbe | PrimaryRebalanceKind::MetadataReply => {
                        return false;
                    }
                    PrimaryRebalanceKind::SourceProbe => Kind::SourceProbe,
                    PrimaryRebalanceKind::SourceReply => Kind::SourceReply,
                    PrimaryRebalanceKind::PartitionProbe => Kind::PartitionProbe,
                    PrimaryRebalanceKind::PartitionReply => Kind::PartitionReply,
                    PrimaryRebalanceKind::PrimaryTransfer => Kind::PrimaryTransfer,
                };
                (
                    kind,
                    header.namespace,
                    header.partition_view,
                    header.ready == 1,
                )
            }
            Command::StartViewChange => {
                let header: &StartViewChangeHeader =
                    bytemuck::checked::from_bytes(&bytes[..size_of::<StartViewChangeHeader>()]);
                (Kind::StartViewChange, header.group, header.view, false)
            }
            Command::DoViewChange => {
                let header: &DoViewChangeHeader =
                    bytemuck::checked::from_bytes(&bytes[..size_of::<DoViewChangeHeader>()]);
                (Kind::DoViewChange, header.group, header.view, false)
            }
            Command::StartView => {
                let header: &StartViewHeader =
                    bytemuck::checked::from_bytes(&bytes[..size_of::<StartViewHeader>()]);
                (Kind::StartView, header.group, header.view, false)
            }
            _ => return false,
        };
        if group != METADATA_GROUP {
            DELIVERED.with_borrow_mut(|delivered| {
                delivered.push(Handoff {
                    tick: 0,
                    kind,
                    group,
                    view,
                    ready,
                    from,
                    to,
                });
            });
        }
        false
    }

    /// The handoff messages of the first transfer a target confirmed after
    /// `after`, in protocol order, once the target started its view.
    fn transfer(trace: &[Handoff], after: u64) -> Option<Vec<Handoff>> {
        let confirmed = trace.iter().position(|handoff| {
            handoff.tick > after && handoff.kind == Kind::PartitionReply && handoff.ready
        })?;
        let reply = trace[confirmed];
        let probed = |handoff: &&Handoff| {
            handoff.tick > after && handoff.group == reply.group && handoff.view == reply.view
        };
        let mut handoffs: Vec<_> = [Kind::SourceProbe, Kind::SourceReply, Kind::PartitionProbe]
            .into_iter()
            .filter_map(|kind| {
                trace[..confirmed]
                    .iter()
                    .rev()
                    .filter(probed)
                    .find(|handoff| handoff.kind == kind)
                    .copied()
            })
            .collect();
        handoffs.push(reply);
        handoffs.extend(
            trace[confirmed..]
                .iter()
                .find(|handoff| {
                    handoff.kind == Kind::PrimaryTransfer
                        && handoff.group == reply.group
                        && handoff.view == reply.view
                })
                .copied(),
        );
        let started = *trace[confirmed..].iter().find(|handoff| {
            handoff.kind == Kind::StartView
                && handoff.group == reply.group
                && handoff.view > reply.view
        })?;
        for kind in [Kind::StartViewChange, Kind::DoViewChange] {
            handoffs.push(*trace[confirmed..].iter().find(|handoff| {
                handoff.kind == kind
                    && handoff.group == started.group
                    && handoff.view == started.view
            })?);
        }
        handoffs.push(started);
        Some(handoffs)
    }

    /// One seeded run. Every run replays the same trace until its crash.
    struct Run {
        sim: Simulator,
        workload: Workload,
        clients: [SimClient; 1],
        invariants: Invariants,
        namespaces: Vec<IggyNamespace>,
        tick: u64,
        replies: u64,
        trace: Vec<Handoff>,
    }

    impl Run {
        /// One fixture group per replica. Every fixture starts on the metadata
        /// primary, so the rebalance has to move all but one of them.
        fn new() -> Self {
            let (mut sim, client) = cluster(SEED);
            let namespaces: Vec<_> = (0..usize::from(sim.replica_count))
                .map(|partition| IggyNamespace::new(1, 1, partition))
                .collect();
            for &namespace in &namespaces {
                sim.init_partition(namespace);
            }
            sim.register_client_with_primary(&client);
            for from in 0..sim.replica_count {
                for to in (0..sim.replica_count).filter(|&to| to != from) {
                    *sim.network
                        .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) =
                        Some(record_handoff);
                }
            }
            DELIVERED.take();
            let mut options = WorkloadOptions::new(SEED, sim.replica_count, namespaces.clone());
            options.weights = ActionWeights::partition_only();
            options.ack_quorum_ratio = 1.0;
            Self {
                sim,
                workload: Workload::new(options),
                clients: [client],
                invariants: Invariants::new(),
                namespaces,
                tick: 0,
                replies: 0,
                trace: Vec::new(),
            }
        }

        fn tick(&mut self) {
            self.replies += workload::run(
                &mut self.sim,
                &mut self.workload,
                &self.clients,
                1,
                u64::MAX,
                &mut self.invariants,
            );
            self.tick += 1;
            let tick = self.tick;
            self.trace.extend(
                DELIVERED
                    .take()
                    .into_iter()
                    .map(|handoff| Handoff { tick, ..handoff }),
            );
        }

        fn tick_until(&mut self, budget: u64, done: impl Fn(&Self) -> bool) -> bool {
            for _ in 0..budget {
                if done(self) {
                    return true;
                }
                self.tick();
            }
            done(self)
        }

        /// Every replica up, every group normal in one view on all of them, and
        /// each replica the primary of exactly one group.
        fn spread(&self) -> bool {
            let mut led = vec![0_usize; usize::from(self.sim.replica_count)];
            for &namespace in &self.namespaces {
                let mut view = None;
                for replica in 0..self.sim.replica_count {
                    let Some(state) = (!self.sim.is_crashed(replica))
                        .then(|| {
                            self.sim
                                .partition_consensus_state(usize::from(replica), namespace)
                        })
                        .flatten()
                    else {
                        return false;
                    };
                    if state.status != Status::Normal
                        || *view.get_or_insert(state.view) != state.view
                    {
                        return false;
                    }
                    if state.is_primary {
                        led[usize::from(replica)] += 1;
                    }
                }
            }
            led.iter().all(|&groups| groups == 1)
        }

        /// Drive to the start of the sweep's transfer window.
        fn prepare(&mut self, sweep: Sweep) {
            if matches!(sweep, Sweep::Return) {
                assert!(
                    self.tick_until(SPREAD_TICKS, Self::spread),
                    "the fixtures must spread before replica {RETURNING} fails"
                );
                self.sim.replica_crash(RETURNING);
                for _ in 0..DOWN_TICKS {
                    self.tick();
                }
                self.sim.replica_restart(RETURNING);
            }
        }
    }

    /// Crash each replica at each handoff message of the sweep's transfer.
    fn sweep(sweep: Sweep) {
        let mut baseline = Run::new();
        baseline.prepare(sweep);
        let start = baseline.tick;
        assert!(
            baseline.tick_until(TRANSFER_TICKS, |run| transfer(&run.trace, start).is_some()),
            "{sweep:?}: a planned transfer must finish without faults"
        );
        let handoffs = transfer(&baseline.trace, start).expect("the transfer finished");
        assert_eq!(
            handoffs
                .iter()
                .map(|handoff| handoff.kind)
                .collect::<Vec<_>>(),
            sweep.handoffs(),
            "{sweep:?}: unexpected handoff messages {handoffs:?}"
        );
        let mut crash_points = HashSet::new();
        for &handoff in &handoffs {
            for victim in 0..baseline.sim.replica_count {
                if crash_points.insert((handoff.tick, victim)) {
                    crash_at(sweep, handoff, victim);
                }
            }
        }
    }

    fn crash_at(sweep: Sweep, handoff: Handoff, victim: u8) {
        let context = format!("{sweep:?}: replica {victim} crashed after {handoff:?}");
        let mut run = Run::new();
        run.prepare(sweep);
        while run.tick < handoff.tick {
            run.tick();
        }
        assert!(
            run.trace.contains(&handoff),
            "{context}: the replay must deliver the handoff at the baseline tick"
        );
        run.sim.replica_crash(victim);
        for _ in 0..DOWN_TICKS {
            run.tick();
        }
        run.sim.replica_restart(victim);
        let restarted_with = run.replies;
        assert!(
            run.tick_until(SPREAD_TICKS, |run| run.spread()
                && run.replies > restarted_with),
            "{context}: the primaries must spread again and serve new work"
        );
        assert!(
            oracle::drive_to_quiesce(
                &mut run.sim,
                &mut run.workload,
                &run.clients,
                QUIESCE_TICKS,
                &mut run.invariants
            ),
            "{context}: {}",
            oracle::quiesce_failure_report(&run.sim, &run.workload)
        );
        assert!(
            oracle::settle_to_stable_view(
                &mut run.sim,
                &mut run.workload,
                QUIESCE_TICKS,
                &mut run.invariants
            ),
            "{context}: the views must settle after the drain"
        );
        oracle::assert_converged(&run.sim, &mut run.workload);
    }

    #[test]
    fn given_the_first_spread_transfer_when_each_replica_crashes_at_each_handoff_should_converge_and_spread()
     {
        sweep(Sweep::Spread);
    }

    #[test]
    fn given_a_transfer_to_a_returned_replica_when_each_replica_crashes_at_each_handoff_should_converge_and_spread()
     {
        sweep(Sweep::Return);
    }
}
