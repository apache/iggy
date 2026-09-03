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

use crate::bus::{SharedSimOutbox, SimOutbox};
use crate::deps::SimSuperblock;
use crate::deps::{MemStorage, SimJournal, SimMuxStateMachine, SimSnapshot};
use configs::server::PersonalAccessTokenConfig;
use configs::server::ServerSystemConfig;
use consensus::{ClientTable, ConsensusClock, LocalPipeline, Sequencer, VsrConsensus, VsrState};
use iggy_common::IggyByteSize;
use iggy_common::variadic;
use journal::Journal;
use metadata::impls::metadata::IggySnapshot;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::mux::WithFactory;
use metadata::stm::snapshot::RestoreSnapshot;
use metadata::stm::stream::{Streams, StreamsInner};
use metadata::stm::user::{Users, UsersInner};
use metadata::{IggyMetadata, apply_committed_prepare};
use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
use server::boot::wire_shell_handlers;
use server::shell::{ShellHandlers, ShellShardHandle};
use server_common::crypto;
use server_common::sharding::{METADATA_GROUP, ShardId};
use shard::shards_table::PapayaShardsTable;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// TODO: Make configurable
const CLUSTER_ID: u128 = 1;

/// Deterministic root credentials the dispatch-shell `SimClient` logs in with.
///
/// Fixed (never env-derived) so the login handshake replays with the seed.
/// Both satisfy the 3..=50 / 3..=100 username/password length bounds
/// `verify_login_credentials` enforces.
pub const SHELL_ROOT_USERNAME: &str = "iggy";
pub const SHELL_ROOT_PASSWORD: &str = "iggy";

// For now there is only one shard per replica,
// we will add support for multiple shards per replica in the future.
//
// `PapayaShardsTable` (the production namespace -> shard routing table)
// instead of the always-`None` `()` impl: each shard owns its own table
// instance, exactly as the server wires it. Until rows are seeded the
// router falls back to the deterministic hash assignment, which at one
// shard per replica always resolves to shard 0.
pub type Replica = shard::IggyShard<
    SharedSimOutbox,
    // MJ: metadata journal, behind an `Rc` so the harness retains it across a
    // replica restart; the WAL bytes and index survive the drop.
    Rc<SimJournal<MemStorage>>,
    SimSnapshot,
    SimMuxStateMachine,
    PapayaShardsTable,
    SimSuperblock,
>;

/// Read-side handoff bundle for the metadata STM.
///
/// Shard 0 (the sole writer) mints one via `factory_bundle`; every peer shard
/// rebuilds a reader-mode mirror from it with `from_factory_bundle`, exactly as
/// the server bootstrap does with its `ServerMetadataBundle`.
/// `Clone + Send + Sync`.
pub type SimMetadataBundle = <variadic!(Users, Streams) as WithFactory>::Bundle;

/// Capacity of each shard inbox in the simulator.
///
/// The router drops frames when an inbox is full (recovered by VSR
/// retransmit in production); sized generously so no drop can happen
/// unless a test injects one on purpose. Tests assert the frame-drop
/// metrics stay zero.
pub const SIM_INBOX_CAPACITY: usize = 8192;

/// Read this replica's persisted metadata checkpoint and its trailer checksum.
///
/// A file that exists but does not load panics rather than reading as `None`:
/// production refuses boot on a torn snapshot, and booting empty instead would turn
/// a storage fault into the silent state loss this path exists to prevent.
///
/// # Panics
/// If `snapshot.bin` exists but cannot be read or decoded.
fn load_local_checkpoint(data_dir: &std::path::Path) -> Option<(IggySnapshot, u128)> {
    let path = data_dir
        .join(metadata::impls::METADATA_DIR)
        .join(metadata::impls::SNAPSHOT_FILE_NAME);
    if !path.exists() {
        return None;
    }
    let loaded = IggySnapshot::load(&path).unwrap_or_else(|error| {
        panic!(
            "metadata checkpoint at {} exists but does not load: {error}",
            path.display(),
        )
    });
    Some(loaded)
}

/// Build one shard of a sim replica around the real inter-shard mesh.
///
/// `senders`/`inbox` come from [`shard::shard_mesh_channels`], so the
/// shard routes through the same `dispatch` -> inbox -> pump path as
/// production instead of the old `without_inbox` bypass.
///
/// `shell` selects the dispatch handlers. Off is the fast path: inert
/// no-ops, so the simulator drives raw client frames straight into
/// `IggyShard::on_message`. On wires the server's real deferred dispatch
/// handlers (via [`wire_shell_handlers`]), exactly as production does, so
/// a client request runs as a task concurrent with the pump.
///
/// Mirrors the server bootstrap's single-writer metadata: the consensus
/// group, journal, snapshot, and the only writable metadata STM live on
/// shard 0. Shard 0 mints a [`SimMetadataBundle`] (returned as the second
/// tuple element); every peer shard passes it back in as `reader_bundle`
/// and rebuilds a reader-mode STM that observes shard 0's committed
/// metadata through the shared left-right read handle. So a partition op
/// homing on a peer shard resolves its namespace against consensus-committed
/// streams, rather than a stale per-shard copy. `reader_bundle` is `None`
/// for shard 0 and `Some` for every peer; the return bundle is `Some` only
/// for shard 0.
///
/// # Panics
/// Panics if `senders` is not in canonical order (a bug in the caller's
/// mesh construction), or if `reader_bundle` disagrees with `shard_idx`
/// (peer without a bundle, or shard 0 with one).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn new_shard(
    replica_id: u8,
    shard_idx: u16,
    name: String,
    bus: &Rc<SimOutbox>,
    replica_count: u8,
    senders: Vec<shard::TaggedSender>,
    inbox: shard::Receiver<shard::ShardFrame>,
    reply_inbox: shard::Receiver<shard::ShardFrame>,
    clock: ConsensusClock,
    shell: bool,
    reader_bundle: Option<SimMetadataBundle>,
    superblock: Option<Rc<SimSuperblock>>,
    metadata_journal: Option<Rc<SimJournal<MemStorage>>>,
    recovered_state: Option<VsrState>,
    incarnation: u128,
    data_dir: Option<std::path::PathBuf>,
    seed_namespaces: &[(server_common::sharding::IggyNamespace, u32)],
    applied_frontier: Arc<AtomicU64>,
) -> (Rc<Replica>, Option<SimMetadataBundle>) {
    // Metadata is single-writer, mirroring the server bootstrap. Shard 0 owns
    // the only writable STM; every peer shard rebuilds a reader-mode mirror from
    // shard 0's factory bundle and sees committed metadata through the shared
    // left-right read handle (each apply `publish`es, bounding reader staleness
    // to one op). Writes never target a peer, so peers seed nothing locally;
    // `reader_bundle` is `Some` exactly for them.
    debug_assert_eq!(
        shard_idx == 0,
        reader_bundle.is_none(),
        "shard 0 is the metadata writer (no bundle); peers are readers (bundle)"
    );
    // Head of the retained metadata WAL, shard 0 only and `None` on a peer shard or a
    // fresh boot. Its committed prefix drives both the STM replay below and the
    // consensus op/commit restore, so a restart recovers committed metadata from its
    // own disk instead of an empty state.
    let solo = replica_count == 1;
    let restored_op = metadata_journal
        .as_ref()
        .and_then(|journal| journal.last_op());
    // Read before the state machine is built: it REPLACES the seeded default rather
    // than layering onto it, so without it a checkpointed replica recovers an empty
    // baseline and loses every op the drain reclaimed.
    let local_checkpoint = data_dir
        .as_deref()
        .filter(|_| shard_idx == 0)
        .and_then(load_local_checkpoint);
    let has_checkpoint = local_checkpoint.is_some();
    // Off by one from the journal's watermark, deliberately: a checkpoint at op N
    // drains `0..=N - 1` and retains N's header for view-change merging, so replaying
    // from the journal floor would re-apply N on top of a snapshot that has it.
    let checkpoint_seq = local_checkpoint
        .as_ref()
        .map_or(0, |(snapshot, _)| snapshot.snapshot().sequence_number);

    let mux = reader_bundle.map_or_else(
        // Writer shard (shard 0). Seed the root user at slab id 0, matching
        // production bootstrap (`ensure_root_user`); it is undeletable in-apply,
        // so it stays in slot 0 and the workload's users land at slab id 1+, out
        // of any delete attempt. Peer shards inherit it through the read handle,
        // so the seed runs only here. Shell mode logs a client in against this
        // user (the login gate Argon2-checks the stored hash), so seed a real
        // hash of `SHELL_ROOT_PASSWORD`; otherwise a deterministic placeholder,
        // since login never runs and a random per-run argon2 salt would break
        // state equality. Committed metadata replays into it from the retained WAL
        // after `IggyMetadata` is built (see below), so the factory bundle is minted
        // only then.
        || {
            // Seeding on top of a restored checkpoint would mint a second root user
            // and shift every workload entity's slab id.
            if let Some((snapshot, _)) = local_checkpoint.as_ref() {
                return SimMuxStateMachine::restore_snapshot(snapshot.snapshot())
                    .expect("the local checkpoint decodes into the metadata state machine");
            }
            let users: Users = UsersInner::new().into();
            let root_password_hash = if shell {
                crypto::hash_with_fixed_salt(SHELL_ROOT_PASSWORD)
            } else {
                "hash".to_string()
            };
            users.ensure_root_user(SHELL_ROOT_USERNAME, &root_password_hash);
            let streams: Streams = StreamsInner::new().into();
            SimMuxStateMachine::new(variadic!(users, streams))
        },
        // Peer shard: reader-mode mirror over shard 0's shared read handle.
        SimMuxStateMachine::from_factory_bundle,
    );

    // The production metadata consensus namespace, not 0: the router
    // routes Register/Logout and metadata view-change frames to shard 0
    // by comparing against this exact value; anything else hashes to an
    // arbitrary shard with no metadata consensus.
    let metadata_consensus = (shard_idx == 0).then(|| {
        let mut consensus = VsrConsensus::with_clock(
            CLUSTER_ID,
            replica_id,
            replica_count,
            METADATA_GROUP,
            SharedSimOutbox(Rc::clone(bus)),
            LocalPipeline::new(),
            clock.clone(),
        );
        // Seed-derived incarnation, bumped per restart by the harness, so a StartView
        // from a previous incarnation is ignored while replay stays deterministic; an
        // OS-random nonce would break byte-identical replay.
        consensus.set_incarnation(incarnation);
        // View/log_view come from the durable superblock; op, commit, and the
        // last-prepare markers come from the retained WAL. Independent inputs,
        // mirroring the server's restore_metadata_consensus.
        let last_header = metadata_journal
            .as_ref()
            .and_then(|journal| journal.last_header());
        if let Some(state) = recovered_state {
            consensus.set_view(state.view);
            consensus.set_log_view(state.log_view);
            consensus.mark_superblock_durable(state.view, state.log_view);
        } else if let Some(header) = last_header {
            // No superblock: a fresh node, one that never changed view, or the
            // first boot after an upgrade. Production infers the view from the
            // last WAL prepare here, so mirror it -- otherwise the branch every
            // existing deployment takes has no sim coverage.
            consensus.set_view(header.view);
        }
        // Prior life is EITHER a non-empty WAL or a recovered superblock: a view
        // change persists without touching the WAL, so an empty journal no longer
        // proves a fresh boot. A clustered replica with a prior life rejoins as a
        // quorum-invisible probing backup, since the live primary re-forms its view
        // and re-commits any uncommitted suffix; a solo replica has no peer to ask,
        // so it inits and resumes as its own primary. Matches
        // restore_metadata_consensus exactly. Production's primary-only re-pipeline
        // of an uncommitted suffix is skipped, since a rejoining replica is always a
        // backup.
        let restored_head = restored_op.unwrap_or(0);
        if !solo && (restored_head > 0 || recovered_state.is_some() || has_checkpoint) {
            consensus.init_as_backup();
            consensus.begin_view_probe();
        } else {
            consensus.init();
        }
        if let (Some(journal), Some(head)) = (metadata_journal.as_ref(), restored_op) {
            let commit_watermark = journal.recovery_commit_watermark(solo).max(checkpoint_seq);
            consensus.sequencer().set_sequence(head);
            consensus.restore_commit_state(commit_watermark, commit_watermark);
            if let Some(header) = last_header {
                consensus.set_last_prepare_checksum(header.checksum);
                consensus.observe_prepare_timestamp(header.timestamp);
            }
            // The suffix past the committed watermark is prepared but not proven
            // committed, so gate reads until the cluster re-commits it, as production
            // does. Solo has no such suffix: the head IS the commit point.
            if commit_watermark < head {
                consensus.set_recovery_barrier(head);
            }
        }
        consensus
    });
    let metadata_snapshot = (shard_idx == 0).then(SimSnapshot::default);

    // A data directory arms the `SnapshotCoordinator`; without one
    // `checkpoint_if_needed` returns immediately and nothing ever checkpoints.
    let metadata = IggyMetadata::new(
        metadata_consensus,
        metadata_journal,
        metadata_snapshot,
        superblock,
        mux,
        data_dir,
    )
    .with_applied_frontier(applied_frontier);

    // Both halves are load-bearing: the pairing keeps a later view-change superblock
    // write from regressing to `(0, 0)`, and the folded table is the floor the replayed
    // suffix advances, so a session below the drained prefix keeps its watermark.
    if let Some((snapshot, checksum)) = local_checkpoint.as_ref() {
        metadata.seed_checkpoint_ref(snapshot.snapshot().sequence_number, *checksum);
        if let Some(table) = snapshot.snapshot().client_table.clone() {
            let capacity = metadata.client_table_capacity();
            let restored = ClientTable::from_snapshot(table, capacity)
                .expect("the local checkpoint's client table decodes");
            assert!(
                metadata.install_client_table(restored),
                "a freshly built plane holds no sessions, so this must install"
            );
        }
    }

    // BEFORE the replay, matching a fresh boot: `CreateStream::apply` takes
    // `vacant_key()`, so seeding after would put a restarted replica's `wl-*` streams
    // below its fillers, the reverse of every peer. Idempotent, so
    // `materialise_partition`'s own seed is then a no-op.
    if shard_idx == 0 {
        let streams = metadata.mux_stm.streams();
        for &(namespace, created_view) in seed_namespaces {
            streams.seed_namespace(namespace, namespace.inner(), created_view);
        }
    }

    // Reconstruct shard 0's committed metadata from the retained WAL, the sim analog
    // of production's snapshot + WAL replay. `apply_committed_prepare` is the SAME
    // apply path the commit walk uses, so it rebuilds BOTH the state machine and the
    // client table (sessions + at-most-once dedup), which a restart would otherwise
    // reset. The consensus setup above already restored `commit_min` to the committed
    // watermark; this only rebuilds derived state, and does nothing on a fresh boot.
    // The commit notifier is unwired during recovery, hence the no-op hook.
    if let Some(journal) = metadata.journal.as_ref() {
        let floor = journal.snapshot_op().max(checkpoint_seq);
        let commit_watermark = journal.recovery_commit_watermark(solo).max(checkpoint_seq);
        for op in (floor + 1)..=commit_watermark {
            let Some(entry) = journal.entry_sync(op) else {
                // Applying across a hole would replay effects onto a state machine
                // that never saw the missing op; the suffix is left for VSR repair.
                tracing::warn!(
                    replica_id,
                    op,
                    floor,
                    commit_watermark,
                    "metadata WAL has no entry at this committed op; stopping replay"
                );
                break;
            };
            // `true`: the frontier is volatile, so a rebuilt plane starts at zero and
            // every op above the floor must mutate the table. A frontier only fences a
            // LIVE table a state transfer just replaced.
            apply_committed_prepare(
                &metadata.mux_stm,
                &metadata.client_table,
                true,
                |_| {},
                entry,
            );
        }
    }
    // Same seed the server bootstrap does after its own replay: the frontier
    // resumes where the commit walk will, so a read on a rebuilt replica does
    // not park until its deadline. No-op on peer shards, which share the cell.
    if let Some(consensus) = metadata.consensus.as_ref() {
        metadata.advance_applied_frontier(consensus.commit_min());
    }
    // Mint the peers' read-side bundle AFTER reconstruction so it reflects the
    // recovered state. Shard 0 only; peers pass it back in as `reader_bundle`.
    let metadata_bundle = (shard_idx == 0).then(|| metadata.mux_stm.factory_bundle());

    let partitions_config = PartitionsConfig {
        messages_required_to_save: 1000,
        size_of_messages_required_to_save: IggyByteSize::from(4 * 1024 * 1024),
        enforce_fsync: false, //Disable fsync for simulation
        validate_checksum: true,
        segment_size: IggyByteSize::from(1024 * 1024 * 1024),
        preallocate_segments: false,
        encryptor: None,
        path_layout: PartitionPathLayout::default(),
    };

    // Shard id is the NODE-LOCAL shard index, never the replica id: the
    // router stamps `target_shard` with the mesh index and
    // `accept_frame_for_self` drops any frame whose stamp differs from
    // `IggyShard::id`. Replica identity lives in `name` and in the VSR
    // ids carried by `PartitionConsensusConfig`.
    let partitions = IggyPartitions::new(ShardId::new(shard_idx), partitions_config);

    // The deferred handlers upgrade this weak self-reference per frame; it
    // stays `None` until the shard is built and downgraded into it below.
    let shard_handle: ShellShardHandle<
        SharedSimOutbox,
        Rc<SimJournal<MemStorage>>,
        SimSnapshot,
        SimSuperblock,
    > = Rc::new(RefCell::new(None));
    let ShellHandlers {
        on_replica_message,
        on_client_request,
        on_metadata_submit,
        on_list_clients,
        on_partition_read,
        // Step 6 keeps this to register client sessions; unused shell-off.
        sessions: _,
    } = if shell {
        wire_shell_handlers(
            &SharedSimOutbox(Rc::clone(bus)),
            &shard_handle,
            Arc::new(ServerSystemConfig::default()),
            // Default-config PAT cap, like the system config above, so sim
            // ingress admits exactly what a default-configured server does.
            PersonalAccessTokenConfig::default().max_tokens_per_user,
        )
    } else {
        ShellHandlers::noop()
    };

    let shard = Rc::new(
        shard::IggyShard::new(
            shard::ShardIdentity::new(shard_idx, name),
            SharedSimOutbox(Rc::clone(bus)),
            on_replica_message,
            on_client_request,
            on_metadata_submit,
            on_list_clients,
            on_partition_read,
            metadata,
            partitions,
            senders,
            inbox,
            reply_inbox,
            PapayaShardsTable::new(),
            shard::PartitionConsensusConfig::with_clock(
                CLUSTER_ID,
                shard::ReplicaTopology::new(replica_id, replica_count),
                SharedSimOutbox(Rc::clone(bus)),
                clock,
            ),
            None,
            shard::metrics::ShardMetrics::for_shard(),
        )
        .expect("sim mesh senders are built in canonical order"),
    );

    // Late-bind the deferred handlers' self-reference. Harmless shell-off:
    // the no-ops never upgrade it.
    *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
    (shard, metadata_bundle)
}
