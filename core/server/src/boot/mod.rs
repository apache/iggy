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

//! Process entry and the per-shard boot narrative.
//!
//! `load_config` -> `prepare_runtime_dirs` -> [`bootstrap`] spawn one OS thread
//! per shard; each runs `shard_main`, the ordered sequence every boot
//! invariant hangs off (pump before listeners, barrier before bind, config
//! write after bind, systemd notify points). The narrative stays whole here;
//! the leaves hold the support it calls into.

mod credentials;
mod handoff;
mod listeners;
mod recovery;
#[cfg(feature = "systemd")]
pub mod systemd;
mod threads;
mod topology;

pub use credentials::apply_default_root_credentials;
pub use threads::ShardHandles;

use crate::boot::credentials::{
    ensure_default_root_user, load_replica_auth, load_replica_tls_ctx,
    validate_root_credentials_env,
};
use crate::boot::handoff::{
    BootstrapBarrier, MetadataHandoff, await_bootstrap_complete, await_metadata_bundle,
    broadcast_metadata_bundle, signal_bootstrap_complete,
};
use crate::boot::listeners::{
    make_replica_delegation_fns, make_shard_zero_client_accept_fns, start_tcp_runtime,
};
use crate::boot::recovery::{
    RecoveredOwnerState, build_shard_for_thread, restore_metadata_consensus,
};
use crate::boot::threads::{
    StopSignals, await_pump_drain, install_panic_hook, join_partial_shard_survivors,
    resolve_shard_assignments, run_shard_thread, spawn_shutdown_watchdog,
    validate_sharding_runtime_knobs,
};
use crate::boot::topology::{RosterCells, resolve_tcp_topology};
use crate::dispatch::partition::make_partition_read_handler;
use crate::dispatch::session_ops::warm_dummy_password_hash;
use crate::dispatch::submit::make_metadata_submit_handler;
use crate::dispatch::{
    make_client_request_handler, make_deferred_client_request_handler,
    make_deferred_replica_message_handler, make_list_clients_handler,
};
use crate::server_error::ServerError;
use crate::session_manager::SessionManager;
use crate::shell::{
    ServerMetadata, ServerMetadataBundle, ServerMuxStateMachine, ShellBus, ShellHandlers,
    ShellShardHandle,
};
use configs::server::{ServerConfig, ServerSystemConfig};
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::{Operation, PrepareHeader};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::replica::handshake::ReplicaHandshakeCtx;
use message_bus::transports::tls::install_default_crypto_provider;
use message_bus::{IggyMessageBus, ReplicaOwnerTable};
use metadata::ReplicaIdentity;
use metadata::impls::metadata::StreamsFrontend;
use metadata::impls::recovery::recover;
use server_common::Message;
use server_common::bootstrap::create_directories;
use server_common::fs_utils::remove_dir_all;
use server_common::log::{Logging, LoggingSettings, TelemetrySettings};
use shard::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};
use shard::{
    LifecycleFrame, Receiver as ShardReceiver, ShardFrame, TaggedSender, channel,
    shard_mesh_channels,
};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use tracing::{error, info, warn};

/// Build the deferred dispatch handlers for `shard_handle` against `bus`.
///
/// They share one fresh [`SessionManager`]. The caller must set the weak
/// self-reference in `shard_handle` once the shard is built, so the
/// handlers can upgrade it per frame.
pub fn wire_shell_handlers<B, MJ, S, SB>(
    bus: &B,
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
    system_config: Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
) -> ShellHandlers
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let sessions = Rc::new(RefCell::new(SessionManager::new()));
    ShellHandlers {
        on_replica_message: make_deferred_replica_message_handler(shard_handle),
        on_client_request: make_deferred_client_request_handler(
            bus,
            shard_handle,
            &sessions,
            system_config,
            max_tokens_per_user,
        ),
        on_metadata_submit: make_metadata_submit_handler(shard_handle),
        on_list_clients: make_list_clients_handler(&sessions),
        on_partition_read: make_partition_read_handler(shard_handle),
        sessions,
    }
}

/// Load the server configuration from the active config provider.
///
/// # Errors
///
/// Returns an error if the configuration cannot be read or parsed.
pub async fn load_config() -> Result<ServerConfig, ServerError> {
    ServerConfig::load().await.map_err(ServerError::Config)
}

/// Prepare the on-disk layout the server boots from and complete late
/// logging init.
///
/// `fresh` wipes the system path first: `late_init` opens a rolling
/// appender under `{system_path}/logs` and `create_directories`
/// materialises exactly what the wipe is meant to remove, so both have to
/// run after it.
///
/// # Errors
///
/// Returns an error if the wipe, directory preparation, or logging setup
/// fails.
pub async fn prepare_runtime_dirs(
    config: &ServerConfig,
    logging: &mut Logging,
    fresh: bool,
) -> Result<(), ServerError> {
    if fresh {
        wipe_system_path(config).await?;
    }
    create_directories(&config.system).await.map_err(|source| {
        error!(
            system_path = %config.system.get_system_path(),
            error = %source,
            "failed to prepare server directories"
        );
        source
    })?;
    logging
        .late_init(
            config.system.get_system_path(),
            &LoggingSettings::from(&config.system.logging),
            &TelemetrySettings::from(&config.telemetry),
        )
        .map_err(ServerError::Logging)?;

    Ok(())
}

/// Delete the configured system path so the server boots on empty state.
async fn wipe_system_path(config: &ServerConfig) -> Result<(), ServerError> {
    let path = config.system.get_system_path();
    // `system.path` is relative by default and IGGY_SYSTEM_PATH-overridable,
    // so report what is actually about to be deleted, not what was configured.
    let resolved = std::path::absolute(&path).unwrap_or_else(|_| PathBuf::from(&path));

    if config.cluster.enabled {
        warn!(
            path = %resolved.display(),
            "--fresh wipes only this replica, which then refills from the cluster by \
             state transfer; wiping a quorum at once destroys committed data, and a \
             service unit file carrying --fresh re-transfers everything on every restart"
        );
    }

    if !Path::new(&path).exists() {
        info!(path = %resolved.display(), "--fresh: system path does not exist, nothing to remove");
        return Ok(());
    }

    warn!(path = %resolved.display(), "--fresh: removing the system path, ALL local data will be deleted");
    // A half-removed directory is worse than no removal at all: the surviving
    // superblock and snapshot no longer pair up, and boot would report the
    // leftovers as a durability violation rather than as a failed wipe.
    remove_dir_all(&path)
        .await
        .map_err(|source| ServerError::FreshWipeFailed {
            path: resolved,
            source,
        })
}

/// Spawn the multi-shard `server` runtime.
///
/// Resolves shard count + CPU affinities from
/// `system.sharding.cpu_allocation`, builds canonical-ordered
/// `(senders, inboxes)` channels, and spawns one OS thread per shard.
///
/// Each thread pins itself (`nix::sched::sched_setaffinity` on Linux via
/// `ShardInfo::bind_cpu`), binds memory to its NUMA node when
/// configured, builds a fresh `compio::runtime::Runtime` (one
/// `io_uring` instance per shard), and runs `shard_main` inside it.
///
/// Returns [`ShardHandles`] containing the cross-thread shutdown flag
/// and the per-shard `JoinHandle`s. The caller (`main.rs`) installs a
/// `ctrlc` handler that flips the flag, then `.join()`s every handle.
///
/// # Errors
///
/// Returns an error if shard allocation fails, the inbox capacity is
/// invalid, or any OS thread fails to spawn. Per-shard recovery /
/// listener / consensus failures surface through the per-thread `Result`
/// the caller observes on `.join()`.
///
/// # Panics
///
/// Panics if [`shard_mesh_channels`] returns an inbox slot already
/// consumed - a bootstrap programming error that would only fire if this
/// function were called twice with the same inboxes.
#[allow(clippy::too_many_lines)]
pub fn bootstrap(
    config: ServerConfig,
    current_replica_id: Option<u8>,
) -> Result<ShardHandles, ServerError> {
    // One process-wide rustls provider, installed before any shard thread
    // exists. rustls is compiled with both `ring` and `aws-lc-rs`, so a
    // `ServerConfig` / `ClientConfig` builder reached before this line panics
    // ("could not determine process-level CryptoProvider") instead of picking
    // one; every TLS surface (client listeners, the replica mesh, HTTP
    // forwarding) resolves the default this call sets. message_bus keeps its
    // own idempotent install in its TLS listeners for embedders that never
    // run this bootstrap; after this line those are no-ops.
    install_default_crypto_provider();
    validate_root_credentials_env(&config)?;
    warm_dummy_password_hash();
    // The sync GetStats read path has no access to server config, so capture
    // the data directory here for its disk-usage reporting.
    crate::responses::init_stats_data_path(config.system.get_system_path().into());
    let (assignments, total_shards) = resolve_shard_assignments(&config.system.sharding)?;
    let shards_count = assignments.len();

    // Re-check the full valid range, not just the zero floor: a caller
    // that built the config without running `ShardingConfig::validate`
    // would otherwise OOM at boot allocating an oversized inbox channel,
    // busy-loop every shutdown watchdog on a zero poll cadence, or wedge
    // process exit on an unbounded drain budget.
    let inbox_capacity = config.system.sharding.inbox_capacity;
    let reply_inbox_capacity = config.system.sharding.reply_inbox_capacity;
    validate_sharding_runtime_knobs(&config.system.sharding)?;

    let (senders, mut inboxes, mut reply_inboxes) =
        shard_mesh_channels(total_shards, inbox_capacity, reply_inbox_capacity);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    // Before the first shard thread exists, so no panic on a shard, in the
    // thread body or in a task compio's `spawn` would swallow, escapes it.
    let first_panic = install_panic_hook(Arc::clone(&shutdown_flag));
    let config = Arc::new(config);
    // One owner table per server process, Arc-cloned into every shard's bus so
    // any shard's bus reads the same atomic slots that the owning
    // shard's installer / disconnect path writes.
    let owner_table = Arc::new(ReplicaOwnerTable::new());

    // Single-shot bundle handoff (see `MetadataHandoff`): shard 0 sends
    // one cloned `ServerMetadataBundle` per peer; each peer drains
    // exactly one. Bounded to the peer count so shard 0's broadcast
    // never blocks past a peer drain. A single-shard deployment (zero
    // peers) still needs a non-zero capacity, so clamp up explicitly
    // rather than relying on crossfire's internal cap=0 -> 1 promotion.
    // If a peer dies before recv, shard 0's `send` eventually sees a
    // disconnected channel; the cross-thread shutdown flag drives every
    // waiter out of its recv loop if shard 0 panics before broadcasting.
    let metadata_peers = shards_count.saturating_sub(1).max(1);
    let (metadata_bundle_tx, metadata_bundle_rx) =
        crossfire::mpmc::bounded_async::<ServerMetadataBundle>(metadata_peers);

    // Reverse barrier (see `BootstrapBarrier`): every peer sends one
    // signal once it finishes loading its on-disk partitions; shard 0
    // drains them all before binding listeners. Bounded to the peer
    // count so a sender never blocks (each peer sends exactly once).
    let (ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(metadata_peers);

    let mut shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerError>>)> =
        Vec::with_capacity(shards_count);
    let roster_cells = RosterCells::default();
    // Shared applied-metadata frontier: shard 0's commit path advances it, every
    // shard's read gate reads it. Minted here, before any shard exists, because
    // a shard holding a private cell would gate reads on a number nothing moves.
    let metadata_applied_frontier = Arc::new(AtomicU64::new(0));
    // Every shard's metric handles, minted before the threads spawn: each
    // shard bumps its own entry, and shard 0's HTTP scrape endpoint registers
    // the whole set (counters are Arc-backed, so cross-thread reads see the
    // owning shard's bumps).
    let shard_metrics_all: Vec<ShardMetrics> = (0..shards_count)
        .map(|_| ShardMetrics::for_shard())
        .collect();
    for (idx, assignment) in assignments.into_iter().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let shard_id = idx as u16;
        let inbox = inboxes[idx]
            .take()
            .expect("shard_mesh_channels populates every inbox slot exactly once");
        let reply_inbox = reply_inboxes[idx]
            .take()
            .expect("shard_mesh_channels populates every reply-inbox slot exactly once");
        let senders_for_shard = senders.clone();
        let config_for_shard = Arc::clone(&config);
        let shutdown_flag_for_shard = Arc::clone(&shutdown_flag);
        let owner_table_for_shard = Arc::clone(&owner_table);
        let metadata_handoff_for_shard = if shard_id == 0 {
            MetadataHandoff::Owner {
                bundle_tx: metadata_bundle_tx.clone(),
            }
        } else {
            MetadataHandoff::Waiter {
                bundle_rx: metadata_bundle_rx.clone(),
            }
        };
        let barrier_for_shard = if shard_id == 0 {
            BootstrapBarrier::Owner {
                ready_rx: ready_rx.clone(),
            }
        } else {
            BootstrapBarrier::Waiter {
                ready_tx: ready_tx.clone(),
            }
        };

        let roster_cells_for_shard = roster_cells.clone();
        let applied_frontier_for_shard = Arc::clone(&metadata_applied_frontier);
        let shard_metrics_for_shard = shard_metrics_all.clone();
        let handle = match thread::Builder::new()
            .name(format!("shard-{shard_id}"))
            .spawn(move || -> Result<(), ServerError> {
                run_shard_thread(
                    shard_id,
                    total_shards,
                    current_replica_id,
                    assignment,
                    senders_for_shard,
                    inbox,
                    reply_inbox,
                    config_for_shard,
                    shutdown_flag_for_shard,
                    metadata_handoff_for_shard,
                    barrier_for_shard,
                    owner_table_for_shard,
                    roster_cells_for_shard,
                    applied_frontier_for_shard,
                    shard_metrics_for_shard,
                )
            }) {
            Ok(handle) => handle,
            Err(source) => {
                // Signal every shard already spawned before propagating, so
                // their watchdog loops drive `bus.shutdown(...)` and the
                // process can exit instead of hanging on stuck OS threads.
                shutdown_flag.store(true, Ordering::Relaxed);
                // Drop bootstrap's own channel clones before joining
                // survivors. Otherwise a peer waiting on `bundle_rx.recv`
                // would never observe the sender side disconnecting and
                // would hang until the shutdown watchdog kicks the bus.
                drop(metadata_bundle_tx);
                drop(metadata_bundle_rx);
                drop(ready_tx);
                drop(ready_rx);
                join_partial_shard_survivors(
                    shard_threads,
                    config.system.sharding.shutdown_join_timeout.get_duration(),
                );
                return Err(ServerError::ShardSpawnFailed { shard_id, source });
            }
        };
        shard_threads.push((shard_id, handle));
    }

    // Drop bootstrap's own channel clones now that every shard owns its
    // half. Keeping them on bootstrap's stack would deadlock a peer
    // whose `bundle_rx.recv` only completes once every sender
    // disconnects.
    drop(metadata_bundle_tx);
    drop(metadata_bundle_rx);
    drop(ready_tx);
    drop(ready_rx);

    info!(
        shards_count,
        "server bootstrap dispatched; awaiting shard runtimes"
    );

    Ok(ShardHandles {
        shutdown_flag,
        shard_threads,
        join_timeout: config.system.sharding.shutdown_join_timeout.get_duration(),
        first_panic,
    })
}

/// Per-shard async lifecycle. Builds the bus, recovers metadata,
/// constructs the `IggyShard` for this shard's slice of partitions,
/// wires listeners on shard 0, and runs the message pump until
/// shutdown.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn shard_main(
    shard_id: u16,
    total_shards: u16,
    replica_id: Option<u8>,
    senders: Vec<TaggedSender>,
    inbox: ShardReceiver<ShardFrame>,
    reply_inbox: ShardReceiver<ShardFrame>,
    config: &ServerConfig,
    shutdown_flag: Arc<AtomicBool>,
    metadata_handoff: MetadataHandoff,
    barrier: BootstrapBarrier,
    owner_table: Arc<ReplicaOwnerTable>,
    roster_cells: RosterCells,
    metadata_applied_frontier: Arc<AtomicU64>,
    shard_metrics_all: Vec<ShardMetrics>,
) -> Result<(), ServerError> {
    let topology = resolve_tcp_topology(config, replica_id)?;
    let bus = Rc::new(IggyMessageBus::with_config_and_owner_table(
        shard_id,
        config,
        owner_table,
    ));
    // Every shard can own a delegated replica connection, so every
    // shard's bus needs the handshake identity (the handshake itself
    // runs on the owning shard, not on shard 0).
    bus.set_replica_handshake_ctx(ReplicaHandshakeCtx {
        cluster_id: topology.cluster_id,
        self_id: topology.self_replica_id,
        replica_count: topology.replica_count,
        auth: load_replica_auth(config).map(Rc::new),
        tls: load_replica_tls_ctx(config, &topology)?.map(Rc::new),
    });

    let drain_timeout = config.system.sharding.shutdown_drain_timeout.get_duration();
    let poll_interval = config.system.sharding.shutdown_poll_interval.get_duration();

    let shutdown_flag_for_handoff = Arc::clone(&shutdown_flag);
    let mut shutdown_watchdog = Some(spawn_shutdown_watchdog(
        Rc::clone(&bus),
        shutdown_flag,
        drain_timeout,
        poll_interval,
    ));

    // Metadata bootstrap is single-writer: shard 0 owns the WAL and the
    // only `WriteHandle`-bearing `MuxStateMachine`. Peer shards receive
    // a `ReadHandleFactory` bundle on the inter-thread channel and
    // rebuild a reader-mode `MuxStateMachine` on their own runtime - no
    // WAL access, no replay. Writes still funnel through shard 0's
    // metadata VSR; per-commit `publish()` (in `WriteCell::apply`)
    // bounds reader staleness to one op.
    let data_dir = Path::new(&config.system.path);
    let (mux_stm, owner_state) = match metadata_handoff {
        MetadataHandoff::Owner { bundle_tx } => {
            // Root is created locally at boot (never journaled), so replay
            // must start from the same baseline or every WAL-created user
            // shifts one slab id and root is lost after the first restart.
            let recovered = recover::<ServerMuxStateMachine>(
                data_dir,
                ReplicaIdentity {
                    cluster: topology.cluster_id,
                    replica_id: topology.self_replica_id,
                    replica_count: topology.replica_count,
                },
                config.metadata.journal_slots,
                config.metadata.clients_table_max,
                |mux_stm| {
                    ensure_default_root_user(mux_stm);
                },
                |mux_stm, client, stamp| {
                    mux_stm
                        .streams()
                        .remove_consumer_group_member(client, stamp);
                },
            )
            .await
            .map_err(ServerError::MetadataRecovery)?;
            ensure_default_root_user(&recovered.mux_stm);
            // The factory bundle hands every peer a read handle over the
            // same `Inner`, so `Arc<TopicStats>` (and the parent
            // `Arc<StreamStats>`) is shared across all shards. Zero the
            // snapshot totals here, once, before any peer can observe the
            // bundle. Per-shard `load_partition` deltas in
            // `build_shard_for_thread` then race only against other
            // atomic adds, never against a concurrent `swap(0)` that
            // would mistake an in-flight delta for the snapshot total
            // and decrement the parent `StreamStats` by it.
            let () = recovered.mux_stm.streams().read(|inner| {
                for (_, stream) in &inner.items {
                    for (_, topic) in &stream.topics {
                        topic.stats.zero_out_all();
                    }
                }
            });
            broadcast_metadata_bundle(
                shard_id,
                &bundle_tx,
                recovered.mux_stm.factory_bundle(),
                total_shards.saturating_sub(1),
                &shutdown_flag_for_handoff,
                poll_interval,
            )
            .await?;
            (
                recovered.mux_stm,
                Some(RecoveredOwnerState {
                    journal: recovered.journal,
                    snapshot: recovered.snapshot,
                    last_applied_op: recovered.last_applied_op,
                    last_journaled_op: recovered.last_journaled_op,
                    client_table: recovered.client_table,
                    superblock: recovered.superblock,
                    recovered_state: recovered.recovered_state,
                    snapshot_checkpoint: recovered.snapshot_checkpoint,
                }),
            )
        }
        MetadataHandoff::Waiter { bundle_rx } => {
            let bundle = await_metadata_bundle(
                shard_id,
                &bundle_rx,
                &shutdown_flag_for_handoff,
                poll_interval,
            )
            .await?;
            (ServerMuxStateMachine::from_factory_bundle(bundle), None)
        }
    };

    // Metadata consensus + journal + snapshot live only on shard 0.
    // `IggyShard::tick_metadata` short-circuits when `consensus.is_none()`,
    // so peer shards have no caller that reads `journal` or `snapshot`.
    let (
        metadata_consensus,
        journal_for_metadata,
        snapshot_for_metadata,
        superblock_for_metadata,
        checkpoint_seed,
        recovered_client_table,
    ) = if let Some(owner) = owner_state {
        // `recover()` already opened the superblock, read `recovered_state`, and
        // verified the on-disk snapshot against its checkpoint pairing BEFORE decoding
        // it. Reuse that superblock rather than re-opening it, which would fork the
        // ping-pong sequence counter. Consensus recovers its true (view, log_view)
        // from `recovered_state` instead of inferring a stale view from the WAL.
        let consensus = restore_metadata_consensus(&owner, &topology, config, Rc::clone(&bus));
        let superblock = Rc::new(owner.superblock);
        (
            Some(consensus),
            Some(owner.journal),
            owner.snapshot,
            Some(superblock),
            owner.snapshot_checkpoint,
            Some(owner.client_table),
        )
    } else {
        (None, None, None, None, (0, 0), None)
    };
    let metadata = ServerMetadata::new(
        metadata_consensus,
        journal_for_metadata,
        snapshot_for_metadata,
        superblock_for_metadata,
        mux_stm,
        Some(PathBuf::from(&config.system.path)),
    )
    .with_applied_frontier(metadata_applied_frontier);
    // Recovery already replayed the committed WAL prefix into the state
    // machine, so the frontier resumes where the commit walk will rather than
    // at zero -- otherwise every read on a rebooted node parks until its
    // deadline. No-op on peer shards, which share shard 0's cell.
    if let Some(consensus) = metadata.consensus.as_ref() {
        metadata.advance_applied_frontier(consensus.commit_min());
    }
    // Size the VSR client table before listeners bind and any client registers.
    // Must precede the recovered-table install below: the setter rebuilds the
    // table from scratch, so running it afterwards would drop every resumed
    // session (and trip its empty-table assert).
    metadata.set_clients_table_max(config.metadata.clients_table_max);
    // Reinstall the sessions recovery restored from the checkpoint and the WAL
    // suffix, so a rebooted node dedups retries and admits continuations from
    // clients that kept their identity across the restart (IGGY-137). Recovery
    // sized this table from the same config value, so the install preserves the
    // configured cap.
    if let Some(client_table) = recovered_client_table {
        // Refusal (a client registered before this ran) keeps the live table
        // and is logged by the callee; boot continues either way.
        let _ = metadata.install_client_table(client_table);
    }
    // Seed the coordinator's last-checkpoint pairing so the first post-boot
    // view-change superblock write records the real (checkpoint_op, checksum)
    // instead of (0, 0). No-op on peer shards, which have no coordinator.
    metadata.seed_checkpoint_ref(checkpoint_seed.0, checkpoint_seed.1);
    // Keep the forced-checkpoint margin >= the configured prepare-queue
    // depth: ops already pipelined while a checkpoint runs append into that
    // margin (config validation keeps journal_slots >= 4x this).
    metadata.set_checkpoint_margin(config.metadata.checkpoint_margin());

    let shard_metrics = shard_metrics_all[usize::from(shard_id)].clone();
    // Notifier install deferred until after tick handler wires below.
    let senders_for_notifier = senders.clone();
    let metrics_for_notifier = shard_metrics.clone();
    // Heap-pin like `run_shard_thread` pins `shard_main`: the builder future
    // carries the whole shard construction state machine and outgrew clippy's
    // `large_futures` cap; one allocation per shard startup.
    let (shard, sessions) = Box::pin(build_shard_for_thread(
        shard_id,
        total_shards,
        config,
        &topology,
        metadata,
        Rc::clone(&bus),
        senders,
        inbox,
        reply_inbox,
        shard_metrics,
        &roster_cells,
    ))
    .await?;

    // Shard 0 owns the metadata consensus; publish its view so every shard's
    // cluster-metadata read (and the SDK's leader discovery) marks the live
    // primary. Detached: dies with this shard's runtime at process exit.
    if shard_id == 0 {
        let publisher_shard = Rc::clone(&shard);
        let publisher_view = Arc::clone(&roster_cells.metadata_view);
        compio::runtime::spawn(async move {
            loop {
                if let Some(consensus) = publisher_shard.plane.metadata().consensus.as_ref() {
                    // While this replica declines its recovered view's
                    // primaryship, that view must not reach the roster: the
                    // delegated shards would compute a leader that never
                    // heartbeats. Publish "unknown" until the election
                    // resolves the role.
                    let published = if consensus.has_ceded_primaryship()
                        && consensus.primary_index(consensus.view()) == consensus.replica()
                    {
                        crate::cluster_meta::METADATA_VIEW_UNKNOWN
                    } else {
                        u64::from(consensus.view())
                    };
                    publisher_view.store(published, Ordering::Relaxed);
                }
                compio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .detach();
    }

    info!(
        shard = shard_id,
        partitions = shard.plane.partitions().len(),
        "server shard initialized"
    );

    // Re-check the cross-thread shutdown flag here, *before* spawning the
    // message pump: it keeps the bus' `background_tasks` vec empty on the
    // shutdown path, and shard 0 would otherwise still open TCP/QUIC/WS
    // listeners for a server that is already tearing down, briefly
    // accepting connections that immediately get torn by the watchdog.
    //
    // The flag is set, so the watchdog is (about to be) driving
    // `bus.shutdown()`; await it so the runtime does not drop mid-drain.
    if shutdown_flag_for_handoff.load(Ordering::Relaxed) {
        if let Some(watchdog) = shutdown_watchdog.take() {
            let _ = watchdog.await;
        }
        return Ok(());
    }

    // Tick handler must install before the notifier so early commits
    // do not broadcast ticks whose handler slot is still `None`.
    let (reconcile_wake_tx, reconcile_wake_rx) = channel::<()>(1);
    let (reconcile_stop_tx, reconcile_stop_rx) = channel::<()>(1);
    crate::partition_reconciler::install_tick_handler(&shard, reconcile_wake_tx);

    // Only shard 0 commits metadata.
    if shard_id == 0 {
        let notifier = make_metadata_commit_notifier(senders_for_notifier, metrics_for_notifier);
        shard.plane.metadata().set_commit_notifier(Some(notifier));
    } else {
        drop(senders_for_notifier);
        drop(metrics_for_notifier);
    }

    // The pump task also drives the consensus timer tick (heartbeats, prepare
    // retransmit, view-change timeouts) as a select! arm, serialized with frame
    // processing - see `run_message_pump`.
    let (stop_tx, stop_rx) = channel(1);
    let pump_shard = Rc::clone(&shard);
    // Owned and awaited by shard_main at exit, NOT `track_background`: the
    // background drain runs inside `bus.shutdown()`, which the Ctrl-C path
    // never drives (the watchdog stands down when the token fires), so a
    // tracked pump would be cancelled by runtime teardown mid final-flush
    // and every graceful shutdown would silently drop the committed journal
    // tail that had not hit a flush threshold yet.
    let pump_shutdown_flag = Arc::clone(&shutdown_flag_for_handoff);
    let mut pump_handle = Some(compio::runtime::spawn(async move {
        // The pump itself flips the shared flag when a commit fault stops it,
        // BEFORE its final flush, so a flush stalling on the failed device
        // still reaches the watchdog and the bounded drain. Every sibling
        // shard's watchdog drives its own graceful stop off the same flag;
        // this shard's watchdog is what fires the token `shard_main` is
        // parked on. The store below backstops the one fault the pump can
        // only observe after that flip: a partition fenced by the final
        // flush itself.
        let fatal = pump_shard
            .run_message_pump(stop_rx, Arc::clone(&pump_shutdown_flag))
            .await;
        if fatal.is_some() {
            pump_shutdown_flag.store(true, Ordering::Relaxed);
        }
        fatal
    }));

    let reconciler_ctx = Rc::new(crate::partition_reconciler::ReconcilerCtx::new(
        Rc::clone(&shard),
        total_shards,
        Rc::new(config.clone()),
        topology.cluster_id,
        topology.self_replica_id,
        topology.replica_count,
    ));
    let reconcile_periodic = config
        .system
        .sharding
        .reconcile_periodic_interval
        .get_duration();
    let reconciler_handle = compio::runtime::spawn({
        let ctx = Rc::clone(&reconciler_ctx);
        async move {
            crate::partition_reconciler::run_reconciler(
                ctx,
                reconcile_wake_rx,
                reconcile_stop_rx,
                reconcile_periodic,
            )
            .await;
        }
    });
    bus.track_background(reconciler_handle);

    // Per-shard heartbeat verifier: evicts connections that stop pinging,
    // releasing their consumer-group membership. Gated on config so a
    // deployment without heartbeats never reaps live sessions.
    let heartbeat_stop_tx = if config.heartbeat.enabled {
        let (hb_stop_tx, hb_stop_rx) = channel::<()>(1);
        let hb_shard = Rc::clone(&shard);
        let hb_sessions = Rc::clone(&sessions);
        let hb_interval = config.heartbeat.interval.get_duration();
        let hb_handle = compio::runtime::spawn(async move {
            crate::dispatch::session_ops::run_heartbeat_verifier(
                hb_shard,
                hb_sessions,
                hb_interval,
                hb_stop_rx,
            )
            .await;
        });
        bus.track_background(hb_handle);
        Some(hb_stop_tx)
    } else {
        None
    };
    // Expired-PAT cleaner: shard 0 only (it owns the metadata consensus
    // group) and only when enabled. Each pass no-ops unless this node is
    // the caught-up metadata primary, so the delete is proposed once and
    // replicated to every replica.
    let pat_cleaner_stop = if shard_id == 0 && config.personal_access_token.cleaner.enabled {
        let (cleaner_stop_tx, cleaner_stop_rx) = channel(1);
        let cleaner_shard = Rc::clone(&shard);
        let interval = config.personal_access_token.cleaner.interval.get_duration();
        let cleaner_handle = compio::runtime::spawn(async move {
            crate::personal_access_token_cleaner::run_pat_cleaner(
                cleaner_shard,
                cleaner_stop_rx,
                interval,
            )
            .await;
        });
        bus.track_background(cleaner_handle);
        Some(cleaner_stop_tx)
    } else {
        None
    };

    // Segment cleaner: runs on every shard (each replica trims its own log,
    // primary and backup alike). Local and unreplicated; gated by the shared
    // data-maintenance config.
    let segment_cleaner_stop = if config.data_maintenance.messages.cleaner_enabled {
        let (stop_tx, stop_rx) = channel(1);
        let cleaner_shard = Rc::clone(&shard);
        let interval = config.data_maintenance.messages.interval.get_duration();
        let cleaner_handle = compio::runtime::spawn(async move {
            crate::segment_cleaner::run_segment_cleaner(cleaner_shard, stop_rx, interval).await;
        });
        bus.track_background(cleaner_handle);
        Some(stop_tx)
    } else {
        None
    };
    let stop_signals = StopSignals {
        pump: stop_tx,
        reconciler: reconcile_stop_tx,
        heartbeat: heartbeat_stop_tx,
        pat_cleaner: pat_cleaner_stop,
        segment_cleaner: segment_cleaner_stop,
    };

    // One keep-alive per process, so shard 0 owns it. Started before the
    // listeners bind: systemd counts `WatchdogSec=` from unit start, not from
    // `READY=1`, so a slow recovery must not look like a hang.
    #[cfg(feature = "systemd")]
    if shard_id == 0 {
        systemd::spawn_watchdog(&bus);
    }

    // Listener fence (see `BootstrapBarrier`). Peers still scan live
    // shared metadata and load their on-disk partitions in
    // `build_shard_for_thread`; the factory-bundle handoff only proves
    // they *received* the bundle, not that they finished loading. Shard
    // 0 must not accept client traffic until every peer's load scan is
    // done, otherwise a partition created by the first client surfaces
    // in a still-running scan with no segment dir on disk and aborts the
    // node with `CannotReadPartitions`. By this point every shard has
    // also spawned its pump + reconciler, so a partition created after
    // the fence takes the runtime reconciler path on its owning shard.
    match barrier {
        BootstrapBarrier::Owner { ready_rx } => {
            await_bootstrap_complete(
                &ready_rx,
                usize::from(total_shards.saturating_sub(1)),
                &shutdown_flag_for_handoff,
                poll_interval,
            )
            .await?;
        }
        BootstrapBarrier::Waiter { ready_tx } => {
            signal_bootstrap_complete(
                shard_id,
                &ready_tx,
                &shutdown_flag_for_handoff,
                poll_interval,
            )
            .await?;
        }
    }

    // Listeners (replica + every client transport) bind on shard 0 only.
    // Shard 0's coordinator round-robins inbound TCP/WS connections to
    // peer shards via fd-transfer. QUIC and TCP-TLS clients terminate
    // locally on shard 0 (their per-connection state is non-portable -
    // see `LifecycleFrame::ClientWsConnectionSetup` rustdoc).
    if shard_id == 0 {
        let coord = shard
            .coordinator()
            .expect("shard 0 always has a coordinator attached by the builder");
        // Reseed the client-id minter above every recovered entry before any
        // listener accepts. The counter is per process; the table it must not
        // collide with was rebuilt from the previous boot's WAL. Keyed by view
        // so a later promotion refolds the table (the minting path calls the
        // same method, see `HttpInner::register_session_once`).
        let boot_view = shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .map_or(0, consensus::VsrConsensus::view);
        coord.seed_client_sequence(
            boot_view,
            shard.plane.metadata().client_table.borrow().client_ids(),
        );
        let on_client_request = make_client_request_handler(
            &shard,
            &sessions,
            Arc::clone(&config.system),
            config.personal_access_token.max_tokens_per_user,
        );
        let (accepted_replica, dialed_replica) =
            make_replica_delegation_fns(Rc::clone(&coord), &bus);
        let accepted_client = make_shard_zero_client_accept_fns(coord, &bus, on_client_request);
        let roster = sessions.borrow().cluster_roster();

        if let Err(error) = start_tcp_runtime(
            &shard,
            config,
            &topology,
            roster,
            accepted_replica,
            dialed_replica,
            accepted_client,
            &shard_metrics_all,
        )
        .await
        {
            stop_signals.fire();
            // The bind failure is the primary fault; the drain verdict only
            // matters for the log it emits.
            let _ = await_pump_drain(pump_handle.take(), config, shard_id).await;
            // Neither the flag nor the bus token has fired yet on this path,
            // so the watchdog is still idle-looping; awaiting it would hang.
            // Detach and let `run_shard_thread`'s unwind flip the flag.
            if let Some(watchdog) = shutdown_watchdog.take() {
                watchdog.detach();
            }
            return Err(error);
        }

        // Every enabled client transport is bound and accepting by here, so
        // this is the first point at which a unit ordered after us may dial.
        #[cfg(feature = "systemd")]
        systemd::notify_ready();
    }

    bus.token().wait().await;
    #[cfg(feature = "systemd")]
    if shard_id == 0 {
        systemd::notify_stopping();
    }
    stop_signals.fire();

    // Await the watchdog even when the drain verdict is an error: the token
    // has fired, so it either stands down within one poll interval or is
    // mid-`bus.shutdown()`, and dropping it there truncates in-flight
    // `ClientForwardFailed` replies.
    let pump_verdict = await_pump_drain(pump_handle.take(), config, shard_id).await;
    if let Some(watchdog) = shutdown_watchdog.take() {
        let _ = watchdog.await;
    }
    pump_verdict?;

    info!(shard = shard_id, "server shard exited cleanly");
    Ok(())
}

/// Build the closure that broadcasts a
/// [`LifecycleFrame::MetadataCommitTick`] to every shard's inbox after a
/// partition-shaped metadata operation commits on shard 0.
///
/// The receiver-side partition reconciliation loop listens for these
/// wake-ups; coalescing is intentional, so `Full` is recorded as a metric
/// and dropped (the periodic tick recovers). Installed via
/// [`metadata::IggyMetadata::set_commit_notifier`] on shard 0 only, the
/// sole writer of the metadata state machine.
fn make_metadata_commit_notifier(
    senders: Vec<TaggedSender>,
    metrics: ShardMetrics,
) -> metadata::CommitNotifier {
    Rc::new(move |operation: Operation| {
        if !operation_triggers_partition_reconcile(operation) {
            return;
        }
        for sender in &senders {
            let frame = ShardFrame::lifecycle(LifecycleFrame::MetadataCommitTick);
            match sender.try_send(frame) {
                Ok(()) => {}
                Err(crossfire::TrySendError::Full(_)) => {
                    metrics.record_frame_drop(
                        frame_drop_variant::METADATA_COMMIT_TICK,
                        frame_drop_reason::FULL,
                    );
                }
                Err(crossfire::TrySendError::Disconnected(_)) => {
                    metrics.record_frame_drop(
                        frame_drop_variant::METADATA_COMMIT_TICK,
                        frame_drop_reason::DISCONNECTED,
                    );
                }
            }
        }
    })
}

/// Filter at the broadcast site, keeping unrelated ops off the SDK reply
/// path. Any new partition-shape op must be added here.
///
/// The bare `CreateTopic` / `CreatePartitions` arms are unreachable: the
/// leader's prepare-builder in `IggyMetadata` rewrites both into their
/// `*WithAssignments` form, stamping each partition's `consensus_group_id`
/// before journaling, so a committed prepare only ever carries the
/// assignment-bearing variant. Kept as defense-in-depth against a future
/// commit path that emits a bare op.
///
/// "Partition-shape" is not only the partition SET: the purge and truncate
/// ops leave the set intact but advance per-partition state (purge
/// generation, delete watermark) that only the reconciler enforces on disk.
/// Omitting them defers the on-disk effect to the periodic safety tick,
/// stretching a purge's client-visible tail to a full
/// `reconcile_periodic_interval`. `DeleteSegments` is absent by design: the
/// leader rewrites it into `TruncatePartition` before journaling, so no
/// commit ever carries it.
const fn operation_triggers_partition_reconcile(op: Operation) -> bool {
    matches!(
        op,
        Operation::CreateTopic
            | Operation::CreateTopicWithAssignments
            | Operation::CreatePartitions
            | Operation::CreatePartitionsWithAssignments
            | Operation::DeleteTopic
            | Operation::DeleteStream
            | Operation::DeletePartitions
            | Operation::PurgeStream
            | Operation::PurgeTopic
            | Operation::TruncatePartition
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciler_driven_ops_broadcast_a_commit_tick() {
        // These commit without touching the partition set, so nothing else
        // signals the reconciler: `reconcile_partition_purges` and
        // `reconcile_segment_truncations` are the only code that turns them
        // into on-disk effect, and they run only when a pass runs. Dropping
        // one from the filter silently downgrades it to the periodic tick.
        for op in [
            Operation::PurgeStream,
            Operation::PurgeTopic,
            Operation::TruncatePartition,
        ] {
            assert!(
                operation_triggers_partition_reconcile(op),
                "{op:?} is enforced by the reconciler and must wake it on commit"
            );
        }
        assert!(
            !operation_triggers_partition_reconcile(Operation::CreateUser),
            "ops with no partition-shape effect must stay off the broadcast"
        );
    }
}
