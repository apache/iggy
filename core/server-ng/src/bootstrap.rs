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

use crate::auth::warm_dummy_password_hash;
use crate::cluster_meta::ClusterRoster;
use crate::config_writer::write_current_config;
use crate::dispatch::{
    make_client_request_handler, make_deferred_client_request_handler,
    make_deferred_replica_message_handler, make_list_clients_handler, make_metadata_submit_handler,
    make_partition_read_handler,
};
use crate::http;
use crate::partition_helpers::{
    configure_consumer_offsets, ensure_initial_segment, validate_namespace_bounds,
};
use crate::segment_recovery::{RecoveredSegment, load_persisted_segments};
use crate::server_error::{ServerNgError, ShardJoinFailure, ShardJoinFailureKind};
use crate::session_manager::SessionManager;
use configs::ng_sharding::{
    INBOX_CAPACITY_MAX, SHUTDOWN_DRAIN_TIMEOUT_MAX, SHUTDOWN_POLL_INTERVAL_MAX,
};
use configs::server_ng::ServerNgConfig;
use consensus::{
    LocalPipeline, MetadataHandle, PartitionsHandle, PipelineEntry, Sequencer, VsrConsensus,
};
// `try_send` / `try_recv` resolve through these traits on `MAsyncTx` /
// `MAsyncRx`; the metadata-handoff loops below depend on the
// non-blocking variants for cancel-safe shutdown polling.
use crossfire::{AsyncRxTrait, AsyncTxTrait};
use iggy_binary_protocol::Operation;
use iggy_common::defaults::{
    DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH,
    MIN_USERNAME_LENGTH,
};
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyByteSize, PartitionStats, variadic};
use journal::Journal;
use journal::prepare_journal::PrepareJournal;
use message_bus::client_listener::{self, RequestHandler};
use message_bus::installer;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::auth::{self, ReplicaAuth};
use message_bus::replica::handshake::{ReplicaHandshakeCtx, ReplicaTlsCtx};
use message_bus::replica::io as replica_io;
use message_bus::replica::listener::{self as replica_listener};
use message_bus::transports::quic::server_config_with_cert;
use message_bus::transports::tls::{
    AcceptAnyServerCert, REPLICA_ALPN, TlsServerCredentials, install_default_crypto_provider,
    load_ca_pem, load_pem, self_signed_for_loopback,
};
use message_bus::{
    AcceptedClientFn, AcceptedQuicClientFn, AcceptedReplicaFn, AcceptedTlsClientFn,
    AcceptedWsClientFn, AcceptedWssClientFn, DialedReplicaFn, IggyMessageBus,
    MAX_INFLIGHT_REPLICA_HANDSHAKES, ReplicaOwnerTable, connector,
};
use metadata::IggyMetadata;
use metadata::MuxStateMachine;
use metadata::impls::metadata::{IggySnapshot, StreamsFrontend};
use metadata::impls::recovery::recover;
use metadata::stm::mux::WithFactory;
use metadata::stm::snapshot::Snapshot;
use metadata::stm::stream::{Partition, Streams};
use metadata::stm::user::Users;
use partitions::{
    IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionsConfig,
};
use rustls::pki_types::ServerName;
use server_common::bootstrap::create_directories;
use server_common::crypto;
use server_common::executor::create_shard_executor;
use server_common::log::{Logging, LoggingSettings, TelemetrySettings};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use shard::builder::IggyShardBuilder;
use shard::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};
use shard::shards_table::{PapayaShardsTable, ShardsTable, calculate_shard_assignment};
use shard::{
    CoordinatorConfig, IggyShard, LifecycleFrame, PartitionConsensusConfig,
    Receiver as ShardReceiver, ShardFrame, ShardIdentity, TaggedSender, channel,
    shard_mesh_channels,
};
use shard_allocator::{ShardAllocator, ShardInfo};
use std::cell::RefCell;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};

const SHARD_REPLICA_ID: u8 = 0;

pub const IGGY_ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";
pub const IGGY_ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";

type ServerNgMuxStateMachine = MuxStateMachine<variadic!(Users, Streams)>;

/// Cross-thread bundle carrying one `ReadHandleFactory` per metadata
/// state. Shard 0 mints one after `recover()` and broadcasts a clone to
/// every peer shard; each peer rebuilds a reader-mode
/// [`ServerNgMuxStateMachine`] on its own runtime, skipping the WAL.
type ServerNgMetadataBundle = <variadic!(Users, Streams) as WithFactory>::Bundle;

pub(crate) type ServerNgMetadata = IggyMetadata<
    VsrConsensus<Rc<IggyMessageBus>>,
    PrepareJournal,
    IggySnapshot,
    ServerNgMuxStateMachine,
>;
pub type ServerNgShard = IggyShard<
    Rc<IggyMessageBus>,
    PrepareJournal,
    IggySnapshot,
    ServerNgMuxStateMachine,
    PapayaShardsTable,
>;

pub(crate) type ServerNgShardHandle = Rc<RefCell<Option<Weak<ServerNgShard>>>>;

/// Result of a multi-shard bootstrap.
///
/// Carries the cross-thread shutdown flag and one OS-thread `JoinHandle`
/// per shard. The caller flips the flag via [`Self::install_ctrlc_handler`]
/// and then drains every shard via [`Self::join_all`].
pub struct ShardHandles {
    shutdown_flag: Arc<AtomicBool>,
    shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerNgError>>)>,
}

impl ShardHandles {
    /// Install a SIGINT/Ctrl-C handler that flips the shutdown flag on
    /// the first signal. A second signal is logged but otherwise
    /// ignored so an in-flight WAL fsync or replica drain runs to
    /// completion.
    ///
    /// # Errors
    ///
    /// Returns the underlying `ctrlc::Error` if the handler cannot be
    /// installed (typically because another handler already owns the
    /// signal).
    pub fn install_ctrlc_handler(&self) -> Result<(), ctrlc::Error> {
        let flag = Arc::clone(&self.shutdown_flag);
        ctrlc::set_handler(move || {
            if flag.swap(true, Ordering::Relaxed) {
                // Second Ctrl-C: leave the shutdown machinery to drain.
                // Refusing to abort here keeps the WAL fsync / replica
                // drain from being interrupted mid-frame.
                warn!("second Ctrl-C ignored; server is already shutting down");
            } else {
                info!("Ctrl-C received; signalling server shutdown");
            }
        })
    }

    /// Drain every shard thread. Each shard's outcome is logged
    /// (`info` on clean exit, `error` on Err or panic). If any shard
    /// failed, returns every failure together as
    /// [`ServerNgError::ShardJoinFailures`] so the operator sees the
    /// full set rather than just the first.
    ///
    /// # Errors
    ///
    /// Returns [`ServerNgError::ShardJoinFailures`] if any shard
    /// returned a `Result::Err` or panicked. The variant carries every
    /// per-shard failure (`ShardJoinFailureKind::Error` or
    /// `ShardJoinFailureKind::Panic`) in shard-id order so the caller
    /// does not need to read the trace log to discover late-failing shards.
    pub fn join_all(self) -> Result<(), ServerNgError> {
        let mut failures: Vec<ShardJoinFailure> = Vec::new();
        // Shards run thread-per-core with compio's blocking fallback pool
        // disabled, so an io_uring opcode the kernel lacks aborts every shard
        // with the same panic. Surface the actionable diagnostic once.
        let mut io_uring_diagnostic_shown = false;
        for (shard_id, handle) in self.shard_threads {
            match handle.join() {
                Ok(Ok(())) => {
                    info!(shard_id, "shard thread exited cleanly");
                }
                Ok(Err(error)) => {
                    error!(shard_id, error = %error, "shard thread returned error");
                    failures.push(ShardJoinFailure {
                        shard_id,
                        kind: ShardJoinFailureKind::Error(Box::new(error)),
                    });
                }
                Err(panic_payload) => {
                    let message = panic_payload_to_string(&*panic_payload);
                    error!(shard_id, message = %message, "shard thread panicked");
                    if !io_uring_diagnostic_shown
                        && message
                            .contains(server_common::diagnostics::ASYNCIFY_POOL_DISABLED_PANIC_MSG)
                    {
                        server_common::diagnostics::print_incomplete_io_uring_ops_info();
                        io_uring_diagnostic_shown = true;
                    }
                    failures.push(ShardJoinFailure {
                        shard_id,
                        kind: ShardJoinFailureKind::Panic { message },
                    });
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(ServerNgError::ShardJoinFailures { failures })
        }
    }
}

/// Best-effort extraction of the panic message from a
/// `Box<dyn Any + Send>` returned by `JoinHandle::join`. Tries the two
/// payload shapes the standard library guarantees (`&'static str` and
/// `String`) and falls back to a placeholder so the panic still surfaces
/// in the error chain.
fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<panic payload not String/&str>".to_string()
}

/// Joins survivor shard threads after a partial-spawn failure without
/// panicking the bootstrap thread on `pthread_create` EAGAIN.
///
/// Bare `thread::spawn` panics on EAGAIN, which is the most likely OS
/// state on this path since the parent `Builder::spawn` already failed
/// for the same reason. A panic would unwind `bootstrap()` while
/// survivor shard threads keep driving their compio runtimes and
/// `io_uring` rings, orphaning them across process exit.
///
/// Uses `thread::Builder::spawn` and hands each survivor over via a
/// one-shot `sync_channel(1)` so an `Err` drops the rx (not the
/// survivor `JoinHandle`), letting us fall back to a sequential
/// `survivor.join()` instead. Once one cleanup spawn fails, treats the
/// OS as exhausted and routes every remaining survivor straight to the
/// sequential pool to avoid re-trying spawn.
///
/// This routine bounds CPU/IO via the survivor's own
/// `shutdown_drain_timeout` (driven by each shard's watchdog after
/// `shutdown_flag` is set by the caller), not via a wall-clock
/// deadline here. If a survivor's `shard_main` blocks past the drain
/// window without observing the flag, this join hangs - that scenario
/// is the same surface as the deferred watchdog-detach gap and is not
/// addressed by this helper.
///
/// TODO(hubcio): no hard time limit on shard shutdown here. If a
/// survivor's `shard_main` never returns, `survivor.join()` blocks
/// forever.
fn join_partial_shard_survivors(
    shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerNgError>>)>,
) {
    let mut joiners = Vec::with_capacity(shard_threads.len());
    let mut sequential_join: Vec<thread::JoinHandle<Result<(), ServerNgError>>> = Vec::new();
    let mut spawn_exhausted = false;
    for (sid, survivor) in shard_threads {
        if spawn_exhausted {
            sequential_join.push(survivor);
            continue;
        }
        let (handover_tx, handover_rx) =
            std::sync::mpsc::sync_channel::<thread::JoinHandle<Result<(), ServerNgError>>>(1);
        match thread::Builder::new()
            .name(format!("shard-{sid}-cleanup"))
            .spawn(move || {
                if let Ok(survivor) = handover_rx.recv() {
                    let _ = survivor.join();
                }
            }) {
            Ok(joiner) => {
                let _ = handover_tx.send(survivor);
                joiners.push(joiner);
            }
            Err(spawn_err) => {
                warn!(
                    error = %spawn_err,
                    shard_id = sid,
                    "cleanup helper thread spawn failed; falling back to sequential survivor join"
                );
                spawn_exhausted = true;
                sequential_join.push(survivor);
            }
        }
    }
    for joiner in joiners {
        let _ = joiner.join();
    }
    for survivor in sequential_join {
        let _ = survivor.join();
    }
}

/// Flips the cross-thread shutdown flag on `Drop` unless disarmed.
///
/// A shard thread that exits via an error `?` or a panic unwind would
/// otherwise leave sibling shards parked forever on `bus.token().wait()`:
/// their watchdogs never observe the flag and the bus has no
/// `Drop`-triggered shutdown. Arming this for the whole thread body makes
/// every non-clean exit drive sibling-shard teardown. Disarmed only on a
/// clean `Ok(())`.
struct ShutdownOnDrop {
    flag: Arc<AtomicBool>,
    armed: bool,
}

impl ShutdownOnDrop {
    const fn new(flag: Arc<AtomicBool>) -> Self {
        Self { flag, armed: true }
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(true, Ordering::Relaxed);
        }
    }
}

/// Shard-local end of the metadata bundle handoff.
///
/// Shard 0 owns the WAL writer and runs `recover()` to build the only
/// `WriteHandle`-bearing [`ServerNgMuxStateMachine`]. It then mints a
/// [`ServerNgMetadataBundle`] (a tuple of `Send + Sync`
/// `ReadHandleFactory`s) and pushes one clone per peer onto `bundle_tx`.
/// Every other shard receives the bundle and rebuilds a reader-mode
/// `MuxStateMachine` on its own runtime - no WAL access, no replay, no
/// `RecoverySync` two-phase fence. The old phase-2 WAL fence is gone
/// because peers no longer scan the WAL. They do still scan live shared
/// metadata to load their on-disk partitions, so a separate listener
/// fence is still required - see [`BootstrapBarrier`].
///
/// The channel is bounded to the peer count so shard 0's `send` never
/// blocks beyond a peer drain. A peer that dies before recv drops its
/// `bundle_rx`, so shard 0's `send` eventually sees a disconnected
/// channel; the cross-thread shutdown flag drives every waiter out of
/// its `recv` loop if shard 0 panics before broadcasting.
enum MetadataHandoff {
    Owner {
        bundle_tx: crossfire::MAsyncTx<crossfire::mpmc::Array<ServerNgMetadataBundle>>,
    },
    Waiter {
        bundle_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<ServerNgMetadataBundle>>,
    },
}

/// Reverse handshake to [`MetadataHandoff`]: gates shard 0's client
/// listeners until every peer has loaded its on-disk partitions.
///
/// Peers build their owned-partition set from live shared metadata and
/// load each segment from disk in `build_shard_for_thread`. If shard 0
/// opened listeners the instant `broadcast_metadata_bundle` returned
/// (peers have only *received* the bundle, not *loaded* partitions), a
/// client could create a partition before a peer's load scan finished.
/// That freshly committed partition would surface in the peer's scan
/// with no segment dir on disk yet, and `load_partition`'s `walk_dir`
/// would fail with `CannotReadPartitions`, aborting the whole node. A
/// partition created after boot must take the runtime reconciler path
/// (which creates its dir), never the bootstrap load path.
///
/// Shard 0 (`Owner`) drains one signal per peer before binding
/// listeners; each peer (`Waiter`) sends one once its load completes.
/// The cross-thread shutdown flag drives both sides out of their poll
/// loop if any shard dies mid-boot.
enum BootstrapBarrier {
    Owner {
        ready_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<u16>>,
    },
    Waiter {
        ready_tx: crossfire::MAsyncTx<crossfire::mpmc::Array<u16>>,
    },
}

struct TcpTopology {
    /// Domain-separation cluster id derived from `cluster.name`; threaded to
    /// every consensus instance and the replica handshake so frames agree.
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    client_listen_addr: SocketAddr,
    replica_listen_addr: Option<SocketAddr>,
    ws_listen_addr: Option<SocketAddr>,
    quic_listen_addr: Option<SocketAddr>,
    tcp_tls_listen_addr: Option<SocketAddr>,
    peers: Vec<(u8, SocketAddr)>,
}

struct LocalClientAcceptFns {
    tcp: AcceptedClientFn,
    ws: AcceptedWsClientFn,
    quic: AcceptedQuicClientFn,
    tcp_tls: AcceptedTlsClientFn,
    wss: AcceptedWssClientFn,
}

#[derive(Default)]
struct BoundClientListeners {
    tcp: Option<SocketAddr>,
    tcp_tls: Option<SocketAddr>,
    ws: Option<SocketAddr>,
    quic: Option<SocketAddr>,
}

/// Load config, prepare directories, and complete late logging init.
///
/// # Errors
///
/// Returns an error if config loading, directory preparation, or logging
/// setup fails.
pub async fn load_config(logging: &mut Logging) -> Result<ServerNgConfig, ServerNgError> {
    let config = ServerNgConfig::load()
        .await
        .map_err(ServerNgError::Config)?;
    // TODO: decouple directory bootstrap from the `server` crate.
    create_directories(&config.system).await.map_err(|source| {
        error!(
            system_path = %config.system.get_system_path(),
            error = %source,
            "failed to prepare server-ng directories"
        );
        source
    })?;
    logging
        .late_init(
            config.system.get_system_path(),
            &LoggingSettings::from(&config.system.logging),
            &TelemetrySettings::from(&config.telemetry),
        )
        .map_err(ServerNgError::Logging)?;

    Ok(config)
}

/// Resolve the operator's `cpu_allocation` into concrete shard
/// assignments plus the checked `u16` shard count.
///
/// Shard ids index `ReplicaOwnerTable` slots as `u16`. `OWNER_NONE`
/// (`u16::MAX`) is reserved as the empty-slot sentinel, so a server
/// configured with `u16::MAX` shards would mint a shard id that
/// collides with the sentinel and an owner-table lookup could never
/// tell that shard apart from an unowned slot. Reject at boot so the
/// invariant is held by the type system, not by hoping the operator
/// never configures 65535 cores worth of shards.
fn resolve_shard_assignments(
    sharding: &configs::ng_sharding::ShardingConfig,
) -> Result<(Vec<ShardInfo>, u16), ServerNgError> {
    let allocator = ShardAllocator::new(&sharding.cpu_allocation, sharding.pin_cores)
        .map_err(ServerNgError::ShardAllocator)?;
    let assignments = allocator
        .to_shard_assignments()
        .map_err(ServerNgError::ShardAllocator)?;
    if assignments.is_empty() {
        return Err(ServerNgError::ShardsCountZero);
    }
    match u16::try_from(assignments.len()) {
        Ok(count) if count < message_bus::OWNER_NONE => Ok((assignments, count)),
        _ => Err(ServerNgError::ShardsCountOverflow {
            count: assignments.len(),
        }),
    }
}

/// Re-validate the runtime sharding knobs that the per-shard runtime
/// consumes directly. Mirrors `ShardingConfig::validate` so a caller
/// that built the config without running it (e.g. tests, embedded
/// usage) cannot OOM at boot or wedge process exit with an out-of-range
/// value.
fn validate_sharding_runtime_knobs(
    sharding: &configs::ng_sharding::ShardingConfig,
) -> Result<(), ServerNgError> {
    let inbox_capacity = sharding.inbox_capacity;
    if inbox_capacity == 0 || inbox_capacity > INBOX_CAPACITY_MAX {
        return Err(ServerNgError::InvalidInboxCapacity {
            value: inbox_capacity,
            max: INBOX_CAPACITY_MAX,
        });
    }
    let drain_timeout = sharding.shutdown_drain_timeout.get_duration();
    if drain_timeout.is_zero() || drain_timeout > SHUTDOWN_DRAIN_TIMEOUT_MAX {
        return Err(ServerNgError::InvalidShutdownDrainTimeout {
            value: drain_timeout,
            max: SHUTDOWN_DRAIN_TIMEOUT_MAX,
        });
    }
    let poll_interval = sharding.shutdown_poll_interval.get_duration();
    if poll_interval.is_zero() || poll_interval > SHUTDOWN_POLL_INTERVAL_MAX {
        return Err(ServerNgError::InvalidShutdownPollInterval {
            value: poll_interval,
            max: SHUTDOWN_POLL_INTERVAL_MAX,
        });
    }
    // Ordering: a poll cadence coarser than the drain budget makes the
    // cross-thread shutdown flag effectively unobservable during teardown.
    if poll_interval > drain_timeout {
        return Err(ServerNgError::ShutdownPollExceedsDrain {
            poll: poll_interval,
            drain: drain_timeout,
        });
    }
    Ok(())
}

/// Spawn the multi-shard `server-ng` runtime.
///
/// Resolves shard count + CPU affinities from
/// `system.sharding.cpu_allocation`, builds canonical-ordered
/// `(senders, inboxes)` channels, and spawns one OS thread per shard.
///
/// Each thread pins itself (`nix::sched::sched_setaffinity` on Linux via
/// [`ShardInfo::bind_cpu`]), binds memory to its NUMA node when
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
    config: ServerNgConfig,
    current_replica_id: Option<u8>,
) -> Result<ShardHandles, ServerNgError> {
    warm_dummy_password_hash();
    let (assignments, total_shards) = resolve_shard_assignments(&config.system.sharding)?;
    let shards_count = assignments.len();

    // Re-check the full valid range, not just the zero floor: a caller
    // that built the config without running `ShardingConfig::validate`
    // would otherwise OOM at boot allocating an oversized inbox channel,
    // busy-loop every shutdown watchdog on a zero poll cadence, or wedge
    // process exit on an unbounded drain budget.
    let inbox_capacity = config.system.sharding.inbox_capacity;
    validate_sharding_runtime_knobs(&config.system.sharding)?;

    let (senders, mut inboxes) = shard_mesh_channels(total_shards, inbox_capacity);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let config = Arc::new(config);
    // One owner table per server process, Arc-cloned into every shard's bus so
    // any shard's bus reads the same atomic slots that the owning
    // shard's installer / disconnect path writes.
    let owner_table = Arc::new(ReplicaOwnerTable::new());

    // Single-shot bundle handoff (see `MetadataHandoff`): shard 0 sends
    // one cloned `ServerNgMetadataBundle` per peer; each peer drains
    // exactly one. Bounded to the peer count so shard 0's broadcast
    // never blocks past a peer drain. A single-shard deployment (zero
    // peers) still needs a non-zero capacity, so clamp up explicitly
    // rather than relying on crossfire's internal cap=0 -> 1 promotion.
    // If a peer dies before recv, shard 0's `send` eventually sees a
    // disconnected channel; the cross-thread shutdown flag drives every
    // waiter out of its recv loop if shard 0 panics before broadcasting.
    let metadata_peers = shards_count.saturating_sub(1).max(1);
    let (metadata_bundle_tx, metadata_bundle_rx) =
        crossfire::mpmc::bounded_async::<ServerNgMetadataBundle>(metadata_peers);

    // Reverse barrier (see `BootstrapBarrier`): every peer sends one
    // signal once it finishes loading its on-disk partitions; shard 0
    // drains them all before binding listeners. Bounded to the peer
    // count so a sender never blocks (each peer sends exactly once).
    let (ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(metadata_peers);

    let mut shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerNgError>>)> =
        Vec::with_capacity(shards_count);
    // Shared metadata-group view: written by shard 0's publisher task, read by
    // every shard's cluster-metadata roster so leader marking works off-shard.
    let metadata_view = Arc::new(AtomicU64::new(crate::cluster_meta::METADATA_VIEW_UNKNOWN));
    for (idx, assignment) in assignments.into_iter().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let shard_id = idx as u16;
        let inbox = inboxes[idx]
            .take()
            .expect("shard_mesh_channels populates every inbox slot exactly once");
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

        let metadata_view_for_shard = Arc::clone(&metadata_view);
        let handle = match thread::Builder::new()
            .name(format!("shard-{shard_id}"))
            .spawn(move || -> Result<(), ServerNgError> {
                run_shard_thread(
                    shard_id,
                    total_shards,
                    current_replica_id,
                    assignment,
                    senders_for_shard,
                    inbox,
                    config_for_shard,
                    shutdown_flag_for_shard,
                    metadata_handoff_for_shard,
                    barrier_for_shard,
                    owner_table_for_shard,
                    metadata_view_for_shard,
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
                join_partial_shard_survivors(shard_threads);
                return Err(ServerNgError::ShardSpawnFailed { shard_id, source });
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
        "server-ng bootstrap dispatched; awaiting shard runtimes"
    );

    Ok(ShardHandles {
        shutdown_flag,
        shard_threads,
    })
}

/// Per-shard OS thread entry. Pins CPU + memory, builds the compio
/// runtime, and `block_on`s `shard_main`.
#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
fn run_shard_thread(
    shard_id: u16,
    total_shards: u16,
    replica_id: Option<u8>,
    assignment: ShardInfo,
    senders: Vec<TaggedSender>,
    inbox: ShardReceiver<ShardFrame>,
    config: Arc<ServerNgConfig>,
    shutdown_flag: Arc<AtomicBool>,
    metadata_handoff: MetadataHandoff,
    barrier: BootstrapBarrier,
    owner_table: Arc<ReplicaOwnerTable>,
    metadata_view: Arc<AtomicU64>,
) -> Result<(), ServerNgError> {
    // Armed for the whole thread body: a post-spawn error `?` or a panic
    // unwind here must flip `shutdown_flag` so sibling watchdogs drive
    // their bus shutdown instead of parking forever on `bus.token().wait()`.
    let mut shutdown_guard = ShutdownOnDrop::new(Arc::clone(&shutdown_flag));

    assignment
        .bind_cpu()
        .map_err(|source| ServerNgError::CpuAffinityFailed { shard_id, source })?;
    assignment
        .bind_memory()
        .map_err(|source| ServerNgError::MemoryAffinityFailed { shard_id, source })?;

    // TODO(hubcio): decouple runtime creation from the `server` crate
    // (mirrors the identical TODO in `main.rs`). Reusing legacy here so
    // server-ng and the legacy server share one io_uring tuning surface.
    let runtime = create_shard_executor()
        .map_err(|source| ServerNgError::ShardRuntimeCreateFailed { shard_id, source })?;

    let result = runtime.block_on(async move {
        // `shard_main`'s future grows past clippy's `large_futures` cap
        // (it ferries the metadata handoff, bus, builders, and inflight
        // I/O in one state machine). Heap-pin it so the top-level
        // `block_on` future stays small; one allocation per startup buys
        // the stack budget back.
        Box::pin(shard_main(
            shard_id,
            total_shards,
            replica_id,
            senders,
            inbox,
            &config,
            shutdown_flag,
            metadata_handoff,
            barrier,
            owner_table,
            metadata_view,
        ))
        .await
    });

    if result.is_ok() {
        shutdown_guard.disarm();
    }
    result
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
    config: &ServerNgConfig,
    shutdown_flag: Arc<AtomicBool>,
    metadata_handoff: MetadataHandoff,
    barrier: BootstrapBarrier,
    owner_table: Arc<ReplicaOwnerTable>,
    metadata_view: Arc<AtomicU64>,
) -> Result<(), ServerNgError> {
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
    spawn_shutdown_watchdog(Rc::clone(&bus), shutdown_flag, drain_timeout, poll_interval);

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
            let recovered = recover::<ServerNgMuxStateMachine>(data_dir, |mux_stm| {
                ensure_default_root_user(mux_stm);
            })
            .await
            .map_err(ServerNgError::MetadataRecovery)?;
            validate_cluster_root_bootstrap(config, &recovered.mux_stm)?;
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
                Some((
                    recovered.journal,
                    recovered.snapshot,
                    recovered.last_applied_op,
                    recovered.last_journaled_op,
                )),
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
            (ServerNgMuxStateMachine::from_factory_bundle(bundle), None)
        }
    };

    // Metadata consensus + journal + snapshot live only on shard 0.
    // `IggyShard::tick_metadata` short-circuits when `consensus.is_none()`,
    // so peer shards have no caller that reads `journal` or `snapshot`.
    let (metadata_consensus, journal_for_metadata, snapshot_for_metadata) =
        if let Some((journal, snapshot, last_applied_op, last_journaled_op)) = owner_state {
            let snapshot_floor = snapshot.as_ref().map_or(0, IggySnapshot::sequence_number);
            let commit_watermark = last_applied_op.unwrap_or(snapshot_floor);
            let restored_op = last_journaled_op.unwrap_or(snapshot_floor);
            let consensus = restore_metadata_consensus(
                &journal,
                restored_op,
                commit_watermark,
                topology.cluster_id,
                topology.self_replica_id,
                topology.replica_count,
                Rc::clone(&bus),
            );
            (Some(consensus), Some(journal), snapshot)
        } else {
            (None, None, None)
        };
    let metadata = ServerNgMetadata::new(
        metadata_consensus,
        journal_for_metadata,
        snapshot_for_metadata,
        mux_stm,
        Some(PathBuf::from(&config.system.path)),
    );
    // Shard 0's copy resolves the `MaxTopicSize::ServerDefault` sentinel at
    // admission; every shard's copy backs the same resolution in responses.
    metadata.set_default_max_topic_size(config.system.topic.max_size.as_bytes_u64());

    let shard_metrics = ShardMetrics::for_shard();
    // Notifier install deferred until after tick handler wires below.
    let senders_for_notifier = senders.clone();
    let metrics_for_notifier = shard_metrics.clone();
    // Heap-pin like `shard_main` above: the builder future carries the whole
    // shard construction state machine and outgrew clippy's `large_futures`
    // cap; one allocation per shard startup.
    let (shard, sessions) = Box::pin(build_shard_for_thread(
        shard_id,
        total_shards,
        config,
        &topology,
        metadata,
        Rc::clone(&bus),
        senders,
        inbox,
        shard_metrics,
        Arc::clone(&metadata_view),
    ))
    .await?;

    // Shard 0 owns the metadata consensus; publish its view so every shard's
    // cluster-metadata read (and the SDK's leader discovery) marks the live
    // primary. Detached: dies with this shard's runtime at process exit.
    if shard_id == 0 {
        let publisher_shard = Rc::clone(&shard);
        let publisher_view = Arc::clone(&metadata_view);
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
        "server-ng shard initialized"
    );

    // Re-check the cross-thread shutdown flag here, *before* spawning the
    // message pump. A sibling shard may have failed in the window between
    // the metadata broadcast and this point; gating before spawn keeps the
    // bus' `background_tasks` vec empty on the shutdown path. Spawn-then-
    // check would leave `bus.track_background(pump_handle)` registering a
    // `JoinHandle` that only `bus.shutdown()` drains, but the watchdog
    // driving `bus.shutdown()` is `.detach()`'d (see TODO at
    // `spawn_shutdown_watchdog`) and may not be scheduled before this
    // function returns `Ok(())` and the compio runtime drops, cancelling
    // the pump mid-`write_vectored_all`.
    //
    // Without this gate shard 0 would also still open TCP/QUIC/WS
    // listeners for a server that is already tearing down, briefly
    // accepting connections that immediately get torn by the watchdog.
    if shutdown_flag_for_handoff.load(Ordering::Relaxed) {
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
    let pump_handle = compio::runtime::spawn(async move {
        pump_shard.run_message_pump(stop_rx).await;
    });
    bus.track_background(pump_handle);

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
            crate::dispatch::run_heartbeat_verifier(hb_shard, hb_sessions, hb_interval, hb_stop_rx)
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
        let on_client_request =
            make_client_request_handler(&shard, &sessions, Arc::clone(&config.system));
        let (accepted_replica, dialed_replica) =
            make_replica_delegation_fns(Rc::clone(&coord), &bus);
        let accepted_client = make_shard_zero_client_accept_fns(coord, &bus, on_client_request);

        if let Err(error) = start_tcp_runtime(
            &shard,
            config,
            &topology,
            accepted_replica,
            dialed_replica,
            accepted_client,
        )
        .await
        {
            let _ = stop_tx.try_send(());
            let _ = reconcile_stop_tx.try_send(());
            if let Some(tx) = &heartbeat_stop_tx {
                let _ = tx.try_send(());
            }
            if let Some(cleaner_stop_tx) = &pat_cleaner_stop {
                let _ = cleaner_stop_tx.try_send(());
            }
            if let Some(tx) = &segment_cleaner_stop {
                let _ = tx.try_send(());
            }
            return Err(error);
        }
    }

    bus.token().wait().await;
    let _ = stop_tx.try_send(());
    let _ = reconcile_stop_tx.try_send(());
    if let Some(tx) = &heartbeat_stop_tx {
        let _ = tx.try_send(());
    }
    if let Some(cleaner_stop_tx) = &pat_cleaner_stop {
        let _ = cleaner_stop_tx.try_send(());
    }
    if let Some(tx) = &segment_cleaner_stop {
        let _ = tx.try_send(());
    }

    info!(shard = shard_id, "server-ng shard exited cleanly");
    Ok(())
}

/// Block until shard 0 broadcasts the metadata factory bundle, or the
/// cross-thread shutdown flag flips. Polled in a `poll_interval` loop
/// so a shard 0 that panics before it broadcasts cannot strand peer
/// shards: the shutdown path flips the flag, every waiter observes it
/// on the next tick, and the server tears down instead of hanging.
///
/// Uses `try_recv` + sleep rather than `timeout(recv())`. Crossfire 3.x
/// documents `recv()` as cancellation-safe (no leak/deadlock) but does
/// not guarantee atomicity for the dropped future's result; `try_recv`
/// keeps each tick fully synchronous and side-effect-free, so the
/// shutdown poll cadence cannot ambiguously consume a bundle.
async fn await_metadata_bundle(
    shard_id: u16,
    bundle_rx: &crossfire::MAsyncRx<crossfire::mpmc::Array<ServerNgMetadataBundle>>,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<ServerNgMetadataBundle, ServerNgError> {
    loop {
        match bundle_rx.try_recv() {
            Ok(bundle) => return Ok(bundle),
            Err(crossfire::TryRecvError::Disconnected) => {
                return Err(ServerNgError::MetadataHandoffAborted { shard_id });
            }
            Err(crossfire::TryRecvError::Empty) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerNgError::MetadataHandoffAborted { shard_id });
                }
                compio::time::sleep(poll_interval).await;
            }
        }
    }
}

/// Push `peers` cloned bundles onto `bundle_tx`, polling each send in a
/// `poll_interval` loop so the cross-thread shutdown flag can interrupt
/// a stalled handoff. Symmetric to [`await_metadata_bundle`]: shutdown
/// observed mid-handshake aborts cleanly rather than stalling on a
/// `send` future that can no longer make progress.
///
/// Uses `try_send` + sleep rather than `timeout(send())`. Crossfire 3.x
/// documents `send()` as cancellation-safe in the leak/deadlock sense
/// but explicitly warns the true result is unknown when `SendFuture` is
/// dropped on cancellation. For a retry loop that re-clones on every
/// tick that would risk publishing the same bundle twice, stuffing the
/// bounded channel past `peers` and stranding a follow-up `send`.
/// `try_send` returns the bundle back inside `TrySendError::Full`, so
/// the loop reuses it instead of re-cloning when the channel is full.
async fn broadcast_metadata_bundle(
    shard_id: u16,
    bundle_tx: &crossfire::MAsyncTx<crossfire::mpmc::Array<ServerNgMetadataBundle>>,
    bundle: ServerNgMetadataBundle,
    peers: u16,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<(), ServerNgError> {
    for _ in 0..peers {
        let mut pending = bundle.clone();
        loop {
            match bundle_tx.try_send(pending) {
                Ok(()) => break,
                Err(crossfire::TrySendError::Disconnected(_)) => {
                    // Every peer dropped its `bundle_rx` before recv. Shard
                    // 0 must not silently continue past handoff: it would
                    // bind listeners and commit consensus state for a
                    // cluster whose peers are gone. Propagate the abort so
                    // `shard_main` short-circuits before further side
                    // effects; `shutdown_flag` will flip via the normal
                    // teardown path.
                    return Err(ServerNgError::MetadataHandoffAborted { shard_id });
                }
                Err(crossfire::TrySendError::Full(returned)) => {
                    if shutdown_flag.load(Ordering::Relaxed) {
                        return Err(ServerNgError::MetadataHandoffAborted { shard_id });
                    }
                    pending = returned;
                    compio::time::sleep(poll_interval).await;
                }
            }
        }
    }
    Ok(())
}

/// Peer side of [`BootstrapBarrier`]: tell shard 0 this shard finished
/// loading its on-disk partitions. Mirrors [`broadcast_metadata_bundle`]'s
/// `try_send`-or-shutdown poll loop so a sibling failure (which flips the
/// shutdown flag) drives this out instead of stranding it on a full
/// channel. The channel is sized to the peer count and each peer sends
/// exactly once, so `Full` is not expected; the branch only keeps the
/// loop interruptible.
async fn signal_bootstrap_complete(
    shard_id: u16,
    ready_tx: &crossfire::MAsyncTx<crossfire::mpmc::Array<u16>>,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<(), ServerNgError> {
    let mut pending = shard_id;
    loop {
        match ready_tx.try_send(pending) {
            Ok(()) => return Ok(()),
            Err(crossfire::TrySendError::Disconnected(_)) => {
                // Shard 0 dropped its `ready_rx` before draining (it
                // aborted before binding listeners). Propagate so this
                // shard short-circuits; the shutdown flag flips via the
                // normal teardown path.
                return Err(ServerNgError::MetadataHandoffAborted { shard_id });
            }
            Err(crossfire::TrySendError::Full(returned)) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerNgError::MetadataHandoffAborted { shard_id });
                }
                pending = returned;
                compio::time::sleep(poll_interval).await;
            }
        }
    }
}

/// Owner side of [`BootstrapBarrier`]: drain one ready signal per peer
/// before shard 0 binds listeners. Polls the shutdown flag so a peer that
/// dies mid-load (flipping the flag) aborts the wait instead of hanging on
/// a signal that will never arrive. A single shard (`peers == 0`) returns
/// immediately.
async fn await_bootstrap_complete(
    ready_rx: &crossfire::MAsyncRx<crossfire::mpmc::Array<u16>>,
    peers: usize,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<(), ServerNgError> {
    let mut remaining = peers;
    while remaining > 0 {
        match ready_rx.try_recv() {
            Ok(_shard_id) => remaining -= 1,
            Err(crossfire::TryRecvError::Disconnected) => {
                return Err(ServerNgError::ShardBootstrapBarrierAborted { remaining });
            }
            Err(crossfire::TryRecvError::Empty) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerNgError::ShardBootstrapBarrierAborted { remaining });
                }
                compio::time::sleep(poll_interval).await;
            }
        }
    }
    Ok(())
}

/// Spawn a per-shard polling task that watches the cross-thread shutdown
/// flag and triggers this shard's bus shutdown on transition. The flag
/// is the only Send signal we have; the bus' shutdown machinery is
/// `!Send` (`Rc<Cell<bool>>` + per-shard `async_channel`), so it must be
/// triggered from within the runtime that owns the bus.
#[allow(clippy::needless_pass_by_value)]
fn spawn_shutdown_watchdog(
    bus: Rc<IggyMessageBus>,
    shutdown_flag: Arc<AtomicBool>,
    drain_timeout: Duration,
    poll_interval: Duration,
) {
    let bus_for_task = Rc::clone(&bus);
    let bus_token = bus.token();
    let watchdog = compio::runtime::spawn(async move {
        loop {
            if shutdown_flag.load(Ordering::Relaxed) {
                break;
            }
            if bus_token.is_triggered() {
                // Bus shutdown was driven from elsewhere (e.g. internal
                // failure path). Watchdog has nothing left to do.
                return;
            }
            compio::time::sleep(poll_interval).await;
        }
        let _ = bus_for_task.shutdown(drain_timeout).await;
    });
    // TODO(hubcio): `.detach()` races bus shutdown: when `bus.token()` is
    // triggered, `shard_main` returns and the runtime drops the watchdog
    // mid-`bus.shutdown()`, truncating in-flight `ClientForwardFailed`
    // replies (terminal per `SendError` docs). Cannot use
    // `bus.track_background(watchdog)` here because the watchdog itself
    // drives `bus.shutdown()`, and the bg-drain loop in `shutdown()`
    // would re-enter awaiting the watchdog's own pending shutdown call
    // (self-deadlock). Fix: extract a `core/task_registry` crate mirroring
    // `core/server`'s task-tracking mechanism, share it between the bus
    // and server-ng so background tasks can be reaped without coupling
    // to the bus shutdown order.
    watchdog.detach();
}

/// Copy the configured cluster roster plus this node's own client ports into
/// the shared [`ClusterRoster`] so the binary `GetClusterMetadata` read serves
/// the real topology. `self_*` back only the cluster-disabled self-synthesis;
/// the HTTP port is read from config since HTTP binds outside this topology.
fn build_cluster_roster(
    config: &ServerNgConfig,
    topology: &TcpTopology,
    metadata_view: Arc<AtomicU64>,
) -> ClusterRoster {
    let http_port = if config.http.enabled {
        parse_socket_addr("http.address", &config.http.address)
            .ok()
            .map(|addr| addr.port())
    } else {
        None
    };
    ClusterRoster {
        enabled: config.cluster.enabled,
        name: config.cluster.name.clone(),
        nodes: config.cluster.nodes.clone(),
        self_ip: topology.client_listen_addr.ip().to_string(),
        self_ports: configs::cluster::TransportPorts {
            tcp: Some(topology.client_listen_addr.port()),
            quic: topology.quic_listen_addr.map(|addr| addr.port()),
            http: http_port,
            websocket: topology.ws_listen_addr.map(|addr| addr.port()),
            tcp_replica: None,
        },
        metadata_view,
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn build_shard_for_thread(
    shard_id: u16,
    total_shards: u16,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    metadata: ServerNgMetadata,
    bus: Rc<IggyMessageBus>,
    senders: Vec<TaggedSender>,
    inbox: ShardReceiver<ShardFrame>,
    metrics: ShardMetrics,
    metadata_view: Arc<AtomicU64>,
) -> Result<(Rc<ServerNgShard>, Rc<RefCell<SessionManager>>), ServerNgError> {
    let shard_local_id = ShardId::new(shard_id);
    let total_partitions = metadata.mux_stm.streams().read(|inner| {
        inner
            .items
            .iter()
            .map(|(_, stream)| {
                stream
                    .topics
                    .iter()
                    .map(|(_, topic)| topic.partitions.len())
                    .sum::<usize>()
            })
            .sum::<usize>()
    });

    // IggyPartitions holds only the partitions owned by this shard
    // (see the filter below at insert time), so the server-wide total
    // is an N-fold overshoot. `ceil(total / shards) * 2` is a coarse
    // upper bound that absorbs hash skew without paying the full
    // multiplier. PapayaShardsTable below stays sized to the server-wide
    // total because every shard routes every namespace.
    let owned_partitions_capacity = total_partitions
        .div_ceil(usize::from(total_shards).max(1))
        .saturating_mul(2);
    // At-rest encryption: built once per shard from the shared config; the
    // ingestion path encrypts on the primary and the poll reply decrypts.
    // A bad key fails the boot rather than silently serving plaintext.
    let encryptor = if config.system.encryption.enabled {
        let aes = Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key)
            .map_err(|error| ServerNgError::Iggy(Box::new(error)))?;
        Some(Arc::new(EncryptorKind::Aes256Gcm(aes)))
    } else {
        None
    };
    let partitions = IggyPartitions::with_capacity(
        shard_local_id,
        PartitionsConfig {
            messages_required_to_save: config.system.partition.messages_required_to_save,
            size_of_messages_required_to_save: config
                .system
                .partition
                .size_of_messages_required_to_save,
            enforce_fsync: config.system.partition.enforce_fsync,
            segment_size: config.system.segment.size,
            encryptor,
        },
        owned_partitions_capacity,
    );
    let shards_table = PapayaShardsTable::with_capacity(total_partitions);

    // Stream-filter inside the `read()` closure: only partitions owned by
    // this shard need the heavy (`Arc<TopicStats>` + `Partition`) clones
    // for the async `load_partition` below. Non-owning entries are pushed
    // straight into `shards_table` here, so no Vec scales with the
    // server-wide partition count.
    let owned = metadata.mux_stm.streams().read(|inner| {
        let mut owned = Vec::with_capacity(owned_partitions_capacity);
        for (_, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                for partition in &topic.partitions {
                    let namespace = IggyNamespace::new(stream.id, topic_id, partition.id);
                    let owning_shard =
                        calculate_shard_assignment(&namespace, u32::from(total_shards));
                    if owning_shard == shard_id {
                        // Shared per-partition stats from the registry: the
                        // same `Arc` backs every shard's `get_topic` reply.
                        let stats = inner.stats_registry.partition(
                            stream.id,
                            topic_id,
                            partition.id,
                            topic.stats.clone(),
                        );
                        owned.push((stream.id, topic_id, stats, partition.clone()));
                    } else {
                        shards_table.insert(
                            namespace,
                            PartitionLocation::new(
                                ShardId::new(owning_shard),
                                partition.created_revision,
                            ),
                        );
                    }
                }
            }
        }
        owned
    });

    // Snapshot totals were zeroed once on shard 0 before the factory
    // bundle was broadcast (see `MetadataHandoff::Owner`). All shards
    // here only add their per-partition deltas, so the shared
    // `Arc<TopicStats>` atomics race only against other atomic adds.
    for (stream_id, topic_id, partition_stats, partition_metadata) in owned {
        validate_namespace_bounds(config, stream_id, topic_id, partition_metadata.id)?;
        let namespace = IggyNamespace::new(stream_id, topic_id, partition_metadata.id);
        let partition = load_partition(
            config,
            namespace,
            partition_stats,
            &partition_metadata,
            topology.cluster_id,
            topology.self_replica_id,
            topology.replica_count,
            Rc::clone(&bus),
        )
        .await?;
        partitions.insert(namespace, partition);
        shards_table.insert(
            namespace,
            PartitionLocation::new(ShardId::new(shard_id), partition_metadata.created_revision),
        );
    }

    let shard_handle = Rc::new(RefCell::new(None));
    // One per-shard SessionManager, shared by the client-request handler
    // (binds sessions) and the get_clients handler (reads them). Created
    // here so both wirings reference the same instance. It also carries this
    // shard's cluster roster for the pre-auth GetClusterMetadata read.
    let sessions = Rc::new(RefCell::new(SessionManager::new()));
    sessions
        .borrow_mut()
        .set_cluster_roster(Rc::new(build_cluster_roster(
            config,
            topology,
            metadata_view,
        )));
    let on_replica_message = make_deferred_replica_message_handler(&shard_handle);
    let on_client_request = make_deferred_client_request_handler(
        &bus,
        &shard_handle,
        &sessions,
        Arc::clone(&config.system),
    );
    let on_metadata_submit = make_metadata_submit_handler(&shard_handle);
    let on_list_clients = make_list_clients_handler(&sessions);
    let on_partition_read = make_partition_read_handler(&shard_handle);
    let shard_name = format!("server-ng-shard-{shard_id}");
    let built = IggyShardBuilder::new(
        ShardIdentity::new(shard_id, shard_name),
        Rc::clone(&bus),
        on_replica_message,
        on_client_request,
        on_metadata_submit,
        on_list_clients,
        on_partition_read,
        metadata,
        partitions,
        senders,
        inbox,
        shards_table,
        PartitionConsensusConfig::new(
            topology.cluster_id,
            shard::ReplicaTopology::new(topology.self_replica_id, topology.replica_count),
            Rc::clone(&bus),
        ),
        CoordinatorConfig::default(),
        metrics,
    )
    .build()
    .map_err(ServerNgError::ShardConstruction)?;

    let shard = Rc::new(built.shard);
    *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
    Ok((shard, sessions))
}

fn restore_metadata_consensus(
    journal: &PrepareJournal,
    restored_op: u64,
    commit_watermark: u64,
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> VsrConsensus<Rc<IggyMessageBus>> {
    let mut consensus = VsrConsensus::new(
        cluster_id,
        self_replica_id,
        replica_count,
        server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
        bus,
        LocalPipeline::new(),
    );

    let last_header = journal
        .last_op()
        .and_then(|op| usize::try_from(op).ok())
        .and_then(|op| journal.header(op).map(|header| *header));
    if let Some(header) = last_header {
        consensus.set_view(header.view);
    }

    // On a RESTART in a cluster (a non-empty WAL proves a prior life),
    // rejoin as a quorum-invisible backup and probe for the current view
    // (`RequestStartView`): the view's primary answers with a `StartView`,
    // the replica adopts it as a backup, and journal repair fills any WAL
    // gap. A probing replica never resumes primaryship -- if this replica
    // IS the current primary-by-index, its probe makes the backups elect
    // past it.
    // The probe re-broadcasts on its timeout, so it needs no live mesh at
    // boot. A FRESH boot (empty WAL) keeps the plain init: the cluster
    // needs its view-0 primary to exist, and a single-replica cluster has
    // no peer to ask.
    if replica_count > 1 && restored_op > 0 {
        consensus.init_as_backup();
        consensus.begin_view_probe();
    } else {
        consensus.init();
    }
    consensus.sequencer().set_sequence(restored_op);
    // The commit point is restored from the WAL's embedded watermark (each
    // journaled prepare carries the primary's commit at send time), NOT from
    // the journal head: journaled does not imply committed, and claiming
    // commit for the un-quorum'd tail both risks split-brain on a later view
    // change and starves the tail of re-replication (it would live in no
    // pipeline). The suffix `(commit_watermark, restored_op]` is re-pipelined
    // below when this replica is the recovered view's primary.
    //
    // TODO(hubcio): the watermark is a lower bound (the last entry stamps
    // the commit point as of its send). Persisting an explicit (view,
    // commit_op) watermark on the commit path would tighten recovery and
    // allow refusing boot on an excessive gap; a backup that recovered a
    // LONGER tail than the cluster's primary still needs uncommitted-suffix
    // truncation when conflicting ops arrive (message repair milestone).
    consensus.restore_commit_state(commit_watermark, commit_watermark);
    if let Some(header) = last_header {
        consensus.set_last_prepare_checksum(header.checksum);
    }

    // The WAL's tail past the watermark is prepared-but-not-provably-committed
    // state. Until the cluster confirms it (re-pipelined below on a resumed
    // primary; via StartView adoption + the local commit walk on a rejoined
    // backup), serving reads would show pre-restart state that clients already
    // saw acked -- gate them on the barrier regardless of role. If the suffix
    // never committed cluster-wide, the barrier times out on the read path and
    // serving resumes (`await_recovery_barrier`).
    if commit_watermark < restored_op {
        consensus.set_recovery_barrier(restored_op);
    }

    // Re-pipeline the prepared-but-uncommitted suffix so the primary's
    // retransmit machinery re-replicates it and quorum can (re-)commit it.
    // A backup's suffix stays journal-only: the primary's traffic either
    // confirms it (re-forward + re-ack path) or supersedes it.
    if consensus.is_primary()
        && !consensus.has_ceded_primaryship()
        && commit_watermark < restored_op
    {
        info!(
            commit_watermark,
            restored_op, "re-pipelining recovered uncommitted metadata suffix"
        );
        let mut pipeline = consensus.pipeline().borrow_mut();
        #[allow(clippy::cast_possible_truncation)]
        for op in (commit_watermark + 1)..=restored_op {
            let Some(header) = journal.header(op as usize) else {
                warn!(
                    op,
                    "recovered journal suffix has a gap; stopping re-pipeline"
                );
                break;
            };
            let mut entry = PipelineEntry::new(*header);
            entry.add_ack(self_replica_id);
            pipeline.push(entry);
        }
    }

    consensus
}

#[allow(clippy::too_many_arguments)]
async fn load_partition(
    config: &ServerNgConfig,
    namespace: IggyNamespace,
    stats: Arc<PartitionStats>,
    partition_metadata: &Partition,
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerNgError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let consensus = VsrConsensus::new(
        cluster_id,
        self_replica_id,
        replica_count,
        namespace.inner(),
        bus,
        LocalPipeline::new(),
    );
    // A recovered partition lost its consensus state with the process: the
    // partition journal is in-memory and segments carry no op numbers, so
    // this replica cannot know the group's (op, commit). In a cluster it
    // boots as a quorum-invisible backup and probes for the current view
    // (`RequestStartView`): the view's primary answers with a `StartView`,
    // journal repair fills the rejoin window, and the commit floor settles
    // at the serving peer's retention point. The probe re-broadcasts on its
    // timeout, so it needs no live mesh at boot. Single-replica groups
    // have no peer to ask and keep the plain init.
    if replica_count > 1 {
        consensus.init_as_backup();
        consensus.begin_view_probe();
    } else {
        consensus.init();
    }

    let recovered_segments =
        load_persisted_segments(config, stream_id, topic_id, partition_id, &stats)
            .await
            .map_err(|source| {
                error!(
                    stream_id,
                    topic_id,
                    partition_id,
                    error = %source,
                    "failed to load partition log during server-ng bootstrap"
                );
                source
            })?;

    let mut partition = IggyPartition::new(stats.clone(), consensus);
    partition.set_partition_dir(config.system.get_partition_path(
        stream_id,
        topic_id,
        partition_id,
    ));
    hydrate_partition_log(
        &mut partition,
        config,
        stream_id,
        topic_id,
        partition_id,
        recovered_segments,
    )
    .await?;

    let current_offset = partition
        .log
        .segments()
        .iter()
        .filter(|segment| segment.size > IggyByteSize::default())
        .map(|segment| segment.end_offset)
        .max()
        .unwrap_or(0);
    partition.created_at = partition_metadata.created_at;
    if partition
        .log
        .segments()
        .iter()
        .any(|segment| segment.size > IggyByteSize::default())
    {
        partition.recovered_durable_offset = Some(current_offset);
    }
    partition.offset.store(current_offset, Ordering::Release);
    partition
        .dirty_offset
        .store(current_offset, Ordering::Relaxed);
    partition.should_increment_offset = partition
        .log
        .segments()
        .iter()
        .any(|segment| segment.size > IggyByteSize::default());
    partition.stats.set_current_offset(current_offset);

    configure_consumer_offsets(&mut partition, config, namespace, current_offset)?;
    ensure_initial_segment(&mut partition, config, stream_id, topic_id, partition_id).await?;

    Ok(partition)
}

async fn hydrate_partition_log(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    recovered_segments: Vec<RecoveredSegment>,
) -> Result<(), ServerNgError> {
    for RecoveredSegment { segment, storage } in recovered_segments {
        partition
            .log
            .add_persisted_segment(segment, storage, None, None);
    }

    if let Some(active_index) = partition.log.segments().len().checked_sub(1) {
        let storage = &partition.log.storages()[active_index];
        if let (Some(messages_reader), Some(index_reader)) = (
            storage.messages_reader.as_ref(),
            storage.index_reader.as_ref(),
        ) {
            let index_path = index_reader.path();
            let index_size = std::fs::metadata(&index_path).map_or(0, |metadata| metadata.len());
            partition.log.messages_writers_mut()[active_index] = Some(Rc::new(
                MessagesWriter::new(
                    &messages_reader.path(),
                    Rc::new(AtomicU64::new(u64::from(messages_reader.file_size()))),
                    config.system.partition.enforce_fsync,
                    true,
                )
                .await
                .map_err(|source| {
                    error!(
                        stream_id,
                        topic_id,
                        partition_id,
                        path = %messages_reader.path(),
                        error = %source,
                        "failed to initialize persisted messages writer"
                    );
                    source
                })?,
            ));
            partition.log.index_writers_mut()[active_index] = Some(Rc::new(
                IggyIndexWriter::new(
                    &index_path,
                    Rc::new(AtomicU64::new(index_size)),
                    config.system.partition.enforce_fsync,
                    true,
                )
                .await
                .map_err(|source| {
                    error!(
                        stream_id,
                        topic_id,
                        partition_id,
                        path = %index_path,
                        error = %source,
                        "failed to initialize persisted sparse index writer"
                    );
                    source
                })?,
            ));
        }
    }

    Ok(())
}

fn resolve_tcp_topology(
    config: &ServerNgConfig,
    current_replica_id: Option<u8>,
) -> Result<TcpTopology, ServerNgError> {
    let default_client_addr = parse_socket_addr("tcp.address", &config.tcp.address)?;
    let default_ws_addr = resolve_optional_listener_addr(
        config.websocket.enabled,
        "websocket.address",
        &config.websocket.address,
    )?;
    let default_quic_addr =
        resolve_optional_listener_addr(config.quic.enabled, "quic.address", &config.quic.address)?;
    if !config.cluster.enabled {
        if let Some(replica_id) = current_replica_id
            && replica_id != SHARD_REPLICA_ID
        {
            return Err(ServerNgError::ReplicaIdRequiresCluster {
                supplied: replica_id,
                default: SHARD_REPLICA_ID,
            });
        }
        return Ok(TcpTopology {
            cluster_id: auth::cluster_domain_id(&config.cluster.name),
            // Keep parity with the current server binary and the integration
            // harness: `--replica-id 0` may be passed unconditionally in
            // single-node mode; any other id is rejected above so the WAL
            // cannot commit under an identity that will later disagree with
            // a cluster.nodes[] entry.
            self_replica_id: SHARD_REPLICA_ID,
            replica_count: 1,
            client_listen_addr: default_client_addr,
            replica_listen_addr: Some(SocketAddr::new(default_client_addr.ip(), 0)),
            ws_listen_addr: default_ws_addr,
            quic_listen_addr: default_quic_addr,
            tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(default_client_addr),
            peers: Vec::new(),
        });
    }

    let self_replica_id = current_replica_id.ok_or(ServerNgError::MissingReplicaId)?;

    let self_node = config
        .cluster
        .nodes
        .iter()
        .find(|node| node.replica_id == self_replica_id)
        .ok_or(ServerNgError::ClusterNodeNotFound {
            replica_id: self_replica_id,
        })?;
    let replica_count = u8::try_from(config.cluster.nodes.len()).map_err(|_| {
        ServerNgError::ClusterReplicaCountTooLarge {
            count: config.cluster.nodes.len(),
        }
    })?;
    let (client_listen_addr, ws_listen_addr, quic_listen_addr) = resolve_cluster_client_addrs(
        self_node,
        default_client_addr,
        default_ws_addr,
        default_quic_addr,
    )?;
    let replica_port =
        self_node
            .ports
            .tcp_replica
            .ok_or(ServerNgError::ClusterReplicaPortMissing {
                replica_id: self_node.replica_id,
            })?;
    let replica_listen_addr = Some(socket_addr_from_parts(
        "cluster.nodes[*].ports.tcp_replica",
        &self_node.ip,
        replica_port,
    )?);
    let peers = resolve_cluster_replica_peers(&config.cluster.nodes, self_replica_id)?;

    Ok(TcpTopology {
        cluster_id: auth::cluster_domain_id(&config.cluster.name),
        self_replica_id,
        replica_count,
        client_listen_addr,
        replica_listen_addr,
        ws_listen_addr,
        quic_listen_addr,
        tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(client_listen_addr),
        peers,
    })
}

fn resolve_optional_listener_addr(
    enabled: bool,
    context: &'static str,
    address: &str,
) -> Result<Option<SocketAddr>, ServerNgError> {
    if enabled {
        return Ok(Some(parse_socket_addr(context, address)?));
    }
    Ok(None)
}

fn resolve_cluster_client_addrs(
    self_node: &configs::cluster::ClusterNodeConfig,
    default_client_addr: SocketAddr,
    default_ws_addr: Option<SocketAddr>,
    default_quic_addr: Option<SocketAddr>,
) -> Result<(SocketAddr, Option<SocketAddr>, Option<SocketAddr>), ServerNgError> {
    let client_port = self_node
        .ports
        .tcp
        .unwrap_or_else(|| default_client_addr.port());
    let client_listen_addr =
        socket_addr_from_parts("cluster.nodes[*].ports.tcp", &self_node.ip, client_port)?;
    let ws_listen_addr = resolve_cluster_optional_addr(
        self_node,
        "cluster.nodes[*].ports.websocket",
        default_ws_addr,
        |ports| ports.websocket,
    )?;
    let quic_listen_addr = resolve_cluster_optional_addr(
        self_node,
        "cluster.nodes[*].ports.quic",
        default_quic_addr,
        |ports| ports.quic,
    )?;
    Ok((client_listen_addr, ws_listen_addr, quic_listen_addr))
}

fn resolve_cluster_optional_addr(
    self_node: &configs::cluster::ClusterNodeConfig,
    context: &'static str,
    default_addr: Option<SocketAddr>,
    port_selector: impl Fn(&configs::cluster::TransportPorts) -> Option<u16>,
) -> Result<Option<SocketAddr>, ServerNgError> {
    let Some(default_addr) = default_addr else {
        return Ok(None);
    };
    let port = port_selector(&self_node.ports).unwrap_or_else(|| default_addr.port());
    socket_addr_from_parts(context, &self_node.ip, port).map(Some)
}

fn resolve_cluster_replica_peers(
    nodes: &[configs::cluster::ClusterNodeConfig],
    self_replica_id: u8,
) -> Result<Vec<(u8, SocketAddr)>, ServerNgError> {
    let mut peers = Vec::with_capacity(nodes.len().saturating_sub(1));
    for node in nodes {
        if node.replica_id == self_replica_id {
            continue;
        }
        let replica_port =
            node.ports
                .tcp_replica
                .ok_or(ServerNgError::ClusterReplicaPortMissing {
                    replica_id: node.replica_id,
                })?;
        peers.push((
            node.replica_id,
            socket_addr_from_parts("cluster.nodes[*].ports.tcp_replica", &node.ip, replica_port)?,
        ));
    }
    Ok(peers)
}

async fn start_tcp_runtime(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerNgError> {
    if config.tcp.enabled && !config.tcp.tls.enabled {
        start_via_replica_io(
            shard,
            config,
            topology,
            accepted_replica,
            dialed_replica,
            accepted_clients,
        )
        .await?;
    } else {
        start_manual_runtime(
            shard,
            config,
            topology,
            accepted_replica,
            dialed_replica,
            accepted_clients,
        )
        .await?;
    }

    // HTTP is served over TCP but sits outside the replica_io / manual client
    // reactor, so it binds independently. Shard-0 gating comes from the sole
    // caller of this function.
    if config.http.enabled {
        let http_addr = parse_socket_addr("http.address", &config.http.address)?;
        let self_ports = configs::cluster::TransportPorts {
            tcp: config
                .tcp
                .enabled
                .then(|| topology.client_listen_addr.port()),
            quic: topology.quic_listen_addr.map(|addr| addr.port()),
            websocket: topology.ws_listen_addr.map(|addr| addr.port()),
            ..Default::default()
        };
        http::start(
            shard,
            http_addr,
            &config.http,
            &config.cluster,
            Arc::clone(&config.system),
            self_ports,
        )
        .await?;
    }

    Ok(())
}

// ws/wss bindings intentionally mirror the transport names (same convention as
// `replica_io::start_on_shard_zero`).
#[allow(clippy::similar_names)]
async fn start_via_replica_io(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerNgError> {
    let replica_addr = topology
        .replica_listen_addr
        .expect("topology must include replica listener address");
    let quic_credentials = topology
        .quic_listen_addr
        .is_some()
        .then(|| load_quic_server_credentials(config))
        .transpose()?;
    let tcp_tls_credentials = topology
        .tcp_tls_listen_addr
        .is_some()
        .then(|| load_tcp_tls_server_credentials(config))
        .transpose()?;
    // `websocket.tls.enabled` upgrades the websocket address to a WSS
    // listener; the plain-WS listener must NOT also bind it (one port, one
    // handshake kind -- a plain upgrade parser fed a TLS ClientHello rejects
    // every connection with an httparse error).
    let wss_enabled = config.websocket.tls.enabled;
    let ws_listen_addr = (!wss_enabled).then_some(topology.ws_listen_addr).flatten();
    let wss_listen_addr = wss_enabled.then_some(topology.ws_listen_addr).flatten();
    let wss_credentials = wss_listen_addr
        .is_some()
        .then(|| load_wss_server_credentials(config))
        .transpose()?;

    let LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
        wss,
    } = accepted_clients;

    let bound = replica_io::start_on_shard_zero(
        &shard.bus,
        replica_addr,
        topology.client_listen_addr,
        ws_listen_addr,
        topology.quic_listen_addr,
        quic_credentials,
        topology.tcp_tls_listen_addr,
        tcp_tls_credentials,
        wss_listen_addr,
        wss_credentials,
        topology.self_replica_id,
        topology.peers.clone(),
        accepted_replica,
        dialed_replica,
        tcp,
        ws_listen_addr.map(|_| ws),
        topology.quic_listen_addr.map(|_| quic),
        topology.tcp_tls_listen_addr.map(|_| tcp_tls),
        wss_listen_addr.map(|_| wss),
        shard.bus.config().reconnect_period,
    )
    .await
    .map_err(|source| {
        error!(
            replica_addr = %replica_addr,
            client_addr = %topology.client_listen_addr,
            error = %source,
            "failed to start server-ng listeners via replica_io"
        );
        source
    })?;
    let Some(bound) = bound else {
        return Ok(());
    };

    write_current_config(
        config,
        Some(topology.self_replica_id),
        Some(bound.client),
        config.cluster.enabled.then_some(bound.replica),
        bound.tcp_tls,
        bound.quic,
        // The WSS listener occupies the configured websocket address slot.
        bound.wss.or(bound.ws),
    )
    .await?;
    if config.cluster.enabled {
        info!(
            shard = shard.id,
            replica = %bound.replica,
            tcp = %bound.client,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.ws,
            quic = ?bound.quic,
            "server-ng listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = %bound.client,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.ws,
            quic = ?bound.quic,
            "server-ng client listeners started"
        );
    }

    Ok(())
}

async fn start_manual_runtime(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerNgError> {
    let bound_replica = if config.cluster.enabled {
        let replica_addr = topology
            .replica_listen_addr
            .expect("cluster-enabled topology must include replica listener address");
        let (replica_listener, bound_addr) =
            replica_listener::bind(replica_addr)
                .await
                .map_err(|source| {
                    error!(
                        replica_addr = %replica_addr,
                        error = %source,
                        "failed to bind replica listener"
                    );
                    source
                })?;
        let token = shard.bus.token();
        let replica_handle = compio::runtime::spawn(async move {
            replica_listener::run(replica_listener, token, accepted_replica).await;
        });
        shard.bus.track_background(replica_handle);
        connector::start(
            &shard.bus,
            topology.self_replica_id,
            topology.peers.clone(),
            dialed_replica,
            shard.bus.config().reconnect_period,
        )
        .await;
        Some(bound_addr)
    } else {
        None
    };

    let bound_clients = start_client_listeners(shard, config, topology, &accepted_clients).await?;
    write_current_config(
        config,
        Some(topology.self_replica_id),
        bound_clients.tcp,
        bound_replica,
        bound_clients.tcp_tls,
        bound_clients.quic,
        bound_clients.ws,
    )
    .await?;

    if config.cluster.enabled {
        info!(
            shard = shard.id,
            replica = ?bound_replica,
            tcp = ?bound_clients.tcp,
            tcp_tls = ?bound_clients.tcp_tls,
            ws = ?bound_clients.ws,
            quic = ?bound_clients.quic,
            "server-ng listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = ?bound_clients.tcp,
            tcp_tls = ?bound_clients.tcp_tls,
            ws = ?bound_clients.ws,
            quic = ?bound_clients.quic,
            "server-ng client listeners started"
        );
    }

    Ok(())
}

fn ensure_default_root_user(mux_stm: &ServerNgMuxStateMachine) {
    if !mux_stm.users().read(|users| users.items.is_empty()) {
        return;
    }

    let (username, password_hash) = create_root_credentials();
    mux_stm.users().ensure_root_user(&username, &password_hash);
}

/// Resolve the root user credentials from `IGGY_ROOT_USERNAME` /
/// `IGGY_ROOT_PASSWORD`, falling back to the default username with a
/// generated password (printed to stdout, mirroring the legacy server).
///
/// Returns `(username, password_hash)`; the plaintext password never
/// leaves this function.
fn create_root_credentials() -> (String, String) {
    let mut username = env::var(IGGY_ROOT_USERNAME_ENV);
    let mut password = env::var(IGGY_ROOT_PASSWORD_ENV);
    assert_eq!(
        username.is_ok(),
        password.is_ok(),
        "When providing the custom root user credentials, both username and password must be set."
    );
    if username.is_ok() && password.is_ok() {
        info!("Using the custom root user credentials.");
    } else {
        info!("Using the default root user credentials...");
        username = Ok(DEFAULT_ROOT_USERNAME.to_string());
        let generated_password = crypto::generate_secret(20..40);
        println!("Generated root user password: {generated_password}");
        password = Ok(generated_password);
    }

    let username = username.expect("Root username is not set.");
    let password = password.expect("Root password is not set.");
    assert!(
        !username.is_empty() && !password.is_empty(),
        "Root user credentials cannot be empty."
    );
    assert!(
        username.len() >= MIN_USERNAME_LENGTH,
        "Root username is too short."
    );
    assert!(
        username.len() <= MAX_USERNAME_LENGTH,
        "Root username is too long."
    );
    assert!(
        password.len() >= MIN_PASSWORD_LENGTH,
        "Root password is too short."
    );
    assert!(
        password.len() <= MAX_PASSWORD_LENGTH,
        "Root password is too long."
    );

    (username, crypto::hash_password(&password))
}

fn validate_cluster_root_bootstrap(
    config: &ServerNgConfig,
    mux_stm: &ServerNgMuxStateMachine,
) -> Result<(), ServerNgError> {
    if !config.cluster.enabled || !mux_stm.users().read(|users| users.items.is_empty()) {
        return Ok(());
    }

    if env::var(IGGY_ROOT_USERNAME_ENV).is_ok() && env::var(IGGY_ROOT_PASSWORD_ENV).is_ok() {
        return Ok(());
    }

    Err(ServerNgError::ClusterRootCredentialsRequired {
        username_env: IGGY_ROOT_USERNAME_ENV,
        password_env: IGGY_ROOT_PASSWORD_ENV,
    })
}

/// Replica delegation callbacks for shard 0's listener and connector.
///
/// Inbound: acquire a slot in the shard-0-global in-flight handshake cap
/// (drop the connection when full), then blind-delegate the raw fd
/// through the coordinator's round-robin. The fd lands on the target
/// shard's inbox as a [`shard::LifecycleFrame::ReplicaInboundSetup`]
/// frame; the owning shard runs the acceptor handshake and acks the
/// slot back. A failed delegation releases the slot immediately.
///
/// Outbound: delegate the dialed fd as
/// [`shard::LifecycleFrame::ReplicaOutboundSetup`] and mark the peer
/// dial-pending so the reconnect sweep skips it until the owning
/// shard's handshake outcome arrives (or the entry expires).
fn make_replica_delegation_fns(
    coord: Rc<shard::coordinator::ShardZeroCoordinator>,
    bus: &Rc<IggyMessageBus>,
) -> (AcceptedReplicaFn, DialedReplicaFn) {
    let inbound_bus = Rc::clone(bus);
    let inbound_coord = Rc::clone(&coord);
    let accepted: AcceptedReplicaFn = Rc::new(move |stream| {
        let Some(slot) = inbound_bus.try_acquire_replica_handshake_slot() else {
            warn!(
                cap = MAX_INFLIGHT_REPLICA_HANDSHAKES,
                "replica handshake in-flight cap reached; dropping inbound"
            );
            return;
        };
        match inbound_coord.delegate_replica_inbound(stream, slot) {
            Ok(target) => {
                info!(slot, target, "inbound replica connection delegated");
            }
            Err(error) => {
                inbound_bus.release_replica_handshake_slot(slot);
                warn!(
                    error = ?error,
                    "delegate_replica_inbound failed; dropping inbound replica connection"
                );
            }
        }
    });

    let outbound_bus = Rc::clone(bus);
    let dialed: DialedReplicaFn =
        Rc::new(
            move |stream, peer_id| match coord.delegate_replica_outbound(stream, peer_id) {
                Ok(target) => {
                    outbound_bus.mark_dial_pending(peer_id);
                    info!(peer_id, target, "outbound replica connection delegated");
                }
                Err(error) => {
                    warn!(
                        peer_id,
                        error = ?error,
                        "delegate_replica_outbound failed; dropping dialed replica connection"
                    );
                }
            },
        );

    (accepted, dialed)
}

/// Shard-0 client accept callbacks. TCP and WS clients are delegated via
/// the coordinator (round-robin to peer shards); QUIC and TCP-TLS install
/// locally on shard 0 because their per-connection state is not portable
/// across shards (`compio_quic` endpoint binds one UDP socket; rustls TLS
/// state ties to the post-handshake reactor).
// ws/wss bindings intentionally mirror the transport names (same convention as
// `replica_io::start_on_shard_zero`).
#[allow(clippy::similar_names)]
fn make_shard_zero_client_accept_fns(
    coord: Rc<shard::coordinator::ShardZeroCoordinator>,
    bus: &Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> LocalClientAcceptFns {
    let quic_bus = Rc::clone(bus);
    let tcp_tls_bus = Rc::clone(bus);
    let wss_bus = Rc::clone(bus);
    let quic_request = on_request.clone();
    let wss_request = on_request.clone();
    let tcp_tls_request = on_request;

    let tcp_coord = Rc::clone(&coord);
    let tcp = Rc::new(move |stream| match tcp_coord.delegate_client(stream) {
        Ok(client_id) => info!(client_id, "TCP client delegated"),
        Err(error) => warn!(error = ?error, "delegate_client failed; dropping TCP client"),
    });

    let ws_coord = Rc::clone(&coord);
    let ws = Rc::new(move |stream| match ws_coord.delegate_ws_client(stream) {
        Ok(client_id) => info!(client_id, "WS client delegated"),
        Err(error) => warn!(error = ?error, "delegate_ws_client failed; dropping WS client"),
    });

    // QUIC and TCP-TLS terminate locally on shard 0 but mint their client
    // ids through the coordinator's `client_seq`, the same counter the
    // delegated TCP/WS path uses. A separate counter here would let a
    // shard-0-local id collide with a delegated id that round-robined to
    // shard 0 (both encode target shard 0) in shard 0's connection
    // registry.
    let quic_coord = Rc::clone(&coord);
    let quic = Rc::new(move |accepted: message_bus::AcceptedQuicConn| {
        let meta = mint_client_meta(&quic_coord, accepted.peer_addr(), ClientTransportKind::Quic);
        installer::install_client_quic(&quic_bus, meta, accepted, quic_request.clone());
    });

    let tcp_tls_coord = Rc::clone(&coord);
    let tcp_tls = Rc::new(move |stream, tls_config| {
        let Some(meta) =
            client_meta_from_stream(&stream, &tcp_tls_coord, ClientTransportKind::TcpTls)
        else {
            return;
        };
        installer::install_client_tcp_tls(
            &tcp_tls_bus,
            meta,
            stream,
            tls_config,
            tcp_tls_request.clone(),
        );
    });

    // WSS terminates locally on shard 0 like TCP-TLS (rustls state is not
    // serialisable across the delegate path), minting ids through the same
    // coordinator counter.
    let wss_coord = coord;
    let wss = Rc::new(move |stream, tls_config| {
        let Some(meta) = client_meta_from_stream(&stream, &wss_coord, ClientTransportKind::Wss)
        else {
            return;
        };
        installer::install_client_wss(&wss_bus, meta, stream, tls_config, wss_request.clone());
    });

    LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
        wss,
    }
}

fn client_meta_from_stream(
    stream: &compio::net::TcpStream,
    coord: &shard::coordinator::ShardZeroCoordinator,
    transport: ClientTransportKind,
) -> Option<ClientConnMeta> {
    let peer_addr = match stream.peer_addr() {
        Ok(peer_addr) => peer_addr,
        Err(error) => {
            warn!(error = %error, "dropping accepted client with unknown peer address");
            return None;
        }
    };
    Some(mint_client_meta(coord, peer_addr, transport))
}

fn mint_client_meta(
    coord: &shard::coordinator::ShardZeroCoordinator,
    peer_addr: SocketAddr,
    transport: ClientTransportKind,
) -> ClientConnMeta {
    ClientConnMeta::new(coord.mint_shard_zero_client_id(), peer_addr, transport)
}

async fn start_client_listeners(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_clients: &LocalClientAcceptFns,
) -> Result<BoundClientListeners, ServerNgError> {
    let mut bound = BoundClientListeners::default();

    if config.tcp.enabled && !config.tcp.tls.enabled {
        let (listener, bound_addr) = client_listener::tcp::bind(topology.client_listen_addr)
            .await
            .map_err(|source| {
                error!(
                    addr = %topology.client_listen_addr,
                    error = %source,
                    "failed to bind TCP client listener"
                );
                source
            })?;
        let token = shard.bus.token();
        let accepted_client = accepted_clients.tcp.clone();
        let client_handle = compio::runtime::spawn(async move {
            client_listener::tcp::run(listener, token, accepted_client).await;
        });
        shard.bus.track_background(client_handle);
        bound.tcp = Some(bound_addr);
    }

    if let Some(ws_addr) = topology.ws_listen_addr {
        bound.ws = Some(start_websocket_listener(shard, config, ws_addr, accepted_clients).await?);
    }

    if let Some(quic_addr) = topology.quic_listen_addr {
        install_default_crypto_provider();
        let credentials = load_quic_server_credentials(config)?;
        let server_config = server_config_with_cert(
            credentials.cert_chain,
            credentials.key_der,
            &shard.bus.config().quic,
        )
        .map_err(|e| {
            let source =
                iggy_common::IggyError::IoError(format!("QUIC server config build failed: {e}"));
            error!(addr = %quic_addr, error = %source, "failed to build QUIC server config");
            source
        })?;
        let (endpoint, bound_addr) = client_listener::quic::bind(quic_addr, server_config)
            .map_err(|source| {
                error!(addr = %quic_addr, error = %source, "failed to bind QUIC listener");
                source
            })?;
        let token = shard.bus.token();
        let handshake_grace = shard.bus.config().handshake_grace;
        let accepted_quic = accepted_clients.quic.clone();
        let quic_handle = compio::runtime::spawn(async move {
            client_listener::quic::run(endpoint, token, accepted_quic, handshake_grace).await;
        });
        shard.bus.track_background(quic_handle);
        bound.quic = Some(bound_addr);
    }

    if config.tcp.enabled && config.tcp.tls.enabled {
        let credentials = load_tcp_tls_server_credentials(config)?;
        let (listener, tls_config, bound_addr) =
            client_listener::tcp_tls::bind(topology.client_listen_addr, credentials).map_err(
                |source| {
                    error!(
                        addr = %topology.client_listen_addr,
                        error = %source,
                        "failed to bind TCP TLS listener"
                    );
                    source
                },
            )?;
        let token = shard.bus.token();
        let accepted_tls = accepted_clients.tcp_tls.clone();
        let tls_handle = compio::runtime::spawn(async move {
            client_listener::tcp_tls::run(listener, tls_config, token, accepted_tls).await;
        });
        shard.bus.track_background(tls_handle);
        bound.tcp_tls = Some(bound_addr);
    }

    Ok(bound)
}

/// Build the replica auth context from cluster config. Returns `None` when the
/// cluster or replica auth is disabled, keeping the handshake in legacy mode.
/// Only the derived MAC key is carried onward in [`ReplicaAuth`]; the raw secret
/// (masked in config logs via `config_env(secret)`) is read here only to derive
/// that key. `ClusterConfig::validate` guarantees a non-empty secret whenever
/// both `cluster.enabled` and `cluster.auth.enabled` are set (validate
/// early-returns `Ok` while `cluster.enabled` is false).
fn load_replica_auth(config: &ServerNgConfig) -> Option<ReplicaAuth> {
    if !config.cluster.enabled || !config.cluster.auth.enabled {
        return None;
    }
    Some(ReplicaAuth::new(
        config.cluster.auth.shared_secret.as_bytes(),
    ))
}

/// Build the replica TLS context from cluster config. Returns `None` when
/// the cluster or replica TLS is disabled. Every shard calls this once at
/// boot: CA mode re-reads the same PEM files per shard; self-signed mode
/// mints a per-shard throwaway certificate. Neither mode carries client
/// certificates, so TLS authenticates the acceptor only; peer
/// authentication comes from the PSK handshake (`ClusterConfig::validate`
/// enforces `cluster.auth.enabled` whenever `cluster.tls.enabled`).
///
/// Both rustls configs are TLS 1.3 only with the [`REPLICA_ALPN`]
/// protocol pinned. The dialer's SNI / certificate-verify name for each
/// peer is the roster entry's `ip` field (a hostname or IP literal, the
/// same string the connector dials).
fn load_replica_tls_ctx(
    config: &ServerNgConfig,
    topology: &TcpTopology,
) -> Result<Option<ReplicaTlsCtx>, ServerNgError> {
    let tls = &config.cluster.tls;
    if !config.cluster.enabled || !tls.enabled {
        return Ok(None);
    }
    install_default_crypto_provider();
    let credential_error = |source: std::io::Error| ServerNgError::ListenerCredentials {
        transport: "cluster.tls",
        source,
    };

    let credentials = if tls.self_signed {
        let san = config
            .cluster
            .nodes
            .iter()
            .find(|node| node.replica_id == topology.self_replica_id)
            .map(|node| node.ip.as_str())
            .ok_or_else(|| {
                credential_error(std::io::Error::other(format!(
                    "replica id {} not present in cluster.nodes",
                    topology.self_replica_id
                )))
            })?;
        let (cert_chain, key_der) = server_common::generate_self_signed_certificate(san)
            .map_err(|error| credential_error(std::io::Error::other(error.to_string())))?;
        TlsServerCredentials {
            cert_chain,
            key_der,
        }
    } else {
        load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(credential_error)?
    };

    let mut server =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(credentials.cert_chain, credentials.key_der)
            .map_err(|error| {
                credential_error(std::io::Error::other(format!(
                    "replica TLS server config rejected credentials: {error}"
                )))
            })?;
    server.alpn_protocols = vec![REPLICA_ALPN.to_vec()];

    let client_builder =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13]);
    let mut client = if tls.self_signed {
        client_builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
            .with_no_client_auth()
    } else {
        let roots = load_ca_pem(Path::new(&tls.ca_file)).map_err(credential_error)?;
        client_builder
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth()
    };
    client.alpn_protocols = vec![REPLICA_ALPN.to_vec()];

    // Replica ids form a bijection onto 0..nodes.len() (validated at
    // boot), so sorting by id yields a Vec indexable by replica id.
    // TODO(hubcio): dynamic replica join will break this positional
    // indexing (sparse ids silently map to the wrong SNI/verify name);
    // key peer names by replica id explicitly before supporting it.
    let mut roster: Vec<_> = config.cluster.nodes.iter().collect();
    roster.sort_unstable_by_key(|node| node.replica_id);
    let peer_names = roster
        .iter()
        .map(|node| {
            ServerName::try_from(node.ip.clone()).map_err(|error| {
                credential_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "cluster node '{}' ip '{}' is not a valid TLS server name: {error}",
                        node.name, node.ip
                    ),
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(ReplicaTlsCtx {
        server: Arc::new(server),
        client: Arc::new(client),
        peer_names,
    }))
}

fn load_tcp_tls_server_credentials(
    config: &ServerNgConfig,
) -> Result<TlsServerCredentials, ServerNgError> {
    let tls = &config.tcp.tls;
    if tls.self_signed && !Path::new(&tls.cert_file).exists() {
        return Ok(self_signed_for_loopback());
    }

    load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
        ServerNgError::ListenerCredentials {
            transport: "tcp.tls",
            source,
        }
    })
}

/// Bind the websocket client listener on `ws_addr`: WSS when
/// `websocket.tls.enabled` (the plain-WS accept loop must not also bind the
/// port -- a plain upgrade parser fed a TLS `ClientHello` rejects every
/// connection with an httparse error), plain WS otherwise.
async fn start_websocket_listener(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    ws_addr: SocketAddr,
    accepted_clients: &LocalClientAcceptFns,
) -> Result<SocketAddr, ServerNgError> {
    if config.websocket.tls.enabled {
        let credentials = load_wss_server_credentials(config)?;
        let (listener, tls_config, bound_addr) = client_listener::wss::bind(ws_addr, credentials)
            .map_err(|source| {
            error!(addr = %ws_addr, error = %source, "failed to bind WSS listener");
            source
        })?;
        let token = shard.bus.token();
        let accepted_wss = accepted_clients.wss.clone();
        let wss_handle = compio::runtime::spawn(async move {
            client_listener::wss::run(listener, tls_config, token, accepted_wss).await;
        });
        shard.bus.track_background(wss_handle);
        Ok(bound_addr)
    } else {
        let (listener, bound_addr) =
            client_listener::ws::bind(ws_addr).await.map_err(|source| {
                error!(addr = %ws_addr, error = %source, "failed to bind websocket listener");
                source
            })?;
        let token = shard.bus.token();
        let accepted_ws = accepted_clients.ws.clone();
        let ws_handle = compio::runtime::spawn(async move {
            client_listener::ws::run(listener, token, accepted_ws).await;
        });
        shard.bus.track_background(ws_handle);
        Ok(bound_addr)
    }
}

fn load_wss_server_credentials(
    config: &ServerNgConfig,
) -> Result<TlsServerCredentials, ServerNgError> {
    let tls = &config.websocket.tls;
    if tls.self_signed && !Path::new(&tls.cert_file).exists() {
        return Ok(self_signed_for_loopback());
    }

    load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
        ServerNgError::ListenerCredentials {
            transport: "websocket.tls",
            source,
        }
    })
}

fn load_quic_server_credentials(
    config: &ServerNgConfig,
) -> Result<replica_io::QuicServerCredentials, ServerNgError> {
    let certificate = &config.quic.certificate;
    if certificate.self_signed {
        let (cert_chain, key_der) = server_common::generate_self_signed_certificate("localhost")
            .map_err(|error| ServerNgError::ListenerCredentials {
                transport: "quic",
                source: std::io::Error::other(error.to_string()),
            })?;
        return Ok(replica_io::QuicServerCredentials {
            cert_chain,
            key_der,
        });
    }

    let credentials = load_pem(
        Path::new(&certificate.cert_file),
        Path::new(&certificate.key_file),
    )
    .map_err(|source| ServerNgError::ListenerCredentials {
        transport: "quic",
        source,
    })?;
    Ok(replica_io::QuicServerCredentials {
        cert_chain: credentials.cert_chain,
        key_der: credentials.key_der,
    })
}

fn parse_socket_addr(context: &'static str, address: &str) -> Result<SocketAddr, ServerNgError> {
    address
        .parse()
        .map_err(|source| ServerNgError::SocketAddressParse {
            context,
            address: address.to_string(),
            source,
        })
}

fn socket_addr_from_parts(
    context: &'static str,
    host: &str,
    port: u16,
) -> Result<SocketAddr, ServerNgError> {
    let ip = host
        .parse::<IpAddr>()
        .map_err(|source| ServerNgError::SocketAddressParse {
            context,
            address: format!("{host}:{port}"),
            source,
        })?;
    Ok(SocketAddr::new(ip, port))
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_on_drop_armed_flips_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        drop(ShutdownOnDrop::new(Arc::clone(&flag)));
        assert!(
            flag.load(Ordering::Relaxed),
            "an armed guard must flip the flag on drop (covers the error `?` \
             and panic-unwind exit paths of run_shard_thread)"
        );
    }

    #[test]
    fn shutdown_on_drop_disarmed_leaves_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut guard = ShutdownOnDrop::new(Arc::clone(&flag));
        guard.disarm();
        drop(guard);
        assert!(
            !flag.load(Ordering::Relaxed),
            "a disarmed guard must not flip the flag (clean `Ok(())` exit)"
        );
    }

    const TEST_POLL_INTERVAL: Duration = Duration::from_millis(50);

    #[compio::test]
    async fn broadcast_metadata_bundle_returns_immediately_with_no_peers() {
        // Single-shard deployment: shard 0 has no peers to fan out to,
        // so the handoff must complete without ever calling `send`.
        let (bundle_tx, _bundle_rx) = crossfire::mpmc::bounded_async::<ServerNgMetadataBundle>(0);
        let flag = Arc::new(AtomicBool::new(false));
        let mux = ServerNgMuxStateMachine::default();
        broadcast_metadata_bundle(
            0,
            &bundle_tx,
            mux.factory_bundle(),
            0,
            &flag,
            TEST_POLL_INTERVAL,
        )
        .await
        .expect("zero peers must not block shard 0");
    }

    #[compio::test]
    async fn metadata_bundle_round_trips_through_channel() {
        // End-to-end: shard 0 mints a bundle, a peer receives it on
        // another runtime, and `from_factory_bundle` constructs a
        // reader-mode mux that observes shard 0's writes via the same
        // LeftRight pair.
        let peers = 1u16;
        let (bundle_tx, bundle_rx) =
            crossfire::mpmc::bounded_async::<ServerNgMetadataBundle>(usize::from(peers));
        let flag = Arc::new(AtomicBool::new(false));

        let owner = ServerNgMuxStateMachine::default();
        let bundle = owner.factory_bundle();
        broadcast_metadata_bundle(0, &bundle_tx, bundle, peers, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("broadcast must succeed with one peer drained");

        let received = await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("peer must receive the broadcast bundle");
        let _peer_mux = ServerNgMuxStateMachine::from_factory_bundle(received);
    }

    #[compio::test]
    async fn broadcast_metadata_bundle_aborts_when_peers_drop_rx() {
        // Shard 0 drives handoff but every peer's `bundle_rx` was dropped
        // before recv. Silently returning Ok would commit listener binds
        // and consensus init for a cluster whose peers are gone; the
        // broadcast must surface the disconnect so `shard_main` aborts.
        let (bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerNgMetadataBundle>(0);
        drop(bundle_rx);
        let flag = Arc::new(AtomicBool::new(false));
        let mux = ServerNgMuxStateMachine::default();

        let err = broadcast_metadata_bundle(
            0,
            &bundle_tx,
            mux.factory_bundle(),
            3,
            &flag,
            TEST_POLL_INTERVAL,
        )
        .await
        .expect_err("dropped rx must surface as MetadataHandoffAborted");
        assert!(
            matches!(err, ServerNgError::MetadataHandoffAborted { shard_id: 0 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_metadata_bundle_aborts_when_owner_drops_without_sending() {
        let (bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerNgMetadataBundle>(1);
        let flag = Arc::new(AtomicBool::new(false));

        // Shard 0 dies before broadcasting; the peer must observe the
        // disconnect and abort instead of hanging forever.
        drop(bundle_tx);

        let err = await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect_err("a peer whose owner never sends must abort");
        assert!(
            matches!(err, ServerNgError::MetadataHandoffAborted { shard_id: 1 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_metadata_bundle_aborts_on_shutdown_flag() {
        // compio 0.19 `JoinHandle` yields `Result<T, JoinError>`; the
        // `ResumeUnwind` impl re-raises a task panic and maps cancellation
        // to `None`.
        use compio::runtime::ResumeUnwind;

        let (_bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerNgMetadataBundle>(1);
        let flag = Arc::new(AtomicBool::new(false));

        let waiter = compio::runtime::spawn({
            let flag = Arc::clone(&flag);
            async move { await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL).await }
        });

        // Owner has not sent yet, but shutdown was requested; the peer
        // must exit via the flag poll instead of hanging.
        compio::time::sleep(TEST_POLL_INTERVAL / 2).await;
        flag.store(true, Ordering::Relaxed);

        let err = waiter
            .await
            .resume_unwind()
            .expect("waiter task was cancelled")
            .expect_err("shutdown flag must abort the bundle wait");
        assert!(
            matches!(err, ServerNgError::MetadataHandoffAborted { shard_id: 1 }),
            "expected MetadataHandoffAborted on shutdown, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_bootstrap_complete_returns_immediately_for_single_shard() {
        // A single-shard server has no peers to wait on; the owner barrier
        // must not block when `peers == 0`.
        let (_ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(1);
        let flag = Arc::new(AtomicBool::new(false));
        await_bootstrap_complete(&ready_rx, 0, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("single-shard server must not block on the barrier");
    }

    #[compio::test]
    async fn await_bootstrap_complete_drains_every_peer_signal() {
        // Two peers report load-complete; shard 0 drains both, then proceeds
        // to bind listeners.
        let (ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(2);
        let flag = Arc::new(AtomicBool::new(false));
        signal_bootstrap_complete(1, &ready_tx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("peer 1 must signal load-complete");
        signal_bootstrap_complete(2, &ready_tx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("peer 2 must signal load-complete");
        await_bootstrap_complete(&ready_rx, 2, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("owner must drain both peer signals");
    }

    #[compio::test]
    async fn await_bootstrap_complete_aborts_on_shutdown_flag() {
        use compio::runtime::ResumeUnwind;

        // `_ready_tx` is held so the channel is not disconnected: the owner
        // must exit via the shutdown flag, not a dropped sender.
        let (_ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(1);
        let flag = Arc::new(AtomicBool::new(false));

        let owner = compio::runtime::spawn({
            let flag = Arc::clone(&flag);
            async move { await_bootstrap_complete(&ready_rx, 1, &flag, TEST_POLL_INTERVAL).await }
        });

        // The peer never signals, but a sibling failure flips the flag; the
        // owner must abort instead of hanging before listeners.
        compio::time::sleep(TEST_POLL_INTERVAL / 2).await;
        flag.store(true, Ordering::Relaxed);

        let err = owner
            .await
            .resume_unwind()
            .expect("owner task was cancelled")
            .expect_err("shutdown flag must abort the barrier wait");
        assert!(
            matches!(
                err,
                ServerNgError::ShardBootstrapBarrierAborted { remaining: 1 }
            ),
            "expected ShardBootstrapBarrierAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn signal_bootstrap_complete_aborts_when_owner_drops_rx() {
        // Shard 0 aborted before draining and dropped its receiver; a peer's
        // signal must surface the disconnect instead of stranding.
        let (ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(1);
        let flag = Arc::new(AtomicBool::new(false));
        drop(ready_rx);

        let err = signal_bootstrap_complete(2, &ready_tx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect_err("dropped rx must surface as an abort");
        assert!(
            matches!(err, ServerNgError::MetadataHandoffAborted { shard_id: 2 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }
}
