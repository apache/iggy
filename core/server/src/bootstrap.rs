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
    build_partition_fresh, configure_consumer_offsets, ensure_initial_segment,
    open_partition_superblock,
};
use crate::segment_recovery::{RecoveredSegment, load_persisted_segments};
use crate::server_error::{ServerError, ShardJoinFailure, ShardJoinFailureKind};
use crate::session_manager::SessionManager;
use compio::runtime::ResumeUnwind;
use configs::server::{ServerConfig, ServerSystemConfig};
use configs::sharding::{
    INBOX_CAPACITY_MAX, SHUTDOWN_DRAIN_TIMEOUT_MAX, SHUTDOWN_POLL_INTERVAL_MAX,
};
use consensus::{
    ClientTable, ConsensusTimers, JoinMode, LocalPipeline, MetadataHandle, PartitionsHandle,
    PipelineEntry, Sequencer, VsrConsensus, VsrRestore,
};
// `try_send` / `try_recv` resolve through these traits on `MAsyncTx` /
// `MAsyncRx`; the metadata-handoff loops below depend on the
// non-blocking variants for cancel-safe shutdown polling.
use consensus::VsrState;
use crossfire::{AsyncRxTrait, AsyncTxTrait};
use iggy_binary_protocol::{Operation, PrepareHeader};
use iggy_common::defaults::{
    DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
    MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use iggy_common::{
    Aes256GcmEncryptor, EncryptorKind, IggyByteSize, PartitionStats, TopicRuntimeOptions, variadic,
};
use journal::prepare_journal::PrepareJournal;
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use journal::{Journal, JournalHandle};
use message_bus::client_listener::{self, RequestHandler};
use message_bus::installer;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::auth::{self, ReplicaAuth};
use message_bus::replica::handshake::{ReplicaHandshakeCtx, ReplicaTlsCtx};
use message_bus::replica::io as replica_io;
use message_bus::replica::listener::{self as replica_listener, MessageHandler};
use message_bus::transports::quic::server_config_with_cert;
use message_bus::transports::tls::{
    AcceptAnyServerCert, REPLICA_ALPN, TlsServerCredentials, install_default_crypto_provider,
    load_ca_pem, load_pem, self_signed_for_loopback,
};
use message_bus::{
    AcceptedClientFn, AcceptedQuicClientFn, AcceptedReplicaFn, AcceptedTlsClientFn,
    AcceptedWsClientFn, AcceptedWssClientFn, ConnectionInstaller, DialedReplicaFn, IggyMessageBus,
    MAX_INFLIGHT_REPLICA_HANDSHAKES, MessageBus, ReplicaOwnerTable, connector,
};
use metadata::IggyMetadata;
use metadata::MuxStateMachine;
use metadata::ReplicaIdentity;
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
use server_common::Message;
use server_common::bootstrap::create_directories;
use server_common::crypto;
use server_common::executor::create_shard_executor;
use server_common::fs_utils::remove_dir_all;
use server_common::log::{Logging, LoggingSettings, TelemetrySettings};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use shard::builder::IggyShardBuilder;
use shard::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};
use shard::shards_table::{PapayaShardsTable, ShardsTable, calculate_shard_assignment};
use shard::{
    CoordinatorConfig, IggyShard, LifecycleFrame, ListClientsHandler, MetadataSubmitHandler,
    PartitionConsensusConfig, PartitionReadHandler, Receiver as ShardReceiver, ShardFrame,
    ShardIdentity, TaggedSender, channel, shard_mesh_channels,
};
use shard_allocator::{ShardAllocator, ShardInfo};
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::panic;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

const SHARD_REPLICA_ID: u8 = 0;

pub const IGGY_ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";
pub const IGGY_ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";

type ServerMuxStateMachine = MuxStateMachine<variadic!(Users, Streams)>;

/// Cross-thread bundle carrying one `ReadHandleFactory` per metadata
/// state. Shard 0 mints one after `recover()` and broadcasts a clone to
/// every peer shard; each peer rebuilds a reader-mode
/// [`ServerMuxStateMachine`] on its own runtime, skipping the WAL.
type ServerMetadataBundle = <variadic!(Users, Streams) as WithFactory>::Bundle;

pub(crate) type ServerMetadata = IggyMetadata<
    VsrConsensus<Rc<IggyMessageBus>>,
    PrepareJournal,
    IggySnapshot,
    ServerMuxStateMachine,
>;

/// The shard type the dispatch layer is generic over.
///
/// `B`/`MJ`/`S`/`SB` are free; the metadata state machine (`M`) and shards
/// table (`T`) are pinned, being identical in production and the simulator.
/// Production instantiates it as [`ServerShard`], defaulting `SB` to the
/// on-disk [`PingPongSuperblock`]; the simulator supplies its own
/// `B`/`MJ`/`S`/`SB`.
pub type ShellShard<B, MJ, S, SB = PingPongSuperblock> =
    IggyShard<B, MJ, S, ServerMuxStateMachine, PapayaShardsTable, SB>;

/// Late-bound self-reference the deferred dispatch handlers upgrade per frame.
pub type ShellShardHandle<B, MJ, S, SB = PingPongSuperblock> =
    Rc<RefCell<Option<Weak<ShellShard<B, MJ, S, SB>>>>>;

/// Bus bounds the dispatch/pump path needs (matches `run_message_pump`).
/// Blanket-impl'd, so it is only shorthand for the four underlying bounds.
pub trait ShellBus: MessageBus + ConnectionInstaller + Clone + 'static {}
impl<B: MessageBus + ConnectionInstaller + Clone + 'static> ShellBus for B {}

/// The five dispatch handlers a shard is built with, plus the
/// [`SessionManager`] the request-plane pair shares.
///
/// Both production ([`build_shard_for_thread`]) and the simulator's shell
/// mode construct these through [`wire_shell_handlers`], so the request
/// plane is wired one way. The simulator's shell-off fast path uses
/// [`ShellHandlers::noop`] instead.
pub struct ShellHandlers {
    pub on_replica_message: MessageHandler,
    pub on_client_request: RequestHandler,
    pub on_metadata_submit: MetadataSubmitHandler,
    pub on_list_clients: ListClientsHandler,
    pub on_partition_read: PartitionReadHandler,
    /// Bound by the client-request handler, read by the get-clients
    /// handler; the caller keeps it to reach locally-homed sessions.
    pub sessions: Rc<RefCell<SessionManager>>,
}

impl ShellHandlers {
    /// Inert handlers for the shell-off fast path: every callback is a
    /// no-op over an empty [`SessionManager`]. Behaviorally identical to
    /// hand-written no-op closures, so a caller can keep one destructure
    /// site across both toggle states.
    #[must_use]
    pub fn noop() -> Self {
        Self {
            on_replica_message: Rc::new(|_, _| {}),
            on_client_request: Rc::new(|_, _| {}),
            on_metadata_submit: Rc::new(|_| {}),
            on_list_clients: Rc::new(|_| {}),
            on_partition_read: Rc::new(|_, _, _| {}),
            sessions: Rc::new(RefCell::new(SessionManager::new())),
        }
    }
}

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
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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

pub type ServerShard = ShellShard<Rc<IggyMessageBus>, PrepareJournal, IggySnapshot>;

/// Result of a multi-shard bootstrap.
///
/// Carries the cross-thread shutdown flag and one OS-thread `JoinHandle`
/// per shard. The caller flips the flag via [`Self::install_ctrlc_handler`]
/// and then drains every shard via [`Self::join_all`], bounded by
/// `join_timeout` (`system.sharding.shutdown_join_timeout`).
pub struct ShardHandles {
    shutdown_flag: Arc<AtomicBool>,
    shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerError>>)>,
    join_timeout: Duration,
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

    /// Drain every shard thread. This is the main thread's park for the
    /// server's whole lifetime, so shards are awaited WITHOUT any time
    /// bound while the server runs; the `shutdown_join_timeout` clock
    /// only starts once the cross-thread shutdown flag flips (Ctrl-C or
    /// a shard failure). Each shard's outcome is logged (`info` on clean
    /// exit, `error` on Err, panic, or wedge). If any shard failed,
    /// returns every failure together as
    /// [`ServerError::ShardJoinFailures`] so the operator sees the
    /// full set rather than just the first.
    ///
    /// A shard whose thread is still running when the post-shutdown
    /// deadline passes is abandoned (its `JoinHandle` dropped, the OS
    /// thread left to die with the process) and reported as
    /// [`ShardJoinFailureKind::Wedged`]: a wedged pump or listener must
    /// not block process exit forever.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::ShardJoinFailures`] if any shard
    /// returned a `Result::Err`, panicked, or wedged past the deadline.
    /// The variant carries every per-shard failure in shard-id order so
    /// the caller does not need to read the trace log to discover
    /// late-failing shards.
    pub fn join_all(self) -> Result<(), ServerError> {
        let mut failures: Vec<ShardJoinFailure> = Vec::new();
        // Armed on the first poll that observes the shutdown flag, shared
        // across all shards: one budget covers the whole drain, not one
        // budget per shard.
        let mut deadline: Option<Instant> = None;
        // Shards run thread-per-core with compio's blocking fallback pool
        // disabled, so an io_uring opcode the kernel lacks aborts every shard
        // with the same panic. Surface the actionable diagnostic once.
        let mut io_uring_diagnostic_shown = false;
        for (shard_id, handle) in self.shard_threads {
            let Some(joined) = join_until_shutdown_deadline(
                handle,
                &self.shutdown_flag,
                self.join_timeout,
                &mut deadline,
            ) else {
                error!(
                    shard_id,
                    waited = ?self.join_timeout,
                    "shard thread still running at the shutdown join deadline; abandoning it"
                );
                failures.push(ShardJoinFailure {
                    shard_id,
                    kind: ShardJoinFailureKind::Wedged {
                        waited: self.join_timeout,
                    },
                });
                continue;
            };
            match joined {
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
            Err(ServerError::ShardJoinFailures { failures })
        }
    }
}

/// Poll cadence for the bounded shard joins. Coarse enough to cost
/// nothing during a normal drain, fine enough that exit latency past
/// the last shard's return stays imperceptible.
const JOIN_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Join `handle`, waiting indefinitely while the server runs. The
/// `join_timeout` clock starts only when `shutdown_flag` is observed set
/// (arming the caller-shared `deadline` once, so all shards drain under
/// ONE budget); a running server parked here for hours must never be
/// mistaken for a wedged shard. `None` means the thread was still
/// running at the post-shutdown deadline and the handle was dropped
/// (the OS thread keeps running detached; process exit reaps it).
/// `JoinHandle` has no timed join, so this polls `is_finished` at
/// [`JOIN_POLL_INTERVAL`]; the closing `join()` on a finished thread
/// returns immediately.
fn join_until_shutdown_deadline(
    handle: thread::JoinHandle<Result<(), ServerError>>,
    shutdown_flag: &AtomicBool,
    join_timeout: Duration,
    deadline: &mut Option<Instant>,
) -> Option<thread::Result<Result<(), ServerError>>> {
    while !handle.is_finished() {
        if deadline.is_none() && shutdown_flag.load(Ordering::Relaxed) {
            *deadline = Some(Instant::now() + join_timeout);
        }
        if let Some(deadline) = deadline
            && Instant::now() >= *deadline
        {
            return None;
        }
        thread::sleep(JOIN_POLL_INTERVAL);
    }
    Some(handle.join())
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

/// Joins survivor shard threads after a partial-spawn failure, bounded
/// by the same `shutdown_join_timeout` budget as the normal exit path.
///
/// Polls every survivor's `is_finished` in one loop instead of spawning
/// per-survivor joiner threads: the likely OS state on this path is
/// `pthread_create` EAGAIN (the parent spawn just failed with it), so
/// nothing here may create threads, and polling drains all survivors in
/// parallel anyway. A survivor still running at the deadline is
/// abandoned with an error log so the failed bootstrap can surface its
/// spawn error instead of hanging on a wedged shard.
fn join_partial_shard_survivors(
    shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerError>>)>,
    join_timeout: Duration,
) {
    let deadline = Instant::now() + join_timeout;
    let mut remaining = shard_threads;
    loop {
        let mut still_running = Vec::with_capacity(remaining.len());
        for (shard_id, survivor) in remaining {
            if survivor.is_finished() {
                let _ = survivor.join();
                info!(shard_id, "survivor shard thread drained");
            } else {
                still_running.push((shard_id, survivor));
            }
        }
        remaining = still_running;
        if remaining.is_empty() || Instant::now() >= deadline {
            break;
        }
        thread::sleep(JOIN_POLL_INTERVAL);
    }
    for (shard_id, _survivor) in remaining {
        error!(
            shard_id,
            waited = ?join_timeout,
            "survivor shard thread still running at the shutdown join deadline; abandoning it"
        );
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
/// `WriteHandle`-bearing [`ServerMuxStateMachine`]. It then mints a
/// [`ServerMetadataBundle`] (a tuple of `Send + Sync`
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
        bundle_tx: crossfire::MAsyncTx<crossfire::mpmc::Array<ServerMetadataBundle>>,
    },
    Waiter {
        bundle_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<ServerMetadataBundle>>,
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
    http_listen_addr: Option<SocketAddr>,
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
    sharding: &configs::sharding::ShardingConfig,
) -> Result<(Vec<ShardInfo>, u16), ServerError> {
    let allocator = ShardAllocator::new(&sharding.cpu_allocation, sharding.pin_cores)
        .map_err(ServerError::ShardAllocator)?;
    let assignments = allocator
        .to_shard_assignments()
        .map_err(ServerError::ShardAllocator)?;
    if assignments.is_empty() {
        return Err(ServerError::ShardsCountZero);
    }
    match u16::try_from(assignments.len()) {
        Ok(count) if count < message_bus::OWNER_NONE => Ok((assignments, count)),
        _ => Err(ServerError::ShardsCountOverflow {
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
    sharding: &configs::sharding::ShardingConfig,
) -> Result<(), ServerError> {
    let inbox_capacity = sharding.inbox_capacity;
    if inbox_capacity == 0 || inbox_capacity > INBOX_CAPACITY_MAX {
        return Err(ServerError::InvalidInboxCapacity {
            value: inbox_capacity,
            max: INBOX_CAPACITY_MAX,
        });
    }
    let drain_timeout = sharding.shutdown_drain_timeout.get_duration();
    if drain_timeout.is_zero() || drain_timeout > SHUTDOWN_DRAIN_TIMEOUT_MAX {
        return Err(ServerError::InvalidShutdownDrainTimeout {
            value: drain_timeout,
            max: SHUTDOWN_DRAIN_TIMEOUT_MAX,
        });
    }
    let poll_interval = sharding.shutdown_poll_interval.get_duration();
    if poll_interval.is_zero() || poll_interval > SHUTDOWN_POLL_INTERVAL_MAX {
        return Err(ServerError::InvalidShutdownPollInterval {
            value: poll_interval,
            max: SHUTDOWN_POLL_INTERVAL_MAX,
        });
    }
    // Ordering: a poll cadence coarser than the drain budget makes the
    // cross-thread shutdown flag effectively unobservable during teardown.
    if poll_interval > drain_timeout {
        return Err(ServerError::ShutdownPollExceedsDrain {
            poll: poll_interval,
            drain: drain_timeout,
        });
    }
    Ok(())
}

/// Spawn the multi-shard `server` runtime.
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
    config: ServerConfig,
    current_replica_id: Option<u8>,
) -> Result<ShardHandles, ServerError> {
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
    validate_sharding_runtime_knobs(&config.system.sharding)?;

    let (senders, mut inboxes) = shard_mesh_channels(total_shards, inbox_capacity);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
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
            .spawn(move || -> Result<(), ServerError> {
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
    config: Arc<ServerConfig>,
    shutdown_flag: Arc<AtomicBool>,
    metadata_handoff: MetadataHandoff,
    barrier: BootstrapBarrier,
    owner_table: Arc<ReplicaOwnerTable>,
    metadata_view: Arc<AtomicU64>,
) -> Result<(), ServerError> {
    // Armed for the whole thread body: a post-spawn error `?` or a panic
    // unwind here must flip `shutdown_flag` so sibling watchdogs drive
    // their bus shutdown instead of parking forever on `bus.token().wait()`.
    let mut shutdown_guard = ShutdownOnDrop::new(Arc::clone(&shutdown_flag));

    assignment
        .bind_cpu()
        .map_err(|source| ServerError::CpuAffinityFailed { shard_id, source })?;
    assignment
        .bind_memory()
        .map_err(|source| ServerError::MemoryAffinityFailed { shard_id, source })?;

    // `enrich_runtime_create_error` folds the io_uring remediation (raise
    // `ulimit -l`, unblock seccomp, kernel-flag floor) into the error, so the
    // guidance survives into the shard-join failure report instead of only
    // stderr. Multi-shard boxes exhaust RLIMIT_MEMLOCK on per-shard rings
    // before the bootstrap runtime does, so this path needs it most.
    let runtime = create_shard_executor().map_err(|source| {
        let source = server_common::diagnostics::enrich_runtime_create_error(source);
        ServerError::ShardRuntimeCreateFailed { shard_id, source }
    })?;

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
    config: &ServerConfig,
    shutdown_flag: Arc<AtomicBool>,
    metadata_handoff: MetadataHandoff,
    barrier: BootstrapBarrier,
    owner_table: Arc<ReplicaOwnerTable>,
    metadata_view: Arc<AtomicU64>,
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
                |mux_stm, client, timestamp| {
                    mux_stm
                        .streams()
                        .remove_consumer_group_member(client, timestamp);
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
    );
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
    let mut pump_handle = Some(compio::runtime::spawn(async move {
        pump_shard.run_message_pump(stop_rx).await;
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

    // One keep-alive per process, so shard 0 owns it. Started before the
    // listeners bind: systemd counts `WatchdogSec=` from unit start, not from
    // `READY=1`, so a slow recovery must not look like a hang.
    #[cfg(feature = "systemd")]
    if shard_id == 0 {
        crate::systemd::spawn_watchdog(&bus);
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
        crate::systemd::notify_ready();
    }

    bus.token().wait().await;
    #[cfg(feature = "systemd")]
    if shard_id == 0 {
        crate::systemd::notify_stopping();
    }
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

/// Await the message pump's completion before the shard returns: its
/// post-loop work includes the final flush of every committed journal to
/// segment storage, and returning first drops the compio runtime, which
/// cancels that flush at its next await point.
///
/// `Err` means the pump was already dead (a panic, or an exit outside the
/// stop protocol), so its final flush never ran and the shard must not
/// report a clean exit. The verdict is the inner `JoinError`; the timeout
/// wrapper alone cannot see it, and a shard that swallows it prints
/// "exited cleanly" over a corpse.
async fn await_pump_drain(
    pump_handle: Option<compio::runtime::JoinHandle<()>>,
    config: &ServerConfig,
    shard_id: u16,
) -> Result<(), ServerError> {
    let Some(pump_handle) = pump_handle else {
        return Ok(());
    };
    let drain_budget = config.system.sharding.shutdown_drain_timeout.get_duration();
    let Ok(join_result) = compio::time::timeout(drain_budget, pump_handle).await else {
        error!(
            shard = shard_id,
            timeout = ?drain_budget,
            "message pump did not drain within the shutdown budget; \
             committed journal tail may not have flushed"
        );
        return Err(ServerError::ShardPumpDrainTimedOut {
            shard_id,
            timeout: drain_budget,
        });
    };
    // `JoinError` renders a panic as the bare "Task has panicked" and the
    // type is not re-exported, so the payload -- the only part with
    // diagnostic value -- is lifted by re-raising into an immediate catch.
    // The panic hook already ran when the task died; `resume_unwind` does
    // not run it again, so nothing is printed twice and the message finally
    // reaches the tracing sink too.
    let reason = match panic::catch_unwind(panic::AssertUnwindSafe(|| join_result.resume_unwind()))
    {
        Ok(Some(())) => return Ok(()),
        Ok(None) => "task was cancelled".to_string(),
        Err(payload) => payload
            .downcast_ref::<&str>()
            .map(|message| (*message).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .map_or_else(
                || "task panicked".to_string(),
                |message| format!("task panicked: {message}"),
            ),
    };
    error!(
        shard = shard_id,
        "message pump died instead of draining ({reason}); \
         committed journal tail may not have flushed"
    );
    Err(ServerError::ShardPumpDied { shard_id, reason })
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
    bundle_rx: &crossfire::MAsyncRx<crossfire::mpmc::Array<ServerMetadataBundle>>,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<ServerMetadataBundle, ServerError> {
    loop {
        match bundle_rx.try_recv() {
            Ok(bundle) => return Ok(bundle),
            Err(crossfire::TryRecvError::Disconnected) => {
                return Err(ServerError::MetadataHandoffAborted { shard_id });
            }
            Err(crossfire::TryRecvError::Empty) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerError::MetadataHandoffAborted { shard_id });
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
    bundle_tx: &crossfire::MAsyncTx<crossfire::mpmc::Array<ServerMetadataBundle>>,
    bundle: ServerMetadataBundle,
    peers: u16,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<(), ServerError> {
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
                    return Err(ServerError::MetadataHandoffAborted { shard_id });
                }
                Err(crossfire::TrySendError::Full(returned)) => {
                    if shutdown_flag.load(Ordering::Relaxed) {
                        return Err(ServerError::MetadataHandoffAborted { shard_id });
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
) -> Result<(), ServerError> {
    let mut pending = shard_id;
    loop {
        match ready_tx.try_send(pending) {
            Ok(()) => return Ok(()),
            Err(crossfire::TrySendError::Disconnected(_)) => {
                // Shard 0 dropped its `ready_rx` before draining (it
                // aborted before binding listeners). Propagate so this
                // shard short-circuits; the shutdown flag flips via the
                // normal teardown path.
                return Err(ServerError::MetadataHandoffAborted { shard_id });
            }
            Err(crossfire::TrySendError::Full(returned)) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerError::MetadataHandoffAborted { shard_id });
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
) -> Result<(), ServerError> {
    let mut remaining = peers;
    while remaining > 0 {
        match ready_rx.try_recv() {
            Ok(_shard_id) => remaining -= 1,
            Err(crossfire::TryRecvError::Disconnected) => {
                return Err(ServerError::ShardBootstrapBarrierAborted { remaining });
            }
            Err(crossfire::TryRecvError::Empty) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerError::ShardBootstrapBarrierAborted { remaining });
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
///
/// The caller owns the returned handle and must await it on the exit paths
/// where shutdown is in progress (flag set or bus token triggered):
/// dropping it there cancels the watchdog mid-`bus.shutdown()`, truncating
/// in-flight `ClientForwardFailed` replies (terminal per `SendError` docs).
/// It cannot go through `bus.track_background` instead: the watchdog itself
/// drives `bus.shutdown()`, and the bg-drain loop in `shutdown()` would
/// re-enter awaiting the watchdog's own pending shutdown call
/// (self-deadlock). The await is bounded: once the token fires the loop
/// stands down within one poll interval, and the shutdown call itself is
/// capped by `drain_timeout`.
#[allow(clippy::needless_pass_by_value)]
fn spawn_shutdown_watchdog(
    bus: Rc<IggyMessageBus>,
    shutdown_flag: Arc<AtomicBool>,
    drain_timeout: Duration,
    poll_interval: Duration,
) -> compio::runtime::JoinHandle<()> {
    let bus_for_task = Rc::clone(&bus);
    let bus_token = bus.token();
    compio::runtime::spawn(async move {
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
    })
}

/// Copy the configured cluster roster plus this node's own client ports into
/// the shared [`ClusterRoster`] so the binary `GetClusterMetadata` read serves
/// the real topology. `self_*` back only the cluster-disabled self-synthesis
/// and carry the requested listener ports from the resolved topology, not the
/// bound ones (a `:0` wildcard is reported as 0).
fn build_cluster_roster(
    config: &ServerConfig,
    topology: &TcpTopology,
    metadata_view: Arc<AtomicU64>,
) -> ClusterRoster {
    ClusterRoster {
        enabled: config.cluster.enabled,
        name: config.cluster.name.clone(),
        nodes: config
            .cluster
            .nodes
            .iter()
            .cloned()
            .map(Into::into)
            .collect(),
        self_ip: topology.client_listen_addr.ip().to_string(),
        self_ports: configs::cluster::TransportPorts {
            tcp: Some(topology.client_listen_addr.port()),
            quic: topology.quic_listen_addr.map(|addr| addr.port()),
            http: topology.http_listen_addr.map(|addr| addr.port()),
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
    config: &ServerConfig,
    topology: &TcpTopology,
    metadata: ServerMetadata,
    bus: Rc<IggyMessageBus>,
    senders: Vec<TaggedSender>,
    inbox: ShardReceiver<ShardFrame>,
    metrics: ShardMetrics,
    metadata_view: Arc<AtomicU64>,
) -> Result<(Rc<ServerShard>, Rc<RefCell<SessionManager>>), ServerError> {
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
            .map_err(|error| ServerError::Iggy(Box::new(error)))?;
        Some(Arc::new(EncryptorKind::Aes256Gcm(aes)))
    } else {
        None
    };
    let partitions = IggyPartitions::with_capacity(
        shard_local_id,
        PartitionsConfig {
            messages_required_to_save: iggy_common::DEFAULT_MESSAGES_REQUIRED_TO_SAVE,
            size_of_messages_required_to_save: IggyByteSize::from(
                iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
            ),
            enforce_fsync: iggy_common::DEFAULT_ENFORCE_FSYNC,
            validate_checksum: config.system.partition.validate_checksum,
            segment_size: IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
            preallocate_segments: iggy_common::DEFAULT_PREALLOCATE_SEGMENTS,
            encryptor,
            path_layout: partitions::PartitionPathLayout {
                streams_root: config.system.get_streams_path(),
                topics_dir: config.system.topic.path.clone(),
                partitions_dir: config.system.partition.path.clone(),
            },
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
                        owned.push((
                            stream.id,
                            topic_id,
                            stats,
                            partition.clone(),
                            TopicRuntimeOptions::from_resource_options(&topic.options),
                        ));
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
    for (stream_id, topic_id, partition_stats, partition_metadata, topic_runtime) in owned {
        let namespace = IggyNamespace::new(stream_id, topic_id, partition_metadata.id);
        let partition = match load_partition(
            config,
            namespace,
            Arc::clone(&partition_stats),
            &partition_metadata,
            topic_runtime,
            topology.cluster_id,
            topology.self_replica_id,
            topology.replica_count,
            Rc::clone(&bus),
        )
        .await
        {
            Ok(partition) => partition,
            // ONE damaged local chain must not take the node down. The shapes
            // this refuses are exactly what a failed state-transfer quarantine
            // leaves behind, so fence that group the same way the runtime path
            // does -- move its segment files aside, keeping the superblock so it
            // cannot re-enter view 0 -- and materialise it fresh. The ordinary
            // rejoin path (repair, then state transfer on a refused floor)
            // recovers its data from a peer.
            Err(ServerError::PartitionChainRefused { dir, reason, .. }) => {
                let partition_dir = dir.to_string_lossy().into_owned();
                error!(
                    stream_id,
                    topic_id,
                    partition_id = partition_metadata.id,
                    partition_dir,
                    %reason,
                    "refusing the recovered segment chain; fencing this partition and \
                     rebuilding it empty for the rejoin path"
                );
                match partitions::state_transfer::quarantine_segment_files(&partition_dir).await {
                    Ok(fenced_dir) => error!(
                        stream_id,
                        topic_id,
                        partition_id = partition_metadata.id,
                        fenced_dir,
                        "quarantined the refused segment files; they are kept for inspection"
                    ),
                    Err(error) => {
                        // NOT rebuilt: `build_partition_fresh` reaches
                        // `ensure_initial_segment`, which opens segment 0 with
                        // `file_exists = false` and TRUNCATES whatever the
                        // failed quarantine left behind. The likeliest failures
                        // (suffix cap exhausted, `create_dir_all`) move zero
                        // files, so rebuilding would destroy the oldest segment
                        // on the first attempt while the higher-offset survivors
                        // keep refusing every boot -- a loop that never
                        // terminates and eats the chain one segment at a time.
                        // Tombstone instead: the namespace stays unmaterialised
                        // and unrouted, the reconciler backs off, and an
                        // operator still has every byte.
                        error!(
                            stream_id,
                            topic_id,
                            partition_id = partition_metadata.id,
                            partition_dir,
                            %error,
                            "failed to quarantine the refused segment files; leaving this \
                             partition tombstoned rather than rebuilding over them"
                        );
                        partition_stats.zero_out_all();
                        partitions.tombstone(namespace);
                        continue;
                    }
                }
                // The refused load already folded its segment counts in.
                partition_stats.zero_out_all();
                build_partition_fresh(
                    config,
                    namespace,
                    partition_stats,
                    partition_metadata.created_revision,
                    topic_runtime,
                    topology.cluster_id,
                    topology.self_replica_id,
                    topology.replica_count,
                    Rc::clone(&bus),
                )
                .await?
            }
            // An untrustworthy superblock fences ONE group, not the node. The
            // segment files stay exactly where they are -- unlike a refused
            // chain, the data on disk is not the thing in doubt -- so there is
            // nothing to quarantine and nothing to rebuild: rebuilding fresh
            // would hand this replica a view-0 identity while a record it
            // cannot read says otherwise. Tombstoned, the namespace stays
            // unmaterialised and unrouted, the reconciler backs off, and an
            // operator has every byte plus a message naming the directory.
            Err(
                error @ (ServerError::PartitionSuperblockIo { .. }
                | ServerError::PartitionSuperblockVersionUnknown { .. }
                | ServerError::PartitionSuperblockUnverifiable { .. }
                | ServerError::PartitionSuperblockUndecodable { .. }
                | ServerError::PartitionSuperblockIdentityMismatch { .. }),
            ) => {
                error!(
                    stream_id,
                    topic_id,
                    partition_id = partition_metadata.id,
                    %error,
                    "cannot trust this partition's durable consensus state; tombstoning the \
                     partition and continuing to boot the rest of the shard"
                );
                partition_stats.zero_out_all();
                partitions.tombstone(namespace);
                continue;
            }
            Err(error) => return Err(error),
        };
        partitions.insert(namespace, partition);
        shards_table.insert(
            namespace,
            PartitionLocation::new(ShardId::new(shard_id), partition_metadata.created_revision),
        );
    }

    let shard_handle = Rc::new(RefCell::new(None));
    // Same wiring path as the simulator's shell mode: one per-shard
    // SessionManager shared by the client-request handler (binds sessions)
    // and the get_clients handler (reads them). It also carries this shard's
    // cluster roster for the GetClusterMetadata read.
    let ShellHandlers {
        on_replica_message,
        on_client_request,
        on_metadata_submit,
        on_list_clients,
        on_partition_read,
        sessions,
    } = wire_shell_handlers(
        &bus,
        &shard_handle,
        Arc::clone(&config.system),
        config.personal_access_token.max_tokens_per_user,
    );
    sessions
        .borrow_mut()
        .set_cluster_roster(Rc::new(build_cluster_roster(
            config,
            topology,
            metadata_view,
        )));
    let shard_name = format!("server-shard-{shard_id}");
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
        CoordinatorConfig {
            skip_shard_zero_for_replicas: config.cluster.coordinator.skip_shard_zero_for_replicas,
            skip_shard_zero_for_clients: config.cluster.coordinator.skip_shard_zero_for_clients,
        },
        metrics,
    )
    .build()
    .map_err(ServerError::ShardConstruction)?;

    let shard = Rc::new(built.shard);
    // Repair pacing is shared by both planes' repair loops, so it is a
    // per-shard tunable set once here rather than per consensus group.
    shard.set_repair_retry_ticks(repair_retry_ticks(config));
    shard.set_superblock_wedged_fatal_failures(superblock_wedged_fatal_failures(config));
    shard.set_served_segment_cache_bytes_max(
        config
            .partition
            .transfer_served_cache_bytes_max
            .as_bytes_u64(),
    );
    shard.set_partition_artifact_len_max(
        config.partition.transfer_artifact_bytes_max.as_bytes_u64(),
    );
    shard.set_repair_chunk_max(config.cluster.repair_chunk_max as u64);
    // Bounds a served state-transfer chunk. A frame above the bus ceiling is
    // rejected by the RECEIVING transport, which tears the replica connection
    // down rather than dropping one message.
    shard.set_bus_max_message_size(
        usize::try_from(config.message_bus.max_message_size.as_bytes_u64()).unwrap_or(usize::MAX),
    );
    *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
    Ok((shard, sessions))
}

// Pin the configs-crate default literals (duplicated there to avoid a
// build-time edge onto the runtime crates) against the runtime constants,
// mirroring the message_bus IOV_MAX pin. A drift on either side fails this
// crate's build until both are reconciled.
const _: () = assert!(
    configs::metadata::DEFAULT_METADATA_PREPARE_QUEUE_DEPTH
        == consensus::PIPELINE_PREPARE_QUEUE_MAX
);
const _: () = assert!(
    configs::metadata::DEFAULT_METADATA_JOURNAL_SLOTS
        == journal::prepare_journal::DEFAULT_SLOT_COUNT
);
const _: () = assert!(
    configs::partition::DEFAULT_PARTITION_PREPARE_QUEUE_DEPTH
        == consensus::PIPELINE_PREPARE_QUEUE_MAX
);
const _: () =
    assert!(configs::metadata::DEFAULT_METADATA_CLIENTS_TABLE_MAX == consensus::CLIENTS_TABLE_MAX);
const _: () =
    assert!(configs::cluster::DEFAULT_VIEW_PROBE_ATTEMPTS_MAX == consensus::PROBE_ATTEMPTS_MAX);
const _: () =
    assert!(configs::partition::DEFAULT_EVICTED_RING_CAPACITY == partitions::EVICTED_RING_CAPACITY);
const _: () = assert!(
    configs::partition::DEFAULT_EVICTED_RING_BYTES_MAX == partitions::EVICTED_RING_BYTES_MAX
);
const _: () = assert!(
    configs::partition::DEFAULT_TRANSFER_ARTIFACT_BYTES_MAX
        == shard::PARTITION_ARTIFACT_LEN_DEFAULT
);
const _: () = assert!(
    configs::partition::DEFAULT_TRANSFER_SERVED_CACHE_BYTES_MAX
        == shard::SERVED_SEGMENT_CACHE_BYTES_DEFAULT
);
const _: () = assert!(configs::cluster::DEFAULT_REPAIR_CHUNK_MAX as u64 == shard::REPAIR_CHUNK_MAX);
const _: () = assert!(
    configs::cluster::STATE_CHUNK_HEADER_LEN
        == size_of::<iggy_binary_protocol::consensus::StateChunkHeader>() as u64
);
// Both prepare-queue ceilings are pinned by the view-change wire, not by memory: a
// `DoViewChange` carries the sender's suffix spanning `commit..=op` with one nack
// bit and one present bit per entry, each bitset a single `u128`. The depth bounds
// `op - commit`, so a depth at or above `DVC_HEADERS_MAX` produces entries the new
// primary can neither adopt nor prove dead. Strictly less than, because the head op
// needs the reserved slot.
const _: () =
    assert!(configs::metadata::MAX_METADATA_PREPARE_QUEUE_DEPTH < consensus::DVC_HEADERS_MAX);
const _: () =
    assert!(configs::partition::MAX_PARTITION_PREPARE_QUEUE_DEPTH < consensus::DVC_HEADERS_MAX);
// `DVC_HEADERS_MAX` is a bare literal in both the wire crate, which sizes the
// bitsets, and the consensus crate, which cannot depend on it the other way around.
// Same u128, so a drift lets one side address entries the other cannot.
const _: () =
    assert!(consensus::DVC_HEADERS_MAX == iggy_binary_protocol::consensus::DVC_HEADERS_MAX);
const _: () = assert!(consensus::DVC_HEADERS_MAX == u128::BITS as usize);
/// Convert a consensus-timer interval to whole ticks, floored at one tick so a
/// sub-tick value still fires and saturated on overflow.
fn duration_to_ticks(interval: Duration) -> u64 {
    let ticks = interval.as_millis() / shard::CONSENSUS_TICK_INTERVAL.as_millis();
    u64::try_from(ticks.max(1)).unwrap_or(u64::MAX)
}

/// `[cluster] superblock_wedged_fatal_timeout` as a consecutive-failure count.
/// Retries pin at the backoff cap after warmup, so the window divided by
/// [`journal::superblock::SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS`] bounds how
/// long a wedged replica may limp before it fail-stops. Zero stays zero
/// (fail-stop disabled).
pub(crate) fn superblock_wedged_fatal_failures(config: &ServerConfig) -> u64 {
    superblock_window_to_failures(
        config
            .cluster
            .superblock_wedged_fatal_timeout
            .get_duration(),
    )
}

fn superblock_window_to_failures(window: Duration) -> u64 {
    if window.is_zero() {
        return 0;
    }
    let cap_micros = u128::from(journal::superblock::SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS);
    u64::try_from((window.as_micros() / cap_micros).max(1)).unwrap_or(u64::MAX)
}

/// `[cluster] heartbeat_timeout` in consensus ticks. Every consensus group
/// (metadata and per-partition planes alike) gets the same window: the failure
/// it guards against - a primary that stopped heartbeating - is host-level, not
/// per-plane.
pub(crate) fn cluster_heartbeat_ticks(config: &ServerConfig) -> u64 {
    duration_to_ticks(config.cluster.heartbeat_timeout.get_duration())
}

/// Floor for the post-restart read-recovery deadline (see
/// [`recovery_barrier_deadline`]). At and below the 5s default heartbeat the
/// worst-case recovery is dominated by the heartbeat-independent term - the
/// `ViewChangeStatus` backstop plus election ceremony and suffix recommit,
/// empirically ~7s - so the scaled value must never fall under this or a
/// fast-heartbeat cluster would 503 legitimate reads mid-recovery. The backstop
/// is the configurable `[cluster] view_change_status_timeout`; raising it past
/// its 5s default is why `recovery_barrier_deadline` scales that knob in too
/// rather than leaning on this floor to cover it.
const RECOVERY_BARRIER_DEADLINE_FLOOR: Duration = Duration::from_secs(15);

/// Safety factor applied to each scaled term of the recovery deadline: a slower
/// heartbeat stretches election and suffix recommit proportionally, and a wider
/// status backstop stretches the ceremony it bounds. 3x reproduces the
/// empirically chosen 15s margin at the shared 5s default (3 x 5s = 15s) and
/// holds that factor as either knob grows.
const RECOVERY_BARRIER_MULTIPLIER: u32 = 3;

/// How long the post-restart read path waits for the recovered WAL suffix to
/// re-commit before failing loud (retryable 503): the largest of the fixed
/// floor, a `[cluster] heartbeat_timeout`-scaled window, and a
/// `[cluster] view_change_status_timeout`-scaled window. Both knobs feed it
/// because either, raised far past its default, stretches worst-case recovery
/// past the fixed floor; see `await_recovery_barrier` for the read-side wait.
pub(crate) fn recovery_barrier_deadline(
    heartbeat: Duration,
    view_change_status: Duration,
) -> Duration {
    // saturating: neither timeout has a config ceiling, plain `*` panics
    heartbeat
        .saturating_mul(RECOVERY_BARRIER_MULTIPLIER)
        .max(view_change_status.saturating_mul(RECOVERY_BARRIER_MULTIPLIER))
        .max(RECOVERY_BARRIER_DEADLINE_FLOOR)
}

/// `[cluster] commit_broadcast_interval` in consensus ticks: how often the
/// primary broadcasts its commit point, the cluster's liveness feed. Applied
/// to every consensus group, matching `cluster_heartbeat_ticks`.
pub(crate) fn commit_broadcast_ticks(config: &ServerConfig) -> u64 {
    duration_to_ticks(config.cluster.commit_broadcast_interval.get_duration())
}

/// `[cluster] prepare_retransmit_interval` in consensus ticks: how often the
/// primary retransmits un-acked prepares. Applied to every consensus group,
/// matching `cluster_heartbeat_ticks`.
pub(crate) fn prepare_retransmit_ticks(config: &ServerConfig) -> u64 {
    duration_to_ticks(config.cluster.prepare_retransmit_interval.get_duration())
}

/// `[cluster] view_change_retransmit_interval` in consensus ticks: how often a
/// replica retransmits its `StartViewChange` / `DoViewChange` during a view
/// change. Applied to every consensus group, matching `cluster_heartbeat_ticks`.
pub(crate) fn view_change_retransmit_ticks(config: &ServerConfig) -> u64 {
    duration_to_ticks(
        config
            .cluster
            .view_change_retransmit_interval
            .get_duration(),
    )
}

/// `[cluster] view_change_status_timeout` in consensus ticks: the stalled
/// view-change backstop before escalating to a fresh election. Applied to every
/// consensus group, matching `cluster_heartbeat_ticks`.
pub(crate) fn view_change_status_ticks(config: &ServerConfig) -> u64 {
    duration_to_ticks(config.cluster.view_change_status_timeout.get_duration())
}

/// `[cluster] request_start_view_retransmit_interval` in consensus ticks: how
/// often a recovering or view-change backup re-requests the current `StartView`.
/// Applied to every consensus group, matching `cluster_heartbeat_ticks`.
pub(crate) fn request_start_view_ticks(config: &ServerConfig) -> u64 {
    duration_to_ticks(
        config
            .cluster
            .request_start_view_retransmit_interval
            .get_duration(),
    )
}

/// The full `[cluster]` timer set every consensus group boots with, built
/// once so the planes cannot diverge in what they apply.
pub(crate) fn consensus_timers(config: &ServerConfig) -> ConsensusTimers {
    ConsensusTimers {
        normal_heartbeat_ticks: cluster_heartbeat_ticks(config),
        commit_message_ticks: commit_broadcast_ticks(config),
        prepare_ticks: prepare_retransmit_ticks(config),
        view_change_retransmit_ticks: view_change_retransmit_ticks(config),
        view_change_status_ticks: view_change_status_ticks(config),
        request_start_view_ticks: request_start_view_ticks(config),
        probe_attempts_max: config.cluster.view_probe_attempts_max,
    }
}

/// `[cluster] repair_retry_interval` in consensus ticks: how long a stalled
/// journal-repair stream waits before re-requesting its window. Both planes'
/// repair loops share it, so it is applied once per shard (not per consensus
/// group). Clamped to `u32`, the width of the session idle-tick counter.
pub(crate) fn repair_retry_ticks(config: &ServerConfig) -> u32 {
    u32::try_from(duration_to_ticks(
        config.cluster.repair_retry_interval.get_duration(),
    ))
    .unwrap_or(u32::MAX)
}

/// Shard 0's half of a metadata recovery: everything [`recover`] produced except the
/// state machine, which every shard receives through the factory bundle.
///
/// Named rather than a positional tuple: the fields are same-typed `Option<u64>`s and
/// `(u64, u128)` pairs that a reorder would silently rebind, and one of them decides
/// what view the replica boots into.
struct RecoveredOwnerState {
    journal: PrepareJournal,
    snapshot: Option<IggySnapshot>,
    last_applied_op: Option<u64>,
    last_journaled_op: Option<u64>,
    client_table: ClientTable,
    superblock: PingPongSuperblock,
    recovered_state: Option<VsrState>,
    snapshot_checkpoint: (u64, u128),
}

/// Rebuild metadata consensus from what recovery read off this replica's own disk.
///
/// Takes the recovery result, topology and config whole rather than the dozen-plus
/// scalars it needs from them: most were `u64` tick counts, where a misordered
/// argument type-checks and mistunes a timeout silently.
fn restore_metadata_consensus(
    owner: &RecoveredOwnerState,
    topology: &TcpTopology,
    config: &ServerConfig,
    bus: Rc<IggyMessageBus>,
) -> VsrConsensus<Rc<IggyMessageBus>> {
    let journal = &owner.journal;
    let replica_count = topology.replica_count;
    let recovered_state = owner.recovered_state;
    let snapshot_floor = owner
        .snapshot
        .as_ref()
        .map_or(0, IggySnapshot::sequence_number);
    let commit_watermark = owner.last_applied_op.unwrap_or(snapshot_floor);
    let restored_op = owner.last_journaled_op.unwrap_or(snapshot_floor);
    let recovery_deadline = recovery_barrier_deadline(
        config.cluster.heartbeat_timeout.get_duration(),
        config.cluster.view_change_status_timeout.get_duration(),
    );
    let prepare_queue_depth = config.metadata.prepare_queue_depth;

    let last_header = journal
        .last_op()
        .and_then(|op| usize::try_from(op).ok())
        .and_then(|op| journal.header(op).map(|header| *header));
    // On a RESTART in a cluster, rejoin as a quorum-invisible backup and
    // probe for the current view (`RequestStartView`): the view's primary
    // answers with a `StartView`, the replica adopts it as a backup, and
    // journal repair fills any WAL gap. A probing replica never resumes
    // primaryship -- if this replica IS the current primary-by-index, its
    // probe makes the backups elect past it.
    // The probe re-broadcasts on its timeout, so it needs no live mesh at
    // boot. A FRESH boot keeps the plain init: the cluster needs its view-0
    // primary to exist, and a single-replica cluster has no peer to ask.
    //
    // Prior life is EITHER a non-empty WAL or a recovered superblock. A view
    // change persists without touching the WAL, so a replica that changed
    // view before its first metadata write comes back with a non-zero view
    // and an empty journal; gating on the WAL alone would `init()` it into
    // `Status::Normal` as primary for a view the cluster may have moved past,
    // with `ceded_primaryship` false and no probe to correct it.
    //
    // The rejoin also awaits a state transfer: snapshot-shaped metadata state
    // (snapshot + client table) is replaced from the live primary the probe
    // finds, then journal repair fills the tail. If the probe exhausts
    // instead -- full-cluster bootstrap, nobody live to fetch from -- the
    // election fallback clears the stage and this local recovery stands.
    let join = if replica_count > 1 && (restored_op > 0 || recovered_state.is_some()) {
        JoinMode::ProbeAsBackup {
            await_state_transfer: true,
        }
    } else {
        JoinMode::Init
    };
    let timers = consensus_timers(config);
    let consensus = VsrConsensus::restored(
        topology.cluster_id,
        topology.self_replica_id,
        replica_count,
        server_common::sharding::METADATA_GROUP,
        bus,
        // Request queue keeps the stock 2x ratio over the prepare queue
        // (32 -> 64 at defaults): buffered requests are cheap relative to
        // in-flight prepares and drain as prepares commit.
        LocalPipeline::with_capacities(prepare_queue_depth, prepare_queue_depth * 2),
        VsrRestore {
            timers: &timers,
            // View and log_view come from the durable superblock when present.
            // A present but unreadable superblock already refused boot in
            // `recover()`, so no durable record means genuinely absent: a
            // fresh node, or one that took writes but never checkpointed or
            // changed view. There, inferring the view from the last WAL
            // prepare is safe, since the persist-before-send gate guarantees
            // this replica never externalized a view beyond what a re-probe
            // re-derives, and it re-probes as a backup.
            durable_view: recovered_state.map(|state| (state.view, state.log_view)),
            view_fallback: last_header.map(|header| header.view),
            // Fresh random incarnation each boot, so a StartView addressed to
            // a previous incarnation still in flight is ignored
            // (`handle_start_view` guard). `| 1` guarantees the non-zero the
            // guard treats as set. The deterministic simulator overrides this
            // with a seed-derived value bumped per restart.
            incarnation: Some(rand::random::<u128>() | 1),
            join,
        },
    );
    consensus.sequencer().set_sequence(restored_op);
    // A SOLO replica's durable journal head IS its commit point: quorum is
    // 1-of-1, so an entry commits the instant it is durable, and the acks
    // the cluster ceremony below would wait on cannot topologically exist.
    // The embedded watermark is structurally one op stale (the commit point
    // is only ever written down inside the NEXT entry), so trusting it solo
    // manufactures an "uncommitted" suffix that provably committed and
    // wedges the recovery barrier forever.
    let commit_watermark = if replica_count == 1 {
        restored_op
    } else {
        commit_watermark
    };
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
        consensus.observe_prepare_timestamp(header.timestamp);
    }

    // The WAL's tail past the watermark is prepared-but-not-provably-committed
    // state. Until the cluster confirms it (re-pipelined below on a resumed
    // primary; via StartView adoption + the local commit walk on a rejoined
    // backup), serving reads would show pre-restart state that clients already
    // saw acked -- gate them on the barrier regardless of role. If the suffix
    // never re-commits cluster-wide, the read path fails loud with a retryable
    // 503 once the paired deadline expires (`await_recovery_barrier`).
    if commit_watermark < restored_op {
        consensus.set_recovery_barrier(restored_op);
        consensus.set_recovery_deadline(recovery_deadline);
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
        consensus.with_pipeline_mut(|pipeline| {
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
                entry.add_ack(topology.self_replica_id);
                pipeline.push(entry);
            }
        });
        // These went in through `Pipeline::push`, not `push_prepare_entry`, and `init`
        // no longer arms the timer: without this the recovered suffix sits in the
        // pipeline with nothing driving its retransmit.
        consensus.sync_prepare_timeout();
    }

    consensus
}

/// Recover this partition's persisted segment chain, stamping each segment
/// with the topic's effective segment size (the per-topic value when the
/// topic was created with one, else the shard-wide configured size).
async fn recover_partition_segments(
    config: &ServerConfig,
    namespace: IggyNamespace,
    runtime_options: TopicRuntimeOptions,
    stats: &PartitionStats,
) -> Result<Vec<RecoveredSegment>, ServerError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let segment_size = runtime_options
        .segment_size
        .unwrap_or_else(|| IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE));
    load_persisted_segments(
        config,
        stream_id,
        topic_id,
        partition_id,
        segment_size,
        stats,
    )
    .await
    .map_err(|source| {
        error!(
            stream_id,
            topic_id,
            partition_id,
            error = %source,
            "failed to load partition log during server bootstrap"
        );
        source
    })
}

#[allow(clippy::too_many_arguments)]
async fn load_partition(
    config: &ServerConfig,
    namespace: IggyNamespace,
    stats: Arc<PartitionStats>,
    partition_metadata: &Partition,
    runtime_options: TopicRuntimeOptions,
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    // (view, log_view) come from the group's durable superblock when present;
    // a present but unverifiable record already refused boot inside
    // `open_partition_superblock`.
    let partition_dir = config
        .system
        .get_partition_path(stream_id, topic_id, partition_id);
    let (superblock, recovered_state) = open_partition_superblock(
        &partition_dir,
        ReplicaIdentity {
            cluster: cluster_id,
            replica_id: self_replica_id,
            replica_count,
        },
    )
    .await?;

    // A recovered partition lost its journal state with the process: the
    // partition journal is in-memory and segments carry no op numbers, so
    // this replica cannot know the group's (op, commit) even when the
    // superblock restored its view. In a cluster it boots as a
    // quorum-invisible backup and probes for the current view
    // (`RequestStartView`): the view's primary answers with a `StartView`,
    // journal repair fills the rejoin window, and the commit floor settles
    // at the serving peer's retention point. The probe re-broadcasts on its
    // timeout, so it needs no live mesh at boot. Single-replica groups
    // have no peer to ask and keep the plain init.
    let join = if replica_count > 1 {
        JoinMode::ProbeAsBackup {
            await_state_transfer: false,
        }
    } else {
        JoinMode::Init
    };
    // Request queue holds 2x the prepare depth (buffered requests drain as
    // prepares commit); depth is the per-partition `[partition]` knob.
    let prepare_queue_depth = config.partition.prepare_queue_depth;
    let timers = consensus_timers(config);
    let consensus = VsrConsensus::restored(
        cluster_id,
        self_replica_id,
        replica_count,
        namespace.inner(),
        bus,
        LocalPipeline::with_capacities(prepare_queue_depth, prepare_queue_depth * 2),
        VsrRestore {
            timers: &timers,
            durable_view: recovered_state
                .as_ref()
                .map(|state| (state.view, state.log_view)),
            view_fallback: None,
            incarnation: None,
            join,
        },
    );

    // No prepare-timestamp floor is restored here: the partition consensus
    // journal is non-durable today, so there is no persisted head to observe
    // (unlike `restore_metadata_consensus`, which observes its restored head).
    // When PartitionJournal becomes durable (the milestone named in the
    // multi-shard wiring commit body), observe the restored head and the max
    // recovered message timestamp here, or an NTP rewind across a restart could
    // regress persisted `base_timestamp`.

    let recovered_segments =
        recover_partition_segments(config, namespace, runtime_options, &stats).await?;

    let mut partition = IggyPartition::new(stats.clone(), consensus);
    partition.set_runtime_options(runtime_options);
    partition.set_superblock(superblock, recovered_state.as_ref());
    // Recovered partitions honor the same config-surfaced ring ceilings as the
    // fresh-create path (build_partition_fresh). Retention is already off for
    // single-replica groups, so this only sizes the multi-replica ring.
    partition.log.journal().inner.set_ring_caps(
        config.partition.evicted_ring_capacity,
        config.partition.evicted_ring_bytes_max.as_bytes_u64(),
    );
    partition.set_partition_dir(partition_dir);
    // Before the hydrate: the durable record is keyed by incarnation, so a
    // `purge.gen` left behind by a previous life of this namespace reads 0.
    partition.set_created_revision(partition_metadata.created_revision);
    partition.hydrate_applied_purge_generation().await?;
    hydrate_partition_log(
        &mut partition,
        stream_id,
        topic_id,
        partition_id,
        recovered_segments,
    )
    .await?;

    let sized_end = partition
        .log
        .segments()
        .iter()
        .filter(|segment| segment.size > IggyByteSize::default())
        .map(|segment| segment.end_offset)
        .max();
    // An empty chain whose segment is named for a nonzero offset is the
    // shape a state-transfer install (or its converge) plants at the group
    // frontier after the origin GC'd everything: the file name carries the
    // frontier, and re-minting offsets from 0 here would fork this
    // replica's batch stamps from the rest of the group after a restart.
    let empty_frontier = partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.start_offset)
        .max()
        .filter(|&start| sized_end.is_none() && start > 0);
    let current_offset = sized_end.or_else(|| empty_frontier.map(|start| start - 1));
    partition.created_at = partition_metadata.created_at;
    partition.recovered_durable_offset = sized_end;
    // The OFFSET COUNTER is restored from that file name (above), but the
    // `installed_frontier` CLAIM deliberately is not: the claim says "everything
    // below me is represented here", and `converge_to_empty_after_failed_install`
    // refuses to make it when staged segments were dropped -- yet a converge
    // plants exactly the same empty `{frontier:020}.log` a legitimate empty
    // install does, so boot provably cannot tell them apart. Re-deriving it here
    // would hand the refused claim back: the repair floor stand-in would accept a
    // commit floor over ops this replica holds zero bytes for, and the replica
    // would pass the serve gate and offer that emptiness onward, making a peer
    // unlink its own chain. Leaving it `None` costs one spurious full
    // re-transfer on the legitimate empty-install restart; a false caught-up
    // claim is not recoverable. A durable home for the frontier (the partition
    // superblock already reserves a field) is what would settle it properly.
    let counter = current_offset.unwrap_or(0);
    partition.offset.store(counter, Ordering::Release);
    partition.dirty_offset.store(counter, Ordering::Relaxed);
    partition.should_increment_offset = current_offset.is_some();
    // The durable frontier is a LOWER BOUND on top of what the segments proved:
    // it is the only carrier left when the segments that named the frontier are
    // gone (an all-GC'd origin's install, a crash inside the swap window), and
    // taking the max means real recovered data always wins.
    partition.restore_offset_frontier(recovered_state.as_ref());
    let current_offset = partition.offset.load(Ordering::Acquire);

    configure_consumer_offsets(&mut partition, config, namespace, current_offset)?;
    ensure_initial_segment(&mut partition, config, stream_id, topic_id, partition_id).await?;

    Ok(partition)
}

/// Reopen writers over a recovered segment chain.
///
/// Takes no `&ServerConfig`: every knob it needs is the partition's own
/// resolved topic option now, which is the whole point of the per-topic move.
async fn hydrate_partition_log(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    recovered_segments: Vec<RecoveredSegment>,
) -> Result<(), ServerError> {
    // The partition's own resolved knobs, not the shard-wide config: a topic
    // created with `enforce_fsync` or a per-topic `segment_size` must get them
    // on the writers reopened over its recovered chain too, or a restart would
    // silently drop back to the node defaults.
    let runtime = partition.runtime_options();
    let enforce_fsync = runtime
        .enforce_fsync
        .unwrap_or(iggy_common::DEFAULT_ENFORCE_FSYNC);
    let segment_size = runtime
        .segment_size
        .unwrap_or_else(|| IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE));
    let preallocate_segments = runtime
        .preallocate_segments
        .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS);
    for RecoveredSegment { segment, storage } in recovered_segments {
        partition
            .log
            .add_persisted_segment(segment, storage, None, None);
    }

    if let Some(active_index) = partition.log.segments().len().checked_sub(1) {
        let storage = &partition.log.storages()[active_index];
        if let (
            Some(messages_reader),
            Some(index_reader),
            Some(storage_messages_writer),
            Some(storage_index_writer),
        ) = (
            storage.messages_reader.as_ref(),
            storage.index_reader.as_ref(),
            storage.messages_writer.as_ref(),
            storage.index_writer.as_ref(),
        ) {
            let index_path = index_reader.path();
            // Share the storage's size counters: the readers bound reads by
            // these atomics, so a writer with a private counter persists bytes
            // the readers never learn about.
            let messages_size_counter = storage_messages_writer.size_counter();
            let index_size_counter = storage_index_writer.size_counter();
            partition.log.messages_writers_mut()[active_index] = Some(Rc::new(
                MessagesWriter::new(
                    &messages_reader.path(),
                    messages_size_counter,
                    enforce_fsync,
                    true,
                    preallocate_segments.then_some(segment_size),
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
                IggyIndexWriter::new(&index_path, index_size_counter, enforce_fsync, true)
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
    config: &ServerConfig,
    current_replica_id: Option<u8>,
) -> Result<TcpTopology, ServerError> {
    let default_client_addr = parse_socket_addr("tcp.address", &config.tcp.address)?;
    let default_ws_addr = resolve_optional_listener_addr(
        config.websocket.enabled,
        "websocket.address",
        &config.websocket.address,
    )?;
    let default_quic_addr =
        resolve_optional_listener_addr(config.quic.enabled, "quic.address", &config.quic.address)?;
    let default_http_addr =
        resolve_optional_listener_addr(config.http.enabled, "http.address", &config.http.address)?;
    if !config.cluster.enabled {
        if let Some(replica_id) = current_replica_id
            && replica_id != SHARD_REPLICA_ID
        {
            return Err(ServerError::ReplicaIdRequiresCluster {
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
            http_listen_addr: default_http_addr,
            tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(default_client_addr),
            peers: Vec::new(),
        });
    }

    let self_replica_id = current_replica_id.ok_or(ServerError::MissingReplicaId)?;

    let self_node = config
        .cluster
        .nodes
        .iter()
        .find(|node| node.replica_id == self_replica_id)
        .ok_or(ServerError::ClusterNodeNotFound {
            replica_id: self_replica_id,
        })?;
    let replica_count = u8::try_from(config.cluster.nodes.len()).map_err(|_| {
        ServerError::ClusterReplicaCountTooLarge {
            count: config.cluster.nodes.len(),
        }
    })?;
    let ClusterClientAddrs {
        client: client_listen_addr,
        ws: ws_listen_addr,
        quic: quic_listen_addr,
        http: http_listen_addr,
    } = resolve_cluster_client_addrs(
        self_node,
        default_client_addr,
        default_ws_addr,
        default_quic_addr,
        default_http_addr,
    )?;
    let replica_port = self_node
        .ports
        .tcp_replica
        .ok_or(ServerError::ClusterPortMissing {
            transport: "tcp_replica",
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
        http_listen_addr,
        tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(client_listen_addr),
        peers,
    })
}

fn resolve_optional_listener_addr(
    enabled: bool,
    context: &'static str,
    address: &str,
) -> Result<Option<SocketAddr>, ServerError> {
    if enabled {
        return Ok(Some(parse_socket_addr(context, address)?));
    }
    Ok(None)
}

/// Client-facing listener addresses resolved for this cluster node. Each port
/// comes from the node's roster entry; there is no fallback to the top-level
/// listener port, an enabled transport without a roster port refuses to boot.
/// Every transport keeps the bind interface from its own `address` config: the
/// roster ip is advertised, not bound.
struct ClusterClientAddrs {
    client: SocketAddr,
    ws: Option<SocketAddr>,
    quic: Option<SocketAddr>,
    http: Option<SocketAddr>,
}

fn resolve_cluster_client_addrs(
    self_node: &configs::cluster::ClusterNodeConfig,
    default_tcp_addr: SocketAddr,
    default_ws_addr: Option<SocketAddr>,
    default_quic_addr: Option<SocketAddr>,
    default_http_addr: Option<SocketAddr>,
) -> Result<ClusterClientAddrs, ServerError> {
    let client_port = self_node.ports.tcp.ok_or(ServerError::ClusterPortMissing {
        transport: "tcp",
        replica_id: self_node.replica_id,
    })?;
    let client =
        merge_roster_port_with_bind_ip("tcp", &self_node.ip, default_tcp_addr, client_port);
    let ws = resolve_cluster_optional_addr(self_node, "websocket", default_ws_addr, |ports| {
        ports.websocket
    })?;
    let quic =
        resolve_cluster_optional_addr(self_node, "quic", default_quic_addr, |ports| ports.quic)?;
    let http =
        resolve_cluster_optional_addr(self_node, "http", default_http_addr, |ports| ports.http)?;
    Ok(ClusterClientAddrs {
        client,
        ws,
        quic,
        http,
    })
}

fn resolve_cluster_optional_addr(
    self_node: &configs::cluster::ClusterNodeConfig,
    transport: &'static str,
    default_addr: Option<SocketAddr>,
    port_selector: impl Fn(&configs::cluster::TransportPorts) -> Option<u16>,
) -> Result<Option<SocketAddr>, ServerError> {
    let Some(default_addr) = default_addr else {
        return Ok(None);
    };
    // No fallback to the top-level port: two same-host nodes leaving the same
    // transport port unset would race for one socket. Either the roster is
    // explicit or the server refuses to boot.
    let port = port_selector(&self_node.ports).ok_or(ServerError::ClusterPortMissing {
        transport,
        replica_id: self_node.replica_id,
    })?;
    Ok(Some(merge_roster_port_with_bind_ip(
        transport,
        &self_node.ip,
        default_addr,
        port,
    )))
}

/// Combine the roster-supplied `port` with the bind interface the transport's
/// own `address` config asked for.
///
/// The roster ip is what the cluster advertises (metadata, follower-to-primary
/// HTTP forwarding targets); the transport's own `address` decides the bind
/// interface. Merging keeps a loopback-only `127.0.0.1` private and a
/// `0.0.0.0` wide in cluster mode instead of silently rebinding to the roster
/// interface, which would strand every co-located dialer (sidecars, health
/// probes, on-host consumers) on `ECONNREFUSED`.
fn merge_roster_port_with_bind_ip(
    transport: &'static str,
    roster_ip: &str,
    bind_addr: SocketAddr,
    port: u16,
) -> SocketAddr {
    let listen_addr = SocketAddr::new(bind_addr.ip(), port);
    if roster_ip_unreachable_from_bind_addr(roster_ip, listen_addr) {
        warn!(
            "{transport} listener binds {listen_addr} but the roster advertises {roster_ip}:{port}; \
             peers and clients dialing the advertised endpoint may not reach this node"
        );
    }
    listen_addr
}

/// Whether a dialer aiming at the advertised roster ip misses `listen_addr`. An
/// unspecified bind covers every interface, and a roster ip that parses as
/// neither IPv4 nor IPv6 (a DNS name, say) can resolve to the bound interface,
/// so both cases stay quiet.
fn roster_ip_unreachable_from_bind_addr(roster_ip: &str, listen_addr: SocketAddr) -> bool {
    !listen_addr.ip().is_unspecified()
        && roster_ip
            .parse::<IpAddr>()
            .is_ok_and(|parsed| parsed != listen_addr.ip())
}

fn resolve_cluster_replica_peers(
    nodes: &[configs::cluster::ClusterNodeConfig],
    self_replica_id: u8,
) -> Result<Vec<(u8, SocketAddr)>, ServerError> {
    let mut peers = Vec::with_capacity(nodes.len().saturating_sub(1));
    for node in nodes {
        if node.replica_id == self_replica_id {
            continue;
        }
        let replica_port = node
            .ports
            .tcp_replica
            .ok_or(ServerError::ClusterPortMissing {
                transport: "tcp_replica",
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
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerError> {
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
    if let Some(http_addr) = topology.http_listen_addr {
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
            config.metadata.clients_table_max,
            config.personal_access_token.max_tokens_per_user,
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
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerError> {
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
            "failed to start server listeners via replica_io"
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
            "server listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = %bound.client,
            tcp_tls = ?bound.tcp_tls,
            ws = ?bound.ws,
            quic = ?bound.quic,
            "server client listeners started"
        );
    }

    Ok(())
}

async fn start_manual_runtime(
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
    dialed_replica: DialedReplicaFn,
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerError> {
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
            "server listeners started"
        );
    } else {
        info!(
            shard = shard.id,
            tcp = ?bound_clients.tcp,
            tcp_tls = ?bound_clients.tcp_tls,
            ws = ?bound_clients.ws,
            quic = ?bound_clients.quic,
            "server client listeners started"
        );
    }

    Ok(())
}

fn ensure_default_root_user(mux_stm: &ServerMuxStateMachine) {
    if !mux_stm.users().read(|users| users.items.is_empty()) {
        return;
    }

    let (username, password_hash) = create_root_credentials();
    mux_stm.users().ensure_root_user(&username, &password_hash);
}

/// Apply `--with-default-root-credentials`.
///
/// Fills in whichever of [`IGGY_ROOT_USERNAME_ENV`] /
/// [`IGGY_ROOT_PASSWORD_ENV`] the operator did not export, so the flag is
/// exactly the sugar for setting both by hand and the environment keeps
/// winning over it.
///
/// # Safety
///
/// Mutates the process environment, so the caller must still be
/// single-threaded.
pub unsafe fn apply_default_root_credentials(enabled: bool) {
    if !enabled {
        return;
    }

    let username_set = env::var(IGGY_ROOT_USERNAME_ENV).is_ok();
    let password_set = env::var(IGGY_ROOT_PASSWORD_ENV).is_ok();
    if username_set && password_set {
        warn!(
            "--with-default-root-credentials ignored: {IGGY_ROOT_USERNAME_ENV} and \
             {IGGY_ROOT_PASSWORD_ENV} are already set"
        );
        return;
    }

    // SAFETY: single-threaded caller, per this function's contract.
    unsafe {
        if !username_set {
            env::set_var(IGGY_ROOT_USERNAME_ENV, DEFAULT_ROOT_USERNAME);
        }
        if !password_set {
            env::set_var(IGGY_ROOT_PASSWORD_ENV, DEFAULT_ROOT_PASSWORD);
        }
    }
    warn!(
        "--with-default-root-credentials: a newly created root user will use the \
         well-known development credentials; INSECURE outside development"
    );
}

/// Resolve the root user credentials from `IGGY_ROOT_USERNAME` /
/// `IGGY_ROOT_PASSWORD`, falling back to the default username with a
/// generated password.
///
/// Returns `(username, password_hash)`; the plaintext password never
/// leaves this function.
fn create_root_credentials() -> (String, String) {
    if let Some((username, password)) = root_credentials_from_env() {
        info!("Using the custom root user credentials.");
        return (username, crypto::hash_password(&password));
    }

    info!("Using the default root user credentials...");
    let password = crypto::generate_secret(20..40);
    // Through tracing, not stdout: this is the only time the operator can read
    // the password, so it has to reach the log file too.
    warn!("Generated root user password: {password}");
    (
        DEFAULT_ROOT_USERNAME.to_string(),
        crypto::hash_password(&password),
    )
}

/// The credentials the operator supplied, `None` when neither variable is
/// set. A half-set pair never reaches here: [`validate_root_credentials`]
/// rejects it at boot.
fn root_credentials_from_env() -> Option<(String, String)> {
    match (
        env::var(IGGY_ROOT_USERNAME_ENV),
        env::var(IGGY_ROOT_PASSWORD_ENV),
    ) {
        (Ok(username), Ok(password)) => Some((username, password)),
        _ => None,
    }
}

/// Reject root-credential misconfiguration before any shard thread exists.
///
/// Shard 0 seeds the root user from inside `recover`'s baseline closure,
/// which cannot fail, so every operator-facing check has to run here or it
/// would have to panic a shard thread instead.
fn validate_root_credentials_env(config: &ServerConfig) -> Result<(), ServerError> {
    // `recover` creates the metadata directory, so its absence is what tells a
    // first cluster boot (root must come out identical on every replica, hence
    // explicit credentials) apart from a restart that recovers the root user it
    // already stored. `--fresh` has already wiped by this point, so a wiped
    // replica is correctly treated as a first boot.
    let fresh_cluster = config.cluster.enabled
        && !Path::new(&config.system.path)
            .join(metadata::impls::METADATA_DIR)
            .exists();

    validate_root_credentials(
        fresh_cluster,
        env::var(IGGY_ROOT_USERNAME_ENV).ok().as_deref(),
        env::var(IGGY_ROOT_PASSWORD_ENV).ok().as_deref(),
    )
}

fn validate_root_credentials(
    explicit_required: bool,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), ServerError> {
    match (username, password) {
        (Some(username), Some(password)) => {
            validate_credential_length(
                IGGY_ROOT_USERNAME_ENV,
                username,
                MIN_USERNAME_LENGTH,
                MAX_USERNAME_LENGTH,
            )?;
            validate_credential_length(
                IGGY_ROOT_PASSWORD_ENV,
                password,
                MIN_PASSWORD_LENGTH,
                MAX_PASSWORD_LENGTH,
            )
        }
        (Some(_), None) => Err(ServerError::RootCredentialsIncomplete {
            provided_env: IGGY_ROOT_USERNAME_ENV,
            missing_env: IGGY_ROOT_PASSWORD_ENV,
        }),
        (None, Some(_)) => Err(ServerError::RootCredentialsIncomplete {
            provided_env: IGGY_ROOT_PASSWORD_ENV,
            missing_env: IGGY_ROOT_USERNAME_ENV,
        }),
        (None, None) if explicit_required => Err(ServerError::ClusterRootCredentialsRequired {
            username_env: IGGY_ROOT_USERNAME_ENV,
            password_env: IGGY_ROOT_PASSWORD_ENV,
        }),
        (None, None) => Ok(()),
    }
}

fn validate_credential_length(
    env_name: &'static str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), ServerError> {
    if (min..=max).contains(&value.len()) {
        Ok(())
    } else {
        Err(ServerError::RootCredentialLength {
            env_name,
            length: value.len(),
            min,
            max,
        })
    }
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
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    topology: &TcpTopology,
    accepted_clients: &LocalClientAcceptFns,
) -> Result<BoundClientListeners, ServerError> {
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
/// Only the derived MAC keys are carried onward in [`ReplicaAuth`]; the raw
/// secrets (masked in config logs via `config_env(secret)`) are read here only
/// to derive them. A non-empty `previous_shared_secret` opens the verify-only
/// rotation acceptance window (see the [`ReplicaAuth`] rustdoc for the rolling
/// rotation procedure). `ClusterConfig::validate` guarantees a non-empty
/// secret whenever both `cluster.enabled` and `cluster.auth.enabled` are set
/// (validate early-returns `Ok` while `cluster.enabled` is false).
fn load_replica_auth(config: &ServerConfig) -> Option<ReplicaAuth> {
    if !config.cluster.enabled || !config.cluster.auth.enabled {
        return None;
    }
    let auth = ReplicaAuth::new(config.cluster.auth.shared_secret.as_bytes());
    let previous_shared_secret = &config.cluster.auth.previous_shared_secret;
    if previous_shared_secret.is_empty() {
        return Some(auth);
    }
    Some(auth.with_previous_secret(previous_shared_secret.as_bytes()))
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
    config: &ServerConfig,
    topology: &TcpTopology,
) -> Result<Option<ReplicaTlsCtx>, ServerError> {
    let tls = &config.cluster.tls;
    if !config.cluster.enabled || !tls.enabled {
        return Ok(None);
    }
    install_default_crypto_provider();
    let credential_error = |source: std::io::Error| ServerError::ListenerCredentials {
        transport: "cluster.tls",
        source,
    };

    let credentials = if tls.self_signed {
        warn_ignored_certificate_files("cluster.tls", &tls.cert_file, &tls.key_file);
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

    // Keyed by replica id, never by roster position: sparse ids (dynamic
    // replica join) would make a positional lookup verify against another
    // peer's SNI name.
    let peer_names = config
        .cluster
        .nodes
        .iter()
        .map(|node| {
            let name = ServerName::try_from(node.ip.clone()).map_err(|error| {
                credential_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "cluster node '{}' ip '{}' is not a valid TLS server name: {error}",
                        node.name, node.ip
                    ),
                ))
            })?;
            Ok((node.replica_id, name))
        })
        .collect::<Result<HashMap<_, _>, ServerError>>()?;

    Ok(Some(ReplicaTlsCtx {
        server: Arc::new(server),
        client: Arc::new(client),
        peer_names,
    }))
}

fn load_tcp_tls_server_credentials(
    config: &ServerConfig,
) -> Result<TlsServerCredentials, ServerError> {
    let tls = &config.tcp.tls;
    if ephemeral_certificate("tcp.tls", tls.self_signed, &tls.cert_file) {
        return Ok(self_signed_for_loopback());
    }

    load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
        ServerError::ListenerCredentials {
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
    shard: &Rc<ServerShard>,
    config: &ServerConfig,
    ws_addr: SocketAddr,
    accepted_clients: &LocalClientAcceptFns,
) -> Result<SocketAddr, ServerError> {
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

fn load_wss_server_credentials(config: &ServerConfig) -> Result<TlsServerCredentials, ServerError> {
    let tls = &config.websocket.tls;
    if ephemeral_certificate("websocket.tls", tls.self_signed, &tls.cert_file) {
        return Ok(self_signed_for_loopback());
    }

    load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
        ServerError::ListenerCredentials {
            transport: "websocket.tls",
            source,
        }
    })
}

fn load_quic_server_credentials(
    config: &ServerConfig,
) -> Result<replica_io::QuicServerCredentials, ServerError> {
    let certificate = &config.quic.certificate;
    if certificate.self_signed {
        warn_ignored_certificate_files(
            "quic.certificate",
            &certificate.cert_file,
            &certificate.key_file,
        );
        let (cert_chain, key_der) = server_common::generate_self_signed_certificate("localhost")
            .map_err(|error| ServerError::ListenerCredentials {
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
    .map_err(|source| ServerError::ListenerCredentials {
        transport: "quic",
        source,
    })?;
    Ok(replica_io::QuicServerCredentials {
        cert_chain: credentials.cert_chain,
        key_der: credentials.key_der,
    })
}

/// Client-listener certificate precedence: `self_signed = true` mints an
/// ephemeral loopback certificate only while `cert_file` is absent from disk.
/// An existing PEM pair wins, so a deployment that lays certificates down
/// serves them without also having to unset the flag - the contract every
/// SDK test lane relies on when it points the server at `core/certs/`.
fn ephemeral_certificate(section: &str, self_signed: bool, cert_file: &str) -> bool {
    if !self_signed {
        return false;
    }
    if Path::new(cert_file).exists() {
        info!(
            "{section}.self_signed = true but cert_file = {cert_file} exists on disk; loading it - remove the file or clear the path to serve an ephemeral certificate"
        );
        return false;
    }
    true
}

/// `self_signed = true` never reads the PEM pair (cluster and QUIC keep the
/// flag authoritative: their generated certificates carry non-loopback SANs),
/// so a cert path resolving on disk looks active to an operator who never
/// asked for it.
fn warn_ignored_certificate_files(section: &str, cert_file: &str, key_file: &str) {
    let found: Vec<String> = [("cert_file", cert_file), ("key_file", key_file)]
        .into_iter()
        .filter(|(_, path)| Path::new(path).exists())
        .map(|(field, path)| format!("{field} = {path}"))
        .collect();
    if found.is_empty() {
        return;
    }

    warn!(
        "{section}.self_signed = true, ignoring certificate files found on disk ({}); set {section}.self_signed = false to load them",
        found.join(", ")
    );
}

fn parse_socket_addr(context: &'static str, address: &str) -> Result<SocketAddr, ServerError> {
    address
        .parse()
        .map_err(|source| ServerError::SocketAddressParse {
            context,
            address: address.to_string(),
            source,
        })
}

fn socket_addr_from_parts(
    context: &'static str,
    host: &str,
    port: u16,
) -> Result<SocketAddr, ServerError> {
    let ip = host
        .parse::<IpAddr>()
        .map_err(|source| ServerError::SocketAddressParse {
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
    fn superblock_fatal_window_converts_to_capped_backoff_retries() {
        assert_eq!(
            superblock_window_to_failures(Duration::ZERO),
            0,
            "zero window must stay the disabled sentinel"
        );
        assert_eq!(
            superblock_window_to_failures(Duration::from_mins(2)),
            120,
            "past warmup one retry rides each 1s backoff cap"
        );
        assert_eq!(
            superblock_window_to_failures(Duration::from_micros(500)),
            1,
            "a sub-cap window still needs one failure to fire"
        );
    }

    #[test]
    fn fresh_cluster_bootstrap_requires_explicit_root_credentials() {
        assert!(matches!(
            validate_root_credentials(true, None, None),
            Err(ServerError::ClusterRootCredentialsRequired {
                username_env: IGGY_ROOT_USERNAME_ENV,
                password_env: IGGY_ROOT_PASSWORD_ENV,
            })
        ));
        validate_root_credentials(true, Some("root"), Some("secret"))
            .expect("both credentials supplied must satisfy the fresh-cluster guard");
    }

    #[test]
    fn single_node_bootstrap_generates_root_credentials_when_unset() {
        validate_root_credentials(false, None, None)
            .expect("a single node mints its own root password");
    }

    #[test]
    fn half_set_root_credentials_are_rejected_in_both_directions() {
        assert!(matches!(
            validate_root_credentials(false, Some("root"), None),
            Err(ServerError::RootCredentialsIncomplete {
                provided_env: IGGY_ROOT_USERNAME_ENV,
                missing_env: IGGY_ROOT_PASSWORD_ENV,
            })
        ));
        assert!(matches!(
            validate_root_credentials(false, None, Some("secret")),
            Err(ServerError::RootCredentialsIncomplete {
                provided_env: IGGY_ROOT_PASSWORD_ENV,
                missing_env: IGGY_ROOT_USERNAME_ENV,
            })
        ));
    }

    #[test]
    fn out_of_range_root_credentials_are_rejected() {
        assert!(matches!(
            validate_root_credentials(false, Some(""), Some("secret")),
            Err(ServerError::RootCredentialLength {
                env_name: IGGY_ROOT_USERNAME_ENV,
                length: 0,
                ..
            })
        ));
        let too_long = "x".repeat(MAX_PASSWORD_LENGTH + 1);
        assert!(matches!(
            validate_root_credentials(false, Some("root"), Some(&too_long)),
            Err(ServerError::RootCredentialLength {
                env_name: IGGY_ROOT_PASSWORD_ENV,
                ..
            })
        ));
    }

    #[test]
    fn default_cluster_heartbeat_timeout_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string,
        // so no static assert can pin it); keep it in lockstep with the
        // built-in the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .heartbeat_timeout
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::NORMAL_HEARTBEAT_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] heartbeat_timeout default drifted from \
             TimeoutManager::NORMAL_HEARTBEAT_TICKS"
        );
    }

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

    #[test]
    fn recovery_barrier_deadline_holds_the_floor_for_small_heartbeats() {
        // Below the 5s default the heartbeat-independent recovery term (~7s of
        // ViewChangeStatus backstop plus ceremony) dominates, so the floor
        // governs however small the heartbeat is; 3 x 5s lands exactly on it.
        // A default-sized status backstop stays on the floor, not above it.
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(1), Duration::from_secs(5)),
            RECOVERY_BARRIER_DEADLINE_FLOOR
        );
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(5), Duration::from_secs(5)),
            RECOVERY_BARRIER_DEADLINE_FLOOR
        );
    }

    #[test]
    fn recovery_barrier_deadline_scales_past_the_floor_for_large_heartbeats() {
        // Once 3 x heartbeat clears the floor the scaled window governs, so a
        // slow-heartbeat cluster is not failed 503 before its longer recovery
        // can finish. A default-sized status backstop stays under it.
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(10), Duration::from_secs(5)),
            Duration::from_secs(30)
        );
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(15), Duration::from_secs(5)),
            Duration::from_secs(45)
        );
    }

    #[test]
    fn recovery_barrier_deadline_scales_with_the_status_backstop() {
        // A raised view-change status backstop stretches worst-case recovery
        // even when the heartbeat stays fast, so the deadline must track it or
        // post-restart reads 503 before a slow election settles.
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(1), Duration::from_secs(10)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn recovery_barrier_deadline_at_config_defaults_matches_the_floor() {
        // Folding the status term in must not move the stock deadline: at the
        // shared 5s defaults each scaled term lands exactly on the 15s floor,
        // so an un-tuned cluster keeps its pre-existing recovery window.
        let cluster = configs::cluster::ClusterConfig::default();
        assert_eq!(
            recovery_barrier_deadline(
                cluster.heartbeat_timeout.get_duration(),
                cluster.view_change_status_timeout.get_duration(),
            ),
            RECOVERY_BARRIER_DEADLINE_FLOOR
        );
    }

    #[test]
    fn recovery_barrier_deadline_saturates_instead_of_panicking() {
        // Neither timeout has a config ceiling, so both multiplies must
        // saturate rather than abort boot on an absurd parseable value.
        assert_eq!(
            recovery_barrier_deadline(Duration::MAX, Duration::from_secs(5)),
            Duration::MAX
        );
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(5), Duration::MAX),
            Duration::MAX
        );
    }

    #[test]
    fn default_commit_broadcast_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string,
        // so no static assert can pin it); keep it in lockstep with the
        // built-in the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .commit_broadcast_interval
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::COMMIT_MESSAGE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] commit_broadcast_interval default drifted from \
             TimeoutManager::COMMIT_MESSAGE_TICKS"
        );
    }

    #[test]
    fn default_prepare_retransmit_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string,
        // so no static assert can pin it); keep it in lockstep with the
        // built-in the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .prepare_retransmit_interval
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::PREPARE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] prepare_retransmit_interval default drifted from \
             TimeoutManager::PREPARE_TICKS"
        );
    }

    #[test]
    fn default_partition_prepare_queue_depth_matches_consensus_constant() {
        // The config default lives in core/server/config.toml and flows
        // through PartitionConfig::default(); keep the embedded value in
        // lockstep with the pipeline depth LocalPipeline::new() (the simulator
        // and tests) runs on, so a default deployment is byte-identical.
        let config_default = configs::partition::PartitionConfig::default().prepare_queue_depth;
        assert_eq!(
            config_default,
            consensus::PIPELINE_PREPARE_QUEUE_MAX,
            "[partition] prepare_queue_depth default drifted from \
             consensus::PIPELINE_PREPARE_QUEUE_MAX"
        );
    }

    #[test]
    fn default_view_change_retransmit_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it). One knob drives both view-change
        // retransmit timers, which are equal by design, so pin it against both.
        let config_default = configs::cluster::ClusterConfig::default()
            .view_change_retransmit_interval
            .get_duration()
            .as_millis();
        let start_view_change =
            u128::from(consensus::TimeoutManager::START_VIEW_CHANGE_MESSAGE_TICKS)
                * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        let do_view_change = u128::from(consensus::TimeoutManager::DO_VIEW_CHANGE_MESSAGE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, start_view_change,
            "[cluster] view_change_retransmit_interval default drifted from \
             TimeoutManager::START_VIEW_CHANGE_MESSAGE_TICKS"
        );
        assert_eq!(
            config_default, do_view_change,
            "[cluster] view_change_retransmit_interval default drifted from \
             TimeoutManager::DO_VIEW_CHANGE_MESSAGE_TICKS"
        );
    }

    #[test]
    fn default_view_change_status_timeout_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it); keep it in lockstep with the built-in
        // the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .view_change_status_timeout
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::VIEW_CHANGE_STATUS_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] view_change_status_timeout default drifted from \
             TimeoutManager::VIEW_CHANGE_STATUS_TICKS"
        );
    }

    #[test]
    fn default_request_start_view_retransmit_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it); keep it in lockstep with the built-in
        // the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .request_start_view_retransmit_interval
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::REQUEST_START_VIEW_MESSAGE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] request_start_view_retransmit_interval default drifted from \
             TimeoutManager::REQUEST_START_VIEW_MESSAGE_TICKS"
        );
    }

    #[test]
    fn default_view_probe_attempts_max_matches_consensus_constant() {
        // Belt and suspenders with the static assert above: that pins the
        // duplicated configs-crate literal, this pins the shipped config.toml
        // value the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default().view_probe_attempts_max;
        assert_eq!(
            config_default,
            consensus::PROBE_ATTEMPTS_MAX,
            "[cluster] view_probe_attempts_max default drifted from \
             consensus::PROBE_ATTEMPTS_MAX"
        );
    }

    #[test]
    fn default_repair_retry_interval_matches_partitions_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it); keep it in lockstep with the built-in
        // the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .repair_retry_interval
            .get_duration()
            .as_millis();
        let built_in =
            u128::from(partitions::REPAIR_RETRY_TICKS) * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] repair_retry_interval default drifted from \
             partitions::REPAIR_RETRY_TICKS"
        );
    }

    #[test]
    fn default_repair_chunk_max_matches_shard_constant() {
        // Belt and suspenders with the static assert above: that pins the
        // duplicated configs-crate literal, this pins the shipped config.toml
        // value the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default().repair_chunk_max;
        assert_eq!(
            config_default as u64,
            shard::REPAIR_CHUNK_MAX,
            "[cluster] repair_chunk_max default drifted from shard::REPAIR_CHUNK_MAX"
        );
    }

    #[test]
    fn default_evicted_ring_capacity_matches_partitions_constant() {
        // Belt and suspenders with the static assert above; this pins the
        // shipped config.toml value.
        let config_default = configs::partition::PartitionConfig::default().evicted_ring_capacity;
        assert_eq!(
            config_default,
            partitions::EVICTED_RING_CAPACITY,
            "[partition] evicted_ring_capacity default drifted from \
             partitions::EVICTED_RING_CAPACITY"
        );
    }

    #[test]
    fn default_evicted_ring_bytes_max_matches_partitions_constant() {
        // Belt and suspenders with the static assert above; this pins the
        // shipped config.toml value.
        let config_default = configs::partition::PartitionConfig::default()
            .evicted_ring_bytes_max
            .as_bytes_u64();
        assert_eq!(
            config_default,
            partitions::EVICTED_RING_BYTES_MAX,
            "[partition] evicted_ring_bytes_max default drifted from \
             partitions::EVICTED_RING_BYTES_MAX"
        );
    }

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
        let (bundle_tx, _bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(0);
        let flag = Arc::new(AtomicBool::new(false));
        let mux = ServerMuxStateMachine::default();
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
            crossfire::mpmc::bounded_async::<ServerMetadataBundle>(usize::from(peers));
        let flag = Arc::new(AtomicBool::new(false));

        let owner = ServerMuxStateMachine::default();
        let bundle = owner.factory_bundle();
        broadcast_metadata_bundle(0, &bundle_tx, bundle, peers, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("broadcast must succeed with one peer drained");

        let received = await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("peer must receive the broadcast bundle");
        let _peer_mux = ServerMuxStateMachine::from_factory_bundle(received);
    }

    #[compio::test]
    async fn broadcast_metadata_bundle_aborts_when_peers_drop_rx() {
        // Shard 0 drives handoff but every peer's `bundle_rx` was dropped
        // before recv. Silently returning Ok would commit listener binds
        // and consensus init for a cluster whose peers are gone; the
        // broadcast must surface the disconnect so `shard_main` aborts.
        let (bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(0);
        drop(bundle_rx);
        let flag = Arc::new(AtomicBool::new(false));
        let mux = ServerMuxStateMachine::default();

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
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 0 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_metadata_bundle_aborts_when_owner_drops_without_sending() {
        let (bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(1);
        let flag = Arc::new(AtomicBool::new(false));

        // Shard 0 dies before broadcasting; the peer must observe the
        // disconnect and abort instead of hanging forever.
        drop(bundle_tx);

        let err = await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect_err("a peer whose owner never sends must abort");
        assert!(
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 1 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_metadata_bundle_aborts_on_shutdown_flag() {
        // compio 0.19 `JoinHandle` yields `Result<T, JoinError>`; the
        // `ResumeUnwind` impl re-raises a task panic and maps cancellation
        // to `None`.
        use compio::runtime::ResumeUnwind;

        let (_bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(1);
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
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 1 }),
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
                ServerError::ShardBootstrapBarrierAborted { remaining: 1 }
            ),
            "expected ShardBootstrapBarrierAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn pump_drain_timeout_is_not_reported_as_clean() {
        let mut config = ServerConfig::default();
        let timeout = Duration::from_millis(1);
        Arc::get_mut(&mut config.system)
            .expect("a fresh ServerConfig owns its system config")
            .sharding
            .shutdown_drain_timeout = iggy_common::IggyDuration::new(timeout);
        let pump = compio::runtime::spawn(std::future::pending::<()>());

        let error = await_pump_drain(Some(pump), &config, 7)
            .await
            .expect_err("a live pump past the drain budget is not a clean exit");
        assert!(matches!(
            error,
            ServerError::ShardPumpDrainTimedOut {
                shard_id: 7,
                timeout: actual,
            } if actual == timeout
        ));
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
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 2 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }

    fn cluster_node(ip: &str, http: Option<u16>) -> configs::cluster::ClusterNodeConfig {
        cluster_node_with_ports(ip, Some(18070), http)
    }

    fn cluster_node_with_ports(
        ip: &str,
        tcp: Option<u16>,
        http: Option<u16>,
    ) -> configs::cluster::ClusterNodeConfig {
        configs::cluster::ClusterNodeConfig {
            name: "node".to_owned(),
            ip: ip.to_owned(),
            advertised_address: None,
            advertised_addresses: Vec::new(),
            replica_id: 0,
            ports: configs::cluster::TransportPorts {
                tcp,
                http,
                ..Default::default()
            },
        }
    }

    fn addr(value: &str) -> SocketAddr {
        value.parse().expect("valid socket address literal")
    }

    #[test]
    fn cluster_http_addr_takes_port_from_roster() {
        // A byte-identical top-level [http].address is shared across nodes on
        // one host; the per-node roster port is the only port source so each
        // node binds a distinct HTTP socket.
        let node = cluster_node("127.0.0.1", Some(18090));
        let addrs = resolve_cluster_client_addrs(
            &node,
            addr("127.0.0.1:8090"),
            None,
            None,
            Some(addr("127.0.0.1:3000")),
        )
        .expect("cluster address resolution must succeed");
        assert_eq!(addrs.http, Some(addr("127.0.0.1:18090")));
    }

    #[test]
    fn cluster_http_addr_merges_config_ip_with_roster_port() {
        // Docker/Helm bind `0.0.0.0` and probe loopback; the roster ip is
        // only the advertised address. Cluster mode must keep the configured
        // interface and take just the port from the roster.
        let node = cluster_node("10.0.0.5", Some(18090));
        let addrs = resolve_cluster_client_addrs(
            &node,
            addr("0.0.0.0:8090"),
            None,
            None,
            Some(addr("0.0.0.0:3000")),
        )
        .expect("cluster address resolution must succeed");
        assert_eq!(addrs.http, Some(addr("0.0.0.0:18090")));
    }

    #[test]
    fn cluster_http_addr_requires_roster_port_for_enabled_transport() {
        // No fallback to the top-level port: a silent default could collide
        // with another same-host node, so a missing roster port for an
        // enabled transport must refuse to boot.
        let node = cluster_node("10.0.0.5", None);
        let result = resolve_cluster_client_addrs(
            &node,
            addr("127.0.0.1:8090"),
            None,
            None,
            Some(addr("127.0.0.1:3000")),
        );
        assert!(matches!(
            result,
            Err(ServerError::ClusterPortMissing {
                transport: "http",
                replica_id: 0,
            })
        ));
    }

    #[test]
    fn cluster_http_addr_is_none_when_http_disabled() {
        // http.enabled = false collapses default_http_addr to None; no roster
        // port can revive a listener the operator turned off.
        let node = cluster_node("127.0.0.1", Some(18090));
        let addrs = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.http, None);
    }

    /// Regression: the shutdown-join deadline must arm at SHUTDOWN, not
    /// at boot. The original bound measured from `join_all` entry, so any
    /// healthy server outliving `shutdown_join_timeout` (30s default) was
    /// abandoned as "wedged" and the process exited - every BDD run died
    /// at t+30s while the test container was still compiling.
    #[test]
    fn join_waits_unbounded_while_the_server_runs() {
        let shutdown_flag = AtomicBool::new(false);
        // Thread outlives a deliberately tiny join budget; with the flag
        // clear the budget must never even arm.
        let handle = thread::spawn(|| -> Result<(), ServerError> {
            thread::sleep(Duration::from_millis(300));
            Ok(())
        });
        let mut deadline = None;
        let joined = join_until_shutdown_deadline(
            handle,
            &shutdown_flag,
            Duration::from_millis(20),
            &mut deadline,
        );
        assert!(
            matches!(joined, Some(Ok(Ok(())))),
            "a running server must be awaited indefinitely, not abandoned as wedged"
        );
        assert!(
            deadline.is_none(),
            "the join deadline must not arm before the shutdown flag flips"
        );
    }

    #[test]
    fn join_abandons_a_wedged_shard_after_the_shutdown_deadline() {
        let shutdown_flag = AtomicBool::new(true);
        // Never finishes: stands in for a wedged pump. The thread leaks
        // into the test process, which exits right after.
        let handle = thread::spawn(|| -> Result<(), ServerError> {
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        });
        let mut deadline = None;
        let joined = join_until_shutdown_deadline(
            handle,
            &shutdown_flag,
            Duration::from_millis(100),
            &mut deadline,
        );
        assert!(
            joined.is_none(),
            "a shard still running past the post-shutdown budget must be abandoned"
        );
        assert!(deadline.is_some(), "the deadline arms once the flag is set");
    }

    #[test]
    fn cluster_tcp_addr_takes_port_from_roster() {
        // Same rule as the other transports: the roster owns the port so
        // same-host nodes sharing one [tcp].address still bind distinct
        // sockets.
        let node = cluster_node("127.0.0.1", None);
        let addrs = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.client, addr("127.0.0.1:18070"));
    }

    #[test]
    fn cluster_tcp_addr_merges_config_ip_with_roster_port() {
        // The roster ip is advertised, not bound. Binding it directly would
        // strand every co-located dialer (sidecars, health probes, on-host
        // consumers) that reaches this node over loopback.
        let node = cluster_node("10.0.0.5", None);
        let addrs = resolve_cluster_client_addrs(&node, addr("0.0.0.0:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.client, addr("0.0.0.0:18070"));
    }

    #[test]
    fn cluster_tcp_addr_requires_roster_port() {
        // tcp is always enabled in cluster mode, so a roster entry without a
        // tcp port refuses to boot rather than falling back to [tcp].address.
        let node = cluster_node_with_ports("10.0.0.5", None, None);
        let result = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None);
        assert!(matches!(
            result,
            Err(ServerError::ClusterPortMissing {
                transport: "tcp",
                replica_id: 0,
            })
        ));
    }

    #[test]
    fn cluster_tcp_addr_keeps_loopback_bind_and_warns_on_roster_mismatch() {
        // A loopback [tcp].address under a routable roster ip is honoured
        // as configured; remote peers cannot reach it, so the mismatch is
        // warned about instead of silently rebinding.
        let node = cluster_node("10.0.0.5", None);
        let addrs = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.client, addr("127.0.0.1:18070"));
        assert!(roster_ip_unreachable_from_bind_addr(&node.ip, addrs.client));
    }

    #[test]
    fn roster_mismatch_warning_is_silent_for_wildcard_and_hostname_rosters() {
        // A wildcard bind covers the roster interface, and a DNS roster entry
        // can resolve to the bound one; neither is a misconfiguration.
        assert!(!roster_ip_unreachable_from_bind_addr(
            "10.0.0.5",
            addr("0.0.0.0:18070")
        ));
        assert!(!roster_ip_unreachable_from_bind_addr(
            "node-1.example.com",
            addr("127.0.0.1:18070")
        ));
        assert!(!roster_ip_unreachable_from_bind_addr(
            "10.0.0.5",
            addr("10.0.0.5:18070")
        ));
    }
}
