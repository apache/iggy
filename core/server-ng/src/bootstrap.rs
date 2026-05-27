/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::config_writer::write_current_config;
use crate::login_register::LoginRegisterError;
use crate::server_error::{ServerNgError, ShardJoinFailure, ShardJoinFailureKind};
use crate::session_manager::SessionManager;
use bytes::Bytes;
use configs::server_ng::ServerNgConfig;
use configs::sharding::{
    INBOX_CAPACITY_MAX, SHUTDOWN_DRAIN_TIMEOUT_MAX, SHUTDOWN_POLL_INTERVAL_MAX,
};
use consensus::{LocalPipeline, MetadataHandle, PartitionsHandle, Sequencer, VsrConsensus};
// `try_send` / `try_recv` resolve through these traits on `MAsyncTx` /
// `MAsyncRx`; the metadata-handoff loops below depend on the
// non-blocking variants for cancel-safe shutdown polling.
use crossfire::{AsyncRxTrait, AsyncTxTrait};
use iggy_binary_protocol::codes::{
    GET_CLUSTER_METADATA_CODE, GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE,
    GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE, PING_CODE, POLL_MESSAGES_CODE,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, DeleteConsumerOffsetRequest, StoreConsumerOffset2Request,
    StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::{PollMessagesRequest, SendMessagesHeader};
use iggy_binary_protocol::requests::personal_access_tokens::{
    CreatePersonalAccessTokenRequest as WireCreatePersonalAccessTokenRequest,
    DeletePersonalAccessTokenRequest as WireDeletePersonalAccessTokenRequest,
};
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::streams::{GetStreamRequest, GetStreamsRequest};
use iggy_binary_protocol::requests::topics::{GetTopicRequest, GetTopicsRequest};
use iggy_binary_protocol::requests::users::{
    GetUserRequest, LoginRegisterRequest, LoginRegisterWithPatRequest,
};
use iggy_binary_protocol::responses::personal_access_tokens::RawPersonalAccessTokenResponse;
use iggy_binary_protocol::responses::streams::StreamResponse;
use iggy_binary_protocol::responses::streams::get_stream::{
    GetStreamResponse, TopicHeader as StreamTopicHeader,
};
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;
use iggy_binary_protocol::responses::system::get_cluster_metadata::{
    ClusterMetadataResponse, ClusterNodeResponse,
};
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;
use iggy_binary_protocol::responses::topics::get_topic::{GetTopicResponse, PartitionResponse};
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::get_users::GetUsersResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::{
    Command2, EvictionReason, GenericHeader, Operation, ReplyHeader, RequestHeader, WireDecode,
    WireEncode, WireIdentifier, WireName, WirePartitioning,
};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyByteSize, IggyError, IggyTimestamp, PartitionStats,
    PersonalAccessToken, TopicStats, UserStatus, variadic,
};
use journal::Journal;
use journal::prepare_journal::PrepareJournal;
use message_bus::client_listener::{self, RequestHandler};
use message_bus::installer;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::io as replica_io;
use message_bus::replica::listener::{self as replica_listener, MessageHandler};
use message_bus::transports::quic::server_config_with_cert;
use message_bus::transports::tls::{
    TlsServerCredentials, install_default_crypto_provider, load_pem, self_signed_for_loopback,
};
use message_bus::{
    AcceptedClientFn, AcceptedQuicClientFn, AcceptedReplicaFn, AcceptedTlsClientFn,
    AcceptedWsClientFn, IggyMessageBus, MessageBus, ReplicaOwnerTable, connector,
};
use metadata::IggyMetadata;
use metadata::MuxStateMachine;
use metadata::impls::metadata::{IggySnapshot, RegisterSubmitError, StreamsFrontend};
use metadata::impls::recovery::recover;
use metadata::stm::consumer_group::ConsumerGroups;
use metadata::stm::mux::WithFactory;
use metadata::stm::snapshot::Snapshot;
use metadata::stm::stream::{Partition, Streams};
use metadata::stm::user::Users;
use metadata::stm::user::{
    CreatePersonalAccessTokenRequest as ReplicatedCreatePersonalAccessTokenRequest,
    DeletePersonalAccessTokenRequest as ReplicatedDeletePersonalAccessTokenRequest,
};
use partitions::{
    IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionsConfig, Segment,
};
use server_common::Message;
use server_common::sharding::{IggyNamespace, LocalIdx, PartitionLocation, ShardId};
// TODO: decouple bootstrap/storage helpers and logging from the `server` crate.
use secrecy::ExposeSecret;
use server::bootstrap::{create_directories, create_shard_executor};
use server::log::logger::Logging;
use server::shard_allocator::{ShardAllocator, ShardInfo};
use server::streaming::partitions::storage::{load_consumer_group_offsets, load_consumer_offsets};
use server::streaming::segments::storage::create_segment_storage;
use server::streaming::users::user::User as LegacyUser;
use server::streaming::utils::crypto;
use server::{IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV};
use shard::builder::IggyShardBuilder;
use shard::metrics::ShardMetrics;
use shard::shards_table::{PapayaShardsTable, calculate_shard_assignment};
use shard::{
    CoordinatorConfig, IggyShard, PartitionConsensusConfig, Receiver as ShardReceiver, ShardFrame,
    ShardIdentity, TaggedSender, channel, shard_mesh_channels,
};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};

const CLUSTER_ID: u128 = 1;
const SHARD_REPLICA_ID: u8 = 0;

type ServerNgMuxStateMachine = MuxStateMachine<variadic!(Users, Streams, ConsumerGroups)>;

/// Cross-thread bundle carrying one `ReadHandleFactory` per metadata
/// state. Shard 0 mints one after `recover()` and broadcasts a clone to
/// every peer shard; each peer rebuilds a reader-mode
/// [`ServerNgMuxStateMachine`] on its own runtime, skipping the WAL.
type ServerNgMetadataBundle = <variadic!(Users, Streams, ConsumerGroups) as WithFactory>::Bundle;

type ServerNgMetadata = IggyMetadata<
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

type ServerNgShardHandle = Rc<RefCell<Option<Weak<ServerNgShard>>>>;

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
/// `RecoverySync` two-phase fence. Phase 2 of the old handshake was
/// only there to keep peer scans away from shard 0's torn-tail repair;
/// with no peer scan that race is structurally gone.
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

struct TcpTopology {
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
            &config.system.logging,
            &config.telemetry,
        )
        .map_err(ServerNgError::Logging)?;

    Ok(config)
}

/// Re-validate the runtime sharding knobs that the per-shard runtime
/// consumes directly. Mirrors `ShardingConfig::validate` so a caller
/// that built the config without running it (e.g. tests, embedded
/// usage) cannot OOM at boot or wedge process exit with an out-of-range
/// value.
fn validate_sharding_runtime_knobs(
    sharding: &configs::sharding::ShardingConfig,
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
pub fn bootstrap(
    config: ServerNgConfig,
    current_replica_id: Option<u8>,
) -> Result<ShardHandles, ServerNgError> {
    let allocator = ShardAllocator::new(&config.system.sharding.cpu_allocation)
        .map_err(ServerNgError::ShardAllocator)?;
    let assignments = allocator
        .to_shard_assignments()
        .map_err(ServerNgError::ShardAllocator)?;
    let shards_count = assignments.len();
    if shards_count == 0 {
        return Err(ServerNgError::ShardsCountZero);
    }
    // Shard ids index `ReplicaOwnerTable` slots as `u16`. `OWNER_NONE`
    // (`u16::MAX`) is reserved as the empty-slot sentinel, so a server
    // configured with `u16::MAX` shards would mint a shard id that
    // collides with the sentinel and an owner-table lookup could never
    // tell that shard apart from an unowned slot. Reject at boot so the
    // invariant is held by the type system above this line, not by hoping
    // the operator never configures 65535 cores worth of shards.
    let total_shards = match u16::try_from(shards_count) {
        Ok(count) if count < message_bus::OWNER_NONE => count,
        _ => {
            return Err(ServerNgError::ShardsCountOverflow {
                count: shards_count,
            });
        }
    };

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

    let mut shard_threads: Vec<(u16, thread::JoinHandle<Result<(), ServerNgError>>)> =
        Vec::with_capacity(shards_count);
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
                    owner_table_for_shard,
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
    owner_table: Arc<ReplicaOwnerTable>,
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
        shard_main(
            shard_id,
            total_shards,
            replica_id,
            senders,
            inbox,
            &config,
            shutdown_flag,
            metadata_handoff,
            owner_table,
        )
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
    owner_table: Arc<ReplicaOwnerTable>,
) -> Result<(), ServerNgError> {
    let topology = resolve_tcp_topology(config, replica_id)?;
    let bus = Rc::new(IggyMessageBus::with_config_and_owner_table(
        shard_id,
        config,
        owner_table,
    ));

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
            let recovered = recover::<ServerNgMuxStateMachine>(data_dir)
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
        if let Some((journal, snapshot, last_applied_op)) = owner_state {
            let restored_op = last_applied_op
                .unwrap_or_else(|| snapshot.as_ref().map_or(0, IggySnapshot::sequence_number));
            let consensus = restore_metadata_consensus(
                &journal,
                restored_op,
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

    let shard_metrics = ShardMetrics::for_shard();
    let shard = build_shard_for_thread(
        shard_id,
        total_shards,
        config,
        &topology,
        metadata,
        Rc::clone(&bus),
        senders,
        inbox,
        shard_metrics,
    )
    .await?;

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

    let (stop_tx, stop_rx) = channel(1);
    let pump_shard = Rc::clone(&shard);
    let pump_handle = compio::runtime::spawn(async move {
        pump_shard.run_message_pump(stop_rx).await;
    });
    bus.track_background(pump_handle);

    // Listeners (replica + every client transport) bind on shard 0 only.
    // Shard 0's coordinator round-robins inbound TCP/WS connections to
    // peer shards via fd-transfer. QUIC and TCP-TLS clients terminate
    // locally on shard 0 (their per-connection state is non-portable -
    // see `LifecycleFrame::ClientWsConnectionSetup` rustdoc).
    //
    // No phase-2 listener fence is needed: peer shards no longer scan
    // the WAL, so a shard-0 append accepted mid-boot cannot race a
    // peer's `truncate_or_fail`. The factory-bundle handoff has already
    // installed reader-mode `MuxStateMachine`s on every peer by the
    // time shard 0 returns from `broadcast_metadata_bundle`.
    if shard_id == 0 {
        let coord = shard
            .coordinator()
            .expect("shard 0 always has a coordinator attached by the builder");
        let on_client_request = make_client_request_handler(&shard);
        let accepted_replica = make_delegating_replica_accept_fn(Rc::clone(&coord));
        let accepted_client = make_shard_zero_client_accept_fns(coord, &bus, on_client_request);

        if let Err(error) =
            start_tcp_runtime(&shard, config, &topology, accepted_replica, accepted_client).await
        {
            let _ = stop_tx.try_send(());
            return Err(error);
        }
    }

    bus.token().wait().await;
    let _ = stop_tx.try_send(());

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
) -> Result<Rc<ServerNgShard>, ServerNgError> {
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
    let mut partitions = IggyPartitions::with_capacity(
        shard_local_id,
        PartitionsConfig {
            messages_required_to_save: config.system.partition.messages_required_to_save,
            size_of_messages_required_to_save: config
                .system
                .partition
                .size_of_messages_required_to_save,
            enforce_fsync: config.system.partition.enforce_fsync,
            segment_size: config.system.segment.size,
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
                        owned.push((stream.id, topic_id, topic.stats.clone(), partition.clone()));
                    } else {
                        // Non-owning entry: only `shard_id` is consulted
                        // by the router; `local_idx` is meaningful only on
                        // the owning shard, so a sentinel `0` is safe.
                        shards_table.insert(
                            namespace,
                            PartitionLocation::new(ShardId::new(owning_shard), LocalIdx::new(0)),
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
    for (stream_id, topic_id, topic_stats, partition_metadata) in owned {
        validate_recovered_namespace(config, stream_id, topic_id, partition_metadata.id)?;
        let namespace = IggyNamespace::new(stream_id, topic_id, partition_metadata.id);
        let partition = load_partition(
            config,
            namespace,
            topic_stats,
            &partition_metadata,
            topology.self_replica_id,
            topology.replica_count,
            Rc::clone(&bus),
        )
        .await?;
        let local_idx = partitions.insert(namespace, partition);
        shards_table.insert(
            namespace,
            PartitionLocation::new(ShardId::new(shard_id), LocalIdx::new(*local_idx)),
        );
    }

    let shard_handle = Rc::new(RefCell::new(None));
    let on_replica_message = make_deferred_replica_message_handler(&shard_handle);
    let on_client_request = make_deferred_client_request_handler(&bus, &shard_handle);
    let on_metadata_submit = make_metadata_submit_handler(&shard_handle);
    let shard_name = format!("server-ng-shard-{shard_id}");
    let built = IggyShardBuilder::new(
        ShardIdentity::new(shard_id, shard_name),
        Rc::clone(&bus),
        on_replica_message,
        on_client_request,
        on_metadata_submit,
        metadata,
        partitions,
        senders,
        inbox,
        shards_table,
        PartitionConsensusConfig::new(CLUSTER_ID, topology.replica_count, Rc::clone(&bus)),
        CoordinatorConfig::default(),
        metrics,
    )
    .build()
    .map_err(ServerNgError::ShardConstruction)?;

    let shard = Rc::new(built.shard);
    *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
    Ok(shard)
}

fn restore_metadata_consensus(
    journal: &PrepareJournal,
    restored_op: u64,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> VsrConsensus<Rc<IggyMessageBus>> {
    let mut consensus = VsrConsensus::new(
        CLUSTER_ID,
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

    consensus.init();
    consensus.sequencer().set_sequence(restored_op);
    // TODO(hubcio): clustered bootstrap does not persist a durable
    // (view, commit_op) watermark, so we collapse commit_min/commit_max to
    // `restored_op` (= last journaled op). VSR consequence: on a view
    // change after partial recovery, a replica that came back with a
    // commit_max below the cluster's true commit_min will accept stale
    // prepares as new and overwrite already-committed log entries
    // (split-brain on the committed prefix).
    //
    // Fix direction: persist (view, commit_op) on the journal-header path
    // (`core/journal/src/prepare_journal.rs` PrepareHeader already carries
    // `view`; extend with `commit_op` or add a sidecar watermark file
    // updated on every commit), seed `restore_commit_state(min, max)`
    // from durable state on recovery, and refuse boot if the gap exceeds
    // a configurable threshold. Tracked under the "durable
    // PartitionJournal + durable (view, commit_op) watermark" milestone
    // named in the multi-shard wiring commit body.
    //
    // Reproducible in `core/simulator` once it grows a restart-from-disk
    // path: today `SimNetwork::enable_process` only un-disables links
    // without replaying `SimJournal` + `SimSnapshot` through
    // `restore_metadata_consensus`, so the flatten cannot trip. Add a
    // crash-restart-replay primitive in the sim, then write a scenario
    // that commits an op on the primary, crashes the primary mid-
    // replicate-ack, triggers a view change, and asserts the recovered
    // replica refuses to re-accept the committed prepare.
    consensus.restore_commit_state(restored_op, restored_op);
    if let Some(header) = last_header {
        consensus.set_last_prepare_checksum(header.checksum);
    }

    consensus
}

const fn validate_recovered_namespace(
    config: &ServerNgConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerNgError> {
    let namespace = &config.extra.namespace;
    if stream_id < namespace.max_streams
        && topic_id < namespace.max_topics
        && partition_id < namespace.max_partitions
    {
        return Ok(());
    }

    Err(ServerNgError::RecoveredNamespaceOutOfBounds {
        stream_id,
        topic_id,
        partition_id,
        max_streams: namespace.max_streams,
        max_topics: namespace.max_topics,
        max_partitions: namespace.max_partitions,
    })
}

async fn load_partition(
    config: &ServerNgConfig,
    namespace: IggyNamespace,
    topic_stats: Arc<TopicStats>,
    partition_metadata: &Partition,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerNgError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let stats = Arc::new(PartitionStats::new(topic_stats));
    let consensus = VsrConsensus::new(
        CLUSTER_ID,
        self_replica_id,
        replica_count,
        namespace.inner(),
        bus,
        LocalPipeline::new(),
    );
    consensus.init();

    // TODO: decouple the loading logic from the `server` crate and load directly
    // into the new `partitions` log/runtime types.
    let loaded_log = server::bootstrap::load_segments(
        &config.system,
        stream_id,
        topic_id,
        partition_id,
        config
            .system
            .get_partition_path(stream_id, topic_id, partition_id),
        stats.clone(),
    )
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
    hydrate_partition_log(
        &mut partition,
        config,
        stream_id,
        topic_id,
        partition_id,
        loaded_log,
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
    loaded_log: server::streaming::partitions::log::SegmentedLog<
        server::streaming::partitions::journal::MemoryMessageJournal,
    >,
) -> Result<(), ServerNgError> {
    // TODO: decouple the loading logic from the `server` crate. This currently
    // adapts the old server segmented log into the new `partitions` log.
    for (segment_index, (segment, storage)) in loaded_log
        .segments()
        .iter()
        .zip(loaded_log.storages().iter().cloned())
        .enumerate()
    {
        validate_recovered_segment(
            stream_id,
            topic_id,
            partition_id,
            segment,
            &storage,
            loaded_log
                .indexes()
                .get(segment_index)
                .and_then(|indexes| indexes.as_ref()),
        )?;
        let max_timestamp = match loaded_log
            .indexes()
            .get(segment_index)
            .and_then(|indexes| indexes.as_ref())
        {
            Some(indexes) => indexes_max_timestamp(indexes),
            None => load_segment_max_timestamp(&storage, stream_id, topic_id, partition_id).await?,
        };
        partition.log.add_persisted_segment(
            convert_segment(segment, max_timestamp),
            storage,
            None,
            None,
        );
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

fn validate_recovered_segment(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    segment: &iggy_common::Segment,
    storage: &server_common::SegmentStorage,
    indexes: Option<&server::streaming::segments::IggyIndexesMut>,
) -> Result<(), ServerNgError> {
    let messages_size_bytes = storage
        .messages_reader
        .as_ref()
        .map_or(0, |reader| u64::from(reader.file_size()));
    let indexed_size_bytes = indexes.map_or(0, |indexes| u64::from(indexes.messages_size()));
    if messages_size_bytes == indexed_size_bytes {
        return Ok(());
    }

    Err(ServerNgError::RecoveredSegmentSizeDivergence {
        stream_id,
        topic_id,
        partition_id,
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        messages_size_bytes,
        indexed_size_bytes,
    })
}

fn convert_segment(segment: &iggy_common::Segment, max_timestamp: u64) -> Segment {
    Segment {
        sealed: segment.sealed,
        start_timestamp: segment.start_timestamp,
        end_timestamp: segment.end_timestamp,
        max_timestamp,
        current_position: u64::from(segment.current_position),
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        size: segment.size,
        max_size: segment.max_size,
    }
}

fn indexes_max_timestamp(indexes: &server::streaming::segments::IggyIndexesMut) -> u64 {
    let mut max_timestamp = 0;
    for index in 0..indexes.count() {
        if let Some(index_view) = indexes.get(index) {
            max_timestamp = max_timestamp.max(index_view.timestamp());
        }
    }

    max_timestamp
}

async fn load_segment_max_timestamp(
    storage: &server_common::SegmentStorage,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<u64, ServerNgError> {
    let Some(index_reader) = storage.index_reader.as_ref() else {
        return Ok(0);
    };

    let indexes = index_reader
        .load_all_indexes_from_disk()
        .await
        .map_err(|source| {
            error!(
                stream_id,
                topic_id,
                partition_id,
                error = %source,
                "failed to load segment indexes while recovering max timestamp"
            );
            source
        })?;
    Ok(indexes_max_timestamp(&indexes))
}

fn configure_consumer_offsets(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
    namespace: IggyNamespace,
    current_offset: u64,
) -> Result<(), ServerNgError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let consumer_offsets_path =
        config
            .system
            .get_consumer_offsets_path(stream_id, topic_id, partition_id);
    let consumer_group_offsets_path =
        config
            .system
            .get_consumer_group_offsets_path(stream_id, topic_id, partition_id);

    let loaded_consumer_offsets = load_partition_consumer_offsets(
        &consumer_offsets_path,
        "consumer",
        stream_id,
        topic_id,
        partition_id,
    )?;
    let consumer_offsets = ConsumerOffsets::with_capacity(loaded_consumer_offsets.len());
    {
        let guard = consumer_offsets.pin();
        for offset in loaded_consumer_offsets {
            let recovered_offset = offset.offset.load(Ordering::Relaxed);
            if recovered_offset > current_offset {
                return Err(ServerNgError::RecoveredConsumerOffsetOutOfBounds {
                    consumer_kind: "consumer",
                    consumer_id: offset.consumer_id as usize,
                    offset: recovered_offset,
                    current_offset,
                    stream_id,
                    topic_id,
                    partition_id,
                });
            }
            guard.insert(offset.consumer_id as usize, offset);
        }
    }

    let loaded_group_offsets = load_partition_consumer_group_offsets(
        &consumer_group_offsets_path,
        stream_id,
        topic_id,
        partition_id,
    )?;
    let consumer_group_offsets = ConsumerGroupOffsets::with_capacity(loaded_group_offsets.len());
    {
        let guard = consumer_group_offsets.pin();
        for (group_id, offset) in loaded_group_offsets {
            let recovered_offset = offset.offset.load(Ordering::Relaxed);
            if recovered_offset > current_offset {
                return Err(ServerNgError::RecoveredConsumerOffsetOutOfBounds {
                    consumer_kind: "consumer group",
                    consumer_id: group_id.0,
                    offset: recovered_offset,
                    current_offset,
                    stream_id,
                    topic_id,
                    partition_id,
                });
            }
            guard.insert(group_id, offset);
        }
    }

    partition.configure_consumer_offset_storage(
        consumer_offsets_path,
        consumer_group_offsets_path,
        consumer_offsets,
        consumer_group_offsets,
        config.system.partition.enforce_fsync,
    );
    Ok(())
}

fn load_partition_consumer_offsets(
    path: &str,
    consumer_kind: &'static str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<Vec<iggy_common::ConsumerOffset>, ServerNgError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    load_consumer_offsets(path).or_else(|source| {
        if matches!(&source, IggyError::CannotReadConsumerOffsets(missing_path) if !Path::new(missing_path).exists())
        {
            return Ok(Vec::new());
        }

        Err(ServerNgError::ConsumerOffsetsLoad {
            consumer_kind,
            stream_id,
            topic_id,
            partition_id,
            path: path.to_string(),
            source: Box::new(source),
        })
    })
}

fn load_partition_consumer_group_offsets(
    path: &str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<Vec<(iggy_common::ConsumerGroupId, iggy_common::ConsumerOffset)>, ServerNgError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    load_consumer_group_offsets(path).or_else(|source| {
        if matches!(&source, IggyError::CannotReadConsumerOffsets(missing_path) if !Path::new(missing_path).exists())
        {
            return Ok(Vec::new());
        }

        Err(ServerNgError::ConsumerOffsetsLoad {
            consumer_kind: "consumer group",
            stream_id,
            topic_id,
            partition_id,
            path: path.to_string(),
            source: Box::new(source),
        })
    })
}

async fn ensure_initial_segment(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerNgError> {
    if partition.log.has_segments() {
        return Ok(());
    }

    // TODO: decouple segment storage creation from the `server` crate.
    let storage =
        create_segment_storage(&config.system, stream_id, topic_id, partition_id, 0, 0, 0)
            .await
            .map_err(|source| {
                error!(
                    stream_id,
                    topic_id,
                    partition_id,
                    error = %source,
                    "failed to create initial segment storage"
                );
                source
            })?;
    let messages_path = config
        .system
        .get_messages_file_path(stream_id, topic_id, partition_id, 0);
    let index_path = config
        .system
        .get_index_path(stream_id, topic_id, partition_id, 0);
    partition.log.add_persisted_segment(
        Segment::new(0, config.system.segment.size),
        storage,
        Some(Rc::new(
            MessagesWriter::new(
                &messages_path,
                Rc::new(AtomicU64::new(0)),
                config.system.partition.enforce_fsync,
                false,
            )
            .await
            .map_err(|source| {
                error!(
                    stream_id,
                    topic_id,
                    partition_id,
                    path = %messages_path,
                    error = %source,
                    "failed to initialize initial messages writer"
                );
                source
            })?,
        )),
        Some(Rc::new(
            IggyIndexWriter::new(
                &index_path,
                Rc::new(AtomicU64::new(0)),
                config.system.partition.enforce_fsync,
                false,
            )
            .await
            .map_err(|source| {
                error!(
                    stream_id,
                    topic_id,
                    partition_id,
                    path = %index_path,
                    error = %source,
                    "failed to initialize initial sparse index writer"
                );
                source
            })?,
        )),
    );
    partition.stats.increment_segments_count(1);

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
    accepted_clients: LocalClientAcceptFns,
) -> Result<(), ServerNgError> {
    if config.tcp.enabled && !config.tcp.tls.enabled {
        return start_via_replica_io(shard, config, topology, accepted_replica, accepted_clients)
            .await;
    }

    start_manual_runtime(shard, config, topology, accepted_replica, accepted_clients).await
}

async fn start_via_replica_io(
    shard: &Rc<ServerNgShard>,
    config: &ServerNgConfig,
    topology: &TcpTopology,
    accepted_replica: AcceptedReplicaFn,
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

    let LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
    } = accepted_clients;

    let bound = replica_io::start_on_shard_zero(
        &shard.bus,
        replica_addr,
        topology.client_listen_addr,
        topology.ws_listen_addr,
        topology.quic_listen_addr,
        quic_credentials,
        topology.tcp_tls_listen_addr,
        tcp_tls_credentials,
        None,
        None,
        CLUSTER_ID,
        topology.self_replica_id,
        topology.replica_count,
        topology.peers.clone(),
        accepted_replica,
        tcp,
        topology.ws_listen_addr.map(|_| ws),
        topology.quic_listen_addr.map(|_| quic),
        topology.tcp_tls_listen_addr.map(|_| tcp_tls),
        None,
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
        bound.ws,
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
        let max_message_size = shard.bus.config().max_message_size;
        let handshake_grace = shard.bus.config().handshake_grace;
        let self_replica_id = topology.self_replica_id;
        let replica_count = topology.replica_count;
        let accepted_replica_for_listener = accepted_replica.clone();
        let replica_handle = compio::runtime::spawn(async move {
            replica_listener::run(
                replica_listener,
                token,
                CLUSTER_ID,
                self_replica_id,
                replica_count,
                accepted_replica_for_listener,
                max_message_size,
                handshake_grace,
            )
            .await;
        });
        shard.bus.track_background(replica_handle);
        connector::start(
            &shard.bus,
            CLUSTER_ID,
            topology.self_replica_id,
            topology.peers.clone(),
            accepted_replica,
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

type ClientRequestQueues = Rc<RefCell<HashMap<u128, VecDeque<Message<GenericHeader>>>>>;
type ActiveClientRequests = Rc<RefCell<HashSet<u128>>>;

fn make_client_request_handler(shard: &Rc<ServerNgShard>) -> RequestHandler {
    let shard = Rc::clone(shard);
    let sessions = Rc::new(RefCell::new(SessionManager::new()));
    let queues: ClientRequestQueues = Rc::new(RefCell::new(HashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(HashSet::new()));
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_for_disconnect = Rc::clone(&shard);
    shard
        .bus
        .set_client_connection_lost_fn(Rc::new(move |client_id| {
            if let Some(vsr_client_id) = sessions_for_disconnect
                .borrow_mut()
                .remove_connection(client_id)
            {
                // Local-table reclaim happens here; then spawn a detached
                // task to submit a cluster-replicated `Logout` so peer
                // replicas drop the slot too. Without this, an
                // already-committed `Register` lingers on every backup
                // until they evict the client themselves -- a
                // local-vs-cluster divergence observable across view
                // changes. `submit_logout_in_process` requires shard 0
                // consensus, which this handler always runs on.
                shard_for_disconnect
                    .plane
                    .metadata()
                    .remove_client_session(vsr_client_id);
                let shard = Rc::clone(&shard_for_disconnect);
                compio::runtime::spawn(async move {
                    // request_id 0 + session 0: synthetic disconnect
                    // logout; `submit_logout_in_process` short-circuits to
                    // current commit when no session entry exists, so a
                    // race with the explicit logout path is a no-op.
                    if let Err(error) = shard
                        .plane
                        .metadata()
                        .submit_logout_in_process(vsr_client_id, 0, 0)
                        .await
                    {
                        warn!(
                            vsr_client_id,
                            ?error,
                            "disconnect logout submit failed; peer slots may linger until eviction"
                        );
                    }
                })
                .detach();
            }
        }));
    Rc::new(move |client_id, message| {
        enqueue_client_request(
            Rc::clone(&shard),
            Rc::clone(&sessions),
            Rc::clone(&queues),
            Rc::clone(&active),
            client_id,
            message,
        );
    })
}

fn make_deferred_replica_message_handler(shard_handle: &ServerNgShardHandle) -> MessageHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |_replica_id, message| {
        if let Some(shard) = upgrade_shard_handle(&shard_handle) {
            shard.dispatch(message);
        }
    })
}

fn make_deferred_client_request_handler(
    bus: &Rc<IggyMessageBus>,
    shard_handle: &ServerNgShardHandle,
) -> RequestHandler {
    let shard_handle = Rc::clone(shard_handle);
    let sessions = Rc::new(RefCell::new(SessionManager::new()));
    let queues: ClientRequestQueues = Rc::new(RefCell::new(HashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(HashSet::new()));
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_handle_for_disconnect = Rc::clone(&shard_handle);
    bus.set_client_connection_lost_fn(Rc::new(move |client_id| {
        if let Some(vsr_client_id) = sessions_for_disconnect
            .borrow_mut()
            .remove_connection(client_id)
            && let Some(shard) = upgrade_shard_handle(&shard_handle_for_disconnect)
        {
            // TODO(vsr): this handler runs on shards > 0 which have no
            // consensus; `submit_logout_in_process` would panic. Local
            // table reclaim only -- bounded local-vs-cluster divergence
            // window until `evict_oldest` reclaims the peer slot. Fix by
            // routing a synthetic Logout request through the inter-shard
            // router to shard 0.
            shard.plane.metadata().remove_client_session(vsr_client_id);
        }
    }));
    Rc::new(move |client_id, message| {
        let shard_handle = Rc::clone(&shard_handle);
        let sessions = Rc::clone(&sessions);
        let queues = Rc::clone(&queues);
        let active = Rc::clone(&active);
        queues
            .borrow_mut()
            .entry(client_id)
            .or_default()
            .push_back(message);
        if !active.borrow_mut().insert(client_id) {
            return;
        }
        compio::runtime::spawn(async move {
            let Some(shard) = upgrade_shard_handle(&shard_handle) else {
                active.borrow_mut().remove(&client_id);
                return;
            };
            drain_client_requests(shard, sessions, queues, active, client_id).await;
        })
        .detach();
    })
}

/// Handler shard 0 runs for an inbound [`shard::MetadataSubmit`]: a peer
/// shard has verified credentials and owns the session locally, and asks
/// shard 0 (the metadata consensus owner) to run only the consensus
/// proposal. Spawns a task so the awaiting peer is woken once the op
/// commits; replies `None` on transient submit failure so the peer never
/// blocks forever.
fn make_metadata_submit_handler(
    shard_handle: &ServerNgShardHandle,
) -> shard::MetadataSubmitHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |submit| {
        let shard_handle = Rc::clone(&shard_handle);
        compio::runtime::spawn(async move {
            let Some(shard) = upgrade_shard_handle(&shard_handle) else {
                return;
            };
            match submit {
                shard::MetadataSubmit::Register {
                    vsr_client_id,
                    reply,
                } => {
                    let session = shard
                        .plane
                        .metadata()
                        .submit_register_in_process(vsr_client_id)
                        .await
                        .ok();
                    let _ = reply.try_send(session);
                }
                shard::MetadataSubmit::Logout {
                    vsr_client_id,
                    session,
                    request,
                    reply,
                } => {
                    let commit = shard
                        .plane
                        .metadata()
                        .submit_logout_in_process(vsr_client_id, session, request)
                        .await
                        .ok();
                    let _ = reply.try_send(commit);
                }
                shard::MetadataSubmit::ClientRequest { request, reply } => {
                    let committed = match request.try_into_typed::<RequestHeader>() {
                        Ok(typed) => shard
                            .plane
                            .metadata()
                            .submit_request_in_process(typed)
                            .await
                            .ok()
                            .map(server_common::Message::into_generic),
                        Err(error) => {
                            warn!(?error, "ClientRequest submit: undecodable request header");
                            None
                        }
                    };
                    let _ = reply.try_send(committed);
                }
            }
        })
        .detach();
    })
}

fn enqueue_client_request(
    shard: Rc<ServerNgShard>,
    sessions: Rc<RefCell<SessionManager>>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
    message: Message<GenericHeader>,
) {
    queues
        .borrow_mut()
        .entry(client_id)
        .or_default()
        .push_back(message);
    if !active.borrow_mut().insert(client_id) {
        return;
    }

    compio::runtime::spawn(async move {
        drain_client_requests(shard, sessions, queues, active, client_id).await;
    })
    .detach();
}

#[allow(clippy::future_not_send)]
async fn drain_client_requests(
    shard: Rc<ServerNgShard>,
    sessions: Rc<RefCell<SessionManager>>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
) {
    loop {
        let Some(message) = pop_next_client_request(&queues, &active, client_id) else {
            return;
        };
        handle_client_request(&shard, &sessions, client_id, message).await;
    }
}

fn pop_next_client_request(
    queues: &ClientRequestQueues,
    active: &ActiveClientRequests,
    client_id: u128,
) -> Option<Message<GenericHeader>> {
    let mut queues = queues.borrow_mut();
    let Some(queue) = queues.get_mut(&client_id) else {
        active.borrow_mut().remove(&client_id);
        return None;
    };
    let message = queue.pop_front();
    if queue.is_empty() {
        queues.remove(&client_id);
    }
    if message.is_none() {
        active.borrow_mut().remove(&client_id);
    }
    message
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_client_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    message: Message<iggy_binary_protocol::GenericHeader>,
) {
    let request = match message.try_into_typed::<RequestHeader>() {
        Ok(request) => request,
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "dropping client request with invalid header"
            );
            return;
        }
    };

    ensure_transport_connection(shard, sessions, transport_client_id);

    let header = *request.header();
    if header.operation == Operation::NonReplicated {
        // Auth bypass guard: only `PING` and `GET_CLUSTER_METADATA` are
        // legitimately pre-auth (liveness probe + connection bootstrap
        // metadata). Every other non-replicated code (`GET_STREAM*`,
        // `GET_TOPIC*`, `GET_STATS`, `POLL_MESSAGES`) reads live state and
        // MUST go through Register first, since server-ng has no
        // per-resource authz layer.
        let nr_code = u32::from_le_bytes(
            request.header().reserved[..4]
                .try_into()
                .unwrap_or_default(),
        );
        let allowed_pre_auth = matches!(nr_code, PING_CODE | GET_CLUSTER_METADATA_CODE);
        if !allowed_pre_auth && sessions.borrow().get_session(transport_client_id).is_none() {
            warn!(
                transport_client_id,
                code = nr_code,
                "dropping pre-auth non-replicated read"
            );
            return;
        }
        handle_non_replicated_request(shard, transport_client_id, request).await;
        return;
    }

    if header.operation == Operation::Register && header.session == 0 && header.request == 0 {
        handle_login_register_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    if header.operation == Operation::Logout {
        handle_logout_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    let bound = sessions.borrow().get_session(transport_client_id);
    if bound.is_none() {
        // Replicated request on an unbound transport. Without this short-
        // circuit, the rewrite below overwrites `header.client` with
        // `transport_client_id` and dispatches; the request_preflight then
        // rejects with `NoSession`/`SessionMismatch` and the failure either
        // disappears silently or emits an Eviction the SDK previously
        // could not decode. Either way the SDK blocked until socket
        // timeout. Emit an empty Reply so the SDK fails fast: the typed
        // decoder downstream rejects the empty body with `InvalidCommand`
        // instead of hanging.
        let commit = current_metadata_commit(shard);
        let reply = build_empty_reply(&header, transport_client_id, 0, commit);
        if let Err(error) = shard
            .bus
            .send_to_client(transport_client_id, reply.into_generic().into_frozen())
            .await
        {
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "failed to surface unbound-session reply"
            );
        } else {
            warn!(
                transport_client_id,
                operation = ?header.operation,
                "dropping replicated request from unbound transport; replied empty"
            );
        }
        return;
    }

    let resolved_namespace = if header.operation.is_partition() {
        match resolve_partition_request_namespace(shard, header.operation, request_body(&request)) {
            Ok(namespace) => Some(namespace),
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "dropping partition request with unresolved namespace"
                );
                return;
            }
        }
    } else {
        None
    };
    let request = request.transmute_header(|header, new_header: &mut RequestHeader| {
        *new_header = header;
        new_header.client = transport_client_id;
        if let Some(namespace) = resolved_namespace {
            new_header.namespace = namespace;
        }
        if let Some((bound_client_id, bound_session)) = bound {
            new_header.client = bound_client_id;
            new_header.session = bound_session;
        }
    });
    let (request, raw_pat_token) =
        match maybe_rewrite_pat_request(sessions, transport_client_id, request) {
            Ok(rewritten) => rewritten,
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "dropping request with invalid PAT replication context"
                );
                return;
            }
        };
    let request_header = *request.header();
    // Replicated request: run consensus on the metadata owner (shard 0) and
    // bring the committed reply back here. This shard owns the connection,
    // so it writes the reply to the socket via the transport client id --
    // shard 0 can't route by the consensus client id (no home-shard bits).
    match submit_client_request_on_owner(shard, request).await {
        Some(reply) => {
            // The raw PAT token never enters consensus (it is non-deterministic
            // and secret), so the committed reply body is empty. Substitute the
            // raw-token response here, on the minting client's home shard, using
            // the confirmed commit position from the committed reply.
            let reply = match build_raw_pat_reply(&request_header, reply, raw_pat_token) {
                Ok(reply) => reply,
                Err(error) => {
                    warn!(
                        transport_client_id,
                        error = %error,
                        "failed to build raw PAT reply"
                    );
                    return;
                }
            };
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "failed to deliver committed reply to client"
                );
            }
        }
        None => {
            // Transient submit failure (not primary / not caught up / dedup
            // absorbed). Stay silent; the SDK read-timeout replays.
            warn!(
                transport_client_id,
                operation = ?header.operation,
                "replicated request not committed (transient); client will replay"
            );
        }
    }
}

#[allow(clippy::future_not_send)]
async fn handle_non_replicated_request(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    const CODE_RANGE: std::ops::Range<usize> = 0..4;
    let code = u32::from_le_bytes(request.header().reserved[CODE_RANGE].try_into().unwrap());
    match code {
        PING_CODE => {
            let commit = current_metadata_commit(shard);
            let reply = build_empty_reply(
                request.header(),
                request.header().client,
                request.header().session,
                commit,
            );
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %error,
                    "failed to send non-replicated ping reply"
                );
            }
        }
        _ => match build_non_replicated_response(shard, code, request_body(&request)) {
            Ok(response) => {
                let commit = current_metadata_commit(shard);
                let reply = response.into_reply(
                    request.header(),
                    request.header().client,
                    request.header().session,
                    commit,
                );
                if let Err(error) = shard
                    .bus
                    .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                    .await
                {
                    warn!(
                        transport_client_id,
                        code,
                        error = %error,
                        "failed to send non-replicated VSR reply"
                    );
                }
            }
            Err(error) => {
                warn!(
                    transport_client_id,
                    code,
                    error = %error,
                    "dropping unsupported non-replicated VSR request"
                );
            }
        },
    }
}

/// Run the consensus `Register` proposal on the metadata owner (shard 0)
/// and return the committed session.
///
/// Credential verification and session binding stay on the calling (home)
/// shard -- only this consensus step must execute where the metadata
/// consensus group lives. On shard 0 it calls in-process directly; on a
/// peer it forwards a [`shard::MetadataSubmit`] to shard 0 and awaits the
/// committed op. A dropped reply (shard-0 inbox full / shutdown) maps to a
/// transient `Canceled`, which the caller wraps so the SDK replays.
#[allow(clippy::future_not_send)]
async fn submit_register_on_owner(
    shard: &Rc<ServerNgShard>,
    vsr_client_id: u128,
) -> Result<u64, RegisterSubmitError> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_register_in_process(vsr_client_id)
            .await;
    }
    let (reply, rx) = shard::channel::<Option<u64>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Register {
        vsr_client_id,
        reply,
    });
    match rx.recv().await {
        Ok(Some(session)) => Ok(session),
        _ => Err(RegisterSubmitError::Canceled),
    }
}

/// Logout counterpart of [`submit_register_on_owner`].
#[allow(clippy::future_not_send)]
async fn submit_logout_on_owner(
    shard: &Rc<ServerNgShard>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, RegisterSubmitError> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_logout_in_process(vsr_client_id, session, request)
            .await;
    }
    let (reply, rx) = shard::channel::<Option<u64>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Logout {
        vsr_client_id,
        session,
        request,
        reply,
    });
    match rx.recv().await {
        Ok(Some(commit)) => Ok(commit),
        _ => Err(RegisterSubmitError::Canceled),
    }
}

/// Submit a replicated client request to the metadata owner (shard 0) and
/// return the committed reply.
///
/// The metadata consensus group lives on shard 0, but the connection lives
/// on the home shard (this shard). Run consensus where it belongs and bring
/// the committed reply back here so the caller can write it to the
/// originating socket -- shard 0 cannot route the reply by the consensus
/// `client` id (it's the VSR id, not the transport/home-shard-encoding id).
/// `None` = transient submit failure (SDK read-timeout replays).
#[allow(clippy::future_not_send)]
async fn submit_client_request_on_owner(
    shard: &Rc<ServerNgShard>,
    request: Message<RequestHeader>,
) -> Option<Message<GenericHeader>> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_request_in_process(request)
            .await
            .ok()
            .map(server_common::Message::into_generic);
    }
    let (reply, rx) = shard::channel::<Option<Message<GenericHeader>>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::ClientRequest {
        request: request.into_generic(),
        reply,
    });
    rx.recv().await.ok().flatten()
}

#[allow(clippy::future_not_send)]
async fn handle_logout_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    let Some((vsr_client_id, session)) = sessions.borrow().get_session(transport_client_id) else {
        warn!(
            transport_client_id,
            "dropping logout for unbound VSR session"
        );
        return;
    };

    let request_id = request.header().request;
    let commit = match submit_logout_on_owner(shard, vsr_client_id, session, request_id).await {
        Ok(commit) => commit,
        Err(error) => {
            warn!(transport_client_id, error = %error, "logout/unregister failed");
            return;
        }
    };

    sessions.borrow_mut().remove_connection(transport_client_id);

    let reply = build_empty_reply(request.header(), vsr_client_id, session, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send logout reply"
        );
    }
}

fn ensure_transport_connection(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) {
    let Some(meta) = shard.bus.client_meta(transport_client_id) else {
        return;
    };
    sessions
        .borrow_mut()
        .ensure_connection(transport_client_id, meta.peer_addr);
}

fn maybe_rewrite_pat_request(
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) -> Result<(Message<RequestHeader>, Option<String>), IggyError> {
    let operation = request.header().operation;
    let user_id = match operation {
        Operation::CreatePersonalAccessToken | Operation::DeletePersonalAccessToken => sessions
            .borrow()
            .get_user_id(transport_client_id)
            .ok_or(IggyError::Unauthenticated)?,
        _ => return Ok((request, None)),
    };

    let body = request_body(&request);
    let mut raw_token = None;
    let rewritten = match operation {
        Operation::CreatePersonalAccessToken => {
            let wire = WireCreatePersonalAccessTokenRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            // Primary mints the raw token + hash here and ships the hash
            // through consensus. Replicas decode the hash directly. Doing
            // this inside `CreatePersonalAccessTokenRequest::apply` would
            // call `ring::rand` per-replica and diverge state. The raw token
            // is returned to this client only (see `handle_client_request`).
            let (raw, token_hash) = mint_pat_raw_and_hash();
            raw_token = Some(raw);
            ReplicatedCreatePersonalAccessTokenRequest {
                user_id,
                name: wire.name,
                expiry: wire.expiry,
                token_hash,
            }
            .to_bytes()
        }
        Operation::DeletePersonalAccessToken => {
            let wire = WireDeletePersonalAccessTokenRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            ReplicatedDeletePersonalAccessTokenRequest {
                user_id,
                name: wire.name,
            }
            .to_bytes()
        }
        _ => unreachable!(),
    };

    Ok((rewrite_request_body(&request, &rewritten)?, raw_token))
}

/// Mints a fresh PAT and returns the raw token plus its hex-encoded SHA-256
/// hash (64 bytes ASCII). Only the hash is replicated; the raw token is
/// returned to the minting client by the home shard (it cannot be reproduced
/// by the deterministic `apply` running on every replica).
fn mint_pat_raw_and_hash() -> (String, [u8; 64]) {
    let (raw, hash) = iggy_common::PersonalAccessToken::mint_raw_and_hash();
    let bytes = hash.as_bytes();
    let mut out = [0u8; 64];
    let len = bytes.len().min(64);
    out[..len].copy_from_slice(&bytes[..len]);
    (raw, out)
}

fn rewrite_request_body(
    request: &Message<RequestHeader>,
    body: &Bytes,
) -> Result<Message<RequestHeader>, IggyError> {
    let total_size = std::mem::size_of::<RequestHeader>()
        .checked_add(body.len())
        .ok_or(IggyError::InvalidConfiguration)?;
    let size = u32::try_from(total_size).map_err(|_| IggyError::InvalidConfiguration)?;
    let mut rewritten = Message::<RequestHeader>::new(total_size);
    let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
        &mut rewritten.as_mut_slice()[..std::mem::size_of::<RequestHeader>()],
    )
    .expect("zeroed bytes are a valid request header");
    *header = *request.header();
    header.size = size;
    rewritten.as_mut_slice()[std::mem::size_of::<RequestHeader>()..].copy_from_slice(body);
    // TODO(vsr): the body changed but `request_checksum` / `checksum` /
    // `checksum_body` were copied verbatim from the original header. Safe
    // today because the SDK initializes `request_checksum` to 0 and the
    // server does not validate it; the moment integrity checking lands,
    // recompute these here (or zero them and re-sign in a follow-up step).
    Ok(rewritten)
}

#[allow(clippy::future_not_send)]
async fn handle_login_register_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    let body = request_body(&request);
    let vsr_client_id = request.header().client;

    if let Ok(wire_request) = LoginRegisterRequest::decode_from(body) {
        match verify_login_credentials(
            shard,
            wire_request.username.as_str(),
            wire_request.password.expose_secret(),
        ) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                )
                .await
                {
                    warn!(transport_client_id, error = %error, "login/register failed");
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(LoginRegisterError::InvalidCredentials) => {
                // Fall through to PAT attempt so a credential payload that
                // collides with a valid PAT payload shape still gets a
                // chance; if PAT also rejects, the final fall-through emits
                // the empty-reply failure path below.
            }
            Err(error) => {
                warn!(transport_client_id, error = %error, "login/register failed");
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    if let Ok(wire_request) = LoginRegisterWithPatRequest::decode_from(body) {
        match verify_pat_credentials(shard, wire_request.token.expose_secret()) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                )
                .await
                {
                    warn!(
                        transport_client_id,
                        error = %error,
                        "login/register with PAT failed"
                    );
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    "login/register with PAT failed"
                );
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    warn!(
        transport_client_id,
        "dropping register request with unsupported payload shape"
    );
    send_login_failure_reply(shard, transport_client_id, request.header()).await;
}

/// Decide whether a failed login/register gets a terminal reply or silence.
///
/// `Transient` / `InvalidClientId` are `NotEvictable` (see
/// [`LoginRegisterError`]'s `TryFrom` for `EvictionReason`): the cluster
/// could not commit *right now* (e.g. a freshly booted primary still
/// catching up, or a cross-shard submit canceled). Staying silent lets the
/// SDK read-timeout replay -- a later attempt lands once the primary is
/// caught up. Replying empty here would instead surface as a hard
/// `InvalidFormat` decode failure and break the replay.
///
/// Terminal auth errors (`InvalidCredentials` / `InvalidToken` /
/// `UserInactive` / `Session`) map to an `EvictionReason`, so we fast-fail
/// with an empty reply rather than make the client wait for a timeout.
/// (TODO: ship a typed `Eviction` frame once the SDK eviction decoder lands
/// on every transport.)
#[allow(clippy::future_not_send)]
async fn surface_login_failure(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request_header: &RequestHeader,
    error: &LoginRegisterError,
) {
    if EvictionReason::try_from(error).is_ok() {
        send_login_failure_reply(shard, transport_client_id, request_header).await;
    }
}

/// Empty Reply on a terminal failed Register. The SDK only decodes
/// `Command2::Reply`; an empty body fails `LoginRegisterResponse` decoding
/// fast instead of hanging until the socket read timeout. Only call for
/// terminal errors -- see [`surface_login_failure`].
#[allow(clippy::future_not_send)]
async fn send_login_failure_reply(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request_header: &RequestHeader,
) {
    let commit = current_metadata_commit(shard);
    let reply = build_empty_reply(request_header, transport_client_id, 0, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send login-failure reply"
        );
    }
}

fn request_body(request: &Message<RequestHeader>) -> &[u8] {
    &request.as_slice()[std::mem::size_of::<RequestHeader>()..request.header().size as usize]
}

fn resolve_partition_request_namespace(
    shard: &Rc<ServerNgShard>,
    operation: Operation,
    body: &[u8],
) -> Result<u64, IggyError> {
    let namespace = match operation {
        Operation::SendMessages => {
            if body.len() < 4 {
                return Err(IggyError::InvalidCommand);
            }
            let metadata_length = u32::from_le_bytes(
                body[..4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if body.len() < 4 + metadata_length {
                return Err(IggyError::InvalidCommand);
            }
            let header = SendMessagesHeader::decode_from(&body[4..4 + metadata_length])
                .map_err(|_| IggyError::InvalidCommand)?;
            resolve_send_messages_namespace(shard, &header)?
        }
        Operation::StoreConsumerOffset => {
            let request = StoreConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            resolve_partition_namespace(
                shard,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
            )?
        }
        Operation::DeleteConsumerOffset => {
            let request = DeleteConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            resolve_partition_namespace(
                shard,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
            )?
        }
        Operation::StoreConsumerOffset2 => {
            let request = StoreConsumerOffset2Request::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            resolve_partition_namespace(
                shard,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
            )?
        }
        Operation::DeleteConsumerOffset2 => {
            let request = DeleteConsumerOffset2Request::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            resolve_partition_namespace(
                shard,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
            )?
        }
        Operation::DeleteSegments => {
            let request =
                DeleteSegmentsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            resolve_partition_namespace(
                shard,
                &request.stream_id,
                &request.topic_id,
                Some(request.partition_id),
            )?
        }
        _ => return Err(IggyError::FeatureUnavailable),
    };
    Ok(namespace.inner())
}

fn resolve_send_messages_namespace(
    shard: &Rc<ServerNgShard>,
    header: &SendMessagesHeader,
) -> Result<IggyNamespace, IggyError> {
    let WirePartitioning::PartitionId(partition_id) = header.partitioning else {
        return Err(IggyError::FeatureUnavailable);
    };
    resolve_partition_namespace(
        shard,
        &header.stream_id,
        &header.topic_id,
        Some(partition_id),
    )
}

fn resolve_partition_namespace(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
) -> Result<IggyNamespace, IggyError> {
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .namespace_from_partition(stream_id, topic_id, partition_id)
        .ok_or(IggyError::InvalidIdentifier)
}

fn build_non_replicated_response(
    shard: &Rc<ServerNgShard>,
    code: u32,
    body: &[u8],
) -> Result<NonReplicatedResponse, IggyError> {
    match code {
        GET_CLUSTER_METADATA_CODE => Ok(NonReplicatedResponse::Bytes(
            build_cluster_metadata_response().to_bytes(),
        )),
        GET_STATS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_stats_response(shard)?.to_bytes(),
        )),
        GET_STREAM_CODE => {
            let request =
                GetStreamRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            build_get_stream_response(shard, &request.stream_id).map(|response| {
                response.map_or(NonReplicatedResponse::Empty, |response| {
                    NonReplicatedResponse::Bytes(response.to_bytes())
                })
            })
        }
        GET_STREAMS_CODE => {
            let _ = GetStreamsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            Ok(NonReplicatedResponse::Bytes(
                build_get_streams_response(shard)?.to_bytes(),
            ))
        }
        GET_TOPIC_CODE => {
            let request =
                GetTopicRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            build_get_topic_response(shard, &request.stream_id, &request.topic_id).map(|response| {
                response.map_or(NonReplicatedResponse::Empty, |response| {
                    NonReplicatedResponse::Bytes(response.to_bytes())
                })
            })
        }
        GET_TOPICS_CODE => {
            let request =
                GetTopicsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            Ok(NonReplicatedResponse::Bytes(
                build_get_topics_response(shard, &request.stream_id)?.to_bytes(),
            ))
        }
        GET_USERS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_get_users_response(shard)?.to_bytes(),
        )),
        GET_USER_CODE => {
            let request =
                GetUserRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            build_get_user_response(shard, &request.user_id).map(|response| {
                response.map_or(NonReplicatedResponse::Empty, |response| {
                    NonReplicatedResponse::Bytes(response.to_bytes())
                })
            })
        }
        POLL_MESSAGES_CODE => {
            let request =
                PollMessagesRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            Ok(NonReplicatedResponse::EmptyPolledMessages {
                partition_id: request.partition_id.unwrap_or(0),
            })
        }
        _ => match iggy_binary_protocol::dispatch::lookup_command(code) {
            Some(meta) if !meta.is_replicated() => Ok(NonReplicatedResponse::Empty),
            Some(_) => Err(IggyError::FeatureUnavailable),
            None => Err(IggyError::InvalidCommand),
        },
    }
}

fn build_cluster_metadata_response() -> ClusterMetadataResponse {
    ClusterMetadataResponse {
        name: "server-ng".to_owned(),
        nodes: vec![ClusterNodeResponse {
            name: "node-0".to_owned(),
            ip: "127.0.0.1".to_owned(),
            tcp_port: 0,
            quic_port: 0,
            http_port: 0,
            websocket_port: 0,
            role: 0,
            status: 0,
        }],
    }
}

fn build_stats_response(shard: &Rc<ServerNgShard>) -> Result<StatsResponse, IggyError> {
    let (streams_count, topics_count, partitions_count, messages_size_bytes, messages_count) =
        shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|streams| -> Result<_, IggyError> {
                let mut topics_count = 0u32;
                let mut partitions_count = 0u32;
                let mut messages_size_bytes = 0u64;
                let mut messages_count = 0u64;
                for (_, stream) in &streams.items {
                    topics_count = topics_count.saturating_add(usize_to_u32(stream.topics.len())?);
                    messages_size_bytes =
                        messages_size_bytes.saturating_add(stream.stats.size_bytes_inconsistent());
                    messages_count =
                        messages_count.saturating_add(stream.stats.messages_count_inconsistent());
                    for (_, topic) in &stream.topics {
                        partitions_count =
                            partitions_count.saturating_add(usize_to_u32(topic.partitions.len())?);
                    }
                }
                Ok((
                    usize_to_u32(streams.items.len())?,
                    topics_count,
                    partitions_count,
                    messages_size_bytes,
                    messages_count,
                ))
            })?;
    let consumer_groups_count = shard
        .plane
        .metadata()
        .mux_stm
        .consumer_groups()
        .read(|groups| usize_to_u32(groups.items.len()))?;

    Ok(StatsResponse {
        process_id: std::process::id(),
        cpu_usage: 0.0,
        total_cpu_usage: 0.0,
        memory_usage: 0,
        total_memory: 0,
        available_memory: 0,
        run_time: 0,
        start_time: 0,
        read_bytes: 0,
        written_bytes: 0,
        messages_size_bytes,
        streams_count,
        topics_count,
        partitions_count,
        segments_count: 0,
        messages_count,
        clients_count: 0,
        consumer_groups_count,
        hostname: "unknown_hostname".to_owned(),
        os_name: "unknown_os_name".to_owned(),
        os_version: "unknown_os_version".to_owned(),
        kernel_version: "unknown_kernel_version".to_owned(),
        iggy_server_version: server::VERSION.to_owned(),
        iggy_server_semver: server::SEMANTIC_VERSION.get_numeric_version().ok(),
        cache_metrics: Vec::new(),
        threads_count: 0,
        free_disk_space: 0,
        total_disk_space: 0,
    })
}

fn build_get_stream_response(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
) -> Result<Option<GetStreamResponse>, IggyError> {
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let Some(stream_id) = resolve_stream_id(streams, stream_id) else {
            return Ok(None);
        };
        let stream = streams
            .items
            .get(stream_id)
            .ok_or(IggyError::InvalidIdentifier)?;
        Ok(Some(GetStreamResponse {
            stream: stream_response(stream)?,
            topics: stream
                .topics
                .iter()
                .map(|(_, topic)| topic_header(topic))
                .collect::<Result<Vec<_>, _>>()?,
        }))
    })
}

fn build_get_streams_response(shard: &Rc<ServerNgShard>) -> Result<GetStreamsResponse, IggyError> {
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        streams
            .items
            .iter()
            .map(|(_, stream)| stream_response(stream))
            .collect::<Result<Vec<_>, _>>()
            .map(|streams| GetStreamsResponse { streams })
    })
}

#[allow(clippy::cast_possible_truncation)]
fn user_response(user: &metadata::stm::user::User) -> Result<UserResponse, IggyError> {
    Ok(UserResponse {
        id: user.id,
        created_at: user.created_at.as_micros(),
        status: user.status.as_code(),
        username: WireName::new(user.username.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
    })
}

fn build_get_users_response(shard: &Rc<ServerNgShard>) -> Result<GetUsersResponse, IggyError> {
    shard.plane.metadata().mux_stm.users().read(|users| {
        users
            .items
            .iter()
            .map(|(_, user)| user_response(user))
            .collect::<Result<Vec<_>, _>>()
            .map(|users| GetUsersResponse { users })
    })
}

fn build_get_user_response(
    shard: &Rc<ServerNgShard>,
    user_id: &WireIdentifier,
) -> Result<Option<UserDetailsResponse>, IggyError> {
    shard.plane.metadata().mux_stm.users().read(|users| {
        let resolved = match user_id {
            WireIdentifier::Numeric(id) => {
                let id = *id as usize;
                users.items.contains(id).then_some(id)
            }
            WireIdentifier::String(name) => users.index.get(name.as_str()).map(|&id| id as usize),
        };
        let Some(id) = resolved else {
            return Ok(None);
        };
        let user = users.items.get(id).ok_or(IggyError::InvalidIdentifier)?;
        Ok(Some(UserDetailsResponse {
            user: user_response(user)?,
            permissions: user
                .permissions
                .as_ref()
                .map(|p| iggy_common::wire_conversions::permissions_to_wire(p)),
        }))
    })
}

fn build_get_topic_response(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Result<Option<GetTopicResponse>, IggyError> {
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let Some(stream_id) = resolve_stream_id(streams, stream_id) else {
            return Ok(None);
        };
        let Some(topic_id) = resolve_topic_id(streams, stream_id, topic_id) else {
            return Ok(None);
        };
        let stream = streams
            .items
            .get(stream_id)
            .ok_or(IggyError::InvalidIdentifier)?;
        let topic = stream
            .topics
            .get(topic_id)
            .ok_or(IggyError::InvalidIdentifier)?;
        Ok(Some(GetTopicResponse {
            topic: topic_header(topic)?,
            partitions: topic
                .partitions
                .iter()
                .map(partition_response)
                .collect::<Result<Vec<_>, _>>()?,
        }))
    })
}

fn build_get_topics_response(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
) -> Result<GetTopicsResponse, IggyError> {
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let Some(stream_id) = resolve_stream_id(streams, stream_id) else {
            return Ok(GetTopicsResponse { topics: Vec::new() });
        };
        let stream = streams
            .items
            .get(stream_id)
            .ok_or(IggyError::InvalidIdentifier)?;
        stream
            .topics
            .iter()
            .map(|(_, topic)| topic_header(topic))
            .collect::<Result<Vec<_>, _>>()
            .map(|topics| GetTopicsResponse { topics })
    })
}

fn resolve_stream_id(
    streams: &metadata::stm::stream::StreamsInner,
    identifier: &WireIdentifier,
) -> Option<usize> {
    match identifier {
        WireIdentifier::Numeric(id) => {
            let id = *id as usize;
            streams.items.contains(id).then_some(id)
        }
        WireIdentifier::String(name) => streams.index.get(name.as_str()).copied(),
    }
}

fn resolve_topic_id(
    streams: &metadata::stm::stream::StreamsInner,
    stream_id: usize,
    identifier: &WireIdentifier,
) -> Option<usize> {
    let stream = streams.items.get(stream_id)?;
    match identifier {
        WireIdentifier::Numeric(id) => {
            let id = *id as usize;
            stream.topics.contains(id).then_some(id)
        }
        WireIdentifier::String(name) => stream.topic_index.get(name.as_str()).copied(),
    }
}

fn stream_response(stream: &metadata::stm::stream::Stream) -> Result<StreamResponse, IggyError> {
    Ok(StreamResponse {
        id: usize_to_u32(stream.id)?,
        created_at: stream.created_at.as_micros(),
        topics_count: usize_to_u32(stream.topics.len())?,
        size_bytes: stream.stats.size_bytes_inconsistent(),
        messages_count: stream.stats.messages_count_inconsistent(),
        name: WireName::new(stream.name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
    })
}

fn topic_header(topic: &metadata::stm::stream::Topic) -> Result<StreamTopicHeader, IggyError> {
    Ok(StreamTopicHeader {
        id: usize_to_u32(topic.id)?,
        created_at: topic.created_at.as_micros(),
        partitions_count: usize_to_u32(topic.partitions.len())?,
        message_expiry: u64::from(topic.message_expiry),
        compression_algorithm: topic.compression_algorithm.as_code(),
        max_topic_size: topic.max_topic_size.as_bytes_u64(),
        replication_factor: topic.replication_factor,
        size_bytes: topic.stats.size_bytes_inconsistent(),
        messages_count: topic.stats.messages_count_inconsistent(),
        name: WireName::new(topic.name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
    })
}

fn partition_response(
    partition: &metadata::stm::stream::Partition,
) -> Result<PartitionResponse, IggyError> {
    Ok(PartitionResponse {
        id: usize_to_u32(partition.id)?,
        created_at: partition.created_at.as_micros(),
        segments_count: 0,
        current_offset: 0,
        size_bytes: 0,
        messages_count: 0,
    })
}

fn usize_to_u32(value: usize) -> Result<u32, IggyError> {
    u32::try_from(value).map_err(|_| IggyError::InvalidIdentifier)
}

fn verify_login_credentials(
    shard: &Rc<ServerNgShard>,
    username: &str,
    password: &str,
) -> Result<u32, LoginRegisterError> {
    shard.plane.metadata().mux_stm.users().read(|users| {
        let Some(user_id) = users.index.get(username).copied() else {
            return Err(LoginRegisterError::InvalidCredentials);
        };
        let Some(user) = users.items.get(user_id as usize) else {
            return Err(LoginRegisterError::InvalidCredentials);
        };
        if user.status != UserStatus::Active {
            return Err(LoginRegisterError::UserInactive);
        }
        if !crypto::verify_password(password, user.password_hash.as_ref()) {
            return Err(LoginRegisterError::InvalidCredentials);
        }
        Ok(user.id)
    })
}

fn verify_pat_credentials(
    shard: &Rc<ServerNgShard>,
    token: &str,
) -> Result<u32, LoginRegisterError> {
    let token_hash = PersonalAccessToken::hash_token(token);
    let now = IggyTimestamp::now();
    shard.plane.metadata().mux_stm.users().read(|users| {
        let Some((user_id, token_name)) =
            users.personal_access_token_index.get(token_hash.as_str())
        else {
            return Err(LoginRegisterError::InvalidToken);
        };
        let Some(pat) = users
            .personal_access_tokens
            .get(user_id)
            .and_then(|tokens| tokens.get(token_name))
        else {
            return Err(LoginRegisterError::InvalidToken);
        };
        if pat.is_expired(now) {
            return Err(LoginRegisterError::InvalidToken);
        }
        let Some(user) = users.items.get(*user_id as usize) else {
            return Err(LoginRegisterError::InvalidToken);
        };
        if user.status != UserStatus::Active {
            return Err(LoginRegisterError::UserInactive);
        }
        Ok(user.id)
    })
}

#[allow(clippy::future_not_send)]
async fn complete_login_register(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    vsr_client_id: u128,
    request_header: &RequestHeader,
    user_id: u32,
) -> Result<(), LoginRegisterError> {
    let existing_session = {
        let sessions = sessions.borrow();
        sessions
            .get_session(transport_client_id)
            .map(|(_, session)| session)
    };
    if let Some(session) = existing_session {
        let commit = current_metadata_commit(shard);
        let reply =
            build_login_register_reply(request_header, vsr_client_id, session, commit, user_id);
        let _ = shard
            .bus
            .send_to_client(transport_client_id, reply.into_generic().into_frozen())
            .await;
        return Ok(());
    }

    {
        let mut sessions = sessions.borrow_mut();
        sessions
            .login(transport_client_id, user_id)
            .map_err(LoginRegisterError::Session)?;
    }

    let session = match submit_register_on_owner(shard, vsr_client_id).await {
        Ok(session) => session,
        Err(error) => {
            let _ = sessions
                .borrow_mut()
                .reset_to_connected(transport_client_id);
            return Err(LoginRegisterError::Transient(error));
        }
    };

    {
        let mut sessions = sessions.borrow_mut();
        if let Err(error) = sessions.bind_session(transport_client_id, vsr_client_id, session) {
            // No local rollback: `submit_register_in_process` above has
            // already committed cluster-wide. A local-only
            // `remove_client_session` here would diverge peers (they retain
            // the slot until they evict the client themselves). The
            // transport-disconnect callback owns local cleanup once the
            // socket closes.
            return Err(LoginRegisterError::Session(error));
        }
    }

    let commit = current_metadata_commit(shard);
    let reply = build_login_register_reply(request_header, vsr_client_id, session, commit, user_id);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send login/register reply"
        );
    }

    Ok(())
}

fn ensure_default_root_user(mux_stm: &ServerNgMuxStateMachine) {
    if !mux_stm.users().read(|users| users.items.is_empty()) {
        return;
    }

    let LegacyUser {
        username, password, ..
    } = server::bootstrap::create_root_user();
    mux_stm.users().ensure_root_user(&username, &password);
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

enum NonReplicatedResponse {
    Empty,
    EmptyPolledMessages { partition_id: u32 },
    Bytes(Bytes),
}

impl NonReplicatedResponse {
    fn into_reply(
        self,
        request_header: &RequestHeader,
        client_id: u128,
        session: u64,
        commit: u64,
    ) -> Message<ReplyHeader> {
        match self {
            Self::Empty => build_empty_reply(request_header, client_id, session, commit),
            Self::EmptyPolledMessages { partition_id } => {
                build_reply_with_body(request_header, client_id, session, commit, 16, |body| {
                    body[..4].copy_from_slice(&partition_id.to_le_bytes());
                    body[4..12].copy_from_slice(&0u64.to_le_bytes());
                    body[12..16].copy_from_slice(&0u32.to_le_bytes());
                })
            }
            Self::Bytes(body) => {
                build_reply_from_bytes(request_header, client_id, session, commit, &body)
            }
        }
    }
}

fn build_empty_reply(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
) -> Message<ReplyHeader> {
    build_reply_with_body(request_header, client_id, session, commit, 0, |_| {})
}

fn build_login_register_reply(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    user_id: u32,
) -> Message<ReplyHeader> {
    build_reply_with_body(request_header, client_id, session, commit, 12, |body| {
        body[..4].copy_from_slice(&user_id.to_le_bytes());
        body[4..12].copy_from_slice(&session.to_le_bytes());
    })
}

fn build_reply_from_bytes(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    body: &Bytes,
) -> Message<ReplyHeader> {
    build_reply_with_body(
        request_header,
        client_id,
        session,
        commit,
        body.len(),
        |out| out.copy_from_slice(body),
    )
}

/// If a raw PAT token was minted (`CreatePersonalAccessToken`), replace the
/// committed reply -- whose body is empty because the raw token never entered
/// consensus -- with a `RawPersonalAccessTokenResponse`, reusing the confirmed
/// commit position from the committed reply. Otherwise the committed reply
/// passes through unchanged.
fn build_raw_pat_reply(
    request_header: &RequestHeader,
    committed: Message<GenericHeader>,
    raw_token: Option<String>,
) -> Result<Message<GenericHeader>, IggyError> {
    let Some(raw) = raw_token else {
        return Ok(committed);
    };
    let header_len = std::mem::size_of::<ReplyHeader>();
    let committed_header =
        bytemuck::checked::try_from_bytes::<ReplyHeader>(&committed.as_slice()[..header_len])
            .map_err(|_| IggyError::InvalidFormat)?;
    let commit = committed_header.commit;
    let token = WireName::new(raw.as_str()).map_err(|_| IggyError::InvalidFormat)?;
    let body = RawPersonalAccessTokenResponse { token }.to_bytes();
    let reply = build_reply_from_bytes(
        request_header,
        request_header.client,
        request_header.session,
        commit,
        &body,
    );
    Ok(reply.into_generic())
}

fn build_reply_with_body(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    body_len: usize,
    write_body: impl FnOnce(&mut [u8]),
) -> Message<ReplyHeader> {
    let header_len = std::mem::size_of::<ReplyHeader>();
    let total_size = header_len + body_len;
    let mut reply = Message::<ReplyHeader>::new(total_size);
    let header_size = u32::try_from(total_size).expect("reply size must fit into u32");
    let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
        &mut reply.as_mut_slice()[..header_len],
    )
    .expect("zeroed bytes are valid");
    *header = ReplyHeader {
        cluster: request_header.cluster,
        size: header_size,
        view: request_header.view,
        release: request_header.release,
        command: Command2::Reply,
        replica: request_header.replica,
        request_checksum: request_header.request_checksum,
        client: client_id,
        op: session,
        commit,
        timestamp: request_header.timestamp,
        request: request_header.request,
        operation: request_header.operation,
        namespace: request_header.namespace,
        ..Default::default()
    };
    write_body(&mut reply.as_mut_slice()[header_len..total_size]);
    reply
}

fn current_metadata_commit(shard: &Rc<ServerNgShard>) -> u64 {
    shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map_or(0, VsrConsensus::commit_max)
}

fn upgrade_shard_handle(shard_handle: &ServerNgShardHandle) -> Option<Rc<ServerNgShard>> {
    shard_handle
        .borrow()
        .as_ref()
        .and_then(std::rc::Weak::upgrade)
}

/// Replica accept callback that ships every inbound connection through
/// the shard-0 coordinator's round-robin fd-delegation. The fd lands on
/// the target shard's inbox as a [`shard::LifecycleFrame::ReplicaConnectionSetup`]
/// frame and is installed on that shard's bus.
fn make_delegating_replica_accept_fn(
    coord: Rc<shard::coordinator::ShardZeroCoordinator>,
) -> AcceptedReplicaFn {
    Rc::new(
        move |stream, peer_id| match coord.delegate_replica(stream, peer_id) {
            Ok(target) => {
                info!(peer_id, target, "replica connection delegated");
            }
            Err(error) => {
                warn!(
                    peer_id,
                    error = ?error,
                    "delegate_replica failed; dropping inbound replica connection"
                );
            }
        },
    )
}

/// Shard-0 client accept callbacks. TCP and WS clients are delegated via
/// the coordinator (round-robin to peer shards); QUIC and TCP-TLS install
/// locally on shard 0 because their per-connection state is not portable
/// across shards (`compio_quic` endpoint binds one UDP socket; rustls TLS
/// state ties to the post-handshake reactor).
fn make_shard_zero_client_accept_fns(
    coord: Rc<shard::coordinator::ShardZeroCoordinator>,
    bus: &Rc<IggyMessageBus>,
    on_request: RequestHandler,
) -> LocalClientAcceptFns {
    let quic_bus = Rc::clone(bus);
    let tcp_tls_bus = Rc::clone(bus);
    let quic_request = on_request.clone();
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

    let tcp_tls_coord = coord;
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

    LocalClientAcceptFns {
        tcp,
        ws,
        quic,
        tcp_tls,
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
        bound.ws = Some(bound_addr);
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
            .unwrap_or_else(|e| std::panic::resume_unwind(e))
            .expect_err("shutdown flag must abort the bundle wait");
        assert!(
            matches!(err, ServerNgError::MetadataHandoffAborted { shard_id: 1 }),
            "expected MetadataHandoffAborted on shutdown, got {err:?}"
        );
    }
}
