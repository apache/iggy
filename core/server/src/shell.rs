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

//! The shell vocabulary.
//!
//! The shard/metadata type aliases the dispatch layer is generic over, the
//! [`ShellBus`] bound, the [`ShellHandlers`] slot struct, and the
//! `[cluster]` timer-to-tick translation every consensus group boots with.
//! Everything here is type- and config-level; construction (wiring the
//! handlers against a live bus) stays in [`crate::bootstrap`].

use crate::session_manager::SessionManager;
use configs::server::ServerConfig;
use consensus::{ConsensusTimers, VsrConsensus};
use iggy_common::variadic;
use journal::prepare_journal::PrepareJournal;
use journal::superblock::PingPongSuperblock;
use message_bus::client_listener::RequestHandler;
use message_bus::replica::listener::MessageHandler;
use message_bus::{ConnectionInstaller, IggyMessageBus, MessageBus};
use metadata::IggyMetadata;
use metadata::MuxStateMachine;
use metadata::impls::metadata::IggySnapshot;
use metadata::stm::mux::WithFactory;
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use shard::shards_table::PapayaShardsTable;
use shard::{IggyShard, ListClientsHandler, MetadataSubmitHandler, PartitionReadHandler};
use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::time::Duration;

pub(crate) type ServerMuxStateMachine = MuxStateMachine<variadic!(Users, Streams)>;

/// Cross-thread bundle carrying one `ReadHandleFactory` per metadata
/// state. Shard 0 mints one after `recover()` and broadcasts a clone to
/// every peer shard; each peer rebuilds a reader-mode
/// [`ServerMuxStateMachine`] on its own runtime, skipping the WAL.
pub(crate) type ServerMetadataBundle = <variadic!(Users, Streams) as WithFactory>::Bundle;

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
/// Both production (`build_shard_for_thread`) and the simulator's shell
/// mode construct these through [`crate::bootstrap::wire_shell_handlers`],
/// so the request plane is wired one way. The simulator's shell-off fast
/// path uses [`ShellHandlers::noop`] instead.
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

pub type ServerShard = ShellShard<Rc<IggyMessageBus>, PrepareJournal, IggySnapshot>;

/// Convert a consensus-timer interval to whole ticks, floored at one tick so a
/// sub-tick value still fires and saturated on overflow.
fn duration_to_ticks(interval: Duration) -> u64 {
    let ticks = interval.as_millis() / shard::CONSENSUS_TICK_INTERVAL.as_millis();
    u64::try_from(ticks.max(1)).unwrap_or(u64::MAX)
}

/// `[cluster] heartbeat_timeout` in consensus ticks. Every consensus group
/// (metadata and per-partition planes alike) gets the same window: the failure
/// it guards against - a primary that stopped heartbeating - is host-level, not
/// per-plane.
pub(crate) fn cluster_heartbeat_ticks(config: &ServerConfig) -> u64 {
    duration_to_ticks(config.cluster.heartbeat_timeout.get_duration())
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
