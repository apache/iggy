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

//! Per-shard frame-drop accounting.
//!
//! [`ShardMetrics`] holds `frame_drops_total{variant, reason}`, bumped wherever
//! a frame is shed instead of delivered -- an inter-shard `try_send` rejected
//! (`Full` / `Disconnected`), a target shard id out of range (`Unroutable`), or
//! a buffer at capacity (`ParkOverflow`):
//! - [`crate::coordinator::ShardZeroCoordinator`] - fd-transfer delegation.
//! - the cross-shard forward closures built in [`crate::builder`].
//! - `IggyShard::try_send_to_target` - consensus and partition frames, labelled
//!   by plane.
//! - `IggyShard::park_if_unmaterialised` - partition frames shed because the
//!   park buffer is at its frame or byte cap.
//! - `IggyShard::retire_parked_frames` - parked frames retired with no client
//!   to answer.
//!
//! The counter uses atomic interior mutability, safe to bump from `!Send`
//! compio reactor contexts. Each shard owns its own instance, and the server
//! exposes every shard's instance through the `[http.metrics]` scrape
//! endpoint via [`ShardMetrics::register`] (one `shard`-labelled
//! sub-registry per shard). Drop-site tracing supplements the counters.

use crate::geo_commits::CommitAnswer;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use consensus::ReplicationStats;
use iggy_binary_protocol::{COMMAND_TABLE, Command, command_index};
use iggy_common::ConsumerKind;
use message_bus::{ReplicaLinkMetrics, ReplicaReadMetrics};
use partitions::CheckpointTrigger;
use server_common::sharding::IggyNamespace;

/// Label for `frame_drops_total`.
///
/// `variant` describes the dropped frame class; `reason` is `"full"` or
/// `"disconnected"` per crossfire `TrySendError`, `"unroutable"` when the
/// target shard id has no sender slot, `"delivery_failed"` when the
/// receiver path could not place the frame, `"misrouted"` when a frame
/// reached a shard that does not own its namespace, or `"park_overflow"` when
/// an un-materialised namespace's park buffer was already at capacity.
///
/// `shard_id` is intentionally NOT a label here: each shard owns its own
/// `Family<FrameDropLabel, Counter>`, so the per-shard scope is implicit
/// in the registry the family is exported through. A future scrape
/// exporter must attach `shard_id` as a target label rather than as part
/// of the per-counter label set to keep cardinality bounded.
#[derive(Clone, Hash, Eq, PartialEq, EncodeLabelSet, Debug)]
pub struct FrameDropLabel {
    pub variant: &'static str,
    pub reason: &'static str,
}

#[derive(Clone, Hash, Eq, PartialEq, EncodeLabelSet, Debug)]
pub struct ConsumerOffsetKindLabel {
    pub kind: &'static str,
}

/// Label of the geo peer gauges: the origin name of the peer, as the
/// configuration gives it.
#[derive(Clone, Hash, Eq, PartialEq, EncodeLabelSet, Debug)]
pub struct GeoPeerLabel {
    pub origin: String,
}

#[derive(Clone, Hash, Eq, PartialEq, EncodeLabelSet, Debug)]
pub struct CheckpointTriggerLabel {
    pub trigger: &'static str,
}

/// Label of the geo peer request counters: the peer's origin and the
/// request, `list` or `get`.
#[derive(Clone, Hash, Eq, PartialEq, EncodeLabelSet, Debug)]
pub struct GeoPeerRequestLabel {
    pub origin: String,
    pub request: &'static str,
}

/// Label for `partition_frames_incarnation_mismatch_total`: the dropped
/// frame's [`Command`], spelled as the drop-site log prints it.
#[derive(Clone, Hash, Eq, PartialEq, EncodeLabelSet, Debug)]
pub struct FrameCommandLabel {
    pub command: CommandLabel,
}

/// A [`Command`] as a label value. It is spelled only when a scrape encodes
/// it, so counting a drop never allocates.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct CommandLabel(pub Command);

// `Command` derives no `Hash`; its discriminant identifies it.
impl Hash for CommandLabel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.0 as u8).hash(state);
    }
}

impl EncodeLabelValue for CommandLabel {
    fn encode(&self, encoder: &mut LabelValueEncoder<'_>) -> std::fmt::Result {
        write!(encoder, "{:?}", self.0)
    }
}

impl From<Command> for FrameCommandLabel {
    fn from(command: Command) -> Self {
        Self {
            command: CommandLabel(command),
        }
    }
}

/// What one partition that places its batches has yet to rewrite into
/// long-term segments, as the shard that leads it found at its last tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LongTermLag {
    pub namespace: IggyNamespace,
    /// Messages it committed that no long-term segment holds.
    pub messages: u64,
    /// Bytes of those messages, `None` until its replica counted them.
    pub bytes: Option<u64>,
}

/// The lags a shard reported last, and the list of the report before it,
/// which the next report fills so that a tick allocates none.
#[derive(Debug, Default)]
struct LongTermLags {
    reported: Vec<LongTermLag>,
    spare: Vec<LongTermLag>,
}

/// Variant labels used in `frame_drops_total`. Exposed as constants to
/// catch typos at compile time and to keep the cardinality bounded.
///
/// `FORWARD_CLIENT_SEND` ticks when the pump cannot send a deny a synchronous
/// caller staged by client id. The production bus refuses every frame sent by
/// id, so only a raw request without a reply channel ends up here.
/// `FORWARD_REPLICA_SEND` ticks when a replica forward fails. VSR retransmit
/// covers its loss, so it stays informational.
///
/// `PARTITION` covers the partition plane: a frame shed because the namespace
/// had not materialised and its park buffer was at capacity
/// (`reason=park_overflow`), a parked frame retired with no client to answer
/// (`reason=park_dropped`), an incarnation rejection, or a routing send the
/// target inbox refused. A shed client request is answered with a retriable
/// status, so the client recovers. A shed *prepare* has nobody to answer and is
/// not covered by retransmit once its op has reached quorum
/// (`consensus::prepare_timeout_targets` skips `ok_quorum_received`), so the backup
/// gap-stops; `tick_partitions`' sweep is what repairs it, escalating to
/// partition state transfer when the primary has evicted the range, and
/// `partition_prepare_gap_drops_total` is what counts the prepares that reached
/// the gap check. The counter therefore signals a data-plane gap or recovery
/// burden, not a requirement for a view change.
pub mod frame_drop_variant {
    pub const CONSENSUS: &str = "consensus";
    pub const FD_TRANSFER: &str = "fd_transfer";
    pub const PARTITION: &str = "partition";
    pub const FORWARD_CLIENT_SEND: &str = "forward_client_send";
    pub const FORWARD_REPLICA_SEND: &str = "forward_replica_send";
    pub const METADATA_COMMIT_TICK: &str = "metadata_commit_tick";
    pub const PARTITION_PLACEMENT_VIEWS: &str = "partition_placement_views";
    pub const PRIMARY_REBALANCE: &str = "primary_rebalance";
    /// A delegated replica handshake's outcome ack to shard 0 was
    /// dropped; the shard-0 deadline expiry recovers the slot / pending
    /// entry, so this stays informational.
    pub const REPLICA_HANDSHAKE_ACK: &str = "replica_handshake_ack";
    /// A replica link's close report to shard 0 was dropped; the next
    /// reconnect sweep finds the link missing and tears its bundle down.
    pub const REPLICA_LINK_CLOSED: &str = "replica_link_closed";
    /// Shard 0's request to close a link of a torn down bundle was
    /// dropped; the next bundle refresh sends it again.
    pub const REPLICA_LINK_CLOSE: &str = "replica_link_close";
    pub const PARTITION_PERSISTENCE_COMPLETED: &str = "partition_persistence_completed";
    /// A disk poll could not reserve completion capacity or return its result
    /// to the owning shard.
    pub const PARTITION_POLL_COMPLETION: &str = "partition_poll_completion";
    /// A reply to a client request found no connection: the installation that
    /// was serving the request when it arrived is gone, or its queue is full.
    /// The reply is never delivered by client id instead.
    pub const CLIENT_REPLY: &str = "client_reply";
}

/// Reason labels used in `frame_drops_total`.
///
/// `UNROUTABLE` ticks when a frame's target shard id has no sender slot
/// (`target >= senders.len()`). Unreachable while every shard seeds its
/// `ShardsTable` identically at boot, but `shard_for` returns a stored
/// `u16` so the index is guarded rather than trusted. `DELIVERY_FAILED`
/// is the receiver-side equivalent: the frame arrived at the owning shard
/// but the local registry refused it. `MISROUTED` ticks when the pump
/// receives a Consensus frame whose target shard is not `self.id`.
/// `PARK_OVERFLOW` ticks when a partition frame arrives for a namespace this
/// shard has not materialised and the per-namespace park buffer is already at
/// its cap, so the frame is shed with no reply. `PARK_DROPPED` ticks when a
/// frame that did park leaves unserved: its namespace became unreachable, its
/// incarnation stamp was rejected, reclassification failed, or a request
/// outlived `MAX_PARKED_PASSES` with no pump to take the deny. A request also
/// bumps `partition_requests_denied_transient_total` when answered; replicated
/// traffic has nobody to answer, so this is the direct record that local bytes
/// were destroyed and repair may be required.
pub mod frame_drop_reason {
    /// Operation discriminant unknown to this build: the sender runs another
    /// release.
    ///
    /// Distinct from `UNPARSABLE` because aligning the releases is the fix, and
    /// until they are aligned, a replication frame's consensus group gap-stops
    /// here.
    pub const UNSUPPORTED_OPERATION: &str = "unsupported_operation";
    /// A consensus frame failed typed decode for any other reason (corrupt
    /// header, bad size, client-bound command on the inbound path).
    pub const UNPARSABLE: &str = "unparsable";
    pub const FULL: &str = "full";
    pub const DISCONNECTED: &str = "disconnected";
    pub const UNROUTABLE: &str = "unroutable";
    pub const DELIVERY_FAILED: &str = "delivery_failed";
    pub const MISROUTED: &str = "misrouted";
    pub const PARK_OVERFLOW: &str = "park_overflow";
    pub const PARK_DROPPED: &str = "park_dropped";
    /// A partition write's reply channel was dropped before a reply arrived
    /// (view-change pipeline reset, park teardown, shutdown): the outcome is
    /// unknown and the client is left to its read-timeout.
    pub const SUBMIT_ABANDONED: &str = "submit_abandoned";
    /// A partition write's reply did not arrive within the submit budget.
    pub const SUBMIT_TIMEOUT: &str = "submit_timeout";
}

// The tables only index the lazy cache below. A `{variant, reason}`
// pair enters the `Family` (and therefore the scrape) the first time a drop
// site produces it, so unproduced combinations never enter the scrape.
const VARIANT_COUNT: usize = 10;
const REASON_COUNT: usize = 11;

const VARIANTS: [&str; VARIANT_COUNT] = [
    frame_drop_variant::CONSENSUS,
    frame_drop_variant::FD_TRANSFER,
    frame_drop_variant::PARTITION,
    frame_drop_variant::FORWARD_CLIENT_SEND,
    frame_drop_variant::FORWARD_REPLICA_SEND,
    frame_drop_variant::METADATA_COMMIT_TICK,
    frame_drop_variant::REPLICA_HANDSHAKE_ACK,
    frame_drop_variant::PARTITION_PERSISTENCE_COMPLETED,
    frame_drop_variant::PARTITION_POLL_COMPLETION,
    frame_drop_variant::CLIENT_REPLY,
];

const REASONS: [&str; REASON_COUNT] = [
    frame_drop_reason::UNSUPPORTED_OPERATION,
    frame_drop_reason::UNPARSABLE,
    frame_drop_reason::FULL,
    frame_drop_reason::DISCONNECTED,
    frame_drop_reason::UNROUTABLE,
    frame_drop_reason::DELIVERY_FAILED,
    frame_drop_reason::MISROUTED,
    frame_drop_reason::PARK_OVERFLOW,
    frame_drop_reason::PARK_DROPPED,
    frame_drop_reason::SUBMIT_ABANDONED,
    frame_drop_reason::SUBMIT_TIMEOUT,
];

fn variant_index(s: &str) -> Option<usize> {
    VARIANTS.iter().position(|v| *v == s)
}

fn reason_index(s: &str) -> Option<usize> {
    REASONS.iter().position(|r| *r == s)
}

#[derive(Clone, Default)]
pub(crate) struct DeferredPollMetrics {
    pub pending: Gauge,
    pub inflight_bytes: Gauge,
    pub rejected: Counter,
    pub failed: Counter,
    pub cancelled: Counter,
    pub expired: Counter,
    pub filled: Counter,
    pub late: Counter,
    pub readiness_pushes: Counter,
    pub readiness_watches: Gauge,
}

impl DeferredPollMetrics {
    fn register(&self, registry: &mut Registry) {
        registry.register(
            "deferred_poll_pending",
            "pending deferred polls",
            self.pending.clone(),
        );
        registry.register(
            "deferred_poll_inflight_bytes",
            "reserved bytes for running or queued deferred reads",
            self.inflight_bytes.clone(),
        );
        registry.register(
            "deferred_poll_rejected",
            "deferred polls rejected before admission",
            self.rejected.clone(),
        );
        registry.register(
            "deferred_poll_failed",
            "admitted deferred polls rejected by a read error, fence, full completion lane or request deadline",
            self.failed.clone(),
        );
        registry.register(
            "deferred_poll_cancelled",
            "deferred polls cancelled by their caller",
            self.cancelled.clone(),
        );
        registry.register(
            "deferred_poll_expired",
            "deferred polls completed at their deadline",
            self.expired.clone(),
        );
        registry.register(
            "deferred_poll_filled",
            "deferred polls answered before their wait deadline",
            self.filled.clone(),
        );
        registry.register(
            "deferred_poll_late",
            "deferred read completions discarded after expiry or cancellation",
            self.late.clone(),
        );
        registry.register(
            "deferred_poll_readiness_pushes",
            "partition states pushed to the shards that watch them for readiness waits",
            self.readiness_pushes.clone(),
        );
        registry.register(
            "deferred_poll_readiness_watches",
            "partitions of this shard that other shards watch for readiness waits",
            self.readiness_watches.clone(),
        );
    }
}

/// Per-shard metric handles.
///
/// Cheap to clone (`Arc` of a `Family` under the hood). Each shard owns
/// one instance produced by [`ShardMetrics::for_shard`]. A known
/// `{variant, reason}` pair's `Counter` is minted through
/// `Family::get_or_create` (a `RwLock` read guard, too dear per drop
/// under VSR retransmit / drop-burst storms) exactly once, on the pair's
/// first drop, and cached; later drops are an array index + atomic
/// increment. Lazy rather than pre-minted so a pair no drop site produces
/// never enters the family, keeping the scrape free of permanent
/// zero-valued series.
///
/// `partitions_materialised_total` / `partitions_removed_total` /
/// `partitions_reconcile_failures_total` are simple unlabelled counters
/// bumped by the partition reconciliation loop; shard id is
/// resolved at scrape time via the per-shard registry, not as a label.
#[derive(Clone)]
pub struct ShardMetrics {
    partition_io_capacity: Gauge,
    partition_io_bytes_max: Gauge,
    partition_io_active_jobs: Gauge,
    partition_io_queued_results: Gauge,
    partition_io_charged_bytes: Gauge,
    partition_io_wait_depth: Gauge,
    partition_io_quarantined_jobs: Gauge,
    partition_io_fenced: Counter,
    partition_io_timeouts: Counter,

    pub(crate) deferred_polls: DeferredPollMetrics,
    partition_wal_disk_bytes: Gauge,
    partition_wal_retained_bytes: Gauge,
    partition_wal_reload_bytes: Gauge,
    partition_wal_reload_budget_bytes: Gauge,
    partition_wal_queued_bytes: Gauge,
    partition_wal_in_flight_bytes: Gauge,
    partition_wal_checkpoints_pending: Gauge,
    partition_wal_batches: Counter,
    partition_wal_prepares: Counter,
    partition_wal_group_commit_waits: Counter,
    partition_repair_ring_entries: Gauge,
    partition_repair_ring_bytes: Gauge,
    partition_repair_ring_budget_bytes: Gauge,
    partition_repair_ring_charged_bytes: Gauge,
    partition_journal_resident_bytes: Gauge,
    partition_journal_command_bytes: Gauge,
    partition_repair_ring_floor_bytes: Gauge,
    partition_repair_ring_bodies_released: Counter,
    partition_repair_ring_entries_dropped: Counter,
    partition_journal_relief_bytes: Gauge,
    partition_journal_relief_excluded_bytes: Gauge,
    partition_journal_flushes_asked: Counter,
    partition_journal_checkpoints_asked: Counter,
    partition_journal_commands_released: Counter,
    partition_wal_checkpoints: Counter,
    partition_wal_checkpoint_starts: Family<CheckpointTriggerLabel, Counter>,
    checkpoint_start_counters: [Counter; CheckpointTrigger::COUNT],
    partition_checkpoint_queue_length: Gauge,
    partition_checkpoints_queued_in_flight: Gauge,
    partition_stop_checkpoints_missed: Counter,
    partition_wal_errors: Counter,
    partition_tick_visits: Counter,
    partition_missed_wakeups: Counter,
    frame_drops: FrameDropMetrics,
    partitions_materialised_total: Counter,
    partitions_removed_total: Counter,
    partitions_reconcile_failures_total: Counter,
    partitions_duplicate_builds_discarded_total: Counter,
    partitions_reconcile_repairs_total: Counter,
    partition_transfer_refusals_total: Counter,
    partition_applied_divergences_total: Counter,
    partitions_untrusted_log: Gauge,
    partitions_in_view_change: Gauge,
    partition_view_change_votes_held: Gauge,
    partitions_under_replicated: Gauge,
    partitions_below_write_quorum: Gauge,
    partitions_owing_retirement: Gauge,
    partition_segment_upload_lag_seconds: Gauge,
    partition_long_term_rewrite_lag_messages: Gauge,
    partition_long_term_unannounced_objects: Gauge,
    partition_long_term_merges_total: Counter,
    partition_long_term_merged_segments_total: Counter,
    partition_long_term_segments: Gauge,
    partition_long_term_dead_objects: Gauge,
    partition_long_term_trims_total: Counter,
    partition_long_term_deleted_objects_total: Counter,
    fresh_objects: Gauge,
    fresh_object_bytes: Gauge,
    fresh_object_oldest_age_seconds: Gauge,
    fresh_objects_unnamed: Gauge,
    fresh_objects_due: Gauge,
    fresh_objects_proving: Gauge,
    fresh_listings_truncated: Gauge,
    fresh_sweeps_held: Gauge,
    fresh_objects_deleted_total: Counter,
    partition_long_term_backlog_bytes: Gauge,
    partition_long_term_lags: Arc<Mutex<LongTermLags>>,
    partition_long_term_delayed_sends_total: Counter,
    retirement_debts_paid_total: Counter,
    partition_frames_rejected_stale_total: Counter,
    partition_frames_rejected_ahead_total: Counter,
    partition_frames_incarnation_mismatch_total: Family<FrameCommandLabel, Counter>,
    partition_frames_dropped_unbuilt_total: Counter,
    partition_held_reads_expired_total: Counter,
    partition_held_reads_refused_total: Counter,
    partition_held_reads: Gauge,
    partition_requests_denied_transient_total: Counter,
    partition_prepare_gap_drops_total: Counter,
    metadata_prepare_gap_drops_total: Counter,
    partition_applied_log_contradictions_total: Counter,
    metadata_applied_log_contradictions_total: Counter,
    metadata_read_frontier_refusals_total: Counter,
    partition_request_frontier_refusals_total: Counter,
    client_requests_denied_queue_full_total: Counter,
    replica_socket_reads_total: Counter,
    replica_inbound_frames_total: Counter,
    commit_batches: BatchCounters,
    commit_reply_batches: BatchCounters,
    replica_outbound_drops_total: Counter,
    replication_live_refusals_total: Counter,
    replication_replayed_prepares_total: Counter,
    replica_bundle_teardowns_total: Counter,
    replica_preamble_refusals_total: Counter,
    replica_handshake_failures_total: Counter,
    replica_bundles_up: Gauge,
    partition_consumer_offsets_denied_total: Family<ConsumerOffsetKindLabel, Counter>,
    consumer_offset_denied_counters: [Counter; ConsumerKind::COUNT],
    partition_consumer_offsets_stranded: Family<ConsumerOffsetKindLabel, Gauge>,
    consumer_offset_stranded_gauges: [Gauge; ConsumerKind::COUNT],
    client_requests: ClientRequestMetrics,
    fetches: FetchMetrics,
    readiness_push_to_fire: Histogram,
    /// How late each consensus tick of the message pump ran after its timer
    /// was due: the time the shard's thread spent on other work first.
    pump_tick_lateness: Histogram,
    /// Fresh objects this shard stored, the ones the store refused, and the
    /// bytes of the stored ones.
    fresh_uploads_total: Counter,
    fresh_upload_failures_total: Counter,
    fresh_upload_bytes_total: Counter,
    /// Attempts at storing a fresh object after one that failed.
    fresh_upload_retries_total: Counter,
    /// Sends to partitions that place their batches that this shard answered
    /// as not accepted, before or after it stored their messages.
    fresh_sends_denied_total: Counter,
    /// Of those, the sends turned away because the waiting sends of their
    /// partition held its share of the bytes, and because the waiting sends
    /// of the shard held all the bytes they may: the bounds
    /// `tiered_storage.fresh_partition_pending_bytes` and
    /// `tiered_storage.fresh_pending_bytes`.
    fresh_sends_denied_partition_share_total: Counter,
    fresh_sends_denied_shard_cap_total: Counter,
    /// Fresh objects whose upload holds one of the shard's upload slots.
    fresh_uploads_in_flight: Gauge,
    /// Sends whose messages this shard holds or stored and whose request
    /// has not reached its partition.
    fresh_staged_sends: Gauge,
    /// The three parts of a send to a partition that places its batches:
    /// the wait of its upload window, the upload of its fresh object, and
    /// the commit of where its messages sit.
    fresh_window_wait: Histogram,
    fresh_upload: Histogram,
    fresh_commit: Histogram,
    /// Bytes of each fresh object this shard stored. Objects far below
    /// `FRESH_OBJECT_BYTES` under load mean that windows close with few
    /// sends, so the store is asked for more objects than the load needs.
    fresh_object_size: Histogram,
    /// How long the oldest proof row of a batch that committed has waited
    /// for a stored object to carry it. Another cluster reads a batch only
    /// once its row is stored.
    fresh_proof_lag_seconds: Gauge<f64, AtomicU64>,
    /// Fresh objects this shard stored that carry proof rows and no data,
    /// because no window closed soon enough after their batches committed.
    fresh_confirmations_total: Counter,
    /// 1 while this node passes the store identity check of tiered storage.
    /// Both object store gauges hold for the whole node, so every shard
    /// exports the same ones, see [`Self::share_object_store_gauges`].
    object_store_identity_verified: Gauge,
    /// 1 while the object store did not answer the last read of its identity
    /// object: the boot probe, then each identity check.
    object_store_unreachable: Gauge,
    /// Per geo peer, 1 while this node reads the peer: a check verified the
    /// cluster that owns the peer's root. Node-wide like the object store
    /// gauges.
    geo_peer_identity_verified: Family<GeoPeerLabel, Gauge>,
    /// Per geo peer, 1 while the peer's store did not answer the last read of
    /// its identity object.
    geo_peer_unreachable: Family<GeoPeerLabel, Gauge>,
    /// Per geo peer, 1 once committed metadata retired its origin, so this
    /// node follows nothing of it again.
    geo_peer_retired: Family<GeoPeerLabel, Gauge>,
    /// Per geo peer, the shadow topics that serve what they hold because the
    /// peer's store did not answer the last listing of its topic objects. Set
    /// by the metadata primary.
    geo_shadow_topics_stale: Family<GeoPeerLabel, Gauge>,
    /// Per geo peer and request, the manifest listings and reads this shard
    /// sent to the peer's store.
    geo_peer_manifest_requests_total: Family<GeoPeerRequestLabel, Counter>,
    /// Per geo peer and request, the ones of those that failed.
    geo_peer_manifest_failures_total: Family<GeoPeerRequestLabel, Counter>,
    /// Per geo peer, the manifests that the shadow partitions here refused
    /// to commit: the alarm of a manifest that goes back, names another
    /// incarnation or does not check.
    geo_peer_manifests_refused_total: Family<GeoPeerLabel, Counter>,
    /// Per geo peer, the commands that committed its manifests, as the
    /// primaries here proposed them.
    geo_peer_manifest_commands_total: Family<GeoPeerLabel, Counter>,
    /// Per geo peer, the Unix time in seconds of the last listing of its
    /// manifests that succeeded. Set by shard 0.
    geo_peer_manifest_listed_seconds: Family<GeoPeerLabel, Gauge>,
    /// Per geo peer, the shadow partitions whose primary is here and whose
    /// committed manifest is behind the one the peer lists.
    geo_shadow_partitions_behind: Family<GeoPeerLabel, Gauge>,
    /// Per geo peer, how long the furthest behind of those has been behind,
    /// in seconds.
    geo_shadow_manifest_lag_seconds: Family<GeoPeerLabel, Gauge>,
    /// Per geo peer and request, the listings and reads of its consumer
    /// group positions that this shard sent to its store.
    geo_peer_position_requests_total: Family<GeoPeerRequestLabel, Counter>,
    /// Per geo peer and request, the ones of those that failed.
    geo_peer_position_failures_total: Family<GeoPeerRequestLabel, Counter>,
    /// Per geo peer, the positions of its consumer groups that the
    /// partitions here proposed to take.
    geo_peer_positions_adopted_total: Family<GeoPeerLabel, Counter>,
    /// Objects of consumer group positions that this shard stored in the
    /// node's store.
    geo_positions_stored_total: Counter,
    /// The ones of those that the store refused.
    geo_positions_store_failures_total: Counter,
    /// Offset stores of consumer groups on geo and shadow partitions that
    /// were answered once the node's store held their positions.
    geo_positions_commits_confirmed_total: Counter,
    /// The ones answered `TransientNotCommitted` instead, as their positions
    /// were not stored in time or their partitions stopped being led here.
    geo_positions_commits_uncertain_total: Counter,
    /// 1 while the group polls of geo and shadow partitions on this shard
    /// wait for the first positions of every geo peer since the node started.
    geo_positions_resume_waiting: Gauge,
}

impl ShardMetrics {
    /// Create a metrics handle for a shard. The handle is per-shard by
    /// virtue of being constructed once per shard; the shard id does not
    /// appear in the label set (see [`FrameDropLabel`] doc).
    #[must_use]
    // One line per metric, and nothing else.
    #[allow(clippy::too_many_lines)]
    pub fn for_shard() -> Self {
        let frame_drops_total: Family<FrameDropLabel, Counter> = Family::default();
        let cached_counters = Arc::new(std::array::from_fn(|_| {
            std::array::from_fn(|_| OnceLock::new())
        }));
        let partition_consumer_offsets_denied_total: Family<ConsumerOffsetKindLabel, Counter> =
            Family::default();
        // End each Family read guard before creating the next series, which
        // needs the same family's write lock on a miss.
        let consumer_offset_denied_counters = ConsumerKind::ALL.map(|kind| {
            partition_consumer_offsets_denied_total
                .get_or_create(&ConsumerOffsetKindLabel {
                    kind: kind.as_str(),
                })
                .clone()
        });
        let partition_wal_checkpoint_starts: Family<CheckpointTriggerLabel, Counter> =
            Family::default();
        let checkpoint_start_counters = CheckpointTrigger::ALL.map(|trigger| {
            partition_wal_checkpoint_starts
                .get_or_create(&CheckpointTriggerLabel {
                    trigger: trigger.as_str(),
                })
                .clone()
        });
        let partition_consumer_offsets_stranded: Family<ConsumerOffsetKindLabel, Gauge> =
            Family::default();
        let consumer_offset_stranded_gauges = ConsumerKind::ALL.map(|kind| {
            partition_consumer_offsets_stranded
                .get_or_create(&ConsumerOffsetKindLabel {
                    kind: kind.as_str(),
                })
                .clone()
        });
        Self {
            partition_io_capacity: Gauge::default(),
            partition_io_bytes_max: Gauge::default(),
            partition_io_active_jobs: Gauge::default(),
            partition_io_queued_results: Gauge::default(),
            partition_io_charged_bytes: Gauge::default(),
            partition_io_wait_depth: Gauge::default(),
            partition_io_quarantined_jobs: Gauge::default(),
            partition_io_fenced: Counter::default(),
            partition_io_timeouts: Counter::default(),
            deferred_polls: DeferredPollMetrics::default(),
            partition_wal_disk_bytes: Gauge::default(),
            partition_wal_retained_bytes: Gauge::default(),
            partition_wal_reload_bytes: Gauge::default(),
            partition_wal_reload_budget_bytes: Gauge::default(),
            partition_wal_queued_bytes: Gauge::default(),
            partition_wal_in_flight_bytes: Gauge::default(),
            partition_wal_checkpoints_pending: Gauge::default(),
            partition_wal_batches: Counter::default(),
            partition_wal_prepares: Counter::default(),
            partition_wal_group_commit_waits: Counter::default(),
            partition_repair_ring_entries: Gauge::default(),
            partition_repair_ring_bytes: Gauge::default(),
            partition_repair_ring_budget_bytes: Gauge::default(),
            partition_repair_ring_charged_bytes: Gauge::default(),
            partition_journal_resident_bytes: Gauge::default(),
            partition_journal_command_bytes: Gauge::default(),
            partition_repair_ring_floor_bytes: Gauge::default(),
            partition_repair_ring_bodies_released: Counter::default(),
            partition_repair_ring_entries_dropped: Counter::default(),
            partition_journal_relief_bytes: Gauge::default(),
            partition_journal_relief_excluded_bytes: Gauge::default(),
            partition_journal_flushes_asked: Counter::default(),
            partition_journal_checkpoints_asked: Counter::default(),
            partition_journal_commands_released: Counter::default(),
            partition_wal_checkpoints: Counter::default(),
            partition_wal_checkpoint_starts,
            checkpoint_start_counters,
            partition_checkpoint_queue_length: Gauge::default(),
            partition_checkpoints_queued_in_flight: Gauge::default(),
            partition_stop_checkpoints_missed: Counter::default(),
            partition_wal_errors: Counter::default(),
            partition_tick_visits: Counter::default(),
            partition_missed_wakeups: Counter::default(),
            frame_drops: FrameDropMetrics {
                total: frame_drops_total,
                cached_counters,
            },
            partitions_materialised_total: Counter::default(),
            partitions_removed_total: Counter::default(),
            partitions_reconcile_failures_total: Counter::default(),
            partitions_duplicate_builds_discarded_total: Counter::default(),
            partitions_reconcile_repairs_total: Counter::default(),
            partition_transfer_refusals_total: Counter::default(),
            partition_applied_divergences_total: Counter::default(),
            partitions_untrusted_log: Gauge::default(),
            partitions_in_view_change: Gauge::default(),
            partition_view_change_votes_held: Gauge::default(),
            partitions_under_replicated: Gauge::default(),
            partitions_below_write_quorum: Gauge::default(),
            partitions_owing_retirement: Gauge::default(),
            partition_segment_upload_lag_seconds: Gauge::default(),
            partition_long_term_rewrite_lag_messages: Gauge::default(),
            partition_long_term_unannounced_objects: Gauge::default(),
            partition_long_term_merges_total: Counter::default(),
            partition_long_term_merged_segments_total: Counter::default(),
            partition_long_term_segments: Gauge::default(),
            partition_long_term_dead_objects: Gauge::default(),
            partition_long_term_trims_total: Counter::default(),
            partition_long_term_deleted_objects_total: Counter::default(),
            fresh_objects: Gauge::default(),
            fresh_object_bytes: Gauge::default(),
            fresh_object_oldest_age_seconds: Gauge::default(),
            fresh_objects_unnamed: Gauge::default(),
            fresh_objects_due: Gauge::default(),
            fresh_objects_proving: Gauge::default(),
            fresh_listings_truncated: Gauge::default(),
            fresh_sweeps_held: Gauge::default(),
            fresh_objects_deleted_total: Counter::default(),
            partition_long_term_backlog_bytes: Gauge::default(),
            partition_long_term_lags: Arc::default(),
            partition_long_term_delayed_sends_total: Counter::default(),
            retirement_debts_paid_total: Counter::default(),
            partition_frames_rejected_stale_total: Counter::default(),
            partition_frames_rejected_ahead_total: Counter::default(),
            partition_frames_incarnation_mismatch_total: Family::default(),
            partition_frames_dropped_unbuilt_total: Counter::default(),
            partition_held_reads_expired_total: Counter::default(),
            partition_held_reads_refused_total: Counter::default(),
            partition_held_reads: Gauge::default(),
            partition_requests_denied_transient_total: Counter::default(),
            partition_prepare_gap_drops_total: Counter::default(),
            metadata_prepare_gap_drops_total: Counter::default(),
            partition_applied_log_contradictions_total: Counter::default(),
            metadata_applied_log_contradictions_total: Counter::default(),
            metadata_read_frontier_refusals_total: Counter::default(),
            partition_request_frontier_refusals_total: Counter::default(),
            client_requests_denied_queue_full_total: Counter::default(),
            replica_socket_reads_total: Counter::default(),
            replica_inbound_frames_total: Counter::default(),
            commit_batches: BatchCounters::default(),
            commit_reply_batches: BatchCounters::default(),
            replica_outbound_drops_total: Counter::default(),
            replication_live_refusals_total: Counter::default(),
            replication_replayed_prepares_total: Counter::default(),
            replica_bundle_teardowns_total: Counter::default(),
            replica_preamble_refusals_total: Counter::default(),
            replica_handshake_failures_total: Counter::default(),
            replica_bundles_up: Gauge::default(),
            partition_consumer_offsets_denied_total,
            consumer_offset_denied_counters,
            partition_consumer_offsets_stranded,
            consumer_offset_stranded_gauges,
            client_requests: ClientRequestMetrics::new(),
            fetches: FetchMetrics::new(),
            readiness_push_to_fire: latency_histogram(),
            pump_tick_lateness: latency_histogram(),
            fresh_uploads_total: Counter::default(),
            fresh_upload_failures_total: Counter::default(),
            fresh_upload_bytes_total: Counter::default(),
            fresh_upload_retries_total: Counter::default(),
            fresh_sends_denied_total: Counter::default(),
            fresh_sends_denied_partition_share_total: Counter::default(),
            fresh_sends_denied_shard_cap_total: Counter::default(),
            fresh_uploads_in_flight: Gauge::default(),
            fresh_staged_sends: Gauge::default(),
            fresh_window_wait: fresh_latency_histogram(),
            fresh_upload: fresh_latency_histogram(),
            fresh_commit: fresh_latency_histogram(),
            fresh_object_size: fresh_object_size_histogram(),
            fresh_proof_lag_seconds: Gauge::default(),
            fresh_confirmations_total: Counter::default(),
            object_store_identity_verified: Gauge::default(),
            object_store_unreachable: Gauge::default(),
            geo_peer_identity_verified: Family::default(),
            geo_peer_unreachable: Family::default(),
            geo_peer_retired: Family::default(),
            geo_shadow_topics_stale: Family::default(),
            geo_peer_manifest_requests_total: Family::default(),
            geo_peer_manifest_failures_total: Family::default(),
            geo_peer_manifests_refused_total: Family::default(),
            geo_peer_manifest_commands_total: Family::default(),
            geo_peer_manifest_listed_seconds: Family::default(),
            geo_shadow_partitions_behind: Family::default(),
            geo_shadow_manifest_lag_seconds: Family::default(),
            geo_peer_position_requests_total: Family::default(),
            geo_peer_position_failures_total: Family::default(),
            geo_peer_positions_adopted_total: Family::default(),
            geo_positions_stored_total: Counter::default(),
            geo_positions_store_failures_total: Counter::default(),
            geo_positions_commits_confirmed_total: Counter::default(),
            geo_positions_commits_uncertain_total: Counter::default(),
            geo_positions_resume_waiting: Gauge::default(),
        }
    }

    /// Counts a client request by its command `code`. A code that the
    /// command table does not hold counts as `unknown`.
    pub fn record_client_request(&self, code: u32) {
        self.client_requests.record(code);
    }

    /// Counts one answered fetch (109).
    pub fn record_fetch(&self, fetch: &FetchRecord) {
        self.fetches.record(fetch);
    }

    pub(crate) fn record_readiness_push_to_fire(&self, elapsed: Duration) {
        self.readiness_push_to_fire.observe(elapsed.as_secs_f64());
    }

    /// An upload window started its upload `wait` after its first send.
    pub(crate) fn record_fresh_window_wait(&self, wait: Duration) {
        self.fresh_window_wait.observe(wait.as_secs_f64());
    }

    /// The oldest proof row this shard has not stored yet has waited for
    /// `lag`, zero when none waits.
    pub(crate) fn set_fresh_proof_lag(&self, lag: Duration) {
        self.fresh_proof_lag_seconds.set(lag.as_secs_f64());
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_proof_lag_value(&self) -> f64 {
        self.fresh_proof_lag_seconds.get()
    }

    /// The shard stored an object of proof rows alone.
    pub(crate) fn record_fresh_confirmation(&self) {
        self.fresh_confirmations_total.inc();
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_confirmations_value(&self) -> u64 {
        self.fresh_confirmations_total.get()
    }

    /// The upload of a fresh object of `bytes` ended after `elapsed`, and
    /// the store holds the object or not.
    pub(crate) fn record_fresh_upload(&self, bytes: u64, elapsed: Duration, stored: bool) {
        self.fresh_upload.observe(elapsed.as_secs_f64());
        if stored {
            self.fresh_uploads_total.inc();
            self.fresh_upload_bytes_total.inc_by(bytes);
            #[allow(clippy::cast_precision_loss)]
            self.fresh_object_size.observe(bytes as f64);
        } else {
            self.fresh_upload_failures_total.inc();
        }
    }

    /// An attempt at storing a fresh object failed, and another follows.
    pub(crate) fn record_fresh_upload_retry(&self) {
        self.fresh_upload_retries_total.inc();
    }

    /// A send to a partition that places its batches was answered as not
    /// accepted.
    pub(crate) fn record_fresh_send_denied(&self) {
        self.fresh_sends_denied_total.inc();
    }

    /// A send was turned away because the waiting sends of its partition
    /// hold the partition's share of the bytes.
    pub(crate) fn record_fresh_send_over_partition_share(&self) {
        self.fresh_sends_denied_partition_share_total.inc();
    }

    /// A send was turned away because the waiting sends of the shard hold
    /// all the bytes they may.
    pub(crate) fn record_fresh_send_over_shard_cap(&self) {
        self.fresh_sends_denied_shard_cap_total.inc();
    }

    /// The shard has `in_flight` uploads of fresh objects and holds `staged`
    /// sends for them.
    pub(crate) fn set_fresh_load(&self, in_flight: usize, staged: usize) {
        self.fresh_uploads_in_flight
            .set(i64::try_from(in_flight).unwrap_or(i64::MAX));
        self.fresh_staged_sends
            .set(i64::try_from(staged).unwrap_or(i64::MAX));
    }

    /// The histogram of the time from a stored object to the answer of each
    /// send in it, for the task that waits for the answer.
    pub(crate) fn fresh_commit_latency(&self) -> Histogram {
        self.fresh_commit.clone()
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_uploads_value(&self) -> (u64, u64, u64) {
        (
            self.fresh_uploads_total.get(),
            self.fresh_upload_failures_total.get(),
            self.fresh_upload_bytes_total.get(),
        )
    }

    /// Test-only read: upload retries, denied sends, uploads in flight and
    /// staged sends.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_load_value(&self) -> (u64, u64, i64, i64) {
        (
            self.fresh_upload_retries_total.get(),
            self.fresh_sends_denied_total.get(),
            self.fresh_uploads_in_flight.get(),
            self.fresh_staged_sends.get(),
        )
    }

    /// Test-only read: sends turned away at their partition's share and at
    /// the shard's cap.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_cap_denials_value(&self) -> (u64, u64) {
        (
            self.fresh_sends_denied_partition_share_total.get(),
            self.fresh_sends_denied_shard_cap_total.get(),
        )
    }

    pub(crate) fn record_pump_tick_lateness(&self, lateness: Duration) {
        self.pump_tick_lateness.observe(lateness.as_secs_f64());
    }

    /// Set `object_store_identity_verified`, for every shard that shares it.
    pub fn set_object_store_identity_verified(&self, verified: bool) {
        self.object_store_identity_verified.set(i64::from(verified));
    }

    /// Set `object_store_unreachable`, for every shard that shares it.
    pub fn set_object_store_unreachable(&self, unreachable: bool) {
        self.object_store_unreachable.set(i64::from(unreachable));
    }

    /// Set both gauges of the geo peer `origin`, for every shard that shares
    /// them.
    pub fn set_geo_peer_status(&self, origin: &str, verified: bool, unreachable: bool) {
        let label = GeoPeerLabel {
            origin: origin.to_owned(),
        };
        self.geo_peer_identity_verified
            .get_or_create(&label)
            .set(i64::from(verified));
        self.geo_peer_unreachable
            .get_or_create(&label)
            .set(i64::from(unreachable));
    }

    /// Mark the geo peer `origin` retired, for every shard that shares the
    /// gauge.
    pub fn set_geo_peer_retired(&self, origin: &str) {
        let label = GeoPeerLabel {
            origin: origin.to_owned(),
        };
        self.geo_peer_retired.get_or_create(&label).set(1);
    }

    /// Export the object store and geo peer gauges of `node` from this shard
    /// as well. Only the boot probe and the identity checks of shard 0 set
    /// them.
    pub fn share_object_store_gauges(&mut self, node: &Self) {
        self.object_store_identity_verified = node.object_store_identity_verified.clone();
        self.object_store_unreachable = node.object_store_unreachable.clone();
        self.geo_peer_identity_verified = node.geo_peer_identity_verified.clone();
        self.geo_peer_unreachable = node.geo_peer_unreachable.clone();
        self.geo_peer_retired = node.geo_peer_retired.clone();
        self.geo_shadow_topics_stale = node.geo_shadow_topics_stale.clone();
    }

    /// Set the count of stale shadow topics of the geo peer `origin`.
    pub fn set_geo_shadow_topics_stale(&self, origin: &str, stale: usize) {
        let label = GeoPeerLabel {
            origin: origin.to_owned(),
        };
        self.geo_shadow_topics_stale
            .get_or_create(&label)
            .set(i64::try_from(stale).unwrap_or(i64::MAX));
    }

    /// A listing of the manifests of a stream of the geo peer `origin`, which
    /// succeeded at the Unix time `listed_at` in seconds or failed.
    pub fn record_peer_manifest_list(&self, origin: &str, listed_at: Option<u64>) {
        let request = GeoPeerRequestLabel {
            origin: origin.to_owned(),
            request: "list",
        };
        self.geo_peer_manifest_requests_total
            .get_or_create(&request)
            .inc();
        match listed_at {
            Some(listed_at) => {
                self.geo_peer_manifest_listed_seconds
                    .get_or_create(&GeoPeerLabel {
                        origin: origin.to_owned(),
                    })
                    .set(i64::try_from(listed_at).unwrap_or(i64::MAX));
            }
            None => {
                self.geo_peer_manifest_failures_total
                    .get_or_create(&request)
                    .inc();
            }
        }
    }

    /// A read of a manifest of the geo peer `origin`.
    pub fn record_peer_manifest_read(&self, origin: &str) {
        self.geo_peer_manifest_requests_total
            .get_or_create(&GeoPeerRequestLabel {
                origin: origin.to_owned(),
                request: "get",
            })
            .inc();
    }

    /// A read of a manifest of the geo peer `origin` that failed.
    pub fn record_peer_manifest_read_failure(&self, origin: &str) {
        self.geo_peer_manifest_failures_total
            .get_or_create(&GeoPeerRequestLabel {
                origin: origin.to_owned(),
                request: "get",
            })
            .inc();
    }

    /// A manifest of the geo peer `origin` that a shadow partition refused.
    pub fn record_peer_manifest_refused(&self, origin: &str) {
        self.geo_peer_manifests_refused_total
            .get_or_create(&GeoPeerLabel {
                origin: origin.to_owned(),
            })
            .inc();
    }

    /// `commands` commands proposed to commit a manifest of the geo peer
    /// `origin`.
    pub fn record_peer_manifest_commands(&self, origin: &str, commands: usize) {
        self.geo_peer_manifest_commands_total
            .get_or_create(&GeoPeerLabel {
                origin: origin.to_owned(),
            })
            .inc_by(u64::try_from(commands).unwrap_or(u64::MAX));
    }

    /// The shadow partitions of the geo peer `origin` whose primary is here
    /// and that are behind its manifests, and how long the furthest behind
    /// has been, in microseconds.
    pub fn set_shadow_manifest_lag(&self, origin: &str, behind: usize, lag_micros: u64) {
        let label = GeoPeerLabel {
            origin: origin.to_owned(),
        };
        self.geo_shadow_partitions_behind
            .get_or_create(&label)
            .set(i64::try_from(behind).unwrap_or(i64::MAX));
        self.geo_shadow_manifest_lag_seconds
            .get_or_create(&label)
            .set(i64::try_from(lag_micros / 1_000_000).unwrap_or(i64::MAX));
    }

    /// A listing or read, `request`, of the consumer group positions of the
    /// geo peer `origin`, and whether it succeeded.
    pub fn record_peer_position_request(
        &self,
        origin: &str,
        request: &'static str,
        succeeded: bool,
    ) {
        let label = GeoPeerRequestLabel {
            origin: origin.to_owned(),
            request,
        };
        self.geo_peer_position_requests_total
            .get_or_create(&label)
            .inc();
        if !succeeded {
            self.geo_peer_position_failures_total
                .get_or_create(&label)
                .inc();
        }
    }

    /// `positions` positions of the geo peer `origin` proposed for adoption.
    pub fn record_peer_positions_adopted(&self, origin: &str, positions: usize) {
        self.geo_peer_positions_adopted_total
            .get_or_create(&GeoPeerLabel {
                origin: origin.to_owned(),
            })
            .inc_by(u64::try_from(positions).unwrap_or(u64::MAX));
    }

    /// An object of consumer group positions that the store took or refused.
    pub fn record_positions_stored(&self, stored: bool) {
        if stored {
            self.geo_positions_stored_total.inc();
        } else {
            self.geo_positions_store_failures_total.inc();
        }
    }

    /// A group offset store whose reply waited for its position, answered.
    pub fn record_geo_commit_answered(&self, answer: CommitAnswer) {
        match answer {
            CommitAnswer::Committed => self.geo_positions_commits_confirmed_total.inc(),
            CommitAnswer::Uncertain => self.geo_positions_commits_uncertain_total.inc(),
        };
    }

    /// Whether the group polls here wait for the positions of the geo peers.
    pub fn set_positions_resume_waiting(&self, waiting: bool) {
        self.geo_positions_resume_waiting.set(i64::from(waiting));
    }

    /// The counts of the manifest requests to the geo peer `origin` and of
    /// its refused manifests: lists, gets, refused.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn peer_manifest_counts(&self, origin: &str) -> (u64, u64, u64) {
        let request = |request| GeoPeerRequestLabel {
            origin: origin.to_owned(),
            request,
        };
        // One guard at a time: a family holds its lock while a guard lives,
        // and creating a series takes it for writing.
        let lists = self
            .geo_peer_manifest_requests_total
            .get_or_create(&request("list"))
            .get();
        let gets = self
            .geo_peer_manifest_requests_total
            .get_or_create(&request("get"))
            .get();
        let refused = self
            .geo_peer_manifests_refused_total
            .get_or_create(&GeoPeerLabel {
                origin: origin.to_owned(),
            })
            .get();
        (lists, gets, refused)
    }

    /// The count of stale shadow topics of the geo peer `origin`.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn geo_shadow_topics_stale_value(&self, origin: &str) -> i64 {
        let label = GeoPeerLabel {
            origin: origin.to_owned(),
        };
        self.geo_shadow_topics_stale.get_or_create(&label).get()
    }

    /// Both gauges of the geo peer `origin`: verified, then unreachable.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn geo_peer_status_value(&self, origin: &str) -> (bool, bool) {
        let label = GeoPeerLabel {
            origin: origin.to_owned(),
        };
        (
            self.geo_peer_identity_verified.get_or_create(&label).get() != 0,
            self.geo_peer_unreachable.get_or_create(&label).get() != 0,
        )
    }

    /// Whether the geo peer `origin` is marked retired.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn geo_peer_retired_value(&self, origin: &str) -> bool {
        let label = GeoPeerLabel {
            origin: origin.to_owned(),
        };
        self.geo_peer_retired.get_or_create(&label).get() != 0
    }

    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn object_store_identity_verified_value(&self) -> bool {
        self.object_store_identity_verified.get() != 0
    }

    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn object_store_unreachable_value(&self) -> bool {
        self.object_store_unreachable.get() != 0
    }

    /// The WAL work of the partitions that a sweep visited. The bytes on disk
    /// and the retained bytes are levels of sleeping partitions too, so
    /// [`Self::set_partition_levels`] publishes them.
    pub fn record_persistence(&self, metrics: &partitions::PersistenceMetrics) {
        self.partition_wal_queued_bytes
            .set(i64::try_from(metrics.queued_bytes).unwrap_or(i64::MAX));
        self.partition_wal_in_flight_bytes
            .set(i64::try_from(metrics.in_flight_bytes).unwrap_or(i64::MAX));
        self.partition_wal_checkpoints_pending
            .set(i64::try_from(metrics.checkpoints_pending).unwrap_or(i64::MAX));
        self.partition_wal_batches.inc_by(metrics.completed_batches);
        self.partition_wal_prepares.inc_by(metrics.batched_prepares);
        self.partition_wal_group_commit_waits
            .inc_by(metrics.group_commit_waits);
        self.partition_wal_checkpoints
            .inc_by(metrics.completed_checkpoints);
        self.partition_wal_errors.inc_by(metrics.failed_writes);
    }

    /// Fold one sweep's worth of replica socket-read deltas in.
    ///
    /// Deltas, never running totals: [`ReplicaReadMetrics`] comes from a
    /// take that resets the source, so feeding cumulative values here
    /// would double count every sweep. Both counters stay at zero on a
    /// shard that owns no plaintext replica link, which is what makes
    /// them identify the link shard.
    pub fn record_replica_reads(&self, metrics: &ReplicaReadMetrics) {
        self.replica_socket_reads_total.inc_by(metrics.reads);
        self.replica_inbound_frames_total.inc_by(metrics.frames);
    }

    /// Count one Commit batch of `entries` entries that this shard sent.
    pub fn record_commit_batch_sent(&self, entries: usize) {
        self.commit_batches.record_sent(entries);
    }

    /// Count one Commit batch of `entries` entries that this shard received.
    pub fn record_commit_batch_received(&self, entries: usize) {
        self.commit_batches.record_received(entries);
    }

    /// Count one reply batch of `entries` entries that this shard sent.
    pub fn record_commit_reply_batch_sent(&self, entries: usize) {
        self.commit_reply_batches.record_sent(entries);
    }

    /// Count one reply batch of `entries` entries that this shard received.
    pub fn record_commit_reply_batch_received(&self, entries: usize) {
        self.commit_reply_batches.record_received(entries);
    }

    /// Commit batch counts as `[batches sent, entries sent, batches received,
    /// entries received]`.
    #[must_use]
    pub fn commit_batch_counts(&self) -> [u64; 4] {
        self.commit_batches.values()
    }

    /// Reply batch counts, in the order of [`Self::commit_batch_counts`].
    #[must_use]
    pub fn commit_reply_batch_counts(&self) -> [u64; 4] {
        self.commit_reply_batches.values()
    }

    /// Fold one sweep's replica link events in. The counters take deltas,
    /// as in [`Self::record_replica_reads`]; `bundles_up` is a gauge.
    /// Bundles, preambles and their refusals live on shard 0 only, so the
    /// other shards report zero there.
    pub fn record_replica_link_stats(&self, stats: &ReplicaLinkMetrics) {
        self.replica_bundle_teardowns_total
            .inc_by(stats.bundle_teardowns);
        self.replica_preamble_refusals_total
            .inc_by(stats.preamble_refusals);
        self.replica_handshake_failures_total
            .inc_by(stats.handshake_failures);
        self.replica_outbound_drops_total
            .inc_by(stats.outbound_drops);
        self.replica_bundles_up
            .set(i64::try_from(stats.bundles_up).unwrap_or(i64::MAX));
    }

    /// Fold one sweep's replication flow events in, drained by
    /// `VsrConsensus::take_replication_stats`. Both stay at zero while every
    /// backup keeps up, so a nonzero value on a run with no fault means that
    /// flow control held prepares back.
    pub fn record_replication(&self, stats: ReplicationStats) {
        self.replication_live_refusals_total
            .inc_by(stats.live_refusals);
        self.replication_replayed_prepares_total
            .inc_by(stats.replayed);
    }

    /// Snapshot of `replication_live_refusals_total`. Test/simulator accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn replication_live_refusals_value(&self) -> u64 {
        self.replication_live_refusals_total.get()
    }

    /// Snapshot of `replication_replayed_prepares_total`. Test/simulator accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn replication_replayed_prepares_value(&self) -> u64 {
        self.replication_replayed_prepares_total.get()
    }

    fn register_checkpoints(&self, registry: &mut Registry) {
        registry.register(
            "partition_wal_checkpoint_starts",
            "partition WAL checkpoints that the shard tick started, by trigger: idle and age \
             come from the checkpoint queue, stop from a graceful stop, demand from the WAL \
             size, the dirty files or a direct request, reload from the reload budget",
            self.partition_wal_checkpoint_starts.clone(),
        );
        registry.register(
            "partition_checkpoint_queue_length",
            "partitions due for a WAL checkpoint that wait in the shard queue",
            self.partition_checkpoint_queue_length.clone(),
        );
        registry.register(
            "partition_checkpoints_queued_in_flight",
            "checkpoints that the shard queue requested and that did not end yet",
            self.partition_checkpoints_queued_in_flight.clone(),
        );
        registry.register(
            "partition_stop_checkpoints_missed",
            "partitions with committed ops above their checkpoint that a graceful stop did not \
             checkpoint; a boot loads their ops again",
            self.partition_stop_checkpoints_missed.clone(),
        );
    }

    fn register_journal_budget(&self, registry: &mut Registry) {
        registry.register(
            "partition_repair_ring_entries",
            "committed entries this shard's partitions retain for peer repair",
            self.partition_repair_ring_entries.clone(),
        );
        registry.register(
            "partition_repair_ring_bytes",
            "payload bytes this shard's partitions retain for peer repair, excluding allocation overhead",
            self.partition_repair_ring_bytes.clone(),
        );
        registry.register(
            "partition_repair_ring_budget_bytes",
            "this shard's share of partition.journal_memory_node_bytes_max",
            self.partition_repair_ring_budget_bytes.clone(),
        );
        registry.register(
            "partition_repair_ring_charged_bytes",
            "allocated bytes this shard's repair rings charge to its share, after the shed",
            self.partition_repair_ring_charged_bytes.clone(),
        );
        registry.register(
            "partition_journal_resident_bytes",
            "allocated bytes the resident journal entries on this shard charge to its share",
            self.partition_journal_resident_bytes.clone(),
        );
        registry.register(
            "partition_journal_command_bytes",
            "allocated bytes the held state machine commands on this shard charge to its share",
            self.partition_journal_command_bytes.clone(),
        );
        registry.register(
            "partition_repair_ring_floor_bytes",
            "charged bytes the shed cannot take: the newest entry of every repair ring",
            self.partition_repair_ring_floor_bytes.clone(),
        );
        registry.register(
            "partition_repair_ring_bodies_released",
            "repair ring entries whose batch the shard budget sent back to its segment",
            self.partition_repair_ring_bodies_released.clone(),
        );
        registry.register(
            "partition_repair_ring_entries_dropped",
            "repair ring entries the shard budget dropped, which peer repair no longer serves",
            self.partition_repair_ring_entries_dropped.clone(),
        );
        registry.register(
            "partition_journal_relief_bytes",
            "resident bytes on this shard that asked or pending flushes free in time, so the shard budget drops nothing for them",
            self.partition_journal_relief_bytes.clone(),
        );
        registry.register(
            "partition_journal_relief_excluded_bytes",
            "committed resident bytes on this shard whose flush did not come in time, for which the shard budget drops",
            self.partition_journal_relief_excluded_bytes.clone(),
        );
        registry.register(
            "partition_journal_flushes_asked",
            "flushes through the commit point that the shard budget asked",
            self.partition_journal_flushes_asked.clone(),
        );
        registry.register(
            "partition_journal_checkpoints_asked",
            "partition WAL checkpoints that the shard budget queued, so that held commands can leave memory",
            self.partition_journal_checkpoints_asked.clone(),
        );
        registry.register(
            "partition_journal_commands_released",
            "checkpointed state machine commands that the shard budget released, which peer repair no longer serves",
            self.partition_journal_commands_released.clone(),
        );
    }

    fn register_persistence(&self, registry: &mut Registry) {
        registry.register(
            "partition_wal_disk_bytes",
            "active partition WAL bytes",
            self.partition_wal_disk_bytes.clone(),
        );
        registry.register(
            "partition_wal_retained_bytes",
            "retained partition prepare bytes charged to the WAL budget",
            self.partition_wal_retained_bytes.clone(),
        );
        registry.register(
            "partition_wal_reload_bytes",
            "retained partition prepare bytes above the WAL checkpoints, which a boot loads again",
            self.partition_wal_reload_bytes.clone(),
        );
        registry.register(
            "partition_wal_reload_budget_bytes",
            "this shard's share of partition.wal_reload_node_bytes_max",
            self.partition_wal_reload_budget_bytes.clone(),
        );
        registry.register(
            "partition_wal_queued_bytes",
            "queued partition WAL bytes",
            self.partition_wal_queued_bytes.clone(),
        );
        registry.register(
            "partition_wal_in_flight_bytes",
            "partition WAL bytes being written",
            self.partition_wal_in_flight_bytes.clone(),
        );
        registry.register(
            "partition_wal_checkpoints_pending",
            "partition checkpoints awaiting storage completion",
            self.partition_wal_checkpoints_pending.clone(),
        );
        registry.register(
            "partition_wal_batches",
            "completed partition WAL durability batches",
            self.partition_wal_batches.clone(),
        );
        registry.register(
            "partition_wal_prepares",
            "prepares covered by completed partition WAL batches",
            self.partition_wal_prepares.clone(),
        );
        registry.register(
            "partition_wal_group_commit_waits",
            "durable groups that took the optional pre-barrier wait",
            self.partition_wal_group_commit_waits.clone(),
        );
        registry.register(
            "partition_wal_checkpoints",
            "completed partition WAL checkpoints",
            self.partition_wal_checkpoints.clone(),
        );
        registry.register(
            "partition_wal_errors",
            "partition WAL writer failures",
            self.partition_wal_errors.clone(),
        );
        registry.register(
            "partition_tick_visits",
            "partitions the consensus tick visited; a quiet partition is visited once per \
             timeout, so a rate near partitions times ticks means that few of them sleep",
            self.partition_tick_visits.clone(),
        );
        registry.register(
            "partition_missed_wakeups",
            "sleeping partitions the consensus tick found with work and woke; non-zero means \
             that a change reached a partition with no wake-up",
            self.partition_missed_wakeups.clone(),
        );
    }

    pub(crate) fn set_partition_io_limits(&self, capacity: usize, bytes_max: usize) {
        self.partition_io_capacity
            .set(i64::try_from(capacity).unwrap_or(i64::MAX));
        self.partition_io_bytes_max
            .set(i64::try_from(bytes_max).unwrap_or(i64::MAX));
    }

    pub(crate) fn partition_io_fenced_counter(&self) -> Counter {
        self.partition_io_fenced.clone()
    }

    pub(crate) fn partition_io_timeouts_counter(&self) -> Counter {
        self.partition_io_timeouts.clone()
    }

    pub(crate) fn set_partition_io(
        &self,
        active: usize,
        queued: usize,
        bytes: usize,
        waiting: usize,
        quarantined: usize,
    ) {
        self.partition_io_active_jobs
            .set(i64::try_from(active).unwrap_or(i64::MAX));
        self.partition_io_queued_results
            .set(i64::try_from(queued).unwrap_or(i64::MAX));
        self.partition_io_charged_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.partition_io_wait_depth
            .set(i64::try_from(waiting).unwrap_or(i64::MAX));
        self.partition_io_quarantined_jobs
            .set(i64::try_from(quarantined).unwrap_or(i64::MAX));
    }

    /// Republished by every partition sweep, summed over every partition of
    /// the shard: the WAL bytes, and what the repair rings actually hold,
    /// which the configured per-partition ceilings do not say. The ceilings
    /// multiply by the partition count on every replica, so this is the only
    /// place an operator can see the real cost of raising them.
    pub fn set_partition_levels(&self, levels: &partitions::GaugeShare) {
        self.partition_wal_disk_bytes
            .set(i64::try_from(levels.wal_disk_bytes).unwrap_or(i64::MAX));
        self.partition_wal_retained_bytes
            .set(i64::try_from(levels.wal_retained_bytes).unwrap_or(i64::MAX));
        self.partition_wal_reload_bytes
            .set(i64::try_from(levels.wal_reload_bytes).unwrap_or(i64::MAX));
        self.partition_repair_ring_entries
            .set(i64::try_from(levels.repair_ring_entries).unwrap_or(i64::MAX));
        self.partition_repair_ring_bytes
            .set(i64::try_from(levels.repair_ring_bytes).unwrap_or(i64::MAX));
    }

    pub fn set_wal_reload_budget(&self, bytes_max: u64) {
        self.partition_wal_reload_budget_bytes
            .set(i64::try_from(bytes_max).unwrap_or(i64::MAX));
    }

    pub fn record_checkpoint_start(&self, trigger: CheckpointTrigger) {
        self.checkpoint_start_counters[trigger.index()].inc();
    }

    pub fn set_checkpoint_queue(&self, length: usize, in_flight: usize) {
        self.partition_checkpoint_queue_length
            .set(i64::try_from(length).unwrap_or(i64::MAX));
        self.partition_checkpoints_queued_in_flight
            .set(i64::try_from(in_flight).unwrap_or(i64::MAX));
    }

    pub fn record_stop_checkpoints_missed(&self, partitions: usize) {
        self.partition_stop_checkpoints_missed
            .inc_by(u64::try_from(partitions).unwrap_or(u64::MAX));
    }

    /// Partitions that one sweep visited.
    pub fn record_partition_tick_visits(&self, visits: usize) {
        self.partition_tick_visits
            .inc_by(u64::try_from(visits).unwrap_or(u64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_tick_visits_value(&self) -> u64 {
        self.partition_tick_visits.get()
    }

    /// A sleeping partition that the sweep found with work.
    pub fn record_partition_missed_wakeup(&self) {
        self.partition_missed_wakeups.inc();
    }

    /// Where the shed left this shard's repair ring budget, and what it took.
    /// The floor above the budget means the budget is too small for the
    /// partitions on the shard. Dropped entries send lagging replicas to
    /// state transfer.
    pub fn record_repair_ring_shed(&self, shed: &partitions::RepairRingShed) {
        self.partition_repair_ring_budget_bytes
            .set(i64::try_from(shed.usage.budget_bytes).unwrap_or(i64::MAX));
        self.partition_repair_ring_charged_bytes
            .set(i64::try_from(shed.usage.charged_bytes).unwrap_or(i64::MAX));
        self.partition_journal_resident_bytes
            .set(i64::try_from(shed.usage.resident_bytes).unwrap_or(i64::MAX));
        self.partition_journal_command_bytes
            .set(i64::try_from(shed.usage.command_bytes).unwrap_or(i64::MAX));
        self.partition_repair_ring_floor_bytes
            .set(i64::try_from(shed.usage.floor_bytes).unwrap_or(i64::MAX));
        self.partition_repair_ring_bodies_released
            .inc_by(shed.bodies_released as u64);
        self.partition_repair_ring_entries_dropped
            .inc_by(shed.entries_dropped as u64);
        self.partition_journal_relief_bytes
            .set(i64::try_from(shed.usage.relief_bytes).unwrap_or(i64::MAX));
        self.partition_journal_relief_excluded_bytes
            .set(i64::try_from(shed.usage.relief_excluded_bytes).unwrap_or(i64::MAX));
        self.partition_journal_flushes_asked
            .inc_by(shed.flushes_asked as u64);
        self.partition_journal_checkpoints_asked
            .inc_by(shed.checkpoints_asked as u64);
        self.partition_journal_commands_released
            .inc_by(shed.commands_released as u64);
    }

    /// Snapshot of what [`Self::record_repair_ring_shed`] published: the
    /// totals the sheds took, and the usage of the last one. Test/simulator
    /// accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn repair_ring_shed_value(&self) -> partitions::RepairRingShed {
        let gauge = |gauge: &Gauge| usize::try_from(gauge.get()).unwrap_or(usize::MAX);
        let counter = |counter: &Counter| usize::try_from(counter.get()).unwrap_or(usize::MAX);
        partitions::RepairRingShed {
            bodies_released: counter(&self.partition_repair_ring_bodies_released),
            entries_dropped: counter(&self.partition_repair_ring_entries_dropped),
            flushes_asked: counter(&self.partition_journal_flushes_asked),
            checkpoints_asked: counter(&self.partition_journal_checkpoints_asked),
            commands_released: counter(&self.partition_journal_commands_released),
            usage: partitions::RepairRingUsage {
                budget_bytes: gauge(&self.partition_repair_ring_budget_bytes),
                charged_bytes: gauge(&self.partition_repair_ring_charged_bytes),
                resident_bytes: gauge(&self.partition_journal_resident_bytes),
                command_bytes: gauge(&self.partition_journal_command_bytes),
                relief_bytes: gauge(&self.partition_journal_relief_bytes),
                relief_excluded_bytes: gauge(&self.partition_journal_relief_excluded_bytes),
                floor_bytes: gauge(&self.partition_repair_ring_floor_bytes),
            },
        }
    }

    /// Count consumer offset capacity denials from explicit client requests
    /// and automatic commit admission during poll completion.
    pub fn record_consumer_offset_denied(&self, kind: ConsumerKind) {
        self.consumer_offset_denied_counters[kind.index()].inc();
    }

    /// Republished by every partition sweep: the sum over this shard's
    /// partitions of offset keys whose file could not be loaded or unlinked.
    /// Such a key stays counted against `consumer_offsets_max` until a later
    /// store or delete of it succeeds, so a non-zero value that never falls is
    /// an offsets directory an operator has to repair.
    pub fn set_consumer_offsets_stranded(&self, kind: ConsumerKind, count: usize) {
        self.consumer_offset_stranded_gauges[kind.index()]
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    #[cfg(test)]
    #[must_use]
    pub fn consumer_offset_denied_value(&self, kind: ConsumerKind) -> u64 {
        self.consumer_offset_denied_counters[kind.index()].get()
    }

    /// Bumped every time a client request is answered with a retryable denial
    /// because that client already has the maximum number of requests queued
    /// behind one the shard has not answered yet.
    ///
    /// The queue only grows while a client pipelines faster than its own
    /// frames are served, so a sustained rate means one connection is stalled
    /// on something - a held metadata read, a slow commit - while it keeps
    /// sending.
    pub fn record_client_request_denied_queue_full(&self) {
        self.client_requests_denied_queue_full_total.inc();
    }

    /// Current value of [`Self::record_client_request_denied_queue_full`], for
    /// tests that assert the denial was counted.
    #[must_use]
    pub fn client_requests_denied_queue_full_value(&self) -> u64 {
        self.client_requests_denied_queue_full_total.get()
    }

    /// Bumped every time a metadata read is refused because this node's
    /// applied frontier never reached what the caller was told committed.
    ///
    /// The counter is the signal, not a log line: a node that lags durably
    /// refuses every held read of every client for as long as it lags, so the
    /// refusal itself logs at `debug!` and this is what a dashboard alerts on.
    /// Any sustained rate means reads on this node are failing retryable while
    /// its commit walk stays behind.
    pub fn record_metadata_read_frontier_refusal(&self) {
        self.metadata_read_frontier_refusals_total.inc();
    }

    /// Current value of [`Self::record_metadata_read_frontier_refusal`], for
    /// tests that assert a refusal was counted rather than scraping it.
    #[must_use]
    pub fn metadata_read_frontier_refusals_value(&self) -> u64 {
        self.metadata_read_frontier_refusals_total.get()
    }

    /// Bumped every time a partition request is refused because this node's
    /// applied frontier did not reach what the caller was told committed: at
    /// the end of its wait for the frontier, or without a wait after an
    /// earlier request of its connection waited in vain at the same frontier.
    pub fn record_partition_request_frontier_refusal(&self) {
        self.partition_request_frontier_refusals_total.inc();
    }

    /// Current value of [`Self::record_partition_request_frontier_refusal`],
    /// for tests that assert a refusal was counted rather than scraping it.
    #[must_use]
    pub fn partition_request_frontier_refusals_value(&self) -> u64 {
        self.partition_request_frontier_refusals_total.get()
    }

    /// Increment `frame_drops_total{variant, reason}` by 1.
    ///
    /// Callers should pass label constants from [`frame_drop_variant`]
    /// and [`frame_drop_reason`]; those hit the cached counter table.
    /// Any unknown pair falls back to the `Family::get_or_create` slow
    /// path so accounting is preserved even if a future caller forgets
    /// to extend the const tables above.
    pub fn record_frame_drop(&self, variant: &'static str, reason: &'static str) {
        self.frame_drops.record(variant, reason);
    }

    /// The completion lane shares these handles once during shard setup.
    pub(crate) const fn frame_drop_metrics(&self) -> &FrameDropMetrics {
        &self.frame_drops
    }

    /// Bumped on the owning shard each time the partition reconciliation
    /// loop materialises a newly committed namespace via
    /// `build_partition_fresh`.
    pub fn record_partition_materialised(&self) {
        self.partitions_materialised_total.inc();
    }

    /// Bumped on the owning shard each time the partition reconciliation
    /// loop drops an `IggyPartition` whose namespace left the committed
    /// metadata.
    pub fn record_partition_removed(&self) {
        self.partitions_removed_total.inc();
    }

    /// Bumped when the pump discards a duplicate `InsertOwned` for a namespace
    /// that is already live. The reconciler's staged-op guard should make this
    /// unreachable, so a non-zero value is a caught correctness anomaly, not
    /// routine churn: the discarded build re-planted segment 0 over the live
    /// incarnation's path and folded its initial segment into the shared stats
    /// before the pump caught it.
    pub fn record_duplicate_partition_build_discarded(&self) {
        self.partitions_duplicate_builds_discarded_total.inc();
    }

    /// Bumped each time `build_partition_fresh` or
    /// `delete_partitions_from_disk` returns `Err`. The reconciler retries
    /// next tick, but a sustained climb surfaces a stuck partition (disk
    /// full, permission denied, ENOENT on a path it cannot recreate, etc.).
    pub fn record_partition_reconcile_failure(&self) {
        self.partitions_reconcile_failures_total.inc();
    }

    /// Bumped when the periodic full reconcile changes a partition that no
    /// committed change and no local record announced. The reconciler's
    /// change sources should make this unreachable, so a non-zero value is a
    /// caught correctness anomaly that only the repair fixed.
    pub fn record_partition_reconcile_repair(&self) {
        self.partitions_reconcile_repairs_total.inc();
    }

    /// Test-only read, mirroring the siblings; the production scrape goes
    /// through the prometheus registry.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partitions_reconcile_repairs_value(&self) -> u64 {
        self.partitions_reconcile_repairs_total.get()
    }

    /// Bumped every time a serving peer refuses a partition state transfer.
    ///
    /// Transient refusals re-arm on a flat interval and charge no failure
    /// count -- deliberately, since the alternative routes through a 1024x
    /// backoff cap that pins a partition for ~17 minutes after the peer has
    /// already caught up -- which also means a partition stuck rejoining for
    /// hours produces no signal of its own. This counter plus the escalating
    /// log level at the refusal site is that signal.
    pub fn record_partition_transfer_refusal(&self) {
        self.partition_transfer_refusals_total.inc();
    }

    /// Test-only read, mirroring the siblings; the production scrape goes
    /// through the prometheus registry.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_transfer_refusals_value(&self) -> u64 {
        self.partition_transfer_refusals_total.get()
    }

    /// Bumped when a view this replica adopts contradicts a partition op it
    /// already applied, and the partition is set aside for a resync.
    ///
    /// Any non-zero value means committed history diverged between replicas:
    /// the resync keeps the node serving, but the local copy it quarantined
    /// into `.fenced.N` holds an op the group did not keep.
    pub fn record_partition_applied_divergence(&self) {
        self.partition_applied_divergences_total.inc();
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_applied_divergences_value(&self) -> u64 {
        self.partition_applied_divergences_total.get()
    }

    /// Republished by every partition sweep: how many of this shard's
    /// partitions elected on a log that may lack acknowledged ops and are
    /// waiting for a trusted quorum or every replica to report before they
    /// serve again.
    pub fn set_partitions_untrusted_log(&self, count: usize) {
        self.partitions_untrusted_log
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partitions_untrusted_log_value(&self) -> i64 {
        self.partitions_untrusted_log.get()
    }

    /// Republished by every partition sweep: the partitions of this shard in
    /// view change, and the backups whose heartbeat vote the sweep held back.
    pub fn set_partition_view_changes(&self, inflight: usize, votes_held: usize) {
        self.partitions_in_view_change
            .set(i64::try_from(inflight).unwrap_or(i64::MAX));
        self.partition_view_change_votes_held
            .set(i64::try_from(votes_held).unwrap_or(i64::MAX));
    }

    /// Test-only read of [`Self::set_partition_view_changes`]: the partitions in
    /// view change, then the votes held.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_view_changes_value(&self) -> (i64, i64) {
        (
            self.partitions_in_view_change.get(),
            self.partition_view_change_votes_held.get(),
        )
    }

    /// Republished by every partition sweep, over the partitions this shard
    /// leads: those with a member out of sync, and those with too few members
    /// in sync to commit. The metadata primary's first shard adds the
    /// partitions whose primary is down among too many members to commit.
    pub fn set_partition_replication(&self, under_replicated: usize, below_write_quorum: usize) {
        self.partitions_under_replicated
            .set(i64::try_from(under_replicated).unwrap_or(i64::MAX));
        self.partitions_below_write_quorum
            .set(i64::try_from(below_write_quorum).unwrap_or(i64::MAX));
    }

    /// Republished with the replication gauges: partitions on this shard that
    /// refuse sessions until a retirement debt payment commits.
    pub fn set_partitions_owing_retirement(&self, owing: usize) {
        self.partitions_owing_retirement
            .set(i64::try_from(owing).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(test)]
    #[must_use]
    pub fn partitions_owing_retirement_value(&self) -> i64 {
        self.partitions_owing_retirement.get()
    }

    /// Republished by every timed upload pass: the age of the oldest sealed
    /// segment that waits for its upload, over the tiered partitions this
    /// shard leads. Zero when none waits.
    pub fn set_segment_upload_lag(&self, lag: Duration) {
        self.partition_segment_upload_lag_seconds
            .set(i64::try_from(lag.as_secs()).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn segment_upload_lag_seconds_value(&self) -> i64 {
        self.partition_segment_upload_lag_seconds.get()
    }

    /// Republished by every sweep of fresh objects: how many objects of
    /// this replica the store holds, their bytes, the age of the oldest, and
    /// how many of them nothing names.
    pub fn set_fresh_objects(&self, objects: usize, bytes: u64, oldest: Duration, unnamed: usize) {
        self.fresh_objects
            .set(i64::try_from(objects).unwrap_or(i64::MAX));
        self.fresh_object_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.fresh_object_oldest_age_seconds
            .set(i64::try_from(oldest.as_secs()).unwrap_or(i64::MAX));
        self.fresh_objects_unnamed
            .set(i64::try_from(unnamed).unwrap_or(i64::MAX));
    }

    /// Republished at the end of every sweep of fresh objects: how many of
    /// its listings stopped before the last key of their scope, how many
    /// objects past their grace period it left in the store, and how many
    /// it kept for the proof rows they hold.
    pub fn set_fresh_sweep_backlog(&self, truncated: usize, due: usize, proving: usize) {
        self.fresh_listings_truncated
            .set(i64::try_from(truncated).unwrap_or(i64::MAX));
        self.fresh_objects_due
            .set(i64::try_from(due).unwrap_or(i64::MAX));
        self.fresh_objects_proving
            .set(i64::try_from(proving).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings: the objects past their grace
    /// that the last sweep kept for their proof rows.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_objects_proving_value(&self) -> i64 {
        self.fresh_objects_proving.get()
    }

    /// Republished by every sweep of fresh objects that ends with the
    /// answers of the shards: whether one could not tell which objects it
    /// needs, so the sweep deleted nothing.
    pub fn set_fresh_sweeps_held(&self, held: bool) {
        self.fresh_sweeps_held.set(i64::from(held));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_sweeps_held_value(&self) -> i64 {
        self.fresh_sweeps_held.get()
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_objects_unnamed_value(&self) -> i64 {
        self.fresh_objects_unnamed.get()
    }

    /// Test-only read, mirroring the siblings: the listings the last sweep
    /// cut short, and the objects it left due.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_sweep_backlog_value(&self) -> (i64, i64) {
        (
            self.fresh_listings_truncated.get(),
            self.fresh_objects_due.get(),
        )
    }

    pub fn record_fresh_objects_deleted(&self, objects: usize) {
        self.fresh_objects_deleted_total.inc_by(objects as u64);
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_objects_value(&self) -> i64 {
        self.fresh_objects.get()
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fresh_objects_deleted_value(&self) -> u64 {
        self.fresh_objects_deleted_total.get()
    }

    /// Republished by the pump's tick: the most messages one partition this
    /// shard leads committed that no long-term segment holds yet, as the
    /// partition's last rewrite pass found. Zero when none waits.
    pub fn set_long_term_rewrite_lag(&self, messages: u64) {
        self.partition_long_term_rewrite_lag_messages
            .set(i64::try_from(messages).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_rewrite_lag_value(&self) -> i64 {
        self.partition_long_term_rewrite_lag_messages.get()
    }

    /// Republished by the pump's tick: the objects in the store that no
    /// long-term log lists and that are not dead, summed over the partitions
    /// this shard leads, as the last listing of each found. Nothing deletes
    /// such an object, so a count that stays above zero is storage that only
    /// a later replace or trim over its offsets frees.
    pub fn set_long_term_unannounced_objects(&self, objects: usize) {
        self.partition_long_term_unannounced_objects
            .set(i64::try_from(objects).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_unannounced_objects_value(&self) -> i64 {
        self.partition_long_term_unannounced_objects.get()
    }

    /// A merge of long-term segments committed on a partition this shard
    /// leads, and put one segment in the place of `replaced`.
    pub fn record_long_term_merge(&self, replaced: usize) {
        self.partition_long_term_merges_total.inc();
        self.partition_long_term_merged_segments_total
            .inc_by(replaced as u64);
    }

    /// Test-only read, mirroring the siblings: the merges that committed
    /// and the segments they replaced.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_merges_value(&self) -> (u64, u64) {
        (
            self.partition_long_term_merges_total.get(),
            self.partition_long_term_merged_segments_total.get(),
        )
    }

    /// Republished by the pump's tick: the most segments the long-term log
    /// of one partition this shard leads holds. A log takes no segment
    /// beyond its limit, so this is how far the fullest one is from it.
    pub fn set_long_term_segments(&self, segments: usize) {
        self.partition_long_term_segments
            .set(i64::try_from(segments).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_segments_value(&self) -> i64 {
        self.partition_long_term_segments.get()
    }

    /// Republished by the pump's tick: the objects no long-term log lists
    /// any more that wait to be deleted, summed over the partitions this
    /// shard leads.
    pub fn set_long_term_dead_objects(&self, objects: usize) {
        self.partition_long_term_dead_objects
            .set(i64::try_from(objects).unwrap_or(i64::MAX));
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_dead_objects_value(&self) -> i64 {
        self.partition_long_term_dead_objects.get()
    }

    /// A partition this shard leads sent a trim of its long-term log.
    pub fn record_long_term_trim(&self) {
        self.partition_long_term_trims_total.inc();
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_trims_value(&self) -> u64 {
        self.partition_long_term_trims_total.get()
    }

    /// A delete removed `objects` long-term objects of a partition this
    /// shard leads from the store.
    pub fn record_long_term_objects_deleted(&self, objects: usize) {
        self.partition_long_term_deleted_objects_total
            .inc_by(objects as u64);
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_deleted_objects_value(&self) -> u64 {
        self.partition_long_term_deleted_objects_total.get()
    }

    /// Republished by the pump's tick: the most bytes of messages one
    /// partition this shard leads committed that no long-term segment holds
    /// yet, as far as its replica counted them.
    pub fn set_long_term_backlog(&self, bytes: u64) {
        self.partition_long_term_backlog_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    /// An empty list for the pump's tick to fill and hand to
    /// [`Self::set_long_term_lags`].
    #[must_use]
    pub fn long_term_lags_to_fill(&self) -> Vec<LongTermLag> {
        let mut spare = std::mem::take(&mut self.lock_long_term_lags().spare);
        spare.clear();
        spare
    }

    /// Republished by the pump's tick: the lag of every partition this shard
    /// leads that places its batches and has a rewrite wait. A scrape sees
    /// the whole list of one tick or of the next, never a part.
    pub fn set_long_term_lags(&self, lags: Vec<LongTermLag>) {
        let mut held = self.lock_long_term_lags();
        held.spare = std::mem::replace(&mut held.reported, lags);
    }

    /// What [`Self::set_long_term_lags`] took last.
    ///
    /// Not a registered series: a series per partition carries the names of
    /// its stream and topic, which a shard does not hold.
    #[must_use]
    pub fn long_term_lags(&self) -> Vec<LongTermLag> {
        self.lock_long_term_lags().reported.clone()
    }

    fn lock_long_term_lags(&self) -> MutexGuard<'_, LongTermLags> {
        self.partition_long_term_lags
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// A send to a partition past its long-term backlog limit waits before
    /// the shard takes it.
    pub(crate) fn record_long_term_delayed_send(&self) {
        self.partition_long_term_delayed_sends_total.inc();
    }

    /// Test-only read: the largest backlog and the delayed sends.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn long_term_backlog_value(&self) -> (i64, u64) {
        (
            self.partition_long_term_backlog_bytes.get(),
            self.partition_long_term_delayed_sends_total.get(),
        )
    }

    pub fn record_retirement_debt_paid(&self) {
        self.retirement_debts_paid_total.inc();
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partitions_under_replicated_value(&self) -> i64 {
        self.partitions_under_replicated.get()
    }

    /// Test-only read, mirroring the siblings.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partitions_below_write_quorum_value(&self) -> i64 {
        self.partitions_below_write_quorum.get()
    }

    /// Bumped when a parked partition frame is answered instead of served
    /// because it was addressed to an incarnation this shard no longer holds
    /// (delete + recreate recycled the namespace's slab keys). Serving it would
    /// have written a dead topic's op into the topic that replaced it, so a
    /// non-zero value is a caught correctness anomaly, not routine churn.
    pub fn record_partition_frame_rejected_stale(&self) {
        self.partition_frames_rejected_stale_total.inc();
    }

    /// Bumped when a parked frame carries an epoch AHEAD of the one its
    /// partition materialised at: the recreate committed between the reconciler
    /// snapshotting the epoch for `InsertOwned` and the pump applying it. Split
    /// from `partition_frames_rejected_stale_total` so that counter keeps meaning
    /// caught correctness anomaly; this direction is an expected race and would
    /// fire the alert on ordinary delete + recreate churn. Both still reject.
    pub fn record_partition_frame_rejected_ahead(&self) {
        self.partition_frames_rejected_ahead_total.inc();
    }

    /// Bumped when a partition control frame is dropped because it addresses
    /// another incarnation of its namespace than the one this shard hosts: the
    /// sender still runs the deleted topic, or already runs its recreation.
    /// Expected while delete + recreate churn converges, so unlike
    /// `partition_frames_rejected_stale_total` it is no anomaly on its own; a
    /// rate that outlives the churn means a peer never converged on the
    /// namespace's current incarnation.
    ///
    /// Returns the drops counted so far for `frame`'s command, so the drop
    /// site's paced log can report them.
    #[must_use]
    pub fn record_partition_incarnation_mismatch(&self, frame: Command) -> u64 {
        self.partition_frames_incarnation_mismatch_total
            .get_or_create(&FrameCommandLabel::from(frame))
            .inc()
            .saturating_add(1)
    }

    /// Snapshot of `partition_frames_incarnation_mismatch_total` for one
    /// command, 0 when none was dropped. Test-only accessor.
    #[cfg(test)]
    #[must_use]
    pub fn partition_incarnation_mismatch_value(&self, frame: Command) -> u64 {
        self.partition_frames_incarnation_mismatch_total
            .get(&FrameCommandLabel::from(frame))
            .map_or(0, |counter| counter.get())
    }

    /// Bumped when a partition control frame is dropped because its partition
    /// is not built on this shard yet: the create is committed and the
    /// reconciler has not built it, or local metadata has not applied the
    /// create. Ordinary after a create, where the incarnation mismatch and
    /// the unknown namespace are not.
    pub fn record_partition_frame_dropped_unbuilt(&self) {
        self.partition_frames_dropped_unbuilt_total.inc();
    }

    /// Snapshot of `partition_frames_dropped_unbuilt_total`. Test/simulator
    /// accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_frames_dropped_unbuilt_value(&self) -> u64 {
        self.partition_frames_dropped_unbuilt_total.get()
    }

    /// Bumped when a held partition read reaches the end of its hold before
    /// its partition can serve: not built, a primary that still starts or,
    /// for a lookup of the primary, an older incarnation. A lookup gets what
    /// the partition here says, or `NotFound`. A batch of offset reads is
    /// answered entry by entry, each as a read of its own. Every other read
    /// is refused as `TransientNotAccepted`.
    pub fn record_partition_held_read_expired(&self) {
        self.partition_held_reads_expired_total.inc();
    }

    /// Snapshot of `partition_held_reads_expired_total`. Test/simulator
    /// accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_held_reads_expired_value(&self) -> u64 {
        self.partition_held_reads_expired_total.get()
    }

    /// Bumped when a partition read that would be held is answered at once,
    /// because the shard or the namespace of the read holds as many reads as
    /// its cap allows. A lookup of a partition that is not built then gets
    /// `NotFound`, the answer that the hold exists to avoid.
    pub fn record_partition_held_read_refused(&self) {
        self.partition_held_reads_refused_total.inc();
    }

    /// Snapshot of `partition_held_reads_refused_total`. Test-only accessor.
    #[cfg(test)]
    #[must_use]
    pub fn partition_held_reads_refused_value(&self) -> u64 {
        self.partition_held_reads_refused_total.get()
    }

    /// Republished by every pump tick: the reads held on this shard as the
    /// shard cap counts them, each read of an offset batch on its own.
    pub fn set_partition_held_reads(&self, held: usize) {
        self.partition_held_reads
            .set(i64::try_from(held).unwrap_or(i64::MAX));
    }

    /// Test-only read of [`Self::set_partition_held_reads`].
    #[cfg(test)]
    #[must_use]
    pub fn partition_held_reads_value(&self) -> i64 {
        self.partition_held_reads.get()
    }

    /// Total frame drops across every `{variant, reason}` pair.
    ///
    /// Simulator assertion hook: a run without injected loss must keep
    /// this at zero, otherwise an inbox silently shed a frame (capacity
    /// too small, or a routing bug). Production scrape goes through the
    /// prometheus registry.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn frame_drops_value(&self) -> u64 {
        self.frame_drops
            .cached_counters
            .iter()
            .flatten()
            .filter_map(OnceLock::get)
            .map(prometheus_client::metrics::counter::Counter::get)
            .sum()
    }

    /// One `reason` summed over every variant, for the assertions that are about
    /// the reason alone: `unroutable` and `misrouted` must stay at zero however
    /// the drop was classified, while `full` and `disconnected` are what a
    /// crashed inbox produces and cannot be asserted on. Listing variants at the
    /// call site would let a new drop site under an unlisted one escape.
    /// Unknown reasons read as zero, as in [`Self::frame_drop_count`].
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn frame_drop_count_for_reason(&self, reason: &'static str) -> u64 {
        let Some(reason_idx) = reason_index(reason) else {
            return 0;
        };
        self.frame_drops
            .cached_counters
            .iter()
            .filter_map(|variant| variant[reason_idx].get())
            .map(prometheus_client::metrics::counter::Counter::get)
            .sum()
    }

    /// Snapshot of `partitions_materialised_total`. Test-only accessor;
    /// production scrape goes through the prometheus registry.
    #[cfg(test)]
    #[must_use]
    pub fn partitions_materialised_value(&self) -> u64 {
        self.partitions_materialised_total.get()
    }

    /// Snapshot of `partitions_removed_total`. Test-only accessor.
    #[cfg(test)]
    #[must_use]
    pub fn partitions_removed_value(&self) -> u64 {
        self.partitions_removed_total.get()
    }

    /// Snapshot of `partitions_reconcile_failures_total`. Test-only accessor.
    #[cfg(test)]
    #[must_use]
    pub fn partitions_reconcile_failures_value(&self) -> u64 {
        self.partitions_reconcile_failures_total.get()
    }

    /// Bumped for every partition request answered with
    /// `TransientNotAccepted` rather than served - a namespace mid-teardown, an
    /// unverified incarnation, a shed park buffer, or a build this shard gave up
    /// on. Counted only once the answer has been handed to a transport or the
    /// pump, so it measures answers delivered rather than attempted. The client
    /// re-issues, so this is retry pressure rather than error rate, and it is
    /// what distinguishes "answered and retried" from the silent sheds it
    /// replaced.
    pub fn record_partition_request_denied_transient(&self) {
        self.partition_requests_denied_transient_total.inc();
    }

    /// Snapshot of `partition_requests_denied_transient_total`. Test/simulator
    /// accessor; production scrape goes through the prometheus registry.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_requests_denied_transient_value(&self) -> u64 {
        self.partition_requests_denied_transient_total.get()
    }

    /// Add the prepares a partition's backup gap check destroyed since the last
    /// sweep. Drained per tick from `IggyPartition::take_prepare_gap_drops`,
    /// and once more when `ConfirmRemove` drops the partition: a tombstoned
    /// namespace is invisible to the sweep, so the tail it left would otherwise
    /// go to the floor with the value.
    ///
    /// Counts the prepares that ARRIVED after a hole, not the holes: a gap
    /// opened by the last prepare of a burst leaves this at zero. A nonzero
    /// value proves the repair driver has work; a zero one proves nothing.
    ///
    /// Deliberately NOT a `frame_drops_total{variant=partition}` reason: that
    /// family means the bus or the router shed a frame, and the simulator
    /// asserts it stays at zero on runs with no injected loss. A gap drop is a
    /// protocol-ordering drop that any real loss produces, and the sweep repairs
    /// it, so folding the two would turn a routing-fault alert into noise.
    ///
    /// Shard-scoped, with no namespace label: a server runs hundreds of groups
    /// per shard, so labelling by namespace is unbounded cardinality, and every
    /// other partition counter in this file is shard-scoped for the same
    /// reason. The per-group detail is in the arm's log line.
    ///
    /// The metadata plane's own gap drop is NOT counted here: it has its own
    /// counter, and its own level-triggered driver in `tick_metadata`. See
    /// [`Self::record_metadata_prepare_gap_drops`].
    pub fn record_partition_prepare_gap_drops(&self, drops: u64) {
        self.partition_prepare_gap_drops_total.inc_by(drops);
    }

    /// Snapshot of `partition_prepare_gap_drops_total`. Test/simulator accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_prepare_gap_drops_value(&self) -> u64 {
        self.partition_prepare_gap_drops_total.get()
    }

    /// Add the prepares the metadata backup gap check destroyed since the last
    /// tick, drained from `IggyMetadata::take_prepare_gap_drops`.
    ///
    /// The sibling of [`Self::record_partition_prepare_gap_drops`], and it
    /// carries every caveat that one does: it counts the prepares that ARRIVED
    /// after a hole rather than the holes, so a nonzero value proves the
    /// metadata repair driver has work and a zero one proves nothing. There is
    /// one metadata group per node, so unlike the partition counter it needs no
    /// argument about namespace cardinality.
    ///
    /// Deliberately NOT a `frame_drops_total` reason, for the same reason: that
    /// family means the bus or the router shed a frame and the simulator
    /// asserts it stays at zero on runs with no injected loss, while a gap drop
    /// is a protocol-ordering drop the tick repairs.
    pub fn record_metadata_prepare_gap_drops(&self, drops: u64) {
        self.metadata_prepare_gap_drops_total.inc_by(drops);
    }

    /// Snapshot of `metadata_prepare_gap_drops_total`. Test/simulator accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn metadata_prepare_gap_drops_value(&self) -> u64 {
        self.metadata_prepare_gap_drops_total.get()
    }

    /// Add the views whose logs a partition refused for contradicting an op
    /// it applied, drained from `VsrConsensus::take_applied_log_contradictions`.
    ///
    /// A refusing replica keeps refusing every later log of that quorum, so it
    /// drops out of its group. Any non-zero value is a safety violation that
    /// needs an operator. Shard-scoped for the reason given at
    /// [`Self::record_partition_prepare_gap_drops`].
    pub fn record_partition_applied_log_contradictions(&self, views: u64) {
        self.partition_applied_log_contradictions_total
            .inc_by(views);
    }

    /// Snapshot of `partition_applied_log_contradictions_total`. Test/simulator
    /// accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_applied_log_contradictions_value(&self) -> u64 {
        self.partition_applied_log_contradictions_total.get()
    }

    /// The metadata sibling of
    /// [`Self::record_partition_applied_log_contradictions`].
    pub fn record_metadata_applied_log_contradictions(&self, views: u64) {
        self.metadata_applied_log_contradictions_total.inc_by(views);
    }

    /// Snapshot of `metadata_applied_log_contradictions_total`. Test/simulator
    /// accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn metadata_applied_log_contradictions_value(&self) -> u64 {
        self.metadata_applied_log_contradictions_total.get()
    }

    /// Snapshot of `partition_frames_rejected_stale_total`. Test/simulator
    /// accessor, readable from any crate under those cfgs so the crates that
    /// drive the reconciler can assert a reject did not happen.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_frames_rejected_stale_value(&self) -> u64 {
        self.partition_frames_rejected_stale_total.get()
    }

    /// Snapshot of `partition_frames_rejected_ahead_total`. Test/simulator
    /// accessor.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_frames_rejected_ahead_value(&self) -> u64 {
        self.partition_frames_rejected_ahead_total.get()
    }

    /// Snapshot of one `frame_drops_total{variant, reason}` pair, or 0 when the
    /// pair is not a known label combination.
    ///
    /// An unknown pair reports 0 rather than falling through to
    /// `Family::get_or_create`, which would materialise a permanent zero-valued
    /// series in the registry as a side effect of a read: a typo'd label in a
    /// test or assertion would then leak a metric series into production scrapes.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn frame_drop_count(&self, variant: &'static str, reason: &'static str) -> u64 {
        match (variant_index(variant), reason_index(reason)) {
            (Some(v_idx), Some(r_idx)) => self.frame_drops.cached_counters[v_idx][r_idx]
                .get()
                .map_or(0, prometheus_client::metrics::counter::Counter::get),
            _ => 0,
        }
    }

    /// Register every metric this handle owns with `registry`, which the
    /// server scopes per shard (a `shard`-labelled sub-registry) before the
    /// `[http.metrics]` scrape encodes it. Names are registered without the
    /// `_total` suffix; the prometheus text exposition appends it for
    /// counters.
    #[allow(clippy::too_many_lines)]
    pub fn register(&self, registry: &mut Registry) {
        self.register_persistence(registry);
        self.register_journal_budget(registry);
        self.register_checkpoints(registry);
        self.deferred_polls.register(registry);
        registry.register(
            "partition_io_capacity",
            "configured partition file-job slots per shard",
            self.partition_io_capacity.clone(),
        );
        registry.register(
            "partition_io_bytes_max",
            "configured allocation ceiling for partition file jobs per shard",
            self.partition_io_bytes_max.clone(),
        );
        registry.register(
            "partition_io_active_jobs",
            "partition file jobs whose physical work has not completed",
            self.partition_io_active_jobs.clone(),
        );
        registry.register(
            "partition_io_queued_results",
            "completed partition file jobs awaiting acceptance by their owner",
            self.partition_io_queued_results.clone(),
        );
        registry.register(
            "partition_io_charged_bytes",
            "bytes reserved for partition file jobs and retained continuations",
            self.partition_io_charged_bytes.clone(),
        );
        registry.register(
            "partition_io_wait_depth",
            "partition continuations ready to run or waiting for file-job capacity",
            self.partition_io_wait_depth.clone(),
        );
        registry.register(
            "partition_io_quarantined_jobs",
            "interrupted partition file jobs retaining their reservation and resource fence",
            self.partition_io_quarantined_jobs.clone(),
        );
        registry.register(
            "partition_io_fenced",
            "partition file jobs fenced after interruption or timeout",
            self.partition_io_fenced.clone(),
        );
        registry.register(
            "partition_io_timeouts",
            "partition file jobs that exceeded their completion deadline",
            self.partition_io_timeouts.clone(),
        );

        registry.register(
            "frame_drops",
            "frames shed instead of delivered, by frame class and refusal reason",
            self.frame_drops.total.clone(),
        );
        registry.register(
            "partitions_materialised",
            "partitions materialised by the reconciliation loop",
            self.partitions_materialised_total.clone(),
        );
        registry.register(
            "partitions_removed",
            "partitions dropped after their namespace left the committed metadata",
            self.partitions_removed_total.clone(),
        );
        registry.register(
            "partitions_reconcile_failures",
            "partition build or delete attempts the reconciler will retry",
            self.partitions_reconcile_failures_total.clone(),
        );
        registry.register(
            "partitions_duplicate_builds_discarded",
            "duplicate partition builds discarded by the pump; non-zero is a caught anomaly",
            self.partitions_duplicate_builds_discarded_total.clone(),
        );
        registry.register(
            "partitions_reconcile_repairs",
            "partitions the periodic full reconcile changed although no change announced them; non-zero is a caught anomaly",
            self.partitions_reconcile_repairs_total.clone(),
        );
        registry.register(
            "partition_transfer_refusals",
            "partition state transfers refused by the serving peer",
            self.partition_transfer_refusals_total.clone(),
        );
        registry.register(
            "partition_applied_divergences",
            "adopted views that contradicted an applied partition op; non-zero means committed \
             history diverged",
            self.partition_applied_divergences_total.clone(),
        );
        registry.register(
            "partitions_untrusted_log",
            "partitions electing on a log that may lack acknowledged ops; a view starts only \
             once a trusted quorum or every replica reports",
            self.partitions_untrusted_log.clone(),
        );
        registry.register(
            "partitions_in_view_change",
            "partitions of this shard in view change, counted once per sweep",
            self.partitions_in_view_change.clone(),
        );
        registry.register(
            "partition_view_change_votes_held",
            "backups whose heartbeat timer fired and whose vote waits for the per-shard view \
             change limits; non-zero while a failover is paced",
            self.partition_view_change_votes_held.clone(),
        );
        registry.register(
            "partitions_under_replicated",
            "partitions this shard leads with a member it cannot reach or that has trailed \
             the commit point for longer than the lag window",
            self.partitions_under_replicated.clone(),
        );
        registry.register(
            "partitions_below_write_quorum",
            "partitions this shard leads with too few members in sync to commit a write; \
             on the metadata primary's first shard also partitions with too few members up \
             whose primary is down",
            self.partitions_below_write_quorum.clone(),
        );
        registry.register(
            "partitions_owing_retirement",
            "partitions on this shard refusing sessions until they retire sessions that \
             finalized while the partition lacked a quorum of its members",
            self.partitions_owing_retirement.clone(),
        );
        registry.register(
            "partition_segment_upload_lag_seconds",
            "age of the oldest sealed segment that waits for its upload to the object store, \
             over the tiered partitions this shard leads",
            self.partition_segment_upload_lag_seconds.clone(),
        );
        registry.register(
            "fresh_objects",
            "fresh objects of this replica that its last sweep listed; fewer than the store \
             holds while fresh_listings_truncated is not zero",
            self.fresh_objects.clone(),
        );
        registry.register(
            "fresh_object_bytes",
            "bytes of the fresh objects of this replica in the object store",
            self.fresh_object_bytes.clone(),
        );
        registry.register(
            "fresh_object_oldest_age_seconds",
            "age of the oldest fresh object of this replica in the object store",
            self.fresh_object_oldest_age_seconds.clone(),
        );
        registry.register(
            "fresh_objects_unnamed",
            "fresh objects of this replica that no placed batch names and that wait out \
             their grace period",
            self.fresh_objects_unnamed.clone(),
        );
        registry.register(
            "fresh_objects_due",
            "fresh objects of this replica past their grace period that its last sweep left \
             in the object store",
            self.fresh_objects_due.clone(),
        );
        registry.register(
            "fresh_objects_proving",
            "fresh objects of this replica past their grace period that its last sweep kept \
             for the proof rows of batches no long-term segment holds yet",
            self.fresh_objects_proving.clone(),
        );
        registry.register(
            "fresh_listings_truncated",
            "listings of the last sweep of fresh objects that stopped at their share of keys, \
             of one per shard and one for the replica",
            self.fresh_listings_truncated.clone(),
        );
        registry.register(
            "fresh_sweeps_held",
            "1 while the last sweep of fresh objects of this replica deleted nothing because \
             a shard could not tell which objects its partitions need, else 0",
            self.fresh_sweeps_held.clone(),
        );
        registry.register(
            "fresh_objects_deleted",
            "fresh objects this replica deleted",
            self.fresh_objects_deleted_total.clone(),
        );
        registry.register(
            "partition_long_term_rewrite_lag_messages",
            "most messages one partition this shard leads committed that no long-term \
             segment holds yet, over the partitions that place their batches",
            self.partition_long_term_rewrite_lag_messages.clone(),
        );
        registry.register(
            "partition_long_term_unannounced_objects",
            "objects of long-term segments in the store that no long-term log lists and \
             that no later state of it rules out, over the partitions this shard leads",
            self.partition_long_term_unannounced_objects.clone(),
        );
        registry.register(
            "partition_long_term_merges",
            "merges of small long-term segments that committed, over the partitions this \
             shard leads",
            self.partition_long_term_merges_total.clone(),
        );
        registry.register(
            "partition_long_term_merged_segments",
            "long-term segments that committed merges replaced, each merge by one segment",
            self.partition_long_term_merged_segments_total.clone(),
        );
        registry.register(
            "partition_long_term_segments",
            "most segments the long-term log of one partition this shard leads holds, \
             of the 65536 a log takes",
            self.partition_long_term_segments.clone(),
        );
        registry.register(
            "partition_long_term_dead_objects",
            "objects of long-term segments that no long-term log lists any more and that \
             wait to be deleted, over the partitions this shard leads",
            self.partition_long_term_dead_objects.clone(),
        );
        registry.register(
            "partition_long_term_trims",
            "trims of a long-term log that the partitions this shard leads sent, one for \
             every retention pass until the trim commits",
            self.partition_long_term_trims_total.clone(),
        );
        registry.register(
            "partition_long_term_deleted_objects",
            "objects of long-term segments that the partitions this shard leads deleted \
             from the store, the log and the index of a segment as one",
            self.partition_long_term_deleted_objects_total.clone(),
        );
        registry.register(
            "partition_long_term_backlog_bytes",
            "most bytes of messages one partition this shard leads committed that no \
             long-term segment holds yet, over the partitions that place their batches",
            self.partition_long_term_backlog_bytes.clone(),
        );
        registry.register(
            "partition_long_term_delayed_sends",
            "sends this shard delayed because their partition's long-term backlog passed \
             tiered_storage.long_term_backlog_limit",
            self.partition_long_term_delayed_sends_total.clone(),
        );
        registry.register(
            "retirement_debts_paid",
            "retirement debts this node paid as a partition primary",
            self.retirement_debts_paid_total.clone(),
        );
        registry.register(
            "partition_frames_rejected_stale",
            "parked frames rejected for a dead incarnation; non-zero is a caught anomaly",
            self.partition_frames_rejected_stale_total.clone(),
        );
        registry.register(
            "partition_frames_rejected_ahead",
            "parked frames rejected for an epoch ahead of the materialised one",
            self.partition_frames_rejected_ahead_total.clone(),
        );
        registry.register(
            "partition_frames_incarnation_mismatch",
            "partition control frames dropped for addressing another incarnation of their \
             namespace, by frame command",
            self.partition_frames_incarnation_mismatch_total.clone(),
        );
        registry.register(
            "partition_frames_dropped_unbuilt",
            "partition control frames dropped because their partition is not built here yet",
            self.partition_frames_dropped_unbuilt_total.clone(),
        );
        registry.register(
            "partition_held_reads_expired",
            "held partition reads that reached the end of their hold before their partition \
             could serve",
            self.partition_held_reads_expired_total.clone(),
        );
        registry.register(
            "partition_held_reads_refused",
            "partition reads answered without a hold because a held-read cap was spent",
            self.partition_held_reads_refused_total.clone(),
        );
        registry.register(
            "partition_held_reads",
            "partition reads held on this shard, each read of an offset batch counted",
            self.partition_held_reads.clone(),
        );
        registry.register(
            "partition_requests_denied_transient",
            "partition requests answered with a retriable transient denial",
            self.partition_requests_denied_transient_total.clone(),
        );
        registry.register(
            "partition_prepare_gap_drops",
            "replicated prepares dropped out of order by a backup's gap check",
            self.partition_prepare_gap_drops_total.clone(),
        );
        registry.register(
            "replica_socket_reads",
            "completed socket reads on this shard's plaintext replica links",
            self.replica_socket_reads_total.clone(),
        );
        registry.register(
            "replica_inbound_frames",
            "frames decoded off this shard's plaintext replica links",
            self.replica_inbound_frames_total.clone(),
        );
        self.commit_batches.register(registry, "commit", "Commit");
        self.commit_reply_batches
            .register(registry, "commit_reply", "CommitReply");
        registry.register(
            "replica_outbound_drops",
            "replica frames dropped because a peer send queue was full",
            self.replica_outbound_drops_total.clone(),
        );
        registry.register(
            "replication_live_refusals",
            "live prepares that a backup's replication window, replay cursor or busy state refused",
            self.replication_live_refusals_total.clone(),
        );
        registry.register(
            "replication_replayed_prepares",
            "prepares resent to a backup by ordered replay",
            self.replication_replayed_prepares_total.clone(),
        );
        registry.register(
            "replica_bundle_teardowns",
            "replica bundles torn down because a link closed or the bundle stayed incomplete",
            self.replica_bundle_teardowns_total.clone(),
        );
        registry.register(
            "replica_preamble_refusals",
            "inbound replica connections closed for a missing or refused placement preamble",
            self.replica_preamble_refusals_total.clone(),
        );
        registry.register(
            "replica_handshake_failures",
            "delegated replica link handshakes that failed or ran out of handshake_grace",
            self.replica_handshake_failures_total.clone(),
        );
        registry.register(
            "replica_bundles_up",
            "peers whose replica links are all installed, so frames flow to them",
            self.replica_bundles_up.clone(),
        );
        registry.register(
            "metadata_prepare_gap_drops",
            "replicated metadata prepares dropped out of order by a backup's gap check",
            self.metadata_prepare_gap_drops_total.clone(),
        );
        registry.register(
            "partition_applied_log_contradictions",
            "views whose partition log this replica refused for naming another prepare at an op \
             it applied; non-zero is a safety violation",
            self.partition_applied_log_contradictions_total.clone(),
        );
        registry.register(
            "metadata_applied_log_contradictions",
            "views whose metadata log this replica refused for naming another prepare at an op \
             it applied; non-zero is a safety violation",
            self.metadata_applied_log_contradictions_total.clone(),
        );
        registry.register(
            "metadata_read_frontier_refusals",
            "metadata reads refused because this node never applied the caller's committed op",
            self.metadata_read_frontier_refusals_total.clone(),
        );
        registry.register(
            "partition_request_frontier_refusals",
            "partition requests refused because this node did not apply the caller's committed \
             metadata op in time",
            self.partition_request_frontier_refusals_total.clone(),
        );
        registry.register(
            "client_requests_denied_queue_full",
            "client requests denied retryable because that client's request queue was full",
            self.client_requests_denied_queue_full_total.clone(),
        );
        registry.register(
            "partition_consumer_offsets_denied",
            "consumer offset creations denied at the per-partition admission limit (best effort: \
             explicit client denials and poll-side reservation refusals)",
            self.partition_consumer_offsets_denied_total.clone(),
        );
        registry.register(
            "partition_consumer_offsets_stranded",
            "consumer offset keys whose file could not be loaded or unlinked, still counted \
             against the limit",
            self.partition_consumer_offsets_stranded.clone(),
        );
        registry.register(
            "client_requests",
            "client requests by command",
            self.client_requests.total.clone(),
        );
        self.fetches.register(registry);
        registry.register(
            "readiness_push_to_fire_seconds",
            "time from applying the pushed partition state that made a parked readiness wait \
             ready to the end of that wait",
            self.readiness_push_to_fire.clone(),
        );
        registry.register(
            "fresh_uploads",
            "fresh objects this shard stored for the partitions that place their batches",
            self.fresh_uploads_total.clone(),
        );
        registry.register(
            "fresh_upload_failures",
            "fresh objects the object store refused, whose sends were answered as not accepted",
            self.fresh_upload_failures_total.clone(),
        );
        registry.register(
            "fresh_upload_bytes",
            "bytes of the fresh objects this shard stored",
            self.fresh_upload_bytes_total.clone(),
        );
        registry.register(
            "fresh_upload_retries",
            "attempts at storing a fresh object after an attempt that failed",
            self.fresh_upload_retries_total.clone(),
        );
        registry.register(
            "fresh_sends_denied",
            "sends to partitions that place their batches that this shard answered as not \
             accepted",
            self.fresh_sends_denied_total.clone(),
        );
        registry.register(
            "fresh_sends_denied_partition_share",
            "sends turned away because the waiting sends of their partition held \
             tiered_storage.fresh_partition_pending_bytes",
            self.fresh_sends_denied_partition_share_total.clone(),
        );
        registry.register(
            "fresh_sends_denied_shard_cap",
            "sends turned away because the waiting sends of the shard held \
             tiered_storage.fresh_pending_bytes",
            self.fresh_sends_denied_shard_cap_total.clone(),
        );
        registry.register(
            "fresh_uploads_in_flight",
            "fresh objects whose upload holds one of the shard's upload slots",
            self.fresh_uploads_in_flight.clone(),
        );
        registry.register(
            "fresh_staged_sends",
            "sends whose messages this shard holds or stored and whose request has not \
             reached its partition",
            self.fresh_staged_sends.clone(),
        );
        registry.register(
            "fresh_window_wait_seconds",
            "how long an upload window stayed open after its first send",
            self.fresh_window_wait.clone(),
        );
        registry.register(
            "fresh_upload_seconds",
            "how long the upload of a fresh object took",
            self.fresh_upload.clone(),
        );
        registry.register(
            "fresh_commit_seconds",
            "how long a send waited for its answer after its fresh object was stored",
            self.fresh_commit.clone(),
        );
        registry.register(
            "fresh_object_size_bytes",
            "bytes of each fresh object this shard stored",
            self.fresh_object_size.clone(),
        );
        registry.register(
            "fresh_confirmations",
            "fresh objects this shard stored that carry proof rows and no data",
            self.fresh_confirmations_total.clone(),
        );
        registry.register(
            "fresh_proof_lag_seconds",
            "how long the oldest proof row of a committed batch has waited for a stored fresh \
             object to carry it",
            self.fresh_proof_lag_seconds.clone(),
        );
        registry.register(
            "pump_tick_lateness_seconds",
            "how late each consensus tick of the message pump ran after its timer was due",
            self.pump_tick_lateness.clone(),
        );
        registry.register(
            "object_store_identity_verified",
            "1 while this node passes the store identity check of tiered storage, so it runs uploads and object deletes",
            self.object_store_identity_verified.clone(),
        );
        registry.register(
            "object_store_unreachable",
            "1 while the object store did not answer the last read of its identity object, at boot or in the check every minute",
            self.object_store_unreachable.clone(),
        );
        registry.register(
            "geo_peer_identity_verified",
            "1 while this node reads the geo peer: a check verified the cluster that owns its root",
            self.geo_peer_identity_verified.clone(),
        );
        registry.register(
            "geo_peer_unreachable",
            "1 while the store of the geo peer did not answer the last read of its identity object",
            self.geo_peer_unreachable.clone(),
        );
        registry.register(
            "geo_peer_retired",
            "1 once committed metadata retired the geo peer, so this node sends no listing and no check to its store",
            self.geo_peer_retired.clone(),
        );
        registry.register(
            "geo_shadow_topics_stale",
            "Shadow topics of the geo peer that serve what they hold, because its store did not answer the last listing",
            self.geo_shadow_topics_stale.clone(),
        );
        registry.register(
            "geo_peer_manifest_requests",
            "Listings and reads of the geo peer's manifests that this shard sent to its store",
            self.geo_peer_manifest_requests_total.clone(),
        );
        registry.register(
            "geo_peer_manifest_failures",
            "Listings and reads of the geo peer's manifests that failed",
            self.geo_peer_manifest_failures_total.clone(),
        );
        registry.register(
            "geo_peer_manifests_refused",
            "Manifests of the geo peer that a shadow partition refused: they go back, name another incarnation or do not check",
            self.geo_peer_manifests_refused_total.clone(),
        );
        registry.register(
            "geo_peer_manifest_commands",
            "Commands that the shadow partitions here proposed to commit the geo peer's manifests",
            self.geo_peer_manifest_commands_total.clone(),
        );
        registry.register(
            "geo_peer_manifest_listed_seconds",
            "Unix time of the last listing of the geo peer's manifests that succeeded",
            self.geo_peer_manifest_listed_seconds.clone(),
        );
        registry.register(
            "geo_shadow_partitions_behind",
            "Shadow partitions whose primary is on this shard and whose committed manifest is behind the geo peer's",
            self.geo_shadow_partitions_behind.clone(),
        );
        registry.register(
            "geo_shadow_manifest_lag_seconds",
            "How long the furthest behind shadow partition on this shard has been behind the geo peer's manifest",
            self.geo_shadow_manifest_lag_seconds.clone(),
        );
        registry.register(
            "geo_peer_position_requests",
            "Listings and reads of the geo peer's consumer group positions that this shard sent to its store",
            self.geo_peer_position_requests_total.clone(),
        );
        registry.register(
            "geo_peer_position_failures",
            "Listings and reads of the geo peer's consumer group positions that failed",
            self.geo_peer_position_failures_total.clone(),
        );
        registry.register(
            "geo_peer_positions_adopted",
            "Consumer group positions of the geo peer that the partitions here proposed to take",
            self.geo_peer_positions_adopted_total.clone(),
        );
        registry.register(
            "geo_positions_stored",
            "Objects of consumer group positions that this shard stored in the node's store",
            self.geo_positions_stored_total.clone(),
        );
        registry.register(
            "geo_positions_store_failures",
            "Objects of consumer group positions that the node's store refused",
            self.geo_positions_store_failures_total.clone(),
        );
        registry.register(
            "geo_positions_commits_confirmed",
            "Offset stores of consumer groups on geo and shadow partitions answered once the node's store held their positions",
            self.geo_positions_commits_confirmed_total.clone(),
        );
        registry.register(
            "geo_positions_commits_uncertain",
            "Offset stores of consumer groups on geo and shadow partitions answered as uncertain, their positions not stored in time or their partitions no longer led here",
            self.geo_positions_commits_uncertain_total.clone(),
        );
        registry.register(
            "geo_positions_resume_waiting",
            "1 while group polls of geo and shadow partitions here wait for every geo peer's positions after a start",
            self.geo_positions_resume_waiting.clone(),
        );
    }

    /// The text scrape of this handle alone, for the assertions on series
    /// without a value accessor, such as histogram counts.
    #[cfg(test)]
    pub(crate) fn scrape(&self) -> String {
        let mut registry = Registry::default();
        self.register(&mut registry);
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &registry)
            .expect("scrape encoding succeeds");
        buffer
    }
}

/// What one fetch (109) served, for [`ShardMetrics::record_fetch`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchRecord {
    pub sections: u64,
    /// The record bytes of the sections. A fetch without any is empty.
    pub bytes: u64,
    /// Ready targets that the fetch left for a later one, because its byte
    /// budget or its read cap ran out.
    pub budget_waits: u64,
    /// Reads that their owner refused for its deferred poll quota.
    pub quota_refusals: u64,
    /// From the end of the readiness wait to the reply.
    pub fire_to_reply: Duration,
}

/// Fetch (109) accounting on the shard of the connection.
#[derive(Clone)]
struct FetchMetrics {
    fetches: Counter,
    sections: Counter,
    bytes: Counter,
    empty: Counter,
    budget_waits: Counter,
    quota_refusals: Counter,
    fire_to_reply: Histogram,
}

impl FetchMetrics {
    fn new() -> Self {
        Self {
            fetches: Counter::default(),
            sections: Counter::default(),
            bytes: Counter::default(),
            empty: Counter::default(),
            budget_waits: Counter::default(),
            quota_refusals: Counter::default(),
            fire_to_reply: latency_histogram(),
        }
    }

    fn record(&self, fetch: &FetchRecord) {
        self.fetches.inc();
        self.sections.inc_by(fetch.sections);
        self.bytes.inc_by(fetch.bytes);
        if fetch.bytes == 0 {
            self.empty.inc();
        }
        self.budget_waits.inc_by(fetch.budget_waits);
        self.quota_refusals.inc_by(fetch.quota_refusals);
        self.fire_to_reply
            .observe(fetch.fire_to_reply.as_secs_f64());
    }

    fn register(&self, registry: &mut Registry) {
        registry.register("fetches", "answered fetch requests", self.fetches.clone());
        registry.register(
            "fetch_sections",
            "partition sections of fetch replies",
            self.sections.clone(),
        );
        registry.register(
            "fetch_bytes",
            "record bytes of fetch replies",
            self.bytes.clone(),
        );
        registry.register(
            "fetch_empty",
            "fetch replies without records",
            self.empty.clone(),
        );
        registry.register(
            "fetch_budget_waits",
            "ready fetch targets left for a later fetch by the byte budget or the read cap",
            self.budget_waits.clone(),
        );
        registry.register(
            "fetch_quota_refusals",
            "fetch reads refused by the deferred poll quota of their owner",
            self.quota_refusals.clone(),
        );
        registry.register(
            "fetch_fire_to_reply_seconds",
            "time from the end of the readiness wait of a fetch to its reply",
            self.fire_to_reply.clone(),
        );
    }
}

/// Latency buckets from 10 us, doubling up to about 5 s.
const LATENCY_BUCKET_START_SECONDS: f64 = 0.000_01;
const LATENCY_BUCKET_FACTOR: f64 = 2.0;
const LATENCY_BUCKET_COUNT: u16 = 20;

fn latency_histogram() -> Histogram {
    Histogram::new(exponential_buckets(
        LATENCY_BUCKET_START_SECONDS,
        LATENCY_BUCKET_FACTOR,
        LATENCY_BUCKET_COUNT,
    ))
}

/// Buckets for the parts of a send that goes to the object store first, which
/// take from 1 ms to a few hundred: every millisecond up to 5 ms, every 5 ms
/// up to 100 ms, every 10 ms up to 200 ms, and from there half as much again
/// per bucket up to about a minute.
fn fresh_latency_histogram() -> Histogram {
    let millis = (1..=5_u32)
        .chain((10..=100).step_by(5))
        .chain((110..=200).step_by(10))
        .map(|millis| f64::from(millis) / 1_000.0);
    Histogram::new(millis.chain(exponential_buckets(0.3, 1.5, 14)))
}

/// Buckets for the bytes of a stored fresh object: each twice the one before,
/// from 1 KiB to 32 MiB. An object closes at 4 MiB, and is larger only when
/// one send is.
fn fresh_object_size_histogram() -> Histogram {
    Histogram::new(exponential_buckets(1024.0, 2.0, 16))
}

/// The `command` of a request whose code the command table does not hold.
const UNKNOWN_COMMAND: &str = "unknown";
const COMMAND_COUNT: usize = COMMAND_TABLE.len();

/// Label for `client_requests_total`: the command name of the table.
#[derive(Clone, Hash, Eq, PartialEq, EncodeLabelSet, Debug)]
struct ClientCommandLabel {
    command: &'static str,
}

/// Client requests by command. As for frame drops, a command enters the
/// scrape with its first request, and its later requests are an array
/// index and an increment.
#[derive(Clone)]
struct ClientRequestMetrics {
    total: Family<ClientCommandLabel, Counter>,
    cached: Arc<[OnceLock<Counter>; COMMAND_COUNT]>,
}

impl ClientRequestMetrics {
    fn new() -> Self {
        Self {
            total: Family::default(),
            cached: Arc::new(std::array::from_fn(|_| OnceLock::new())),
        }
    }

    fn record(&self, code: u32) {
        let cached = command_index(code)
            .and_then(|index| Some((self.cached.get(index)?, COMMAND_TABLE.get(index)?.name)));
        match cached {
            Some((counter, command)) => counter
                .get_or_init(|| {
                    self.total
                        .get_or_create(&ClientCommandLabel { command })
                        .clone()
                })
                .inc(),
            None => self
                .total
                .get_or_create(&ClientCommandLabel {
                    command: UNKNOWN_COMMAND,
                })
                .inc(),
        };
    }
}

/// Frame drop accounting shared with the registered shard metrics.
/// Clones retain the same family and lazy cache, so detached work records
/// visible drops without carrying unrelated counters or creating unused series.
#[derive(Clone)]
pub(crate) struct FrameDropMetrics {
    total: Family<FrameDropLabel, Counter>,
    cached_counters: Arc<[[OnceLock<Counter>; REASON_COUNT]; VARIANT_COUNT]>,
}

impl FrameDropMetrics {
    pub(crate) fn record(&self, variant: &'static str, reason: &'static str) {
        if let (Some(variant_index), Some(reason_index)) =
            (variant_index(variant), reason_index(reason))
        {
            self.cached_counters[variant_index][reason_index]
                .get_or_init(|| {
                    self.total
                        .get_or_create(&FrameDropLabel { variant, reason })
                        .clone()
                })
                .inc();
        } else {
            self.total
                .get_or_create(&FrameDropLabel { variant, reason })
                .inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::codes::FETCH_MESSAGES_CODE;

    #[test]
    fn frame_drop_counter_increments_per_label_set() {
        let metrics = ShardMetrics::for_shard();
        metrics.record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL);
        metrics.record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL);
        metrics.record_frame_drop(
            frame_drop_variant::CONSENSUS,
            frame_drop_reason::DISCONNECTED,
        );

        let count = |variant, reason| {
            metrics
                .frame_drops
                .total
                .get_or_create(&FrameDropLabel { variant, reason })
                .get()
        };

        assert_eq!(
            count(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL),
            2,
            "two FULL drops land on one label set",
        );
        assert_eq!(
            count(
                frame_drop_variant::CONSENSUS,
                frame_drop_reason::DISCONNECTED,
            ),
            1,
            "a distinct reason gets its own counter",
        );
    }

    #[test]
    fn frame_drop_count_for_reason_sums_one_reason_across_variants() {
        let metrics = ShardMetrics::for_shard();
        metrics.record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::UNROUTABLE);
        metrics.record_frame_drop(frame_drop_variant::PARTITION, frame_drop_reason::UNROUTABLE);
        metrics.record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL);

        assert_eq!(
            metrics.frame_drop_count_for_reason(frame_drop_reason::UNROUTABLE),
            2,
            "one reason sums across every variant that recorded it",
        );
        assert_eq!(
            metrics.frame_drop_count_for_reason(frame_drop_reason::MISROUTED),
            0,
            "an unproduced reason reads as zero, not as the total",
        );
        assert_eq!(
            metrics.frame_drops_value(),
            3,
            "the per-reason view must not disturb the total",
        );
    }

    #[test]
    fn persistence_scrape_distinguishes_retained_budget_from_wal_file_bytes() {
        let metrics = ShardMetrics::for_shard();
        metrics.set_partition_levels(&partitions::GaugeShare {
            wal_disk_bytes: 4096,
            wal_retained_bytes: 65536,
            wal_reload_bytes: 8192,
            ..Default::default()
        });
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &registry).unwrap();
        assert!(
            buffer
                .lines()
                .any(|line| line == "partition_wal_disk_bytes 4096")
        );
        assert!(
            buffer
                .lines()
                .any(|line| line == "partition_wal_retained_bytes 65536")
        );
        assert!(
            buffer
                .lines()
                .any(|line| line == "partition_wal_reload_bytes 8192")
        );
    }

    #[test]
    fn replica_link_stats_add_up_and_bundles_up_follows_the_last_sweep() {
        let metrics = ShardMetrics::for_shard();
        metrics.record_replica_link_stats(&ReplicaLinkMetrics {
            bundle_teardowns: 2,
            preamble_refusals: 1,
            handshake_failures: 3,
            outbound_drops: 2,
            bundles_up: 1,
        });
        metrics.record_replica_link_stats(&ReplicaLinkMetrics {
            bundle_teardowns: 1,
            outbound_drops: 1,
            bundles_up: 2,
            ..ReplicaLinkMetrics::default()
        });
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &registry)
            .expect("scrape encoding succeeds");
        for line in [
            "replica_bundle_teardowns_total 3",
            "replica_preamble_refusals_total 1",
            "replica_handshake_failures_total 3",
            "replica_outbound_drops_total 3",
            "replica_bundles_up 2",
        ] {
            assert!(buffer.lines().any(|scraped| scraped == line), "{line}");
        }
    }

    #[test]
    fn unproduced_pairs_never_enter_the_scrape() {
        // The lazy fast-path cache must not mint the full variant x reason
        // cross product: a pair no drop site produced would otherwise sit in
        // every scrape as a permanent zero-valued series.
        let metrics = ShardMetrics::for_shard();
        metrics.record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL);
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &registry)
            .expect("scrape encoding succeeds");
        assert!(
            buffer.contains(frame_drop_variant::CONSENSUS),
            "the produced pair must appear in the scrape",
        );
        assert!(
            !buffer.contains(frame_drop_reason::PARK_OVERFLOW),
            "a pair no drop site produced must not appear in the scrape",
        );
    }

    #[test]
    fn cached_counter_aliases_family_entry() {
        // Cached fast-path counters must point at the same underlying
        // atomic as the `Family`'s get_or_create entry, otherwise a future
        // scrape exporter would observe zero while the fast path
        // incremented the cache and the slow path queried the family.
        let metrics = ShardMetrics::for_shard();
        for _ in 0..5 {
            metrics.record_frame_drop(frame_drop_variant::PARTITION, frame_drop_reason::UNROUTABLE);
        }
        let from_family = metrics
            .frame_drops
            .total
            .get_or_create(&FrameDropLabel {
                variant: frame_drop_variant::PARTITION,
                reason: frame_drop_reason::UNROUTABLE,
            })
            .get();
        assert_eq!(from_family, 5);
    }

    #[test]
    fn consumer_offset_denials_use_one_cached_series_per_kind() {
        let metrics = ShardMetrics::for_shard();
        metrics.record_consumer_offset_denied(ConsumerKind::Consumer);
        metrics.record_consumer_offset_denied(ConsumerKind::Consumer);
        metrics.record_consumer_offset_denied(ConsumerKind::ConsumerGroup);
        metrics.record_consumer_offset_denied(ConsumerKind::ExternalGroup);

        assert_eq!(
            metrics.consumer_offset_denied_value(ConsumerKind::Consumer),
            2
        );
        assert_eq!(
            metrics.consumer_offset_denied_value(ConsumerKind::ConsumerGroup),
            1
        );
        assert_eq!(
            metrics.consumer_offset_denied_value(ConsumerKind::ExternalGroup),
            1
        );
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &registry)
            .expect("scrape encoding succeeds");
        assert_eq!(
            buffer
                .lines()
                .filter(|line| line.starts_with("partition_consumer_offsets_denied_total"))
                .count(),
            3
        );
    }

    #[test]
    fn client_requests_count_by_command_name_and_unknown_codes_together() {
        let metrics = ShardMetrics::for_shard();
        let unknown_code = u32::MAX;
        metrics.record_client_request(FETCH_MESSAGES_CODE);
        metrics.record_client_request(FETCH_MESSAGES_CODE);
        metrics.record_client_request(unknown_code);
        let scrape = metrics.scrape();
        let mut series: Vec<_> = scrape
            .lines()
            .filter(|line| line.starts_with("client_requests_total"))
            .collect();
        // A family encodes its series in hash order.
        series.sort_unstable();
        assert_eq!(
            series,
            [
                "client_requests_total{command=\"message.fetch\"} 2",
                "client_requests_total{command=\"unknown\"} 1",
            ],
            "a command without requests stays out of the scrape",
        );
    }

    #[test]
    fn object_store_gauges_follow_their_last_setting() {
        let metrics = ShardMetrics::for_shard();
        let series = |name: &str| {
            metrics
                .scrape()
                .lines()
                .find(|line| line.starts_with(&format!("{name} ")))
                .map(str::to_owned)
        };

        assert_eq!(
            series("object_store_identity_verified").as_deref(),
            Some("object_store_identity_verified 0")
        );
        assert_eq!(
            series("object_store_unreachable").as_deref(),
            Some("object_store_unreachable 0")
        );
        metrics.set_object_store_identity_verified(true);
        metrics.set_object_store_unreachable(true);
        assert_eq!(
            series("object_store_identity_verified").as_deref(),
            Some("object_store_identity_verified 1")
        );
        assert_eq!(
            series("object_store_unreachable").as_deref(),
            Some("object_store_unreachable 1")
        );
        metrics.set_object_store_unreachable(false);
        assert_eq!(
            series("object_store_unreachable").as_deref(),
            Some("object_store_unreachable 0")
        );
    }

    #[test]
    fn shared_object_store_gauges_show_the_settings_of_shard_0_on_every_shard() {
        let shard_0 = ShardMetrics::for_shard();
        let mut shard_1 = ShardMetrics::for_shard();
        shard_1.share_object_store_gauges(&shard_0);

        shard_0.set_object_store_identity_verified(true);
        shard_0.set_object_store_unreachable(true);

        assert!(shard_1.object_store_identity_verified_value());
        assert!(shard_1.object_store_unreachable_value());
        let scrape = shard_1.scrape();
        assert!(
            scrape
                .lines()
                .any(|line| line == "object_store_identity_verified 1")
        );
        assert!(
            scrape
                .lines()
                .any(|line| line == "object_store_unreachable 1")
        );
        shard_0.set_object_store_identity_verified(false);
        shard_0.set_object_store_unreachable(false);
        assert!(!shard_1.object_store_identity_verified_value());
        assert!(!shard_1.object_store_unreachable_value());
    }

    #[test]
    fn fetch_records_add_up_and_count_empty_replies() {
        let metrics = ShardMetrics::for_shard();
        metrics.record_fetch(&FetchRecord {
            sections: 2,
            bytes: 300,
            budget_waits: 1,
            quota_refusals: 1,
            fire_to_reply: Duration::from_micros(50),
        });
        metrics.record_fetch(&FetchRecord::default());
        let scrape = metrics.scrape();
        for series in [
            "fetches_total 2",
            "fetch_sections_total 2",
            "fetch_bytes_total 300",
            "fetch_empty_total 1",
            "fetch_budget_waits_total 1",
            "fetch_quota_refusals_total 1",
            "fetch_fire_to_reply_seconds_count 2",
        ] {
            assert!(
                scrape.lines().any(|line| line == series),
                "{series} is not in the scrape:\n{scrape}"
            );
        }
    }

    #[test]
    fn given_fresh_uploads_when_they_settle_should_record_the_size_of_each_stored_object() {
        let metrics = ShardMetrics::for_shard();
        metrics.record_fresh_upload(1_000, Duration::from_millis(5), true);
        metrics.record_fresh_upload(3_000_000, Duration::from_millis(5), true);
        metrics.record_fresh_upload(500, Duration::from_millis(5), false);
        let scrape = metrics.scrape();
        for series in [
            "fresh_object_size_bytes_count 2",
            "fresh_object_size_bytes_sum 3001000.0",
            "fresh_object_size_bytes_bucket{le=\"1024.0\"} 1",
            "fresh_object_size_bytes_bucket{le=\"2097152.0\"} 1",
            "fresh_object_size_bytes_bucket{le=\"4194304.0\"} 2",
            "fresh_object_size_bytes_bucket{le=\"33554432.0\"} 2",
        ] {
            assert!(
                scrape.lines().any(|line| line == series),
                "{series} is not in the scrape:\n{scrape}"
            );
        }
    }

    #[test]
    fn unknown_label_set_falls_back_to_family() {
        // A label outside the const tables must still record via the slow
        // path so we never silently drop accounting. A scrape that filters
        // on the unknown pair must show the bump.
        let metrics = ShardMetrics::for_shard();
        metrics.record_frame_drop("unexpected_variant", "unexpected_reason");
        let from_family = metrics
            .frame_drops
            .total
            .get_or_create(&FrameDropLabel {
                variant: "unexpected_variant",
                reason: "unexpected_reason",
            })
            .get();
        assert_eq!(from_family, 1);
    }
}

/// Batches and entries of one batched command that a shard sent and received.
#[derive(Clone, Default)]
struct BatchCounters {
    batches_sent: Counter,
    entries_sent: Counter,
    batches_received: Counter,
    entries_received: Counter,
}

impl BatchCounters {
    fn record_sent(&self, entries: usize) {
        self.batches_sent.inc();
        self.entries_sent.inc_by(entries as u64);
    }

    fn record_received(&self, entries: usize) {
        self.batches_received.inc();
        self.entries_received.inc_by(entries as u64);
    }

    fn values(&self) -> [u64; 4] {
        [
            self.batches_sent.get(),
            self.entries_sent.get(),
            self.batches_received.get(),
            self.entries_received.get(),
        ]
    }

    fn register(&self, registry: &mut Registry, prefix: &str, command: &str) {
        registry.register(
            format!("{prefix}_batches_sent"),
            format!("{command} batches that this shard sent"),
            self.batches_sent.clone(),
        );
        registry.register(
            format!("{prefix}_batch_entries_sent"),
            format!("{command} entries in the batches that this shard sent"),
            self.entries_sent.clone(),
        );
        registry.register(
            format!("{prefix}_batches_received"),
            format!("{command} batches that this shard received"),
            self.batches_received.clone(),
        );
        registry.register(
            format!("{prefix}_batch_entries_received"),
            format!("{command} entries in the batches that this shard received"),
            self.entries_received.clone(),
        );
    }
}
