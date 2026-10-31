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

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use iggy_binary_protocol::{COMMAND_TABLE, Command, command_index};
use iggy_common::ConsumerKind;
use message_bus::{ReplicaLinkMetrics, ReplicaReadMetrics};

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
    partition_wal_queued_bytes: Gauge,
    partition_wal_in_flight_bytes: Gauge,
    partition_wal_checkpoints_pending: Gauge,
    partition_wal_batches: Counter,
    partition_wal_prepares: Counter,
    partition_wal_group_commit_waits: Counter,
    partition_repair_ring_entries: Gauge,
    partition_repair_ring_bytes: Gauge,
    partition_wal_checkpoints: Counter,
    partition_wal_errors: Counter,
    frame_drops: FrameDropMetrics,
    partitions_materialised_total: Counter,
    partitions_removed_total: Counter,
    partitions_reconcile_failures_total: Counter,
    partitions_duplicate_builds_discarded_total: Counter,
    partition_transfer_refusals_total: Counter,
    partition_applied_divergences_total: Counter,
    partitions_untrusted_log: Gauge,
    partitions_under_replicated: Gauge,
    partitions_below_write_quorum: Gauge,
    partitions_owing_retirement: Gauge,
    retirement_debts_paid_total: Counter,
    partition_frames_rejected_stale_total: Counter,
    partition_frames_rejected_ahead_total: Counter,
    partition_frames_incarnation_mismatch_total: Family<FrameCommandLabel, Counter>,
    partition_requests_denied_transient_total: Counter,
    partition_prepare_gap_drops_total: Counter,
    metadata_prepare_gap_drops_total: Counter,
    partition_applied_log_contradictions_total: Counter,
    metadata_applied_log_contradictions_total: Counter,
    metadata_read_frontier_refusals_total: Counter,
    client_requests_denied_queue_full_total: Counter,
    replica_socket_reads_total: Counter,
    replica_inbound_frames_total: Counter,
    replica_outbound_drops_total: Counter,
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
}

impl ShardMetrics {
    /// Create a metrics handle for a shard. The handle is per-shard by
    /// virtue of being constructed once per shard; the shard id does not
    /// appear in the label set (see [`FrameDropLabel`] doc).
    #[must_use]
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
            partition_wal_queued_bytes: Gauge::default(),
            partition_wal_in_flight_bytes: Gauge::default(),
            partition_wal_checkpoints_pending: Gauge::default(),
            partition_wal_batches: Counter::default(),
            partition_wal_prepares: Counter::default(),
            partition_wal_group_commit_waits: Counter::default(),
            partition_repair_ring_entries: Gauge::default(),
            partition_repair_ring_bytes: Gauge::default(),
            partition_wal_checkpoints: Counter::default(),
            partition_wal_errors: Counter::default(),
            frame_drops: FrameDropMetrics {
                total: frame_drops_total,
                cached_counters,
            },
            partitions_materialised_total: Counter::default(),
            partitions_removed_total: Counter::default(),
            partitions_reconcile_failures_total: Counter::default(),
            partitions_duplicate_builds_discarded_total: Counter::default(),
            partition_transfer_refusals_total: Counter::default(),
            partition_applied_divergences_total: Counter::default(),
            partitions_untrusted_log: Gauge::default(),
            partitions_under_replicated: Gauge::default(),
            partitions_below_write_quorum: Gauge::default(),
            partitions_owing_retirement: Gauge::default(),
            retirement_debts_paid_total: Counter::default(),
            partition_frames_rejected_stale_total: Counter::default(),
            partition_frames_rejected_ahead_total: Counter::default(),
            partition_frames_incarnation_mismatch_total: Family::default(),
            partition_requests_denied_transient_total: Counter::default(),
            partition_prepare_gap_drops_total: Counter::default(),
            metadata_prepare_gap_drops_total: Counter::default(),
            partition_applied_log_contradictions_total: Counter::default(),
            metadata_applied_log_contradictions_total: Counter::default(),
            metadata_read_frontier_refusals_total: Counter::default(),
            client_requests_denied_queue_full_total: Counter::default(),
            replica_socket_reads_total: Counter::default(),
            replica_inbound_frames_total: Counter::default(),
            replica_outbound_drops_total: Counter::default(),
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

    pub fn record_persistence(&self, metrics: &partitions::PersistenceMetrics) {
        self.partition_wal_disk_bytes
            .set(i64::try_from(metrics.disk_bytes).unwrap_or(i64::MAX));
        self.partition_wal_retained_bytes
            .set(i64::try_from(metrics.retained_bytes).unwrap_or(i64::MAX));
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
            "partition_wal_checkpoints",
            "completed partition WAL checkpoints",
            self.partition_wal_checkpoints.clone(),
        );
        registry.register(
            "partition_wal_errors",
            "partition WAL writer failures",
            self.partition_wal_errors.clone(),
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

    /// Republished by every partition sweep: what the repair rings on this
    /// shard actually hold, which the configured per-partition ceilings do not
    /// say. The ceilings multiply by the partition count on every replica, so
    /// this is the only place an operator can see the real cost of raising
    /// them.
    pub fn set_repair_ring(&self, entries: usize, bytes: u64) {
        self.partition_repair_ring_entries
            .set(i64::try_from(entries).unwrap_or(i64::MAX));
        self.partition_repair_ring_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
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
        registry.register(
            "replica_outbound_drops",
            "replica frames dropped because a peer send queue was full",
            self.replica_outbound_drops_total.clone(),
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
        metrics.record_persistence(&partitions::PersistenceMetrics {
            disk_bytes: 4096,
            retained_bytes: 65536,
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
