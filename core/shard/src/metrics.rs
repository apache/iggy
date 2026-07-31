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
//! - `IggyShard::apply_reconcile_ops` - parked frames whose re-dispatch onto
//!   this shard's own inbox was refused.
//!
//! The counter uses atomic interior mutability, safe to bump from `!Send`
//! compio reactor contexts. Each shard owns its own instance. It is not
//! yet exposed through a scrape endpoint; every drop site also logs via
//! `tracing`, so the counter is a structured complement to those logs
//! until a per-shard exporter lands.
//!
//! TODO(hubcio): register `frame_drops_total` with a per-shard prometheus
//! exporter so the counter is scrape-able; until then the `tracing`
//! drop-site logs are the only alertable signal.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;

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

/// Variant labels used in `frame_drops_total`. Exposed as constants to
/// catch typos at compile time and to keep the cardinality bounded.
///
/// `FORWARD_CLIENT_SEND` ticks when the cross-shard client-reply forward
/// closure fails. Unlike `CONSENSUS` drops (which VSR retransmit
/// recovers), a `FORWARD_CLIENT_SEND` drop is terminal: the client never
/// receives the reply and request / response semantics break above the
/// bus. Operators should alert on the drop-site `tracing` logs (the
/// counter is not scrape-able yet, see the module doc) and size
/// `inbox_capacity` for the worst-case cross-shard reply burst.
/// `FORWARD_REPLICA_SEND` is the symmetric variant for replica forwards;
/// VSR retransmit covers its loss so it stays informational.
///
/// `PARTITION` covers the partition plane: a frame shed because the namespace
/// had not materialised and its park buffer was at capacity
/// (`reason=park_overflow`), a re-dispatch the shard's own inbox refused, or a
/// routing send the target inbox refused. A shed client request is answered with
/// a retriable status, so the client recovers -- but a shed *prepare* is not
/// covered by retransmit once its op has reached quorum
/// (`consensus::retransmit_targets` skips `ok_quorum_received`, and the
/// partition plane creates a repair session only in `on_start_view`), so it
/// leaves that backup behind until an unrelated view change.
//
// TODO(krishna): give the partition plane a normal-status repair driver so a
// shed or refused prepare is repaired without waiting for a view change. Until
// then `variant=partition` is the only signal that a backup may be stranded
// behind `commit_max`.
pub mod frame_drop_variant {
    pub const CONSENSUS: &str = "consensus";
    pub const FD_TRANSFER: &str = "fd_transfer";
    pub const PARTITION: &str = "partition";
    pub const FORWARD_CLIENT_SEND: &str = "forward_client_send";
    pub const FORWARD_REPLICA_SEND: &str = "forward_replica_send";
    pub const METADATA_COMMIT_TICK: &str = "metadata_commit_tick";
    /// A delegated replica handshake's outcome ack to shard 0 was
    /// dropped; the shard-0 deadline expiry recovers the slot / pending
    /// entry, so this stays informational.
    pub const REPLICA_HANDSHAKE_ACK: &str = "replica_handshake_ack";
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
/// its cap, so the frame is shed with no reply.
pub mod frame_drop_reason {
    pub const FULL: &str = "full";
    pub const DISCONNECTED: &str = "disconnected";
    pub const UNROUTABLE: &str = "unroutable";
    pub const DELIVERY_FAILED: &str = "delivery_failed";
    pub const MISROUTED: &str = "misrouted";
    pub const PARK_OVERFLOW: &str = "park_overflow";
}

const VARIANT_COUNT: usize = 7;
const REASON_COUNT: usize = 6;

const VARIANTS: [&str; VARIANT_COUNT] = [
    frame_drop_variant::CONSENSUS,
    frame_drop_variant::FD_TRANSFER,
    frame_drop_variant::PARTITION,
    frame_drop_variant::FORWARD_CLIENT_SEND,
    frame_drop_variant::FORWARD_REPLICA_SEND,
    frame_drop_variant::METADATA_COMMIT_TICK,
    frame_drop_variant::REPLICA_HANDSHAKE_ACK,
];

const REASONS: [&str; REASON_COUNT] = [
    frame_drop_reason::FULL,
    frame_drop_reason::DISCONNECTED,
    frame_drop_reason::UNROUTABLE,
    frame_drop_reason::DELIVERY_FAILED,
    frame_drop_reason::MISROUTED,
    frame_drop_reason::PARK_OVERFLOW,
];

fn variant_index(s: &str) -> Option<usize> {
    VARIANTS.iter().position(|v| *v == s)
}

fn reason_index(s: &str) -> Option<usize> {
    REASONS.iter().position(|r| *r == s)
}

/// Per-shard metric handles.
///
/// Cheap to clone (`Arc` of a `Family` under the hood). Each shard owns
/// one instance produced by [`ShardMetrics::for_shard`]. The
/// `VARIANT_COUNT * REASON_COUNT` cross product of `Counter`s is minted
/// at construction so the drop-site hot path never re-enters
/// `Family::get_or_create` (which acquires a `RwLock` read guard per
/// drop and stalls under VSR retransmit / drop-burst storms).
///
/// `partitions_materialised_total` / `partitions_removed_total` /
/// `partitions_reconcile_failures_total` are simple unlabelled counters
/// bumped by the partition reconciliation loop; shard id is
/// resolved at scrape time via the per-shard registry, not as a label.
#[derive(Clone)]
pub struct ShardMetrics {
    frame_drops_total: Family<FrameDropLabel, Counter>,
    cached_counters: [[Counter; REASON_COUNT]; VARIANT_COUNT],
    partitions_materialised_total: Counter,
    partitions_removed_total: Counter,
    partitions_reconcile_failures_total: Counter,
    partition_frames_rejected_stale_total: Counter,
    partition_requests_denied_transient_total: Counter,
}

impl ShardMetrics {
    /// Create a metrics handle for a shard. The handle is per-shard by
    /// virtue of being constructed once per shard; the shard id does not
    /// appear in the label set (see [`FrameDropLabel`] doc).
    ///
    /// All `VARIANT_COUNT * REASON_COUNT` counters are pre-registered
    /// with the underlying [`Family`] so the drop-site hot path is a
    /// constant-time array index + atomic increment.
    #[must_use]
    pub fn for_shard() -> Self {
        let frame_drops_total: Family<FrameDropLabel, Counter> = Family::default();
        let cached_counters = std::array::from_fn(|v_idx| {
            std::array::from_fn(|r_idx| {
                frame_drops_total
                    .get_or_create(&FrameDropLabel {
                        variant: VARIANTS[v_idx],
                        reason: REASONS[r_idx],
                    })
                    .clone()
            })
        });
        Self {
            frame_drops_total,
            cached_counters,
            partitions_materialised_total: Counter::default(),
            partitions_removed_total: Counter::default(),
            partitions_reconcile_failures_total: Counter::default(),
            partition_frames_rejected_stale_total: Counter::default(),
            partition_requests_denied_transient_total: Counter::default(),
        }
    }

    /// Increment `frame_drops_total{variant, reason}` by 1.
    ///
    /// Callers should pass label constants from [`frame_drop_variant`]
    /// and [`frame_drop_reason`]; those hit the cached counter table.
    /// Any unknown pair falls back to the `Family::get_or_create` slow
    /// path so accounting is preserved even if a future caller forgets
    /// to extend the const tables above.
    pub fn record_frame_drop(&self, variant: &'static str, reason: &'static str) {
        if let (Some(v_idx), Some(r_idx)) = (variant_index(variant), reason_index(reason)) {
            self.cached_counters[v_idx][r_idx].inc();
        } else {
            self.frame_drops_total
                .get_or_create(&FrameDropLabel { variant, reason })
                .inc();
        }
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

    /// Bumped each time `build_partition_fresh` or
    /// `delete_partitions_from_disk` returns `Err`. The reconciler retries
    /// next tick, but a sustained climb surfaces a stuck partition (disk
    /// full, permission denied, ENOENT on a path it cannot recreate, etc.).
    pub fn record_partition_reconcile_failure(&self) {
        self.partitions_reconcile_failures_total.inc();
    }

    /// Bumped when a parked partition frame is answered instead of served
    /// because it was addressed to an incarnation this shard no longer holds
    /// (delete + recreate recycled the namespace's slab keys). Serving it would
    /// have written a dead topic's op into the topic that replaced it, so a
    /// non-zero value is a caught correctness anomaly, not routine churn.
    ///
    /// Like every counter in this module it is not scrape-able yet (see the
    /// module-level `TODO(hubcio)`); the `warn!` at the reject site is what an
    /// operator can actually alert on today.
    pub fn record_partition_frame_rejected_stale(&self) {
        self.partition_frames_rejected_stale_total.inc();
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
        self.cached_counters
            .iter()
            .flatten()
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

    /// Snapshot of `partition_frames_rejected_stale_total`. Test/simulator
    /// accessor, readable from any crate under those cfgs so the crates that
    /// drive the reconciler can assert a reject did not happen.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn partition_frames_rejected_stale_value(&self) -> u64 {
        self.partition_frames_rejected_stale_total.get()
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
            (Some(v_idx), Some(r_idx)) => self.cached_counters[v_idx][r_idx].get(),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                .frame_drops_total
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
            .frame_drops_total
            .get_or_create(&FrameDropLabel {
                variant: frame_drop_variant::PARTITION,
                reason: frame_drop_reason::UNROUTABLE,
            })
            .get();
        assert_eq!(from_family, 5);
    }

    #[test]
    fn unknown_label_set_falls_back_to_family() {
        // A label outside the const tables must still record via the slow
        // path so we never silently drop accounting. A scrape that filters
        // on the unknown pair must show the bump.
        let metrics = ShardMetrics::for_shard();
        metrics.record_frame_drop("unexpected_variant", "unexpected_reason");
        let from_family = metrics
            .frame_drops_total
            .get_or_create(&FrameDropLabel {
                variant: "unexpected_variant",
                reason: "unexpected_reason",
            })
            .get();
        assert_eq!(from_family, 1);
    }
}
