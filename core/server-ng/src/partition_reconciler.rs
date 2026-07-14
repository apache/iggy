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

//! Partition reconciliation loop.
//!
//! One task per shard. On wake (commit tick or periodic safety tick),
//! diff committed `Streams` STM against local `IggyPartitions`:
//! - non-owned namespaces: seed `shards_table` row pointing at owner.
//! - owned namespaces: `build_partition_fresh` then enqueue
//!   `ReconcileOp::InsertOwned` for pump-side apply.
//! - ghosts: two-phase tombstone, disk delete, `ConfirmRemove`.
//!
//! # Dormant race: reply ships before partition materialises
//!
//! `metadata::on_ack` fires the commit notifier and emits the wire reply
//! immediately after STM apply, but the owning shard's reconciler wakes
//! asynchronously and only enqueues `ReconcileOp::InsertOwned` once
//! `build_partition_fresh` finishes (mkdir + segment open + fallocate,
//! multi-millisecond). Until the pump drains the queue,
//! `shards_table.shard_for(ns)` returns `None` and `router::route_typed`
//! drops any partition op silently with
//! `frame_drops_total{variant=partition,reason=unroutable}` (see
//! `shard/src/router.rs:147-162`).
//!
//! Today this race is **unreachable** from any SDK: the `vsr` feature
//! gate only wires VSR framing for `users` + `personal_access_tokens`;
//! `topics.rs` and `partitions.rs` `binary_impls` still emit pre-VSR
//! encoding server-ng can't dispatch. The first SDK trait impl that adds
//! a `#[cfg(feature = "vsr")]` branch for `create_topic` or
//! `send_messages` surfaces the race as a silent first-produce drop after
//! every `create_topic`.
//!
//! TODO: block VSR-ification of `topics.rs` / `partitions.rs`
//! `binary_impls` on a materialization barrier (cross-shard
//! `MaterializedAck` from each assigned shard back to shard 0; shard 0
//! holds the reply in `client_table` until all acks arrive). Sync-block
//! alternative was deferred.

use crate::bootstrap::ServerNgShard;
use crate::partition_helpers::{build_partition_fresh, delete_partitions_from_disk};
use ahash::{AHashMap, AHashSet};
use configs::server_ng::ServerNgConfig;
use consensus::{MetadataHandle, PartitionsHandle};
use futures::FutureExt;
use iggy_common::{ConsumerGroupId, IggyTimestamp};
use metadata::impls::metadata::StreamsFrontend;
use partitions::delete_persisted_offset;
use server_common::sharding::{IggyNamespace, ShardId};
use shard::MetadataSubmit;
use shard::ReconcileOp;
use shard::shards_table::{ShardsTable, calculate_shard_assignment};
use shard::{Receiver, Sender};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, trace, warn};

const BACKOFF_BASE: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_mins(1);

/// Doubles per attempt, clamped at `BACKOFF_MAX`.
fn next_backoff(attempts: u32) -> Duration {
    let shift = attempts.saturating_sub(1).min(6);
    let multiplier = 1_u32.checked_shl(shift).unwrap_or(1);
    BACKOFF_BASE.saturating_mul(multiplier).min(BACKOFF_MAX)
}

#[derive(Debug, Clone, Copy)]
struct FailureRecord {
    attempts: u32,
    next_retry_at: Instant,
}

/// Separate retry budgets so a stuck disk-delete cannot throttle a re-create.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FailureCause {
    Add,
    Delete,
}

pub struct ReconcilerCtx {
    pub shard: Rc<ServerNgShard>,
    pub total_shards: u16,
    pub config: Rc<ServerNgConfig>,
    pub cluster_id: u128,
    pub self_replica_id: u8,
    pub replica_count: u8,
    failure_state: RefCell<AHashMap<(IggyNamespace, FailureCause), FailureRecord>>,
    /// `Streams::revision` observed at the end of the last pass that fully
    /// converged. Paired with `last_pass_noop` for the fast-skip in
    /// [`reconcile_once`] (no O(N) scan when nothing changed).
    last_revision: Cell<Option<u64>>,
    /// `true` when the previous pass made no changes. Only then is a
    /// same-`revision` pass safe to skip.
    last_pass_noop: Cell<bool>,
}

impl ReconcilerCtx {
    #[must_use]
    pub fn new(
        shard: Rc<ServerNgShard>,
        total_shards: u16,
        config: Rc<ServerNgConfig>,
        cluster_id: u128,
        self_replica_id: u8,
        replica_count: u8,
    ) -> Self {
        Self {
            shard,
            total_shards,
            config,
            cluster_id,
            self_replica_id,
            replica_count,
            failure_state: RefCell::new(AHashMap::new()),
            last_revision: Cell::new(None),
            last_pass_noop: Cell::new(false),
        }
    }

    fn is_backed_off(&self, ns: IggyNamespace, cause: FailureCause, now: Instant) -> bool {
        let state = self.failure_state.borrow();
        if state.is_empty() {
            return false;
        }
        state
            .get(&(ns, cause))
            .is_some_and(|record| record.next_retry_at > now)
    }

    /// `true` when a prior teardown of `ns` recorded a disk-delete failure
    /// not since cleared. Teardown clears the `FailureCause::Delete` record
    /// (via [`Self::record_success`]) exactly when it enqueues
    /// `ConfirmRemove`, and sets it only on a delete that failed without
    /// enqueuing one, so it doubles as "no `ConfirmRemove` in flight for
    /// `ns`": the signal [`reconcile_additions`] uses to tell a
    /// permanently-wedged tombstone (retry the delete) from one whose drop
    /// is genuinely pending (defer).
    fn has_pending_delete_failure(&self, ns: IggyNamespace) -> bool {
        let state = self.failure_state.borrow();
        if state.is_empty() {
            return false;
        }
        state.contains_key(&(ns, FailureCause::Delete))
    }

    fn record_success(&self, ns: IggyNamespace, cause: FailureCause) {
        if self.failure_state.borrow().is_empty() {
            return;
        }
        self.failure_state.borrow_mut().remove(&(ns, cause));
    }

    fn record_failure(&self, ns: IggyNamespace, cause: FailureCause, now: Instant) {
        let mut state = self.failure_state.borrow_mut();
        let entry = state.entry((ns, cause)).or_insert(FailureRecord {
            attempts: 0,
            next_retry_at: now,
        });
        entry.attempts = entry.attempts.saturating_add(1);
        entry.next_retry_at = now + next_backoff(entry.attempts);
    }

    /// Drop records whose namespace left both target and local sets;
    /// otherwise a failed-then-deleted namespace's stale backoff
    /// would throttle a future same-namespace re-create.
    fn prune_failure_state_stale(
        &self,
        target_set: &AHashSet<IggyNamespace>,
        local_set: &AHashSet<IggyNamespace>,
    ) {
        let mut state = self.failure_state.borrow_mut();
        if state.is_empty() {
            return;
        }
        state.retain(|(ns, _cause), _record| target_set.contains(ns) || local_set.contains(ns));
    }
}

pub type WakeTx = Sender<()>;
pub type WakeRx = Receiver<()>;

/// One initial reconcile before the wait loop so a shard that comes up
/// before shard 0's first `MetadataCommitTick` still converges.
pub async fn run_reconciler(
    ctx: Rc<ReconcilerCtx>,
    wake_rx: WakeRx,
    stop_rx: Receiver<()>,
    periodic: Duration,
) {
    debug!(
        shard = ctx.shard.id,
        total_shards = ctx.total_shards,
        periodic_ms = periodic.as_millis(),
        "partition reconciler starting"
    );
    reconcile_once(&ctx).await;

    loop {
        let sleep = compio::time::sleep(periodic);
        // Biased for the same reason as the shard pump: unbiased `select!`
        // polls arms in process-random order, which a deterministic
        // simulator cannot seed. Listed order is the intended priority.
        futures::select_biased! {
            _ = stop_rx.recv().fuse() => break,
            recv = wake_rx.recv().fuse() => {
                if recv.is_err() {
                    break;
                }
                while wake_rx.try_recv().is_ok() {}
                reconcile_once(&ctx).await;
            }
            () = sleep.fuse() => {
                reconcile_once(&ctx).await;
            }
        }
    }

    debug!(shard = ctx.shard.id, "partition reconciler exited");
}

#[derive(Default)]
struct PassCounters {
    materialised: usize,
    routed: usize,
    removed_local: usize,
    removed_routed: usize,
    backoff_skipped: usize,
    /// Stale incarnations (slab-key reuse) torn down for rebuild.
    stale: usize,
    /// Consumer-group offsets reclaimed for groups deleted while their topic
    /// survived (a bare `DeleteConsumerGroup`, not a topic/stream delete).
    cg_offsets_purged: usize,
    /// Committed delete watermarks not yet fully enforced on local segments.
    /// Counted so the pass does not arm the fast-skip: the pump can be
    /// blocked by a consumer barrier or by a rejoin whose offsets land via
    /// journal repair, and neither unblocking bumps `Streams::revision`.
    trims_pending: usize,
}

impl PassCounters {
    const fn total(&self) -> usize {
        self.materialised
            + self.routed
            + self.removed_local
            + self.removed_routed
            + self.backoff_skipped
            + self.stale
            + self.cg_offsets_purged
            + self.trims_pending
    }
}

/// Diff target vs local; materialise missing, tear down ghosts. Idempotent.
/// Returns `false` when the pass fast-skipped (nothing changed), `true`
/// when it ran the full diff. Callers in production discard the result;
/// tests assert the skip.
async fn reconcile_once(ctx: &ReconcilerCtx) -> bool {
    let shard_id = ctx.shard.id;
    let revision = current_revision(ctx);

    // Cooperative-revocation completion runs every tick, before the fast-skip:
    // a timeout fires on wall-clock and a drain on partition-offset state, and
    // neither bumps `Streams::revision`, so the skip would otherwise starve an
    // idle group's pending revocations forever. Cheap no-op when none pending.
    reconcile_pending_revocations(ctx);

    // Fast-skip: committed partition set unchanged since the last
    // fully-converged pass and no backoff retry due, so the O(N) diff is
    // pure waste. Safe because reconcile is level-triggered: the next
    // partition-shaping commit bumps `revision` and a pending retry keeps
    // `failure_state` non-empty, either of which forces the next pass.
    if ctx.last_revision.get() == Some(revision)
        && ctx.last_pass_noop.get()
        && ctx.failure_state.borrow().is_empty()
    {
        trace!(
            shard = shard_id,
            revision, "reconciler fast-skip (no change)"
        );
        return false;
    }

    let target = snapshot_target_namespaces(ctx);
    let target_set: AHashSet<IggyNamespace> = target.iter().map(|(ns, _)| *ns).collect();
    let mut counters = PassCounters::default();

    reconcile_additions(ctx, target, &mut counters).await;
    reconcile_removals(ctx, &target_set, &mut counters).await;
    reconcile_consumer_group_offsets(ctx, &mut counters).await;
    reconcile_segment_truncations(ctx, &mut counters);
    reconcile_partition_purges(ctx);

    let local_set: AHashSet<IggyNamespace> =
        ctx.shard.plane.partitions().namespaces().copied().collect();
    ctx.prune_failure_state_stale(&target_set, &local_set);

    // Arm the fast-skip only when this pass converged (did nothing). A
    // working pass (including a staleness teardown that rebuilds on the
    // next pass) leaves `last_pass_noop = false` so the follow-up pass
    // still runs even though `revision` did not change.
    let did_work = counters.total() > 0;
    ctx.last_revision.set(Some(revision));
    ctx.last_pass_noop.set(!did_work);

    if did_work {
        debug!(
            shard = shard_id,
            revision,
            materialised = counters.materialised,
            routed = counters.routed,
            removed_local = counters.removed_local,
            removed_routed = counters.removed_routed,
            backoff_skipped = counters.backoff_skipped,
            stale = counters.stale,
            "partition reconciler pass complete"
        );
    } else {
        trace!(
            shard = shard_id,
            "partition reconciler pass complete (no-op)"
        );
    }

    true
}

async fn reconcile_additions(
    ctx: &ReconcilerCtx,
    target: Vec<(IggyNamespace, u64)>,
    counters: &mut PassCounters,
) {
    let shard_id = ctx.shard.id;
    let partitions = ctx.shard.plane.partitions();
    let total_shards = u32::from(ctx.total_shards);

    for (ns, epoch) in target {
        if partitions.contains(&ns) {
            // Tombstoned but still in the map. Two cases, told apart by
            // whether teardown's disk delete succeeded:
            //
            //   * Succeeded -> a `ConfirmRemove` is in flight. The pump
            //     drops the partition and clears the tombstone, then
            //     `signal_reconcile_wake` re-wakes us to rebuild within one
            //     pump-iter. Building over a path mid-unlink would race, so
            //     defer.
            //   * Failed -> no `ConfirmRemove` enqueued, so the tombstone
            //     never lifts. Paired with a same-key recreate landing `ns`
            //     back in the target, this pass would defer forever while
            //     `reconcile_removals` no longer sees a ghost: the partition
            //     is fenced permanently and every data-plane frame dropped.
            //     Re-drive teardown to retry the delete.
            //
            // A recorded `FailureCause::Delete` is the authoritative "no
            // ConfirmRemove in flight" signal (see
            // [`ReconcilerCtx::has_pending_delete_failure`]).
            if partitions.is_tombstoned(&ns) {
                if !ctx.has_pending_delete_failure(ns) {
                    trace!(
                        shard = shard_id,
                        ns_raw = ns.inner(),
                        "additions: ns tombstoned + in-map; rebuild deferred to post-ConfirmRemove wake"
                    );
                    continue;
                }
                trace!(
                    shard = shard_id,
                    ns_raw = ns.inner(),
                    "additions: ns tombstoned + in-map with failed disk delete; re-driving teardown to retry delete"
                );
                tear_down_owned_partition(ctx, ns, counters).await;
                continue;
            }

            // Staleness: the namespace tuple is built from reused slab
            // keys, so a delete+recreate of the same (stream, topic,
            // partition) yields an identical `ns` whose committed
            // `created_revision` differs from the epoch recorded when the
            // local partition materialised. A mismatch (or a missing
            // routing row on a live partition, an invariant violation)
            // means the local partition is a prior incarnation carrying
            // stale segments/offsets/log. Tear it down; the
            // post-ConfirmRemove wake rebuilds it fresh next pass.
            if ctx.shard.shards_table().epoch_for(ns) == Some(epoch) {
                continue;
            }
            trace!(
                shard = shard_id,
                ns_raw = ns.inner(),
                target_epoch = epoch,
                "additions: stale incarnation (slab-key reuse); tearing down for rebuild"
            );
            counters.stale += 1;
            tear_down_owned_partition(ctx, ns, counters).await;
            continue;
        }

        let owning_shard = calculate_shard_assignment(&ns, total_shards);
        if owning_shard != shard_id {
            if !shards_table_contains(ctx, ns) {
                ctx.shard.enqueue_reconcile_op(ReconcileOp::InsertRouted {
                    namespace: ns,
                    owner: ShardId::new(owning_shard),
                    epoch,
                });
                counters.routed += 1;
            }
            continue;
        }

        let now = Instant::now();
        if ctx.is_backed_off(ns, FailureCause::Add, now) {
            counters.backoff_skipped += 1;
            continue;
        }

        // Resolve the shared stats `Arc` only for namespaces actually
        // built, not once per committed partition every pass. A topic that
        // vanished between the target snapshot and this read defers to the
        // next pass.
        let Some(partition_stats) = fetch_partition_stats(ctx, ns) else {
            continue;
        };

        match build_partition_fresh(
            ctx.config.as_ref(),
            ns,
            partition_stats,
            ctx.cluster_id,
            ctx.self_replica_id,
            ctx.replica_count,
            Rc::clone(&ctx.shard.bus),
        )
        .await
        {
            Ok(partition) => {
                ctx.shard.enqueue_reconcile_op(ReconcileOp::InsertOwned {
                    namespace: ns,
                    partition: Box::new(partition),
                    epoch,
                });
                ctx.record_success(ns, FailureCause::Add);
                counters.materialised += 1;
            }
            Err(err) => {
                ctx.record_failure(ns, FailureCause::Add, now);
                ctx.shard.metrics().record_partition_reconcile_failure();
                error!(
                    shard = shard_id,
                    stream_id = ns.stream_id(),
                    topic_id = ns.topic_id(),
                    partition_id = ns.partition_id(),
                    error = %err,
                    "reconciler failed to materialize partition"
                );
            }
        }
    }
}

async fn reconcile_removals(
    ctx: &ReconcilerCtx,
    target_set: &AHashSet<IggyNamespace>,
    counters: &mut PassCounters,
) {
    let partitions = ctx.shard.plane.partitions();
    let shards_table = ctx.shard.shards_table();

    let owned_ghosts: Vec<IggyNamespace> = partitions
        .namespaces()
        .copied()
        .filter(|ns| !target_set.contains(ns))
        .collect();
    for ns in owned_ghosts {
        tear_down_owned_partition(ctx, ns, counters).await;
    }

    // Skip namespaces still locally owned (disk-delete-failed ghosts):
    // pruning their shards_table row would strand peer routing.
    let still_owned: AHashSet<IggyNamespace> = partitions.namespaces().copied().collect();
    let routed_ghosts: Vec<IggyNamespace> = shards_table
        .namespaces()
        .into_iter()
        .filter(|ns| !target_set.contains(ns) && !still_owned.contains(ns))
        .collect();
    for ns in routed_ghosts {
        ctx.shard
            .enqueue_reconcile_op(ReconcileOp::RemoveRouted { namespace: ns });
        counters.removed_routed += 1;
    }
}

/// Two-phase owned-partition teardown shared by the removals pass (a ghost
/// no longer in the committed target) and the additions pass (a stale
/// incarnation after slab-key reuse). Fences writes synchronously
/// (tombstone + `shards_table` row removal), unlinks the on-disk
/// hierarchy, then enqueues `ConfirmRemove` so the pump drops the
/// in-memory partition. On disk-delete failure the namespace stays
/// tombstoned + backed off and retries on a later pass; the in-memory
/// partition is never dropped before its data is gone.
async fn tear_down_owned_partition(
    ctx: &ReconcilerCtx,
    ns: IggyNamespace,
    counters: &mut PassCounters,
) {
    let shard_id = ctx.shard.id;
    let partitions = ctx.shard.plane.partitions();
    let shards_table = ctx.shard.shards_table();

    // Partition paths share one on-disk root across all shards on a node
    // (`get_partition_path` has no `shard_id` prefix), so a delete here
    // unlinks data any other shard owning the same ns would see. If hashing
    // now points at a peer (stale reader-mode STM during a
    // delete-then-recreate race, or a hash-function change across an
    // upgrade), refuse the delete and surface the inconsistency instead of
    // panicking the pump; the partition stays addressable via its existing
    // local entry until an operator resolves the conflict.
    let hash_owner = calculate_shard_assignment(&ns, u32::from(ctx.total_shards));
    if hash_owner != shard_id {
        ctx.shard.metrics().record_partition_reconcile_failure();
        error!(
            shard = shard_id,
            ns_raw = ns.inner(),
            hash_owner,
            "teardown target hashes to peer shard; refusing disk delete to avoid cross-shard data loss"
        );
        ctx.record_failure(ns, FailureCause::Delete, Instant::now());
        return;
    }

    let now = Instant::now();
    if ctx.is_backed_off(ns, FailureCause::Delete, now) {
        counters.backoff_skipped += 1;
        return;
    }

    // Fence writes BEFORE awaiting disk delete. Tombstone is RefCell
    // (cross-task callable) and shards_table is papaya, both safe to mutate
    // directly from the reconciler. Routing through the pump's ReconcileOp
    // queue here would race the unlink against in-flight on_request /
    // on_replicate / on_ack frames that haven't observed the queued
    // tombstone yet. Idempotent on retry: already-tombstoned namespace
    // stays tombstoned; already-removed shards_table row is a no-op.
    if !partitions.is_tombstoned(&ns) {
        partitions.tombstone(ns);
    }
    shards_table.remove(&ns);

    if let Err(err) = delete_partitions_from_disk(
        ns.stream_id(),
        ns.topic_id(),
        ns.partition_id(),
        ctx.config.as_ref(),
    )
    .await
    {
        ctx.record_failure(ns, FailureCause::Delete, now);
        ctx.shard.metrics().record_partition_reconcile_failure();
        error!(
            shard = shard_id,
            stream_id = ns.stream_id(),
            topic_id = ns.topic_id(),
            partition_id = ns.partition_id(),
            error = %err,
            "reconciler failed to delete partition directory"
        );
        return;
    }

    ctx.shard
        .enqueue_reconcile_op(ReconcileOp::ConfirmRemove { namespace: ns });
    ctx.record_success(ns, FailureCause::Delete);
    counters.removed_local += 1;
}

/// Reclaim consumer-group offsets left behind by a `DeleteConsumerGroup` whose
/// topic still exists (a topic/stream delete already drops the whole partition
/// directory, offsets included). For each owned partition, any stored
/// consumer-group offset whose group id is no longer present in the topic's
/// committed metadata is removed (in-memory entry + persisted file). Monotonic,
/// never-reused group ids make this purely reclamation -- a recreated group
/// gets a fresh id and never reads a dead group's offset -- so it is safe to do
/// lazily on the reconcile pass rather than synchronously on delete.
async fn reconcile_consumer_group_offsets(ctx: &ReconcilerCtx, counters: &mut PassCounters) {
    let live_groups = snapshot_topic_live_groups(ctx);
    let partitions = ctx.shard.plane.partitions();
    let owned: Vec<IggyNamespace> = partitions.namespaces().copied().collect();
    for ns in owned {
        let live = live_groups.get(&(ns.stream_id(), ns.topic_id()));
        // Take the in-memory removes + owned unlink paths under a closure-scoped
        // borrow that cannot escape into the await below. Holding a raw
        // `&IggyPartition` across `delete_persisted_offset().await` would let the
        // pump task realloc the partitions vec underneath us (a UAF).
        let paths = partitions.with_partition(&ns, |partition| {
            partition.reclaim_dead_group_offsets(|group_id| {
                live.is_some_and(|set| set.contains(&group_id))
            })
        });
        let Some(paths) = paths else {
            continue;
        };
        for path in paths {
            if let Err(err) = delete_persisted_offset(&path).await {
                warn!(
                    shard = ctx.shard.id,
                    ns_raw = ns.inner(),
                    error = %err,
                    "reconciler failed to reclaim deleted consumer-group offset"
                );
                continue;
            }
            counters.cg_offsets_purged += 1;
        }
    }
}

/// Complete cooperative consumer-group revocations whose source member has
/// drained the partition (`committed >= last_polled`), was never polled, or
/// timed out. Reads pending revocations from metadata + local partition offset
/// state, then submits a `CompleteRevocation` op to shard 0 (the metadata
/// consensus owner). Idempotent + fire-and-forget: a not-yet-completable or
/// transiently-failed revocation is retried next pass.
#[allow(clippy::cast_possible_truncation)]
fn reconcile_pending_revocations(ctx: &ReconcilerCtx) {
    let streams = ctx.shard.plane.metadata().mux_stm.streams();
    // O(1) fast-skip before the walk: `consumer_group_pending_revocations`
    // allocates a vec and walks every stream/topic/group/member, and the
    // reconciler hits this every tick. `has_pending_revocations` reads the
    // maintained counter, so the common (nothing-pending) case pays nothing.
    if !streams.has_pending_revocations() {
        return;
    }
    let pending = streams.consumer_group_pending_revocations();
    if pending.is_empty() {
        return;
    }
    let partitions = ctx.shard.plane.partitions();
    let now = IggyTimestamp::now().as_micros();
    let timeout = ctx.config.consumer_group.rebalancing_timeout.as_micros();
    for (stream_id, topic_id, group_id, source_client_id, partition_id, created_at) in pending {
        let ns = IggyNamespace::new(stream_id as usize, topic_id as usize, partition_id as usize);
        // The partition lives on its owner shard; only that shard's reconciler
        // can read its offsets. Other shards skip (the owner completes it).
        let Some(partition) = partitions.get_by_ns(&ns) else {
            continue;
        };
        let key = ConsumerGroupId(group_id as usize);
        let last_polled = partition
            .last_polled_offsets
            .pin()
            .get(&key)
            .map(|offset| offset.offset.load(std::sync::atomic::Ordering::Relaxed));
        let committed = partition
            .consumer_group_offsets
            .pin()
            .get(&key)
            .map(|offset| offset.offset.load(std::sync::atomic::Ordering::Relaxed));
        let timed_out = now.saturating_sub(created_at) >= timeout;
        // None: never polled -> nothing in flight, hand off now. Some(polled):
        // only safe once the source committed what it was served (or timeout).
        let completable =
            last_polled.is_none_or(|polled| committed.is_some_and(|c| c >= polled) || timed_out);
        if !completable {
            continue;
        }
        let (reply, _rx) = shard::channel::<Option<u64>>(1);
        ctx.shard
            .forward_metadata_submit(MetadataSubmit::CompleteRevocation {
                stream_id,
                topic_id,
                group_id,
                source_client_id,
                partition_id,
                reply,
            });
    }
}

/// `(stream_id, topic_id) -> live consumer-group offset keys` from committed
/// metadata. The partition plane keys a group's offset by the monotonic group
/// id (the store path is rewritten to it; the read path resolves it), so the
/// live-set carries those ids too -- otherwise the reconciler would treat every
/// live offset as orphaned and purge it.
fn snapshot_topic_live_groups(ctx: &ReconcilerCtx) -> AHashMap<(usize, usize), AHashSet<u64>> {
    ctx.shard.plane.metadata().mux_stm.streams().read(|inner| {
        let mut map: AHashMap<(usize, usize), AHashSet<u64>> = AHashMap::new();
        for (_, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                if topic.consumer_groups.is_empty() {
                    continue;
                }
                map.insert(
                    (stream.id, topic_id),
                    topic
                        .consumer_groups
                        .values()
                        .map(|group| group.id)
                        .collect(),
                );
            }
        }
        map
    })
}

/// Committed `(namespace, created_revision)` pairs. The epoch lets the
/// additions pass detect a stale local incarnation after slab-key reuse
/// without an `Arc<TopicStats>` clone per partition; stats are fetched
/// lazily in [`fetch_partition_stats`] only for namespaces actually built.
fn snapshot_target_namespaces(ctx: &ReconcilerCtx) -> Vec<(IggyNamespace, u64)> {
    ctx.shard.plane.metadata().mux_stm.streams().read(|inner| {
        // TODO(krishna): O(committed partitions) per non-skipped pass (here +
        // reconcile_removals). The revision fast-skip hides this in steady
        // state but not under sustained churn; switch to an incremental diff
        // keyed on the changed namespaces if it bottlenecks large clusters.
        let mut entries = Vec::new();
        for (_, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                for partition in &topic.partitions {
                    let ns = IggyNamespace::new(stream.id, topic_id, partition.id);
                    entries.push((ns, partition.created_revision));
                }
            }
        }
        entries
    })
}

/// Monotonic `Streams::revision`. Stable between passes iff no
/// partition-shaping op committed since, which is the fast-skip signal.
fn current_revision(ctx: &ReconcilerCtx) -> u64 {
    ctx.shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|inner| inner.revision)
}

/// Clone the parent topic's `Arc<TopicStats>` for a single namespace.
/// `None` if the topic vanished between the target snapshot and this read.
fn fetch_partition_stats(
    ctx: &ReconcilerCtx,
    ns: IggyNamespace,
) -> Option<Arc<iggy_common::PartitionStats>> {
    ctx.shard.plane.metadata().mux_stm.streams().read(|inner| {
        let stream = inner.items.get(ns.stream_id())?;
        let topic = stream.topics.get(ns.topic_id())?;
        // Get-or-create in the shared registry so the owning shard's counters
        // are the same `Arc` every shard's `get_topic` reply reads.
        Some(inner.stats_registry.partition(
            ns.stream_id(),
            ns.topic_id(),
            ns.partition_id(),
            topic.stats.clone(),
        ))
    })
}

fn shards_table_contains(ctx: &ReconcilerCtx, ns: IggyNamespace) -> bool {
    ctx.shard.shards_table().shard_for(ns).is_some()
}

/// Enforce committed `TruncatePartition` watermarks: for each owned partition
/// carrying a non-zero delete watermark, stage a pump-side trim to that offset.
/// Idempotent — the pump no-ops once a partition is trimmed past the watermark,
/// so a redundant pass triggered by an unrelated revision bump is harmless.
/// A watermark whose enforcement is still incomplete (first local segment
/// starts below it) counts as pending work: the pump may be blocked by a
/// consumer barrier or by a rejoin whose offsets arrive via journal repair,
/// and neither unblocking bumps `Streams::revision`, so the pass must keep
/// the reconciler ticking until the layout converges.
fn reconcile_segment_truncations(ctx: &ReconcilerCtx, counters: &mut PassCounters) {
    let partitions = ctx.shard.plane.partitions();
    let namespaces: Vec<_> = partitions.namespaces().copied().collect();
    let streams = ctx.shard.plane.metadata().mux_stm.streams();
    for namespace in namespaces {
        let watermark = streams.partition_delete_watermark(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
        );
        if watermark == 0 {
            continue;
        }
        ctx.shard.request_truncate_partition(namespace, watermark);
        let trimmed = partitions
            .get_by_ns(&namespace)
            .and_then(|partition| partition.log.segments().first())
            .is_none_or(|first| first.start_offset >= watermark);
        if !trimmed {
            counters.trims_pending += 1;
        }
    }
}

/// Stage a `PurgePartition` reset for every owned partition whose committed
/// `PurgeTopic` generation is newer than the one the local partition last
/// applied. The pump re-checks the generation before wiping, so a redundant
/// pass (e.g. from an unrelated revision bump) is a no-op.
fn reconcile_partition_purges(ctx: &ReconcilerCtx) {
    let partitions = ctx.shard.plane.partitions();
    let namespaces: Vec<_> = partitions.namespaces().copied().collect();
    let streams = ctx.shard.plane.metadata().mux_stm.streams();
    for namespace in namespaces {
        let committed = streams.partition_purge_generation(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
        );
        let applied = partitions
            .get_by_ns(&namespace)
            .map_or(0, partitions::IggyPartition::applied_purge_generation);
        if committed > applied {
            ctx.shard.request_purge_partition(namespace, committed);
        }
    }
}

pub fn install_tick_handler(shard: &Rc<ServerNgShard>, wake_tx: WakeTx) {
    let shard_id = shard.id;
    let handler = Rc::new(move || {
        if let Err(err) = wake_tx.try_send(()) {
            trace!(shard = shard_id, "tick wake dropped: {err}");
        }
    });
    shard.set_metadata_tick_handler(Some(handler));
}

#[cfg(test)]
mod tests {
    use super::{FailureCause, FailureRecord, ReconcilerCtx, reconcile_once};
    use configs::server_ng::{NgSystemConfig, ServerNgConfig};
    use consensus::{MetadataHandle, PartitionsHandle};
    use iggy_binary_protocol::codec::WireEncode;
    use iggy_binary_protocol::primitives::identifier::WireName;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::partitions::{
        CreatePartitionsRequest, CreatePartitionsWithAssignmentsRequest,
    };
    use iggy_binary_protocol::requests::streams::{CreateStreamRequest, DeleteStreamRequest};
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest, DeleteTopicRequest,
    };
    use iggy_binary_protocol::{Command2, Operation, PrepareHeader, WireIdentifier};
    use message_bus::IggyMessageBus;
    use metadata::IggyMetadata;
    use metadata::MuxStateMachine;
    use metadata::impls::metadata::IggySnapshot;
    use metadata::stm::StateMachine;
    use metadata::stm::stream::Streams;
    use metadata::stm::user::Users;
    use partitions::{IggyPartitions, PartitionsConfig};
    use server_common::Message;
    use server_common::sharding::{IggyNamespace, ShardId};
    use shard::shards_table::{PapayaShardsTable, ShardsTable, calculate_shard_assignment};
    use shard::{IggyShard, PartitionConsensusConfig, ShardIdentity};
    use std::mem::size_of;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Instant;
    use tempfile::TempDir;

    type TestMux = MuxStateMachine<iggy_common::variadic!(Users, Streams)>;
    type TestShard = IggyShard<
        Rc<IggyMessageBus>,
        journal::prepare_journal::PrepareJournal,
        IggySnapshot,
        TestMux,
        PapayaShardsTable,
    >;

    const CLUSTER_ID: u128 = 1;

    /// Sanity test that ensures the `()` channel can coalesce wakes
    /// without blocking the producer when the consumer hasn't drained
    /// yet. Production behaviour relies on this: the metadata commit
    /// notifier runs on the metadata commit path and cannot await.
    #[test]
    fn wake_channel_coalesces_drops_when_full() {
        let (tx, rx) = shard::channel::<()>(1);
        assert!(tx.try_send(()).is_ok());
        assert!(
            tx.try_send(()).is_err(),
            "second send must fail; capacity 1 enforces coalescing"
        );
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    /// Build a `Message<PrepareHeader>` carrying `request` as its body and
    /// `operation` stamped in the header. Bypasses the VSR pipeline (no
    /// journal, no view, no client): the state machine reads only
    /// `header.operation` and `header.size`, so the rest is left zeroed.
    fn build_prepare<R: WireEncode>(
        op: u64,
        operation: Operation,
        request: &R,
    ) -> Message<PrepareHeader> {
        let body = request.to_bytes();
        let header_size = size_of::<PrepareHeader>();
        let total_size = header_size + body.len();
        let mut msg = Message::<PrepareHeader>::new(total_size);
        msg.as_mut_slice()[header_size..total_size].copy_from_slice(&body);
        let header = bytemuck::checked::try_from_bytes_mut::<PrepareHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes form a valid PrepareHeader");
        header.command = Command2::Prepare;
        header.size = u32::try_from(total_size).expect("prepare size fits u32");
        header.op = op;
        header.operation = operation;
        msg
    }

    fn assignment(partition_id: u32, consensus_group_id: u64) -> CreatedPartitionAssignment {
        CreatedPartitionAssignment {
            partition_id,
            consensus_group_id,
        }
    }

    /// Drive a `CreateStream` commit through the state machine. The STM
    /// assigns slab keys from 0 for the first stream on a fresh STM.
    fn seed_stream(mux: &TestMux, op: u64, name: &str) {
        let req = CreateStreamRequest {
            name: WireName::new(name).expect("test stream name fits WireName"),
        };
        mux.update(build_prepare(op, Operation::CreateStream, &req))
            .expect("CreateStream apply succeeds");
    }

    /// Drive a `CreateTopicWithAssignments` commit.
    fn seed_topic(
        mux: &TestMux,
        op: u64,
        stream_id: u32,
        name: &str,
        assignments: Vec<CreatedPartitionAssignment>,
    ) {
        let req = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(stream_id),
                partitions_count: u32::try_from(assignments.len())
                    .expect("partitions count fits u32"),
                compression_algorithm: 0,
                message_expiry: 0,
                max_topic_size: 0,
                replication_factor: 1,
                name: WireName::new(name).expect("test topic name fits WireName"),
            },
            partitions: assignments,
        };
        mux.update(build_prepare(
            op,
            Operation::CreateTopicWithAssignments,
            &req,
        ))
        .expect("CreateTopicWithAssignments apply succeeds");
    }

    fn seed_delete_topic(mux: &TestMux, op: u64, stream_id: u32, topic_id: u32) {
        let req = DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(topic_id),
        };
        mux.update(build_prepare(op, Operation::DeleteTopic, &req))
            .expect("DeleteTopic apply succeeds");
    }

    fn seed_delete_stream(mux: &TestMux, op: u64, stream_id: u32) {
        let req = DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(stream_id),
        };
        mux.update(build_prepare(op, Operation::DeleteStream, &req))
            .expect("DeleteStream apply succeeds");
    }

    fn seed_create_consumer_group(
        mux: &TestMux,
        op: u64,
        stream_id: u32,
        topic_id: u32,
        name: &str,
    ) {
        use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
        let req = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(topic_id),
            name: WireName::new(name).expect("test group name fits WireName"),
        };
        mux.update(build_prepare(op, Operation::CreateConsumerGroup, &req))
            .expect("CreateConsumerGroup apply succeeds");
    }

    fn seed_delete_consumer_group(
        mux: &TestMux,
        op: u64,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    ) {
        use iggy_binary_protocol::requests::consumer_groups::DeleteConsumerGroupRequest;
        let req = DeleteConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(topic_id),
            group_id: WireIdentifier::numeric(group_id),
        };
        mux.update(build_prepare(op, Operation::DeleteConsumerGroup, &req))
            .expect("DeleteConsumerGroup apply succeeds");
    }

    fn seed_join_consumer_group(
        mux: &TestMux,
        op: u64,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
        client_id: u128,
    ) {
        use metadata::stm::consumer_group::JoinConsumerGroupRequest;
        let req = JoinConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(topic_id),
            group_id: WireIdentifier::numeric(group_id),
            client_id,
            in_flight: Vec::new(),
        };
        mux.update(build_prepare(op, Operation::JoinConsumerGroup, &req))
            .expect("JoinConsumerGroup apply succeeds");
    }

    fn test_config(tmp: &TempDir) -> ServerNgConfig {
        let mut cfg = ServerNgConfig::default();
        // `NgSystemConfig` is not `Clone`, so `Arc::make_mut` is out; build a
        // fresh value via struct-update syntax and swap the Arc wholesale.
        // Only `path` differs from the default; every other field uses the
        // runtime's defaults.
        let system = NgSystemConfig {
            path: tmp.path().to_string_lossy().into_owned(),
            ..NgSystemConfig::default()
        };
        cfg.system = Arc::new(system);
        cfg
    }

    /// Assemble a fully functional `ServerNgShard` for reconciler tests.
    /// Uses `IggyShard::without_inbox` so no inter-shard pump runs; the
    /// reconciler can be driven directly by `reconcile_once`.
    fn build_test_shard(shard_id: u16, config: &ServerNgConfig, mux: TestMux) -> Rc<TestShard> {
        let bus = Rc::new(IggyMessageBus::with_config(shard_id, config));
        let metadata: IggyMetadata<
            consensus::VsrConsensus<Rc<IggyMessageBus>>,
            journal::prepare_journal::PrepareJournal,
            IggySnapshot,
            _,
        > = IggyMetadata::new(None, None, None, mux, None);
        let partitions = IggyPartitions::new(
            ShardId::new(shard_id),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                enforce_fsync: false,
                segment_size: config.system.segment.size,
                encryptor: None,
            },
        );
        let shards_table = PapayaShardsTable::new();
        let partition_consensus = PartitionConsensusConfig::new(
            CLUSTER_ID,
            shard::ReplicaTopology::new(0, 1),
            Rc::clone(&bus),
        );
        let shard = TestShard::without_inbox(
            ShardIdentity::new(shard_id, format!("test-shard-{shard_id}")),
            Rc::clone(&bus),
            metadata,
            partitions,
            shards_table,
            partition_consensus,
        );
        Rc::new(shard)
    }

    fn make_ctx(
        shard: Rc<TestShard>,
        total_shards: u16,
        config: Rc<ServerNgConfig>,
    ) -> Rc<ReconcilerCtx> {
        Rc::new(ReconcilerCtx::new(
            shard,
            total_shards,
            config,
            CLUSTER_ID,
            0,
            1,
        ))
    }

    /// Tests run reconcile + pump-side apply inline since no real pump exists.
    async fn reconcile_pass(ctx: &ReconcilerCtx) {
        reconcile_once(ctx).await;
        ctx.shard.apply_reconcile_ops();
    }

    /// Single-shard scenario: every committed partition is owned locally.
    /// After one reconcile pass every namespace must be materialised in
    /// `partitions` and addressable through `shards_table`. Disk
    /// hierarchy is created under the tempdir's system path; idempotent
    /// retries are exercised by a second pass.
    #[compio::test]
    async fn reconcile_materialises_owned_partitions_single_shard() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-a");
        seed_topic(
            &mux,
            2,
            0,
            "topic-a",
            vec![assignment(0, 1), assignment(1, 2), assignment(2, 3)],
        );

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;

        let partitions = shard.plane.partitions();
        let shards_table = shard.shards_table();
        for partition_id in 0..3 {
            let ns = IggyNamespace::new(0, 0, partition_id);
            assert!(
                partitions.contains(&ns),
                "namespace {ns:?} must be materialised on its owning shard"
            );
            assert_eq!(
                shards_table.shard_for(ns),
                Some(0),
                "shards_table must point at the owning shard"
            );
        }
        assert_eq!(partitions.len(), 3, "exactly three partitions materialised");

        // Idempotency: a second pass with no new commits must not double-
        // insert or re-create disk hierarchy.
        reconcile_pass(&ctx).await;
        assert_eq!(
            partitions.len(),
            3,
            "second pass over an unchanged target must be a no-op"
        );
    }

    /// Regression (deferred-apply window): the reconciler stages
    /// `ReconcileOp::InsertOwned` from a task separate from the pump that
    /// applies it, so under a commit burst it can run a second pass before
    /// the pump drains the first pass's staged ops. Both passes then
    /// observe `!contains(ns)` and build the same namespace. The pump's
    /// apply must be idempotent, else the second `insert` orphans the first
    /// partition (leaked VSR group + writers) and inflates `len`.
    /// `reconcile_pass` applies inline and cannot surface this, so here we
    /// run two passes and only then drain once.
    #[compio::test]
    async fn deferred_apply_window_does_not_duplicate_owned_partition() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-a");
        seed_topic(
            &mux,
            2,
            0,
            "topic-a",
            vec![assignment(0, 1), assignment(1, 2)],
        );

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        // Two passes with no pump drain in between: models the reconciler,
        // woken by a second commit tick, running pass N+1 before the pump
        // applies pass N's `InsertOwned`. Both passes see the namespaces as
        // unmaterialised and stage a build for each, so the queue holds two
        // `InsertOwned` per namespace when the pump finally drains.
        reconcile_once(&ctx).await;
        reconcile_once(&ctx).await;

        ctx.shard.apply_reconcile_ops();

        let partitions = shard.plane.partitions();
        assert_eq!(
            partitions.len(),
            2,
            "deferred-apply window must not duplicate partitions: \
             each namespace materialises exactly once"
        );
        for partition_id in 0..2 {
            let ns = IggyNamespace::new(0, 0, partition_id);
            assert!(
                partitions.contains(&ns),
                "namespace {ns:?} must be addressable exactly once"
            );
            assert_eq!(
                shard.shards_table().shard_for(ns),
                Some(0),
                "shards_table must point at the owning shard"
            );
        }
    }

    /// Multi-shard scenario: only the partition whose hash maps to
    /// `self.shard_id` is materialised; every other namespace gets a
    /// `shards_table` row pointing at the owning shard but no
    /// `IggyPartition` instance.
    #[compio::test]
    async fn reconcile_only_materialises_namespaces_owned_by_self() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let total_shards: u16 = 4;

        // Pick a partition count where the murmur3 distribution lands
        // entries on at least two distinct shards out of four, then
        // run the test against the most-loaded shard. This makes the
        // assertion "self_owned > 0 && routed_only > 0" structural
        // rather than dependent on a fixed shard_id matching the
        // arbitrary hash output.
        let partition_count: u32 = 16;
        let mut counts: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
        for partition_id in 0..partition_count {
            let ns = IggyNamespace::new(0, 0, partition_id as usize);
            *counts
                .entry(calculate_shard_assignment(&ns, u32::from(total_shards)))
                .or_insert(0) += 1;
        }
        let (shard_id, _) = counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(s, c)| (*s, *c))
            .expect("hash distribution must populate at least one shard");
        assert!(
            counts.len() >= 2,
            "test partition count must yield a multi-shard distribution; got {counts:?}"
        );

        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-shard-aware");
        let assignments: Vec<CreatedPartitionAssignment> = (0..partition_count)
            .map(|partition_id| assignment(partition_id, u64::from(partition_id) + 10))
            .collect();
        seed_topic(&mux, 2, 0, "topic-shard-aware", assignments);

        let shard = build_test_shard(shard_id, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), total_shards, Rc::new(config));

        reconcile_pass(&ctx).await;

        let partitions = shard.plane.partitions();
        let shards_table = shard.shards_table();
        let mut owned = 0usize;
        let mut routed_only = 0usize;
        for partition_id in 0..partition_count {
            let ns = IggyNamespace::new(0, 0, partition_id as usize);
            let expected_owner = calculate_shard_assignment(&ns, u32::from(total_shards));
            if expected_owner == shard_id {
                assert!(
                    partitions.contains(&ns),
                    "namespace {ns:?} owned by self must be materialised"
                );
                owned += 1;
            } else {
                assert!(
                    !partitions.contains(&ns),
                    "namespace {ns:?} owned by shard {expected_owner} \
                     must NOT be materialised on shard {shard_id}"
                );
                routed_only += 1;
            }
            assert_eq!(
                shards_table.shard_for(ns),
                Some(expected_owner),
                "shards_table must always resolve the owning shard"
            );
        }
        assert_eq!(
            partitions.len(),
            owned,
            "IggyPartitions size must match the count of self-owned namespaces"
        );
        assert!(
            owned > 0,
            "test must run on a shard that owns ≥ 1 partition"
        );
        assert!(
            routed_only > 0,
            "test must run with ≥ 1 partition owned by another shard"
        );
    }

    /// `CreatePartitions` on an existing topic adds new namespaces; the
    /// reconciler picks them up on the next pass without touching the
    /// partitions it already materialised.
    #[compio::test]
    async fn reconcile_picks_up_create_partitions_increments() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-b");
        seed_topic(
            &mux,
            2,
            0,
            "topic-b",
            vec![assignment(0, 1), assignment(1, 2)],
        );

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;
        assert_eq!(shard.plane.partitions().len(), 2);

        // Now commit two additional partitions on the same topic.
        // `CreatePartitionsWithAssignments` applies request-relative
        // offsets, so partition_id=0,1 below resolve to absolute ids
        // 2,3 once the STM adds the base offset.
        shard
            .plane
            .metadata()
            .mux_stm
            .update(build_prepare(
                3,
                Operation::CreatePartitionsWithAssignments,
                &CreatePartitionsWithAssignmentsRequest {
                    request: CreatePartitionsRequest {
                        stream_id: WireIdentifier::numeric(0),
                        topic_id: WireIdentifier::numeric(0),
                        partitions_count: 2,
                    },
                    partitions: vec![assignment(0, 3), assignment(1, 4)],
                },
            ))
            .expect("CreatePartitions apply succeeds");

        reconcile_pass(&ctx).await;
        assert_eq!(
            shard.plane.partitions().len(),
            4,
            "reconciler must materialise the two new partitions"
        );
        for partition_id in 0..4 {
            let ns = IggyNamespace::new(0, 0, partition_id);
            assert!(
                shard.plane.partitions().contains(&ns),
                "namespace {ns:?} must be materialised after CreatePartitions"
            );
        }
    }

    /// `DeleteTopic` removes every partition under the topic on the next
    /// reconcile pass: owning shard drops the `IggyPartition`, every
    /// shard prunes its `shards_table` row, and the on-disk hierarchy
    /// is removed.
    #[compio::test]
    async fn reconcile_removes_partitions_on_delete_topic() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-c");
        seed_topic(
            &mux,
            2,
            0,
            "topic-c",
            vec![assignment(0, 1), assignment(1, 2)],
        );

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;
        // Verify disk hierarchy exists before the delete commits.
        let partition_root_before = ctx.config.system.get_partition_path(0, 0, 0);
        assert!(
            std::path::Path::new(&partition_root_before).exists(),
            "partition directory must exist post-materialisation"
        );

        seed_delete_topic(&shard.plane.metadata().mux_stm, 3, 0, 0);
        reconcile_pass(&ctx).await;

        assert_eq!(
            shard.plane.partitions().len(),
            0,
            "DeleteTopic must drop every partition under it"
        );
        for partition_id in 0..2 {
            let ns = IggyNamespace::new(0, 0, partition_id);
            assert!(
                !shard.plane.partitions().contains(&ns),
                "namespace {ns:?} must be removed from IggyPartitions"
            );
            assert_eq!(
                shard.shards_table().shard_for(ns),
                None,
                "shards_table row must be pruned for {ns:?}"
            );
            let path = ctx.config.system.get_partition_path(
                ns.stream_id(),
                ns.topic_id(),
                ns.partition_id(),
            );
            assert!(
                !std::path::Path::new(&path).exists(),
                "on-disk hierarchy for {ns:?} must be removed"
            );
        }
    }

    /// `DeleteStream` removes everything beneath it in one shot: every
    /// topic, every partition, every routing row.
    #[compio::test]
    async fn reconcile_removes_partitions_on_delete_stream() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-d");
        seed_topic(
            &mux,
            2,
            0,
            "topic-d1",
            vec![assignment(0, 1), assignment(1, 2)],
        );
        seed_topic(&mux, 3, 0, "topic-d2", vec![assignment(0, 3)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;
        assert_eq!(
            shard.plane.partitions().len(),
            3,
            "two topics × (2+1) partitions must materialise before delete"
        );

        seed_delete_stream(&shard.plane.metadata().mux_stm, 4, 0);
        reconcile_pass(&ctx).await;
        assert_eq!(
            shard.plane.partitions().len(),
            0,
            "DeleteStream must remove every partition transitively"
        );
        assert!(
            shard.shards_table().namespaces().is_empty(),
            "shards_table must be empty after DeleteStream"
        );
    }

    /// A delete+recreate of the same (stream, topic, partition) tuple
    /// reuses the freed slab key, so the namespace is byte-identical but
    /// its committed `created_revision` is greater. The reconciler must
    /// notice the stale local partition (old segments / offsets / log),
    /// tear it down, and rebuild fresh rather than keep serving the prior
    /// incarnation under the recycled identity.
    #[compio::test]
    async fn reconcile_rebuilds_stale_partition_after_slab_key_reuse() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-reuse");
        seed_topic(&mux, 2, 0, "topic-reuse", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;
        let ns = IggyNamespace::new(0, 0, 0);
        assert!(shard.plane.partitions().contains(&ns));
        let epoch_before = shard
            .shards_table()
            .epoch_for(ns)
            .expect("materialised row carries an epoch");

        // Delete then recreate the SAME tuple. The STM frees + reuses
        // topic slab key 0, so `ns` is identical but `created_revision`
        // is strictly greater. The reconciler never ran between the two
        // commits, so the stale partition is still materialised here.
        seed_delete_topic(&shard.plane.metadata().mux_stm, 3, 0, 0);
        seed_topic(
            &shard.plane.metadata().mux_stm,
            4,
            0,
            "topic-reuse",
            vec![assignment(0, 1)],
        );

        // Pass 1: detect the stale incarnation and tear it down. The
        // absent partition afterwards proves the old one was dropped, not
        // merely left in place.
        reconcile_pass(&ctx).await;
        assert!(
            !shard.plane.partitions().contains(&ns),
            "stale partition must be torn down before rebuild"
        );

        // Pass 2: rebuild fresh at the new epoch.
        reconcile_pass(&ctx).await;
        assert!(
            shard.plane.partitions().contains(&ns),
            "fresh partition must materialise after the teardown"
        );
        let epoch_after = shard.shards_table().epoch_for(ns);
        assert!(epoch_after.is_some(), "rebuilt row must carry an epoch");
        assert_ne!(
            epoch_after,
            Some(epoch_before),
            "rebuilt row must carry a new epoch, proving the stale partition was replaced"
        );
    }

    /// Once converged, a pass with an unchanged
    /// `Streams::revision` fast-skips the O(N) diff instead of re-scanning
    /// every committed namespace every periodic tick. A fresh
    /// partition-shaping commit bumps the revision and defeats the skip.
    #[compio::test]
    async fn reconcile_fast_skips_when_revision_unchanged() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-skip");
        seed_topic(&mux, 2, 0, "topic-skip", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        // First pass materialises (work); the verify pass that follows a
        // working pass does nothing and arms the fast-skip.
        assert!(reconcile_once(&ctx).await, "first pass must run");
        ctx.shard.apply_reconcile_ops();
        assert!(
            reconcile_once(&ctx).await,
            "the verify pass after a working pass must still run"
        );
        ctx.shard.apply_reconcile_ops();

        // No commit since: revision unchanged + last pass a no-op → skip.
        assert!(
            !reconcile_once(&ctx).await,
            "unchanged revision after convergence must fast-skip the diff"
        );

        // A new partition-shaping commit bumps the revision → next pass runs.
        seed_topic(
            &shard.plane.metadata().mux_stm,
            3,
            0,
            "topic-skip-2",
            vec![assignment(0, 2)],
        );
        assert!(
            reconcile_once(&ctx).await,
            "a fresh commit must defeat the fast-skip"
        );
    }

    /// Permanent-tombstone-wedge regression: a teardown whose disk delete
    /// fails sets the tombstone and removes the `shards_table` row but never
    /// enqueues `ConfirmRemove`, so the tombstone never lifts. If the same
    /// `(stream, topic, partition)` is then recreated, `ns` is back in the
    /// committed target: the additions pass used to see `contains +
    /// is_tombstoned` and defer forever while the removals pass no longer
    /// treated `ns` as a ghost, fencing the partition for good and dropping
    /// every data-plane frame. The additions pass must instead notice the
    /// recorded delete failure (no `ConfirmRemove` in flight) and re-drive
    /// teardown, retrying the delete so the partition recovers.
    #[compio::test]
    async fn reconcile_recovers_permanently_wedged_tombstone() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-wedge");
        seed_topic(&mux, 2, 0, "topic-wedge", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;
        let ns = IggyNamespace::new(0, 0, 0);
        let partitions = shard.plane.partitions();
        assert!(partitions.contains(&ns));
        let partition_root = ctx.config.system.get_partition_path(0, 0, 0);
        assert!(std::path::Path::new(&partition_root).exists());

        // Reconstruct the post-failed-teardown state: tombstone set +
        // shards_table row gone + a `FailureCause::Delete` record, but the
        // partition still in the map and its directory still on disk (the
        // disk delete "failed"). `ns` is still in the committed target, so
        // this is the recreate-after-failed-delete shape. The injected
        // record's `next_retry_at` is captured now, so it is already due by
        // the time teardown checks the backoff (the monotonic clock only
        // advances).
        partitions.tombstone(ns);
        shard.shards_table().remove(&ns);
        ctx.failure_state.borrow_mut().insert(
            (ns, FailureCause::Delete),
            FailureRecord {
                attempts: 1,
                next_retry_at: Instant::now(),
            },
        );

        // Pass 1: additions must re-drive teardown (the delete now succeeds,
        // the directory is present), enqueue `ConfirmRemove`, and the inline
        // pump drops the partition + clears the tombstone. Without the fix
        // this pass defers and leaves the partition tombstoned forever.
        reconcile_pass(&ctx).await;
        assert!(
            !partitions.contains(&ns),
            "re-driven teardown must drop the wedged partition"
        );
        assert!(
            !partitions.is_tombstoned(&ns),
            "ConfirmRemove must clear the tombstone once the delete succeeds"
        );
        assert!(
            !std::path::Path::new(&partition_root).exists(),
            "re-driven teardown must delete the on-disk hierarchy"
        );

        // Pass 2: with the tombstone cleared the partition rebuilds fresh
        // and is addressable again.
        reconcile_pass(&ctx).await;
        assert!(
            partitions.contains(&ns),
            "partition must rebuild fresh after the wedge is cleared"
        );
        assert!(!partitions.is_tombstoned(&ns));
        assert_eq!(
            shard.shards_table().shard_for(ns),
            Some(0),
            "rebuilt partition must be addressable through shards_table"
        );
        assert!(
            std::path::Path::new(&partition_root).exists(),
            "rebuilt partition must recreate its on-disk hierarchy"
        );
    }

    /// The wedge fix must not break the legitimate defer: when teardown's
    /// disk delete SUCCEEDED a `ConfirmRemove` is in flight, so the
    /// additions pass must still defer the rebuild to the post-drain wake
    /// rather than re-driving teardown. The absence of a
    /// `FailureCause::Delete` record is exactly what separates this from the
    /// wedge, so none is injected here.
    #[compio::test]
    async fn reconcile_defers_rebuild_while_confirm_remove_in_flight() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-defer");
        seed_topic(&mux, 2, 0, "topic-defer", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;
        let ns = IggyNamespace::new(0, 0, 0);
        let partitions = shard.plane.partitions();
        assert!(partitions.contains(&ns));
        let partition_root = ctx.config.system.get_partition_path(0, 0, 0);

        // Post-successful-teardown, pre-drain state: tombstone set +
        // shards_table row gone, NO delete failure (the disk delete
        // succeeded and a `ConfirmRemove` is queued). The partition is left
        // in the map to model the not-yet-drained pump queue.
        partitions.tombstone(ns);
        shard.shards_table().remove(&ns);

        // A pass with no inline drain must defer: the partition stays in the
        // map, stays tombstoned, and its directory is untouched (teardown
        // was NOT re-driven).
        reconcile_once(&ctx).await;
        assert!(
            partitions.contains(&ns),
            "defer must leave the partition in the map"
        );
        assert!(
            partitions.is_tombstoned(&ns),
            "defer must not clear the tombstone"
        );
        assert!(
            std::path::Path::new(&partition_root).exists(),
            "defer must not re-drive teardown: the directory must remain"
        );
    }

    /// A bare `DeleteConsumerGroup` (topic survives) leaves the group's offsets
    /// on the partition. The reconciler must reclaim a deleted group's offset
    /// while leaving a still-live group's offset untouched.
    #[compio::test]
    async fn reconcile_reclaims_offsets_of_deleted_consumer_group() {
        use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset};

        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-cg");
        seed_topic(&mux, 2, 0, "topic-cg", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        reconcile_pass(&ctx).await;

        let ns = IggyNamespace::new(0, 0, 0);
        assert!(shard.plane.partitions().contains(&ns));

        // Two groups: "dead" gets id 1, "live" gets id 2 (per-topic monotonic).
        let stm = &shard.plane.metadata().mux_stm;
        seed_create_consumer_group(stm, 3, 0, 0, "dead");
        seed_create_consumer_group(stm, 4, 0, 0, "live");

        // Offsets are keyed by the monotonic group id (the id the store path is
        // rewritten to and the read path / live-set resolve), not the name hash.
        let dead_key: u32 = 1;
        let live_key: u32 = 2;
        {
            let partitions = shard.plane.partitions();
            let partition = partitions.get_by_ns(&ns).expect("partition materialised");
            partition.consumer_group_offsets.pin().insert(
                ConsumerGroupId(dead_key as usize),
                ConsumerOffset::new(ConsumerKind::ConsumerGroup, dead_key, 7, String::new()),
            );
            partition.consumer_group_offsets.pin().insert(
                ConsumerGroupId(live_key as usize),
                ConsumerOffset::new(ConsumerKind::ConsumerGroup, live_key, 9, String::new()),
            );
        }

        // Delete the "dead" group (id 1); "live" (id 2) stays.
        seed_delete_consumer_group(stm, 5, 0, 0, 1);
        reconcile_pass(&ctx).await;

        let partitions = shard.plane.partitions();
        let partition = partitions
            .get_by_ns(&ns)
            .expect("partition still materialised");
        let mut ids = partition.consumer_group_offset_ids();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![u64::from(live_key)],
            "deleted group's offset reclaimed; live group's offset retained"
        );
    }

    /// A partition-count change must re-run consumer-group assignment: a new
    /// partition gets assigned, a removed one is dropped. Pure metadata test --
    /// the assignment lives in the Streams STM.
    #[compio::test]
    async fn create_delete_partitions_reassigns_consumer_group() {
        use metadata::impls::metadata::StreamsFrontend;

        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-rp");
        seed_topic(
            &mux,
            2,
            0,
            "topic-rp",
            vec![assignment(0, 1), assignment(1, 2)],
        );
        seed_create_consumer_group(&mux, 3, 0, 0, "cg");
        // Single member owns every partition (group id 1, the first in topic).
        seed_join_consumer_group(&mux, 4, 0, 0, 1, 100);

        let group = WireIdentifier::numeric(1);
        let stream = WireIdentifier::numeric(0);
        let topic = WireIdentifier::numeric(0);
        let assigned = |mux: &TestMux| -> Vec<u32> {
            let (_, mut partitions) = mux
                .streams()
                .consumer_group_member_assignment(&stream, &topic, &group, 100)
                .expect("member assignment present");
            partitions.sort_unstable();
            partitions
        };
        assert_eq!(
            assigned(&mux),
            vec![0, 1],
            "joined member owns both partitions"
        );

        // Add one partition (request-relative id 0 rebases to absolute id 2).
        mux.update(build_prepare(
            5,
            Operation::CreatePartitionsWithAssignments,
            &CreatePartitionsWithAssignmentsRequest {
                request: CreatePartitionsRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partitions_count: 1,
                },
                partitions: vec![assignment(0, 3)],
            },
        ))
        .expect("CreatePartitions apply succeeds");
        assert_eq!(
            assigned(&mux),
            vec![0, 1, 2],
            "added partition must be reassigned to the member"
        );

        // Remove one partition; the member drops the highest id.
        mux.update(build_prepare(
            6,
            Operation::DeletePartitions,
            &iggy_binary_protocol::requests::partitions::DeletePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 1,
            },
        ))
        .expect("DeletePartitions apply succeeds");
        assert_eq!(
            assigned(&mux),
            vec![0, 1],
            "removed partition must be dropped from the assignment"
        );
    }

    /// A disconnect (`remove_consumer_group_member`) drops the client from
    /// every group it joined and rebalances its partitions onto the survivors.
    #[compio::test]
    async fn disconnect_removes_member_from_groups_and_rebalances() {
        use metadata::impls::metadata::StreamsFrontend;

        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-dc");
        seed_topic(
            &mux,
            2,
            0,
            "topic-dc",
            vec![assignment(0, 1), assignment(1, 2)],
        );
        seed_create_consumer_group(&mux, 3, 0, 0, "cg"); // group id 1
        seed_join_consumer_group(&mux, 4, 0, 0, 1, 100);
        seed_join_consumer_group(&mux, 5, 0, 0, 1, 200);

        let stream = WireIdentifier::numeric(0);
        let topic = WireIdentifier::numeric(0);
        let group = WireIdentifier::numeric(1);
        let assigned = |client: u128| -> Option<Vec<u32>> {
            mux.streams()
                .consumer_group_member_assignment(&stream, &topic, &group, client)
                .map(|(_, mut partitions)| {
                    partitions.sort_unstable();
                    partitions
                })
        };
        // Two members, two partitions: each owns one.
        assert_eq!(assigned(100).map(|p| p.len()), Some(1));
        assert_eq!(assigned(200).map(|p| p.len()), Some(1));

        // Client 100 disconnects.
        mux.streams()
            .remove_consumer_group_member(100, iggy_common::IggyTimestamp::default());

        assert_eq!(
            assigned(100),
            None,
            "disconnected client must leave the group"
        );
        assert_eq!(
            assigned(200),
            Some(vec![0, 1]),
            "survivor must take over the disconnected member's partitions"
        );
    }
}
