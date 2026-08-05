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
//! # Materialisation race: the reply ships before the partition exists
//!
//! `metadata::on_ack` fires the commit notifier and emits the wire reply
//! immediately after STM apply, but the owning shard's reconciler wakes
//! asynchronously and only enqueues `ReconcileOp::InsertOwned` once
//! `build_partition_fresh` finishes (mkdir + segment open + fallocate,
//! multi-millisecond). A client that produces the instant `create_topic`
//! returns therefore races the partition into existence, on every shard at
//! once.
//!
//! The race is closed at the **owning shard**, not by the routing table:
//!
//! - `router::route_typed` treats a missing row as "not seeded yet", not
//!   "unroutable", and falls back to `calculate_shard_assignment`. The frame
//!   always reaches the shard that will own the partition.
//! - `IggyShard::park_if_unmaterialised` holds it there until the matching
//!   `InsertOwned` lands, then re-queues it onto this shard's inbox -- but not to
//!   a DIFFERENT incarnation than the one it was addressed to. Each parked frame
//!   carries the committed `created_revision` observed when it was parked, and a
//!   drain whose epoch disagrees with that stamp answers the client instead of
//!   serving it: recycled slab keys make the namespace byte-identical, so such a
//!   frame would otherwise land a dead topic's write inside the topic that
//!   replaced it. One gap, recorded below: the stamp is re-derived if the frame
//!   re-enters the park path from the inbox. A frame parked with NO stamp is
//!   served; see `redispatch_parked_frames` for why a missing committed revision
//!   is not evidence of a prior incarnation. Re-queuing appends, so a parked
//!   frame is ordered behind whatever is already in the inbox. A frame the inbox
//!   refuses is re-parked rather than answered, since the deny would ride the
//!   same full sender, and the pump re-drives it (`retry_reparked_frames`) once
//!   a slot frees.
//! - `IggyShard::serves_committed_incarnation` refuses a namespace whose
//!   committed `created_revision` disagrees with the epoch on the local row, so
//!   a request arriving mid-teardown cannot be acked against the incarnation
//!   teardown is about to erase. It discriminates the shard's own state, not the
//!   frame's provenance, which is why the park stamp above is separate.
//! - Nothing is left unanswered: a tombstoned namespace, an overflowing park
//!   buffer, and a namespace this shard has given up materialising
//!   ([`reconcile_parked_frames`]) all reply with a retriable status, so a
//!   lockstep transport never waits out its read timeout on silence.
//!
//! `shards_table` is therefore a **cache of a deterministic hash**, never a
//! readiness proof: every shard derives the same rows from the same committed
//! metadata, and a row may exist before its partition does. Nothing may treat
//! presence as "the owner is ready" - `dispatch::wait_for_partition_routable`
//! documents why the owner-readiness probe that used to live there was both
//! unnecessary and ineffective.
//!
//! Keeping the table a hint is what makes it repairable: a pass that runs
//! re-derives the full row set from committed metadata, so a lost row is
//! rewritten. Note the qualifier -- the revision fast-skip below returns before
//! reading `shards_table` at all, so repair is driven by the signals that defeat
//! that skip (a partition-shaping commit, a pending retry, unfinished work, a
//! non-empty park buffer), not by every tick. An earlier design made the owner the sole writer and pushed
//! rows to peers to promote presence into a materialisation proof; it bought
//! nothing the owner-side fences above do not already guarantee, and it traded
//! that level-triggered repair for cross-core delta propagation that has to be
//! ordered, retried, and repaired to stay correct.
//!
//! Park residency is bounded on three axes, since the frame count alone bounds
//! nothing useful (`Message::into_generic` is a retag, so each entry keeps its
//! whole buffer, up to 64 MiB): a per-namespace frame cap, byte budgets per
//! namespace and per shard, and an age in reconciler passes.
//!
//! They apply asymmetrically, because the two frame classes fail differently. A
//! shed request costs a retry: answered with a retriable status, re-issued by
//! the SDK. A shed prepare is permanent loss on this replica, with no client to
//! answer and `consensus::retransmit_targets` skipping any op that already
//! reached quorum.
//!
//! All three bind a request: refused when admitting it would cross a byte budget
//! or the frame cap, answered past `MAX_PARKED_PASSES`. Only the byte budgets
//! bind a prepare, and only once one is spent. Excluding the frame cap is
//! deliberate: a header-only frame charges `MESSAGE_ALIGN`, so 128 is 512 KiB
//! against a 4 MiB namespace budget, and a shared cap would shed small prepares
//! before any byte budget spoke. Prepares admit 1024 header-only frames per
//! namespace, overshooting each budget by one frame rather than stopping at it,
//! which also makes an oversize frame parkable: against an empty entry a 5 MiB
//! append fails the per-namespace check every attempt. A parked prepare dies only
//! for a namespace this shard cannot serve, tombstoned or not hashing here.
//!
//! Everything leaving the buffer unserved is counted under
//! `frame_drops_total{variant=partition}`: `park_overflow` when shed on arrival,
//! `park_dropped` when it parked and then lost its namespace. A prepare has
//! nobody to answer, so the counter is the only record it existed.
//!
//! # Known gaps
//!
//! Recorded here because both were previously carried as a TODO on the
//! materialization barrier this module used to promise, and the barrier is gone
//! (see above) while these are not:
//!
//! TODO(krishna): a shed or discarded *prepare* has no recovery once its op has
//! reached quorum. `consensus::retransmit_targets` skips entries with
//! `ok_quorum_received`, and the partition plane creates a repair session only
//! in `on_start_view` -- `tick_partitions` re-drives an existing session but
//! cannot open one -- so the backup stays behind `commit_max` until an unrelated
//! view change. It needs a normal-status repair driver. The park policy above
//! shrinks the exposure to two cases, a genuinely exhausted byte budget and a
//! namespace this shard cannot serve, but only the repair driver removes it.
//!
//! TODO(krishna): the park stamp is not stable across re-entry. A re-dispatched
//! frame still in the inbox when a delete + recreate completes (`ConfirmRemove`
//! removes and untombstones in one arm, then the rebuild lands) re-enters
//! `park_if_unmaterialised` and is re-stamped with the NEW revision and
//! `passes: 0`, then served against the replacement: the write the stamp exists
//! to block. Narrow (a full delete + recreate has to finish while one frame
//! waits), but the guarantee is not absolute the way the bullet above reads.
//! Closing it needs the frame to carry provenance through the inbox instead of
//! re-deriving it on arrival.
//!
//! TODO(krishna): re-dispatch APPENDS to the inbox, so a parked prepare loses its
//! arrival position. `router.rs`'s `select_biased!` puts the consensus tick (which
//! runs `apply_reconcile_ops`, and with it the re-dispatch) above the inbox arm,
//! so a parked op N is re-queued *behind* an op N+1 that was already sitting in
//! the inbox. The partition plane then sees N+1 first, rejects it against its
//! backup gap check, and N+1 is gone -- with no normal-status repair driver to
//! refetch it (see the TODO below). Ordering has to be restored at the plane, by
//! buffering out-of-order prepares rather than dropping them, or by re-dispatching
//! through a priority path that preserves op order.
//!
//! TODO(krishna): `serves_committed_incarnation` and the park stamp both call
//! `Streams::created_revision_for_namespace`, now on the per-request fence path.
//! It indexes directly and falls back to a scan only if partition ids are not
//! dense, so the common case is O(1) -- but nothing in the type enforces that
//! density, and a future sparse layout silently reverts every fenced request to a
//! full scan. It wants a partition-id-keyed map in the STM.
//!
//! TODO(krishna): the transient deny answers with `IggyError::TransientNotAccepted`,
//! which the SDK treats as a leader-liveness signal. It replays same-session for
//! its `transient_deadline` first -- which is the right response and usually long
//! enough for the namespace to materialise -- but past that deadline `tcp_client`
//! runs `handle_leader_redirection` and reconnects, re-registering and losing the
//! session. Every cause of a park deny is node-local convergence, so that failover
//! cannot help; it needs a distinct "retry here shortly" code that does not move
//! the client.
//!
//! TODO(krishna): replicated traffic is deliberately exempt from the incarnation
//! fence, since a backup must apply whatever the primary admitted.
//! `PrepareHeader` carries no incarnation, so a backup still holding a prior one
//! cannot tell that an arriving prepare belongs to its replacement. Parked
//! prepares are covered by the epoch stamp above; one arriving against an
//! already-materialised stale incarnation is not. Closing it needs a wire-level
//! discriminator, like `checkpoint_id` on every prepare
//! -- `PrepareHeader.reserved` has room, but it is a `#[repr(C)]` wire change.

use crate::bootstrap::ServerNgShard;
use crate::partition_helpers::{build_partition_fresh, delete_partitions_from_disk};
use ahash::{AHashMap, AHashSet};
use configs::server_ng::ServerNgConfig;
use consensus::{MetadataHandle, PartitionsHandle};
use futures::FutureExt;
use iggy_common::{ConsumerGroupId, IggyTimestamp};
use message_bus::MessageBus;
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

/// Consecutive same-cause failures before [`ReconcilerCtx::record_failure`]
/// escalates to an operator-visible error (the backoff is capped, so
/// retries alone never surface a permanently failing partition).
const ESCALATE_AFTER_ATTEMPTS: u32 = 10;

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
        // Operator escalation: persistent on-disk corruption makes every
        // retry fail identically, and unlike the boot path (which refuses
        // loudly and fatally), this loop would hide the dead partition
        // behind per-attempt error logs forever. Escalate once the backoff
        // has long been at its ceiling, then again each doubling so a log
        // pipeline cannot miss it.
        if entry.attempts >= ESCALATE_AFTER_ATTEMPTS
            && (entry.attempts == ESCALATE_AFTER_ATTEMPTS || entry.attempts.is_power_of_two())
        {
            error!(
                namespace_raw = ns.inner(),
                ?cause,
                attempts = entry.attempts,
                "partition reconciliation keeps failing; retries cannot repair \
                 persistent on-disk damage -- operator intervention needed \
                 (inspect the partition directory; moving it aside lets the \
                 reconciler rebuild the replica from its group)"
            );
        }
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
        let sleep = ctx.shard.bus.sleep(periodic);
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
    /// Namespaces the sweep acted on (see [`reconcile_parked_frames`]):
    /// discarded because this shard cannot serve them, or aged past
    /// `MAX_PARKED_PASSES` while it still might. Namespaces, not frames, and
    /// acted on is not answered: aging answers requests, discarding also
    /// destroys prepares.
    parked_reclaimed: usize,
    /// Rebuilds deferred until an in-flight `ConfirmRemove` drains. Counted
    /// so the pass does not arm the fast-skip: the pump's drop clears the
    /// tombstone and re-wakes us without bumping `Streams::revision`, so an
    /// armed skip would swallow that wake and strand the rebuild forever.
    deferred: usize,
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
            + self.deferred
            + self.parked_reclaimed
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
    // partition-shaping commit bumps `revision`, a pending retry keeps
    // `failure_state` non-empty, and a pass that found work it could not
    // finish (a deferred rebuild, an incomplete trim) leaves
    // `last_pass_noop` false; any of the three forces the next pass.
    //
    // A non-empty park buffer is the fourth signal. Parking does not bump
    // `revision` and does not wake the reconciler, so without this a frame that
    // parks in a converged steady state is held for the process lifetime while
    // its client burns the full response read-timeout -- exactly what
    // `reconcile_parked_frames` exists to prevent. Held frames also occupy the
    // shard-wide byte budget, so one stranded namespace would shed every other
    // namespace's legitimate convergence window.
    if ctx.last_revision.get() == Some(revision)
        && ctx.last_pass_noop.get()
        && ctx.failure_state.borrow().is_empty()
        && !ctx.shard.has_parked_partition_frames()
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

    let staged = reconcile_additions(ctx, target, &mut counters).await;
    reconcile_removals(ctx, &target_set, &mut counters).await;
    reconcile_parked_frames(ctx, &staged, &mut counters);
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
            deferred = counters.deferred,
            parked_reclaimed = counters.parked_reclaimed,
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

/// Returns the namespaces whose `ReconcileOp::InsertOwned` this pass staged. The
/// pump applies the op on its own task, so they are not in `IggyPartitions` yet
/// and [`reconcile_parked_frames`] would read them as un-materialised, aging
/// their frames on the pass that built them.
async fn reconcile_additions(
    ctx: &ReconcilerCtx,
    target: Vec<(IggyNamespace, u64)>,
    counters: &mut PassCounters,
) -> AHashSet<IggyNamespace> {
    let shard_id = ctx.shard.id;
    let partitions = ctx.shard.plane.partitions();
    let total_shards = u32::from(ctx.total_shards);
    let mut staged = AHashSet::new();

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
                    counters.deferred += 1;
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
            // Compare the epoch, not just presence: a delete + recreate recycles
            // the slab keys, so the row survives with the DEAD incarnation's
            // `created_revision`. A presence-only gate never refreshes it, and
            // nothing else writes a non-owner's row.
            if !shards_table_has_epoch(ctx, ns, epoch) {
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
                staged.insert(ns);
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

    staged
}

/// Retire parked frames the shard cannot serve, age the ones it might.
///
/// `park_if_unmaterialised` holds a frame until `ReconcileOp::InsertOwned` lands;
/// the only other drains are `ConfirmRemove` and `RemoveRouted`. Neither can name
/// a namespace that was never built, since it is in neither `IggyPartitions` (no
/// owned ghost for `reconcile_removals`) nor `shards_table` (the owner seeds a
/// row only via `InsertOwned`, and emits `InsertRouted` only for namespaces it
/// does NOT own). Without this sweep the frames are held for the process lifetime
/// and every waiting client burns its full read-timeout.
///
/// Immediate discard needs positive evidence this shard can never serve the
/// namespace, because it destroys prepares outright. Two signals carry it: the
/// namespace is tombstoned (a wedged disk delete can postpone the
/// `ConfirmRemove` that would otherwise answer them indefinitely), or it does not
/// hash here, so no `InsertOwned` for it ever lands.
///
/// A failed `build_partition_fresh` is not that evidence, which is why the
/// backoff no longer discards. `next_backoff(1)` is one second, so the first
/// transient ENOSPC destroyed every parked frame for a namespace that rebuilds a
/// second later; the 60s clamp the old reasoning leaned on takes seven
/// consecutive failures.
///
/// Absence from the target set is not evidence either. It covers both a namespace
/// that left committed metadata and one this replica has not applied yet, and
/// local state cannot tell them apart: `snapshot_target_namespaces` reads this
/// node's committed metadata, so a lagging backup reports a namespace it is
/// milliseconds from committing exactly as it reports a deleted one. It is also
/// snapshotted before `reconcile_additions` awaits `build_partition_fresh`, so a
/// topic committing during those awaits is judged against a stale set.
///
/// Everything else is aged: building, backed off, still committing, genuinely
/// deleted, or materialised with frames the inbox refused.
/// [`shard::IggyShard::age_parked_partition_frames`] answers CLIENT REQUESTS past
/// `MAX_PARKED_PASSES` and leaves prepares alone, so no client waits out its read
/// timeout and no committed op dies on a local-convergence signal. Residency
/// only; see `ParkedFrame::passes`.
///
/// `staged_this_pass` is exempt: its `InsertOwned` is queued but not applied, so
/// it reads as un-materialised here. Not a one-pass concession.
/// `reconcile_additions` has no cross-pass guard against a queued-but-unapplied
/// op (it tests `partitions.contains`, false the whole time it sits in the
/// queue), so it re-stages every pass until the pump drains. The exemption
/// therefore covers arbitrary pump lag; dropping it ages frames on every
/// commit-driven pass the pump falls behind.
fn reconcile_parked_frames(
    ctx: &ReconcilerCtx,
    staged_this_pass: &AHashSet<IggyNamespace>,
    counters: &mut PassCounters,
) {
    let parked = ctx.shard.parked_namespaces();
    if parked.is_empty() {
        return;
    }
    let partitions = ctx.shard.plane.partitions();
    let total_shards = u32::from(ctx.total_shards);
    for ns in parked {
        if staged_this_pass.contains(&ns) {
            continue;
        }
        // Tombstoned namespaces are still in the map, so `contains` below reads
        // them as materialised.
        let not_ours = calculate_shard_assignment(&ns, total_shards) != ctx.shard.id;
        let tombstoned = partitions.is_tombstoned(&ns);
        if tombstoned || not_ours {
            debug!(
                shard = ctx.shard.id,
                ns_raw = ns.inner(),
                tombstoned,
                not_ours,
                "discarding parked frames for a namespace this shard cannot serve"
            );
            ctx.shard.discard_parked_partition_frames(ns);
            counters.parked_reclaimed += 1;
            continue;
        }
        // Materialised with frames still parked means the re-dispatch hit a full
        // inbox and re-parked them. The pump retries every iteration, so aging is
        // only the backstop for an inbox that never drains. Without it they have
        // no exit: `reconcile_additions` stages no second `InsertOwned` for a
        // namespace already in `IggyPartitions`.
        if ctx.shard.age_parked_partition_frames(ns) > 0 {
            counters.parked_reclaimed += 1;
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

/// `true` when this shard's routing row for `ns` already records `epoch`. A row
/// carrying any other epoch (or none) is stale and must be rewritten, since the
/// namespace is byte-identical across incarnations.
fn shards_table_has_epoch(ctx: &ReconcilerCtx, ns: IggyNamespace, epoch: u64) -> bool {
    ctx.shard.shards_table().epoch_for(ns) == Some(epoch)
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
    use super::{
        FailureCause, FailureRecord, ReconcilerCtx, delete_partitions_from_disk, reconcile_once,
    };
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
    use iggy_binary_protocol::{
        Command2, GenericHeader, Operation, PrepareHeader, ReplyHeader, RoutedRequestHeader,
        WireIdentifier,
    };
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
    use shard::{IggyShard, PartitionConsensusConfig, ReconcileOp, ShardIdentity};
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

    /// Build a partition-plane replicated `Prepare` for `namespace`, as a backup
    /// receives it from the primary. The frame a client never sees: it has no
    /// client to answer, so anything that discards it is silent data loss.
    fn build_partition_prepare(namespace: IggyNamespace, op: u64) -> Message<GenericHeader> {
        let header_size = size_of::<PrepareHeader>();
        let mut msg = Message::<PrepareHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<PrepareHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes form a valid PrepareHeader");
        header.command = Command2::Prepare;
        header.size = u32::try_from(header_size).expect("prepare size fits u32");
        header.operation = Operation::SendMessages;
        header.group = namespace.inner();
        header.op = op;
        msg.into_generic()
    }

    async fn park_one_prepare(shard: &TestShard, namespace: IggyNamespace, op: u64) {
        shard
            .on_message(build_partition_prepare(namespace, op))
            .await;
    }

    /// Build a partition-plane client `Request` for `namespace`, as the pump
    /// receives it off the wire. Only the routing fields matter: parking reads
    /// `operation` + `namespace` and never touches the body.
    fn build_partition_request(namespace: IggyNamespace) -> Message<GenericHeader> {
        build_partition_request_sized(namespace, 0)
    }

    /// Park one client request for `namespace` through the real pump entry
    /// point, so the epoch stamp and the park accounting are the production
    /// ones. The namespace must be unmaterialised, or the frame is delivered to
    /// the plane instead of parked.
    async fn park_one_request(shard: &TestShard, namespace: IggyNamespace) {
        shard.on_message(build_partition_request(namespace)).await;
    }

    /// [`build_partition_request`] with `body_len` trailing payload bytes, so a
    /// test can drive the park buffer's byte budget rather than its frame cap.
    fn build_partition_request_sized(
        namespace: IggyNamespace,
        body_len: usize,
    ) -> Message<GenericHeader> {
        let header_size = size_of::<RoutedRequestHeader>();
        let total_size = header_size + body_len;
        let mut msg = Message::<RoutedRequestHeader>::new(total_size);
        let header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes form a valid RoutedRequestHeader");
        header.command = Command2::Request;
        header.size = u32::try_from(total_size).expect("request size fits u32");
        header.operation = Operation::SendMessages;
        header.group = namespace.inner();
        // Header validation rejects a zero session / request on a non-register
        // op, and the park path runs after that validation.
        header.session = 1;
        header.request = TEST_REQUEST_ID;
        header.client = TEST_CLIENT_ID;
        msg.into_generic()
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
        > = IggyMetadata::new(None, None, None, None, mux, None);
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

    /// [`build_test_shard`] with a sender mesh, for tests asserting on work
    /// handed back to the pump (transient denies, parked-frame re-dispatch).
    /// Caller must keep the returned receiver alive; dropping it turns every
    /// `try_send` into `Disconnected`.
    ///
    /// Mesh covers `0..=shard_id` since consumers index `senders[shard_id]`.
    /// Peer receivers are dropped, so a misroute fails loudly instead of landing
    /// in this shard's inbox and reading as success.
    fn build_test_shard_with_inbox(
        shard_id: u16,
        config: &ServerNgConfig,
        mux: TestMux,
        capacity: usize,
    ) -> (Rc<TestShard>, shard::Receiver<shard::ShardFrame>) {
        let mut senders = Vec::with_capacity(usize::from(shard_id) + 1);
        let mut own_rx = None;
        for peer in 0..=shard_id {
            let (tx, rx) = shard::shard_channel(peer, capacity);
            senders.push(tx);
            if peer == shard_id {
                own_rx = Some(rx);
            }
        }
        let mut shard = Rc::into_inner(build_test_shard(shard_id, config, mux))
            .expect("freshly built shard is uniquely owned");
        shard.attach_senders(senders);
        (
            Rc::new(shard),
            own_rx.expect("the loop covers shard_id itself"),
        )
    }

    /// Drain a test shard's inbox into `(re-dispatched frames, staged client
    /// sends)`: served parked frames vs answers headed for a client.
    fn drain_inbox(rx: &shard::Receiver<shard::ShardFrame>) -> (usize, usize) {
        let mut served = 0;
        let mut answered = 0;
        while let Ok(frame) = rx.try_recv() {
            match frame {
                shard::ShardFrame::Consensus { .. } => served += 1,
                shard::ShardFrame::Lifecycle(shard::LifecycleFrame::ForwardClientSend {
                    ..
                }) => answered += 1,
                _ => {}
            }
        }
        (served, answered)
    }

    /// Count the `ForwardClientSend` frames sitting in a test shard's inbox: the
    /// staged transient denies, which is what actually reaches a client.
    fn drain_staged_client_sends(rx: &shard::Receiver<shard::ShardFrame>) -> usize {
        drain_inbox(rx).1
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

    /// The window between the recreate committing and the reconciler
    /// converging is a data-loss race: the namespace is byte-identical across
    /// incarnations, so a `SendMessages` arriving inside it would otherwise be
    /// journaled and acked against the PRIOR partition, which the reconciler
    /// then deletes. The shard must refuse to serve until the epoch it stored
    /// on the routing row matches the committed `created_revision` again.
    #[compio::test]
    async fn fence_denies_partition_request_until_recreated_incarnation_converges() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-fence");
        seed_topic(&mux, 2, 0, "topic-fence", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        reconcile_pass(&ctx).await;
        assert!(
            shard.serves_committed_incarnation(Operation::SendMessages, ns.inner()),
            "a converged partition must serve normal traffic; the fence must not \
             deny the steady state"
        );

        // Delete + recreate the SAME tuple, reusing the freed slab key. No
        // reconcile pass runs in between, so the prior incarnation is still
        // materialised under the recycled identity.
        seed_delete_topic(&shard.plane.metadata().mux_stm, 3, 0, 0);
        seed_topic(
            &shard.plane.metadata().mux_stm,
            4,
            0,
            "topic-fence",
            vec![assignment(0, 1)],
        );
        assert!(
            !shard.serves_committed_incarnation(Operation::SendMessages, ns.inner()),
            "a send landing before the reconciler tears the prior incarnation down \
             must be denied; delivering it acks a batch that teardown then erases"
        );

        // Pass 1 tears the stale incarnation down, pass 2 rebuilds it at the
        // committed epoch; only then may traffic resume.
        reconcile_pass(&ctx).await;
        reconcile_pass(&ctx).await;
        assert!(
            shard.plane.partitions().contains(&ns),
            "fresh partition must materialise after the teardown"
        );
        assert!(
            shard.serves_committed_incarnation(Operation::SendMessages, ns.inner()),
            "traffic must resume once the rebuilt row carries the committed epoch"
        );
    }

    /// The fence proves an incarnation by pairing the committed
    /// `created_revision` with the epoch on the routing row. Neither side alone
    /// is a proof, so a missing row (not yet materialised) and a missing commit
    /// (namespace deleted, teardown pending) both deny. Metadata operations
    /// address no partition incarnation and must pass regardless.
    #[compio::test]
    async fn fence_denies_partition_request_when_incarnation_is_unverifiable() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-unverifiable");
        seed_topic(&mux, 2, 0, "topic-unverifiable", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        // Committed, but no reconcile pass has run: no row to prove against.
        assert!(
            shard.shards_table().epoch_for(ns).is_none(),
            "precondition: the routing row is written by the reconciler"
        );
        assert!(
            !shard.serves_committed_incarnation(Operation::SendMessages, ns.inner()),
            "a committed namespace with no routing row cannot be proven current"
        );

        reconcile_pass(&ctx).await;

        // Deleted, teardown not yet applied: the row outlives the commit.
        seed_delete_topic(&shard.plane.metadata().mux_stm, 3, 0, 0);
        assert!(
            shard.shards_table().epoch_for(ns).is_some(),
            "precondition: the row survives until the reconciler removes it"
        );
        assert!(
            !shard.serves_committed_incarnation(Operation::SendMessages, ns.inner()),
            "a namespace no longer committed must be denied even while its row lingers"
        );
        assert!(
            shard.serves_committed_incarnation(Operation::CreateStream, ns.inner()),
            "the fence guards partition operations only; metadata traffic carries \
             no partition incarnation"
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

    /// Deferral-arms-the-fast-skip regression: a deferred rebuild is pending
    /// work, but a pass that found nothing else used to report `did_work =
    /// false` and arm the fast-skip. The wake the pump fires after draining
    /// `ConfirmRemove` carries no revision bump (dropping a partition is not a
    /// metadata commit), so it landed on the armed guard and was swallowed:
    /// the rebuild never ran, the namespace stayed unroutable, and every
    /// parked data-plane frame hung until the client timed out.
    #[compio::test]
    async fn reconcile_rebuilds_after_deferred_confirm_remove_drains() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-defer-skip");
        seed_topic(&mux, 2, 0, "topic-defer-skip", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        reconcile_pass(&ctx).await;
        let ns = IggyNamespace::new(0, 0, 0);
        let partitions = shard.plane.partitions();
        assert!(partitions.contains(&ns));

        // Post-successful-teardown, pre-drain state: fenced, unlinked, and a
        // `ConfirmRemove` queued but not yet applied. `ns` is still in the
        // committed target, so the additions pass can only defer the rebuild.
        partitions.tombstone(ns);
        shard.shards_table().remove(&ns);
        delete_partitions_from_disk(
            ns.stream_id(),
            ns.topic_id(),
            ns.partition_id(),
            ctx.config.as_ref(),
        )
        .await
        .expect("teardown disk delete succeeds");
        shard.enqueue_reconcile_op(ReconcileOp::ConfirmRemove { namespace: ns });

        reconcile_once(&ctx).await;
        assert!(
            partitions.is_tombstoned(&ns),
            "the pass under test must be the deferring one"
        );

        // Pump drains: partition dropped, tombstone cleared, reconciler woken.
        // No commit happened, so `revision` is unchanged and `last_pass_noop`
        // is the only thing that can keep the woken pass alive.
        shard.apply_reconcile_ops();
        assert!(!partitions.contains(&ns));

        assert!(
            reconcile_once(&ctx).await,
            "the post-ConfirmRemove wake must run a full pass: a deferring \
             pass has not converged and must not arm the fast-skip"
        );
        shard.apply_reconcile_ops();
        assert!(
            partitions.contains(&ns),
            "the deferred rebuild must materialise once the drop drains"
        );
        assert_eq!(
            shard.shards_table().shard_for(ns),
            Some(0),
            "rebuilt partition must be addressable again"
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

        // Two groups: "dead" gets id 0, "live" gets id 1 (per-topic monotonic).
        let stm = &shard.plane.metadata().mux_stm;
        seed_create_consumer_group(stm, 3, 0, 0, "dead");
        seed_create_consumer_group(stm, 4, 0, 0, "live");

        // Offsets are keyed by the monotonic group id (the id the store path is
        // rewritten to and the read path / live-set resolve), not the name hash.
        let dead_key: u32 = 0;
        let live_key: u32 = 1;
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

        // Delete the "dead" group (id 0); "live" (id 1) stays.
        seed_delete_consumer_group(stm, 5, 0, 0, 0);
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
        // Single member owns every partition (group id 0, the first in topic).
        seed_join_consumer_group(&mux, 4, 0, 0, 0, 100);

        let group = WireIdentifier::numeric(0);
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
        seed_create_consumer_group(&mux, 3, 0, 0, "cg"); // group id 0
        seed_join_consumer_group(&mux, 4, 0, 0, 0, 100);
        seed_join_consumer_group(&mux, 5, 0, 0, 0, 200);

        let stream = WireIdentifier::numeric(0);
        let topic = WireIdentifier::numeric(0);
        let group = WireIdentifier::numeric(0);
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

    /// A namespace deleted before its build finished is named by nothing: it is
    /// absent from `IggyPartitions`, so the removals pass sees no owned ghost,
    /// and absent from `shards_table`, since the owner seeds a row only via
    /// `InsertOwned`. Neither `ConfirmRemove` nor `RemoveRouted` can therefore
    /// reach its parked frames, and without the sweep they are held for the
    /// process lifetime while every waiting client burns its read timeout.
    ///
    /// Reclaim is via the age bound, not on sight of the namespace leaving the
    /// target set: "absent from committed metadata" reads identically for a
    /// deleted namespace and for one a metadata-lagging replica has not applied
    /// yet, so reclaiming on that would destroy live in-flight traffic. The first
    /// pass must therefore hold the frames, and a few passes later they are gone.
    #[compio::test]
    async fn parked_frames_are_reclaimed_when_the_namespace_leaves_metadata() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-reclaim");
        seed_topic(&mux, 2, 0, "topic-reclaim", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        // No pass yet, so the namespace is committed but unmaterialised.
        park_one_request(&shard, ns).await;
        assert_eq!(
            shard.parked_namespaces(),
            vec![ns],
            "a request for an unmaterialised namespace must park"
        );

        // Delete before any pass builds it: the reconciler never materialises
        // it, so nothing drains the entry the normal way.
        seed_delete_topic(&shard.plane.metadata().mux_stm, 3, 0, 0);
        reconcile_pass(&ctx).await;
        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "the first pass must not destroy the frame: absence from the target set \
             is also what a not-yet-applied commit looks like"
        );

        // Every subsequent pass ages it, and the park buffer keeps defeating the
        // revision fast-skip until it drains.
        for _ in 0..=PARK_MAX_PASSES {
            reconcile_pass(&ctx).await;
        }

        assert!(
            shard.parked_namespaces().is_empty(),
            "frames for a namespace that left metadata must be answered and reclaimed"
        );
        assert_eq!(
            drain_staged_client_sends(&inbox),
            1,
            "and the waiting client must get a retriable answer"
        );
    }

    /// A frame parked before this node's metadata knew the namespace carries no
    /// epoch stamp, and `None` must NOT read as "prior incarnation": on a
    /// metadata-lagging replica it is the ordinary case, since the partition
    /// primary materialises and replicates as soon as its own metadata commits.
    /// Rejecting it destroys live traffic -- silently for a replicated prepare,
    /// which has no client to answer -- and the pre-stamp code served it.
    #[compio::test]
    async fn unstamped_parked_frame_is_served_not_rejected_as_stale() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-unstamped");
        seed_topic(&mux, 2, 0, "topic-known", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        // Topic slab 1 does not exist yet, so there is no committed
        // `created_revision` to stamp: the frame parks with `epoch: None`.
        let unknown = IggyNamespace::new(0, 1, 0);
        park_one_request(&shard, unknown).await;
        assert_eq!(
            shard.parked_frame_count(unknown),
            1,
            "a request for a namespace this node has not applied must park"
        );

        // The commit this node was lagging behind now lands, and the pass
        // materialises the namespace.
        seed_topic(
            &shard.plane.metadata().mux_stm,
            3,
            0,
            "topic-late",
            vec![assignment(0, 2)],
        );
        reconcile_pass(&ctx).await;

        assert_eq!(
            shard.parked_frame_count(unknown),
            0,
            "materialisation must drain the park entry"
        );
        let (served, answered) = drain_inbox(&inbox);
        assert_eq!(
            served, 1,
            "the unstamped frame must be re-dispatched onto the pump, not rejected"
        );
        assert_eq!(
            answered, 0,
            "and it must not be answered with a deny instead of served"
        );
        assert_eq!(
            shard.metrics().partition_frames_rejected_stale_value(),
            0,
            "an absent stamp is not evidence of a prior incarnation"
        );
    }

    /// The shard-wide byte budget is a running total maintained at each mutation
    /// site rather than rescanned per arriving frame. A total that fails to debit
    /// on drain silently wedges the budget: the shard would shed every namespace's
    /// frames while nothing is actually parked. Exercise each way frames leave --
    /// re-dispatch on materialisation, the age bound, and reclaim -- and assert the
    /// total returns to empty.
    #[compio::test]
    async fn park_byte_total_returns_to_zero_on_every_drain_path() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-bytes");
        seed_topic(&mux, 2, 0, "topic-bytes", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 16);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        // Drain path 1: materialisation re-dispatches.
        let late = IggyNamespace::new(0, 1, 0);
        park_one_request(&shard, late).await;
        assert!(shard.has_parked_partition_frames());
        seed_topic(
            &shard.plane.metadata().mux_stm,
            3,
            0,
            "topic-bytes-late",
            vec![assignment(0, 2)],
        );
        reconcile_pass(&ctx).await;
        assert!(
            !shard.has_parked_partition_frames(),
            "re-dispatch must debit the parked-byte total"
        );

        // Drain path 2: the age bound answers the frame.
        let never = IggyNamespace::new(0, 9, 0);
        park_one_request(&shard, never).await;
        assert!(shard.has_parked_partition_frames());
        for _ in 0..=PARK_MAX_PASSES {
            shard.age_parked_partition_frames(never);
        }
        assert!(
            !shard.has_parked_partition_frames(),
            "aging out must debit the parked-byte total"
        );

        // Drain path 3: an explicit reclaim.
        park_one_request(&shard, never).await;
        assert!(shard.has_parked_partition_frames());
        shard.discard_parked_partition_frames(never);
        assert!(
            !shard.has_parked_partition_frames(),
            "reclaim must debit the parked-byte total"
        );

        drop(inbox);
    }

    /// The replicated-prepare shape, which no other test covers and where both
    /// park critical are worst: a prepare has no client, so `deny_parked_frame`
    /// no-ops on it and anything that discards it loses committed data silently,
    /// with no normal-status repair driver to refetch it.
    ///
    /// A backup receives the prepare before its own metadata commits (so the frame
    /// parks unstamped), then applies the commit and materialises. The prepare must
    /// be re-dispatched, not rejected as a prior incarnation.
    #[compio::test]
    async fn unstamped_parked_prepare_is_served_after_materialisation() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-prepare");
        seed_topic(&mux, 2, 0, "topic-known", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        // The primary replicates ahead of this node's metadata: topic slab 1 is
        // not committed here yet, so the prepare parks with `epoch: None`.
        let lagging = IggyNamespace::new(0, 1, 0);
        park_one_prepare(&shard, lagging, 7).await;
        assert_eq!(
            shard.parked_frame_count(lagging),
            1,
            "a prepare for a namespace this backup has not applied must park"
        );

        // The metadata commit catches up and the pass materialises the namespace.
        seed_topic(
            &shard.plane.metadata().mux_stm,
            3,
            0,
            "topic-late",
            vec![assignment(0, 2)],
        );
        reconcile_pass(&ctx).await;

        let (served, answered) = drain_inbox(&inbox);
        assert_eq!(
            served, 1,
            "the parked prepare must be re-dispatched; discarding it is silent \
             committed-data loss, since a prepare has no client to answer"
        );
        assert_eq!(answered, 0, "a prepare has no client deny to send");
        assert_eq!(
            shard.metrics().partition_frames_rejected_stale_value(),
            0,
            "an unstamped prepare is not a prior incarnation"
        );
    }

    /// A parked prepare whose stamp names a DIFFERENT incarnation must still be
    /// dropped: applying a dead topic's op into the topic that recycled its slab
    /// keys diverges this replica. This is the half of the fence that stays.
    #[compio::test]
    async fn stamped_parked_prepare_from_a_prior_incarnation_is_rejected() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-stale");
        seed_topic(&mux, 2, 0, "topic-first", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        // Parked against the FIRST incarnation, so it carries that revision.
        park_one_prepare(&shard, ns, 7).await;
        assert_eq!(shard.parked_frame_count(ns), 1);

        // Delete and recreate: same namespace keys, new committed revision.
        seed_delete_topic(&shard.plane.metadata().mux_stm, 3, 0, 0);
        seed_topic(
            &shard.plane.metadata().mux_stm,
            4,
            0,
            "topic-recreated",
            vec![assignment(0, 2)],
        );
        reconcile_pass(&ctx).await;

        let (served, _answered) = drain_inbox(&inbox);
        assert_eq!(
            served, 0,
            "a prepare stamped with the dead incarnation must not be served against \
             its replacement"
        );
        assert_eq!(
            shard.metrics().partition_frames_rejected_stale_value(),
            1,
            "and the reject must be counted"
        );
    }

    /// Parking does not bump `Streams::revision` and does not wake the reconciler,
    /// so a frame that parks in a converged steady state would be held for the
    /// process lifetime if the revision fast-skip could still fire. A non-empty
    /// park buffer must therefore defeat the skip.
    #[compio::test]
    async fn park_buffer_defeats_the_revision_fast_skip() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-skip");
        seed_topic(&mux, 2, 0, "topic-skip", vec![assignment(0, 1)]);

        let (shard, _inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));

        // Converge: the first pass materialises, the verify pass after it arms
        // the skip (same sequence as `reconcile_fast_skips_when_revision_unchanged`).
        assert!(reconcile_once(&ctx).await, "first pass runs the full diff");
        ctx.shard.apply_reconcile_ops();
        assert!(
            reconcile_once(&ctx).await,
            "the verify pass after a working pass must still run"
        );
        ctx.shard.apply_reconcile_ops();
        assert!(
            !reconcile_once(&ctx).await,
            "a converged pass with an unchanged revision must fast-skip"
        );

        // Park a frame for a namespace that is NOT materialised, without touching
        // the revision, and the skip must stop firing.
        let unbuilt = IggyNamespace::new(0, 0, 7);
        park_one_request(&shard, unbuilt).await;
        assert!(
            shard.has_parked_partition_frames(),
            "the frame must be parked for this test to mean anything"
        );
        assert!(
            reconcile_once(&ctx).await,
            "a non-empty park buffer must defeat the fast-skip so the sweep can run"
        );
    }

    /// Namespace stays committed, `build_partition_fresh` keeps failing. One
    /// failure must destroy nothing: `next_backoff(1)` is a second, the rebuild
    /// usually lands on the next pass, and the sweep cannot tell a transient
    /// ENOSPC from a permanent one. Request gets the age bound, prepare is kept.
    #[compio::test]
    async fn a_backed_off_build_ages_requests_and_retains_prepares() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-backoff");
        seed_topic(&mux, 2, 0, "topic-backoff", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 16);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        park_one_request(&shard, ns).await;
        park_one_prepare(&shard, ns, 7).await;
        assert_eq!(shard.parked_namespaces(), vec![ns]);

        // Stand in for a failed build (ENOSPC / EPERM): the additions pass skips
        // a backed-off namespace, so it stays committed and unmaterialised.
        ctx.record_failure(ns, FailureCause::Add, Instant::now());
        reconcile_pass(&ctx).await;

        assert!(
            !ctx.shard.plane.partitions().contains(&ns),
            "a backed-off namespace must not have been built"
        );
        assert_eq!(
            shard.parked_frame_count(ns),
            2,
            "one failed build must not destroy anything; the backoff is a second"
        );

        // Held only until the request outlives the admission window.
        for _ in 0..PARK_MAX_PASSES {
            reconcile_pass(&ctx).await;
        }
        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "the request must age out, so no client waits out its read timeout"
        );
        assert_eq!(
            drain_staged_client_sends(&inbox),
            1,
            "and it must be answered, not dropped"
        );
        assert_eq!(
            park_dropped_count(&shard),
            0,
            "the prepare must be retained: destroying it is unrecoverable"
        );

        // Retained across an unbounded number of further passes.
        for _ in 0..(PARK_MAX_PASSES * 4) {
            reconcile_pass(&ctx).await;
        }
        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "prepares are bounded by the byte budget, never by the age bound"
        );
    }

    /// Delete + recreate recycles the slab keys, so a frame parked against the
    /// dead incarnation is byte-identical to one for its replacement. Draining
    /// it into the new partition would land a dead topic's write inside the live
    /// one, and the incarnation fence cannot catch it: that compares the
    /// committed revision against the routing row, both of which describe the
    /// NEW incarnation. Only the epoch stamped at park time separates them.
    #[compio::test]
    async fn parked_frames_from_a_prior_incarnation_are_not_served_by_its_replacement() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-epoch");
        seed_topic(&mux, 2, 0, "topic-epoch", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        // Parked against the first incarnation, before any pass builds it.
        park_one_request(&shard, ns).await;
        assert_eq!(shard.parked_namespaces(), vec![ns]);
        assert_eq!(
            shard.metrics().partition_frames_rejected_stale_value(),
            0,
            "nothing rejected yet"
        );

        // Recreate the same tuple. The namespace is unchanged; only
        // `created_revision` moves.
        seed_delete_topic(&shard.plane.metadata().mux_stm, 3, 0, 0);
        seed_topic(
            &shard.plane.metadata().mux_stm,
            4,
            0,
            "topic-epoch",
            vec![assignment(0, 1)],
        );

        // The pass builds the SECOND incarnation and drains the park entry.
        reconcile_pass(&ctx).await;

        assert!(
            shard.plane.partitions().contains(&ns),
            "the recreated incarnation must materialise"
        );
        assert!(
            shard.parked_namespaces().is_empty(),
            "the park entry must be drained by the materialisation"
        );
        assert_eq!(
            shard.metrics().partition_frames_rejected_stale_value(),
            1,
            "the frame stamped with the dead incarnation must be rejected, not \
             re-dispatched into its replacement"
        );
    }

    /// Past the per-namespace cap the frame is gone either way, but a client
    /// request must still be answered: the transports decode replies in
    /// lockstep, so a silent shed leaves the connection waiting out its full
    /// response read-timeout.
    ///
    /// Registers an in-process client, so the assertion is that a reply reached
    /// a waiter, not that a counter moved. With no client every send fails
    /// `ClientNotFound`, and a counter bumped before the send reports an answer
    /// nobody received.
    #[compio::test]
    async fn park_overflow_answers_the_client_instead_of_shedding_silently() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-overflow");
        seed_topic(&mux, 2, 0, "topic-overflow", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ns = IggyNamespace::new(0, 0, 0);
        let (_slot, reply_rx) = register_waiting_client(&shard);

        // Fill the buffer to its cap, then one more.
        for _ in 0..PARK_CAP {
            park_one_request(&shard, ns).await;
        }
        assert_eq!(
            park_overflow_count(&shard),
            0,
            "everything up to the cap parks without shedding"
        );

        assert_eq!(
            shard.metrics().partition_requests_denied_transient_value(),
            0,
            "nothing has been answered yet; the parked frames are still waiting"
        );

        park_one_request(&shard, ns).await;
        assert_eq!(
            park_overflow_count(&shard),
            1,
            "the frame past the cap must be shed and counted, not parked"
        );
        assert_eq!(
            shard.parked_frame_count(ns),
            PARK_CAP,
            "the shed frame must not have grown the buffer past its cap"
        );
        // The point of the fix: shedding is unavoidable at the cap, silence is
        // not. Without the deny the connection waits out its whole response
        // read-timeout on a frame that is already gone.
        let reply = reply_rx
            .await
            .expect("the shed request must reach the waiting client");
        assert_eq!(
            reply_status(&reply),
            iggy_common::IggyError::TransientNotAccepted.as_code(),
            "the shed request must be answered with a retriable status"
        );
        assert_eq!(
            shard.metrics().partition_requests_denied_transient_value(),
            1,
            "and the counter must credit that delivered answer"
        );
    }

    /// Credit only denies the bus delivered. Incrementing before
    /// `send_to_client` reported an answer with no client registered, blinding
    /// `park_overflow_answers_the_client_instead_of_shedding_silently`.
    #[compio::test]
    async fn overflow_deny_is_not_counted_when_the_client_is_gone() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-overflow-gone");
        seed_topic(&mux, 2, 0, "topic-overflow-gone", vec![assignment(0, 1)]);

        // No client registered: every `send_to_client` fails `ClientNotFound`.
        let shard = build_test_shard(0, &config, mux);
        let ns = IggyNamespace::new(0, 0, 0);

        for _ in 0..=PARK_CAP {
            park_one_request(&shard, ns).await;
        }

        assert_eq!(
            park_overflow_count(&shard),
            1,
            "the frame past the cap is still shed and counted"
        );
        assert_eq!(
            shard.metrics().partition_requests_denied_transient_value(),
            0,
            "a deny the bus could not deliver must not be counted as an answer"
        );
        assert_eq!(
            shard.metrics().frame_drop_count(
                shard::metrics::frame_drop_variant::PARTITION,
                shard::metrics::frame_drop_reason::DELIVERY_FAILED,
            ),
            1,
            "it must be counted as an undelivered reply instead"
        );
    }

    /// The age bound answers requests and steps over prepares. Expiring a
    /// prepare is permanent loss (no client, and `retransmit_targets` skips an op
    /// already at quorum), and passes are commit-driven, so a create burst
    /// elapses four in milliseconds across every parked namespace at once.
    #[compio::test]
    async fn aging_answers_requests_and_never_expires_a_prepare() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-prepare-age");
        seed_topic(&mux, 2, 0, "topic-prepare-age", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ns = IggyNamespace::new(0, 0, 0);

        park_one_prepare(&shard, ns, 7).await;
        park_one_request(&shard, ns).await;
        for _ in 0..=PARK_MAX_PASSES {
            shard.age_parked_partition_frames(ns);
        }

        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "the request ages out; the prepare stays"
        );
        assert_eq!(
            drain_staged_client_sends(&inbox),
            1,
            "the request must be answered rather than dropped"
        );
        assert_eq!(
            shard.metrics().partition_requests_denied_transient_value(),
            1
        );
        assert_eq!(
            park_dropped_count(&shard),
            0,
            "nothing may be destroyed on an age bound"
        );
    }

    /// The one path that still destroys a prepare: namespace gone from this
    /// shard, so holding it buys nothing. No client, so `park_dropped` is the
    /// only record the op existed.
    #[compio::test]
    async fn a_discarded_prepare_is_counted_even_though_nobody_can_be_answered() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-prepare-discard");
        seed_topic(&mux, 2, 0, "topic-prepare-discard", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ns = IggyNamespace::new(0, 0, 0);

        park_one_prepare(&shard, ns, 7).await;
        shard.discard_parked_partition_frames(ns);

        assert_eq!(shard.parked_frame_count(ns), 0, "the prepare is gone");
        assert_eq!(
            drain_staged_client_sends(&inbox),
            0,
            "a prepare has no client to answer"
        );
        assert_eq!(
            shard.metrics().partition_requests_denied_transient_value(),
            0,
            "and must not be reported as an answered request"
        );
        assert_eq!(
            park_dropped_count(&shard),
            1,
            "the destroyed op must leave a record; silence here is invisible loss"
        );
    }

    /// A frame larger than the per-namespace cap failed the check even against
    /// an empty entry, so it could never park. Unrecoverable for a prepare:
    /// `retransmit_targets` skips an op already at quorum.
    #[compio::test]
    async fn a_frame_over_the_namespace_byte_cap_still_parks_into_an_empty_entry() {
        const OVER_NAMESPACE_CAP: usize = 5 * 1024 * 1024;

        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-oversize");
        seed_topic(&mux, 2, 0, "topic-oversize", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ns = IggyNamespace::new(0, 0, 0);

        shard
            .on_message(build_partition_request_sized(ns, OVER_NAMESPACE_CAP))
            .await;
        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "the first frame of an empty entry must park regardless of the per-namespace cap"
        );
        assert_eq!(
            park_overflow_count(&shard),
            0,
            "and must not be shed doing it"
        );

        // The waiver is for the first frame only; the cap still applies after.
        shard
            .on_message(build_partition_request_sized(ns, OVER_NAMESPACE_CAP))
            .await;
        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "a second oversize frame must be shed, or one namespace eats the shard budget"
        );
        assert_eq!(park_overflow_count(&shard), 1);
    }

    /// A namespace whose build is still in flight keeps its frames -- but not
    /// forever, or the park buffer grows with a namespace that never materialises.
    /// The bound is in reconciler passes so the simulator's virtual clock governs
    /// it.
    ///
    /// Driven through `age_parked_partition_frames` directly. The sweep calls it
    /// once per pass for a namespace still building, and that branch is the only
    /// way a committed, non-backed-off namespace reaches the bound - which a unit
    /// test cannot stage, since its build completes on the first pass.
    ///
    /// Uses a shard with a live inbox: the deny is staged onto the pump, so a
    /// shard with no sender would report the frame answered while nothing was
    /// ever handed anywhere.
    #[compio::test]
    async fn parked_frames_are_answered_once_they_outlive_their_admission_window() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-age");
        seed_topic(&mux, 2, 0, "topic-age", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ns = IggyNamespace::new(0, 0, 0);

        park_one_request(&shard, ns).await;
        assert_eq!(shard.parked_frame_count(ns), 1);

        // Each pass ages the frame; it survives until it is over the bound.
        for pass in 0..PARK_MAX_PASSES {
            assert_eq!(
                shard.age_parked_partition_frames(ns),
                0,
                "pass {pass} is still inside the admission window"
            );
            assert_eq!(shard.parked_frame_count(ns), 1);
        }
        assert_eq!(
            shard.age_parked_partition_frames(ns),
            1,
            "the pass past the bound must answer the frame"
        );
        assert_eq!(shard.parked_frame_count(ns), 0);
        assert_eq!(
            drain_staged_client_sends(&inbox),
            1,
            "the answer must actually reach the pump, not just the counter"
        );
        assert_eq!(
            shard.metrics().partition_requests_denied_transient_value(),
            1,
            "and it must be answered with a retriable status, not dropped"
        );
    }

    /// The counter must credit only denies the pump accepted. It previously
    /// incremented before the `try_send`, so a shard whose inbox refused the frame
    /// (or had no sender at all) still reported the client answered.
    #[compio::test]
    async fn transient_deny_is_not_counted_when_the_inbox_cannot_take_it() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-deny-drop");
        seed_topic(&mux, 2, 0, "topic-deny-drop", vec![assignment(0, 1)]);

        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 8);
        let ns = IggyNamespace::new(0, 0, 0);
        park_one_request(&shard, ns).await;

        // Kill the pump side, so every staged frame is refused.
        drop(inbox);

        for _ in 0..=PARK_MAX_PASSES {
            shard.age_parked_partition_frames(ns);
        }

        assert_eq!(shard.parked_frame_count(ns), 0, "the frame still ages out");
        assert_eq!(
            shard.metrics().partition_requests_denied_transient_value(),
            0,
            "a deny the inbox refused must not be counted as an answer"
        );
    }

    /// The frame cap is request-only. Applied to prepares it is the binding
    /// constraint for any footprint under `NAMESPACE_BUDGET / PARK_CAP` (32 KiB),
    /// so header-only prepares would shed at 128 frames, 512 KiB into a 4 MiB
    /// budget, and the byte budgets would never speak. Small-append prepares
    /// would then be destroyed exactly as before the class split, at every
    /// replica count: quorum always leaves at least one surplus backup, so a
    /// lagging one loses shed prepares with nobody noticing.
    #[compio::test]
    async fn the_frame_cap_bounds_requests_only_so_small_prepares_reach_the_byte_budget() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-frame-cap");
        seed_topic(&mux, 2, 0, "topic-frame-cap", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ns = IggyNamespace::new(0, 0, 0);

        // Well past the frame cap, prepares keep parking.
        let beyond_cap = PARK_CAP + 72;
        for op in 0..beyond_cap {
            park_one_prepare(&shard, ns, op as u64).await;
        }
        assert_eq!(
            shard.parked_frame_count(ns),
            beyond_cap,
            "the frame cap must not shed prepares"
        );
        assert_eq!(park_overflow_count(&shard), 0);

        // A request into that same deep entry is still capped.
        park_one_request(&shard, ns).await;
        assert_eq!(
            shard.parked_frame_count(ns),
            beyond_cap,
            "a request past the cap must still be shed"
        );
        assert_eq!(park_overflow_count(&shard), 1);

        // The per-namespace byte budget is what finally stops the prepares.
        for op in beyond_cap..=HEADER_FRAMES_PER_NAMESPACE {
            park_one_prepare(&shard, ns, op as u64).await;
        }
        assert_eq!(
            shard.parked_frame_count(ns),
            HEADER_FRAMES_PER_NAMESPACE,
            "prepares must admit up to the byte budget, not the frame cap"
        );
        assert_eq!(
            park_overflow_count(&shard),
            2,
            "and the frame crossing the byte budget is the only further shed"
        );
    }

    /// The frame cap bounds count, not residency: `Message::into_generic` is a
    /// retag, so each entry keeps its whole buffer, up to 64 MiB. With only a
    /// frame cap one namespace could pin 128 x 64 MiB, so bytes must bite first.
    ///
    /// Single namespace, so this proves the PER-NAMESPACE cap: 1 MiB bodies
    /// charge 1052672 each, so the 4th crosses 4 MiB, nowhere near the shard-wide
    /// 16 MiB that
    /// [`park_shard_wide_byte_budget_sheds_a_namespace_that_would_cross_it`]
    /// covers.
    #[compio::test]
    async fn park_namespace_byte_budget_sheds_large_frames_before_the_frame_cap() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-bytes");
        seed_topic(&mux, 2, 0, "topic-bytes", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let ns = IggyNamespace::new(0, 0, 0);

        for _ in 0..PARK_CAP {
            shard
                .on_message(build_partition_request_sized(ns, MIB_BODY))
                .await;
            if park_overflow_count(&shard) > 0 {
                break;
            }
        }

        assert_eq!(
            shard.parked_frame_count(ns),
            PER_NAMESPACE_MIB_FRAMES,
            "the per-namespace budget admits {PER_NAMESPACE_MIB_FRAMES} x {MIB_FOOTPRINT} \
             and sheds the next"
        );
        assert_eq!(park_overflow_count(&shard), 1);
    }

    /// Shard-wide budget on its own terms: fill several namespaces to just under
    /// it, then one frame into a fresh namespace. The per-namespace check waives
    /// the first frame of an empty entry, so only the shard-wide check can shed.
    ///
    /// Needs its own test because nothing else reaches it: a single namespace
    /// hits its quarter-sized cap first, leaving `MAX_PARKED_BYTES` unexercised.
    #[compio::test]
    async fn park_shard_wide_byte_budget_sheds_a_namespace_that_would_cross_it() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-shard-bytes");
        seed_topic(&mux, 2, 0, "topic-shard-bytes", vec![assignment(0, 1)]);

        let shard = build_test_shard(0, &config, mux);
        let filled = SHARD_BUDGET / (PER_NAMESPACE_MIB_FRAMES * MIB_FOOTPRINT);
        for topic_id in 0..filled {
            let ns = IggyNamespace::new(0, topic_id, 0);
            for _ in 0..PER_NAMESPACE_MIB_FRAMES {
                shard
                    .on_message(build_partition_request_sized(ns, MIB_BODY))
                    .await;
            }
        }
        assert_eq!(
            park_overflow_count(&shard),
            0,
            "{filled} namespaces x {PER_NAMESPACE_MIB_FRAMES} frames must all fit"
        );

        // One frame into an empty entry: per-namespace waived, shard-wide not.
        // Two distinct fresh namespaces, so the entryless shed path runs twice.
        // It has no `ParkEntry` to warn once from and is gated shard-wide
        // instead; the gate is log volume only, so the counter still records
        // every shed.
        for offset in 0..2 {
            let crossing = IggyNamespace::new(0, filled + offset, 0);
            shard
                .on_message(build_partition_request_sized(crossing, MIB_BODY))
                .await;
            assert_eq!(
                shard.parked_frame_count(crossing),
                0,
                "the frame that would cross the shard-wide budget must be shed"
            );
        }
        assert_eq!(park_overflow_count(&shard), 2);
    }

    /// A refused re-dispatch re-parks the frame, and by then the namespace is
    /// materialised, closing every other exit: the sweep skips a namespace in
    /// `IggyPartitions` and `reconcile_additions` stages no second
    /// `InsertOwned`. Before the pump retry the frame sat until a topic delete,
    /// unanswered, with its bytes charged and the fast-skip never re-arming.
    #[compio::test]
    async fn a_re_parked_frame_is_re_dispatched_once_the_inbox_drains() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-repark");
        seed_topic(&mux, 2, 0, "topic-repark", vec![assignment(0, 1)]);

        // Capacity 1: `enqueue_reconcile_op`'s `ReconcileApply` marker takes the
        // only slot, so the re-dispatch that follows is refused with `Full`.
        let (shard, inbox) = build_test_shard_with_inbox(0, &config, mux, 1);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        park_one_request(&shard, ns).await;
        reconcile_pass(&ctx).await;

        assert!(
            shard.plane.partitions().contains(&ns),
            "the namespace must have materialised"
        );
        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "the full inbox must have re-parked the frame rather than dropping it"
        );

        // What the pump does every iteration: consume a frame, then re-drive.
        let (_served, _answered) = drain_inbox(&inbox);
        shard.apply_reconcile_ops();

        assert_eq!(
            shard.parked_frame_count(ns),
            0,
            "the freed slot must let the retry drain the entry"
        );
        assert!(
            !shard.has_parked_partition_frames(),
            "and the byte budget must return, so the revision fast-skip can re-arm"
        );
        assert_eq!(
            drain_inbox(&inbox).0,
            1,
            "the frame must reach the pump as a consensus frame, not be answered away"
        );
    }

    /// A namespace mid-teardown is still in `IggyPartitions`, so it reads as
    /// materialised while the fence forbids serving it. `ConfirmRemove` would
    /// answer its frames, but a disk delete that keeps failing never enqueues
    /// one, so the sweep has to.
    #[compio::test]
    async fn parked_frames_of_a_tombstoned_namespace_are_reclaimed_without_confirm_remove() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-tombstone-park");
        seed_topic(&mux, 2, 0, "topic-tombstone-park", vec![assignment(0, 1)]);

        let (shard, _inbox) = build_test_shard_with_inbox(0, &config, mux, 1);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        park_one_request(&shard, ns).await;
        reconcile_pass(&ctx).await;
        assert_eq!(
            shard.parked_frame_count(ns),
            1,
            "the full inbox must have re-parked the frame"
        );

        // Teardown's synchronous fence, without the `ConfirmRemove` a wedged
        // disk delete never reaches.
        shard.plane.partitions().tombstone(ns);
        shard.shards_table().remove(&ns);

        reconcile_pass(&ctx).await;
        assert_eq!(
            shard.parked_frame_count(ns),
            0,
            "frames behind a tombstone must not wait on a ConfirmRemove that may never come"
        );
        assert!(
            shard.plane.partitions().is_tombstoned(&ns),
            "and the fence must still be standing, so this was the sweep's doing"
        );
    }

    /// Residency backstop: an inbox that never drains must not hold a re-parked
    /// frame forever, so `MAX_PARKED_PASSES` covers a materialised namespace too.
    #[compio::test]
    async fn a_re_parked_frame_ages_out_when_the_inbox_never_drains() {
        let tmp = TempDir::new().expect("tempdir for system path");
        let config = test_config(&tmp);
        let mux = TestMux::default();
        seed_stream(&mux, 1, "stream-repark-age");
        seed_topic(&mux, 2, 0, "topic-repark-age", vec![assignment(0, 1)]);

        let (shard, _inbox) = build_test_shard_with_inbox(0, &config, mux, 1);
        let ctx = make_ctx(Rc::clone(&shard), 1, Rc::new(config));
        let ns = IggyNamespace::new(0, 0, 0);

        park_one_request(&shard, ns).await;
        reconcile_pass(&ctx).await;
        assert_eq!(shard.parked_frame_count(ns), 1, "re-parked on a full inbox");

        for _ in 0..=PARK_MAX_PASSES {
            reconcile_pass(&ctx).await;
        }
        assert_eq!(
            shard.parked_frame_count(ns),
            0,
            "a materialised namespace must still be aged, or the frame is stranded"
        );
        assert!(!shard.has_parked_partition_frames());
    }

    /// Mirrors `MAX_PARKED_PER_NAMESPACE` in `shard::park_if_unmaterialised`.
    const PARK_CAP: usize = 128;
    /// Mirrors `MAX_PARKED_BYTES`.
    const SHARD_BUDGET: usize = 16 * 1024 * 1024;
    /// Mirrors `MAX_PARKED_BYTES_PER_NAMESPACE`.
    const NAMESPACE_BUDGET: usize = SHARD_BUDGET / 4;
    /// Body the byte-budget tests park, and its charged footprint: a parked
    /// frame keeps its whole `MESSAGE_ALIGN`-granular buffer, so the header
    /// pushes a 1 MiB body into the next page.
    const MIB_BODY: usize = 1024 * 1024;
    const MIB_FOOTPRINT: usize = MIB_BODY + 4096;
    /// Footprint of a header-only frame: buffers are `MESSAGE_ALIGN`-granular.
    const HEADER_FOOTPRINT: usize = 4096;
    /// Header-only frames one namespace admits before its byte budget refuses
    /// more. Requests stop at [`PARK_CAP`] long before this; prepares do not.
    const HEADER_FRAMES_PER_NAMESPACE: usize = NAMESPACE_BUDGET / HEADER_FOOTPRINT;
    /// [`MIB_BODY`] frames one namespace admits before its budget refuses more.
    const PER_NAMESPACE_MIB_FRAMES: usize = NAMESPACE_BUDGET / MIB_FOOTPRINT;
    /// Mirrors `MAX_PARKED_PASSES`.
    const PARK_MAX_PASSES: u32 = 3;
    /// Top 16 bits are the owning shard, so this resolves to shard 0's registry
    /// and `send_to_client` takes the local path instead of a forward fn.
    const TEST_CLIENT_ID: u128 = 1;
    const TEST_REQUEST_ID: u64 = 1;

    /// `cluster::multi_shard_partition_convergence` exists to drive the
    /// cross-core path, which only happens for namespaces the connection's shard
    /// does not own. That property is a murmur3 outcome, invisible from the
    /// integration test itself: it would stay green while silently degrading to
    /// single-shard if the hash or the shard count changed. Pin it here, where
    /// the assignment is a pure function, over the namespaces that test creates
    /// (stream 0, topics 0..8, partition 0 - the slab keys the STM hands out).
    #[test]
    fn integration_topic_set_straddles_both_shards() {
        let owners: Vec<u16> = (0..8)
            .map(|topic_id| calculate_shard_assignment(&IggyNamespace::new(0, topic_id, 0), 2))
            .collect();
        let on_shard_one = owners.iter().filter(|owner| **owner == 1).count();
        assert!(
            on_shard_one > 0 && on_shard_one < owners.len(),
            "the integration test's topics must land on both shards, else it \
             silently stops covering the cross-core path; got {owners:?}"
        );
    }

    fn park_overflow_count(shard: &TestShard) -> u64 {
        shard.metrics().frame_drop_count(
            shard::metrics::frame_drop_variant::PARTITION,
            shard::metrics::frame_drop_reason::PARK_OVERFLOW,
        )
    }

    fn park_dropped_count(shard: &TestShard) -> u64 {
        shard.metrics().frame_drop_count(
            shard::metrics::frame_drop_variant::PARTITION,
            shard::metrics::frame_drop_reason::PARK_DROPPED,
        )
    }

    /// Register the client id [`build_partition_request`] stamps plus a waiter
    /// for its request id, so a deny reaching the bus resolves a oneshot. The
    /// guard keeps the slot installed.
    fn register_waiting_client(
        shard: &TestShard,
    ) -> (
        message_bus::ReplySlotGuard<'_, u128>,
        futures::channel::oneshot::Receiver<message_bus::BusMessage>,
    ) {
        shard
            .bus
            .clients()
            .insert_in_process(TEST_CLIENT_ID)
            .expect("fresh client key");
        shard
            .bus
            .clients()
            .install_reply_slot(TEST_CLIENT_ID, TEST_REQUEST_ID)
            .expect("reply slot installs")
    }

    fn reply_status(reply: &message_bus::BusMessage) -> u32 {
        bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &reply.as_ref()[..size_of::<ReplyHeader>()],
        )
        .expect("deny reply carries a valid ReplyHeader")
        .status
    }
}
