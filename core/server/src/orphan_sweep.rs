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

//! The sweep of this cluster's store root for the objects of partition
//! incarnations that committed metadata no longer holds. What counts as an
//! orphan, and when one is deleted, is in [`partitions::OrphanCandidates`]
//! and the module it belongs to.
//!
//! Shard 0 of the caught-up metadata primary sweeps, while the shard holds
//! an object deleter, which it does only while its node passes the store
//! identity check of [`crate::store_identity`]. A sweep reads the revision
//! of committed metadata and the incarnations it holds first. Then it reads
//! the store identity object, and lists the root in housekeeping jobs of a
//! few pages each, and drops the incarnations committed metadata holds from
//! what each job listed as the job ends. An identity object that names
//! another cluster ends the sweep. An orphan that is due goes to the queue
//! of [`crate::incarnation_deletes`], whose delete checks committed metadata
//! and the view, and reads the claim of the root again.
//!
//! The sweeps time their cadence and the grace with the monotonic clock of
//! the node, so a step of its wall clock neither starts a sweep early nor
//! ends a grace early.
//!
//! What the sweeps found lives in memory only. It is dropped when the node
//! stops being the metadata primary, its view changes, it loses its object
//! deleter, or a sweep does not list the whole root.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use consensus::{MetadataHandle, VsrConsensus};
use iggy_common::IggyTimestamp;
use iggy_object_store::{ObjectPrefix, TierJobCancelled, TierScope, TierScopeToken};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::StreamsInner;
use partitions::{
    ClaimStamp, ObjectDeleter, OrphanCandidates, OrphanGuard, RootListing, SweepFindings,
    SweepStamp, sweep_interval,
};
use tracing::{debug, info, warn};

use crate::incarnation_deletes::{IncarnationDeletes, namespace_of};
use crate::shell::ServerShard;
use crate::store_identity::own_cluster_uid;

/// The most incarnations a log line that names them lists.
const LOGGED_INCARNATIONS_MAX: usize = 16;

/// The sweep of orphan incarnations of one shard.
pub struct OrphanSweep {
    /// `None` when the sweep is off: no backend, or `orphan_grace` at zero.
    grace: Option<Duration>,
    /// Shared with the task of the sweep, which settles what it found.
    state: Rc<RefCell<State>>,
}

#[derive(Default)]
struct State {
    candidates: OrphanCandidates,
    /// The orphans the last sweep found due, which the next pass queues.
    due: Vec<(ObjectPrefix, OrphanGuard)>,
    /// The sweep in flight with its number. Dropping the scope cancels it.
    running: Option<(u64, TierScope)>,
    /// Number of the last sweep this node started.
    sweeps: u64,
    /// When the next sweep may start, by the monotonic clock. `None` before
    /// the first, so the first pass of a primary sweeps.
    next_sweep_at: Option<Instant>,
    /// Whether the gauges show what a sweep found.
    reported: bool,
}

impl OrphanSweep {
    #[must_use]
    pub fn new(config: &TieredStorageConfig) -> Self {
        let grace = config.orphan_grace.get_duration();
        Self {
            grace: (config.backend != TieredStorageBackend::None && !grace.is_zero())
                .then_some(grace),
            state: Rc::default(),
        }
    }

    /// Queue the orphans the last sweep found due with `deletes` at `now`,
    /// and start a sweep in a detached task when one is due at `monotonic`,
    /// the same moment by the monotonic clock. A pass while that task runs
    /// starts nothing. A shard that may not sweep drops what the sweeps found
    /// and stops its task.
    pub fn pass(
        &self,
        shard: &Rc<ServerShard>,
        deletes: &IncarnationDeletes,
        now: IggyTimestamp,
        monotonic: Instant,
    ) {
        let Some(grace) = self.grace else {
            return;
        };
        let (Some(view), Some(cluster_uid)) = (sweeping_view(shard), own_cluster_uid(shard)) else {
            if self.drop_findings() {
                shard.metrics().set_orphan_findings(0, 0);
            }
            return;
        };
        let mut state = self.state.borrow_mut();
        let due = std::mem::take(&mut state.due);
        if !due.is_empty() {
            let offered = due.len();
            let queued = deletes.queue_orphans(shard, due, now);
            info!(
                offered,
                queued, "queued the delete of orphan incarnations that the sweeps found due"
            );
        }
        if state.running.is_some() || state.next_sweep_at.is_some_and(|next| monotonic < next) {
            return;
        }
        state.next_sweep_at = Some(monotonic + sweep_interval(grace));
        // Read before any listing: an incarnation above the revision may have
        // been created while the listing ran.
        let committed = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(Committed::read);
        let scope = TierScope::new();
        state.sweeps += 1;
        let sweep = Sweep {
            id: state.sweeps,
            owner: Rc::clone(shard),
            state: Rc::clone(&self.state),
            token: scope.token(),
            view,
            cluster_uid,
            committed,
            grace,
            started: monotonic,
        };
        state.running = Some((sweep.id, scope));
        drop(state);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        shard.bus.spawn(sweep.run());
    }

    /// Drop what the sweeps found and cancel the sweep in flight.
    pub fn stop(&self) {
        self.drop_findings();
    }

    /// [`Self::stop`]. Returns whether anything was dropped.
    fn drop_findings(&self) -> bool {
        let mut state = self.state.borrow_mut();
        let held = state.running.is_some()
            || state.reported
            || !state.candidates.is_empty()
            || !state.due.is_empty();
        *state = State {
            sweeps: state.sweeps,
            ..State::default()
        };
        held
    }

    /// Whether a sweep is in flight.
    #[cfg(test)]
    pub fn running(&self) -> bool {
        self.state.borrow().running.is_some()
    }

    /// How many orphans the last full sweep found.
    #[cfg(test)]
    pub fn candidates(&self) -> usize {
        self.state.borrow().candidates.len()
    }
}

/// The metadata view in which `shard` sweeps: shard 0 of the caught-up
/// metadata primary, while it holds an object deleter. `None` otherwise.
fn sweeping_view(shard: &ServerShard) -> Option<u32> {
    let metadata = shard.plane.metadata();
    if shard.id != 0 || !metadata.is_caught_up_primary() || shard.object_deleter().is_none() {
        return None;
    }
    metadata.consensus.as_ref().map(VsrConsensus::view)
}

/// One sweep of the root, which runs its jobs one after another.
struct Sweep {
    id: u64,
    owner: Rc<ServerShard>,
    state: Rc<RefCell<State>>,
    token: TierScopeToken,
    view: u32,
    cluster_uid: u128,
    committed: Committed,
    grace: Duration,
    /// When the pass that started the sweep ran, by the monotonic clock,
    /// which the previous sweep ended before.
    started: Instant,
}

/// What committed metadata held before the listing: its revision, and the
/// incarnations of its partitions, read together.
struct Committed {
    revision: u64,
    incarnations: BTreeSet<ObjectPrefix>,
}

impl Committed {
    fn read(inner: &StreamsInner) -> Self {
        let mut incarnations = BTreeSet::new();
        for (stream_id, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                for partition in &topic.partitions {
                    incarnations.insert(ObjectPrefix {
                        stream_id: stream_id as u64,
                        topic_id: topic_id as u64,
                        partition_id: partition.id as u64,
                        created_revision: partition.created_revision,
                    });
                }
            }
        }
        Self {
            revision: inner.revision,
            incarnations,
        }
    }

    /// Whether committed metadata held a partition of exactly the
    /// incarnation `prefix`. Ids that name no namespace cannot be checked,
    /// so they count as held, and nothing deletes them.
    fn holds(&self, prefix: &ObjectPrefix) -> bool {
        namespace_of(prefix).is_none() || self.incarnations.contains(prefix)
    }
}

/// Why a sweep did not list the whole root.
enum Stopped {
    /// Its scope was cancelled, or the node may no longer sweep.
    Cancelled,
    /// The store did not answer, or the identity object is not this
    /// cluster's claim.
    NotFull,
}

impl From<TierJobCancelled> for Stopped {
    fn from(_: TierJobCancelled) -> Self {
        Self::Cancelled
    }
}

impl Sweep {
    async fn run(self) {
        let swept = self.sweep().await;
        let mut state = self.state.borrow_mut();
        if state.running.as_ref().is_none_or(|(id, _)| *id != self.id) {
            return;
        }
        state.running = None;
        let metrics = self.owner.metrics();
        match swept {
            Ok((claim, listing)) => {
                let findings = SweepFindings::of(&listing, self.committed.revision, |prefix| {
                    self.committed.holds(prefix)
                });
                let stamp = SweepStamp {
                    view: self.view,
                    claim: claim.clone(),
                };
                let due =
                    state
                        .candidates
                        .settle(stamp, &findings.orphans, self.started, self.grace);
                state.due.extend(due.into_iter().map(|(prefix, newest)| {
                    let guard = OrphanGuard {
                        view: self.view,
                        claim: claim.clone(),
                        newest,
                    };
                    (prefix, guard)
                }));
                metrics.record_orphan_sweep();
                metrics
                    .set_orphan_findings(findings.orphans.len(), findings.objects_before_claim());
                state.reported = true;
                log_findings(&listing, &findings, state.due.len());
            }
            // A sweep that stopped early, cancelled or not, must not let the
            // next full sweep count on the one before it.
            Err(Stopped::Cancelled | Stopped::NotFull) => {
                state.candidates.clear();
                metrics.set_orphan_findings(0, 0);
                state.reported = false;
            }
        }
    }

    /// Read the claim of the root and list all of it.
    async fn sweep(&self) -> Result<(ClaimStamp, RootListing), Stopped> {
        let claim = match self.deleter()?.read_claim(self.token.clone()).await? {
            Ok(Some(claim)) if claim.cluster_uid == self.cluster_uid => claim,
            Ok(found) => {
                self.owner.metrics().record_orphan_identity_skip();
                warn!(
                    found = found.map(|claim| format!("{:032x}", claim.cluster_uid)),
                    "the store identity object does not name this cluster, so the sweep of \
                     orphan incarnations deletes nothing"
                );
                return Err(Stopped::NotFull);
            }
            Err(error) => {
                warn!(
                    %error,
                    "reading the store identity object for the sweep of orphan incarnations \
                     failed; the next sweep starts over"
                );
                return Err(Stopped::NotFull);
            }
        };
        let mut listing = RootListing::default();
        let mut after = None;
        loop {
            let pages = match self
                .deleter()?
                .list_root(after, claim.claimed_at, self.token.clone())
                .await?
            {
                Ok(pages) => pages,
                Err(error) => {
                    warn!(
                        %error,
                        "listing the object store root for orphan incarnations failed; the next \
                         sweep starts over"
                    );
                    return Err(Stopped::NotFull);
                }
            };
            let mut part = pages.listing;
            part.incarnations
                .retain(|prefix, _| !self.committed.holds(prefix));
            listing.merge(part);
            after = pages.next;
            if after.is_none() {
                break;
            }
        }
        self.deleter()?;
        Ok((claim, listing))
    }

    /// The deleter for the next job, while the node may still sweep in the
    /// view the sweep started in.
    fn deleter(&self) -> Result<Rc<dyn ObjectDeleter>, Stopped> {
        if sweeping_view(&self.owner) != Some(self.view) {
            return Err(Stopped::Cancelled);
        }
        self.owner.object_deleter().ok_or(Stopped::Cancelled)
    }
}

fn log_findings(listing: &RootListing, findings: &SweepFindings, due: usize) {
    debug!(
        unheld = listing.incarnations.len(),
        orphans = findings.orphans.len(),
        due,
        unrecognised = listing.unrecognised,
        "swept the object store root for orphan incarnations"
    );
    if !findings.orphans.is_empty() {
        let sample: Vec<&ObjectPrefix> = findings
            .orphans
            .keys()
            .take(LOGGED_INCARNATIONS_MAX)
            .collect();
        info!(
            orphans = findings.orphans.len(),
            due,
            ?sample,
            "found partition incarnations with objects in the store that committed metadata no \
             longer holds; each is deleted once the sweeps find the same objects of it for \
             tiered_storage.orphan_grace"
        );
    }
    if !findings.before_claim.is_empty() {
        let sample: Vec<&ObjectPrefix> = findings
            .before_claim
            .keys()
            .take(LOGGED_INCARNATIONS_MAX)
            .collect();
        warn!(
            incarnations = findings.before_claim.len(),
            objects = findings.objects_before_claim(),
            ?sample,
            "found partition incarnations that committed metadata does not hold with objects no \
             newer than this cluster's claim of the object store root, in whole seconds, so a \
             cluster before it may have left them; the sweep never deletes them, and an operator \
             may. The claim is when the store committed the identity object, and a repair or a \
             recovery that puts it again moves the claim later, so objects this cluster wrote \
             before then count as older than the claim too"
        );
    }
}
