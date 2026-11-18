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

//! The manifest object of a tiered partition, written by its primary.
//!
//! The manifest lists the segments the store holds, for a reader that has
//! the bucket and nothing else. The primary builds it from its chain: the
//! sealed segments whose hash a committed floor advance matched. An advance
//! commits only after its segment is stored, so the manifest never names an
//! object that an upload has not finished.
//!
//! A pass publishes when the manifest would differ from the one this primary
//! last stored. A new primary stores one on its first pass, whatever the
//! store holds. It stores the same one again [`MANIFEST_REPEAT_AFTER`]
//! later: the put of a deposed primary can reach the store after its job was
//! cancelled, and would otherwise leave an older manifest there until the
//! next change.

use std::cmp::Ordering;
use std::time::Duration;

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, ManifestSegment, ObjectStore, ObjectStoreError, PartitionManifest, PutBody,
    PutOptions, TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest, TierScope,
    TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use tracing::{debug, warn};

use crate::IggyPartition;
use crate::segment_chain::SegmentRecord;

/// How long after its first manifest a primary stores the same one again.
/// Longer than any put of the primary before it can stay in flight.
pub const MANIFEST_REPEAT_AFTER: Duration = Duration::from_mins(2);

/// The result of a publish: the store's answer, or that the job's scope was
/// cancelled first.
pub type ManifestPublishOutcome = Result<Result<(), ObjectStoreError>, TierJobCancelled>;

/// Stores the manifests of a shard's partitions, behind a pointer, so the
/// shard does not carry the executor's type.
pub trait ManifestPublisher {
    /// Store `manifest` under `scope`. The job stops when the scope is
    /// cancelled, as when its partition loses primacy.
    fn publish(
        &self,
        manifest: PartitionManifest,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, ManifestPublishOutcome>;
}

/// Publishes through a tier executor, in its upload class.
///
/// A manifest is the last step of an upload, and a reader sees no segment
/// until it is stored, so it must not wait behind the work that housekeeping
/// yields to.
pub struct ExecutorManifestPublisher<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorManifestPublisher<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the publishes go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> ManifestPublisher for ExecutorManifestPublisher<E> {
    fn publish(
        &self,
        manifest: PartitionManifest,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, ManifestPublishOutcome> {
        let key = self.layout.manifest_key(&manifest.prefix);
        let body = manifest.encode();
        let request =
            TierJobRequest::new(TierJobClass::Upload, scope).with_bytes(body.len() as u64);
        self.executor
            .submit(request, move |store| async move {
                let options = PutOptions {
                    crc32c: Some(crc32c::crc32c(&body)),
                    ..PutOptions::default()
                };
                store.put(&key, PutBody::from_bytes(body), &options).await
            })
            .boxed_local()
    }
}

/// What a manifest says the store holds: its floor, its trim point and how
/// many segments it lists.
///
/// The floor and the trim point only rise. Between two changes of either,
/// the list only gets shorter, as a replace puts one segment in the place of
/// several. So two manifests of one incarnation that agree on all three list
/// the same segments, and of two that differ the greater one is the later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestProgress {
    floor: Option<u64>,
    trim_point: Option<u64>,
    segments: usize,
}

impl ManifestProgress {
    #[cfg(test)]
    pub(crate) const fn new(floor: Option<u64>, trim_point: Option<u64>, segments: usize) -> Self {
        Self {
            floor,
            trim_point,
            segments,
        }
    }
}

impl From<&PartitionManifest> for ManifestProgress {
    fn from(manifest: &PartitionManifest) -> Self {
        Self {
            floor: manifest.floor,
            trim_point: manifest.trim_point,
            segments: manifest.segments.len(),
        }
    }
}

impl Ord for ManifestProgress {
    fn cmp(&self, other: &Self) -> Ordering {
        self.floor
            .cmp(&other.floor)
            .then(self.trim_point.cmp(&other.trim_point))
            .then(other.segments.cmp(&self.segments))
    }
}

impl PartialOrd for ManifestProgress {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// What a replica publishes while it is the primary of a tiered partition.
/// Nothing here is persisted.
#[derive(Default)]
pub struct ManifestPublishes {
    /// The view this state belongs to, `None` until the replica acts as the
    /// primary.
    view: Option<u32>,
    /// What the last manifest this view stored holds.
    published: Option<ManifestProgress>,
    /// When this view's first manifest was stored.
    first_published_at: Option<IggyTimestamp>,
    /// Whether a manifest was stored [`MANIFEST_REPEAT_AFTER`] or more after
    /// the first one.
    repeated: bool,
    /// The job in flight: its number, what its manifest holds, and the scope
    /// whose end stops it.
    running: Option<(u64, ManifestProgress, TierScope)>,
}

impl ManifestPublishes {
    fn repeat_due(&self, now: IggyTimestamp) -> bool {
        !self.repeated
            && self
                .first_published_at
                .is_some_and(|first| now.as_micros() >= repeat_at(first))
    }
}

fn repeat_at(first_published_at: IggyTimestamp) -> u64 {
    let wait = u64::try_from(MANIFEST_REPEAT_AFTER.as_micros()).unwrap_or(u64::MAX);
    first_published_at.as_micros().saturating_add(wait)
}

impl From<&SegmentRecord> for ManifestSegment {
    fn from(record: &SegmentRecord) -> Self {
        Self {
            start_offset: record.start_offset,
            end_offset: record.end_offset,
            start_timestamp: record.start_timestamp,
            end_timestamp: record.end_timestamp,
            max_timestamp: record.max_timestamp,
            size: record.size,
            crc32c: record.crc32c,
            hash: record.hash,
        }
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The manifest of what the store holds of this partition, as this
    /// replica's chain records it. `None` without a chain.
    ///
    /// Also `None` while the chain cannot describe every segment up to the
    /// tiered floor: an advance committed that this replica has not matched
    /// against a segment of its own yet, or its own segment differs from the
    /// stored one. A manifest that stopped short of the floor would be older
    /// than one an earlier primary stored, and a reader refuses a manifest
    /// that goes back.
    #[must_use]
    pub fn manifest(&self, now: IggyTimestamp) -> Option<PartitionManifest> {
        if self.places_batches() {
            return Some(self.long_term_manifest(now));
        }
        let chain = self.segment_chain.as_ref()?;
        let trim_point = self.chain_trim_point();
        let segments: Vec<ManifestSegment> =
            chain.stored_segments().map(ManifestSegment::from).collect();
        let listed_up_to = segments.last().map(|segment| segment.end_offset);
        let floor = self.tiered_floor().max(trim_point);
        if floor > listed_up_to.max(trim_point) {
            return None;
        }
        Some(PartitionManifest {
            prefix: self.object_prefix()?,
            published_at: now.as_micros(),
            floor: listed_up_to.or(trim_point),
            trim_point,
            segments_keyed_by_both_offsets: false,
            segments,
        })
    }

    /// The manifest of a partition that places its batches: its long-term
    /// log. Every segment of the log was stored before its command was sent,
    /// so the manifest names no object that is not there.
    fn long_term_manifest(&self, now: IggyTimestamp) -> PartitionManifest {
        PartitionManifest {
            prefix: crate::object_prefix(self.namespace(), self.created_revision()),
            published_at: now.as_micros(),
            floor: self.long_term_floor(),
            trim_point: self.long_term_trim_point(),
            segments_keyed_by_both_offsets: true,
            segments: self.long_term_segments().to_vec(),
        }
    }

    /// What a manifest of the long-term log as it is now would hold.
    pub(crate) fn long_term_progress(&self) -> ManifestProgress {
        ManifestProgress {
            floor: self.long_term_floor(),
            trim_point: self.long_term_trim_point(),
            segments: self.long_term_segments().len(),
        }
    }

    /// Whether this replica is the one that publishes the partition's
    /// manifest: the settled primary of a tiered partition, or of one that
    /// places its batches.
    fn publishes_manifest(&self) -> bool {
        self.deletes_objects() || (self.places_batches() && self.state_machines_current())
    }

    /// Whether this replica is the primary of a partition that places its
    /// batches and no pass ran over it in this view. Until its first publish,
    /// a reader finds the manifest an earlier primary stored, which may list
    /// fewer segments than are committed.
    #[must_use]
    pub fn manifest_awaits_first_pass(&self) -> bool {
        self.places_batches()
            && self.publishes_manifest()
            && self.manifest_publishes.view != Some(self.consensus().view())
    }

    /// Start the publish of this partition's manifest, when this replica is
    /// the primary, none is in flight, and the manifest differs from the
    /// one this view last stored or is due again. The caller runs it under
    /// the returned scope and hands the result to
    /// [`Self::finish_manifest_publish`] with the returned number.
    ///
    /// A replica that is not the primary cancels the job it started, so a
    /// pass over a demoted replica is what stops its job.
    pub fn begin_manifest_publish(
        &mut self,
        now: IggyTimestamp,
    ) -> Option<(u64, PartitionManifest, TierScopeToken)> {
        if !self.publishes_manifest() {
            self.manifest_publishes = ManifestPublishes::default();
            return None;
        }
        let manifest = self.manifest(now)?;
        let progress = ManifestProgress::from(&manifest);
        let publishes = self.manifest_publishes_of_this_view();
        if publishes.running.is_some()
            || (publishes.published == Some(progress) && !publishes.repeat_due(now))
        {
            return None;
        }
        let scope = TierScope::new();
        let token = scope.token();
        self.manifest_publish_jobs += 1;
        let id = self.manifest_publish_jobs;
        self.manifest_publishes.running = Some((id, progress, scope));
        Some((id, manifest, token))
    }

    /// Take the result of the publish numbered `id`. A result of a job this
    /// partition does not wait for, or one that arrives after primacy moved,
    /// changes nothing. A publish that failed is repeated by a later pass.
    ///
    /// Returns whether the long-term log moved while the manifest was being
    /// stored. No publish starts while one runs, so the caller stages the
    /// next one.
    pub fn finish_manifest_publish(
        &mut self,
        id: u64,
        outcome: &ManifestPublishOutcome,
        now: IggyTimestamp,
    ) -> bool {
        let awaited = self
            .manifest_publishes
            .running
            .as_ref()
            .is_some_and(|(running, _, _)| *running == id);
        if !awaited {
            return false;
        }
        if !self.publishes_manifest()
            || self.manifest_publishes.view != Some(self.consensus().view())
        {
            self.manifest_publishes = ManifestPublishes::default();
            return false;
        }
        let namespace_raw = self.namespace().inner();
        let view = self.consensus().view();
        let long_term = self.places_batches().then(|| self.long_term_progress());
        let publishes = &mut self.manifest_publishes;
        let Some((_, progress, _scope)) = publishes.running.take() else {
            return false;
        };
        match outcome {
            Ok(Ok(())) => {
                match publishes.first_published_at {
                    Some(first) => publishes.repeated |= now.as_micros() >= repeat_at(first),
                    None => publishes.first_published_at = Some(now),
                }
                publishes.published = Some(progress);
                self.long_term_deletes.manifest_stored(view, progress, now);
                debug!(
                    namespace_raw,
                    floor = ?progress.floor,
                    trim_point = ?progress.trim_point,
                    segments = progress.segments,
                    "stored the manifest of the partition"
                );
                long_term.is_some_and(|current| current != progress)
            }
            Ok(Err(error)) => {
                warn!(
                    namespace_raw,
                    %error,
                    "storing the manifest of the partition failed and is tried again"
                );
                false
            }
            Err(TierJobCancelled) => false,
        }
    }

    /// What the manifest this view last stored holds, `None` before its
    /// first one.
    pub(crate) fn stored_manifest_progress(&self) -> Option<ManifestProgress> {
        self.manifest_publishes
            .published
            .filter(|_| self.manifest_publishes.view == Some(self.consensus().view()))
    }

    /// Whether the manifest this view last stored already leaves out every
    /// segment at or below this replica's trim point, so their objects can go
    /// without a reader finding a listed segment missing.
    fn manifest_covers_trim_point(&self) -> bool {
        let Some(trim_point) = self.chain_trim_point() else {
            return true;
        };
        self.manifest_publishes.view == Some(self.consensus().view())
            && self
                .manifest_publishes
                .published
                .is_some_and(|published| published.trim_point >= Some(trim_point))
    }

    /// Whether this replica is the one that deletes the partition's objects
    /// and has yet to store a manifest that leaves out what its trim point
    /// retired. Its deletes wait for that manifest.
    #[must_use]
    pub fn object_deletes_await_manifest(&self) -> bool {
        self.deletes_objects() && !self.manifest_covers_trim_point()
    }

    /// The publish state of the current view. What an earlier view left is
    /// dropped, and its job cancelled.
    fn manifest_publishes_of_this_view(&mut self) -> &mut ManifestPublishes {
        let view = self.consensus().view();
        if self.manifest_publishes.view != Some(view) {
            self.manifest_publishes = ManifestPublishes {
                view: Some(view),
                ..ManifestPublishes::default()
            };
        }
        &mut self.manifest_publishes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn progress(
        floor: Option<u64>,
        trim_point: Option<u64>,
        segments: usize,
    ) -> ManifestProgress {
        ManifestProgress::new(floor, trim_point, segments)
    }

    #[test]
    fn given_the_states_of_one_history_when_ordering_their_progress_should_put_each_after_the_one_before()
     {
        // Two publishes, a replace of both segments, two more publishes, a
        // trim of the first segment and a replace of the two that are left.
        let history = [
            progress(None, None, 0),
            progress(Some(9), None, 1),
            progress(Some(19), None, 2),
            progress(Some(19), None, 1),
            progress(Some(29), None, 2),
            progress(Some(39), None, 3),
            progress(Some(39), Some(19), 2),
            progress(Some(39), Some(19), 1),
        ];

        for (earlier, later) in history.iter().zip(&history[1..]) {
            assert!(earlier < later, "{earlier:?} is before {later:?}");
        }
        assert!(history.is_sorted());
    }

    #[test]
    fn given_a_shorter_list_at_a_lower_floor_when_ordering_progress_should_take_the_higher_floor_as_later()
     {
        assert!(progress(Some(9), Some(4), 1) < progress(Some(19), None, 5));
        assert!(progress(Some(19), None, 1) < progress(Some(19), Some(4), 5));
    }
}
