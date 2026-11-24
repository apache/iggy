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

//! Random histories of one tiered partition: uploads, uploads cut short by a
//! crash, truncations, evictions, primacy changes, manifest publishes that
//! the store takes, refuses or keeps in flight, delete passes and time.
//!
//! A reader holds only the store. Every object the manifest there names must
//! be stored, and once the primary's passes settle, the manifest lists what
//! the primary's chain file records as stored.

use super::eviction::{KEEP_NOTHING, evict};
use super::floor_eviction::apply;
use super::manifest_publish::regain_primacy;
use super::remote_poll::layout;
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::segment_chain::{ChainRecord, read_chain};
use crate::tiered_floor::AdvanceTieredFloor;
use crate::{
    ExecutorDeleter, ExecutorManifestPublisher, MANIFEST_REPEAT_AFTER, ManifestPublisher,
    ObjectDeleter, upload_segment,
};
use iggy_object_store::{
    InlineTierExecutor, ManifestSegment, MemoryStore, ObjectStore, ObjectStoreError,
    PartitionManifest, PutBody, PutOptions, SegmentKey, SegmentObject, TierExecutor, TierJobHandle,
    TierJobRequest,
};
use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

/// One-message ops of each history. Two fill a segment, so the partition
/// seals twelve segments and keeps the last op in its active one.
const OPS: u64 = 25;
/// A put of a deposed primary that lands after the first manifest of the
/// next one, over a change, takes a few hundred histories to come up.
const HISTORIES: u64 = 512;
const STEPS_PER_HISTORY: usize = 96;
/// The longest [`Step::Wait`], past the grace period of a retired object.
const LONGEST_WAIT: Duration = Duration::from_mins(15);

#[derive(Debug, Clone, Copy)]
enum Step {
    /// Store the oldest sealed segment above the floor and commit its
    /// advance: this replica's upload while it is the primary, the upload of
    /// another primary otherwise.
    Upload,
    /// Store that segment, and crash before its advance commits.
    Orphan,
    /// Commit a truncation through one of the two oldest sealed segments.
    Trim,
    /// Evict what the floor confirms.
    Evict,
    /// Primacy moves to another replica.
    Depose,
    /// Primacy comes back in a later view.
    Regain,
    /// A cleaner pass: a manifest publish that the store takes, then a delete
    /// pass.
    Pass,
    /// A manifest publish that the store refuses.
    RefusedPublish,
    /// A manifest publish whose put stays in flight.
    HoldPublish,
    /// The put in flight lands, or fails once it was in flight longer than
    /// any put can be.
    LandPublish,
    Wait,
}

/// Drawn evenly, so a step that appears more often runs more often.
const STEPS: [Step; 20] = [
    Step::Upload,
    Step::Upload,
    Step::Upload,
    Step::Upload,
    Step::Orphan,
    Step::Trim,
    Step::Evict,
    Step::Evict,
    Step::Depose,
    Step::Regain,
    Step::Pass,
    Step::Pass,
    Step::Pass,
    Step::Pass,
    Step::RefusedPublish,
    Step::HoldPublish,
    Step::LandPublish,
    Step::Wait,
    Step::Wait,
    Step::Wait,
];

#[compio::test]
async fn given_random_uploads_trims_and_primary_changes_when_a_reader_decodes_the_manifest_should_find_the_chain_s_segments_and_only_stored_objects()
 {
    for seed in 0..HISTORIES {
        Box::pin(replay_history(seed)).await;
    }
}

async fn replay_history(seed: u64) {
    let mut rng = SplitMix64(seed);
    let mut history = Box::pin(History::new(seed)).await;
    for _ in 0..STEPS_PER_HISTORY {
        let step = STEPS[rng.below(STEPS.len())];
        Box::pin(history.run(step, &mut rng)).await;
        history.assert_manifest_names_only_stored_objects().await;
    }
    Box::pin(history.settle()).await;
    history.assert_manifest_names_only_stored_objects().await;
}

/// One executor for uploads, publishes and deletes, so all of them reach one
/// store.
#[derive(Clone)]
struct Shared(Rc<InlineTierExecutor<MemoryStore>>);

impl TierExecutor for Shared {
    type Store = MemoryStore;

    fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        self.0.submit(request, job)
    }
}

struct History {
    seed: u64,
    directory: tempfile::TempDir,
    partition: IggyPartition<IggyMessageBus>,
    executor: Shared,
    now: IggyTimestamp,
    op: u64,
    committed_up_to: u64,
    /// A publish whose put is in flight: its number, its manifest, and when
    /// it started.
    held: Option<(u64, PartitionManifest, IggyTimestamp)>,
    steps: Vec<Step>,
}

impl History {
    async fn new(seed: u64) -> Self {
        let (directory, partition) = Box::pin(tiered_partition(&segment_records(OPS), 0)).await;
        Self {
            seed,
            directory,
            partition,
            executor: Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new()))),
            now: IggyTimestamp::now(),
            op: 100,
            committed_up_to: 0,
            held: None,
            steps: Vec::new(),
        }
    }

    fn store(&self) -> &MemoryStore {
        self.executor.0.store()
    }

    async fn run(&mut self, step: Step, rng: &mut SplitMix64) {
        self.steps.push(step);
        match step {
            Step::Upload => Box::pin(self.upload(true)).await,
            Step::Orphan => Box::pin(self.upload(false)).await,
            Step::Trim => Box::pin(self.trim(rng)).await,
            Step::Evict => {
                if let Some(floor) = self.partition.tiered_floor() {
                    Box::pin(evict(&mut self.partition, floor, KEEP_NOTHING)).await;
                }
            }
            Step::Depose => loop {
                let view = self.partition.consensus.view() + 1;
                self.partition.consensus.set_view(view);
                if !self.partition.consensus.is_primary() {
                    break;
                }
            },
            Step::Regain => regain_primacy(&mut self.partition),
            Step::Pass => Box::pin(self.pass()).await,
            Step::RefusedPublish => {
                if let Some((id, _, _)) = self.partition.begin_manifest_publish(self.now) {
                    let refused = Ok(Err(ObjectStoreError::Unavailable("503".to_owned())));
                    self.partition
                        .finish_manifest_publish(id, &refused, self.now);
                }
            }
            Step::HoldPublish => {
                if self.held.is_none()
                    && let Some((id, manifest, _)) = self.partition.begin_manifest_publish(self.now)
                {
                    self.held = Some((id, manifest, self.now));
                }
            }
            Step::LandPublish => Box::pin(self.land()).await,
            Step::Wait => {
                let longest = u64::try_from(LONGEST_WAIT.as_micros()).unwrap();
                let wait = rng.next_u64() % (longest + 1);
                self.now = IggyTimestamp::from(self.now.as_micros() + wait);
            }
        }
    }

    async fn upload(&mut self, commits: bool) {
        let Some(upload) = self.partition.next_segment_upload() else {
            return;
        };
        upload_segment(self.store(), &layout(), &upload)
            .await
            .unwrap();
        if !commits {
            return;
        }
        let advance = if self.partition.consensus.is_primary() {
            self.partition.note_segment_uploaded(&upload.record)
        } else {
            // Another primary stores the same bytes and sends this advance.
            let next = self.partition.tiered_floor().map_or(0, |floor| floor + 1);
            Some(AdvanceTieredFloor {
                start: next.min(upload.record.start_offset),
                up_to: upload.record.end_offset,
                hash: upload.record.hash,
            })
        };
        if let Some(advance) = advance {
            self.op += 1;
            Box::pin(apply(&mut self.partition, self.op, advance)).await;
        }
    }

    async fn trim(&mut self, rng: &mut SplitMix64) {
        let sealed: Vec<u64> = self
            .partition
            .log
            .segments()
            .iter()
            .filter(|segment| segment.sealed)
            .map(|segment| segment.end_offset)
            .collect();
        if sealed.is_empty() {
            return;
        }
        let up_to = sealed[rng.below(sealed.len().min(2))];
        if self
            .partition
            .chain_trim_point()
            .is_some_and(|trim_point| trim_point >= up_to)
        {
            return;
        }
        let before: Vec<(u64, u64)> = self
            .partition
            .log
            .segments()
            .iter()
            .map(|segment| (segment.start_offset, segment.end_offset))
            .collect();
        Box::pin(self.partition.remove_sealed_segments_up_to(up_to)).await;
        Box::pin(settle_partition_io(&mut self.partition, &repair_config())).await;
        self.committed_up_to = self.committed_up_to.max(up_to);
        self.partition.note_committed_truncation(up_to);
        // The partition stamps a removal with the wall clock, which this
        // history runs ahead of, so the grace period of each removed segment
        // starts again on the history's clock.
        if self.partition.deletes_objects() {
            let front = self.partition.log.segments()[0].start_offset;
            for (start_offset, end_offset) in before {
                if start_offset < front {
                    self.partition.object_deletes.retired.retire(
                        start_offset,
                        end_offset,
                        self.now,
                    );
                }
            }
        }
    }

    /// Publish before deleting, as the cleaner stages them.
    async fn pass(&mut self) {
        let publisher = ExecutorManifestPublisher::new(self.executor.clone(), layout());
        if let Some((id, manifest, scope)) = self.partition.begin_manifest_publish(self.now) {
            let outcome = publisher.publish(manifest, scope).await;
            self.partition
                .finish_manifest_publish(id, &outcome, self.now);
            assert!(
                matches!(outcome, Ok(Ok(()))),
                "{}: {outcome:?}",
                self.trace()
            );
            self.assert_manifest_lists_the_chain_s_stored_segments()
                .await;
        }
        // As the shard does: no delete while the manifest still lists what
        // it would remove.
        if self.partition.object_deletes_await_manifest() {
            return;
        }
        let deleter = ExecutorDeleter::new(self.executor.clone(), layout());
        if let Some((job, scope)) = self
            .partition
            .begin_object_delete(self.now, self.committed_up_to)
        {
            let id = job.id;
            let outcome = deleter.run(job, scope).await;
            self.partition.finish_object_delete(id, outcome, self.now);
        }
    }

    /// A put that was sent lands whether or not its job was cancelled since,
    /// but only within [`MANIFEST_REPEAT_AFTER`] of its start.
    async fn land(&mut self) {
        let Some((id, manifest, started_at)) = self.held.take() else {
            return;
        };
        let deadline =
            started_at.as_micros() + u64::try_from(MANIFEST_REPEAT_AFTER.as_micros()).unwrap();
        let outcome = if self.now.as_micros() <= deadline {
            self.store()
                .put(
                    &layout().manifest_key(&manifest.prefix),
                    PutBody::from_bytes(manifest.encode()),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
            Ok(Ok(()))
        } else {
            Ok(Err(ObjectStoreError::Transport("timed out".to_owned())))
        };
        self.partition
            .finish_manifest_publish(id, &outcome, self.now);
    }

    /// End the put in flight, give the replica primacy, let the repeat of
    /// its first manifest fall due, and run passes until none publishes.
    async fn settle(&mut self) {
        Box::pin(self.land()).await;
        if !self.partition.deletes_objects() {
            regain_primacy(&mut self.partition);
        }
        self.now = IggyTimestamp::from(
            self.now.as_micros() + u64::try_from(MANIFEST_REPEAT_AFTER.as_micros()).unwrap(),
        );
        let publisher = ExecutorManifestPublisher::new(self.executor.clone(), layout());
        while let Some((id, manifest, scope)) = self.partition.begin_manifest_publish(self.now) {
            let outcome = publisher.publish(manifest, scope).await;
            self.partition
                .finish_manifest_publish(id, &outcome, self.now);
        }
        self.assert_manifest_lists_the_chain_s_stored_segments()
            .await;
    }

    async fn stored_manifest(&self) -> Option<PartitionManifest> {
        let key = layout().manifest_key(&PREFIX);
        let size = self.store().head(&key).await.ok()?.size;
        let object = self.store().get_range(&key, 0..size).await.unwrap();
        Some(PartitionManifest::decode(&object.bytes).unwrap())
    }

    async fn assert_manifest_names_only_stored_objects(&self) {
        let Some(manifest) = self.stored_manifest().await else {
            return;
        };
        for segment in &manifest.segments {
            let [log, index] = [SegmentObject::Log, SegmentObject::Index].map(|object| {
                layout().segment_key(&SegmentKey {
                    prefix: PREFIX,
                    start_offset: segment.start_offset,
                    object,
                })
            });
            let held = self.store().head(&log).await;
            assert!(
                held.as_ref().is_ok_and(
                    |held| held.size == segment.size && held.crc32c == Some(segment.crc32c)
                ),
                "{}: the manifest names {log}, and the store holds {held:?}",
                self.trace()
            );
            assert!(
                self.store().head(&index).await.is_ok(),
                "{}: the manifest names the segment of {index}, and the store lacks it",
                self.trace()
            );
        }
    }

    /// The segments the chain file records as stored are the live ones that
    /// a recorded floor advance matched, by their last offset and hash.
    async fn assert_manifest_lists_the_chain_s_stored_segments(&self) {
        let manifest = self.stored_manifest().await;
        let chain = read_chain(&self.directory.path().to_string_lossy())
            .await
            .unwrap()
            .unwrap();
        let advanced: BTreeSet<_> = chain
            .records
            .iter()
            .filter_map(|record| match record {
                ChainRecord::Advanced(advance) => Some((advance.up_to, advance.hash)),
                _ => None,
            })
            .collect();
        let stored: Vec<ManifestSegment> = chain
            .live_segments()
            .into_values()
            .filter_map(|record| record.segment().copied())
            .filter(|segment| advanced.contains(&(segment.end_offset, segment.hash)))
            .map(|segment| ManifestSegment::from(&segment))
            .collect();
        let trim_point = chain.trim_point();
        let expected = PartitionManifest {
            prefix: chain.prefix,
            published_at: manifest
                .as_ref()
                .map_or(0, |manifest| manifest.published_at),
            floor: stored
                .last()
                .map(|segment| segment.end_offset)
                .or(trim_point),
            trim_point,
            segments_keyed_by_both_offsets: false,
            segments: stored,
        };
        assert_eq!(manifest.as_ref(), Some(&expected), "{}", self.trace());
        for remote in chain.remote_segments() {
            assert!(
                expected
                    .segments
                    .iter()
                    .any(|segment| segment.start_offset == remote.start_offset),
                "{}: the chain holds the segment at {} only remotely, and the manifest leaves \
                 it out",
                self.trace(),
                remote.start_offset
            );
        }
    }

    fn trace(&self) -> String {
        format!("seed {} after {:?}", self.seed, self.steps)
    }
}

/// splitmix64, so a failing history replays from its seed alone.
struct SplitMix64(u64);

impl SplitMix64 {
    const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
    const FIRST_MIX: u64 = 0xBF58_476D_1CE4_E5B9;
    const SECOND_MIX: u64 = 0x94D0_49BB_1331_11EB;

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(Self::GAMMA);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(Self::FIRST_MIX);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(Self::SECOND_MIX);
        mixed ^ (mixed >> 31)
    }

    /// Below `bound`, with a bias too small to matter for these bounds.
    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next_u64() % u64::try_from(bound).unwrap()).unwrap()
    }
}
