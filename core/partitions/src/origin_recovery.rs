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

//! What a cluster rebuilt on empty disks takes back of a geo partition of
//! the cluster that wrote its store before, the retired one.
//!
//! The store keeps three things of the partition: the manifest, which lists
//! its long-term segments up to the floor F, and the self-describing fresh
//! objects, whose indexes hold the batches above it. A proof row binds a
//! batch to the offset and the timestamp it committed at, so the batches
//! the proofs name from F on without a gap come back exactly, up to P. The
//! batches no proof names carry only the offsets their writer expected,
//! their claims. The ones that follow P without a gap come back at those
//! offsets, up to the cutover Q, unless adoption is off:
//!
//! - Of two claims on one offset, the one of the higher view wins, then
//!   the one of the later writer life, then the later object of that life.
//!   A batch that loses one of its offsets is dropped whole.
//! - The walk from P takes the batch that starts at the next offset, and
//!   stops at the first offset no batch takes.
//! - An adopted batch commits at `max(previous + 1, sealed_at)`, the
//!   timestamp of the batch before it plus one or when its object was
//!   sealed, so timestamps keep rising.
//!
//! Two valid proofs that bind one offset to different batches mean that the
//! retired cluster's consensus diverged. The proven range then ends below
//! that offset and nothing is adopted.
//!
//! Only objects of the self-describing format count. One of the format
//! before, or of another cluster, is reported and skipped.
//!
//! The result is a [`RecoveryPlan`] per partition, which the rebuilt cluster
//! stores before any node builds the partition. Each replica then installs
//! the same plan into its empty partition, as a state transfer installs
//! placed segments: the manifest's segments as its long-term log and the
//! taken batches as its placed batches, with the next offset after Q.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Range;

use bytes::Bytes;
use consensus::StateArtifact;
use consensus::state_manifest::artifact_kind;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader, BatchProducer};
use iggy_binary_protocol::placement::{PLACED_BATCH_SIZE, PlacedBatch, Placement};
use iggy_object_store::{
    FreshBatchRow, FreshDirectoryRow, FreshIndex, FreshObjectError, FreshObjectId, FreshProofRow,
    KeyLayout, ListRequest, ManifestError, ObjectPrefix, ObjectStore, ObjectStoreError,
    PartitionManifest, PutBody, PutMode, PutOptions, UserMetadata,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::sharding::IggyNamespace;

use crate::IggyPartition;
use crate::long_term::{LONG_TERM_LOG_MACHINE, LongTermLog};
use crate::producer_state::{ProducerTable, StampedBatch};
use crate::proven_batch::ProvenBatch;
use crate::segment_chain::object_prefix;
use crate::state_machine::{PartitionStateMachine, StateMachineSnapshot};
use crate::state_transfer::{PartitionInstallError, PartitionTransferUnavailable, SpillError};
use crate::types::PartitionsConfig;

/// Bytes of the first read of each fresh object, which holds the whole
/// index of most objects.
pub const FRESH_INDEX_FIRST_READ: u64 = 64 * 1024;

/// The most batches one plan takes, which keeps a plan below 1.3 GiB.
pub const RECOVERY_PLAN_BATCHES_MAX: usize = 4 * 1024 * 1024;

const PLAN_MAGIC: u64 = u64::from_le_bytes(*b"IGGYRPL1");
const PLAN_HEADER_LEN: usize = 8 + 16 + 4 * 8 + 8;
const PLAN_CHECKSUM_LEN: usize = 4;

/// The indexes of the retired cluster's fresh objects, and the objects the
/// scan did not take.
#[derive(Debug, Default)]
pub struct FreshScan {
    indexes: BTreeMap<FreshObjectId, FreshIndex>,
    skipped: Vec<SkippedObject>,
}

/// A fresh object the scan did not take, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedObject {
    pub object_id: FreshObjectId,
    pub reason: SkipReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// The object starts with the magic of another format, such as the one
    /// before objects described themselves.
    NotSelfDescribing,
    /// The index does not decode.
    Unreadable(FreshObjectError),
    /// Another cluster wrote the object.
    OtherCluster(u128),
    /// The object went between the listing and its read.
    Gone,
}

/// What the recovery of one partition takes and drops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRecovery {
    pub plan: RecoveryPlan,
    /// The batches the plan leaves out, in the order the walk met them.
    pub dropped: Vec<DroppedBatch>,
    /// The first offset two valid proofs bind to different batches, when
    /// the proven range ended there.
    pub conflict_at: Option<u64>,
}

/// A batch of the retired cluster that the recovery does not take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedBatch {
    /// The object that holds its messages.
    pub object_id: FreshObjectId,
    /// Where its messages start in that object.
    pub position: u64,
    /// The offsets its proof binds it to, or that its writer claimed.
    pub offsets: Range<u64>,
    pub reason: DropReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// A proof names it, and the proven range stopped below it.
    ProvenPastGap,
    /// Its proof does not rebuild the batch it names.
    InvalidProof,
    /// Its claim starts below the end of the proven range.
    OverlapsProven,
    /// A claim of a higher view, writer life or object takes one of its
    /// offsets.
    LostOverlap,
    /// The walk stopped below its claim.
    PastGap,
    /// Adoption of the batches no proof names is off.
    AdoptionOff,
    /// Two proofs bind one offset to different batches, so no claim is
    /// adopted.
    ProofsConflict,
    /// Its row carries a producer that no batch header carries.
    InvalidProducer,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecoveryPlanError {
    #[error("the manifest describes {found:?}, and the partition is {expected:?}")]
    ManifestOfAnotherPartition {
        expected: ObjectPrefix,
        found: ObjectPrefix,
    },
    #[error("the manifest of {0:?} lists segments that are not long-term segments")]
    NotLongTermSegments(ObjectPrefix),
    #[error("the plan holds {0} batches, more than a plan takes")]
    TooManyBatches(usize),
    #[error("the plan is {0} bytes, too short for its fields")]
    Truncated(usize),
    #[error("the plan starts with {0:#018x}, a version this build does not read")]
    UnsupportedVersion(u64),
    #[error("the plan's checksum is {stored:#010x}, and its bytes hash to {computed:#010x}")]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("the plan's manifest does not decode: {0}")]
    Manifest(ManifestError),
    #[error("the plan's batch at offset {offset} does not continue the ones before it")]
    Batches { offset: u64 },
    #[error("the plan's proven range ends at {0}, which is not where a batch ends")]
    ProvenEnd(u64),
}

/// What one replica installs into its empty partition: see the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub retired_cluster_uid: u128,
    pub prefix: ObjectPrefix,
    /// The retired cluster's last manifest of the partition.
    pub manifest: Option<PartitionManifest>,
    /// One past the last offset that a proof names, at a batch boundary.
    pub proven_end: u64,
    /// The batches from the manifest's floor on, proven first, each starting
    /// where the one before it ends.
    pub batches: Vec<PlacedBatch>,
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryInstallError {
    #[error("the plan is of {plan:?}, and the partition is {partition:?}")]
    OtherPartition {
        plan: ObjectPrefix,
        partition: ObjectPrefix,
    },
    #[error("the partition does not place its batches")]
    DoesNotPlaceBatches,
    #[error("the partition holds messages or applied an operation already")]
    NotEmpty,
    #[error("the manifest's segments are no run of offsets a long-term log takes")]
    LongTermLog,
    #[error("the partition cannot describe its empty state: {0:?}")]
    Snapshot(PartitionTransferUnavailable),
    #[error("the plan's placed batches could not be staged: {0:?}")]
    Stage(SpillError),
    #[error("the install failed: {0:?}")]
    Install(PartitionInstallError),
}

/// List every fresh object under the root of `layout` and decode the index
/// of each one that `retired_cluster_uid` wrote.
///
/// # Errors
/// The store's error for a listing or a read that failed. An object that
/// went between the listing and its read is skipped as [`SkipReason::Gone`].
pub async fn scan_fresh_objects<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    retired_cluster_uid: u128,
) -> Result<FreshScan, ObjectStoreError> {
    let prefix = layout.fresh_prefix();
    let mut scan = FreshScan::default();
    let mut continuation: Option<String> = None;
    loop {
        let request = ListRequest::new(&prefix);
        let request = continuation
            .as_deref()
            .map_or(request, |continuation| request.after(continuation));
        let page = store.list(&request).await?;
        for listed in &page.objects {
            let Some(object_id) = layout
                .parse_fresh_key(&listed.key)
                .map(FreshObjectId::from_raw)
            else {
                continue;
            };
            match read_index(store, &listed.key, object_id).await? {
                Ok(index) if index.header().cluster_uid == retired_cluster_uid => {
                    scan.insert(index);
                }
                Ok(index) => scan.skip(
                    object_id,
                    SkipReason::OtherCluster(index.header().cluster_uid),
                ),
                Err(reason) => scan.skip(object_id, reason),
            }
        }
        continuation = page.next;
        if continuation.is_none() {
            return Ok(scan);
        }
    }
}

/// The index of the object under `key`, read with one ranged read, or two
/// when the index is longer than the first.
async fn read_index<S: ObjectStore>(
    store: &S,
    key: &str,
    object_id: FreshObjectId,
) -> Result<Result<FreshIndex, SkipReason>, ObjectStoreError> {
    let mut range = 0..FRESH_INDEX_FIRST_READ;
    loop {
        let read = match store.get_range(key, range.clone()).await {
            Ok(read) => read,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(Err(SkipReason::Gone)),
            Err(ObjectStoreError::InvalidRange { .. }) => {
                return Ok(Err(SkipReason::Unreadable(FreshObjectError::Incomplete {
                    have: 0,
                    need: range.end,
                })));
            }
            Err(error) => return Err(error),
        };
        match FreshIndex::decode(&read.bytes, object_id) {
            Err(FreshObjectError::Incomplete { need, .. })
                if need > range.end && need <= read.size =>
            {
                range = 0..need;
            }
            Ok(index) => return Ok(Ok(index)),
            Err(FreshObjectError::UnknownMagic(_)) => {
                return Ok(Err(SkipReason::NotSelfDescribing));
            }
            Err(error) => return Ok(Err(SkipReason::Unreadable(error))),
        }
    }
}

impl FreshScan {
    /// Take the index of an object of the retired cluster.
    pub fn insert(&mut self, index: FreshIndex) {
        self.indexes.insert(index.header().object_id, index);
    }

    pub fn skip(&mut self, object_id: FreshObjectId, reason: SkipReason) {
        self.skipped.push(SkippedObject { object_id, reason });
    }

    /// The objects taken, ascending by id.
    pub fn indexes(&self) -> impl Iterator<Item = &FreshIndex> {
        self.indexes.values()
    }

    #[must_use]
    pub fn skipped(&self) -> &[SkippedObject] {
        &self.skipped
    }

    /// Each object that holds a directory row of `namespace` of revision
    /// `created_revision`, with that row.
    fn rows_of(
        &self,
        namespace: u64,
        created_revision: u64,
    ) -> impl Iterator<Item = (&FreshIndex, &FreshDirectoryRow)> {
        self.indexes.values().filter_map(move |index| {
            index
                .partition(namespace)
                .filter(|row| row.created_revision == created_revision)
                .map(|row| (index, row))
        })
    }
}

/// What the store gives back of the partition `prefix`: see the module docs.
///
/// # Errors
/// [`RecoveryPlanError::ManifestOfAnotherPartition`] for a manifest of
/// another prefix, and [`RecoveryPlanError::NotLongTermSegments`] for one
/// whose segments are keyed by their first offset alone, as those of a
/// tiered topic are.
pub fn plan_partition(
    retired_cluster_uid: u128,
    prefix: ObjectPrefix,
    manifest: Option<PartitionManifest>,
    scan: &FreshScan,
    adopt_unproven: bool,
) -> Result<PartitionRecovery, RecoveryPlanError> {
    if let Some(manifest) = &manifest {
        if manifest.prefix != prefix {
            return Err(RecoveryPlanError::ManifestOfAnotherPartition {
                expected: prefix,
                found: manifest.prefix,
            });
        }
        if !manifest.segments.is_empty() && !manifest.segments_keyed_by_both_offsets {
            return Err(RecoveryPlanError::NotLongTermSegments(prefix));
        }
    }
    let namespace = namespace_of(&prefix);
    let published_end = manifest
        .as_ref()
        .and_then(|manifest| manifest.floor)
        .map_or(0, |floor| floor.saturating_add(1));
    let mut dropped = Vec::new();
    let proofs = ProvenRows::collect(scan, namespace, prefix.created_revision, published_end);
    dropped.extend(proofs.invalid.iter().cloned());
    let walk = proofs.walk(published_end);
    dropped.extend(walk.past_gap);
    let proven_end = walk.end;
    let mut batches = walk.batches;
    let candidates = Candidates::collect(scan, namespace, prefix.created_revision, &proofs.named);
    let previous_timestamp = batches
        .last()
        .map(|batch| batch.header.base_timestamp)
        .or_else(|| {
            manifest
                .as_ref()
                .and_then(|manifest| manifest.segments.last())
                .map(|segment| segment.max_timestamp)
        })
        .unwrap_or(0);
    let blocked = if walk.conflict_at.is_some() {
        Some(DropReason::ProofsConflict)
    } else if adopt_unproven {
        None
    } else {
        Some(DropReason::AdoptionOff)
    };
    let adopted = candidates.adopt(proven_end, previous_timestamp, blocked, &mut dropped);
    batches.extend(adopted);
    Ok(PartitionRecovery {
        plan: RecoveryPlan {
            retired_cluster_uid,
            prefix,
            manifest,
            proven_end,
            batches,
        },
        dropped,
        conflict_at: walk.conflict_at,
    })
}

/// The valid proofs of one partition above the floor, by the offset they
/// bind, and every batch a proof names at any offset.
struct ProvenRows {
    rows: BTreeMap<u64, (FreshProofRow, PlacedBatch)>,
    /// Offsets two valid proofs bind to different batches.
    conflicts: BTreeSet<u64>,
    /// `(object, position)` of every batch a proof of the partition names,
    /// valid or not.
    named: HashSet<(FreshObjectId, u64)>,
    invalid: Vec<DroppedBatch>,
}

/// Where the walk of the proven batches from the floor stopped.
struct ProvenWalk {
    end: u64,
    batches: Vec<PlacedBatch>,
    past_gap: Vec<DroppedBatch>,
    conflict_at: Option<u64>,
}

impl ProvenRows {
    fn collect(
        scan: &FreshScan,
        namespace: u64,
        created_revision: u64,
        published_end: u64,
    ) -> Self {
        let mut proven = Self {
            rows: BTreeMap::new(),
            conflicts: BTreeSet::new(),
            named: HashSet::new(),
            invalid: Vec::new(),
        };
        for (index, row) in scan.rows_of(namespace, created_revision) {
            for proof in index.proofs_of(row) {
                proven.named.insert((proof.object_id, proof.position));
                // Published offsets come back from the manifest, and a proof
                // that reaches below the floor names no batch above it.
                if proof.base_offset < published_end {
                    continue;
                }
                let rebuilt = scan
                    .indexes
                    .get(&proof.object_id)
                    .and_then(|source| ProvenBatch::find(proof, row, source).ok())
                    .and_then(|found| PlacedBatch::try_from(found).ok());
                let Some(placed) = rebuilt else {
                    proven.invalid.push(DroppedBatch {
                        object_id: proof.object_id,
                        position: proof.position,
                        offsets: proven_offsets(proof),
                        reason: DropReason::InvalidProof,
                    });
                    continue;
                };
                match proven.rows.get(&proof.base_offset) {
                    None => {
                        proven.rows.insert(proof.base_offset, (*proof, placed));
                    }
                    // Every object of a primary carries the proofs it has not
                    // seen stored yet, so a proof repeats.
                    Some((known, _)) if known == proof => {}
                    Some(_) => {
                        proven.conflicts.insert(proof.base_offset);
                    }
                }
            }
        }
        proven
    }

    /// The proven batches from `published_end` on without a gap, and the
    /// proven ones past where they stop.
    fn walk(&self, published_end: u64) -> ProvenWalk {
        let mut next = published_end;
        let mut batches = Vec::new();
        let mut conflict_at = None;
        while let Some((proof, placed)) = self.rows.get(&next) {
            if self.conflicts.contains(&next) {
                conflict_at = Some(next);
                break;
            }
            let Some(end) = proof.end_offset() else {
                break;
            };
            batches.push(*placed);
            next = end;
        }
        let past_gap = self
            .rows
            .range(next..)
            .map(|(_, (proof, _))| DroppedBatch {
                object_id: proof.object_id,
                position: proof.position,
                offsets: proven_offsets(proof),
                reason: DropReason::ProvenPastGap,
            })
            .collect();
        ProvenWalk {
            end: next,
            batches,
            past_gap,
            conflict_at,
        }
    }
}

fn proven_offsets(proof: &FreshProofRow) -> Range<u64> {
    proof.base_offset..proof.end_offset().unwrap_or(u64::MAX)
}

/// The batches of one partition that no proof names, with the offsets their
/// writer claimed.
struct Candidates<'a> {
    claims: Vec<Candidate<'a>>,
}

#[derive(Clone)]
struct Candidate<'a> {
    object_id: FreshObjectId,
    position: u64,
    offsets: Range<u64>,
    view: u32,
    sealed_at: u64,
    row: &'a FreshDirectoryRow,
    batch: &'a FreshBatchRow,
}

impl Candidate<'_> {
    /// The order of two claims on one offset: the higher view first, then
    /// the later writer life, then the later object of that life. The id
    /// and the position after those only make the order total.
    fn rank(&self) -> (u32, u64, u64, u128, u64) {
        let id = self.object_id.get();
        let started_at = u64::try_from(id >> 64).unwrap_or(u64::MAX);
        let sequence = u64::try_from(id & u128::from(FreshObjectId::SEQUENCE_MAX)).unwrap_or(0);
        (self.view, started_at, sequence, id, self.position)
    }

    fn dropped(&self, reason: DropReason) -> DroppedBatch {
        DroppedBatch {
            object_id: self.object_id,
            position: self.position,
            offsets: self.offsets.clone(),
            reason,
        }
    }
}

impl<'a> Candidates<'a> {
    fn collect(
        scan: &'a FreshScan,
        namespace: u64,
        created_revision: u64,
        named: &HashSet<(FreshObjectId, u64)>,
    ) -> Self {
        let mut claims = Vec::new();
        for (index, row) in scan.rows_of(namespace, created_revision) {
            let header = index.header();
            let mut start = row.claimed_first_offset;
            for (position, batch) in index.batches_of(row) {
                let end = start.saturating_add(u64::from(batch.message_count));
                if !named.contains(&(header.object_id, position)) {
                    claims.push(Candidate {
                        object_id: header.object_id,
                        position,
                        offsets: start..end,
                        view: row.view,
                        sealed_at: header.sealed_at,
                        row,
                        batch,
                    });
                }
                start = end;
            }
        }
        Self { claims }
    }

    /// The claims adopted from `proven_end` on, unless `blocked` names why
    /// none is. `previous_timestamp` is the timestamp of the batch below
    /// `proven_end`. What is not adopted goes to `dropped`.
    fn adopt(
        self,
        proven_end: u64,
        previous_timestamp: u64,
        blocked: Option<DropReason>,
        dropped: &mut Vec<DroppedBatch>,
    ) -> Vec<PlacedBatch> {
        // Only the claims above the proven range are candidates.
        let mut eligible: Vec<Candidate<'_>> = Vec::with_capacity(self.claims.len());
        for claim in self.claims {
            if claim.offsets.end <= proven_end {
                continue;
            }
            if claim.offsets.start < proven_end {
                dropped.push(claim.dropped(DropReason::OverlapsProven));
            } else if let Some(reason) = blocked {
                dropped.push(claim.dropped(reason));
            } else {
                eligible.push(claim);
            }
        }
        eligible.sort_by_key(|claim| std::cmp::Reverse(claim.rank()));
        // Each claim against every higher-ranked one: a claim that shares an
        // offset with one loses whole, and still takes its offsets from the
        // claims below it.
        let mut taken = Claimed::default();
        let mut whole = BTreeMap::new();
        for claim in eligible {
            let offsets = claim.offsets.clone();
            if taken.overlaps(&offsets) {
                dropped.push(claim.dropped(DropReason::LostOverlap));
            } else {
                whole.insert(offsets.start, claim);
            }
            taken.add(offsets);
        }
        let mut adopted = Vec::new();
        let mut next = proven_end;
        let mut timestamp = previous_timestamp;
        while let Some(claim) = whole.remove(&next) {
            timestamp = timestamp.saturating_add(1).max(claim.sealed_at);
            let Some(placed) = placed_batch(
                claim.row,
                claim.batch,
                claim.object_id,
                claim.position,
                next,
                timestamp,
            ) else {
                dropped.push(claim.dropped(DropReason::InvalidProducer));
                break;
            };
            adopted.push(placed);
            next = claim.offsets.end;
        }
        dropped.extend(
            whole
                .into_values()
                .map(|claim| claim.dropped(DropReason::PastGap)),
        );
        adopted
    }
}

/// The offsets that the claims taken so far hold, merged into disjoint
/// ranges keyed by their start.
#[derive(Default)]
struct Claimed {
    ranges: BTreeMap<u64, u64>,
}

impl Claimed {
    fn overlaps(&self, offsets: &Range<u64>) -> bool {
        self.ranges
            .range(..offsets.end)
            .next_back()
            .is_some_and(|(_, end)| *end > offsets.start)
    }

    fn add(&mut self, offsets: Range<u64>) {
        let (mut start, mut end) = (offsets.start, offsets.end);
        let touching: Vec<(u64, u64)> = self
            .ranges
            .range(..=end)
            .rev()
            .take_while(|(_, other_end)| **other_end >= start)
            .map(|(other_start, other_end)| (*other_start, *other_end))
            .collect();
        for (other_start, other_end) in touching {
            self.ranges.remove(&other_start);
            start = start.min(other_start);
            end = end.max(other_end);
        }
        self.ranges.insert(start, end);
    }
}

/// The placed batch of the batch row `batch` under `row`, whose messages
/// start at `position` of `object_id`, committed at `base_offset` and
/// `base_timestamp`. `None` for a producer that no batch header carries.
fn placed_batch(
    row: &FreshDirectoryRow,
    batch: &FreshBatchRow,
    object_id: FreshObjectId,
    position: u64,
    base_offset: u64,
    base_timestamp: u64,
) -> Option<PlacedBatch> {
    let producer = match batch.producer {
        Some(producer) => Some(BatchProducer::new(
            producer.producer_id,
            producer.epoch,
            producer.base_sequence,
        )?),
        None => None,
    };
    let length = u64::from(batch.blob_length);
    Some(PlacedBatch::new(
        BatchHeader {
            partition_id: IggyNamespace::from_raw(row.namespace).partition_id() as u64,
            base_offset,
            base_timestamp,
            origin_timestamp: batch.origin_timestamp,
            batch_length: BATCH_HEADER_SIZE as u64 + length,
            batch_checksum: 0,
            message_count: batch.message_count,
            producer,
        },
        Placement {
            object_id: object_id.get(),
            position,
            length,
            blob_checksum: batch.blob_checksum,
            proven_below: row.proven_below,
        },
    ))
}

/// The raw namespace of the partition that `prefix` names in the cluster
/// that wrote it.
pub fn namespace_of(prefix: &ObjectPrefix) -> u64 {
    let id = |value: u64| usize::try_from(value).unwrap_or(usize::MAX);
    IggyNamespace::new(
        id(prefix.stream_id),
        id(prefix.topic_id),
        id(prefix.partition_id),
    )
    .inner()
}

impl PartitionRecovery {
    /// How many of the plan's batches proofs name.
    #[must_use]
    pub fn proven_batches(&self) -> usize {
        self.plan
            .batches
            .iter()
            .take_while(|batch| batch.header.base_offset < self.plan.proven_end)
            .count()
    }

    /// How many of the plan's batches only their claims place.
    #[must_use]
    pub fn adopted_batches(&self) -> usize {
        self.plan.batches.len() - self.proven_batches()
    }
}

impl RecoveryPlan {
    /// One past the last offset of the manifest, where the plan's batches
    /// start.
    #[must_use]
    pub fn published_end(&self) -> u64 {
        self.manifest
            .as_ref()
            .and_then(|manifest| manifest.floor)
            .map_or(0, |floor| floor.saturating_add(1))
    }

    /// One past the last offset the plan takes: where the rebuilt
    /// partition's next batch starts.
    #[must_use]
    pub fn cutover(&self) -> u64 {
        self.batches.last().map_or_else(
            || self.published_end(),
            |batch| {
                batch
                    .header
                    .base_offset
                    .saturating_add(u64::from(batch.header.message_count))
            },
        )
    }

    /// Whether the plan gives the partition anything: a segment or a
    /// batch.
    #[must_use]
    pub fn holds_anything(&self) -> bool {
        self.cutover() > 0
            || self
                .manifest
                .as_ref()
                .is_some_and(|manifest| !manifest.segments.is_empty())
    }

    /// `magic u64 "IGGYRPL1" | retired_cluster_uid u128 | stream_id u64 |
    /// topic_id u64 | partition_id u64 | created_revision u64 | proven_end
    /// u64 | manifest length u32 | manifest | batch count u32 | placed
    /// batches | crc32c u32`, little-endian. A manifest length of zero is no
    /// manifest.
    ///
    /// # Errors
    /// What [`Self::decode`] would refuse of these fields.
    pub fn encode(&self) -> Result<Vec<u8>, RecoveryPlanError> {
        self.check()?;
        let manifest = self
            .manifest
            .as_ref()
            .map_or_else(Vec::new, PartitionManifest::encode);
        let mut bytes = Vec::with_capacity(
            PLAN_HEADER_LEN
                + 4
                + manifest.len()
                + 4
                + self.batches.len() * PLACED_BATCH_SIZE
                + PLAN_CHECKSUM_LEN,
        );
        bytes.extend_from_slice(&PLAN_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&self.retired_cluster_uid.to_le_bytes());
        for value in [
            self.prefix.stream_id,
            self.prefix.topic_id,
            self.prefix.partition_id,
            self.prefix.created_revision,
            self.proven_end,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let manifest_len = u32::try_from(manifest.len())
            .map_err(|_| RecoveryPlanError::Truncated(manifest.len()))?;
        bytes.extend_from_slice(&manifest_len.to_le_bytes());
        bytes.extend_from_slice(&manifest);
        let count = u32::try_from(self.batches.len())
            .map_err(|_| RecoveryPlanError::TooManyBatches(self.batches.len()))?;
        bytes.extend_from_slice(&count.to_le_bytes());
        for batch in &self.batches {
            let start = bytes.len();
            bytes.resize(start + PLACED_BATCH_SIZE, 0);
            batch.encode_into(&mut bytes[start..]);
        }
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    /// # Errors
    /// See [`RecoveryPlanError`]. A plan that fails here must not be used in
    /// part.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryPlanError> {
        let truncated = || RecoveryPlanError::Truncated(bytes.len());
        let (body, checksum) = bytes
            .split_last_chunk::<PLAN_CHECKSUM_LEN>()
            .ok_or_else(truncated)?;
        let mut fields = PlanFields(body);
        let magic = fields.u64().ok_or_else(truncated)?;
        if magic != PLAN_MAGIC {
            return Err(RecoveryPlanError::UnsupportedVersion(magic));
        }
        let stored = u32::from_le_bytes(*checksum);
        let computed = crc32c::crc32c(body);
        if stored != computed {
            return Err(RecoveryPlanError::ChecksumMismatch { stored, computed });
        }
        let retired_cluster_uid = fields.u128().ok_or_else(truncated)?;
        let prefix = ObjectPrefix {
            stream_id: fields.u64().ok_or_else(truncated)?,
            topic_id: fields.u64().ok_or_else(truncated)?,
            partition_id: fields.u64().ok_or_else(truncated)?,
            created_revision: fields.u64().ok_or_else(truncated)?,
        };
        let proven_end = fields.u64().ok_or_else(truncated)?;
        let manifest_len = fields.u32().ok_or_else(truncated)?;
        let manifest = fields
            .take(usize::try_from(manifest_len).unwrap_or(usize::MAX))
            .ok_or_else(truncated)?;
        let manifest = (!manifest.is_empty())
            .then(|| PartitionManifest::decode(manifest))
            .transpose()
            .map_err(RecoveryPlanError::Manifest)?;
        let count = usize::try_from(fields.u32().ok_or_else(truncated)?).unwrap_or(usize::MAX);
        if count > RECOVERY_PLAN_BATCHES_MAX {
            return Err(RecoveryPlanError::TooManyBatches(count));
        }
        let batches_len = count.checked_mul(PLACED_BATCH_SIZE).ok_or_else(truncated)?;
        let batches = fields.take(batches_len).ok_or_else(truncated)?;
        if !fields.0.is_empty() {
            return Err(truncated());
        }
        let mut decoded = Vec::with_capacity(count);
        for bytes in batches.as_chunks::<PLACED_BATCH_SIZE>().0 {
            let offset = decoded
                .last()
                .map_or(0, |batch: &PlacedBatch| batch.header.base_offset);
            decoded.push(
                PlacedBatch::decode(bytes).map_err(|_| RecoveryPlanError::Batches { offset })?,
            );
        }
        let plan = Self {
            retired_cluster_uid,
            prefix,
            manifest,
            proven_end,
            batches: decoded,
        };
        plan.check()?;
        Ok(plan)
    }

    /// Store the plan under its key, over any object there. The recovery
    /// stores it before the rebuilt cluster owns the root, and builds the
    /// same plan from the same store when it runs again.
    ///
    /// # Errors
    /// The refusals of [`Self::encode`] as
    /// [`ObjectStoreError::InvalidRequest`], and any error of the put.
    pub async fn store<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<(), ObjectStoreError> {
        let bytes = self
            .encode()
            .map_err(|error| ObjectStoreError::InvalidRequest(error.to_string()))?;
        let options = PutOptions {
            mode: PutMode::Overwrite,
            metadata: UserMetadata::default(),
            crc32c: Some(crc32c::crc32c(&bytes)),
        };
        store
            .put(
                &layout.recovery_plan_key(self.retired_cluster_uid, &self.prefix),
                PutBody::from_bytes(Bytes::from(bytes)),
                &options,
            )
            .await
    }

    /// The plan of `prefix` that the recovery of `retired_cluster_uid`
    /// stored, `None` when it stored none.
    ///
    /// # Errors
    /// Any error of the read but a missing object, and a plan that does not
    /// decode or names another partition as
    /// [`ObjectStoreError::Malformed`].
    pub async fn read<S: ObjectStore>(
        store: &S,
        layout: &KeyLayout,
        retired_cluster_uid: u128,
        prefix: &ObjectPrefix,
    ) -> Result<Option<Self>, ObjectStoreError> {
        let key = layout.recovery_plan_key(retired_cluster_uid, prefix);
        let size = match store.head(&key).await {
            Ok(head) => head.size,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let read = match store.get_range(&key, 0..size).await {
            Ok(read) => read,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        match Self::decode(&read.bytes) {
            Ok(plan)
                if plan.prefix == *prefix && plan.retired_cluster_uid == retired_cluster_uid =>
            {
                Ok(Some(plan))
            }
            Ok(plan) => Err(ObjectStoreError::Malformed(format!(
                "the recovery plan under {key} is of {:?}",
                plan.prefix
            ))),
            Err(error) => Err(ObjectStoreError::Malformed(format!(
                "the recovery plan under {key} does not decode: {error}"
            ))),
        }
    }

    fn check(&self) -> Result<(), RecoveryPlanError> {
        if self.batches.len() > RECOVERY_PLAN_BATCHES_MAX {
            return Err(RecoveryPlanError::TooManyBatches(self.batches.len()));
        }
        if let Some(manifest) = &self.manifest {
            if manifest.prefix != self.prefix {
                return Err(RecoveryPlanError::ManifestOfAnotherPartition {
                    expected: self.prefix,
                    found: manifest.prefix,
                });
            }
            if !manifest.segments.is_empty() && !manifest.segments_keyed_by_both_offsets {
                return Err(RecoveryPlanError::NotLongTermSegments(self.prefix));
            }
        }
        let partition_id = self.prefix.partition_id;
        let mut next = self.published_end();
        let mut proven_at_boundary = self.proven_end == next;
        for batch in &self.batches {
            let header = &batch.header;
            if header.base_offset != next
                || header.message_count == 0
                || header.partition_id != partition_id
            {
                return Err(RecoveryPlanError::Batches {
                    offset: header.base_offset,
                });
            }
            next = next.saturating_add(u64::from(header.message_count));
            proven_at_boundary |= self.proven_end == next;
        }
        if !proven_at_boundary {
            return Err(RecoveryPlanError::ProvenEnd(self.proven_end));
        }
        Ok(())
    }
}

/// Reads fields off the front of a plan whose checksum matched.
struct PlanFields<'a>(&'a [u8]);

impl<'a> PlanFields<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (field, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(field)
    }

    fn u128(&mut self) -> Option<u128> {
        self.take(16)?.try_into().ok().map(u128::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.take(8)?.try_into().ok().map(u64::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.take(4)?.try_into().ok().map(u32::from_le_bytes)
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Install `plan` into this partition, which must be built and empty and
    /// not yet serve any frame. Every replica installs the same plan, so the
    /// group starts from the same state at op 0: the manifest's segments as
    /// the long-term log, the plan's batches as committed placed batches, the
    /// producers those batches name, and the next offset at the plan's
    /// cutover.
    ///
    /// # Errors
    /// See [`RecoveryInstallError`].
    pub async fn install_recovery_plan(
        &mut self,
        config: &PartitionsConfig,
        plan: &RecoveryPlan,
    ) -> Result<(), RecoveryInstallError> {
        let own = object_prefix(self.namespace(), self.created_revision());
        if plan.prefix != own {
            return Err(RecoveryInstallError::OtherPartition {
                plan: plan.prefix,
                partition: own,
            });
        }
        if !self.places_batches() {
            return Err(RecoveryInstallError::DoesNotPlaceBatches);
        }
        if self.offset_space.append_live
            || self.offset_space.committed_seeded
            || self.consensus().commit_min() > 0
        {
            return Err(RecoveryInstallError::NotEmpty);
        }
        let mut offsets = self
            .offsets_wire_snapshot()
            .map_err(RecoveryInstallError::Snapshot)?;
        offsets.next_offset = plan.cutover();
        offsets.machines = long_term_snapshot(plan)?;
        offsets.producers = producers_of(&plan.batches);
        let mut staged = Vec::new();
        if !plan.batches.is_empty() {
            let mut body = vec![0; plan.batches.len() * PLACED_BATCH_SIZE];
            for (batch, bytes) in plan
                .batches
                .iter()
                .zip(body.as_chunks_mut::<PLACED_BATCH_SIZE>().0)
            {
                batch.encode_into(bytes);
            }
            let artifact =
                StateArtifact::for_bytes(artifact_kind::SEGMENT_LOG, plan.published_end(), &body);
            staged.push(
                self.spill_transfer_segment(&artifact, body, None)
                    .await
                    .map_err(RecoveryInstallError::Stage)?,
            );
        }
        self.install_state_transfer(config, 0, staged, &offsets.encode())
            .await
            .map_err(RecoveryInstallError::Install)?;
        Ok(())
    }
}

/// The machines of a partition whose long-term log lists the plan's
/// manifest, at op 0. `None` without a segment.
fn long_term_snapshot(
    plan: &RecoveryPlan,
) -> Result<Option<StateMachineSnapshot>, RecoveryInstallError> {
    let Some(manifest) = plan
        .manifest
        .as_ref()
        .filter(|manifest| !manifest.segments.is_empty())
    else {
        return Ok(None);
    };
    let log = LongTermLog::listing(&manifest.segments).ok_or(RecoveryInstallError::LongTermLog)?;
    let mut state = Vec::new();
    log.snapshot_into(&mut state);
    Ok(Some(StateMachineSnapshot::from_parts(
        0,
        &[(LONG_TERM_LOG_MACHINE, &state)],
    )))
}

/// The producer table that committing `batches` in order leaves.
fn producers_of(batches: &[PlacedBatch]) -> Vec<(u64, crate::producer_state::ProducerEntry)> {
    let mut table = ProducerTable::from_entries(std::iter::empty());
    for batch in batches {
        if let Some(producer) = batch.header.producer {
            table.record(&StampedBatch {
                producer,
                message_count: batch.header.message_count,
                base_offset: batch.header.base_offset,
                timestamp: batch.header.base_timestamp,
            });
        }
    }
    table
        .entries()
        .map(|(producer_id, entry)| (producer_id, *entry))
        .collect()
}

#[cfg(test)]
mod tests;
