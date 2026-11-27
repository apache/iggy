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

use super::*;
use futures::executor::block_on;
use iggy_binary_protocol::placement;
use iggy_object_store::{
    FreshBatch, FreshObject, FreshObjectHeader, FreshPartition, FreshProducer, ManifestSegment,
    MemoryStore, SealedFreshObject,
};

const RETIRED: u128 = 0x0a0a;
const OTHER: u128 = 0x0b0b;
const NOW: u64 = 1_700_000_000_000_000;
const EARLY: u64 = NOW;
const LATE: u64 = NOW + 1_000_000;

fn prefix() -> ObjectPrefix {
    ObjectPrefix {
        stream_id: 1,
        topic_id: 2,
        partition_id: 3,
        created_revision: 7,
    }
}

fn namespace() -> u64 {
    namespace_of(&prefix())
}

fn layout() -> KeyLayout {
    KeyLayout::new("iggy").unwrap()
}

fn id(started_at: u64, sequence: u64) -> FreshObjectId {
    FreshObjectId::new(0, 1, started_at, sequence).unwrap()
}

/// The manifest of segments from 0 through `floor`, the last of which
/// commits its last batch at `max_timestamp`.
fn manifest(floor: u64, max_timestamp: u64) -> PartitionManifest {
    PartitionManifest {
        prefix: prefix(),
        published_at: NOW,
        floor: Some(floor),
        trim_point: None,
        segments_keyed_by_both_offsets: true,
        segments: vec![ManifestSegment {
            start_offset: 0,
            end_offset: floor,
            start_timestamp: NOW - 10,
            end_timestamp: max_timestamp,
            max_timestamp,
            size: 4096,
            crc32c: 1,
            hash: [2; iggy_object_store::SEGMENT_HASH_LEN],
        }],
    }
}

/// One object of `cluster_uid` that holds the partition's batches of these
/// message counts, claimed from `claimed_first`, and `proofs`.
struct Object {
    id: FreshObjectId,
    cluster_uid: u128,
    view: u32,
    sealed_at: u64,
    claimed_first: u64,
    counts: Vec<u32>,
    proofs: Vec<FreshProofRow>,
}

impl Object {
    fn new(object_id: FreshObjectId, view: u32, claimed_first: u64, counts: &[u32]) -> Self {
        Self {
            id: object_id,
            cluster_uid: RETIRED,
            view,
            sealed_at: NOW,
            claimed_first,
            counts: counts.to_vec(),
            proofs: Vec::new(),
        }
    }

    fn proving(object_id: FreshObjectId, proofs: Vec<FreshProofRow>) -> Self {
        Self {
            proofs,
            ..Self::new(object_id, 1, 0, &[])
        }
    }

    fn sealed_at(self, sealed_at: u64) -> Self {
        Self { sealed_at, ..self }
    }

    fn sealed(&self) -> SealedFreshObject {
        let batches = self
            .counts
            .iter()
            .enumerate()
            .map(|(index, count)| {
                let blob = vec![u8::try_from(index % 251).unwrap(); 16 + index];
                FreshBatch {
                    message_count: *count,
                    origin_timestamp: NOW - 1,
                    blob_checksum: placement::blob_checksum(&blob),
                    producer: None,
                    blob,
                }
            })
            .collect();
        SealedFreshObject::seal(
            FreshObjectHeader {
                object_id: self.id,
                cluster_uid: self.cluster_uid,
                sealed_at: self.sealed_at,
            },
            &[FreshPartition {
                namespace: namespace(),
                created_revision: prefix().created_revision,
                view: self.view,
                claimed_first_offset: self.claimed_first,
                proven_below: 0,
                batches,
                proofs: self.proofs.clone(),
            }],
        )
        .unwrap()
    }

    fn index(&self) -> FreshIndex {
        self.sealed().index
    }
}

/// The proof that batch `batch` of `source` committed at `base_offset` and
/// `base_timestamp`.
fn proof(
    source: &FreshIndex,
    batch: usize,
    base_offset: u64,
    base_timestamp: u64,
) -> FreshProofRow {
    let row = source.partition(namespace()).unwrap();
    let (position, batch_row) = source.batches_of(row).nth(batch).unwrap();
    let object_id = source.header().object_id;
    let placed = placed_batch(
        row,
        batch_row,
        object_id,
        position,
        base_offset,
        base_timestamp,
    )
    .unwrap();
    let mut bytes = [0; PLACED_BATCH_SIZE];
    placed.encode_into(&mut bytes);
    FreshProofRow {
        object_id,
        position,
        base_offset,
        base_timestamp,
        message_count: batch_row.message_count,
        placed_checksum: placement::checksum_of(&bytes),
    }
}

fn scan(indexes: impl IntoIterator<Item = FreshIndex>) -> FreshScan {
    let mut scan = FreshScan::default();
    for index in indexes {
        scan.insert(index);
    }
    scan
}

fn plan(
    manifest: Option<PartitionManifest>,
    scan: &FreshScan,
    adopt_unproven: bool,
) -> PartitionRecovery {
    plan_partition(RETIRED, prefix(), manifest, scan, adopt_unproven).unwrap()
}

/// `(base_offset, message_count)` of each batch the plan takes.
fn offsets(recovery: &PartitionRecovery) -> Vec<(u64, u32)> {
    recovery
        .plan
        .batches
        .iter()
        .map(|batch| (batch.header.base_offset, batch.header.message_count))
        .collect()
}

fn reasons(recovery: &PartitionRecovery) -> Vec<(Range<u64>, DropReason)> {
    recovery
        .dropped
        .iter()
        .map(|dropped| (dropped.offsets.clone(), dropped.reason))
        .collect()
}

/// Three batches above floor 9: offsets 10..12, 12..15 and 15..16.
fn three_batches() -> FreshIndex {
    Object::new(id(EARLY, 1), 1, 10, &[2, 3, 1]).index()
}

#[test]
fn given_proofs_with_a_gap_when_planning_should_take_the_proven_batches_up_to_the_gap() {
    let source = three_batches();
    let first = Object::proving(
        id(EARLY, 2),
        vec![
            proof(&source, 0, 10, NOW + 10),
            proof(&source, 1, 12, NOW + 12),
        ],
    )
    .index();
    // The third batch committed at 16, so offset 15 has no proof.
    let past_gap = Object::proving(id(EARLY, 3), vec![proof(&source, 2, 16, NOW + 16)]).index();

    let recovery = plan(
        Some(manifest(9, NOW)),
        &scan([source, first, past_gap]),
        true,
    );

    assert_eq!(recovery.plan.published_end(), 10);
    assert_eq!(recovery.plan.proven_end, 15);
    assert_eq!(recovery.plan.cutover(), 15);
    assert_eq!(offsets(&recovery), [(10, 2), (12, 3)]);
    assert_eq!(
        recovery
            .plan
            .batches
            .iter()
            .map(|batch| batch.header.base_timestamp)
            .collect::<Vec<_>>(),
        [NOW + 10, NOW + 12]
    );
    assert_eq!(reasons(&recovery), [(16..17, DropReason::ProvenPastGap)]);
    assert_eq!(recovery.conflict_at, None);
}

#[test]
fn given_a_proof_that_two_objects_repeat_when_planning_should_take_its_batch_once() {
    let source = three_batches();
    let first = Object::proving(id(EARLY, 2), vec![proof(&source, 0, 10, NOW + 10)]).index();
    let again = Object::proving(
        id(EARLY, 3),
        vec![
            proof(&source, 0, 10, NOW + 10),
            proof(&source, 1, 12, NOW + 12),
            proof(&source, 2, 15, NOW + 15),
        ],
    )
    .index();

    let recovery = plan(Some(manifest(9, NOW)), &scan([source, first, again]), true);

    assert_eq!(offsets(&recovery), [(10, 2), (12, 3), (15, 1)]);
    assert_eq!(recovery.plan.proven_end, 16);
    assert!(recovery.dropped.is_empty(), "{:?}", recovery.dropped);
}

#[test]
fn given_two_different_proofs_of_one_offset_when_planning_should_end_the_proven_range_below_it_and_adopt_nothing()
 {
    let source = three_batches();
    let first = Object::proving(
        id(EARLY, 2),
        vec![
            proof(&source, 0, 10, NOW + 10),
            proof(&source, 1, 12, NOW + 12),
        ],
    )
    .index();
    let diverged = Object::proving(id(EARLY, 3), vec![proof(&source, 1, 12, NOW + 99)]).index();

    let recovery = plan(
        Some(manifest(9, NOW)),
        &scan([source, first, diverged]),
        true,
    );

    assert_eq!(recovery.conflict_at, Some(12));
    assert_eq!(recovery.plan.proven_end, 12);
    assert_eq!(offsets(&recovery), [(10, 2)]);
    assert_eq!(
        reasons(&recovery),
        [
            (12..15, DropReason::ProvenPastGap),
            (15..16, DropReason::ProofsConflict)
        ]
    );
}

#[test]
fn given_claims_on_one_offset_when_planning_should_keep_the_higher_view_then_the_later_life_then_the_later_object()
 {
    let objects = [
        // A later life, but a lower view: loses to the next one.
        Object::new(id(LATE, 9), 1, 0, &[3]),
        Object::new(id(EARLY, 1), 2, 0, &[3]),
        // One view: the later life wins over a later object of the earlier
        // one.
        Object::new(id(EARLY, 2), 2, 3, &[2]),
        Object::new(id(LATE, 1), 2, 3, &[2]),
        // One view and one life: the later object wins.
        Object::new(id(LATE, 2), 2, 5, &[1]),
        Object::new(id(LATE, 3), 2, 5, &[1]),
    ];

    let recovery = plan(None, &scan(objects.iter().map(Object::index)), true);

    let adopted: Vec<_> = recovery
        .plan
        .batches
        .iter()
        .map(|batch| FreshObjectId::from_raw(batch.placement.object_id))
        .collect();
    assert_eq!(adopted, [id(EARLY, 1), id(LATE, 1), id(LATE, 3)]);
    assert_eq!(offsets(&recovery), [(0, 3), (3, 2), (5, 1)]);
    assert_eq!(recovery.plan.proven_end, 0);
    assert_eq!(recovery.plan.cutover(), 6);
    let mut lost: Vec<_> = recovery
        .dropped
        .iter()
        .map(|dropped| (dropped.object_id, dropped.reason))
        .collect();
    lost.sort_by_key(|(object_id, _)| *object_id);
    assert_eq!(
        lost,
        [
            (id(EARLY, 2), DropReason::LostOverlap),
            (id(LATE, 2), DropReason::LostOverlap),
            (id(LATE, 9), DropReason::LostOverlap),
        ]
    );
}

#[test]
fn given_a_claim_that_loses_one_offset_when_planning_should_drop_it_whole_and_stop_there() {
    let objects = [
        Object::new(id(EARLY, 1), 1, 0, &[2, 3]),
        // Takes offset 3 of the second batch above, so that batch goes, and
        // nothing starts at 2.
        Object::new(id(EARLY, 2), 1, 3, &[1, 1]),
    ];

    let recovery = plan(None, &scan(objects.iter().map(Object::index)), true);

    assert_eq!(offsets(&recovery), [(0, 2)]);
    assert_eq!(recovery.plan.cutover(), 2);
    let mut dropped = reasons(&recovery);
    dropped.sort_by_key(|(offsets, _)| offsets.start);
    assert_eq!(
        dropped,
        [
            (2..5, DropReason::LostOverlap),
            (3..4, DropReason::PastGap),
            (4..5, DropReason::PastGap),
        ]
    );
}

#[test]
fn given_claims_after_the_proven_range_when_planning_should_stamp_timestamps_that_rise_from_the_last_proven_one()
 {
    let source = Object::new(id(EARLY, 1), 1, 10, &[2])
        .sealed_at(NOW + 5)
        .index();
    let proving = Object::proving(id(EARLY, 2), vec![proof(&source, 0, 10, NOW + 200)]).index();
    // Sealed before the proven batch committed, then long after it.
    let sealed_early = Object::new(id(EARLY, 3), 1, 12, &[1])
        .sealed_at(NOW + 150)
        .index();
    let sealed_late = Object::new(id(EARLY, 4), 1, 13, &[4])
        .sealed_at(NOW + 500)
        .index();

    let recovery = plan(
        Some(manifest(9, NOW + 100)),
        &scan([source, proving, sealed_early, sealed_late]),
        true,
    );

    assert_eq!(offsets(&recovery), [(10, 2), (12, 1), (13, 4)]);
    let stamps: Vec<_> = recovery
        .plan
        .batches
        .iter()
        .map(|batch| batch.header.base_timestamp)
        .collect();
    assert_eq!(stamps, [NOW + 200, NOW + 201, NOW + 500]);
    assert_eq!(recovery.plan.proven_end, 12);
    assert_eq!(recovery.plan.cutover(), 17);
    assert_eq!(
        (recovery.proven_batches(), recovery.adopted_batches()),
        (1, 2)
    );
}

#[test]
fn given_no_proven_batch_when_planning_should_stamp_timestamps_that_rise_from_the_manifest() {
    let claim = Object::new(id(EARLY, 1), 1, 10, &[1, 1])
        .sealed_at(NOW + 50)
        .index();

    let recovery = plan(Some(manifest(9, NOW + 100)), &scan([claim]), true);

    let stamps: Vec<_> = recovery
        .plan
        .batches
        .iter()
        .map(|batch| batch.header.base_timestamp)
        .collect();
    assert_eq!(stamps, [NOW + 101, NOW + 102]);
}

#[test]
fn given_adoption_off_when_planning_should_take_only_the_proven_range() {
    let source = Object::new(id(EARLY, 1), 1, 10, &[2, 1]).index();
    let proving = Object::proving(id(EARLY, 2), vec![proof(&source, 0, 10, NOW + 10)]).index();

    let recovery = plan(Some(manifest(9, NOW)), &scan([source, proving]), false);

    assert_eq!(offsets(&recovery), [(10, 2)]);
    assert_eq!(recovery.plan.cutover(), 12);
    assert_eq!(reasons(&recovery), [(12..13, DropReason::AdoptionOff)]);
}

#[test]
fn given_a_claim_that_reaches_into_the_proven_range_when_planning_should_drop_it() {
    let source = Object::new(id(EARLY, 1), 1, 10, &[5]).index();
    let proving = Object::proving(id(EARLY, 2), vec![proof(&source, 0, 10, NOW + 10)]).index();
    // Claims 13..16 and 16..17, after a refusal moved its writer's
    // prediction.
    let stale = Object::new(id(EARLY, 3), 1, 13, &[3, 1]).index();

    let recovery = plan(
        Some(manifest(9, NOW)),
        &scan([source, proving, stale]),
        true,
    );

    assert_eq!(offsets(&recovery), [(10, 5)]);
    assert_eq!(
        reasons(&recovery),
        [
            (13..16, DropReason::OverlapsProven),
            (16..17, DropReason::PastGap)
        ]
    );
}

#[test]
fn given_a_proof_whose_batch_does_not_rebuild_when_planning_should_drop_it() {
    let source = three_batches();
    let mut damaged = proof(&source, 0, 10, NOW + 10);
    damaged.placed_checksum ^= 1;
    let proving = Object::proving(id(EARLY, 2), vec![damaged]).index();

    let recovery = plan(Some(manifest(9, NOW)), &scan([source, proving]), true);

    assert_eq!(recovery.plan.proven_end, 10);
    assert_eq!(
        reasons(&recovery)[0],
        (10..12, DropReason::InvalidProof),
        "{:?}",
        recovery.dropped
    );
    // The batch a proof names is not a claim either.
    assert_eq!(offsets(&recovery), []);
}

#[test]
fn given_a_manifest_of_another_partition_when_planning_should_refuse_it() {
    let mut other = manifest(9, NOW);
    other.prefix.partition_id += 1;
    let mut tiered = manifest(9, NOW);
    tiered.segments_keyed_by_both_offsets = false;

    assert!(matches!(
        plan_partition(RETIRED, prefix(), Some(other), &FreshScan::default(), true),
        Err(RecoveryPlanError::ManifestOfAnotherPartition { .. })
    ));
    assert_eq!(
        plan_partition(RETIRED, prefix(), Some(tiered), &FreshScan::default(), true),
        Err(RecoveryPlanError::NotLongTermSegments(prefix()))
    );
}

fn recovered() -> PartitionRecovery {
    let source = Object::new(id(EARLY, 1), 1, 10, &[2, 1]).index();
    let proving = Object::proving(id(EARLY, 2), vec![proof(&source, 0, 10, NOW + 10)]).index();
    plan(Some(manifest(9, NOW)), &scan([source, proving]), true)
}

#[test]
fn given_a_plan_when_encoding_should_decode_to_the_same_plan() {
    let plan = recovered().plan;
    let empty = RecoveryPlan {
        manifest: None,
        proven_end: 0,
        batches: Vec::new(),
        ..plan
    };

    for plan in [plan, empty] {
        assert_eq!(RecoveryPlan::decode(&plan.encode().unwrap()), Ok(plan));
    }
}

#[test]
fn given_a_plan_a_reader_would_refuse_when_encoding_or_decoding_should_refuse_it() {
    let plan = recovered().plan;
    let mut gap = plan.clone();
    gap.batches.remove(0);
    let mut inside = plan.clone();
    inside.proven_end = 11;
    let mut other = plan.clone();
    other.prefix.partition_id += 1;

    assert_eq!(gap.encode(), Err(RecoveryPlanError::Batches { offset: 12 }));
    assert_eq!(inside.encode(), Err(RecoveryPlanError::ProvenEnd(11)));
    assert!(matches!(
        other.encode(),
        Err(RecoveryPlanError::ManifestOfAnotherPartition { .. })
    ));
    let encoded = plan.encode().unwrap();
    for at in [0, 9, encoded.len() / 2, encoded.len() - 1] {
        let mut damaged = encoded.clone();
        damaged[at] ^= 1;
        assert!(RecoveryPlan::decode(&damaged).is_err(), "byte {at}");
    }
    assert!(RecoveryPlan::decode(&encoded[..encoded.len() - 1]).is_err());
}

#[test]
fn given_a_stored_plan_when_reading_it_back_should_find_it_under_its_prefix_alone() {
    let store = MemoryStore::new();
    let plan = recovered().plan;
    let mut other = prefix();
    other.partition_id += 1;

    block_on(plan.store(&store, &layout())).unwrap();

    assert_eq!(
        block_on(RecoveryPlan::read(&store, &layout(), RETIRED, &prefix())).unwrap(),
        Some(plan)
    );
    assert_eq!(
        block_on(RecoveryPlan::read(&store, &layout(), RETIRED, &other)).unwrap(),
        None
    );
    assert_eq!(
        block_on(RecoveryPlan::read(&store, &layout(), OTHER, &prefix())).unwrap(),
        None
    );
}

fn put(store: &MemoryStore, key: &str, bytes: Bytes) {
    block_on(store.put(key, PutBody::from_bytes(bytes), &PutOptions::default())).unwrap();
}

#[test]
fn given_objects_of_other_formats_and_clusters_when_scanning_should_take_only_the_retired_clusters_self_describing_ones()
 {
    let store = MemoryStore::new();
    let layout = layout();
    let ours = Object::new(id(EARLY, 1), 1, 0, &[1]).sealed();
    let theirs = Object {
        cluster_uid: OTHER,
        ..Object::new(id(EARLY, 2), 1, 0, &[1])
    }
    .sealed();
    let mut older = FreshObject::new();
    older.push(b"messages of the format before");
    let truncated = ours.bytes.slice(..40);
    put(&store, &layout.fresh_key(id(EARLY, 1).get()), ours.bytes);
    put(&store, &layout.fresh_key(id(EARLY, 2).get()), theirs.bytes);
    put(
        &store,
        &layout.fresh_key(id(EARLY, 3).get()),
        older.into_bytes(),
    );
    put(&store, &layout.fresh_key(id(EARLY, 4).get()), truncated);
    put(
        &store,
        &format!("{}00/0001/not-a-fresh-object", layout.fresh_prefix()),
        Bytes::from_static(b"x"),
    );

    let scan = block_on(scan_fresh_objects(&store, &layout, RETIRED)).unwrap();

    let taken: Vec<_> = scan
        .indexes()
        .map(|index| index.header().object_id)
        .collect();
    assert_eq!(taken, [id(EARLY, 1)]);
    assert_eq!(
        scan.skipped()
            .iter()
            .map(|skipped| (skipped.object_id, skipped.reason.clone()))
            .collect::<Vec<_>>(),
        [
            (id(EARLY, 2), SkipReason::OtherCluster(OTHER)),
            (id(EARLY, 3), SkipReason::NotSelfDescribing),
            (
                id(EARLY, 4),
                SkipReason::Unreadable(FreshObjectError::Incomplete { have: 40, need: 64 })
            ),
        ]
    );
}

#[test]
fn given_an_index_longer_than_the_first_read_when_scanning_should_read_the_rest_and_decode_it() {
    let store = MemoryStore::new();
    let layout = layout();
    let counts = vec![1; 2000];
    let long = Object::new(id(EARLY, 1), 1, 0, &counts).sealed();
    assert!(long.index.index_len() > FRESH_INDEX_FIRST_READ);
    put(&store, &layout.fresh_key(id(EARLY, 1).get()), long.bytes);

    let scan = block_on(scan_fresh_objects(&store, &layout, RETIRED)).unwrap();

    assert_eq!(scan.indexes().collect::<Vec<_>>(), [&long.index]);
    assert_eq!(scan.skipped(), []);
}

#[test]
fn given_an_adopted_batch_of_an_idempotent_producer_when_installing_should_record_the_producer() {
    let object = Object::new(id(EARLY, 1), 1, 0, &[2]).sealed();
    let index = &object.index;
    let row = *index.partition(namespace()).unwrap();
    let (position, batch) = index.batches_of(&row).next().unwrap();
    let mut batch = *batch;
    batch.producer = Some(FreshProducer {
        producer_id: 42,
        epoch: 3,
        base_sequence: 0,
    });

    let placed = placed_batch(&row, &batch, id(EARLY, 1), position, 0, NOW).unwrap();
    let producers = producers_of(&[placed]);

    assert_eq!(producers.len(), 1);
    assert_eq!(producers[0].0, 42);
    assert_eq!(producers[0].1.epoch, 3);
    batch.producer = Some(FreshProducer {
        producer_id: 42,
        epoch: u16::MAX,
        base_sequence: 0,
    });
    assert_eq!(
        placed_batch(&row, &batch, id(EARLY, 1), position, 0, NOW),
        None
    );
}
