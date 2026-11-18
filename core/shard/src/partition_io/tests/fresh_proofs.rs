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

//! Fresh objects that describe themselves: the index of each object, and
//! the proof rows that bind the batches that committed to where they did.

use super::fresh_writes::{
    GatedUploader, LONG_INTERVAL, TestUploader, committed, committed_sends, layout,
    place_partitions, placing_owner, stored_placed_batches, submit_send, submit_send_to,
};
use super::segment_uploads::{namespace, pumped, until};
use super::*;
use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, PlacedBatch};
use iggy_object_store::{FreshIndex, FreshObjectId, FreshProofRow, MemoryStore, ObjectStore};
use partitions::ProvenBatch;

const CLUSTER_UID: u128 = 0x00c1_05e5_0000_0000_0000_0000_0000_0042;

/// A fresh object the store holds, as a reader of the store alone finds it.
struct StoredObject {
    id: FreshObjectId,
    index: FreshIndex,
    bytes: bytes::Bytes,
}

/// Every fresh object in `store`, in the order of their ids, each read
/// whole and its index decoded under its key.
#[allow(clippy::future_not_send)]
async fn stored_objects(store: &MemoryStore) -> Vec<StoredObject> {
    let mut keys = store.keys();
    keys.sort();
    let mut objects = Vec::new();
    for key in keys {
        let Some(id) = layout().parse_fresh_key(&key) else {
            continue;
        };
        let id = FreshObjectId::from_raw(id);
        let size = store.head(&key).await.unwrap().size;
        let bytes = store.get_range(&key, 0..size).await.unwrap().bytes;
        let index = FreshIndex::decode(&bytes, id)
            .unwrap_or_else(|error| panic!("object {key} does not decode: {error}"));
        objects.push(StoredObject { id, index, bytes });
    }
    objects
}

/// The distinct proof rows of `partition` across `objects`, by the offset
/// of their batch, each with the objects that carry it.
fn proof_rows(
    objects: &[StoredObject],
    partition: IggyNamespace,
) -> BTreeMap<u64, Vec<(FreshProofRow, FreshObjectId)>> {
    let mut rows: BTreeMap<u64, Vec<(FreshProofRow, FreshObjectId)>> = BTreeMap::new();
    for object in objects {
        let Some(row) = object.index.partition(partition.inner()) else {
            continue;
        };
        for proof in object.index.proofs_of(row) {
            rows.entry(proof.base_offset)
                .or_default()
                .push((*proof, object.id));
        }
    }
    rows
}

/// The placed batch that `proof` rebuilds from the rows of the objects
/// that hold it, as a reader of the store alone gets it.
fn rebuilt(
    objects: &[StoredObject],
    partition: IggyNamespace,
    proof: &FreshProofRow,
) -> [u8; PLACED_BATCH_SIZE] {
    let source = objects
        .iter()
        .find(|object| object.id == proof.object_id)
        .expect("the object that holds the blob is stored");
    let row = source
        .index
        .partition(partition.inner())
        .expect("the source object holds the partition");
    let rebuilt = PlacedBatch::try_from(ProvenBatch::find(proof, row, &source.index).unwrap())
        .expect("the rows rebuild the committed placed batch");
    let start = usize::try_from(rebuilt.placement.position).unwrap();
    let end = start + usize::try_from(rebuilt.placement.length).unwrap();
    assert!(
        rebuilt.placement.matches(&source.bytes[start..end]),
        "the placement names the blob of the batch"
    );
    let mut bytes = [0; PLACED_BATCH_SIZE];
    rebuilt.encode_into(&mut bytes);
    bytes
}

/// A shard that leads `PARTITIONS` placing partitions takes one send per
/// partition in each round, and the next round starts once the last one
/// committed. Every object decodes as a reader finds it. The row of each
/// committed batch rides an object of its own round or of the next one, one
/// distinct row per batch, and a reader rebuilds from them exactly the
/// placed batches the partition committed.
#[compio::test]
async fn given_sends_in_rounds_when_each_round_commits_should_prove_its_batches_in_the_objects_of_the_next()
 {
    const PARTITIONS: usize = 4;
    const ROUNDS: u64 = 4;
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let placing = place_partitions(&owner, PARTITIONS);
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_cluster_uid(CLUSTER_UID);
    owner.set_fresh_upload_pace(Duration::from_millis(20), 2);

    let objects_after_round = pumped(&owner, async {
        let mut request = 0;
        let mut objects_after_round = Vec::new();
        for round in 1..=ROUNDS {
            for partition in &placing {
                request += 1;
                owner.offer_fresh_send(sized_send_request(*partition, request, 100), channel(1).0);
            }
            until(&owner, |owner| {
                placing
                    .iter()
                    .all(|partition| committed(owner, *partition) == round)
            })
            .await;
            objects_after_round.push(uploader.store().keys().len());
        }
        objects_after_round
    })
    .await;

    let objects = Box::pin(stored_objects(uploader.store())).await;
    assert_eq!(objects.len(), *objects_after_round.last().unwrap());
    for object in &objects {
        assert_eq!(object.index.header().object_id, object.id);
        assert_eq!(object.index.header().cluster_uid, CLUSTER_UID);
        assert!(!object.index.is_confirmation());
        assert_eq!(object.index.object_len(), object.bytes.len() as u64);
    }
    let round_of = |object: FreshObjectId| {
        let position = objects
            .iter()
            .position(|stored| stored.id == object)
            .unwrap();
        1 + objects_after_round
            .iter()
            .position(|after| position < *after)
            .unwrap() as u64
    };
    for partition in &placing {
        let rows = proof_rows(&objects, *partition);
        let offsets: Vec<u64> = rows.keys().copied().collect();
        assert_eq!(
            offsets,
            (0..ROUNDS - 1).collect::<Vec<_>>(),
            "the batches of every round but the last are proven in {partition:?}"
        );
        for (offset, carried) in &rows {
            assert!(
                carried.iter().all(|(row, _)| *row == carried[0].0),
                "one row for the batch at {offset}: {carried:?}"
            );
            let (row, carrier) = carried[0];
            assert_ne!(carrier, row.object_id);
            assert!(
                round_of(carrier) <= round_of(row.object_id) + 1,
                "the batch at {offset} is proven in its own round or the next"
            );
        }
    }
    let committed_batches = stored_placed_batches(directory.path());
    for (offset, carried) in proof_rows(&objects, namespace()) {
        let mut committed = [0; PLACED_BATCH_SIZE];
        committed_batches[usize::try_from(offset).unwrap()].encode_into(&mut committed);
        assert_eq!(
            rebuilt(&objects, namespace(), &carried[0].0),
            committed,
            "the rows rebuild the batch at {offset} byte for byte"
        );
        assert_eq!(
            carried[0].0.placed_checksum,
            placement::checksum_of(&committed)
        );
    }
    let claims: Vec<u64> = objects
        .iter()
        .filter_map(|object| object.index.partition(namespace().inner()))
        .filter(|row| row.batch_count > 0)
        .map(|row| row.claimed_first_offset)
        .collect();
    assert_eq!(
        claims,
        (0..ROUNDS).collect::<Vec<_>>(),
        "with nothing refused, each claim is where its batch committed"
    );
}

/// The store refuses for good the object that carries the row of the first
/// batch. The next object carries it again, and claims where its own batch
/// commits after the refused send.
#[compio::test]
async fn given_an_object_that_carried_proof_rows_when_the_store_refuses_it_for_good_should_carry_them_in_the_next_object()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        uploader.refuses.set(true);
        let refused = submit_send(&owner, 2, 100);
        until(&owner, |_| !refused.is_empty()).await;
        uploader.refuses.set(false);
        let _third = submit_send(&owner, 3, 100);
        until(&owner, |owner| committed_sends(owner) == 2).await;
    })
    .await;

    let objects = Box::pin(stored_objects(uploader.store())).await;
    assert_eq!(objects.len(), 2, "the refused object is not in the store");
    let rows = proof_rows(&objects, namespace());
    assert_eq!(rows.keys().copied().collect::<Vec<_>>(), [0]);
    let (row, carrier) = rows[&0][0];
    assert_eq!(row.object_id, objects[0].id);
    assert_eq!(carrier, objects[1].id);
    let committed_batches = stored_placed_batches(directory.path());
    let mut committed = [0; PLACED_BATCH_SIZE];
    committed_batches[0].encode_into(&mut committed);
    assert_eq!(rebuilt(&objects, namespace(), &row), committed);
    let second = objects[1].index.partition(namespace().inner()).unwrap();
    assert_eq!(
        second.claimed_first_offset, 1,
        "the refused send no longer counts in the claim"
    );
    assert_eq!(
        second.proven_below, 0,
        "the row of the batch at 0 was in no stored object"
    );
}

/// The rows of the partitions this shard leads ride the same object, also
/// that of a partition with no send in it.
#[compio::test]
async fn given_two_partitions_when_both_commit_should_prove_both_in_one_object() {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let other = place_partitions(&owner, 2)[1];
    let uploader = TestUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(LONG_INTERVAL, 1);

    pumped(&owner, async {
        // Stored at once, so the two sends after it share the next window.
        let _alone = submit_send(&owner, 1, 100);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        let _first = submit_send(&owner, 2, 100);
        let _second = submit_send_to(&owner, other, 3, 100);
        until(&owner, |owner| {
            committed_sends(owner) == 2 && committed(owner, other) == 1
        })
        .await;
        // The tick takes the rows of a partition with no send in the window.
        until(&owner, |owner| {
            owner.fresh_proofs_waiting().contains(&other)
        })
        .await;
        let _third = submit_send(&owner, 4, 100);
        until(&owner, |owner| committed_sends(owner) == 3).await;
    })
    .await;

    let objects = Box::pin(stored_objects(uploader.store())).await;
    assert_eq!(objects.len(), 3);
    let last = objects.last().unwrap();
    let proven: Vec<u64> = last
        .index
        .directory()
        .iter()
        .filter(|row| row.proof_count > 0)
        .map(|row| row.namespace)
        .collect();
    assert_eq!(proven, [namespace().inner(), other.inner()]);
    let other_row = last.index.partition(other.inner()).unwrap();
    assert_eq!(
        other_row.batch_count, 0,
        "proof rows alone for the other partition"
    );
}

/// An object that carries the row of a batch is still uploading when the
/// next one is sealed. The next one claims no proof of that batch, and the
/// one after the upload ends does.
#[compio::test]
async fn given_an_object_whose_proof_rows_still_upload_when_sealing_the_next_should_claim_them_unproven()
 {
    let directory = tempfile::tempdir().unwrap();
    let owner = Box::pin(placing_owner(directory.path())).await;
    let uploader = GatedUploader::new();
    owner.set_fresh_uploader(Some(uploader.clone()));
    owner.set_fresh_upload_pace(Duration::ZERO, 3);

    pumped(&owner, async {
        let _first = submit_send(&owner, 1, 100);
        until(&owner, |_| uploader.started() == 1).await;
        uploader.settle(0, true);
        until(&owner, |owner| committed_sends(owner) == 1).await;
        // Carries the row of the batch at 0, and holds its upload.
        let _second = submit_send(&owner, 2, 100);
        until(&owner, |_| uploader.started() == 2).await;
        let _third = submit_send(&owner, 3, 100);
        until(&owner, |_| uploader.started() == 3).await;
        uploader.settle(1, true);
        uploader.settle(2, true);
        until(&owner, |owner| committed_sends(owner) == 3).await;
        let _fourth = submit_send(&owner, 4, 100);
        until(&owner, |_| uploader.started() == 4).await;
        uploader.settle(3, true);
        until(&owner, |owner| committed_sends(owner) == 4).await;
    })
    .await;

    let claims: Vec<u64> = stored_placed_batches(directory.path())
        .iter()
        .map(|placed| placed.placement.proven_below)
        .collect();
    assert_eq!(claims, [0, 0, 0, 1]);
}
