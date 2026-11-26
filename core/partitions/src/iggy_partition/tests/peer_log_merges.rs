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

//! A partition that follows the log of an origin that places its batches.
//! Such an origin keys its long-term segments by both their offsets, merges
//! small ones into one, and trims them by its topic's retention.
//!
//! Most origins here are a tiered partition whose uploaded segments are
//! stored again under keys with both offsets, which is what a long-term
//! segment is to a reader: the same bytes and the same index. The last test
//! follows a partition that places its batches through its own rewrite,
//! merge and retention.

use super::floor_eviction::apply;
use super::long_term_merge::{commit_replace, merge_pass};
use super::peer_log::{
    Gated, InFlight, MemoryIndexFiles, ORIGIN_ROOT, Polled, accepted, follower,
    follower_without_files, listed, log_shape, manifest_of, origin_in, origin_layout, plan, polled,
    unattached, until_gated,
};
use super::placed_batches::{
    commit_long_term_request, object_bytes, placing_leader_of_three_with, publish_manifest,
    store_segment_up_to, store_with, whole_and_placed,
};
use super::remote_poll::layout;
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::long_term::concatenated;
use crate::{
    MergePass, MergeSettled, PeerLogError, PeerManifestContradiction, PeerManifestRefusal,
    RemoteObjects, RemoteSegments, SegmentLocation, upload_segment,
};
use futures::channel::oneshot;
use iggy_common::{
    IggyExpiry, IggyTimestamp, PollingStrategy, TopicCreateOptions, TopicRuntimeOptions, TopicType,
};
use iggy_object_store::{
    FsStore, LongTermKey, ManifestSegment, MemoryStore, ObjectStore, PartitionManifest, PutBody,
    PutOptions, RecordingStore, SEGMENT_HASH_LEN, SegmentKey, SegmentObject, StoreCall,
};
use std::cell::{Cell, RefCell};
use std::time::Duration;

fn keyed_object(segment: &ManifestSegment, object: SegmentObject) -> String {
    origin_layout().long_term_key(&LongTermKey {
        prefix: PREFIX,
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        object,
    })
}

async fn whole_object(store: &FsStore, key: &str) -> Vec<u8> {
    ObjectStore::get_range(store, key, 0..u64::MAX)
        .await
        .unwrap()
        .bytes
        .to_vec()
}

async fn put(store: &FsStore, key: &str, bytes: Vec<u8>) {
    store
        .put(key, PutBody::from_bytes(bytes), &PutOptions::default())
        .await
        .unwrap();
}

pub(super) async fn delete_segments(store: &FsStore, segments: &[ManifestSegment]) {
    let keys: Vec<String> = segments
        .iter()
        .flat_map(|segment| SegmentObject::ALL.map(|object| keyed_object(segment, object)))
        .collect();
    assert!(store.delete(&keys).await.unwrap().is_complete());
}

/// An origin whose store holds its sealed segments under keys with both
/// offsets and under no other key.
pub(super) struct KeyedOrigin {
    _directory: tempfile::TempDir,
    pub(super) partition: Box<IggyPartition<IggyMessageBus>>,
    pub(super) bucket: tempfile::TempDir,
    pub(super) store: FsStore,
    /// Segments of [`BATCHES_PER_SEGMENT`] one-message batches, oldest
    /// first.
    pub(super) segments: Vec<ManifestSegment>,
}

/// An origin that committed `ops` one-message batches, the last of which
/// sits in its active segment.
pub(super) async fn keyed_origin(ops: u64) -> KeyedOrigin {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (directory, origin) = Box::pin(tiered_partition(&segment_records(ops), 0)).await;
    let mut origin = Box::new(origin);
    let mut op = 100;
    while let Some(next) = origin.next_segment_upload() {
        upload_segment(&store, &origin_layout(), &next)
            .await
            .unwrap();
        let advance = origin.note_segment_uploaded(&next.record).unwrap();
        Box::pin(apply(&mut origin, op, advance)).await;
        op += 1;
    }
    let segments = origin.manifest(IggyTimestamp::now()).unwrap().segments;
    for segment in &segments {
        for object in SegmentObject::ALL {
            let by_start = origin_layout().segment_key(&SegmentKey {
                prefix: PREFIX,
                start_offset: segment.start_offset,
                object,
            });
            let bytes = Box::pin(whole_object(&store, &by_start)).await;
            Box::pin(put(&store, &keyed_object(segment, object), bytes)).await;
            assert!(store.delete(&[by_start]).await.unwrap().is_complete());
        }
    }
    KeyedOrigin {
        _directory: directory,
        partition: origin,
        bucket,
        store,
        segments,
    }
}

/// A manifest of an origin that keys its segments by both offsets.
pub(super) fn keyed(trim_point: Option<u64>, segments: &[ManifestSegment]) -> PartitionManifest {
    PartitionManifest {
        prefix: PREFIX,
        published_at: 1,
        floor: segments
            .last()
            .map(|segment| segment.end_offset)
            .or(trim_point),
        trim_point,
        segments_keyed_by_both_offsets: true,
        segments: segments.to_vec(),
    }
}

/// What the origin lists for the segment that holds `sources` back to back.
pub(super) fn merged(sources: &[ManifestSegment]) -> ManifestSegment {
    concatenated(sources, [7; SEGMENT_HASH_LEN]).unwrap()
}

/// Store the segment that holds `sources` back to back, as a merge of the
/// origin does: one log, and an index of its bytes.
pub(super) async fn store_merged(store: &FsStore, sources: &[ManifestSegment]) -> ManifestSegment {
    let merged = merged(sources);
    let (mut log, mut index) = (Vec::new(), Vec::new());
    for source in sources {
        let entries = Box::pin(whole_object(
            store,
            &keyed_object(source, SegmentObject::Index),
        ))
        .await;
        for entry in entries.as_chunks::<IGGY_INDEX_SIZE>().0 {
            let (offset_and_timestamp, position) = entry.split_at(2 * size_of::<u64>());
            let position = u64::from_le_bytes(position.try_into().unwrap()) + log.len() as u64;
            index.extend_from_slice(offset_and_timestamp);
            index.extend_from_slice(&position.to_le_bytes());
        }
        log.extend(
            Box::pin(whole_object(
                store,
                &keyed_object(source, SegmentObject::Log),
            ))
            .await,
        );
    }
    assert_eq!(log.len() as u64, merged.size);
    Box::pin(put(store, &keyed_object(&merged, SegmentObject::Log), log)).await;
    Box::pin(put(
        store,
        &keyed_object(&merged, SegmentObject::Index),
        index,
    ))
    .await;
    merged
}

/// What the polls from every offset up to `last`, and from the first
/// timestamp, return.
pub(super) async fn polls_up_to<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    last: u64,
) -> Vec<Polled> {
    let mut polls = Vec::new();
    for offset in 0..=last {
        polls.push(Box::pin(polled(partition, PollingStrategy::offset(offset), 100)).await);
    }
    polls.push(
        Box::pin(polled(
            partition,
            PollingStrategy::timestamp(IggyTimestamp::from(0)),
            100,
        ))
        .await,
    );
    polls
}

/// The bytes and the messages the partition counts, and the bytes a poll may
/// see.
pub(super) fn held<B: MessageBus>(partition: &IggyPartition<B>) -> (u64, u64, u64) {
    (
        partition.stats.size_bytes_inconsistent(),
        partition.stats.messages_count_inconsistent(),
        partition.committed_bytes,
    )
}

pub(super) fn remote(start: u64, end: u64) -> (u64, u64, SegmentLocation) {
    (start, end, SegmentLocation::Remote)
}

pub(super) fn tail(start: u64) -> (u64, u64, SegmentLocation) {
    (start, start, SegmentLocation::Local)
}

fn read_keys(calls: &[StoreCall]) -> Vec<&str> {
    assert_ne!(calls, []);
    calls
        .iter()
        .map(|call| match call {
            StoreCall::GetRange { key, .. } => key.as_str(),
            other => panic!("the follower sent {other:?}"),
        })
        .collect()
}

#[compio::test]
async fn given_manifests_that_key_segments_by_both_offsets_when_a_follower_polls_should_read_under_keys_of_both_offsets()
 {
    let mut origin = Box::pin(keyed_origin(RECEIVER_OPS)).await;
    let recording = RecordingStore::new(FsStore::new(origin.bucket.path()));
    let calls = recording.calls();
    let (directory, mut follower) = follower(Rc::new(recording));

    let _ = follower
        .apply_peer_manifest(&keyed(None, &origin.segments))
        .unwrap();

    assert!(follower.peer_log().unwrap().keyed_by_both_offsets());
    assert_eq!(
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 10)).await,
        Box::pin(polled(&mut origin.partition, PollingStrategy::offset(0), 4)).await
    );
    let calls = calls.all();
    for key in read_keys(&calls) {
        assert!(origin_layout().parse_long_term_key(key).is_some(), "{key}");
    }
    assert!(
        directory
            .path()
            .join(format!("{:020}-{:020}.index", 0, 1))
            .exists(),
        "the fetched index is named as its object is"
    );
}

#[compio::test]
async fn given_manifests_that_key_segments_by_their_first_offset_when_a_follower_polls_should_read_under_those_keys()
 {
    let bucket = tempfile::tempdir().unwrap();
    let store = FsStore::new(bucket.path());
    let (_origin_directory, mut origin, manifest) = Box::pin(origin_in(&store)).await;
    let recording = RecordingStore::new(FsStore::new(bucket.path()));
    let calls = recording.calls();
    let (directory, mut follower) = follower(Rc::new(recording));
    assert!(!manifest.segments_keyed_by_both_offsets);

    let _ = follower.apply_peer_manifest(&manifest).unwrap();

    assert!(!follower.peer_log().unwrap().keyed_by_both_offsets());
    assert_eq!(
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 10)).await,
        Box::pin(polled(&mut origin, PollingStrategy::offset(0), 4)).await
    );
    let calls = calls.all();
    for key in read_keys(&calls) {
        assert!(origin_layout().parse_segment_key(key).is_some(), "{key}");
    }
    assert!(directory.path().join(format!("{:020}.index", 0)).exists());
}

#[compio::test]
async fn given_a_log_that_holds_segments_when_a_manifest_keys_them_the_other_way_should_refuse_it()
{
    let by_both = keyed(None, &[listed(0, 9)]);
    let by_first = manifest_of(None, &[(0, 9), (10, 19)]);
    for (applied, flipped, expected) in [
        (by_both.clone(), by_first, true),
        (
            manifest_of(None, &[(0, 9)]),
            keyed(None, &[listed(0, 9), listed(10, 19)]),
            false,
        ),
    ] {
        let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
        let _ = follower.apply_peer_manifest(&applied).unwrap();
        let before = log_shape(&follower);

        assert_eq!(
            follower.apply_peer_manifest(&flipped),
            Err(PeerManifestRefusal::KeyScheme {
                expected,
                found: !expected
            })
        );
        assert_eq!(log_shape(&follower), before);
        assert_eq!(
            follower.peer_log().unwrap().keyed_by_both_offsets(),
            expected
        );
    }

    // A log without a segment holds no key, so it takes either.
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let _ = follower.apply_peer_manifest(&by_both).unwrap();
    let _ = follower.apply_peer_manifest(&keyed(Some(9), &[])).unwrap();
    let _ = follower
        .apply_peer_manifest(&manifest_of(Some(9), &[(10, 19)]))
        .unwrap();
    assert!(!follower.peer_log().unwrap().keyed_by_both_offsets());
}

#[compio::test]
async fn given_a_manifest_that_merges_a_run_in_the_middle_and_then_one_at_the_front_when_applying_should_return_the_same_messages()
 {
    let mut origin = Box::pin(keyed_origin(11)).await;
    let segments = origin.segments.clone();
    assert_eq!(segments.len(), 5);
    let (directory, mut follower) = follower(Rc::new(FsStore::new(origin.bucket.path())));
    let _ = follower
        .apply_peer_manifest(&keyed(None, &segments))
        .unwrap();
    let before = Box::pin(polls_up_to(&mut follower, 9)).await;
    let counted = held(&follower);
    let segments_counted = follower.stats.segments_count_inconsistent();
    assert_eq!(
        before[0],
        Box::pin(polled(
            &mut origin.partition,
            PollingStrategy::offset(0),
            10
        ))
        .await
    );
    let first_index = directory.path().join(format!("{:020}-{:020}.index", 0, 1));
    assert!(first_index.exists());

    let middle = Box::pin(store_merged(&origin.store, &segments[1..4])).await;
    Box::pin(delete_segments(&origin.store, &segments[1..4])).await;
    let applied = follower
        .apply_peer_manifest(&keyed(None, &[segments[0], middle, segments[4]]))
        .unwrap();

    assert_eq!(
        (applied.appended, applied.trimmed, applied.replaced),
        (0, 0, 3)
    );
    assert_eq!(
        log_shape(&follower).0,
        [remote(0, 1), remote(2, 7), remote(8, 9), tail(10)]
    );
    assert_eq!(held(&follower), counted, "a merge moves no message");
    assert_eq!(
        follower.stats.segments_count_inconsistent(),
        segments_counted - 2
    );
    assert_eq!(Box::pin(polls_up_to(&mut follower, 9)).await, before);
    Box::pin(applied.remove_retired_indexes(&crate::DiskIndexFiles)).await;
    assert!(first_index.exists(), "the first segment stayed");

    let front = Box::pin(store_merged(&origin.store, &[segments[0], middle])).await;
    Box::pin(delete_segments(&origin.store, &[segments[0], middle])).await;
    let applied = follower
        .apply_peer_manifest(&keyed(None, &[front, segments[4]]))
        .unwrap();

    assert_eq!(
        (applied.appended, applied.trimmed, applied.replaced),
        (0, 0, 2)
    );
    assert_eq!(
        log_shape(&follower).0,
        [remote(0, 7), remote(8, 9), tail(10)]
    );
    assert_eq!(held(&follower), counted);
    assert_eq!(
        follower.stats.segments_count_inconsistent(),
        segments_counted - 3
    );
    assert_eq!(Box::pin(polls_up_to(&mut follower, 9)).await, before);
    Box::pin(applied.remove_retired_indexes(&crate::DiskIndexFiles)).await;
    assert!(
        !first_index.exists(),
        "the index of a replaced segment is retired"
    );
    assert!(
        directory
            .path()
            .join(format!("{:020}-{:020}.index", 0, 7))
            .exists(),
        "the merged segment has an index of its own"
    );
}

#[compio::test]
async fn given_a_follower_without_files_when_a_merge_replaces_polled_segments_should_remove_the_indexes_their_polls_fetched()
 {
    let origin = Box::pin(keyed_origin(11)).await;
    let segments = origin.segments.clone();
    let index_files = MemoryIndexFiles::default();
    let mut follower =
        follower_without_files(Rc::new(FsStore::new(origin.bucket.path())), &index_files);
    let _ = follower
        .apply_peer_manifest(&keyed(None, &segments))
        .unwrap();
    let _ = Box::pin(polls_up_to(&mut follower, 9)).await;
    let replaced_indexes: Vec<String> = segments[..2]
        .iter()
        .map(|segment| {
            format!(
                "{:020}-{:020}.index",
                segment.start_offset, segment.end_offset
            )
        })
        .collect();
    assert!(replaced_indexes.iter().all(|name| index_files.holds(name)));

    let front = Box::pin(store_merged(&origin.store, &segments[..2])).await;
    Box::pin(delete_segments(&origin.store, &segments[..2])).await;
    let mut listed = vec![front];
    listed.extend_from_slice(&segments[2..]);
    let applied = follower.apply_peer_manifest(&keyed(None, &listed)).unwrap();
    Box::pin(applied.remove_retired_indexes(&index_files)).await;

    assert!(
        replaced_indexes.iter().all(|name| !index_files.holds(name)),
        "the indexes of the replaced segments are removed where their polls put them"
    );
    assert!(index_files.holds(&format!("{:020}-{:020}.index", 4, 5)));
}

#[compio::test]
async fn given_a_listed_segment_that_is_no_merge_of_applied_ones_when_applying_should_refuse_it_as_diverged()
 {
    let [a, b, c, d] = [listed(0, 9), listed(10, 19), listed(20, 29), listed(30, 39)];
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[a, b, c, d]))
        .unwrap();
    let before = log_shape(&follower);
    let applied_state = follower.peer_log().unwrap().applied();
    let whole = merged(&[b, c]);

    let cases = [
        (
            "another size",
            vec![
                a,
                ManifestSegment {
                    size: whole.size + 1,
                    ..whole
                },
                d,
            ],
            PeerManifestContradiction::DoesNotAddUp,
        ),
        (
            "another checksum",
            vec![
                a,
                ManifestSegment {
                    crc32c: whole.crc32c ^ 1,
                    ..whole
                },
                d,
            ],
            PeerManifestContradiction::DoesNotAddUp,
        ),
        (
            "another first timestamp",
            vec![
                a,
                ManifestSegment {
                    start_timestamp: whole.start_timestamp + 1,
                    ..whole
                },
                d,
            ],
            PeerManifestContradiction::DoesNotAddUp,
        ),
        (
            "another last timestamp",
            vec![
                a,
                ManifestSegment {
                    end_timestamp: whole.end_timestamp + 1,
                    ..whole
                },
                d,
            ],
            PeerManifestContradiction::DoesNotAddUp,
        ),
        (
            "another newest timestamp",
            vec![
                a,
                ManifestSegment {
                    max_timestamp: whole.max_timestamp + 1,
                    ..whole
                },
                d,
            ],
            PeerManifestContradiction::DoesNotAddUp,
        ),
        (
            "an end and the next start inside an applied segment",
            vec![
                a,
                listed(10, 14),
                ManifestSegment {
                    start_offset: 15,
                    ..whole
                },
                d,
            ],
            PeerManifestContradiction::EndsOffBoundary,
        ),
        (
            "an end inside an applied segment",
            vec![
                a,
                ManifestSegment {
                    end_offset: 24,
                    ..whole
                },
                listed(25, 29),
                d,
            ],
            PeerManifestContradiction::EndsOffBoundary,
        ),
    ];
    for (case, segments, contradiction) in cases {
        assert_eq!(
            follower.apply_peer_manifest(&keyed(None, &segments)),
            Err(PeerManifestRefusal::Diverged {
                start_offset: 10,
                contradiction
            }),
            "{case}"
        );
        assert_eq!(log_shape(&follower), before, "{case}");
        assert_eq!(
            follower.peer_log().unwrap().applied(),
            applied_state,
            "{case}"
        );
    }
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[a, whole, d]))
        .unwrap();

    // The segment after `d` reached no manifest this partition fetched.
    let applied = follower
        .apply_peer_manifest(&keyed(None, &[a, whole, merged(&[d, listed(40, 49)])]))
        .unwrap();
    assert_eq!((applied.appended, applied.replaced), (0, 1));
    assert_eq!(
        log_shape(&follower).0,
        [remote(0, 9), remote(10, 29), remote(30, 49), tail(50)]
    );
}

/// A segment keyed by its first offset alone has the key of the first
/// segment it would replace, so no manifest of such an origin merges.
#[compio::test]
async fn given_segments_keyed_by_their_first_offset_when_a_manifest_merges_two_should_refuse_it_as_diverged()
 {
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let applied = manifest_of(None, &[(0, 9), (10, 19)]);
    let _ = follower.apply_peer_manifest(&applied).unwrap();
    let before = log_shape(&follower);

    let merging = PartitionManifest {
        segments: vec![merged(&applied.segments)],
        ..applied
    };

    let merging_unseen = manifest_of(None, &[(0, 29)]);
    for manifest in [merging, merging_unseen] {
        assert_eq!(
            follower.apply_peer_manifest(&manifest),
            Err(PeerManifestRefusal::Diverged {
                start_offset: 0,
                contradiction: PeerManifestContradiction::MergesUnderOneKey
            })
        );
        assert_eq!(log_shape(&follower), before);
    }
}

#[compio::test]
async fn given_a_manifest_with_more_segments_at_the_same_floor_and_trim_point_when_applying_should_refuse_it_as_going_back()
 {
    let [a, b, c, d] = [listed(0, 9), listed(10, 19), listed(20, 29), listed(30, 39)];
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    let older = keyed(None, &[a, b, c, d]);
    let newer = keyed(None, &[a, merged(&[b, c]), d]);
    let _ = follower.apply_peer_manifest(&older).unwrap();
    let _ = follower.apply_peer_manifest(&newer).unwrap();
    let before = log_shape(&follower);

    assert_eq!(
        follower.apply_peer_manifest(&older),
        Err(PeerManifestRefusal::GoesBack {
            floor: Some(39),
            trim_point: None,
            segments: 4,
            applied_floor: Some(39),
            applied_trim_point: None,
            applied_segments: 3,
        }),
        "the put of a deposed primary can land after a newer one"
    );
    assert_eq!(log_shape(&follower), before);
    assert_eq!(
        follower.apply_peer_manifest(&newer).unwrap().replaced,
        0,
        "the same manifest again changes nothing"
    );
}

#[compio::test]
async fn given_merges_and_trims_in_any_order_when_applying_should_keep_the_log_one_run_and_count_what_it_holds()
 {
    let [a, b, c, d] = [listed(0, 9), listed(10, 19), listed(20, 29), listed(30, 39)];
    let (bc, e) = (merged(&[b, c]), listed(40, 49));
    // (manifest, (appended, trimmed, replaced), the log after it, messages)
    let steps = [
        (
            keyed(None, &[a, b, c, d]),
            (4, 0, 0),
            vec![
                remote(0, 9),
                remote(10, 19),
                remote(20, 29),
                remote(30, 39),
                tail(40),
            ],
            40,
        ),
        // A merge, then a trim in front of the merged segment.
        (
            keyed(None, &[a, bc, d]),
            (0, 0, 2),
            vec![remote(0, 9), remote(10, 29), remote(30, 39), tail(40)],
            40,
        ),
        (
            keyed(Some(9), &[bc, d]),
            (0, 1, 0),
            vec![remote(10, 29), remote(30, 39), tail(40)],
            30,
        ),
        // A trim that removes the merged segment, and a segment after it.
        (
            keyed(Some(29), &[d, e]),
            (1, 1, 0),
            vec![remote(30, 39), remote(40, 49), tail(50)],
            20,
        ),
    ];
    let (_directory, mut follower) = follower(Rc::new(MemoryStore::new()));
    for (manifest, counts, log, messages) in steps {
        let applied = follower.apply_peer_manifest(&manifest).unwrap();
        assert_eq!(
            (applied.appended, applied.trimmed, applied.replaced),
            counts
        );
        assert_eq!(log_shape(&follower).0, log);
        assert_eq!(follower.stats.messages_count_inconsistent(), messages);
        assert_eq!(
            follower.stats.size_bytes_inconsistent(),
            messages * 100,
            "every listed segment holds 100 bytes for an offset"
        );
    }

    // A merge, a trim and a new segment in one manifest: the follower
    // missed the manifests in between.
    let (_directory, mut late) = super::peer_log::follower(Rc::new(MemoryStore::new()));
    let _ = late
        .apply_peer_manifest(&keyed(None, &[a, b, c, d]))
        .unwrap();
    let at_once = late
        .apply_peer_manifest(&keyed(Some(9), &[merged(&[b, c, d]), e]))
        .unwrap();
    assert_eq!(
        (at_once.appended, at_once.trimmed, at_once.replaced),
        (1, 1, 3)
    );
    assert_eq!(
        log_shape(&late).0,
        [remote(10, 39), remote(40, 49), tail(50)]
    );
    assert_eq!(late.stats.messages_count_inconsistent(), 40);
}

#[compio::test]
async fn given_a_poll_in_flight_when_a_merge_replaces_its_segments_and_the_origin_deletes_them_should_ask_for_a_retry_and_serve_it()
 {
    let mut origin = Box::pin(keyed_origin(RECEIVER_OPS)).await;
    let segments = origin.segments.clone();
    let (release, gate) = oneshot::channel();
    let gated = Rc::new(Gated {
        store: FsStore::new(origin.bucket.path()),
        gate: RefCell::new(Some(gate)),
        waiting: Cell::new(false),
    });
    let (_directory, mut follower) = follower(gated.clone());
    let _ = follower
        .apply_peer_manifest(&keyed(None, &segments))
        .unwrap();
    let mut poll: InFlight =
        Box::pin(plan(&mut follower, PollingStrategy::offset(0), 10).execute());
    Box::pin(until_gated(&gated, &mut poll)).await;

    let whole = Box::pin(store_merged(&origin.store, &segments)).await;
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[whole]))
        .unwrap();
    Box::pin(delete_segments(&origin.store, &segments)).await;
    let expected = Box::pin(polled(&mut origin.partition, PollingStrategy::offset(0), 4)).await;
    // The merged segment starts where the first of the replaced ones did,
    // and the fetch of that one's index is still waiting on the store.
    let planned_after = compio::time::timeout(
        Duration::from_secs(5),
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 10)),
    )
    .await
    .expect("a poll of the merged segment waits for no fetch of a replaced one");
    assert_eq!(planned_after, expected);
    release.send(()).unwrap();
    let result = poll.await;

    assert!(result.read_error.is_some(), "the objects are gone");
    assert_eq!(
        accepted(&mut follower, result),
        Err(IggyError::TransientNotAccepted)
    );
    assert_eq!(
        Box::pin(polled(&mut follower, PollingStrategy::offset(0), 10)).await,
        expected
    );
}

#[compio::test]
async fn given_reads_that_ended_before_a_merge_when_completing_should_refuse_the_one_of_replaced_segments_and_accept_the_other()
 {
    let mut origin = Box::pin(keyed_origin(7)).await;
    let segments = origin.segments.clone();
    assert_eq!(segments.len(), 3);
    let (_directory, mut follower) = follower(Rc::new(FsStore::new(origin.bucket.path())));
    let _ = follower
        .apply_peer_manifest(&keyed(None, &segments))
        .unwrap();
    let over_replaced = plan(&mut follower, PollingStrategy::offset(1), 10)
        .execute()
        .await;
    let beside = plan(&mut follower, PollingStrategy::offset(4), 10)
        .execute()
        .await;
    assert_eq!(over_replaced.read_error, None);

    let front = Box::pin(store_merged(&origin.store, &segments[..2])).await;
    let _ = follower
        .apply_peer_manifest(&keyed(None, &[front, segments[2]]))
        .unwrap();

    assert_eq!(
        accepted(&mut follower, over_replaced),
        Err(IggyError::TransientNotAccepted),
        "its plan named segments the log no longer holds"
    );
    assert_eq!(
        accepted(&mut follower, beside).unwrap(),
        Box::pin(polled(&mut origin.partition, PollingStrategy::offset(4), 2)).await,
        "a read of a segment that stayed is accepted as it is"
    );
}

/// The manifest that the origin's store holds.
async fn stored_manifest(store: &MemoryStore) -> PartitionManifest {
    let prefix = crate::object_prefix(IggyNamespace::new(1, 1, 0), 0);
    PartitionManifest::decode(&Box::pin(object_bytes(store, &layout().manifest_key(&prefix))).await)
        .unwrap()
}

/// Rewrite the origin's placed batches up to `up_to` into one long-term
/// segment and commit it, as its rewrite pass does.
async fn rewrite_up_to(
    origin: &mut IggyPartition<IggyMessageBus>,
    store: &Rc<MemoryStore>,
    up_to: u64,
    request: u64,
) -> ManifestSegment {
    let segment = Box::pin(store_segment_up_to(origin, store, up_to)).await;
    let command = origin.publish_segment_request(&segment, request).unwrap();
    Box::pin(commit_long_term_request(origin, command, false)).await;
    segment
}

/// Read the offsets `first..=last` from the follower: in one poll, from
/// below `first`, and one message at a time. Each way has to give the
/// messages the origin's own poll gives, once and in order.
async fn assert_follows(
    follower: &mut IggyPartition<IggyMessageBus>,
    origin: &mut IggyPartition<IggyMessageBus>,
    first: u64,
    last: u64,
) {
    let count = u32::try_from(last - first + 1).unwrap();
    let expected = Box::pin(polled(origin, PollingStrategy::offset(first), count)).await;
    assert_eq!((expected.0, expected.1), (Some(last), count));
    assert_eq!(
        Box::pin(polled(follower, PollingStrategy::offset(first), 100)).await,
        expected
    );
    assert_eq!(
        Box::pin(polled(follower, PollingStrategy::offset(0), 100)).await,
        expected,
        "a poll below the first retained offset starts there"
    );
    let mut one_by_one = Vec::new();
    for offset in first..=last {
        let (matched, messages, bytes) =
            Box::pin(polled(follower, PollingStrategy::offset(offset), 1)).await;
        assert_eq!((matched, messages), (Some(offset), 1));
        one_by_one.extend(bytes);
    }
    assert_eq!(one_by_one, expected.2);
    assert_eq!(follower.offsets().commit_offset, last);
}

/// The whole path between two clusters. The origin places its batches,
/// rewrites them into long-term segments, merges the small ones, trims by
/// its retention, and publishes a manifest after each change. The follower
/// has the origin's store and nothing else.
#[compio::test]
async fn given_an_origin_that_publishes_merges_and_trims_when_a_follower_applies_its_manifests_should_read_every_retained_message_once_in_order()
 {
    let (_whole, placed, object) = whole_and_placed(8);
    let store = Box::pin(store_with(object)).await;
    let (_origin_directory, mut origin) =
        Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let prefix = crate::object_prefix(origin.namespace(), origin.created_revision());

    // The reading cluster holds the topic as one that is no geo topic of
    // its own, so the partition places no batch. One made from the origin's
    // own options would, and could not follow.
    let (_refused_directory, mut placing) = unattached();
    placing.set_runtime_options(TopicRuntimeOptions::from(&TopicCreateOptions {
        topic_type: Some(TopicType::Geo),
        ..TopicCreateOptions::default()
    }));
    assert_eq!(
        placing.follow_peer_log(ORIGIN_ROOT, prefix),
        Err(PeerLogError::PlacesBatches)
    );
    let (_directory, mut follower) = unattached();
    follower.set_runtime_options(TopicRuntimeOptions::from(&TopicCreateOptions::default()));
    follower.follow_peer_log(ORIGIN_ROOT, prefix).unwrap();
    follower.set_remote_segments(Rc::new(RemoteSegments::new(
        Rc::clone(&store) as Rc<dyn RemoteObjects>,
        layout(),
    )));

    // Three small segments: offsets 0, 1 and 2..=3.
    let mut small = Vec::new();
    for (request, up_to) in (1..).zip([0, 1, 3]) {
        small.push(Box::pin(rewrite_up_to(&mut origin, &store, up_to, request)).await);
    }
    Box::pin(publish_manifest(&mut origin, &store)).await;
    let manifest = Box::pin(stored_manifest(&store)).await;
    assert!(manifest.segments_keyed_by_both_offsets);
    assert_eq!(manifest.segments, small);
    let applied = follower.apply_peer_manifest(&manifest).unwrap();
    assert_eq!(applied.appended, 3);
    Box::pin(assert_follows(&mut follower, &mut origin, 0, 3)).await;
    let counted = held(&follower);

    // The origin's merge job puts one segment in the place of the three.
    let whole = Box::pin(merge_pass(&mut origin, &store, MergePass::Scan))
        .await
        .unwrap();
    Box::pin(commit_replace(&mut origin, &whole)).await;
    assert_eq!(
        origin.settle_segment_merge(),
        MergeSettled::Committed { replaced: 3 }
    );
    Box::pin(publish_manifest(&mut origin, &store)).await;
    let applied = follower
        .apply_peer_manifest(&Box::pin(stored_manifest(&store)).await)
        .unwrap();
    assert_eq!(
        (applied.appended, applied.trimmed, applied.replaced),
        (0, 0, 3)
    );
    // The origin deletes the replaced objects an hour after that manifest.
    let replaced: Vec<String> = small
        .iter()
        .flat_map(|segment| {
            SegmentObject::ALL.map(|object| {
                layout().long_term_key(&LongTermKey {
                    prefix,
                    start_offset: segment.start_offset,
                    end_offset: segment.end_offset,
                    object,
                })
            })
        })
        .collect();
    assert!(store.delete(&replaced).await.unwrap().is_complete());
    assert_eq!(held(&follower), counted);
    Box::pin(assert_follows(&mut follower, &mut origin, 0, 3)).await;

    // Two more segments: offsets 4..=5 and 6..=7.
    let fourth = Box::pin(rewrite_up_to(&mut origin, &store, 5, 60)).await;
    Box::pin(rewrite_up_to(&mut origin, &store, 7, 61)).await;
    Box::pin(publish_manifest(&mut origin, &store)).await;
    let applied = follower
        .apply_peer_manifest(&Box::pin(stored_manifest(&store)).await)
        .unwrap();
    assert_eq!(applied.appended, 2);
    Box::pin(assert_follows(&mut follower, &mut origin, 0, 7)).await;

    // The topic's size budget holds the last segment and the one before
    // it, so retention trims the merged segment.
    let trim_to = origin
        .long_term_retention_end(
            IggyTimestamp::now(),
            IggyExpiry::NeverExpire,
            Some(fourth.size),
        )
        .unwrap();
    assert_eq!(trim_to, whole.end_offset);
    let trim = origin.trim_segments_request(trim_to, 70).unwrap();
    Box::pin(commit_long_term_request(&mut origin, trim, false)).await;
    Box::pin(publish_manifest(&mut origin, &store)).await;
    let manifest = Box::pin(stored_manifest(&store)).await;
    assert_eq!(manifest.trim_point, Some(3));
    let applied = follower.apply_peer_manifest(&manifest).unwrap();
    assert_eq!(
        (applied.appended, applied.trimmed, applied.replaced),
        (0, 1, 0)
    );
    let trimmed: Vec<String> = SegmentObject::ALL
        .map(|object| {
            layout().long_term_key(&LongTermKey {
                prefix,
                start_offset: whole.start_offset,
                end_offset: whole.end_offset,
                object,
            })
        })
        .to_vec();
    assert!(store.delete(&trimmed).await.unwrap().is_complete());
    Box::pin(assert_follows(&mut follower, &mut origin, 4, 7)).await;
    assert_eq!(follower.stats.messages_count_inconsistent(), 4);
}
