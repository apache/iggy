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

use super::placed_batches::{
    SharedStore, commit_long_term_request, commit_published_segment, long_term_key, long_term_logs,
    lose_primacy, object_bytes, placing_leader_of_three_with, publish_manifest,
    read_from_the_bucket_alone, regain_primacy, store_with, whole_and_placed, writer,
};
use super::remote_poll::layout;
use super::*;
use crate::long_term::{
    LONG_TERM_SEGMENTS_MAX, PUBLISH_SEGMENT, PublishSegment, REPLACE_SEGMENTS, ReplaceSegments,
};
use crate::state_machine::{Applied, MachineCommand};
use crate::{
    ExecutorLongTermMerger, LONG_TERM_MERGE_MIN_BYTES, LongTermMerger, LongTermWriter, MergeError,
    MergePass, MergeSettled,
};
use iggy_object_store::{
    InlineTierExecutor, ManifestSegment, MemoryStore, ObjectStore, PutBody, PutOptions,
    SEGMENT_HASH_LEN, SegmentObject,
};

type MemoryMerger = ExecutorLongTermMerger<InlineTierExecutor<SharedStore>>;

fn merger(store: &Rc<MemoryStore>) -> MemoryMerger {
    ExecutorLongTermMerger::new(
        InlineTierExecutor::new(SharedStore(Rc::clone(store))),
        layout(),
    )
}

/// A leader of three that committed one batch for every offset up to
/// `committed`, and whose long-term log holds a segment ending at each
/// offset of `ends`, stored in the returned store and committed by its own
/// command.
async fn leader_with_segments(
    committed: u64,
    ends: &[u64],
) -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Rc<MemoryStore>,
    Vec<ManifestSegment>,
) {
    let (_whole, placed, object) = whole_and_placed(committed + 1);
    let store = Box::pin(store_with(object)).await;
    let (directory, mut partition) = Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let mut segments = Vec::new();
    for (request, up_to) in (1..).zip(ends) {
        let (id, mut rewrite, scope) = partition.begin_segment_rewrite().unwrap();
        rewrite.up_to = *up_to;
        let outcome = writer(&store).write(rewrite, scope).await;
        let segment = partition.finish_segment_rewrite(id, outcome).unwrap();
        let command = partition
            .publish_segment_request(&segment, request)
            .unwrap();
        Box::pin(commit_long_term_request(&mut partition, command, false)).await;
        segments.push(segment);
    }
    assert_eq!(partition.long_term_segments(), segments);
    (directory, partition, store, segments)
}

/// Start the best merge, run it, and hand its result back. Returns the
/// segment it stored, whose command has yet to commit.
async fn merge_pass(
    partition: &mut IggyPartition<IggyMessageBus>,
    store: &Rc<MemoryStore>,
    pass: MergePass,
) -> Option<ManifestSegment> {
    let (id, merge, scope) = partition.begin_segment_merge(pass)?;
    let outcome = merger(store).merge(merge, scope).await;
    partition.finish_segment_merge(id, outcome)
}

/// Send the replace of `merged` as the primary does, and commit it.
async fn commit_replace(partition: &mut IggyPartition<IggyMessageBus>, merged: &ManifestSegment) {
    let request = partition.replace_segments_request(merged, 50).unwrap();
    Box::pin(commit_long_term_request(partition, request, false)).await;
}

fn files_under(directory: &Path) -> Vec<String> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            names.extend(files_under(&path));
        } else {
            names.push(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
    names
}

/// A reader of the bucket finds the same messages in the same bytes before
/// and after a merge. The merged segment is one object, with an index of
/// its own bytes and not the indexes of its sources one after another.
#[compio::test]
async fn given_small_segments_when_merging_should_store_one_object_that_reads_as_the_segments_did()
{
    let (directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(3, &[0, 1, 3])).await;
    Box::pin(publish_manifest(&mut partition, &store)).await;
    let (before, batches_before, log_before) = Box::pin(read_from_the_bucket_alone(&store)).await;
    assert_eq!(before.segments, segments);
    assert_eq!(partition.segment_merge_reclaim(), Some((2, 0)));

    let merged = Box::pin(merge_pass(&mut partition, &store, MergePass::Scan))
        .await
        .unwrap();

    assert_eq!((merged.start_offset, merged.end_offset), (0, 3));
    assert_eq!(merged.size, log_before.len() as u64);
    assert_eq!(merged.crc32c, crc32c::crc32c(&log_before));
    assert_eq!(merged.hash, *blake3::hash(&log_before).as_bytes());
    assert_eq!(long_term_logs(&store), 4, "three sources and one output");
    assert_eq!(
        Box::pin(object_bytes(
            &store,
            &long_term_key(0, 3, SegmentObject::Log)
        ))
        .await,
        log_before
    );
    let index = Box::pin(object_bytes(
        &store,
        &long_term_key(0, 3, SegmentObject::Index),
    ))
    .await;
    let first_index = Box::pin(object_bytes(
        &store,
        &long_term_key(0, 0, SegmentObject::Index),
    ))
    .await;
    assert_eq!(index, first_index, "one entry, for the first batch");
    assert!(
        files_under(directory.path())
            .iter()
            .all(|name| !name.contains(".merge.")),
        "the files the merge wrote beside the partition's are gone"
    );
    assert_eq!(
        partition.long_term_segments(),
        segments,
        "the log lists the sources until the replace commits"
    );

    Box::pin(commit_replace(&mut partition, &merged)).await;

    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Committed { replaced: 3 }
    );
    assert_eq!(partition.long_term_segments(), [merged]);
    assert_eq!(partition.long_term_floor(), Some(3));
    Box::pin(publish_manifest(&mut partition, &store)).await;
    let (after, batches_after, log_after) = Box::pin(read_from_the_bucket_alone(&store)).await;
    assert_eq!(after.segments, [merged]);
    assert_eq!(batches_after, batches_before);
    assert_eq!(log_after, log_before);
}

/// Put other bytes of the same length under the key of the segment's log.
async fn damage_source(store: &MemoryStore, segment: &ManifestSegment) {
    let key = long_term_key(segment.start_offset, segment.end_offset, SegmentObject::Log);
    let mut bytes = Box::pin(object_bytes(store, &key)).await;
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    store
        .put(&key, PutBody::from_bytes(bytes), &PutOptions::default())
        .await
        .unwrap();
}

/// A merge of bytes that are not the committed ones would give them a hash
/// of their own, and the long-term log would vouch for it from then on.
#[compio::test]
async fn given_a_source_whose_object_holds_other_bytes_when_merging_should_store_nothing_and_not_try_again_in_the_view()
 {
    let (_directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(3, &[0, 1, 3])).await;
    Box::pin(damage_source(&store, &segments[1])).await;
    let keys = store.keys();
    let (id, merge, scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();

    let outcome = merger(&store).merge(merge, scope).await;

    assert!(
        matches!(
            outcome,
            Ok(Err(MergeError::SourceDiverged { start_offset: 1 }))
        ),
        "{outcome:?}"
    );
    assert_eq!(partition.finish_segment_merge(id, outcome), None);
    assert_eq!(store.keys(), keys);
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Nothing);
    assert_eq!(partition.segment_merge_reclaim(), None);
    assert!(partition.begin_segment_merge(MergePass::Scan).is_none());
    assert!(partition.begin_segment_merge(MergePass::Drain).is_none());

    // The next view knows nothing of it, and finds it again.
    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    assert!(partition.begin_segment_merge(MergePass::Scan).is_some());
}

#[compio::test]
async fn given_a_source_whose_object_has_another_size_when_merging_should_store_nothing() {
    let (_directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(1, &[0, 1])).await;
    let key = long_term_key(0, 0, SegmentObject::Log);
    let mut bytes = Box::pin(object_bytes(&store, &key)).await;
    bytes.push(0);
    store
        .put(&key, PutBody::from_bytes(bytes), &PutOptions::default())
        .await
        .unwrap();
    let keys = store.keys();
    let (id, merge, scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();
    assert_eq!(merge.sources, segments);

    let outcome = merger(&store).merge(merge, scope).await;

    assert!(
        matches!(
            outcome,
            Ok(Err(MergeError::SourceDiverged { start_offset: 0 }))
        ),
        "{outcome:?}"
    );
    assert_eq!(partition.finish_segment_merge(id, outcome), None);
    assert_eq!(store.keys(), keys);
}

/// Primacy moves while a merge runs. Its result belongs to a view that is
/// over, so the replica keeps nothing of it. Whoever leads next picks the
/// same segments, finds the merged object under its key, and commits it.
#[compio::test]
async fn given_a_merge_in_flight_when_primacy_moves_should_drop_its_result_and_the_next_primary_adopts_the_object()
 {
    let (_directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(3, &[0, 1, 3])).await;
    let (id, merge, scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();
    let outcome = merger(&store).merge(merge, scope).await;
    let stored = *outcome.as_ref().unwrap().as_ref().unwrap();
    let keys = store.keys();

    lose_primacy(&mut partition);

    assert_eq!(partition.finish_segment_merge(id, outcome), None);
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Nothing);
    assert!(partition.begin_segment_merge(MergePass::Scan).is_none());
    assert!(!partition.segment_merge_running());
    assert_eq!(partition.long_term_segments(), segments);

    regain_primacy(&mut partition);
    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Nothing,
        "what an earlier view stored is found again, not remembered"
    );
    let adopted = Box::pin(merge_pass(&mut partition, &store, MergePass::Scan)).await;

    assert_eq!(adopted, Some(stored));
    assert_eq!(store.keys(), keys, "nothing was stored a second time");
    Box::pin(commit_replace(&mut partition, &stored)).await;
    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Committed { replaced: 3 }
    );
    assert_eq!(partition.long_term_segments(), [stored]);
    // A command that was sent twice changes nothing the second time.
    Box::pin(commit_replace(&mut partition, &stored)).await;
    assert_eq!(partition.long_term_segments(), [stored]);
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Nothing);
}

/// The result of a merge of an earlier view can arrive after the replica
/// leads again and started another. It is not taken for that one's.
#[compio::test]
async fn given_a_result_of_an_earlier_view_when_another_merge_runs_should_not_take_it_for_that_one()
{
    let (_directory, mut partition, store, _segments) =
        Box::pin(leader_with_segments(3, &[0, 1, 3])).await;
    let (earlier, merge, scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();
    let outcome = merger(&store).merge(merge, scope).await;
    lose_primacy(&mut partition);
    assert!(partition.begin_segment_merge(MergePass::Scan).is_none());
    regain_primacy(&mut partition);
    let (later, _merge, _scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();
    assert_ne!(earlier, later);

    assert_eq!(partition.finish_segment_merge(earlier, outcome), None);

    assert!(partition.segment_merge_running());
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Nothing);
}

/// The merged segment is stored, and its command has yet to commit. Every
/// pass gets the segment to send again, and none starts another merge.
#[compio::test]
async fn given_a_stored_merge_whose_command_is_not_committed_when_passes_run_should_send_it_again_and_start_nothing()
 {
    let (_directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(3, &[0, 1, 3])).await;
    let (id, merge, scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();
    assert!(
        partition.begin_segment_merge(MergePass::Scan).is_none(),
        "one at a time"
    );
    assert!(partition.segment_merge_running());
    let outcome = merger(&store).merge(merge, scope).await;

    let merged = partition.finish_segment_merge(id, outcome).unwrap();

    for _ in 0..2 {
        assert_eq!(
            partition.settle_segment_merge(),
            MergeSettled::Waiting(merged)
        );
        assert!(partition.begin_segment_merge(MergePass::Scan).is_none());
        assert!(partition.begin_segment_merge(MergePass::Drain).is_none());
        assert_eq!(partition.segment_merge_reclaim(), None);
    }
    assert_eq!(partition.long_term_segments(), segments);

    Box::pin(commit_replace(&mut partition, &merged)).await;

    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Committed { replaced: 3 }
    );
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Nothing);
}

/// A trim removes the first source while the merge runs. No replace can
/// list a segment that starts where none does, so the merged object is
/// garbage: it waits for its delete with the trimmed one, and the next
/// merge starts from what the log holds now.
#[compio::test]
async fn given_a_trim_under_a_running_merge_when_it_ends_should_retire_its_output() {
    let (_directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(3, &[0, 1, 3])).await;
    let (id, merge, scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();
    let outcome = merger(&store).merge(merge, scope).await;
    let trim = partition.trim_segments_request(0, 60).unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;
    assert_eq!(partition.long_term_segments(), &segments[1..]);
    assert_eq!(partition.queued_dead_long_term_objects(), 1);

    let merged = partition.finish_segment_merge(id, outcome).unwrap();

    assert_eq!((merged.start_offset, merged.end_offset), (0, 3));
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Retired);
    assert_eq!(partition.queued_dead_long_term_objects(), 2);
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Nothing);
    assert_eq!(partition.long_term_segments(), &segments[1..]);
    let (_, next, _scope) = partition.begin_segment_merge(MergePass::Drain).unwrap();
    assert_eq!(next.sources, &segments[1..]);
}

fn apply(
    partition: &mut IggyPartition<IggyMessageBus>,
    op: u64,
    kind: crate::state_machine::CommandKind,
    payload: &[u8],
) -> Applied {
    partition
        .state_machines
        .apply(op, MachineCommand { kind, payload })
        .unwrap()
}

/// A segment of one offset that is too large to merge.
fn healthy_segment(offset: u64) -> ManifestSegment {
    ManifestSegment {
        start_offset: offset,
        end_offset: offset,
        start_timestamp: 0,
        end_timestamp: 0,
        max_timestamp: 0,
        size: LONG_TERM_MERGE_MIN_BYTES,
        crc32c: 0,
        hash: [0; SEGMENT_HASH_LEN],
    }
}

/// A full log refuses the next segment, and takes a replace, which makes it
/// shorter. The publish that waited is taken after it.
#[compio::test]
async fn given_a_full_long_term_log_when_a_merge_commits_should_take_it_and_then_the_waiting_publish()
 {
    let (_directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(1, &[0, 1])).await;
    let full = LONG_TERM_SEGMENTS_MAX as u64;
    for offset in 2..full {
        let payload = PublishSegment(healthy_segment(offset)).encode();
        assert_eq!(
            apply(&mut partition, 100 + offset, PUBLISH_SEGMENT, &payload),
            Applied::Changed
        );
    }
    assert_eq!(partition.long_term_segments().len(), LONG_TERM_SEGMENTS_MAX);
    let waiting = PublishSegment(healthy_segment(full)).encode();
    assert_eq!(
        apply(&mut partition, 100 + full, PUBLISH_SEGMENT, &waiting),
        Applied::Refused
    );
    assert_eq!(partition.segment_merge_reclaim(), Some((1, 0)));

    let merged = Box::pin(merge_pass(&mut partition, &store, MergePass::Scan))
        .await
        .unwrap();
    assert_eq!((merged.start_offset, merged.end_offset), (0, 1));
    assert_eq!(merged.size, segments[0].size + segments[1].size);
    let replace = ReplaceSegments(merged).encode();

    assert_eq!(
        apply(&mut partition, 101 + full, REPLACE_SEGMENTS, &replace),
        Applied::Changed
    );
    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Committed { replaced: 2 }
    );
    assert_eq!(
        apply(&mut partition, 102 + full, PUBLISH_SEGMENT, &waiting),
        Applied::Changed
    );
    assert_eq!(partition.long_term_segments().len(), LONG_TERM_SEGMENTS_MAX);
    assert_eq!(partition.long_term_floor(), Some(full));
}

/// A merge and a rewrite change the log in different places, so neither
/// waits for the other: not while one runs, and not while the command of
/// one has yet to commit.
#[compio::test]
async fn given_a_merge_in_flight_when_a_rewrite_is_due_should_start_the_rewrite_at_once() {
    let (_directory, mut partition, store, segments) =
        Box::pin(leader_with_segments(3, &[0, 1])).await;
    assert_eq!(partition.long_term_lag(), 2);
    let (id, merge, scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();

    let (rewrite_id, rewrite, rewrite_scope) = partition
        .begin_segment_rewrite()
        .expect("a rewrite starts while a merge runs");
    assert_eq!((rewrite.start_offset, rewrite.up_to), (2, 3));

    let outcome = merger(&store).merge(merge, scope).await;
    let merged = partition.finish_segment_merge(id, outcome).unwrap();
    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Waiting(merged)
    );
    let outcome = writer(&store).write(rewrite, rewrite_scope).await;
    let rewritten = partition
        .finish_segment_rewrite(rewrite_id, outcome)
        .expect("a rewrite ends while a merged segment waits for its command");
    assert_eq!(partition.unpublished_segment(), Some(rewritten));
    let publish = partition.publish_segment_request(&rewritten, 70).unwrap();
    Box::pin(commit_long_term_request(&mut partition, publish, false)).await;
    assert_eq!(
        partition.long_term_segments(),
        [segments[0], segments[1], rewritten]
    );
    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Waiting(merged),
        "a publish does not settle the merge"
    );

    Box::pin(commit_replace(&mut partition, &merged)).await;

    assert_eq!(partition.long_term_segments(), [merged, rewritten]);
    assert_eq!(partition.unpublished_segment(), None);
}

#[compio::test]
async fn given_a_rewrite_in_flight_when_a_merge_is_due_should_start_the_merge() {
    let (_directory, mut partition, _store, segments) =
        Box::pin(leader_with_segments(3, &[0, 1])).await;
    let _rewrite = partition.begin_segment_rewrite().unwrap();

    let (_, merge, _scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();

    assert_eq!(merge.sources, segments);
}

/// A merged segment that came out small is merged again with what follows
/// it. A run that grows by a segment at a time would be rewritten once for
/// each, so only a shard's scan does that, and the pass after a merge
/// leaves it.
#[compio::test]
async fn given_a_merged_segment_that_came_out_small_when_the_next_one_follows_should_merge_it_again_only_in_a_scan()
 {
    let (_directory, mut partition, store, _segments) =
        Box::pin(leader_with_segments(3, &[0, 1])).await;
    let merged = Box::pin(merge_pass(&mut partition, &store, MergePass::Scan))
        .await
        .unwrap();
    Box::pin(commit_replace(&mut partition, &merged)).await;
    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Committed { replaced: 2 }
    );
    let (id, rewrite, scope) = partition.begin_segment_rewrite().unwrap();
    let outcome = writer(&store).write(rewrite, scope).await;
    let rewritten = partition.finish_segment_rewrite(id, outcome).unwrap();
    let publish = partition.publish_segment_request(&rewritten, 70).unwrap();
    Box::pin(commit_long_term_request(&mut partition, publish, false)).await;
    assert_eq!(partition.long_term_segments(), [merged, rewritten]);

    assert!(partition.begin_segment_merge(MergePass::Drain).is_none());

    assert_eq!(partition.segment_merge_reclaim(), Some((1, 0)));
    let (_, merge, _scope) = partition.begin_segment_merge(MergePass::Scan).unwrap();
    assert_eq!(merge.sources, [merged, rewritten]);
}

#[compio::test]
async fn given_a_backup_when_a_merge_pass_runs_should_merge_nothing() {
    let (_directory, mut partition, _store, _segments) =
        Box::pin(leader_with_segments(3, &[0, 1, 3])).await;
    assert_eq!(partition.segment_merge_reclaim(), Some((2, 0)));
    lose_primacy(&mut partition);

    assert_eq!(partition.segment_merge_reclaim(), None);
    assert!(partition.begin_segment_merge(MergePass::Scan).is_none());
    assert_eq!(partition.settle_segment_merge(), MergeSettled::Nothing);
    assert!(!partition.segment_merge_running());
}

/// Every partition registers the long-term log, and only one that places
/// its batches has objects under the keys a merge reads.
#[compio::test]
async fn given_a_partition_of_whole_batches_when_a_merge_pass_runs_should_merge_nothing() {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 1)).await;
    partition.register_builtin_state_machines().unwrap();
    Box::pin(commit_records(&mut partition, 1, &segment_records(2))).await;
    for offset in 0..2 {
        let small = ManifestSegment {
            size: 1,
            ..healthy_segment(offset)
        };
        commit_published_segment(&mut partition, &small, 100 + offset);
    }
    assert_eq!(partition.long_term_segments().len(), 2);
    assert!(partition.state_machines_current());
    assert!(partition.partition_dir().is_some());

    assert_eq!(partition.segment_merge_reclaim(), None);
    assert!(partition.begin_segment_merge(MergePass::Scan).is_none());
}
