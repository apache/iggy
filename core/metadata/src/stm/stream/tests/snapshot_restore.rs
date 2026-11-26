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

/// A checkpoint reads a stream's total and each of its topics' as separate
/// loads while the partition plane keeps counting, so the two can disagree
/// in either direction. The boot restore adopts neither level, and journal
/// replay runs over what it produces: a replayed `DeleteTopic` over an
/// adopted torn shape would roll a topic back against a stream that never
/// held it, clamp, and raise the underflow alarm on every boot after.
#[test]
fn given_a_torn_checkpoint_when_restoring_at_boot_should_adopt_neither_level() {
    let mut snapshot = Streams::from(inner_with_registered_partition()).to_snapshot();
    // Topics summing above their stream is the torn shape: the stream was
    // read first, and the topic kept counting before its own read.
    snapshot.items[0].1.stats = StatsSnapshot {
        size_bytes: 100,
        messages_count: 1,
        segments_count: 1,
    };
    snapshot.items[0].1.topics[0].1.stats = StatsSnapshot {
        size_bytes: 200,
        messages_count: 2,
        segments_count: 2,
    };

    let restored = Streams::from_snapshot(snapshot).expect("snapshot restore");

    let (stream_size, stream_messages, stream_segments, topic_size) = restored.read(|inner| {
        let stream = inner.items.get(0).expect("restored stream");
        let topic = stream.topics.get(0).expect("restored topic");
        (
            stream.stats.size_bytes_inconsistent(),
            stream.stats.messages_count_inconsistent(),
            stream.stats.segments_count_inconsistent(),
            topic.stats.size_bytes_inconsistent(),
        )
    });
    assert_eq!(topic_size, 0);
    assert_eq!(
        stream_size, 0,
        "the stream total is snapshotted independently of its topics, so it has to go too"
    );
    assert_eq!(stream_messages, 0);
    assert_eq!(stream_segments, 0);
}

// The restore command is absorbed on BOTH left-right buffers. Minting a
// registry per call would hand the two buffers different `Arc`s, so a
// direct partition-plane increment would land on one buffer and vanish on
// the next publish -- the `messages_count_inconsistent` failure the
// registry exists to prevent.
#[test]
fn in_place_restore_keeps_one_registry_across_both_buffers() {
    let mut first = inner_with_registered_partition();
    let snapshot = Streams::from(first.clone()).to_snapshot();
    let mut second = first.clone();

    first.restore_in_place(snapshot.clone());
    second.restore_in_place(snapshot);

    assert!(
        Arc::ptr_eq(&first.stats_registry, &second.stats_registry),
        "both buffers must keep the one shared registry"
    );
    let from_first = first
        .stats_registry
        .partition_get(0, 0, 0)
        .expect("survivor keeps its partition stats");
    let from_second = second
        .stats_registry
        .partition_get(0, 0, 0)
        .expect("survivor keeps its partition stats");
    assert!(Arc::ptr_eq(&from_first, &from_second));
}

// Partition counters live only in the registry (never snapshotted), so an
// install that dropped them would leave every already-materialized
// partition reading zeroes with no way to recover them.
#[test]
fn in_place_restore_keeps_survivor_partition_stats() {
    let mut inner = inner_with_registered_partition();
    let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
    stats.increment_messages_count(42);
    let snapshot = Streams::from(inner.clone()).to_snapshot();

    inner.restore_in_place(snapshot);

    let after = inner
        .stats_registry
        .partition_get(0, 0, 0)
        .expect("partition survived the restore, so its stats must too");
    assert!(Arc::ptr_eq(&stats, &after));
    assert_eq!(after.messages_count_inconsistent(), 42);
}

// Slab keys are recycled: an entry left behind by a stream the snapshot
// does not carry would hand its counters to whatever lands in that slot.
#[test]
fn in_place_restore_prunes_entries_the_snapshot_dropped() {
    let mut inner = inner_with_registered_partition();
    // A second stream that the snapshot below will not contain.
    let empty = Streams::from(StreamsInner::new()).to_snapshot();
    assert!(inner.stats_registry.partition_get(0, 0, 0).is_some());

    inner.restore_in_place(empty);

    assert!(
        inner.stats_registry.partition_get(0, 0, 0).is_none(),
        "a partition the snapshot dropped must not keep its registry entry"
    );
}

/// The eviction has to empty what it drops. A partition the snapshot does
/// not carry stays mounted until the reconciler reaches it, holding the
/// same `Arc`, and its `ConfirmRemove` rolls those counters back through it.
/// The rebuild counts survivors only, so an entry dropped full leaves that
/// rollback to come out of a live sibling's totals.
#[test]
fn given_a_pruned_entry_when_its_partition_is_torn_down_later_should_leave_survivors_alone() {
    let mut inner = inner_with_registered_partition();
    let doomed = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
    doomed.increment_size_bytes(512);

    // A donor tree with the same shape but a partition this node's entry
    // cannot be, so the retain drops it and the restore registers nothing
    // in its place.
    let mut snapshot = Streams::from(inner.clone()).to_snapshot();
    snapshot.items[0].1.topics[0].1.partitions[0].created_revision += 1;
    inner.restore_in_place(snapshot);
    assert!(inner.stats_registry.partition_get(0, 0, 0).is_none());

    let survivor = inner.stats_registry.partition(
        0,
        0,
        &committed_partition(&inner, 0, 0, 0),
        inner.items[0].topics[0].stats.clone(),
    );
    survivor.increment_size_bytes(900);

    // The mounted stale incarnation, reaching its drop point.
    doomed.zero_out_all();

    assert_eq!(
        inner.items[0].topics[0].stats.size_bytes_inconsistent(),
        900,
        "the survivor's bytes must not pay for the pruned entry's rollback"
    );
    assert_eq!(inner.items[0].stats.size_bytes_inconsistent(), 900);
}

/// Admission is all that stands between a client and a namespace collision.
/// `IggyNamespace::new` used to mask, so slab key `MAX_TOPICS` packed
/// byte-identically to key 0: same shard, consensus group, directory and
/// authorization scope, no error.
///
/// Topics are the affordable ceiling to drive end to end. Streams and
/// partitions use the same guard but sit at `1 << 20` and 1,000,000, so
/// materialising either costs hundreds of megabytes for no extra coverage.
#[test]
fn given_the_topic_slab_ceiling_when_creating_one_more_should_reject() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    for index in 0..MAX_TOPICS {
        let request = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 0, &format!("t{index}")),
            derived_options: WireOptions::empty(),
            partitions: Vec::new(),
        };
        let reply = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());
        assert_eq!(reply.code, 0, "topic {index} must be admitted");
    }

    let overflow = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 0, "one-too-many"),
        derived_options: WireOptions::empty(),
        partitions: Vec::new(),
    };
    let reply = StateHandler::apply(&overflow, &mut inner, IggyTimestamp::now());
    assert_eq!(reply.code, u32::from(CreateTopicResult::TooManyTopics));

    let stream = inner.items.get(0).expect("stream survives the rejection");
    assert_eq!(
        stream.topics.len(),
        MAX_TOPICS,
        "a rejected create must not mutate the slab"
    );
    assert!(
        !stream.topic_index.contains_key("one-too-many"),
        "a rejected create must not leave a name-index entry"
    );
}

/// `MAX_PARTITIONS_PER_REQUEST` bounds one call, not the running total, so
/// the additive path is where a topic walks past the packed field. Seeded at
/// the boundary rather than driven there, since the guard is on the resolved
/// absolute id.
#[test]
fn given_partition_ids_at_the_ceiling_when_adding_more_should_reject() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    let seed = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 1, "t"),
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: u32::try_from(MAX_PARTITIONS - 2).unwrap(),
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&seed, &mut inner, IggyTimestamp::now()).code,
        0
    );

    // Resolves to MAX_PARTITIONS - 1: the last legal id.
    let last = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 1,
        },
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 2,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&last, &mut inner, IggyTimestamp::now()).code,
        0,
        "the final in-range id must still be admitted"
    );

    let overflow = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 1,
        },
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 3,
            members: Vec::new(),
        }],
    };
    let reply = StateHandler::apply(&overflow, &mut inner, IggyTimestamp::now());
    assert_eq!(
        reply.code,
        u32::from(CreatePartitionsResult::PartitionIdSpaceExhausted)
    );
    let topic = inner.items.get(0).unwrap().topics.get(0).unwrap();
    assert_eq!(
        topic.partitions.len(),
        2,
        "a rejected batch must not append"
    );
}

/// Topic create carries ABSOLUTE partition ids, so it needs its own bound.
/// The additive guard above never sees them.
#[test]
fn given_an_out_of_range_partition_id_on_topic_create_should_reject() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    let request = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 1, "t"),
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: u32::try_from(MAX_PARTITIONS).unwrap(),
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    let reply = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());
    assert_eq!(
        reply.code,
        u32::from(CreateTopicResult::PartitionIdSpaceExhausted),
        "a partition-id violation must not report a topic-count verdict"
    );
    assert_eq!(
        inner.items.get(0).unwrap().topics.len(),
        0,
        "a rejected create must not leave a half-built topic"
    );
}
