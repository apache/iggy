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

/// A stats read takes the counts instead of walking every partition, so
/// each apply that adds or drops a resource has to move them.
#[test]
fn given_creates_and_deletes_when_counting_resources_should_match_a_walk() {
    let mut inner = StreamsInner::new();
    for name in ["alpha", "beta", "gamma"] {
        commit_counted(
            &mut inner,
            &CreateStreamRequest {
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            },
        );
    }
    create_counted_topic(&mut inner, 0, "orders", 1, 2);
    create_counted_topic(&mut inner, 0, "users", 3, 1);
    create_counted_topic(&mut inner, 1, "events", 4, 3);
    create_counted_group(&mut inner, 0, 0, "billing");
    create_counted_group(&mut inner, 0, 0, "audit");
    create_counted_group(&mut inner, 1, 0, "readers");
    commit_counted(
        &mut inner,
        &CreatePartitionsWithAssignmentsRequest {
            request: WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 2,
            },
            partitions: partition_assignments(7, 2),
        },
    );
    commit_counted(
        &mut inner,
        &DeletePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 1,
        },
    );
    commit_counted(
        &mut inner,
        &DeleteConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            group_id: WireIdentifier::numeric(0),
        },
    );
    commit_counted(
        &mut inner,
        &DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(1),
        },
    );
    commit_counted(
        &mut inner,
        &DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(1),
        },
    );
    let expected = ResourceCounts {
        streams: 2,
        topics: 1,
        partitions: 3,
        consumer_groups: 1,
    };
    assert_eq!(inner.resource_counts(), expected);

    let snapshot = Streams::from(inner.clone()).to_snapshot();
    let restored = StreamsInner::inner_from_snapshot(snapshot, Arc::new(StatsRegistry::default()));
    assert_eq!(restored.resource_counts(), expected);
}

/// The node totals are the root of the rollup: a deleted stream has to take
/// what its partitions counted off them, and a restore has to rebuild them
/// from the partition entries that survive.
#[test]
fn given_counted_partitions_when_a_stream_goes_should_take_its_totals_off_the_node() {
    let mut inner = StreamsInner::new();
    for (stream_id, name) in ["kept", "dropped"].into_iter().enumerate() {
        commit_counted(
            &mut inner,
            &CreateStreamRequest {
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            },
        );
        let stream_id = u32::try_from(stream_id).unwrap();
        let first_group = u64::from(stream_id) * 2 + 1;
        create_counted_topic(&mut inner, stream_id, "topic", first_group, 2);
    }
    let register = |inner: &StreamsInner, stream_id: usize, partition_id: usize| {
        inner.stats_registry.partition(
            stream_id,
            0,
            &committed_partition(inner, stream_id, 0, partition_id),
            inner.items[stream_id].topics[0].stats.clone(),
        )
    };
    let kept = register(&inner, 0, 0);
    // Materialised but empty, so no rebuild may count it as holding segments.
    let _idle = register(&inner, 0, 1);
    let dropped = register(&inner, 1, 0);
    kept.increment_size_bytes(100);
    kept.increment_messages_count(10);
    kept.increment_segments_count(2);
    dropped.increment_size_bytes(50);
    dropped.increment_messages_count(5);
    dropped.increment_segments_count(1);
    // A restore can leave a stream total that none of its topics counts.
    inner.items[1].stats.increment_size_bytes(7);
    assert_eq!(node_totals(&inner), (157, 15, 3, 2));

    commit_counted(
        &mut inner,
        &DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(1),
        },
    );
    assert_eq!(node_totals(&inner), (100, 10, 2, 1));

    // Totals that drifted from the partitions both ways: a rollback taken
    // from the stream alone, and a handle that outlived its entry and still
    // adds to its parents.
    inner.items[0].stats.decrement_size_bytes(40);
    inner.items[0].stats.decrement_messages_count(4);
    PartitionStats::new(inner.items[0].topics[0].stats.clone()).increment_segments_count(3);
    assert_eq!(node_totals(&inner), (60, 6, 5, 2));
    let snapshot = Streams::from(inner.clone()).to_snapshot();
    inner.restore_in_place(snapshot);
    assert_eq!(node_totals(&inner), (100, 10, 2, 1));
}

/// The rebuild runs on the metadata shard while the owning shard keeps
/// rotating, so a segment that lands after the sample is already on the
/// node and has to stay there.
#[test]
fn given_a_rotation_after_the_sample_when_rebuilding_totals_should_keep_it_on_the_node() {
    let mut inner = StreamsInner::new();
    commit_counted(
        &mut inner,
        &CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        },
    );
    create_counted_topic(&mut inner, 0, "topic", 1, 1);
    let partition = inner.stats_registry.partition(
        0,
        0,
        &committed_partition(&inner, 0, 0, 0),
        inner.items[0].topics[0].stats.clone(),
    );
    partition.increment_segments_count(2);

    let sample = inner.stats_registry.sample_counters();
    partition.increment_segments_count(1);
    inner
        .stats_registry
        .rebuild_from_sample(&inner.items, &sample);

    assert_eq!(node_totals(&inner), (0, 0, 3, 1));
}

/// `vacant_key()` is both the id handed to the next `CreateStream` and the
/// input to the `MAX_STREAMS` guard, so it must depend only on committed
/// state. `StreamsSnapshot` carries `items` and `revision` and nothing else,
/// which is why [`IdSlab`] derives the key from the occupied set instead of
/// from a free list: a divergence here forks the state machine, since a
/// restored replica would assign a different stream id for the same log
/// entry, hence a different packed namespace, shard owner, consensus group,
/// on-disk directory and authorization scope.
///
/// Descending deletes are the case that catches it. `slab::Slab` recycled
/// keys LIFO and rebuilt that head by ascending scan, so this asserted
/// `1 == 0` before the swap.
#[test]
fn given_descending_deletes_when_round_tripping_a_snapshot_should_keep_vacant_key() {
    let mut inner = StreamsInner::new();
    for name in ["a", "b", "c"] {
        create_stream(&mut inner, name);
    }
    delete_stream(&mut inner, 1);
    delete_stream(&mut inner, 0);
    let before = inner.items.vacant_key();

    let snapshot = Streams::from(inner.clone()).to_snapshot();
    let restored = StreamsInner::inner_from_snapshot(snapshot, Arc::new(StatsRegistry::default()));

    assert_eq!(
        restored.items.vacant_key(),
        before,
        "the id the next CreateStream is assigned must not depend on whether \
         a snapshot was restored in between"
    );
}

/// Topic ids come from the per-stream `topics` arena by the same
/// `vacant_key()` route, so they forked identically. Held separate from the
/// stream case because the two arenas are restored by different code paths.
#[test]
fn given_descending_topic_deletes_when_round_tripping_a_snapshot_should_keep_vacant_key() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");
    for name in ["a", "b", "c"] {
        let request = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 0, name),
            derived_options: WireOptions::empty(),
            partitions: Vec::new(),
        };
        assert_eq!(
            StateHandler::apply(&request, &mut inner, IggyTimestamp::now()).code,
            0
        );
    }
    for topic_id in [1, 0] {
        let request = DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(topic_id),
        };
        assert_eq!(
            StateHandler::apply(&request, &mut inner, IggyTimestamp::now()).code,
            0,
            "topic {topic_id} must delete"
        );
    }
    let before = inner.items.get(0).unwrap().topics.vacant_key();

    let snapshot = Streams::from(inner.clone()).to_snapshot();
    let restored = StreamsInner::inner_from_snapshot(snapshot, Arc::new(StatsRegistry::default()));

    assert_eq!(
        restored.items.get(0).unwrap().topics.vacant_key(),
        before,
        "the id the next CreateTopic is assigned must not depend on whether \
         a snapshot was restored in between"
    );
}

/// The `CreateStream` ceiling cannot be driven end to end: `MAX_STREAMS`
/// live slab entries would cost hundreds of megabytes, and `slab` offers no
/// way to seed a high key (no `insert_at`, no key setter, and `FromIterator`
/// leaves `vacant_key()` on a hole rather than past the end). Testing the
/// comparison directly is what is available, so it is factored out.
#[test]
fn given_a_slab_key_at_the_ceiling_when_admitting_should_refuse() {
    assert!(admits_slab_key(0, MAX_STREAMS));
    assert!(admits_slab_key(MAX_STREAMS - 1, MAX_STREAMS));
    assert!(!admits_slab_key(MAX_STREAMS, MAX_STREAMS));
    assert!(!admits_slab_key(MAX_STREAMS + 1, MAX_STREAMS));

    assert!(admits_slab_key(MAX_TOPICS - 1, MAX_TOPICS));
    assert!(!admits_slab_key(MAX_TOPICS, MAX_TOPICS));
}

/// Two in-range entries sharing an id pack to ONE namespace, so the range
/// guard alone still admits the aliasing this module exists to prevent.
#[test]
fn given_duplicate_partition_ids_on_topic_create_should_reject() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    let request = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 2, "t"),
        derived_options: WireOptions::empty(),
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 7,
                consensus_group_id: 1,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 7,
                consensus_group_id: 2,
                members: Vec::new(),
            },
        ],
    };
    let reply = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());
    assert_eq!(
        reply.code,
        u32::from(CreateTopicResult::InvalidPartitionsCount),
        "a duplicate id is a malformed batch, not a capacity verdict"
    );
    assert_eq!(
        inner.items.get(0).unwrap().topics.len(),
        0,
        "a rejected create must not leave a half-built topic"
    );
}

/// Same hole on the additive path: distinct offsets are what keep resolved
/// ids distinct, and nothing upstream guarantees it.
#[test]
fn given_duplicate_partition_offsets_when_adding_should_reject() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    let seed = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 1, "t"),
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&seed, &mut inner, IggyTimestamp::now()).code,
        0
    );

    let duplicate = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 2,
        },
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 2,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 3,
                members: Vec::new(),
            },
        ],
    };
    let reply = StateHandler::apply(&duplicate, &mut inner, IggyTimestamp::now());
    assert_eq!(
        reply.code,
        u32::from(CreatePartitionsResult::InvalidPartitionsCount)
    );

    let topic = inner.items.get(0).unwrap().topics.get(0).unwrap();
    assert_eq!(
        topic.partitions.len(),
        1,
        "a rejected batch must not append"
    );
}

/// Distinct offsets in the same batch must still be admitted, so the
/// uniqueness guard above cannot be passing for the wrong reason.
#[test]
fn given_distinct_partition_offsets_when_adding_should_admit() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "s");

    let seed = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 1, "t"),
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&seed, &mut inner, IggyTimestamp::now()).code,
        0
    );

    let distinct = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 2,
        },
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 2,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 1,
                consensus_group_id: 3,
                members: Vec::new(),
            },
        ],
    };
    assert_eq!(
        StateHandler::apply(&distinct, &mut inner, IggyTimestamp::now()).code,
        0
    );

    let topic = inner.items.get(0).unwrap().topics.get(0).unwrap();
    let ids: Vec<usize> = topic
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect();
    assert_eq!(
        ids,
        vec![0, 1, 2],
        "resolved ids must stay dense and distinct"
    );
}
