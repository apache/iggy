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

#[test]
fn given_topic_that_places_batches_when_checking_should_report_store_data() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    create_topic_with_partitions(&mut inner, 1);
    assert!(!Streams::from(inner.clone()).has_topic_with_store_data());

    inner.items[0].topics[0].resolved.runtime.places_batches = true;

    assert!(!inner.items[0].topics[0].resolved.tiered_storage);
    assert!(Streams::from(inner).has_topic_with_store_data());
}

#[test]
fn given_topic_turned_tiered_when_checking_should_report_store_data() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    create_topic_with_partitions(&mut inner, 1);
    assert!(!Streams::from(inner.clone()).has_topic_with_store_data());
    let mut explicit = ResourceOptions::new();
    explicit.insert(
        HeaderKey::from(OptionKey::TieredStorage),
        OptionValue::explicit(true.encode().unwrap()),
    );
    let enable = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: None,
        explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All).unwrap(),
        derived_options: WireOptions::empty(),
    };

    assert_eq!(
        StateHandler::apply(&enable, &mut inner, IggyTimestamp::now()).code,
        0
    );
    assert!(Streams::from(inner).has_topic_with_store_data());
}

/// Every replica of every partition starts its chain file when the topic
/// turns tiered storage on, so the enable records one change per
/// partition. A patch that leaves the topic tiered records nothing.
#[test]
fn given_a_topic_when_tiered_storage_is_enabled_should_record_each_partition_once() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    create_topic_with_partitions(&mut inner, 2);
    let created = inner.revision;
    let mut explicit = ResourceOptions::new();
    explicit.insert(
        HeaderKey::from(OptionKey::TieredStorage),
        OptionValue::explicit(true.encode().unwrap()),
    );
    let enable = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: None,
        explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All).unwrap(),
        derived_options: WireOptions::empty(),
    };

    assert_eq!(
        StateHandler::apply(&enable, &mut inner, IggyTimestamp::now()).code,
        0
    );
    assert!(inner.items[0].topics[0].resolved.tiered_storage);
    let enabled = inner.revision;
    assert_eq!(enabled, created + 1);
    let change = |partition_id| {
        (
            enabled,
            PartitionChange {
                namespace: IggyNamespace::new(0, 0, partition_id),
                created_revision: created,
                kind: PartitionChangeKind::TieringEnabled,
                skips_object_deletes: false,
            },
        )
    };
    assert_eq!(changes_since(&inner, created), [change(0), change(1)]);

    assert_eq!(
        StateHandler::apply(&enable, &mut inner, IggyTimestamp::now()).code,
        0
    );
    assert_eq!(inner.revision, enabled);
    assert_eq!(changes_since(&inner, enabled), []);
}

#[test]
fn given_a_topic_without_partitions_when_tiered_storage_is_enabled_should_keep_the_revision() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    create_topic_with_partitions(&mut inner, 0);
    let created = inner.revision;
    let mut explicit = ResourceOptions::new();
    explicit.insert(
        HeaderKey::from(OptionKey::TieredStorage),
        OptionValue::explicit(true.encode().unwrap()),
    );
    let enable = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: None,
        explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All).unwrap(),
        derived_options: WireOptions::empty(),
    };

    assert_eq!(
        StateHandler::apply(&enable, &mut inner, IggyTimestamp::now()).code,
        0
    );

    assert!(inner.items[0].topics[0].resolved.tiered_storage);
    assert_eq!(inner.revision, created);
}

/// Readers act on every partition a commit touched, so each applied
/// create, truncate and removal records one change per partition at the
/// revision its commit bumped to.
#[test]
fn given_partition_commits_when_reading_changes_should_list_each_partition_at_its_revision() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let start = inner.revision;
    create_topic_with_partitions(&mut inner, 2);
    let created = inner.revision;
    let first = IggyNamespace::new(0, 0, 0);
    let second = IggyNamespace::new(0, 0, 1);
    let change = |revision, namespace, kind| {
        (
            revision,
            PartitionChange {
                namespace,
                created_revision: created,
                kind,
                skips_object_deletes: false,
            },
        )
    };
    assert_eq!(
        changes_since(&inner, start),
        [
            change(created, first, PartitionChangeKind::Created),
            change(created, second, PartitionChangeKind::Created),
        ]
    );

    let truncate = TruncatePartitionRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partition_id: 0,
        up_to_offset: 10,
        expected_history: Some(created),
    };
    // The second truncate leaves the watermark where it is and records too.
    for _ in 0..2 {
        let apply = StateHandler::apply(&truncate, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
    }
    let truncated = inner.revision;
    assert_eq!(
        changes_since(&inner, created),
        [
            change(truncated - 1, first, PartitionChangeKind::Truncated),
            change(truncated, first, PartitionChangeKind::Truncated),
        ]
    );

    let delete_topic = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete_topic,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);
    let removed = inner.revision;
    assert_eq!(
        changes_since(&inner, truncated),
        [
            change(removed, first, PartitionChangeKind::Removed),
            change(removed, second, PartitionChangeKind::Removed),
        ]
    );
}

/// The deletes of removed incarnations leave the objects of a geo topic to
/// the sweep of its tombstone, so every change names the topic's type.
#[test]
fn given_a_geo_and_a_normal_topic_when_their_partitions_change_should_mark_only_the_geo_ones() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let start = inner.revision;
    let geo = TopicCreateOptions {
        topic_type: Some(TopicType::Geo),
        geo_mode: Some(GeoMode::Geo),
        ..TopicCreateOptions::default()
    };
    for (topic_id, name, options) in [
        (0, "geo", geo.to_wire().unwrap()),
        (1, "normal", WireOptions::empty()),
    ] {
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: WireCreateTopicRequest {
                options,
                ..make_topic_request(0, 2, name)
            },
            derived_options: WireOptions::empty(),
            partitions: (0..2)
                .map(|partition_id| CreatedPartitionAssignment {
                    initial_view: 0,
                    partition_id,
                    consensus_group_id: 10 * topic_id + u64::from(partition_id) + 1,
                    members: Vec::new(),
                })
                .collect(),
        };
        let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
            &create_topic,
            &mut inner,
            IggyTimestamp::now(),
        );
        assert_eq!(apply.code, 0);
    }
    assert!(inner.items[0].topics[0].is_geo());
    assert!(!inner.items[0].topics[1].is_geo());
    for topic_id in 0..2 {
        let truncate = TruncatePartitionRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(topic_id),
            partition_id: 0,
            up_to_offset: 10,
            expected_history: None,
        };
        let apply = StateHandler::apply(&truncate, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
    }
    let delete_stream = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(0),
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete_stream,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    let changes = changes_since(&inner, start);
    for kind in [
        PartitionChangeKind::Created,
        PartitionChangeKind::Truncated,
        PartitionChangeKind::Removed,
    ] {
        for skips_object_deletes in [true, false] {
            assert!(
                changes.iter().any(|(_, change)| change.kind == kind
                    && change.skips_object_deletes == skips_object_deletes),
                "{kind:?} of a {} topic: {changes:?}",
                if skips_object_deletes {
                    "geo"
                } else {
                    "normal"
                }
            );
        }
    }
    for (_, change) in changes {
        assert_eq!(
            change.skips_object_deletes,
            change.namespace.topic_id() == 0,
            "{change:?}"
        );
    }
}

/// A reader the feed cannot serve must rebuild from the tree: after an
/// eviction, after a restore, and with a cursor from a newer state.
#[test]
fn given_cursor_the_feed_cannot_serve_when_reading_changes_should_return_none() {
    let change = PartitionChange {
        namespace: IggyNamespace::new(0, 0, 0),
        created_revision: 1,
        kind: PartitionChangeKind::Truncated,
        skips_object_deletes: false,
    };
    let mut changes = RecentPartitionChanges::default();
    let mut revision = 0;
    for _ in 0..=RECENT_PARTITION_CHANGES_RETAINED {
        revision += 1;
        changes.push(revision, change);
    }
    assert!(changes.since(0).is_none(), "the oldest change was evicted");
    assert_eq!(
        changes.since(1).map(Iterator::count),
        Some(RECENT_PARTITION_CHANGES_RETAINED)
    );

    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    create_topic_with_partitions(&mut inner, 1);
    let revision = inner.revision;
    assert!(
        inner.partition_changes_since(revision + 1).is_none(),
        "a cursor from a newer state"
    );
    let restored =
        Streams::from_snapshot(Streams::from(inner).to_snapshot()).expect("restore snapshot");
    let (served, forgotten) = restored.read(|inner| {
        (
            inner.partition_changes_since(revision).map(Iterator::count),
            inner.partition_changes_since(revision - 1).is_none(),
        )
    });
    assert_eq!(served, Some(0));
    assert!(forgotten, "a restore forgets the changes before it");
}

/// Deleting a partition has to roll its bytes out of the topic and stream
/// totals, not merely drop its registry entry: the aggregates are counters
/// the partition increments through its parent `Arc`, so an evicted entry
/// leaves what it contributed behind and `get_topic` / `get_stream` /
/// `/metrics` keep reporting deleted data until a restart rebuilds them.
#[test]
fn given_counted_partition_when_apply_delete_partitions_should_roll_it_out_of_the_parents() {
    let mut inner = inner_with_registered_partition();
    let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
    stats.increment_segments_count(1);
    stats.increment_messages_count(7);
    stats.increment_size_bytes(512);
    assert_eq!(
        inner.items[0].stats.size_bytes_inconsistent(),
        512,
        "partition counters must roll up before the delete, or the test proves nothing"
    );

    let delete = DeletePartitionsRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partitions_count: 1,
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    let topic_stats = &inner.items[0].topics[0].stats;
    assert_eq!(topic_stats.size_bytes_inconsistent(), 0);
    assert_eq!(topic_stats.messages_count_inconsistent(), 0);
    assert_eq!(topic_stats.segments_count_inconsistent(), 0);
    let stream_stats = &inner.items[0].stats;
    assert_eq!(stream_stats.size_bytes_inconsistent(), 0);
    assert_eq!(stream_stats.messages_count_inconsistent(), 0);
    assert_eq!(stream_stats.segments_count_inconsistent(), 0);
    assert!(
        inner.stats_registry.partition_get(0, 0, 0).is_none(),
        "the entry has to go with the rollback: partition ids are re-minted \
         from max + 1, so the next create lands on this key"
    );
}

/// The apply runs on BOTH left-right buffers off one shared registry, so the
/// rollback must land exactly once. A second pass that decremented again
/// would wrap the unsigned totals rather than settle at zero.
#[test]
fn given_replayed_delete_partitions_when_applied_twice_should_not_double_roll_back() {
    let mut first = inner_with_registered_partition();
    let stats = first.stats_registry.partition_get(0, 0, 0).expect("stats");
    stats.increment_messages_count(7);
    stats.increment_size_bytes(512);
    let mut second = first.clone();

    let delete = DeletePartitionsRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partitions_count: 1,
    };
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut first,
        IggyTimestamp::now(),
    );
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut second,
        IggyTimestamp::now(),
    );

    assert!(
        first.stats_registry.partition_get(0, 0, 0).is_none(),
        "the first apply evicts; the second has to find nothing to roll back"
    );
    let topic_stats = &second.items[0].topics[0].stats;
    assert_eq!(topic_stats.size_bytes_inconsistent(), 0);
    assert_eq!(topic_stats.messages_count_inconsistent(), 0);
    let stream_stats = &second.items[0].stats;
    assert_eq!(
        stream_stats.size_bytes_inconsistent(),
        0,
        "the second buffer's apply must not subtract the bytes again"
    );
    assert_eq!(stream_stats.messages_count_inconsistent(), 0);
}

/// `delete_topic` has the same duty one level up: dropping the topic entry
/// and its partitions leaves the stream total carrying the deleted topic.
#[test]
fn given_counted_topic_when_apply_delete_topic_should_roll_it_out_of_the_stream() {
    let mut inner = inner_with_registered_partition();
    let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
    stats.increment_segments_count(1);
    stats.increment_messages_count(7);
    stats.increment_size_bytes(512);
    assert_eq!(
        inner.items[0].stats.size_bytes_inconsistent(),
        512,
        "partition counters must roll up before the delete, or the test proves nothing"
    );

    let delete = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    let stream_stats = &inner.items[0].stats;
    assert_eq!(stream_stats.size_bytes_inconsistent(), 0);
    assert_eq!(stream_stats.messages_count_inconsistent(), 0);
    assert_eq!(stream_stats.segments_count_inconsistent(), 0);
    assert!(
        inner.stats_registry.partition_get(0, 0, 0).is_none(),
        "a recycled topic slab key would otherwise inherit these counters"
    );
}

/// A partition keeps its `Arc` until the reconciler tears it down, which
/// happens after the commit that acked the delete, so writes in that window
/// land in a topic whose registry entry for them is already gone. Deleting
/// the topic has to settle that residue on the stream: the partition sweep
/// finds nothing to roll back, and only the topic's own swap can reach it.
#[test]
fn given_topic_residue_no_partition_entry_covers_when_apply_delete_topic_should_settle_it() {
    let mut inner = inner_with_registered_partition();
    let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");

    let delete_partitions = DeletePartitionsRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partitions_count: 1,
    };
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete_partitions,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert!(inner.stats_registry.partition_get(0, 0, 0).is_none());

    // The still-mounted partition, writing through the handle it cached
    // before the delete.
    stats.increment_size_bytes(100);
    stats.increment_messages_count(2);
    assert_eq!(inner.items[0].stats.size_bytes_inconsistent(), 100);

    let delete_topic = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete_topic,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    let stream_stats = &inner.items[0].stats;
    assert_eq!(
        stream_stats.size_bytes_inconsistent(),
        0,
        "bytes no surviving partition entry accounts for still owe the stream a rollback"
    );
    assert_eq!(stream_stats.messages_count_inconsistent(), 0);
}

/// A restore replaces the whole tree while the receiver's partition
/// counters keep running, and it adopts no aggregate of its own: without
/// the rebuild every topic and stream would come back at zero over live
/// children. Rolling a partition out of a parent that never counted it
/// subtracts past zero, and the totals are unsigned: `get_stream` /
/// `get_topic` / `/stats` would serve ~1.8e19 until the process restarted.
#[test]
fn given_a_restore_over_live_partition_counters_when_deleting_should_not_wrap() {
    let mut inner = inner_with_registered_partition();
    let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
    stats.increment_segments_count(3);
    stats.increment_messages_count(7);
    stats.increment_size_bytes(512);

    // The donor captured this tree before any of that landed.
    let snapshot = Streams::from(inner.clone()).to_snapshot();
    inner.restore_in_place(snapshot);

    // The rebuild has to put the receiver's own children back into the
    // parents, or the delete below has nothing to subtract from.
    assert_eq!(
        inner.items[0].topics[0].stats.size_bytes_inconsistent(),
        512
    );
    assert_eq!(inner.items[0].stats.size_bytes_inconsistent(), 512);

    let delete = DeletePartitionsRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partitions_count: 1,
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    let topic_stats = &inner.items[0].topics[0].stats;
    assert_eq!(topic_stats.size_bytes_inconsistent(), 0);
    assert_eq!(topic_stats.messages_count_inconsistent(), 0);
    assert_eq!(topic_stats.segments_count_inconsistent(), 0);
    let stream_stats = &inner.items[0].stats;
    assert_eq!(stream_stats.size_bytes_inconsistent(), 0);
    assert_eq!(stream_stats.messages_count_inconsistent(), 0);
    // Its own `clamped_sub!` expansion and its own `UnderflowSite`, so the
    // u32 wiring needs an assertion of its own.
    assert_eq!(stream_stats.segments_count_inconsistent(), 0);
}

/// `DeletePartitions` truncates the tail while the registry selects by id,
/// and only a primary that minted dense ids makes those the same set. With
/// ids `[3, 4]` a count-based predicate selects both, which strips the
/// SURVIVOR's bytes from its parents and resets the offset its clients
/// store against -- `IggyPartition` reads an empty offset space and denies
/// every `store_consumer_offset` after that.
#[test]
fn given_non_zero_base_partition_ids_when_apply_delete_partitions_should_keep_the_survivor() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "alpha");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 2, "logs"),
        derived_options: WireOptions::empty(),
        partitions: vec![
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 3,
                consensus_group_id: 1,
                members: Vec::new(),
            },
            CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 4,
                consensus_group_id: 2,
                members: Vec::new(),
            },
        ],
    };
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &create_topic,
        &mut inner,
        IggyTimestamp::now(),
    );
    let topic_stats = inner.items[0].topics[0].stats.clone();
    let survivor = inner.stats_registry.partition(
        0,
        0,
        &committed_partition(&inner, 0, 0, 3),
        topic_stats.clone(),
    );
    let removed =
        inner
            .stats_registry
            .partition(0, 0, &committed_partition(&inner, 0, 0, 4), topic_stats);
    survivor.increment_size_bytes(512);
    survivor.set_current_offset(42);
    removed.increment_size_bytes(64);

    let delete = DeletePartitionsRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partitions_count: 1,
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    assert!(
        inner.stats_registry.partition_get(0, 0, 4).is_none(),
        "the truncated partition's entry must go"
    );
    let survivor = inner
        .stats_registry
        .partition_get(0, 0, 3)
        .expect("the retained partition keeps its entry");
    assert_eq!(survivor.size_bytes_inconsistent(), 512);
    assert_eq!(
        survivor.current_offset(),
        42,
        "zeroing a live partition's offset denies every store_consumer_offset above it"
    );
    assert_eq!(
        inner.items[0].topics[0].stats.size_bytes_inconsistent(),
        512
    );
    assert_eq!(inner.items[0].stats.size_bytes_inconsistent(), 512);
}

/// The sweep has to reach every partition of the topic, not the first one.
#[test]
fn given_many_counted_partitions_when_apply_delete_topic_should_roll_all_of_them_out() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "alpha");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 4, "logs"),
        derived_options: WireOptions::empty(),
        partitions: (0..4)
            .map(|partition_id| CreatedPartitionAssignment {
                initial_view: 0,
                partition_id,
                consensus_group_id: u64::from(partition_id) + 1,
                members: Vec::new(),
            })
            .collect(),
    };
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &create_topic,
        &mut inner,
        IggyTimestamp::now(),
    );
    let topic_stats = inner.items[0].topics[0].stats.clone();
    for partition_id in 0..4 {
        let stats = inner.stats_registry.partition(
            0,
            0,
            &committed_partition(&inner, 0, 0, partition_id),
            topic_stats.clone(),
        );
        stats.increment_size_bytes(512);
        stats.increment_messages_count(7);
        stats.increment_segments_count(1);
    }
    assert_eq!(inner.items[0].stats.size_bytes_inconsistent(), 512 * 4);

    let delete = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    let stream_stats = &inner.items[0].stats;
    assert_eq!(stream_stats.size_bytes_inconsistent(), 0);
    assert_eq!(stream_stats.messages_count_inconsistent(), 0);
    assert_eq!(stream_stats.segments_count_inconsistent(), 0);
    for partition_id in 0..4 {
        assert!(
            inner
                .stats_registry
                .partition_get(0, 0, partition_id)
                .is_none(),
            "partition {partition_id} kept its entry"
        );
    }
}

/// `remove_stream` is evict-only on purpose: `StreamStats` is the rollup
/// root, so nothing above it is owed the counters back. What it still has
/// to do is take the whole subtree's entries with it, or a recycled stream
/// slab key hands its topics' partitions the predecessor's counters.
#[test]
fn given_a_counted_stream_when_apply_delete_stream_should_evict_its_whole_subtree() {
    let mut inner = inner_with_registered_partition();
    let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
    stats.increment_size_bytes(512);
    stats.increment_messages_count(7);

    let delete = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(0),
    };
    let apply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(apply.code, 0);

    assert!(
        inner.stats_registry.partition_get(0, 0, 0).is_none(),
        "the partition entry must not outlive its stream"
    );
    // Re-creating over the freed slab key must start from zero rather than
    // inherit what the probe above counted.
    create_stream(&mut inner, "beta");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 1, "logs"),
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    let _ = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &create_topic,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(inner.items[0].stats.size_bytes_inconsistent(), 0);
    assert_eq!(inner.items[0].topics[0].stats.size_bytes_inconsistent(), 0);
}

// Drives the real `State::apply` path (parse -> dispatch -> left/right ->
// read-back) so both `absorb_first` and `absorb_second` run, and pins that
// they agree: a duplicate create returns the conflict code AND leaves
// exactly one stream.
#[test]
fn given_duplicate_create_when_applied_through_state_should_converge_both_buffers() {
    use crate::stm::State;
    use iggy_common::Either;

    let streams = Streams::default();
    let Either::Left(first) = streams
        .apply(make_create_stream_prepare("dup", 1))
        .expect("first apply ok")
    else {
        panic!("CreateStream must be handled by the Streams state");
    };
    assert_eq!(first.code, 0);

    let Either::Left(second) = streams
        .apply(make_create_stream_prepare("dup", 2))
        .expect("second apply ok")
    else {
        panic!("CreateStream must be handled by the Streams state");
    };
    assert_eq!(
        second.code,
        u32::from(CreateStreamResult::NameAlreadyExists)
    );

    let count = streams.read(|inner| inner.items.len());
    assert_eq!(count, 1, "duplicate must not insert a second stream");
}

/// A delete leaves every surviving id above the hole naming an index that
/// is no longer its own, so the direct hit has to fall through to the scan
/// or the survivor resolves to nothing and its namespace stops routing.
#[test]
fn given_sparse_partition_ids_when_resolving_should_fall_back_to_the_scan() {
    let inner = inner_with_registered_partition();
    let template = committed_partition(&inner, 0, 0, 0);
    let partition = |id| Partition {
        id,
        ..template.clone()
    };
    let dense = vec![partition(0), partition(1)];
    let sparse = vec![partition(3), partition(7)];

    assert_eq!(
        find_partition(&dense, 1).map(|partition| partition.id),
        1.into()
    );
    assert_eq!(
        find_partition(&sparse, 3).map(|partition| partition.id),
        3.into()
    );
    assert_eq!(
        find_partition(&sparse, 7).map(|partition| partition.id),
        7.into()
    );
    assert!(find_partition(&sparse, 0).is_none());
    assert!(find_partition(&sparse, 1).is_none());
    assert!(find_partition(&dense, 2).is_none());
}

/// A fetch resolves its stream, topic and group once, so each of its
/// partitions must still get the answer of the three calls that a poll of
/// that partition alone runs: the group fence, the namespace and the poll
/// metadata.
#[test]
fn given_a_group_poll_of_many_partitions_when_resolving_once_should_match_one_poll_each() {
    const CLIENT: u128 = 7;
    const OTHER_CLIENT: u128 = 8;
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "alpha");
    let create_topic = CreateTopicWithAssignmentsRequest {
        request: make_topic_request(0, 4, "logs"),
        derived_options: WireOptions::empty(),
        partitions: (0..4)
            .map(|partition_id| CreatedPartitionAssignment {
                initial_view: 0,
                partition_id,
                consensus_group_id: u64::from(partition_id) + 1,
                members: Vec::new(),
            })
            .collect(),
    };
    let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
    let topic = &mut inner.items[0].topics[0];
    let incarnation = topic.partitions[0].created_revision;
    let owner = |client_id, incarnation| ConsumerGroupAssignment {
        incarnation,
        owner: Some(ConsumerGroupOwner {
            client_id,
            session: 1,
            generation: 5,
        }),
        pending: None,
    };
    let mut group = ConsumerGroup::new(3, Arc::from("readers"));
    group.members.insert(ConsumerGroupMember {
        partitions: vec![0, 1, 2, 3],
        pending_revocations: vec![1],
        ..ConsumerGroupMember::new(0, CLIENT)
    });
    // Partition 0 serves the client, 1 drains a handoff, 2 is owned by
    // another client, 3 belongs to an older incarnation and 4 is missing.
    group.assignments.insert(0, owner(CLIENT, incarnation));
    group.assignments.insert(1, owner(CLIENT, incarnation));
    group
        .assignments
        .insert(2, owner(OTHER_CLIENT, incarnation));
    group.assignments.insert(3, owner(CLIENT, incarnation + 1));
    topic.consumer_groups.insert(group.id, Box::new(group));
    topic.consumer_group_index.insert(Arc::from("readers"), 3);
    let streams = Streams::from(inner);
    let stream_id = WireIdentifier::numeric(0);
    let topic_id = WireIdentifier::numeric(0);
    let partitions = [0, 1, 2, 3, 4];

    for (group_id, client_id) in [
        (WireIdentifier::named("readers").unwrap(), CLIENT),
        (WireIdentifier::named("readers").unwrap(), OTHER_CLIENT),
        (WireIdentifier::numeric(9), CLIENT),
    ] {
        let (resolved_group, targets) = streams.poll_targets(
            &stream_id,
            &topic_id,
            Some(&group_id),
            client_id,
            &partitions,
        );
        for (&partition_id, target) in partitions.iter().zip(targets) {
            let one_poll = streams
                .consumer_group_fence(
                    &stream_id,
                    &topic_id,
                    &group_id,
                    client_id,
                    partition_id,
                    true,
                )
                .map_or(PollTarget::NotOwned, |group| {
                    assert_eq!(resolved_group, Some(group));
                    streams
                        .namespace_from_partition(&stream_id, &topic_id, partition_id)
                        .map_or(PollTarget::NotFound, |namespace| {
                            streams
                                .poll_metadata(namespace, Some(group), client_id)
                                .map_or(PollTarget::Unavailable, |metadata| PollTarget::Pollable {
                                    namespace,
                                    metadata,
                                })
                        })
                });
            assert_eq!(target, one_poll, "partition {partition_id} of {group_id}");
        }
    }
    let (group, targets) = streams.poll_targets(&stream_id, &topic_id, None, CLIENT, &partitions);
    assert_eq!(group, None);
    assert!(matches!(targets[0], PollTarget::Pollable { .. }));
    assert_eq!(targets[4], PollTarget::NotFound);
    let (_, targets) = streams.poll_targets(
        &WireIdentifier::numeric(5),
        &topic_id,
        None,
        CLIENT,
        &partitions,
    );
    assert_eq!(targets, vec![PollTarget::NotFound; partitions.len()]);
}
