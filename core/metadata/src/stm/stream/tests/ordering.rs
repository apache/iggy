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
fn given_a_member_beyond_the_node_count_when_checking_the_cluster_should_name_it() {
    let streams = Streams::default();
    let namespace = IggyNamespace::new(0, 0, 0);
    streams.seed_namespace_on(namespace, 1, 0, iggy_common::Durability::Persisted, &[1, 4]);
    assert_eq!(streams.member_outside_cluster(3), Some((namespace, 4)));
    assert_eq!(streams.member_outside_cluster(5), None);
}

#[test]
fn given_random_stream_creates_and_deletes_when_applied_should_keep_the_creation_index_exact() {
    let mut inner = StreamsInner::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    for step in 1..=400u64 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let live: Vec<usize> = inner.items.iter().map(|(id, _)| id).collect();
        if live.is_empty() || !seed.is_multiple_of(3) {
            create_stream_at(&mut inner, &format!("s{step}"), step);
        } else {
            let pick = usize::try_from(seed % live.len() as u64).unwrap();
            delete_stream_at(&mut inner, live[pick], step);
        }
        assert_eq!(
            inner.created_index,
            stream_creation_keys(&inner),
            "step {step}"
        );
    }
    assert!(inner.created_index.len() > 10);
    let snapshot = Streams::from(inner.clone()).to_snapshot();
    let restored = StreamsInner::inner_from_snapshot(snapshot, Arc::default());
    assert_eq!(restored.created_index, stream_creation_keys(&inner));
}

#[test]
fn given_a_reused_stream_id_when_walking_by_creation_should_list_the_new_stream_last() {
    let mut inner = StreamsInner::new();
    for (micros, name) in [(10, "a"), (20, "b"), (30, "c")] {
        create_stream_at(&mut inner, name, micros);
    }
    delete_stream_at(&mut inner, 0, 40);
    create_stream_at(&mut inner, "d", 50);
    assert_eq!(inner.items[0].name.as_ref(), "d");

    let by_created = |after, descending| {
        inner
            .streams_by_created(after, descending)
            .map(|((_, id), _)| id)
            .collect::<Vec<_>>()
    };
    assert_eq!(by_created(None, false), [1, 2, 0]);
    assert_eq!(by_created(None, true), [0, 2, 1]);
    assert_eq!(by_created(Some((20, 1)), false), [2, 0]);
    assert_eq!(by_created(Some((50, 0)), true), [2, 1]);
    assert_eq!(
        by_created(Some((10, 0)), false),
        [1, 2, 0],
        "a deleted key still splits"
    );
    let by_id: Vec<usize> = inner.items.walk(None, false).map(|(id, _)| id).collect();
    assert_eq!(by_id, [0, 1, 2]);
}

#[test]
fn given_a_reused_topic_id_when_sorting_by_creation_should_list_the_new_topic_last() {
    let create_topic_at = |inner: &mut StreamsInner, name: &str, micros: u64| {
        let create = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 1, name),
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: micros,
                members: Vec::new(),
            }],
        };
        let reply = crate::stm::lifecycle::apply_with_lifecycle_completion(
            &create,
            inner,
            IggyTimestamp::from(micros),
        );
        assert_eq!(reply.code, 0);
    };
    let mut inner = StreamsInner::new();
    create_stream_at(&mut inner, "stream", 1);
    for (micros, name) in [(10, "a"), (20, "b"), (30, "c")] {
        create_topic_at(&mut inner, name, micros);
    }
    let delete = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
    };
    let reply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::from(40),
    );
    assert_eq!(reply.code, 0);
    create_topic_at(&mut inner, "d", 50);

    let snapshot = Streams::from(inner.clone()).to_snapshot();
    let restored = StreamsInner::inner_from_snapshot(snapshot, Arc::default());
    for stream in [&inner.items[0], &restored.items[0]] {
        assert_eq!(stream.topics[0].name.as_ref(), "d");
        let ids = |after, descending| {
            stream
                .topics_by_created_after(after, descending)
                .map(|((_, id), _)| id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(None, false), [1, 2, 0]);
        assert_eq!(ids(Some((50, 0)), true), [2, 1]);
    }
}

#[test]
fn given_partition_deletes_and_creates_when_walking_should_yield_ascending_ids() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    create_topic_with_partitions(&mut inner, 4);
    let delete = DeletePartitionsRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partitions_count: 2,
    };
    let reply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(reply.code, 0);
    let create = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 3,
        },
        partitions: (0..3)
            .map(|partition_id| CreatedPartitionAssignment {
                initial_view: 0,
                partition_id,
                consensus_group_id: 100 + u64::from(partition_id),
                members: Vec::new(),
            })
            .collect(),
    };
    let reply = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &create,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(reply.code, 0);

    let walk = |topic: &Topic, after, descending| {
        topic
            .partitions_after(after, descending)
            .map(|partition| partition.id)
            .collect::<Vec<_>>()
    };
    let topic = &inner.items[0].topics[0];
    assert_eq!(walk(topic, None, false), [0, 1, 2, 3, 4]);
    assert_eq!(walk(topic, Some(1), false), [2, 3, 4]);
    assert_eq!(walk(topic, Some(3), true), [2, 1, 0]);
    let snapshot = Streams::from(inner.clone()).to_snapshot();
    let restored = StreamsInner::inner_from_snapshot(snapshot, Arc::default());
    assert_eq!(
        walk(&restored.items[0].topics[0], None, false),
        [0, 1, 2, 3, 4]
    );

    let mut sparse = topic.clone();
    sparse.partitions.remove(2);
    assert_eq!(walk(&sparse, Some(2), false), [3, 4]);
    assert_eq!(walk(&sparse, Some(2), true), [1, 0]);
    assert_eq!(walk(&sparse, None, true), [4, 3, 1, 0]);
}
