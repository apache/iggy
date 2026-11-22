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

/// Pins the asymmetry that hid the fork: ascending deletes leave a free-list
/// head the ascending rebuild happens to reproduce, so only the descending
/// order diverges. Without this, a fix that accidentally broke the agreeing
/// case would still look correct.
#[test]
fn given_ascending_deletes_when_round_tripping_a_snapshot_should_keep_vacant_key() {
    let mut inner = StreamsInner::new();
    for name in ["a", "b", "c"] {
        create_stream(&mut inner, name);
    }
    delete_stream(&mut inner, 0);
    delete_stream(&mut inner, 1);
    let before = inner.items.vacant_key();

    let snapshot = Streams::from(inner.clone()).to_snapshot();
    let restored = StreamsInner::inner_from_snapshot(snapshot, Arc::new(StatsRegistry::default()));

    assert_eq!(restored.items.vacant_key(), before);
}

#[test]
fn given_directory_reports_when_batching_for_a_peer_should_relay_committed_ones_narrowed_and_retain_in_chunks()
 {
    let streams = Streams::default();
    let stored_by_peer = IggyNamespace::new(0, 0, 0);
    let elsewhere = IggyNamespace::new(1, 0, 0);
    streams.seed_namespace_on(
        stored_by_peer,
        1,
        0,
        iggy_common::Durability::Persisted,
        &[1],
    );
    streams.seed_namespace_on(elsewhere, 2, 0, iggy_common::Durability::Persisted, &[2]);
    let status = |namespace: IggyNamespace| PartitionStatusEntry {
        namespace: namespace.inner(),
        created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
        view: 0,
        commit: 0,
        current_offset: 0,
        messages_count: 0,
        size_bytes: 0,
        segments_count: 1,
        in_sync_nodes: 0,
    };
    streams.record_partition_statuses(
        &[status(stored_by_peer), status(elsewhere)],
        0,
        3,
        Instant::now(),
    );
    let (batch, cursor) = streams.partition_status_batch(None, 4, 4);
    assert_eq!(
        batch,
        [status(stored_by_peer), status(elsewhere)],
        "a peer storing a partition still takes its report, for the in-sync nodes"
    );
    assert_eq!(cursor, Some(elsewhere.inner()));
    streams.record_partition_statuses(
        &[PartitionStatusEntry {
            commit: 1,
            in_sync_nodes: 0b111,
            ..status(elsewhere)
        }],
        0,
        3,
        Instant::now(),
    );
    streams.set_partition_directory_ready_nodes(0b011);
    assert_eq!(
        streams.partition_status(elsewhere).unwrap().in_sync_nodes,
        0b011,
        "a node the metadata primary cannot hear is not in sync"
    );
    streams.set_partition_directory_ready_nodes(u32::MAX);

    streams.record_partition_statuses(
        &[PartitionStatusEntry {
            created_revision: 99,
            ..status(elsewhere)
        }],
        0,
        3,
        Instant::now(),
    );
    assert_eq!(
        streams.retain_partition_directory(None, 1),
        Some(stored_by_peer.inner())
    );
    assert_eq!(
        streams.retain_partition_directory(Some(stored_by_peer.inner()), 1),
        Some(elsewhere.inner())
    );
    assert_eq!(
        streams.retain_partition_directory(Some(elsewhere.inner()), 1),
        None
    );
    assert_eq!(
        streams.partition_status_batch(None, 4, 4).0,
        [status(stored_by_peer)],
        "the stale incarnation's report is gone once the sweep reached it"
    );
}

#[test]
fn given_members_down_when_counting_partitions_without_a_ready_quorum_should_leave_those_with_a_ready_primary()
 {
    let streams = Streams::default();
    let seed = |stream: usize, view: u32, members: &[u8]| {
        let namespace = IggyNamespace::new(stream, 0, 0);
        streams.seed_namespace_on(
            namespace,
            u64::try_from(stream + 1).unwrap(),
            view,
            iggy_common::Durability::Persisted,
            members,
        );
        namespace
    };
    seed(0, 0, &[0, 1]);
    seed(1, 1, &[0, 1]);
    seed(2, 0, &[0, 1, 2]);
    seed(3, 0, &[0]);
    let moved = seed(4, 0, &[0, 1]);
    seed(5, 0, &[]);
    streams.record_partition_statuses(
        &[PartitionStatusEntry {
            namespace: moved.inner(),
            created_revision: streams.created_revision_for_namespace(moved).unwrap(),
            view: 1,
            commit: 0,
            current_offset: 0,
            messages_count: 0,
            size_bytes: 0,
            segments_count: 1,
            in_sync_nodes: 0b011,
        }],
        2,
        3,
        Instant::now(),
    );
    let counts = |live: &[bool]| {
        full_pass(
            &streams,
            live,
            Instant::now(),
            None,
            &mut PartitionReplicationPass::default(),
            1,
        )
        .0
    };
    assert_eq!(
        counts(&[false, true, true]).below_write_quorum,
        2,
        "only the pair led by the down node and the single copy on it"
    );
    assert_eq!(counts(&[true; 3]).below_write_quorum, 0);
}

/// A primary reports only while it leads, so a pair whose members are
/// both up but neither leads, as after both came back on empty storage,
/// keeps the last healthy report until it goes stale.
#[test]
fn given_reports_no_primary_refreshed_when_counting_should_count_no_member_in_sync() {
    let streams = Streams::default();
    let reported = IggyNamespace::new(0, 0, 0);
    let silent = IggyNamespace::new(1, 0, 0);
    streams.seed_namespace_on(reported, 1, 0, iggy_common::Durability::Persisted, &[0, 1]);
    streams.seed_namespace_on(silent, 2, 0, iggy_common::Durability::Persisted, &[1, 2]);
    let healthy = PartitionStatusEntry {
        namespace: reported.inner(),
        created_revision: streams.created_revision_for_namespace(reported).unwrap(),
        view: 0,
        commit: 0,
        current_offset: 0,
        messages_count: 0,
        size_bytes: 0,
        segments_count: 1,
        in_sync_nodes: 0b011,
    };
    let received = Instant::now();
    let cutoff = received + Duration::from_secs(1);
    let later = received + Duration::from_secs(10);
    streams.record_partition_statuses(&[healthy], 2, 3, received);
    let mut pass = PartitionReplicationPass::default();
    let mut counts = |now: Instant, stale_before: Option<Instant>| {
        full_pass(&streams, &[true; 3], now, stale_before, &mut pass, 1).0
    };
    assert_eq!(
        counts(received, None),
        PartitionReplication::default(),
        "a partition yet to report is left out"
    );
    assert_eq!(
        counts(later, Some(received)),
        PartitionReplication::default(),
        "both were seen at the cutoff"
    );
    assert_eq!(
        counts(later, Some(cutoff)),
        PartitionReplication {
            under_replicated: 2,
            below_write_quorum: 2,
        }
    );
    assert_eq!(
        streams.partition_status(reported).unwrap().in_sync_nodes,
        0,
        "GetTopic lists no member in sync either"
    );

    streams.record_partition_statuses(&[healthy], 2, 3, later);
    assert_eq!(
        counts(later, Some(cutoff)),
        PartitionReplication {
            under_replicated: 1,
            below_write_quorum: 1,
        },
        "a primary that reports again holds its members"
    );
    assert_eq!(
        streams.partition_status(reported).unwrap().in_sync_nodes,
        0b011
    );
}

/// A partition without its primary needs a view-change quorum to elect,
/// three of four, while a replication quorum is only two.
#[test]
fn given_four_members_without_their_primary_when_two_remain_should_count_them_as_unable_to_elect() {
    let streams: Streams = StreamsInner::new().into();
    streams.seed_namespace_on(
        IggyNamespace::new(0, 0, 0),
        1,
        0,
        iggy_common::Durability::Persisted,
        &[0, 1, 2, 3],
    );
    let counts = |live: &[bool]| {
        full_pass(
            &streams,
            live,
            Instant::now(),
            None,
            &mut PartitionReplicationPass::default(),
            1,
        )
        .0
    };
    assert_eq!(
        counts(&[false, false, true, true]),
        PartitionReplication {
            under_replicated: 1,
            below_write_quorum: 1,
        }
    );
    assert_eq!(
        counts(&[false, true, true, true]).below_write_quorum,
        0,
        "three members elect a new primary"
    );
    assert_eq!(
        counts(&[true, false, false, true]),
        PartitionReplication::default(),
        "a live primary that has not reported yet is not counted"
    );
}

/// Each call resumes where the last one stopped, past empty streams and
/// topics, so a pass in chunks counts what a pass in one chunk counts. A
/// chunk as large as the tree ends the pass in one call.
#[test]
fn given_a_pass_in_chunks_when_counting_should_match_a_pass_in_one_chunk() {
    const PARTITIONS: usize = 6;
    let streams = Streams::default();
    let persisted = iggy_common::Durability::Persisted;
    let reported = IggyNamespace::new(0, 0, 0);
    let every_node = IggyNamespace::new(2, 0, 0);
    // Topic 0/1 and stream 1 are empty fillers. Node 2 leads topic 0/2.
    streams.seed_namespace_on(IggyNamespace::new(0, 0, 2), 1, 0, persisted, &[0, 1, 2]);
    streams.seed_namespace_on(IggyNamespace::new(0, 2, 1), 2, 1, persisted, &[1, 2]);
    streams.seed_namespace_on(every_node, 3, 0, persisted, &[]);
    let seen = Instant::now();
    let later = seen + Duration::from_secs(10);
    let report = |namespace: IggyNamespace, in_sync_nodes| PartitionStatusEntry {
        namespace: namespace.inner(),
        created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
        view: 0,
        commit: 0,
        current_offset: 0,
        messages_count: 0,
        size_bytes: 0,
        segments_count: 1,
        in_sync_nodes,
    };
    streams.record_partition_statuses(
        &[report(reported, 0b011), report(every_node, 0b111)],
        0,
        3,
        later,
    );
    let live = [true, true, false];
    for limit in [1, 2, 4, 5, PARTITIONS, usize::MAX] {
        let mut pass = PartitionReplicationPass::default();
        assert_eq!(
            full_pass(&streams, &live, seen, None, &mut pass, limit),
            (
                PartitionReplication {
                    under_replicated: 3,
                    below_write_quorum: 2,
                },
                PARTITIONS.div_ceil(limit)
            ),
            "limit {limit}: the short report and topic 0/2, whose primary is down"
        );
        assert_eq!(
            full_pass(
                &streams,
                &live,
                later,
                Some(seen + Duration::from_secs(1)),
                &mut pass,
                limit
            )
            .0,
            PartitionReplication {
                under_replicated: 5,
                below_write_quorum: 4,
            },
            "limit {limit}: and the two the first pass found without a report"
        );
    }
}

/// A partition keeps the time it was first found without a report while
/// every pass finds it so, and counts once that time is before the stale
/// cutoff. The end of a pass forgets one that reported meanwhile.
#[test]
fn given_partitions_without_reports_when_passes_run_in_chunks_should_keep_each_first_sighting_until_a_report()
 {
    let streams = Streams::default();
    let reporting = IggyNamespace::new(0, 0, 0);
    let silent = IggyNamespace::new(0, 0, 1);
    streams.seed_namespace_on(silent, 1, 0, iggy_common::Durability::Persisted, &[0, 1]);
    let incarnation = |namespace: IggyNamespace| {
        (
            namespace.inner(),
            streams.created_revision_for_namespace(namespace).unwrap(),
        )
    };
    let seen = Instant::now();
    let later = seen + Duration::from_secs(10);
    let live = [true; 2];
    let mut pass = PartitionReplicationPass::default();
    assert_eq!(
        full_pass(&streams, &live, seen, None, &mut pass, 1).0,
        PartitionReplication::default()
    );
    assert_eq!(
        full_pass(&streams, &live, later, Some(seen), &mut pass, 1).0,
        PartitionReplication::default(),
        "both were first found at the cutoff, not before it"
    );
    streams.record_partition_statuses(
        &[PartitionStatusEntry {
            namespace: reporting.inner(),
            created_revision: incarnation(reporting).1,
            view: 0,
            commit: 0,
            current_offset: 0,
            messages_count: 0,
            size_bytes: 0,
            segments_count: 1,
            in_sync_nodes: 0b11,
        }],
        0,
        2,
        later,
    );
    assert_eq!(
        full_pass(
            &streams,
            &live,
            later,
            Some(seen + Duration::from_millis(1)),
            &mut pass,
            1
        )
        .0,
        PartitionReplication {
            under_replicated: 1,
            below_write_quorum: 1,
        },
        "only the silent one"
    );
    assert_eq!(
        pass.unreported.since.len(),
        1,
        "the reported one is forgotten"
    );
    assert_eq!(pass.unreported.since.get(&incarnation(silent)), Some(&seen));
    assert!(pass.unreported.previous.is_empty());
}

/// A pass resumes by position. Partitions deleted ahead of its cursor are
/// not counted, ones created behind it wait for the next pass, and the end
/// of each pass forgets the deleted ones.
#[test]
fn given_a_delete_and_a_create_during_a_pass_when_counting_should_skip_the_deleted_and_count_the_created_next_pass()
 {
    const LIMIT: usize = 3;
    let seeded = Streams::default();
    seeded.seed_namespace(IggyNamespace::new(0, 0, 1), 1, 0);
    seeded.seed_namespace(IggyNamespace::new(1, 0, 2), 2, 0);
    seeded.seed_namespace(IggyNamespace::new(1, 1, 0), 3, 0);
    let mut inner = seeded.read(StreamsInner::clone);
    // Node 0 leads every partition and is down, so each one counts.
    let live = [false, false, true];
    let counted = |partitions| PartitionReplication {
        under_replicated: partitions,
        below_write_quorum: partitions,
    };
    let call = |inner: &StreamsInner, pass: &mut PartitionReplicationPass| {
        Streams::from(inner.clone()).partition_replication(&live, Instant::now(), None, pass, LIMIT)
    };
    let mut pass = PartitionReplicationPass::default();
    assert_eq!(call(&inner, &mut pass), None);
    assert_eq!(call(&inner, &mut pass), Some(counted(6)));
    assert_eq!(
        call(&inner, &mut pass),
        None,
        "the next pass stops inside topic 1/0"
    );

    let delete = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(0),
    };
    let deleted = crate::stm::lifecycle::apply_with_lifecycle_completion(
        &delete,
        &mut inner,
        IggyTimestamp::now(),
    );
    assert_eq!(deleted.code, 0);
    let create = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 2,
        },
        partitions: (0..2)
            .map(|partition_id| CreatedPartitionAssignment {
                initial_view: 0,
                partition_id,
                consensus_group_id: u64::from(partition_id) + 1,
                members: Vec::new(),
            })
            .collect(),
    };
    assert_eq!(
        StateHandler::apply(&create, &mut inner, IggyTimestamp::now()).code,
        0
    );

    assert_eq!(
        call(&inner, &mut pass),
        Some(counted(4)),
        "three before the delete, then topic 1/1, and not the two created behind"
    );
    assert!(pass.unreported.previous.is_empty());
    assert_eq!(
        pass.unreported.since.len(),
        4,
        "the deleted partitions the pass did not reach are forgotten"
    );
    assert_eq!(call(&inner, &mut pass), None);
    assert_eq!(
        call(&inner, &mut pass),
        Some(counted(5)),
        "the next pass counts the created ones"
    );
    assert_eq!(
        pass.unreported.since.len(),
        5,
        "and forgets the deleted one it counted before"
    );
}
