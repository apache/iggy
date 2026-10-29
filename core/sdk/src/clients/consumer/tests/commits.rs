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

//! The commit and shutdown tests of the consumer.

use std::convert::identity;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use futures::StreamExt;
use iggy_binary_protocol::PartitionContext;
use iggy_binary_protocol::codes::{
    DELETE_CONSUMER_OFFSET_CODE, FETCH_MESSAGES_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_common::{Checkpoint, IggyDuration, IggyError, NonZeroIggyDuration};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};

use super::{
    LevelReports, PARTITION_PRIMARY, POLL_RETRY_INTERVAL, POLLED_PARTITION, ROUTE_INCARNATION,
    builder_for, builder_on, committing_consumer, connect_to_three_replicas, deleted_partitions,
    next_message, reading_consumer, reply_of, script_primary, standalone, start_fetch,
    stored_offsets,
};
use crate::clients::consumer::{ClientCommitPolicy, CommitEvent, IggyConsumer, ReceivedMessage};
use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::reader::ReadError;
use crate::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use crate::links::OPERATION_TIMEOUT;
use crate::links::fake_cluster::{Answer, FakeCluster, TEST_BUDGET, endpoint, eventually};

/// The other partition of the topic of `connect_to_three_replicas`.
const OTHER_PARTITION: u32 = 0;
/// The offset the first store leaves on the server.
const STORED_OFFSET: u64 = 100;
/// An offset behind [`STORED_OFFSET`] that a second store asks for.
const LOWER_OFFSET: u64 = 10;
/// The message that a consumer yielded before the read under test.
const DELIVERED: u64 = 4;
/// A position of a partition under a real incarnation. Metadata revisions start at 1, so no
/// real partition has the incarnation 0 of an uncaptured context. An uncaptured offset write
/// takes the incarnation of the route, so only the route incarnation puts captured and
/// uncaptured writes of a partition on one lane, which the order checks need.
const CAPTURED: PartitionContext = PartitionContext {
    incarnation: ROUTE_INCARNATION,
    owner_generation: 0,
    metadata_op: 1,
};
/// The commits with which a replica answers the offset writes of one test, in order.
const FIRST_COMMIT: u64 = 1;
const SECOND_COMMIT: u64 = 2;
/// A policy whose final commit `shutdown` sends.
const EACH_MESSAGE: ClientCommitPolicy = ClientCommitPolicy {
    interval: None,
    on: Some(CommitEvent::EachMessage),
};
const COMMIT_INTERVAL: Duration = Duration::from_millis(100);
/// Long enough for the interval to tick more than once.
const AFTER_TICKS: Duration = COMMIT_INTERVAL.saturating_mul(3);
const NTH: u32 = 2;
/// The two replies that the commit events count over.
const FIRST_REPLY: [u64; 3] = [0, 1, 2];
const SECOND_REPLY: [u64; 2] = [3, 4];
/// The message whose handler fails.
const FAILING_OFFSET: u64 = 2;
/// The commit that each event queues at each message of [`FIRST_REPLY`] and [`SECOND_REPLY`].
/// [`SECOND_REPLY`] is already queued in the reader when the first batch ends.
const QUEUED_BY_EVENT: [(CommitEvent, [Option<u64>; 5]); 3] = [
    (
        CommitEvent::EachMessage,
        [Some(0), Some(1), Some(2), Some(3), Some(4)],
    ),
    (
        CommitEvent::EveryNthMessage(NTH),
        [Some(0), None, Some(2), None, Some(4)],
    ),
    (CommitEvent::EachBatch, [None, None, Some(2), None, Some(4)]),
];

#[tokio::test(start_paused = true)]
async fn default_consumer_does_not_commit_delivered_messages() {
    const BATCH: [u64; 2] = [10, 11];
    let (mut consumer, cluster) = reading_consumer(ClientCommitPolicy::default(), &[&BATCH]).await;
    for offset in BATCH {
        assert_eq!(
            next_message(&mut consumer).await.message.header.offset,
            offset
        );
    }
    assert!(consumer.pending_commits.is_empty());

    consumer.shutdown().await.unwrap();

    assert_eq!(
        consumer.get_last_consumed_offset(POLLED_PARTITION),
        Some(BATCH[1])
    );
    assert_eq!(consumer.get_last_stored_offset(POLLED_PARTITION), None);
    assert!(stored_offsets(&cluster).is_empty());
}

#[tokio::test(start_paused = true)]
async fn shutdown_should_not_move_a_higher_stored_offset_back_under_replay() {
    const CONSUMED: u64 = 9;
    const STORED: u64 = 19;
    let (mut consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], |builder| {
        builder.allow_replay().client_commit(EACH_MESSAGE)
    })
    .await;
    let progress = &consumer.state.progress;
    progress.deliver(&captured_checkpoint(&consumer, POLLED_PARTITION, CONSUMED));
    progress.record_commit(captured_checkpoint(&consumer, POLLED_PARTITION, STORED));

    consumer.shutdown().await.unwrap();

    assert!(stored_offsets(&cluster).is_empty());
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(STORED)
    );
}

#[tokio::test(start_paused = true)]
async fn store_offset_without_a_consumed_position_should_record_the_stored_offset() {
    const OFFSET: u64 = 7;
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;

    consumer
        .store_offset(OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();

    assert_eq!(stored_offsets(&cluster), [(POLLED_PARTITION, OFFSET)]);
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(OFFSET)
    );
}

#[tokio::test(start_paused = true)]
async fn store_offset_without_a_consumed_position_should_send_every_offset() {
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;

    consumer
        .store_offset(STORED_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();
    consumer
        .store_offset(LOWER_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();

    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, LOWER_OFFSET)
        ]
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(LOWER_OFFSET)
    );
}

#[tokio::test(start_paused = true)]
async fn store_offset_without_a_consumed_position_should_store_a_lower_offset_under_replay() {
    let (consumer, cluster) =
        consumer_without_reader(&[POLLED_PARTITION], IggyConsumerBuilder::allow_replay).await;

    consumer
        .store_offset(STORED_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();
    consumer
        .store_offset(LOWER_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();

    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, LOWER_OFFSET)
        ]
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(LOWER_OFFSET)
    );
}

#[tokio::test(start_paused = true)]
async fn commit_should_send_after_an_uncaptured_scalar_store() {
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;

    consumer
        .store_offset(STORED_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();
    consumer
        .commit(captured_checkpoint(
            &consumer,
            POLLED_PARTITION,
            LOWER_OFFSET,
        ))
        .await
        .unwrap();

    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, LOWER_OFFSET)
        ]
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(LOWER_OFFSET)
    );
}

/// A recreated partition starts again at 0, and the offset stored without a consumed position
/// can belong to its earlier incarnation, which the record cannot tell apart.
#[tokio::test(start_paused = true)]
async fn commit_should_send_offset_zero_after_one_stored_without_a_consumed_position() {
    const FIRST_OFFSET: u64 = 0;
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;

    consumer
        .store_offset(STORED_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();
    consumer
        .commit(captured_checkpoint(
            &consumer,
            POLLED_PARTITION,
            FIRST_OFFSET,
        ))
        .await
        .unwrap();

    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, FIRST_OFFSET)
        ]
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(FIRST_OFFSET)
    );
}

#[tokio::test(start_paused = true)]
async fn store_offset_without_a_consumed_position_should_not_compare_a_captured_store() {
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;

    consumer
        .commit(captured_checkpoint(
            &consumer,
            POLLED_PARTITION,
            STORED_OFFSET,
        ))
        .await
        .unwrap();
    consumer
        .store_offset(LOWER_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();

    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, LOWER_OFFSET)
        ]
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(LOWER_OFFSET)
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_should_store_the_captured_position_after_an_uncaptured_scalar_store() {
    let (mut consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], |builder| {
        builder.client_commit(EACH_MESSAGE)
    })
    .await;
    consumer
        .store_offset(STORED_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();
    consumer.state.progress.deliver(&captured_checkpoint(
        &consumer,
        POLLED_PARTITION,
        LOWER_OFFSET,
    ));

    consumer.shutdown().await.unwrap();

    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, LOWER_OFFSET)
        ]
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(LOWER_OFFSET)
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_deleted_offset_when_committing_the_same_position_should_send_again() {
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;
    let checkpoint = captured_checkpoint(&consumer, POLLED_PARTITION, STORED_OFFSET);
    consumer.commit(checkpoint).await.unwrap();

    consumer
        .delete_offset(Some(POLLED_PARTITION))
        .await
        .unwrap();

    assert_eq!(consumer.get_last_stored_offset(POLLED_PARTITION), None);
    consumer.commit(checkpoint).await.unwrap();
    assert_eq!(deleted_partitions(&cluster), [POLLED_PARTITION]);
    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, STORED_OFFSET)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_rejected_delete_when_committing_the_same_position_should_refresh_coverage() {
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;
    let checkpoint = captured_checkpoint(&consumer, POLLED_PARTITION, STORED_OFFSET);
    consumer.commit(checkpoint).await.unwrap();
    script_primary(
        &cluster,
        DELETE_CONSUMER_OFFSET_CODE,
        [Answer::CommitRejected(
            SECOND_COMMIT,
            IggyError::InvalidCommand,
        )],
    );

    assert_eq!(
        consumer.delete_offset(Some(POLLED_PARTITION)).await,
        Err(IggyError::InvalidCommand)
    );
    consumer.commit(checkpoint).await.unwrap();

    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(STORED_OFFSET)
    );
    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, STORED_OFFSET),
            (POLLED_PARTITION, STORED_OFFSET)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn given_an_uncertain_store_when_deleting_should_resolve_before_sending_the_delete() {
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], identity).await;
    script_primary(
        &cluster,
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Silence, Answer::Commit(FIRST_COMMIT)],
    );
    assert!(matches!(
        consumer
            .commit(captured_checkpoint(
                &consumer,
                POLLED_PARTITION,
                LOWER_OFFSET
            ))
            .await,
        Err(IggyError::OperationUncertain(_))
    ));

    consumer
        .delete_offset(Some(POLLED_PARTITION))
        .await
        .unwrap();

    let mutations: Vec<_> = cluster
        .codes(&endpoint(PARTITION_PRIMARY))
        .into_iter()
        .filter(|code| {
            matches!(
                *code,
                STORE_CONSUMER_OFFSET_CODE | DELETE_CONSUMER_OFFSET_CODE
            )
        })
        .collect();
    assert_eq!(
        mutations,
        [
            STORE_CONSUMER_OFFSET_CODE,
            STORE_CONSUMER_OFFSET_CODE,
            DELETE_CONSUMER_OFFSET_CODE
        ]
    );
    assert_eq!(deleted_partitions(&cluster), [POLLED_PARTITION]);
    assert_eq!(consumer.get_last_stored_offset(POLLED_PARTITION), None);
}

#[tokio::test(start_paused = true)]
async fn given_multiple_final_positions_when_shutdown_times_out_should_share_one_deadline() {
    const PARTITIONS: [u32; 2] = [OTHER_PARTITION, POLLED_PARTITION];
    let (mut consumer, cluster) =
        consumer_without_reader(&PARTITIONS, |builder| builder.client_commit(EACH_MESSAGE)).await;
    for partition in PARTITIONS {
        consumer
            .state
            .progress
            .deliver(&captured_checkpoint(&consumer, partition, LOWER_OFFSET));
    }
    script_primary(
        &cluster,
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Silence, Answer::Silence],
    );
    let started = Instant::now();

    consumer.shutdown().await.unwrap();

    assert!(started.elapsed() <= OPERATION_TIMEOUT);
    let stored = stored_offsets(&cluster);
    assert_eq!(
        stored.len(),
        1,
        "the first store spends the deadline: {stored:?}"
    );
    assert_eq!(stored[0].1, LOWER_OFFSET);
    for partition in PARTITIONS {
        assert_eq!(consumer.get_last_stored_offset(partition), None);
    }
}

#[tokio::test(start_paused = true)]
async fn given_an_inflight_background_store_when_shutdown_aborts_it_should_resolve_and_flush() {
    let (mut consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], |builder| {
        builder
            .client_commit(EACH_MESSAGE)
            .offset_drain_timeout(IggyDuration::new(POLL_RETRY_INTERVAL))
    })
    .await;
    consumer.state.progress.deliver(&captured_checkpoint(
        &consumer,
        POLLED_PARTITION,
        LOWER_OFFSET,
    ));
    script_primary(
        &cluster,
        STORE_CONSUMER_OFFSET_CODE,
        [
            Answer::Silence,
            Answer::Commit(FIRST_COMMIT),
            Answer::Commit(SECOND_COMMIT),
        ],
    );
    consumer.store_offset_task = Some(consumer.store_pending_commits_in_background());
    consumer.background_commit_task = Some(consumer.store_offsets_in_background(commit_interval()));
    eventually("background store reached the primary", || {
        cluster.count(STORE_CONSUMER_OFFSET_CODE) == 1
    })
    .await;

    consumer.shutdown().await.unwrap();

    let stores = cluster.received(STORE_CONSUMER_OFFSET_CODE);
    assert_eq!(stores.len(), 3);
    assert_eq!(stores[0].header.request, stores[1].header.request);
    assert_ne!(stores[1].header.request, stores[2].header.request);
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(LOWER_OFFSET)
    );
}

#[tokio::test]
async fn interval_commit_should_report_a_failed_store() {
    let recorder = LevelReports::errors();
    let _subscriber = tracing::subscriber::set_default(recorder.clone());
    let consumer = builder_for(standalone()).build();
    start_generations(&consumer, &[POLLED_PARTITION]);
    consumer
        .state
        .progress
        .deliver(&captured_checkpoint(&consumer, POLLED_PARTITION, DELIVERED));

    // The client is not connected, so every store fails.
    let store_task = consumer.store_pending_commits_in_background();
    let interval_task = consumer
        .store_offsets_in_background(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap());
    let reports = recorder.wait().await;
    interval_task.abort();
    store_task.abort();

    assert!(
        !reports.is_empty(),
        "a failed interval commit must be reported"
    );
    let failure = IggyError::Disconnected.to_string();
    assert!(
        reports.iter().all(|report| report.contains(&failure)),
        "{reports:?}"
    );
}

#[tokio::test]
async fn queued_commit_should_report_a_failed_store() {
    let recorder = LevelReports::errors();
    let _subscriber = tracing::subscriber::set_default(recorder.clone());
    let mut consumer = builder_for(standalone()).build();
    consumer.initialized = true;
    start_generations(&consumer, &[POLLED_PARTITION]);
    let task = consumer.store_pending_commits_in_background();

    // The client is not connected, so the store fails.
    consumer.send_store_offset(captured_checkpoint(&consumer, POLLED_PARTITION, DELIVERED));
    let reports = recorder.wait().await;
    task.abort();

    assert_eq!(
        reports.len(),
        1,
        "a failed queued commit must be reported once"
    );
    assert!(
        reports[0].contains(&IggyError::Disconnected.to_string()),
        "{reports:?}"
    );
}

#[test]
fn send_store_offset_should_keep_the_latest_offset_per_partition() {
    const FIRST_QUEUED: u64 = 5;
    const LATEST_QUEUED: u64 = 7;
    const OTHER_QUEUED: u64 = 3;
    let mut consumer = builder_for(standalone()).build();
    consumer.initialized = true;

    for (partition, offset) in [
        (POLLED_PARTITION, FIRST_QUEUED),
        (POLLED_PARTITION, LATEST_QUEUED),
        (OTHER_PARTITION, OTHER_QUEUED),
    ] {
        consumer.send_store_offset(Checkpoint::new(
            partition,
            offset,
            PartitionContext::default(),
        ));
    }

    let mut queued: Vec<(u32, u64)> = consumer
        .pending_commits
        .iter()
        .map(|entry| (*entry.key(), entry.value().offset()))
        .collect();
    queued.sort_unstable();
    assert_eq!(
        queued,
        [
            (OTHER_PARTITION, OTHER_QUEUED),
            (POLLED_PARTITION, LATEST_QUEUED)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_commit_event_when_the_stream_yields_should_queue_its_commit_at_the_yield() {
    for (event, expected) in QUEUED_BY_EVENT {
        let (mut consumer, _cluster) = event_consumer(event).await;
        let mut queued = Vec::with_capacity(expected.len());

        for offset in FIRST_REPLY.into_iter().chain(SECOND_REPLY) {
            assert_eq!(
                next_message(&mut consumer).await.message.header.offset,
                offset
            );
            queued.push(take_queued(&consumer.pending_commits));
        }

        assert_eq!(queued, expected, "{event:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn given_a_commit_event_when_consuming_should_queue_its_commit_after_the_handler_also_after_a_failure()
 {
    for (event, expected) in QUEUED_BY_EVENT {
        let (mut consumer, _cluster) = event_consumer(event).await;
        let (shutdown, shutdown_rx) = oneshot::channel();
        let handler = TakeQueuedCommits {
            pending_commits: Arc::clone(&consumer.pending_commits),
            last: SECOND_REPLY[1],
            taken: Mutex::default(),
            shutdown: Mutex::new(Some(shutdown)),
        };

        let consumed = timeout(
            TEST_BUDGET,
            consumer.consume_messages(&handler, shutdown_rx),
        )
        .await;

        assert_eq!(consumed, Ok(Ok(())), "{event:?}");
        let mut queued = handler.taken.into_inner().unwrap();
        queued.push(take_queued(&consumer.pending_commits));
        // Each handler sees the commit of the message before its own.
        let mut after_each_handler = vec![None];
        after_each_handler.extend(expected);
        assert_eq!(queued, after_each_handler, "{event:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn given_an_interval_when_the_stream_yields_part_of_a_batch_should_store_the_last_yielded_message()
 {
    const BATCH: [u64; 5] = [0, 1, 2, 3, 4];
    const LAST_YIELDED: u64 = 2;
    let (mut consumer, cluster) = reading_consumer(every_interval(), &[&BATCH]).await;
    for offset in 0..=LAST_YIELDED {
        assert_eq!(
            next_message(&mut consumer).await.message.header.offset,
            offset
        );
    }

    eventually("the interval stored the last yielded message", || {
        !stored_offsets(&cluster).is_empty()
    })
    .await;
    sleep(AFTER_TICKS).await;

    assert_eq!(
        stored_offsets(&cluster),
        [(POLLED_PARTITION, LAST_YIELDED)],
        "the end of the batch was never yielded"
    );
}

/// The consumer records a message as delivered when it yields it, so the interval can store a
/// message whose handler still runs.
#[tokio::test(start_paused = true)]
async fn given_an_interval_when_consuming_should_store_a_message_while_its_handler_runs() {
    const BATCH: [u64; 3] = [0, 1, 2];
    let (mut consumer, cluster) = reading_consumer(every_interval(), &[&BATCH]).await;
    let (shutdown, shutdown_rx) = oneshot::channel();
    let handler = AwaitStoreOfLast {
        cluster: Arc::clone(&cluster),
        last: BATCH[2],
        handled: Mutex::default(),
        shutdown: Mutex::new(Some(shutdown)),
    };

    let consumed = timeout(
        TEST_BUDGET,
        consumer.consume_messages(&handler, shutdown_rx),
    )
    .await;

    assert_eq!(consumed, Ok(Ok(())));
    assert_eq!(*handler.handled.lock().unwrap(), BATCH);
    assert_eq!(stored_offsets(&cluster), [(POLLED_PARTITION, BATCH[2])]);
}

/// The store task is the only sender, so a tick and an event queue into one slot per partition.
#[tokio::test(start_paused = true)]
async fn given_a_store_in_flight_when_an_interval_tick_and_an_event_queue_commits_should_send_only_the_latest_after_it()
 {
    const BATCH: [u64; 4] = [0, 1, 2, 3];
    let policy = ClientCommitPolicy {
        interval: Some(commit_interval()),
        on: Some(CommitEvent::EveryNthMessage(NTH)),
    };
    let (mut consumer, cluster) = reading_consumer(policy, &[&BATCH]).await;
    cluster.hold(STORE_CONSUMER_OFFSET_CODE);
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        BATCH[0]
    );
    eventually("the store task sent the commit of the event", || {
        cluster.count(STORE_CONSUMER_OFFSET_CODE) == 1
    })
    .await;
    for offset in &BATCH[1..] {
        assert_eq!(
            next_message(&mut consumer).await.message.header.offset,
            *offset
        );
    }

    sleep(AFTER_TICKS).await;

    let last = BATCH[BATCH.len() - 1];
    assert_eq!(
        consumer
            .pending_commits
            .get(&POLLED_PARTITION)
            .map(|queued| queued.offset()),
        Some(last),
        "the event skips the last message, so only the tick queued it"
    );
    assert_eq!(cluster.count(STORE_CONSUMER_OFFSET_CODE), 1);
    cluster.release(STORE_CONSUMER_OFFSET_CODE);
    eventually("the store task sent the queued commit", || {
        cluster.count(STORE_CONSUMER_OFFSET_CODE) == 2
    })
    .await;
    sleep(AFTER_TICKS).await;
    assert_eq!(
        stored_offsets(&cluster),
        [(POLLED_PARTITION, BATCH[0]), (POLLED_PARTITION, last)]
    );
}

#[tokio::test(start_paused = true)]
async fn given_on_read_when_shutting_down_after_a_partial_yield_should_move_the_offset_back_to_the_last_yielded_message()
 {
    const READ: [u64; 4] = [0, 1, 2, 3];
    const LAST_YIELDED: u64 = 1;
    let (mut consumer, cluster) = committing_consumer().await;
    script_primary(&cluster, FETCH_MESSAGES_CODE, [reply_of(&READ)]);
    for offset in 0..=LAST_YIELDED {
        assert_eq!(
            next_message(&mut consumer).await.message.header.offset,
            offset
        );
    }
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(READ[3]),
        "the server stored the end of the read"
    );

    consumer.shutdown().await.unwrap();

    assert_eq!(stored_offsets(&cluster), [(POLLED_PARTITION, LAST_YIELDED)]);
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(LAST_YIELDED)
    );
}

#[tokio::test(start_paused = true)]
async fn given_on_read_when_shutting_down_before_a_yield_should_rewind_before_the_queued_batch() {
    const PARTITION_START: u64 = 0;
    const LATER_START: u64 = 5;
    for first in [LATER_START, PARTITION_START] {
        let (mut consumer, cluster) = committing_consumer().await;
        script_primary(
            &cluster,
            FETCH_MESSAGES_CODE,
            [reply_of(&[first, first + 1])],
        );
        start_fetch(&mut consumer, &cluster).await;

        consumer.shutdown().await.unwrap();

        match first.checked_sub(1) {
            Some(before) => {
                assert_eq!(stored_offsets(&cluster), [(POLLED_PARTITION, before)]);
                assert!(deleted_partitions(&cluster).is_empty());
            }
            None => {
                assert!(stored_offsets(&cluster).is_empty());
                assert_eq!(
                    deleted_partitions(&cluster),
                    [POLLED_PARTITION],
                    "no offset comes before the start of the partition"
                );
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn given_a_lost_committing_fetch_when_shutting_down_should_skip_its_partition_and_return_the_blocked_lane()
 {
    let (mut consumer, cluster) = committing_consumer().await;
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [
            reply_of(&[DELIVERED]),
            Answer::Refuse(IggyError::TransientNotCommitted),
        ],
    );
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        DELIVERED
    );

    let lost = timeout(TEST_BUDGET, consumer.next())
        .await
        .unwrap()
        .unwrap();
    let operation = cluster.received(FETCH_MESSAGES_CODE)[1].header.request;
    assert_eq!(
        lost.unwrap_err(),
        ReadError::new(
            Some(POLLED_PARTITION),
            IggyError::FetchProgressUncertain(operation)
        )
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        None,
        "the server can have committed the batch of the lost reply"
    );

    let shut_down = consumer.shutdown().await;

    assert_eq!(
        shut_down,
        Err(IggyError::PartitionLaneBlocked(POLLED_PARTITION, operation))
    );
    assert!(stored_offsets(&cluster).is_empty());
    assert!(deleted_partitions(&cluster).is_empty());
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_that_outlasts_the_drain_timeout_when_shutting_down_should_wait_for_it_and_store_the_delivered_offset_again()
 {
    const REPLY_DELAY: Duration = Duration::from_millis(100);
    let (mut consumer, cluster) = committing_consumer().await;
    consumer.offset_drain_timeout = IggyDuration::new(POLL_RETRY_INTERVAL);
    assert!(REPLY_DELAY > consumer.offset_drain_timeout.get_duration());
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&[DELIVERED]), reply_of(&[DELIVERED + 1])],
    );
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        DELIVERED
    );
    cluster.hold(FETCH_MESSAGES_CODE);
    start_fetch(&mut consumer, &cluster).await;
    let started = Instant::now();

    let (shut_down, ()) = tokio::join!(consumer.shutdown(), async {
        sleep(REPLY_DELAY).await;
        cluster.release(FETCH_MESSAGES_CODE);
    });

    shut_down.unwrap();
    assert!(started.elapsed() >= REPLY_DELAY);
    assert_eq!(
        stored_offsets(&cluster),
        [(POLLED_PARTITION, DELIVERED)],
        "the server committed a batch that the application never got"
    );
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(DELIVERED)
    );
}

/// Takes, in the handler of each message, the commit that the policy queued before it. It fails
/// at [`FAILING_OFFSET`] and stops the consumption after `last`.
struct TakeQueuedCommits {
    pending_commits: Arc<DashMap<u32, Checkpoint>>,
    last: u64,
    taken: Mutex<Vec<Option<u64>>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl MessageConsumer for TakeQueuedCommits {
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        self.taken
            .lock()
            .unwrap()
            .push(take_queued(&self.pending_commits));
        let offset = received.message.header.offset;
        if offset == self.last {
            self.shutdown
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
        }
        if offset == FAILING_OFFSET {
            return Err(IggyError::Error);
        }
        Ok(())
    }
}

/// Records each offset. In the handler of `last`, it waits until `cluster` stored that message,
/// and then stops the consumption.
struct AwaitStoreOfLast {
    cluster: Arc<FakeCluster>,
    last: u64,
    handled: Mutex<Vec<u64>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl MessageConsumer for AwaitStoreOfLast {
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        let offset = received.message.header.offset;
        self.handled.lock().unwrap().push(offset);
        if offset == self.last {
            let stored = (received.partition_id, offset);
            eventually(
                "the interval stored the message of the running handler",
                || stored_offsets(&self.cluster).contains(&stored),
            )
            .await;
            self.shutdown
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
        }
        Ok(())
    }
}

/// A consumer of `partitions` on a client of `connect_to_three_replicas`, without a reader. Each
/// partition has a generation, as a reader of it gives it, so the consumer commits its own
/// checkpoints. `partitions` is sorted.
async fn consumer_without_reader(
    partitions: &[u32],
    configure: impl FnOnce(IggyConsumerBuilder) -> IggyConsumerBuilder,
) -> (IggyConsumer, Arc<FakeCluster>) {
    let (client, cluster) = connect_to_three_replicas().await;
    let consumer =
        configure(builder_on(client, standalone()).partitions(partitions.iter().copied())).build();
    start_generations(&consumer, partitions);
    (consumer, cluster)
}

/// Starts a generation of each of the sorted `partitions`, as a reader of them does.
fn start_generations(consumer: &IggyConsumer, partitions: &[u32]) {
    consumer.state.progress.assign(partitions, &[]);
}

/// A checkpoint at `offset` that `consumer` delivered in the current generation of `partition`.
fn captured_checkpoint(consumer: &IggyConsumer, partition: u32, offset: u64) -> Checkpoint {
    let progress = &consumer.state.progress;
    let generation = progress.generation(partition).unwrap();
    Checkpoint::stamped(
        partition,
        offset,
        CAPTURED,
        Some(progress.stamp(generation)),
    )
}

/// A consumer that queues on `event`, with [`FIRST_REPLY`] and [`SECOND_REPLY`] in its reader.
/// Its store task is stopped, so each commit that the policy queues stays in `pending_commits`.
async fn event_consumer(event: CommitEvent) -> (IggyConsumer, Arc<FakeCluster>) {
    let policy = ClientCommitPolicy {
        interval: None,
        on: Some(event),
    };
    let (mut consumer, cluster) = reading_consumer(policy, &[&FIRST_REPLY, &SECOND_REPLY]).await;
    consumer.store_offset_task.take().unwrap().abort();
    (consumer, cluster)
}

fn every_interval() -> ClientCommitPolicy {
    ClientCommitPolicy {
        interval: Some(commit_interval()),
        on: None,
    }
}

fn commit_interval() -> NonZeroIggyDuration {
    NonZeroIggyDuration::new(COMMIT_INTERVAL).unwrap()
}

/// The offset of the commit that the policy queued for `POLLED_PARTITION`.
fn take_queued(pending_commits: &DashMap<u32, Checkpoint>) -> Option<u64> {
    pending_commits
        .remove(&POLLED_PARTITION)
        .map(|(_, checkpoint)| checkpoint.offset())
}
