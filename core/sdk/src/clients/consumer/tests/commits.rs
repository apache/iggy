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

use futures::StreamExt;
use iggy_binary_protocol::codes::{
    CANCEL_DEFERRED_REQUEST_CODE, DELETE_CONSUMER_OFFSET_CODE, FETCH_MESSAGES_CODE,
    POLL_MESSAGES_DEFERRED_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::requests::messages::FetchMessagesRequest;
use iggy_binary_protocol::{PartitionContext, WireDecode, WirePollingStrategy};
use iggy_common::{
    AutoCommit, Checkpoint, CommitAfter, CommitEvery, CommitPolicy, IggyDuration, IggyError,
    NonZeroIggyDuration, PollOptions, PollingStrategy,
};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};

use super::{
    PARTITION_PRIMARY, POLL_RETRY_INTERVAL, POLLED_PARTITION, ROUTE_INCARNATION, answer_lookups,
    builder_on, committing_consumer, connect_to_three_replicas, deleted_partitions, next_message,
    poll_reply_of, reading_consumer, reply_of, script_primary, standalone, start_fetch,
    stored_offsets,
};
use crate::clients::consumer::{IggyConsumer, IggyConsumerState, ReceivedMessage};
use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::reader::ReadError;
use crate::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use crate::links::fake_cluster::{Answer, FakeCluster, TEST_BUDGET, endpoint, eventually};

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
/// A policy that stores each message that it counts.
const EACH_MESSAGE: CommitPolicy = CommitPolicy::Auto(AutoCommit {
    interval: None,
    every: Some(CommitEvery::Message),
    after: CommitAfter::Delivery,
});
/// Longer than any test, so only the close or the shutdown stores.
const NEVER: Duration = Duration::from_secs(24 * 60 * 60);
/// The batch that the counting tests yield.
const BATCH: [u64; 3] = [0, 1, 2];

#[tokio::test(start_paused = true)]
async fn default_consumer_does_not_commit_delivered_messages() {
    let (mut consumer, cluster) = reading_consumer(CommitPolicy::Manual, &[&BATCH]).await;
    for offset in BATCH {
        assert_eq!(
            next_message(&mut consumer).await.message.header.offset,
            offset
        );
    }

    consumer.shutdown().await.unwrap();

    assert_eq!(
        consumer.get_last_consumed_offset(POLLED_PARTITION),
        Some(BATCH[2])
    );
    assert_eq!(consumer.get_last_stored_offset(POLLED_PARTITION), None);
    assert!(stored_offsets(&cluster).is_empty());
}

#[tokio::test(start_paused = true)]
async fn flush_should_not_move_a_higher_stored_offset_back_under_replay() {
    const CONSUMED: u64 = 9;
    const STORED: u64 = 19;
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], |builder| {
        builder
            .allow_replay()
            .poll_options(auto_options(EACH_MESSAGE))
    })
    .await;
    let progress = &consumer.state.progress;
    progress.count(
        captured_checkpoint(&consumer, POLLED_PARTITION, CONSUMED),
        1,
        false,
    );
    progress.record_commit(captured_checkpoint(&consumer, POLLED_PARTITION, STORED));

    progress.flush_counted().await.unwrap();

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
async fn flush_should_store_the_captured_position_after_an_uncaptured_scalar_store() {
    let (consumer, cluster) = consumer_without_reader(&[POLLED_PARTITION], |builder| {
        builder.poll_options(auto_options(EACH_MESSAGE))
    })
    .await;
    consumer
        .store_offset(STORED_OFFSET, Some(POLLED_PARTITION))
        .await
        .unwrap();
    consumer.state.progress.count(
        captured_checkpoint(&consumer, POLLED_PARTITION, LOWER_OFFSET),
        1,
        false,
    );

    consumer.state.progress.flush_counted().await.unwrap();

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
async fn given_auto_commit_when_the_stream_yields_should_count_each_message_as_its_policy_says() {
    for (after, counted) in [
        (CommitAfter::Delivery, Some(BATCH[2])),
        (CommitAfter::Processing, Some(BATCH[1])),
        (CommitAfter::Marked, None),
    ] {
        let (mut consumer, _cluster) = reading_consumer(auto(after), &[&BATCH]).await;

        for offset in BATCH {
            assert_eq!(
                next_message(&mut consumer).await.message.header.offset,
                offset
            );
        }

        assert_eq!(
            counted_offset(&consumer.state()),
            counted,
            "{after:?}: delivery counts at the yield, processing when the stream is polled again"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn given_auto_commit_when_consuming_should_count_before_the_handler_on_delivery_and_after_it_on_processing()
 {
    for (after, counted_in_handler) in [
        (
            CommitAfter::Delivery,
            [Some(BATCH[0]), Some(BATCH[1]), Some(BATCH[2])],
        ),
        (
            CommitAfter::Processing,
            [None, Some(BATCH[0]), Some(BATCH[1])],
        ),
    ] {
        let (mut consumer, _cluster) = reading_consumer(auto(after), &[&BATCH]).await;
        let (shutdown, receiver) = oneshot::channel();
        let handler = RecordCounted {
            state: consumer.state(),
            last: BATCH[2],
            counted: Mutex::default(),
            shutdown: Mutex::new(Some(shutdown)),
        };

        timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            *handler.counted.lock().unwrap(),
            counted_in_handler,
            "{after:?}"
        );
        assert_eq!(
            counted_offset(&consumer.state()),
            Some(BATCH[2]),
            "{after:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn given_auto_commit_after_processing_when_a_handler_fails_should_end_the_consumption_without_counting_its_message()
 {
    let (mut consumer, _cluster) = reading_consumer(auto(CommitAfter::Processing), &[&BATCH]).await;
    let (_shutdown, receiver) = oneshot::channel();

    let consumed = timeout(
        TEST_BUDGET,
        consumer.consume_messages(&FailAt(BATCH[1]), receiver),
    )
    .await
    .unwrap();

    assert_eq!(
        consumed,
        Err(ReadError::new(Some(POLLED_PARTITION), IggyError::Error))
    );
    assert_eq!(
        counted_offset(&consumer.state()),
        Some(BATCH[0]),
        "the failed message stays uncounted, so the next run reads it again"
    );
}

#[tokio::test(start_paused = true)]
async fn given_marked_auto_commit_when_messages_are_marked_should_count_only_the_marks_forward() {
    let (mut consumer, _cluster) = reading_consumer(auto(CommitAfter::Marked), &[&BATCH]).await;
    let mut checkpoints = Vec::new();
    for _ in BATCH {
        checkpoints.push(next_message(&mut consumer).await.checkpoint());
    }
    assert_eq!(
        counted_offset(&consumer.state()),
        None,
        "a yield counts nothing"
    );

    consumer.mark(checkpoints[1]).unwrap();
    consumer.state().mark(checkpoints[0]).unwrap();

    assert_eq!(counted_offset(&consumer.state()), Some(BATCH[1]));
}

#[tokio::test(start_paused = true)]
async fn given_manual_or_on_read_when_marking_should_refuse_it() {
    for commit in [CommitPolicy::Manual, CommitPolicy::OnRead] {
        let (consumer, _cluster) = consumer_without_reader(&[POLLED_PARTITION], |builder| {
            builder.poll_options(auto_options(commit))
        })
        .await;
        let checkpoint = captured_checkpoint(&consumer, POLLED_PARTITION, DELIVERED);

        assert_eq!(
            consumer.mark(checkpoint),
            Err(IggyError::InvalidCommand),
            "{commit:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn given_auto_commit_after_processing_when_shutting_down_should_store_the_last_yielded_message()
 {
    let (mut consumer, cluster) = reading_consumer(auto(CommitAfter::Processing), &[&BATCH]).await;
    for _ in BATCH {
        next_message(&mut consumer).await;
    }

    consumer.shutdown().await.unwrap();

    assert_eq!(stored_offsets(&cluster), [(POLLED_PARTITION, BATCH[2])]);
    assert_eq!(
        consumer.get_last_stored_offset(POLLED_PARTITION),
        Some(BATCH[2])
    );
}

#[tokio::test(start_paused = true)]
async fn given_auto_commit_on_each_message_when_the_stream_yields_should_store_the_yielded_message()
{
    let (mut consumer, cluster) = reading_consumer(EACH_MESSAGE, &[&BATCH]).await;

    let yielded = next_message(&mut consumer).await;

    eventually("the store task stores the counted message", || {
        stored_offsets(&cluster) == [(POLLED_PARTITION, yielded.message.header.offset)]
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn given_on_read_when_shutting_down_after_a_partial_yield_should_move_the_offset_back_to_the_last_yielded_message()
 {
    const READ: [u64; 4] = [0, 1, 2, 3];
    const LAST_YIELDED: u64 = 1;
    let (mut consumer, cluster) = committing_consumer(READ[0]).await;
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

/// A close cancels a committing fetch that waits on the server, and the one reply to the cancel
/// tells whether the server stored.
#[tokio::test(start_paused = true)]
async fn given_on_read_when_shutting_down_with_a_parked_fetch_should_cancel_it_and_rewind_what_it_stored()
 {
    const FIRST: u64 = 5;
    const MAX_WAIT: Duration = Duration::from_secs(30);
    for (case, answer, stored) in [
        (
            "cancel won",
            Answer::Refuse(IggyError::TransientNotAccepted),
            Vec::new(),
        ),
        (
            "store won",
            reply_of(&[FIRST, FIRST + 1]),
            vec![(POLLED_PARTITION, FIRST - 1)],
        ),
    ] {
        let (client, cluster) = connect_to_three_replicas().await;
        answer_lookups(&cluster);
        script_primary(
            &cluster,
            POLL_MESSAGES_DEFERRED_CODE,
            [poll_reply_of(&[FIRST])],
        );
        let mut consumer = builder_on(client, standalone())
            .partitions([POLLED_PARTITION])
            .poll_options(PollOptions {
                commit: CommitPolicy::OnRead,
                ..PollOptions::wait(IggyDuration::from(MAX_WAIT))
            })
            .build();
        consumer.init().await.unwrap();
        script_primary(&cluster, FETCH_MESSAGES_CODE, [answer]);
        cluster.hold(FETCH_MESSAGES_CODE);
        start_fetch(&mut consumer, &cluster).await;
        let started = Instant::now();
        let shutdown = tokio::spawn(async move { consumer.shutdown().await });

        eventually("the close cancelled the parked fetch", || {
            cluster.count(CANCEL_DEFERRED_REQUEST_CODE) == 1
        })
        .await;
        cluster.release(FETCH_MESSAGES_CODE);
        shutdown.await.unwrap().unwrap();

        assert!(
            started.elapsed() < MAX_WAIT,
            "{case}: the close took {:?}",
            started.elapsed()
        );
        assert_eq!(stored_offsets(&cluster), stored, "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn given_on_read_when_the_application_stores_an_offset_ahead_should_read_on_after_it_and_keep_it_at_shutdown()
 {
    const READ: [u64; 2] = [0, 1];
    const SKIPPED_TO: u64 = 7;
    let (mut consumer, cluster) = committing_consumer(READ[0]).await;
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&READ), reply_of(&[SKIPPED_TO + 1, SKIPPED_TO + 2])],
    );
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        READ[0]
    );

    consumer
        .store_offset(SKIPPED_TO, Some(POLLED_PARTITION))
        .await
        .unwrap();

    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        SKIPPED_TO + 1,
        "the messages up to the stored offset are never delivered"
    );
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    let after_store = FetchMessagesRequest::decode_from(&fetches.last().unwrap().body).unwrap();
    assert_eq!(
        after_store.targets[0].strategy,
        WirePollingStrategy::offset(SKIPPED_TO + 1),
        "the read after the store starts after the stored offset"
    );
    consumer.shutdown().await.unwrap();
    assert_eq!(
        stored_offsets(&cluster),
        [
            (POLLED_PARTITION, SKIPPED_TO),
            (POLLED_PARTITION, SKIPPED_TO + 1)
        ],
        "the close moves the offset back to the last yielded message, after the store"
    );
}

/// A restart resumes at the sought offset, because the seek stored the one before it.
#[tokio::test(start_paused = true)]
async fn given_on_read_when_seeking_to_an_offset_should_store_the_offset_before_it_and_read_from_it()
 {
    const SOUGHT: u64 = 7;
    let (mut consumer, cluster) = committing_consumer(0).await;
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&[0, 1]), reply_of(&[SOUGHT, SOUGHT + 1])],
    );
    assert_eq!(next_message(&mut consumer).await.message.header.offset, 0);

    consumer
        .seek(POLLED_PARTITION, PollingStrategy::offset(SOUGHT))
        .await
        .unwrap();

    assert_eq!(
        stored_offsets(&cluster),
        [(POLLED_PARTITION, SOUGHT - 1)],
        "the seek stores the offset before its start"
    );
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        SOUGHT,
        "the rest of the held batch is dropped"
    );
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    let after_seek = FetchMessagesRequest::decode_from(&fetches.last().unwrap().body).unwrap();
    assert_eq!(
        after_seek.targets[0].strategy,
        WirePollingStrategy::offset(SOUGHT),
        "the read after the seek starts at the sought offset"
    );
    consumer.shutdown().await.unwrap();
    assert_eq!(
        stored_offsets(&cluster),
        [(POLLED_PARTITION, SOUGHT - 1), (POLLED_PARTITION, SOUGHT)],
        "the close moves the offset back to the last yielded message"
    );
}

#[tokio::test(start_paused = true)]
async fn given_on_read_when_a_seek_store_is_refused_should_fail_the_seek_and_keep_the_position() {
    let (mut consumer, cluster) = committing_consumer(0).await;
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&[0, 1]), reply_of(&[0])],
    );
    assert_eq!(next_message(&mut consumer).await.message.header.offset, 0);

    script_primary(
        &cluster,
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Refuse(IggyError::Unauthorized)],
    );
    assert_eq!(
        consumer
            .seek(POLLED_PARTITION, PollingStrategy::offset(7))
            .await,
        Err(IggyError::Unauthorized),
        "a refused store fails the seek"
    );
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        1,
        "a failed seek keeps the held batch and the position"
    );

    consumer
        .seek(POLLED_PARTITION, PollingStrategy::first())
        .await
        .unwrap();
    assert_eq!(
        stored_offsets(&cluster).len(),
        1,
        "a seek to the first offset stores nothing"
    );
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        0,
        "the read after the seek starts at the first offset"
    );
}

#[tokio::test(start_paused = true)]
async fn given_on_read_when_shutting_down_before_a_yield_should_rewind_before_the_queued_batch() {
    const PARTITION_START: u64 = 0;
    const LATER_START: u64 = 5;
    for first in [LATER_START, PARTITION_START] {
        let (mut consumer, cluster) = committing_consumer(first).await;
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

/// The server can have committed the batch of a lost reply, so the shutdown stores the
/// delivered offset again, and the lost reply ends nothing.
#[tokio::test(start_paused = true)]
async fn given_a_lost_committing_fetch_when_shutting_down_should_store_the_delivered_offset_again()
{
    let (mut consumer, cluster) = committing_consumer(DELIVERED).await;
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
    let read_on = timeout(TEST_BUDGET, consumer.next()).await;
    assert!(read_on.is_err(), "a lost reply is no error: {read_on:?}");
    assert!(cluster.count(FETCH_MESSAGES_CODE) >= 2);

    consumer.shutdown().await.unwrap();

    assert_eq!(stored_offsets(&cluster), [(POLLED_PARTITION, DELIVERED)]);
    assert!(deleted_partitions(&cluster).is_empty());
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_that_outlasts_the_drain_timeout_when_shutting_down_should_wait_for_it_and_store_the_delivered_offset_again()
 {
    const REPLY_DELAY: Duration = Duration::from_millis(100);
    let (mut consumer, cluster) = committing_consumer(DELIVERED).await;
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

/// Records the counted offset of its partition when its handler starts, and stops the
/// consumption after `last`.
struct RecordCounted {
    state: IggyConsumerState,
    last: u64,
    counted: Mutex<Vec<Option<u64>>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl MessageConsumer for RecordCounted {
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        self.counted
            .lock()
            .unwrap()
            .push(counted_offset(&self.state));
        if received.message.header.offset == self.last {
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

/// Fails the handler of the message at its offset.
struct FailAt(u64);

impl MessageConsumer for FailAt {
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        if received.message.header.offset == self.0 {
            return Err(IggyError::Error);
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

/// `CommitPolicy::Auto` that counts `after`, and stores only at the close or the shutdown.
fn auto(after: CommitAfter) -> CommitPolicy {
    CommitPolicy::Auto(AutoCommit {
        interval: NonZeroIggyDuration::new(NEVER).ok(),
        every: None,
        after,
    })
}

/// The default options of a consumer with `commit`.
fn auto_options(commit: CommitPolicy) -> PollOptions {
    PollOptions {
        commit,
        ..PollOptions::wait(IggyDuration::ONE_SECOND)
    }
}

/// The offset of the latest counted message of `POLLED_PARTITION`.
fn counted_offset(state: &IggyConsumerState) -> Option<u64> {
    state
        .progress
        .last_counted(POLLED_PARTITION)
        .map(|checkpoint| checkpoint.offset())
}
