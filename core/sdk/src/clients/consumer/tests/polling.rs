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

//! The polling and lifecycle tests of the consumer.

use std::convert::identity;
use std::iter::repeat_n;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures::{Stream, StreamExt};
use iggy_binary_protocol::codes::{
    FETCH_MESSAGES_CODE, GET_CONSUMER_GROUP_CODE, GET_STREAM_CODE, JOIN_CONSUMER_GROUP_CODE,
    POLL_MESSAGES_DEFERRED_CODE, STORE_CONSUMER_OFFSET_CODE, SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::messages::{DeferredPollMessagesRequest, FetchMessagesRequest};
use iggy_binary_protocol::{PartitionContext, WireDecode};
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{
    Aes256GcmEncryptor, BinaryTransport, Checkpoint, Client, Consumer, ConsumerGroupClient,
    ConsumerOffsetClient, DiagnosticEvent, EncryptorKind, Identifier, IggyError,
    NonZeroIggyDuration, PollOptions, PollingKind, PollingStrategy, ServerCommitPolicy,
    TcpClientReconnectionConfig, group_cache_key,
};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};

use super::{
    LevelReports, POLL_RETRY_INTERVAL, POLL_TIMEOUT, POLLED_PARTITION, answer_lookups, assignment,
    builder_for, builder_on, committing_consumer, connect_to_three_replicas,
    connect_to_three_replicas_with, existing_consumer_group, fetch_of, group, next_message,
    reading_consumer, reply_of, script_primary, standalone,
};
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::consumer::{ClientCommitPolicy, CommitEvent, IggyConsumer, ReceivedMessage};
use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::reader::ReadError;
use crate::clients::reader::fixtures::{refused, served};
use crate::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use crate::links::fake_cluster::{Answer, FakeCluster, TEST_BUDGET, endpoint, eventually};
use crate::links::{OPERATION_TIMEOUT, ReconnectPolicy};
use crate::tcp::tcp_client::TcpClient;

const METADATA_PRIMARY: u8 = 0;
/// The other partition of the topic of `connect_to_three_replicas`.
const OTHER_PARTITION: u32 = 0;
const OTHER_GROUP: u32 = 2;
const FIRST_GENERATION: u64 = 1;
const NEXT_GENERATION: u64 = 2;
/// More assignment refreshes than a test lives through.
const GROUP_REFRESHES: usize = 64;
/// The commit of each scripted metadata reply.
const COMMIT_OP: u64 = 1;
/// A prefetch below one batch, which leaves the reader one slot.
const ONE_SLOT: u32 = 1;
const OFFSET: u64 = 5;
/// The message that the consumer yielded before the read under test.
const DELIVERED: u64 = 11;
const BATCH: [u64; 3] = [10, 11, 12];
const STORED_OFFSET: u64 = 4;
const CONTINUED_AT: u64 = 3;
const NTH: u32 = 2;
const ENCRYPTION_KEY: [u8; 32] = [1; 32];
/// The stream lookups of an `init` that stops before any request, and of one that reaches the
/// server.
const NO_LOOKUP: usize = 0;
const ONE_LOOKUP: usize = 1;

/// Fails the test on any message.
struct RejectMessages;

impl MessageConsumer for RejectMessages {
    async fn consume(&self, _: ReceivedMessage) -> Result<(), IggyError> {
        panic!("no message may reach the handler")
    }
}

/// Records the offset of each message. Built with [`Self::until`], it stops the consumption
/// after the message at `last`.
#[derive(Default)]
struct RecordMessages {
    offsets: Mutex<Vec<u64>>,
    stop_after: Mutex<Option<(u64, oneshot::Sender<()>)>>,
}

impl RecordMessages {
    fn until(last: u64) -> (Self, oneshot::Receiver<()>) {
        let (shutdown, receiver) = oneshot::channel();
        let handler = Self {
            offsets: Mutex::default(),
            stop_after: Mutex::new(Some((last, shutdown))),
        };
        (handler, receiver)
    }

    fn offsets(&self) -> Vec<u64> {
        self.offsets.lock().unwrap().clone()
    }
}

impl MessageConsumer for RecordMessages {
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        let offset = received.message.header.offset;
        self.offsets.lock().unwrap().push(offset);
        let mut stop_after = self.stop_after.lock().unwrap();
        if stop_after.as_ref().is_some_and(|(last, _)| *last == offset) {
            let (_, shutdown) = stop_after.take().unwrap();
            shutdown.send(()).unwrap();
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn given_a_blocked_poll_when_consuming_callbacks_should_return_the_retained_operation() {
    let (mut consumer, cluster) = committing_consumer().await;
    script_primary(
        &cluster,
        STORE_CONSUMER_OFFSET_CODE,
        [
            Answer::Refuse(IggyError::TransientNotCommitted),
            Answer::Refuse(IggyError::RequestTooOld),
        ],
    );
    let stored = consumer
        .client
        .read()
        .await
        .store_consumer_offset(
            &standalone(),
            consumer.stream(),
            consumer.topic(),
            Some(POLLED_PARTITION),
            STORED_OFFSET,
        )
        .await;
    let store = cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
        .header
        .request;
    assert_eq!(stored, Err(IggyError::OperationUncertain(store)));
    let (_shutdown, receiver) = oneshot::channel();

    let blocked = timeout(
        TEST_BUDGET,
        consumer.consume_messages(&RejectMessages, receiver),
    )
    .await
    .unwrap();

    assert_eq!(
        blocked,
        Err(ReadError::new(
            Some(POLLED_PARTITION),
            IggyError::PartitionLaneBlocked(POLLED_PARTITION, store)
        ))
    );

    let (mut consumer, cluster) = committing_consumer().await;
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [
            reply_of(&[DELIVERED]),
            Answer::Refuse(IggyError::TransientNotCommitted),
        ],
    );
    let handler = RecordMessages::default();
    let (_shutdown, receiver) = oneshot::channel();

    let lost = timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver))
        .await
        .unwrap();

    let operation = cluster.received(FETCH_MESSAGES_CODE)[1].header.request;
    assert_eq!(
        lost,
        Err(ReadError::new(
            Some(POLLED_PARTITION),
            IggyError::FetchProgressUncertain(operation)
        ))
    );
    assert_eq!(handler.offsets(), [DELIVERED]);
}

/// `HistoryUnavailable` stops its partition until a `seek`, which the loop cannot make.
#[tokio::test(start_paused = true)]
async fn given_history_unavailable_when_consuming_callbacks_should_end_with_its_partition() {
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [
            reply_of(&BATCH),
            fetch_of(&[refused(POLLED_PARTITION, &IggyError::HistoryUnavailable)]),
        ],
    );
    let mut consumer = builder_on(client, standalone())
        .partitions([POLLED_PARTITION])
        .polling_strategy(PollingStrategy::first())
        .build();
    consumer.init().await.unwrap();
    let handler = RecordMessages::default();
    let (_shutdown, receiver) = oneshot::channel();

    let consumed = timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver)).await;

    assert_eq!(
        consumed,
        Ok(Err(ReadError::new(
            Some(POLLED_PARTITION),
            IggyError::HistoryUnavailable
        )))
    );
    assert_eq!(handler.offsets(), BATCH);
}

/// The reader reads a message that does not decode again until a `seek` to its skip, which the
/// loop cannot make.
#[tokio::test(start_paused = true)]
async fn given_a_message_that_does_not_decode_when_consuming_callbacks_should_end_with_its_skip() {
    let undecodable = BATCH[BATCH.len() - 1] + 1;
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    let (mut header, _) = served(POLLED_PARTITION, &[undecodable], undecodable + 1);
    header.records_len = 1;
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&BATCH), fetch_of(&[(header, vec![0])])],
    );
    let mut consumer = builder_on(client, standalone())
        .partitions([POLLED_PARTITION])
        .polling_strategy(PollingStrategy::first())
        .build();
    consumer.init().await.unwrap();
    let handler = RecordMessages::default();
    let (_shutdown, receiver) = oneshot::channel();

    let consumed = timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver)).await;

    let skip = PollingStrategy::continuation(
        POLLED_PARTITION,
        PollingKind::Offset,
        undecodable + 1,
        PartitionContext::default(),
    );
    assert_eq!(
        consumed,
        Ok(Err(ReadError {
            partition_id: Some(POLLED_PARTITION),
            error: IggyError::InvalidMessagesCount,
            skip: Some(skip),
        }))
    );
    assert_eq!(handler.offsets(), BATCH);
}

/// With `OnRead`, the server stores the offset of a read whose reply does not decode. A read from
/// `Next` then has no known start, so its partition stays stopped until a `seek`, which the loop
/// cannot make.
#[tokio::test(start_paused = true)]
async fn given_an_on_read_poll_from_next_that_does_not_decode_when_consuming_callbacks_should_end_with_its_partition()
 {
    let (mut consumer, cluster) = committing_consumer().await;
    // A record above `max_bytes` makes the reader poll it alone.
    let oversized = refused(POLLED_PARTITION, &IggyError::InvalidSizeBytes);
    script_primary(&cluster, FETCH_MESSAGES_CODE, [fetch_of(&[oversized])]);
    // Shorter than the reply header.
    script_primary(
        &cluster,
        POLL_MESSAGES_DEFERRED_CODE,
        [Answer::Body(vec![0])],
    );
    let (_shutdown, receiver) = oneshot::channel();

    let consumed = timeout(
        TEST_BUDGET,
        consumer.consume_messages(&RejectMessages, receiver),
    )
    .await;

    let poll = DeferredPollMessagesRequest::decode_from(
        &cluster.received(POLL_MESSAGES_DEFERRED_CODE)[0].body,
    )
    .unwrap()
    .poll;
    assert_eq!(
        (poll.strategy, poll.auto_commit),
        (WirePollingStrategy::next(), true)
    );
    assert_eq!(
        consumed,
        Ok(Err(ReadError::new(
            Some(POLLED_PARTITION),
            IggyError::InvalidNumberEncoding
        )))
    );
}

/// Each run stops inside the batch, so a loop that picked a ready message over the signal would
/// hand one over in about half of the runs.
#[tokio::test(start_paused = true)]
async fn given_a_shutdown_signal_inside_a_batch_when_consuming_callbacks_should_hand_over_no_further_message()
 {
    const RUNS: u64 = 16;
    let offsets: Vec<u64> = (0..RUNS).collect();
    let (mut consumer, _cluster) =
        reading_consumer(ClientCommitPolicy::default(), &[offsets.as_slice()]).await;
    for offset in offsets {
        let (handler, receiver) = RecordMessages::until(offset);

        let consumed = timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver)).await;

        assert_eq!(consumed, Ok(Ok(())));
        assert_eq!(handler.offsets(), [offset]);
    }
}

#[tokio::test(start_paused = true)]
async fn standalone_consumer_should_stop_yielding_messages_after_shutdown() {
    let (consumer, _cluster) = reading_consumer(ClientCommitPolicy::default(), &[&BATCH]).await;

    assert_stream_ends_after_shutdown(consumer).await;
}

#[tokio::test(start_paused = true)]
async fn consumer_group_should_stop_yielding_messages_after_shutdown() {
    let (consumer, _cluster) = reading_member(&[POLLED_PARTITION], [reply_of(&BATCH)]).await;

    assert_stream_ends_after_shutdown(consumer).await;
}

#[tokio::test]
async fn consumer_group_should_not_create_fetch_worker_after_shutdown() {
    let mut consumer = builder_for(group()).build();

    consumer.shutdown().await.unwrap();

    assert!(matches!(poll_now(&mut consumer), Poll::Ready(None)));
    assert!(consumer.reader.is_none());
}

#[tokio::test(start_paused = true)]
async fn group_member_should_refuse_the_partitions_set_on_the_builder() {
    let initialized =
        init_without_stream(group(), |builder| builder.partitions([POLLED_PARTITION])).await;

    assert_eq!(initialized, (Err(IggyError::InvalidCommand), NO_LOOKUP));
}

#[tokio::test(start_paused = true)]
async fn given_a_standalone_consumer_without_partitions_or_with_a_continuation_start_when_initializing_should_refuse_it_before_any_request()
 {
    let continuation = PollingStrategy::continuation(
        POLLED_PARTITION,
        PollingKind::Offset,
        CONTINUED_AT,
        PartitionContext::default(),
    );

    let without_partitions = init_without_stream(standalone(), identity).await;
    let continued = init_without_stream(standalone(), |builder| {
        builder
            .partitions([POLLED_PARTITION])
            .polling_strategy(continuation)
    })
    .await;

    assert_eq!(
        without_partitions,
        (Err(IggyError::InvalidCommand), NO_LOOKUP)
    );
    assert_eq!(continued, (Err(IggyError::InvalidCommand), NO_LOOKUP));
}

#[tokio::test(start_paused = true)]
async fn standalone_consumer_should_keep_the_partition_set_on_the_builder() {
    let (consumer, cluster) = reading_consumer(ClientCommitPolicy::default(), &[]).await;

    assert_eq!(
        consumer.reader.as_ref().unwrap().partitions(),
        [POLLED_PARTITION]
    );
    let fetch =
        FetchMessagesRequest::decode_from(&cluster.received(FETCH_MESSAGES_CODE)[0].body).unwrap();
    let targets: Vec<u32> = fetch
        .targets
        .iter()
        .map(|target| target.partition_id)
        .collect();
    assert_eq!(targets, [POLLED_PARTITION]);
}

#[tokio::test(start_paused = true)]
async fn group_member_should_drop_the_rest_of_its_batch_once_the_assignment_changes() {
    let (mut consumer, _cluster) = reading_member(&[POLLED_PARTITION], [reply_of(&BATCH)]).await;
    for offset in &BATCH[..2] {
        assert_eq!(
            next_message(&mut consumer).await.message.header.offset,
            *offset
        );
    }

    reassign(&consumer, NEXT_GENERATION).await;

    assert!(poll_now(&mut consumer).is_pending());
    assert!(consumer.current.is_none());
    assert_eq!(
        consumer.get_last_consumed_offset(POLLED_PARTITION),
        Some(BATCH[1])
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_committed_batch_when_the_generation_or_assignment_changes_should_hand_over_the_rest()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_group(&cluster, &[POLLED_PARTITION]);
    let mut consumer = builder_on(client, group()).poll_options(on_read()).build();
    consumer.init().await.unwrap();
    script_primary(&cluster, FETCH_MESSAGES_CODE, [reply_of(&BATCH)]);
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        BATCH[0]
    );

    publish(&consumer, &[DiagnosticEvent::Disconnected]).await;
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        BATCH[1],
        "the server committed the batch, so a new epoch keeps it"
    );
    reassign(&consumer, NEXT_GENERATION).await;
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        BATCH[2],
        "the server committed the batch, so a new assignment keeps it"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_reread_up_to_the_delivered_offset_when_polled_should_hand_over_only_newer_messages()
 {
    let (mut consumer, _cluster) = rereading_consumer().await;

    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        DELIVERED + 1,
        "the application already took the messages up to {DELIVERED}"
    );
}

#[tokio::test(start_paused = true)]
async fn given_replay_when_a_reread_up_to_the_delivered_offset_is_polled_should_hand_it_over_again()
{
    let (mut consumer, _cluster) = rereading_consumer().await;
    consumer.allow_replay = true;

    for offset in DELIVERED - 1..=DELIVERED + 1 {
        assert_eq!(
            next_message(&mut consumer).await.message.header.offset,
            offset,
            "replay hands over every message that the batch read"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn group_member_tracks_delivered_positions_separately_per_partition() {
    const OTHER_OFFSET: u64 = 7;
    let reply = fetch_of(&[
        served(POLLED_PARTITION, &BATCH[..2], BATCH[2]),
        served(OTHER_PARTITION, &[OTHER_OFFSET], OTHER_OFFSET + 1),
    ]);
    let (mut consumer, _cluster) =
        reading_member(&[OTHER_PARTITION, POLLED_PARTITION], [reply]).await;

    for _ in 0..3 {
        next_message(&mut consumer).await;
    }

    assert_eq!(
        consumer.get_last_consumed_offset(POLLED_PARTITION),
        Some(BATCH[1])
    );
    assert_eq!(
        consumer.get_last_consumed_offset(OTHER_PARTITION),
        Some(OTHER_OFFSET)
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_reply_of_several_partitions_when_polling_should_hand_over_every_batch_in_reply_order()
 {
    const OTHER_OFFSET: u64 = 7;
    const HANDED_OVER: [(u32, u64); 3] = [
        (POLLED_PARTITION, 0),
        (POLLED_PARTITION, 1),
        (OTHER_PARTITION, OTHER_OFFSET),
    ];
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    let reply = fetch_of(&[
        served(POLLED_PARTITION, &[HANDED_OVER[0].1, HANDED_OVER[1].1], 2),
        served(OTHER_PARTITION, &[OTHER_OFFSET], OTHER_OFFSET + 1),
    ]);
    script_primary(&cluster, FETCH_MESSAGES_CODE, [reply, Answer::Silence]);
    let mut consumer = builder_on(client, standalone())
        .partitions([OTHER_PARTITION, POLLED_PARTITION])
        .polling_strategy(PollingStrategy::first())
        .prefetch_messages(ONE_SLOT)
        .build();
    consumer.init().await.unwrap();

    let mut handed_over = Vec::new();
    for _ in 1..HANDED_OVER.len() {
        let received = next_message(&mut consumer).await;
        handed_over.push((received.partition_id, received.message.header.offset));
        sleep(POLL_RETRY_INTERVAL).await;
        assert_eq!(
            cluster.count(FETCH_MESSAGES_CODE),
            1,
            "the reply holds the only slot until its last message"
        );
    }
    let last = next_message(&mut consumer).await;
    handed_over.push((last.partition_id, last.message.header.offset));

    assert_eq!(handed_over, HANDED_OVER);
    eventually("the last message returns the slot", || {
        cluster.count(FETCH_MESSAGES_CODE) == 2
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn given_a_failed_join_when_initializing_a_group_member_should_return_it_as_the_init_error() {
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        [existing_consumer_group()],
    );
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        JOIN_CONSUMER_GROUP_CODE,
        [Answer::CommitRejected(COMMIT_OP, IggyError::Unauthorized)],
    );
    let mut consumer = builder_on(client, group()).build();

    let initialized = consumer.init().await;

    assert_eq!(initialized, Err(IggyError::Unauthorized));
    assert!(!consumer.initialized);
    assert!(consumer.reader.is_none());
}

#[tokio::test(start_paused = true)]
async fn init_should_refuse_an_encryptor_with_on_read() {
    let encryptor = Arc::new(EncryptorKind::Aes256Gcm(
        Aes256GcmEncryptor::new(&ENCRYPTION_KEY).unwrap(),
    ));
    let committing = Arc::clone(&encryptor);

    let refused = init_without_stream(standalone(), |builder| {
        builder
            .partitions([POLLED_PARTITION])
            .encryptor(committing)
            .poll_options(on_read())
    })
    .await;
    let accepted = init_without_stream(standalone(), |builder| {
        builder.partitions([POLLED_PARTITION]).encryptor(encryptor)
    })
    .await;

    assert_eq!(refused, (Err(IggyError::InvalidConfiguration), NO_LOOKUP));
    assert_reaches_the_server(accepted, "an encryptor without OnRead");
}

#[tokio::test(start_paused = true)]
async fn given_each_commit_policy_when_initializing_should_refuse_only_the_policies_that_cannot_run()
 {
    for policy in client_commit_policies() {
        let without_on_read = init_without_stream(standalone(), |builder| {
            builder.partitions([POLLED_PARTITION]).client_commit(policy)
        })
        .await;
        let with_on_read = init_without_stream(standalone(), |builder| {
            builder
                .partitions([POLLED_PARTITION])
                .client_commit(policy)
                .poll_options(on_read())
        })
        .await;

        assert_reaches_the_server(without_on_read, &format!("{policy:?}"));
        if policy == ClientCommitPolicy::default() {
            assert_reaches_the_server(with_on_read, "OnRead with the default policy");
        } else {
            assert_eq!(
                with_on_read,
                (Err(IggyError::InvalidConfiguration), NO_LOOKUP),
                "OnRead with {policy:?}"
            );
        }
    }

    let never_commits = init_without_stream(standalone(), |builder| {
        builder
            .partitions([POLLED_PARTITION])
            .client_commit(ClientCommitPolicy {
                interval: None,
                on: Some(CommitEvent::EveryNthMessage(0)),
            })
    })
    .await;

    assert_eq!(
        never_commits,
        (Err(IggyError::InvalidConfiguration), NO_LOOKUP)
    );
}

#[tokio::test(start_paused = true)]
async fn given_an_uncertain_join_when_resolution_is_uncertain_should_keep_the_operation() {
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_consumer_group(), 2),
    );
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        JOIN_CONSUMER_GROUP_CODE,
        [Answer::Silence, Answer::Silence, Answer::Commit(COMMIT_OP)],
    );
    let mut consumer = builder_on(client, group()).build();
    answer_lookups(&cluster);
    let first = consumer.init().await;
    let op = cluster.received(JOIN_CONSUMER_GROUP_CODE)[0].header.request;
    assert_eq!(first, Err(IggyError::OperationUncertain(op)));

    answer_lookups(&cluster);
    assert_eq!(
        consumer.init().await,
        Err(IggyError::OperationUncertain(op))
    );
    assert_eq!(cluster.count(GET_CONSUMER_GROUP_CODE), 1);
    assert!(consumer.reader.is_none());

    answer_lookups(&cluster);
    consumer.init().await.unwrap();
    let joins = cluster.received(JOIN_CONSUMER_GROUP_CODE);
    assert_eq!(joins.len(), 4);
    assert_eq!(joins[1].header.request, op);
    assert_eq!(joins[2].header.request, op);
    assert_ne!(joins[3].header.request, op);
}

#[tokio::test(start_paused = true)]
async fn given_an_uncertain_join_when_resolution_is_rejected_should_report_and_allow_a_new_join() {
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_consumer_group(), 2),
    );
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        JOIN_CONSUMER_GROUP_CODE,
        [
            Answer::Silence,
            Answer::CommitRejected(COMMIT_OP, IggyError::Unauthorized),
        ],
    );
    let mut consumer = builder_on(client, group()).build();
    answer_lookups(&cluster);
    assert!(matches!(
        consumer.init().await,
        Err(IggyError::OperationUncertain(_))
    ));

    answer_lookups(&cluster);
    assert_eq!(consumer.init().await, Err(IggyError::Unauthorized));
    assert!(consumer.reader.is_none());

    answer_lookups(&cluster);
    consumer.init().await.unwrap();
    assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 3);
}

#[tokio::test(start_paused = true)]
async fn given_another_callers_uncertain_join_when_initializing_a_group_should_leave_it_retained() {
    let (client, cluster) = connect_to_three_replicas().await;
    let mut consumer = builder_on(client, group()).build();
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        JOIN_CONSUMER_GROUP_CODE,
        [Answer::Silence],
    );
    let first = consumer
        .client
        .read()
        .await
        .join_consumer_group(
            consumer.stream(),
            consumer.topic(),
            &Identifier::numeric(OTHER_GROUP).unwrap(),
        )
        .await;
    let op = cluster.received(JOIN_CONSUMER_GROUP_CODE)[0].header.request;
    assert_eq!(first, Err(IggyError::OperationUncertain(op)));
    answer_lookups(&cluster);
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        [existing_consumer_group()],
    );

    assert_eq!(consumer.init().await, Err(IggyError::LaneBlocked(op)));

    assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 1);
    assert!(consumer.reader.is_none());
    consumer
        .client
        .read()
        .await
        .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
        .await
        .unwrap();
    assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 2);
}

#[tokio::test(start_paused = true)]
async fn given_a_poll_lost_to_a_disconnect_when_consuming_callbacks_should_deliver_the_next_poll() {
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [Answer::HangUp, reply_of(&[OFFSET]), Answer::Silence],
    );
    let mut consumer = standalone_reader(client).build();
    consumer.init().await.unwrap();
    let (handler, receiver) = RecordMessages::until(OFFSET);

    let consumed = timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver)).await;

    assert_eq!(consumed, Ok(Ok(())));
    assert_eq!(handler.offsets(), [OFFSET]);
}

#[tokio::test(start_paused = true)]
async fn given_a_last_replica_link_that_gives_up_when_a_keepalive_restarts_the_links_should_pause_polling_and_keep_consuming_callbacks()
 {
    let one_retry = TcpClientReconnectionConfig {
        max_retries: Some(1),
        ..TcpClientReconnectionConfig::default()
    };
    let (mut consumer, cluster) = disconnected_consumer(ReconnectPolicy::from(&one_retry)).await;
    cluster.down.lock().unwrap().clear();
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&[OFFSET]), Answer::Silence],
    );
    let (handler, receiver) = RecordMessages::until(OFFSET);
    let client = consumer.client.clone();
    let heartbeat = async {
        sleep(POLL_TIMEOUT).await;
        client.read().await.keepalive().await
    };

    let (consumed, keepalive) = tokio::join!(
        timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver)),
        heartbeat
    );

    assert_eq!(keepalive, Ok(true));
    assert_eq!(consumed, Ok(Ok(())));
    assert_eq!(handler.offsets(), [OFFSET]);
}

#[tokio::test(start_paused = true)]
async fn given_reconnection_disabled_when_every_replica_link_is_lost_should_end_consuming_callbacks()
 {
    let disabled = TcpClientReconnectionConfig {
        enabled: false,
        ..TcpClientReconnectionConfig::default()
    };
    let (mut consumer, _cluster) = disconnected_consumer(ReconnectPolicy::from(&disabled)).await;
    let (_shutdown, receiver) = oneshot::channel();

    let consumed = timeout(
        POLL_TIMEOUT,
        consumer.consume_messages(&RejectMessages, receiver),
    )
    .await;

    // The read finds no link, or it loses its link, depending on when the links went down.
    assert!(
        matches!(
            consumed,
            Ok(Err(ReadError {
                error: IggyError::Disconnected | IggyError::NotConnected,
                ..
            }))
        ),
        "{consumed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_shut_down_client_when_consuming_callbacks_should_end_with_client_shutdown() {
    let (mut consumer, _cluster) = reading_consumer(ClientCommitPolicy::default(), &[]).await;
    consumer.client.read().await.shutdown().await.unwrap();
    let (_shutdown, receiver) = oneshot::channel();

    let consumed = timeout(
        POLL_TIMEOUT,
        consumer.consume_messages(&RejectMessages, receiver),
    )
    .await;

    assert!(
        matches!(
            consumed,
            Ok(Err(ReadError {
                error: IggyError::ClientShutdown,
                ..
            }))
        ),
        "{consumed:?}"
    );
}

#[tokio::test]
async fn given_an_uninitialized_consumer_when_consuming_callbacks_should_end_at_once() {
    let mut consumer = builder_for(standalone()).build();
    let (_shutdown, receiver) = oneshot::channel();

    let consumed = timeout(
        POLL_TIMEOUT,
        consumer.consume_messages(&RejectMessages, receiver),
    )
    .await;

    assert_eq!(
        consumed,
        Ok(Err(ReadError::new(None, IggyError::InvalidConfiguration)))
    );
}

#[tokio::test(start_paused = true)]
async fn given_the_default_client_commit_and_next_when_consuming_callbacks_should_warn_once() {
    let warnings = consume_three_messages(identity).await;

    assert_eq!(
        warnings.len(),
        1,
        "a run warns once, not per message: {warnings:?}"
    );
    assert!(warnings[0].contains("never stores offsets"), "{warnings:?}");
}

#[tokio::test(start_paused = true)]
async fn given_a_commit_policy_or_a_fixed_start_when_consuming_callbacks_should_not_warn() {
    let each_message = ClientCommitPolicy {
        interval: None,
        on: Some(CommitEvent::EachMessage),
    };
    let client_commit = consume_three_messages(|builder| builder.client_commit(each_message)).await;
    let server_commit = consume_three_messages(|builder| builder.poll_options(on_read())).await;
    let fixed_start =
        consume_three_messages(|builder| builder.polling_strategy(PollingStrategy::first())).await;

    for (case, warnings) in [
        ("a client commit policy", client_commit),
        ("OnRead", server_commit),
        ("a fixed start", fixed_start),
    ] {
        assert!(warnings.is_empty(), "{case}: {warnings:?}");
    }
}

#[tokio::test]
async fn given_an_uninitialized_consumer_when_polled_should_fail_each_poll_and_warn_once() {
    let mut consumer = builder_for(standalone()).build();
    let recorder = LevelReports::warnings();
    let _subscriber = tracing::subscriber::set_default(recorder.clone());

    let first = consumer.next().await;
    let second = consumer.next().await;

    for polled in [first, second] {
        assert!(
            matches!(
                polled,
                Some(Err(ReadError {
                    error: IggyError::InvalidConfiguration,
                    ..
                }))
            ),
            "every poll before init fails: {polled:?}"
        );
    }
    let warnings = recorder.events.lock().unwrap().clone();
    assert_eq!(
        warnings.len(),
        1,
        "only the first poll before init warns: {warnings:?}"
    );
    assert!(
        warnings[0].contains("was polled before init()"),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn given_an_uninitialized_consumer_when_seeking_should_refuse_with_invalid_configuration() {
    let mut consumer = builder_for(standalone())
        .partitions([POLLED_PARTITION])
        .build();

    let sought = consumer
        .seek(POLLED_PARTITION, PollingStrategy::offset(OFFSET))
        .await;

    assert_eq!(sought, Err(IggyError::InvalidConfiguration));
}

#[tokio::test(start_paused = true)]
async fn given_a_seek_when_polled_should_drop_the_rest_of_the_batch_and_read_from_the_new_start() {
    const SOUGHT: u64 = 30;
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&BATCH), reply_of(&[SOUGHT]), Answer::Silence],
    );
    // One slot, so the reader reads nothing while the batch is current, and the seek starts
    // the next read.
    let mut consumer = standalone_reader(client)
        .prefetch_messages(ONE_SLOT)
        .build();
    consumer.init().await.unwrap();
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        BATCH[0]
    );

    consumer
        .seek(POLLED_PARTITION, PollingStrategy::offset(SOUGHT))
        .await
        .unwrap();

    assert!(consumer.current.is_none());
    assert_eq!(
        next_message(&mut consumer).await.message.header.offset,
        SOUGHT,
        "the rest of the batch before the seek is dropped"
    );
    let fetch =
        FetchMessagesRequest::decode_from(&cluster.received(FETCH_MESSAGES_CODE)[1].body).unwrap();
    assert_eq!(
        fetch.targets[0].strategy,
        WirePollingStrategy::offset(SOUGHT)
    );
}

async fn assert_stream_ends_after_shutdown(mut consumer: IggyConsumer) {
    next_message(&mut consumer).await;
    assert!(consumer.current.is_some(), "the rest of the batch waits");

    consumer.shutdown().await.unwrap();

    assert!(consumer.current.is_none());
    assert!(matches!(poll_now(&mut consumer), Poll::Ready(None)));
}

/// One poll of `consumer` that does not wait.
fn poll_now(consumer: &mut IggyConsumer) -> Poll<Option<Result<ReceivedMessage, ReadError>>> {
    Pin::new(consumer).poll_next(&mut Context::from_waker(Waker::noop()))
}

/// The outcome of `init` for a consumer that `configure` sets up, on replicas that know no
/// stream, with the stream lookups that reached them.
async fn init_without_stream(
    consumer: Consumer,
    configure: impl FnOnce(IggyConsumerBuilder) -> IggyConsumerBuilder,
) -> (Result<(), IggyError>, usize) {
    let (client, cluster) = connect_to_three_replicas().await;
    let mut consumer = configure(builder_on(client, consumer)).build();
    let initialized = consumer.init().await;
    (initialized, cluster.count(GET_STREAM_CODE))
}

/// Asserts that `init` accepted the configuration of `case`: it looked the stream up and found
/// none.
fn assert_reaches_the_server(initialized: (Result<(), IggyError>, usize), case: &str) {
    assert!(
        matches!(
            initialized,
            (Err(IggyError::StreamIdNotFound(_)), ONE_LOOKUP)
        ),
        "{case}: {initialized:?}"
    );
}

/// Every shape of a client policy.
fn client_commit_policies() -> [ClientCommitPolicy; 6] {
    let interval = Some(NonZeroIggyDuration::ONE_SECOND);
    [
        ClientCommitPolicy::default(),
        ClientCommitPolicy { interval, on: None },
        ClientCommitPolicy {
            interval: None,
            on: Some(CommitEvent::EachMessage),
        },
        ClientCommitPolicy {
            interval: None,
            on: Some(CommitEvent::EveryNthMessage(NTH)),
        },
        ClientCommitPolicy {
            interval: None,
            on: Some(CommitEvent::EachBatch),
        },
        ClientCommitPolicy {
            interval,
            on: Some(CommitEvent::EachBatch),
        },
    ]
}

fn on_read() -> PollOptions {
    PollOptions {
        server_commit: ServerCommitPolicy::OnRead,
        ..PollOptions::default()
    }
}

/// A consumer of `POLLED_PARTITION` on `client` that reads from the first offset.
fn standalone_reader(client: TcpClient) -> IggyConsumerBuilder {
    builder_on(client, standalone())
        .partitions([POLLED_PARTITION])
        .polling_strategy(PollingStrategy::first())
}

/// An initialized group member assigned `partitions`, whose fetches serve `replies`. The fetch
/// after them stays unanswered.
async fn reading_member(
    partitions: &[u32],
    replies: impl IntoIterator<Item = Answer>,
) -> (IggyConsumer, Arc<FakeCluster>) {
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_group(&cluster, partitions);
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        replies.into_iter().chain([Answer::Silence]),
    );
    let mut consumer = builder_on(client, group()).build();
    consumer.init().await.unwrap();
    (consumer, cluster)
}

/// Scripts a group that exists and assigns `partitions` at each refresh.
fn script_group(cluster: &FakeCluster, partitions: &[u32]) {
    let metadata_primary = endpoint(METADATA_PRIMARY);
    cluster.script(
        &metadata_primary,
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_consumer_group(), GROUP_REFRESHES),
    );
    cluster.script(
        &metadata_primary,
        SYNC_CONSUMER_GROUP_CODE,
        repeat_n(
            assignment(FIRST_GENERATION, partitions.to_vec()),
            GROUP_REFRESHES,
        ),
    );
}

/// Moves the assignment of the group member `consumer` to `generation`, as a rebalance does.
async fn reassign(consumer: &IggyConsumer, generation: u64) {
    let state = consumer
        .client
        .read()
        .await
        .deferred_group_state()
        .await
        .unwrap();
    let key = group_cache_key(consumer.stream(), consumer.topic(), &consumer.consumer.id);
    state.set_assignment(key, generation, vec![POLLED_PARTITION]);
}

/// A consumer whose reader holds a batch that reads the messages around [`DELIVERED`] again,
/// in the partition history in which the application took [`DELIVERED`].
async fn rereading_consumer() -> (IggyConsumer, Arc<FakeCluster>) {
    let (consumer, cluster) = reading_consumer(
        ClientCommitPolicy::default(),
        &[&[DELIVERED - 1, DELIVERED, DELIVERED + 1]],
    )
    .await;
    // `served` writes the default context into each section.
    consumer.state.progress.deliver(&Checkpoint::new(
        POLLED_PARTITION,
        DELIVERED,
        PartitionContext::default(),
    ));
    (consumer, cluster)
}

/// An initialized consumer on a client under `policy` whose replicas went down after its first
/// fetch. It returns once the client reports the loss of its last link.
async fn disconnected_consumer(policy: ReconnectPolicy) -> (IggyConsumer, Arc<FakeCluster>) {
    let (client, cluster) = connect_to_three_replicas_with(policy).await;
    answer_lookups(&cluster);
    script_primary(&cluster, FETCH_MESSAGES_CODE, [Answer::Silence]);
    let mut consumer = standalone_reader(client).build();
    consumer.init().await.unwrap();
    eventually("the consumer reads", || {
        cluster.count(FETCH_MESSAGES_CODE) == 1
    })
    .await;
    let mut events = consumer.client.read().await.subscribe_events().await;
    take_every_replica_down(&cluster);
    timeout(TEST_BUDGET, async {
        while events.recv().await.unwrap() != DiagnosticEvent::Disconnected {}
    })
    .await
    .unwrap();
    (consumer, cluster)
}

/// Takes every replica of `cluster` down, which closes the links to them.
fn take_every_replica_down(cluster: &FakeCluster) {
    let endpoints: Vec<String> = cluster
        .nodes
        .lock()
        .unwrap()
        .iter()
        .map(|node| endpoint(node.replica_id))
        .collect();
    for endpoint in endpoints {
        cluster.down.lock().unwrap().insert(endpoint.clone());
        cluster.drop_connections(&endpoint);
    }
}

/// Publishes `events` in order on the client of `consumer`.
async fn publish(consumer: &IggyConsumer, events: &[DiagnosticEvent]) {
    let client = consumer.client.read().await;
    let ClientWrapper::Tcp(client) = &*client else {
        panic!("a test consumer runs on a TCP client");
    };
    for event in events {
        client.publish_event(*event).await;
    }
}

/// Runs `consume_messages` over three messages of a consumer of `POLLED_PARTITION` that
/// `configure` sets up, and returns the warnings that the run logged.
async fn consume_three_messages(
    configure: impl FnOnce(IggyConsumerBuilder) -> IggyConsumerBuilder,
) -> Vec<String> {
    const OFFSETS: [u64; 3] = [1, 2, 3];
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        [reply_of(&OFFSETS), Answer::Silence],
    );
    let mut consumer =
        configure(builder_on(client, standalone()).partitions([POLLED_PARTITION])).build();
    consumer.init().await.unwrap();
    let (handler, receiver) = RecordMessages::until(OFFSETS[2]);
    let recorder = LevelReports::warnings();
    let _subscriber = tracing::subscriber::set_default(recorder.clone());

    let consumed = timeout(TEST_BUDGET, consumer.consume_messages(&handler, receiver)).await;

    assert_eq!(consumed, Ok(Ok(())));
    assert_eq!(handler.offsets(), OFFSETS);
    recorder.events.lock().unwrap().clone()
}
