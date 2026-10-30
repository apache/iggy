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

use crate::common::global_context::{GlobalContext, OpenConsumer};
use crate::helpers::env::{root_password, root_username};
use crate::steps::auth::given_authenticated_as_root;
use cucumber::{given, then, when};
use futures::StreamExt;
use iggy::prelude::{
    AutoCommit, Client, CommitAfter, CommitPolicy, Consumer, Identifier, IggyClient, IggyDuration,
    IggyError, IggyMessage, MessageClient, Partitioning, PollOptions, PolledMessages,
    PollingStrategy, StreamClient, StreamCreateOptions, TopicClient, TopicCreateOptions,
    UserClient,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinError;
use tokio::time::{Instant, sleep, timeout, timeout_at};

const MESSAGES_COUNT: u32 = 10;
/// A message that reaches the server before the call answers it at once, so a
/// start step waits until its call is parked.
const PARK_TIME: Duration = Duration::from_millis(100);
/// The wait of a consumer whose step names none.
const CONSUMER_MAX_WAIT_SECS: u64 = 10;

#[given(
    regex = r#"^a stream named "([^"]+)" with a topic named "([^"]+)" with (\d+) partitions exists$"#
)]
pub async fn given_stream_with_topic(
    world: &mut GlobalContext,
    stream_name: String,
    topic_name: String,
    partitions: u32,
) {
    let client = world.client.as_ref().expect("client");
    let stream_id = Identifier::named(&stream_name).expect("stream name");
    if client
        .get_stream(&stream_id)
        .await
        .expect("get stream")
        .is_some()
    {
        client
            .delete_stream(&stream_id)
            .await
            .expect("delete stream");
    }
    let stream = client
        .create_stream(&stream_name, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let topic = client
        .create_topic(
            &stream_id,
            &topic_name,
            &TopicCreateOptions {
                partitions_count: Some(partitions),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    world.last_stream_id = Some(stream.id);
    world.last_stream_name = Some(stream.name);
    world.last_topic_id = Some(topic.id);
    world.last_topic_name = Some(topic.name);
    world.last_topic_partitions = Some(partitions);
}

#[when(
    regex = r"^I start a deferred poll of partition (\d+) from offset (\d+) with a max wait of (\d+) seconds?$"
)]
pub async fn when_start_deferred_poll(
    world: &mut GlobalContext,
    partition: u32,
    offset: u64,
    max_wait_secs: u64,
) {
    start_deferred_poll(
        world,
        partition,
        Consumer::default(),
        PollingStrategy::offset(offset),
        poll_options(max_wait_secs),
    )
    .await;
}

#[when(
    regex = r#"^I start a deferred poll of partition (\d+) for consumer "([^"]+)" with the on read commit policy, a min count of (\d+) and a max wait of (\d+) seconds?$"#
)]
pub async fn when_start_committing_deferred_poll(
    world: &mut GlobalContext,
    partition: u32,
    consumer_name: String,
    min_count: u32,
    max_wait_secs: u64,
) {
    start_deferred_poll(
        world,
        partition,
        named_consumer(&consumer_name),
        PollingStrategy::next(),
        PollOptions {
            min_count,
            commit: CommitPolicy::OnRead,
            ..poll_options(max_wait_secs)
        },
    )
    .await;
}

/// The poll runs on the client of the scenario, which a later step disconnects.
#[when(
    regex = r#"^I start a deferred poll of partition (\d+) for consumer "([^"]+)" with the on read commit policy and a max wait of (\d+) seconds?$"#
)]
pub async fn when_start_committing_poll_on_own_client(
    world: &mut GlobalContext,
    partition: u32,
    consumer_name: String,
    max_wait_secs: u64,
) {
    let client = Arc::new(world.client.take().expect("client"));
    world.polling_client = Some(Arc::clone(&client));
    let stream_id = stream_identifier(world);
    let topic_id = topic_identifier(world);
    let consumer = named_consumer(&consumer_name);
    let options = PollOptions {
        commit: CommitPolicy::OnRead,
        ..poll_options(max_wait_secs)
    };
    // A read that stores nothing routes the partition first, so the committing poll leaves the
    // client at once. A disconnect before it left would end it as never sent.
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            partition,
            &consumer,
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1,
                commit: CommitPolicy::Manual,
                ..PollOptions::default()
            },
        )
        .await
        .expect("route the partition");
    world.deferred_poll_started = Some(Instant::now());
    let poll = tokio::spawn(async move {
        client
            .poll_messages(
                &stream_id,
                &topic_id,
                partition,
                &consumer,
                &PollingStrategy::next(),
                options,
            )
            .await
    });
    sleep(PARK_TIME).await;
    assert!(!poll.is_finished(), "the deferred poll must park");
    world.deferred_poll = Some(poll);
}

/// The SDK's own disconnect, which signs nothing out on the server.
#[when(regex = r"^the client disconnects before the deferred poll answers$")]
pub async fn when_client_disconnects(world: &mut GlobalContext) {
    let client = world.polling_client.as_ref().expect("a deferred poll runs");
    client.disconnect().await.expect("disconnect");
}

#[then(regex = r"^the deferred poll should fail as uncertain$")]
pub async fn then_deferred_poll_fails_as_uncertain(world: &mut GlobalContext) {
    let poll = world.deferred_poll.take().expect("deferred poll started");
    let outcome = timeout(Duration::from_secs(CONSUMER_MAX_WAIT_SECS), poll)
        .await
        .expect("the deferred poll ends after the disconnect")
        .expect("the deferred poll task");
    let Err(IggyError::FetchProgressUncertain(operation)) = outcome else {
        panic!("the deferred poll must fail as uncertain, not with {outcome:?}");
    };
    world.lost_operation = Some(operation);
}

/// The same client object connects and signs in again, in a new session.
#[when(regex = r"^the client connects again$")]
pub async fn when_client_connects_again(world: &mut GlobalContext) {
    let client = world.polling_client.take().expect("a deferred poll ran");
    let client = Arc::try_unwrap(client).expect("the ended poll released its client");
    client.connect().await.expect("connect again");
    client
        .login_user(&root_username(), &root_password())
        .await
        .expect("sign in again");
    world.client = Some(client);
}

#[then(
    regex = r#"^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy and the on read commit policy should fail as blocked by the deferred poll$"#
)]
pub async fn then_on_read_poll_fails_as_blocked(
    world: &mut GlobalContext,
    partition: u32,
    consumer_name: String,
) {
    let operation = world
        .lost_operation
        .expect("a deferred poll failed as uncertain");
    let client = world.client.as_ref().expect("client");
    let polled = client
        .poll_messages(
            &stream_identifier(world),
            &topic_identifier(world),
            partition,
            &named_consumer(&consumer_name),
            &PollingStrategy::next(),
            PollOptions {
                count: 1,
                commit: CommitPolicy::OnRead,
                ..PollOptions::default()
            },
        )
        .await;
    assert_eq!(
        polled.map(drop),
        Err(IggyError::PartitionLaneBlocked(partition, operation)),
        "the poll must be blocked by the lost poll"
    );
}

#[when(regex = r"^I abandon the operation of the deferred poll$")]
pub async fn when_abandon_lost_poll(world: &mut GlobalContext) {
    let operation = world
        .lost_operation
        .expect("a deferred poll failed as uncertain");
    let client = world.client.as_ref().expect("client");
    client
        .abandon_operation(operation)
        .await
        .expect("abandon the lost poll");
}

#[when(regex = r#"^I send a message with payload "([^"]+)" to partition (\d+)$"#)]
pub async fn when_send_to_partition(world: &mut GlobalContext, payload: String, partition: u32) {
    let client = world.client.as_ref().expect("client");
    let message = IggyMessage::builder()
        .payload(payload.into())
        .build()
        .expect("message");
    client
        .send_messages(
            &stream_identifier(world),
            &topic_identifier(world),
            &Partitioning::partition_id(partition),
            &mut [message],
        )
        .await
        .expect("send message");
}

#[then(regex = r#"^the deferred poll should return the payload "([^"]+)" within (\d+) seconds?$"#)]
pub async fn then_deferred_poll_returns(
    world: &mut GlobalContext,
    payload: String,
    within_secs: u64,
) {
    let poll = world.deferred_poll.take().expect("deferred poll started");
    let polled = timeout(Duration::from_secs(within_secs), poll)
        .await
        .unwrap_or_else(|_| panic!("the deferred poll did not return within {within_secs} s"))
        .expect("deferred poll task")
        .expect("deferred poll");
    assert_single_payload(&polled, &payload);
}

#[then(
    regex = r"^the deferred poll should return no messages between (\d+) and (\d+) seconds after it started$"
)]
pub async fn then_deferred_poll_returns_nothing(
    world: &mut GlobalContext,
    from_secs: u64,
    to_secs: u64,
) {
    let started = world.deferred_poll_started.expect("deferred poll started");
    let poll = world.deferred_poll.take().expect("deferred poll started");
    let polled = timeout_at(started + Duration::from_secs(to_secs), poll)
        .await
        .unwrap_or_else(|_| panic!("the deferred poll did not return within {to_secs} s"))
        .expect("deferred poll task")
        .expect("deferred poll");
    let elapsed = started.elapsed();
    assert!(
        polled.messages.is_empty(),
        "the deferred poll must return no messages: {polled:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(from_secs),
        "the deferred poll returned after {elapsed:?}, before {from_secs} s"
    );
}

#[when(
    regex = r"^I open a partition reader on partitions (\d+), (\d+) and (\d+) from offset (\d+) with a max wait of (\d+) seconds?$"
)]
pub async fn when_open_partition_reader(
    world: &mut GlobalContext,
    first: u32,
    second: u32,
    third: u32,
    offset: u64,
    max_wait_secs: u64,
) {
    let client = world.client.as_ref().expect("client");
    let reader = client
        .reader(
            &stream_identifier(world),
            &topic_identifier(world),
            &Consumer::default(),
        )
        .partitions([first, second, third])
        .start(PollingStrategy::offset(offset))
        .options(poll_options(max_wait_secs))
        .build()
        .await
        .expect("build reader");
    world.reader = Some(reader);
}

#[then(
    regex = r#"^the partition reader should return the payload "([^"]+)" from partition (\d+) within (\d+) seconds?$"#
)]
pub async fn then_partition_reader_returns(
    world: &mut GlobalContext,
    payload: String,
    partition: u32,
    within_secs: u64,
) {
    let reader = world.reader.as_mut().expect("partition reader open");
    // The reader never yields an empty batch, so its first batch comes from the first
    // partition that gets a message.
    let Ok(read) = timeout(Duration::from_secs(within_secs), reader.next()).await else {
        panic!("the partition reader did not return within {within_secs} s");
    };
    let read = read.expect("partition reader batch");
    assert_eq!(
        read.partition_id, partition,
        "the batch must come from partition {partition}"
    );
    assert_single_payload(&read, &payload);
}

#[when(
    regex = r#"^I open a consumer in group "([^"]+)" on the topic with a max wait of (\d+) seconds?$"#
)]
pub async fn when_open_group_consumer(
    world: &mut GlobalContext,
    group_name: String,
    max_wait_secs: u64,
) {
    open_group_consumer(world, &group_name, poll_options(max_wait_secs)).await;
}

#[when(
    regex = r#"^I open a consumer in group "([^"]+)" on the topic that commits after processing$"#
)]
pub async fn when_open_processing_group_consumer(world: &mut GlobalContext, group_name: String) {
    let options = PollOptions {
        commit: CommitPolicy::Auto(AutoCommit {
            after: CommitAfter::Processing,
            ..AutoCommit::default()
        }),
        ..poll_options(CONSUMER_MAX_WAIT_SECS)
    };
    open_group_consumer(world, &group_name, options).await;
}

#[then(regex = r#"^the consumer should return the payload "([^"]+)" within (\d+) seconds?$"#)]
pub async fn then_consumer_returns(world: &mut GlobalContext, payload: String, within_secs: u64) {
    let OpenConsumer(consumer) = world.consumer.as_mut().expect("consumer open");
    let Ok(received) = timeout(Duration::from_secs(within_secs), consumer.next()).await else {
        panic!("the consumer did not return within {within_secs} s");
    };
    let received = received
        .expect("the consumer stream ended")
        .expect("consumed message");
    assert_eq!(
        String::from_utf8_lossy(&received.message.payload),
        payload,
        "the consumer must return the payload {payload:?}"
    );
}

#[when(regex = r"^I close the consumer$")]
pub async fn when_close_consumer(world: &mut GlobalContext) {
    let OpenConsumer(mut consumer) = world.consumer.take().expect("consumer open");
    consumer.shutdown().await.expect("shut down the consumer");
}

#[when(regex = r"^I cancel the deferred poll after (\d+) milliseconds$")]
pub async fn when_cancel_deferred_poll(world: &mut GlobalContext, after_ms: u64) {
    sleep(Duration::from_millis(after_ms)).await;
    let poll = world.deferred_poll.take().expect("deferred poll started");
    // Dropping the parked request sends the cancellation (108) on its link.
    poll.abort();
    let outcome = poll.await;
    assert!(
        outcome.as_ref().is_err_and(JoinError::is_cancelled),
        "the deferred poll must end as cancelled, not with {outcome:?}"
    );
}

#[when(regex = r"^I wait (\d+) seconds?$")]
pub async fn when_wait(_world: &mut GlobalContext, seconds: u64) {
    sleep(Duration::from_secs(seconds)).await;
}

#[then(
    regex = r#"^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy should return the payload "([^"]+)"$"#
)]
pub async fn then_next_poll_returns(
    world: &mut GlobalContext,
    partition: u32,
    consumer_name: String,
    payload: String,
) {
    poll_next_and_expect(
        world,
        partition,
        &consumer_name,
        CommitPolicy::Manual,
        &payload,
    )
    .await;
}

#[then(
    regex = r#"^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy and the on read commit policy should return the payload "([^"]+)"$"#
)]
pub async fn then_on_read_poll_returns(
    world: &mut GlobalContext,
    partition: u32,
    consumer_name: String,
    payload: String,
) {
    poll_next_and_expect(
        world,
        partition,
        &consumer_name,
        CommitPolicy::OnRead,
        &payload,
    )
    .await;
}

/// Polls one message from `Next`, so a poll that commits stores only the offset of the payload
/// it returns.
async fn poll_next_and_expect(
    world: &GlobalContext,
    partition: u32,
    consumer_name: &str,
    commit: CommitPolicy,
    payload: &str,
) {
    let client = world.client.as_ref().expect("client");
    let polled = client
        .poll_messages(
            &stream_identifier(world),
            &topic_identifier(world),
            partition,
            &named_consumer(consumer_name),
            &PollingStrategy::next(),
            PollOptions {
                count: 1,
                commit,
                ..PollOptions::default()
            },
        )
        .await
        .expect("poll messages");
    let first = polled
        .messages
        .first()
        .map(|message| String::from_utf8_lossy(&message.payload));
    assert_eq!(
        first.as_deref(),
        Some(payload),
        "the first message must have payload {payload:?}: {polled:?}"
    );
}

async fn open_group_consumer(world: &mut GlobalContext, group_name: &str, options: PollOptions) {
    let client = world.client.as_ref().expect("client");
    let stream = world.last_stream_name.as_deref().expect("stream created");
    let topic = world.last_topic_name.as_deref().expect("topic created");
    let mut consumer = client
        .consumer_group(group_name, stream, topic)
        .expect("consumer group builder")
        .create_consumer_group_if_not_exists()
        .poll_options(options)
        .build();
    consumer.init().await.expect("init the consumer");
    world.consumer = Some(OpenConsumer(consumer));
}

/// The SDK defaults with the count of these steps and the max wait of the step.
fn poll_options(max_wait_secs: u64) -> PollOptions {
    PollOptions {
        count: MESSAGES_COUNT,
        ..PollOptions::wait(IggyDuration::from(Duration::from_secs(max_wait_secs)))
    }
}

async fn start_deferred_poll(
    world: &mut GlobalContext,
    partition: u32,
    consumer: Consumer,
    strategy: PollingStrategy,
    options: PollOptions,
) {
    let client = background_client(world).await;
    let stream_id = stream_identifier(world);
    let topic_id = topic_identifier(world);
    world.deferred_poll_started = Some(Instant::now());
    let poll = tokio::spawn(async move {
        client
            .poll_messages(
                &stream_id, &topic_id, partition, &consumer, &strategy, options,
            )
            .await
    });
    sleep(PARK_TIME).await;
    assert!(!poll.is_finished(), "the deferred poll must park");
    world.deferred_poll = Some(poll);
}

/// A spawned call owns its client, so it gets a connection of its own. The
/// world keeps that client, so that a cancelled call still has a link for its
/// cancellation.
async fn background_client(world: &mut GlobalContext) -> Arc<IggyClient> {
    if world.background_client.is_none() {
        let mut background = GlobalContext {
            server_addr: world.server_addr.clone(),
            ..GlobalContext::default()
        };
        given_authenticated_as_root(&mut background).await;
        world.background_client = background.client.map(Arc::new);
    }
    Arc::clone(world.background_client.as_ref().expect("background client"))
}

fn assert_single_payload(polled: &PolledMessages, expected: &str) {
    let payloads: Vec<_> = polled
        .messages
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload))
        .collect();
    assert_eq!(
        payloads,
        [expected],
        "the poll must return one message with payload {expected:?}"
    );
}

fn named_consumer(name: &str) -> Consumer {
    Consumer::new(Identifier::named(name).expect("consumer name"))
}

fn stream_identifier(world: &GlobalContext) -> Identifier {
    Identifier::numeric(world.last_stream_id.expect("stream created")).expect("stream id")
}

fn topic_identifier(world: &GlobalContext) -> Identifier {
    Identifier::numeric(world.last_topic_id.expect("topic created")).expect("topic id")
}
