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

use crate::common::global_context::GlobalContext;
use crate::steps::auth::given_authenticated_as_root;
use cucumber::{given, then, when};
use iggy::prelude::{
    Consumer, ConsumerGroupClient, Identifier, IggyClient, IggyDuration, IggyMessage,
    MessageClient, Partitioning, PollOptions, PolledMessages, PollingStrategy, ServerCommitPolicy,
    StreamClient, StreamCreateOptions, TopicClient, TopicCreateOptions,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinError;
use tokio::time::{Instant, sleep, timeout, timeout_at};

const MESSAGES_COUNT: u32 = 10;
/// A message that reaches the server before the call answers it at once, so a
/// start step waits until its call is parked.
const PARK_TIME: Duration = Duration::from_millis(100);
const ASSIGNMENT_TIMEOUT: Duration = Duration::from_secs(5);
const ASSIGNMENT_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    regex = r#"^I start a deferred poll of partition (\d+) for consumer "([^"]+)" with auto commit, a min count of (\d+) and a max wait of (\d+) seconds?$"#
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
            server_commit: ServerCommitPolicy::OnRead,
            ..poll_options(max_wait_secs)
        },
    )
    .await;
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
    regex = r"^I start a fetch on partitions (\d+), (\d+) and (\d+) from offset (\d+) with a max wait of (\d+) seconds?$"
)]
pub async fn when_start_fetch(
    world: &mut GlobalContext,
    first: u32,
    second: u32,
    third: u32,
    offset: u64,
    max_wait_secs: u64,
) {
    let client = background_client(world).await;
    let mut reader = client
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
    // The reader never yields an empty batch, so its first batch comes from the
    // first partition that gets a message.
    let fetch = tokio::spawn(async move { reader.next().await });
    sleep(PARK_TIME).await;
    assert!(!fetch.is_finished(), "the fetch must park");
    world.fetch = Some(fetch);
}

#[then(regex = r"^the fetch should return only partition (\d+) within (\d+) seconds?$")]
pub async fn then_fetch_returns(world: &mut GlobalContext, partition: u32, within_secs: u64) {
    let fetch = world.fetch.take().expect("fetch started");
    let polled = timeout(Duration::from_secs(within_secs), fetch)
        .await
        .unwrap_or_else(|_| panic!("the fetch did not return within {within_secs} s"))
        .expect("fetch task")
        .expect("fetch");
    assert_eq!(
        polled.partition_id, partition,
        "the fetch must serve only partition {partition}"
    );
    assert_single_payload(&polled, "ready");
}

#[given(regex = r#"^I am the only member of consumer group "([^"]+)" on the topic$"#)]
pub async fn given_sole_group_member(world: &mut GlobalContext, group_name: String) {
    let client = world.client.as_ref().expect("client");
    let stream_id = stream_identifier(world);
    let topic_id = topic_identifier(world);
    client
        .create_consumer_group(&stream_id, &topic_id, &group_name)
        .await
        .expect("create consumer group");
    let group_id = Identifier::named(&group_name).expect("group name");
    client
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("join consumer group");
    // The join can return before the coordinator assigns the partitions. The
    // first group poll then syncs the whole assignment.
    let partitions = world.last_topic_partitions.expect("topic created");
    let deadline = Instant::now() + ASSIGNMENT_TIMEOUT;
    loop {
        let group = client
            .get_consumer_group(&stream_id, &topic_id, &group_id)
            .await
            .expect("get consumer group")
            .expect("consumer group exists");
        if group
            .members
            .iter()
            .any(|member| member.partitions_count == partitions)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the member did not get every partition within {ASSIGNMENT_TIMEOUT:?}: {group:?}"
        );
        sleep(ASSIGNMENT_POLL_INTERVAL).await;
    }
    world.last_consumer_group_name = Some(group_name);
}

#[then(
    regex = r#"^a group fetch with a max wait of (\d+) seconds? should return the payload "([^"]+)" within (\d+) seconds?$"#
)]
pub async fn then_group_fetch_returns(
    world: &mut GlobalContext,
    max_wait_secs: u64,
    payload: String,
    within_secs: u64,
) {
    // A group reader joins in `build()` and leaves in `close()`, so one reader
    // serves every step of the scenario, with the max wait of the first step.
    if world.group_reader.is_none() {
        let client = world.client.as_ref().expect("client");
        let group_name = world
            .last_consumer_group_name
            .as_ref()
            .expect("consumer group joined");
        let group = Consumer::group(Identifier::named(group_name).expect("group name"));
        let reader = client
            .reader(&stream_identifier(world), &topic_identifier(world), &group)
            .start(PollingStrategy::next())
            .options(PollOptions {
                server_commit: ServerCommitPolicy::OnRead,
                ..poll_options(max_wait_secs)
            })
            .build()
            .await
            .expect("build group reader");
        world.group_reader = Some(reader);
    }
    let reader = world.group_reader.as_mut().expect("group reader");
    let Ok(read) = timeout(Duration::from_secs(within_secs), reader.next()).await else {
        panic!("the group fetch did not return within {within_secs} s");
    };
    assert_single_payload(&read.expect("group fetch"), &payload);
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
    let client = world.client.as_ref().expect("client");
    let polled = client
        .poll_messages(
            &stream_identifier(world),
            &topic_identifier(world),
            partition,
            &named_consumer(&consumer_name),
            &PollingStrategy::next(),
            PollOptions {
                count: MESSAGES_COUNT,
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
        Some(payload.as_str()),
        "the first message must have payload {payload:?}: {polled:?}"
    );
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
