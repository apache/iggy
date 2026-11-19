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

//! A consumer group that moves between two clusters on `fs`, `dc-a` and
//! `dc-b`. The group reads the geo topic of `dc-a` there, and the shadow of
//! that topic in `dc-b`. Its positions travel through the buckets.

use std::collections::BTreeMap;
use std::time::Duration;

use iggy::prelude::*;
use iggy_object_store::{GeoPosition, PositionValue};
use integration::harness::{
    GeoCluster, GeoClusterPair, GeoSide, wait_for_consumer_group_assignment,
};
use serde_json::json;
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

const STREAM: &str = "orders";
const TOPIC: &str = "payments";
const GROUP: &str = "billing";
const PUBLISH_INTERVAL: Duration = Duration::from_millis(250);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// How long a group may take to read what a test expects, or a position to
/// reach the other cluster.
const DEADLINE: Duration = Duration::from_secs(60);
const RETRY: Duration = Duration::from_millis(50);

#[tokio::test]
async fn given_a_group_drained_in_one_datacenter_when_it_starts_in_the_other_should_resume_on_the_exact_next_message()
 {
    let mut pair = started_pair().await;
    let ids = create_and_follow(&pair).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    send(&client_a, 0..100).await;
    wait_until_shadow_serves(&client_b, ids, 100).await;
    let group_a = Group::join(&client_a, ids.own()).await;
    let consumed = group_a.consume(&client_a, 37).await;
    assert_eq!(consumed, (0..37).collect::<Vec<_>>());
    group_a.leave(&client_a).await;

    let http = HttpClient::login_root(pair.a().harness()).await;
    let response = http
        .post_json(
            &format!("/streams/{STREAM}/topics/{TOPIC}/consumer-groups/{GROUP}/drain?timeout=30s"),
            &json!({}),
        )
        .await;
    assert_eq!(response.status(), 200, "the drain stores the positions");
    let drained: serde_json::Value = response.json().await.expect("a drain body");
    assert_eq!(drained["positions"][0]["offset"], 36, "{drained}");
    assert_eq!(drained["not_held"], json!([]), "{drained}");
    let stored = pair
        .a()
        .bucket()
        .positions_objects()
        .await
        .expect("dc-a's bucket reads");
    assert!(
        stored.iter().any(|object| object.partitions[&0][GROUP]
            == GeoPosition {
                reset: 0,
                value: PositionValue::Offset(36)
            }),
        "the drain answers once the bucket holds the position: {stored:?}"
    );

    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a stops after the drain");
    let group_b = Group::create(&client_b, ids.shadow()).await;
    group_b.wait_for_offset(&client_b, Some(36)).await;
    let group_b = group_b.joined(&client_b).await;
    let resumed = group_b.consume(&client_b, 10).await;
    assert_eq!(
        resumed,
        (37..47).collect::<Vec<_>>(),
        "the group resumes on the next message"
    );
}

#[tokio::test]
async fn given_a_group_that_moved_back_in_one_datacenter_when_the_other_holds_a_higher_offset_should_keep_the_move_in_both()
 {
    let pair = started_pair().await;
    let ids = create_and_follow(&pair).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    send(&client_a, 0..100).await;
    wait_until_shadow_serves(&client_b, ids, 100).await;
    let group_a = Group::join(&client_a, ids.own()).await;
    group_a.consume(&client_a, 60).await;
    group_a.leave(&client_a).await;
    let group_b = Group::create(&client_b, ids.shadow()).await;
    group_b.wait_for_offset(&client_b, Some(59)).await;

    let group_b = group_b.joined(&client_b).await;
    group_b.store(&client_b, 20).await;
    group_b.leave(&client_b).await;

    group_a.wait_for_offset(&client_a, Some(20)).await;
    // Several rounds of both clusters: neither moves the group forward again.
    sleep(4 * (PUBLISH_INTERVAL + POLL_INTERVAL)).await;
    assert_eq!(group_b.offset(&client_b).await, Some(20), "dc-b keeps it");
    assert_eq!(group_a.offset(&client_a).await, Some(20), "dc-a took it");
    let group_a = group_a.joined(&client_a).await;
    assert_eq!(
        group_a.consume(&client_a, 5).await,
        (21..26).collect::<Vec<_>>(),
        "dc-a reads on from the move"
    );
}

#[tokio::test]
async fn given_a_group_consuming_in_one_datacenter_when_that_datacenter_fails_should_resume_in_the_other_skipping_none_and_rereading_at_most_two_intervals()
 {
    const MESSAGES: u64 = 600;
    const PACE: Duration = Duration::from_millis(20);
    const CONSUME_FOR: Duration = Duration::from_secs(6);
    let mut pair = started_pair().await;
    let ids = create_and_follow(&pair).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    send(&client_a, 0..MESSAGES).await;
    wait_until_shadow_serves(&client_b, ids, MESSAGES).await;
    let group_b = Group::create(&client_b, ids.shadow()).await;
    let group_a = Group::join(&client_a, ids.own()).await;

    let mut consumed_at = BTreeMap::new();
    let started = Instant::now();
    while started.elapsed() < CONSUME_FOR {
        for offset in group_a.poll(&client_a, 1).await {
            consumed_at.insert(offset, Instant::now());
        }
        sleep(PACE).await;
    }
    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a fails");
    let failed_at = Instant::now();
    let last = *consumed_at.keys().last().expect("dc-a consumed");
    assert_eq!(
        consumed_at.keys().copied().collect::<Vec<_>>(),
        (0..=last).collect::<Vec<_>>()
    );

    let group_b = group_b.joined(&client_b).await;
    let resumed = group_b.consume(&client_b, MESSAGES - last - 1).await;
    let first = *resumed.first().expect("dc-b reads");
    assert!(first <= last + 1, "dc-b skipped {} to {first}", last + 1);
    assert_eq!(
        resumed,
        (first..first + resumed.len() as u64).collect::<Vec<_>>(),
        "each message once, in order"
    );
    let reread: Vec<u64> = (first..=last).collect();
    let bound = PUBLISH_INTERVAL + POLL_INTERVAL;
    // One reconcile tick of dc-b and the commits on both sides add to the
    // two intervals.
    let slack = Duration::from_secs(1);
    for offset in &reread {
        let age = failed_at.duration_since(consumed_at[offset]);
        assert!(
            age <= bound + slack,
            "dc-b re-read offset {offset}, which dc-a consumed {age:?} before it failed; \
             it re-read {} messages from {first}",
            reread.len()
        );
    }
    eprintln!(
        "dc-a consumed through {last}; dc-b resumed at {first}, re-reading {} messages",
        reread.len()
    );
}

#[tokio::test]
async fn given_a_group_that_moved_on_in_the_other_datacenter_when_its_first_datacenter_returns_should_resume_from_the_others_position()
 {
    let mut pair = started_pair().await;
    let ids = create_and_follow(&pair).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    send(&client_a, 0..100).await;
    wait_until_shadow_serves(&client_b, ids, 100).await;
    let group_a = Group::join(&client_a, ids.own()).await;
    group_a.consume(&client_a, 30).await;
    group_a.leave(&client_a).await;
    let group_b = Group::create(&client_b, ids.shadow()).await;
    group_b.wait_for_offset(&client_b, Some(29)).await;
    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a fails");

    let group_b = group_b.joined(&client_b).await;
    assert_eq!(
        group_b.consume(&client_b, 40).await,
        (30..70).collect::<Vec<_>>()
    );
    group_b.leave(&client_b).await;
    wait_for_bucket_offset(pair.b(), 69).await;
    pair.cluster_mut(GeoSide::A)
        .start()
        .await
        .expect("dc-a returns");
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let group_a = Group::create(&client_a, ids.own())
        .await
        .joined(&client_a)
        .await;

    assert_eq!(
        group_a.consume(&client_a, 5).await,
        (70..75).collect::<Vec<_>>(),
        "the group's first read in dc-a after its return waits for dc-b's position"
    );
}

/// The topic of `dc-a` and its shadow in `dc-b`.
#[derive(Debug, Clone, Copy)]
struct Ids {
    shadow_stream_id: u32,
    topic_id: u32,
}

impl Ids {
    fn own(self) -> (Identifier, Identifier) {
        (named(STREAM), named(TOPIC))
    }

    fn shadow(self) -> (Identifier, Identifier) {
        (numeric(self.shadow_stream_id), numeric(self.topic_id))
    }
}

/// The group on one topic of one cluster.
struct Group {
    stream: Identifier,
    topic: Identifier,
}

impl Group {
    async fn create(client: &IggyClient, (stream, topic): (Identifier, Identifier)) -> Self {
        retry(|| async {
            match client.create_consumer_group(&stream, &topic, GROUP).await {
                Err(IggyError::ConsumerGroupNameAlreadyExists(_, _)) => Ok(()),
                created => created.map(|_| ()),
            }
        })
        .await;
        Self { stream, topic }
    }

    async fn join(client: &IggyClient, topic: (Identifier, Identifier)) -> Self {
        Self::create(client, topic).await.joined(client).await
    }

    async fn joined(self, client: &IggyClient) -> Self {
        let group = named(GROUP);
        retry(|| client.join_consumer_group(&self.stream, &self.topic, &group)).await;
        wait_for_consumer_group_assignment(
            client,
            &self.stream,
            &self.topic,
            &named(GROUP),
            1,
            DEADLINE,
        )
        .await;
        self
    }

    async fn leave(&self, client: &IggyClient) {
        let group = named(GROUP);
        retry(|| client.leave_consumer_group(&self.stream, &self.topic, &group)).await;
    }

    /// One poll of the next `count` messages, committing them. Empty when
    /// the cluster refused the poll.
    async fn poll(&self, client: &IggyClient, count: u32) -> Vec<u64> {
        client
            .poll_messages(
                &self.stream,
                &self.topic,
                0,
                &Consumer::group(named(GROUP)),
                &PollingStrategy::next(),
                PollOptions {
                    count,
                    commit: CommitPolicy::OnRead,
                    ..PollOptions::default()
                },
            )
            .await
            .map(|polled| {
                polled
                    .messages
                    .iter()
                    .map(|message| message.header.offset)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Poll and commit until `count` messages arrived.
    async fn consume(&self, client: &IggyClient, count: u64) -> Vec<u64> {
        let deadline = Instant::now() + DEADLINE;
        let mut offsets = Vec::new();
        while (offsets.len() as u64) < count {
            let left = u32::try_from(count - offsets.len() as u64)
                .unwrap_or(u32::MAX)
                .min(10);
            offsets.extend(self.poll(client, left).await);
            assert!(
                Instant::now() < deadline,
                "the group read {} of {count} messages: {offsets:?}",
                offsets.len()
            );
            sleep(RETRY).await;
        }
        offsets
    }

    async fn store(&self, client: &IggyClient, offset: u64) {
        let consumer = Consumer::group(named(GROUP));
        retry(|| {
            client.store_consumer_offset(&consumer, &self.stream, &self.topic, Some(0), offset)
        })
        .await;
    }

    async fn offset(&self, client: &IggyClient) -> Option<u64> {
        client
            .get_consumer_offset(
                &Consumer::group(named(GROUP)),
                &self.stream,
                &self.topic,
                Some(0),
            )
            .await
            .ok()
            .flatten()
            .map(|offset| offset.stored_offset)
    }

    async fn wait_for_offset(&self, client: &IggyClient, expected: Option<u64>) {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let offset = self.offset(client).await;
            if offset == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the group's offset stayed {offset:?}, not {expected:?}"
            );
            sleep(RETRY).await;
        }
    }
}

async fn started_pair() -> GeoClusterPair {
    let mut pair = GeoClusterPair::builder()
        .extra_env("IGGY_GEO_POSITIONS_PUBLISH_INTERVAL", "250ms")
        .extra_env("IGGY_GEO_POSITIONS_POLL_INTERVAL", "250ms")
        .extra_env("IGGY_SHARDING_RECONCILE_PERIODIC_INTERVAL", "250ms")
        .extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s")
        .build()
        .expect("the pair");
    pair.start().await.expect("both clusters boot");
    pair
}

/// Create the geo topic of one partition in `dc-a` and wait until `dc-b`
/// adopts it.
async fn create_and_follow(pair: &GeoClusterPair) -> Ids {
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    client_a
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    let topic = pair
        .a()
        .create_geo_topic(&client_a, &named(STREAM), TOPIC, 1)
        .await
        .expect("dc-a takes the geo topic");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let stream = pair
        .b()
        .wait_for_shadow_topic(&client_b, STREAM, topic.id)
        .await
        .expect("dc-b adopts the geo topic");
    Ids {
        shadow_stream_id: stream.id,
        topic_id: topic.id,
    }
}

/// Send the messages `indexes` to the geo topic of `dc-a`, ten a send.
async fn send(client: &IggyClient, indexes: std::ops::Range<u64>) {
    let all: Vec<u64> = indexes.collect();
    for chunk in all.chunks(10) {
        let mut messages: Vec<IggyMessage> = chunk
            .iter()
            .map(|index| {
                IggyMessage::builder()
                    .payload(format!("message-{index}").into_bytes().into())
                    .build()
                    .expect("a message")
            })
            .collect();
        client
            .send_messages(
                &named(STREAM),
                &named(TOPIC),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .expect("dc-a takes the send");
    }
}

/// Wait until the shadow in `dc-b` serves the first `count` messages.
async fn wait_until_shadow_serves(client: &IggyClient, ids: Ids, count: u64) {
    let deadline = Instant::now() + DEADLINE;
    let (stream, topic) = ids.shadow();
    loop {
        let served = client
            .poll_messages(
                &stream,
                &topic,
                0,
                &Consumer::default(),
                &PollingStrategy::offset(count - 1),
                PollOptions {
                    count: 1,
                    ..PollOptions::default()
                },
            )
            .await
            .is_ok_and(|polled| !polled.messages.is_empty());
        if served {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "dc-b never served offset {}",
            count - 1
        );
        sleep(RETRY).await;
    }
}

/// Wait until the bucket of `cluster` holds `offset` as the group's
/// position on partition 0.
async fn wait_for_bucket_offset(cluster: &GeoCluster, offset: u64) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let objects = cluster
            .bucket()
            .positions_objects()
            .await
            .expect("the bucket reads");
        if objects.iter().any(|object| {
            object
                .partitions
                .get(&0)
                .and_then(|groups| groups.get(GROUP))
                .is_some_and(|position| position.value == PositionValue::Offset(offset))
        }) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the bucket of {} never held offset {offset}: {objects:?}",
            cluster.origin()
        );
        sleep(RETRY).await;
    }
}

/// Run `call` until it succeeds, as an SDK does for a retryable refusal.
async fn retry<F, Fut, T>(mut call: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, IggyError>>,
{
    let deadline = Instant::now() + DEADLINE;
    loop {
        match call().await {
            Ok(value) => return value,
            Err(error) => {
                assert!(Instant::now() < deadline, "still refused: {error}");
                sleep(RETRY).await;
            }
        }
    }
}

fn named(name: &str) -> Identifier {
    Identifier::named(name).expect("a valid identifier")
}

fn numeric(id: u32) -> Identifier {
    Identifier::numeric(id).expect("a valid identifier")
}
