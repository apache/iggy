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
/// The nodes of each cluster in the tests whose partitions have different
/// leaders, and the partitions of their geo topic.
const NODES: usize = 3;
const PARTITIONS: u32 = 3;
const PUBLISH_INTERVAL: Duration = Duration::from_millis(250);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// How long a group may take to read what a test expects, or a position to
/// reach the other cluster.
const DEADLINE: Duration = Duration::from_secs(60);
const RETRY: Duration = Duration::from_millis(50);

#[tokio::test]
async fn given_a_group_drained_on_every_node_of_one_datacenter_when_it_starts_in_the_other_should_resume_each_partition_on_the_exact_next_message()
 {
    let mut pair = started_pair(NODES).await;
    let ids = create_and_follow(&pair, PARTITIONS).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    for partition in 0..PARTITIONS {
        send(&client_a, partition, 0..100).await;
        wait_until_shadow_serves(&client_b, ids, partition, 100).await;
    }
    let group_a = Group::join(&client_a, ids.own()).await;
    let consumed: BTreeMap<u32, u64> = (0..PARTITIONS)
        .map(|partition| (partition, 30 + 7 * u64::from(partition)))
        .collect();
    for (partition, count) in &consumed {
        let read = group_a.on(*partition).consume(&client_a, *count).await;
        assert_eq!(read, (0..*count).collect::<Vec<_>>());
    }
    group_a.leave(&client_a).await;

    let mut drained = BTreeMap::new();
    for node in 0..NODES {
        let answer = drain(pair.a(), node).await;
        assert_eq!(answer["not_held"], json!([]), "node {node}: {answer}");
        let not_led: Vec<u64> = serde_json::from_value(answer["not_led"].clone())
            .expect("the partitions the node does not lead");
        if node == 0 {
            assert!(
                !not_led.is_empty(),
                "the leaders of the partitions spread over the nodes: {answer}"
            );
        }
        let positions = answer["positions"].as_array().expect("positions");
        let mut covered = not_led;
        for position in positions {
            let partition = position["partition_id"].as_u64().expect("a partition id");
            let offset = position["offset"].as_u64().expect("an offset");
            covered.push(partition);
            assert_eq!(
                drained.insert(partition, offset),
                None,
                "only the node that leads a partition drains it: {answer}"
            );
        }
        covered.sort_unstable();
        assert_eq!(
            covered,
            (0..u64::from(PARTITIONS)).collect::<Vec<_>>(),
            "node {node}: {answer}"
        );
    }
    let expected: BTreeMap<u64, u64> = consumed
        .iter()
        .map(|(partition, count)| (u64::from(*partition), count - 1))
        .collect();
    assert_eq!(drained, expected, "one drain stores each partition");
    let stored = pair
        .a()
        .bucket()
        .positions_objects()
        .await
        .expect("dc-a's bucket reads");
    for (partition, offset) in &expected {
        let partition = u32::try_from(*partition).expect("a partition id");
        assert!(
            stored.iter().any(|object| object
                .partitions
                .get(&partition)
                .and_then(|groups| groups.get(GROUP))
                == Some(&GeoPosition {
                    reset: 0,
                    value: PositionValue::Offset(*offset)
                })),
            "the drains answer once the bucket holds the positions: {stored:?}"
        );
    }
    for object in &stored {
        assert!(
            object.partitions.len() < PARTITIONS as usize,
            "node {} publishes partitions it does not lead: {object:?}",
            object.key.replica
        );
    }

    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a stops after the drains");
    let group_b = Group::create(&client_b, ids.shadow()).await;
    for (partition, count) in &consumed {
        group_b
            .on(*partition)
            .wait_for_offset(&client_b, Some(count - 1))
            .await;
    }
    let group_b = group_b.joined(&client_b).await;
    for (partition, count) in &consumed {
        assert_eq!(
            group_b.on(*partition).consume(&client_b, 10).await,
            (*count..count + 10).collect::<Vec<_>>(),
            "the group resumes partition {partition} on the next message"
        );
    }
}

#[tokio::test]
async fn given_a_partition_whose_leader_fails_when_another_node_takes_it_over_should_publish_the_position_it_had()
 {
    let mut pair = started_pair(NODES).await;
    let ids = create_and_follow(&pair, PARTITIONS).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    for partition in 0..PARTITIONS {
        send(&client_a, partition, 0..20).await;
    }
    let group_a = Group::join(&client_a, ids.own()).await;
    for partition in 0..PARTITIONS {
        group_a.on(partition).consume(&client_a, 10).await;
    }
    group_a.leave(&client_a).await;
    for partition in 0..PARTITIONS {
        wait_for_bucket_offset_on(pair.a(), partition, 9).await;
    }
    // Rounds of every node, in which a backup would store what it holds too.
    sleep(4 * PUBLISH_INTERVAL).await;
    let mut holders = BTreeMap::new();
    for partition in 0..PARTITIONS {
        let replicas = wait_for_bucket_offset_on(pair.a(), partition, 9).await;
        assert_eq!(
            replicas.len(),
            1,
            "only the leader of partition {partition} publishes it: {replicas:?}"
        );
        holders.insert(partition, replicas[0]);
    }
    let (partition, leader) = holders
        .into_iter()
        .find(|(_, replica)| *replica != 0)
        .expect("a partition led by another node than the first");

    pair.cluster_mut(GeoSide::A)
        .harness_mut()
        .kill_node(usize::from(leader))
        .expect("the leader fails");

    let deadline = Instant::now() + DEADLINE;
    loop {
        let replicas = wait_for_bucket_offset_on(pair.a(), partition, 9).await;
        if replicas.iter().any(|replica| *replica != leader) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no node took over partition {partition} from node {leader} in the bucket"
        );
        sleep(RETRY).await;
    }
}

#[tokio::test]
async fn given_a_group_that_moved_back_in_one_datacenter_when_the_other_holds_a_higher_offset_should_keep_the_move_in_both()
 {
    let pair = started_pair(1).await;
    let ids = create_and_follow(&pair, 1).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    send(&client_a, 0, 0..100).await;
    wait_until_shadow_serves(&client_b, ids, 0, 100).await;
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
    let mut pair = started_pair(1).await;
    let ids = create_and_follow(&pair, 1).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    send(&client_a, 0, 0..MESSAGES).await;
    wait_until_shadow_serves(&client_b, ids, 0, MESSAGES).await;
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
    let failed_at = Instant::now();
    pair.cluster_mut(GeoSide::A)
        .harness_mut()
        .kill_cluster()
        .expect("dc-a fails");
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
    // A reconcile pass of dc-b, the fold and the adoption commits and the
    // store calls add to the two intervals, more on a loaded host.
    let slack = Duration::from_millis(1500);
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
    let mut pair = started_pair(1).await;
    let ids = create_and_follow(&pair, 1).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    send(&client_a, 0, 0..100).await;
    wait_until_shadow_serves(&client_b, ids, 0, 100).await;
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

/// The group on one topic of one cluster, read on one partition.
struct Group {
    stream: Identifier,
    topic: Identifier,
    partition: u32,
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
        Self {
            stream,
            topic,
            partition: 0,
        }
    }

    /// The group, read on `partition`.
    fn on(&self, partition: u32) -> Self {
        Self {
            stream: self.stream.clone(),
            topic: self.topic.clone(),
            partition,
        }
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
                self.partition,
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
            client.store_consumer_offset(
                &consumer,
                &self.stream,
                &self.topic,
                Some(self.partition),
                offset,
            )
        })
        .await;
    }

    async fn offset(&self, client: &IggyClient) -> Option<u64> {
        client
            .get_consumer_offset(
                &Consumer::group(named(GROUP)),
                &self.stream,
                &self.topic,
                Some(self.partition),
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

async fn started_pair(nodes: usize) -> GeoClusterPair {
    let mut pair = GeoClusterPair::builder()
        .nodes(nodes)
        .extra_env("IGGY_GEO_POSITIONS_PUBLISH_INTERVAL", "250ms")
        .extra_env("IGGY_GEO_POSITIONS_POLL_INTERVAL", "250ms")
        .extra_env("IGGY_SHARDING_RECONCILE_PERIODIC_INTERVAL", "250ms")
        .extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s")
        .build()
        .expect("the pair");
    pair.start().await.expect("both clusters boot");
    pair
}

/// Create the geo topic of `partitions` partitions in `dc-a` and wait until
/// `dc-b` adopts it.
async fn create_and_follow(pair: &GeoClusterPair, partitions: u32) -> Ids {
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    client_a
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    let topic = pair
        .a()
        .create_geo_topic(&client_a, &named(STREAM), TOPIC, partitions)
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

/// Send the messages `indexes` to `partition` of the geo topic of `dc-a`,
/// ten a send.
async fn send(client: &IggyClient, partition: u32, indexes: std::ops::Range<u64>) {
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
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .expect("dc-a takes the send");
    }
}

/// Wait until the shadow in `dc-b` serves the first `count` messages of
/// `partition`.
async fn wait_until_shadow_serves(client: &IggyClient, ids: Ids, partition: u32, count: u64) {
    let deadline = Instant::now() + DEADLINE;
    let (stream, topic) = ids.shadow();
    loop {
        let served = client
            .poll_messages(
                &stream,
                &topic,
                partition,
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
            "dc-b never served offset {} of partition {partition}",
            count - 1
        );
        sleep(RETRY).await;
    }
}

/// Wait until the bucket of `cluster` holds `offset` as the group's
/// position on partition 0.
async fn wait_for_bucket_offset(cluster: &GeoCluster, offset: u64) {
    wait_for_bucket_offset_on(cluster, 0, offset).await;
}

/// Wait until the bucket of `cluster` holds `offset` as the group's
/// position on `partition`, and return the replicas whose objects hold it.
async fn wait_for_bucket_offset_on(cluster: &GeoCluster, partition: u32, offset: u64) -> Vec<u8> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let objects = cluster
            .bucket()
            .positions_objects()
            .await
            .expect("the bucket reads");
        let replicas: Vec<u8> = objects
            .iter()
            .filter(|object| {
                object
                    .partitions
                    .get(&partition)
                    .and_then(|groups| groups.get(GROUP))
                    .is_some_and(|position| position.value == PositionValue::Offset(offset))
            })
            .map(|object| object.key.replica)
            .collect();
        if !replicas.is_empty() {
            return replicas;
        }
        assert!(
            Instant::now() < deadline,
            "the bucket of {} never held offset {offset} of partition {partition}: {objects:?}",
            cluster.origin()
        );
        sleep(RETRY).await;
    }
}

/// Drain the group on node `node` of `cluster`, and return its answer.
async fn drain(cluster: &GeoCluster, node: usize) -> serde_json::Value {
    let address = cluster
        .harness()
        .node(node)
        .http_addr()
        .expect("the node's HTTP address");
    let http = HttpClient::login_root_no_redirect(format!("http://{address}")).await;
    let response = http
        .post_json(
            &format!("/streams/{STREAM}/topics/{TOPIC}/consumer-groups/{GROUP}/drain?timeout=30s"),
            &json!({}),
        )
        .await;
    assert_eq!(response.status(), 200, "node {node} stores the positions");
    response.json().await.expect("a drain body")
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
