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

//! One consumer group that reads a geo topic together with its shadow topics
//! through the SDK's geo consumer, on two clusters on `fs`, `dc-a` and
//! `dc-b`. The tests hold no code that looks for the shadow topics.

use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use futures::StreamExt;
use iggy::prelude::*;
use integration::harness::{GeoCluster, GeoClusterPair};
use tokio::time::{Instant, sleep, timeout};

const STREAM: &str = "orders";
const TOPIC: &str = "payments";
const GROUP: &str = "billing";
const PARTITIONS: u32 = 2;
/// Messages that each origin sends to each partition.
const MESSAGES: u32 = 50;
const BATCH: u32 = 10;
/// How long a consumer may take to read what a test expects. A shadow topic
/// serves a message once its origin published it and this cluster listed the
/// manifest.
const READ_TIMEOUT: Duration = Duration::from_secs(120);
/// How long a stored position may take to reach the other cluster.
const POSITION_TIMEOUT: Duration = Duration::from_secs(60);
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// The indexes of the messages that a consumer read, in the order it read
/// them, by the origin that wrote them and their partition.
type Read = BTreeMap<(String, u32), Vec<u32>>;

#[tokio::test]
async fn given_producers_in_both_datacenters_when_one_group_reads_the_topic_with_its_shadows_should_read_every_message_once_in_origin_order()
 {
    let pair = started_pair().await;
    create_topic(pair.a()).await;
    let topic_b = create_topic(pair.b()).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    pair.a()
        .wait_for_shadow_topic(&client_a, STREAM, topic_b.id)
        .await
        .expect("dc-a adopts the topic of dc-b");
    let mut consumer = geo_consumer(&client_a).await;

    tokio::join!(
        produce(&client_a, pair.a().origin(), 0..MESSAGES),
        produce(&client_b, pair.b().origin(), 0..MESSAGES),
    );
    let mut read = Read::new();
    consume(
        &mut consumer,
        pair.a().origin(),
        2 * PARTITIONS * MESSAGES,
        &mut read,
    )
    .await;
    consumer.shutdown().await.expect("the consumer shuts down");

    assert_read(&read, &[pair.a().origin(), pair.b().origin()]);
    let shadow = named(&pair.a().shadow_stream_name(STREAM));
    for stream in [named(STREAM), shadow] {
        for partition in 0..PARTITIONS {
            assert_eq!(
                stored_offset(&client_a, &stream, partition).await,
                Some(u64::from(MESSAGES - 1)),
                "the commits of partition {partition} reached the topic of {stream}"
            );
        }
    }
}

#[tokio::test]
async fn given_a_group_reading_the_topic_with_its_shadows_when_the_other_datacenter_creates_the_topic_later_should_read_it_without_a_restart()
 {
    let pair = started_pair().await;
    create_topic(pair.a()).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    produce(&client_a, pair.a().origin(), 0..MESSAGES).await;
    let mut consumer = geo_consumer(&client_a).await;
    let mut read = Read::new();
    consume(
        &mut consumer,
        pair.a().origin(),
        PARTITIONS * MESSAGES,
        &mut read,
    )
    .await;

    create_topic(pair.b()).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    produce(&client_b, pair.b().origin(), 0..MESSAGES).await;
    consume(
        &mut consumer,
        pair.a().origin(),
        PARTITIONS * MESSAGES,
        &mut read,
    )
    .await;
    consumer.shutdown().await.expect("the consumer shuts down");

    assert_read(&read, &[pair.a().origin(), pair.b().origin()]);
}

#[tokio::test]
async fn given_a_group_reading_the_topic_with_its_shadows_when_it_moves_to_the_other_datacenter_should_resume_each_log_on_the_next_message()
 {
    let pair = started_pair().await;
    let topic_a = create_topic(pair.a()).await;
    let topic_b = create_topic(pair.b()).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    pair.a()
        .wait_for_shadow_topic(&client_a, STREAM, topic_b.id)
        .await
        .expect("dc-a adopts the topic of dc-b");
    pair.b()
        .wait_for_shadow_topic(&client_b, STREAM, topic_a.id)
        .await
        .expect("dc-b adopts the topic of dc-a");
    tokio::join!(
        produce(&client_a, pair.a().origin(), 0..MESSAGES),
        produce(&client_b, pair.b().origin(), 0..MESSAGES),
    );
    let shadow_in_a = named(&pair.a().shadow_stream_name(STREAM));
    let shadow_in_b = named(&pair.b().shadow_stream_name(STREAM));
    wait_until_served(&client_a, &shadow_in_a).await;
    wait_until_served(&client_b, &shadow_in_b).await;
    // A group of dc-b without members takes the positions that dc-a stores.
    for stream in [named(STREAM), shadow_in_b.clone()] {
        create_group(&client_b, &stream).await;
    }
    let total = 2 * PARTITIONS * MESSAGES;
    let taken_in_a = total / 3;

    let mut read_in_a = Read::new();
    let mut consumer = geo_consumer(&client_a).await;
    consume(&mut consumer, pair.a().origin(), taken_in_a, &mut read_in_a).await;
    consumer
        .shutdown()
        .await
        .expect("the consumer in dc-a stops");
    for origin in [pair.a().origin(), pair.b().origin()] {
        assert!(
            read_in_a.keys().any(|(read, _)| read == origin),
            "dc-a read no message of {origin}: {read_in_a:?}"
        );
    }
    for (origin, stream) in [
        (pair.a().origin(), &shadow_in_b),
        (pair.b().origin(), &named(STREAM)),
    ] {
        for partition in 0..PARTITIONS {
            let committed = read_in_a
                .get(&(origin.to_owned(), partition))
                .and_then(|indexes| indexes.last())
                .map(|index| u64::from(*index));
            wait_for_stored_offset(&client_b, stream, partition, committed).await;
        }
    }
    let mut read_in_b = Read::new();
    let mut consumer = geo_consumer(&client_b).await;
    consume(
        &mut consumer,
        pair.b().origin(),
        total - taken_in_a,
        &mut read_in_b,
    )
    .await;
    consumer
        .shutdown()
        .await
        .expect("the consumer in dc-b stops");

    for origin in [pair.a().origin(), pair.b().origin()] {
        for partition in 0..PARTITIONS {
            let key = (origin.to_owned(), partition);
            let before = read_in_a.get(&key).cloned().unwrap_or_default();
            let resumed_at = u32::try_from(before.len()).expect("a count of messages");
            assert_eq!(
                before,
                (0..resumed_at).collect::<Vec<_>>(),
                "dc-a read partition {partition} of {origin} in order"
            );
            assert_eq!(
                read_in_b.get(&key).cloned().unwrap_or_default(),
                (resumed_at..MESSAGES).collect::<Vec<_>>(),
                "dc-b resumes partition {partition} of {origin} on the message after the last commit"
            );
        }
    }
}

async fn started_pair() -> GeoClusterPair {
    let mut pair = GeoClusterPair::builder()
        .extra_env("IGGY_GEO_POSITIONS_PUBLISH_INTERVAL", "250ms")
        .extra_env("IGGY_GEO_POSITIONS_POLL_INTERVAL", "250ms")
        .extra_env("IGGY_SHARDING_RECONCILE_PERIODIC_INTERVAL", "250ms")
        .extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s")
        .extra_env("IGGY_GEO_LONG_TERM_REWRITE_MAX_INTERVAL", "1s")
        .build()
        .expect("the pair");
    pair.start().await.expect("both clusters boot");
    pair
}

/// Creates the stream and its geo topic in `cluster`.
async fn create_topic(cluster: &GeoCluster) -> TopicDetails {
    let client = cluster.root_client().await.expect("a client");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    cluster
        .create_geo_topic(&client, &named(STREAM), TOPIC, PARTITIONS)
        .await
        .unwrap_or_else(|error| panic!("{} never took the geo topic: {error}", cluster.origin()))
}

/// The group's geo consumer in the cluster of `client`, over the topic and
/// its shadow topics. It commits each message itself.
async fn geo_consumer(client: &IggyClient) -> IggyGeoConsumer {
    let mut consumer = client
        .consumer_group(GROUP, STREAM, TOPIC)
        .expect("a consumer group")
        .poll_options(PollOptions {
            count: BATCH,
            commit: CommitPolicy::Manual,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
        .with_shadows()
        .discovery_interval(NonZeroIggyDuration::new(DISCOVERY_INTERVAL).expect("an interval"))
        .build();
    consumer.init().await.expect("the geo consumer starts");
    consumer
}

/// Reads `count` messages with `consumer` of the cluster of `local`, commits
/// each one, and adds it to `read`. The origin that each message names must
/// be the one in its payload.
async fn consume(consumer: &mut IggyGeoConsumer, local: &str, count: u32, read: &mut Read) {
    let deadline = Instant::now() + READ_TIMEOUT;
    let mut taken = 0;
    while taken < count {
        let left = deadline.saturating_duration_since(Instant::now());
        let item = timeout(left, consumer.next())
            .await
            .unwrap_or_else(|_| panic!("{taken} of {count} messages in {READ_TIMEOUT:?}: {read:?}"))
            .expect("the consumer goes on");
        let received = match item {
            Ok(received) => received,
            Err(error) => {
                eprintln!("a read error, which the consumer retries: {error}");
                continue;
            }
        };
        let (origin, partition, index) = parse(&received.message.payload);
        assert_eq!(
            received.origin.name().unwrap_or(local),
            origin,
            "the origin of {received:?}"
        );
        assert_eq!(received.origin.is_local(), origin == local);
        assert_eq!(received.partition_id, partition);
        consumer
            .commit(received.checkpoint())
            .await
            .expect("the commit is acknowledged");
        read.entry((origin, partition)).or_default().push(index);
        taken += 1;
    }
}

/// `read` holds every message of each origin of `origins`, each once and in
/// order, and no other log.
fn assert_read(read: &Read, origins: &[&str]) {
    for origin in origins {
        for partition in 0..PARTITIONS {
            assert_eq!(
                read.get(&((*origin).to_owned(), partition)),
                Some(&(0..MESSAGES).collect::<Vec<_>>()),
                "partition {partition} of {origin}"
            );
        }
    }
    assert_eq!(
        read.len(),
        origins.len() * PARTITIONS as usize,
        "the logs read: {:?}",
        read.keys().collect::<Vec<_>>()
    );
}

/// Sends the messages `indexes` of `origin` to each partition of the geo
/// topic, [`BATCH`] in a send.
async fn produce(client: &IggyClient, origin: &str, indexes: Range<u32>) {
    for partition in 0..PARTITIONS {
        let mut index = indexes.start;
        while index < indexes.end {
            let end = (index + BATCH).min(indexes.end);
            let mut messages: Vec<IggyMessage> = (index..end)
                .map(|index| {
                    IggyMessage::builder()
                        .payload(payload(origin, partition, index).into_bytes().into())
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
                .unwrap_or_else(|error| panic!("{origin} refused a send: {error}"));
            index = end;
        }
    }
}

/// Waits until the shadow topic in `stream` serves every message of each
/// partition.
async fn wait_until_served(client: &IggyClient, stream: &Identifier) {
    let deadline = Instant::now() + READ_TIMEOUT;
    for partition in 0..PARTITIONS {
        loop {
            let served = client
                .poll_messages(
                    stream,
                    &named(TOPIC),
                    partition,
                    &Consumer::default(),
                    &PollingStrategy::offset(u64::from(MESSAGES - 1)),
                    PollOptions {
                        count: 1,
                        ..PollOptions::default()
                    },
                )
                .await
                .is_ok_and(|polled| !polled.messages.is_empty());
            if served {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{stream} never served the last message of partition {partition}"
            );
            sleep(POLL_INTERVAL).await;
        }
    }
}

async fn create_group(client: &IggyClient, stream: &Identifier) {
    let deadline = Instant::now() + POSITION_TIMEOUT;
    loop {
        match client
            .create_consumer_group(stream, &named(TOPIC), GROUP)
            .await
        {
            Ok(_) | Err(IggyError::ConsumerGroupNameAlreadyExists(..)) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "the group on {stream} was never created: {error}"
                );
                sleep(POLL_INTERVAL).await;
            }
        }
    }
}

/// The offset that the group stored on `partition` of the topic in `stream`.
async fn stored_offset(client: &IggyClient, stream: &Identifier, partition: u32) -> Option<u64> {
    client
        .get_consumer_offset(
            &Consumer::group(named(GROUP)),
            stream,
            &named(TOPIC),
            Some(partition),
        )
        .await
        .expect("read the group's offset")
        .map(|offset| offset.stored_offset)
}

async fn wait_for_stored_offset(
    client: &IggyClient,
    stream: &Identifier,
    partition: u32,
    expected: Option<u64>,
) {
    let deadline = Instant::now() + POSITION_TIMEOUT;
    loop {
        let stored = stored_offset(client, stream, partition).await;
        if stored == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the group's offset of partition {partition} of {stream} stayed {stored:?}, not {expected:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

fn payload(origin: &str, partition: u32, index: u32) -> String {
    format!("geo-msg:{origin}:{partition}:{index:05}")
}

/// The origin, the partition and the index that a payload of [`payload`]
/// names.
fn parse(payload: &[u8]) -> (String, u32, u32) {
    let text = std::str::from_utf8(payload).expect("a text payload");
    let mut parts = text.split(':').skip(1);
    let mut next = || {
        parts
            .next()
            .unwrap_or_else(|| panic!("a geo payload: {text}"))
    };
    let origin = next().to_owned();
    let partition = next().parse().expect("a partition");
    let index = next().parse().expect("an index");
    (origin, partition, index)
}

fn named(name: &str) -> Identifier {
    Identifier::named(name).expect("a valid identifier")
}
