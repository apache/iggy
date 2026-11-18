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

//! Two independent clusters on this host, `dc-a` and `dc-b`, with the same
//! geo topic each. Each keeps its topic in an `fs` bucket of its own and
//! reads the other's bucket as its geo peer.

use std::collections::BTreeSet;
use std::fs::File;
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use iggy::prelude::*;
use iggy_object_store::{LongTermKey, SegmentKey, SegmentObject, TopicObject, TopicState};
use integration::harness::geo_clusters::{READ_PASSES, READ_PASSES_TIMEOUT};
use integration::harness::{Bucket, GeoCluster, GeoClusterPair, GeoSide};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

const STREAM: &str = "orders";
const TOPIC: &str = "payments";
const PARTITIONS: u32 = 2;
/// Messages that one round sends to each partition.
const MESSAGES: u32 = 50;
const BATCH: u32 = 10;
/// How long a cluster may take to verify the identity of its peer: a check
/// right after its first committed operation, and one every 5 s until one
/// finds the peer's claim.
const VERIFY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a cluster may take to list a message in a manifest: a long-term
/// rewrite of the partition, then the publish of its manifest.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a deleted topic may take to leave the peer: the origin writes
/// the tombstone on its sweep, once a minute, and the peer reads a known
/// object again every 30 s.
const REMOVE_TIMEOUT: Duration = Duration::from_secs(180);
/// How long a published message may take to reach a consumer of the shadow
/// topic.
const FOLLOW_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// The ids of the geo topic that one cluster created.
#[derive(Debug, Clone, Copy)]
struct GeoTopic {
    stream_id: u32,
    topic_id: u32,
}

#[tokio::test]
async fn given_two_clusters_that_read_each_other_when_each_creates_a_geo_topic_should_verify_the_peer_and_adopt_its_topic()
 {
    let mut pair = started_pair().await;
    for side in GeoSide::BOTH {
        wait_for_peer_gauges(pair.cluster(side), (1, 0)).await;
    }

    let created = create_topics(&pair).await;

    for side in GeoSide::BOTH {
        let cluster = pair.cluster(side);
        let peer_topic = created[side.peer().index()];
        let client = cluster.root_client().await.expect("a client");
        let shadow = cluster
            .wait_for_shadow_topic(&client, STREAM, peer_topic.topic_id)
            .await
            .expect("the peer's topic is adopted");
        let topic = client
            .get_topic(&numeric(shadow.id), &numeric(peer_topic.topic_id))
            .await
            .expect("get the shadow topic")
            .expect("the shadow topic exists");
        assert_eq!(topic.name, TOPIC, "on {}", cluster.origin());
        let partition_ids: Vec<u32> = topic
            .partitions
            .iter()
            .map(|partition| partition.id)
            .collect();
        assert_eq!(partition_ids, [0, 1], "the peer's partitions");
        let own = client
            .get_topic(&named(STREAM), &named(TOPIC))
            .await
            .expect("get the own topic")
            .expect("the own topic stays");
        assert_eq!(own.id, created[side.index()].topic_id);
    }
    let [uid_a, uid_b] = [bucket_owner(pair.a()).await, bucket_owner(pair.b()).await];
    assert_ne!(uid_a, uid_b, "each bucket is claimed by its own cluster");
    for side in GeoSide::BOTH {
        let cluster = pair.cluster(side);
        let objects = cluster.bucket().topic_objects().await.expect("objects");
        assert_eq!(objects.len(), 1, "{}: {objects:?}", cluster.origin());
        assert_eq!(objects[0].partition_count, PARTITIONS);
        assert_eq!(
            objects[0].key.topic_id,
            u64::from(created[side.index()].topic_id)
        );
    }
    assert_only_peers_read(&mut pair).await;
}

#[tokio::test]
async fn given_the_same_geo_topic_in_both_clusters_when_both_produce_at_once_should_number_each_on_its_own_and_keep_each_origins_objects_in_its_bucket()
 {
    let mut pair = started_pair().await;
    let created = create_topics(&pair).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");

    tokio::join!(
        produce(&client_a, pair.a().origin(), 0..MESSAGES),
        produce(&client_b, pair.b().origin(), 0..MESSAGES),
    );

    for (side, client) in [(GeoSide::A, &client_a), (GeoSide::B, &client_b)] {
        let cluster = pair.cluster(side);
        assert_reads_own(client, cluster.origin(), MESSAGES).await;
        let object = wait_for_published(cluster, created[side.index()], MESSAGES).await;
        assert_bucket_holds_only_its_own(cluster, &object, MESSAGES).await;
    }
    assert_only_peers_read(&mut pair).await;
}

#[tokio::test]
async fn given_two_three_node_clusters_when_both_produce_to_the_same_geo_topic_should_adopt_each_others_and_keep_each_origins_objects_in_its_bucket()
 {
    let mut pair = GeoClusterPair::builder()
        .nodes(3)
        .build()
        .expect("the pair");
    pair.start().await.expect("both clusters boot");
    let created = create_topics(&pair).await;

    for side in GeoSide::BOTH {
        let cluster = pair.cluster(side);
        let client = cluster.root_client().await.expect("a client");
        produce(&client, cluster.origin(), 0..MESSAGES).await;
        cluster
            .wait_for_shadow_topic(&client, STREAM, created[side.peer().index()].topic_id)
            .await
            .expect("the peer's topic is adopted");
        assert_reads_own(&client, cluster.origin(), MESSAGES).await;
        let object = wait_for_published(cluster, created[side.index()], MESSAGES).await;
        assert_bucket_holds_only_its_own(cluster, &object, MESSAGES).await;
    }
    assert_only_peers_read(&mut pair).await;
}

#[tokio::test]
async fn given_two_clusters_when_one_stops_and_returns_should_keep_taking_sends_and_publish_all_it_took_without_writing_to_the_peer()
 {
    let mut pair = started_pair().await;
    let created = create_topics(&pair).await;
    let topic_a = created[GeoSide::A.index()];
    let topic_b = created[GeoSide::B.index()];
    {
        let client_a = pair.a().root_client().await.expect("a client of dc-a");
        produce(&client_a, pair.a().origin(), 0..MESSAGES).await;
    }
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    produce(&client_b, pair.b().origin(), 0..MESSAGES).await;
    pair.b()
        .wait_for_shadow_topic(&client_b, STREAM, topic_a.topic_id)
        .await
        .expect("dc-b adopts the topic of dc-a");

    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a stops");
    let crashed_put = plant_crashed_put(pair.a().bucket());
    let watch = pair
        .a()
        .bucket()
        .watch()
        .expect("a watch on the bucket of dc-a");

    produce(&client_b, pair.b().origin(), MESSAGES..2 * MESSAGES).await;
    assert_reads_own(&client_b, pair.b().origin(), 2 * MESSAGES).await;
    wait_for_published(pair.b(), topic_b, 2 * MESSAGES).await;
    pair.b()
        .wait_for_shadow_topic(&client_b, STREAM, topic_a.topic_id)
        .await
        .expect("dc-b keeps the shadow of the stopped dc-a");
    watch
        .wait_for_read_passes(READ_PASSES, READ_PASSES_TIMEOUT)
        .await
        .expect("dc-b reads the bucket of the stopped dc-a");
    wait_for_stale_gauge(pair.b(), 0).await;
    assert_eq!(watch.changes(), [], "dc-b changed the bucket of dc-a");
    assert!(
        crashed_put.exists(),
        "dc-b removed the scratch file of a put of dc-a"
    );
    drop(watch);
    std::fs::remove_file(&crashed_put).expect("remove the scratch file");

    pair.cluster_mut(GeoSide::A)
        .start()
        .await
        .expect("dc-a returns");
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    assert_reads_own(&client_a, pair.a().origin(), MESSAGES).await;
    produce(&client_a, pair.a().origin(), MESSAGES..2 * MESSAGES).await;
    assert_reads_own(&client_a, pair.a().origin(), 2 * MESSAGES).await;
    let object = wait_for_published(pair.a(), topic_a, 2 * MESSAGES).await;
    assert_bucket_holds_only_its_own(pair.a(), &object, 2 * MESSAGES).await;
    pair.b()
        .wait_for_shadow_topic(&client_b, STREAM, topic_a.topic_id)
        .await
        .expect("dc-b follows dc-a again");
    wait_for_stale_gauge(pair.b(), 0).await;

    let changes = pair
        .changes_while_stopped(GeoSide::B)
        .await
        .expect("dc-a reads the bucket of the stopped dc-b");
    assert_eq!(changes, [], "dc-a changed the bucket of dc-b");
}

#[tokio::test]
async fn given_a_geo_topic_that_the_peer_adopted_when_its_origin_deletes_it_should_leave_a_tombstone_and_the_peer_should_retire_the_shadow()
 {
    let mut pair = started_pair().await;
    let created = create_topics(&pair).await;
    let topic_a = created[GeoSide::A.index()];
    let topic_b = created[GeoSide::B.index()];
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    pair.a()
        .wait_for_shadow_topic(&client_a, STREAM, topic_b.topic_id)
        .await
        .expect("dc-a adopts the topic of dc-b");
    pair.b()
        .wait_for_shadow_topic(&client_b, STREAM, topic_a.topic_id)
        .await
        .expect("dc-b adopts the topic of dc-a");
    let live = live_topic_object(pair.a().bucket(), topic_a)
        .await
        .expect("the topic object of dc-a");

    client_a
        .delete_topic(&named(STREAM), &named(TOPIC))
        .await
        .expect("dc-a deletes its geo topic");

    let deadline = Instant::now() + REMOVE_TIMEOUT;
    loop {
        let objects = pair.a().bucket().topic_objects().await.expect("objects");
        let tombstone = objects.iter().find(|object| object.key == live.key);
        if let Some(object) = tombstone
            && matches!(object.state, TopicState::Tombstone { .. })
        {
            assert_eq!(object.origin, pair.a().origin());
            assert_eq!(object.cluster_uid, live.cluster_uid);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dc-a never wrote the tombstone: {tombstone:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
    let shadow_stream = named(&pair.b().shadow_stream_name(STREAM));
    while client_b
        .get_stream(&shadow_stream)
        .await
        .expect("get the shadow stream of dc-b")
        .is_some()
    {
        assert!(
            Instant::now() < deadline,
            "dc-b kept the shadow of the deleted topic"
        );
        sleep(POLL_INTERVAL).await;
    }

    let own = client_b
        .get_topic(&named(STREAM), &named(TOPIC))
        .await
        .expect("get the topic of dc-b")
        .expect("the topic of dc-b stays");
    assert_eq!(own.id, topic_b.topic_id);
    produce(&client_b, pair.b().origin(), 0..BATCH).await;
    pair.a()
        .wait_for_shadow_topic(&client_a, STREAM, topic_b.topic_id)
        .await
        .expect("dc-a keeps the shadow of the topic of dc-b");
    assert_only_peers_read(&mut pair).await;
}

#[tokio::test]
async fn given_the_same_geo_topic_in_both_clusters_when_both_produce_at_once_should_let_consumers_read_the_local_and_the_shadow_topic_once_in_origin_order()
 {
    let mut pair = started_pair().await;
    let created = create_topics(&pair).await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");

    tokio::join!(
        produce(&client_a, pair.a().origin(), 0..MESSAGES),
        produce(&client_b, pair.b().origin(), 0..MESSAGES),
    );

    for (side, client) in [(GeoSide::A, &client_a), (GeoSide::B, &client_b)] {
        let cluster = pair.cluster(side);
        let peer_topic = created[side.peer().index()];
        wait_for_published(pair.cluster(side.peer()), peer_topic, MESSAGES).await;
        assert_consumes(
            client,
            &named(STREAM),
            &named(TOPIC),
            cluster.origin(),
            0..MESSAGES,
        )
        .await;
        let shadow = cluster
            .wait_for_shadow_topic(client, STREAM, peer_topic.topic_id)
            .await
            .expect("the peer's topic is adopted");
        assert_consumes(
            client,
            &numeric(shadow.id),
            &numeric(peer_topic.topic_id),
            cluster.peer_origin(),
            0..MESSAGES,
        )
        .await;
    }
    assert_only_peers_read(&mut pair).await;
}

#[tokio::test]
async fn given_a_cluster_that_published_its_messages_when_it_stops_should_let_the_peer_serve_them_from_the_shadow()
 {
    let mut pair = started_pair().await;
    let created = create_topics(&pair).await;
    let topic_a = created[GeoSide::A.index()];
    {
        let client_a = pair.a().root_client().await.expect("a client of dc-a");
        produce(&client_a, pair.a().origin(), 0..MESSAGES).await;
    }
    wait_for_published(pair.a(), topic_a, MESSAGES).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let shadow = pair
        .b()
        .wait_for_shadow_topic(&client_b, STREAM, topic_a.topic_id)
        .await
        .expect("dc-b adopts the topic of dc-a");

    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a stops");
    let watch = pair
        .a()
        .bucket()
        .watch()
        .expect("a watch on the bucket of dc-a");

    produce(&client_b, pair.b().origin(), 0..MESSAGES).await;
    assert_reads_own(&client_b, pair.b().origin(), MESSAGES).await;
    assert_consumes(
        &client_b,
        &numeric(shadow.id),
        &numeric(topic_a.topic_id),
        pair.a().origin(),
        0..MESSAGES,
    )
    .await;
    watch
        .wait_for_read_passes(READ_PASSES, READ_PASSES_TIMEOUT)
        .await
        .expect("dc-b reads the bucket of the stopped dc-a");
    assert_eq!(watch.changes(), [], "dc-b changed the bucket of dc-a");
}

#[tokio::test]
async fn given_a_stopped_cluster_when_it_returns_should_bring_what_it_took_before_and_after_to_the_peers_shadow()
 {
    let mut pair = started_pair().await;
    let created = create_topics(&pair).await;
    let topic_a = created[GeoSide::A.index()];
    {
        let client_a = pair.a().root_client().await.expect("a client of dc-a");
        produce(&client_a, pair.a().origin(), 0..MESSAGES).await;
    }
    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a stops");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    produce(&client_b, pair.b().origin(), 0..MESSAGES).await;

    pair.cluster_mut(GeoSide::A)
        .start()
        .await
        .expect("dc-a returns");
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    produce(&client_a, pair.a().origin(), MESSAGES..2 * MESSAGES).await;
    wait_for_published(pair.a(), topic_a, 2 * MESSAGES).await;

    let shadow = pair
        .b()
        .wait_for_shadow_topic(&client_b, STREAM, topic_a.topic_id)
        .await
        .expect("dc-b follows dc-a");
    assert_consumes(
        &client_b,
        &numeric(shadow.id),
        &numeric(topic_a.topic_id),
        pair.a().origin(),
        0..2 * MESSAGES,
    )
    .await;
    assert_only_peers_read(&mut pair).await;
}

async fn started_pair() -> GeoClusterPair {
    let mut pair = GeoClusterPair::builder().build().expect("the pair");
    pair.start().await.expect("both clusters boot");
    pair
}

/// Creates the stream and the geo topic of the same names in both clusters,
/// and returns their ids by side.
async fn create_topics(pair: &GeoClusterPair) -> [GeoTopic; 2] {
    let mut created = Vec::with_capacity(2);
    for side in GeoSide::BOTH {
        let cluster = pair.cluster(side);
        let client = cluster.root_client().await.expect("a client");
        let stream = client
            .create_stream(STREAM, &StreamCreateOptions::default())
            .await
            .expect("create the stream");
        let topic = cluster
            .create_geo_topic(&client, &named(STREAM), TOPIC, PARTITIONS)
            .await
            .unwrap_or_else(|error| {
                panic!("{} never took the geo topic: {error}", cluster.origin())
            });
        created.push(GeoTopic {
            stream_id: stream.id,
            topic_id: topic.id,
        });
    }
    [created[0], created[1]]
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

/// Each partition of the geo topic of `origin` holds its first `count`
/// messages at offsets 0 to `count - 1`, in the order they were sent.
async fn assert_reads_own(client: &IggyClient, origin: &str, count: u32) {
    for partition in 0..PARTITIONS {
        let mut payloads = Vec::new();
        loop {
            let polled = client
                .poll_messages(
                    &named(STREAM),
                    &named(TOPIC),
                    partition,
                    &Consumer::default(),
                    &PollingStrategy::offset(payloads.len() as u64),
                    PollOptions {
                        count: 100,
                        ..PollOptions::default()
                    },
                )
                .await
                .unwrap_or_else(|error| panic!("a poll of {origin}: {error}"));
            if polled.messages.is_empty() {
                break;
            }
            for message in &polled.messages {
                assert_eq!(message.header.offset, payloads.len() as u64);
                payloads.push(String::from_utf8_lossy(&message.payload).into_owned());
            }
        }
        assert_eq!(
            payloads,
            payloads_of(origin, partition, 0..count),
            "partition {partition} of {origin}"
        );
    }
}

/// A consumer reads each partition of `topic` from where it stopped and gets
/// the messages `indexes` of `origin`, each once and in the order `origin`
/// took them, and nothing after them.
async fn assert_consumes(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    origin: &str,
    indexes: Range<u32>,
) {
    let consumer = Consumer::new(named("reader"));
    let options = PollOptions {
        count: BATCH,
        commit: CommitPolicy::OnRead,
        ..PollOptions::default()
    };
    for partition in 0..PARTITIONS {
        let expected = payloads_of(origin, partition, indexes.clone());
        let mut received = Vec::with_capacity(expected.len());
        let deadline = Instant::now() + FOLLOW_TIMEOUT;
        while received.len() < expected.len() {
            let polled = client
                .poll_messages(
                    stream,
                    topic,
                    partition,
                    &consumer,
                    &PollingStrategy::next(),
                    options,
                )
                .await
                .unwrap_or_else(|error| panic!("a poll of the topic of {origin}: {error}"));
            if polled.messages.is_empty() {
                assert!(
                    Instant::now() < deadline,
                    "{} of the {} messages of {origin} reached partition {partition}",
                    received.len(),
                    expected.len()
                );
                sleep(POLL_INTERVAL).await;
            }
            received.extend(
                polled
                    .messages
                    .iter()
                    .map(|message| String::from_utf8_lossy(&message.payload).into_owned()),
            );
        }
        assert_eq!(received, expected, "partition {partition} of {origin}");
        let rest = client
            .poll_messages(
                stream,
                topic,
                partition,
                &consumer,
                &PollingStrategy::next(),
                options,
            )
            .await
            .expect("a poll past the end");
        assert!(rest.messages.is_empty(), "messages past the end");
    }
}

/// Waits until a manifest of each partition of `topic` in the bucket of
/// `cluster` lists its first `count` messages, and returns the topic's
/// object.
async fn wait_for_published(cluster: &GeoCluster, topic: GeoTopic, count: u32) -> TopicObject {
    let last = u64::from(count) - 1;
    let deadline = Instant::now() + PUBLISH_TIMEOUT;
    loop {
        if let Some(object) = live_topic_object(cluster.bucket(), topic).await {
            let manifests = cluster.bucket().manifests().await.expect("manifests");
            let floors: Vec<Option<u64>> = (0..PARTITIONS)
                .map(|partition| {
                    let prefix = object.key.partition_prefix(u64::from(partition));
                    manifests
                        .iter()
                        .find(|manifest| manifest.prefix == prefix)
                        .and_then(|manifest| manifest.floor)
                })
                .collect();
            if floors.iter().all(|floor| *floor == Some(last)) {
                return object;
            }
            assert!(
                Instant::now() < deadline,
                "{} published its partitions up to {floors:?}, not {last}",
                cluster.origin()
            );
        } else {
            assert!(
                Instant::now() < deadline,
                "{} stored no object of its geo topic",
                cluster.origin()
            );
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// The bucket of `cluster` holds every message that its origin took,
/// `count` per partition, and none of its peer. Its topic objects name its
/// origin and cluster, and each of its manifests is one of a partition of
/// `object` and lists only segments in this bucket.
async fn assert_bucket_holds_only_its_own(cluster: &GeoCluster, object: &TopicObject, count: u32) {
    let bucket = cluster.bucket();
    let (origin, peer) = (cluster.origin(), cluster.peer_origin());
    let files = bucket.files().expect("the files of the bucket");
    let mut own = BTreeSet::new();
    for (path, bytes) in &files {
        assert!(
            messages_in(bytes, peer).is_empty(),
            "{path:?} in the bucket of {origin} holds messages of {peer}"
        );
        own.extend(messages_in(bytes, origin));
    }
    let missing: Vec<String> = (0..PARTITIONS)
        .flat_map(|partition| payloads_of(origin, partition, 0..count))
        .filter(|message| !own.contains(message))
        .collect();
    assert!(
        missing.is_empty(),
        "messages of {origin} not in its bucket: {missing:?}"
    );

    let cluster_uid = bucket_owner(cluster).await;
    for topic_object in bucket.topic_objects().await.expect("topic objects") {
        assert_eq!(
            (topic_object.origin.as_str(), topic_object.cluster_uid),
            (origin, cluster_uid),
            "a topic object in the bucket of {origin}"
        );
    }
    for manifest in bucket.manifests().await.expect("manifests") {
        assert!(
            (0..PARTITIONS)
                .any(|partition| manifest.prefix == object.key.partition_prefix(partition.into())),
            "the bucket of {origin} holds a manifest of {:?}",
            manifest.prefix
        );
        for segment in &manifest.segments {
            let key = if manifest.segments_keyed_by_both_offsets {
                bucket.layout().long_term_key(&LongTermKey {
                    prefix: manifest.prefix,
                    start_offset: segment.start_offset,
                    end_offset: segment.end_offset,
                    object: SegmentObject::Log,
                })
            } else {
                bucket.layout().segment_key(&SegmentKey {
                    prefix: manifest.prefix,
                    start_offset: segment.start_offset,
                    object: SegmentObject::Log,
                })
            };
            let log = bucket
                .object(&key)
                .await
                .expect("read a listed segment")
                .unwrap_or_else(|| panic!("{key} that a manifest of {origin} lists is missing"));
            assert!(
                messages_in(&log, peer).is_empty(),
                "{key} holds messages of {peer}"
            );
        }
    }
}

/// The live object of `topic` in `bucket`.
async fn live_topic_object(bucket: &Bucket, topic: GeoTopic) -> Option<TopicObject> {
    bucket
        .topic_objects()
        .await
        .expect("topic objects")
        .into_iter()
        .find(|object| {
            object.key.stream_id == u64::from(topic.stream_id)
                && object.key.topic_id == u64::from(topic.topic_id)
                && object.state == TopicState::Live
        })
}

/// The cluster that claimed the bucket of `cluster`.
async fn bucket_owner(cluster: &GeoCluster) -> u128 {
    cluster
        .bucket()
        .cluster_uid()
        .await
        .expect("the identity of the bucket")
        .unwrap_or_else(|| panic!("{} never claimed its bucket", cluster.origin()))
}

/// Leaves what a put that crashed an hour ago leaves in `bucket`: a scratch
/// file beside the topic objects, which a listing of them walks past.
fn plant_crashed_put(bucket: &Bucket) -> PathBuf {
    let path = bucket
        .path()
        .join(bucket.layout().topics_prefix())
        .join(".crashed.0123456789abcdef0123456789abcdef");
    let file = File::create(&path).expect("a scratch file");
    file.set_modified(SystemTime::now() - Duration::from_hours(2))
        .expect("an old modification time");
    path
}

/// Stops each cluster in turn while the other reads its bucket, and asserts
/// that the reader changed nothing in it.
async fn assert_only_peers_read(pair: &mut GeoClusterPair) {
    for side in GeoSide::BOTH {
        let changes = pair
            .changes_while_stopped(side)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{} never read the bucket of the stopped {}: {error}",
                    pair.cluster(side.peer()).origin(),
                    pair.cluster(side).origin()
                )
            });
        assert_eq!(
            changes,
            [],
            "{} changed the bucket of {}",
            pair.cluster(side.peer()).origin(),
            pair.cluster(side).origin()
        );
    }
}

/// Waits until `cluster` reports its peer verified and reachable as given.
async fn wait_for_peer_gauges(cluster: &GeoCluster, expected: (u64, u64)) {
    let http = HttpClient::login_root(cluster.harness()).await;
    let deadline = Instant::now() + VERIFY_TIMEOUT;
    loop {
        let exposition = scrape(&http).await;
        let peer = cluster.peer_origin();
        let found = (
            gauge(&exposition, "geo_peer_identity_verified", peer),
            gauge(&exposition, "geo_peer_unreachable", peer),
        );
        if found == (Some(expected.0), Some(expected.1)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} reports its peer {peer} as {found:?}, not {expected:?}",
            cluster.origin()
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Waits until `cluster` counts `expected` stale shadow topics of its peer.
async fn wait_for_stale_gauge(cluster: &GeoCluster, expected: u64) {
    let http = HttpClient::login_root(cluster.harness()).await;
    let deadline = Instant::now() + VERIFY_TIMEOUT;
    loop {
        let found = gauge(
            &scrape(&http).await,
            "geo_shadow_topics_stale",
            cluster.peer_origin(),
        );
        if found == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} counts {found:?} stale shadow topics, not {expected}",
            cluster.origin()
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn scrape(http: &HttpClient) -> String {
    let response = http.get("/metrics").await;
    assert!(response.status().is_success(), "/metrics scrape failed");
    response.text().await.expect("the metrics text")
}

/// The value of the `name` series of the peer `origin`.
fn gauge(exposition: &str, name: &str, origin: &str) -> Option<u64> {
    let series = format!("{name}{{");
    let label = format!("origin=\"{origin}\"");
    exposition
        .lines()
        .filter(|line| line.starts_with(&series))
        .find(|line| line.contains(&label))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}

fn payload(origin: &str, partition: u32, index: u32) -> String {
    format!("geo-msg:{origin}:{partition}:{index:05}")
}

fn payloads_of(origin: &str, partition: u32, indexes: Range<u32>) -> Vec<String> {
    indexes
        .map(|index| payload(origin, partition, index))
        .collect()
}

/// Every message of `origin` whose payload `bytes` hold.
fn messages_in(bytes: &[u8], origin: &str) -> BTreeSet<String> {
    let marker = format!("geo-msg:{origin}:");
    let len = payload(origin, 0, 0).len();
    let mut found = BTreeSet::new();
    let mut start = 0;
    while let Some(at) = bytes[start..]
        .windows(marker.len())
        .position(|window| window == marker.as_bytes())
    {
        let begin = start + at;
        if let Some(message) = bytes.get(begin..begin + len) {
            found.insert(String::from_utf8_lossy(message).into_owned());
        }
        start = begin + 1;
    }
    found
}

fn named(name: &str) -> Identifier {
    Identifier::named(name).expect("a valid identifier")
}

fn numeric(id: u32) -> Identifier {
    Identifier::numeric(id).expect("a valid identifier")
}
