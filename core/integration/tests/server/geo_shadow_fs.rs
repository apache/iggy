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

//! Two clusters on this host: `dc-a` keeps its geo topics in an `fs` store,
//! and `dc-b` reads that store as its peer and serves the topics as shadow
//! streams.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use iggy::prelude::*;
use iggy_object_store::{
    FsStore, KeyLayout, ListRequest, ObjectStore, PartitionManifest, PutBody, PutOptions,
};
use integration::harness::{TestBinary, TestHarness, TestServerConfig};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

const STREAM: &str = "orders";
const SHADOW_STREAM: &str = "__shadow_dc-a_orders";
const LOCAL_TOPIC: &str = "local-only";
const GEO_TOPIC: &str = "payments";
const PARTITIONS: u32 = 2;
/// How long `dc-a` may take to accept a geo topic: its node claims the store
/// first.
const READY_DEADLINE: Duration = Duration::from_secs(60);
/// How long `dc-b` may take to adopt a topic: the identity check of its peer,
/// then a pass that lists and reads the peer's objects.
const ADOPT_DEADLINE: Duration = Duration::from_secs(60);
/// How long a deleted topic may take to leave `dc-b`: the sweep of `dc-a`
/// that writes the tombstone runs once a minute, `dc-b` reads a known object
/// again every 30 s, and the tombstone grace of `dc-b` is two seconds.
const REMOVE_DEADLINE: Duration = Duration::from_secs(180);
/// How long the stale gauge may take to move: one listing.
const STALE_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// How long a message sent to `dc-a` may take to reach `dc-b`: the rewrite
/// of `dc-a` into a long-term segment waits up to 10 s, and `dc-b` lists
/// the peer's manifests every 5 s.
const FOLLOW_DEADLINE: Duration = Duration::from_secs(90);
const FIRST_SENDS: u64 = 30;
const SECOND_SENDS: u64 = 20;

#[tokio::test]
async fn given_a_geo_topic_in_a_peer_when_followed_should_adopt_it_refuse_writes_and_drop_it_after_its_delete()
 {
    let store = tempfile::tempdir().expect("a directory for the store of dc-a");
    let mut dc_a = origin_cluster(store.path());
    dc_a.start().await.expect("dc-a boots");
    let origin = dc_a.tcp_root_client().await.expect("a client of dc-a");
    let geo_topic_id = create_topics(&origin).await;

    let mut dc_b = follower_cluster(store.path(), 1);
    dc_b.start().await.expect("dc-b boots");
    let follower = dc_b.tcp_root_client().await.expect("a client of dc-b");
    follower
        .create_stream("local", &StreamCreateOptions::default())
        .await
        .expect("a stream of dc-b");

    let shadow = wait_for_shadow_topic(&follower, geo_topic_id).await;
    assert!(
        shadow.topics.iter().all(|topic| topic.name != LOCAL_TOPIC),
        "a normal topic of the peer is not adopted: {:?}",
        shadow.topics
    );
    let topic = follower
        .get_topic(
            &named(SHADOW_STREAM),
            &Identifier::numeric(geo_topic_id).unwrap(),
        )
        .await
        .expect("get the shadow topic")
        .expect("the shadow topic exists");
    assert_eq!(topic.name, GEO_TOPIC);
    let partition_ids: Vec<u32> = topic
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect();
    assert_eq!(partition_ids, [0, 1], "the peer's partition ids");

    assert_client_writes_are_refused(&follower, geo_topic_id).await;
    assert_reads_and_groups_work(&follower, geo_topic_id).await;
    assert_local_topics_work(&follower).await;

    origin
        .delete_topic(&named(STREAM), &named(GEO_TOPIC))
        .await
        .expect("delete the geo topic in dc-a");
    let deadline = Instant::now() + REMOVE_DEADLINE;
    while follower
        .get_stream(&named(SHADOW_STREAM))
        .await
        .expect("get the shadow stream")
        .is_some()
    {
        assert!(
            Instant::now() < deadline,
            "dc-b kept the shadow stream after the topic left dc-a"
        );
        sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test]
async fn given_a_followed_peer_when_its_store_goes_away_and_back_should_mark_its_shadow_topics_stale_and_following()
 {
    let store = tempfile::tempdir().expect("a directory for the store of dc-a");
    let mut dc_a = origin_cluster(store.path());
    dc_a.start().await.expect("dc-a boots");
    let origin = dc_a.tcp_root_client().await.expect("a client of dc-a");
    let geo_topic_id = create_topics(&origin).await;
    let mut dc_b = follower_cluster(store.path(), 1);
    dc_b.start().await.expect("dc-b boots");
    let follower = dc_b.tcp_root_client().await.expect("a client of dc-b");
    follower
        .create_stream("local", &StreamCreateOptions::default())
        .await
        .expect("a stream of dc-b");
    wait_for_shadow_topic(&follower, geo_topic_id).await;
    let http = HttpClient::login_root(&dc_b).await;
    wait_for_stale_gauge(&http, 0).await;

    let away = store.path().with_extension("away");
    std::fs::rename(store.path(), &away).expect("take the store of dc-a away");
    wait_for_stale_gauge(&http, 1).await;
    assert!(
        follower
            .get_topic(
                &named(SHADOW_STREAM),
                &Identifier::numeric(geo_topic_id).unwrap()
            )
            .await
            .expect("get the shadow topic")
            .is_some(),
        "a stale shadow topic serves what it holds"
    );
    assert_local_topics_work(&follower).await;

    std::fs::rename(&away, store.path()).expect("bring the store of dc-a back");
    wait_for_stale_gauge(&http, 0).await;
}

#[tokio::test]
async fn given_a_three_node_follower_when_restarted_and_its_primary_stops_should_keep_the_same_shadow_streams_on_every_node()
 {
    let store = tempfile::tempdir().expect("a directory for the store of dc-a");
    let mut dc_a = origin_cluster(store.path());
    dc_a.start().await.expect("dc-a boots");
    let origin = dc_a.tcp_root_client().await.expect("a client of dc-a");
    let geo_topic_id = create_topics(&origin).await;
    let mut dc_b = follower_cluster(store.path(), 3);
    dc_b.start().await.expect("dc-b boots");
    let follower = dc_b.tcp_root_client().await.expect("a client of dc-b");
    let adopted = wait_for_shadow_topic(&follower, geo_topic_id).await;

    assert_every_node_holds(&dc_b, &[0, 1, 2], adopted.id, geo_topic_id).await;

    dc_b.restart_cluster().await.expect("dc-b restarts");
    assert_every_node_holds(&dc_b, &[0, 1, 2], adopted.id, geo_topic_id).await;

    dc_b.node_mut(0)
        .stop()
        .expect("stop the metadata primary of dc-b");
    assert_every_node_holds(&dc_b, &[1, 2], adopted.id, geo_topic_id).await;
}

#[tokio::test]
async fn given_messages_in_a_peer_geo_topic_when_followed_should_serve_each_once_at_the_peers_offsets_through_a_restart()
 {
    let store = tempfile::tempdir().expect("a directory for the store of dc-a");
    let mut dc_a = origin_cluster(store.path());
    dc_a.start().await.expect("dc-a boots");
    let origin = dc_a.tcp_root_client().await.expect("a client of dc-a");
    let topic_id = create_geo_topic(&origin, 1).await;
    send_to_origin(&origin, 0..FIRST_SENDS).await;
    let mut dc_b = follower_cluster(store.path(), 1);
    dc_b.start().await.expect("dc-b boots");
    let follower = dc_b.tcp_root_client().await.expect("a client of dc-b");
    wait_for_shadow_topic(&follower, topic_id).await;
    let consumer = Consumer::new(Identifier::numeric(1).expect("the consumer id"));

    let mut seen = read_next(&follower, topic_id, &consumer, FIRST_SENDS).await;
    send_to_origin(&origin, FIRST_SENDS..FIRST_SENDS + SECOND_SENDS).await;
    seen.extend(read_next(&follower, topic_id, &consumer, SECOND_SENDS).await);

    let expected: Vec<(u64, Vec<u8>)> = (0..FIRST_SENDS + SECOND_SENDS)
        .map(|index| (index, payload(index)))
        .collect();
    assert_eq!(seen, expected, "each message once, in the origin's order");
    assert_eq!(
        read_from_start(&origin, &named(STREAM), &named(GEO_TOPIC)).await,
        expected,
        "the origin holds the same messages at the same offsets"
    );
    let http = HttpClient::login_root(&dc_b).await;
    let metrics = scrape(&http).await;
    assert!(
        sum(
            &metrics,
            "geo_peer_manifest_requests_total",
            "request=\"list\""
        ) > 0
    );
    assert_eq!(sum(&metrics, "geo_peer_manifests_refused_total", ""), 0);

    dc_b.restart_server().await.expect("dc-b restarts");
    let follower = dc_b.tcp_root_client().await.expect("a client of dc-b");
    let shadow_topic = Identifier::numeric(topic_id).expect("the topic id");
    let deadline = Instant::now() + FOLLOW_DEADLINE;
    loop {
        let read = read_from_start(&follower, &named(SHADOW_STREAM), &shadow_topic).await;
        if read == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dc-b serves {} of the messages after a restart",
            read.len()
        );
        sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test]
async fn given_a_followed_peer_when_its_store_holds_a_regressing_manifest_should_refuse_it_and_keep_serving()
 {
    let store = tempfile::tempdir().expect("a directory for the store of dc-a");
    let mut dc_a = origin_cluster(store.path());
    dc_a.start().await.expect("dc-a boots");
    let origin = dc_a.tcp_root_client().await.expect("a client of dc-a");
    let topic_id = create_geo_topic(&origin, 1).await;
    send_to_origin(&origin, 0..FIRST_SENDS).await;
    let mut dc_b = follower_cluster(store.path(), 1);
    dc_b.start().await.expect("dc-b boots");
    let follower = dc_b.tcp_root_client().await.expect("a client of dc-b");
    wait_for_shadow_topic(&follower, topic_id).await;
    let consumer = Consumer::new(Identifier::numeric(1).expect("the consumer id"));
    let served = read_next(&follower, topic_id, &consumer, FIRST_SENDS).await;
    let (key, original) = wait_for_manifest_with_segments(store.path()).await;

    // Its floor is below the one that dc-b committed.
    let regressing = PartitionManifest {
        floor: original.trim_point,
        segments: Vec::new(),
        ..original.clone()
    };
    put_manifest(store.path(), &key, &regressing).await;
    let http = HttpClient::login_root(&dc_b).await;
    let deadline = Instant::now() + FOLLOW_DEADLINE;
    while sum(&scrape(&http).await, "geo_peer_manifests_refused_total", "") == 0 {
        assert!(
            Instant::now() < deadline,
            "dc-b never raised the alarm of a regressing manifest"
        );
        sleep(POLL_INTERVAL).await;
    }

    let shadow_topic = Identifier::numeric(topic_id).expect("the topic id");
    assert_eq!(
        read_from_start(&follower, &named(SHADOW_STREAM), &shadow_topic).await,
        served,
        "a refused manifest changes nothing"
    );
    put_manifest(store.path(), &key, &original).await;
}

/// The single node of `dc-a`, whose store is the `fs` root `store`.
fn origin_cluster(store: &Path) -> TestHarness {
    let extra_envs = HashMap::from([
        ("IGGY_GEO_ORIGIN".to_owned(), "dc-a".to_owned()),
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "fs".to_owned()),
        (
            "IGGY_TIERED_STORAGE_PATH".to_owned(),
            store.to_string_lossy().into_owned(),
        ),
    ]);
    TestHarness::builder()
        .test_name(test_name("dc-a"))
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness of dc-a")
}

/// `nodes` nodes of `dc-b`, which reads `dc-a` from the `fs` root `store`.
fn follower_cluster(store: &Path, nodes: usize) -> TestHarness {
    let extra_envs = HashMap::from([
        ("IGGY_GEO_ORIGIN".to_owned(), "dc-b".to_owned()),
        ("IGGY_GEO_TOMBSTONE_GRACE".to_owned(), "2s".to_owned()),
        ("IGGY_GEO_PEERS_0_ORIGIN".to_owned(), "dc-a".to_owned()),
        ("IGGY_GEO_PEERS_0_BACKEND".to_owned(), "fs".to_owned()),
        (
            "IGGY_GEO_PEERS_0_PATH".to_owned(),
            store.to_string_lossy().into_owned(),
        ),
    ]);
    let builder = TestHarness::builder()
        .test_name(test_name("dc-b"))
        .server(TestServerConfig::builder().extra_envs(extra_envs).build());
    let builder = if nodes > 1 {
        builder.cluster_nodes(nodes)
    } else {
        builder
    };
    builder.build().expect("the harness of dc-b")
}

/// A name for the harness of `cluster` in this test, apart from the other
/// cluster's.
fn test_name(cluster: &str) -> String {
    let test = std::thread::current()
        .name()
        .unwrap_or("geo_shadow_fs")
        .replace("::", "_");
    format!("{test}_{cluster}")
}

fn named(name: &str) -> Identifier {
    Identifier::named(name).expect("a valid identifier")
}

/// Creates the stream `orders` in `dc-a` with a normal topic and then a geo
/// topic, and returns the geo topic's id.
async fn create_topics(origin: &IggyClient) -> u32 {
    origin
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream of dc-a");
    origin
        .create_topic(
            &named(STREAM),
            LOCAL_TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create a normal topic in dc-a");
    let options = TopicCreateOptions {
        partitions_count: Some(PARTITIONS),
        topic_type: Some(TopicType::Geo),
        ..TopicCreateOptions::default()
    };
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        match origin
            .create_topic(&named(STREAM), GEO_TOPIC, &options)
            .await
        {
            Ok(topic) => return topic.id,
            Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(error) => panic!("dc-a never accepted the geo topic: {error}"),
        }
    }
}

/// Waits until `dc-b` serves the shadow topic `topic_id`, and returns its
/// shadow stream.
async fn wait_for_shadow_topic(follower: &IggyClient, topic_id: u32) -> StreamDetails {
    let deadline = Instant::now() + ADOPT_DEADLINE;
    loop {
        if let Some(stream) = follower
            .get_stream(&named(SHADOW_STREAM))
            .await
            .expect("get the shadow stream")
            && stream.topics.iter().any(|topic| topic.id == topic_id)
        {
            return stream;
        }
        assert!(
            Instant::now() < deadline,
            "dc-b never adopted the geo topic of dc-a"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn assert_client_writes_are_refused(follower: &IggyClient, topic_id: u32) {
    let stream = named(SHADOW_STREAM);
    let topic = Identifier::numeric(topic_id).expect("the topic id");
    let refusals: Vec<(&str, Result<(), IggyError>)> = vec![
        (
            "create topic",
            follower
                .create_topic(&stream, "mine", &TopicCreateOptions::default())
                .await
                .map(|_| ()),
        ),
        (
            "update topic",
            follower
                .update_topic(
                    &stream,
                    &topic,
                    &TopicUpdateOptions {
                        name: Some("renamed".to_owned()),
                        ..TopicUpdateOptions::default()
                    },
                )
                .await,
        ),
        ("delete topic", follower.delete_topic(&stream, &topic).await),
        (
            "create partitions",
            follower.create_partitions(&stream, &topic, 1).await,
        ),
        (
            "delete partitions",
            follower.delete_partitions(&stream, &topic, 1).await,
        ),
        (
            "delete segments",
            follower.delete_segments(&stream, &topic, 0, 1).await,
        ),
        (
            "update stream",
            follower
                .update_stream(
                    &stream,
                    &StreamUpdateOptions {
                        name: Some("renamed".to_owned()),
                        ..StreamUpdateOptions::default()
                    },
                )
                .await,
        ),
        ("delete stream", follower.delete_stream(&stream).await),
        (
            "send messages",
            follower
                .send_messages(
                    &stream,
                    &topic,
                    &Partitioning::partition_id(0),
                    &mut [IggyMessage::builder()
                        .payload("written in dc-b".into())
                        .build()
                        .expect("a message")],
                )
                .await
                .map(|_| ()),
        ),
    ];
    for (mutation, result) in refusals {
        assert_eq!(
            result.map_err(|error| error.as_code()),
            Err(IggyError::ShadowStreamReadOnly.as_code()),
            "{mutation} on a shadow stream"
        );
    }

    let reserved = follower
        .create_stream("__shadow_mine", &StreamCreateOptions::default())
        .await
        .map(|_| ());
    assert_eq!(
        reserved.map_err(|error| error.as_code()),
        Err(IggyError::InvalidStreamName.as_code())
    );
    let renamed = follower
        .update_stream(
            &named("local"),
            &StreamUpdateOptions {
                name: Some("__shadow_mine".to_owned()),
                ..StreamUpdateOptions::default()
            },
        )
        .await;
    assert_eq!(
        renamed.map_err(|error| error.as_code()),
        Err(IggyError::InvalidStreamName.as_code())
    );
}

/// A shadow topic takes reads and consumer groups like any topic.
async fn assert_reads_and_groups_work(follower: &IggyClient, topic_id: u32) {
    let stream = named(SHADOW_STREAM);
    let topic = Identifier::numeric(topic_id).expect("the topic id");
    follower
        .create_consumer_group(&stream, &topic, "readers")
        .await
        .expect("a consumer group on a shadow topic");
    let polled = follower
        .poll_messages(
            &stream,
            &topic,
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 10,
                ..PollOptions::default()
            },
        )
        .await
        .expect("a poll of a shadow partition");
    assert!(
        polled.messages.is_empty(),
        "dc-a sent nothing to the topic, so the partition serves nothing"
    );
}

/// A topic of `dc-b`'s own takes sends and polls as before.
async fn assert_local_topics_work(follower: &IggyClient) {
    let stream = named("local");
    let topic = named("own");
    if follower
        .get_topic(&stream, &topic)
        .await
        .expect("get the local topic")
        .is_none()
    {
        follower
            .create_topic(
                &stream,
                "own",
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .expect("create a topic of dc-b");
    }
    let mut messages = vec![
        IggyMessage::builder()
            .payload("local".into())
            .build()
            .expect("a message"),
    ];
    follower
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send to a topic of dc-b");
    let polled = follower
        .poll_messages(
            &stream,
            &topic,
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 100,
                ..PollOptions::default()
            },
        )
        .await
        .expect("poll a topic of dc-b");
    assert!(!polled.messages.is_empty());
}

/// Waits until `/metrics` counts `expected` stale shadow topics of `dc-a`.
async fn wait_for_stale_gauge(http: &HttpClient, expected: u64) {
    let deadline = Instant::now() + STALE_DEADLINE;
    loop {
        let response = http.get("/metrics").await;
        assert!(response.status().is_success(), "/metrics scrape failed");
        let exposition = response.text().await.expect("the metrics text");
        let found = exposition
            .lines()
            .filter(|line| line.starts_with("geo_shadow_topics_stale{"))
            .find(|line| line.contains("origin=\"dc-a\""))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse::<u64>().ok());
        if found == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the stale gauge of dc-a stayed at {found:?}, expected {expected}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Every node in `nodes` serves the shadow stream `stream_id` with the
/// shadow topic `topic_id`, and a poll of each of its partitions.
async fn assert_every_node_holds(
    harness: &TestHarness,
    nodes: &[usize],
    stream_id: u32,
    topic_id: u32,
) {
    for &node in nodes {
        let deadline = Instant::now() + ADOPT_DEADLINE;
        loop {
            let found = match harness.root_client_for_node(node).await {
                Ok(client) => serves_shadow_topic(&client, stream_id, topic_id).await,
                Err(_) => false,
            };
            if found {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node {node} of dc-b does not hold shadow stream {stream_id} with topic {topic_id}"
            );
            sleep(POLL_INTERVAL).await;
        }
    }
}

async fn serves_shadow_topic(client: &IggyClient, stream_id: u32, topic_id: u32) -> bool {
    let stream = Identifier::numeric(stream_id).unwrap();
    let topic = Identifier::numeric(topic_id).unwrap();
    let held = client
        .get_stream(&stream)
        .await
        .ok()
        .flatten()
        .filter(|details| details.name == SHADOW_STREAM)
        .is_some_and(|details| details.topics.iter().any(|shadow| shadow.id == topic_id));
    if !held {
        return false;
    }
    for partition_id in 0..PARTITIONS {
        let polled = client
            .poll_messages(
                &stream,
                &topic,
                partition_id,
                &Consumer::default(),
                &PollingStrategy::offset(0),
                PollOptions {
                    count: 1,
                    ..PollOptions::default()
                },
            )
            .await;
        if polled.is_err() {
            return false;
        }
    }
    true
}

/// Creates the stream `orders` in `dc-a` with a geo topic of `partitions`
/// partitions, and returns the topic's id.
async fn create_geo_topic(origin: &IggyClient, partitions: u32) -> u32 {
    origin
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream of dc-a");
    let options = TopicCreateOptions {
        partitions_count: Some(partitions),
        topic_type: Some(TopicType::Geo),
        ..TopicCreateOptions::default()
    };
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        match origin
            .create_topic(&named(STREAM), GEO_TOPIC, &options)
            .await
        {
            Ok(topic) => return topic.id,
            Err(IggyError::FeatureUnavailable | IggyError::TransientNotAccepted)
                if Instant::now() < deadline =>
            {
                sleep(POLL_INTERVAL).await;
            }
            Err(error) => panic!("dc-a never accepted the geo topic: {error}"),
        }
    }
}

fn payload(index: u64) -> Vec<u8> {
    format!("message-{index}").into_bytes()
}

/// Sends one message for each index to partition 0 of the geo topic.
async fn send_to_origin(origin: &IggyClient, indexes: std::ops::Range<u64>) {
    for index in indexes {
        let mut messages = [IggyMessage::builder()
            .payload(payload(index).into())
            .build()
            .expect("a message")];
        origin
            .send_messages(
                &named(STREAM),
                &named(GEO_TOPIC),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .expect("send to the geo topic of dc-a");
    }
}

/// Polls the next messages of `consumer` from partition 0 of the shadow
/// topic, committing its offset, until `count` arrived.
async fn read_next(
    follower: &IggyClient,
    topic_id: u32,
    consumer: &Consumer,
    count: u64,
) -> Vec<(u64, Vec<u8>)> {
    let topic = Identifier::numeric(topic_id).expect("the topic id");
    let mut read = Vec::new();
    let deadline = Instant::now() + FOLLOW_DEADLINE;
    while (read.len() as u64) < count {
        let polled = follower
            .poll_messages(
                &named(SHADOW_STREAM),
                &topic,
                0,
                consumer,
                &PollingStrategy::next(),
                PollOptions {
                    count: 10,
                    commit: CommitPolicy::OnRead,
                    ..PollOptions::default()
                },
            )
            .await
            .expect("a poll of the shadow partition");
        read.extend(
            polled
                .messages
                .into_iter()
                .map(|message| (message.header.offset, message.payload.to_vec())),
        );
        if (read.len() as u64) < count {
            assert!(
                Instant::now() < deadline,
                "dc-b served {} of {count} messages",
                read.len()
            );
            sleep(POLL_INTERVAL).await;
        }
    }
    read
}

/// Every message of partition 0 from its first offset.
async fn read_from_start(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Vec<(u64, Vec<u8>)> {
    let mut read = Vec::new();
    loop {
        let polled = match client
            .poll_messages(
                stream,
                topic,
                0,
                &Consumer::default(),
                &PollingStrategy::offset(read.len() as u64),
                PollOptions {
                    count: 100,
                    ..PollOptions::default()
                },
            )
            .await
        {
            Ok(polled) => polled,
            Err(_) => return read,
        };
        if polled.messages.is_empty() {
            return read;
        }
        read.extend(
            polled
                .messages
                .into_iter()
                .map(|message| (message.header.offset, message.payload.to_vec())),
        );
    }
}

async fn scrape(http: &HttpClient) -> String {
    let response = http.get("/metrics").await;
    assert!(response.status().is_success(), "/metrics scrape failed");
    response.text().await.expect("the metrics text")
}

/// The sum over every shard of the series `name` of `dc-a` whose labels hold
/// `label`.
fn sum(exposition: &str, name: &str, label: &str) -> u64 {
    exposition
        .lines()
        .filter(|line| line.starts_with(&format!("{name}{{")))
        .filter(|line| line.contains("origin=\"dc-a\"") && line.contains(label))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// Waits until the store of `dc-a` holds a manifest that lists a segment,
/// and returns its key and the manifest.
async fn wait_for_manifest_with_segments(store: &Path) -> (String, PartitionManifest) {
    let deadline = Instant::now() + FOLLOW_DEADLINE;
    loop {
        if let Some(found) = read_manifest(store).await
            && !found.1.segments.is_empty()
        {
            return found;
        }
        assert!(
            Instant::now() < deadline,
            "dc-a never published a manifest that lists a segment"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The key and the content of the one manifest of stream 0 in the `fs`
/// store at `store`. The provider runs on compio, not on the harness's
/// runtime.
async fn read_manifest(store: &Path) -> Option<(String, PartitionManifest)> {
    let path = store.to_owned();
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = FsStore::new(&path);
                let layout = KeyLayout::new("iggy").expect("the default prefix");
                let prefix = layout.stream_manifests_prefix(0);
                let page = store.list(&ListRequest::new(&prefix)).await.ok()?;
                let key = page
                    .objects
                    .into_iter()
                    .map(|object| object.key)
                    .find(|key| layout.parse_manifest_key(key).is_some())?;
                let object = store.get_range(&key, 0..u64::MAX).await.ok()?;
                let manifest = PartitionManifest::decode(&object.bytes).ok()?;
                Some((key, manifest))
            })
    })
    .await
    .expect("the read task")
}

/// Puts `manifest` under `key` in the `fs` store at `store`, over what is
/// there.
async fn put_manifest(store: &Path, key: &str, manifest: &PartitionManifest) {
    let path = store.to_owned();
    let key = key.to_owned();
    let bytes = manifest.encode();
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                FsStore::new(&path)
                    .put(&key, PutBody::from_bytes(bytes), &PutOptions::default())
                    .await
                    .expect("put the manifest");
            });
    })
    .await
    .expect("the put task");
}
