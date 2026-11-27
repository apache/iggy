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

//! Two clusters on this host, `dc-a` and `dc-b`, each with an `fs` bucket of
//! its own, where `dc-b` retires `dc-a` as an origin that never returns.

use std::time::Duration;

use iggy::prelude::*;
use integration::harness::geo_clusters::READ_PASSES_TIMEOUT;
use integration::harness::{BucketEvent, BucketWatch, GeoCluster, GeoClusterPair, GeoSide};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

const STREAM: &str = "orders";
/// A geo topic whose messages never expire.
const KEPT: &str = "payments";
/// A geo topic whose messages live [`EXPIRY`].
const EXPIRING: &str = "events";
const EXPIRY: Duration = Duration::from_secs(20);
const PARTITIONS: u32 = 2;
const MESSAGES: u32 = 30;
/// How long a cluster may take to list a message in a manifest.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a retirement may take to reach the gauges: the identity checks
/// of shard 0 look for it every second.
const RETIRE_TIMEOUT: Duration = Duration::from_secs(20);
/// Four manifest listings and four passes of the shadow topics, in each of
/// which a cluster that follows its peer reads the peer's bucket.
const QUIET: Duration = Duration::from_secs(20);
/// How long a shadow topic may take to go once its expiry passed: a pass of
/// the metadata primary every 5 s, then the removal of its partitions.
const REMOVE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// How long a new watch takes to walk the bucket and watch each of its
/// directories. The walk opens each one, and the watch records that too.
const WATCH_SETTLE: Duration = Duration::from_secs(1);

#[tokio::test]
async fn given_a_peer_that_never_returns_when_retired_should_send_its_bucket_only_polls_and_expire_its_shadow_topics()
 {
    let mut pair = started_pair().await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    client_a
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    let kept = create_geo_topic(pair.a(), &client_a, KEPT, IggyExpiry::NeverExpire).await;
    let expiring = create_geo_topic(
        pair.a(),
        &client_a,
        EXPIRING,
        IggyExpiry::ExpireDuration(EXPIRY.into()),
    )
    .await;
    produce(&client_a, pair.a().origin()).await;
    wait_for_published(pair.a(), kept.id).await;
    drop(client_a);
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let shadow = pair
        .b()
        .wait_for_shadow_topic(&client_b, STREAM, expiring.id)
        .await
        .expect("dc-b adopts the expiring topic");
    pair.b()
        .wait_for_shadow_topic(&client_b, STREAM, kept.id)
        .await
        .expect("dc-b adopts the kept topic");
    assert_consumes(&client_b, shadow.id, kept.id, "before", pair.a().origin()).await;

    pair.cluster_mut(GeoSide::A)
        .stop()
        .await
        .expect("dc-a stops for good");
    let (following, since) = settled_watch(pair.a()).await;
    let deadline = Instant::now() + READ_PASSES_TIMEOUT;
    while reads_since(&following, since).is_empty() {
        assert!(
            Instant::now() < deadline,
            "dc-b never read the bucket of dc-a while it followed it"
        );
        sleep(POLL_INTERVAL).await;
    }
    drop(following);

    client_b
        .retire_geo_origin(pair.a().origin())
        .await
        .expect("dc-b retires dc-a");
    wait_for_retired_gauge(pair.b()).await;
    let (retired, since) = settled_watch(pair.a()).await;
    sleep(QUIET).await;
    assert_eq!(
        reads_since(&retired, since),
        [],
        "dc-b read the bucket of the retired dc-a without a poll"
    );
    assert_eq!(retired.changes(), [], "dc-b changed the bucket of dc-a");

    drop(client_b);
    let cluster_b = pair.cluster_mut(GeoSide::B);
    cluster_b.stop().await.expect("dc-b stops");
    cluster_b.start().await.expect("dc-b starts again");
    wait_for_retired_gauge(pair.b()).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    assert_consumes(&client_b, shadow.id, kept.id, "after", pair.a().origin()).await;
    sleep(QUIET).await;
    assert_eq!(
        retired.changes(),
        [],
        "dc-b changed the bucket of dc-a after its restart"
    );
    assert!(
        !reads_since(&retired, since).is_empty(),
        "the polls after the restart read the bucket of dc-a"
    );

    let remaining = wait_for_topics(&client_b, shadow.id, |topics| {
        !topics.contains(&expiring.id)
    })
    .await;
    assert_eq!(
        remaining,
        [kept.id],
        "the topic that never expires stays, the expired one goes"
    );
}

#[tokio::test]
async fn given_a_retired_peer_when_it_comes_back_or_a_client_deletes_its_shadow_should_adopt_nothing_and_take_the_delete()
 {
    let pair = started_pair().await;
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    client_a
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    let kept = create_geo_topic(pair.a(), &client_a, KEPT, IggyExpiry::NeverExpire).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let shadow = pair
        .b()
        .wait_for_shadow_topic(&client_b, STREAM, kept.id)
        .await
        .expect("dc-b adopts the topic of dc-a");
    let shadow_id = Identifier::numeric(shadow.id).unwrap();
    let topic_id = Identifier::numeric(kept.id).unwrap();
    assert!(matches!(
        client_b.delete_topic(&shadow_id, &topic_id).await,
        Err(IggyError::ShadowStreamReadOnly)
    ));
    assert!(matches!(
        client_b.retire_geo_origin("DC-A").await,
        Err(IggyError::InvalidFormat)
    ));

    for _ in 0..2 {
        client_b
            .retire_geo_origin(pair.a().origin())
            .await
            .expect("dc-b retires dc-a, again too");
    }
    wait_for_retired_gauge(pair.b()).await;
    let late = create_geo_topic(pair.a(), &client_a, EXPIRING, IggyExpiry::NeverExpire).await;
    sleep(QUIET).await;
    let topics = topic_ids(&client_b, shadow.id).await;
    assert_eq!(
        topics,
        Some(vec![kept.id]),
        "dc-b adopted topic {} of the retired dc-a",
        late.id
    );

    client_b
        .delete_topic(&shadow_id, &topic_id)
        .await
        .expect("a client deletes a shadow topic of the retired dc-a");
    wait_for_topics(&client_b, shadow.id, <[u32]>::is_empty).await;
    client_b
        .delete_stream(&shadow_id)
        .await
        .expect("a client deletes a shadow stream of the retired dc-a");
    let deadline = Instant::now() + REMOVE_TIMEOUT;
    while topic_ids(&client_b, shadow.id).await.is_some() {
        assert!(Instant::now() < deadline, "the shadow stream stays");
        sleep(POLL_INTERVAL).await;
    }
    sleep(QUIET).await;
    let name = pair.b().shadow_stream_name(STREAM);
    assert!(
        client_b
            .get_stream(&Identifier::named(&name).unwrap())
            .await
            .expect("a read of the streams")
            .is_none(),
        "dc-b adopted the stream of the retired dc-a again"
    );
}

async fn started_pair() -> GeoClusterPair {
    let mut pair = GeoClusterPair::builder().build().expect("the pair");
    pair.start().await.expect("both clusters boot");
    pair
}

/// Create the geo topic `name` in the stream [`STREAM`] of `cluster`, while
/// the cluster refuses geo topics as unavailable at first.
async fn create_geo_topic(
    cluster: &GeoCluster,
    client: &IggyClient,
    name: &str,
    message_expiry: IggyExpiry,
) -> TopicDetails {
    let options = TopicCreateOptions {
        partitions_count: Some(PARTITIONS),
        topic_type: Some(TopicType::Geo),
        geo_mode: Some(cluster.geo_mode()),
        message_expiry: Some(message_expiry),
        ..TopicCreateOptions::default()
    };
    let deadline = Instant::now() + PUBLISH_TIMEOUT;
    loop {
        match client
            .create_topic(&Identifier::named(STREAM).unwrap(), name, &options)
            .await
        {
            Ok(topic) => return topic,
            Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(error) => panic!("{} refused the geo topic {name}: {error}", cluster.origin()),
        }
    }
}

/// Send [`MESSAGES`] messages of `origin` to each partition of [`KEPT`].
async fn produce(client: &IggyClient, origin: &str) {
    for partition in 0..PARTITIONS {
        let mut messages: Vec<IggyMessage> = (0..MESSAGES)
            .map(|index| {
                IggyMessage::builder()
                    .payload(payload(origin, partition, index).into_bytes().into())
                    .build()
                    .expect("a message")
            })
            .collect();
        client
            .send_messages(
                &Identifier::named(STREAM).unwrap(),
                &Identifier::named(KEPT).unwrap(),
                &Partitioning::partition_id(partition),
                &mut messages,
            )
            .await
            .unwrap_or_else(|error| panic!("{origin} refused a send: {error}"));
    }
}

/// Wait until a manifest of each partition of the topic `topic_id` in the
/// bucket of `cluster` lists all [`MESSAGES`] messages.
async fn wait_for_published(cluster: &GeoCluster, topic_id: u32) {
    let last = u64::from(MESSAGES) - 1;
    let deadline = Instant::now() + PUBLISH_TIMEOUT;
    loop {
        let manifests = cluster.bucket().manifests().await.expect("manifests");
        let published = manifests
            .iter()
            .filter(|manifest| manifest.prefix.topic_id == u64::from(topic_id))
            .filter(|manifest| manifest.floor == Some(last))
            .count();
        if published == PARTITIONS as usize {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} published {published} of {PARTITIONS} partitions",
            cluster.origin()
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The consumer `consumer` reads every message of `origin` from each
/// partition of the shadow topic, once and in order.
async fn assert_consumes(
    client: &IggyClient,
    stream_id: u32,
    topic_id: u32,
    consumer: &str,
    origin: &str,
) {
    let consumer = Consumer::new(Identifier::named(consumer).unwrap());
    let options = PollOptions {
        count: 10,
        commit: CommitPolicy::OnRead,
        ..PollOptions::default()
    };
    for partition in 0..PARTITIONS {
        let expected: Vec<String> = (0..MESSAGES)
            .map(|index| payload(origin, partition, index))
            .collect();
        let mut received = Vec::with_capacity(expected.len());
        let deadline = Instant::now() + PUBLISH_TIMEOUT;
        while received.len() < expected.len() {
            let polled = client
                .poll_messages(
                    &Identifier::numeric(stream_id).unwrap(),
                    &Identifier::numeric(topic_id).unwrap(),
                    partition,
                    &consumer,
                    &PollingStrategy::next(),
                    options,
                )
                .await
                .unwrap_or_else(|error| panic!("a poll of the shadow topic: {error}"));
            if polled.messages.is_empty() {
                assert!(
                    Instant::now() < deadline,
                    "{} of {MESSAGES} messages reached partition {partition}",
                    received.len()
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
        assert_eq!(received, expected, "partition {partition}");
    }
}

/// A watch on the bucket of `cluster`, once it watches every directory, and
/// when it did.
async fn settled_watch(cluster: &GeoCluster) -> (BucketWatch, Instant) {
    let watch = cluster.bucket().watch().expect("a watch on the bucket");
    sleep(WATCH_SETTLE).await;
    (watch, Instant::now())
}

/// The reads that `watch` saw after `since`.
fn reads_since(watch: &BucketWatch, since: Instant) -> Vec<BucketEvent> {
    watch
        .reads()
        .into_iter()
        .filter(|event| matches!(event, BucketEvent::Read { at, .. } if *at > since.into_std()))
        .collect()
}

/// Wait until every node of `cluster` reports its peer retired.
async fn wait_for_retired_gauge(cluster: &GeoCluster) {
    let deadline = Instant::now() + RETIRE_TIMEOUT;
    let http = HttpClient::login_root(cluster.harness()).await;
    loop {
        let response = http.get("/metrics").await;
        assert!(response.status().is_success(), "/metrics scrape failed");
        let exposition = response.text().await.expect("the metrics text");
        let label = format!("origin=\"{}\"", cluster.peer_origin());
        if exposition
            .lines()
            .filter(|line| line.starts_with("geo_peer_retired{"))
            .any(|line| line.contains(&label) && line.ends_with(" 1"))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} never reported {} retired",
            cluster.origin(),
            cluster.peer_origin()
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The ids of the topics in the stream `stream_id`, `None` without it.
async fn topic_ids(client: &IggyClient, stream_id: u32) -> Option<Vec<u32>> {
    client
        .get_stream(&Identifier::numeric(stream_id).unwrap())
        .await
        .expect("a read of the stream")
        .map(|stream| stream.topics.iter().map(|topic| topic.id).collect())
}

/// Wait until the topic ids of the stream `stream_id` satisfy `done`, and
/// return them.
async fn wait_for_topics(
    client: &IggyClient,
    stream_id: u32,
    done: impl Fn(&[u32]) -> bool,
) -> Vec<u32> {
    let deadline = Instant::now() + EXPIRY + REMOVE_TIMEOUT;
    loop {
        let topics = topic_ids(client, stream_id)
            .await
            .expect("the shadow stream stays while it holds a topic");
        if done(&topics) {
            return topics;
        }
        assert!(
            Instant::now() < deadline,
            "the shadow stream holds the topics {topics:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

fn payload(origin: &str, partition: u32, index: u32) -> String {
    format!("geo-msg:{origin}:{partition}:{index:05}")
}
