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

//! A server with the `fs` object store and a geo topic on it.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::locking::IggyRwLockFn;
use iggy::prelude::*;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader, BatchProducer};
use iggy_binary_protocol::codes::CREATE_TOPIC_CODE;
use iggy_binary_protocol::placement::{self, PLACED_BATCH_SIZE, PlacedBatch, Placement};
use iggy_binary_protocol::requests::topics::CreateTopicRequest;
use iggy_binary_protocol::{WireEncode, WireName};
use iggy_common::BinaryTransport;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_object_store::{
    FreshIndex, FreshObjectId, FreshProofRow, FsStore, KeyLayout, ObjectStore,
};
use integration::harness::{TestHarness, TestServerConfig, wait_for_consumer_group_assignment};
use server_common::sharding::IggyNamespace;
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;
use super::tiered_storage_fs::{METRICS_DEADLINE, settled_read_cache_counts};

const STREAM: &str = "geo-stream";
const TOPIC: &str = "geo-topic";
const PARTITION_ID: u32 = 0;
const MESSAGES: u64 = 20;
const MESSAGE_BYTES: usize = 1024;
/// How long the cluster may take to accept a geo topic: the node claims the
/// store first, and the metadata primary then has to see it ready.
const READY_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// How long the bucket may take to prove the last committed batch: one
/// object of proof rows alone after a wait and an upload.
const PROVEN_DEADLINE: Duration = Duration::from_secs(30);
/// How long the rewrite of a partition may take to take every message into
/// long-term segments: its interval grows up to 10 s, and its segment then
/// has to commit.
const REWRITE_DEADLINE: Duration = Duration::from_secs(90);

/// A consumer at the tail reads what the shard uploaded from memory, and one
/// behind the rewrite reads the long-term segments that the rewrite handed
/// to the cache, so neither asks the bucket.
#[tokio::test]
async fn given_a_geo_topic_when_consumers_read_before_and_after_its_rewrite_should_not_ask_the_bucket()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_geo_topic(&client, &stream, TOPIC, None)
        .await
        .expect("create the geo topic");
    send(&client, &stream, &topic, 0..MESSAGES).await;

    assert_reads_every_message(&client, &stream, &topic, MESSAGES).await;
    let tail =
        settled_read_cache_counts(&http, METRICS_DEADLINE, |counts| counts.block_hits > 0).await;
    assert_eq!(tail.demand_bytes, 0, "the tail read the bucket: {tail:?}");
    let rewritten = settled_read_cache_counts(&http, REWRITE_DEADLINE, |counts| {
        counts.fresh_releases > 0 && counts.held_bytes == 0
    })
    .await;
    assert!(rewritten.carried_bytes > 0, "{rewritten:?}");
    assert_reads_every_message(&client, &stream, &topic, MESSAGES).await;
    let behind =
        settled_read_cache_counts(&http, METRICS_DEADLINE, |counts| *counts != rewritten).await;

    assert!(behind.block_hits > rewritten.block_hits, "{behind:?}");
    assert_eq!(
        behind.demand_bytes, 0,
        "a read behind the rewrite read the bucket: {rewritten:?}, then {behind:?}"
    );
}

#[tokio::test]
async fn given_a_geo_topic_when_producing_should_store_the_sends_and_read_them_back() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_geo_topic(&client, &stream, TOPIC, None)
        .await
        .expect("create the geo topic");

    let details = client
        .get_topic(&stream, &topic)
        .await
        .expect("get topic")
        .expect("the topic exists");
    let resolved = details.resolved_options();
    assert_eq!(resolved.topic_type, TopicType::Geo);
    assert_eq!(resolved.geo_mode, Some(GeoMode::Geo));
    assert_eq!(
        resolved.partition_resize_policy,
        PartitionResizePolicy::Fixed
    );
    let mode = details
        .options
        .get(&HeaderKey::from(OptionKey::GeoMode))
        .expect("the mode is stored");
    assert!(!mode.explicit, "admission derived the mode");

    // `TopicCreateOptions` sends `replicated`, which the geo mode ignores.
    let durability = details
        .options
        .get(&HeaderKey::from(OptionKey::Durability))
        .expect("the durability is stored");
    assert_eq!(resolved.runtime.durability, Durability::Persisted);
    assert!(!durability.explicit, "the geo mode set the durability");

    send(&client, &stream, &topic, 0..MESSAGES).await;
    // A placed send is uploaded before it commits, and an uploaded object is
    // deleted only once a long-term segment holds its messages.
    let placed = placed_objects(store.path());
    assert!(
        !placed.is_empty(),
        "no committed send left an object in the store: {:?}",
        files(store.path())
    );
    assert_reads_every_message(&client, &stream, &topic, MESSAGES).await;
}

/// A geo topic in the `geo` mode has no durability choice: whatever a create
/// names, or with no `durability` at all, every transport reports `persisted`
/// as derived. A normal topic keeps the durability it names.
#[tokio::test]
async fn given_a_geo_topic_when_created_naming_any_durability_or_none_should_report_persisted() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let tcp = harness.tcp_root_client().await.expect("tcp root client");
    let http = harness.http_root_client().await.expect("http root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    tcp.create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    wait_until_ready(&tcp, &stream).await;
    let geo = TopicCreateOptions {
        partitions_count: Some(1),
        topic_type: Some(TopicType::Geo),
        ..TopicCreateOptions::default()
    };

    tcp.create_topic(&stream, "sdk-defaults", &geo)
        .await
        .expect("create a geo topic with the SDK's defaults");
    http.create_topic(
        &stream,
        "http-replicated",
        &TopicCreateOptions {
            durability: Durability::Replicated,
            ..geo.clone()
        },
    )
    .await
    .expect("create a geo topic that names replicated over HTTP");
    let create = CreateTopicRequest {
        stream_id: identifier_to_wire(&stream).expect("wire stream identifier"),
        partitions_count: 1,
        name: WireName::new("no-key").expect("wire topic name"),
        options: geo
            .to_explicit_wire(|_| false)
            .expect("a block without the durability keys"),
    }
    .to_bytes();
    match &*tcp.client().read().await {
        ClientWrapper::Tcp(client) => client
            .send_raw_with_response(CREATE_TOPIC_CODE, create)
            .await
            .expect("create a geo topic without the key"),
        _ => panic!("the harness root client speaks TCP"),
    };
    tcp.create_topic(
        &stream,
        "normal",
        &TopicCreateOptions {
            partitions_count: Some(1),
            ..TopicCreateOptions::default()
        },
    )
    .await
    .expect("create a normal topic");

    for (name, expected, explicit) in [
        ("sdk-defaults", Durability::Persisted, false),
        ("http-replicated", Durability::Persisted, false),
        ("no-key", Durability::Persisted, false),
        ("normal", Durability::Replicated, true),
    ] {
        let topic = Identifier::named(name).expect("topic identifier");
        for (transport, client) in [("tcp", &tcp), ("http", &http)] {
            let details = client
                .get_topic(&stream, &topic)
                .await
                .expect("get topic")
                .expect("the topic exists");
            let durability = details
                .options
                .get(&HeaderKey::from(OptionKey::Durability))
                .expect("the durability is stored");
            assert_eq!(
                details.resolved_options().runtime.durability,
                expected,
                "{name} over {transport}"
            );
            assert_eq!(durability.explicit, explicit, "{name} over {transport}");
        }
    }
}

/// A reader of the bucket alone, with no access to the cluster, finds every
/// committed message once: the proof rows of the fresh objects bind each
/// committed batch to its offset, and the batch rows of the object that
/// holds it rebuild the placed batch the partition committed.
#[tokio::test]
async fn given_a_geo_topic_when_producing_should_prove_every_committed_message_in_the_bucket() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_geo_topic(&client, &stream, TOPIC, None)
        .await
        .expect("create the geo topic");
    let stream_id = client
        .get_stream(&stream)
        .await
        .expect("get stream")
        .expect("the stream exists")
        .id;
    let topic_id = client
        .get_topic(&stream, &topic)
        .await
        .expect("get topic")
        .expect("the topic exists")
        .id;
    let partition =
        IggyNamespace::new(stream_id as usize, topic_id as usize, PARTITION_ID as usize);

    send(&client, &stream, &topic, 0..MESSAGES).await;

    let deadline = Instant::now() + PROVEN_DEADLINE;
    let (objects, rows) = loop {
        let objects = read_fresh_objects(store.path());
        let rows = proof_rows(&objects, partition);
        if rows.len() as u64 >= MESSAGES {
            break (objects, rows);
        }
        assert!(
            Instant::now() < deadline,
            "the bucket proves {} of {MESSAGES} batches with {} objects",
            rows.len(),
            objects.len()
        );
        sleep(POLL_INTERVAL).await;
    };
    assert_eq!(
        rows.keys().copied().collect::<Vec<_>>(),
        (0..MESSAGES).collect::<Vec<_>>(),
        "one batch per send, each proven"
    );
    for (offset, row) in &rows {
        let blob = proven_blob(&objects, partition, row);
        let found: Vec<u64> = (0..MESSAGES)
            .filter(|index| contains(blob, &payload(*index)))
            .collect();
        assert_eq!(
            found,
            [*offset],
            "the batch at {offset} holds its own message"
        );
    }
}

#[tokio::test]
async fn given_a_geo_topic_when_the_server_restarts_should_read_the_sends_and_take_more() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    {
        let client = harness.tcp_root_client().await.expect("tcp root client");
        client
            .create_stream(STREAM, &StreamCreateOptions::default())
            .await
            .expect("create stream");
        create_geo_topic(&client, &stream, TOPIC, None)
            .await
            .expect("create the geo topic");
        send(&client, &stream, &topic, 0..MESSAGES).await;
    }

    harness.restart_server().await.expect("the server restarts");

    let client = harness.tcp_root_client().await.expect("tcp root client");
    assert_reads_every_message(&client, &stream, &topic, MESSAGES).await;
    send(&client, &stream, &topic, MESSAGES..2 * MESSAGES).await;
    assert_reads_every_message(&client, &stream, &topic, 2 * MESSAGES).await;
}

#[tokio::test]
async fn given_a_geo_topic_when_changing_it_should_keep_its_partitions_and_options() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_geo_topic(&client, &stream, TOPIC, None)
        .await
        .expect("create the geo topic");

    let resize = client
        .create_partitions(&stream, &topic, 1)
        .await
        .expect_err("a geo topic refuses new partitions");
    assert_eq!(
        resize.as_code(),
        IggyError::PartitionResizeDisabled.as_code()
    );
    let shrink = client
        .delete_partitions(&stream, &topic, 1)
        .await
        .expect_err("a geo topic refuses to lose partitions");
    assert_eq!(
        shrink.as_code(),
        IggyError::PartitionResizeDisabled.as_code()
    );
    for update in [
        TopicUpdateOptions {
            message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                Duration::from_secs(3600),
            ))),
            ..TopicUpdateOptions::default()
        },
        TopicUpdateOptions {
            name: Some("renamed".to_owned()),
            ..TopicUpdateOptions::default()
        },
    ] {
        let refused = client
            .update_topic(&stream, &topic, &update)
            .await
            .expect_err("a geo topic refuses an update");
        assert_eq!(
            refused.as_code(),
            IggyError::OptionNotUpdatable(String::new()).as_code(),
            "{update:?}"
        );
    }
    let details = client
        .get_topic(&stream, &topic)
        .await
        .expect("get topic")
        .expect("the topic keeps its name");
    assert_eq!(details.partitions_count, 1);

    // The cluster can hold geo topics now, and still refuses the mode
    // clients cannot pick yet.
    assert_eq!(
        create_geo_topic(&client, &stream, "geo-tiered", Some(GeoMode::GeoTiered)).await,
        Err(IggyError::FeatureUnavailable)
    );
}

/// A geo topic reports the durations it sets, and an update can change
/// them and nothing else of the topic. A normal topic takes none.
#[tokio::test]
async fn given_a_geo_topic_with_its_own_durations_when_reading_and_changing_them_should_report_each_change()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let duration = |text: &str| Some(text.parse::<IggyDuration>().expect("a duration"));
    let created = GeoTopicDurations {
        tombstone_grace: duration("6h"),
        fresh_upload_interval: duration("0"),
        ..GeoTopicDurations::default()
    };
    create_geo_topic_with(
        &client,
        &stream,
        TOPIC,
        TopicCreateOptions {
            geo_durations: created,
            ..TopicCreateOptions::default()
        },
    )
    .await
    .expect("create the geo topic with its durations");
    let durations = || async {
        client
            .get_topic(&stream, &topic)
            .await
            .expect("get topic")
            .expect("the topic exists")
            .resolved_options()
            .geo_durations
    };
    assert_eq!(durations().await, created);

    let update = |geo_durations: GeoTopicDurations| TopicUpdateOptions {
        geo_durations,
        ..TopicUpdateOptions::default()
    };
    client
        .update_topic(
            &stream,
            &topic,
            &update(GeoTopicDurations {
                tombstone_grace: duration("1h"),
                positions_publish_interval: duration("2s"),
                ..GeoTopicDurations::default()
            }),
        )
        .await
        .expect("a geo topic takes a change of its durations");
    assert_eq!(
        durations().await,
        GeoTopicDurations {
            tombstone_grace: duration("1h"),
            positions_publish_interval: duration("2s"),
            fresh_upload_interval: duration("0"),
            ..GeoTopicDurations::default()
        }
    );

    let refusals = [
        (
            TopicUpdateOptions {
                raw_reset: ["tombstone_grace".to_owned()].into(),
                ..TopicUpdateOptions::default()
            },
            IggyError::OptionNotResettable(String::new()),
        ),
        (
            TopicUpdateOptions {
                message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                    Duration::from_secs(3600),
                ))),
                ..update(created)
            },
            IggyError::OptionNotUpdatable(String::new()),
        ),
    ];
    for (refused, expected) in refusals {
        let error = client
            .update_topic(&stream, &topic, &refused)
            .await
            .expect_err("the geo topic refuses the update");
        assert_eq!(error.as_code(), expected.as_code(), "{refused:?}");
    }

    let normal = TopicCreateOptions {
        partitions_count: Some(1),
        geo_durations: created,
        ..TopicCreateOptions::default()
    };
    let refused = client
        .create_topic(&stream, "normal", &normal)
        .await
        .expect_err("a normal topic refuses a duration");
    assert_eq!(
        refused.as_code(),
        IggyError::InvalidOptionValue(String::new()).as_code()
    );
    client
        .create_topic(
            &stream,
            "normal",
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create a normal topic");
    let refused = client
        .update_topic(
            &stream,
            &Identifier::named("normal").expect("topic identifier"),
            &update(created),
        )
        .await
        .expect_err("a normal topic refuses a duration in an update");
    assert_eq!(
        refused.as_code(),
        IggyError::InvalidOptionValue(String::new()).as_code()
    );
}

/// The offsets that a group stores itself are answered once the bucket holds
/// them, and each geo topic spaces those uploads by its own interval.
#[tokio::test]
async fn given_two_geo_topics_with_their_own_offset_upload_intervals_when_their_groups_store_offsets_should_answer_each_at_its_pace()
 {
    const STORES: u64 = 4;
    const SLOW: Duration = Duration::from_secs(1);
    const QUICK: Duration = Duration::from_millis(20);
    const GROUP: &str = "group";
    let store = tempfile::tempdir().expect("a directory for the object store");
    // A round of every log carries the waiting stores too, so they come
    // rarely here and leave each topic to its own interval.
    let mut harness = server_with_fs_store_and(
        store.path(),
        &[("IGGY_GEO_POSITIONS_PUBLISH_INTERVAL", "60s")],
    );
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let mut answered = Vec::new();
    for (name, interval) in [("slow", SLOW), ("quick", QUICK)] {
        create_geo_topic_with(
            &client,
            &stream,
            name,
            TopicCreateOptions {
                geo_durations: GeoTopicDurations {
                    consumer_offset_upload_interval: Some(IggyDuration::new(interval)),
                    ..GeoTopicDurations::default()
                },
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create the geo topic");
        let topic = Identifier::named(name).expect("topic identifier");
        send(&client, &stream, &topic, 0..STORES).await;
        client
            .create_consumer_group(&stream, &topic, GROUP)
            .await
            .expect("create the group");
        let group = Identifier::named(GROUP).expect("group identifier");
        client
            .join_consumer_group(&stream, &topic, &group)
            .await
            .expect("join the group");
        wait_for_consumer_group_assignment(&client, &stream, &topic, &group, 1, READY_DEADLINE)
            .await;
        let started = Instant::now();
        for offset in 0..STORES {
            client
                .store_consumer_offset(
                    &Consumer::group(group.clone()),
                    &stream,
                    &topic,
                    Some(PARTITION_ID),
                    offset,
                )
                .await
                .expect("the store is answered once the bucket holds it");
        }
        answered.push(started.elapsed());
    }

    let (slow, quick) = (answered[0], answered[1]);
    // Each store after the first waits for the interval since the last put.
    let spaced = SLOW * u32::try_from(STORES - 1).expect("a few stores") * 4 / 5;
    assert!(slow >= spaced, "the slow topic answered in {slow:?}");
    assert!(
        quick < slow / 3,
        "the quick topic answered in {quick:?}, the slow one in {slow:?}"
    );
}

fn server_with_fs_store(store: &Path) -> TestHarness {
    server_with_fs_store_and(store, &[])
}

/// [`server_with_fs_store`] with the server settings `more` on top.
fn server_with_fs_store_and(store: &Path, more: &[(&str, &str)]) -> TestHarness {
    let mut extra_envs = HashMap::from([
        // A geo topic takes messages once the metadata primary stored its
        // object, and a node without an origin stores none.
        ("IGGY_GEO_ORIGIN".to_owned(), "dc-a".to_owned()),
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "fs".to_owned()),
        (
            "IGGY_TIERED_STORAGE_PATH".to_owned(),
            store.to_string_lossy().into_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_owned(),
            "100ms".to_owned(),
        ),
    ]);
    extra_envs.extend(
        more.iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
    );
    TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// Creates a one-partition geo topic once the cluster accepts geo topics,
/// and returns the refusal of any other error.
async fn create_geo_topic(
    client: &IggyClient,
    stream: &Identifier,
    name: &str,
    geo_mode: Option<GeoMode>,
) -> Result<(), IggyError> {
    create_geo_topic_with(
        client,
        stream,
        name,
        TopicCreateOptions {
            geo_mode,
            ..TopicCreateOptions::default()
        },
    )
    .await
}

/// [`create_geo_topic`] with `options` on top of the type and the count.
async fn create_geo_topic_with(
    client: &IggyClient,
    stream: &Identifier,
    name: &str,
    options: TopicCreateOptions,
) -> Result<(), IggyError> {
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        topic_type: Some(TopicType::Geo),
        ..options
    };
    wait_until_ready(client, stream).await;
    client
        .create_topic(stream, name, &options)
        .await
        .map(|_| ())
}

/// Waits until the cluster accepts a geo topic, probing with one that is
/// deleted again, so the caller's create meets a ready cluster.
async fn wait_until_ready(client: &IggyClient, stream: &Identifier) {
    const PROBE: &str = "geo-ready-probe";
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        topic_type: Some(TopicType::Geo),
        ..TopicCreateOptions::default()
    };
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        match client.create_topic(stream, PROBE, &options).await {
            Ok(_) => break,
            Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(error) => panic!("the cluster never accepted a geo topic: {error}"),
        }
    }
    client
        .delete_topic(stream, &Identifier::named(PROBE).expect("probe identifier"))
        .await
        .expect("delete the probe topic");
}

/// Sends one message per index of `indexes`, each in a send of its own.
async fn send(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    indexes: std::ops::Range<u64>,
) {
    for index in indexes {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload(index).into())
                .build()
                .expect("build message"),
        ];
        client
            .send_messages(
                stream,
                topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .expect("send messages");
    }
}

async fn assert_reads_every_message(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    count: u64,
) {
    let mut offset = 0;
    while offset < count {
        let polled = client
            .poll_messages(
                stream,
                topic,
                PARTITION_ID,
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                PollOptions {
                    count: 10,
                    ..PollOptions::default()
                },
            )
            .await
            .unwrap_or_else(|error| panic!("poll from offset {offset}: {error}"));
        assert!(
            !polled.messages.is_empty(),
            "the poll from offset {offset} returned no message"
        );
        for message in &polled.messages {
            assert_eq!(message.header.offset, offset);
            assert_eq!(
                message.payload.as_ref(),
                payload(offset).as_slice(),
                "the payload at offset {offset}"
            );
            offset += 1;
        }
    }
}

fn payload(index: u64) -> Vec<u8> {
    let mut payload = vec![(index % 251) as u8; MESSAGE_BYTES];
    payload[..8].copy_from_slice(&index.to_le_bytes());
    payload
}

/// The objects under `root` that hold placed messages: fresh objects, and
/// the long-term segments they are rewritten into.
fn placed_objects(root: &Path) -> Vec<PathBuf> {
    files(root)
        .into_iter()
        .filter(|path| {
            path.components().any(|part| part.as_os_str() == "fresh")
                || path.extension().is_some_and(|extension| extension == "log")
        })
        .collect()
}

/// Every file under `root`, at any depth.
fn files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// A fresh object of the bucket, decoded under its key.
struct FreshInBucket {
    id: FreshObjectId,
    index: FreshIndex,
    bytes: Vec<u8>,
}

/// Every fresh object under the `fs` store at `root`, read through the
/// provider on a compio runtime of its own thread, as the provider does not
/// run on the harness's.
fn read_fresh_objects(root: &Path) -> Vec<FreshInBucket> {
    let layout = KeyLayout::new("iggy").expect("the default prefix");
    let keys: Vec<(String, u128)> = files(root)
        .into_iter()
        .filter_map(|path| {
            let key = path.strip_prefix(root).ok()?.to_string_lossy().into_owned();
            let id = layout.parse_fresh_key(&key)?;
            Some((key, id))
        })
        .collect();
    let root = root.to_owned();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = FsStore::new(&root);
                let mut objects = Vec::new();
                for (key, id) in keys {
                    // An object still being written is not there yet.
                    let Ok(meta) = store.head(&key).await else {
                        continue;
                    };
                    let bytes = store
                        .get_range(&key, 0..meta.size)
                        .await
                        .expect("a stored object reads back")
                        .bytes
                        .to_vec();
                    let id = FreshObjectId::from_raw(id);
                    let index = FreshIndex::decode(&bytes, id)
                        .unwrap_or_else(|error| panic!("{key} does not decode: {error}"));
                    objects.push(FreshInBucket { id, index, bytes });
                }
                objects
            })
    })
    .join()
    .expect("the read does not panic")
}

/// The proof rows of `partition` in `objects`, by the offset of their
/// batch. Two different rows for one batch fail the test.
fn proof_rows(objects: &[FreshInBucket], partition: IggyNamespace) -> BTreeMap<u64, FreshProofRow> {
    let mut rows = BTreeMap::new();
    for object in objects {
        let Some(row) = object.index.partition(partition.inner()) else {
            continue;
        };
        for proof in object.index.proofs_of(row) {
            if let Some(earlier) = rows.insert(proof.base_offset, *proof) {
                assert_eq!(
                    earlier, *proof,
                    "two rows for the batch at {}",
                    proof.base_offset
                );
            }
        }
    }
    rows
}

/// The messages of the batch `proof` names, once the rows of the object
/// that holds them rebuild the placed batch the partition committed.
fn proven_blob<'a>(
    objects: &'a [FreshInBucket],
    partition: IggyNamespace,
    proof: &FreshProofRow,
) -> &'a [u8] {
    let source = objects
        .iter()
        .find(|object| object.id == proof.object_id)
        .expect("the object that holds the batch is in the bucket");
    let (row, batch) = source
        .index
        .batch_at(partition.inner(), proof.position)
        .expect("a batch of the partition starts where the row says");
    assert_eq!(batch.message_count, proof.message_count);
    let header = BatchHeader {
        partition_id: u64::from(PARTITION_ID),
        base_offset: proof.base_offset,
        base_timestamp: proof.base_timestamp,
        origin_timestamp: batch.origin_timestamp,
        batch_length: (BATCH_HEADER_SIZE as u64) + u64::from(batch.blob_length),
        batch_checksum: 0,
        message_count: batch.message_count,
        producer: batch.producer.and_then(|producer| {
            BatchProducer::new(producer.producer_id, producer.epoch, producer.base_sequence)
        }),
    };
    let placement = Placement {
        object_id: proof.object_id.get(),
        position: proof.position,
        length: u64::from(batch.blob_length),
        blob_checksum: batch.blob_checksum,
        proven_below: row.proven_below,
    };
    let mut placed = [0; PLACED_BATCH_SIZE];
    PlacedBatch::new(header, placement).encode_into(&mut placed);
    assert_eq!(
        placement::checksum_of(&placed),
        proof.placed_checksum,
        "the rows rebuild the committed batch at {}",
        proof.base_offset
    );
    let start = usize::try_from(proof.position).expect("a position fits");
    let blob = &source.bytes[start..start + batch.blob_length as usize];
    assert!(
        placement.matches(blob),
        "the blob is the one its placement names"
    );
    blob
}

fn contains(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|window| window == needle)
}
