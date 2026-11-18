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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use tokio::time::{Instant, sleep};

const STREAM: &str = "geo-stream";
const TOPIC: &str = "geo-topic";
const PARTITION_ID: u32 = 0;
const MESSAGES: u64 = 20;
const MESSAGE_BYTES: usize = 1024;
/// How long the cluster may take to accept a geo topic: the node claims the
/// store first, and the metadata primary then has to see it ready.
const READY_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

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

fn server_with_fs_store(store: &Path) -> TestHarness {
    let extra_envs = HashMap::from([
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
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        topic_type: Some(TopicType::Geo),
        geo_mode,
        ..TopicCreateOptions::default()
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
