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

//! A geo topic on a three-node cluster whose nodes share one `fs` object
//! store: each replica of a partition holds the placed batches, and any of
//! them reads the messages from the store.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use tokio::time::sleep;

const STREAM: &str = "geo-cluster-stream";
const TOPIC: &str = "geo-cluster-topic";
const PARTITION_ID: u32 = 0;
const MESSAGES: u64 = 20;
/// More sends than `EVICTED_RING_CAPACITY` keeps for repair, so a node that
/// lost them all cannot take them from another replica's journal.
const MESSAGES_PAST_REPAIR: u64 = 200;
const EVICTED_RING_CAPACITY: &str = "64";
const MESSAGE_BYTES: usize = 1024;
/// What a replica keeps of one send to a geo topic: the batch header and
/// where the messages sit in the object store.
const PLACED_BATCH_BYTES: u64 = 320;
const INSTALL_MARKER: &str = "partition state transfer installed";
/// How long the cluster may take to accept a geo topic, to elect a primary
/// after a node stops, and to transfer a partition to a node that lost it.
const SETTLE_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[tokio::test]
async fn given_a_geo_topic_on_three_nodes_when_one_stops_should_read_the_sends_and_take_more() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = cluster_with_fs_store(store.path(), &[]);
    harness.start().await.expect("the cluster boots");
    seed_geo_topic(&harness, MESSAGES, None).await;

    harness.stop_node(1).expect("stop node 1");

    send_and_read_through(&harness, &[0, 2], MESSAGES).await;
}

/// The node that returns holds nothing, and the other replicas still have
/// every send in their journals, so repair brings the placed batches back.
/// With a second node stopped, no send commits unless that copy takes part.
#[tokio::test]
async fn given_a_geo_topic_on_three_nodes_when_a_node_returns_without_its_disk_should_repair_the_placed_batches()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = cluster_with_fs_store(store.path(), &[]);
    harness.start().await.expect("the cluster boots");
    seed_geo_topic(&harness, MESSAGES, None).await;

    harness
        .restart_node_from_clean_slate(2)
        .expect("restart node 2 without its data");
    await_placed_batches(&harness, 2, MESSAGES).await;
    harness.stop_node(1).expect("stop node 1");

    send_and_read_through(&harness, &[0, 2], MESSAGES).await;
}

/// As above, with more sends than a replica keeps for repair: the node gets
/// the segments of placed batches in a state transfer.
#[tokio::test]
async fn given_a_geo_topic_past_the_repair_window_when_a_node_returns_without_its_disk_should_transfer_the_placed_batches()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = cluster_with_fs_store(
        store.path(),
        &[
            ("IGGY_SHARDING_CPU_ALLOCATION", "0..1"),
            (
                "IGGY_PARTITION_EVICTED_RING_CAPACITY",
                EVICTED_RING_CAPACITY,
            ),
        ],
    );
    harness.start().await.expect("the cluster boots");
    seed_geo_topic(&harness, MESSAGES_PAST_REPAIR, Some(1)).await;

    harness
        .restart_node_from_clean_slate(2)
        .expect("restart node 2 without its data");
    let deadline = Instant::now() + SETTLE_DEADLINE;
    while !harness.node(2).stdout_contains(INSTALL_MARKER) {
        assert!(
            Instant::now() < deadline,
            "node 2 never installed the transferred partition"
        );
        sleep(POLL_INTERVAL).await;
    }
    await_placed_batches(&harness, 2, MESSAGES_PAST_REPAIR).await;
    harness.stop_node(1).expect("stop node 1");

    send_and_read_through(&harness, &[0, 2], MESSAGES_PAST_REPAIR).await;
}

fn cluster_with_fs_store(store: &Path, extra: &[(&str, &str)]) -> TestHarness {
    let mut extra_envs = HashMap::from([
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
        extra
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
    );
    TestHarness::builder()
        .cluster_nodes(3)
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// Creates the geo topic once the cluster accepts one, sends `count`
/// messages and reads them back.
async fn seed_geo_topic(harness: &TestHarness, count: u64, messages_required_to_save: Option<u32>) {
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        topic_type: Some(TopicType::Geo),
        messages_required_to_save,
        ..TopicCreateOptions::default()
    };
    let deadline = Instant::now() + SETTLE_DEADLINE;
    loop {
        match client.create_topic(&stream, TOPIC, &options).await {
            Ok(_) => break,
            Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(error) => panic!("the cluster never accepted a geo topic: {error}"),
        }
    }
    for index in 0..count {
        send(&client, &stream, &topic, index)
            .await
            .expect("send messages");
    }
    assert_eq!(read_all(&client, &stream, &topic).await, Ok(count));
}

/// Waits until the segments of node `node` hold `count` placed batches. Read
/// off its disk: a poll would be answered by the primary.
async fn await_placed_batches(harness: &TestHarness, node: usize, count: u64) {
    let data_path = harness.node(node).data_path();
    let deadline = Instant::now() + SETTLE_DEADLINE;
    loop {
        let held = segment_bytes(&data_path);
        if held == count * PLACED_BATCH_BYTES {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the segments of node {node} hold {held} bytes, not the {count} placed batches"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The bytes of every partition segment under `root`. A segment is named by
/// its start offset alone, which the files of the prepare journal are not.
fn segment_bytes(root: &Path) -> u64 {
    let mut total = 0;
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "log")
                && path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .is_some_and(|stem| {
                        stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit())
                    })
            {
                total += entry.metadata().map_or(0, |metadata| metadata.len());
            }
        }
    }
    total
}

/// Sends one more message through whichever of `nodes` takes it and reads
/// every message back, `held` of them from before.
async fn send_and_read_through(harness: &TestHarness, nodes: &[usize], held: u64) {
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    let deadline = Instant::now() + SETTLE_DEADLINE;
    let mut sent = false;
    loop {
        if let Some(client) = connect_any(harness, nodes).await {
            sent = sent || send(&client, &stream, &topic, held).await.is_ok();
            if sent && read_all(&client, &stream, &topic).await == Ok(held + 1) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "nodes {nodes:?} never served the {held} messages and one more (sent: {sent})"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn connect_any(harness: &TestHarness, nodes: &[usize]) -> Option<IggyClient> {
    for &node in nodes {
        if let Ok(builder) = harness.node(node).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
        {
            return Some(client);
        }
    }
    None
}

async fn send(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    index: u64,
) -> Result<(), IggyError> {
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
        .map(|_| ())
}

/// Reads the partition from its start and returns how many messages it
/// holds, each checked against the payload of its offset.
async fn read_all(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Result<u64, IggyError> {
    let mut offset = 0;
    loop {
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
            .await?;
        if polled.messages.is_empty() {
            return Ok(offset);
        }
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
