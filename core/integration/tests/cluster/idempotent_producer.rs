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

//! Idempotent producers: a batch carries its producer id, epoch and
//! base sequence in the batch header, through the prepare, to the segment
//! file and back to polls.

use iggy::prelude::*;
use iggy_binary_protocol::batch::BatchHeader;
use integration::harness::{TestBinary, TestHarness};
use integration::iggy_harness;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const STREAM_NAME: &str = "producers";
const TOPIC_NAME: &str = "orders";
const PARTITION_ID: u32 = 0;
const RECOVER_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[iggy_harness(cluster_nodes = 1, server(sharding.cpu_allocation = "0..1"))]
async fn given_a_producer_batch_when_the_node_restarts_should_serve_it_and_keep_the_producer(
    harness: &mut TestHarness,
) {
    let client = connect(harness).await;
    create_topic(&client).await;
    let producer = BatchProducer::new(7_000, 2, 0).expect("a producer within the field limits");
    let payloads = ["first", "second", "third"];

    let sent = send(&client, producer, &payloads).await;
    assert_eq!(sent, Some(0), "the first batch lands at offset 0");
    drop(client);

    harness.node_mut(0).stop().expect("stop the node");
    harness.node_mut(0).start().expect("restart the node");

    let client = reconnect(harness).await;
    assert_eq!(poll_payloads(&client).await, payloads);
    let stored = stored_batches(&harness.node(0).data_path());
    assert!(
        stored
            .iter()
            .any(|header| header.producer == Some(producer) && header.message_count == 3),
        "the segment file must hold the batch with its producer, found {stored:?}"
    );
}

pub(crate) async fn connect(harness: &TestHarness) -> IggyClient {
    harness
        .node(0)
        .tcp_client()
        .expect("tcp client builder")
        .with_root_login()
        .connect()
        .await
        .expect("connect to the node")
}

/// A client of the restarted node, once it serves the topic again.
async fn reconnect(harness: &TestHarness) -> IggyClient {
    let deadline = Instant::now() + RECOVER_TIMEOUT;
    loop {
        if let Ok(builder) = harness.node(0).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
            && client
                .get_topic(&stream_id(), &topic_id())
                .await
                .is_ok_and(|topic| topic.is_some())
        {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "the node did not recover within {RECOVER_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// One persisted partition that writes each commit to its segment.
pub(crate) async fn create_topic(client: &IggyClient) {
    client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    client
        .create_topic(
            &stream_id(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(1),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

/// The offset of the batch's first message, as the reply confirms it.
pub(crate) async fn send(
    client: &IggyClient,
    producer: BatchProducer,
    payloads: &[&str],
) -> Option<u64> {
    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .map(|payload| IggyMessage::from_str(payload).expect("message"))
        .collect();
    client
        .send_producer_messages(
            &stream_id(),
            &topic_id(),
            &Partitioning::partition_id(PARTITION_ID),
            producer,
            &mut messages,
        )
        .await
        .expect("send a producer batch")
        .confirmations
        .first()
        .map(|confirmation| confirmation.base_offset)
}

pub(crate) async fn poll_payloads(client: &IggyClient) -> Vec<String> {
    client
        .poll_messages(
            &stream_id(),
            &topic_id(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            100,
            false,
        )
        .await
        .expect("poll the partition")
        .messages
        .iter()
        .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
        .collect()
}

fn stream_id() -> Identifier {
    Identifier::named(STREAM_NAME).expect("stream identifier")
}

fn topic_id() -> Identifier {
    Identifier::named(TOPIC_NAME).expect("topic identifier")
}

/// The header of every batch in the node's segment files.
fn stored_batches(data_path: &Path) -> Vec<BatchHeader> {
    let mut segments = Vec::new();
    collect_segments(data_path, &mut segments);
    let mut headers = Vec::new();
    for segment in segments {
        let bytes = std::fs::read(&segment).expect("read a segment file");
        let mut position = 0;
        while let Ok(header) = BatchHeader::decode(&bytes[position..]) {
            position += header.total_size();
            headers.push(header);
            if position >= bytes.len() {
                break;
            }
        }
    }
    headers
}

fn collect_segments(directory: &Path, segments: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_segments(&path, segments);
        } else if path.extension().is_some_and(|extension| extension == "log") {
            segments.push(path);
        }
    }
}
