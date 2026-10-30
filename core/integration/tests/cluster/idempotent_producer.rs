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
//! file and back to polls. The partition writes each batch of a producer
//! once, also when another connection sends it again after a primary change.

use iggy::prelude::*;
use iggy_binary_protocol::batch::BatchHeader;
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::wire_conversions::identifier_to_wire;
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
const NODES: usize = 3;
const COMMIT_SPREAD: Duration = Duration::from_secs(1);
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
    assert_eq!(
        send(&client, producer, &payloads).await,
        Some(0),
        "the WAL replay must rebuild the producer table"
    );
    assert_eq!(poll_payloads(&client).await, payloads);
    let stored = stored_batches(&harness.node(0).data_path());
    assert!(
        stored
            .iter()
            .any(|header| header.producer == Some(producer) && header.message_count == 3),
        "the segment file must hold the batch with its producer, found {stored:?}"
    );
}

#[iggy_harness(cluster_nodes = 1, server(sharding.cpu_allocation = "0..1"))]
async fn given_a_producer_when_batches_repeat_skip_or_go_back_an_epoch_should_write_each_once(
    harness: &mut TestHarness,
) {
    let producer = |epoch, base_sequence| BatchProducer::new(9_000, epoch, base_sequence).unwrap();
    let first = connect(harness).await;
    create_topic(&first).await;
    assert_eq!(
        send(&first, producer(0, 0), &["a", "b", "c"]).await,
        Some(0)
    );
    // A new connection is a new session, as a gateway that takes over has.
    drop(first);

    let second = connect(harness).await;
    assert_eq!(
        send(&second, producer(0, 0), &["a", "b", "c"]).await,
        Some(0),
        "a resent batch gets the offset of its first write"
    );
    assert_eq!(send(&second, producer(0, 3), &["d"]).await, Some(3));
    assert_eq!(
        try_send(&second, producer(0, 9), &["gap"]).await,
        Err(IggyError::ProducerSequenceOutOfOrder)
    );
    assert_eq!(send(&second, producer(1, 0), &["e"]).await, Some(4));
    assert_eq!(
        try_send(&second, producer(0, 4), &["stale"]).await,
        Err(IggyError::ProducerEpochFenced)
    );
    let unknown = BatchProducer::new(9_001, 0, 5).unwrap();
    assert_eq!(
        try_send(&second, unknown, &["late"]).await,
        Err(IggyError::ProducerUnknown),
        "a producer that the partition does not hold starts at sequence 0"
    );
    assert_eq!(poll_payloads(&second).await, ["a", "b", "c", "d", "e"]);
}

#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "0..1"))]
async fn given_a_batch_resent_after_its_primary_stops_when_sent_through_another_node_should_answer_its_first_offset(
    harness: &mut TestHarness,
) {
    let producer = |base_sequence| BatchProducer::new(11_000, 0, base_sequence).unwrap();
    let other = BatchProducer::new(12_000, 0, 0).unwrap();
    let first = harness
        .root_client_for_node(0)
        .await
        .expect("connect to node 0");
    create_topic(&first).await;
    assert_eq!(send(&first, producer(0), &["a", "b", "c"]).await, Some(0));
    // The next prepare carries the commit of the first, and the wait covers a
    // commit heartbeat, so the backups record the batch before the stop.
    assert_eq!(send(&first, other, &["x"]).await, Some(3));
    sleep(COMMIT_SPREAD).await;
    let primary = partition_primary(&first).await;
    drop(first);

    harness
        .node_mut(primary)
        .stop()
        .expect("stop the partition primary");
    let survivor = (0..NODES)
        .find(|node| *node != primary)
        .expect("a node survives");
    let deadline = Instant::now() + RECOVER_TIMEOUT;
    let (second, resent) = loop {
        if let Ok(client) = harness.root_client_for_node(survivor).await
            && let Ok(resent) = try_send(&client, producer(0), &["a", "b", "c"]).await
        {
            break (client, resent);
        }
        assert!(
            Instant::now() < deadline,
            "no node answered the resent batch within {RECOVER_TIMEOUT:?}"
        );
        sleep(POLL_INTERVAL).await;
    };

    assert_eq!(resent, Some(0), "the new primary must know the first write");
    assert_eq!(send(&second, producer(3), &["d"]).await, Some(4));
    assert_eq!(poll_payloads(&second).await, ["a", "b", "c", "x", "d"]);
}

/// A 128 MiB WAL checkpoints after two batches of 33 MiB, as in `partition_dedup`.
#[iggy_harness(
    cluster_nodes = 1,
    server(partition.wal_bytes_max = "134225920 B", sharding.cpu_allocation = "0..1")
)]
async fn given_a_checkpointed_producer_table_when_the_node_restarts_should_still_know_the_first_write(
    harness: &mut TestHarness,
) {
    const CHECKPOINT_OP: u64 = 2;
    let producer = |base_sequence| BatchProducer::new(13_000, 0, base_sequence).unwrap();
    let client = connect(harness).await;
    create_topic(&client).await;
    assert_eq!(
        try_send_messages(&client, Some(producer(0)), bulky_messages(1)).await,
        Ok(Some(0))
    );
    assert_eq!(
        try_send_messages(&client, None, bulky_messages(2)).await,
        Ok(Some(u64::from(BULKY_MESSAGES)))
    );
    await_receipt_checkpoint(harness, CHECKPOINT_OP).await;
    drop(client);

    harness.node_mut(0).stop().expect("stop the node");
    harness.node_mut(0).start().expect("restart the node");

    let client = reconnect(harness).await;
    assert_eq!(
        try_send_messages(&client, Some(producer(0)), bulky_messages(1)).await,
        Ok(Some(0)),
        "the checkpoint must hold the first write"
    );
    assert_eq!(
        send(&client, producer(BULKY_MESSAGES), &["next"]).await,
        Some(2 * u64::from(BULKY_MESSAGES)),
        "the resent batch must not take offsets"
    );
}

#[iggy_harness(cluster_nodes = 1, server(sharding.cpu_allocation = "0..1"))]
async fn given_two_clients_and_a_new_sign_in_when_taking_producer_ids_should_never_repeat_one(
    harness: &mut TestHarness,
) {
    let first = connect(harness).await;
    create_topic(&first).await;
    let second = connect(harness).await;

    let first_id = first.next_producer_id().await.expect("an id of the first");
    let second_id = second
        .next_producer_id()
        .await
        .expect("an id of the second");
    assert_ne!(first_id >> 20, second_id >> 20, "each session owns a range");
    assert_eq!(first.next_producer_id().await, Ok(first_id + 1));

    first.logout_user().await.expect("sign out");
    first
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("sign in again");
    let renewed = first
        .next_producer_id()
        .await
        .expect("an id of the new session");
    assert!(
        renewed >> 20 > (first_id >> 20).max(second_id >> 20),
        "a new session starts a range above every earlier one"
    );

    let producer = BatchProducer::new(renewed, 0, 0).expect("an id within the field limits");
    assert_eq!(send(&first, producer, &["a"]).await, Some(0));
    assert_eq!(send(&second, producer, &["a"]).await, Some(0));
}

#[iggy_harness(cluster_nodes = 1, server(sharding.cpu_allocation = "0..1"))]
async fn given_an_idempotent_producer_when_its_chunk_comes_again_should_answer_the_first_offset(
    harness: &mut TestHarness,
) {
    let client = connect(harness).await;
    create_topic(&client).await;
    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .expect("a producer builder")
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(DirectConfig::builder().batch_length(1).build())
        .idempotent()
        .build();
    producer.init().await.expect("init the producer");
    let messages = ["a", "b"]
        .iter()
        .map(|payload| IggyMessage::from_str(payload).expect("message"))
        .collect();

    let sent = producer.send(messages).await.expect("send two chunks");

    let offsets: Vec<u64> = sent
        .confirmations
        .iter()
        .map(|confirmation| confirmation.base_offset)
        .collect();
    assert_eq!(offsets, [0, 1]);
    // A batch can show in more than one `.log` file under the data path.
    let mut written: Vec<BatchProducer> = Vec::new();
    for producer in stored_batches(&harness.node(0).data_path())
        .iter()
        .filter_map(|header| header.producer)
    {
        if !written.contains(&producer) {
            written.push(producer);
        }
    }
    let id = written
        .first()
        .expect("a batch with a producer")
        .producer_id();
    let numbered = |base_sequence| BatchProducer::new(id, 0, base_sequence).unwrap();
    assert_eq!(written, [numbered(0), numbered(1)]);
    assert_eq!(
        send(&client, numbered(0), &["a"]).await,
        Some(0),
        "the first chunk, sent again after a lost reply, answers its first offset"
    );
    assert_eq!(poll_payloads(&client).await, ["a", "b"]);
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
    try_send(client, producer, payloads)
        .await
        .expect("send a producer batch")
}

async fn try_send(
    client: &IggyClient,
    producer: BatchProducer,
    payloads: &[&str],
) -> Result<Option<u64>, IggyError> {
    let messages = payloads
        .iter()
        .map(|payload| IggyMessage::from_str(payload).expect("message"))
        .collect();
    try_send_messages(client, Some(producer), messages).await
}

async fn try_send_messages(
    client: &IggyClient,
    producer: Option<BatchProducer>,
    mut messages: Vec<IggyMessage>,
) -> Result<Option<u64>, IggyError> {
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    let response = match producer {
        Some(producer) => {
            client
                .send_producer_messages(
                    &stream_id(),
                    &topic_id(),
                    PARTITION_ID,
                    producer,
                    &mut messages,
                )
                .await?
        }
        None => {
            client
                .send_messages(&stream_id(), &topic_id(), &partitioning, &mut messages)
                .await?
        }
    };
    Ok(response
        .confirmations
        .first()
        .map(|confirmation| confirmation.base_offset))
}

const BULKY_MESSAGES: u32 = 33;

/// 33 messages of 1 MiB, a quarter of the 128 MiB WAL.
fn bulky_messages(fill: u8) -> Vec<IggyMessage> {
    (0..BULKY_MESSAGES)
        .map(|_| {
            IggyMessage::builder()
                .payload(vec![fill; 1024 * 1024].into())
                .build()
                .expect("build a message")
        })
        .collect()
}

/// Wait until the partition published the retry checkpoint of `op`, which also
/// lets it reclaim the WAL below.
async fn await_receipt_checkpoint(harness: &TestHarness, op: u64) {
    let name = format!("receipts-{op}.checkpoint");
    let deadline = Instant::now() + RECOVER_TIMEOUT;
    loop {
        let mut files = Vec::new();
        collect_files(&harness.node(0).data_path(), &mut files, &|path| {
            path.file_name().is_some_and(|file| *file == *name)
        });
        if !files.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the partition never published {name}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The node that holds the partition's primary.
async fn partition_primary(client: &IggyClient) -> usize {
    let request = GetPartitionRouteRequest {
        stream_id: identifier_to_wire(&stream_id()).expect("stream id"),
        topic_id: identifier_to_wire(&topic_id()).expect("topic id"),
        partition_id: PARTITION_ID,
    };
    let route = client
        .send_binary_request(GET_PARTITION_ROUTE_CODE, request.to_bytes())
        .await
        .expect("route the partition");
    usize::from(
        PartitionRouteResponse::decode_from(&route)
            .expect("decode the route")
            .primary_replica,
    )
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
    collect_files(data_path, &mut segments, &|path| {
        path.extension().is_some_and(|extension| extension == "log")
    });
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

fn collect_files(directory: &Path, files: &mut Vec<PathBuf>, wanted: &dyn Fn(&Path) -> bool) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files, wanted);
        } else if wanted(&path) {
            files.push(path);
        }
    }
}
