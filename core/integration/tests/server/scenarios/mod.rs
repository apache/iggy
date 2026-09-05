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

pub mod authentication_scenario;
pub mod concurrent_produce_consume_scenario;
pub mod concurrent_scenario;
pub mod consumer_group_auto_commit_reconnection_scenario;
pub mod consumer_group_duplicate_name_create_scenario;
pub mod consumer_group_join_scenario;
pub mod consumer_group_new_messages_after_restart_scenario;
pub mod consumer_group_offset_cleanup_scenario;
pub mod consumer_group_with_multiple_clients_polling_messages_scenario;
pub mod consumer_group_with_single_client_polling_messages_scenario;
pub mod consumer_timestamp_polling_scenario;
// Cross-protocol PAT visibility (create via HTTP, list via TCP across shards,
// and the reverse). Runs under vsr too: the server serves the PAT routes on its
// shard-0 HTTP listener and the create/delete commit through the metadata STM,
// so the token replicates to every shard a TCP client may land on.
pub mod cross_protocol_pat_scenario;
pub mod delete_stats_rollback_scenario;
pub mod encryption_scenario;
pub mod invalid_consumer_offset_scenario;
pub mod log_rotation_scenario;
pub mod message_cleanup_scenario;
pub mod message_headers_scenario;
pub mod message_size_scenario;
pub mod offset_scenario;
pub mod permissions_scenario;
pub mod purge_delete_scenario;
pub mod read_during_persistence_scenario;
pub mod reconnect_after_restart_scenario;
pub mod restart_offset_skip_scenario;
pub mod segment_rotation_race_scenario;
pub mod single_message_per_batch_scenario;
pub mod snapshot_scenario;
pub mod stale_client_consumer_group_scenario;
pub mod stream_size_validation_scenario;
pub mod system_scenario;
pub mod tcp_tls_scenario;
pub mod timestamp_scenario;
pub mod user_scenario;
pub mod websocket_tls_scenario;

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{TestHarness, delete_user};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const PARTITION_ID: u32 = 0;
const POLL_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_NAME: &str = "test-consumer-group";
const USERNAME_1: &str = "user1";
const USERNAME_2: &str = "user2";
const USERNAME_3: &str = "user3";
const CONSUMER_KIND: ConsumerKind = ConsumerKind::Consumer;
const MESSAGES_COUNT: u32 = 1337;

// The partition plane applies committed ops asynchronously on the owning shard
// (sends fold into the shared stats at commit-apply; purge/delete zero them
// when the reconciler drives the wipe), so a read racing that window can see a
// pre-apply value. Retry until the expectation holds, then make the terminal
// assertion for a real mismatch.
const STATS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
const STATS_RETRY_INTERVAL: Duration = Duration::from_millis(100);

const MESSAGE_PAYLOAD_SIZE_BYTES: u64 = 57;
// The server accounts the actual on-disk batch framing: one 256-byte
// `SendMessages` command header per append pass plus a 48-byte per-message
// header (`server_common::send_messages::COMMAND_HEADER_SIZE` and
// `iggy_binary_protocol::batch::BATCH_MESSAGE_HEADER_SIZE`).
const NG_BATCH_HEADER_SIZE: u64 = 256;
const NG_MESSAGE_HEADER_SIZE: u64 = 48;

/// What the server counts for one append pass of `messages_count` messages
/// built by [`create_messages`].
const fn batch_size(messages_count: u64) -> u64 {
    NG_BATCH_HEADER_SIZE + (NG_MESSAGE_HEADER_SIZE + MESSAGE_PAYLOAD_SIZE_BYTES) * messages_count
}

/// One append pass worth of messages, sized so [`batch_size`] predicts what the
/// server will report for them.
fn create_messages(messages_count: u64) -> Vec<IggyMessage> {
    (0..messages_count)
        .map(|offset| {
            let payload = Bytes::from(vec![0xD; MESSAGE_PAYLOAD_SIZE_BYTES as usize]);
            IggyMessage::builder()
                .id(u128::from(offset) + 1)
                .payload(payload)
                .build()
                .expect("Failed to create message")
        })
        .collect()
}

/// Fetch the stream until its totals match or [`STATS_CONVERGENCE_TIMEOUT`]
/// expires, then assert on the last read.
async fn validate_stream(
    client: &IggyClient,
    stream_name: &str,
    expected_size: u64,
    expected_messages_count: u64,
) {
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let stream = loop {
        let stream = client
            .get_stream(&Identifier::named(stream_name).unwrap())
            .await
            .unwrap()
            .expect("Failed to get stream");
        if (stream.size == expected_size && stream.messages_count == expected_messages_count)
            || Instant::now() >= deadline
        {
            break stream;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };
    assert_eq!(stream.size, expected_size, "stream size mismatch");
    assert_eq!(
        stream.messages_count, expected_messages_count,
        "stream messages_count mismatch"
    );
}

/// Topic-level counterpart of [`validate_stream`].
async fn validate_topic(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    expected_size: u64,
    expected_messages_count: u64,
) {
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let topic = loop {
        let topic = client
            .get_topic(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
            )
            .await
            .unwrap()
            .expect("Failed to get topic");
        if (topic.size == expected_size && topic.messages_count == expected_messages_count)
            || Instant::now() >= deadline
        {
            break topic;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };
    assert_eq!(topic.size, expected_size, "topic size mismatch");
    assert_eq!(
        topic.messages_count, expected_messages_count,
        "topic messages_count mismatch"
    );
}

/// Server-wide counterpart of [`validate_stream`], reading `[stats]` rather
/// than one entity.
async fn validate_system_stats(
    client: &IggyClient,
    expected_size: u64,
    expected_messages_count: u64,
) {
    let deadline = Instant::now() + STATS_CONVERGENCE_TIMEOUT;
    let stats = loop {
        let stats = client.get_stats().await.unwrap();
        if (stats.messages_count == expected_messages_count
            && stats.messages_size_bytes.as_bytes_u64() == expected_size)
            || Instant::now() >= deadline
        {
            break stats;
        }
        sleep(STATS_RETRY_INTERVAL).await;
    };
    assert_eq!(
        stats.messages_count, expected_messages_count,
        "system stats messages_count mismatch"
    );
    assert_eq!(
        stats.messages_size_bytes.as_bytes_u64(),
        expected_size,
        "system stats messages_size_bytes mismatch"
    );
}

/// Poll until the partition serves `expected_count` messages or
/// [`POLL_CONVERGENCE_TIMEOUT`] expires, returning the last poll result.
///
/// `send_messages` acks at consensus commit while the owning shard applies
/// the batch asynchronously (see the materialisation race note at the top
/// of `server/src/partition_reconciler.rs`), so the first read after a
/// send burst can observe fewer messages than were acked. Retrying absorbs
/// that convergence window without weakening the caller's assertion: real
/// message loss still returns short and fails it once the deadline expires.
async fn poll_until_expected_count(
    client: &IggyClient,
    stream_name: &str,
    topic_name: &str,
    strategy: &PollingStrategy,
    expected_count: u32,
) -> PolledMessages {
    let deadline = Instant::now() + POLL_CONVERGENCE_TIMEOUT;
    loop {
        let polled = client
            .poll_messages(
                &Identifier::named(stream_name).unwrap(),
                &Identifier::named(topic_name).unwrap(),
                Some(PARTITION_ID),
                &Consumer::default(),
                strategy,
                expected_count,
                false,
            )
            .await
            .unwrap();
        if polled.messages.len() as u32 == expected_count || Instant::now() >= deadline {
            return polled;
        }
        sleep(POLL_RETRY_INTERVAL).await;
    }
}

async fn create_client(harness: &TestHarness) -> IggyClient {
    harness
        .new_client()
        .await
        .expect("Failed to create new client")
}

async fn get_consumer_group(client: &IggyClient) -> ConsumerGroupDetails {
    client
        .get_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get consumer group")
}

async fn join_consumer_group(client: &IggyClient) {
    client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();
}

async fn leave_consumer_group(client: &IggyClient) {
    client
        .leave_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();
}

async fn cleanup(system_client: &IggyClient, delete_users: bool) {
    if delete_users {
        delete_user(system_client, USERNAME_1).await;
        delete_user(system_client, USERNAME_2).await;
        delete_user(system_client, USERNAME_3).await;
    }
    system_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
