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
#[cfg(not(feature = "vsr"))]
pub mod bench_scenario;
// Concurrent produce+consume race regression: trips a metadata-plane
// consensus race under vsr (`on_ack: committed prepare must be in journal`
// panic on the primary); skip until the metadata races are fixed.
#[cfg(not(feature = "vsr"))]
pub mod concurrent_produce_consume_scenario;
#[cfg(not(feature = "vsr"))]
pub mod concurrent_scenario;
pub mod consumer_group_auto_commit_reconnection_scenario;
// Only consumed by `cg_vsr` (server-ng); legacy `cg.rs` has no equivalent.
#[cfg(feature = "vsr")]
pub mod consumer_group_duplicate_name_create_scenario;
pub mod consumer_group_join_scenario;
pub mod consumer_group_new_messages_after_restart_scenario;
pub mod consumer_group_offset_cleanup_scenario;
pub mod consumer_group_with_multiple_clients_polling_messages_scenario;
pub mod consumer_group_with_single_client_polling_messages_scenario;
pub mod consumer_timestamp_polling_scenario;
pub mod create_message_payload;
#[cfg(not(feature = "vsr"))]
pub mod cross_protocol_pat_scenario;
#[cfg(not(feature = "vsr"))]
pub mod encryption_scenario;
#[cfg(not(feature = "vsr"))]
pub mod invalid_consumer_offset_scenario;
// Asserts server log-file rotation/archival policies; server-ng's file
// logger only captures bootstrap output (shard-thread logs never reach the
// file), so volume-based rotation rules cannot trigger.
#[cfg(not(feature = "vsr"))]
pub mod log_rotation_scenario;
pub mod message_cleanup_scenario;
pub mod message_headers_scenario;
pub mod message_size_scenario;
pub mod offset_scenario;
#[cfg(not(feature = "vsr"))]
pub mod permissions_scenario;
pub mod purge_delete_scenario;
pub mod read_during_persistence_scenario;
#[cfg(not(feature = "vsr"))]
pub mod reconnect_after_restart_scenario;
#[cfg(not(feature = "vsr"))]
pub mod restart_offset_skip_scenario;
#[cfg(not(feature = "vsr"))]
pub mod segment_rotation_race_scenario;
pub mod single_message_per_batch_scenario;
#[cfg(not(feature = "vsr"))]
pub mod snapshot_scenario;
pub mod stale_client_consumer_group_scenario;
#[cfg(not(feature = "vsr"))]
pub mod stream_size_validation_scenario;
#[cfg(feature = "vsr")]
pub mod stress_produce_consume_scenario;
#[cfg(not(feature = "vsr"))]
pub mod system_scenario;
#[cfg(not(feature = "vsr"))]
pub mod tcp_tls_scenario;
pub mod timestamp_scenario;
#[cfg(not(feature = "vsr"))]
pub mod user_scenario;
#[cfg(not(feature = "vsr"))]
pub mod websocket_tls_scenario;

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
#[cfg(not(feature = "vsr"))]
const CONSUMER_KIND: ConsumerKind = ConsumerKind::Consumer;
const MESSAGES_COUNT: u32 = 1337;

/// Poll until the partition serves `expected_count` messages or
/// [`POLL_CONVERGENCE_TIMEOUT`] expires, returning the last poll result.
///
/// `send_messages` acks at consensus commit while the owning shard applies
/// the batch asynchronously (see the materialisation race note at the top
/// of `server-ng/src/partition_reconciler.rs`), so the first read after a
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

#[cfg(not(feature = "vsr"))]
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
