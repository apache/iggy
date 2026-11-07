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

//! At-least-once delivery of the Iceberg sink. Every test ends with a sentinel message: the
//! runtime delivers a partition in order, so once the snapshot that holds the sentinel is in
//! the table, every delivery before it is too, and a duplicate shows in the row count.

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{
    DEFAULT_NAMESPACE, DEFAULT_TABLE, DEFERRED_CHECKPOINT_BATCH_LENGTH, IcebergAtLeastOnceFixture,
    IcebergDeferredCheckpointFixture, IcebergOps,
};
use bytes::Bytes;
use iggy::prelude::{IggyClient, IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use integration::harness::{ConnectorsRuntimeHandle, seeds};
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const POLL_INTERVAL: Duration = Duration::from_millis(500);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(60);
/// A start after a kill waits for the session of the killed runtime to end before its
/// partitions move to the new one.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(120);

/// The batch that fails while the catalog is down is handed over again by the runtime once
/// the sink's own commit retries run out, and lands once the catalog is back.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/iceberg/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_at_least_once_when_the_catalog_is_down_longer_than_the_commit_retries_should_commit_every_row_once(
    harness: &TestHarness,
    fixture: IcebergAtLeastOnceFixture,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    send(&client, json_messages(1, 3)).await;
    wait_for_commit_of(&fixture, 2, DELIVERY_TIMEOUT).await;

    fixture
        .stop_catalog()
        .await
        .expect("the catalog should stop");
    send(&client, json_messages(4, 4)).await;
    wait_for_redelivery(
        harness
            .connectors_runtime()
            .expect("connector runtime should be available"),
    )
    .await;
    fixture
        .start_catalog()
        .await
        .expect("the catalog should start again");
    send(&client, json_messages(8, 1)).await;

    wait_for_commit_of(&fixture, 7, DELIVERY_TIMEOUT).await;
    assert_eq!(
        committed_rows(&fixture).await,
        8,
        "every row should be committed once"
    );
}

/// The sink committed every message, in several batches, but the runtime stored no offset
/// before it was killed. The restart hands every message over again, starting with a batch that
/// does not carry the newest append, and the table must not hold any of them twice.
#[iggy_harness(
    server(
        heartbeat.interval = "8s",
        consumer_group.heartbeat_interval = "1s",
        connectors_runtime(config_path = "tests/connectors/iceberg/sink.toml")
    ),
    seed = seeds::connector_stream
)]
async fn given_at_least_once_when_the_runtime_is_killed_before_storing_offsets_should_hold_every_row_once(
    harness: &mut TestHarness,
    fixture: IcebergDeferredCheckpointFixture,
) {
    let backlog = 3 * DEFERRED_CHECKPOINT_BATCH_LENGTH as usize;
    let last = backlog as u64 - 1;
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    send(&client, json_messages(1, backlog)).await;
    wait_for_commit_of(&fixture, last, DELIVERY_TIMEOUT).await;

    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connector runtime should be available")
        .kill()
        .expect("the runtime should be killed");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("the runtime should start again");
    send(&client, json_messages(backlog as u128 + 1, 1)).await;

    wait_for_commit_of(&fixture, last + 1, RECOVERY_TIMEOUT).await;
    assert_eq!(
        committed_rows(&fixture).await,
        backlog as u64 + 1,
        "the messages delivered again should not be appended twice"
    );
    let skipped_first_batch = format!(
        "Skipping {DEFERRED_CHECKPOINT_BATCH_LENGTH} messages from offset: 0 to offset: {}",
        DEFERRED_CHECKPOINT_BATCH_LENGTH - 1
    );
    let (logs, _) = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .collect_logs();
    assert!(
        logs.contains(&skipped_first_batch),
        "the restart should deliver the backlog again and skip it, from its first batch on"
    );
}

async fn send(client: &IggyClient, mut messages: Vec<IggyMessage>) {
    let stream_id: Identifier = seeds::names::STREAM.try_into().expect("valid stream name");
    let topic_id: Identifier = seeds::names::TOPIC.try_into().expect("valid topic name");
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("the messages should be sent");
}

fn json_messages(first_id: u128, count: usize) -> Vec<IggyMessage> {
    create_test_messages(count)
        .iter()
        .zip(first_id..)
        .map(|(row, id)| {
            let payload = serde_json::to_vec(row).expect("the row should serialize");
            IggyMessage::builder()
                .id(id)
                .payload(Bytes::from(payload))
                .build()
                .expect("the message should build")
        })
        .collect()
}

/// The rows that the table's snapshots appended.
async fn committed_rows(fixture: &impl IcebergOps) -> u64 {
    let metadata = fixture
        .get_table_metadata(DEFAULT_NAMESPACE, DEFAULT_TABLE)
        .await
        .expect("the table metadata should load");
    metadata
        .metadata
        .snapshots
        .unwrap_or_default()
        .iter()
        .filter_map(|snapshot| snapshot.summary.as_ref()?.added_records.as_deref())
        .map(|records| records.parse::<u64>().expect("added-records is a count"))
        .sum()
}

/// Waits until a snapshot records the message at `offset` as the last one it holds.
async fn wait_for_commit_of(fixture: &impl IcebergOps, offset: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let offset = offset.to_string();
    loop {
        let committed = fixture
            .get_table_metadata(DEFAULT_NAMESPACE, DEFAULT_TABLE)
            .await
            .ok()
            .and_then(|metadata| metadata.metadata.snapshots)
            .unwrap_or_default()
            .iter()
            .any(|snapshot| {
                snapshot
                    .summary
                    .as_ref()
                    .and_then(|summary| summary.last_offset.as_deref())
                    == Some(offset.as_str())
            });
        if committed {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no snapshot holds the message at offset {offset}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Waits for the runtime to hand a failed batch over again.
async fn wait_for_redelivery(runtime: &ConnectorsRuntimeHandle) {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while !runtime
        .collect_logs()
        .0
        .contains("Delivering again from offsets")
    {
        assert!(
            Instant::now() < deadline,
            "the runtime should deliver the failed batch again"
        );
        sleep(POLL_INTERVAL).await;
    }
}
