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

use super::{DatabaseRecord, POLL_ATTEMPTS, POLL_INTERVAL_MS, TEST_MESSAGE_COUNT};
use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{
    MySqlOps, MySqlSourceAliasedTrackingFixture, MySqlSourceCaseMismatchedTrackingFixture,
    MySqlSourceComputedTrackingFixture, MySqlSourceDeleteFixture,
    MySqlSourceDescendingQueryFixture, MySqlSourceJsonDirectFixture, MySqlSourceJsonFixture,
    MySqlSourceJsonTrackingFixture, MySqlSourceMarkFixture, MySqlSourceMissingPayloadColumnFixture,
    MySqlSourceNoMetadataFixture, MySqlSourceNullTrackingFixture, MySqlSourceOps,
    MySqlSourceRawFixture, MySqlSourceTextTrackingFixture, MySqlSourceTimestampDeleteFixture,
    MySqlSourceTimestampTrackingFixture, MySqlSourceTinyintTrackingFixture,
};
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, IggyTimestamp, PollingStrategy};
use integration::harness::seeds;
use integration::iggy_harness;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::sleep;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn json_rows_source_produces_messages_to_iggy(
    harness: &TestHarness,
    fixture: MySqlSourceJsonFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let window_start_micros = IggyTimestamp::now().as_micros();
    let test_messages = create_test_messages(TEST_MESSAGE_COUNT);
    for msg in &test_messages {
        fixture
            .insert_row(
                &pool,
                msg.id as i32,
                &msg.name,
                msg.count as i32,
                msg.amount,
                msg.active,
                msg.timestamp,
            )
            .await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<DatabaseRecord> = Vec::new();
    let mut header_timestamps: Vec<(u64, u64)> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice(&msg.payload) {
                    received.push(record);
                    header_timestamps.push((msg.header.timestamp, msg.header.origin_timestamp));
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    // Header timestamps are microseconds on the wire. The runtime currently
    // stamps them itself and drops the source's own values, so this guards the
    // unit end to end for the day they get wired through; the plugin-side units
    // are pinned by mysql_source's own unit test.
    let window_end_micros = IggyTimestamp::now().as_micros();
    let window = window_start_micros..=window_end_micros;
    for (i, (timestamp, origin_timestamp)) in header_timestamps.iter().enumerate() {
        assert!(
            window.contains(timestamp),
            "timestamp {timestamp} at record {i} is not microseconds in {window:?}"
        );
        assert!(
            window.contains(origin_timestamp),
            "origin timestamp {origin_timestamp} at record {i} is not microseconds in {window:?}"
        );
    }

    for (i, record) in received.iter().enumerate() {
        assert_eq!(
            record.table_name,
            fixture.table_name(),
            "Table name mismatch at record {i}"
        );
        assert_eq!(
            record.operation_type, "SELECT",
            "Operation type mismatch at record {i}"
        );
        assert_eq!(
            record.data, test_messages[i],
            "Message data mismatch at record {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn bare_payload_source_omits_metadata_envelope(
    harness: &TestHarness,
    fixture: MySqlSourceNoMetadataFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let test_messages = create_test_messages(TEST_MESSAGE_COUNT);
    for msg in &test_messages {
        fixture
            .insert_row(
                &pool,
                msg.id as i32,
                &msg.name,
                msg.count as i32,
                msg.amount,
                msg.active,
                msg.timestamp,
            )
            .await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, payload) in received.iter().enumerate() {
        let object = payload
            .as_object()
            .unwrap_or_else(|| panic!("Record {i} is not a JSON object: {payload}"));
        assert!(
            !object.contains_key("table_name") && !object.contains_key("operation_type"),
            "Record {i} should be a bare column map with no metadata envelope, got {payload}"
        );
        assert_eq!(
            object["id"].as_u64(),
            Some(test_messages[i].id),
            "Bare-payload id mismatch at record {i}"
        );
        assert_eq!(
            object["name"].as_str(),
            Some(test_messages[i].name.as_str()),
            "Bare-payload name mismatch at record {i}"
        );
        assert_eq!(
            object["active"].as_bool(),
            Some(test_messages[i].active),
            "Bare-payload active mismatch at record {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn raw_rows_source_produces_raw_messages_to_iggy(
    harness: &TestHarness,
    fixture: MySqlSourceRawFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let payloads: Vec<Vec<u8>> = vec![
        b"hello world".to_vec(),
        vec![0x00, 0x01, 0x02, 0xFF, 0xFE],
        serde_json::to_vec(&serde_json::json!({"key": "value", "number": 42}))
            .expect("Failed to serialize json"),
    ];

    for (i, payload) in payloads.iter().enumerate() {
        fixture.insert_payload(&pool, (i + 1) as i32, payload).await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<Vec<u8>> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                received.push(msg.payload.to_vec());
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, payload) in received.iter().enumerate() {
        assert_eq!(payload, &payloads[i], "Payload mismatch at index {i}");
    }
}

/// A `payload_column` that no column matches must fail the table rather than
/// silently serializing the whole row: the stream is configured as `raw`, so the
/// fallback would publish a JSON envelope carrying a base64 rendering of the very
/// blob the consumer expects verbatim.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn missing_payload_column_source_produces_no_messages(
    harness: &TestHarness,
    fixture: MySqlSourceMissingPayloadColumnFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_payload(&pool, (i + 1) as i32, b"hello world")
            .await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<Vec<u8>> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                received.push(msg.payload.to_vec());
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.is_empty(),
        "Expected no messages when payload_column is absent from the table, got {}: {:?}",
        received.len(),
        received
            .iter()
            .map(|payload| String::from_utf8_lossy(payload).into_owned())
            .collect::<Vec<_>>()
    );

    // The rows stay in MySQL so the operator can fix the config and replay them.
    let pool = fixture
        .create_pool()
        .await
        .expect("Failed to recreate pool");
    let remaining: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM `{}`",
        fixture.table_name()
    )))
    .fetch_one(&pool)
    .await
    .expect("Failed to count rows");
    pool.close().await;
    assert_eq!(
        remaining, TEST_MESSAGE_COUNT as i64,
        "Rows must be left untouched when the table fails to process"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn json_direct_rows_source_produces_json_messages_to_iggy(
    harness: &TestHarness,
    fixture: MySqlSourceJsonDirectFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let json_payloads: Vec<serde_json::Value> = vec![
        serde_json::json!({"name": "Alice", "score": 100}),
        serde_json::json!({"items": ["a", "b", "c"]}),
        serde_json::json!({"nested": {"deep": {"value": 42}}}),
    ];

    for (i, payload) in json_payloads.iter().enumerate() {
        fixture.insert_json(&pool, (i + 1) as i32, payload).await;
    }
    pool.close().await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    for (i, payload) in received.iter().enumerate() {
        assert_eq!(
            payload, &json_payloads[i],
            "JSON payload mismatch at index {i}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn delete_after_read_source_removes_rows_after_producing(
    harness: &TestHarness,
    fixture: MySqlSourceDeleteFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, &format!("row_{i}"), (i * 10) as i32)
            .await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    let mut final_count = -1i64;
    for _ in 0..POLL_ATTEMPTS {
        final_count = fixture.count_rows(&pool).await;
        if final_count == 0 {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(
        final_count, 0,
        "Expected 0 rows after delete_after_read, got {final_count}"
    );

    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn processed_column_source_marks_rows_after_producing(
    harness: &TestHarness,
    fixture: MySqlSourceMarkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, &format!("row_{i}"), (i * 10) as i32)
            .await;
    }

    let initial_unprocessed = fixture.count_unprocessed(&pool).await;
    let initial_processed = fixture.count_processed(&pool).await;
    assert_eq!(
        initial_unprocessed + initial_processed,
        TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} total rows before processing, got {} unprocessed + {} processed",
        initial_unprocessed,
        initial_processed
    );

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(json) = serde_json::from_slice(&msg.payload) {
                    received.push(json);
                }
            }
            if received.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert!(
        received.len() >= TEST_MESSAGE_COUNT,
        "Expected at least {TEST_MESSAGE_COUNT} messages, got {}",
        received.len()
    );

    let mut final_unprocessed = -1i64;
    let mut final_processed = -1i64;
    for _ in 0..POLL_ATTEMPTS {
        final_unprocessed = fixture.count_unprocessed(&pool).await;
        final_processed = fixture.count_processed(&pool).await;
        if final_unprocessed == 0 && final_processed == TEST_MESSAGE_COUNT as i64 {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(
        final_unprocessed, 0,
        "Expected 0 unprocessed rows after processing, got {final_unprocessed}"
    );
    assert_eq!(
        final_processed, TEST_MESSAGE_COUNT as i64,
        "Expected {TEST_MESSAGE_COUNT} processed rows after processing, got {final_processed}"
    );

    let total_count = fixture.count_rows(&pool).await;
    assert_eq!(
        total_count, TEST_MESSAGE_COUNT as i64,
        "Rows should not be deleted, expected {TEST_MESSAGE_COUNT}, got {total_count}"
    );

    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn state_persists_across_connector_restart(
    harness: &mut TestHarness,
    fixture: MySqlSourceJsonFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let first_batch = create_test_messages(TEST_MESSAGE_COUNT);
    for msg in &first_batch {
        fixture
            .insert_row(
                &pool,
                msg.id as i32,
                &msg.name,
                msg.count as i32,
                msg.amount,
                msg.active,
                msg.timestamp,
            )
            .await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "state_test_consumer".try_into().unwrap();

    let client = harness.root_client().await.unwrap();
    let received_before = {
        let mut received: Vec<DatabaseRecord> = Vec::new();
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(polled) = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    None,
                    &Consumer::new(consumer_id.clone()),
                    &PollingStrategy::next(),
                    10,
                    true,
                )
                .await
            {
                for msg in polled.messages {
                    if let Ok(record) = serde_json::from_slice(&msg.payload) {
                        received.push(record);
                    }
                }
                if received.len() >= TEST_MESSAGE_COUNT {
                    break;
                }
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        received
    };
    assert_eq!(received_before.len(), TEST_MESSAGE_COUNT);

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");

    let second_batch_start_id = (TEST_MESSAGE_COUNT + 1) as i32;
    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(
                &pool,
                second_batch_start_id + i as i32,
                &format!("user_batch2_{i}"),
                ((TEST_MESSAGE_COUNT + i) * 10) as i32,
                (TEST_MESSAGE_COUNT + i) as f64 * 99.99,
                i % 2 == 0,
                iggy_common::IggyTimestamp::now().as_micros() as i64,
            )
            .await;
    }

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors");
    sleep(Duration::from_secs(2)).await;

    let mut received_after: Vec<DatabaseRecord> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice(&msg.payload) {
                    received_after.push(record);
                }
            }
            if received_after.len() >= TEST_MESSAGE_COUNT {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(received_after.len(), TEST_MESSAGE_COUNT);

    for record in &received_after {
        assert!(
            record.data.id > TEST_MESSAGE_COUNT as u64,
            "After restart, got ID {} from first batch",
            record.data.id
        );
    }

    pool.close().await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn descending_custom_query_publishes_nothing_and_leaves_rows_in_place(
    harness: &TestHarness,
    fixture: MySqlSourceDescendingQueryFixture,
) {
    // A custom query ordered DESC returns readable rows whose tracking values
    // decrease. Taking the last row's value as the cursor would move it backwards
    // and skip every row above it, so the connector must abandon the cycle: nothing
    // is published, and the rows stay in MySQL to be picked up once the query is
    // corrected.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture.insert_row(&pool, &format!("row_{i}")).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "descending_consumer".try_into().unwrap();

    let mut received = 0usize;
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received += polled.messages.len();
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(
        received, 0,
        "Expected no messages from a descending custom query, got {received}"
    );

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;
    assert_eq!(
        remaining, TEST_MESSAGE_COUNT as i64,
        "Expected all {TEST_MESSAGE_COUNT} rows to remain in MySQL, found {remaining}"
    );

    // Both assertions above also hold when the connector never starts, so pin down
    // that the rows were fetched and then rejected: only the guard names the column
    // and the pair of values that decreased.
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime should be running")
        .collect_logs();
    let logs = format!("{stdout}{stderr}");
    assert!(
        logs.contains("rows arrived out of order by tracking_column 'id'"),
        "Expected the ordering guard to reject the descending batch, not open() to fail"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn descending_computed_tracking_column_publishes_nothing_and_leaves_rows_in_place(
    harness: &TestHarness,
    fixture: MySqlSourceComputedTrackingFixture,
) {
    // The tracking column is an alias the query computes, so it has no
    // information_schema row and the comparison order can only come from the type
    // MySQL reports for it in the result set.
    //
    // The ids are chosen so that only the typed reading catches the reversal:
    // descending gives 100 then 99, which decreases numerically but *increases* by
    // collation, so a comparison that has to satisfy both readings lets the batch
    // through and strands row 99. Consecutive ids would not isolate this - 9 then 8
    // decreases under both.
    const IDS: [i32; 2] = [99, 100];

    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for id in IDS {
        fixture.insert_row(&pool, id, &format!("row_{id}")).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "computed_tracking_consumer".try_into().unwrap();

    let mut received = 0usize;
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received += polled.messages.len();
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    assert_eq!(
        received, 0,
        "Expected no messages from a descending computed tracking column, got {received}"
    );

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;
    assert_eq!(
        remaining,
        IDS.len() as i64,
        "Expected all {} rows to remain in MySQL, found {remaining}",
        IDS.len()
    );

    // The alias has no information_schema row, so a batch reaching the guard at all
    // means the result set supplied the comparison order. A failed open() would
    // satisfy both assertions above without any of that happening.
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime should be running")
        .collect_logs();
    let logs = format!("{stdout}{stderr}");
    assert!(
        logs.contains("rows arrived out of order by tracking_column 'tracking_id'"),
        "Expected the ordering guard to reject the descending batch, not open() to fail"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn text_tracking_column_keeps_reading_values_below_the_numeric_maximum(
    harness: &TestHarness,
    fixture: MySqlSourceTextTrackingFixture,
) {
    // `code` is a VARCHAR, so MySQL orders it by collation: '100' sorts before '2'
    // and the largest value is '5'. Writing the offset into the query as a bare
    // number makes MySQL convert the column and the literal to a double, filtering
    // numerically while ORDER BY sorts by collation, which walks the cursor up to
    // the numeric maximum '100'. A later '7' is then below that filter forever,
    // even though it sorts after '5' and has never been read.
    const SEEDED: [&str; 6] = ["1", "2", "5", "10", "42", "100"];
    const LATE: &str = "7";

    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for code in SEEDED {
        fixture.insert_row(&pool, code).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "text_tracking_consumer".try_into().unwrap();

    let mut seen: Vec<String> = Vec::new();
    let mut inserted_late = false;
    let mut late_seen = false;

    for _ in 0..(POLL_ATTEMPTS * 2) {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                50,
                true,
            )
            .await
        {
            for message in polled.messages {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&message.payload)
                    && let Some(code) = json.get("code").and_then(|code| code.as_str())
                {
                    seen.push(code.to_string());
                }
            }
        }

        // '7' sorts after the collation maximum '5' but sits below the numeric
        // maximum '100', so it is only reachable while the filter stays lexical.
        // It goes in once the seeded rows have been read, so the cursor has
        // already had every chance to climb.
        if !inserted_late && SEEDED.iter().all(|code| seen.iter().any(|s| s == code)) {
            fixture.insert_row(&pool, LATE).await;
            inserted_late = true;
        }
        if inserted_late && seen.iter().any(|code| code == LATE) {
            late_seen = true;
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    pool.close().await;
    assert!(
        inserted_late,
        "Expected every seeded code to be published before inserting '{LATE}', got {seen:?}"
    );
    assert!(
        late_seen,
        "Row '{LATE}' was never published: the offset filter compared numerically \
         while MySQL ordered by collation, so it is stranded below the cursor. Saw {seen:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn first_fetch_on_an_unresolved_table_discards_a_batch_its_filter_disagreed_with(
    harness: &TestHarness,
    fixture: MySqlSourceTextTrackingFixture,
) {
    // The table is created after the runtime is up, so the startup probe defers and
    // the first poll builds its filter from the unresolved kind: `code > 0`, bare.
    // MySQL coerces the column to a double for that comparison, which drops '0abc',
    // while ORDER BY sorts by collation, where '0abc' precedes '500'. The result set
    // then reports VARCHAR, so the filter that ran is not the one the corrected kind
    // would have written and the batch cannot be trusted: publishing it would take
    // the cursor to '500' and strand '0abc' below it permanently. Both codes
    // arriving is what separates discarding the batch from publishing it.
    //
    // There is a filter on the first poll at all only because the connector's sample
    // config.toml, which source.toml points config_dir at, sets initial_offset = "0";
    // the fixture cannot set it, since the env layer parses a numeric string as a
    // number and fails plugin init. Without it the poll is unfiltered, both rows
    // publish in one batch, and the log assertion below is what fails.
    const BELOW_THE_NUMERIC_FILTER: &str = "0abc";
    const ABOVE_IT: &str = "500";

    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for code in [BELOW_THE_NUMERIC_FILTER, ABOVE_IT] {
        fixture.insert_row(&pool, code).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "stale_kind_consumer".try_into().unwrap();

    let mut seen: BTreeSet<String> = BTreeSet::new();
    for _ in 0..(POLL_ATTEMPTS * 2) {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for message in polled.messages {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&message.payload)
                    && let Some(code) = json.get("code").and_then(|code| code.as_str())
                {
                    seen.insert(code.to_string());
                }
            }
        }
        if seen.len() >= 2 {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    pool.close().await;

    assert!(
        seen.contains(BELOW_THE_NUMERIC_FILTER),
        "Row '{BELOW_THE_NUMERIC_FILTER}' was never published: the first batch was published \
         under a numeric filter the column does not sort by, which parked the cursor at \
         '{ABOVE_IT}'. Saw {seen:?}"
    );
    assert!(
        seen.contains(ABOVE_IT),
        "Row '{ABOVE_IT}' was never published, so the table stopped rather than rebuilding \
         its query from the type the result set reported. Saw {seen:?}"
    );

    // Which poll published them matters: the second one, after the discarded batch
    // resolved the kind. Only the discard names the two literals that disagreed.
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime should be running")
        .collect_logs();
    let logs = format!("{stdout}{stderr}");
    assert!(
        logs.contains("the poll filtered on 'code'"),
        "Expected the first batch to be discarded for filtering on a stale comparison order"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn custom_query_aliasing_the_tracking_column_publishes_nothing_and_stays_disabled(
    harness: &TestHarness,
    fixture: MySqlSourceAliasedTrackingFixture,
) {
    // `SELECT id AS row_id` returns no `id`, so no row can yield a cursor. Publishing
    // the batch anyway would emit every row and leave the offset unset, replaying the
    // same result set on every poll. The query cannot correct itself, so the table is
    // disabled: rows inserted later must not be published either.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture.insert_row(&pool, &format!("row_{i}")).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "aliased_tracking_consumer".try_into().unwrap();

    let mut received = 0usize;
    let mut inserted_more = false;
    for attempt in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received += polled.messages.len();
        }

        // New rows partway through: a table that merely skipped a cycle would pick
        // these up, a disabled one stays silent.
        if !inserted_more && attempt == POLL_ATTEMPTS / 2 {
            for i in 0..TEST_MESSAGE_COUNT {
                fixture.insert_row(&pool, &format!("late_{i}")).await;
            }
            inserted_more = true;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let expected_rows = (TEST_MESSAGE_COUNT * 2) as i64;
    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;

    assert_eq!(
        received, 0,
        "Expected no messages from a query that does not project the tracking column, \
         got {received}"
    );
    assert_eq!(
        remaining, expected_rows,
        "Expected all {expected_rows} rows to remain in MySQL, found {remaining}"
    );

    // Publishing nothing is also what a per-row rejection would produce, so pin down
    // which check fired: only the result-set check can name the columns the query
    // returned, and only it disables the table.
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime should be running")
        .collect_logs();
    let logs = format!("{stdout}{stderr}");
    assert!(
        logs.contains("the result set has no column 'id'"),
        "Expected the result-set check to reject the batch and name the tracking column"
    );
    assert!(
        logs.contains("row_id, name"),
        "Expected the error to list the columns the query actually returned"
    );
    assert!(
        logs.contains("the table is disabled"),
        "Expected the table to be disabled rather than retried, since the query cannot \
         correct itself"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn null_tracking_value_publishes_nothing_until_the_row_is_repaired(
    harness: &TestHarness,
    fixture: MySqlSourceNullTrackingFixture,
) {
    // A NULL tracking value yields no cursor, so the batch must fail before anything
    // is published. Unlike a query that cannot project the column, the row itself can
    // be fixed, so the table stays enabled: once the values are backfilled every row
    // must arrive, which is also what proves the offset never moved while it stalled.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture.insert_row(&pool, &format!("row_{i}")).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "null_tracking_consumer".try_into().unwrap();

    let mut received = 0usize;
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received += polled.messages.len();
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(
        received, 0,
        "Expected no messages while every tracking value is NULL, got {received}"
    );

    fixture.backfill_tracking_values(&pool).await;

    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received += polled.messages.len();
        }
        if received >= TEST_MESSAGE_COUNT {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;

    // At-least-once, so a repaired row may arrive more than once. What the assertion
    // is for is that the rows arrive at all, without restarting the connector: the
    // table was never disabled, and the offset never moved past rows it had not
    // published. The zero above is what makes the pair discriminating.
    assert!(
        received >= TEST_MESSAGE_COUNT,
        "Expected all {TEST_MESSAGE_COUNT} rows once the tracking values were repaired, \
         got {received}: the table was disabled, or the offset advanced past rows that \
         were never published"
    );
    assert_eq!(
        remaining, TEST_MESSAGE_COUNT as i64,
        "Expected all {TEST_MESSAGE_COUNT} rows to remain in MySQL, found {remaining}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn timestamp_tracking_column_resumes_from_the_cursor_it_published(
    harness: &TestHarness,
    fixture: MySqlSourceTimestampTrackingFixture,
) {
    // MySQL prefix-parses the ` UTC` a TIMESTAMP cursor used to carry, so the second
    // wave arrives whichever way the connector renders it and this test does not
    // discriminate that fix: the DML path in
    // `timestamp_primary_key_deletes_the_rows_it_published` does. What is pinned here
    // is the payload keeping chrono's own rendering, trailing zone and all, since that
    // is the published wire format and must not move when the cursor rendering does,
    // plus TIMESTAMP tracking carrying across a cursor boundary at all.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, i as i64, &format!("row_{i}"))
            .await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "timestamp_tracking_consumer".try_into().unwrap();

    let mut received: Vec<serde_json::Value> = Vec::new();
    let mut names: BTreeSet<String> = BTreeSet::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                    if let Some(name) = record["data"]["name"].as_str() {
                        names.insert(name.to_string());
                    }
                    received.push(record);
                }
            }
        }
        if names.len() >= TEST_MESSAGE_COUNT {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    assert_eq!(
        names.len(),
        TEST_MESSAGE_COUNT,
        "Expected the first {TEST_MESSAGE_COUNT} rows before the cursor is exercised, got {names:?}"
    );

    // Every one of these sits above the cursor the first wave left behind, so they
    // are reachable only through the offset literal the connector wrote from it.
    for i in TEST_MESSAGE_COUNT..TEST_MESSAGE_COUNT * 2 {
        fixture
            .insert_row(&pool, i as i64, &format!("row_{i}"))
            .await;
    }

    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                    if let Some(name) = record["data"]["name"].as_str() {
                        names.insert(name.to_string());
                    }
                    received.push(record);
                }
            }
        }
        if names.len() >= TEST_MESSAGE_COUNT * 2 {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;

    // Counting distinct rows rather than messages: delivery is at-least-once, so a
    // replay is allowed, but a row stranded below a cursor that did not parse is not.
    assert_eq!(
        names.len(),
        TEST_MESSAGE_COUNT * 2,
        "Expected every row across the cursor boundary, got {names:?}"
    );
    assert_eq!(
        remaining,
        (TEST_MESSAGE_COUNT * 2) as i64,
        "Expected all {} rows to remain in MySQL, found {remaining}",
        TEST_MESSAGE_COUNT * 2
    );

    for (i, record) in received.iter().enumerate() {
        let updated_at = record["data"]["updated_at"]
            .as_str()
            .unwrap_or_else(|| panic!("record {i} carries no updated_at: {record}"));
        assert!(
            updated_at.ends_with(" UTC"),
            "record {i} published '{updated_at}': the payload rendering is the wire format \
             every existing deployment already parses, and only the cursor changes"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn timestamp_primary_key_deletes_the_rows_it_published(
    harness: &TestHarness,
    fixture: MySqlSourceTimestampDeleteFixture,
) {
    // `primary_key_column` defaults to `tracking_column`, so this config binds a
    // TIMESTAMP into the delete predicate. That predicate runs inside DML, where the
    // default STRICT_TRANS_TABLES turns a datetime conversion the SELECT path only
    // warns about into a hard error: the delete fails, the batch is dropped before
    // publishing, the offset stays put, and the same rows retry every poll. Nothing
    // arriving and nothing being deleted are the same failure here.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    for i in 0..TEST_MESSAGE_COUNT {
        fixture
            .insert_row(&pool, i as i64, &format!("row_{i}"))
            .await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "timestamp_delete_consumer".try_into().unwrap();

    let mut names: BTreeSet<String> = BTreeSet::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                    && let Some(name) = record["data"]["name"].as_str()
                {
                    names.insert(name.to_string());
                }
            }
        }
        if names.len() >= TEST_MESSAGE_COUNT {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;

    assert_eq!(
        names.len(),
        TEST_MESSAGE_COUNT,
        "Expected every row to be published, got {names:?}"
    );
    assert_eq!(
        remaining, 0,
        "Expected the published rows to be deleted by their TIMESTAMP key, {remaining} remain"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn json_tracking_column_fails_startup_even_with_a_custom_query(
    harness: &TestHarness,
    fixture: MySqlSourceJsonTrackingFixture,
) {
    // A JSON column has no ordered scalar form, so it can never produce a cursor. The
    // type is a property of the column that a projection preserves, so a custom_query
    // does not excuse it: the connector must refuse to start rather than poll a table
    // whose offset can never advance. The fixture creates the table before the runtime
    // starts, since a table that does not exist yet defers validation instead.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    for i in 0..TEST_MESSAGE_COUNT {
        fixture.insert_row(&pool, &format!("row_{i}")).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "json_tracking_consumer".try_into().unwrap();

    let mut received = 0usize;
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received += polled.messages.len();
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;

    assert_eq!(
        received, 0,
        "Expected no messages from a connector that must not start, got {received}"
    );
    assert_eq!(
        remaining, TEST_MESSAGE_COUNT as i64,
        "Expected all {TEST_MESSAGE_COUNT} rows to remain in MySQL, found {remaining}"
    );

    // Refusing at startup is what separates this from a connector that runs and
    // rejects every batch; both publish nothing. Which check refused is not visible
    // here - the SDK reduces open()'s error to a status code, so the runtime logs
    // only that init failed. The fixture's query therefore has to satisfy every
    // other startup check for this to mean anything, which is why it filters on
    // $offset and orders by it.
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime should be running")
        .collect_logs();
    let logs = format!("{stdout}{stderr}");
    assert!(
        logs.contains("Plugin initialization failed"),
        "Expected the connector to fail open() on a JSON tracking column"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn case_mismatched_tracking_column_publishes_every_row(
    harness: &TestHarness,
    fixture: MySqlSourceCaseMismatchedTrackingFixture,
) {
    // `tracking_column = "ID"` against a column the table declares `id`. MySQL
    // resolves identifiers case-insensitively, so the startup probe finds the column
    // and the query the connector builds selects it - the result set then names it
    // `id`, and this run reaches two of the sites that match it against the
    // configured name: the projection check, which rejects the batch, and the row
    // loop, where no row yields a cursor. Either one comparing exactly publishes
    // nothing, so rows arriving at all separates the fixed connector from both. The
    // result-set kind lookup is a third such site and is not covered here: it needs
    // an unresolved kind to matter, and this table is probed before the first poll.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    for i in 0..TEST_MESSAGE_COUNT {
        fixture.insert_row(&pool, &format!("row_{i}")).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "case_mismatched_consumer".try_into().unwrap();

    let mut names: BTreeSet<String> = BTreeSet::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                    && let Some(name) = record["data"]["name"].as_str()
                {
                    names.insert(name.to_string());
                }
            }
        }
        if names.len() >= TEST_MESSAGE_COUNT {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;

    // Counting distinct rows rather than messages: delivery is at-least-once, so a
    // replay is allowed.
    assert_eq!(
        names.len(),
        TEST_MESSAGE_COUNT,
        "Expected every row to be published under a case-mismatched tracking_column, got {names:?}"
    );
    assert_eq!(
        remaining, TEST_MESSAGE_COUNT as i64,
        "Expected all {TEST_MESSAGE_COUNT} rows to remain in MySQL, found {remaining}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mysql/source.toml")),
    seed = seeds::connector_stream
)]
async fn tinyint_tracking_value_under_a_custom_query_publishes_nothing(
    harness: &TestHarness,
    fixture: MySqlSourceTinyintTrackingFixture,
) {
    // The startup gate judges the column the table declares, so it never sees this
    // one: the query aliases `is_active` to a name the table does not have, and the
    // probe defers to the result set. sqlx names a width-1 tinyint `BOOLEAN` and
    // decodes it to true/false, which has no ordered scalar form, so the value is
    // only refused once the row is in hand. The batch must fail before anything is
    // published, since a cursor of "true" would advance nothing and re-read the same
    // rows forever.
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");

    for i in 0..TEST_MESSAGE_COUNT {
        fixture.insert_row(&pool, &format!("row_{i}")).await;
    }

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "tinyint_tracking_consumer".try_into().unwrap();

    let mut received = 0usize;
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            received += polled.messages.len();
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    let remaining = fixture.count_rows(&pool).await;
    pool.close().await;

    assert_eq!(
        received, 0,
        "Expected no messages from a tracking value with no ordered scalar form, got {received}"
    );
    assert_eq!(
        remaining, TEST_MESSAGE_COUNT as i64,
        "Expected all {TEST_MESSAGE_COUNT} rows to remain in MySQL, found {remaining}"
    );

    // A connector that failed to start publishes nothing either, and the startup gate
    // rejecting the aliased column would be the wrong reason for this to pass: only
    // the per-row check names the column and the row it stalled on.
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime should be running")
        .collect_logs();
    let logs = format!("{stdout}{stderr}");
    assert!(
        logs.contains("has no usable value for tracking_column 'flag'"),
        "Expected the row loop to refuse the BOOLEAN tracking value, not open() to fail"
    );
}
