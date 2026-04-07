/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses you to
 * you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

use super::TEST_MESSAGE_COUNT;
use crate::connectors::fixtures::InfluxDbSourceFixture;
use iggy_common::MessageClient;
use iggy_common::Utc;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use integration::harness::seeds;
use integration::iggy_harness;
use serde_json::Value;
use tracing::info;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_polls_and_produces_messages(
    harness: &TestHarness,
    fixture: InfluxDbSourceFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    let lines: Vec<String> = (0..TEST_MESSAGE_COUNT)
        .map(|i| {
            format!(
                "sensor_readings,loc=lab v={v} {base_ts}",
                v = 20.0 + i as f64,
                base_ts = base_ts + i as u64 * 1000,
            )
        })
        .collect();
    let line_refs: Vec<&str> = lines.iter().map(String::as_str).collect();

    fixture
        .write_lines(&line_refs)
        .await
        .expect("Failed to write lines to InfluxDB");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut total = 0usize;
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
            .expect("poll_messages failed");

        total += polled.messages.len();
        if total >= TEST_MESSAGE_COUNT {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        total >= TEST_MESSAGE_COUNT,
        "Expected {TEST_MESSAGE_COUNT} messages, got {total}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_message_payload_structure(
    harness: &TestHarness,
    fixture: InfluxDbSourceFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    fixture
        .write_lines(&[&format!("sensor_readings,loc=roof humidity=78.5 {base_ts}")])
        .await
        .expect("Failed to write line");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Value> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
            .expect("poll_messages failed");

        for m in polled.messages {
            if let Ok(v) = serde_json::from_slice::<Value>(&m.payload) {
                msgs.push(v);
            }
        }
        if !msgs.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    info!("Received messages {:?}", msgs);
    assert_eq!(msgs.len(), 1, "Expected 1 message, got {}", msgs.len());
    let m = &msgs[0];
    assert!(
        m.get("measurement").is_some(),
        "missing 'measurement': {{m}}"
    );
    assert!(m.get("timestamp").is_some(), "missing 'timestamp': {{m}}");
    assert!(m.get("value").is_some(), "missing 'value': {{m}}");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_empty_bucket_produces_no_messages(
    harness: &TestHarness,
    fixture: InfluxDbSourceFixture,
) {
    // Write nothing — bucket intentionally empty for this measurement.
    let _ = &fixture;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let polled = client
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &consumer,
            &PollingStrategy::next(),
            100,
            false,
        )
        .await
        .expect("poll_messages failed");

    assert_eq!(
        polled.messages.len(),
        0,
        "Expected 0 messages for empty bucket, got {}",
        polled.messages.len()
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/influxdb/source.toml")),
    seed = seeds::connector_stream
)]
async fn influxdb_source_multiple_measurements(
    harness: &TestHarness,
    fixture: InfluxDbSourceFixture,
) {
    let base_ts: u64 = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    fixture
        .write_lines(&[
            &format!("temperature,room=living v=21.5 {base_ts}"),
            &format!("humidity,room=living v=55.0 {}", base_ts + 1000),
            &format!("pressure,room=living v=1013.25 {}", base_ts + 2000),
        ])
        .await
        .expect("Failed to write lines");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer = Consumer::default();

    let mut msgs: Vec<Value> = Vec::new();
    for _ in 0..100 {
        let polled = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &consumer,
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
            .expect("poll_messages failed");

        for m in polled.messages {
            if let Ok(v) = serde_json::from_slice::<Value>(&m.payload) {
                msgs.push(v);
            }
        }
        if msgs.len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    info!("influxdb_source_multiple_measurements Received {:#?}", msgs);
    assert_eq!(msgs.len(), 3, "Expected 3 messages, got {}", msgs.len());

    let measurements: Vec<&str> = msgs
        .iter()
        .filter_map(|m| m["measurement"].as_str())
        .collect();
    assert!(measurements.contains(&"temperature"), "missing temperature");
    assert!(measurements.contains(&"humidity"), "missing humidity");
    assert!(measurements.contains(&"pressure"), "missing pressure");
}
