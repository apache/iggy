/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Integration tests for the InfluxDB source connector.
//! File: core/integration/tests/connectors/influxdb/influxdb_source.rs

// ── testcontainers ────────────────────────────────────────────────────────────
// FIX: use testcontainers via testcontainers_modules re-export
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::core::IntoContainerPort; // FIX: trait for .tcp()
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── iggy imports ─────────────────────────────────────────────────────────────
// FIX: iggy::client, iggy::consumer, iggy::messages, iggy::utils don't exist
use iggy::prelude::IggyClient;
use iggy_binary_protocol::MessageClient; // FIX: provides poll_messages on IggyClient
// FIX: removed unused IggyMessage and Partitioning (source only polls, never sends)
use iggy_common::{Consumer, PollingStrategy};

// ── std / tokio / reqwest ─────────────────────────────────────────────────────
use reqwest::Client as HttpClient;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

// ── local helpers ─────────────────────────────────────────────────────────────
// FIX: `crate::connectors::common` does not exist — use module-local helpers
use crate::connectors::influxdb::test_utils::{
    ConnectorRuntimeHandle, build_iggy_client, cleanup_stream, create_stream_and_topic,
    start_connector_runtime, wait_for_messages,
};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const INFLUXDB_IMAGE: &str = "influxdb";
const INFLUXDB_VERSION: &str = "2.7-alpine";
const INFLUXDB_PORT: u16 = 8086;

const INFLUXDB_ORG: &str = "iggy-src-org";
const INFLUXDB_BUCKET: &str = "iggy-src-bucket";
const INFLUXDB_TOKEN: &str = "iggy-src-secret-token";
const INFLUXDB_USERNAME: &str = "iggy-admin";
const INFLUXDB_PASSWORD: &str = "iggy-password";

const STREAM_NAME: &str = "influxdb-source-test-stream";
const TOPIC_NAME: &str = "influxdb-source-test-topic";
const MEASUREMENT: &str = "sensor_readings";

// ─────────────────────────────────────────────────────────────────────────────
// Container helper
// FIX: return owned String, not &str (E0277 Sized fix)
// ─────────────────────────────────────────────────────────────────────────────

async fn start_influxdb() -> (ContainerAsync<GenericImage>, String) {
    // FIX: with_wait_for must be on GenericImage BEFORE with_exposed_port
    // FIX: explicit ContainerAsync<GenericImage> annotation resolves E0282 on get_host/get_host_port
    let container: ContainerAsync<GenericImage> =
        GenericImage::new(INFLUXDB_IMAGE, INFLUXDB_VERSION)
            .with_wait_for(WaitFor::message_on_stdout("Listening"))
            .with_exposed_port(INFLUXDB_PORT.tcp())
            .with_env_var("DOCKER_INFLUXDB_INIT_MODE", "setup")
            .with_env_var("DOCKER_INFLUXDB_INIT_USERNAME", INFLUXDB_USERNAME)
            .with_env_var("DOCKER_INFLUXDB_INIT_PASSWORD", INFLUXDB_PASSWORD)
            .with_env_var("DOCKER_INFLUXDB_INIT_ORG", INFLUXDB_ORG)
            .with_env_var("DOCKER_INFLUXDB_INIT_BUCKET", INFLUXDB_BUCKET)
            .with_env_var("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", INFLUXDB_TOKEN)
            .start()
            .await
            .expect("Failed to start InfluxDB container");

    let host = container.get_host().await.expect("get_host").to_string();
    let port: u16 = container
        .get_host_port_ipv4(INFLUXDB_PORT)
        .await
        .expect("get_port");

    (container, format!("http://{}:{}", host, port))
}

// ─────────────────────────────────────────────────────────────────────────────
// Write Line Protocol data into InfluxDB
// ─────────────────────────────────────────────────────────────────────────────

async fn write_influxdb_lines(base_url: &str, lines: &[&str]) {
    let http = HttpClient::new();
    let resp = http
        .post(format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ms",
            base_url, INFLUXDB_ORG, INFLUXDB_BUCKET
        ))
        .header("Authorization", format!("Token {}", INFLUXDB_TOKEN))
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(lines.join("\n"))
        .send()
        .await
        .expect("InfluxDB write failed");

    assert!(
        resp.status().is_success() || resp.status().as_u16() == 204,
        "InfluxDB write error: {}",
        resp.status()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Consume all messages from an Iggy topic
// FIX: MessageClient trait in scope; Consumer / PollingStrategy from iggy_common
// FIX: msg.payload is Vec<u8> directly on IggyMessage (not msg.message.payload)
// ─────────────────────────────────────────────────────────────────────────────

async fn consume_all_messages(iggy: &IggyClient, stream: &str, topic: &str) -> Vec<Value> {
    let mut all = Vec::new();
    let consumer = Consumer::default();

    loop {
        let polled = iggy
            .poll_messages(
                &stream.try_into().unwrap(),
                &topic.try_into().unwrap(),
                Some(1),
                &consumer,
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
            .expect("poll_messages failed");

        if polled.messages.is_empty() {
            break;
        }
        for m in polled.messages {
            // FIX: IggyMessage has .payload: Vec<u8> directly
            if let Ok(v) = serde_json::from_slice::<Value>(&m.payload) {
                all.push(v);
            }
        }
    }
    all
}

// ─────────────────────────────────────────────────────────────────────────────
// Connector TOML config builder
// ─────────────────────────────────────────────────────────────────────────────

fn source_config(
    influxdb_url: &str,
    stream: &str,
    topic: &str,
    flux_query: &str,
    initial_offset: &str,
) -> String {
    let escaped = flux_query.replace('\\', "\\\\").replace('"', "\\\"");
    format!(
        r#"
[sources.influxdb]
enabled       = true
name          = "InfluxDB source – integration test"
path          = "target/release/libiggy_connector_influxdb_source"
config_format = "toml"

[[sources.influxdb.streams]]
stream = "{stream}"
topic  = "{topic}"
schema = "json"

[sources.influxdb.plugin_config]
url              = "{url}"
org              = "{org}"
token            = "{token}"
query            = "{query}"
poll_interval    = "500ms"
batch_size       = 100
cursor_field     = "_time"
initial_offset   = "{offset}"
include_metadata = true
max_retries      = 3
retry_delay      = "50ms"
timeout          = "10s"
circuit_breaker_threshold = 5
circuit_breaker_cool_down = "5s"
"#,
        stream = stream,
        topic = topic,
        url = influxdb_url,
        org = INFLUXDB_ORG,
        token = INFLUXDB_TOKEN,
        query = escaped,
        offset = initial_offset,
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Happy path: InfluxDB rows → Iggy messages
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_source_polls_and_produces_messages() {
    let (_c, url) = start_influxdb().await;

    let base_ts: u64 = 1_700_000_000_000;
    let lines: Vec<String> = (0..5)
        .map(|i| {
            format!(
                "{m},loc=lab v={v} {ts}",
                m = MEASUREMENT,
                v = 20.0 + i as f64,
                ts = base_ts + i * 1000
            )
        })
        .collect();
    write_influxdb_lines(&url, &lines.iter().map(String::as_str).collect::<Vec<_>>()).await;

    let iggy: IggyClient = build_iggy_client().await;
    create_stream_and_topic(&iggy, STREAM_NAME, TOPIC_NAME, 1).await;

    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:$cursor) |> filter(fn:(r)=>r._measurement=="{m}") |> limit(n:$limit)"#,
        b = INFLUXDB_BUCKET,
        m = MEASUREMENT
    );
    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&source_config(
        &url,
        STREAM_NAME,
        TOPIC_NAME,
        &flux,
        "2023-01-01T00:00:00Z",
    ))
    .await;

    wait_for_messages(&iggy, STREAM_NAME, TOPIC_NAME, 5, Duration::from_secs(15)).await;
    let msgs = consume_all_messages(&iggy, STREAM_NAME, TOPIC_NAME).await;
    assert_eq!(msgs.len(), 5, "Expected 5, got {}", msgs.len());

    cleanup_stream(&iggy, STREAM_NAME).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Payload contains correct InfluxDB fields
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_source_message_payload_structure() {
    let (_c, url) = start_influxdb().await;

    let ts: u64 = 1_700_000_100_000;
    write_influxdb_lines(
        &url,
        &[&format!(
            "{m},loc=roof humidity=78.5 {ts}",
            m = MEASUREMENT,
            ts = ts
        )],
    )
    .await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-payload", STREAM_NAME);
    let topic = format!("{}-payload", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:$cursor) |> filter(fn:(r)=>r._measurement=="{m}") |> limit(n:$limit)"#,
        b = INFLUXDB_BUCKET,
        m = MEASUREMENT
    );
    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&source_config(
        &url,
        &stream,
        &topic,
        &flux,
        "2023-01-01T00:00:00Z",
    ))
    .await;

    wait_for_messages(&iggy, &stream, &topic, 1, Duration::from_secs(15)).await;
    let msgs = consume_all_messages(&iggy, &stream, &topic).await;
    assert_eq!(msgs.len(), 1);

    let m = &msgs[0];
    assert!(m.get("measurement").is_some(), "missing 'measurement': {m}");
    assert!(m.get("timestamp").is_some(), "missing 'timestamp': {m}");
    assert!(m.get("value").is_some(), "missing 'value': {m}");
    assert_eq!(m["measurement"].as_str().unwrap_or(""), MEASUREMENT);

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Cursor advances: only new records on each poll
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_source_cursor_advances_incrementally() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-cursor", STREAM_NAME);
    let topic = format!("{}-cursor", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:$cursor) |> filter(fn:(r)=>r._measurement=="{m}") |> limit(n:$limit)"#,
        b = INFLUXDB_BUCKET,
        m = MEASUREMENT
    );

    // Batch 1: 3 old records
    let old_ts: u64 = 1_690_000_000_000;
    let b1: Vec<String> = (0..3)
        .map(|i| {
            format!(
                "{m},loc=a v={i} {ts}",
                m = MEASUREMENT,
                i = i,
                ts = old_ts + i * 1000
            )
        })
        .collect();
    write_influxdb_lines(&url, &b1.iter().map(String::as_str).collect::<Vec<_>>()).await;

    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&source_config(
        &url,
        &stream,
        &topic,
        &flux,
        "2023-01-01T00:00:00Z",
    ))
    .await;
    wait_for_messages(&iggy, &stream, &topic, 3, Duration::from_secs(15)).await;

    // Batch 2: 2 newer records — cursor should advance and pick only these up
    let new_ts: u64 = 1_700_100_000_000;
    let b2: Vec<String> = (0..2)
        .map(|i| {
            format!(
                "{m},loc=b v={v} {ts}",
                m = MEASUREMENT,
                v = i + 10,
                ts = new_ts + i * 1000
            )
        })
        .collect();
    write_influxdb_lines(&url, &b2.iter().map(String::as_str).collect::<Vec<_>>()).await;

    wait_for_messages(&iggy, &stream, &topic, 5, Duration::from_secs(15)).await;
    let msgs = consume_all_messages(&iggy, &stream, &topic).await;
    assert_eq!(
        msgs.len(),
        5,
        "Cursor did not advance correctly: expected 5, got {}",
        msgs.len()
    );

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Empty bucket → zero messages
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_source_empty_bucket_produces_no_messages() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-empty", STREAM_NAME);
    let topic = format!("{}-empty", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:$cursor) |> filter(fn:(r)=>r._measurement=="nonexistent") |> limit(n:$limit)"#,
        b = INFLUXDB_BUCKET
    );
    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&source_config(
        &url,
        &stream,
        &topic,
        &flux,
        "2023-01-01T00:00:00Z",
    ))
    .await;

    sleep(Duration::from_secs(4)).await;
    let msgs = consume_all_messages(&iggy, &stream, &topic).await;
    assert_eq!(msgs.len(), 0, "Expected 0 messages, got {}", msgs.len());

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5 — Multiple measurements polled together
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_source_multiple_measurements() {
    let (_c, url) = start_influxdb().await;

    let ts: u64 = 1_700_200_000_000;
    write_influxdb_lines(
        &url,
        &[
            &format!("temperature,room=living v=21.5 {ts}"),
            &format!("humidity,room=living v=55.0 {}", ts + 1000),
            &format!("pressure,room=living v=1013.25 {}", ts + 2000),
        ],
    )
    .await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-multi", STREAM_NAME);
    let topic = format!("{}-multi", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:$cursor) |> filter(fn:(r)=>r._measurement=="temperature" or r._measurement=="humidity" or r._measurement=="pressure") |> limit(n:$limit)"#,
        b = INFLUXDB_BUCKET
    );
    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&source_config(
        &url,
        &stream,
        &topic,
        &flux,
        "2023-01-01T00:00:00Z",
    ))
    .await;

    wait_for_messages(&iggy, &stream, &topic, 3, Duration::from_secs(15)).await;
    let msgs = consume_all_messages(&iggy, &stream, &topic).await;
    assert_eq!(msgs.len(), 3, "Expected 3, got {}", msgs.len());

    let measurements: Vec<&str> = msgs
        .iter()
        .filter_map(|m| m["measurement"].as_str())
        .collect();
    assert!(measurements.contains(&"temperature"), "missing temperature");
    assert!(measurements.contains(&"humidity"), "missing humidity");
    assert!(measurements.contains(&"pressure"), "missing pressure");

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6 — Circuit breaker: unreachable InfluxDB → no messages produced
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_source_circuit_breaker_limits_retries() {
    let bad_url = "http://127.0.0.1:19999";

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-cb", STREAM_NAME);
    let topic = format!("{}-cb", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:$cursor) |> limit(n:$limit)"#,
        b = INFLUXDB_BUCKET
    );
    let config = format!(
        r#"
[sources.influxdb_cb]
enabled       = true
name          = "InfluxDB source CB test"
path          = "target/release/libiggy_connector_influxdb_source"
config_format = "toml"

[[sources.influxdb_cb.streams]]
stream = "{stream}"
topic  = "{topic}"
schema = "json"

[sources.influxdb_cb.plugin_config]
url              = "{url}"
org              = "{org}"
token            = "{token}"
query            = "{query}"
poll_interval    = "200ms"
max_retries      = 1
retry_delay      = "10ms"
timeout          = "500ms"
max_open_retries = 1
circuit_breaker_threshold = 2
circuit_breaker_cool_down = "60s"
"#,
        stream = stream,
        topic = topic,
        url = bad_url,
        org = INFLUXDB_ORG,
        token = INFLUXDB_TOKEN,
        query = flux.replace('"', "\\\""),
    );

    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&config).await;
    sleep(Duration::from_secs(3)).await;

    let msgs = consume_all_messages(&iggy, &stream, &topic).await;
    assert_eq!(
        msgs.len(),
        0,
        "Expected 0 messages when InfluxDB unreachable, got {}",
        msgs.len()
    );

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7 — Cursor survives connector restart
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_source_cursor_survives_restart() {
    let (_c, url) = start_influxdb().await;

    let ts: u64 = 1_700_300_000_000;
    let lines: Vec<String> = (0..3)
        .map(|i| {
            format!(
                "{m},host=h{i} v={i} {ts}",
                m = MEASUREMENT,
                i = i,
                ts = ts + i * 1000
            )
        })
        .collect();
    write_influxdb_lines(&url, &lines.iter().map(String::as_str).collect::<Vec<_>>()).await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-restart", STREAM_NAME);
    let topic = format!("{}-restart", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:$cursor) |> filter(fn:(r)=>r._measurement=="{m}") |> limit(n:$limit)"#,
        b = INFLUXDB_BUCKET,
        m = MEASUREMENT
    );
    let config = source_config(&url, &stream, &topic, &flux, "2023-01-01T00:00:00Z");

    // First run — pick up initial 3
    {
        let _rt: ConnectorRuntimeHandle = start_connector_runtime(&config).await;
        wait_for_messages(&iggy, &stream, &topic, 3, Duration::from_secs(15)).await;
    } // connector stops, state persisted

    // Write 2 more records
    let new_ts: u64 = 1_700_400_000_000;
    let new_lines: Vec<String> = (0..2)
        .map(|i| {
            format!(
                "{m},host=new{i} v={v} {ts}",
                m = MEASUREMENT,
                i = i,
                v = 100 + i,
                ts = new_ts + i * 1000
            )
        })
        .collect();
    write_influxdb_lines(
        &url,
        &new_lines.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;

    // Second run — should resume from cursor, pick up only the 2 new records
    {
        let _rt: ConnectorRuntimeHandle = start_connector_runtime(&config).await;
        wait_for_messages(&iggy, &stream, &topic, 5, Duration::from_secs(15)).await;
    }

    let msgs = consume_all_messages(&iggy, &stream, &topic).await;
    assert_eq!(
        msgs.len(),
        5,
        "Expected 5 after restart (3+2), got {}",
        msgs.len()
    );

    cleanup_stream(&iggy, &stream).await;
}
