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

//! Integration tests for the InfluxDB sink connector.
//! File: core/integration/tests/connectors/influxdb/influxdb_sink.rs

// ── testcontainers ────────────────────────────────────────────────────────────
// FIX: the workspace re-exports testcontainers under testcontainers_modules;
//      there is no top-level `testcontainers` crate dependency, only the
//      re-export path shown in the compiler hint.
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::GenericImage;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::core::IntoContainerPort; // FIX: trait for .tcp()
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── iggy imports ─────────────────────────────────────────────────────────────
// FIX: iggy::client, iggy::messages, iggy::utils, iggy::models do NOT exist.
//      Correct paths confirmed from compiler output and SDK source:
//        - IggyClient     → iggy::prelude
//        - send_messages  → iggy_binary_protocol::MessageClient (trait)
//        - IggyMessage    → iggy_common
//        - Partitioning   → iggy_common
use iggy::prelude::IggyClient;
use iggy_binary_protocol::MessageClient; // FIX: provides send_messages on IggyClient
use iggy_common::{IggyMessage, Partitioning};
use std::str::FromStr; // FIX: required for IggyMessage::from_str()

// ── std / tokio / reqwest ─────────────────────────────────────────────────────
use reqwest::Client as HttpClient;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::sleep;

// ── test harness helpers (defined in this file — no `connectors::common`) ────
// FIX: `crate::connectors::common` does not exist.  All helpers are local.
use crate::connectors::influxdb::test_utils::{
    ConnectorRuntimeHandle, build_iggy_client, cleanup_stream, create_stream_and_topic,
    start_connector_runtime, wait_for_connector,
};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const INFLUXDB_IMAGE: &str = "influxdb";
const INFLUXDB_VERSION: &str = "2.7-alpine";
const INFLUXDB_PORT: u16 = 8086;

const INFLUXDB_ORG: &str = "iggy-test-org";
const INFLUXDB_BUCKET: &str = "iggy-test-bucket";
const INFLUXDB_TOKEN: &str = "iggy-super-secret-test-token";
const INFLUXDB_USERNAME: &str = "iggy-admin";
const INFLUXDB_PASSWORD: &str = "iggy-password";

const STREAM_NAME: &str = "influxdb-sink-test-stream";
const TOPIC_NAME: &str = "influxdb-sink-test-topic";
const CONSUMER_GROUP: &str = "influxdb-sink-cg";
const MEASUREMENT: &str = "iggy_messages";

// ─────────────────────────────────────────────────────────────────────────────
// Container helper
// FIX: return (ContainerAsync<GenericImage>, String) — owned String, not &str,
//      so the binding in each test is Sized.
// ─────────────────────────────────────────────────────────────────────────────

async fn start_influxdb() -> (ContainerAsync<GenericImage>, String) {
    // FIX: with_wait_for must be called on GenericImage BEFORE with_exposed_port;
    //      after with_exposed_port the type becomes ContainerRequest which lacks it.
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
// InfluxDB Flux query helper
// ─────────────────────────────────────────────────────────────────────────────

async fn query_influxdb(base_url: &str, flux_query: &str) -> Vec<Value> {
    let http = HttpClient::new();
    let resp = http
        .post(format!("{}/api/v2/query?org={}", base_url, INFLUXDB_ORG))
        .header("Authorization", format!("Token {}", INFLUXDB_TOKEN))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&json!({
            "query": flux_query,
            "dialect": { "annotations": [], "delimiter": ",", "header": true, "commentPrefix": "#" }
        }))
        .send()
        .await
        .expect("InfluxDB query failed");

    assert!(
        resp.status().is_success(),
        "InfluxDB query returned {}",
        resp.status()
    );
    resp.json::<Vec<Value>>().await.unwrap_or_default()
}

async fn count_influxdb_records(base_url: &str) -> usize {
    let flux = format!(
        r#"from(bucket:"{b}") |> range(start:-1h) |> filter(fn:(r)=>r._measurement=="{m}")"#,
        b = INFLUXDB_BUCKET,
        m = MEASUREMENT
    );
    query_influxdb(base_url, &flux).await.len()
}

// ─────────────────────────────────────────────────────────────────────────────
// Connector TOML config builder
// ─────────────────────────────────────────────────────────────────────────────

fn sink_config(influxdb_url: &str, stream: &str, topic: &str) -> String {
    format!(
        r#"
[sinks.influxdb]
enabled       = true
name          = "InfluxDB sink – integration test"
path          = "target/release/libiggy_connector_influxdb_sink"
config_format = "toml"

[[sinks.influxdb.streams]]
stream         = "{stream}"
topics         = ["{topic}"]
schema         = "json"
batch_length   = 10
poll_interval  = "100ms"
consumer_group = "{cg}"

[sinks.influxdb.plugin_config]
url                      = "{url}"
org                      = "{org}"
bucket                   = "{bucket}"
token                    = "{token}"
measurement              = "{measurement}"
precision                = "ms"
batch_size               = 10
max_retries              = 3
retry_delay              = "50ms"
timeout                  = "10s"
include_metadata         = true
include_stream_tag       = true
include_topic_tag        = true
include_partition_tag    = true
include_checksum         = true
include_origin_timestamp = true
payload_format           = "json"
circuit_breaker_threshold = 5
circuit_breaker_cool_down = "5s"
"#,
        stream = stream,
        topic = topic,
        cg = CONSUMER_GROUP,
        url = influxdb_url,
        org = INFLUXDB_ORG,
        bucket = INFLUXDB_BUCKET,
        token = INFLUXDB_TOKEN,
        measurement = MEASUREMENT,
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Message factory
// FIX: IggyMessage::from_str — matches actual SDK type
// ─────────────────────────────────────────────────────────────────────────────

fn msg(payload: &str) -> IggyMessage {
    IggyMessage::from_str(payload).expect("build IggyMessage")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Happy path: 5 messages → InfluxDB
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_sink_writes_messages_to_bucket() {
    let (_c, url) = start_influxdb().await;

    // FIX: explicit type annotation resolves E0282 on build_iggy_client()
    let iggy: IggyClient = build_iggy_client().await;
    create_stream_and_topic(&iggy, STREAM_NAME, TOPIC_NAME, 1).await;

    let config = sink_config(&url, STREAM_NAME, TOPIC_NAME);
    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&config).await;
    wait_for_connector(&iggy, STREAM_NAME, TOPIC_NAME, CONSUMER_GROUP).await;

    let mut messages: Vec<IggyMessage> = (1u32..=5)
        .map(|i| {
            msg(&format!(
                r#"{{"sensor_id":{i},"temp":{}}}"#,
                20.0 + i as f64
            ))
        })
        .collect();

    // FIX: MessageClient trait in scope → .send_messages available on IggyClient
    iggy.send_messages(
        &STREAM_NAME.try_into().unwrap(),
        &TOPIC_NAME.try_into().unwrap(),
        &Partitioning::partition_id(1),
        &mut messages,
    )
    .await
    .expect("send_messages failed");

    sleep(Duration::from_secs(3)).await;

    let count = count_influxdb_records(&url).await;
    assert_eq!(count, 5, "Expected 5 records in InfluxDB, found {count}");

    cleanup_stream(&iggy, STREAM_NAME).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Payload fields stored correctly
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_sink_payload_fields_stored_correctly() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-fields", STREAM_NAME);
    let topic = format!("{}-fields", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let _rt: ConnectorRuntimeHandle =
        start_connector_runtime(&sink_config(&url, &stream, &topic)).await;
    wait_for_connector(&iggy, &stream, &topic, CONSUMER_GROUP).await;

    iggy.send_messages(
        &stream.as_str().try_into().unwrap(),
        &topic.as_str().try_into().unwrap(),
        &Partitioning::partition_id(1),
        &mut vec![msg(r#"{"device":"sensor-42","reading":99.5}"#)],
    )
    .await
    .unwrap();

    sleep(Duration::from_secs(3)).await;

    let rows = query_influxdb(
        &url,
        &format!(
            r#"from(bucket:"{b}") |> range(start:-1h) |> filter(fn:(r)=>r._measurement=="{m}") |> filter(fn:(r)=>r._field=="payload_json")"#,
            b = INFLUXDB_BUCKET,
            m = MEASUREMENT
        ),
    )
    .await;

    assert!(!rows.is_empty(), "No payload_json rows found");
    let v = rows[0]["_value"].as_str().unwrap_or("");
    assert!(v.contains("sensor-42"), "payload_json missing data: {v}");

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Stream/topic tags written as InfluxDB tags
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_sink_stream_topic_tags_present() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-tags", STREAM_NAME);
    let topic = format!("{}-tags", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let _rt: ConnectorRuntimeHandle =
        start_connector_runtime(&sink_config(&url, &stream, &topic)).await;
    wait_for_connector(&iggy, &stream, &topic, CONSUMER_GROUP).await;

    iggy.send_messages(
        &stream.as_str().try_into().unwrap(),
        &topic.as_str().try_into().unwrap(),
        &Partitioning::partition_id(1),
        &mut vec![msg(r#"{"v":1}"#)],
    )
    .await
    .unwrap();

    sleep(Duration::from_secs(3)).await;

    let rows = query_influxdb(
        &url,
        &format!(
            r#"from(bucket:"{b}") |> range(start:-1h) |> filter(fn:(r)=>r._measurement=="{m}") |> filter(fn:(r)=>r.stream=="{s}")"#,
            b = INFLUXDB_BUCKET,
            m = MEASUREMENT,
            s = stream,
        ),
    )
    .await;

    assert!(!rows.is_empty(), "No rows with stream tag '{stream}'");
    assert_eq!(rows[0]["topic"].as_str().unwrap_or(""), topic);

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Large batch (500 messages)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_sink_large_batch() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-batch", STREAM_NAME);
    let topic = format!("{}-batch", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let _rt: ConnectorRuntimeHandle =
        start_connector_runtime(&sink_config(&url, &stream, &topic)).await;
    wait_for_connector(&iggy, &stream, &topic, CONSUMER_GROUP).await;

    for chunk_start in (0..500usize).step_by(100) {
        let mut msgs: Vec<IggyMessage> = (chunk_start..chunk_start + 100)
            .map(|i| msg(&format!(r#"{{"seq":{i}}}"#)))
            .collect();
        iggy.send_messages(
            &stream.as_str().try_into().unwrap(),
            &topic.as_str().try_into().unwrap(),
            &Partitioning::partition_id(1),
            &mut msgs,
        )
        .await
        .unwrap();
    }

    sleep(Duration::from_secs(8)).await;

    assert_eq!(
        count_influxdb_records(&url).await,
        500,
        "Expected 500 records"
    );

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5 — Connector picks up backlogged messages after late start
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_sink_recovers_after_late_start() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-recovery", STREAM_NAME);
    let topic = format!("{}-recovery", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    // Produce before connector starts
    let mut msgs: Vec<IggyMessage> = (0..10).map(|i| msg(&format!(r#"{{"i":{i}}}"#))).collect();
    iggy.send_messages(
        &stream.as_str().try_into().unwrap(),
        &topic.as_str().try_into().unwrap(),
        &Partitioning::partition_id(1),
        &mut msgs,
    )
    .await
    .unwrap();

    let _rt: ConnectorRuntimeHandle =
        start_connector_runtime(&sink_config(&url, &stream, &topic)).await;
    wait_for_connector(&iggy, &stream, &topic, CONSUMER_GROUP).await;

    sleep(Duration::from_secs(5)).await;

    assert_eq!(
        count_influxdb_records(&url).await,
        10,
        "Expected 10 backlogged records"
    );

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6 — Multiple partitions
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_sink_multiple_partitions() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-multi", STREAM_NAME);
    let topic = format!("{}-multi", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 3).await;

    let _rt: ConnectorRuntimeHandle =
        start_connector_runtime(&sink_config(&url, &stream, &topic)).await;
    wait_for_connector(&iggy, &stream, &topic, CONSUMER_GROUP).await;

    for partition_id in 1u32..=3 {
        iggy.send_messages(
            &stream.as_str().try_into().unwrap(),
            &topic.as_str().try_into().unwrap(),
            &Partitioning::partition_id(partition_id),
            &mut vec![msg(&format!(r#"{{"partition":{partition_id}}}"#))],
        )
        .await
        .unwrap();
    }

    sleep(Duration::from_secs(4)).await;
    assert_eq!(count_influxdb_records(&url).await, 3, "Expected 3 records");

    cleanup_stream(&iggy, &stream).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7 — Text payload format
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_influxdb_sink_text_payload_format() {
    let (_c, url) = start_influxdb().await;

    let iggy: IggyClient = build_iggy_client().await;
    let stream = format!("{}-text", STREAM_NAME);
    let topic = format!("{}-text", TOPIC_NAME);
    create_stream_and_topic(&iggy, &stream, &topic, 1).await;

    let config = format!(
        r#"
[sinks.influxdb_text]
enabled       = true
name          = "InfluxDB sink text – integration test"
path          = "target/release/libiggy_connector_influxdb_sink"
config_format = "toml"

[[sinks.influxdb_text.streams]]
stream         = "{stream}"
topics         = ["{topic}"]
schema         = "text"
batch_length   = 5
poll_interval  = "100ms"
consumer_group = "{cg}"

[sinks.influxdb_text.plugin_config]
url            = "{url}"
org            = "{org}"
bucket         = "{bucket}"
token          = "{token}"
measurement    = "iggy_text_test"
precision      = "ms"
payload_format = "text"
max_retries    = 3
retry_delay    = "50ms"
"#,
        stream = stream,
        topic = topic,
        cg = CONSUMER_GROUP,
        url = url,
        org = INFLUXDB_ORG,
        bucket = INFLUXDB_BUCKET,
        token = INFLUXDB_TOKEN,
    );

    let _rt: ConnectorRuntimeHandle = start_connector_runtime(&config).await;
    wait_for_connector(&iggy, &stream, &topic, CONSUMER_GROUP).await;

    iggy.send_messages(
        &stream.as_str().try_into().unwrap(),
        &topic.as_str().try_into().unwrap(),
        &Partitioning::partition_id(1),
        &mut vec![msg("hello influxdb text payload")],
    )
    .await
    .unwrap();

    sleep(Duration::from_secs(3)).await;

    let rows = query_influxdb(
        &url,
        &format!(
            r#"from(bucket:"{b}") |> range(start:-1h) |> filter(fn:(r)=>r._measurement=="iggy_text_test") |> filter(fn:(r)=>r._field=="payload_text")"#,
            b = INFLUXDB_BUCKET,
        ),
    )
    .await;

    assert!(!rows.is_empty(), "No text payload rows found");
    let v = rows[0]["_value"].as_str().unwrap_or("");
    assert!(
        v.contains("hello influxdb text payload"),
        "payload_text mismatch: {v}"
    );

    cleanup_stream(&iggy, &stream).await;
}
