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

use crate::connectors::{ConnectorsRuntime, IggySetup, setup_runtime};
use bytes::Bytes;
use iggy::prelude::IggyMessage;
use serial_test::serial;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::time::sleep;
use tracing::info;

const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "postgres";
const POSTGRES_DB: &str = "postgres";
const SINK_TABLE: &str = "iggy_sink_events";

/// Maximum number of attempts before giving up waiting for sink rows.
const POLL_ATTEMPTS: usize = 30;
/// Delay between attempts.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Start a Postgres container and return the container, the JDBC URL (for the
/// connector), the sqlx URL (for verification) and the driver JAR path.
async fn setup_postgres_container() -> Result<
    (ContainerAsync<Postgres>, String, String, String),
    Box<dyn std::error::Error + Send + Sync>,
> {
    info!("Starting Postgres container for JDBC sink testing...");

    let postgres = Postgres::default().start().await?;
    let host = postgres.get_host().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;

    let jdbc_url = format!("jdbc:postgresql://{host}:{port}/{POSTGRES_DB}");
    let sqlx_url =
        format!("postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{POSTGRES_DB}");
    let driver_jar = get_postgres_driver_jar().await?;

    info!("Postgres container started at {host}:{port}");
    Ok((postgres, jdbc_url, sqlx_url, driver_jar))
}

/// Get the PostgreSQL JDBC driver, downloading (and caching) it if necessary.
async fn get_postgres_driver_jar() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let jdbc_test_dir = format!("{target_dir}/test-jdbc-drivers");
    let jar_path = format!("{jdbc_test_dir}/postgresql-42.7.1.jar");

    std::fs::create_dir_all(&jdbc_test_dir)?;

    if std::path::Path::new(&jar_path).exists() {
        info!("PostgreSQL JDBC driver found at {jar_path}");
        return Ok(std::fs::canonicalize(&jar_path)?
            .to_string_lossy()
            .to_string());
    }

    info!("Downloading PostgreSQL JDBC driver...");
    let download_url = "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar";
    let response = reqwest::get(download_url).await?;
    if !response.status().is_success() {
        return Err(format!("Failed to download driver: HTTP {}", response.status()).into());
    }
    let bytes = response.bytes().await?;
    std::fs::write(&jar_path, bytes)?;

    info!("PostgreSQL JDBC driver downloaded to {jar_path}");
    Ok(std::fs::canonicalize(&jar_path)?
        .to_string_lossy()
        .to_string())
}

/// Build the environment variable overrides for the JDBC Postgres sink connector.
/// The stream/topic the sink consumes from is fixed in the connector TOML; only
/// the dynamic connection settings need to be injected here.
fn build_jdbc_sink_env(jdbc_url: &str, driver_jar: &str) -> HashMap<String, String> {
    let prefix = "IGGY_CONNECTORS_SINK_JDBC_SINK_PG";
    let mut envs = HashMap::new();

    let mut set = |suffix: &str, value: &str| {
        envs.insert(format!("{prefix}_{suffix}"), value.to_owned());
    };

    set("PLUGIN_CONFIG_JDBC_URL", jdbc_url);
    set("PLUGIN_CONFIG_DRIVER_CLASS", "org.postgresql.Driver");
    set("PLUGIN_CONFIG_DRIVER_JAR_PATH", driver_jar);
    set("PLUGIN_CONFIG_USERNAME", POSTGRES_USER);
    set("PLUGIN_CONFIG_PASSWORD", POSTGRES_PASSWORD);
    set("PLUGIN_CONFIG_TARGET_TABLE", SINK_TABLE);
    set("PLUGIN_CONFIG_AUTO_CREATE_TABLE", "true");
    set("PLUGIN_CONFIG_PAYLOAD_FORMAT", "json");
    set("PLUGIN_CONFIG_BATCH_SIZE", "100");
    set("PLUGIN_CONFIG_INCLUDE_METADATA", "true");

    envs
}

/// Start the connectors runtime with the JDBC Postgres sink configured.
async fn setup_jdbc_postgres_sink(
    jdbc_url: &str,
    driver_jar: &str,
) -> Result<
    (ConnectorsRuntime, crate::connectors::ConnectorsIggyClient),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let iggy_setup = IggySetup::default();
    let envs = build_jdbc_sink_env(jdbc_url, driver_jar);

    let mut runtime = setup_runtime();
    runtime
        .init("jdbc/config_sink_postgres.toml", Some(envs), iggy_setup)
        .await;

    let client = runtime.create_client().await;
    Ok((runtime, client))
}

/// Build JSON `IggyMessage`s with sequential ids from the given payloads.
fn build_json_messages(payloads: &[serde_json::Value]) -> Vec<IggyMessage> {
    payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            let bytes = serde_json::to_vec(payload).expect("Failed to serialize payload");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(bytes))
                .build()
                .expect("Failed to build message")
        })
        .collect()
}

/// Poll the sink table until `expected` rows appear (or attempts are exhausted),
/// returning (iggy_offset, iggy_stream, iggy_topic, payload-as-json) per row.
async fn wait_for_rows(
    sqlx_url: &str,
    expected: usize,
) -> Vec<(i64, String, String, serde_json::Value)> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(sqlx_url)
        .await
        .expect("Failed to connect to Postgres for verification");

    let query = format!(
        "SELECT iggy_offset, iggy_stream, iggy_topic, payload FROM {SINK_TABLE} ORDER BY iggy_offset"
    );

    for _ in 0..POLL_ATTEMPTS {
        // The table may not exist yet until the sink's open() runs.
        // Query string is built only from a compile-time constant table name.
        if let Ok(rows) = sqlx::query(sqlx::AssertSqlSafe(query.clone()))
            .fetch_all(&pool)
            .await
            && rows.len() >= expected
        {
            return rows
                .iter()
                .map(|row| {
                    let offset: i64 = row.get("iggy_offset");
                    let stream: String = row.get("iggy_stream");
                    let topic: String = row.get("iggy_topic");
                    let payload: String = row.get("payload");
                    let value: serde_json::Value =
                        serde_json::from_str(&payload).expect("Stored payload is not valid JSON");
                    (offset, stream, topic, value)
                })
                .collect();
        }
        sleep(POLL_INTERVAL).await;
    }

    Vec::new()
}

/// Test: JSON messages produced to Iggy are written as rows in Postgres by the
/// JDBC sink, with payload and metadata columns populated.
#[tokio::test]
#[serial]
async fn test_jdbc_postgres_sink_writes_rows() {
    let (_container, jdbc_url, sqlx_url, driver_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Skipping test: Failed to setup Postgres: {e}");
            return;
        }
    };

    let (_runtime, client) = setup_jdbc_postgres_sink(&jdbc_url, &driver_jar)
        .await
        .expect("Failed to setup sink runtime");

    let payloads = vec![
        serde_json::json!({"id": 1, "name": "alice", "active": true}),
        serde_json::json!({"id": 2, "name": "bob", "active": false}),
        serde_json::json!({"id": 3, "name": "carol", "active": true}),
    ];
    let mut messages = build_json_messages(&payloads);

    client
        .send_messages(&mut messages)
        .await
        .expect("Failed to send messages to Iggy");

    info!("Waiting for the JDBC sink to write rows to Postgres...");
    let rows = wait_for_rows(&sqlx_url, payloads.len()).await;

    assert_eq!(
        rows.len(),
        payloads.len(),
        "Expected {} rows written to Postgres by the JDBC sink, got {}",
        payloads.len(),
        rows.len()
    );

    for (i, (offset, stream, topic, payload)) in rows.iter().enumerate() {
        assert_eq!(*offset, i as i64, "Offset mismatch at row {i}");
        assert_eq!(stream, DEFAULT_STREAM, "Stream mismatch at row {i}");
        assert_eq!(topic, DEFAULT_TOPIC, "Topic mismatch at row {i}");
        assert_eq!(payload, &payloads[i], "Payload mismatch at row {i}");
    }
}

/// Test: a single message round-trips through the sink with the correct value.
#[tokio::test]
#[serial]
async fn test_jdbc_postgres_sink_single_message() {
    let (_container, jdbc_url, sqlx_url, driver_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Skipping test: Failed to setup Postgres: {e}");
            return;
        }
    };

    let (_runtime, client) = setup_jdbc_postgres_sink(&jdbc_url, &driver_jar)
        .await
        .expect("Failed to setup sink runtime");

    let payloads = vec![serde_json::json!({"answer": 42})];
    let mut messages = build_json_messages(&payloads);
    client
        .send_messages(&mut messages)
        .await
        .expect("Failed to send message to Iggy");

    let rows = wait_for_rows(&sqlx_url, 1).await;
    assert_eq!(
        rows.len(),
        1,
        "Expected exactly 1 row written by the JDBC sink"
    );
    assert_eq!(
        rows[0].3.get("answer").and_then(|v| v.as_i64()),
        Some(42),
        "Expected answer=42 in stored payload"
    );
}

// Matches IggySetup::default() (DEFAULT_TEST_STREAM / DEFAULT_TEST_TOPIC).
const DEFAULT_STREAM: &str = "test_stream";
const DEFAULT_TOPIC: &str = "test_topic";
