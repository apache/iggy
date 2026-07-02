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
use serial_test::serial;
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

/// Maximum number of poll attempts before giving up
const POLL_ATTEMPTS: usize = 30;
/// Delay between poll attempts
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Setup Postgres container with test data
async fn setup_postgres_container()
-> Result<(ContainerAsync<Postgres>, String, String), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting Postgres container for JDBC testing...");

    let postgres = Postgres::default().start().await?;

    let host = postgres.get_host().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let jdbc_url: String = format!("jdbc:postgresql://{}:{}/{}", host, port, POSTGRES_DB);

    let postgres_jar: String = get_postgres_driver_jar().await?;

    info!("Postgres container started at {}:{}", host, port);
    Ok((postgres, jdbc_url, postgres_jar))
}

/// Get PostgreSQL JDBC driver, downloading if necessary
async fn get_postgres_driver_jar() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let jdbc_test_dir = format!("{}/test-jdbc-drivers", target_dir);
    let jar_path = format!("{}/postgresql-42.7.1.jar", jdbc_test_dir);

    std::fs::create_dir_all(&jdbc_test_dir)?;

    if std::path::Path::new(&jar_path).exists() {
        info!("PostgreSQL JDBC driver found at {}", jar_path);
        let absolute_path = std::fs::canonicalize(&jar_path)?
            .to_string_lossy()
            .to_string();
        return Ok(absolute_path);
    }

    info!("Downloading PostgreSQL JDBC driver...");
    let download_url = "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar";

    let response = reqwest::get(download_url).await?;
    if !response.status().is_success() {
        return Err(format!("Failed to download driver: HTTP {}", response.status()).into());
    }

    let bytes = response.bytes().await?;
    std::fs::write(&jar_path, bytes)?;

    info!("PostgreSQL JDBC driver downloaded to {}", jar_path);
    let absolute_path = std::fs::canonicalize(&jar_path)?
        .to_string_lossy()
        .to_string();
    Ok(absolute_path)
}

/// Build the environment variables for a JDBC Postgres source connector.
fn build_jdbc_env(
    jdbc_url: &str,
    postgres_jar: &str,
    query: &str,
    mode: &str,
    iggy_setup: &IggySetup,
) -> HashMap<String, String> {
    let mut envs = HashMap::new();

    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_JDBC_URL".to_owned(),
        jdbc_url.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_DRIVER_CLASS".to_owned(),
        "org.postgresql.Driver".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_DRIVER_JAR_PATH".to_owned(),
        postgres_jar.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_USERNAME".to_owned(),
        POSTGRES_USER.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_PASSWORD".to_owned(),
        POSTGRES_PASSWORD.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_QUERY".to_owned(),
        query.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_POLL_INTERVAL".to_owned(),
        "1s".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_BATCH_SIZE".to_owned(),
        "100".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_MODE".to_owned(),
        mode.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_ENABLE_CONNECTION_POOL".to_owned(),
        "false".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_SNAKE_CASE_COLUMNS".to_owned(),
        "false".to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_INCLUDE_METADATA".to_owned(),
        "true".to_owned(),
    );

    // Stream configuration
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_STREAMS_0_STREAM".to_owned(),
        iggy_setup.stream.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_STREAMS_0_TOPIC".to_owned(),
        iggy_setup.topic.to_owned(),
    );
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_STREAMS_0_SCHEMA".to_owned(),
        "json".to_owned(),
    );

    envs
}

/// Poll messages from Iggy with retry logic, returning deserialized JSON values.
async fn poll_messages_with_retry(
    client: &crate::connectors::ConnectorsIggyClient,
    expected_count: usize,
) -> Vec<serde_json::Value> {
    let mut received: Vec<serde_json::Value> = Vec::new();
    for attempt in 0..POLL_ATTEMPTS {
        let polled_messages = client
            .get_messages()
            .await
            .expect("Failed to poll messages");

        for msg in &polled_messages.messages {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                received.push(value);
            }
        }

        if received.len() >= expected_count {
            info!(
                "Received {} messages after {} attempts",
                received.len(),
                attempt + 1
            );
            return received;
        }

        sleep(POLL_INTERVAL).await;
    }

    received
}

/// Setup connector runtime with JDBC source for Postgres
async fn setup_jdbc_postgres_source(
    jdbc_url: &str,
    postgres_jar: &str,
    query: &str,
    mode: &str,
) -> Result<
    (ConnectorsRuntime, crate::connectors::ConnectorsIggyClient),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let iggy_setup = IggySetup::default();
    let envs = build_jdbc_env(jdbc_url, postgres_jar, query, mode, &iggy_setup);

    let mut runtime = setup_runtime();
    runtime
        .init("jdbc/config_postgres.toml", Some(envs), iggy_setup)
        .await;

    let client = runtime.create_client().await;
    Ok((runtime, client))
}

/// Test: basic bulk mode query produces messages with correct structure
#[tokio::test]
#[serial]
async fn test_jdbc_postgres_source_basic() {
    let (_postgres_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Skipping test: Failed to setup Postgres: {}", e);
            return;
        }
    };

    let query = "SELECT 1 as id, 'test' as name";
    let (_runtime, client) = setup_jdbc_postgres_source(&jdbc_url, &postgres_jar, query, "bulk")
        .await
        .expect("Failed to setup runtime");

    info!("Waiting for JDBC connector to poll from Postgres...");
    let messages = poll_messages_with_retry(&client, 1).await;

    assert!(
        !messages.is_empty(),
        "Expected at least 1 message from JDBC Postgres source"
    );

    // Verify message structure: should have metadata wrapping (include_metadata=true)
    let first = &messages[0];
    assert!(
        first.get("data").is_some(),
        "Expected 'data' field in message (include_metadata=true), got: {}",
        first
    );
    assert_eq!(
        first.get("operation_type").and_then(|v| v.as_str()),
        Some("SELECT"),
        "Expected operation_type=SELECT"
    );

    // Verify the actual data content
    let data = first.get("data").unwrap();
    assert_eq!(
        data.get("id").and_then(|v| v.as_i64()),
        Some(1),
        "Expected id=1 in data"
    );
    assert_eq!(
        data.get("name").and_then(|v| v.as_str()),
        Some("test"),
        "Expected name='test' in data"
    );
}

/// Test: bulk mode with multiple rows from an actual table
#[tokio::test]
#[serial]
async fn test_jdbc_postgres_source_multiple_rows() {
    let (postgres_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Skipping test: Failed to setup Postgres: {}", e);
            return;
        }
    };

    // Use a multi-row SELECT to simulate table data without needing DDL
    let query = r#"
        SELECT * FROM (VALUES
            (1, 'alice', true),
            (2, 'bob', false),
            (3, 'carol', true)
        ) AS t(id, name, active)
    "#;

    let (_runtime, client) = setup_jdbc_postgres_source(&jdbc_url, &postgres_jar, query, "bulk")
        .await
        .expect("Failed to setup runtime");

    info!("Waiting for JDBC connector to poll multiple rows...");
    let messages = poll_messages_with_retry(&client, 3).await;

    assert!(
        messages.len() >= 3,
        "Expected at least 3 messages, got {}",
        messages.len()
    );

    // Verify each row has the expected structure
    for msg in &messages[..3] {
        let data = msg.get("data").expect("Missing 'data' field");
        assert!(data.get("id").is_some(), "Missing 'id' column in row data");
        assert!(
            data.get("name").is_some(),
            "Missing 'name' column in row data"
        );
        assert!(
            data.get("active").is_some(),
            "Missing 'active' column in row data"
        );
    }

    // Verify specific values for the first row
    let first_data = messages[0].get("data").unwrap();
    assert_eq!(first_data.get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        first_data.get("name").and_then(|v| v.as_str()),
        Some("alice")
    );

    // Keep container alive until assertions complete
    drop(postgres_container);
}

/// Test: message contains timestamp field when metadata is enabled
#[tokio::test]
#[serial]
async fn test_jdbc_postgres_source_metadata_fields() {
    let (_postgres_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Skipping test: Failed to setup Postgres: {}", e);
            return;
        }
    };

    let query = "SELECT 42 as value";
    let (_runtime, client) = setup_jdbc_postgres_source(&jdbc_url, &postgres_jar, query, "bulk")
        .await
        .expect("Failed to setup runtime");

    let messages = poll_messages_with_retry(&client, 1).await;
    assert!(!messages.is_empty(), "Expected at least 1 message");

    let msg = &messages[0];

    // Verify all metadata fields are present
    assert!(
        msg.get("timestamp").is_some(),
        "Missing 'timestamp' metadata field"
    );
    assert!(
        msg.get("operation_type").is_some(),
        "Missing 'operation_type' metadata field"
    );
    assert!(msg.get("data").is_some(), "Missing 'data' metadata field");

    // table_name should be null for SELECT queries without a specific table
    // (this is expected behavior for computed queries)
    assert!(
        msg.get("table_name").is_some(),
        "Missing 'table_name' metadata field"
    );
}

/// Derive a sqlx (`postgres://`) URL from the connector's JDBC URL so the test
/// can seed the table the source reads from.
fn pg_sqlx_url(jdbc_url: &str) -> String {
    let host_and_db = jdbc_url
        .strip_prefix("jdbc:postgresql://")
        .unwrap_or(jdbc_url);
    format!("postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host_and_db}")
}

/// Collect the `data.id` integer from each polled (metadata-wrapped) message.
fn collect_ids(messages: &[serde_json::Value]) -> Vec<i64> {
    messages
        .iter()
        .filter_map(|m| {
            m.get("data")
                .and_then(|d| d.get("id"))
                .and_then(|v| v.as_i64())
        })
        .collect()
}

/// Test: incremental mode advances its tracking offset across polls; newly
/// inserted rows are delivered exactly once and previously read rows are not
/// re-delivered.
#[tokio::test]
#[serial]
async fn test_jdbc_postgres_source_incremental_advances_offset() {
    let (_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Skipping test: Failed to setup Postgres: {e}");
            return;
        }
    };

    // Seed a real table BEFORE the source starts polling.
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg_sqlx_url(&jdbc_url))
        .await
        .expect("Failed to connect to Postgres for seeding");
    sqlx::query("CREATE TABLE inc_test (id INT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("Failed to create table");
    sqlx::query("INSERT INTO inc_test (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .execute(&pool)
        .await
        .expect("Failed to insert initial rows");

    let query = "SELECT id, name FROM inc_test WHERE id > {last_offset} ORDER BY id";
    let (_runtime, client) =
        setup_jdbc_postgres_source(&jdbc_url, &postgres_jar, query, "incremental")
            .await
            .expect("Failed to setup runtime");

    // First batch: ids 1..3.
    let first = poll_messages_with_retry(&client, 3).await;
    let mut first_ids = collect_ids(&first);
    first_ids.sort_unstable();
    assert_eq!(first_ids, vec![1, 2, 3], "Expected ids 1,2,3 on first poll");

    // Insert more rows; only these (id > last_offset) should arrive next.
    sqlx::query("INSERT INTO inc_test (id, name) VALUES (4, 'd'), (5, 'e')")
        .execute(&pool)
        .await
        .expect("Failed to insert additional rows");

    let second = poll_messages_with_retry(&client, 2).await;
    let mut second_ids = collect_ids(&second);
    second_ids.sort_unstable();
    assert_eq!(
        second_ids,
        vec![4, 5],
        "Expected only the new ids 4,5 (offset must have advanced past 3), got {second_ids:?}"
    );
}

/// Test: a single poll over many rows succeeds. This exercises the JNI
/// local-reference frame management in `read_rows`: a few-hundred-row result set
/// creates hundreds of per-column local references in one native call, which
/// would overflow the JNI local reference table (and abort the JVM) if each row
/// were not read inside its own local frame.
#[tokio::test]
#[serial]
async fn test_jdbc_postgres_source_large_result_set() {
    let (_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Skipping test: Failed to setup Postgres: {e}");
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg_sqlx_url(&jdbc_url))
        .await
        .expect("Failed to connect to Postgres for seeding");
    sqlx::query("CREATE TABLE big_test (id INT PRIMARY KEY, name TEXT, val NUMERIC(12,2))")
        .execute(&pool)
        .await
        .expect("Failed to create table");
    sqlx::query(
        "INSERT INTO big_test (id, name, val) \
         SELECT g, 'row_' || g, (g * 1.5)::numeric(12,2) FROM generate_series(1, 300) g",
    )
    .execute(&pool)
    .await
    .expect("Failed to insert rows");

    // batch_size well above the row count so the whole table is read in a single
    // poll (one read_rows call → hundreds of local refs).
    let iggy_setup = IggySetup::default();
    let query = "SELECT id, name, val FROM big_test ORDER BY id";
    let mut envs = build_jdbc_env(&jdbc_url, &postgres_jar, query, "bulk", &iggy_setup);
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_BATCH_SIZE".to_owned(),
        "5000".to_owned(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init("jdbc/config_postgres.toml", Some(envs), iggy_setup)
        .await;
    let client = runtime.create_client().await;

    // The runtime would have crashed on the oversized poll without per-row local
    // frames; receiving a healthy batch of well-formed messages proves it did not.
    let messages = poll_messages_with_retry(&client, 150).await;
    assert!(
        messages.len() >= 150,
        "Expected the source to stream a large result set without crashing; got {} messages",
        messages.len()
    );
    for msg in &messages[..150] {
        let data = msg.get("data").expect("Missing 'data' field");
        assert!(data.get("id").and_then(|v| v.as_i64()).is_some());
        assert!(data.get("name").and_then(|v| v.as_str()).is_some());
    }
}
