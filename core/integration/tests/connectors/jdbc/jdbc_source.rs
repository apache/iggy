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
use std::collections::{BTreeSet, HashMap};
use std::io::Cursor;
use std::time::{Duration, Instant};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::time::sleep;
use tracing::info;
use zip::ZipArchive;

const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "postgres";
const POSTGRES_DB: &str = "postgres";

/// How long to wait for the source to deliver what a test expects.
///
/// A deadline, not an attempt count: an attempt count doubles as a cap on the
/// total a collect-until-N loop can ever drain (attempts x batch size), so a
/// test asking for more than a fraction of that cap fails on a slow runner even
/// though the source delivered everything.
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between poll attempts
const POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Messages requested per poll, kept well above any single expectation below so
/// the deadline alone bounds how long the source may take.
const POLL_BATCH: u32 = 500;

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

/// The class the connector hands to `Class.forName`. Checking that the archive
/// actually contains it is what makes the integrity check below meaningful.
const DRIVER_CLASS_ENTRY: &str = "org/postgresql/Driver.class";

/// Whether `bytes` is a driver JAR the JVM can actually load from: a readable
/// ZIP archive that contains the driver class.
///
/// Magic bytes plus a minimum size are not enough. A download truncated past
/// that size still starts with the ZIP local-file-header magic while its central
/// directory is gone, so it passes a size check yet cannot be read as an
/// archive. It is then cached and reused by every later JDBC test in the job,
/// and the only symptom is a `ClassNotFoundException` on `Class.forName` that
/// fails the connector's `open()`, so the runtime skips the source and every
/// JDBC test fails having received no messages at all. Opening the archive and
/// looking the entry up tests the property the JVM needs, and a cached file that
/// fails it is deleted and re-downloaded rather than reused.
fn looks_like_jar(bytes: &[u8]) -> bool {
    let Ok(mut archive) = ZipArchive::new(Cursor::new(bytes)) else {
        return false;
    };
    archive.by_name(DRIVER_CLASS_ENTRY).is_ok()
}

/// Get the PostgreSQL JDBC driver, downloading and integrity-checking it if a
/// valid copy is not already cached. Downloads from Maven Central, verifies the
/// bytes open as an archive holding the driver class before persisting, and
/// writes via a temp file + atomic rename so a partial or corrupt download can
/// never be cached and reused.
async fn get_postgres_driver_jar() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let jdbc_test_dir = format!("{target_dir}/test-jdbc-drivers");
    let jar_path = format!("{jdbc_test_dir}/postgresql-42.7.1.jar");

    std::fs::create_dir_all(&jdbc_test_dir)?;

    // Reuse the cached jar only if it is actually a valid JAR; a previously
    // cached bad download must self-heal rather than fail every run.
    if std::path::Path::new(&jar_path).exists() {
        match std::fs::read(&jar_path) {
            Ok(bytes) if looks_like_jar(&bytes) => {
                info!("PostgreSQL JDBC driver found at {jar_path}");
                return Ok(std::fs::canonicalize(&jar_path)?
                    .to_string_lossy()
                    .to_string());
            }
            _ => {
                info!("Cached JDBC driver at {jar_path} is invalid; re-downloading");
                let _ = std::fs::remove_file(&jar_path);
            }
        }
    }

    info!("Downloading PostgreSQL JDBC driver...");
    let download_url =
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar";
    let response = reqwest::get(download_url).await?;
    if !response.status().is_success() {
        return Err(format!("Failed to download driver: HTTP {}", response.status()).into());
    }
    let bytes = response.bytes().await?;
    if !looks_like_jar(&bytes) {
        return Err(format!(
            "Downloaded JDBC driver is not a readable JAR containing {DRIVER_CLASS_ENTRY} \
             ({} bytes); the download was truncated or the endpoint returned an error page",
            bytes.len()
        )
        .into());
    }

    // Write to a unique temp file, then atomically rename, so a crash or a
    // concurrent test never observes a half-written jar at `jar_path`.
    let tmp_path = format!("{jar_path}.{}.tmp", std::process::id());
    std::fs::write(&tmp_path, &bytes)?;
    std::fs::rename(&tmp_path, &jar_path)?;

    info!("PostgreSQL JDBC driver downloaded to {jar_path}");
    Ok(std::fs::canonicalize(&jar_path)?
        .to_string_lossy()
        .to_string())
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

/// Poll until at least `expected_count` messages have been collected or
/// `POLL_TIMEOUT` elapses, returning them deserialized.
async fn poll_messages_with_retry(
    client: &crate::connectors::ConnectorsIggyClient,
    expected_count: usize,
) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut received: Vec<serde_json::Value> = Vec::new();

    loop {
        let polled_messages = client
            .get_messages(POLL_BATCH)
            .await
            .expect("Failed to poll messages");

        for msg in &polled_messages.messages {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                received.push(value);
            }
        }

        if received.len() >= expected_count {
            info!("Received {} messages", received.len());
            return received;
        }

        if Instant::now() >= deadline {
            return received;
        }

        sleep(POLL_INTERVAL).await;
    }
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
async fn bulk_query_produces_message_to_iggy() {
    let (_postgres_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => panic!("Failed to set up Postgres container: {e}"),
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
async fn bulk_query_produces_multiple_rows_to_iggy() {
    let (postgres_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => panic!("Failed to set up Postgres container: {e}"),
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
async fn source_includes_metadata_fields_when_enabled() {
    let (_postgres_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => panic!("Failed to set up Postgres container: {e}"),
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

/// Poll until every id in `expected` has been seen, or `timeout` elapses.
/// Returns the distinct ids seen, ascending, plus the raw number of messages
/// received.
///
/// A source can deliver a row more than once: bulk mode re-runs its query every
/// poll interval, and delivery is at-least-once, so a nacked batch is re-read.
/// Matching on the distinct ids seen, rather than on a fixed-length prefix of the
/// received messages, keeps a re-delivered batch from failing an otherwise
/// healthy run. The returned count separates "the source delivered nothing" from
/// "it delivered messages that did not carry the expected `data.id`", which a
/// bare id list cannot express: unmatched messages are dropped silently.
async fn poll_until_ids_seen(
    client: &crate::connectors::ConnectorsIggyClient,
    expected: &[i64],
    timeout: Duration,
) -> (Vec<i64>, usize) {
    let deadline = Instant::now() + timeout;
    let mut seen: BTreeSet<i64> = BTreeSet::new();
    let mut received = 0usize;

    loop {
        let polled_messages = client
            .get_messages(POLL_BATCH)
            .await
            .expect("Failed to poll messages");

        for msg in &polled_messages.messages {
            received += 1;
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&msg.payload)
                && let Some(id) = value
                    .get("data")
                    .and_then(|data| data.get("id"))
                    .and_then(|id| id.as_i64())
            {
                seen.insert(id);
            }
        }

        if expected.iter().all(|id| seen.contains(id)) {
            info!("Saw expected ids {expected:?} in {received} received messages");
            break;
        }

        if Instant::now() >= deadline {
            break;
        }

        sleep(POLL_INTERVAL).await;
    }

    (seen.into_iter().collect(), received)
}

/// Test: incremental mode advances its tracking offset across polls; newly
/// inserted rows are delivered exactly once and previously read rows are not
/// re-delivered.
#[tokio::test]
#[serial]
async fn incremental_mode_advances_offset_across_polls() {
    let (_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => panic!("Failed to set up Postgres container: {e}"),
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
    let (first_ids, first_received) = poll_until_ids_seen(&client, &[1, 2, 3], POLL_TIMEOUT).await;
    assert_eq!(
        first_ids,
        vec![1, 2, 3],
        "Expected ids 1,2,3 on the first poll; got ids {first_ids:?} from {first_received} \
         received message(s)"
    );

    // Insert more rows; only these (id > last_offset) should arrive next.
    sqlx::query("INSERT INTO inc_test (id, name) VALUES (4, 'd'), (5, 'e')")
        .execute(&pool)
        .await
        .expect("Failed to insert additional rows");

    let (second_ids, second_received) = poll_until_ids_seen(&client, &[4, 5], POLL_TIMEOUT).await;
    assert_eq!(
        second_ids,
        vec![4, 5],
        "Expected only the new ids 4,5 (offset must have advanced past 3); got ids \
         {second_ids:?} from {second_received} received message(s)"
    );
}

/// Test: a single poll over many rows succeeds. This exercises the JNI
/// local-reference frame management in `read_rows`: a few-hundred-row result set
/// creates hundreds of per-column local references in one native call, which
/// would overflow the JNI local reference table (and abort the JVM) if each row
/// were not read inside its own local frame.
#[tokio::test]
#[serial]
async fn large_result_set_streams_without_crashing() {
    let (_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => panic!("Failed to set up Postgres container: {e}"),
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

/// Test: the source keeps polling and recovers after a query that raises a
/// SQLException on every poll. The query targets a table that does not exist
/// yet, so `executeQuery` throws each cycle; once the table is created the very
/// next poll must succeed and deliver its rows.
///
/// This is the regression guard for exception clearing: a thrown Java exception
/// left pending would make the following JNI call (the statement `close()` in
/// the error path, or the next poll's `isValid`) run with an exception pending,
/// which the JNI spec forbids and which aborts the embedded JVM (the whole
/// runtime process). If that happened the runtime would die and never deliver
/// the post-recovery rows below.
#[tokio::test]
#[serial]
async fn source_recovers_after_repeated_query_errors() {
    let (_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => panic!("Failed to set up Postgres container: {e}"),
    };

    // Start the source against a table that does not exist yet: every poll
    // raises "relation does not exist" (SQLState 42P01).
    let query = "SELECT id, name FROM recover_test ORDER BY id";
    let (_runtime, client) = setup_jdbc_postgres_source(&jdbc_url, &postgres_jar, query, "bulk")
        .await
        .expect("Failed to setup runtime");

    // Let the source fail across several poll cycles (poll interval is 1s).
    sleep(Duration::from_secs(4)).await;

    // Now create and seed the table; the next successful poll should deliver it.
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg_sqlx_url(&jdbc_url))
        .await
        .expect("Failed to connect to Postgres for seeding");
    sqlx::query("CREATE TABLE recover_test (id INT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("Failed to create table");
    sqlx::query("INSERT INTO recover_test (id, name) VALUES (1, 'a'), (2, 'b')")
        .execute(&pool)
        .await
        .expect("Failed to insert rows");

    let (ids, received) = poll_until_ids_seen(&client, &[1, 2], POLL_TIMEOUT).await;
    assert_eq!(
        ids,
        vec![1, 2],
        "Source must recover after repeated query failures and deliver ids 1,2; \
         got ids {ids:?} from {received} received message(s)"
    );
}

/// Test: bulk mode fails closed when the result set is larger than batch_size,
/// rather than silently syncing a truncated subset. With batch_size below the
/// row count the source errors every poll and delivers nothing (in particular,
/// never a truncated partial set).
#[tokio::test]
#[serial]
async fn bulk_result_larger_than_batch_size_fails_closed() {
    let (_container, jdbc_url, postgres_jar) = match setup_postgres_container().await {
        Ok(result) => result,
        Err(e) => panic!("Failed to set up Postgres container: {e}"),
    };

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg_sqlx_url(&jdbc_url))
        .await
        .expect("Failed to connect to Postgres for seeding");
    sqlx::query("CREATE TABLE trunc_test (id INT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("Failed to create table");
    sqlx::query("INSERT INTO trunc_test (id) SELECT generate_series(1, 5)")
        .execute(&pool)
        .await
        .expect("Failed to insert rows");

    // Bulk mode with batch_size below the 5-row result set.
    let iggy_setup = IggySetup::default();
    let query = "SELECT id FROM trunc_test ORDER BY id";
    let mut envs = build_jdbc_env(&jdbc_url, &postgres_jar, query, "bulk", &iggy_setup);
    envs.insert(
        "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_BATCH_SIZE".to_owned(),
        "2".to_owned(),
    );

    let mut runtime = setup_runtime();
    runtime
        .init("jdbc/config_postgres.toml", Some(envs), iggy_setup)
        .await;
    let client = runtime.create_client().await;

    // Several poll cycles (poll interval is 1s). A fail-closed source delivers
    // nothing, and in particular never the truncated 2-row subset.
    sleep(Duration::from_secs(4)).await;
    let polled = client
        .get_messages(POLL_BATCH)
        .await
        .expect("Failed to poll messages");
    assert!(
        polled.messages.is_empty(),
        "bulk truncation must fail closed and deliver nothing, got {} messages",
        polled.messages.len()
    );
}
