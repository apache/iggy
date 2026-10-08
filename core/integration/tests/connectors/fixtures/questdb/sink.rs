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

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use reqwest_middleware::ClientWithMiddleware as HttpClient;
use tokio::time::sleep;
use tracing::info;

use super::container::{
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SINK_BATCH_SIZE, ENV_SINK_CONNECTION_STRING,
    ENV_SINK_FLUSH_TIMEOUT, ENV_SINK_INCLUDE_HEADERS, ENV_SINK_INCLUDE_OFFSET_COLUMN,
    ENV_SINK_INCLUDE_PARTITION_COLUMN, ENV_SINK_INCLUDE_STREAM_COLUMN,
    ENV_SINK_INCLUDE_TOPIC_COLUMN, ENV_SINK_INTEGER_COLUMNS, ENV_SINK_LOG_REJECTED_PAYLOAD,
    ENV_SINK_NUMBERS_AS_DOUBLE, ENV_SINK_PATH, ENV_SINK_STREAMS_0_CONSUMER_GROUP,
    ENV_SINK_STREAMS_0_SCHEMA, ENV_SINK_STREAMS_0_STREAM, ENV_SINK_STREAMS_0_TOPICS,
    ENV_SINK_SYMBOL_COLUMNS, ENV_SINK_TABLE, ENV_SINK_TIMESTAMP_FIELD, ENV_SINK_TIMESTAMP_SOURCE,
    ENV_SINK_TIMESTAMP_UNIT, ENV_SINK_UUID_COLUMNS, HEALTH_CHECK_ATTEMPTS,
    HEALTH_CHECK_INTERVAL_MS, QuestDbContainer, QuestDbOps, SINK_PLUGIN_PATH, create_http_client,
    ensure_plugin_built,
};

const POLL_ATTEMPTS: usize = 120;
const POLL_INTERVAL_MS: u64 = 100;

pub const SINK_TABLE: &str = "iggy_events";

/// Knobs the individual tests vary. Everything not set here falls back to the
/// connector's own defaults, so a default fixture also exercises those.
#[derive(Debug, Clone, Default)]
pub struct QuestDbSinkOptions {
    pub timestamp_source: Option<String>,
    pub timestamp_field: Option<String>,
    pub timestamp_unit: Option<String>,
    pub symbol_columns: Option<Vec<String>>,
    pub uuid_columns: Option<Vec<String>>,
    pub integer_columns: Option<Vec<String>>,
    pub include_stream_column: Option<bool>,
    pub include_topic_column: Option<bool>,
    pub include_partition_column: Option<bool>,
    pub include_offset_column: Option<bool>,
    pub include_headers: Option<bool>,
    pub numbers_as_double: Option<bool>,
    pub log_rejected_payload: Option<bool>,
    pub batch_size: Option<u32>,
    pub flush_timeout: Option<String>,
    /// Schema the runtime decodes the stream with. `None` → "json".
    pub schema: Option<String>,
    /// Enable QWP store-and-forward against a temp directory owned by the
    /// fixture. The sink runs in the connectors-runtime process on the host,
    /// so this is a plain host path rather than a container volume.
    pub store_and_forward: bool,
    /// DDL executed once the container is healthy and before the connectors
    /// runtime starts, for tests that need the table to exist with settings
    /// QWP cannot infer, such as `DEDUP UPSERT KEYS`.
    pub pre_create_ddl: Option<String>,
}

pub struct QuestDbSinkFixture {
    container: QuestDbContainer,
    http_client: HttpClient,
    pub options: QuestDbSinkOptions,
    /// Kept alive for the test's lifetime so the slot directory survives.
    sf_dir: Option<tempfile::TempDir>,
}

impl QuestDbOps for QuestDbSinkFixture {
    fn container(&self) -> &QuestDbContainer {
        &self.container
    }
    fn http_client(&self) -> &HttpClient {
        &self.http_client
    }
}

impl QuestDbSinkFixture {
    pub fn table(&self) -> String {
        SINK_TABLE.to_string()
    }

    /// Poll until `table` holds exactly `expected` rows. The table does not
    /// exist until the sink's first successful flush, which is why a missing
    /// table is treated as "not yet" rather than an error.
    ///
    /// A count past `expected` is an error, not a success: the connector is
    /// at-least-once, so a duplicate row is the documented failure mode, and a
    /// helper that returned on "at least" would let every caller's
    /// `assert_eq!(count, expected)` pass with one in the table.
    pub async fn wait_for_rows(&self, expected: usize) -> Result<usize, TestBinaryError> {
        let table = self.table();
        let mut last = None;
        for _ in 0..POLL_ATTEMPTS {
            if let Ok(Some(count)) = self.count_rows(&table).await {
                last = Some(count);
                if count == expected {
                    info!("Found {count} rows in QuestDB table {table} (expected {expected})");
                    return Ok(count);
                }
                if count > expected {
                    return Err(TestBinaryError::InvalidState {
                        message: format!(
                            "Expected exactly {expected} rows in {table} but observed {count}, so a duplicate landed"
                        ),
                    });
                }
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected {expected} rows in {table} but observed {last:?} after {POLL_ATTEMPTS} attempts"
            ),
        })
    }

    /// Column name to type, as QuestDB actually created them.
    pub async fn column_types(&self) -> Result<HashMap<String, String>, TestBinaryError> {
        let query = format!("show columns from '{}'", self.table());
        let value = self.exec(&query).await?;
        // Surface a query error rather than returning an empty map, which a
        // test asserting a column is absent would read as success.
        if let Some(error) = value.get("error") {
            return Err(TestBinaryError::InvalidState {
                message: format!("QuestDB rejected `{query}`: {error}"),
            });
        }
        let mut columns = HashMap::new();
        if let Some(rows) = value.get("dataset").and_then(serde_json::Value::as_array) {
            for row in rows {
                let name = row.get(0).and_then(serde_json::Value::as_str);
                let kind = row.get(1).and_then(serde_json::Value::as_str);
                if let (Some(name), Some(kind)) = (name, kind) {
                    columns.insert(name.to_string(), kind.to_string());
                }
            }
        }
        Ok(columns)
    }

    /// Every value of `column`, in insertion order.
    ///
    /// Deliberately unordered: QuestDB cannot `ORDER BY` an array column, and
    /// callers that care about order sort the values themselves.
    pub async fn column_values(
        &self,
        column: &str,
    ) -> Result<Vec<serde_json::Value>, TestBinaryError> {
        let query = format!("select \"{column}\" from '{}'", self.table());
        let value = self.exec(&query).await?;
        // Surface a query error instead of reporting it as "no rows", which
        // otherwise looks like the sink wrote nothing.
        if let Some(error) = value.get("error") {
            return Err(TestBinaryError::InvalidState {
                message: format!("QuestDB rejected `{query}`: {error}"),
            });
        }
        let rows = value
            .get("dataset")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(rows
            .into_iter()
            .filter_map(|row| row.get(0).cloned())
            .collect())
    }

    /// The QWP store-and-forward root, when the fixture enabled it.
    pub fn sf_dir(&self) -> Option<&std::path::Path> {
        self.sf_dir.as_ref().map(|dir| dir.path())
    }

    /// Slot directories the client has minted under the store-and-forward root.
    /// The pooled sender names them `<sender_id>-ingest-<index>`.
    pub fn sf_slots(&self) -> Vec<std::path::PathBuf> {
        let Some(root) = self.sf_dir() else {
            return Vec::new();
        };
        let Ok(entries) = std::fs::read_dir(root) else {
            return Vec::new();
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect()
    }

    /// Segment files inside `slot`. The client names them `sf-*.sfa` and trims
    /// them once the server acknowledges, so their presence means bytes are
    /// parked on disk awaiting delivery.
    pub fn sf_segments(slot: &std::path::Path) -> Vec<std::path::PathBuf> {
        let Ok(entries) = std::fs::read_dir(slot) else {
            return Vec::new();
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "sfa"))
            .collect()
    }

    /// Poll until at least one store-and-forward slot exists on disk.
    pub async fn wait_for_sf_slot(&self) -> Result<std::path::PathBuf, TestBinaryError> {
        for _ in 0..POLL_ATTEMPTS {
            if let Some(slot) = self.sf_slots().into_iter().next() {
                return Ok(slot);
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "No store-and-forward slot appeared under {:?}",
                self.sf_dir()
            ),
        })
    }

    pub async fn simulate_outage(&self) -> Result<(), TestBinaryError> {
        self.container.simulate_outage().await
    }

    pub async fn resume(&self) -> Result<(), TestBinaryError> {
        self.container.resume().await
    }

    pub async fn setup_with_options(options: QuestDbSinkOptions) -> Result<Self, TestBinaryError> {
        // Check before pulling or starting anything: a missing plugin should cost
        // a second and name its own fix, not a container start and a timeout.
        ensure_plugin_built()?;
        let container = QuestDbContainer::start().await?;
        let http_client = create_http_client();
        let sf_dir = if options.store_and_forward {
            Some(
                tempfile::tempdir().map_err(|e| TestBinaryError::FixtureSetup {
                    fixture_type: "QuestDbSink".to_string(),
                    message: format!("Failed to create store-and-forward dir: {e}"),
                })?,
            )
        } else {
            None
        };
        let fixture = Self {
            container,
            http_client,
            options,
            sf_dir,
        };

        for attempt in 0..HEALTH_CHECK_ATTEMPTS {
            let url = format!("{}/ping", fixture.container.base_url);
            match fixture.http_client.get(&url).send().await {
                Ok(response) if response.status().as_u16() == 204 => {
                    info!("QuestDB /ping OK after {} attempts", attempt + 1);
                    if let Some(ddl) = fixture.options.pre_create_ddl.clone() {
                        let result = fixture.exec(&ddl).await?;
                        if let Some(error) = result.get("error") {
                            return Err(TestBinaryError::FixtureSetup {
                                fixture_type: "QuestDbSink".to_string(),
                                message: format!("Pre-create DDL failed: {error}"),
                            });
                        }
                    }
                    return Ok(fixture);
                }
                Ok(response) => info!(
                    "QuestDB /ping status {} (attempt {})",
                    response.status(),
                    attempt + 1
                ),
                Err(error) => info!("QuestDB /ping error on attempt {}: {error}", attempt + 1),
            }
            sleep(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS)).await;
        }

        Err(TestBinaryError::FixtureSetup {
            fixture_type: "QuestDbSink".to_string(),
            message: format!(
                "QuestDB /ping did not return 204 after {HEALTH_CHECK_ATTEMPTS} attempts"
            ),
        })
    }
}

#[async_trait]
impl TestFixture for QuestDbSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::setup_with_options(QuestDbSinkOptions::default()).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        // Store-and-forward is a connect-string concern, not a plugin config
        // key, so it is appended here rather than exposed as its own env var.
        let mut connection_string = self.container.connection_string();
        if let Some(dir) = self.sf_dir() {
            connection_string.push_str(&format!("sf_dir={};sender_id=iggy_test;", dir.display()));
        }
        envs.insert(ENV_SINK_CONNECTION_STRING.to_string(), connection_string);
        envs.insert(ENV_SINK_TABLE.to_string(), self.table());
        envs.insert(
            ENV_SINK_STREAMS_0_STREAM.to_string(),
            DEFAULT_TEST_STREAM.to_string(),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_TOPICS.to_string(),
            format!("[{DEFAULT_TEST_TOPIC}]"),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_SCHEMA.to_string(),
            self.options
                .schema
                .clone()
                .unwrap_or_else(|| "json".to_string()),
        );
        envs.insert(
            ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
            "questdb_sink_cg".to_string(),
        );
        envs.insert(ENV_SINK_PATH.to_string(), SINK_PLUGIN_PATH.to_string());

        if let Some(value) = &self.options.timestamp_source {
            envs.insert(ENV_SINK_TIMESTAMP_SOURCE.to_string(), value.clone());
        }
        if let Some(value) = &self.options.timestamp_field {
            envs.insert(ENV_SINK_TIMESTAMP_FIELD.to_string(), value.clone());
        }
        if let Some(value) = &self.options.timestamp_unit {
            envs.insert(ENV_SINK_TIMESTAMP_UNIT.to_string(), value.clone());
        }
        if let Some(values) = &self.options.symbol_columns {
            envs.insert(ENV_SINK_SYMBOL_COLUMNS.to_string(), toml_list(values));
        }
        if let Some(values) = &self.options.uuid_columns {
            envs.insert(ENV_SINK_UUID_COLUMNS.to_string(), toml_list(values));
        }
        if let Some(values) = &self.options.integer_columns {
            envs.insert(ENV_SINK_INTEGER_COLUMNS.to_string(), toml_list(values));
        }
        if let Some(value) = self.options.include_stream_column {
            envs.insert(
                ENV_SINK_INCLUDE_STREAM_COLUMN.to_string(),
                value.to_string(),
            );
        }
        if let Some(value) = self.options.include_topic_column {
            envs.insert(ENV_SINK_INCLUDE_TOPIC_COLUMN.to_string(), value.to_string());
        }
        if let Some(value) = self.options.include_partition_column {
            envs.insert(
                ENV_SINK_INCLUDE_PARTITION_COLUMN.to_string(),
                value.to_string(),
            );
        }
        if let Some(value) = self.options.include_offset_column {
            envs.insert(
                ENV_SINK_INCLUDE_OFFSET_COLUMN.to_string(),
                value.to_string(),
            );
        }
        if let Some(value) = self.options.numbers_as_double {
            envs.insert(ENV_SINK_NUMBERS_AS_DOUBLE.to_string(), value.to_string());
        }
        if let Some(value) = self.options.include_headers {
            envs.insert(ENV_SINK_INCLUDE_HEADERS.to_string(), value.to_string());
        }
        if let Some(value) = self.options.log_rejected_payload {
            envs.insert(ENV_SINK_LOG_REJECTED_PAYLOAD.to_string(), value.to_string());
        }
        if let Some(value) = self.options.batch_size {
            envs.insert(ENV_SINK_BATCH_SIZE.to_string(), value.to_string());
        }
        if let Some(value) = &self.options.flush_timeout {
            envs.insert(ENV_SINK_FLUSH_TIMEOUT.to_string(), value.clone());
        }
        envs
    }
}

/// The runtime parses list-valued env vars as TOML arrays.
fn toml_list(values: &[String]) -> String {
    let quoted: Vec<String> = values.iter().map(|value| format!("\"{value}\"")).collect();
    format!("[{}]", quoted.join(","))
}

// ── Named variants ────────────────────────────────────────────────────────────
/// Declares a named fixture that delegates to [`QuestDbSinkFixture`].
///
/// Each variant differs only by its options, so the two delegating methods are
/// written once here. The same shape as `fixtures/influxdb/sink.rs`.
macro_rules! delegate_fixture {
    ($(#[$meta:meta])* $wrapper:ident, $opts:expr) => {
        $(#[$meta])*
        pub struct $wrapper(pub QuestDbSinkFixture);

        #[async_trait]
        impl TestFixture for $wrapper {
            async fn setup() -> Result<Self, TestBinaryError> {
                QuestDbSinkFixture::setup_with_options($opts)
                    .await
                    .map(Self)
            }

            fn connectors_runtime_envs(&self) -> HashMap<String, String> {
                self.0.connectors_runtime_envs()
            }
        }
    };
}

delegate_fixture!(
    /// `numbers_as_double` turned off, so a column's type follows whichever
    /// record defined it first. This is the opt-out, not the default.
    QuestDbSinkTypedNumbersFixture,
    QuestDbSinkOptions {
        numbers_as_double: Some(false),
        ..Default::default()
    }
);

delegate_fixture!(
    /// A pre-created table whose declared types are deliberately the opposite of
    /// what the connector sends, so one record exercises both coercion
    /// directions: `measured` is a DOUBLE column receiving a LONG frame, and
    /// `counted` is a LONG column receiving a DOUBLE frame.
    QuestDbSinkCoercionFixture,
    QuestDbSinkOptions {
        integer_columns: Some(vec!["measured".to_string()]),
        include_stream_column: Some(false),
        include_topic_column: Some(false),
        pre_create_ddl: Some(format!(
            "create table {SINK_TABLE} (measured DOUBLE, counted LONG, \
             big LONG, timestamp TIMESTAMP) \
             timestamp(timestamp) partition by DAY WAL"
        )),
        ..Default::default()
    }
);

delegate_fixture!(
    QuestDbSinkTypedFixture,
    QuestDbSinkOptions {
        symbol_columns: Some(vec!["side".to_string()]),
        uuid_columns: Some(vec!["trade_id".to_string()]),
        integer_columns: Some(vec!["amount".to_string()]),
        include_partition_column: Some(true),
        include_offset_column: Some(true),
        log_rejected_payload: Some(true),
        ..Default::default()
    }
);

delegate_fixture!(
    /// Store-and-forward enabled, with a flush timeout short enough that an
    /// outage surfaces quickly instead of stalling the test.
    QuestDbSinkStoreAndForwardFixture,
    QuestDbSinkOptions {
        store_and_forward: true,
        flush_timeout: Some("5s".to_string()),
        ..Default::default()
    }
);

delegate_fixture!(
    /// A batch size far below the message count, so `consume` must chunk.
    QuestDbSinkSmallBatchFixture,
    QuestDbSinkOptions {
        batch_size: Some(7),
        include_offset_column: Some(true),
        ..Default::default()
    }
);

delegate_fixture!(
    /// Stamps rows from the producer's clock rather than the server's.
    QuestDbSinkOriginTimestampFixture,
    QuestDbSinkOptions {
        timestamp_source: Some("origin".to_string()),
        ..Default::default()
    }
);

delegate_fixture!(
    /// Lets QuestDB stamp the designated timestamp on arrival.
    QuestDbSinkServerTimestampFixture,
    QuestDbSinkOptions {
        timestamp_source: Some("server".to_string()),
        ..Default::default()
    }
);

delegate_fixture!(
    /// Text-schema stream, which has no field structure and lands in `payload`.
    QuestDbSinkTextFixture,
    QuestDbSinkOptions {
        schema: Some("text".to_string()),
        ..Default::default()
    }
);

delegate_fixture!(
    /// Raw-schema stream: the payload arrives as bytes with no field structure.
    QuestDbSinkRawFixture,
    QuestDbSinkOptions {
        schema: Some("raw".to_string()),
        ..Default::default()
    }
);

delegate_fixture!(
    /// Table pre-created with `DEDUP UPSERT KEYS`, which is the recipe the README
    /// gives operators for the at-least-once delivery model.
    QuestDbSinkDedupFixture,
    QuestDbSinkOptions {
        timestamp_source: Some("payload".to_string()),
        timestamp_field: Some("event_time".to_string()),
        timestamp_unit: Some("micros".to_string()),
        include_stream_column: Some(false),
        include_topic_column: Some(false),
        pre_create_ddl: Some(format!(
            // `revision` is deliberately outside the dedup keys: a second
            // delivery carrying a higher revision proves it reached the
            // table, which a row count alone cannot show.
            "create table {SINK_TABLE} (seq LONG, revision LONG, timestamp TIMESTAMP_NS) \
             timestamp(timestamp) partition by DAY WAL \
             DEDUP UPSERT KEYS(timestamp, seq)"
        )),
        ..Default::default()
    }
);

delegate_fixture!(
    /// Writes each Apache Iggy message header as its own `header_*` column.
    QuestDbSinkHeadersFixture,
    QuestDbSinkOptions {
        include_headers: Some(true),
        ..Default::default()
    }
);

delegate_fixture!(
    QuestDbSinkPayloadTimestampFixture,
    QuestDbSinkOptions {
        timestamp_source: Some("payload".to_string()),
        timestamp_field: Some("event_time".to_string()),
        timestamp_unit: Some("micros".to_string()),
        ..Default::default()
    }
);
