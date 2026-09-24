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

use super::container::{
    ENV_SOURCE_BOOTSTRAP_SERVERS, ENV_SOURCE_DATABASE, ENV_SOURCE_INCLUDE_METADATA,
    ENV_SOURCE_PATH, ENV_SOURCE_POLL_INTERVAL, ENV_SOURCE_STARTING_OFFSET,
    ENV_SOURCE_STREAMS_0_SCHEMA, ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC,
    ENV_SOURCE_TABLE, FlussContainer,
};
use async_trait::async_trait;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{Date, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
use integration::harness::seeds;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;
use tokio::time::sleep;

const DATABASE: &str = "iggy_test";
const TABLE: &str = "events";
const POLL_INTERVAL: &str = "100ms";
const SLOW_POLL_INTERVAL: &str = "5s";
const EXISTING_ROW_COUNT: usize = 3;
const ALL_TYPES_COLUMN_COUNT: usize = 17;
const READY_ATTEMPTS: usize = 20;
const READY_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Fluss log table read by the source connector under test.
pub struct FlussSourceFixture {
    container: FlussContainer,
}

impl FlussSourceFixture {
    /// Appends one row per payload and flushes, so the rows are readable once this returns.
    ///
    /// Bucket leadership is assigned shortly after the tablet server registers, so the first
    /// writes can still be rejected with `NotLeaderOrFollower`. The client retries internally
    /// but gives up before leadership settles, hence the retry here.
    pub async fn append_rows(&self, payloads: &[String]) -> Result<(), TestBinaryError> {
        let rows: Vec<GenericRow> = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| {
                let mut row = GenericRow::new(2);
                row.set_field(0, index as i32);
                row.set_field(1, payload.as_str());
                row
            })
            .collect();
        self.append(&rows).await
    }

    /// Starts a cluster and creates the table the connector reads, with the given schema.
    async fn start(schema: fn() -> fluss::error::Result<Schema>) -> Result<Self, TestBinaryError> {
        let fixture = Self {
            container: FlussContainer::start().await?,
        };
        fixture.create_table_when_ready(schema).await?;
        Ok(fixture)
    }

    async fn append(&self, rows: &[GenericRow<'_>]) -> Result<(), TestBinaryError> {
        let mut last_error = None;
        for _ in 0..READY_ATTEMPTS {
            match self.try_append(rows).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
            sleep(READY_RETRY_DELAY).await;
        }
        Err(last_error.unwrap_or_else(|| TestBinaryError::FixtureSetup {
            fixture_type: "FlussSourceFixture".to_string(),
            message: "Failed to append rows".to_string(),
        }))
    }

    /// Creates the database and an append-only log table.
    ///
    /// Runs during `setup()`, before the harness starts the connectors runtime, because the
    /// source connector resolves the table schema in `open()` and fails initialization when
    /// the table is missing.
    async fn create_table_when_ready(
        &self,
        schema: fn() -> fluss::error::Result<Schema>,
    ) -> Result<(), TestBinaryError> {
        let mut last_error = None;
        for _ in 0..READY_ATTEMPTS {
            match self.try_create_table(schema).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
            sleep(READY_RETRY_DELAY).await;
        }
        Err(last_error.unwrap_or_else(|| TestBinaryError::FixtureSetup {
            fixture_type: "FlussSourceFixture".to_string(),
            message: "Failed to create table".to_string(),
        }))
    }

    async fn try_create_table(
        &self,
        schema: fn() -> fluss::error::Result<Schema>,
    ) -> Result<(), TestBinaryError> {
        let connection = self.connect().await?;
        let admin = connection.get_admin().map_err(|error| self.error(error))?;
        admin
            .create_database(DATABASE, None, true)
            .await
            .map_err(|error| self.error(error))?;

        let schema = schema().map_err(|error| self.error(error))?;
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .build()
            .map_err(|error| self.error(error))?;
        admin
            .create_table(&Self::table_path(), &descriptor, true)
            .await
            .map_err(|error| self.error(error))?;
        Ok(())
    }

    async fn try_append(&self, rows: &[GenericRow<'_>]) -> Result<(), TestBinaryError> {
        let connection = self.connect().await?;
        let table = connection
            .get_table(&Self::table_path())
            .await
            .map_err(|error| self.error(error))?;
        let writer = table
            .new_append()
            .map_err(|error| self.error(error))?
            .create_writer()
            .map_err(|error| self.error(error))?;

        for row in rows {
            writer.append(row).map_err(|error| self.error(error))?;
        }
        writer.flush().await.map_err(|error| self.error(error))?;
        Ok(())
    }

    async fn connect(&self) -> Result<FlussConnection, TestBinaryError> {
        let config = Config {
            bootstrap_servers: self.container.bootstrap_servers.clone(),
            ..Config::default()
        };
        FlussConnection::new(config)
            .await
            .map_err(|error| self.error(error))
    }

    fn table_path() -> TablePath {
        TablePath::new(DATABASE, TABLE)
    }

    fn error(&self, error: fluss::error::Error) -> TestBinaryError {
        TestBinaryError::FixtureSetup {
            fixture_type: "FlussSourceFixture".to_string(),
            message: format!("Apache Fluss client failure: {error}"),
        }
    }
}

#[async_trait]
impl TestFixture for FlussSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::start(events_schema).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SOURCE_BOOTSTRAP_SERVERS.to_string(),
                self.container.bootstrap_servers.clone(),
            ),
            (ENV_SOURCE_DATABASE.to_string(), DATABASE.to_string()),
            (ENV_SOURCE_TABLE.to_string(), TABLE.to_string()),
            (
                ENV_SOURCE_POLL_INTERVAL.to_string(),
                POLL_INTERVAL.to_string(),
            ),
            (ENV_SOURCE_INCLUDE_METADATA.to_string(), "true".to_string()),
            (
                ENV_SOURCE_STREAMS_0_STREAM.to_string(),
                seeds::names::STREAM.to_string(),
            ),
            (
                ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
                seeds::names::TOPIC.to_string(),
            ),
            (ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
            (
                ENV_SOURCE_PATH.to_string(),
                "../../target/debug/libiggy_connector_fluss_source".to_string(),
            ),
        ])
    }
}

/// Polls slowly enough to leave time for restarting Apache Iggy before the SDK's
/// consecutive-NACK limit stops the source.
pub struct FlussSourceSlowPollFixture {
    inner: FlussSourceFixture,
}

impl Deref for FlussSourceSlowPollFixture {
    type Target = FlussSourceFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for FlussSourceSlowPollFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: FlussSourceFixture::setup().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(
            ENV_SOURCE_POLL_INTERVAL.to_string(),
            SLOW_POLL_INTERVAL.to_string(),
        );
        envs
    }
}

/// Starts from `latest` over a table that already holds rows, so none of those rows reaching
/// Apache Iggy shows the source began at the tail.
pub struct FlussSourceLatestFixture {
    inner: FlussSourceFixture,
    existing_payloads: Vec<String>,
}

impl FlussSourceLatestFixture {
    /// Rows appended before the connectors runtime started.
    pub fn existing_payloads(&self) -> &[String] {
        &self.existing_payloads
    }
}

impl Deref for FlussSourceLatestFixture {
    type Target = FlussSourceFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for FlussSourceLatestFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = FlussSourceFixture::setup().await?;
        let existing_payloads: Vec<String> = (0..EXISTING_ROW_COUNT)
            .map(|index| format!("before-start-{index}"))
            .collect();
        inner.append_rows(&existing_payloads).await?;
        Ok(Self {
            inner,
            existing_payloads,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SOURCE_STARTING_OFFSET.to_string(), "latest".to_string());
        envs
    }
}

/// A log table with a column of every scalar type the source maps. The rows come back
/// decoded from Arrow by a real server, which is the path production reads, rather than
/// from a row built in memory.
pub struct FlussSourceAllTypesFixture {
    inner: FlussSourceFixture,
}

impl FlussSourceAllTypesFixture {
    /// Appends one row with a value in every column, then one with every column null.
    pub async fn append_sample_rows(&self) -> Result<(), TestBinaryError> {
        let error = |error| self.inner.error(error);
        let mut filled = GenericRow::new(ALL_TYPES_COLUMN_COUNT);
        filled.set_field(0, true);
        filled.set_field(1, 7i8);
        filled.set_field(2, 300i16);
        filled.set_field(3, 70_000i32);
        filled.set_field(4, 9_000_000_000i64);
        filled.set_field(5, 1.5f32);
        filled.set_field(6, 2.25f64);
        filled.set_field(7, "abcde");
        filled.set_field(8, "hello");
        filled.set_field(
            9,
            Decimal::from_unscaled_long(12_345, 10, 2).map_err(error)?,
        );
        filled.set_field(10, Date::new(19_782));
        filled.set_field(11, Time::new(45_296_789));
        filled.set_field(
            12,
            TimestampNtz::from_millis_nanos(1_700_000_000_123, 456_000).map_err(error)?,
        );
        filled.set_field(
            13,
            TimestampNtz::from_millis_nanos(1_700_000_000_123, 456_789).map_err(error)?,
        );
        filled.set_field(
            14,
            TimestampLtz::from_millis_nanos(1_700_000_000_123, 456_000).map_err(error)?,
        );
        filled.set_field(15, [1u8, 2, 3].as_slice());
        filled.set_field(16, [4u8, 5, 6].as_slice());

        let empty = GenericRow::new(ALL_TYPES_COLUMN_COUNT);
        self.inner.append(&[filled, empty]).await
    }
}

#[async_trait]
impl TestFixture for FlussSourceAllTypesFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: FlussSourceFixture::start(all_types_schema).await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

fn events_schema() -> fluss::error::Result<Schema> {
    Schema::builder()
        .column("id", DataTypes::int())
        .column("payload", DataTypes::string())
        .build()
}

/// Column order matches the positions `append_sample_rows` writes.
fn all_types_schema() -> fluss::error::Result<Schema> {
    Schema::builder()
        .column("bool_col", DataTypes::boolean())
        .column("tinyint_col", DataTypes::tinyint())
        .column("smallint_col", DataTypes::smallint())
        .column("int_col", DataTypes::int())
        .column("bigint_col", DataTypes::bigint())
        .column("float_col", DataTypes::float())
        .column("double_col", DataTypes::double())
        .column("char_col", DataTypes::char(5))
        .column("string_col", DataTypes::string())
        .column("decimal_col", DataTypes::decimal(10, 2))
        .column("date_col", DataTypes::date())
        .column("time_col", DataTypes::time_with_precision(3))
        .column("timestamp_col", DataTypes::timestamp())
        .column(
            "timestamp_nanos_col",
            DataTypes::timestamp_with_precision(9),
        )
        .column("timestamp_ltz_col", DataTypes::timestamp_ltz())
        .column("bytes_col", DataTypes::bytes())
        .column("binary_col", DataTypes::binary(3))
        .build()
}
