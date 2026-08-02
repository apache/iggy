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
    ENV_SOURCE_PATH, ENV_SOURCE_POLL_INTERVAL, ENV_SOURCE_STREAMS_0_SCHEMA,
    ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TABLE, FlussContainer,
};
use async_trait::async_trait;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use integration::harness::seeds;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

const DATABASE: &str = "iggy_test";
const TABLE: &str = "events";
const POLL_INTERVAL: &str = "100ms";
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
        let mut last_error = None;
        for _ in 0..READY_ATTEMPTS {
            match self.try_append_rows(payloads).await {
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

    /// Creates the database and an append-only log table with an `id` and a `payload` column.
    ///
    /// Runs during `setup()`, before the harness starts the connectors runtime, because the
    /// source connector resolves the table schema in `open()` and fails initialization when
    /// the table is missing.
    async fn create_table_when_ready(&self) -> Result<(), TestBinaryError> {
        let mut last_error = None;
        for _ in 0..READY_ATTEMPTS {
            match self.try_create_table().await {
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

    async fn try_create_table(&self) -> Result<(), TestBinaryError> {
        let connection = self.connect().await?;
        let admin = connection.get_admin().map_err(|error| self.error(error))?;
        admin
            .create_database(DATABASE, None, true)
            .await
            .map_err(|error| self.error(error))?;

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("payload", DataTypes::string())
            .build()
            .map_err(|error| self.error(error))?;
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

    async fn try_append_rows(&self, payloads: &[String]) -> Result<(), TestBinaryError> {
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

        for (index, payload) in payloads.iter().enumerate() {
            let mut row = GenericRow::new(2);
            row.set_field(0, index as i32);
            row.set_field(1, payload.as_str());
            writer.append(&row).map_err(|error| self.error(error))?;
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
        let fixture = Self {
            container: FlussContainer::start().await?,
        };
        fixture.create_table_when_ready().await?;
        Ok(fixture)
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
