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

use std::{collections::HashMap, time::Duration};

use crate::connectors::fixtures::fluss::fixture_error;

use super::cluster::FlussCluster;
use async_trait::async_trait;
use fluss::row::ColumnarRow;
use fluss::{
    client::{EARLIEST_OFFSET, FlussConnection},
    metadata::{TableInfo, TablePath},
};
use integration::harness::{TestBinaryError, TestFixture, seeds};
use tokio::time::{Instant, timeout_at};

const DEFAULT_FLUSS_VERSION: &str = "0.9.1-incubating";
const DEFAULT_SINK_DB: &str = "fluss";
const DEFAULT_SINK_TABLE: &str = "iggy_messages";

const ENV_SINK_BOOTSTRAP_SERVERS: &str =
    "IGGY_CONNECTORS_SINK_FLUSS_PLUGIN_CONFIG_BOOTSTRAP_SERVERS";
const ENV_SINK_TARGET_TABLE: &str = "IGGY_CONNECTORS_SINK_FLUSS_PLUGIN_CONFIG_TARGET_TABLE";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_CONSUMER_GROUP";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_FLUSS_PATH";

fn create_test_table_path() -> TablePath {
    TablePath::new(DEFAULT_SINK_DB, DEFAULT_SINK_TABLE)
}

pub struct FlussSinkFixture {
    cluster: FlussCluster,
}

impl FlussSinkFixture {
    pub async fn get_fluss_connection(&self) -> Result<FlussConnection, TestBinaryError> {
        self.cluster.get_connection().await
    }

    pub async fn get_test_table(&self) -> Result<TableInfo, TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let admin = connection
            .get_admin()
            .map_err(|error| fixture_error(format!("Failed to get Fluss admin: {error}")))?;

        admin
            .get_table_info(&create_test_table_path())
            .await
            .map_err(|error| fixture_error(format!("Failed to get Fluss test table: {error}")))
    }

    pub async fn read_from_test_table(
        &self,
        timeout: u64,
    ) -> Result<Vec<ColumnarRow>, TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let table_path = create_test_table_path();
        let table = connection
            .get_table(&table_path)
            .await
            .map_err(|e| fixture_error(format!("Failed to get table: {}", e)))?;

        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .map_err(|e| fixture_error(format!("Failed to create log scanner: {}", e)))?;

        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .map_err(|e| fixture_error(format!("Failed to subscribe to log scanner: {}", e)))?;

        let deadline = Instant::now() + Duration::from_secs(timeout);

        let mut rows: Vec<ColumnarRow> = Vec::new();

        loop {
            let records = match timeout_at(deadline, log_scanner.poll(Duration::from_secs(5))).await
            {
                Ok(Err(e)) => {
                    return Err(fixture_error(format!("Failed to poll log scanner: {}", e)));
                }
                Ok(Ok(records)) => records,
                Err(_) => break,
            };

            for record in records {
                rows.push(record.row);
            }
        }

        Ok(rows)
    }

    pub async fn check_if_test_table_exists(&self) -> Result<bool, TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let admin = connection
            .get_admin()
            .map_err(|e| fixture_error(format!("Error getting Fluss admin instance {}", e)))?;
        let exists = admin
            .table_exists(&create_test_table_path())
            .await
            .map_err(|e| fixture_error(format!("Error checking if table exists {}", e)))?;
        Ok(exists)
    }

    pub async fn wait_for_test_table(&self, timeout: u64) -> Result<(), TestBinaryError> {
        let deadline = Instant::now() + Duration::from_secs(timeout);
        loop {
            let timeout_at = timeout_at(deadline, self.check_if_test_table_exists()).await;
            match timeout_at {
                Ok(Ok(false)) => {}
                Ok(Ok(true)) => return Ok(()),
                Ok(Err(e)) => {
                    return Err(fixture_error(
                        format!("Checking the table has failed with: {}", e).to_string(),
                    ));
                }
                Err(_) => {
                    return Err(fixture_error(
                        format!(
                            "Test table was not created within the timeout of {}s",
                            timeout
                        )
                        .to_string(),
                    ));
                }
            };

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[async_trait]
impl TestFixture for FlussSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let cluster = FlussCluster::new(DEFAULT_FLUSS_VERSION).await?;
        Ok(Self { cluster })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SINK_BOOTSTRAP_SERVERS.to_string(),
                self.cluster.coordinator_address.clone(),
            ),
            (
                ENV_SINK_TARGET_TABLE.to_string(),
                DEFAULT_SINK_TABLE.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_STREAM.to_string(),
                seeds::names::STREAM.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_TOPICS.to_string(),
                format!("[{}]", seeds::names::TOPIC),
            ),
            (ENV_SINK_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
            (
                ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
                seeds::names::CONSUMER_GROUP.to_string(),
            ),
            (
                ENV_SINK_PATH.to_string(),
                "../../target/debug/libiggy_connector_fluss_sink".to_string(),
            ),
        ])
    }
}
