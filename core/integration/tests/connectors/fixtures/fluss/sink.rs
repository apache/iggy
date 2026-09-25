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
use fluss::row::{ColumnarRow, DataGetters, GenericRow};
use fluss::{
    client::{EARLIEST_OFFSET, FlussConnection},
    metadata::{DataTypes, Schema, TableDescriptor, TableInfo, TablePath},
};
use integration::harness::{TestBinaryError, TestFixture, seeds};
use tokio::time::{Instant, timeout_at};

const DEFAULT_FLUSS_VERSION: &str = "1.0.0";
const DEFAULT_SINK_DB: &str = "fluss";
const DEFAULT_SINK_TABLE: &str = "iggy_messages";
const PARTITIONED_SINK_TABLE: &str = "partitioned_iggy_messages";
const PRIMARY_KEY_SINK_TABLE: &str = "primary_key_events";
const TABLE_CREATION_TIMEOUT_S: u64 = 10;

const ENV_SINK_BOOTSTRAP_SERVERS: &str =
    "IGGY_CONNECTORS_SINK_FLUSS_PLUGIN_CONFIG_BOOTSTRAP_SERVERS";
const ENV_SINK_ROUTER_TYPE: &str = "IGGY_CONNECTORS_SINK_FLUSS_PLUGIN_CONFIG_ROUTER_TYPE";
const ENV_SINK_TARGET_TABLE: &str = "IGGY_CONNECTORS_SINK_FLUSS_PLUGIN_CONFIG_TARGET_TABLE";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_FLUSS_STREAMS_0_CONSUMER_GROUP";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_FLUSS_PATH";

fn create_test_table_path(table_name: &str) -> TablePath {
    TablePath::new(DEFAULT_SINK_DB, table_name)
}

pub struct FlussSinkFixture {
    cluster: FlussCluster,
    router_type: &'static str,
    target_table: &'static str,
}

impl FlussSinkFixture {
    async fn setup_with(
        router_type: &'static str,
        target_table: &'static str,
    ) -> Result<Self, TestBinaryError> {
        let cluster = FlussCluster::new(DEFAULT_FLUSS_VERSION).await?;
        Ok(Self {
            cluster,
            router_type,
            target_table,
        })
    }

    pub fn test_table_path(&self) -> TablePath {
        create_test_table_path(self.target_table)
    }

    pub async fn get_fluss_connection(&self) -> Result<FlussConnection, TestBinaryError> {
        self.cluster.get_connection().await
    }

    pub async fn get_test_table(&self) -> Result<TableInfo, TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let admin = connection
            .get_admin()
            .map_err(|error| fixture_error(format!("Failed to get Fluss admin: {error}")))?;

        admin
            .get_table_info(&self.test_table_path())
            .await
            .map_err(|error| fixture_error(format!("Failed to get Fluss test table: {error}")))
    }

    pub async fn read_from_test_table(
        &self,
        expected_row_count: usize,
        timeout: u64,
    ) -> Result<Vec<ColumnarRow>, TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let table_path = self.test_table_path();
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

        let mut rows = Vec::with_capacity(expected_row_count);

        loop {
            let records = match timeout_at(deadline, log_scanner.poll(Duration::from_secs(5))).await
            {
                Ok(Err(e)) => {
                    return Err(fixture_error(format!("Failed to poll log scanner: {}", e)));
                }
                Ok(Ok(records)) => records,
                Err(_) => break,
            };

            rows.extend(records.into_iter().map(|record| record.row));
            if rows.len() >= expected_row_count {
                return Ok(rows);
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
            .table_exists(&self.test_table_path())
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
        Self::setup_with("static", DEFAULT_SINK_TABLE).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SINK_BOOTSTRAP_SERVERS.to_string(),
                self.cluster.coordinator_address.clone(),
            ),
            (
                ENV_SINK_TARGET_TABLE.to_string(),
                self.target_table.to_string(),
            ),
            (
                ENV_SINK_ROUTER_TYPE.to_string(),
                self.router_type.to_string(),
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

pub struct FlussMultiSinkFixture(FlussSinkFixture);

impl std::ops::Deref for FlussMultiSinkFixture {
    type Target = FlussSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl TestFixture for FlussMultiSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        FlussSinkFixture::setup_with("multi", DEFAULT_SINK_TABLE)
            .await
            .map(Self)
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.0.connectors_runtime_envs()
    }
}

pub struct FlussPrimaryKeySinkFixture(FlussSinkFixture);

impl std::ops::Deref for FlussPrimaryKeySinkFixture {
    type Target = FlussSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FlussPrimaryKeySinkFixture {
    async fn create_test_table(&self) -> Result<(), TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let admin = connection
            .get_admin()
            .map_err(|error| fixture_error(format!("Failed to get Fluss admin: {error}")))?;
        let schema = Schema::builder()
            .column("id", DataTypes::bigint())
            .column("name", DataTypes::string())
            .column("op", DataTypes::string())
            .primary_key(vec!["id"])
            .map_err(|error| {
                fixture_error(format!(
                    "Failed to configure primary-key table schema: {error}"
                ))
            })?
            .build()
            .map_err(|error| {
                fixture_error(format!("Failed to build primary-key table schema: {error}"))
            })?;
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec!["id".to_string()])
            .build()
            .map_err(|error| {
                fixture_error(format!(
                    "Failed to build primary-key table descriptor: {error}"
                ))
            })?;

        let deadline = Instant::now() + Duration::from_secs(TABLE_CREATION_TIMEOUT_S);
        loop {
            match admin
                .create_table(&self.test_table_path(), &descriptor, true)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if Instant::now() >= deadline => {
                    return Err(fixture_error(format!(
                        "Failed to create primary-key test table: {error}"
                    )));
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    pub async fn lookup_test_value(&self, id: i64) -> Result<Option<String>, TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let table = connection
            .get_table(&self.test_table_path())
            .await
            .map_err(|error| fixture_error(format!("Failed to get primary-key table: {error}")))?;
        let mut lookuper = table
            .new_lookup()
            .map_err(|error| fixture_error(format!("Failed to create table lookup: {error}")))?
            .create_lookuper()
            .map_err(|error| fixture_error(format!("Failed to create table lookuper: {error}")))?;
        let mut key = GenericRow::new(3);
        key.set_field(0, id);
        let result = lookuper
            .lookup(&key)
            .await
            .map_err(|error| fixture_error(format!("Failed to look up test row: {error}")))?;
        let row = result
            .get_single_row()
            .map_err(|error| fixture_error(format!("Failed to read lookup result: {error}")))?;

        row.map(|row| {
            row.get_string(1)
                .map(str::to_owned)
                .map_err(|error| fixture_error(format!("Failed to read test value: {error}")))
        })
        .transpose()
    }
}

#[async_trait]
impl TestFixture for FlussPrimaryKeySinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let fixture = FlussSinkFixture::setup_with("multi", PRIMARY_KEY_SINK_TABLE)
            .await
            .map(Self)?;
        fixture.create_test_table().await?;
        Ok(fixture)
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.0.connectors_runtime_envs()
    }
}

pub struct FlussPartitionedSinkFixture(FlussSinkFixture);

impl std::ops::Deref for FlussPartitionedSinkFixture {
    type Target = FlussSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FlussPartitionedSinkFixture {
    pub async fn read_from_partitioned_test_table(
        &self,
        expected_row_count: usize,
        timeout: u64,
    ) -> Result<Vec<ColumnarRow>, TestBinaryError> {
        let connection = self.get_fluss_connection().await?;
        let table_path = self.test_table_path();
        let admin = connection
            .get_admin()
            .map_err(|error| fixture_error(format!("Failed to get Fluss admin: {error}")))?;
        let table_info = admin
            .get_table_info(&table_path)
            .await
            .map_err(|error| fixture_error(format!("Failed to get Fluss test table: {error}")))?;
        let partition_infos = admin
            .list_partition_infos(&table_path)
            .await
            .map_err(|error| fixture_error(format!("Failed to list test partitions: {error}")))?;
        let table = connection
            .get_table(&table_path)
            .await
            .map_err(|error| fixture_error(format!("Failed to get table: {error}")))?;
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .map_err(|error| fixture_error(format!("Failed to create log scanner: {error}")))?;

        for partition_info in partition_infos {
            for bucket in 0..table_info.get_num_buckets() {
                log_scanner
                    .subscribe_partition(partition_info.get_partition_id(), bucket, EARLIEST_OFFSET)
                    .await
                    .map_err(|error| {
                        fixture_error(format!(
                            "Failed to subscribe to partitioned log scanner: {error}"
                        ))
                    })?;
            }
        }

        let deadline = Instant::now() + Duration::from_secs(timeout);
        let mut rows = Vec::with_capacity(expected_row_count);
        loop {
            let records = match timeout_at(deadline, log_scanner.poll(Duration::from_secs(5))).await
            {
                Ok(Err(error)) => {
                    return Err(fixture_error(format!(
                        "Failed to poll partitioned log scanner: {error}"
                    )));
                }
                Ok(Ok(records)) => records,
                Err(_) => break,
            };

            rows.extend(records.into_iter().map(|record| record.row));
            if rows.len() >= expected_row_count {
                return Ok(rows);
            }
        }

        Ok(rows)
    }
}

#[async_trait]
impl TestFixture for FlussPartitionedSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        FlussSinkFixture::setup_with("multi", PARTITIONED_SINK_TABLE)
            .await
            .map(Self)
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.0.connectors_runtime_envs()
    }
}
