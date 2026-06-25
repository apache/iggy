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

use integration::harness::TestBinaryError;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};

use crate::connectors::fixtures;
use testcontainers_modules::{
    mysql,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};

pub(super) const MYSQL_PORT: u16 = 3306;

pub(super) const ENV_SOURCE_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_CONNECTION_STRING";
pub(super) const ENV_SOURCE_TABLES: &str = "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_TABLES";
pub(super) const ENV_SOURCE_TRACKING_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_TRACKING_COLUMN";
pub(super) const ENV_SOURCE_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_STREAMS_0_STREAM";
pub(super) const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_MYSQL_STREAMS_0_TOPIC";
pub(super) const ENV_SOURCE_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_STREAMS_0_SCHEMA";
pub(super) const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_POLL_INTERVAL";
pub(super) const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_MYSQL_PATH";
pub(super) const ENV_SOURCE_PAYLOAD_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_PAYLOAD_COLUMN";
pub(super) const ENV_SOURCE_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub(super) const ENV_SOURCE_DELETE_AFTER_READ: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_DELETE_AFTER_READ";
pub(super) const ENV_SOURCE_PRIMARY_KEY_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_PRIMARY_KEY_COLUMN";
pub(super) const ENV_SOURCE_PROCESSED_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_PROCESSED_COLUMN";
pub(super) const ENV_SOURCE_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SOURCE_MYSQL_PLUGIN_CONFIG_INCLUDE_METADATA";

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";

pub(super) const ENV_SOURCE_PLUGIN_PATH: &str = "../../target/debug/libiggy_connector_mysql_source";

pub trait MySqlOps: Sync {
    fn container(&self) -> &MySqlContainer;

    fn create_pool(
        &self,
    ) -> impl std::future::Future<Output = Result<Pool<MySql>, TestBinaryError>> + Send {
        self.container().create_pool()
    }
}

pub trait MySqlSourceOps: MySqlOps {
    fn table_name(&self) -> &str;

    fn count_rows<'a>(
        &'a self,
        pool: &'a Pool<MySql>,
    ) -> impl std::future::Future<Output = i64> + Send + 'a {
        async move {
            let query = format!("SELECT COUNT(*) FROM `{}`", self.table_name());
            let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(query))
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
            count.0
        }
    }
}

pub struct MySqlContainer {
    #[allow(dead_code)]
    container: ContainerAsync<mysql::Mysql>,
    pub(super) connection_string: String,
}

impl MySqlContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = mysql::Mysql::default()
            .with_container_name(fixtures::unique_container_name("mysql"))
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MySqlContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(MYSQL_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MySqlContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        let connection_string = format!("mysql://root@localhost:{host_port}/test");

        Ok(Self {
            container,
            connection_string,
        })
    }

    pub async fn create_pool(&self) -> Result<Pool<MySql>, TestBinaryError> {
        MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&self.connection_string)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MySqlContainer".to_string(),
                message: format!("Failed to connect: {e}"),
            })
    }
}
