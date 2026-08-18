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
use tiberius::{Client, Config, Query, Row};
use tokio::net::TcpStream;

use crate::connectors::fixtures;
use testcontainers_modules::{
    mssql_server,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};

pub type DBClient = Client<Compat<TcpStream>>;

pub(super) const MSSQL_PORT: u16 = 5432;
pub(super) const DEFAULT_POLL_ATTEMPTS: usize = 100;
pub(super) const DEFAULT_POLL_INTERVAL_MS: u64 = 50;

pub(super) const ENV_SOURCE_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_CONNECTION_STRING";
pub(super) const ENV_SOURCE_TABLES: &str = "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_TABLES";
pub(super) const ENV_SOURCE_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_STREAMS_0_STREAM";
pub(super) const ENV_SOURCE_STREAMS_0_TOPIC: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_STREAMS_0_TOPIC";
pub(super) const ENV_SOURCE_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_STREAMS_0_SCHEMA";
pub(super) const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_POLL_INTERVAL";
pub(super) const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_MSSQL_PATH";
pub(super) const ENV_SOURCE_PAYLOAD_COLUMN: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_PAYLOAD_COLUMN";
pub(super) const ENV_SOURCE_PAYLOAD_FORMAT: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_PAYLOAD_FORMAT";
pub(super) const ENV_SOURCE_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_INCLUDE_METADATA";

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";


/// Trait for MSSQL fixtures with common container operations.
pub trait MSSQLOps: Sync {
    fn container(&self) -> &MSSQLContainer;
}

/// Extension of `MSSQLOps` for source fixtures that operate on a specific table.
pub trait MSSQLSourceOps: MSSQLOps {
    fn table_name(&self) -> &str;

    async fn count_rows<'a>(
        &'a self,
        client: &'a DBClient,
    ) -> impl std::future::Future<Output = i64> + Send + 'a {
        async move {
            let query = Query::new(format!("SELECT COUNT(*) as c FROM {}", self.table_name()));
            let count = query
				.query(client)
                .await
				.unwrap_or_else(|e| panic!("Failed to retrieve rows: {e}"))
				.into_first_result()
                .await
				.unwrap_or_else(|e| panic!("Failed to count rows: {e}"));
            count[0].try_get::<i64, _>("c");
        }
    }
}

/// Base container management for MSSQL fixtures.
pub struct MSSQLContainer {
    #[allow(dead_code)]
    container: ContainerAsync<mssql_server::MssqlServer>,
    pub(super) connection_string: String,
}

impl MSSQLContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = mssql_server::MssqlServer::default()
            .with_container_name(fixtures::unique_container_name("mssql"))
			.with_accept_eula()
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MSSQLContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(MSSQL_PORT)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "MSSQLContainer".to_string(),
                message: format!("Failed to get port: {e}"),
            })?;

        let connection_string = format!(
			"Server=tcp:{},{};Database=test;User Id=sa;Password=yourStrong(!)Password;TrustServerCertificate=True;",
			mssql_server.get_host().unwrap(),
			mssql_server.get_host_port_ipv4(1433).unwrap()
		);

        Ok(Self {
            container,
            connection_string,
        })
    }
}
