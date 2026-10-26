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
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt, ReuseDirective,
};
use tiberius::{AuthMethod, Client, Config, Row};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::info;

const MSSQL_IMAGE: &str = "mcr.microsoft.com/mssql/server";
const MSSQL_TAG: &str = "2022-CU27-ubuntu-22.04";
const MSSQL_PORT: u16 = 1433;
// A fixed name with ReuseDirective::Always shares one slow-starting 2 GB container. Each test gets a database.
pub(super) const MSSQL_CONTAINER_NAME: &str = "iggy-test-mssql";
pub(super) const SA_PASSWORD: &str = "Iggy_Test_Passw0rd!";
const CONTAINER_START_ATTEMPTS: u32 = 30;
const CONTAINER_START_RETRY_DELAY: Duration = Duration::from_secs(1);
const READY_TIMEOUT: Duration = Duration::from_secs(300);
const READY_INTERVAL: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_TEST_TOPIC: &str = "test_topic";

pub(super) const ENV_SOURCE_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_CONNECTION_STRING";
pub(super) const ENV_SOURCE_TABLES: &str = "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_TABLES";
pub(super) const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_POLL_INTERVAL";
pub(super) const ENV_SOURCE_VERBOSE_LOGGING: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_VERBOSE_LOGGING";
pub(super) const ENV_SOURCE_START_POSITION: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_START_POSITION";
pub(super) const ENV_SOURCE_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_BATCH_SIZE";
pub(super) const ENV_SOURCE_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_STREAMS_0_STREAM";
pub(super) const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_MSSQL_STREAMS_0_TOPIC";
pub(super) const ENV_SOURCE_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SOURCE_MSSQL_STREAMS_0_SCHEMA";
pub(super) const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_MSSQL_PATH";

pub(super) type SqlClient = Client<Compat<TcpStream>>;

pub(super) struct MssqlContainer {
    // Held so testcontainers' Drop runs on test exit. ReuseDirective::Always leaves the container running.
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) port: u16,
}

impl MssqlContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = start_shared_container().await?;
        let port = container
            .get_host_port_ipv4(MSSQL_PORT)
            .await
            .map_err(|error| setup_error(format!("Failed to get SQL Server port: {error}")))?;
        let started = Self { container, port };
        started.wait_until_ready().await?;
        Ok(started)
    }

    pub(super) fn connection_string(&self, database: &str, password: Option<&str>) -> String {
        format!(
            "Server=tcp:127.0.0.1,{};Database={database};User Id=sa;Password={};TrustServerCertificate=true",
            self.port,
            password.unwrap_or(SA_PASSWORD)
        )
    }

    pub(super) async fn connect(&self, database: &str) -> Result<SqlClient, TestBinaryError> {
        connect_to(self.port, database).await
    }

    async fn wait_until_ready(&self) -> Result<(), TestBinaryError> {
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut last_error = String::new();
        while Instant::now() < deadline {
            match self.connect("master").await {
                Ok(mut client) => {
                    match timeout(CONNECT_TIMEOUT, execute(&mut client, "SELECT 1")).await {
                        Ok(Ok(_)) => return Ok(()),
                        Ok(Err(error)) => last_error = error.to_string(),
                        Err(_) => last_error = "SELECT 1 timed out".to_string(),
                    }
                }
                Err(error) => last_error = error.to_string(),
            }
            sleep(READY_INTERVAL).await;
        }
        Err(setup_error(format!(
            "SQL Server was not ready after {READY_TIMEOUT:?}: {last_error}"
        )))
    }
}

pub(super) async fn connect_to(port: u16, database: &str) -> Result<SqlClient, TestBinaryError> {
    // A stalled login would otherwise hang readiness and teardown.
    timeout(CONNECT_TIMEOUT, login(port, database))
        .await
        .map_err(|_| {
            setup_error(format!(
                "SQL Server login timed out after {CONNECT_TIMEOUT:?}"
            ))
        })?
}

async fn login(port: u16, database: &str) -> Result<SqlClient, TestBinaryError> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server("sa", SA_PASSWORD));
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|error| setup_error(format!("Failed to reach SQL Server: {error}")))?;
    tcp.set_nodelay(true)
        .map_err(|error| setup_error(format!("Failed to set TCP_NODELAY: {error}")))?;
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|error| setup_error(format!("Failed to log in to SQL Server: {error}")))
}

/// Runs a batch and returns the rows of every result set.
pub(super) async fn execute(
    client: &mut SqlClient,
    sql: &str,
) -> Result<Vec<Row>, tiberius::error::Error> {
    let results = client.simple_query(sql).await?.into_results().await?;
    Ok(results.into_iter().flatten().collect())
}

/// Creates the shared container, or attaches to it if another test won the create race.
async fn start_shared_container() -> Result<ContainerAsync<GenericImage>, TestBinaryError> {
    let mut conflict = String::new();
    for attempt in 1..=CONTAINER_START_ATTEMPTS {
        let result = GenericImage::new(MSSQL_IMAGE, MSSQL_TAG)
            .with_exposed_port(MSSQL_PORT.tcp())
            .with_wait_for(WaitFor::Nothing)
            .with_env_var("ACCEPT_EULA", "Y")
            .with_env_var("MSSQL_SA_PASSWORD", SA_PASSWORD)
            // Without the Agent the capture job never runs and CDC records nothing.
            .with_env_var("MSSQL_AGENT_ENABLED", "true")
            .with_mapped_port(0, MSSQL_PORT.tcp())
            .with_container_name(MSSQL_CONTAINER_NAME)
            .with_reuse(ReuseDirective::Always)
            .start()
            .await;
        match result {
            Ok(container) => return Ok(container),
            Err(error) => {
                let message = error.to_string();
                if !message.contains("is already in use") {
                    return Err(setup_error(format!(
                        "Failed to start SQL Server container: {message}"
                    )));
                }
                info!("SQL Server container name taken (attempt {attempt}), retrying to attach");
                conflict = message;
                sleep(CONTAINER_START_RETRY_DELAY).await;
            }
        }
    }
    Err(setup_error(format!(
        "Failed to attach to container '{MSSQL_CONTAINER_NAME}' after {CONTAINER_START_ATTEMPTS} attempts: {conflict}"
    )))
}

pub(super) fn setup_error(message: String) -> TestBinaryError {
    TestBinaryError::FixtureSetup {
        fixture_type: "MssqlContainer".to_string(),
        message,
    }
}
