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
use std::io::Cursor;
use std::time::Duration;

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture, seeds};
use sha2::{Digest, Sha256};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Pool, Postgres};
use testcontainers_modules::testcontainers::core::wait::LogWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::{Instant, sleep};
use zip::ZipArchive;

use super::postgres::PostgresContainer;
use super::unique_container_name;

const POSTGRES_DRIVER_CLASS_ENTRY: &str = "org/postgresql/Driver.class";
const POSTGRES_DRIVER_SHA256: &str =
    "49bba9c3200d4f64ae73903d56ce1bd09c74517dfe31acb44745506b4fcede53";
const MYSQL_DRIVER_CLASS_ENTRY: &str = "com/mysql/cj/jdbc/Driver.class";
const MYSQL_DRIVER_SHA256: &str =
    "e2a3b2fc726a1ac64e998585db86b30fa8bf3f706195b78bb77c5f99bf877bd9";
const MYSQL_IMAGE: &str = "mysql";
const MYSQL_TAG: &str = "8.0.33";
const MYSQL_PORT: u16 = 3306;
const MYSQL_DATABASE: &str = "iggy";
const MYSQL_USERNAME: &str = "iggy";
const MYSQL_PASSWORD: &str = "iggy_password";

const ENV_JDBC_URL: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_JDBC_URL";
const ENV_DRIVER_CLASS: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_DRIVER_CLASS";
const ENV_DRIVER_JAR_PATH: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_DRIVER_JAR_PATH";
const ENV_USERNAME: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_USERNAME";
const ENV_PASSWORD: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_PASSWORD";
const ENV_QUERY: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_QUERY";
const ENV_POLL_INTERVAL: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_POLL_INTERVAL";
const ENV_BATCH_SIZE: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_BATCH_SIZE";
const ENV_MODE: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_MODE";
const ENV_TRACKING_COLUMN: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_TRACKING_COLUMN";
const ENV_INITIAL_OFFSET: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_INITIAL_OFFSET";
const ENV_INCLUDE_METADATA: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_INCLUDE_METADATA";
const ENV_QUERY_TIMEOUT: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_PLUGIN_CONFIG_QUERY_TIMEOUT_MS";
const ENV_STREAM: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_STREAMS_0_STREAM";
const ENV_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_STREAMS_0_TOPIC";
const ENV_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_JDBC_PG_STREAMS_0_SCHEMA";

const ENV_MYSQL_JDBC_URL: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_JDBC_URL";
const ENV_MYSQL_DRIVER_CLASS: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_DRIVER_CLASS";
const ENV_MYSQL_DRIVER_JAR_PATH: &str =
    "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_DRIVER_JAR_PATH";
const ENV_MYSQL_USERNAME: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_USERNAME";
const ENV_MYSQL_PASSWORD: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_PASSWORD";
const ENV_MYSQL_QUERY: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_QUERY";
const ENV_MYSQL_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_POLL_INTERVAL";
const ENV_MYSQL_BATCH_SIZE: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_PLUGIN_CONFIG_BATCH_SIZE";
const ENV_MYSQL_STREAM: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_STREAMS_0_STREAM";
const ENV_MYSQL_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_STREAMS_0_TOPIC";
const ENV_MYSQL_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_JDBC_MYSQL_STREAMS_0_SCHEMA";

#[derive(Clone, Copy)]
enum Scenario {
    Empty,
    Incremental,
    TextCursor,
    LargeResult,
    BulkOverflow,
    TieBoundary,
    NumericTieBoundary,
}

struct JdbcPostgresFixture {
    container: PostgresContainer,
    jdbc_url: String,
    driver_jar_path: String,
}

impl JdbcPostgresFixture {
    async fn setup(scenario: Scenario) -> Result<Self, TestBinaryError> {
        let container = PostgresContainer::start().await?;
        let host_and_port = container
            .connection_string()
            .rsplit('@')
            .next()
            .ok_or_else(|| fixture_error("PostgreSQL connection string has no host"))?;
        let fixture = Self {
            jdbc_url: format!("jdbc:postgresql://{host_and_port}/postgres"),
            driver_jar_path: postgres_driver_jar().await?,
            container,
        };
        fixture.seed_database(scenario).await?;
        Ok(fixture)
    }

    async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.container.create_pool().await
    }

    async fn seed_database(&self, scenario: Scenario) -> Result<(), TestBinaryError> {
        let pool = self.create_pool().await?;
        let statements: &[&str] = match scenario {
            Scenario::Empty => &[],
            Scenario::Incremental => &[
                "CREATE TABLE inc_test (id INT PRIMARY KEY, name TEXT)",
                "INSERT INTO inc_test (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            ],
            Scenario::TextCursor => &[
                "CREATE TABLE text_cursor_test (id INT PRIMARY KEY, cursor_value TEXT UNIQUE NOT NULL)",
                r"INSERT INTO text_cursor_test (id, cursor_value) VALUES (1, E'a\\b'), (2, 'z')",
            ],
            Scenario::LargeResult => &[
                "CREATE TABLE big_test (id INT PRIMARY KEY, name TEXT, val NUMERIC(12,2))",
                "INSERT INTO big_test (id, name, val) SELECT g, 'row_' || g, (g * 1.5)::numeric(12,2) FROM generate_series(1, 300) g",
            ],
            Scenario::BulkOverflow => &[
                "CREATE TABLE trunc_test (id INT PRIMARY KEY)",
                "INSERT INTO trunc_test (id) SELECT generate_series(1, 5)",
            ],
            Scenario::TieBoundary => &[
                "CREATE TABLE tie_test (id INT PRIMARY KEY, position INT NOT NULL)",
                "INSERT INTO tie_test (id, position) VALUES (1, 1), (2, 2), (3, 2), (4, 3)",
            ],
            Scenario::NumericTieBoundary => &[
                "CREATE TABLE numeric_tie_test (id INT PRIMARY KEY, position NUMERIC NOT NULL)",
                "INSERT INTO numeric_tie_test (id, position) VALUES (1, 1), (2, 2.0), (3, 2.00), (4, 3)",
            ],
        };
        for statement in statements {
            sqlx::query(*statement)
                .execute(&pool)
                .await
                .map_err(|error| fixture_error(format!("failed to seed PostgreSQL: {error}")))?;
        }
        pool.close().await;
        Ok(())
    }

    fn envs(
        &self,
        query: &str,
        mode: &str,
        batch_size: u32,
        tracking_column: &str,
        initial_offset: Option<&str>,
    ) -> HashMap<String, String> {
        let mut envs = HashMap::from([
            (ENV_JDBC_URL.to_string(), self.jdbc_url.clone()),
            (
                ENV_DRIVER_CLASS.to_string(),
                "org.postgresql.Driver".to_string(),
            ),
            (
                ENV_DRIVER_JAR_PATH.to_string(),
                self.driver_jar_path.clone(),
            ),
            (ENV_USERNAME.to_string(), "postgres".to_string()),
            (ENV_PASSWORD.to_string(), "postgres".to_string()),
            (ENV_QUERY.to_string(), query.to_string()),
            (ENV_POLL_INTERVAL.to_string(), "100ms".to_string()),
            (ENV_BATCH_SIZE.to_string(), batch_size.to_string()),
            (ENV_MODE.to_string(), mode.to_string()),
            (ENV_TRACKING_COLUMN.to_string(), tracking_column.to_string()),
            (ENV_INCLUDE_METADATA.to_string(), "true".to_string()),
            (ENV_STREAM.to_string(), seeds::names::STREAM.to_string()),
            (ENV_TOPIC.to_string(), seeds::names::TOPIC.to_string()),
            (ENV_SCHEMA.to_string(), "json".to_string()),
        ]);
        if let Some(initial_offset) = initial_offset {
            envs.insert(ENV_INITIAL_OFFSET.to_string(), initial_offset.to_string());
        }
        envs
    }
}

macro_rules! jdbc_fixture {
    ($name:ident, $scenario:expr, $query:expr, $mode:expr, $batch_size:expr, $tracking:expr, $offset:expr) => {
        pub struct $name {
            base: JdbcPostgresFixture,
        }

        #[async_trait]
        impl TestFixture for $name {
            async fn setup() -> Result<Self, TestBinaryError> {
                Ok(Self {
                    base: JdbcPostgresFixture::setup($scenario).await?,
                })
            }

            fn connectors_runtime_envs(&self) -> HashMap<String, String> {
                self.base
                    .envs($query, $mode, $batch_size, $tracking, $offset)
            }
        }
    };
}

jdbc_fixture!(
    JdbcBulkFixture,
    Scenario::Empty,
    "SELECT 1 AS id, 'test' AS name",
    "bulk",
    100,
    "id",
    None
);

impl JdbcIncrementalFixture {
    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.base.create_pool().await
    }
}

impl JdbcRecoveryFixture {
    pub async fn create_pool(&self) -> Result<Pool<Postgres>, TestBinaryError> {
        self.base.create_pool().await
    }
}
jdbc_fixture!(
    JdbcBulkRowsFixture,
    Scenario::Empty,
    "SELECT * FROM (VALUES (1, 'alice', true), (2, 'bob', false), (3, 'carol', true)) AS t(id, name, active)",
    "bulk",
    100,
    "id",
    None
);
jdbc_fixture!(
    JdbcMetadataFixture,
    Scenario::Empty,
    "SELECT 42 AS value",
    "bulk",
    100,
    "id",
    None
);
jdbc_fixture!(
    JdbcIncrementalFixture,
    Scenario::Incremental,
    "SELECT id, name FROM inc_test WHERE id > {last_offset} ORDER BY id",
    "incremental",
    2,
    "id",
    None
);
jdbc_fixture!(
    JdbcTextCursorFixture,
    Scenario::TextCursor,
    "SELECT id, cursor_value FROM text_cursor_test WHERE cursor_value > {last_offset} ORDER BY cursor_value",
    "incremental",
    100,
    "cursor_value",
    Some(r"a\b")
);
jdbc_fixture!(
    JdbcLargeResultFixture,
    Scenario::LargeResult,
    "SELECT id, name, val FROM big_test ORDER BY id",
    "bulk",
    5000,
    "id",
    None
);
jdbc_fixture!(
    JdbcRecoveryFixture,
    Scenario::Empty,
    "SELECT id, name FROM recover_test ORDER BY id",
    "bulk",
    100,
    "id",
    None
);
jdbc_fixture!(
    JdbcBulkOverflowFixture,
    Scenario::BulkOverflow,
    "SELECT id FROM trunc_test ORDER BY id",
    "bulk",
    2,
    "id",
    None
);
jdbc_fixture!(
    JdbcTieBoundaryFixture,
    Scenario::TieBoundary,
    "SELECT id, position FROM tie_test WHERE position > {last_offset} ORDER BY position, id",
    "incremental",
    2,
    "position",
    None
);
jdbc_fixture!(
    JdbcNumericTieBoundaryFixture,
    Scenario::NumericTieBoundary,
    "SELECT id, position FROM numeric_tie_test WHERE position > {last_offset} ORDER BY position, id",
    "incremental",
    2,
    "position",
    None
);

pub struct JdbcQueryTimeoutFixture {
    base: JdbcPostgresFixture,
}

#[async_trait]
impl TestFixture for JdbcQueryTimeoutFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            base: JdbcPostgresFixture::setup(Scenario::Empty).await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self
            .base
            .envs("SELECT 1 AS id FROM pg_sleep(2)", "bulk", 100, "id", None);
        envs.insert(ENV_QUERY_TIMEOUT.to_string(), "1000".to_string());
        envs
    }
}

pub struct JdbcMySqlFixture {
    _container: ContainerAsync<GenericImage>,
    jdbc_url: String,
    driver_jar_path: String,
}

#[async_trait]
impl TestFixture for JdbcMySqlFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(MYSQL_IMAGE, MYSQL_TAG)
            .with_wait_for(WaitFor::log(
                LogWaitStrategy::stderr("ready for connections").with_times(2),
            ))
            .with_exposed_port(MYSQL_PORT.tcp())
            .with_env_var("MYSQL_ROOT_PASSWORD", "root_password")
            .with_env_var("MYSQL_DATABASE", MYSQL_DATABASE)
            .with_env_var("MYSQL_USER", MYSQL_USERNAME)
            .with_env_var("MYSQL_PASSWORD", MYSQL_PASSWORD)
            .with_container_name(unique_container_name("jdbc-mysql"))
            .start()
            .await
            .map_err(|error| fixture_error(format!("failed to start MySQL: {error}")))?;
        let port = container
            .get_host_port_ipv4(MYSQL_PORT.tcp())
            .await
            .map_err(|error| fixture_error(format!("failed to resolve MySQL port: {error}")))?;
        let database_url =
            format!("mysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@127.0.0.1:{port}/{MYSQL_DATABASE}");
        let deadline = Instant::now() + Duration::from_secs(30);
        let pool = loop {
            match MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&database_url)
                .await
            {
                Ok(pool) => break pool,
                Err(_) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(200)).await;
                }
                Err(error) => {
                    return Err(fixture_error(format!(
                        "failed to connect to MySQL: {error}"
                    )));
                }
            }
        };
        sqlx::query("CREATE TABLE mysql_cursor_test (id INT PRIMARY KEY, name VARCHAR(32))")
            .execute(&pool)
            .await
            .map_err(|error| fixture_error(format!("failed to create MySQL table: {error}")))?;
        sqlx::query("INSERT INTO mysql_cursor_test (id, name) VALUES (1, 'a'), (2, 'b')")
            .execute(&pool)
            .await
            .map_err(|error| fixture_error(format!("failed to seed MySQL: {error}")))?;
        pool.close().await;

        Ok(Self {
            _container: container,
            jdbc_url: format!(
                "jdbc:mysql://127.0.0.1:{port}/{MYSQL_DATABASE}?useSSL=false&serverTimezone=UTC"
            ),
            driver_jar_path: mysql_driver_jar().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (ENV_MYSQL_JDBC_URL.to_string(), self.jdbc_url.clone()),
            (
                ENV_MYSQL_DRIVER_CLASS.to_string(),
                "com.mysql.cj.jdbc.Driver".to_string(),
            ),
            (
                ENV_MYSQL_DRIVER_JAR_PATH.to_string(),
                self.driver_jar_path.clone(),
            ),
            (ENV_MYSQL_USERNAME.to_string(), MYSQL_USERNAME.to_string()),
            (ENV_MYSQL_PASSWORD.to_string(), MYSQL_PASSWORD.to_string()),
            (
                ENV_MYSQL_QUERY.to_string(),
                "SELECT id, name FROM mysql_cursor_test WHERE id > {last_offset} ORDER BY id"
                    .to_string(),
            ),
            (ENV_MYSQL_POLL_INTERVAL.to_string(), "100ms".to_string()),
            (ENV_MYSQL_BATCH_SIZE.to_string(), "2".to_string()),
            (
                ENV_MYSQL_STREAM.to_string(),
                seeds::names::STREAM.to_string(),
            ),
            (ENV_MYSQL_TOPIC.to_string(), seeds::names::TOPIC.to_string()),
            (ENV_MYSQL_SCHEMA.to_string(), "json".to_string()),
        ])
    }
}

async fn postgres_driver_jar() -> Result<String, TestBinaryError> {
    jdbc_driver_jar(
        "postgresql-42.7.1.jar",
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar",
        POSTGRES_DRIVER_SHA256,
        POSTGRES_DRIVER_CLASS_ENTRY,
    )
    .await
}

async fn mysql_driver_jar() -> Result<String, TestBinaryError> {
    jdbc_driver_jar(
        "mysql-connector-j-8.0.33.jar",
        "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar",
        MYSQL_DRIVER_SHA256,
        MYSQL_DRIVER_CLASS_ENTRY,
    )
    .await
}

async fn jdbc_driver_jar(
    file_name: &str,
    download_url: &str,
    expected_sha256: &str,
    driver_class_entry: &str,
) -> Result<String, TestBinaryError> {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let jdbc_test_dir = format!("{target_dir}/test-jdbc-drivers");
    let jar_path = format!("{jdbc_test_dir}/{file_name}");
    std::fs::create_dir_all(&jdbc_test_dir)
        .map_err(|error| fixture_error(format!("failed to create driver cache: {error}")))?;

    if std::path::Path::new(&jar_path).exists() {
        match std::fs::read(&jar_path) {
            Ok(bytes) if is_expected_driver_jar(&bytes, expected_sha256, driver_class_entry) => {
                return canonical_path(&jar_path);
            }
            _ => {
                std::fs::remove_file(&jar_path).map_err(|error| {
                    fixture_error(format!("failed to remove invalid cached driver: {error}"))
                })?;
            }
        }
    }

    let response = reqwest::get(download_url)
        .await
        .map_err(|error| fixture_error(format!("failed to download JDBC driver: {error}")))?
        .error_for_status()
        .map_err(|error| fixture_error(format!("JDBC driver download failed: {error}")))?;
    let bytes = response
        .bytes()
        .await
        .map_err(|error| fixture_error(format!("failed to read JDBC driver: {error}")))?;
    if !is_expected_driver_jar(&bytes, expected_sha256, driver_class_entry) {
        return Err(fixture_error(format!(
            "downloaded JDBC driver failed SHA-256 or archive validation for {driver_class_entry}"
        )));
    }

    let temporary_path = format!("{jar_path}.{}.tmp", std::process::id());
    std::fs::write(&temporary_path, &bytes)
        .map_err(|error| fixture_error(format!("failed to cache JDBC driver: {error}")))?;
    std::fs::rename(&temporary_path, &jar_path)
        .map_err(|error| fixture_error(format!("failed to publish JDBC driver cache: {error}")))?;
    canonical_path(&jar_path)
}

fn is_expected_driver_jar(bytes: &[u8], expected_sha256: &str, driver_class_entry: &str) -> bool {
    if format!("{:x}", Sha256::digest(bytes)) != expected_sha256 {
        return false;
    }
    let Ok(mut archive) = ZipArchive::new(Cursor::new(bytes)) else {
        return false;
    };
    archive.by_name(driver_class_entry).is_ok()
}

fn canonical_path(path: &str) -> Result<String, TestBinaryError> {
    std::fs::canonicalize(path)
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|error| fixture_error(format!("failed to resolve JDBC driver path: {error}")))
}

fn fixture_error(message: impl Into<String>) -> TestBinaryError {
    TestBinaryError::FixtureSetup {
        fixture_type: "JDBC".to_string(),
        message: message.into(),
    }
}
