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
    DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_BATCH_SIZE, ENV_SOURCE_CONNECTION_STRING,
    ENV_SOURCE_PATH, ENV_SOURCE_POLL_INTERVAL, ENV_SOURCE_START_POSITION,
    ENV_SOURCE_STREAMS_0_SCHEMA, ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC,
    ENV_SOURCE_TABLES, ENV_SOURCE_VERBOSE_LOGGING, MSSQL_CONTAINER_NAME, MssqlContainer, SqlClient,
    connect_to, execute, setup_error,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

pub const MSSQL_ORDERS_INSTANCE: &str = "dbo_orders";
const ALL_TYPES_TABLE: &str = "CREATE TABLE dbo.types ( \
    id INT PRIMARY KEY, c_tinyint TINYINT, c_smallint SMALLINT, c_bigint BIGINT, c_bit BIT, \
    c_decimal DECIMAL(10, 2), c_numeric NUMERIC(38, 10), c_money MONEY, c_smallmoney SMALLMONEY, \
    c_real REAL, c_float FLOAT, c_char CHAR(5), c_varchar VARCHAR(20), c_nchar NCHAR(4), \
    c_nvarchar NVARCHAR(20), c_text TEXT, c_ntext NTEXT, c_date DATE, c_time TIME(7), \
    c_datetime DATETIME, c_datetime2 DATETIME2(7), c_smalldatetime SMALLDATETIME, \
    c_datetimeoffset DATETIMEOFFSET(3), c_uniqueidentifier UNIQUEIDENTIFIER, c_binary BINARY(4), \
    c_varbinary VARBINARY(MAX), c_image IMAGE, c_rowversion ROWVERSION, c_xml XML, \
    c_geography GEOGRAPHY, c_geometry GEOMETRY, c_hierarchyid HIERARCHYID, \
    c_computed AS (c_tinyint + 1)); \
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'types', @role_name = NULL";
const DEFAULT_TABLES: &str = "[dbo.orders,dbo.customers,sales.order items]";
const DATABASE_PREFIX: &str = "iggy_test_";
// Databases left by earlier runs are dropped once they are this old, so a live test is never touched.
const STALE_DATABASE_MINUTES: u32 = 30;
const JOB_CONTROL_ATTEMPTS: u32 = 60;
const JOB_CONTROL_INTERVAL: Duration = Duration::from_millis(500);
const BACKUP_DIRECTORY: &str = "/var/opt/mssql/data";

/// One database with CDC on three tables, read by the real capture job.
pub struct MssqlSourceFixture {
    container: MssqlContainer,
    database: String,
    settings: Settings,
}

struct Settings {
    poll_interval: &'static str,
    batch_size: usize,
    capture_job: CaptureJob,
    tables: &'static str,
    password: Option<&'static str>,
    extra_sql: Option<&'static str>,
    start_position: &'static str,
    /// Orders inserted and captured before the connector starts.
    seeded_orders: usize,
    /// Logs every batch at info, so a test can count batches.
    verbose_logging: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            poll_interval: "100ms",
            batch_size: 1000,
            capture_job: CaptureJob::Running,
            tables: DEFAULT_TABLES,
            password: None,
            extra_sql: None,
            start_position: "earliest",
            seeded_orders: 0,
            verbose_logging: false,
        }
    }
}

/// Declares a fixture that derefs to [`MssqlSourceFixture`] and changes only its settings.
macro_rules! fixture_variant {
    ($(#[$meta:meta])* $name:ident, $settings:expr) => {
        $(#[$meta])*
        pub struct $name {
            inner: MssqlSourceFixture,
        }

        impl std::ops::Deref for $name {
            type Target = MssqlSourceFixture;

            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        #[async_trait]
        impl TestFixture for $name {
            async fn setup() -> Result<Self, TestBinaryError> {
                Ok(Self {
                    inner: MssqlSourceFixture::setup_with($settings).await?,
                })
            }

            fn connectors_runtime_envs(&self) -> HashMap<String, String> {
                self.inner.envs()
            }
        }
    };
}

fixture_variant!(
    /// Capture job stopped: tests call [`MssqlSourceFixture::scan`], and batches hold at most 10 changes.
    MssqlSourceManualScanFixture,
    Settings {
        poll_interval: "2s",
        batch_size: 10,
        capture_job: CaptureJob::Stopped,
        verbose_logging: true,
        ..Settings::default()
    }
);

fixture_variant!(
    /// The connection string carries a wrong password.
    MssqlInvalidCredentialsFixture,
    Settings {
        password: Some("Wrong_Passw0rd!"),
        ..Settings::default()
    }
);

fixture_variant!(
    /// `dbo.plain` exists but has no capture instance.
    MssqlUncapturedTableFixture,
    Settings {
        tables: "[dbo.orders,dbo.plain]",
        extra_sql: Some("CREATE TABLE dbo.plain (id INT PRIMARY KEY)"),
        ..Settings::default()
    }
);

fixture_variant!(
    /// `start_position = "latest"`, with three orders captured before the connector starts.
    MssqlLatestStartFixture,
    Settings {
        start_position: "latest",
        seeded_orders: 3,
        ..Settings::default()
    }
);

fixture_variant!(
    /// `dbo.variants` captures a `sql_variant` column, which the connector does not support.
    MssqlUnsupportedTypeFixture,
    Settings {
        tables: "[dbo.variants]",
        extra_sql: Some(
            "CREATE TABLE dbo.variants (id INT PRIMARY KEY, value SQL_VARIANT NULL); \
             EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'variants', \
             @role_name = NULL"
        ),
        ..Settings::default()
    }
);

fixture_variant!(
    /// `dbo.types` captures one column of every supported type.
    MssqlAllTypesFixture,
    Settings {
        tables: "[dbo.types]",
        extra_sql: Some(ALL_TYPES_TABLE),
        ..Settings::default()
    }
);

fixture_variant!(
    /// The database was enabled for CDC and then disabled again, so it has the tables but no CDC.
    MssqlCdcDisabledFixture,
    Settings {
        extra_sql: Some("EXEC sys.sp_cdc_disable_db"),
        capture_job: CaptureJob::None,
        ..Settings::default()
    }
);

fixture_variant!(
    /// `dbo.missing` does not exist.
    MssqlMissingTableFixture,
    Settings {
        tables: "[dbo.orders,dbo.missing]",
        ..Settings::default()
    }
);

fixture_variant!(
    /// Two configured names that resolve to one table under the default case-insensitive collation.
    MssqlDuplicateTableFixture,
    Settings {
        tables: "[dbo.orders,dbo.ORDERS]",
        ..Settings::default()
    }
);

impl MssqlSourceFixture {
    pub async fn client(&self) -> Result<SqlClient, TestBinaryError> {
        self.container.connect(&self.database).await
    }

    pub async fn execute(&self, sql: &str) {
        let mut client = self.client().await.expect("connect to test database");
        execute(&mut client, sql)
            .await
            .unwrap_or_else(|error| panic!("SQL failed: {error}\n{sql}"));
    }

    /// Runs one capture scan. Retries while a stopping capture job still holds the log reader.
    pub async fn scan(&self) {
        let mut client = self.client().await.expect("connect to test database");
        let mut last_error = String::new();
        for _ in 0..JOB_CONTROL_ATTEMPTS {
            match execute(&mut client, "EXEC sys.sp_cdc_scan").await {
                Ok(_) => return,
                Err(error) => last_error = error.to_string(),
            }
            sleep(JOB_CONTROL_INTERVAL).await;
        }
        panic!("sp_cdc_scan kept failing: {last_error}");
    }

    /// The commit LSN of the `position`-th change row (1-based) of a capture instance, as hex.
    pub async fn change_lsn(&self, capture_instance: &str, position: usize) -> String {
        let mut client = self.client().await.expect("connect to test database");
        let sql = format!(
            "SELECT CONVERT(VARCHAR(22), __$start_lsn, 1) FROM cdc.[{capture_instance}_CT] \
             ORDER BY __$start_lsn, __$seqval, __$operation \
             OFFSET {} ROWS FETCH NEXT 1 ROWS ONLY",
            position - 1
        );
        let rows = execute(&mut client, &sql).await.expect("read change LSN");
        rows.first()
            .and_then(|row| row.get::<&str, _>(0))
            .map(str::to_string)
            .unwrap_or_else(|| panic!("no change row {position} in {capture_instance}"))
    }

    /// Deletes every change row of the instance below `low_water_mark`, as the cleanup job would.
    pub async fn cleanup_below(&self, capture_instance: &str, low_water_mark: &str) {
        self.execute(&format!(
            "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'{capture_instance}', \
             @low_water_mark = {low_water_mark}, @threshold = 100000"
        ))
        .await;
    }

    pub async fn recovery_fork_guid(&self) -> String {
        let mut client = self.client().await.expect("connect to test database");
        let rows = execute(
            &mut client,
            "SELECT CONVERT(VARCHAR(36), recovery_fork_guid) FROM sys.database_recovery_status WHERE database_id = DB_ID()",
        )
        .await
        .expect("read recovery_fork_guid");
        rows.first()
            .and_then(|row| row.get::<&str, _>(0))
            .map(str::to_string)
            .expect("recovery_fork_guid row")
    }

    /// Disables CDC on the database, which drops every capture instance and the capture job.
    pub async fn disable_cdc(&self) {
        self.execute("EXEC sys.sp_cdc_disable_db").await;
    }

    /// Adds a second capture instance on `dbo.orders`.
    pub async fn add_orders_instance(&self, capture_instance: &str) {
        self.execute(&format!(
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', \
             @role_name = NULL, @capture_instance = N'{capture_instance}'"
        ))
        .await;
    }

    /// Drops a capture instance of `dbo.orders` with all of its change rows.
    pub async fn drop_orders_instance(&self, capture_instance: &str) {
        self.execute(&format!(
            "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'orders', \
             @capture_instance = N'{capture_instance}'"
        ))
        .await;
    }

    /// Waits until the capture job has written `count` change rows for the instance.
    pub async fn wait_for_change_rows(&self, capture_instance: &str, count: usize) {
        let mut client = self.client().await.expect("connect to test database");
        let sql = format!("SELECT COUNT(*) FROM cdc.[{capture_instance}_CT]");
        let mut seen = 0;
        for _ in 0..JOB_CONTROL_ATTEMPTS * 4 {
            let rows = execute(&mut client, &sql).await.expect("count change rows");
            seen = rows
                .first()
                .and_then(|row| row.get::<i32, _>(0))
                .unwrap_or_default() as usize;
            if seen >= count {
                return;
            }
            sleep(JOB_CONTROL_INTERVAL).await;
        }
        panic!("capture job wrote {seen} of {count} change rows for {capture_instance}");
    }

    /// Kills every other session on the test database, which drops the connector's connection.
    pub async fn kill_other_sessions(&self) -> usize {
        let mut client = self.client().await.expect("connect to test database");
        let rows = execute(
            &mut client,
            "SELECT session_id FROM sys.dm_exec_sessions \
             WHERE database_id = DB_ID() AND session_id <> @@SPID AND is_user_process = 1 \
             AND program_name NOT LIKE N'SQLAgent%'",
        )
        .await
        .expect("list sessions");
        let sessions: Vec<i16> = rows.iter().filter_map(|row| row.get::<i16, _>(0)).collect();
        for session in &sessions {
            execute(&mut client, &format!("KILL {session}"))
                .await
                .expect("kill session");
        }
        sessions.len()
    }

    /// Backs the database up and restores the backup over itself, keeping CDC.
    pub async fn backup_and_restore(&self) {
        let database = &self.database;
        let backup = format!("{BACKUP_DIRECTORY}/{database}.bak");
        let mut master = self.container.connect("master").await.expect("connect");
        execute(
            &mut master,
            &format!("BACKUP DATABASE [{database}] TO DISK = N'{backup}' WITH INIT"),
        )
        .await
        .expect("backup");
        // OFFLINE, not SINGLE_USER: a running connector could take the single slot before RESTORE.
        execute(
            &mut master,
            &format!(
                "ALTER DATABASE [{database}] SET OFFLINE WITH ROLLBACK IMMEDIATE; \
                 RESTORE DATABASE [{database}] FROM DISK = N'{backup}' WITH REPLACE, KEEP_CDC"
            ),
        )
        .await
        .expect("restore");
        // The backup lives in the shared container, so it is removed rather than left to pile up.
        let removed = std::process::Command::new("docker")
            .args(["exec", MSSQL_CONTAINER_NAME, "rm", "-f", &backup])
            .status();
        if !removed.is_ok_and(|status| status.success()) {
            tracing::warn!("Could not remove backup {backup} from {MSSQL_CONTAINER_NAME}");
        }
    }

    /// Drops the database and creates a new one with the same name, tables and CDC setup.
    pub async fn recreate_database(&self) {
        let mut master = self.container.connect("master").await.expect("connect");
        drop_database(&mut master, &self.database)
            .await
            .expect("drop database");
        create_database(&self.container, &self.database)
            .await
            .expect("re-create database");
    }

    async fn setup_with(settings: Settings) -> Result<Self, TestBinaryError> {
        let container = MssqlContainer::start().await?;
        let mut master = container.connect("master").await?;
        drop_stale_databases(&mut master).await?;
        let database = format!("{DATABASE_PREFIX}{}", Uuid::new_v4().simple());
        create_database(&container, &database).await?;
        let mut client = container.connect(&database).await?;
        if let Some(sql) = settings.extra_sql {
            execute(&mut client, sql)
                .await
                .map_err(|error| setup_error(format!("Fixture SQL failed: {error}")))?;
        }
        match settings.capture_job {
            CaptureJob::Running => restart_capture_job(&mut client).await?,
            CaptureJob::Stopped => stop_capture_job(&mut client).await?,
            CaptureJob::None => {}
        }
        let fixture = Self {
            container,
            database,
            settings,
        };
        if fixture.settings.seeded_orders > 0 {
            let values: Vec<String> = (1..=fixture.settings.seeded_orders)
                .map(|id| format!("({id}, 1.00)"))
                .collect();
            fixture
                .execute(&format!(
                    "INSERT INTO dbo.orders (id, amount) VALUES {}",
                    values.join(", ")
                ))
                .await;
            fixture
                .wait_for_change_rows(MSSQL_ORDERS_INSTANCE, fixture.settings.seeded_orders)
                .await;
        }
        Ok(fixture)
    }

    fn envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SOURCE_CONNECTION_STRING.to_string(),
                self.container
                    .connection_string(&self.database, self.settings.password),
            ),
            (
                ENV_SOURCE_TABLES.to_string(),
                self.settings.tables.to_string(),
            ),
            (
                ENV_SOURCE_POLL_INTERVAL.to_string(),
                self.settings.poll_interval.to_string(),
            ),
            (
                ENV_SOURCE_START_POSITION.to_string(),
                self.settings.start_position.to_string(),
            ),
            (
                ENV_SOURCE_VERBOSE_LOGGING.to_string(),
                self.settings.verbose_logging.to_string(),
            ),
            (
                ENV_SOURCE_BATCH_SIZE.to_string(),
                self.settings.batch_size.to_string(),
            ),
            (
                ENV_SOURCE_STREAMS_0_STREAM.to_string(),
                DEFAULT_TEST_STREAM.to_string(),
            ),
            (
                ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
                DEFAULT_TEST_TOPIC.to_string(),
            ),
            (ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string()),
            (
                ENV_SOURCE_PATH.to_string(),
                "../../target/debug/libiggy_connector_mssql_source".to_string(),
            ),
        ])
    }
}

#[async_trait]
impl TestFixture for MssqlSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::setup_with(Settings::default()).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.envs()
    }
}

/// Drops the test database so its capture job stops polling in the shared container.
impl Drop for MssqlSourceFixture {
    fn drop(&mut self) {
        let port = self.container.port;
        let database = self.database.clone();
        // Drop cannot await, and the test's runtime may be shutting down, so a short-lived runtime does the work.
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()?;
            runtime.block_on(async {
                let mut master = connect_to(port, "master").await.ok()?;
                drop_database(&mut master, &database).await.ok()
            })
        })
        .join();
        if !matches!(dropped, Ok(Some(()))) {
            tracing::warn!(
                "Could not drop test database {}, the stale sweep will",
                self.database
            );
        }
    }
}

enum CaptureJob {
    Running,
    Stopped,
    /// CDC is disabled on the database, so there is no job.
    None,
}

async fn create_database(
    container: &MssqlContainer,
    database: &str,
) -> Result<(), TestBinaryError> {
    let mut master = container.connect("master").await?;
    execute(&mut master, &format!("CREATE DATABASE [{database}]"))
        .await
        .map_err(|error| setup_error(format!("CREATE DATABASE failed: {error}")))?;
    let mut client = container.connect(database).await?;
    execute(
        &mut client,
        "EXEC sys.sp_cdc_enable_db; \
         CREATE TABLE dbo.orders (id INT PRIMARY KEY, amount DECIMAL(10, 2) NOT NULL, note NVARCHAR(MAX) NULL); \
         CREATE TABLE dbo.customers (id INT PRIMARY KEY, visits INT NOT NULL); \
         EXEC('CREATE SCHEMA sales'); \
         CREATE TABLE sales.[order items] (line_no INT PRIMARY KEY, qty INT NOT NULL); \
         EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL; \
         EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'customers', @role_name = NULL; \
         EXEC sys.sp_cdc_enable_table @source_schema = N'sales', @source_name = N'order items', @role_name = NULL; \
         EXEC sys.sp_cdc_change_job @job_type = N'capture', @maxtrans = 10, @pollinginterval = 1",
    )
    .await
    .map_err(|error| setup_error(format!("CDC setup failed: {error}")))?;
    Ok(())
}

/// Restarts the capture job so the short polling interval set at creation takes effect.
async fn restart_capture_job(client: &mut SqlClient) -> Result<(), TestBinaryError> {
    stop_capture_job(client).await?;
    retry_job_control(client, "EXEC sys.sp_cdc_start_job @job_type = N'capture'").await
}

async fn stop_capture_job(client: &mut SqlClient) -> Result<(), TestBinaryError> {
    retry_job_control(client, "EXEC sys.sp_cdc_stop_job @job_type = N'capture'").await
}

async fn retry_job_control(client: &mut SqlClient, sql: &str) -> Result<(), TestBinaryError> {
    let mut last_error = String::new();
    for _ in 0..JOB_CONTROL_ATTEMPTS {
        match execute(client, sql).await {
            Ok(_) => return Ok(()),
            // 22268: the job is already in the requested state.
            Err(error) if error.code() == Some(22268) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }
        sleep(JOB_CONTROL_INTERVAL).await;
    }
    Err(setup_error(format!("{sql} kept failing: {last_error}")))
}

async fn drop_database(
    master: &mut SqlClient,
    database: &str,
) -> Result<(), tiberius::error::Error> {
    execute(
        master,
        &format!(
            "IF DB_ID(N'{database}') IS NOT NULL BEGIN \
             ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; \
             DROP DATABASE [{database}]; END"
        ),
    )
    .await
    .map(|_| ())
}

async fn drop_stale_databases(master: &mut SqlClient) -> Result<(), TestBinaryError> {
    let rows = execute(
        master,
        &format!(
            "SELECT name FROM sys.databases WHERE name LIKE N'{DATABASE_PREFIX}%' \
             AND create_date < DATEADD(MINUTE, -{STALE_DATABASE_MINUTES}, GETDATE())"
        ),
    )
    .await
    .map_err(|error| setup_error(format!("Failed to list stale databases: {error}")))?;
    let names: Vec<String> = rows
        .iter()
        .filter_map(|row| row.get::<&str, _>(0).map(str::to_string))
        .collect();
    for name in names {
        // A failed drop only leaves a stale database behind, so it does not fail the test.
        let _ = drop_database(master, &name).await;
    }
    Ok(())
}
