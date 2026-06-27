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

// Doris fixture. One Doris container is shared across every doris test in this
// CI job (or local `cargo test` session) via testcontainers' reusable-containers
// feature: the container is named `iggy-test-doris` and marked
// `ReuseDirective::Always`, so the first test creates it and every subsequent
// test (in this or any other test process on the same Docker daemon) attaches
// to it. This is what makes `cargo test` and CI follow the same path — the old
// orchestration in `.github/actions/rust/pre-merge` is gone.
//
// Cross-process sharing matters because nextest runs each test in its own
// process. An in-process `OnceCell` would reboot Doris every time, and the new
// container would race the previous one for the BE's 1:1-mapped 8040 port.
//
// The container outlives the test session by design (that's what enables
// reuse). CI runners are ephemeral so it dies with them; locally, `docker rm
// -f iggy-test-doris` forces a fresh boot.
//
// Two host-level prerequisites cannot live inside the container:
//   * `vm.max_map_count >= 2_000_000` — Doris 4.0.3's `start_be.sh` hard-exits
//     otherwise. `check_vm_max_map_count` reads `/proc/sys/vm/max_map_count`
//     and returns a self-describing error if the host is below the threshold.
//   * Routable Docker bridge IPs — the FE returns a 307 to a BE address that
//     is the BE container's internal Docker network IP. Those addresses are
//     routable from a Linux Docker host but not from macOS Docker Desktop, so
//     these tests are effectively Linux-only without extra networking work.

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use std::collections::HashMap;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt, ReuseDirective,
};
use tokio::time::sleep;
use tracing::info;
use uuid::Uuid;

const DORIS_IMAGE: &str = "docker.io/apache/doris";
// Apache's maintained single-container line is now tagged `<version>-all` /
// `<version>-all-slim` (the old one-off `doris-all-in-one-2.1.0` was pushed once
// in 2024 and never refreshed). `-slim` is the smaller base, so it pulls faster
// on CI runners.
const DORIS_TAG: &str = "4.0.3-all-slim";
// Fixed name + `ReuseDirective::Always` is what makes the container survive
// across nextest's per-test processes. Stable name means every test process
// inspecting the Docker daemon finds the same one.
const DORIS_CONTAINER_NAME: &str = "iggy-test-doris";
const FE_HTTP_PORT: u16 = 8030;
const FE_MYSQL_PORT: u16 = 9030;
const BE_HTTP_PORT: u16 = 8040;
const FE_HEALTH_ENDPOINT: &str = "/api/health";

const DEFAULT_TEST_TABLE: &str = "test_topic";
const DEFAULT_USER: &str = "root";
const DEFAULT_PASSWORD: &str = "";

// Doris's all-in-one image typically reports `Alive: true` within ~40s of
// container start, but a cold Docker host or constrained CI runner can push
// that out. The wait window is sized to swallow those outliers without
// hanging a test slot for a truly broken cluster.
const DEFAULT_BE_REGISTRATION_ATTEMPTS: usize = 180;
const DEFAULT_BE_REGISTRATION_INTERVAL_MS: u64 = 2000;

// 90 seconds (180 × 500ms) — bulk tests with 1000 rows can take ~30 s on a
// fresh container, and a Doris instance under memory pressure (e.g. when it
// is the 3rd or 4th container to spin up serially) takes longer.
pub const DEFAULT_POLL_ATTEMPTS: usize = 180;
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 500;

// vm.max_map_count threshold Doris 4.0.3's start_be.sh enforces (`exit 1` if
// the running kernel reports less). The number is Doris's, not ours. Only
// referenced by the Linux precheck; gated to keep non-Linux builds warning-free.
#[cfg(target_os = "linux")]
const REQUIRED_VM_MAX_MAP_COUNT: u64 = 2_000_000;

// 4.0.3 sizes the FE for a dedicated host (8GB -Xms) and lets the BE consume
// ~90% of RAM via `mem_limit = auto`. Both blow past a 16GB CI runner that also
// has to host the cargo build + the test binaries; cap both to fit beside that
// workload. ~Matches the 2.1.0 image's effective footprint.
const FE_HEAP_OVERRIDE_SED: &str = "s/-Xmx8192m -Xms8192m/-Xmx2048m -Xms2048m/";
const BE_MEM_LIMIT_OVERRIDE: &str = "mem_limit = 4096M";

const ENV_SINK_FE_URL: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_FE_URL";
const ENV_SINK_DATABASE: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_DATABASE";
const ENV_SINK_TABLE: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_TABLE";
const ENV_SINK_USERNAME: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_USERNAME";
const ENV_SINK_PASSWORD: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_PASSWORD";
const ENV_SINK_LABEL_PREFIX: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_LABEL_PREFIX";
const ENV_SINK_MAX_FILTER_RATIO: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_MAX_FILTER_RATIO";
const ENV_SINK_COLUMNS: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_COLUMNS";
const ENV_SINK_FORMAT: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_OUTPUT_FORMAT";
const ENV_SINK_BATCH_SIZE: &str = "IGGY_CONNECTORS_SINK_DORIS_PLUGIN_CONFIG_BATCH_SIZE";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_TOPICS";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_DORIS_STREAMS_0_CONSUMER_GROUP";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_DORIS_PATH";

/// The schema used by `TestMessage` in the integration tests.
///
/// `replication_num = 1` because the all-in-one container has a single BE. The
/// table storage medium is left to Doris's default (the 2.1 all-in-one image
/// advertised HDD-only and needed an explicit `storage_medium = HDD` override;
/// the 4.0 image no longer does).
const TEST_TABLE_DDL_TEMPLATE: &str = "
CREATE TABLE IF NOT EXISTS {db}.{table} (
    id BIGINT NOT NULL,
    name VARCHAR(64) NOT NULL,
    count INT NOT NULL,
    amount DOUBLE NOT NULL,
    active BOOLEAN NOT NULL,
    timestamp BIGINT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    \"replication_num\" = \"1\"
);
";

/// Same shape as `TEST_TABLE_DDL_TEMPLATE` plus an extra `calculated INT NOT
/// NULL` column. The columns-mapping test populates `calculated` via a Stream
/// Load `columns` derived expression (`calculated = count + 1`); without that
/// header the load would fail because `calculated` has no source field in the
/// JSON payload.
const TEST_TABLE_WITH_CALCULATED_DDL_TEMPLATE: &str = "
CREATE TABLE IF NOT EXISTS {db}.{table} (
    id BIGINT NOT NULL,
    name VARCHAR(64) NOT NULL,
    count INT NOT NULL,
    amount DOUBLE NOT NULL,
    active BOOLEAN NOT NULL,
    timestamp BIGINT NOT NULL,
    calculated INT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    \"replication_num\" = \"1\"
);
";

/// Stream Load `columns` header used by `DorisSinkColumnsMappingFixture`. The
/// six leading names match the JSON payload's keys; `calculated = count + 1`
/// instructs Doris to derive the seventh column from the loaded `count`.
pub const COLUMNS_MAPPING_HEADER: &str =
    "id, name, count, amount, active, timestamp, calculated = count + 1";

/// Stream Load `columns` header for `DorisSinkCsvFixture`. CSV is positional, so
/// the connector needs the explicit column order to map each message's JSON
/// values into the right CSV fields. Matches `TEST_TABLE_DDL_TEMPLATE`'s order.
const CSV_COLUMNS_HEADER: &str = "id, name, count, amount, active, timestamp";

/// Linux-only host precheck. macOS / Windows have no `/proc/sys/vm/max_map_count`
/// (and the all-in-one image is already unusable on macOS for the BE-redirect
/// routing reason — see the file header). Treat the check as a no-op there; the
/// container start will surface any real issue.
#[cfg(target_os = "linux")]
fn check_vm_max_map_count() -> Result<(), TestBinaryError> {
    let v: u64 = std::fs::read_to_string("/proc/sys/vm/max_map_count")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    if v < REQUIRED_VM_MAX_MAP_COUNT {
        return Err(TestBinaryError::FixtureSetup {
            fixture_type: "SharedDoris".to_string(),
            message: format!(
                "Doris 4.0.3 BE refuses to start unless vm.max_map_count >= \
                 {REQUIRED_VM_MAX_MAP_COUNT} (current: {v}). \
                 Run: `sudo sysctl -w vm.max_map_count={REQUIRED_VM_MAX_MAP_COUNT}`"
            ),
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn check_vm_max_map_count() -> Result<(), TestBinaryError> {
    Ok(())
}

pub struct DorisContainer {
    // Held only so testcontainers' Drop runs on test exit. ReuseDirective::Always
    // makes that Drop a no-op (the container is left running for the next test
    // to attach to), but keeping the handle around is still required to keep
    // the bollard client connection alive.
    _container: ContainerAsync<GenericImage>,
    fe_url: String,
    fe_mysql_host_port: u16,
    // Unique per test, so many tests can share one cluster without their loads
    // colliding. Created during setup; the connector writes into it.
    database: String,
}

impl DorisContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        check_vm_max_map_count()?;

        // Custom entrypoint patches FE heap + BE mem_limit before handing off
        // to the image's normal entry_point.sh; SKIP_CHECK_ULIMIT bypasses the
        // image's swap/ulimit gates so we needn't swapoff the runner. Only
        // runs on first boot; ignored on attach because the existing container
        // already has its config applied.
        let entrypoint_cmd = format!(
            "sed -i '{FE_HEAP_OVERRIDE_SED}' /opt/apache-doris/fe/conf/fe.conf && \
             echo '{BE_MEM_LIMIT_OVERRIDE}' >> /opt/apache-doris/be/conf/be.conf && \
             exec bash /usr/local/bin/entry_point.sh"
        );

        // FE HTTP and FE MySQL get ephemeral host ports (the connector and
        // tests connect via the resolved mapping). BE HTTP must be pinned
        // 1:1 — the FE always returns Location: http://127.0.0.1:8040/...
        // for the Stream Load redirect, and that's only reachable from the
        // host if container:8040 is bound to host:8040.
        //
        // `with_container_name` + `with_reuse(Always)` is what makes the
        // container survive across nextest's per-test processes: the first
        // test creates `iggy-test-doris`, every later test (in any process)
        // attaches to it. The 1:1 BE port is therefore held continuously by
        // one container, never racing with itself across container restarts.
        let container = GenericImage::new(DORIS_IMAGE, DORIS_TAG)
            // GenericImage's own with_entrypoint/with_wait_for must come before
            // any ImageExt method, which turns GenericImage into ContainerRequest.
            .with_entrypoint("bash")
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new(FE_HEALTH_ENDPOINT)
                    .with_port(FE_HTTP_PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_env_var("SKIP_CHECK_ULIMIT", "true")
            .with_cmd(["-c", entrypoint_cmd.as_str()])
            .with_mapped_port(0, FE_HTTP_PORT.tcp())
            .with_mapped_port(0, FE_MYSQL_PORT.tcp())
            .with_mapped_port(BE_HTTP_PORT, BE_HTTP_PORT.tcp())
            .with_container_name(DORIS_CONTAINER_NAME)
            .with_reuse(ReuseDirective::Always)
            .start()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "DorisContainer".to_string(),
                message: format!("Failed to start container: {e}"),
            })?;

        let ports = container
            .ports()
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "DorisContainer".to_string(),
                message: format!("Failed to read mapped ports: {e}"),
            })?;

        let fe_http_host_port = ports.map_to_host_port_ipv4(FE_HTTP_PORT).ok_or_else(|| {
            TestBinaryError::FixtureSetup {
                fixture_type: "DorisContainer".to_string(),
                message: "No host mapping for Doris FE HTTP port".to_string(),
            }
        })?;
        let fe_mysql_host_port = ports.map_to_host_port_ipv4(FE_MYSQL_PORT).ok_or_else(|| {
            TestBinaryError::FixtureSetup {
                fixture_type: "DorisContainer".to_string(),
                message: "No host mapping for Doris FE MySQL port".to_string(),
            }
        })?;

        info!(
            "Doris container ready (name={DORIS_CONTAINER_NAME}): FE HTTP -> {fe_http_host_port}, FE MySQL -> {fe_mysql_host_port}"
        );

        // A fresh database per test so any number of tests can share one
        // cluster without their Stream Loads colliding. `simple()` drops the
        // hyphens so the name stays a valid Doris identifier ([A-Za-z0-9_]).
        let database = format!("iggy_test_db_{}", Uuid::new_v4().simple());

        let this = Self {
            _container: container,
            fe_url: format!("http://127.0.0.1:{fe_http_host_port}"),
            fe_mysql_host_port,
            database,
        };

        // Required on first boot; fast-paths in <1s once the BE is already
        // alive on subsequent test-process attaches.
        this.wait_for_be_alive().await?;
        this.create_test_database().await?;
        Ok(this)
    }

    pub fn fe_url(&self) -> String {
        self.fe_url.clone()
    }

    /// The unique per-test database this fixture's connector writes into.
    pub fn database(&self) -> &str {
        &self.database
    }

    async fn create_pool(&self) -> Result<MySqlPool, TestBinaryError> {
        // sqlx ordinarily emits a `SET sql_mode = (SELECT CONCAT(...))`
        // session init when connecting to MySQL. Doris rejects that with
        // "Set statement does't support non-constant expr.", which kills
        // the connection before any query runs. Disabling these two flags
        // suppresses both offending SET statements.
        let opts = MySqlConnectOptions::new()
            .host("127.0.0.1")
            .port(self.fe_mysql_host_port)
            .username(DEFAULT_USER)
            .pipes_as_concat(false)
            .no_engine_substitution(false);

        MySqlPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts)
            .await
            .map_err(|e| TestBinaryError::FixtureSetup {
                fixture_type: "DorisContainer".to_string(),
                message: format!("Failed to connect to Doris over MySQL: {e}"),
            })
    }

    /// Doris reports BE alive=true via `SHOW BACKENDS` only after the BE has
    /// successfully registered with the FE. Stream Load won't work until then,
    /// so we block here.
    async fn wait_for_be_alive(&self) -> Result<(), TestBinaryError> {
        let mut last_diag = String::from("never connected");
        for attempt in 0..DEFAULT_BE_REGISTRATION_ATTEMPTS {
            match self.create_pool().await {
                Ok(pool) => {
                    // `SHOW BACKENDS` cannot be sent through `sqlx::query` —
                    // sqlx prepares the statement and Doris's MySQL frontend
                    // rejects PREPARE for anything other than SELECT/INSERT
                    // ("Only support prepare SelectStmt or InsertStmt now").
                    // `sqlx::raw_sql` skips PREPARE and dispatches the bytes
                    // directly, so SHOW/DDL works.
                    match sqlx::raw_sql("SHOW BACKENDS").fetch_all(&pool).await {
                        Ok(rows) => {
                            let any_alive = rows.iter().any(|row| {
                                use sqlx::Row;
                                row.try_get::<String, _>("Alive")
                                    .map(|v| v.eq_ignore_ascii_case("true"))
                                    .unwrap_or(false)
                            });
                            if any_alive {
                                info!("Doris BE registered alive after {attempt} attempts");
                                return Ok(());
                            }
                            last_diag = format!("{} backend rows, none alive", rows.len());
                        }
                        Err(e) => {
                            last_diag = format!("SHOW BACKENDS failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    last_diag = format!("connect failed: {e}");
                }
            }
            // Surface progress every ~30 seconds so a stuck cluster is
            // distinguishable from one that's still bootstrapping.
            if attempt > 0 && attempt % (30_000 / DEFAULT_BE_REGISTRATION_INTERVAL_MS as usize) == 0
            {
                info!("Doris BE not yet alive after {attempt} attempts ({last_diag})");
            }
            sleep(Duration::from_millis(DEFAULT_BE_REGISTRATION_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::FixtureSetup {
            fixture_type: "DorisContainer".to_string(),
            message: format!(
                "Doris BE did not register within {DEFAULT_BE_REGISTRATION_ATTEMPTS} attempts ({last_diag})"
            ),
        })
    }

    async fn create_test_database(&self) -> Result<(), TestBinaryError> {
        let pool = self.create_pool().await?;
        // raw_sql avoids the PREPARE path Doris doesn't accept for DDL.
        // sqlx 0.9's `raw_sql` requires `SqlSafeStr`; the input here is a UUID
        // we just generated, so `AssertSqlSafe` is appropriate.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            self.database
        )))
        .execute(&pool)
        .await
        .map_err(|e| TestBinaryError::FixtureSetup {
            fixture_type: "DorisContainer".to_string(),
            message: format!("Failed to create test database: {e}"),
        })?;
        Ok(())
    }
}

#[async_trait]
pub trait DorisOps: Sync {
    fn container(&self) -> &DorisContainer;

    /// The unique per-test database this fixture's connector writes into.
    fn database(&self) -> &str {
        self.container().database()
    }

    async fn pool(&self) -> Result<MySqlPool, TestBinaryError> {
        self.container().create_pool().await
    }

    async fn create_table(&self, database: &str, table: &str) -> Result<(), TestBinaryError> {
        self.create_table_with_template(database, table, TEST_TABLE_DDL_TEMPLATE)
            .await
    }

    async fn create_table_with_template(
        &self,
        database: &str,
        table: &str,
        ddl_template: &str,
    ) -> Result<(), TestBinaryError> {
        let pool = self.pool().await?;
        let ddl = ddl_template
            .replace("{db}", database)
            .replace("{table}", table);

        // Even after `SHOW BACKENDS` reports `Alive: true`, the FE briefly
        // doesn't know about the BE's storage paths and rejects CREATE TABLE
        // with "Failed to find enough backend ... storage medium: ...". The
        // BE typically catches up within a couple of seconds once it sends
        // its first tablet/disk report. Retry through that window.
        let mut last_err: Option<sqlx::Error> = None;
        for _ in 0..30 {
            // Borrow `ddl` (not move) — this runs inside the retry loop.
            match sqlx::raw_sql(sqlx::AssertSqlSafe(ddl.as_str()))
                .execute(&pool)
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("Failed to find enough backend") {
                        last_err = Some(e);
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    return Err(TestBinaryError::FixtureSetup {
                        fixture_type: "DorisOps".to_string(),
                        message: format!("Failed to create table {database}.{table}: {e}"),
                    });
                }
            }
        }
        Err(TestBinaryError::FixtureSetup {
            fixture_type: "DorisOps".to_string(),
            message: format!(
                "Failed to create table {database}.{table} after 30 retries waiting for BE storage report: {}",
                last_err.map(|e| e.to_string()).unwrap_or_default(),
            ),
        })
    }

    /// Returns true iff `database.table` is registered in Doris's
    /// `information_schema.tables`. Used by the missing-target-table test to
    /// assert the connector did NOT silently auto-create on a failed load.
    async fn table_exists(&self, database: &str, table: &str) -> Result<bool, TestBinaryError> {
        let pool = self.pool().await?;
        let rows = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "SELECT TABLE_NAME FROM information_schema.tables \
             WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}'"
        )))
        .fetch_all(&pool)
        .await
        .map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to query information_schema for {database}.{table}: {e}"),
        })?;
        Ok(!rows.is_empty())
    }

    async fn count_rows(&self, database: &str, table: &str) -> Result<i64, TestBinaryError> {
        let pool = self.pool().await?;
        // SELECT supports PREPARE in Doris but raw_sql is consistent with
        // the rest of the fixture and avoids any future surprises.
        use sqlx::Row;
        let row = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) AS c FROM {database}.{table}"
        )))
        .fetch_one(&pool)
        .await
        .map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to count rows in {database}.{table}: {e}"),
        })?;
        // Doris returns COUNT(*) as BIGINT; sqlx decodes that to i64.
        let count: i64 = row.try_get(0).map_err(|e| TestBinaryError::InvalidState {
            message: format!("Failed to read count column: {e}"),
        })?;
        Ok(count)
    }

    async fn wait_for_rows(
        &self,
        database: &str,
        table: &str,
        expected: i64,
    ) -> Result<i64, TestBinaryError> {
        let mut last = 0i64;
        for _ in 0..DEFAULT_POLL_ATTEMPTS {
            // Doris is not strongly consistent for very freshly loaded data
            // until the publish phase completes. Poll until the expected
            // count is observed (or until we exhaust attempts).
            if let Ok(c) = self.count_rows(database, table).await {
                last = c;
                if c == expected {
                    return Ok(c);
                }
                // An overshoot means a duplicate landed (e.g. a label-window
                // expiry replay). Surface it immediately with a clear message.
                if c > expected {
                    return Err(TestBinaryError::InvalidState {
                        message: format!(
                            "Expected exactly {expected} rows in {database}.{table} but observed {c} — a duplicate landed (label-window expiry replay?)"
                        ),
                    });
                }
            }
            sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
        }
        Err(TestBinaryError::InvalidState {
            message: format!(
                "Expected {expected} rows in {database}.{table} but observed {last} after {DEFAULT_POLL_ATTEMPTS} polls"
            ),
        })
    }
}

fn build_connector_envs(fe_url: &str, database: &str) -> HashMap<String, String> {
    use integration::harness::seeds;

    HashMap::from([
        (ENV_SINK_FE_URL.to_string(), fe_url.to_string()),
        (ENV_SINK_DATABASE.to_string(), database.to_string()),
        (ENV_SINK_TABLE.to_string(), DEFAULT_TEST_TABLE.to_string()),
        (ENV_SINK_USERNAME.to_string(), DEFAULT_USER.to_string()),
        (ENV_SINK_PASSWORD.to_string(), DEFAULT_PASSWORD.to_string()),
        (ENV_SINK_LABEL_PREFIX.to_string(), "iggy_test".to_string()),
        (ENV_SINK_BATCH_SIZE.to_string(), "1000".to_string()),
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
            "doris_sink".to_string(),
        ),
        (
            ENV_SINK_PATH.to_string(),
            "../../target/debug/libiggy_connector_doris_sink".to_string(),
        ),
    ])
}

/// Doris fixture where the test is responsible for creating the table
/// (e.g. to exercise different DDL or to verify failure when the table
/// is absent).
pub struct DorisSinkFixture {
    container: DorisContainer,
}

impl DorisOps for DorisSinkFixture {
    fn container(&self) -> &DorisContainer {
        &self.container
    }
}

#[async_trait]
impl TestFixture for DorisSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let container = DorisContainer::start().await?;
        Ok(Self { container })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        build_connector_envs(&self.container.fe_url(), self.container.database())
    }
}

/// Doris fixture where the target table is pre-created during setup.
/// Mirrors `QuickwitPreCreatedFixture` so tests can rely on the table
/// being present from the moment the connector starts.
pub struct DorisSinkPreCreatedFixture {
    inner: DorisSinkFixture,
}

impl std::ops::Deref for DorisSinkPreCreatedFixture {
    type Target = DorisSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DorisOps for DorisSinkPreCreatedFixture {
    fn container(&self) -> &DorisContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkPreCreatedFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = DorisSinkFixture::setup().await?;
        inner
            .create_table(inner.container.database(), DEFAULT_TEST_TABLE)
            .await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

/// Pre-created table fixture that additionally tells the connector to use
/// `max_filter_ratio = 0.5`, so a batch with up to half non-conforming rows
/// still loads the conforming subset.
pub struct DorisSinkMaxFilterRatioFixture {
    inner: DorisSinkPreCreatedFixture,
}

impl std::ops::Deref for DorisSinkMaxFilterRatioFixture {
    type Target = DorisSinkPreCreatedFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DorisOps for DorisSinkMaxFilterRatioFixture {
    fn container(&self) -> &DorisContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkMaxFilterRatioFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = DorisSinkPreCreatedFixture::setup().await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_MAX_FILTER_RATIO.to_string(), "0.5".to_string());
        envs
    }
}

/// Pre-creates a target table whose schema has an extra column (`calculated`)
/// that does NOT exist in the JSON payload, and configures the connector with
/// a `columns` Stream Load header that derives that column from `count`. The
/// load fails without the `columns` config flowing through correctly, so a
/// passing test proves the config wiring end-to-end.
pub struct DorisSinkColumnsMappingFixture {
    inner: DorisSinkFixture,
}

impl std::ops::Deref for DorisSinkColumnsMappingFixture {
    type Target = DorisSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DorisOps for DorisSinkColumnsMappingFixture {
    fn container(&self) -> &DorisContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkColumnsMappingFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = DorisSinkFixture::setup().await?;
        inner
            .create_table_with_template(
                inner.container.database(),
                DEFAULT_TEST_TABLE,
                TEST_TABLE_WITH_CALCULATED_DDL_TEMPLATE,
            )
            .await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(
            ENV_SINK_COLUMNS.to_string(),
            COLUMNS_MAPPING_HEADER.to_string(),
        );
        envs
    }
}

/// Pre-creates the standard test table and configures the connector to emit
/// `format = "csv"`. CSV is positional, so it also sets the matching `columns`
/// header; a passing load proves the connector's JSON->CSV serialization
/// (control-char framing, enclose/escape, column order) round-trips through Doris.
pub struct DorisSinkCsvFixture {
    inner: DorisSinkFixture,
}

impl std::ops::Deref for DorisSinkCsvFixture {
    type Target = DorisSinkFixture;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DorisOps for DorisSinkCsvFixture {
    fn container(&self) -> &DorisContainer {
        self.inner.container()
    }
}

#[async_trait]
impl TestFixture for DorisSinkCsvFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let inner = DorisSinkFixture::setup().await?;
        inner
            .create_table(inner.container.database(), DEFAULT_TEST_TABLE)
            .await?;
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = self.inner.connectors_runtime_envs();
        envs.insert(ENV_SINK_FORMAT.to_string(), "csv".to_string());
        envs.insert(ENV_SINK_COLUMNS.to_string(), CSV_COLUMNS_HEADER.to_string());
        envs
    }
}
