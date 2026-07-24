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

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iggy_common::serde_secret;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use jni::objects::{GlobalRef, JByteArray, JObject, JString, JThrowable, JValue};
use jni::{JNIEnv, JavaVM};
use regex::Regex;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Clear any pending Java exception on the current thread. The JNI spec forbids
/// making most calls while an exception is pending; doing so aborts the whole
/// embedded JVM (the entire connectors-runtime process). Every fallible JNI
/// call in this connector clears on error before returning so a thrown Java
/// exception is never left pending for the next call on this thread. No-op when
/// nothing is pending.
fn clear_pending_exception(env: &mut JNIEnv) {
    let _ = env.exception_clear();
}

/// Best-effort `close()` on a JDBC handle used in error/cleanup paths. Clears
/// any pending exception first (`close()` is a `CallVoidMethod`, which JNI
/// forbids while an exception is pending) and again afterwards in case the
/// close itself throws.
fn best_effort_close(env: &mut JNIEnv, handle: &JObject) {
    clear_pending_exception(env);
    let _ = env.call_method(handle, "close", "()V", &[]);
    clear_pending_exception(env);
}

/// Evaluate a fallible JNI call; on error clear any pending Java exception and
/// return `Error::Connection` with context. Keeps a thrown Java exception from
/// being left pending for the next JNI call on this thread.
macro_rules! jni {
    ($env:expr, $call:expr, $ctx:expr) => {
        match $call {
            Ok(value) => value,
            Err(err) => {
                clear_pending_exception(&mut *$env);
                return Err(Error::Connection(format!("{}: {err}", $ctx)));
            }
        }
    };
}

/// Like [`jni!`] but returns `Error::InitError`, for the connection-setup path.
macro_rules! jni_init {
    ($env:expr, $call:expr, $ctx:expr) => {
        match $call {
            Ok(value) => value,
            Err(err) => {
                clear_pending_exception(&mut *$env);
                return Err(Error::InitError(format!("{}: {err}", $ctx)));
            }
        }
    };
}

/// Lock a mutex, mapping a poisoned lock to a returned `Error` instead of
/// panicking. A thread that panicked while holding the lock then surfaces as a
/// logged, recoverable failure rather than a permanent panic loop on every
/// subsequent poll.
fn lock_mutex<'a, T>(mutex: &'a Mutex<T>, what: &str) -> Result<MutexGuard<'a, T>, Error> {
    mutex
        .lock()
        .map_err(|_| Error::Connection(format!("{what} mutex poisoned")))
}

/// Cached compiled regex patterns for password sanitization
static RE_USER_PASS_AT: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"://([^:]+):([^@?;/]+)@").unwrap());
static RE_PASSWORD_PARAM: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)(password|pwd|pass)=([^;&\s]+)").unwrap());
static RE_ORACLE_PASS: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"thin:([^/]+)/([^@]+)@").unwrap());

/// Regexes that remove the incremental offset predicate `{tracking_column} >
/// {last_offset}` on the first (no-offset) poll while preserving any other
/// `WHERE` conditions. All are case- and whitespace-tolerant. They are applied in
/// order so an operator-added companion condition (e.g.
/// `AND {tracking_column} IS NOT NULL`) does not leave a dangling `WHERE ... AND`
/// or leading `AND ...` fragment. See [`strip_offset_predicate`].
static RE_OFFSET_PREDICATE_AND_AFTER: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    Regex::new(r"(?i)\bWHERE\s+\{tracking_column\}\s*>\s*\{last_offset\}\s+AND\s+").unwrap()
});
static RE_OFFSET_PREDICATE_AND_BEFORE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| {
        Regex::new(r"(?i)\s+AND\s+\{tracking_column\}\s*>\s*\{last_offset\}").unwrap()
    });
static RE_OFFSET_PREDICATE_BARE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    Regex::new(r"(?i)\bWHERE\s+\{tracking_column\}\s*>\s*\{last_offset\}").unwrap()
});

const CONNECTOR_NAME: &str = "JDBC source";

/// Poll interval used when `poll_interval` is unset or empty.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Parse the configured `poll_interval` humantime string into a `Duration`,
/// falling back to [`DEFAULT_POLL_INTERVAL`] when unset, empty, or unparseable.
/// `validate_config` separately rejects a set-but-unparseable value so a typo
/// surfaces at `open()` rather than silently defaulting.
fn parse_poll_interval(poll_interval: Option<&str>) -> Duration {
    poll_interval
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| humantime::parse_duration(value).ok())
        .unwrap_or(DEFAULT_POLL_INTERVAL)
}

/// Source mode for the JDBC connector
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Re-run the query on every poll, capped at `batch_size` rows via JDBC
    /// `setMaxRows`. There is no pagination beyond that cap, so rather than sync a
    /// truncated subset, bulk mode fails closed: the poll probes with
    /// `batch_size + 1` rows and errors if the result set is larger than
    /// `batch_size`. Raise `batch_size` to cover the full table, or use
    /// incremental mode, for tables larger than a batch.
    Bulk,
    /// Track the last offset and fetch only rows beyond it on each poll.
    Incremental,
}

/// Configuration for JDBC source connector
#[derive(Clone, Deserialize, Serialize)]
pub struct JdbcSourceConfig {
    /// JDBC connection URL (e.g., "jdbc:mysql://localhost:3306/mydb")
    /// Can include credentials: "jdbc:mysql://localhost:3306/mydb?user=root&password=secret"
    #[serde(serialize_with = "serde_secret::serialize_secret")]
    pub jdbc_url: SecretString,

    /// JDBC driver class name (e.g., "com.mysql.cj.jdbc.Driver")
    pub driver_class: String,

    /// Path to JDBC driver JAR file
    pub driver_jar_path: String,

    /// Database username (optional if included in jdbc_url)
    #[serde(default)]
    pub username: Option<String>,

    /// Database password (optional if included in jdbc_url)
    #[serde(default, serialize_with = "serde_secret::serialize_optional_secret")]
    pub password: Option<SecretString>,

    /// SQL query to execute for fetching data
    /// Can use {last_offset} placeholder for incremental reads
    pub query: String,

    /// Polling interval as a humantime string (e.g., "30s", "5m", "1h"). Parsed
    /// once at construction; defaults to 5s when unset.
    #[serde(default)]
    pub poll_interval: Option<String>,

    /// Batch size - maximum rows to fetch per poll
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,

    /// Tracking column for incremental reads (e.g., "id", "updated_at")
    #[serde(default)]
    pub tracking_column: Option<String>,

    /// Initial offset value for the first poll
    #[serde(default)]
    pub initial_offset: Option<String>,

    /// Source mode: "bulk" (full table scan) or "incremental" (track last offset)
    #[serde(default = "default_mode")]
    pub mode: Mode,

    /// Convert column names to snake_case
    #[serde(default)]
    pub snake_case_columns: bool,

    /// Include metadata in output (table name, operation type, timestamp)
    #[serde(default = "default_true")]
    pub include_metadata: bool,

    /// JVM options (e.g., ["-Xmx512m", "-Xms128m"])
    #[serde(default)]
    pub jvm_options: Vec<String>,

    /// Timeout for the per-poll `Connection.isValid` liveness check (default:
    /// 5000). JDBC expresses this timeout in whole seconds, so the value is
    /// converted to seconds and clamped to the 1..=5s range (the check runs on a
    /// shared worker and must stay short); it does not govern connection
    /// establishment.
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_ms: u64,
}

fn default_connection_timeout() -> u64 {
    5000
}

fn default_batch_size() -> u32 {
    1000
}

fn default_mode() -> Mode {
    Mode::Incremental
}

fn default_true() -> bool {
    true
}

impl std::fmt::Debug for JdbcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JdbcSourceConfig")
            .field(
                "jdbc_url",
                &sanitize_jdbc_url(self.jdbc_url.expose_secret()),
            )
            .field("driver_class", &self.driver_class)
            .field("driver_jar_path", &self.driver_jar_path)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("query", &self.query)
            .field("poll_interval", &self.poll_interval)
            .field("batch_size", &self.batch_size)
            .field("tracking_column", &self.tracking_column)
            .field("initial_offset", &self.initial_offset)
            .field("mode", &self.mode)
            .field("snake_case_columns", &self.snake_case_columns)
            .field("include_metadata", &self.include_metadata)
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .finish()
    }
}

/// Internal state tracking for the JDBC source
#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    /// Last tracked offset value (for incremental mode)
    last_offset: Option<String>,

    /// Total rows processed
    processed_rows: u64,

    /// Last poll timestamp
    last_poll_time: DateTime<Utc>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            last_offset: None,
            processed_rows: 0,
            last_poll_time: Utc::now(),
        }
    }
}

/// Database record structure for output messages
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseRecord {
    pub table_name: Option<String>,
    pub operation_type: String,
    pub timestamp: DateTime<Utc>,
    pub data: serde_json::Value,
}

/// JDBC Source Connector
#[derive(Debug)]
pub struct JdbcSource {
    id: u32,
    config: JdbcSourceConfig,
    jvm: Option<Arc<JavaVM>>,
    // Behind a Mutex so `poll()` (&self) can transparently re-establish a dead
    // direct connection without `&mut self`.
    connection: Mutex<Option<GlobalRef>>,
    state: Arc<Mutex<State>>,
    // Poll interval parsed once from `config.poll_interval` at construction.
    poll_interval: Duration,
    // Scheduled start of the next poll, used to pace polls at a fixed cadence
    // that does not drift with per-poll work time. `None` until the first poll.
    next_poll_at: Mutex<Option<Instant>>,
}

/// Sanitize JDBC URL by masking passwords for logging
fn sanitize_jdbc_url(url: &str) -> String {
    // Pattern 1: user:password@host format (MySQL, PostgreSQL)
    let url = RE_USER_PASS_AT.replace_all(url, "://$1:***@");

    // Pattern 2: password=value format (PostgreSQL, SQL Server, H2)
    let url = RE_PASSWORD_PARAM.replace_all(&url, "$1=***");

    // Pattern 3: Oracle user/password@host format
    let url = RE_ORACLE_PASS.replace_all(&url, "thin:$1/***@");

    url.to_string()
}

impl JdbcSource {
    /// Create a new JDBC source connector
    pub fn new(id: u32, config: JdbcSourceConfig, connector_state: Option<ConnectorState>) -> Self {
        // Restore state from persistent storage if available
        let state = connector_state
            .and_then(|cs| cs.deserialize::<State>(CONNECTOR_NAME, id))
            .unwrap_or_else(|| {
                let mut default_state = State::default();
                // Use initial_offset from config if provided
                if let Some(ref initial_offset) = config.initial_offset {
                    default_state.last_offset = Some(initial_offset.clone());
                }
                default_state
            });

        let poll_interval = parse_poll_interval(config.poll_interval.as_deref());
        Self {
            id,
            config,
            jvm: None,
            connection: Mutex::new(None),
            state: Arc::new(Mutex::new(state)),
            poll_interval,
            next_poll_at: Mutex::new(None),
        }
    }

    /// Obtain the process-wide JVM, creating it on first use. JNI permits only a
    /// single JVM per OS process, so this is shared across all JDBC connector
    /// instances (see [`get_or_create_jvm`]).
    fn initialize_jvm(&mut self) -> Result<(), Error> {
        info!("Initializing JVM for JDBC source connector [{}]", self.id);
        let jvm = get_or_create_jvm(&self.config.driver_jar_path, &self.config.jvm_options)?;
        self.jvm = Some(jvm);
        Ok(())
    }

    /// Load the JDBC driver and open the database connection.
    fn create_connection(&mut self) -> Result<(), Error> {
        let jvm = self
            .jvm
            .as_ref()
            .ok_or_else(|| Error::InitError("JVM not initialized".to_string()))?;

        let mut env = jvm
            .attach_current_thread()
            .map_err(|e| Error::InitError(format!("Failed to attach thread to JVM: {}", e)))?;

        info!("Loading JDBC driver: {}", self.config.driver_class);

        // Load driver using Class.forName() which triggers static initialization
        info!(
            "Loading driver class via Class.forName: {}",
            self.config.driver_class
        );

        let class_class = jni_init!(
            env,
            env.find_class("java/lang/Class"),
            "Failed to find Class"
        );

        let driver_class_name = jni_init!(
            env,
            env.new_string(&self.config.driver_class),
            "Failed to create class name string"
        );

        // Call Class.forName(className) to load and initialize the driver
        jni_init!(
            env,
            env.call_static_method(
                class_class,
                "forName",
                "(Ljava/lang/String;)Ljava/lang/Class;",
                &[JValue::Object(&driver_class_name.into())],
            ),
            format!("Failed to load driver class '{}'", self.config.driver_class)
        );

        info!("JDBC driver loaded and registered successfully");

        info!(
            "Creating direct JDBC connection to: {}",
            sanitize_jdbc_url(self.config.jdbc_url.expose_secret())
        );
        let conn = self.create_direct_connection_internal(&mut env)?;
        *lock_mutex(&self.connection, "connection")? = Some(conn);

        Ok(())
    }

    /// Create a direct JDBC connection via DriverManager, inside its own JNI
    /// local-reference frame so the transient class/loader/URL locals do not
    /// accumulate on the caller's frame. The returned handle is a `GlobalRef`, so
    /// it survives the frame pop.
    fn create_direct_connection_internal(&self, env: &mut JNIEnv) -> Result<GlobalRef, Error> {
        env.push_local_frame(16)
            .map_err(|e| Error::InitError(format!("Failed to push local frame: {e}")))?;
        let result = self.create_direct_connection_inner(env);
        // SAFETY: `result` holds only a global reference (or an error); no JNI
        // local reference escapes the frame.
        let _ = unsafe { env.pop_local_frame(&JObject::null()) };
        result
    }

    fn create_direct_connection_inner(&self, env: &mut JNIEnv) -> Result<GlobalRef, Error> {
        // Set the thread context class loader to help DriverManager find the driver
        let current_thread_class = jni_init!(
            env,
            env.find_class("java/lang/Thread"),
            "Failed to find Thread class"
        );

        let current_thread = jni_init!(
            env,
            env.call_static_method(
                current_thread_class,
                "currentThread",
                "()Ljava/lang/Thread;",
                &[],
            )
            .and_then(|v| v.l()),
            "Failed to get current thread"
        );

        // Get the class loader that loaded the driver
        let driver_class = jni_init!(
            env,
            env.find_class(self.config.driver_class.replace('.', "/")),
            "Failed to find driver class"
        );

        let driver_class_loader = jni_init!(
            env,
            env.call_method(
                &driver_class,
                "getClassLoader",
                "()Ljava/lang/ClassLoader;",
                &[],
            )
            .and_then(|v| v.l()),
            "Failed to get driver class loader"
        );

        // Set the context class loader
        jni_init!(
            env,
            env.call_method(
                &current_thread,
                "setContextClassLoader",
                "(Ljava/lang/ClassLoader;)V",
                &[JValue::Object(&driver_class_loader)],
            ),
            "Failed to set context class loader"
        );

        info!(
            "Set thread context class loader for driver: {}",
            self.config.driver_class
        );

        // Get connection from DriverManager
        let driver_manager = jni_init!(
            env,
            env.find_class("java/sql/DriverManager"),
            "Failed to find DriverManager"
        );

        let jdbc_url = jni_init!(
            env,
            env.new_string(self.config.jdbc_url.expose_secret()),
            "Failed to create JDBC URL string"
        );

        // If username/password are provided separately, use 3-arg getConnection
        let connection_obj = if let (Some(username), Some(password)) =
            (&self.config.username, &self.config.password)
        {
            info!("Using separate username/password authentication");
            let username_jstring = jni_init!(
                env,
                env.new_string(username),
                "Failed to create username string"
            );
            let password_jstring = jni_init!(
                env,
                env.new_string(password.expose_secret()),
                "Failed to create password string"
            );

            jni_init!(
                env,
                env.call_static_method(
                    driver_manager,
                    "getConnection",
                    "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/sql/Connection;",
                    &[
                        JValue::Object(&jdbc_url.into()),
                        JValue::Object(&username_jstring.into()),
                        JValue::Object(&password_jstring.into()),
                    ],
                )
                .and_then(|v| v.l()),
                "Failed to create JDBC connection with credentials"
            )
        } else {
            info!("Using connection string with embedded credentials");
            jni_init!(
                env,
                env.call_static_method(
                    driver_manager,
                    "getConnection",
                    "(Ljava/lang/String;)Ljava/sql/Connection;",
                    &[JValue::Object(&jdbc_url.into())],
                )
                .and_then(|v| v.l()),
                "Failed to create JDBC connection from URL"
            )
        };

        let global_ref = env
            .new_global_ref(connection_obj)
            .map_err(|e| Error::InitError(format!("Failed to create global reference: {e}")))?;

        info!("Direct database connection established successfully");
        Ok(global_ref)
    }

    /// Acquire the direct connection, transparently re-establishing it if it has
    /// dropped since the last poll.
    fn get_connection<'local>(&self, env: &mut JNIEnv<'local>) -> Result<JObject<'local>, Error> {
        // Hold the connection lock across the whole check/close/create/store
        // sequence so correctness does not depend on there being a single caller:
        // a concurrent caller can no longer observe or replace the handle
        // mid-reconnect. The lock is a std Mutex held only across synchronous JNI
        // work (no .await), and neither `connection_is_valid` nor
        // `create_direct_connection_internal` re-locks it, so this cannot deadlock.
        let mut guard = lock_mutex(&self.connection, "connection")?;

        let needs_reconnect = match guard.as_ref() {
            Some(conn) => !self.connection_is_valid(env, conn.as_obj()),
            None => true,
        };

        if needs_reconnect {
            info!("Direct JDBC connection is not valid; re-establishing");
            // Best-effort close of the old handle, then drop it before creating
            // the replacement so a failed reconnect leaves no stale reference.
            if let Some(old) = guard.as_ref() {
                best_effort_close(env, old.as_obj());
            }
            *guard = None;
            *guard = Some(self.create_direct_connection_internal(env)?);
        }

        let conn = guard
            .as_ref()
            .ok_or_else(|| Error::Connection("No connection available".to_string()))?;
        let local_ref = env
            .new_local_ref(conn.as_obj())
            .map_err(|e| Error::Connection(format!("Failed to create local ref: {e}")))?;
        Ok(local_ref)
    }

    /// Best-effort `Connection.isValid(timeout)` check. Returns false on any
    /// JNI error so the caller re-establishes the connection.
    fn connection_is_valid(&self, env: &mut JNIEnv, conn: &JObject) -> bool {
        // Cap the liveness check short: it runs on a shared block_in_place worker,
        // so a dead connection must not block it for tens of seconds.
        let timeout_secs = (self.config.connection_timeout_ms / 1000).clamp(1, 5) as i32;
        match env
            .call_method(conn, "isValid", "(I)Z", &[JValue::Int(timeout_secs)])
            .and_then(|v| v.z())
        {
            Ok(valid) => valid,
            Err(_) => {
                // isValid may throw; clear so the reconnect path's next JNI call
                // is not made with an exception pending.
                clear_pending_exception(env);
                false
            }
        }
    }

    /// Execute query and fetch results.
    ///
    /// The mutex is held only briefly: once to read the current offset for
    /// query building, and once after the JNI work to write the updated state.
    fn execute_query(&self, env: &mut JNIEnv) -> Result<Vec<ProducedMessage>, Error> {
        let connection = self.get_connection(env)?;

        // Read current state snapshot (short lock)
        let query = {
            let state = lock_mutex(&self.state, "state")?;
            self.build_query(&state)
        }?;
        // Logged at debug: the built query embeds the substituted offset value.
        debug!("Executing query: {}", query);

        let (messages, row_count, max_offset) =
            self.execute_statement_and_fetch_rows(env, &connection, &query)?;

        // Fail closed on bulk truncation. The bulk fetch probes with batch_size+1
        // rows, so seeing more than batch_size means the result set is larger than
        // one batch and bulk mode (which has no pagination) would otherwise sync
        // only an arbitrary subset. Erroring surfaces the misconfiguration instead
        // of silently dropping rows. Full cross-database OFFSET pagination is a
        // separate follow-up.
        if self.config.mode == Mode::Bulk && row_count > self.config.batch_size as u64 {
            return Err(Error::InvalidConfigValue(format!(
                "bulk query returned more than batch_size ({}) rows; bulk mode does not paginate \
                 and would sync only a truncated subset. Increase batch_size to cover the full \
                 result set, or use incremental mode with an ordered tracking_column.",
                self.config.batch_size
            )));
        }

        // Update state with results (short lock)
        {
            let mut state = lock_mutex(&self.state, "state")?;
            if let Some(offset) = max_offset {
                state.last_offset = Some(offset);
            }
            state.processed_rows += row_count;
            state.last_poll_time = Utc::now();
            info!(
                "Fetched {} rows, total processed: {}",
                row_count, state.processed_rows
            );
        }

        Ok(messages)
    }

    /// Prepare a JDBC statement, execute it, and read all result rows into messages.
    fn execute_statement_and_fetch_rows(
        &self,
        env: &mut JNIEnv,
        connection: &JObject,
        query: &str,
    ) -> Result<(Vec<ProducedMessage>, u64, Option<String>), Error> {
        let query_jstring = jni!(env, env.new_string(query), "Failed to create query string");

        let statement = match env
            .call_method(
                connection,
                "prepareStatement",
                "(Ljava/lang/String;)Ljava/sql/PreparedStatement;",
                &[JValue::Object(&query_jstring.into())],
            )
            .and_then(|v| v.l())
        {
            Ok(s) => s,
            Err(_) => return Err(classify_query_failure(env, "prepare statement")),
        };

        // Use setMaxRows for database-agnostic row limiting instead of SQL LIMIT clause.
        // This works across all JDBC drivers (MySQL, Oracle, SQL Server, H2, etc.).
        // In bulk mode fetch one extra row so the caller can detect an oversized
        // (truncated) result set and fail closed rather than sync an arbitrary
        // subset; incremental mode pages via the offset, so batch_size is the cap.
        let max_rows = match self.config.mode {
            Mode::Bulk => self.config.batch_size.saturating_add(1),
            Mode::Incremental => self.config.batch_size,
        };
        if let Err(err) = env.call_method(
            &statement,
            "setMaxRows",
            "(I)V",
            &[JValue::Int(max_rows.min(i32::MAX as u32) as i32)],
        ) {
            best_effort_close(env, &statement);
            return Err(Error::Connection(format!("Failed to set max rows: {err}")));
        }

        let result_set = match env
            .call_method(&statement, "executeQuery", "()Ljava/sql/ResultSet;", &[])
            .and_then(|v| v.l())
        {
            Ok(rs) => rs,
            Err(_) => {
                // Read and classify the pending SQLException FIRST: this clears
                // it, which then makes the statement close() safe (close() is a
                // JNI call and must not run with an exception pending).
                let error = classify_query_failure(env, "execute query");
                best_effort_close(env, &statement);
                return Err(error);
            }
        };

        // On any read error the callees clear the pending exception; close the
        // statement here before propagating so it is not leaked.
        let columns = match self.read_column_metadata(env, &result_set) {
            Ok(columns) => columns,
            Err(err) => {
                best_effort_close(env, &statement);
                return Err(err);
            }
        };
        let (messages, row_count, max_offset) = match self.read_rows(env, &result_set, &columns) {
            Ok(result) => result,
            Err(err) => {
                best_effort_close(env, &statement);
                return Err(err);
            }
        };

        // Close statement (best-effort)
        best_effort_close(env, &statement);

        Ok((messages, row_count, max_offset))
    }

    /// Read column names and types from the ResultSet metadata.
    fn read_column_metadata(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
    ) -> Result<Vec<(String, i32)>, Error> {
        // Read all column metadata inside its own JNI local frame so the
        // metadata object and per-column name references are reclaimed; a very
        // wide table would otherwise accumulate one local ref per column on the
        // outer frame for the whole poll.
        env.push_local_frame(16)
            .map_err(|e| Error::Connection(format!("Failed to push local frame: {}", e)))?;
        let result = self.read_column_metadata_inner(env, result_set);
        // SAFETY: the returned Vec is owned Rust data; no JNI reference escapes.
        let _ = unsafe { env.pop_local_frame(&JObject::null()) };
        result
    }

    fn read_column_metadata_inner(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
    ) -> Result<Vec<(String, i32)>, Error> {
        let metadata = jni!(
            env,
            env.call_method(
                result_set,
                "getMetaData",
                "()Ljava/sql/ResultSetMetaData;",
                &[],
            )
            .and_then(|v| v.l()),
            "Failed to get metadata"
        );

        let column_count = jni!(
            env,
            env.call_method(&metadata, "getColumnCount", "()I", &[])
                .and_then(|v| v.i()),
            "Failed to get column count"
        );

        info!("Query returned {} columns", column_count);

        // Clamp a driver-supplied count before using it as an allocation size: a
        // negative i32 would sign-extend to an enormous usize and abort on alloc.
        let mut columns = Vec::with_capacity((column_count.max(0) as usize).min(8192));
        for i in 1..=column_count {
            let col_name = self.get_column_name(env, &metadata, i)?;
            let col_type = self.get_column_type(env, &metadata, i)?;
            columns.push((col_name, col_type));
        }

        Ok(columns)
    }

    /// Iterate over result set rows and convert each to a ProducedMessage.
    fn read_rows(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
        columns: &[(String, i32)],
    ) -> Result<(Vec<ProducedMessage>, u64, Option<String>), Error> {
        // setMaxRows caps the result set at batch_size, so that is the known
        // upper bound; clamp the pre-allocation so an extreme batch_size cannot
        // request an absurd allocation up front.
        let mut messages = Vec::with_capacity((self.config.batch_size as usize).min(8192));
        let mut row_count: u64 = 0;
        let mut last_offset: Option<String> = None;

        loop {
            let has_next = jni!(
                env,
                env.call_method(result_set, "next", "()Z", &[])
                    .and_then(|v| v.z()),
                "Failed to fetch next row"
            );

            if !has_next {
                break;
            }

            // Read each row inside its own JNI local-reference frame so the per
            // -column local refs (getObject/getString/getBytes results) are
            // reclaimed every iteration; otherwise a large result set would
            // overflow the JNI local reference table and abort the JVM.
            env.push_local_frame(32)
                .map_err(|e| Error::Connection(format!("Failed to push local frame: {}", e)))?;
            let row_result = self.read_single_row(env, result_set, columns);
            // SAFETY: `read_single_row` returns only owned Rust data (a JSON map
            // and an optional String); no JNI local reference escapes the frame.
            let _ = unsafe { env.pop_local_frame(&JObject::null()) };
            let (row_data, offset) = row_result?;

            // Take the tracking value of the LAST row as the next offset. Rows
            // arrive in ascending tracking order (validate_config enforces
            // ORDER BY the tracking column ascending), so the last row is the
            // high-water mark. Using the last row rather than a Rust-side max
            // keeps the cursor consistent with the database's own ordering, and
            // degrades to re-reads (safe, at-least-once) rather than skips if the
            // ordering is ever imperfect.
            if let Some(offset) = offset {
                last_offset = Some(offset);
            }

            let message = self.build_message(row_data)?;
            messages.push(message);
            row_count += 1;
        }

        // In incremental mode a non-empty batch that never yielded a tracking
        // value means the tracking column is absent from the result set: the
        // offset could never advance, so the same batch would be re-read forever.
        // Fail loudly instead of stalling silently. (A NULL tracking value in a
        // present column is already rejected per-row by tracking_offset_or_error.)
        if self.config.mode == Mode::Incremental && row_count > 0 && last_offset.is_none() {
            return Err(Error::InvalidConfigValue(format!(
                "tracking column '{}' is not present in the query result; incremental mode cannot \
                 advance its offset. Include it in the SELECT list.",
                self.config.tracking_column.as_deref().unwrap_or("")
            )));
        }

        Ok((messages, row_count, last_offset))
    }

    /// Extract data from a single result set row, returning the row map and optional offset.
    fn read_single_row(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
        columns: &[(String, i32)],
    ) -> Result<(serde_json::Map<String, serde_json::Value>, Option<String>), Error> {
        let mut row_data = serde_json::Map::new();
        let mut offset = None;

        for (idx, (col_name, col_type)) in columns.iter().enumerate() {
            let col_idx = (idx + 1) as i32;
            let value = self.extract_column_value(env, result_set, col_idx, col_type)?;

            let final_col_name = if self.config.snake_case_columns {
                to_snake_case(col_name)
            } else {
                col_name.clone()
            };

            // A snake_case conversion (or a query with duplicate labels) can map
            // two source columns onto the same key; the later value silently wins.
            // Warn so the loss is diagnosable rather than invisible.
            if row_data.contains_key(&final_col_name) {
                warn!(
                    "Column '{col_name}' maps to key '{final_col_name}', which already exists in the row; the earlier value is overwritten"
                );
            }

            row_data.insert(final_col_name.clone(), value);

            // Track offset from the first column matching the tracking column
            // (by driver label or normalized key, case-insensitively). Only the
            // first match counts: a collapsed/duplicate label must not let a later
            // column silently overwrite the offset with an unrelated value.
            if offset.is_none()
                && let Some(ref tracking_col) = self.config.tracking_column
                && tracking_column_matches(tracking_col, col_name, &final_col_name)
            {
                let value = self.extract_offset_value(&row_data, &final_col_name);
                offset = self.tracking_offset_or_error(value, tracking_col)?;
            }
        }

        Ok((row_data, offset))
    }

    /// Resolve a tracking-column value into an offset. In incremental mode a NULL
    /// or empty value is a hard error: the row is emitted but the offset cannot
    /// advance past NULL, so it would be re-read (and re-emitted) every poll.
    fn tracking_offset_or_error(
        &self,
        value: Option<String>,
        tracking_col: &str,
    ) -> Result<Option<String>, Error> {
        match value {
            Some(value) => Ok(Some(value)),
            None if self.config.mode == Mode::Incremental => {
                Err(Error::InvalidRecordValue(format!(
                    "tracking column '{tracking_col}' is NULL or empty in a returned row; incremental \
                 mode cannot advance its offset past NULL values. Exclude them in the query, e.g. \
                 add `AND {tracking_col} IS NOT NULL`."
                )))
            }
            None => Ok(None),
        }
    }

    /// Build a ProducedMessage from row data, optionally wrapping in DatabaseRecord metadata.
    fn build_message(
        &self,
        row_data: serde_json::Map<String, serde_json::Value>,
    ) -> Result<ProducedMessage, Error> {
        let now = Utc::now();
        let payload = if self.config.include_metadata {
            let record = DatabaseRecord {
                table_name: None,
                operation_type: "SELECT".to_string(),
                timestamp: now,
                data: serde_json::Value::Object(row_data),
            };
            serde_json::to_vec(&record)
                .map_err(|e| Error::Serialization(format!("Failed to serialize record: {e}")))?
        } else {
            serde_json::to_vec(&serde_json::Value::Object(row_data))
                .map_err(|e| Error::Serialization(format!("Failed to serialize row data: {e}")))?
        };

        let now_ms = now.timestamp_millis() as u64;
        Ok(ProducedMessage {
            id: Some(Uuid::new_v4().as_u128()),
            payload,
            headers: None,
            checksum: None,
            timestamp: Some(now_ms),
            origin_timestamp: Some(now_ms),
        })
    }

    /// Build the query for this poll by substituting the `{tracking_column}` and
    /// `{last_offset}` placeholders. Row limiting is handled via JDBC setMaxRows
    /// rather than SQL LIMIT to ensure cross-database compatibility.
    fn build_query(&self, state: &State) -> Result<String, Error> {
        let mut query = self.config.query.clone();

        if self.config.mode != Mode::Incremental {
            return finalize_query(query);
        }

        let offset = state
            .last_offset
            .as_deref()
            .or(self.config.initial_offset.as_deref());

        // Without an offset yet, drop the incremental predicate but keep the rest
        // of the query (e.g. an ORDER BY) intact.
        if offset.is_none() {
            query = strip_offset_predicate(&query);
        }

        // Substitute {tracking_column} wherever it still appears (a WHERE and/or
        // an ORDER BY), validating it as a plain identifier to avoid injection.
        if query.contains("{tracking_column}") {
            let column = self.config.tracking_column.as_deref().ok_or_else(|| {
                Error::InvalidConfigValue(
                    "query uses {tracking_column} but tracking_column is not set".to_string(),
                )
            })?;
            if !is_valid_identifier(column) {
                return Err(Error::InvalidConfigValue(format!(
                    "tracking_column '{column}' is not a valid SQL identifier"
                )));
            }
            query = query.replace("{tracking_column}", column);
        }

        // Substitute the offset value (quoted and escaped) when we have one.
        if let Some(offset) = offset {
            query = query.replace("{last_offset}", &quote_sql_literal(offset));
        }

        finalize_query(query)
    }

    /// Get column name from ResultSetMetaData
    fn get_column_name(
        &self,
        env: &mut JNIEnv,
        metadata: &JObject,
        column_index: i32,
    ) -> Result<String, Error> {
        let col_name_obj = jni!(
            env,
            env.call_method(
                metadata,
                "getColumnName",
                "(I)Ljava/lang/String;",
                &[JValue::Int(column_index)],
            )
            .and_then(|v| v.l()),
            "Failed to get column name"
        );

        let col_name: String = jni!(
            env,
            env.get_string(&JString::from(col_name_obj)),
            "Failed to convert column name"
        )
        .into();

        Ok(col_name)
    }

    /// Get column type from ResultSetMetaData
    fn get_column_type(
        &self,
        env: &mut JNIEnv,
        metadata: &JObject,
        column_index: i32,
    ) -> Result<i32, Error> {
        let col_type = jni!(
            env,
            env.call_method(
                metadata,
                "getColumnType",
                "(I)I",
                &[JValue::Int(column_index)],
            )
            .and_then(|v| v.i()),
            "Failed to get column type"
        );

        Ok(col_type)
    }

    /// Return `value`, or JSON `null` when the last primitive getter read a SQL
    /// NULL (detected via `ResultSet.wasNull()`).
    fn null_or(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let was_null = jni!(
            env,
            env.call_method(result_set, "wasNull", "()Z", &[])
                .and_then(|v| v.z()),
            "Failed to check wasNull"
        );
        Ok(if was_null {
            serde_json::Value::Null
        } else {
            value
        })
    }

    /// Extract column value based on JDBC type
    fn extract_column_value(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
        column_index: i32,
        sql_type: &i32,
    ) -> Result<serde_json::Value, Error> {
        use java::sql::Types;

        // Primitive getters (getInt/getBoolean/...) return 0/false for SQL NULL,
        // so `null_or` consults ResultSet.wasNull() after the getter to tell an
        // actual NULL from a zero value. Object getters (getString/getBytes)
        // return a null reference for SQL NULL and are null-checked directly, so
        // there is no separate getObject probe (one JNI call per column, not two).
        match *sql_type {
            Types::BIT | Types::BOOLEAN => {
                let value = jni!(
                    env,
                    env.call_method(
                        result_set,
                        "getBoolean",
                        "(I)Z",
                        &[JValue::Int(column_index)]
                    )
                    .and_then(|v| v.z()),
                    "Failed to get boolean"
                );
                self.null_or(env, result_set, serde_json::Value::Bool(value))
            }
            Types::TINYINT | Types::SMALLINT | Types::INTEGER => {
                let value = jni!(
                    env,
                    env.call_method(result_set, "getInt", "(I)I", &[JValue::Int(column_index)])
                        .and_then(|v| v.i()),
                    "Failed to get int"
                );
                self.null_or(env, result_set, serde_json::json!(value))
            }
            Types::BIGINT => {
                let value = jni!(
                    env,
                    env.call_method(result_set, "getLong", "(I)J", &[JValue::Int(column_index)])
                        .and_then(|v| v.j()),
                    "Failed to get long"
                );
                self.null_or(env, result_set, serde_json::json!(value))
            }
            Types::FLOAT | Types::REAL => {
                let value = jni!(
                    env,
                    env.call_method(result_set, "getFloat", "(I)F", &[JValue::Int(column_index)])
                        .and_then(|v| v.f()),
                    "Failed to get float"
                );
                self.null_or(env, result_set, serde_json::json!(value))
            }
            Types::DOUBLE => {
                let value = jni!(
                    env,
                    env.call_method(
                        result_set,
                        "getDouble",
                        "(I)D",
                        &[JValue::Int(column_index)]
                    )
                    .and_then(|v| v.d()),
                    "Failed to get double"
                );
                self.null_or(env, result_set, serde_json::json!(value))
            }
            // NUMERIC/DECIMAL can carry more precision than an f64 can represent
            // (e.g. money/large decimals), so emit them as strings to avoid
            // silent precision loss.
            Types::NUMERIC | Types::DECIMAL => {
                self.get_column_as_string(env, result_set, column_index)
            }
            // Binary columns are base64-encoded so arbitrary bytes survive the
            // round-trip through JSON.
            Types::BINARY | Types::VARBINARY | Types::LONGVARBINARY => {
                let bytes_obj = jni!(
                    env,
                    env.call_method(
                        result_set,
                        "getBytes",
                        "(I)[B",
                        &[JValue::Int(column_index)]
                    )
                    .and_then(|v| v.l()),
                    "Failed to get bytes"
                );
                if bytes_obj.is_null() {
                    return Ok(serde_json::Value::Null);
                }
                let buf = jni!(
                    env,
                    env.convert_byte_array(JByteArray::from(bytes_obj)),
                    "Failed to convert bytes"
                );
                use base64::Engine;
                Ok(serde_json::Value::String(
                    base64::engine::general_purpose::STANDARD.encode(&buf),
                ))
            }
            // Date/time types are read via their driver string form. Route
            // through the null-safe getString path so a NULL date/time yields
            // JSON null instead of failing the whole poll on get_string(null).
            Types::TIMESTAMP | Types::DATE | Types::TIME => {
                self.get_column_as_string(env, result_set, column_index)
            }
            // Default: getString for all other types (CHAR, VARCHAR, etc.)
            _ => self.get_column_as_string(env, result_set, column_index),
        }
    }

    /// Read a column via `ResultSet.getString`, returning JSON `null` when the
    /// value is SQL NULL.
    fn get_column_as_string(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
        column_index: i32,
    ) -> Result<serde_json::Value, Error> {
        let value = jni!(
            env,
            env.call_method(
                result_set,
                "getString",
                "(I)Ljava/lang/String;",
                &[JValue::Int(column_index)],
            )
            .and_then(|v| v.l()),
            "Failed to get string"
        );

        if value.is_null() {
            Ok(serde_json::Value::Null)
        } else {
            let str_value: String = jni!(
                env,
                env.get_string(&JString::from(value)),
                "Failed to convert string"
            )
            .into();
            Ok(serde_json::Value::String(str_value))
        }
    }

    /// Extract the tracking-column value as a string offset, or `None` when it is
    /// SQL NULL / empty / not comparable. In incremental mode a `None` here is
    /// turned into a hard error by [`Self::tracking_offset_or_error`] (a NULL
    /// tracking value cannot be watermarked), so the tracking column must be
    /// NOT NULL; see the README.
    fn extract_offset_value(
        &self,
        row_data: &serde_json::Map<String, serde_json::Value>,
        col_name: &str,
    ) -> Option<String> {
        match row_data.get(col_name) {
            Some(serde_json::Value::Number(n)) => Some(n.to_string()),
            Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        }
    }

    /// Validate configuration before touching the JVM or the database, so bad
    /// config surfaces immediately at `open()` with an actionable message rather
    /// than as an opaque wrapped JVM error or only after the first poll sleep.
    fn validate_config(&self) -> Result<(), Error> {
        // The driver JAR must exist; a missing path otherwise surfaces as an
        // opaque ClassNotFound wrapped deep inside JVM startup.
        if !std::path::Path::new(&self.config.driver_jar_path).exists() {
            return Err(Error::InvalidConfigValue(format!(
                "driver_jar_path '{}' does not exist; set it to the path of the JDBC driver JAR",
                self.config.driver_jar_path
            )));
        }

        // batch_size drives JDBC setMaxRows. Zero means "no limit" to JDBC, which
        // would defeat both the row cap and the bulk truncation probe, so require
        // at least 1. Cap below i32::MAX so the bulk `batch_size + 1` probe still
        // fits in the i32 setMaxRows takes and stays distinguishable from a full
        // batch.
        const MAX_BATCH_SIZE: u32 = i32::MAX as u32 - 1;
        if self.config.batch_size == 0 || self.config.batch_size > MAX_BATCH_SIZE {
            return Err(Error::InvalidConfigValue(format!(
                "batch_size must be between 1 and {MAX_BATCH_SIZE}, got {}",
                self.config.batch_size
            )));
        }

        // A set poll_interval must be a valid humantime string; an unparseable
        // value would otherwise silently fall back to the default.
        if let Some(value) = self.config.poll_interval.as_deref()
            && !value.trim().is_empty()
            && humantime::parse_duration(value.trim()).is_err()
        {
            return Err(Error::InvalidConfigValue(format!(
                "poll_interval '{value}' is not a valid duration (e.g. \"30s\", \"5m\", \"1h\")"
            )));
        }

        // The query must be non-empty; an empty query only fails later at
        // prepareStatement with an opaque driver error.
        if self.config.query.trim().is_empty() {
            return Err(Error::InvalidConfigValue(
                "query must not be empty".to_string(),
            ));
        }

        // A set initial_offset must be non-blank: an empty value would build
        // `WHERE tracking > ''` (a type error or always-false on many databases)
        // rather than the intended cold-start scan.
        if let Some(initial_offset) = self.config.initial_offset.as_deref()
            && initial_offset.trim().is_empty()
        {
            return Err(Error::InvalidConfigValue(
                "initial_offset must not be empty; omit it to start from the beginning".to_string(),
            ));
        }

        // Separate-credential auth requires both username and password; a
        // half-set pair would silently fall through to URL-embedded credentials.
        if self.config.username.is_some() != self.config.password.is_some() {
            return Err(Error::InvalidConfigValue(
                "username and password must both be set (for separate authentication) or both be \
                 unset (to use credentials embedded in the JDBC URL)"
                    .to_string(),
            ));
        }

        // Incremental mode invariants. Without a tracking column the offset can
        // never advance, so every poll re-reads the same rows. Without ordering
        // by that column, setMaxRows returns an arbitrary subset, and advancing
        // the offset to its max permanently skips the unread lower keys.
        if self.config.mode == Mode::Incremental {
            let Some(tracking_column) = self
                .config
                .tracking_column
                .as_deref()
                .filter(|column| !column.trim().is_empty())
            else {
                return Err(Error::InvalidConfigValue(
                    "incremental mode requires a non-empty tracking_column so the offset can \
                     advance; set tracking_column, or use mode = \"bulk\""
                        .to_string(),
                ));
            };
            if !query_orders_by_tracking_column(&self.config.query, tracking_column) {
                return Err(Error::InvalidConfigValue(format!(
                    "incremental mode requires the query to order by the tracking column so each \
                     batch is a contiguous ascending range; add `ORDER BY {tracking_column}` (or \
                     `ORDER BY {{tracking_column}}`) to the query"
                )));
            }
        }

        // Dry-run the query build so an unresolved placeholder or an invalid
        // tracking_column fails now instead of after the first poll interval.
        let state = lock_mutex(&self.state, "state")?;
        self.build_query(&state)?;
        Ok(())
    }
}

#[async_trait]
impl Source for JdbcSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!("Opening JDBC source connector [{}]", self.id);
        info!(
            "Configuration: JDBC URL={}, Driver={}, Mode={:?}",
            sanitize_jdbc_url(self.config.jdbc_url.expose_secret()),
            self.config.driver_class,
            self.config.mode
        );

        // Fail fast on bad config before starting the JVM or opening a connection.
        self.validate_config()?;

        // Initialize JVM
        self.initialize_jvm()?;

        // Create database connection
        self.create_connection()?;

        info!("JDBC source connector [{}] opened successfully", self.id);
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // Pace polls on a fixed cadence measured from a scheduled start instant,
        // so per-poll work time does not accumulate as drift and the first poll
        // is not delayed by a full interval. The schedule is clamped forward to
        // `now` whenever it has fallen behind (a long pause, a poll that overran
        // the interval, or the runtime re-polling immediately after an error),
        // so a lagging schedule can never collapse the sleep into a busy loop
        // that hammers the database.
        let scheduled = {
            let mut next = lock_mutex(&self.next_poll_at, "next_poll_at")?;
            let scheduled = next.map_or_else(Instant::now, |planned| planned.max(Instant::now()));
            *next = Some(scheduled + self.poll_interval);
            scheduled
        };
        let now = Instant::now();
        if scheduled > now {
            tokio::time::sleep(scheduled - now).await;
        }

        // The JDBC/JNI fetch is synchronous, blocking work; run it via
        // block_in_place so it does not monopolize a shared async-runtime worker
        // while other connectors need to make progress. The connectors runtime is
        // multi-threaded, which block_in_place requires.
        let messages = tokio::task::block_in_place(|| -> Result<Vec<ProducedMessage>, Error> {
            let jvm = self
                .jvm
                .as_ref()
                .ok_or_else(|| Error::InitError("JVM not initialized".to_string()))?;
            let mut env = jvm
                .attach_current_thread()
                .map_err(|e| Error::InitError(format!("Failed to attach thread: {e}")))?;
            // Defensive: clear any exception left pending by a prior failed poll
            // on this thread before issuing JNI calls.
            clear_pending_exception(&mut env);
            // Bound this poll's local references (the connection local ref, the
            // query string, and the statement/result-set handles) to a frame
            // reclaimed when the poll returns. A tokio worker thread stays
            // attached to the JVM across polls (attach_current_thread returns a
            // no-detach nested guard once attached), so without this frame those
            // per-poll locals accumulate on the thread's top-level frame and
            // eventually overflow the JNI local reference table, aborting the JVM.
            env.push_local_frame(16)
                .map_err(|e| Error::Connection(format!("Failed to push local frame: {e}")))?;
            let result = self.execute_query(&mut env);
            // SAFETY: execute_query returns only owned Rust data (messages); no
            // JNI local reference escapes the frame.
            let _ = unsafe { env.pop_local_frame(&JObject::null()) };
            result
        })?;

        // Persist state so offsets survive connector restarts
        let connector_state = {
            let state = lock_mutex(&self.state, "state")?;
            ConnectorState::serialize(&*state, CONNECTOR_NAME, self.id)
        };

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: connector_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing JDBC source connector [{}]", self.id);

        if self.jvm.is_some() {
            // Closing the JDBC connection is blocking JNI work; run it off the
            // async worker like the poll path.
            tokio::task::block_in_place(|| -> Result<(), Error> {
                let Some(jvm) = self.jvm.as_ref() else {
                    return Ok(());
                };
                let Ok(mut env) = jvm.attach_current_thread() else {
                    return Ok(());
                };
                let mut guard = lock_mutex(&self.connection, "connection")?;
                if let Some(connection) = guard.as_ref() {
                    best_effort_close(&mut env, connection.as_obj());
                    info!("Database connection closed");
                }
                *guard = None;
                Ok(())
            })?;
        }

        let state = lock_mutex(&self.state, "state")?;
        info!(
            "JDBC source connector [{}] closed. Total rows processed: {}",
            self.id, state.processed_rows
        );

        Ok(())
    }
}

/// Convert string to snake_case
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_is_upper = false;

    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 && !prev_is_upper {
                result.push('_');
            }
            result.extend(ch.to_lowercase());
            prev_is_upper = true;
        } else {
            result.push(ch);
            prev_is_upper = false;
        }
    }

    result
}

/// Process-wide JVM. JNI allows only one `JavaVM` per OS process, so every JDBC
/// connector instance in this dynamic library shares this one.
static GLOBAL_JVM: Mutex<Option<Arc<JavaVM>>> = Mutex::new(None);

/// Return the process JVM, creating it on first use within this dynamic
/// library. The first caller's `jvm_options`/classpath win; later callers (e.g.
/// a second JDBC connector of the same type) reuse the existing VM instead of
/// failing with `JNI_EEXIST`.
///
/// Limitation: a JDBC *source* and a JDBC *sink* are separate dynamic libraries
/// and do not share this static, so configuring both in the *same* connectors
/// runtime process is not supported (the second to start cannot create a second
/// JVM). Run them in separate runtime processes.
fn get_or_create_jvm(driver_jar_path: &str, jvm_options: &[String]) -> Result<Arc<JavaVM>, Error> {
    let mut guard = lock_mutex(&GLOBAL_JVM, "jvm")?;
    if let Some(jvm) = guard.as_ref() {
        info!("Reusing existing process JVM");
        return Ok(jvm.clone());
    }

    let classpath_option = format!("-Djava.class.path={driver_jar_path}");
    let mut args_builder = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&classpath_option);
    for option in jvm_options {
        args_builder = args_builder.option(option);
    }
    let jvm_args = args_builder
        .build()
        .map_err(|e| Error::InitError(format!("Failed to build JVM arguments: {e:?}")))?;
    let jvm = JavaVM::new(jvm_args)
        .map_err(|e| Error::InitError(format!("Failed to create JVM: {e:?}")))?;

    info!("JVM initialized successfully (classpath: {driver_jar_path})");
    let arc = Arc::new(jvm);
    *guard = Some(arc.clone());
    Ok(arc)
}

/// Quote a value as a SQL string literal for substituting the incremental
/// `{last_offset}` value, which originates from a (DB-controlled) tracking-column
/// value. Backslashes are escaped before single quotes are doubled: doubling
/// quotes alone is insufficient under MySQL's default `sql_mode`, where a
/// backslash is an escape character and a trailing `\` could otherwise consume
/// the closing quote and break out of the literal. Binding the offset as a
/// `PreparedStatement` parameter is the DB-agnostic long-term fix (tracked as a
/// follow-up); this keeps the string-substitution path safe in the meantime.
fn quote_sql_literal(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "''");
    format!("'{escaped}'")
}

/// Reject a query that still contains an unresolved placeholder, so an invalid
/// statement is never sent to the driver. This guards misconfigurations such as
/// a bulk-mode query using `{last_offset}`, or an incremental query whose
/// predicate could not be auto-removed on the first (no-offset) poll.
fn finalize_query(query: String) -> Result<String, Error> {
    if query.contains("{tracking_column}") || query.contains("{last_offset}") {
        return Err(Error::InvalidConfigValue(
            "query still contains an unresolved {tracking_column}/{last_offset} placeholder; \
             placeholders are only resolved in incremental mode, and require either a persisted \
             offset, an initial_offset, or the exact 'WHERE {tracking_column} > {last_offset}' form"
                .to_string(),
        ));
    }
    Ok(query)
}

/// Remove the incremental offset predicate `{tracking_column} > {last_offset}`
/// from a query for the cold-start (no-offset) poll, preserving any other `WHERE`
/// conditions. Handles the offset term followed by `AND ...`, preceded by
/// `... AND`, or standing alone, so a companion condition such as
/// `AND {tracking_column} IS NOT NULL` survives as a valid `WHERE`.
fn strip_offset_predicate(query: &str) -> String {
    let query = RE_OFFSET_PREDICATE_AND_AFTER.replace_all(query, "WHERE ");
    let query = RE_OFFSET_PREDICATE_AND_BEFORE.replace_all(&query, "");
    RE_OFFSET_PREDICATE_BARE
        .replace_all(&query, "")
        .into_owned()
}

/// Check that an incremental query's result set is ordered ascending by the
/// tracking column, so `setMaxRows` returns a contiguous ascending prefix rather
/// than an arbitrary subset (which would let the advancing offset skip unread
/// lower keys). The tracking column must be the FIRST ordering term of the outer
/// `ORDER BY`, and must not be descending.
///
/// This is a lexical check, not a SQL parser, so it errs strict: it inspects the
/// last `ORDER BY` in the text (the outer query's, not a subquery's), takes the
/// first ordering term, and requires it to be the `{tracking_column}` placeholder
/// or an identifier whose final path segment equals the tracking column
/// (case-insensitive). A trailing `DESC` is rejected, and a composite
/// `ORDER BY other, tracking` (tracking not primary) is rejected, because
/// truncation then yields a prefix ordered by `other`.
fn query_orders_by_tracking_column(query: &str, tracking_column: &str) -> bool {
    let lower = query.to_lowercase();
    let Some(pos) = lower.rfind("order by") else {
        return false;
    };
    let after = lower[pos + "order by".len()..].trim_start();
    let first_term = after.split(',').next().unwrap_or("").trim();
    let mut tokens = first_term.split_whitespace();
    let key = tokens.next().unwrap_or("");
    // Any explicit descending direction breaks ascending offset advancement.
    if tokens.any(|token| token == "desc") {
        return false;
    }
    if key.starts_with("{tracking_column}") {
        return true;
    }
    // Compare the final path segment (`t.updated_at` -> `updated_at`), keeping
    // only leading identifier characters so trailing punctuation is ignored.
    let key_ident: String = key
        .rsplit('.')
        .next()
        .unwrap_or(key)
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let tracking_lower = tracking_column.to_lowercase();
    let tracking_ident = tracking_lower.rsplit('.').next().unwrap_or(&tracking_lower);
    !key_ident.is_empty() && key_ident == tracking_ident
}

/// Whether a configured `tracking_column` name refers to this result column.
/// Compares case-insensitively against both the raw driver label and the
/// normalized (snake_cased) output key: drivers fold identifier case (e.g.
/// PostgreSQL lowercases unquoted names) and snake_case output would otherwise
/// never match the configured name, silently stalling the offset.
fn tracking_column_matches(tracking_column: &str, raw_name: &str, normalized_name: &str) -> bool {
    tracking_column.eq_ignore_ascii_case(raw_name)
        || tracking_column.eq_ignore_ascii_case(normalized_name)
}

/// Validate that a string is a safe SQL identifier for interpolation: ASCII
/// letters, digits, underscore, and dot (for `table.column`), starting with a
/// letter or underscore. Prevents injection through the `{tracking_column}`
/// placeholder.
fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// Classify a JDBC `SQLState` class (first 2 chars) as transient vs permanent.
/// `08` connection, `40` rollback/serialization, `53` resources, `57` operator
/// intervention, `58` system error are transient; everything else (and an
/// unknown/absent state) is permanent.
fn is_transient_sql_state(sql_state: Option<&str>) -> bool {
    // Use `get` rather than slicing: the SQLState comes from the driver and is
    // not guaranteed ASCII, so `&s[..2]` could panic on a multi-byte boundary.
    match sql_state.and_then(|s| s.get(..2)) {
        Some(class) => matches!(class, "08" | "40" | "53" | "57" | "58"),
        None => false,
    }
}

/// Inspect and CLEAR the pending Java exception after a failed query JNI call,
/// returning a classified `Error`: transient SQL states (connection/resource
/// classes) map to `Error::Connection`, permanent ones (syntax/constraint) to
/// `Error::InvalidRecordValue`. Clearing is required so the next JNI call on this
/// thread is not aborted.
///
/// NOTE: the runtime does not yet branch on this distinction (there is no
/// per-variant backoff in the poll loop today), so the classification is
/// currently informational: it shapes the error variant and log message and is
/// kept ready for when SDK-side backoff lands. Do not claim differentiated
/// runtime backoff until that exists.
fn classify_query_failure(env: &mut JNIEnv, action: &str) -> Error {
    let (sql_state, message) = take_pending_sql_exception(env);
    let transient = is_transient_sql_state(sql_state.as_deref());
    let state = sql_state.as_deref().unwrap_or("?");
    let msg = format!("Failed to {action} (SQLState {state}): {message}");
    if transient {
        Error::Connection(msg)
    } else {
        Error::InvalidRecordValue(msg)
    }
}

/// Take the pending Java exception (clearing it) and return its `SQLState` (if a
/// `java.sql.SQLException`) and message.
fn take_pending_sql_exception(env: &mut JNIEnv) -> (Option<String>, String) {
    let throwable = match env.exception_occurred() {
        Ok(t) if !t.is_null() => t,
        _ => return (None, "unknown error".to_string()),
    };
    let _ = env.exception_clear();

    let message = throwable_string_method(env, &throwable, "getMessage")
        .unwrap_or_else(|| "unknown error".to_string());
    let sql_state = if env
        .is_instance_of(&throwable, "java/sql/SQLException")
        .unwrap_or(false)
    {
        throwable_string_method(env, &throwable, "getSQLState")
    } else {
        None
    };
    (sql_state, message)
}

/// Call a no-arg `String`-returning method on a throwable; None on JNI error/null.
fn throwable_string_method(
    env: &mut JNIEnv,
    throwable: &JThrowable,
    method: &str,
) -> Option<String> {
    let obj = env
        .call_method(throwable, method, "()Ljava/lang/String;", &[])
        .ok()?
        .l()
        .ok()?;
    if obj.is_null() {
        return None;
    }
    env.get_string(&JString::from(obj)).ok().map(|s| s.into())
}

/// JDBC SQL Types constants
mod java {
    pub mod sql {
        #[allow(dead_code)]
        pub struct Types;

        #[allow(dead_code)]
        impl Types {
            pub const BIT: i32 = -7;
            pub const TINYINT: i32 = -6;
            pub const SMALLINT: i32 = 5;
            pub const INTEGER: i32 = 4;
            pub const BIGINT: i32 = -5;
            pub const FLOAT: i32 = 6;
            pub const REAL: i32 = 7;
            pub const DOUBLE: i32 = 8;
            pub const NUMERIC: i32 = 2;
            pub const DECIMAL: i32 = 3;
            pub const CHAR: i32 = 1;
            pub const VARCHAR: i32 = 12;
            pub const LONGVARCHAR: i32 = -1;
            pub const DATE: i32 = 91;
            pub const TIME: i32 = 92;
            pub const TIMESTAMP: i32 = 93;
            pub const BINARY: i32 = -2;
            pub const VARBINARY: i32 = -3;
            pub const LONGVARBINARY: i32 = -4;
            pub const NULL: i32 = 0;
            pub const BOOLEAN: i32 = 16;
        }
    }
}

// Export the connector via SDK macro
source_connector!(JdbcSource);

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal, valid bulk-mode config for tests that need a `JdbcSourceConfig`.
    fn base_config() -> JdbcSourceConfig {
        JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        }
    }

    /// Write a throwaway file to stand in for a driver JAR and return its path,
    /// so `validate_config`'s existence check passes.
    fn write_temp_jar(name: &str) -> String {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, b"jar").expect("write temp jar");
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn test_parse_poll_interval() {
        assert_eq!(parse_poll_interval(Some("30s")), Duration::from_secs(30));
        assert_eq!(parse_poll_interval(Some("5m")), Duration::from_secs(300));
        // Unset, empty, and unparseable all fall back to the default.
        assert_eq!(parse_poll_interval(None), DEFAULT_POLL_INTERVAL);
        assert_eq!(parse_poll_interval(Some("  ")), DEFAULT_POLL_INTERVAL);
        assert_eq!(
            parse_poll_interval(Some("not-a-duration")),
            DEFAULT_POLL_INTERVAL
        );
    }

    #[test]
    fn test_validate_config_rejects_bad_poll_interval() {
        let jar = write_temp_jar("jdbc_validate_poll_interval.jar");
        let mut config = base_config();
        config.driver_jar_path = jar;
        config.poll_interval = Some("banana".to_string());
        let source = JdbcSource::new(1, config, None);
        let err = source
            .validate_config()
            .expect_err("must reject bad poll_interval");
        assert!(matches!(err, Error::InvalidConfigValue(msg) if msg.contains("poll_interval")));
    }

    #[test]
    fn test_quote_sql_literal_escapes_backslash() {
        // Backslash is doubled before quotes so a trailing backslash cannot
        // consume the closing quote under MySQL's default sql_mode.
        assert_eq!(quote_sql_literal(r"a\b"), r"'a\\b'");
        assert_eq!(quote_sql_literal(r"end\"), r"'end\\'");
        assert_eq!(quote_sql_literal(r"\'"), r"'\\'''");
    }

    #[test]
    fn test_build_query_removes_predicate_case_and_whitespace_insensitive() {
        for query in [
            "select * from t where {tracking_column} > {last_offset} order by id",
            "SELECT * FROM t WHERE  {tracking_column}   >   {last_offset} ORDER BY id",
            "SELECT * FROM t where {tracking_column}>{last_offset} ORDER BY id",
        ] {
            let mut config = base_config();
            config.query = query.to_string();
            config.mode = Mode::Incremental;
            config.tracking_column = Some("id".to_string());
            let source = JdbcSource::new(1, config, None);
            let state = State::default();
            let built = source.build_query(&state).expect("build query");
            assert!(
                !built.contains("{last_offset}") && !built.contains("{tracking_column}"),
                "predicate not removed for query variant: {query} -> {built}"
            );
            assert!(built.to_lowercase().contains("order by id"));
        }
    }

    #[test]
    fn test_strip_offset_predicate_preserves_other_conditions() {
        // Offset term followed by a companion condition (the README-advised
        // IS NOT NULL): the AND and the companion condition survive.
        assert_eq!(
            strip_offset_predicate(
                "SELECT * FROM t WHERE {tracking_column} > {last_offset} AND {tracking_column} IS NOT NULL ORDER BY {tracking_column}"
            ),
            "SELECT * FROM t WHERE {tracking_column} IS NOT NULL ORDER BY {tracking_column}"
        );
        // Offset term preceded by a condition.
        assert_eq!(
            strip_offset_predicate(
                "SELECT * FROM t WHERE active = 1 AND {tracking_column} > {last_offset} ORDER BY id"
            ),
            "SELECT * FROM t WHERE active = 1 ORDER BY id"
        );
        // Bare offset term (no other condition) drops the whole WHERE.
        assert!(
            !strip_offset_predicate(
                "SELECT * FROM t WHERE {tracking_column} > {last_offset} ORDER BY id"
            )
            .contains("{last_offset}")
        );
    }

    #[test]
    fn test_build_query_cold_start_compound_predicate_is_valid() {
        let mut config = base_config();
        config.mode = Mode::Incremental;
        config.tracking_column = Some("id".to_string());
        config.query = "SELECT * FROM t WHERE {tracking_column} > {last_offset} AND {tracking_column} IS NOT NULL ORDER BY {tracking_column}".to_string();
        let source = JdbcSource::new(1, config, None);
        // Cold start: no persisted offset, no initial_offset.
        let built = source.build_query(&State::default()).expect("build query");
        assert_eq!(built, "SELECT * FROM t WHERE id IS NOT NULL ORDER BY id");
        assert!(!built.contains("{last_offset}") && !built.contains("{tracking_column}"));
        // No dangling AND / empty WHERE.
        assert!(!built.to_uppercase().contains("WHERE AND"));
        assert!(!built.to_uppercase().contains("AND ORDER"));
    }

    #[test]
    fn test_validate_config_rejects_half_set_credentials() {
        let jar = write_temp_jar("jdbc_validate_half_creds.jar");
        let mut config = base_config();
        config.driver_jar_path = jar;
        config.username = Some("user".to_string());
        config.password = None;
        let source = JdbcSource::new(1, config, None);
        assert!(matches!(
            source.validate_config(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn test_validate_config_rejects_missing_driver_jar() {
        let mut config = base_config();
        config.driver_jar_path = "/nonexistent/path/to/driver.jar".to_string();
        let source = JdbcSource::new(1, config, None);
        let err = source.validate_config().expect_err("missing jar must fail");
        assert!(matches!(err, Error::InvalidConfigValue(msg) if msg.contains("driver_jar_path")));
    }

    #[test]
    fn test_validate_config_dry_runs_query() {
        let jar = write_temp_jar("jdbc_validate_dry_run.jar");
        let mut config = base_config();
        config.driver_jar_path = jar;
        // Passes the incremental invariants (tracking_column set, ordered by it)
        // but the non-canonical predicate leaves {last_offset} unresolved with no
        // offset, so the dry-run build_query must reject it now.
        config.mode = Mode::Incremental;
        config.tracking_column = Some("id".to_string());
        config.query = "SELECT * FROM t WHERE x >= {last_offset} ORDER BY id".to_string();
        let source = JdbcSource::new(1, config, None);
        assert!(matches!(
            source.validate_config(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn test_validate_config_accepts_valid_bulk() {
        let jar = write_temp_jar("jdbc_validate_ok.jar");
        let mut config = base_config();
        config.driver_jar_path = jar;
        let source = JdbcSource::new(1, config, None);
        assert!(source.validate_config().is_ok());
    }

    #[test]
    fn test_validate_config_accepts_valid_incremental() {
        let jar = write_temp_jar("jdbc_validate_ok_incremental.jar");
        let mut config = base_config();
        config.driver_jar_path = jar;
        config.mode = Mode::Incremental;
        config.tracking_column = Some("id".to_string());
        // Canonical placeholder predicate so the cold-start (no offset) build
        // auto-removes the WHERE; ordered by the tracking column.
        config.query =
            "SELECT id, name FROM t WHERE {tracking_column} > {last_offset} ORDER BY {tracking_column}"
                .to_string();
        let source = JdbcSource::new(1, config, None);
        assert!(source.validate_config().is_ok());
    }

    #[test]
    fn test_validate_config_incremental_requires_tracking_column() {
        let jar = write_temp_jar("jdbc_validate_no_tracking.jar");
        let mut config = base_config();
        config.driver_jar_path = jar;
        config.mode = Mode::Incremental;
        config.tracking_column = None;
        config.query = "SELECT id FROM t ORDER BY id".to_string();
        let source = JdbcSource::new(1, config, None);
        let err = source
            .validate_config()
            .expect_err("must require tracking_column");
        assert!(matches!(err, Error::InvalidConfigValue(msg) if msg.contains("tracking_column")));
    }

    #[test]
    fn test_validate_config_incremental_requires_order_by_tracking_column() {
        let jar = write_temp_jar("jdbc_validate_no_order.jar");
        let mut config = base_config();
        config.driver_jar_path = jar;
        config.mode = Mode::Incremental;
        config.tracking_column = Some("id".to_string());
        // No ORDER BY: an unordered incremental query can skip rows on truncation.
        config.query = "SELECT id FROM t WHERE id > {last_offset}".to_string();
        let source = JdbcSource::new(1, config, None);
        let err = source.validate_config().expect_err("must require ORDER BY");
        assert!(
            matches!(err, Error::InvalidConfigValue(msg) if msg.to_lowercase().contains("order by"))
        );
    }

    #[test]
    fn test_query_orders_by_tracking_column_accepts_valid() {
        assert!(query_orders_by_tracking_column(
            "SELECT * FROM t WHERE id > {last_offset} ORDER BY id",
            "id"
        ));
        // Placeholder form, case/whitespace variance, explicit ASC.
        assert!(query_orders_by_tracking_column(
            "select * from t order by {tracking_column}",
            "updated_at"
        ));
        assert!(query_orders_by_tracking_column(
            "SELECT * FROM t ORDER BY Updated_At ASC",
            "updated_at"
        ));
        // Table-qualified column, and the outer ORDER BY after a subquery.
        assert!(query_orders_by_tracking_column(
            "SELECT * FROM t ORDER BY t.updated_at",
            "updated_at"
        ));
        assert!(query_orders_by_tracking_column(
            "SELECT * FROM (SELECT * FROM t ORDER BY x) s ORDER BY id",
            "id"
        ));
    }

    #[test]
    fn test_query_orders_by_tracking_column_rejects_invalid() {
        // No ORDER BY.
        assert!(!query_orders_by_tracking_column(
            "SELECT * FROM t WHERE id > 0",
            "id"
        ));
        // Different column.
        assert!(!query_orders_by_tracking_column(
            "SELECT * FROM t ORDER BY name",
            "id"
        ));
        // Descending breaks ascending offset advancement.
        assert!(!query_orders_by_tracking_column(
            "SELECT * FROM t ORDER BY updated_at DESC",
            "updated_at"
        ));
        // Substring-only match must not pass (id is a substring of valid_flag/id_backup).
        assert!(!query_orders_by_tracking_column(
            "SELECT * FROM t ORDER BY valid_flag",
            "id"
        ));
        assert!(!query_orders_by_tracking_column(
            "SELECT * FROM t ORDER BY id_backup",
            "id"
        ));
        // Tracking column not the primary (first) ordering term.
        assert!(!query_orders_by_tracking_column(
            "SELECT * FROM t ORDER BY name, id",
            "id"
        ));
    }

    #[test]
    fn test_validate_config_rejects_empty_query_and_blank_offsets() {
        let jar = write_temp_jar("jdbc_validate_blanks.jar");
        // Empty query.
        let mut config = base_config();
        config.driver_jar_path = jar.clone();
        config.query = "   ".to_string();
        let source = JdbcSource::new(1, config, None);
        assert!(
            matches!(source.validate_config(), Err(Error::InvalidConfigValue(msg)) if msg.contains("query"))
        );

        // Blank initial_offset.
        let mut config = base_config();
        config.driver_jar_path = jar.clone();
        config.initial_offset = Some("  ".to_string());
        let source = JdbcSource::new(1, config, None);
        assert!(
            matches!(source.validate_config(), Err(Error::InvalidConfigValue(msg)) if msg.contains("initial_offset"))
        );

        // Blank tracking_column in incremental mode.
        let mut config = base_config();
        config.driver_jar_path = jar;
        config.mode = Mode::Incremental;
        config.tracking_column = Some("".to_string());
        config.query = "SELECT id FROM t ORDER BY id".to_string();
        let source = JdbcSource::new(1, config, None);
        assert!(
            matches!(source.validate_config(), Err(Error::InvalidConfigValue(msg)) if msg.contains("tracking_column"))
        );
    }

    #[test]
    fn test_validate_config_rejects_bad_batch_size() {
        let jar = write_temp_jar("jdbc_validate_batch_size.jar");
        for bad in [0u32, i32::MAX as u32] {
            let mut config = base_config();
            config.driver_jar_path = jar.clone();
            config.batch_size = bad;
            let source = JdbcSource::new(1, config, None);
            let err = source
                .validate_config()
                .expect_err("must reject invalid batch_size");
            assert!(matches!(err, Error::InvalidConfigValue(msg) if msg.contains("batch_size")));
        }
    }

    #[test]
    fn test_tracking_column_matches_case_and_normalization() {
        // Case-insensitive against the raw driver label (driver case-folding).
        assert!(tracking_column_matches(
            "OrderDate",
            "orderdate",
            "orderdate"
        ));
        // Matches the normalized (snake_cased) output key.
        assert!(tracking_column_matches(
            "order_date",
            "OrderDate",
            "order_date"
        ));
        // Plain lowercase match.
        assert!(tracking_column_matches("id", "id", "id"));
        // Genuinely different column does not match.
        assert!(!tracking_column_matches("id", "name", "name"));
    }

    #[test]
    fn test_tracking_offset_or_error_rejects_null_in_incremental() {
        let mut config = base_config();
        config.mode = Mode::Incremental;
        config.tracking_column = Some("id".to_string());
        let source = JdbcSource::new(1, config, None);
        // Non-null resolves; NULL/empty (None) is a hard error in incremental mode.
        assert_eq!(
            source
                .tracking_offset_or_error(Some("42".to_string()), "id")
                .unwrap(),
            Some("42".to_string())
        );
        assert!(matches!(
            source.tracking_offset_or_error(None, "id"),
            Err(Error::InvalidRecordValue(_))
        ));
    }

    #[test]
    fn test_tracking_offset_or_error_allows_null_in_bulk() {
        let source = JdbcSource::new(1, base_config(), None); // base_config is bulk
        assert_eq!(source.tracking_offset_or_error(None, "id").unwrap(), None);
    }

    #[test]
    fn test_sanitize_jdbc_url_mysql_format() {
        let url = "jdbc:mysql://root:SuperSecret123@localhost:3306/mydb";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(sanitized, "jdbc:mysql://root:***@localhost:3306/mydb");
        assert!(!sanitized.contains("SuperSecret123"));
    }

    #[test]
    fn test_sanitize_jdbc_url_postgresql_query_params() {
        let url = "jdbc:postgresql://localhost:5432/mydb?user=admin&password=P@ssw0rd&ssl=true";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(
            sanitized,
            "jdbc:postgresql://localhost:5432/mydb?user=admin&password=***&ssl=true"
        );
        assert!(!sanitized.contains("P@ssw0rd"));
    }

    #[test]
    fn test_sanitize_jdbc_url_oracle_format() {
        let url = "jdbc:oracle:thin:system/oracle123@localhost:1521:XE";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(sanitized, "jdbc:oracle:thin:system/***@localhost:1521:XE");
        assert!(!sanitized.contains("oracle123"));
    }

    #[test]
    fn test_sanitize_jdbc_url_sqlserver_format() {
        let url = "jdbc:sqlserver://localhost:1433;user=sa;password=MySecretPass;database=Sales";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(
            sanitized,
            "jdbc:sqlserver://localhost:1433;user=sa;password=***;database=Sales"
        );
        assert!(!sanitized.contains("MySecretPass"));
    }

    #[test]
    fn test_sanitize_jdbc_url_h2_format() {
        let url = "jdbc:h2:mem:testdb;USER=sa;PASSWORD=secret";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(sanitized, "jdbc:h2:mem:testdb;USER=sa;PASSWORD=***");
        assert!(!sanitized.contains("secret"));
    }

    #[test]
    fn test_sanitize_jdbc_url_case_insensitive() {
        let url1 = "jdbc:postgresql://localhost?password=secret";
        let url2 = "jdbc:postgresql://localhost?PASSWORD=secret";
        let url3 = "jdbc:postgresql://localhost?pwd=secret";
        let url4 = "jdbc:postgresql://localhost?PWD=secret";

        for url in [url1, url2, url3, url4] {
            let sanitized = sanitize_jdbc_url(url);
            assert!(!sanitized.contains("secret"), "Failed for URL: {}", url);
            assert!(sanitized.contains("***"));
        }
    }

    #[test]
    fn test_sanitize_jdbc_url_no_password() {
        let url = "jdbc:h2:mem:testdb";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(sanitized, url);
    }

    #[test]
    fn test_sanitize_jdbc_url_multiple_passwords() {
        let url = "jdbc:postgresql://localhost?password=secret1&pwd=secret2";
        let sanitized = sanitize_jdbc_url(url);
        assert!(!sanitized.contains("secret1"));
        assert!(!sanitized.contains("secret2"));
        assert_eq!(
            sanitized,
            "jdbc:postgresql://localhost?password=***&pwd=***"
        );
    }

    #[test]
    fn test_build_query_incremental_with_offset() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM users WHERE id > {last_offset} ORDER BY id".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: Some("0".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        // With initial offset (no last_offset yet)
        let state = State {
            last_offset: None,
            processed_rows: 0,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state).expect("build query");
        assert_eq!(query, "SELECT * FROM users WHERE id > '0' ORDER BY id");

        // With tracked offset
        let state = State {
            last_offset: Some("42".to_string()),
            processed_rows: 42,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state).expect("build query");
        assert_eq!(query, "SELECT * FROM users WHERE id > '42' ORDER BY id");
    }

    #[test]
    fn test_build_query_substitutes_tracking_column() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query:
                "SELECT * FROM orders WHERE {tracking_column} > {last_offset} ORDER BY {tracking_column}"
                    .to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: Some("updated_at".to_string()),
            initial_offset: Some("2024-01-01".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = State {
            last_offset: Some("2024-06-15".to_string()),
            processed_rows: 0,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state).expect("build query");
        assert_eq!(
            query,
            "SELECT * FROM orders WHERE updated_at > '2024-06-15' ORDER BY updated_at"
        );
    }

    #[test]
    fn test_build_query_no_offset_substitutes_tracking_column_in_order_by() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query:
                "SELECT * FROM orders WHERE {tracking_column} > {last_offset} ORDER BY {tracking_column}"
                    .to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: Some("updated_at".to_string()),
            initial_offset: None,
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        // No last_offset and no initial_offset: the WHERE predicate is dropped,
        // but the ORDER BY {tracking_column} must still be substituted.
        let query = source
            .build_query(&State {
                last_offset: None,
                processed_rows: 0,
                last_poll_time: Utc::now(),
            })
            .expect("build query");
        assert!(!query.contains("{tracking_column}"), "got: {query}");
        assert!(!query.contains("{last_offset}"), "got: {query}");
        assert!(query.contains("ORDER BY updated_at"), "got: {query}");
    }

    #[test]
    fn test_build_query_rejects_injection_in_tracking_column() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM t WHERE {tracking_column} > {last_offset}".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: Some("id; DROP TABLE t".to_string()),
            initial_offset: Some("0".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        assert!(source.build_query(&State::default()).is_err());
    }

    #[test]
    fn test_build_query_bulk_mode_no_limit_appended() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM products".to_string(),
            poll_interval: Some("60s".to_string()),
            batch_size: 5000,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: false,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = State::default();
        let query = source.build_query(&state).expect("build query");
        // build_query should NOT append LIMIT; row limiting is done via setMaxRows
        assert_eq!(query, "SELECT * FROM products");
        assert!(!query.to_uppercase().contains("LIMIT"));
    }

    #[test]
    fn test_state_restoration_from_connector_state() {
        let original_state = State {
            last_offset: Some("2024-06-15 12:00:00".to_string()),
            processed_rows: 1500,
            last_poll_time: Utc::now(),
        };
        let connector_state = ConnectorState::serialize(&original_state, CONNECTOR_NAME, 1)
            .expect("Failed to serialize state");

        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM orders WHERE updated_at > {last_offset}".to_string(),
            poll_interval: Some("30s".to_string()),
            batch_size: 1000,
            tracking_column: Some("updated_at".to_string()),
            initial_offset: Some("2024-01-01 00:00:00".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, Some(connector_state));
        let state = source.state.lock().unwrap();
        assert_eq!(state.last_offset, Some("2024-06-15 12:00:00".to_string()));
        assert_eq!(state.processed_rows, 1500);
    }

    #[test]
    fn test_quote_sql_literal_escapes_single_quotes() {
        assert_eq!(quote_sql_literal("42"), "'42'");
        assert_eq!(
            quote_sql_literal("2024-01-01 00:00:00"),
            "'2024-01-01 00:00:00'"
        );
        assert_eq!(quote_sql_literal("o'brien"), "'o''brien'");
        assert_eq!(
            quote_sql_literal("x'; DROP TABLE t; --"),
            "'x''; DROP TABLE t; --'"
        );
    }

    #[test]
    fn test_is_transient_sql_state() {
        for s in [
            "08001", "08006", "40001", "40P01", "53300", "57P01", "58030",
        ] {
            assert!(is_transient_sql_state(Some(s)), "{s} should be transient");
        }
        for s in ["22001", "23505", "42601", "42P01", "99999"] {
            assert!(!is_transient_sql_state(Some(s)), "{s} should be permanent");
        }
        assert!(!is_transient_sql_state(None));
        assert!(!is_transient_sql_state(Some("")));
    }

    #[test]
    fn test_is_valid_identifier() {
        assert!(is_valid_identifier("id"));
        assert!(is_valid_identifier("updated_at"));
        assert!(is_valid_identifier("t.updated_at"));
        assert!(!is_valid_identifier("id; DROP TABLE t"));
        assert!(!is_valid_identifier("1col"));
        assert!(!is_valid_identifier("col name"));
        assert!(!is_valid_identifier(""));
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("OrderDate"), "order_date");
        assert_eq!(to_snake_case("updatedAt"), "updated_at");
        assert_eq!(to_snake_case("ID"), "id"); // consecutive uppers stay together
        assert_eq!(to_snake_case("already_snake"), "already_snake");
        assert_eq!(to_snake_case("simple"), "simple");
    }

    // =========================================================================
    // Config deserialization tests
    // =========================================================================

    #[test]
    fn test_config_deserialization_minimal_toml() {
        let toml_str = r#"
            jdbc_url = "jdbc:h2:mem:test"
            driver_class = "org.h2.Driver"
            driver_jar_path = "/tmp/h2.jar"
            query = "SELECT * FROM users"
            poll_interval = "30s"
        "#;
        let config: JdbcSourceConfig =
            toml::from_str(toml_str).expect("Failed to parse minimal TOML config");
        assert_eq!(config.driver_class, "org.h2.Driver");
        assert_eq!(config.query, "SELECT * FROM users");
        assert_eq!(config.poll_interval.as_deref(), Some("30s"));
        assert_eq!(
            parse_poll_interval(config.poll_interval.as_deref()),
            Duration::from_secs(30)
        );
        // Verify defaults are applied
        assert_eq!(config.mode, Mode::Incremental);
        assert_eq!(config.batch_size, 1000);
        assert!(config.include_metadata);
        assert!(!config.snake_case_columns);
        assert_eq!(config.connection_timeout_ms, 5000);
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.tracking_column.is_none());
        assert!(config.initial_offset.is_none());
        assert!(config.jvm_options.is_empty());
    }

    #[test]
    fn test_config_deserialization_full_toml() {
        let toml_str = r#"
            jdbc_url = "jdbc:mysql://localhost:3306/mydb"
            driver_class = "com.mysql.cj.jdbc.Driver"
            driver_jar_path = "/opt/drivers/mysql.jar"
            username = "admin"
            password = "s3cret"
            query = "SELECT * FROM orders WHERE id > {last_offset} ORDER BY id"
            poll_interval = "5m"
            batch_size = 500
            tracking_column = "id"
            initial_offset = "0"
            mode = "incremental"
            snake_case_columns = true
            include_metadata = false
            jvm_options = ["-Xmx512m", "-Xms128m"]
            connection_timeout_ms = 60000
        "#;
        let config: JdbcSourceConfig =
            toml::from_str(toml_str).expect("Failed to parse full TOML config");
        assert_eq!(config.driver_class, "com.mysql.cj.jdbc.Driver");
        assert_eq!(config.username.as_deref(), Some("admin"));
        assert!(config.password.is_some());
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.tracking_column.as_deref(), Some("id"));
        assert_eq!(config.initial_offset.as_deref(), Some("0"));
        assert_eq!(config.mode, Mode::Incremental);
        assert!(config.snake_case_columns);
        assert!(!config.include_metadata);
        assert_eq!(config.jvm_options, vec!["-Xmx512m", "-Xms128m"]);
        assert_eq!(config.connection_timeout_ms, 60000);
        assert_eq!(
            parse_poll_interval(config.poll_interval.as_deref()),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn test_config_deserialization_bulk_mode() {
        let toml_str = r#"
            jdbc_url = "jdbc:h2:mem:test"
            driver_class = "org.h2.Driver"
            driver_jar_path = "/tmp/h2.jar"
            query = "SELECT * FROM products"
            poll_interval = "1h"
            mode = "bulk"
        "#;
        let config: JdbcSourceConfig =
            toml::from_str(toml_str).expect("Failed to parse bulk mode config");
        assert_eq!(config.mode, Mode::Bulk);
        assert_eq!(
            parse_poll_interval(config.poll_interval.as_deref()),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn test_config_deserialization_invalid_mode_fails() {
        let toml_str = r#"
            jdbc_url = "jdbc:h2:mem:test"
            driver_class = "org.h2.Driver"
            driver_jar_path = "/tmp/h2.jar"
            query = "SELECT 1"
            poll_interval = "1s"
            mode = "invalid_mode"
        "#;
        let result = toml::from_str::<JdbcSourceConfig>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for invalid mode, but got: {:?}",
            result
        );
    }

    // =========================================================================
    // State restoration tests
    // =========================================================================

    #[test]
    fn test_state_restoration_with_malformed_bytes_falls_back_to_default() {
        let connector_state = ConnectorState(vec![0xFF, 0xFE, 0xFD, 0x00]);

        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, Some(connector_state));
        let state = source.state.lock().unwrap();
        // Should fall back to default state
        assert!(state.last_offset.is_none());
        assert_eq!(state.processed_rows, 0);
    }

    #[test]
    fn test_state_restoration_with_empty_bytes_falls_back_to_default() {
        let connector_state = ConnectorState(vec![]);

        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, Some(connector_state));
        let state = source.state.lock().unwrap();
        assert!(state.last_offset.is_none());
        assert_eq!(state.processed_rows, 0);
    }

    #[test]
    fn test_state_restoration_none_uses_initial_offset() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM orders WHERE id > {last_offset}".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: Some("100".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = source.state.lock().unwrap();
        assert_eq!(state.last_offset, Some("100".to_string()));
        assert_eq!(state.processed_rows, 0);
    }

    #[test]
    fn test_state_restoration_none_without_initial_offset() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM products".to_string(),
            poll_interval: Some("60s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = source.state.lock().unwrap();
        assert!(state.last_offset.is_none());
        assert_eq!(state.processed_rows, 0);
    }

    // =========================================================================
    // extract_offset_value tests
    // =========================================================================

    #[test]
    fn test_extract_offset_value_with_integer() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), serde_json::json!(42));
        assert_eq!(
            source.extract_offset_value(&row, "id"),
            Some("42".to_string())
        );
    }

    #[test]
    fn test_extract_offset_value_with_string() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let mut row = serde_json::Map::new();
        row.insert(
            "updated_at".to_string(),
            serde_json::json!("2024-06-15 12:00:00"),
        );
        assert_eq!(
            source.extract_offset_value(&row, "updated_at"),
            Some("2024-06-15 12:00:00".to_string())
        );
    }

    #[test]
    fn test_extract_offset_value_with_float() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let mut row = serde_json::Map::new();
        row.insert("version".to_string(), serde_json::json!(3.5));
        assert_eq!(
            source.extract_offset_value(&row, "version"),
            Some("3.5".to_string())
        );
    }

    #[test]
    fn test_extract_offset_value_with_null() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), serde_json::Value::Null);
        // A SQL NULL tracking value must not become the persisted offset.
        assert_eq!(source.extract_offset_value(&row, "id"), None);
    }

    #[test]
    fn test_extract_offset_value_missing_column() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let row = serde_json::Map::new();
        assert_eq!(source.extract_offset_value(&row, "nonexistent"), None);
    }

    // =========================================================================
    // build_query edge case tests
    // =========================================================================

    #[test]
    fn test_build_query_incremental_no_offset_no_initial_removes_where_clause() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM users WHERE {tracking_column} > {last_offset} ORDER BY id"
                .to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: None,
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = State {
            last_offset: None,
            processed_rows: 0,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state).expect("build query");
        // The WHERE clause placeholder should be removed
        assert!(
            !query.contains("{last_offset}"),
            "Query should not contain unresolved placeholder: {}",
            query
        );
    }

    #[test]
    fn test_build_query_rejects_unresolved_placeholder() {
        // Incremental, no offset, and a non-canonical predicate that the
        // auto-remove does not match: the unresolved {last_offset} must produce
        // an error rather than being shipped to the driver as invalid SQL.
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM t WHERE id >= {last_offset}".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        assert!(source.build_query(&State::default()).is_err());
    }

    #[test]
    fn test_build_query_bulk_mode_ignores_offset() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM users ORDER BY id".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: Some("0".to_string()),
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = State {
            last_offset: Some("42".to_string()),
            processed_rows: 42,
            last_poll_time: Utc::now(),
        };
        // In bulk mode the query is used verbatim; the tracked offset is ignored.
        let query = source.build_query(&state).expect("build query");
        assert_eq!(query, "SELECT * FROM users ORDER BY id");
    }

    // =========================================================================
    // Mode enum tests
    // =========================================================================

    #[test]
    fn test_mode_serialization_roundtrip() {
        let incremental = Mode::Incremental;
        let serialized = serde_json::to_string(&incremental).unwrap();
        assert_eq!(serialized, r#""incremental""#);
        let deserialized: Mode = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Mode::Incremental);

        let bulk = Mode::Bulk;
        let serialized = serde_json::to_string(&bulk).unwrap();
        assert_eq!(serialized, r#""bulk""#);
        let deserialized: Mode = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, Mode::Bulk);
    }

    #[test]
    fn test_mode_deserialization_rejects_unknown() {
        let result = serde_json::from_str::<Mode>(r#""streaming""#);
        assert!(
            result.is_err(),
            "Unknown mode 'streaming' should fail deserialization"
        );
    }

    // =========================================================================
    // Debug impl tests (ensures secrets are not leaked)
    // =========================================================================

    #[test]
    fn test_config_debug_does_not_leak_password() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:mysql://root:SuperSecret@localhost/db"),
            driver_class: "com.mysql.cj.jdbc.Driver".to_string(),
            driver_jar_path: "/tmp/mysql.jar".to_string(),
            username: Some("admin".to_string()),
            password: Some(SecretString::from("MyP@ssw0rd")),
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };

        let debug_output = format!("{:?}", config);
        assert!(
            !debug_output.contains("SuperSecret"),
            "Debug output should not contain JDBC URL password: {}",
            debug_output
        );
        assert!(
            !debug_output.contains("MyP@ssw0rd"),
            "Debug output should not contain password field: {}",
            debug_output
        );
        assert!(
            debug_output.contains("***"),
            "Debug output should contain masked password: {}",
            debug_output
        );
    }

    #[test]
    fn test_config_debug_without_password() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT 1".to_string(),
            poll_interval: Some("10s".to_string()),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            connection_timeout_ms: 30000,
        };

        let debug_output = format!("{:?}", config);
        // Should not panic and should contain the struct name
        assert!(debug_output.contains("JdbcSourceConfig"));
    }
}
