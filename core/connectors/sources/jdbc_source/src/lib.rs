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
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use jni::objects::{GlobalRef, JByteArray, JObject, JString, JThrowable, JValue};
use jni::{JNIEnv, JavaVM};
use regex::Regex;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

mod secret_string_serde {
    use secrecy::SecretString;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SecretString, D::Error> {
        let s = String::deserialize(d)?;
        Ok(SecretString::from(s))
    }
    #[allow(unused_variables)]
    pub fn serialize<S: Serializer>(val: &SecretString, s: S) -> Result<S::Ok, S::Error> {
        "".serialize(s)
    }
}

mod opt_secret_string_serde {
    use secrecy::SecretString;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<SecretString>, D::Error> {
        let s: Option<String> = Option::deserialize(d)?;
        Ok(s.map(SecretString::from))
    }
    #[allow(unused_variables)]
    pub fn serialize<S: Serializer>(val: &Option<SecretString>, s: S) -> Result<S::Ok, S::Error> {
        Option::<String>::None.serialize(s)
    }
}

/// Cached compiled regex patterns for password sanitization
static RE_USER_PASS_AT: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"://([^:]+):([^@?;/]+)@").unwrap());
static RE_PASSWORD_PARAM: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)(password|pwd|pass)=([^;&\s]+)").unwrap());
static RE_ORACLE_PASS: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"thin:([^/]+)/([^@]+)@").unwrap());

const CONNECTOR_NAME: &str = "JDBC source";

/// Source mode for the JDBC connector
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Full table scan on every poll
    Bulk,
    /// Track last offset and only fetch new rows
    Incremental,
}

/// Configuration for JDBC source connector
#[derive(Clone, Deserialize, Serialize)]
pub struct JdbcSourceConfig {
    /// JDBC connection URL (e.g., "jdbc:mysql://localhost:3306/mydb")
    /// Can include credentials: "jdbc:mysql://localhost:3306/mydb?user=root&password=secret"
    #[serde(with = "secret_string_serde")]
    pub jdbc_url: SecretString,

    /// JDBC driver class name (e.g., "com.mysql.cj.jdbc.Driver")
    pub driver_class: String,

    /// Path to JDBC driver JAR file
    pub driver_jar_path: String,

    /// Database username (optional if included in jdbc_url)
    #[serde(default)]
    pub username: Option<String>,

    /// Database password (optional if included in jdbc_url)
    #[serde(default, with = "opt_secret_string_serde")]
    pub password: Option<SecretString>,

    /// SQL query to execute for fetching data
    /// Can use {last_offset} placeholder for incremental reads
    pub query: String,

    /// Polling interval (e.g., "30s", "5m", "1h")
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,

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

    /// Enable connection pooling via HikariCP
    #[serde(default)]
    pub enable_connection_pool: bool,

    /// Maximum pool size (default: 10)
    #[serde(default = "default_pool_size")]
    pub max_pool_size: u32,

    /// Minimum idle connections (default: 2)
    #[serde(default = "default_min_idle")]
    pub min_idle: u32,

    /// Connection timeout in milliseconds (default: 30000)
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_ms: u64,
}

fn default_pool_size() -> u32 {
    10
}

fn default_min_idle() -> u32 {
    2
}

fn default_connection_timeout() -> u64 {
    30000
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
            .field("enable_connection_pool", &self.enable_connection_pool)
            .field("max_pool_size", &self.max_pool_size)
            .field("min_idle", &self.min_idle)
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
    connection_pool: Option<GlobalRef>, // HikariDataSource if pooling enabled
    state: Arc<Mutex<State>>,
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

        Self {
            id,
            config,
            jvm: None,
            connection: Mutex::new(None),
            connection_pool: None,
            state: Arc::new(Mutex::new(state)),
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

    /// Load JDBC driver and create connection (or connection pool)
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

        let class_class = env
            .find_class("java/lang/Class")
            .map_err(|e| Error::InitError(format!("Failed to find Class: {}", e)))?;

        let driver_class_name = env
            .new_string(&self.config.driver_class)
            .map_err(|e| Error::InitError(format!("Failed to create class name string: {}", e)))?;

        // Call Class.forName(className) to load and initialize the driver
        env.call_static_method(
            class_class,
            "forName",
            "(Ljava/lang/String;)Ljava/lang/Class;",
            &[JValue::Object(&driver_class_name.into())],
        )
        .map_err(|e| {
            Error::InitError(format!(
                "Failed to load driver class '{}': {}",
                self.config.driver_class, e
            ))
        })?;

        info!("JDBC driver loaded and registered successfully");

        if self.config.enable_connection_pool {
            info!(
                "Setting up HikariCP connection pool to: {}",
                sanitize_jdbc_url(self.config.jdbc_url.expose_secret())
            );
            let pool = self.create_connection_pool_internal(&mut env)?;
            self.connection_pool = Some(pool);
        } else {
            info!(
                "Creating direct JDBC connection to: {}",
                sanitize_jdbc_url(self.config.jdbc_url.expose_secret())
            );
            let conn = self.create_direct_connection_internal(&mut env)?;
            *self.connection.lock().expect("connection mutex poisoned") = Some(conn);
        }

        Ok(())
    }

    /// Create a direct JDBC connection via DriverManager
    fn create_direct_connection_internal(&self, env: &mut JNIEnv) -> Result<GlobalRef, Error> {
        // Set the thread context class loader to help DriverManager find the driver
        let current_thread_class = env
            .find_class("java/lang/Thread")
            .map_err(|e| Error::InitError(format!("Failed to find Thread class: {}", e)))?;

        let current_thread = env
            .call_static_method(
                current_thread_class,
                "currentThread",
                "()Ljava/lang/Thread;",
                &[],
            )
            .map_err(|e| Error::InitError(format!("Failed to get current thread: {}", e)))?
            .l()
            .map_err(|e| Error::InitError(format!("Failed to extract thread object: {}", e)))?;

        // Get the class loader that loaded the driver
        let driver_class = env
            .find_class(self.config.driver_class.replace('.', "/"))
            .map_err(|e| Error::InitError(format!("Failed to find driver class: {}", e)))?;

        let driver_class_loader = env
            .call_method(
                &driver_class,
                "getClassLoader",
                "()Ljava/lang/ClassLoader;",
                &[],
            )
            .map_err(|e| Error::InitError(format!("Failed to get driver class loader: {}", e)))?
            .l()
            .map_err(|e| Error::InitError(format!("Failed to extract class loader: {}", e)))?;

        // Set the context class loader
        env.call_method(
            &current_thread,
            "setContextClassLoader",
            "(Ljava/lang/ClassLoader;)V",
            &[JValue::Object(&driver_class_loader)],
        )
        .map_err(|e| Error::InitError(format!("Failed to set context class loader: {}", e)))?;

        info!(
            "Set thread context class loader for driver: {}",
            self.config.driver_class
        );

        // Get connection from DriverManager
        let driver_manager = env
            .find_class("java/sql/DriverManager")
            .map_err(|e| Error::InitError(format!("Failed to find DriverManager: {}", e)))?;

        let jdbc_url = env
            .new_string(self.config.jdbc_url.expose_secret())
            .map_err(|e| Error::InitError(format!("Failed to create JDBC URL string: {}", e)))?;

        // If username/password are provided separately, use 3-arg getConnection
        let connection = if let (Some(username), Some(password)) =
            (&self.config.username, &self.config.password)
        {
            info!("Using separate username/password authentication");
            let username_jstring = env.new_string(username).map_err(|e| {
                Error::InitError(format!("Failed to create username string: {}", e))
            })?;
            let password_jstring = env.new_string(password.expose_secret()).map_err(|e| {
                Error::InitError(format!("Failed to create password string: {}", e))
            })?;

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
            .map_err(|e| {
                Error::InitError(format!(
                    "Failed to create JDBC connection with credentials: {}",
                    e
                ))
            })?
        } else {
            info!("Using connection string with embedded credentials");
            env.call_static_method(
                driver_manager,
                "getConnection",
                "(Ljava/lang/String;)Ljava/sql/Connection;",
                &[JValue::Object(&jdbc_url.into())],
            )
            .map_err(|e| {
                Error::InitError(format!("Failed to create JDBC connection from URL: {}", e))
            })?
        };

        let connection_obj = connection
            .l()
            .map_err(|e| Error::InitError(format!("Failed to get connection object: {}", e)))?;

        let global_ref = env
            .new_global_ref(connection_obj)
            .map_err(|e| Error::InitError(format!("Failed to create global reference: {}", e)))?;

        info!("Direct database connection established successfully");
        Ok(global_ref)
    }

    /// Create HikariCP connection pool
    fn create_connection_pool_internal(&self, env: &mut JNIEnv) -> Result<GlobalRef, Error> {
        info!(
            "Initializing HikariCP with max_pool_size={}, min_idle={}",
            self.config.max_pool_size, self.config.min_idle
        );

        let hikari_config_class = env.find_class("com/zaxxer/hikari/HikariConfig").map_err(
            |e| {
                Error::InitError(format!(
                    "Failed to find HikariConfig class. Ensure HikariCP JAR is in classpath: {}",
                    e
                ))
            },
        )?;

        let hikari_config = env
            .new_object(hikari_config_class, "()V", &[])
            .map_err(|e| Error::InitError(format!("Failed to create HikariConfig: {}", e)))?;

        let jdbc_url_jstring = env
            .new_string(self.config.jdbc_url.expose_secret())
            .map_err(|e| Error::InitError(format!("Failed to create JDBC URL: {}", e)))?;
        env.call_method(
            &hikari_config,
            "setJdbcUrl",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&jdbc_url_jstring.into())],
        )
        .map_err(|e| Error::InitError(format!("Failed to set JDBC URL: {}", e)))?;

        if let Some(username) = &self.config.username {
            let username_jstring = env
                .new_string(username)
                .map_err(|e| Error::InitError(format!("Failed to create username: {}", e)))?;
            env.call_method(
                &hikari_config,
                "setUsername",
                "(Ljava/lang/String;)V",
                &[JValue::Object(&username_jstring.into())],
            )
            .map_err(|e| Error::InitError(format!("Failed to set username: {}", e)))?;
        }

        if let Some(password) = &self.config.password {
            let password_jstring = env
                .new_string(password.expose_secret())
                .map_err(|e| Error::InitError(format!("Failed to create password: {}", e)))?;
            env.call_method(
                &hikari_config,
                "setPassword",
                "(Ljava/lang/String;)V",
                &[JValue::Object(&password_jstring.into())],
            )
            .map_err(|e| Error::InitError(format!("Failed to set password: {}", e)))?;
        }

        let driver_class_jstring = env
            .new_string(&self.config.driver_class)
            .map_err(|e| Error::InitError(format!("Failed to create driver class name: {}", e)))?;
        env.call_method(
            &hikari_config,
            "setDriverClassName",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&driver_class_jstring.into())],
        )
        .map_err(|e| Error::InitError(format!("Failed to set driver class: {}", e)))?;

        env.call_method(
            &hikari_config,
            "setMaximumPoolSize",
            "(I)V",
            &[JValue::Int(
                self.config.max_pool_size.min(i32::MAX as u32) as i32
            )],
        )
        .map_err(|e| Error::InitError(format!("Failed to set max pool size: {}", e)))?;

        env.call_method(
            &hikari_config,
            "setMinimumIdle",
            "(I)V",
            &[JValue::Int(self.config.min_idle.min(i32::MAX as u32) as i32)],
        )
        .map_err(|e| Error::InitError(format!("Failed to set min idle: {}", e)))?;

        env.call_method(
            &hikari_config,
            "setConnectionTimeout",
            "(J)V",
            &[JValue::Long(self.config.connection_timeout_ms as i64)],
        )
        .map_err(|e| Error::InitError(format!("Failed to set connection timeout: {}", e)))?;

        let hikari_datasource_class = env
            .find_class("com/zaxxer/hikari/HikariDataSource")
            .map_err(|e| {
                Error::InitError(format!("Failed to find HikariDataSource class: {}", e))
            })?;

        let datasource = env
            .new_object(
                hikari_datasource_class,
                "(Lcom/zaxxer/hikari/HikariConfig;)V",
                &[JValue::Object(&hikari_config)],
            )
            .map_err(|e| Error::InitError(format!("Failed to create HikariDataSource: {}", e)))?;

        let global_ref = env.new_global_ref(datasource).map_err(|e| {
            Error::InitError(format!("Failed to create global reference for pool: {}", e))
        })?;

        info!("HikariCP connection pool created successfully");
        Ok(global_ref)
    }

    /// Acquire a connection. Returns the connection plus whether it was borrowed
    /// from the pool (and therefore must be `close()`d to return it to the pool).
    /// In direct mode a dead connection is transparently re-established.
    fn get_connection<'local>(
        &self,
        env: &mut JNIEnv<'local>,
    ) -> Result<(JObject<'local>, bool), Error> {
        if let Some(pool) = &self.connection_pool {
            let connection = env
                .call_method(
                    pool.as_obj(),
                    "getConnection",
                    "()Ljava/sql/Connection;",
                    &[],
                )
                .map_err(|e| {
                    Error::Connection(format!("Failed to get connection from pool: {}", e))
                })?
                .l()
                .map_err(|e| {
                    Error::Connection(format!("Failed to extract connection object: {}", e))
                })?;
            return Ok((connection, true));
        }

        // Direct connection: validate and re-establish if it has dropped.
        let needs_reconnect = {
            let guard = self.connection.lock().expect("connection mutex poisoned");
            match guard.as_ref() {
                Some(conn) => !self.connection_is_valid(env, conn.as_obj()),
                None => true,
            }
        };
        if needs_reconnect {
            info!("Direct JDBC connection is not valid; re-establishing");
            let new_conn = self.create_direct_connection_internal(env)?;
            *self.connection.lock().expect("connection mutex poisoned") = Some(new_conn);
        }

        let guard = self.connection.lock().expect("connection mutex poisoned");
        let conn = guard
            .as_ref()
            .ok_or_else(|| Error::Connection("No connection available".to_string()))?;
        let local_ref = env
            .new_local_ref(conn.as_obj())
            .map_err(|e| Error::Connection(format!("Failed to create local ref: {}", e)))?;
        Ok((local_ref, false))
    }

    /// Best-effort `Connection.isValid(timeout)` check. Returns false on any
    /// JNI error so the caller re-establishes the connection.
    fn connection_is_valid(&self, env: &mut JNIEnv, conn: &JObject) -> bool {
        let timeout_secs = (self.config.connection_timeout_ms / 1000).clamp(1, 30) as i32;
        env.call_method(conn, "isValid", "(I)Z", &[JValue::Int(timeout_secs)])
            .and_then(|v| v.z())
            .unwrap_or(false)
    }

    /// Execute query and fetch results.
    ///
    /// The mutex is held only briefly: once to read the current offset for
    /// query building, and once after the JNI work to write the updated state.
    fn execute_query(&self, env: &mut JNIEnv) -> Result<Vec<ProducedMessage>, Error> {
        let (connection, pooled) = self.get_connection(env)?;

        // Read current state snapshot (short lock)
        let query = {
            let state = self.state.lock().expect("state mutex poisoned");
            self.build_query(&state)
        };
        info!("Executing query: {}", query);

        // Execute statement and fetch all rows (no lock held). A pooled
        // connection must be returned to the pool afterwards (close() on a
        // Hikari connection returns it rather than destroying it), otherwise
        // the pool is exhausted after `max_pool_size` polls.
        let result = self.execute_statement_and_fetch_rows(env, &connection, &query);
        if pooled {
            let _ = env.call_method(&connection, "close", "()V", &[]);
        }
        let (messages, row_count, max_offset) = result?;

        // Update state with results (short lock)
        {
            let mut state = self.state.lock().expect("state mutex poisoned");
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
        let query_jstring = env
            .new_string(query)
            .map_err(|e| Error::Connection(format!("Failed to create query string: {}", e)))?;

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
        // This works across all JDBC drivers (MySQL, Oracle, SQL Server, H2, etc.)
        env.call_method(
            &statement,
            "setMaxRows",
            "(I)V",
            &[JValue::Int(
                self.config.batch_size.min(i32::MAX as u32) as i32
            )],
        )
        .map_err(|e| Error::Connection(format!("Failed to set max rows: {}", e)))?;

        let result_set = match env
            .call_method(&statement, "executeQuery", "()Ljava/sql/ResultSet;", &[])
            .and_then(|v| v.l())
        {
            Ok(rs) => rs,
            Err(_) => {
                let _ = env.call_method(&statement, "close", "()V", &[]);
                return Err(classify_query_failure(env, "execute query"));
            }
        };

        let columns = self.read_column_metadata(env, &result_set)?;
        let (messages, row_count, max_offset) = self.read_rows(env, &result_set, &columns)?;

        // Close statement (best-effort)
        let _ = env.call_method(&statement, "close", "()V", &[]);

        Ok((messages, row_count, max_offset))
    }

    /// Read column names and types from the ResultSet metadata.
    fn read_column_metadata(
        &self,
        env: &mut JNIEnv,
        result_set: &JObject,
    ) -> Result<Vec<(String, i32)>, Error> {
        let metadata = env
            .call_method(
                result_set,
                "getMetaData",
                "()Ljava/sql/ResultSetMetaData;",
                &[],
            )
            .map_err(|e| Error::Connection(format!("Failed to get metadata: {}", e)))?
            .l()
            .map_err(|e| Error::Connection(format!("Failed to get metadata object: {}", e)))?;

        let column_count = env
            .call_method(&metadata, "getColumnCount", "()I", &[])
            .map_err(|e| Error::Connection(format!("Failed to get column count: {}", e)))?
            .i()
            .map_err(|e| Error::Connection(format!("Failed to extract column count: {}", e)))?;

        info!("Query returned {} columns", column_count);

        let mut columns = Vec::with_capacity(column_count as usize);
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
        let mut messages = Vec::new();
        let mut row_count: u64 = 0;
        let mut max_offset: Option<String> = None;

        loop {
            let has_next = env
                .call_method(result_set, "next", "()Z", &[])
                .map_err(|e| Error::Connection(format!("Failed to fetch next row: {}", e)))?
                .z()
                .map_err(|e| Error::Connection(format!("Failed to extract boolean: {}", e)))?;

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

            if let Some(offset) = offset {
                max_offset = Some(offset);
            }

            let message = self.build_message(row_data)?;
            messages.push(message);
            row_count += 1;
        }

        Ok((messages, row_count, max_offset))
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

            row_data.insert(final_col_name.clone(), value);

            // Track offset if this is the tracking column
            if let Some(ref tracking_col) = self.config.tracking_column
                && col_name == tracking_col
            {
                offset = Some(self.extract_offset_value(&row_data, &final_col_name));
            }
        }

        Ok((row_data, offset))
    }

    /// Build a ProducedMessage from row data, optionally wrapping in DatabaseRecord metadata.
    fn build_message(
        &self,
        row_data: serde_json::Map<String, serde_json::Value>,
    ) -> Result<ProducedMessage, Error> {
        let payload = if self.config.include_metadata {
            let record = DatabaseRecord {
                table_name: None,
                operation_type: "SELECT".to_string(),
                timestamp: Utc::now(),
                data: serde_json::Value::Object(row_data),
            };
            serde_json::to_vec(&record)
                .map_err(|e| Error::Serialization(format!("Failed to serialize record: {}", e)))?
        } else {
            serde_json::to_vec(&serde_json::Value::Object(row_data))
                .map_err(|e| Error::Serialization(format!("Failed to serialize row data: {}", e)))?
        };

        Ok(ProducedMessage {
            id: Some(Uuid::new_v4().as_u128()),
            payload,
            headers: None,
            checksum: None,
            timestamp: Some(Utc::now().timestamp_millis() as u64),
            origin_timestamp: Some(Utc::now().timestamp_millis() as u64),
        })
    }

    /// Build query with offset placeholder replacement.
    /// Row limiting is handled via JDBC setMaxRows rather than SQL LIMIT
    /// to ensure cross-database compatibility.
    fn build_query(&self, state: &State) -> String {
        let mut query = self.config.query.clone();

        if self.config.mode == Mode::Incremental {
            if let Some(ref offset) = state.last_offset {
                query = query.replace("{last_offset}", &quote_sql_literal(offset));
            } else if let Some(ref initial) = self.config.initial_offset {
                query = query.replace("{last_offset}", &quote_sql_literal(initial));
            } else {
                // Remove WHERE clause if no offset available
                query = query.replace("WHERE {tracking_column} > {last_offset}", "");
            }
        }

        query
    }

    /// Get column name from ResultSetMetaData
    fn get_column_name(
        &self,
        env: &mut JNIEnv,
        metadata: &JObject,
        column_index: i32,
    ) -> Result<String, Error> {
        let col_name_obj = env
            .call_method(
                metadata,
                "getColumnName",
                "(I)Ljava/lang/String;",
                &[JValue::Int(column_index)],
            )
            .map_err(|e| Error::Connection(format!("Failed to get column name: {}", e)))?
            .l()
            .map_err(|e| Error::Connection(format!("Failed to get column name object: {}", e)))?;

        let col_name: String = env
            .get_string(&JString::from(col_name_obj))
            .map_err(|e| Error::Connection(format!("Failed to convert column name: {}", e)))?
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
        let col_type = env
            .call_method(
                metadata,
                "getColumnType",
                "(I)I",
                &[JValue::Int(column_index)],
            )
            .map_err(|e| Error::Connection(format!("Failed to get column type: {}", e)))?
            .i()
            .map_err(|e| Error::Connection(format!("Failed to extract column type: {}", e)))?;

        Ok(col_type)
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

        // Check if null first
        let obj = env
            .call_method(
                result_set,
                "getObject",
                "(I)Ljava/lang/Object;",
                &[JValue::Int(column_index)],
            )
            .map_err(|e| Error::Connection(format!("Failed to get object: {}", e)))?
            .l()
            .map_err(|e| Error::Connection(format!("Failed to get object reference: {}", e)))?;

        if obj.is_null() {
            return Ok(serde_json::Value::Null);
        }

        match *sql_type {
            Types::BIT | Types::BOOLEAN => {
                let value = env
                    .call_method(
                        result_set,
                        "getBoolean",
                        "(I)Z",
                        &[JValue::Int(column_index)],
                    )
                    .map_err(|e| Error::Connection(format!("Failed to get boolean: {}", e)))?
                    .z()
                    .map_err(|e| Error::Connection(format!("Failed to extract boolean: {}", e)))?;
                Ok(serde_json::Value::Bool(value))
            }
            Types::TINYINT | Types::SMALLINT | Types::INTEGER => {
                let value = env
                    .call_method(result_set, "getInt", "(I)I", &[JValue::Int(column_index)])
                    .map_err(|e| Error::Connection(format!("Failed to get int: {}", e)))?
                    .i()
                    .map_err(|e| Error::Connection(format!("Failed to extract int: {}", e)))?;
                Ok(serde_json::json!(value))
            }
            Types::BIGINT => {
                let value = env
                    .call_method(result_set, "getLong", "(I)J", &[JValue::Int(column_index)])
                    .map_err(|e| Error::Connection(format!("Failed to get long: {}", e)))?
                    .j()
                    .map_err(|e| Error::Connection(format!("Failed to extract long: {}", e)))?;
                Ok(serde_json::json!(value))
            }
            Types::FLOAT | Types::REAL => {
                let value = env
                    .call_method(result_set, "getFloat", "(I)F", &[JValue::Int(column_index)])
                    .map_err(|e| Error::Connection(format!("Failed to get float: {}", e)))?
                    .f()
                    .map_err(|e| Error::Connection(format!("Failed to extract float: {}", e)))?;
                Ok(serde_json::json!(value))
            }
            Types::DOUBLE => {
                let value = env
                    .call_method(
                        result_set,
                        "getDouble",
                        "(I)D",
                        &[JValue::Int(column_index)],
                    )
                    .map_err(|e| Error::Connection(format!("Failed to get double: {}", e)))?
                    .d()
                    .map_err(|e| Error::Connection(format!("Failed to extract double: {}", e)))?;
                Ok(serde_json::json!(value))
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
                let bytes_obj = env
                    .call_method(
                        result_set,
                        "getBytes",
                        "(I)[B",
                        &[JValue::Int(column_index)],
                    )
                    .map_err(|e| Error::Connection(format!("Failed to get bytes: {}", e)))?
                    .l()
                    .map_err(|e| Error::Connection(format!("Failed to get bytes object: {}", e)))?;
                if bytes_obj.is_null() {
                    return Ok(serde_json::Value::Null);
                }
                let buf = env
                    .convert_byte_array(JByteArray::from(bytes_obj))
                    .map_err(|e| Error::Connection(format!("Failed to convert bytes: {}", e)))?;
                use base64::Engine;
                Ok(serde_json::Value::String(
                    base64::engine::general_purpose::STANDARD.encode(&buf),
                ))
            }
            Types::TIMESTAMP | Types::DATE | Types::TIME => {
                let value = env
                    .call_method(
                        result_set,
                        "getString",
                        "(I)Ljava/lang/String;",
                        &[JValue::Int(column_index)],
                    )
                    .map_err(|e| Error::Connection(format!("Failed to get timestamp: {}", e)))?
                    .l()
                    .map_err(|e| {
                        Error::Connection(format!("Failed to get timestamp object: {}", e))
                    })?;
                let str_value: String = env
                    .get_string(&JString::from(value))
                    .map_err(|e| Error::Connection(format!("Failed to convert timestamp: {}", e)))?
                    .into();
                Ok(serde_json::Value::String(str_value))
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
        let value = env
            .call_method(
                result_set,
                "getString",
                "(I)Ljava/lang/String;",
                &[JValue::Int(column_index)],
            )
            .map_err(|e| Error::Connection(format!("Failed to get string: {}", e)))?
            .l()
            .map_err(|e| Error::Connection(format!("Failed to get string object: {}", e)))?;

        if value.is_null() {
            Ok(serde_json::Value::Null)
        } else {
            let str_value: String = env
                .get_string(&JString::from(value))
                .map_err(|e| Error::Connection(format!("Failed to convert string: {}", e)))?
                .into();
            Ok(serde_json::Value::String(str_value))
        }
    }

    /// Extract offset value as string
    fn extract_offset_value(
        &self,
        row_data: &serde_json::Map<String, serde_json::Value>,
        col_name: &str,
    ) -> String {
        row_data
            .get(col_name)
            .map(|v| match v {
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::String(s) => s.clone(),
                _ => v.to_string(),
            })
            .unwrap_or_default()
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

        // Initialize JVM
        self.initialize_jvm()?;

        // Create database connection
        self.create_connection()?;

        info!("JDBC source connector [{}] opened successfully", self.id);
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // Sleep for poll interval
        tokio::time::sleep(self.config.poll_interval).await;

        let jvm = self
            .jvm
            .as_ref()
            .ok_or_else(|| Error::InitError("JVM not initialized".to_string()))?;

        let mut env = jvm
            .attach_current_thread()
            .map_err(|e| Error::InitError(format!("Failed to attach thread: {}", e)))?;

        // Execute query and fetch results
        let messages = self.execute_query(&mut env)?;

        // Persist state so offsets survive connector restarts
        let connector_state = {
            let state = self.state.lock().expect("state mutex poisoned");
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

        if let Some(jvm) = &self.jvm
            && let Ok(mut env) = jvm.attach_current_thread()
        {
            // Close connection pool if exists
            if let Some(pool) = &self.connection_pool {
                let _ = env.call_method(pool.as_obj(), "close", "()V", &[]);
                info!("Connection pool closed");
            }

            // Close direct connection if exists
            if let Some(connection) = self
                .connection
                .lock()
                .expect("connection mutex poisoned")
                .as_ref()
            {
                let _ = env.call_method(connection.as_obj(), "close", "()V", &[]);
                info!("Database connection closed");
            }
        }

        let state = self.state.lock().expect("state mutex poisoned");
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
            result.push(ch.to_lowercase().next().unwrap());
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
    let mut guard = GLOBAL_JVM.lock().expect("jvm mutex poisoned");
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

/// Quote a value as a SQL string literal, escaping embedded single quotes by
/// doubling them. Used to substitute the incremental `{last_offset}` value,
/// which originates from a (DB-controlled) tracking-column value.
fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Classify a JDBC `SQLState` class (first 2 chars) as transient vs permanent.
/// `08` connection, `40` rollback/serialization, `53` resources, `57` operator
/// intervention, `58` system error are transient; everything else (and an
/// unknown/absent state) is permanent.
fn is_transient_sql_state(sql_state: Option<&str>) -> bool {
    match sql_state {
        Some(s) if s.len() >= 2 => matches!(&s[..2], "08" | "40" | "53" | "57" | "58"),
        _ => false,
    }
}

/// Inspect and CLEAR the pending Java exception after a failed query JNI call,
/// returning a classified `Error`: transient SQL states map to
/// `Error::Connection` (so the runtime re-polls and the connection is
/// re-validated), permanent ones to `Error::InvalidRecordValue`. Clearing is
/// required so the next JNI call on this thread is not aborted.
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: Some("0".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        // With initial offset (no last_offset yet)
        let state = State {
            last_offset: None,
            processed_rows: 0,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state);
        assert_eq!(query, "SELECT * FROM users WHERE id > '0' ORDER BY id");

        // With tracked offset
        let state = State {
            last_offset: Some("42".to_string()),
            processed_rows: 42,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state);
        assert_eq!(query, "SELECT * FROM users WHERE id > '42' ORDER BY id");
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
            poll_interval: Duration::from_secs(60),
            batch_size: 5000,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: false,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = State::default();
        let query = source.build_query(&state);
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
            poll_interval: Duration::from_secs(30),
            batch_size: 1000,
            tracking_column: Some("updated_at".to_string()),
            initial_offset: Some("2024-01-01 00:00:00".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
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
        assert_eq!(config.poll_interval, Duration::from_secs(30));
        // Verify defaults are applied
        assert_eq!(config.mode, Mode::Incremental);
        assert_eq!(config.batch_size, 1000);
        assert!(config.include_metadata);
        assert!(!config.snake_case_columns);
        assert!(!config.enable_connection_pool);
        assert_eq!(config.max_pool_size, 10);
        assert_eq!(config.min_idle, 2);
        assert_eq!(config.connection_timeout_ms, 30000);
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
            enable_connection_pool = true
            max_pool_size = 20
            min_idle = 5
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
        assert!(config.enable_connection_pool);
        assert_eq!(config.max_pool_size, 20);
        assert_eq!(config.min_idle, 5);
        assert_eq!(config.connection_timeout_ms, 60000);
        assert_eq!(config.poll_interval, Duration::from_secs(300));
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
        assert_eq!(config.poll_interval, Duration::from_secs(3600));
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: Some("100".to_string()),
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
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
            poll_interval: Duration::from_secs(60),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), serde_json::json!(42));
        assert_eq!(source.extract_offset_value(&row, "id"), "42");
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
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
            "2024-06-15 12:00:00"
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let mut row = serde_json::Map::new();
        row.insert("version".to_string(), serde_json::json!(3.5));
        assert_eq!(source.extract_offset_value(&row, "version"), "3.5");
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), serde_json::Value::Null);
        assert_eq!(source.extract_offset_value(&row, "id"), "null");
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);

        let row = serde_json::Map::new();
        assert_eq!(source.extract_offset_value(&row, "nonexistent"), "");
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: None,
            mode: Mode::Incremental,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = State {
            last_offset: None,
            processed_rows: 0,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state);
        // The WHERE clause placeholder should be removed
        assert!(
            !query.contains("{last_offset}"),
            "Query should not contain unresolved placeholder: {}",
            query
        );
    }

    #[test]
    fn test_build_query_bulk_mode_ignores_offset() {
        let config = JdbcSourceConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            query: "SELECT * FROM users WHERE id > {last_offset}".to_string(),
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: Some("id".to_string()),
            initial_offset: Some("0".to_string()),
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };
        let source = JdbcSource::new(1, config, None);
        let state = State {
            last_offset: Some("42".to_string()),
            processed_rows: 42,
            last_poll_time: Utc::now(),
        };
        let query = source.build_query(&state);
        // In bulk mode, offset placeholders are NOT replaced
        assert!(
            query.contains("{last_offset}"),
            "Bulk mode should not replace offset placeholders: {}",
            query
        );
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
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
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            tracking_column: None,
            initial_offset: None,
            mode: Mode::Bulk,
            snake_case_columns: false,
            include_metadata: true,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        };

        let debug_output = format!("{:?}", config);
        // Should not panic and should contain the struct name
        assert!(debug_output.contains("JdbcSourceConfig"));
    }
}
