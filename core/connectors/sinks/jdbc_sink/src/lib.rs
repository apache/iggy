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

//! Generic JDBC sink connector for Iggy.
//!
//! Consumes messages from Iggy topics and writes them, in batches, into any
//! JDBC-compliant database (MySQL, PostgreSQL, Oracle, SQL Server, H2, ...).
//!
//! The connector talks to the database through an embedded JVM via JNI, using
//! the standard `java.sql` API and (optionally) a HikariCP connection pool,
//! mirroring the JDBC *source* connector so a single JDBC driver JAR works for
//! both directions.
//!
//! Write semantics are **INSERT-only** (matching the other Iggy sink
//! connectors): each message becomes one row. The payload is stored in a
//! single column (`text`/`json`/`bytes`), alongside optional Iggy metadata
//! columns.

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use jni::objects::{GlobalRef, JObject, JString, JThrowable, JValue};
use jni::{JNIEnv, JavaVM};
use regex::Regex;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, warn};

const CONNECTOR_NAME: &str = "JDBC sink";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_PAYLOAD_COLUMN: &str = "payload";

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

/// Cached compiled regex patterns for password sanitization in log output.
static RE_USER_PASS_AT: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"://([^:]+):([^@?;/]+)@").unwrap());
static RE_PASSWORD_PARAM: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)(password|pwd|pass)=([^;&\s]+)").unwrap());
static RE_ORACLE_PASS: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"thin:([^/]+)/([^@]+)@").unwrap());

/// How the message payload is written into the destination `payload` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    /// Store the raw payload bytes as a UTF-8 string (default).
    #[default]
    Text,
    /// Validate the payload as JSON, then store the JSON text as a string.
    Json,
    /// Store the raw payload bytes as a binary column.
    Bytes,
}

impl PayloadFormat {
    fn from_config(s: Option<&str>) -> Self {
        match s.map(|s| s.to_lowercase()).as_deref() {
            Some("json") | Some("jsonb") => PayloadFormat::Json,
            Some("bytes") | Some("binary") | Some("bytea") => PayloadFormat::Bytes,
            _ => PayloadFormat::Text,
        }
    }

    /// Portable-ish DDL type used by `auto_create_table`. Production users are
    /// encouraged to pre-create tables with database-native types instead.
    fn ddl_type(&self) -> &'static str {
        match self {
            PayloadFormat::Text | PayloadFormat::Json => "TEXT",
            PayloadFormat::Bytes => "VARBINARY(65535)",
        }
    }
}

/// Configuration for the JDBC sink connector.
#[derive(Clone, Deserialize, Serialize)]
pub struct JdbcSinkConfig {
    /// JDBC connection URL (e.g. "jdbc:mysql://localhost:3306/mydb").
    #[serde(with = "secret_string_serde")]
    pub jdbc_url: SecretString,

    /// JDBC driver class name (e.g. "com.mysql.cj.jdbc.Driver").
    pub driver_class: String,

    /// Path to the JDBC driver JAR file.
    pub driver_jar_path: String,

    /// Database username (optional if embedded in `jdbc_url`).
    #[serde(default)]
    pub username: Option<String>,

    /// Database password (optional if embedded in `jdbc_url`); masked in logs.
    #[serde(default, with = "opt_secret_string_serde")]
    pub password: Option<SecretString>,

    /// Target table to insert rows into.
    pub target_table: String,

    /// Max messages per INSERT batch (default 100).
    #[serde(default)]
    pub batch_size: Option<u32>,

    /// Create the target table on `open` if it does not exist (default false).
    #[serde(default)]
    pub auto_create_table: Option<bool>,

    /// Include Iggy metadata columns: offset, timestamp, stream, topic,
    /// partition id (default true).
    #[serde(default)]
    pub include_metadata: Option<bool>,

    /// Include the `iggy_checksum` column (default true).
    #[serde(default)]
    pub include_checksum: Option<bool>,

    /// Include the `iggy_origin_timestamp` column (default true).
    #[serde(default)]
    pub include_origin_timestamp: Option<bool>,

    /// Payload column format: "text" (default), "json", or "bytes".
    #[serde(default)]
    pub payload_format: Option<String>,

    /// Name of the payload column (default "payload").
    #[serde(default)]
    pub payload_column: Option<String>,

    /// Log at INFO instead of DEBUG for per-batch progress (default false).
    #[serde(default)]
    pub verbose_logging: Option<bool>,

    /// Max retry attempts for a failing batch (default 3).
    #[serde(default)]
    pub max_retries: Option<u32>,

    /// Delay between retries, e.g. "1s", "500ms" (default "1s").
    #[serde(default)]
    pub retry_delay: Option<String>,

    /// Extra JVM options (e.g. ["-Xmx512m"]).
    #[serde(default)]
    pub jvm_options: Vec<String>,

    /// Enable HikariCP connection pooling (default false).
    #[serde(default)]
    pub enable_connection_pool: bool,

    /// Maximum pool size (default 10).
    #[serde(default = "default_pool_size")]
    pub max_pool_size: u32,

    /// Minimum idle connections (default 2).
    #[serde(default = "default_min_idle")]
    pub min_idle: u32,

    /// Connection timeout in milliseconds (default 30000).
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

impl std::fmt::Debug for JdbcSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JdbcSinkConfig")
            .field(
                "jdbc_url",
                &sanitize_jdbc_url(self.jdbc_url.expose_secret()),
            )
            .field("driver_class", &self.driver_class)
            .field("driver_jar_path", &self.driver_jar_path)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("target_table", &self.target_table)
            .field("batch_size", &self.batch_size)
            .field("auto_create_table", &self.auto_create_table)
            .field("include_metadata", &self.include_metadata)
            .field("include_checksum", &self.include_checksum)
            .field("include_origin_timestamp", &self.include_origin_timestamp)
            .field("payload_format", &self.payload_format)
            .field("payload_column", &self.payload_column)
            .field("enable_connection_pool", &self.enable_connection_pool)
            .field("max_pool_size", &self.max_pool_size)
            .field("min_idle", &self.min_idle)
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .finish()
    }
}

/// Runtime counters for observability.
#[derive(Debug, Default)]
struct State {
    messages_processed: u64,
    insertion_errors: u64,
}

/// JDBC sink connector.
#[derive(Debug)]
pub struct JdbcSink {
    id: u32,
    config: JdbcSinkConfig,
    jvm: Option<Arc<JavaVM>>,
    connection: Option<GlobalRef>,
    connection_pool: Option<GlobalRef>, // HikariDataSource if pooling enabled
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
}

/// Sanitize a JDBC URL by masking passwords for logging.
fn sanitize_jdbc_url(url: &str) -> String {
    let url = RE_USER_PASS_AT.replace_all(url, "://$1:***@");
    let url = RE_PASSWORD_PARAM.replace_all(&url, "$1=***");
    let url = RE_ORACLE_PASS.replace_all(&url, "thin:$1/***@");
    url.to_string()
}

/// Quote a SQL identifier (table/column name) with double quotes, escaping any
/// embedded quotes, and reject names containing null bytes. Prevents SQL
/// injection through configured table/column names.
fn quote_identifier(name: &str) -> Result<String, Error> {
    if name.is_empty() {
        return Err(Error::InvalidConfigValue(
            "Identifier cannot be empty".to_string(),
        ));
    }
    if name.contains('\0') {
        return Err(Error::InvalidConfigValue(
            "Identifier cannot contain null characters".to_string(),
        ));
    }
    let escaped = name.replace('"', "\"\"");
    Ok(format!("\"{escaped}\""))
}

impl JdbcSink {
    /// Construct a new JDBC sink. Invoked by the `sink_connector!` macro with
    /// the deserialized plugin config.
    pub fn new(id: u32, config: JdbcSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = humantime::parse_duration(delay_str).unwrap_or(Duration::from_secs(1));
        Self {
            id,
            config,
            jvm: None,
            connection: None,
            connection_pool: None,
            state: Mutex::new(State::default()),
            verbose,
            retry_delay,
        }
    }

    fn batch_size(&self) -> usize {
        self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize
    }

    fn include_metadata(&self) -> bool {
        self.config.include_metadata.unwrap_or(true)
    }

    fn include_checksum(&self) -> bool {
        self.config.include_checksum.unwrap_or(true)
    }

    fn include_origin_timestamp(&self) -> bool {
        self.config.include_origin_timestamp.unwrap_or(true)
    }

    fn payload_format(&self) -> PayloadFormat {
        PayloadFormat::from_config(self.config.payload_format.as_deref())
    }

    fn payload_column(&self) -> &str {
        self.config
            .payload_column
            .as_deref()
            .unwrap_or(DEFAULT_PAYLOAD_COLUMN)
    }

    fn max_retries(&self) -> u32 {
        self.config
            .max_retries
            .unwrap_or(DEFAULT_MAX_RETRIES)
            .max(1)
    }

    /// Obtain the process-wide JVM, creating it on first use. JNI permits only a
    /// single JVM per OS process, so this is shared across all JDBC sink
    /// instances (see [`get_or_create_jvm`]).
    fn initialize_jvm(&mut self) -> Result<(), Error> {
        info!("Initializing JVM for JDBC sink connector [{}]", self.id);
        let jvm = get_or_create_jvm(&self.config.driver_jar_path, &self.config.jvm_options)?;
        self.jvm = Some(jvm);
        Ok(())
    }

    /// Load the JDBC driver and create a direct connection or a HikariCP pool.
    fn create_connection(&mut self) -> Result<(), Error> {
        let jvm = self
            .jvm
            .as_ref()
            .ok_or_else(|| Error::InitError("JVM not initialized".to_string()))?;

        let mut env = jvm
            .attach_current_thread()
            .map_err(|e| Error::InitError(format!("Failed to attach thread to JVM: {e}")))?;

        info!(
            "Loading JDBC driver via Class.forName: {}",
            self.config.driver_class
        );

        let class_class = env
            .find_class("java/lang/Class")
            .map_err(|e| Error::InitError(format!("Failed to find Class: {e}")))?;
        let driver_class_name = env
            .new_string(&self.config.driver_class)
            .map_err(|e| Error::InitError(format!("Failed to create class name string: {e}")))?;
        env.call_static_method(
            class_class,
            "forName",
            "(Ljava/lang/String;)Ljava/lang/Class;",
            &[JValue::Object(&driver_class_name.into())],
        )
        .map_err(|e| {
            Error::InitError(format!(
                "Failed to load driver class '{}': {e}",
                self.config.driver_class
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
            self.connection = Some(conn);
        }

        Ok(())
    }

    /// Create a direct JDBC connection via DriverManager.
    fn create_direct_connection_internal(&self, env: &mut JNIEnv) -> Result<GlobalRef, Error> {
        // Set the thread context class loader to the driver's loader so
        // DriverManager can locate the driver loaded from the JAR.
        let thread_class = env
            .find_class("java/lang/Thread")
            .map_err(|e| Error::InitError(format!("Failed to find Thread class: {e}")))?;
        let current_thread = env
            .call_static_method(thread_class, "currentThread", "()Ljava/lang/Thread;", &[])
            .map_err(|e| Error::InitError(format!("Failed to get current thread: {e}")))?
            .l()
            .map_err(|e| Error::InitError(format!("Failed to extract thread object: {e}")))?;

        let driver_class = env
            .find_class(self.config.driver_class.replace('.', "/"))
            .map_err(|e| Error::InitError(format!("Failed to find driver class: {e}")))?;
        let driver_class_loader = env
            .call_method(
                &driver_class,
                "getClassLoader",
                "()Ljava/lang/ClassLoader;",
                &[],
            )
            .map_err(|e| Error::InitError(format!("Failed to get driver class loader: {e}")))?
            .l()
            .map_err(|e| Error::InitError(format!("Failed to extract class loader: {e}")))?;
        env.call_method(
            &current_thread,
            "setContextClassLoader",
            "(Ljava/lang/ClassLoader;)V",
            &[JValue::Object(&driver_class_loader)],
        )
        .map_err(|e| Error::InitError(format!("Failed to set context class loader: {e}")))?;

        let driver_manager = env
            .find_class("java/sql/DriverManager")
            .map_err(|e| Error::InitError(format!("Failed to find DriverManager: {e}")))?;
        let jdbc_url = env
            .new_string(self.config.jdbc_url.expose_secret())
            .map_err(|e| Error::InitError(format!("Failed to create JDBC URL string: {e}")))?;

        let connection = if let (Some(username), Some(password)) =
            (&self.config.username, &self.config.password)
        {
            let username_jstring = env
                .new_string(username)
                .map_err(|e| Error::InitError(format!("Failed to create username string: {e}")))?;
            let password_jstring = env
                .new_string(password.expose_secret())
                .map_err(|e| Error::InitError(format!("Failed to create password string: {e}")))?;
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
                    "Failed to create JDBC connection with credentials: {e}"
                ))
            })?
        } else {
            env.call_static_method(
                driver_manager,
                "getConnection",
                "(Ljava/lang/String;)Ljava/sql/Connection;",
                &[JValue::Object(&jdbc_url.into())],
            )
            .map_err(|e| {
                Error::InitError(format!("Failed to create JDBC connection from URL: {e}"))
            })?
        };

        let connection_obj = connection
            .l()
            .map_err(|e| Error::InitError(format!("Failed to get connection object: {e}")))?;
        let global_ref = env
            .new_global_ref(connection_obj)
            .map_err(|e| Error::InitError(format!("Failed to create global reference: {e}")))?;

        info!("Direct database connection established successfully");
        Ok(global_ref)
    }

    /// Create a HikariCP connection pool.
    fn create_connection_pool_internal(&self, env: &mut JNIEnv) -> Result<GlobalRef, Error> {
        info!(
            "Initializing HikariCP with max_pool_size={}, min_idle={}",
            self.config.max_pool_size, self.config.min_idle
        );

        let hikari_config_class = env.find_class("com/zaxxer/hikari/HikariConfig").map_err(
            |e| {
                Error::InitError(format!(
                    "Failed to find HikariConfig class. Ensure HikariCP JAR is in classpath: {e}"
                ))
            },
        )?;
        let hikari_config = env
            .new_object(hikari_config_class, "()V", &[])
            .map_err(|e| Error::InitError(format!("Failed to create HikariConfig: {e}")))?;

        let jdbc_url_jstring = env
            .new_string(self.config.jdbc_url.expose_secret())
            .map_err(|e| Error::InitError(format!("Failed to create JDBC URL: {e}")))?;
        env.call_method(
            &hikari_config,
            "setJdbcUrl",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&jdbc_url_jstring.into())],
        )
        .map_err(|e| Error::InitError(format!("Failed to set JDBC URL: {e}")))?;

        if let Some(username) = &self.config.username {
            let username_jstring = env
                .new_string(username)
                .map_err(|e| Error::InitError(format!("Failed to create username: {e}")))?;
            env.call_method(
                &hikari_config,
                "setUsername",
                "(Ljava/lang/String;)V",
                &[JValue::Object(&username_jstring.into())],
            )
            .map_err(|e| Error::InitError(format!("Failed to set username: {e}")))?;
        }

        if let Some(password) = &self.config.password {
            let password_jstring = env
                .new_string(password.expose_secret())
                .map_err(|e| Error::InitError(format!("Failed to create password: {e}")))?;
            env.call_method(
                &hikari_config,
                "setPassword",
                "(Ljava/lang/String;)V",
                &[JValue::Object(&password_jstring.into())],
            )
            .map_err(|e| Error::InitError(format!("Failed to set password: {e}")))?;
        }

        let driver_class_jstring = env
            .new_string(&self.config.driver_class)
            .map_err(|e| Error::InitError(format!("Failed to create driver class name: {e}")))?;
        env.call_method(
            &hikari_config,
            "setDriverClassName",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&driver_class_jstring.into())],
        )
        .map_err(|e| Error::InitError(format!("Failed to set driver class: {e}")))?;

        env.call_method(
            &hikari_config,
            "setMaximumPoolSize",
            "(I)V",
            &[JValue::Int(
                self.config.max_pool_size.min(i32::MAX as u32) as i32
            )],
        )
        .map_err(|e| Error::InitError(format!("Failed to set max pool size: {e}")))?;
        env.call_method(
            &hikari_config,
            "setMinimumIdle",
            "(I)V",
            &[JValue::Int(self.config.min_idle.min(i32::MAX as u32) as i32)],
        )
        .map_err(|e| Error::InitError(format!("Failed to set min idle: {e}")))?;
        env.call_method(
            &hikari_config,
            "setConnectionTimeout",
            "(J)V",
            &[JValue::Long(self.config.connection_timeout_ms as i64)],
        )
        .map_err(|e| Error::InitError(format!("Failed to set connection timeout: {e}")))?;

        let hikari_datasource_class = env
            .find_class("com/zaxxer/hikari/HikariDataSource")
            .map_err(|e| Error::InitError(format!("Failed to find HikariDataSource class: {e}")))?;
        let datasource = env
            .new_object(
                hikari_datasource_class,
                "(Lcom/zaxxer/hikari/HikariConfig;)V",
                &[JValue::Object(&hikari_config)],
            )
            .map_err(|e| Error::InitError(format!("Failed to create HikariDataSource: {e}")))?;
        let global_ref = env.new_global_ref(datasource).map_err(|e| {
            Error::InitError(format!("Failed to create global reference for pool: {e}"))
        })?;

        info!("HikariCP connection pool created successfully");
        Ok(global_ref)
    }

    /// Acquire a connection. Returns the connection object and whether it was
    /// borrowed from the pool (and therefore must be closed after use).
    fn acquire_connection<'local>(
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
                .map_err(|e| Error::Connection(format!("Failed to get connection from pool: {e}")))?
                .l()
                .map_err(|e| {
                    Error::Connection(format!("Failed to extract connection object: {e}"))
                })?;
            Ok((connection, true))
        } else if let Some(conn) = &self.connection {
            let local_ref = env
                .new_local_ref(conn.as_obj())
                .map_err(|e| Error::Connection(format!("Failed to create local ref: {e}")))?;
            Ok((local_ref, false))
        } else {
            Err(Error::Connection(
                "No connection or pool available".to_string(),
            ))
        }
    }

    /// Create the target table if `auto_create_table` is enabled. Uses
    /// portable-ish ANSI types; production deployments should pre-create the
    /// table with database-native types.
    fn ensure_table_exists(&self, env: &mut JNIEnv) -> Result<(), Error> {
        if !self.config.auto_create_table.unwrap_or(false) {
            return Ok(());
        }

        let sql = self.build_create_table_sql()?;
        info!("Ensuring target table exists: {sql}");

        let (connection, pooled) = self.acquire_connection(env)?;
        let result = self.execute_update(env, &connection, &sql);
        if pooled {
            let _ = env.call_method(&connection, "close", "()V", &[]);
        }
        result.map(|_| ()).map_err(|e| {
            Error::InitError(format!(
                "Failed to create table '{}': {e}",
                self.config.target_table
            ))
        })
    }

    /// Validate connectivity at open time with a trivial `SELECT 1`, so a bad
    /// URL/credentials fails fast on open rather than on the first batch.
    fn smoke_test(&self, env: &mut JNIEnv) -> Result<(), Error> {
        let (connection, pooled) = self.acquire_connection(env)?;
        let result = self.execute_update(env, &connection, "SELECT 1");
        if pooled {
            let _ = env.call_method(&connection, "close", "()V", &[]);
        }
        result.map(|_| ()).map_err(|e| {
            Error::InitError(format!("Database connectivity test (SELECT 1) failed: {e}"))
        })
    }

    /// Build the `CREATE TABLE IF NOT EXISTS` statement for the configured
    /// column layout.
    fn build_create_table_sql(&self) -> Result<String, Error> {
        let quoted_table = quote_identifier(&self.config.target_table)?;
        let quoted_payload = quote_identifier(self.payload_column())?;

        let mut sql = format!("CREATE TABLE IF NOT EXISTS {quoted_table} (");
        sql.push_str("id VARCHAR(40)");

        if self.include_metadata() {
            sql.push_str(", iggy_offset BIGINT");
            sql.push_str(", iggy_timestamp BIGINT");
            sql.push_str(", iggy_stream VARCHAR(255)");
            sql.push_str(", iggy_topic VARCHAR(255)");
            sql.push_str(", iggy_partition_id INTEGER");
        }
        if self.include_checksum() {
            sql.push_str(", iggy_checksum BIGINT");
        }
        if self.include_origin_timestamp() {
            sql.push_str(", iggy_origin_timestamp BIGINT");
        }

        sql.push_str(&format!(
            ", {quoted_payload} {}",
            self.payload_format().ddl_type()
        ));
        sql.push(')');
        Ok(sql)
    }

    /// Build a parameterized single-row INSERT statement (one `?` placeholder
    /// per bound column) and report how many parameters each row binds.
    fn build_insert_sql(&self) -> Result<(String, u32), Error> {
        let quoted_table = quote_identifier(&self.config.target_table)?;
        let quoted_payload = quote_identifier(self.payload_column())?;

        let mut columns = vec!["id".to_string()];
        if self.include_metadata() {
            columns.push("iggy_offset".to_string());
            columns.push("iggy_timestamp".to_string());
            columns.push("iggy_stream".to_string());
            columns.push("iggy_topic".to_string());
            columns.push("iggy_partition_id".to_string());
        }
        if self.include_checksum() {
            columns.push("iggy_checksum".to_string());
        }
        if self.include_origin_timestamp() {
            columns.push("iggy_origin_timestamp".to_string());
        }
        columns.push(quoted_payload);

        let params_per_row = columns.len() as u32;
        let placeholders = vec!["?"; columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {quoted_table} ({}) VALUES ({placeholders})",
            columns.join(", ")
        );
        Ok((sql, params_per_row))
    }

    /// Write all messages, chunked into batches. Errors on a batch are counted
    /// and logged; the call only returns `Err` when a batch ultimately fails so
    /// the runtime can surface the failure.
    fn write_messages(
        &self,
        env: &mut JNIEnv,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let (sql, _params_per_row) = self.build_insert_sql()?;
        let (connection, pooled) = self.acquire_connection(env)?;

        // A transient failure (connectivity, deadlock) is surfaced so the
        // runtime stops the sink and a restart retries the batch. A permanent
        // failure (bad data, constraint/syntax error) is counted and SKIPPED so
        // one poison batch does not permanently halt ingestion.
        let mut first_transient_error: Option<Error> = None;
        let table = &self.config.target_table;
        for batch in messages.chunks(self.batch_size()) {
            if let Err((e, is_transient)) = self.insert_batch_with_retry(
                env,
                &connection,
                &sql,
                batch,
                topic_metadata,
                messages_metadata,
            ) {
                let mut state = self.state.lock().expect("state mutex poisoned");
                state.insertion_errors += batch.len() as u64;
                if is_transient {
                    error!("Transient failure inserting batch into '{table}': {e}");
                    if first_transient_error.is_none() {
                        first_transient_error = Some(e);
                    }
                } else {
                    error!(
                        "Permanent failure inserting batch into '{table}'; skipping {} message(s): {e}",
                        batch.len()
                    );
                }
            }
        }

        if pooled {
            let _ = env.call_method(&connection, "close", "()V", &[]);
        }

        if let Some(e) = first_transient_error {
            return Err(e);
        }

        let msg_count = messages.len();
        {
            let mut state = self.state.lock().expect("state mutex poisoned");
            state.messages_processed += msg_count as u64;
        }
        let table = &self.config.target_table;
        if self.verbose {
            info!(
                "JDBC sink [{}] wrote {msg_count} messages to '{table}'",
                self.id
            );
        } else {
            debug!(
                "JDBC sink [{}] wrote {msg_count} messages to '{table}'",
                self.id
            );
        }
        Ok(())
    }

    /// Execute one batch insert, retrying on transient failures. Returns
    /// `(error, is_transient)` so the caller can decide whether to surface
    /// (transient → restart retries) or skip (permanent data error).
    fn insert_batch_with_retry(
        &self,
        env: &mut JNIEnv,
        connection: &JObject,
        sql: &str,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<(), (Error, bool)> {
        let max_retries = self.max_retries();
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            match self.insert_batch(
                env,
                connection,
                sql,
                messages,
                topic_metadata,
                messages_metadata,
            ) {
                Ok(()) => return Ok(()),
                Err((e, is_transient)) => {
                    if !is_transient || attempts >= max_retries {
                        return Err((e, is_transient));
                    }
                    warn!(
                        "Transient error inserting batch (attempt {attempts}/{max_retries}): {e}. Retrying..."
                    );
                    std::thread::sleep(self.retry_delay * attempts);
                }
            }
        }
    }

    /// Prepare the statement, bind every message as a batch row, and execute.
    /// Returns `(error, is_transient)` on failure so the caller can decide
    /// whether to retry.
    fn insert_batch(
        &self,
        env: &mut JNIEnv,
        connection: &JObject,
        sql: &str,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<(), (Error, bool)> {
        let sql_jstring = env.new_string(sql).map_err(|e| {
            (
                Error::Connection(format!("Failed to create SQL string: {e}")),
                false,
            )
        })?;
        let statement = match env
            .call_method(
                connection,
                "prepareStatement",
                "(Ljava/lang/String;)Ljava/sql/PreparedStatement;",
                &[JValue::Object(&sql_jstring.into())],
            )
            .and_then(|v| v.l())
        {
            Ok(s) => s,
            Err(_) => return Err(classify_jni_failure(env, "prepare statement")),
        };

        let format = self.payload_format();
        for message in messages {
            // Bind + addBatch each message inside its own JNI local-reference
            // frame: the bound parameter refs (id/stream/topic strings, payload
            // string/byte[]) are reclaimed every iteration. addBatch copies the
            // parameters into the batch, so nothing needs to escape the frame.
            // Without this a large batch would overflow the local reference
            // table and abort the JVM.
            if env.push_local_frame(32).is_err() {
                let _ = env.call_method(&statement, "close", "()V", &[]);
                return Err((
                    Error::CannotStoreData("Failed to push JNI local frame".to_string()),
                    true,
                ));
            }
            let bind = self.bind_row(
                env,
                &statement,
                message,
                topic_metadata,
                messages_metadata,
                format,
            );
            let add_failed =
                bind.is_ok() && env.call_method(&statement, "addBatch", "()V", &[]).is_err();
            // SAFETY: addBatch already copied the parameter values into the
            // batch; no JNI local reference from this iteration needs to escape.
            let _ = unsafe { env.pop_local_frame(&JObject::null()) };

            if let Err(e) = bind {
                let _ = env.call_method(&statement, "close", "()V", &[]);
                // Binding errors (bad UTF-8/JSON) are data problems, permanent.
                return Err((e, false));
            }
            if add_failed {
                let err = classify_jni_failure(env, "add batch");
                let _ = env.call_method(&statement, "close", "()V", &[]);
                return Err(err);
            }
        }

        let failed = env
            .call_method(&statement, "executeBatch", "()[I", &[])
            .is_err();
        let result = if failed {
            Err(classify_jni_failure(env, "execute batch"))
        } else {
            Ok(())
        };
        let _ = env.call_method(&statement, "close", "()V", &[]);
        result
    }

    /// Bind a single message's columns onto the prepared statement (1-indexed).
    fn bind_row(
        &self,
        env: &mut JNIEnv,
        statement: &JObject,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        format: PayloadFormat,
    ) -> Result<(), Error> {
        let mut idx: i32 = 1;

        set_string(env, statement, idx, &message.id.to_string())?;
        idx += 1;

        if self.include_metadata() {
            set_long(env, statement, idx, message.offset as i64)?;
            idx += 1;
            set_long(env, statement, idx, message.timestamp as i64)?;
            idx += 1;
            set_string(env, statement, idx, &topic_metadata.stream)?;
            idx += 1;
            set_string(env, statement, idx, &topic_metadata.topic)?;
            idx += 1;
            set_int(env, statement, idx, messages_metadata.partition_id as i32)?;
            idx += 1;
        }
        if self.include_checksum() {
            set_long(env, statement, idx, message.checksum as i64)?;
            idx += 1;
        }
        if self.include_origin_timestamp() {
            set_long(env, statement, idx, message.origin_timestamp as i64)?;
            idx += 1;
        }

        let payload_bytes = message
            .payload
            .try_to_bytes()
            .map_err(|e| Error::Serialization(format!("Failed to read payload bytes: {e}")))?;

        match format {
            PayloadFormat::Text => {
                // Tolerant of binary schemas (Raw/Avro/FlatBuffer): valid UTF-8 is
                // stored verbatim, otherwise the bytes are base64-encoded so a
                // binary payload is preserved rather than rejected.
                let text = payload_text_repr(payload_bytes);
                set_string(env, statement, idx, &text)?;
            }
            PayloadFormat::Json => {
                // Validate it parses as JSON, then store the (compact) text.
                let value: serde_json::Value =
                    serde_json::from_slice(&payload_bytes).map_err(|e| {
                        Error::InvalidRecordValue(format!("Payload is not valid JSON: {e}"))
                    })?;
                set_string(env, statement, idx, &value.to_string())?;
            }
            PayloadFormat::Bytes => {
                set_bytes(env, statement, idx, &payload_bytes)?;
            }
        }
        Ok(())
    }

    /// Execute a non-query statement (DDL) via `Statement.execute`.
    fn execute_update(
        &self,
        env: &mut JNIEnv,
        connection: &JObject,
        sql: &str,
    ) -> Result<(), Error> {
        let statement = env
            .call_method(connection, "createStatement", "()Ljava/sql/Statement;", &[])
            .and_then(|v| v.l())
            .map_err(|e| Error::Connection(format!("Failed to create statement: {e}")))?;
        let sql_jstring = env
            .new_string(sql)
            .map_err(|e| Error::Connection(format!("Failed to create SQL string: {e}")))?;
        let result = env
            .call_method(
                &statement,
                "execute",
                "(Ljava/lang/String;)Z",
                &[JValue::Object(&sql_jstring.into())],
            )
            .map(|_| ())
            .map_err(|e| Error::CannotStoreData(format!("Failed to execute statement: {e}")));
        let _ = env.call_method(&statement, "close", "()V", &[]);
        result
    }
}

fn set_string(env: &mut JNIEnv, statement: &JObject, idx: i32, value: &str) -> Result<(), Error> {
    let jstr = env
        .new_string(value)
        .map_err(|e| Error::Connection(format!("Failed to create string param: {e}")))?;
    env.call_method(
        statement,
        "setString",
        "(ILjava/lang/String;)V",
        &[JValue::Int(idx), JValue::Object(&jstr.into())],
    )
    .map(|_| ())
    .map_err(|e| Error::CannotStoreData(format!("Failed to set string param {idx}: {e}")))
}

fn set_long(env: &mut JNIEnv, statement: &JObject, idx: i32, value: i64) -> Result<(), Error> {
    env.call_method(
        statement,
        "setLong",
        "(IJ)V",
        &[JValue::Int(idx), JValue::Long(value)],
    )
    .map(|_| ())
    .map_err(|e| Error::CannotStoreData(format!("Failed to set long param {idx}: {e}")))
}

fn set_int(env: &mut JNIEnv, statement: &JObject, idx: i32, value: i32) -> Result<(), Error> {
    env.call_method(
        statement,
        "setInt",
        "(II)V",
        &[JValue::Int(idx), JValue::Int(value)],
    )
    .map(|_| ())
    .map_err(|e| Error::CannotStoreData(format!("Failed to set int param {idx}: {e}")))
}

fn set_bytes(env: &mut JNIEnv, statement: &JObject, idx: i32, value: &[u8]) -> Result<(), Error> {
    let byte_array = env
        .byte_array_from_slice(value)
        .map_err(|e| Error::Connection(format!("Failed to create byte array param: {e}")))?;
    env.call_method(
        statement,
        "setBytes",
        "(I[B)V",
        &[JValue::Int(idx), JValue::Object(&byte_array.into())],
    )
    .map(|_| ())
    .map_err(|e| Error::CannotStoreData(format!("Failed to set bytes param {idx}: {e}")))
}

/// Process-wide JVM. JNI allows only one `JavaVM` per OS process, so every JDBC
/// sink instance in this dynamic library shares this one.
static GLOBAL_JVM: Mutex<Option<Arc<JavaVM>>> = Mutex::new(None);

/// Return the process JVM, creating it on first use within this dynamic library.
/// The first caller's `jvm_options`/classpath win; later callers (e.g. a second
/// JDBC sink) reuse the existing VM instead of failing with `JNI_EEXIST`.
///
/// Limitation: a JDBC source and a JDBC sink are separate dynamic libraries and
/// do not share this static, so configuring both in the *same* connectors
/// runtime process is not supported. Run them in separate runtime processes.
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

/// Render payload bytes for a text column: valid UTF-8 is returned verbatim,
/// otherwise the raw bytes are base64-encoded so binary payloads (e.g. Avro,
/// FlatBuffer, arbitrary Raw) survive instead of being rejected.
fn payload_text_repr(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(e) => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(e.as_bytes())
        }
    }
}

/// Classify a JDBC `SQLState` (its 2-char class) as transient (worth retrying)
/// vs permanent. `08` = connection exception, `40` = transaction rollback
/// (serialization failure / deadlock), `53` = insufficient resources, `57` =
/// operator intervention (e.g. admin shutdown), `58` = system error. Everything
/// else (`22` data exception, `23` integrity-constraint violation, `42`
/// syntax/access) is permanent (retrying will not help). Unknown/absent state
/// is treated as permanent so bad data does not loop forever.
fn is_transient_sql_state(sql_state: Option<&str>) -> bool {
    match sql_state {
        Some(s) if s.len() >= 2 => matches!(&s[..2], "08" | "40" | "53" | "57" | "58"),
        _ => false,
    }
}

/// Inspect and CLEAR the pending Java exception after a failed JNI call, build a
/// classified `(Error, is_transient)` from its `SQLState` and message. Must be
/// called immediately after the failing call (before any other JNI call), and
/// clearing is required so subsequent JNI calls on this thread are not aborted.
fn classify_jni_failure(env: &mut JNIEnv, action: &str) -> (Error, bool) {
    let (sql_state, message) = take_pending_sql_exception(env);
    let transient = is_transient_sql_state(sql_state.as_deref());
    let state = sql_state.as_deref().unwrap_or("?");
    let msg = format!("Failed to {action} (SQLState {state}): {message}");
    if transient {
        (Error::CannotStoreData(msg), true)
    } else {
        (Error::InvalidRecordValue(msg), false)
    }
}

/// Take the pending Java exception, clearing it, and return its `SQLState` (if it
/// is a `java.sql.SQLException`) and its message.
fn take_pending_sql_exception(env: &mut JNIEnv) -> (Option<String>, String) {
    let throwable = match env.exception_occurred() {
        Ok(t) if !t.is_null() => t,
        _ => return (None, "unknown error".to_string()),
    };
    // Clear immediately so subsequent JNI calls on this thread work.
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

/// Call a no-arg `String`-returning method on a throwable, returning None on any
/// JNI error or null result.
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

#[async_trait]
impl Sink for JdbcSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening {CONNECTOR_NAME} connector [{}]. Target table: {}, URL: {}",
            self.id,
            self.config.target_table,
            sanitize_jdbc_url(self.config.jdbc_url.expose_secret())
        );

        if self.config.target_table.is_empty() {
            return Err(Error::InvalidConfigValue(
                "target_table must not be empty".to_string(),
            ));
        }

        self.initialize_jvm()?;
        self.create_connection()?;

        let jvm = self
            .jvm
            .as_ref()
            .ok_or_else(|| Error::InitError("JVM not initialized".to_string()))?;
        let mut env = jvm
            .attach_current_thread()
            .map_err(|e| Error::InitError(format!("Failed to attach thread: {e}")))?;
        self.smoke_test(&mut env)?;
        self.ensure_table_exists(&mut env)?;

        info!(
            "{CONNECTOR_NAME} connector [{}] opened successfully",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let jvm = self
            .jvm
            .as_ref()
            .ok_or_else(|| Error::InitError("JVM not initialized".to_string()))?;
        let mut env = jvm
            .attach_current_thread()
            .map_err(|e| Error::InitError(format!("Failed to attach thread: {e}")))?;

        self.write_messages(&mut env, topic_metadata, &messages_metadata, &messages)
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing {CONNECTOR_NAME} connector [{}]", self.id);

        if let Some(jvm) = &self.jvm
            && let Ok(mut env) = jvm.attach_current_thread()
        {
            if let Some(pool) = &self.connection_pool {
                let _ = env.call_method(pool.as_obj(), "close", "()V", &[]);
                info!("Connection pool closed");
            }
            if let Some(connection) = &self.connection {
                let _ = env.call_method(connection.as_obj(), "close", "()V", &[]);
                info!("Database connection closed");
            }
        }

        let state = self.state.lock().expect("state mutex poisoned");
        info!(
            "{CONNECTOR_NAME} connector [{}] closed. Processed {} messages with {} errors",
            self.id, state.messages_processed, state.insertion_errors
        );
        Ok(())
    }
}

// Export the connector via SDK macro.
sink_connector!(JdbcSink);

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> JdbcSinkConfig {
        JdbcSinkConfig {
            jdbc_url: SecretString::from("jdbc:h2:mem:test"),
            driver_class: "org.h2.Driver".to_string(),
            driver_jar_path: "/tmp/h2.jar".to_string(),
            username: None,
            password: None,
            target_table: "messages".to_string(),
            batch_size: None,
            auto_create_table: None,
            include_metadata: None,
            include_checksum: None,
            include_origin_timestamp: None,
            payload_format: None,
            payload_column: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
            jvm_options: vec![],
            enable_connection_pool: false,
            max_pool_size: 10,
            min_idle: 2,
            connection_timeout_ms: 30000,
        }
    }

    #[test]
    fn given_mysql_url_should_mask_password() {
        let url = "jdbc:mysql://root:SuperSecret123@localhost:3306/mydb";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(sanitized, "jdbc:mysql://root:***@localhost:3306/mydb");
        assert!(!sanitized.contains("SuperSecret123"));
    }

    #[test]
    fn given_query_param_password_should_mask_case_insensitive() {
        for url in [
            "jdbc:postgresql://localhost?password=secret",
            "jdbc:postgresql://localhost?PASSWORD=secret",
            "jdbc:postgresql://localhost?pwd=secret",
        ] {
            let sanitized = sanitize_jdbc_url(url);
            assert!(!sanitized.contains("secret"), "failed for {url}");
            assert!(sanitized.contains("***"));
        }
    }

    #[test]
    fn given_oracle_url_should_mask_password() {
        let url = "jdbc:oracle:thin:system/oracle123@localhost:1521:XE";
        let sanitized = sanitize_jdbc_url(url);
        assert_eq!(sanitized, "jdbc:oracle:thin:system/***@localhost:1521:XE");
        assert!(!sanitized.contains("oracle123"));
    }

    #[test]
    fn given_url_without_password_should_be_unchanged() {
        let url = "jdbc:h2:mem:testdb";
        assert_eq!(sanitize_jdbc_url(url), url);
    }

    #[test]
    fn given_text_payload_repr_should_passthrough_utf8_and_base64_binary() {
        // Valid UTF-8 passes through unchanged.
        assert_eq!(payload_text_repr(b"hello".to_vec()), "hello");
        assert_eq!(payload_text_repr(br#"{"a":1}"#.to_vec()), r#"{"a":1}"#);
        // Invalid UTF-8 (binary) is base64-encoded rather than rejected.
        let binary = vec![0xff, 0xfe, 0x00, 0x01];
        let encoded = payload_text_repr(binary.clone());
        use base64::Engine;
        assert_eq!(
            encoded,
            base64::engine::general_purpose::STANDARD.encode(&binary)
        );
    }

    #[test]
    fn given_sql_state_should_classify_transient_vs_permanent() {
        // Transient classes: connection (08), rollback/serialization (40),
        // resources (53), operator intervention (57), system error (58).
        for s in [
            "08001", "08006", "40001", "40P01", "53300", "57P01", "58030",
        ] {
            assert!(is_transient_sql_state(Some(s)), "{s} should be transient");
        }
        // Permanent: data (22), constraint (23), syntax/access (42), plus
        // unknown/absent.
        for s in ["22001", "23505", "42601", "42P01", "99999"] {
            assert!(!is_transient_sql_state(Some(s)), "{s} should be permanent");
        }
        assert!(!is_transient_sql_state(None));
        assert!(!is_transient_sql_state(Some("")));
        assert!(!is_transient_sql_state(Some("0")));
    }

    #[test]
    fn given_payload_format_strings_should_map_correctly() {
        assert_eq!(
            PayloadFormat::from_config(Some("json")),
            PayloadFormat::Json
        );
        assert_eq!(
            PayloadFormat::from_config(Some("JSONB")),
            PayloadFormat::Json
        );
        assert_eq!(
            PayloadFormat::from_config(Some("bytes")),
            PayloadFormat::Bytes
        );
        assert_eq!(
            PayloadFormat::from_config(Some("binary")),
            PayloadFormat::Bytes
        );
        assert_eq!(
            PayloadFormat::from_config(Some("text")),
            PayloadFormat::Text
        );
        assert_eq!(PayloadFormat::from_config(None), PayloadFormat::Text);
        assert_eq!(
            PayloadFormat::from_config(Some("weird")),
            PayloadFormat::Text
        );
    }

    #[test]
    fn given_all_columns_enabled_should_build_full_insert() {
        let sink = JdbcSink::new(1, test_config());
        let (sql, params) = sink.build_insert_sql().expect("build insert");
        assert!(sql.starts_with("INSERT INTO \"messages\" ("));
        assert!(sql.contains("iggy_offset"));
        assert!(sql.contains("iggy_timestamp"));
        assert!(sql.contains("iggy_stream"));
        assert!(sql.contains("iggy_topic"));
        assert!(sql.contains("iggy_partition_id"));
        assert!(sql.contains("iggy_checksum"));
        assert!(sql.contains("iggy_origin_timestamp"));
        assert!(sql.contains("\"payload\""));
        // id + 5 metadata + checksum + origin_ts + payload = 9
        assert_eq!(params, 9);
        assert_eq!(sql.matches('?').count(), 9);
    }

    #[test]
    fn given_metadata_disabled_should_build_minimal_insert() {
        let mut config = test_config();
        config.include_metadata = Some(false);
        config.include_checksum = Some(false);
        config.include_origin_timestamp = Some(false);
        let sink = JdbcSink::new(1, config);
        let (sql, params) = sink.build_insert_sql().expect("build insert");
        assert!(!sql.contains("iggy_offset"));
        assert!(!sql.contains("iggy_checksum"));
        assert!(!sql.contains("iggy_origin_timestamp"));
        assert!(sql.contains("\"payload\""));
        // id + payload = 2
        assert_eq!(params, 2);
        assert_eq!(sql.matches('?').count(), 2);
    }

    #[test]
    fn given_custom_payload_column_should_use_it() {
        let mut config = test_config();
        config.payload_column = Some("body".to_string());
        let sink = JdbcSink::new(1, config);
        let (sql, _) = sink.build_insert_sql().expect("build insert");
        assert!(sql.contains("\"body\""));
        assert!(!sql.contains("\"payload\""));
    }

    #[test]
    fn given_auto_create_should_build_create_table_with_payload_type() {
        let mut config = test_config();
        config.payload_format = Some("text".to_string());
        let sink = JdbcSink::new(1, config);
        let sql = sink.build_create_table_sql().expect("build create");
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"messages\" ("));
        assert!(sql.contains("id VARCHAR(40)"));
        assert!(sql.contains("\"payload\" TEXT"));
    }

    #[test]
    fn given_bytes_format_create_table_should_use_binary_type() {
        let mut config = test_config();
        config.payload_format = Some("bytes".to_string());
        let sink = JdbcSink::new(1, config);
        let sql = sink.build_create_table_sql().expect("build create");
        assert!(sql.contains("\"payload\" VARBINARY"));
    }

    #[test]
    fn given_table_name_with_quotes_should_escape() {
        assert_eq!(
            quote_identifier("tbl\"name").expect("quote"),
            "\"tbl\"\"name\""
        );
    }

    #[test]
    fn given_injection_attempt_in_table_should_escape() {
        let q = quote_identifier("messages\"; DROP TABLE users; --").expect("quote");
        assert_eq!(q, "\"messages\"\"; DROP TABLE users; --\"");
    }

    #[test]
    fn given_empty_identifier_should_fail() {
        assert!(quote_identifier("").is_err());
    }

    #[test]
    fn given_null_byte_identifier_should_fail() {
        assert!(quote_identifier("a\0b").is_err());
    }

    #[test]
    fn given_default_config_should_use_defaults() {
        let sink = JdbcSink::new(1, test_config());
        assert_eq!(sink.batch_size(), DEFAULT_BATCH_SIZE as usize);
        assert_eq!(sink.max_retries(), DEFAULT_MAX_RETRIES);
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
        assert!(sink.include_metadata());
        assert!(sink.include_checksum());
        assert!(sink.include_origin_timestamp());
        assert_eq!(sink.payload_column(), DEFAULT_PAYLOAD_COLUMN);
        assert_eq!(sink.payload_format(), PayloadFormat::Text);
    }

    #[test]
    fn given_zero_batch_size_should_floor_to_one() {
        let mut config = test_config();
        config.batch_size = Some(0);
        let sink = JdbcSink::new(1, config);
        assert_eq!(sink.batch_size(), 1);
    }

    #[test]
    fn given_custom_retry_delay_should_parse_humantime() {
        let mut config = test_config();
        config.retry_delay = Some("500ms".to_string());
        let sink = JdbcSink::new(1, config);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
    }

    #[test]
    fn given_debug_output_should_not_leak_secrets() {
        let mut config = test_config();
        config.jdbc_url = SecretString::from("jdbc:mysql://root:TopSecret@host:3306/db");
        config.password = Some(SecretString::from("TopSecret"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("TopSecret"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn given_minimal_toml_should_deserialize_with_defaults() {
        let toml_str = r#"
            jdbc_url = "jdbc:h2:mem:test"
            driver_class = "org.h2.Driver"
            driver_jar_path = "/tmp/h2.jar"
            target_table = "events"
        "#;
        let config: JdbcSinkConfig = toml::from_str(toml_str).expect("parse minimal toml");
        assert_eq!(config.target_table, "events");
        assert_eq!(config.driver_class, "org.h2.Driver");
        assert!(config.batch_size.is_none());
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(!config.enable_connection_pool);
        assert_eq!(config.max_pool_size, 10);
        assert_eq!(config.min_idle, 2);
        assert_eq!(config.connection_timeout_ms, 30000);
    }

    #[test]
    fn given_full_toml_should_deserialize_all_fields() {
        let toml_str = r#"
            jdbc_url = "jdbc:mysql://localhost:3306/mydb"
            driver_class = "com.mysql.cj.jdbc.Driver"
            driver_jar_path = "/opt/drivers/mysql.jar"
            username = "admin"
            password = "s3cret"
            target_table = "orders"
            batch_size = 500
            auto_create_table = true
            include_metadata = false
            include_checksum = false
            include_origin_timestamp = false
            payload_format = "json"
            payload_column = "doc"
            verbose_logging = true
            max_retries = 5
            retry_delay = "2s"
            jvm_options = ["-Xmx512m"]
            enable_connection_pool = true
            max_pool_size = 20
            min_idle = 5
            connection_timeout_ms = 60000
        "#;
        let config: JdbcSinkConfig = toml::from_str(toml_str).expect("parse full toml");
        assert_eq!(config.username.as_deref(), Some("admin"));
        assert!(config.password.is_some());
        assert_eq!(config.batch_size, Some(500));
        assert_eq!(config.auto_create_table, Some(true));
        assert_eq!(config.payload_format.as_deref(), Some("json"));
        assert_eq!(config.payload_column.as_deref(), Some("doc"));
        assert_eq!(config.max_retries, Some(5));
        assert_eq!(config.jvm_options, vec!["-Xmx512m"]);
        assert!(config.enable_connection_pool);
        assert_eq!(config.max_pool_size, 20);

        let sink = JdbcSink::new(1, config);
        // metadata/checksum/origin all disabled → id + payload = 2 params
        let (_, params) = sink.build_insert_sql().expect("build insert");
        assert_eq!(params, 2);
        assert_eq!(sink.batch_size(), 500);
    }
}
