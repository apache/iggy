/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::collections::HashMap;

use crate::clickhouse_client::ClickHouseClient;
use crate::clickhouse_inserter::ClickHouseInserter;
use clickhouse::Row;
use iggy_connector_sdk::sink_connector;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

mod clickhouse_client;
mod clickhouse_inserter;
mod generic_inserter;
mod sink;

sink_connector!(ClickHouseSink);

/// Default configuration values for ClickHouse sink
pub struct ClickHouseSinkDefaults;

impl ClickHouseSinkDefaults {
    pub const COMPRESSION_ENABLED: bool = true;
    pub const TLS_ENABLED: bool = false;
    pub const TOKEN_AUTH: bool = false;
    pub const MAX_BATCH_SIZE: u32 = 1000;
    pub const CHUNK_SIZE: usize = 1024 * 1024; //1MB
    pub const RETRY: bool = true;
    pub const MAX_RETRY: u32 = 3;
    pub const MAX_RETRY_LIMIT: u32 = 10;
    pub const RETRY_BASE_DELAY: u64 = 500; //500 milliseconds
    pub const MAX_RETRY_DELAY_MS: u64 = 900_000; // 15 minutes in milliseconds
    pub const INCLUDE_METADATA: bool = false;
    pub const VERBOSE_LOGGING: bool = false;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClickHouseSinkConfig {
    pub url: String,

    pub compression_enabled: Option<bool>,

    pub tls_enabled: Option<bool>,

    // Server verification
    pub tls_root_ca_cert: Option<String>,

    // Client authentication (mTLS)
    pub tls_client_cert: Option<String>,
    pub tls_client_key: Option<String>,

    // Authentication
    pub auth_type: AuthType,
    pub username: Option<String>,
    pub password: Option<String>,

    pub jwt_token: Option<String>,

    // Role
    pub role: Option<Vec<String>>,

    // Database and table
    pub table: String,
    pub database: Option<String>,

    // Batch configuration
    pub max_batch_size: Option<u32>,

    // Chunk size
    pub chunk_size: Option<usize>,

    // Retry configuration
    pub retry: Option<bool>,
    pub max_retry: Option<u32>,
    pub base_delay: Option<u64>,

    // Insert configuration
    pub insert_type: InsertType,
    pub payload_data_type: Option<String>,

    /// When true, includes all metadata fields (offset, timestamp, checksum, etc.).
    pub include_metadata: Option<bool>,

    pub field_mappings: Option<HashMap<String, String>>,

    pub verbose_logging: Option<bool>,
}

impl ClickHouseSinkConfig {
    pub fn compression_enabled(&self) -> bool {
        self.compression_enabled
            .unwrap_or(ClickHouseSinkDefaults::COMPRESSION_ENABLED)
    }

    pub fn tls_enabled(&self) -> bool {
        self.tls_enabled
            .unwrap_or(ClickHouseSinkDefaults::TLS_ENABLED)
    }

    pub fn max_batch_size(&self) -> u32 {
        self.max_batch_size
            .unwrap_or(ClickHouseSinkDefaults::MAX_BATCH_SIZE)
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
            .unwrap_or(ClickHouseSinkDefaults::CHUNK_SIZE)
    }

    pub fn retry(&self) -> bool {
        self.retry.unwrap_or(ClickHouseSinkDefaults::RETRY)
    }

    pub fn max_retry(&self) -> u32 {
        self.max_retry.unwrap_or(ClickHouseSinkDefaults::MAX_RETRY)
    }

    pub fn retry_base_delay(&self) -> u64 {
        self.base_delay
            .unwrap_or(ClickHouseSinkDefaults::RETRY_BASE_DELAY)
    }

    pub fn include_metadata(&self) -> bool {
        self.include_metadata
            .unwrap_or(ClickHouseSinkDefaults::INCLUDE_METADATA)
    }

    pub fn verbose_logging(&self) -> bool {
        self.verbose_logging
            .unwrap_or(ClickHouseSinkDefaults::VERBOSE_LOGGING)
    }
}

#[derive(Debug)]
struct ClickHouseSink {
    //id
    pub id: u32,
    //client
    client: Option<ClickHouseClient>,
    //config
    config: ClickHouseSinkConfig,
    state: Mutex<State>,
    verbose: bool,
}

#[derive(Debug, Default)]
struct State {
    messages_processed: u64,
    insert_attempt_failed: u64,
    insert_batch_failed: u64,
}

/// Insert type for ClickHouse
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InsertType {
    /// JSONEachRow format - flexible, supports field mappings
    #[default]
    Json,
    /// RowBinary - efficient but requires predefined structs
    RowBinary,
}

impl std::fmt::Display for InsertType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertType::Json => write!(f, "json"),
            InsertType::RowBinary => write!(f, "rowbinary"),
        }
    }
}

impl std::str::FromStr for InsertType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(InsertType::Json),
            "rowbinary" | "row_binary" => Ok(InsertType::RowBinary),
            _ => Err(format!(
                "Invalid insert type: '{}'. Valid options: 'json', 'rowbinary'",
                s
            )),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    #[default]
    None,
    Credential,
    Jwt,
}

impl std::fmt::Display for AuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthType::None => write!(f, "none"),
            AuthType::Credential => write!(f, "credential"),
            AuthType::Jwt => write!(f, "jwt"),
        }
    }
}

impl std::str::FromStr for AuthType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(AuthType::None),
            "credential" | "cred" | "creds" => Ok(AuthType::Credential),
            "jwt" => Ok(AuthType::Jwt),
            _ => Err(format!(
                "Invalid auth type: '{}'. Valid options: 'none', 'credential','jwt'",
                s
            )),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub enum ClickHouseInsertFormat {
    #[default]
    #[serde(rename = "JSONEachRow")]
    JsonEachRow,
}

impl ClickHouseInsertFormat {
    pub const fn as_str(&self) -> &'static str {
        match self {
            ClickHouseInsertFormat::JsonEachRow => "JSONEachRow",
        }
    }
}

impl std::fmt::Display for ClickHouseInsertFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for ClickHouseInsertFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "JSONEachRow" => Ok(ClickHouseInsertFormat::JsonEachRow),
            _ => Err(format!(
                "Invalid ClickHouse insert format: '{}'. Currently only 'JSONEachRow' is supported.",
                s
            )),
        }
    }
}

/// Message row without metadata
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MessageRowWithoutMetadata {
    pub payload: String,
}

/// Message row with full metadata
/// All metadata fields use the `iggy_` prefix to distinguish them from payload data
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MessageRowWithMetadata {
    pub iggy_stream: String,
    pub iggy_topic: String,
    pub iggy_partition_id: u32,
    pub iggy_id: String,
    pub iggy_offset: u64,
    pub iggy_checksum: u64,
    pub iggy_timestamp: u64,
    pub iggy_origin_timestamp: u64,
    pub payload: String,
}

// #[derive(Error, Debug)]
// pub enum FormatError {
//     #[error("Invalid header (expected {expected:?}, got {found:?})")]
//     InvalidHeader {
//         expected: String,
//         found: String,
//     },
//     #[error("Missing attribute: {0}")]
//     MissingAttribute(String),
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_config_all_none() -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            compression_enabled: None,
            tls_enabled: None,
            tls_root_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            auth_type: AuthType::None,
            username: None,
            password: None,
            jwt_token: None,
            role: None,
            table: "test_table".to_string(),
            database: None,
            max_batch_size: None,
            chunk_size: None,
            retry: None,
            max_retry: None,
            base_delay: None,
            insert_type: InsertType::Json,
            payload_data_type: None,
            include_metadata: None,
            field_mappings: None,
            verbose_logging: None,
        }
    }

    // ── Config default accessor tests ────────────────────────────────────────

    #[test]
    fn given_default_config_should_use_all_defaults() {
        let config = test_config_all_none();

        assert_eq!(
            config.compression_enabled(),
            ClickHouseSinkDefaults::COMPRESSION_ENABLED
        );
        assert_eq!(config.tls_enabled(), ClickHouseSinkDefaults::TLS_ENABLED);
        assert_eq!(
            config.max_batch_size(),
            ClickHouseSinkDefaults::MAX_BATCH_SIZE
        );
        assert_eq!(config.chunk_size(), ClickHouseSinkDefaults::CHUNK_SIZE);
        assert_eq!(config.retry(), ClickHouseSinkDefaults::RETRY);
        assert_eq!(config.max_retry(), ClickHouseSinkDefaults::MAX_RETRY);
        assert_eq!(
            config.retry_base_delay(),
            ClickHouseSinkDefaults::RETRY_BASE_DELAY
        );
        assert_eq!(
            config.include_metadata(),
            ClickHouseSinkDefaults::INCLUDE_METADATA
        );
        assert_eq!(
            config.verbose_logging(),
            ClickHouseSinkDefaults::VERBOSE_LOGGING
        );
    }

    #[test]
    fn given_custom_config_should_override_all_defaults() {
        let mut config = test_config_all_none();
        config.compression_enabled = Some(false);
        config.tls_enabled = Some(true);
        config.max_batch_size = Some(500);
        config.chunk_size = Some(2048);
        config.retry = Some(false);
        config.max_retry = Some(10);
        config.base_delay = Some(1000);
        config.include_metadata = Some(true);
        config.verbose_logging = Some(true);

        assert!(!config.compression_enabled());
        assert!(config.tls_enabled());
        assert_eq!(config.max_batch_size(), 500);
        assert_eq!(config.chunk_size(), 2048);
        assert!(!config.retry());
        assert_eq!(config.max_retry(), 10);
        assert_eq!(config.retry_base_delay(), 1000);
        assert!(config.include_metadata());
        assert!(config.verbose_logging());
    }

    // ── InsertType parsing tests ─────────────────────────────────────────────

    #[test]
    fn given_valid_insert_type_string_should_parse() {
        assert!(matches!(InsertType::from_str("json"), Ok(InsertType::Json)));
        assert!(matches!(
            InsertType::from_str("rowbinary"),
            Ok(InsertType::RowBinary)
        ));
        assert!(matches!(
            InsertType::from_str("row_binary"),
            Ok(InsertType::RowBinary)
        ));
    }

    #[test]
    fn given_invalid_insert_type_string_should_fail() {
        assert!(InsertType::from_str("xml").is_err());
        assert!(InsertType::from_str("").is_err());
    }

    // ── AuthType parsing tests ───────────────────────────────────────────────

    #[test]
    fn given_valid_auth_type_string_should_parse() {
        assert!(matches!(AuthType::from_str("none"), Ok(AuthType::None)));
        assert!(matches!(
            AuthType::from_str("credential"),
            Ok(AuthType::Credential)
        ));
        assert!(matches!(
            AuthType::from_str("cred"),
            Ok(AuthType::Credential)
        ));
        assert!(matches!(
            AuthType::from_str("creds"),
            Ok(AuthType::Credential)
        ));
        assert!(matches!(AuthType::from_str("jwt"), Ok(AuthType::Jwt)));
    }

    #[test]
    fn given_invalid_auth_type_string_should_fail() {
        assert!(AuthType::from_str("oauth").is_err());
        assert!(AuthType::from_str("").is_err());
    }

    // ── ClickHouseInsertFormat parsing tests ─────────────────────────────────

    #[test]
    fn given_valid_insert_format_string_should_parse() {
        assert!(matches!(
            ClickHouseInsertFormat::from_str("JSONEachRow"),
            Ok(ClickHouseInsertFormat::JsonEachRow)
        ));
    }

    #[test]
    fn given_invalid_insert_format_string_should_fail() {
        assert!(ClickHouseInsertFormat::from_str("CSV").is_err());
        assert!(ClickHouseInsertFormat::from_str("jsoneachrow").is_err());
    }

    #[test]
    fn given_json_each_row_format_as_str_should_return_correct_value() {
        assert_eq!(ClickHouseInsertFormat::JsonEachRow.as_str(), "JSONEachRow");
    }
}
