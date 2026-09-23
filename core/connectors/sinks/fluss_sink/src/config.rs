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

use fluss::{
    config::{Config as FlussConfig, NoKeyAssigner},
    error::Error,
    metadata::{SchemaBuilder, TableDescriptorBuilder},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadFormat {
    Bytea,
    #[default]
    Json,
    Text,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouterType {
    #[default]
    Static,
    Multi,
}

const DEFAULT_FLUSS_WRITER_RETRIES: i32 = 3;
const DEFAULT_TARGET_DATABASE: &str = "fluss";
const DEFAULT_TARGET_TABLE: &str = "iggy_messages";
const DEFAULT_ROUTE_KEY: &str = "table";
const DEFAULT_AUTO_CREATE_TABLE: bool = true;
const DEFAULT_INCLUDE_METADATA: bool = true;
const DEFAULT_INCLUDE_CHECKSUM: bool = true;
const DEFAULT_INCLUDE_TIMESTAMP: bool = true;

#[derive(Default, Deserialize, Clone)]
pub struct FlussSinkConfig {
    pub bootstrap_servers: Option<String>,
    pub writer_request_max_size: Option<i32>,
    pub writer_acks: Option<String>,
    pub writer_retries: Option<i32>,
    pub writer_batch_size: Option<i32>,
    pub writer_bucket_no_key_assigner: Option<NoKeyAssigner>,
    pub writer_batch_timeout_ms: Option<i64>,
    pub writer_enable_idempotence: Option<bool>,
    pub writer_max_inflight_requests_per_bucket: Option<usize>,
    pub writer_buffer_memory_size: Option<usize>,
    pub writer_buffer_wait_timeout_ms: Option<String>,
    pub connect_timeout_ms: Option<u64>,
    pub security_protocol: Option<String>,
    pub security_sasl_mechanism: Option<String>,
    pub security_sasl_username: Option<String>,
    pub security_sasl_password: Option<SecretString>,
    pub target_database: Option<String>,
    pub target_table: Option<String>,
    pub auto_create_table: Option<bool>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub payload_format: Option<PayloadFormat>,
    pub route_key: Option<String>,
    pub router_type: Option<RouterType>,
    pub tables: Option<HashMap<String, TableConfig>>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Default, Deserialize, Clone, PartialEq, Eq)]
pub struct TableConfig {
    pub primary_keys: Option<Vec<String>>,
    pub partitioned_by: Option<Vec<String>>,
    pub bucket_keys: Option<Vec<String>>,
    pub bucket_count: Option<i32>,
    pub properties: Option<HashMap<String, String>>,
}

impl TableConfig {
    pub(crate) fn enrich_schema_builder(&self, mut builder: SchemaBuilder) -> SchemaBuilder {
        if let Some(primary_keys) = &self.primary_keys
            && !primary_keys.is_empty()
        {
            builder = builder
                .primary_key(primary_keys.clone())
                .expect("Enrich only once");
        }

        builder
    }

    pub(crate) fn enrich_descriptor_builder(
        &self,
        mut builder: TableDescriptorBuilder,
    ) -> TableDescriptorBuilder {
        if let Some(bucket_keys) = &self.bucket_keys {
            builder = builder.distributed_by(self.bucket_count, bucket_keys.to_owned());
        }
        if let Some(partitioned_by) = &self.partitioned_by {
            builder = builder.partitioned_by(partitioned_by.to_owned());
        }
        if let Some(properties) = &self.properties {
            builder = builder.properties(properties.to_owned());
        }
        builder
    }
}

#[derive(Clone)]
pub(crate) struct ResolvedFlussSinkConfig {
    pub(crate) bootstrap_servers: String,
    pub(crate) writer_request_max_size: i32,
    pub(crate) writer_acks: String,
    pub(crate) writer_retries: i32,
    pub(crate) writer_batch_size: i32,
    pub(crate) writer_bucket_no_key_assigner: NoKeyAssigner,
    pub(crate) writer_batch_timeout_ms: i64,
    pub(crate) writer_enable_idempotence: bool,
    pub(crate) writer_max_inflight_requests_per_bucket: usize,
    pub(crate) writer_buffer_memory_size: usize,
    pub(crate) writer_buffer_wait_timeout_ms: String,
    pub(crate) connect_timeout_ms: u64,
    pub(crate) security_protocol: String,
    pub(crate) security_sasl_mechanism: String,
    pub(crate) security_sasl_username: String,
    pub(crate) security_sasl_password: SecretString,
    pub(crate) target_database: String,
    pub(crate) target_table: String,
    pub(crate) auto_create_table: bool,
    pub(crate) include_metadata: bool,
    pub(crate) include_checksum: bool,
    pub(crate) include_origin_timestamp: bool,
    pub(crate) payload_format: PayloadFormat,
    pub(crate) route_key: String,
    pub(crate) router_type: RouterType,
    pub(crate) tables: HashMap<String, TableConfig>,
    pub(crate) verbose_logging: bool,
}

impl Default for ResolvedFlussSinkConfig {
    fn default() -> Self {
        let fluss_config = FlussConfig::default();
        Self {
            bootstrap_servers: fluss_config.bootstrap_servers,
            writer_request_max_size: fluss_config.writer_request_max_size,
            writer_acks: fluss_config.writer_acks,
            writer_retries: DEFAULT_FLUSS_WRITER_RETRIES,
            writer_batch_size: fluss_config.writer_batch_size,
            writer_bucket_no_key_assigner: fluss_config.writer_bucket_no_key_assigner,
            writer_batch_timeout_ms: fluss_config.writer_batch_timeout_ms,
            writer_enable_idempotence: fluss_config.writer_enable_idempotence,
            writer_max_inflight_requests_per_bucket: fluss_config
                .writer_max_inflight_requests_per_bucket,
            writer_buffer_memory_size: fluss_config.writer_buffer_memory_size,
            writer_buffer_wait_timeout_ms: fluss_config.writer_buffer_wait_timeout_ms.to_string(),
            connect_timeout_ms: fluss_config.connect_timeout_ms,
            security_protocol: fluss_config.security_protocol,
            security_sasl_mechanism: fluss_config.security_sasl_mechanism,
            security_sasl_username: fluss_config.security_sasl_username,
            security_sasl_password: fluss_config.security_sasl_password.into(),
            target_database: DEFAULT_TARGET_DATABASE.to_owned(),
            target_table: DEFAULT_TARGET_TABLE.to_owned(),
            auto_create_table: DEFAULT_AUTO_CREATE_TABLE,
            include_metadata: DEFAULT_INCLUDE_METADATA,
            include_checksum: DEFAULT_INCLUDE_CHECKSUM,
            include_origin_timestamp: DEFAULT_INCLUDE_TIMESTAMP,
            payload_format: PayloadFormat::default(),
            route_key: DEFAULT_ROUTE_KEY.to_owned(),
            router_type: RouterType::default(),
            tables: HashMap::new(),
            verbose_logging: false,
        }
    }
}

impl From<FlussSinkConfig> for ResolvedFlussSinkConfig {
    fn from(config: FlussSinkConfig) -> Self {
        let defaults = Self::default();
        Self {
            bootstrap_servers: config
                .bootstrap_servers
                .unwrap_or(defaults.bootstrap_servers),
            writer_request_max_size: config
                .writer_request_max_size
                .unwrap_or(defaults.writer_request_max_size),
            writer_acks: config.writer_acks.unwrap_or(defaults.writer_acks),
            writer_retries: config.writer_retries.unwrap_or(defaults.writer_retries),
            writer_batch_size: config
                .writer_batch_size
                .unwrap_or(defaults.writer_batch_size),
            writer_bucket_no_key_assigner: config
                .writer_bucket_no_key_assigner
                .unwrap_or(defaults.writer_bucket_no_key_assigner),
            writer_batch_timeout_ms: config
                .writer_batch_timeout_ms
                .unwrap_or(defaults.writer_batch_timeout_ms),
            writer_enable_idempotence: config
                .writer_enable_idempotence
                .unwrap_or(defaults.writer_enable_idempotence),
            writer_max_inflight_requests_per_bucket: config
                .writer_max_inflight_requests_per_bucket
                .unwrap_or(defaults.writer_max_inflight_requests_per_bucket),
            writer_buffer_memory_size: config
                .writer_buffer_memory_size
                .unwrap_or(defaults.writer_buffer_memory_size),
            writer_buffer_wait_timeout_ms: config
                .writer_buffer_wait_timeout_ms
                .unwrap_or(defaults.writer_buffer_wait_timeout_ms),
            connect_timeout_ms: config
                .connect_timeout_ms
                .unwrap_or(defaults.connect_timeout_ms),
            security_protocol: config
                .security_protocol
                .unwrap_or(defaults.security_protocol),
            security_sasl_mechanism: config
                .security_sasl_mechanism
                .unwrap_or(defaults.security_sasl_mechanism),
            security_sasl_username: config
                .security_sasl_username
                .unwrap_or(defaults.security_sasl_username),
            security_sasl_password: config
                .security_sasl_password
                .unwrap_or(defaults.security_sasl_password),
            target_database: config.target_database.unwrap_or(defaults.target_database),
            target_table: config.target_table.unwrap_or(defaults.target_table),
            auto_create_table: config
                .auto_create_table
                .unwrap_or(defaults.auto_create_table),
            include_metadata: config.include_metadata.unwrap_or(defaults.include_metadata),
            include_checksum: config.include_checksum.unwrap_or(defaults.include_checksum),
            include_origin_timestamp: config
                .include_origin_timestamp
                .unwrap_or(defaults.include_origin_timestamp),
            payload_format: config.payload_format.unwrap_or(defaults.payload_format),
            route_key: config.route_key.unwrap_or(defaults.route_key),
            router_type: config.router_type.unwrap_or(defaults.router_type),
            tables: config.tables.unwrap_or(defaults.tables),
            verbose_logging: config.verbose_logging.unwrap_or(false),
        }
    }
}

impl TryFrom<&ResolvedFlussSinkConfig> for FlussConfig {
    type Error = Error;

    fn try_from(config: &ResolvedFlussSinkConfig) -> Result<Self, Self::Error> {
        let writer_buffer_wait_timeout_ms =
            config
                .writer_buffer_wait_timeout_ms
                .parse()
                .map_err(|error| Error::UnexpectedError {
                    message: format!(
                        "invalid writer_buffer_wait_timeout_ms '{}': {error}",
                        config.writer_buffer_wait_timeout_ms
                    ),
                    source: Some(Box::new(error)),
                })?;

        Ok(Self {
            bootstrap_servers: config.bootstrap_servers.clone(),
            writer_request_max_size: config.writer_request_max_size,
            writer_acks: config.writer_acks.clone(),
            writer_retries: config.writer_retries,
            writer_batch_size: config.writer_batch_size,
            writer_bucket_no_key_assigner: config.writer_bucket_no_key_assigner,
            writer_batch_timeout_ms: config.writer_batch_timeout_ms,
            writer_enable_idempotence: config.writer_enable_idempotence,
            writer_max_inflight_requests_per_bucket: config.writer_max_inflight_requests_per_bucket,
            writer_buffer_memory_size: config.writer_buffer_memory_size,
            writer_buffer_wait_timeout_ms,
            connect_timeout_ms: config.connect_timeout_ms,
            security_protocol: config.security_protocol.clone(),
            security_sasl_mechanism: config.security_sasl_mechanism.clone(),
            security_sasl_username: config.security_sasl_username.clone(),
            security_sasl_password: config.security_sasl_password.expose_secret().to_string(),
            ..FlussConfig::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use fluss::{config::Config as FlussConfig, error::Error};
    use serde_json::json;

    use super::{
        DEFAULT_FLUSS_WRITER_RETRIES, FlussSinkConfig, PayloadFormat, ResolvedFlussSinkConfig,
        TableConfig,
    };

    #[test]
    fn given_default_sink_config_when_converting_should_use_connector_retry_default() {
        let sink_config = ResolvedFlussSinkConfig::from(FlussSinkConfig::default());
        let fluss_config = FlussConfig::try_from(&sink_config).expect("Sink config should convert");
        let actual = serde_json::to_value(fluss_config).expect("Fluss config should serialize");
        let expected = serde_json::to_value(FlussConfig {
            writer_retries: DEFAULT_FLUSS_WRITER_RETRIES,
            ..FlussConfig::default()
        })
        .expect("Fluss config should serialize");

        assert_eq!(actual, expected);
    }

    #[test]
    fn given_explicit_writer_retries_when_converting_should_preserve_value() {
        let config = FlussSinkConfig {
            writer_retries: Some(i32::MAX),
            ..FlussSinkConfig::default()
        };
        let config = ResolvedFlussSinkConfig::from(config);

        let fluss_config = FlussConfig::try_from(&config).expect("Sink config should convert");

        assert_eq!(fluss_config.writer_retries, i32::MAX);
    }

    #[test]
    fn given_existing_sink_config_when_resolving_should_apply_fluss_defaults() {
        let config: FlussSinkConfig = serde_json::from_value(json!({
            "bootstrap_servers": "localhost:9123",
            "target_database": "analytics",
            "target_table": "events",
            "auto_create_table": true,
            "include_metadata": true,
            "include_checksum": true,
            "include_origin_timestamp": true,
            "payload_format": "json"
        }))
        .expect("Existing Fluss sink config should deserialize");
        let config = ResolvedFlussSinkConfig::from(config);

        assert_eq!(config.writer_batch_size, 2 * 1024 * 1024);
        assert_eq!(config.writer_retries, DEFAULT_FLUSS_WRITER_RETRIES);
        assert_eq!(config.writer_buffer_wait_timeout_ms, u64::MAX.to_string());
        assert_eq!(config.payload_format, PayloadFormat::Json);
        assert_eq!(config.target_database, "analytics");
        assert_eq!(config.target_table, "events");
        assert!(config.tables.is_empty());
    }

    #[test]
    fn given_table_settings_when_resolving_should_preserve_settings_by_qualified_table_name() {
        let config: FlussSinkConfig = serde_json::from_value(json!({
            "tables": {
                "analytics.orders": {
                    "primary_keys": ["order_id", "region"],
                    "partitioned_by": ["region"],
                    "bucket_keys": ["customer_id"],
                    "bucket_count": 12,
                    "properties": {
                        "table.datalake.enabled": "true"
                    }
                }
            }
        }))
        .expect("Table settings should deserialize");

        let config = ResolvedFlussSinkConfig::from(config);

        assert_eq!(
            config.tables.get("analytics.orders"),
            Some(&TableConfig {
                primary_keys: Some(vec!["order_id".to_string(), "region".to_string()]),
                partitioned_by: Some(vec!["region".to_string()]),
                bucket_keys: Some(vec!["customer_id".to_string()]),
                bucket_count: Some(12),
                properties: Some(HashMap::from([(
                    "table.datalake.enabled".to_string(),
                    "true".to_string(),
                )])),
            })
        );
    }

    #[test]
    fn given_supported_payload_formats_when_deserializing_should_return_matching_variants() {
        for (value, expected) in [
            ("bytea", PayloadFormat::Bytea),
            ("json", PayloadFormat::Json),
            ("text", PayloadFormat::Text),
        ] {
            let config: FlussSinkConfig = serde_json::from_value(json!({
                "payload_format": value
            }))
            .expect("Supported payload format should deserialize");

            assert_eq!(config.payload_format, Some(expected));
        }
    }

    #[test]
    fn given_unsupported_payload_format_when_deserializing_should_fail() {
        let config = serde_json::from_value::<FlussSinkConfig>(json!({
            "payload_format": "xml"
        }));

        assert!(config.is_err());
    }

    #[test]
    fn given_u64_max_as_string_when_converting_should_parse_value() {
        let config: FlussSinkConfig = serde_json::from_value(json!({
            "writer_buffer_wait_timeout_ms": u64::MAX.to_string()
        }))
        .expect("String-encoded u64 should deserialize");
        let config = ResolvedFlussSinkConfig::from(config);
        let fluss_config = FlussConfig::try_from(&config).expect("Sink config should convert");

        assert_eq!(fluss_config.writer_buffer_wait_timeout_ms, u64::MAX);
    }

    #[test]
    fn given_numeric_buffer_wait_timeout_when_deserializing_should_fail() {
        let config = serde_json::from_value::<FlussSinkConfig>(json!({
            "writer_buffer_wait_timeout_ms": 100
        }));

        assert!(config.is_err());
    }

    #[test]
    fn given_invalid_buffer_wait_timeout_when_converting_should_return_fluss_error() {
        let config = FlussSinkConfig {
            writer_buffer_wait_timeout_ms: Some("invalid".to_string()),
            ..FlussSinkConfig::default()
        };
        let config = ResolvedFlussSinkConfig::from(config);

        let error = FlussConfig::try_from(&config).expect_err("Invalid value should fail");

        assert!(matches!(
            error,
            Error::UnexpectedError { message, .. }
                if message.contains("invalid writer_buffer_wait_timeout_ms 'invalid'")
        ));
    }

    #[test]
    fn given_explicit_boolean_values_when_resolving_should_preserve_values() {
        let default_config = ResolvedFlussSinkConfig::from(FlussSinkConfig::default());
        let explicit_config = ResolvedFlussSinkConfig::from(FlussSinkConfig {
            auto_create_table: Some(false),
            include_metadata: Some(false),
            verbose_logging: Some(true),
            ..FlussSinkConfig::default()
        });

        assert!(!default_config.verbose_logging);
        assert!(!explicit_config.auto_create_table);
        assert!(!explicit_config.include_metadata);
        assert!(explicit_config.verbose_logging);
    }
}
