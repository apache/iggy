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

use fluss::config::{Config as FlussConfig, NoKeyAssigner};
use iggy_connector_sdk::Error;
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct FlussSinkConfig {
    pub bootstrap_servers: String,
    pub writer_request_max_size: i32,
    pub writer_acks: String,
    pub writer_retries: i32,
    pub writer_batch_size: i32,
    pub writer_bucket_no_key_assigner: NoKeyAssigner,
    pub writer_batch_timeout_ms: i64,
    pub writer_enable_idempotence: bool,
    pub writer_max_inflight_requests_per_bucket: usize,
    pub writer_buffer_memory_size: usize,
    pub writer_buffer_wait_timeout_ms: String,
    pub connect_timeout_ms: u64,
    pub security_protocol: String,
    pub security_sasl_mechanism: String,
    pub security_sasl_username: String,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub security_sasl_password: SecretString,
    pub target_database: String,
    pub target_table: String,
    pub auto_create_table: bool,
    pub include_metadata: bool,
    pub include_checksum: bool,
    pub include_origin_timestamp: bool,
    pub payload_format: PayloadFormat,
}

impl Default for FlussSinkConfig {
    fn default() -> Self {
        let fluss_config = FlussConfig::default();
        Self {
            bootstrap_servers: fluss_config.bootstrap_servers,
            writer_request_max_size: fluss_config.writer_request_max_size,
            writer_acks: fluss_config.writer_acks,
            writer_retries: fluss_config.writer_retries,
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
            target_database: "fluss".to_string(),
            target_table: "iggy_messages".to_string(),
            auto_create_table: true,
            include_metadata: true,
            include_checksum: true,
            include_origin_timestamp: true,
            payload_format: PayloadFormat::default(),
        }
    }
}

impl TryFrom<&FlussSinkConfig> for FlussConfig {
    type Error = Error;

    fn try_from(config: &FlussSinkConfig) -> Result<Self, Self::Error> {
        let writer_buffer_wait_timeout_ms =
            config
                .writer_buffer_wait_timeout_ms
                .parse()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!(
                        "invalid writer_buffer_wait_timeout_ms '{}': {error}",
                        config.writer_buffer_wait_timeout_ms
                    ))
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
    use fluss::config::Config as FlussConfig;
    use iggy_connector_sdk::Error;
    use serde_json::json;

    use super::{FlussSinkConfig, PayloadFormat};

    #[test]
    fn given_default_sink_config_when_converting_should_match_fluss_defaults() {
        let sink_config = FlussSinkConfig::default();
        let fluss_config = FlussConfig::try_from(&sink_config).expect("Sink config should convert");
        let actual = serde_json::to_value(fluss_config).expect("Fluss config should serialize");
        let expected =
            serde_json::to_value(FlussConfig::default()).expect("Fluss config should serialize");

        assert_eq!(actual, expected);
    }

    #[test]
    fn given_existing_sink_config_when_deserializing_should_apply_fluss_defaults() {
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

        assert_eq!(config.writer_batch_size, 2 * 1024 * 1024);
        assert_eq!(config.writer_buffer_wait_timeout_ms, u64::MAX.to_string());
        assert_eq!(config.payload_format, PayloadFormat::Json);
        assert_eq!(config.target_database, "analytics");
        assert_eq!(config.target_table, "events");
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

            assert_eq!(config.payload_format, expected);
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
    fn given_invalid_buffer_wait_timeout_when_converting_should_return_invalid_config_value() {
        let config = FlussSinkConfig {
            writer_buffer_wait_timeout_ms: "invalid".to_string(),
            ..FlussSinkConfig::default()
        };

        let error = FlussConfig::try_from(&config).expect_err("Invalid value should fail");

        assert!(matches!(
            error,
            Error::InvalidConfigValue(message)
                if message.contains("invalid writer_buffer_wait_timeout_ms 'invalid'")
        ));
    }
}
