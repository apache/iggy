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

use std::time::Duration;

use iggy_common::{IGGY_MESSAGE_HEADER_SIZE, MAX_MESSAGE_SIZE_UPPER_BYTES, MAX_PAYLOAD_SIZE};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3SourceConfig {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub path_style: Option<bool>,
    pub prefix: Option<String>,
    pub poll_interval: Option<String>,
    pub custom_delimiter: Option<String>,
    pub max_record_bytes: Option<usize>,
    pub max_batch_bytes: Option<usize>,
    pub max_batch_messages: Option<usize>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResolvedConfig {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub path_style: bool,
    pub prefix: Option<String>,
    pub poll_interval: Duration,
    pub delimiter: Vec<u8>,
    pub max_record_bytes: usize,
    pub max_batch_bytes: usize,
    pub max_batch_messages: usize,
    pub verbose_logging: bool,
}

const DEFAULT_POLL_INTERVAL: &str = "5s";
const MAX_DELIMITER_BYTES: usize = 64;
const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024; // 1MiB
const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_BATCH_MESSAGES: usize = 1_000;
// Reserve space for stream/topic identifiers and the request envelope.
const REQUEST_ENVELOPE_BYTES: usize = 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum ConfigError {
    EmptyBucket,
    EmptyRegion,
    EmptyEndpoint,
    InvalidPollInterval,
    ZeroPollInterval,
    EmptyDelimiter,
    DelimiterTooLong,
    ZeroMaxRecordBytes,
    ZeroMaxBatchBytes,
    ZeroMaxBatchMessages,
    MaxRecordBytesExceedsMaxBatchBytes,
    MaxRecordBytesExceedsIggyPayloadLimit,
    BatchSizeOverflow,
    BatchExceedsIggyRequestLimit,
}

impl TryFrom<&S3SourceConfig> for ResolvedConfig {
    type Error = ConfigError;

    fn try_from(config: &S3SourceConfig) -> Result<Self, Self::Error> {
        let resolved = Self {
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            endpoint: config.endpoint.clone(),
            path_style: config.path_style.unwrap_or(config.endpoint.is_some()),
            prefix: config.prefix.clone().filter(|prefix| !prefix.is_empty()),
            poll_interval: config
                .poll_interval
                .as_deref()
                .unwrap_or(DEFAULT_POLL_INTERVAL)
                .parse::<humantime::Duration>()
                .map_err(|_| ConfigError::InvalidPollInterval)?
                .into(),
            delimiter: config
                .custom_delimiter
                .as_deref()
                .unwrap_or("\n")
                .as_bytes()
                .to_vec(),
            max_record_bytes: config.max_record_bytes.unwrap_or(DEFAULT_MAX_RECORD_BYTES),
            max_batch_bytes: config.max_batch_bytes.unwrap_or(DEFAULT_MAX_BATCH_BYTES),
            max_batch_messages: config
                .max_batch_messages
                .unwrap_or(DEFAULT_MAX_BATCH_MESSAGES),
            verbose_logging: config.verbose_logging.unwrap_or(false),
        };
        resolved.validate()?;
        Ok(resolved)
    }
}

impl ResolvedConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.bucket.is_empty() {
            return Err(ConfigError::EmptyBucket);
        }
        if self
            .region
            .as_ref()
            .is_some_and(|region| region.trim().is_empty())
        {
            return Err(ConfigError::EmptyRegion);
        }
        if self
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.trim().is_empty())
        {
            return Err(ConfigError::EmptyEndpoint);
        }
        if self.poll_interval.is_zero() {
            return Err(ConfigError::ZeroPollInterval);
        }
        if self.delimiter.is_empty() {
            return Err(ConfigError::EmptyDelimiter);
        }
        if self.delimiter.len() > MAX_DELIMITER_BYTES {
            return Err(ConfigError::DelimiterTooLong);
        }
        if self.max_record_bytes == 0 {
            return Err(ConfigError::ZeroMaxRecordBytes);
        }
        if self.max_batch_bytes == 0 {
            return Err(ConfigError::ZeroMaxBatchBytes);
        }
        if self.max_batch_messages == 0 {
            return Err(ConfigError::ZeroMaxBatchMessages);
        }
        if self.max_record_bytes > MAX_PAYLOAD_SIZE as usize {
            return Err(ConfigError::MaxRecordBytesExceedsIggyPayloadLimit);
        }
        if self.max_record_bytes > self.max_batch_bytes {
            return Err(ConfigError::MaxRecordBytesExceedsMaxBatchBytes);
        }
        let encoded_bound = self
            .max_batch_messages
            .checked_mul(IGGY_MESSAGE_HEADER_SIZE)
            .and_then(|headers| headers.checked_add(self.max_batch_bytes))
            .and_then(|bytes| bytes.checked_add(REQUEST_ENVELOPE_BYTES))
            .ok_or(ConfigError::BatchSizeOverflow)?;
        if encoded_bound as u64 > MAX_MESSAGE_SIZE_UPPER_BYTES {
            return Err(ConfigError::BatchExceedsIggyRequestLimit);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> S3SourceConfig {
        serde_json::from_str(r#"{"bucket":"events"}"#).expect("configuration should deserialize")
    }

    #[test]
    fn given_empty_region_or_endpoint_when_resolved_should_reject_before_requests() {
        for value in ["", " \t"] {
            let config = S3SourceConfig {
                region: Some(value.into()),
                ..test_config()
            };
            assert_eq!(
                ResolvedConfig::try_from(&config),
                Err(ConfigError::EmptyRegion)
            );
            let config = S3SourceConfig {
                endpoint: Some(value.into()),
                ..test_config()
            };
            assert_eq!(
                ResolvedConfig::try_from(&config),
                Err(ConfigError::EmptyEndpoint)
            );
        }
    }

    #[test]
    fn given_empty_prefix_when_resolved_should_normalize_without_trimming_keys() {
        for (prefix, expected) in [("", None), (" ", Some(" ")), ("logs/", Some("logs/"))] {
            let config = S3SourceConfig {
                prefix: Some(prefix.into()),
                ..test_config()
            };
            assert_eq!(
                ResolvedConfig::try_from(&config)
                    .expect("config")
                    .prefix
                    .as_deref(),
                expected
            );
        }
    }

    #[test]
    fn given_unknown_config_fields_when_deserialized_should_reject_typos() {
        for field in ["prefx", "max_batch_message", "access_key"] {
            let mut config = serde_json::json!({"bucket": "events"});
            config[field] = serde_json::json!("unused");
            let error =
                serde_json::from_value::<S3SourceConfig>(config).expect_err("unknown field");
            assert!(error.to_string().contains(field));
        }
    }

    #[test]
    fn given_excessive_batch_limits_when_resolved_should_reject_overflow_and_protocol_ceiling() {
        let overflow = S3SourceConfig {
            max_batch_messages: Some(usize::MAX),
            ..test_config()
        };
        assert_eq!(
            ResolvedConfig::try_from(&overflow),
            Err(ConfigError::BatchSizeOverflow)
        );
        let oversized = S3SourceConfig {
            max_batch_bytes: Some(MAX_MESSAGE_SIZE_UPPER_BYTES as usize),
            ..test_config()
        };
        assert_eq!(
            ResolvedConfig::try_from(&oversized),
            Err(ConfigError::BatchExceedsIggyRequestLimit)
        );
    }

    #[test]
    fn given_minimal_config_when_resolved_should_apply_defaults() {
        let resolved = ResolvedConfig::try_from(&test_config())
            .expect("configuration should resolve successfully");

        assert_eq!(
            resolved,
            ResolvedConfig {
                bucket: "events".to_string(),
                region: None,
                endpoint: None,
                path_style: false,
                prefix: None,
                poll_interval: Duration::from_secs(5),
                delimiter: b"\n".to_vec(),
                max_record_bytes: 1024 * 1024,
                max_batch_bytes: 4 * 1024 * 1024,
                max_batch_messages: 1_000,
                verbose_logging: false,
            }
        );
    }

    #[test]
    fn given_custom_endpoint_when_resolved_should_preserve_connection_settings_and_enable_path_style()
     {
        let config: S3SourceConfig = serde_json::from_str(
            r#"{"bucket":"events","region":"us-east-1","endpoint":"http://localhost:4566"}"#,
        )
        .expect("configuration should deserialize");

        let resolved = ResolvedConfig::try_from(&config).expect("configuration should resolve");

        assert_eq!(resolved.region.as_deref(), Some("us-east-1"));
        assert_eq!(resolved.endpoint.as_deref(), Some("http://localhost:4566"));
        assert!(resolved.path_style);
    }

    #[test]
    fn given_explicit_path_style_when_resolved_should_override_endpoint_default() {
        for endpoint in [None, Some("http://localhost:4566".to_string())] {
            for path_style in [false, true] {
                let config = S3SourceConfig {
                    endpoint: endpoint.clone(),
                    path_style: Some(path_style),
                    ..test_config()
                };

                let resolved =
                    ResolvedConfig::try_from(&config).expect("configuration should resolve");

                assert_eq!(resolved.path_style, path_style);
            }
        }
    }

    #[test]
    fn given_empty_bucket_when_resolved_should_return_empty_bucket_error() {
        let config = S3SourceConfig {
            bucket: String::new(),
            ..test_config()
        };

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::EmptyBucket)
        );
    }

    #[test]
    fn given_prefix_in_json_when_resolved_should_preserve_prefix() {
        let json = r#"{"bucket":"events", "prefix":"logs"}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");
        let resolved =
            ResolvedConfig::try_from(&config).expect("configuration should resolve successfully");

        assert_eq!(resolved.prefix, Some("logs".to_string()));
    }

    #[test]
    fn given_valid_poll_interval_when_resolved_should_return_configured_duration() {
        let json = r#"{"bucket":"events", "prefix":"logs", "poll_interval": "150ms"}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");
        let resolved =
            ResolvedConfig::try_from(&config).expect("configuration should resolve successfully");

        assert_eq!(resolved.poll_interval, Duration::from_millis(150));
    }

    #[test]
    fn given_invalid_poll_interval_when_resolved_should_return_invalid_poll_interval_error() {
        let json = r#"{"bucket":"events", "prefix":"logs", "poll_interval": "invalid"}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::InvalidPollInterval)
        );
    }

    #[test]
    fn given_zero_poll_interval_when_resolved_should_return_zero_poll_interval_error() {
        let json = r#"{"bucket":"events", "prefix":"logs", "poll_interval": "0"}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::ZeroPollInterval)
        );
    }

    #[test]
    fn given_custom_delimiter_when_resolved_should_preserve_literal_bytes() {
        let json = r#"{"bucket":"events", "custom_delimiter":"||"}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");
        let resolved =
            ResolvedConfig::try_from(&config).expect("configuration should resolve successfully");

        assert_eq!(resolved.delimiter, b"||".to_vec());
    }

    #[test]
    fn given_empty_custom_delimiter_when_resolved_should_return_empty_delimiter_error() {
        let json = r#"{"bucket":"events", "custom_delimiter":""}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::EmptyDelimiter)
        );
    }

    #[test]
    fn given_custom_delimiter_at_maximum_bytes_when_resolved_should_succeed() {
        let delimiter = "é".repeat(MAX_DELIMITER_BYTES / "é".len());
        let config = S3SourceConfig {
            bucket: "events".to_string(),
            custom_delimiter: Some(delimiter.clone()),
            ..test_config()
        };

        let resolved =
            ResolvedConfig::try_from(&config).expect("maximum-size delimiter should be valid");

        assert_eq!(delimiter.len(), MAX_DELIMITER_BYTES);
        assert_eq!(resolved.delimiter, delimiter.as_bytes().to_vec());
    }

    #[test]
    fn given_oversized_custom_delimiter_when_resolved_should_return_delimiter_too_long_error() {
        let delimiter = "é".repeat(MAX_DELIMITER_BYTES / "é".len() + 1);
        let config = S3SourceConfig {
            bucket: "events".to_string(),
            custom_delimiter: Some(delimiter.clone()),
            ..test_config()
        };

        assert!(delimiter.chars().count() < MAX_DELIMITER_BYTES);
        assert!(delimiter.len() > MAX_DELIMITER_BYTES);
        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::DelimiterTooLong)
        );
    }

    #[test]
    fn given_multibyte_custom_delimiter_when_resolved_should_preserve_utf8_bytes() {
        let json = r#"{"bucket":"events", "custom_delimiter":"💥"}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");
        let resolved =
            ResolvedConfig::try_from(&config).expect("configuration should resolve successfully");

        assert_eq!(resolved.delimiter, "💥".as_bytes().to_vec());
    }

    #[test]
    fn given_custom_newline_when_resolved_should_preserve_newline_bytes() {
        let json = r#"{"bucket":"events", "custom_delimiter":"\n"}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");
        let resolved =
            ResolvedConfig::try_from(&config).expect("configuration should resolve successfully");

        assert_eq!(resolved.delimiter, b"\n".to_vec());
    }

    #[test]
    fn given_zero_max_record_bytes_when_resolved_should_return_zero_max_record_bytes_error() {
        let json = r#"{"bucket":"events", "max_record_bytes":0}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::ZeroMaxRecordBytes)
        );
    }

    #[test]
    fn given_zero_max_batch_bytes_when_resolved_should_return_zero_max_batch_bytes_error() {
        let json = r#"{"bucket":"events", "max_batch_bytes":0}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::ZeroMaxBatchBytes)
        );
    }

    #[test]
    fn given_max_record_bytes_larger_than_max_batch_bytes_when_resolved_should_return_error() {
        let json = r#"{"bucket":"events", "max_record_bytes":2, "max_batch_bytes":1}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::MaxRecordBytesExceedsMaxBatchBytes)
        );
    }

    #[test]
    fn given_custom_record_limit_exceeding_default_batch_limit_when_resolved_should_return_error() {
        let json = r#"{"bucket":"events", "max_record_bytes":8388608}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::MaxRecordBytesExceedsMaxBatchBytes)
        );
    }

    #[test]
    fn given_default_record_limit_exceeding_custom_batch_limit_when_resolved_should_return_error() {
        let json = r#"{"bucket":"events","max_batch_bytes":524288}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::MaxRecordBytesExceedsMaxBatchBytes)
        );
    }

    #[test]
    fn given_max_record_bytes_at_iggy_payload_limit_when_resolved_should_succeed() {
        let limit = MAX_PAYLOAD_SIZE as usize;
        let json = format!(
            r#"{{"bucket":"events","max_record_bytes":{limit},"max_batch_bytes":{limit}}}"#
        );
        let config: S3SourceConfig =
            serde_json::from_str(&json).expect("configuration should deserialize");

        let resolved =
            ResolvedConfig::try_from(&config).expect("configuration should resolve successfully");

        assert_eq!(resolved.max_record_bytes, limit);
    }

    #[test]
    fn given_max_record_bytes_exceeding_iggy_payload_limit_when_resolved_should_return_error() {
        let oversized = MAX_PAYLOAD_SIZE as usize + 1;
        let json = format!(
            r#"{{"bucket":"events","max_record_bytes":{oversized},"max_batch_bytes":{oversized}}}"#
        );
        let config: S3SourceConfig =
            serde_json::from_str(&json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::MaxRecordBytesExceedsIggyPayloadLimit)
        );
    }

    #[test]
    fn given_max_record_bytes_equal_to_max_batch_bytes_when_resolved_should_succeed() {
        let json = r#"{"bucket":"events", "max_record_bytes":1024, "max_batch_bytes":1024}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        let resolved =
            ResolvedConfig::try_from(&config).expect("configuration should resolve successfully");

        assert_eq!(resolved.max_record_bytes, resolved.max_batch_bytes);
    }

    #[test]
    fn given_zero_max_batch_messages_when_resolved_should_return_zero_max_batch_messages_error() {
        let json = r#"{"bucket":"events", "max_batch_messages":0}"#;
        let config: S3SourceConfig =
            serde_json::from_str(json).expect("configuration should deserialize");

        assert_eq!(
            ResolvedConfig::try_from(&config),
            Err(ConfigError::ZeroMaxBatchMessages)
        );
    }
}
