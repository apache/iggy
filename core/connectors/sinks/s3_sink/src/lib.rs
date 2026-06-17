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

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use iggy_connector_sdk::{Error, sink_connector};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

mod buffer;
mod client;
mod formatter;
mod path;
mod sink;

sink_connector!(S3Sink);

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_FILE_SIZE: &str = "8MiB";
const DEFAULT_PATH_TEMPLATE: &str = "{stream}/{topic}/{date}/{hour}";
const DEFAULT_OUTPUT_FORMAT: &str = "json_lines";
const MAX_S3_SINGLE_PUT_SIZE: u64 = 5 * 1024 * 1024 * 1024; // 5 GiB

#[derive(Clone, Serialize, Deserialize)]
pub struct S3SinkConfig {
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(
        default,
        serialize_with = "iggy_common::serde_secret::serialize_optional_secret"
    )]
    pub access_key_id: Option<SecretString>,
    #[serde(
        default,
        serialize_with = "iggy_common::serde_secret::serialize_optional_secret"
    )]
    pub secret_access_key: Option<SecretString>,
    #[serde(default = "default_path_template")]
    pub path_template: String,
    #[serde(default = "default_file_rotation")]
    pub file_rotation: FileRotation,
    #[serde(default = "default_max_file_size")]
    pub max_file_size: String,
    #[serde(default)]
    pub max_messages_per_file: Option<u64>,
    #[serde(default = "default_output_format")]
    pub output_format: String,
    #[serde(default = "default_true")]
    pub include_metadata: bool,
    #[serde(default)]
    pub include_headers: bool,
    /// Total number of attempts (including the initial one) before giving up.
    /// `max_attempts = 3` means 1 initial try + 2 retries. The `max_retries`
    /// alias is accepted for convenience but follows the same "total attempts"
    /// semantics.
    #[serde(default, alias = "max_retries")]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub retry_delay: Option<String>,
    #[serde(default)]
    pub path_style: Option<bool>,
}

impl fmt::Debug for S3SinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3SinkConfig")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"[REDACTED]")
            .field("secret_access_key", &"[REDACTED]")
            .field("path_template", &self.path_template)
            .field("file_rotation", &self.file_rotation)
            .field("max_file_size", &self.max_file_size)
            .field("max_messages_per_file", &self.max_messages_per_file)
            .field("output_format", &self.output_format)
            .field("include_metadata", &self.include_metadata)
            .field("include_headers", &self.include_headers)
            .field("max_attempts", &self.max_attempts)
            .field("retry_delay", &self.retry_delay)
            .field("path_style", &self.path_style)
            .finish()
    }
}

fn default_path_template() -> String {
    DEFAULT_PATH_TEMPLATE.to_string()
}

fn default_file_rotation() -> FileRotation {
    FileRotation::Size
}

fn default_max_file_size() -> String {
    DEFAULT_MAX_FILE_SIZE.to_string()
}

fn default_output_format() -> String {
    DEFAULT_OUTPUT_FORMAT.to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileRotation {
    Size,
    Messages,
}

impl fmt::Display for FileRotation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileRotation::Size => write!(f, "size"),
            FileRotation::Messages => write!(f, "messages"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputFormat {
    JsonLines,
    JsonArray,
    Raw,
}

impl TryFrom<&str> for OutputFormat {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "json_lines" | "jsonl" | "jsonlines" => Ok(OutputFormat::JsonLines),
            "json_array" => Ok(OutputFormat::JsonArray),
            "raw" => Ok(OutputFormat::Raw),
            other => Err(Error::InvalidConfigValue(format!(
                "Unknown output format: '{other}'. Expected: json_lines, json_array, or raw"
            ))),
        }
    }
}

impl OutputFormat {
    pub fn file_extension(&self) -> &'static str {
        match self {
            OutputFormat::JsonLines => "jsonl",
            OutputFormat::JsonArray => "json",
            OutputFormat::Raw => "bin",
        }
    }
}

/// Parsed and validated config fields, constructed only inside `open()`.
/// Avoids accessing unresolved zero/default placeholder values before
/// the sink is fully initialized.
#[derive(Debug)]
pub(crate) struct ResolvedConfig {
    pub max_file_size_bytes: u64,
    pub max_messages: u64,
    pub output_format: OutputFormat,
    pub retry_delay: std::time::Duration,
    pub max_attempts: u32,
}

pub struct S3Sink {
    id: u32,
    config: S3SinkConfig,
    bucket: Option<Box<s3::Bucket>>,
    buffers: DashMap<BufferKey, Arc<tokio::sync::Mutex<buffer::FileBuffer>>>,
    resolved: Option<ResolvedConfig>,
    state: tokio::sync::Mutex<SinkState>,
}

impl fmt::Debug for S3Sink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Sink")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("bucket", &self.bucket.as_ref().map(|b| &b.name))
            .field("buffers_count", &self.buffers.len())
            .field("resolved", &self.resolved)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BufferKey {
    pub stream: String,
    pub topic: String,
    pub partition_id: u32,
}

#[derive(Debug)]
struct SinkState {
    messages_received: u64,
    messages_uploaded: u64,
    messages_lost: u64,
}

impl S3Sink {
    pub fn new(id: u32, config: S3SinkConfig) -> Self {
        S3Sink {
            id,
            config,
            bucket: None,
            buffers: DashMap::new(),
            resolved: None,
            state: tokio::sync::Mutex::new(SinkState {
                messages_received: 0,
                messages_uploaded: 0,
                messages_lost: 0,
            }),
        }
    }

    pub fn validate_and_parse_config(&mut self) -> Result<(), Error> {
        if self.config.bucket.is_empty() {
            return Err(Error::InvalidConfigValue(
                "bucket must not be empty".to_owned(),
            ));
        }
        if self.config.region.is_empty() {
            return Err(Error::InvalidConfigValue(
                "region must not be empty".to_owned(),
            ));
        }
        if self.config.path_template.is_empty() {
            return Err(Error::InvalidConfigValue(
                "path_template must not be empty".to_owned(),
            ));
        }

        let output_format = OutputFormat::try_from(self.config.output_format.as_str())?;
        let max_file_size_bytes = parse_file_size(&self.config.max_file_size)?;

        if max_file_size_bytes == 0 {
            return Err(Error::InvalidConfigValue(
                "max_file_size must be greater than 0".to_owned(),
            ));
        }
        if max_file_size_bytes > MAX_S3_SINGLE_PUT_SIZE {
            return Err(Error::InvalidConfigValue(format!(
                "max_file_size ({}) exceeds S3 single PutObject limit of 5 GiB",
                self.config.max_file_size
            )));
        }

        let delay_str = self
            .config
            .retry_delay
            .as_deref()
            .unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = humantime::Duration::from_str(delay_str)
            .map(|d| d.into())
            .map_err(|e| {
                Error::InvalidConfigValue(format!("Invalid retry_delay '{delay_str}': {e}"))
            })?;

        let mut max_messages = u64::MAX;
        if self.config.file_rotation == FileRotation::Messages {
            match self.config.max_messages_per_file {
                None => {
                    return Err(Error::InvalidConfigValue(
                        "file_rotation is 'messages' but max_messages_per_file is not configured"
                            .to_owned(),
                    ));
                }
                Some(0) => {
                    return Err(Error::InvalidConfigValue(
                        "max_messages_per_file must be greater than 0".to_owned(),
                    ));
                }
                Some(n) => {
                    max_messages = n;
                }
            }
        } else if let Some(n) = self.config.max_messages_per_file {
            if n == 0 {
                return Err(Error::InvalidConfigValue(
                    "max_messages_per_file must be greater than 0".to_owned(),
                ));
            }
            max_messages = n;
        }

        let max_attempts = self.config.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);

        self.resolved = Some(ResolvedConfig {
            max_file_size_bytes,
            max_messages,
            output_format,
            retry_delay,
            max_attempts,
        });

        Ok(())
    }

    pub(crate) fn resolved(&self) -> &ResolvedConfig {
        self.resolved
            .as_ref()
            .expect("BUG: resolved config accessed before open()")
    }
}

fn parse_file_size(s: &str) -> Result<u64, Error> {
    byte_unit::Byte::from_str(s)
        .map(|b| b.as_u64())
        .map_err(|e| Error::InvalidConfigValue(format!("Invalid file size '{s}': {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_file_size_mib() {
        assert_eq!(parse_file_size("8MiB").unwrap(), 8 * 1024 * 1024);
    }

    #[test]
    fn parse_file_size_mb() {
        assert_eq!(parse_file_size("10MB").unwrap(), 10_000_000);
    }

    #[test]
    fn parse_file_size_invalid() {
        assert!(parse_file_size("not_a_size").is_err());
    }

    #[test]
    fn output_format_json_lines_variants() {
        assert_eq!(
            OutputFormat::try_from("json_lines").unwrap(),
            OutputFormat::JsonLines
        );
        assert_eq!(
            OutputFormat::try_from("jsonl").unwrap(),
            OutputFormat::JsonLines
        );
        assert_eq!(
            OutputFormat::try_from("JSONLINES").unwrap(),
            OutputFormat::JsonLines
        );
    }

    #[test]
    fn output_format_json_array() {
        assert_eq!(
            OutputFormat::try_from("json_array").unwrap(),
            OutputFormat::JsonArray
        );
    }

    #[test]
    fn output_format_json_alias_removed() {
        assert!(OutputFormat::try_from("json").is_err());
    }

    #[test]
    fn output_format_raw() {
        assert_eq!(OutputFormat::try_from("raw").unwrap(), OutputFormat::Raw);
    }

    #[test]
    fn output_format_invalid() {
        assert!(OutputFormat::try_from("xml").is_err());
    }

    #[test]
    fn file_extensions() {
        assert_eq!(OutputFormat::JsonLines.file_extension(), "jsonl");
        assert_eq!(OutputFormat::JsonArray.file_extension(), "json");
        assert_eq!(OutputFormat::Raw.file_extension(), "bin");
    }

    #[test]
    fn file_rotation_display() {
        assert_eq!(FileRotation::Size.to_string(), "size");
        assert_eq!(FileRotation::Messages.to_string(), "messages");
    }

    #[test]
    fn config_deserialization_defaults() {
        let json = r#"{"bucket":"test","region":"us-east-1"}"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.bucket, "test");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.path_template, DEFAULT_PATH_TEMPLATE);
        assert_eq!(config.max_file_size, DEFAULT_MAX_FILE_SIZE);
        assert_eq!(config.output_format, DEFAULT_OUTPUT_FORMAT);
        assert!(config.include_metadata);
        assert!(!config.include_headers);
        assert_eq!(config.file_rotation, FileRotation::Size);
        assert!(config.prefix.is_none());
        assert!(config.endpoint.is_none());
        assert!(config.access_key_id.is_none());
        assert!(config.secret_access_key.is_none());
    }

    #[test]
    fn config_deserialization_full() {
        let json = r#"{
            "bucket": "my-bucket",
            "region": "eu-west-1",
            "prefix": "data/raw",
            "endpoint": "http://localhost:9000",
            "access_key_id": "AKIA...",
            "secret_access_key": "secret",
            "path_template": "{stream}/{topic}",
            "file_rotation": "messages",
            "max_file_size": "16MiB",
            "max_messages_per_file": 5000,
            "output_format": "json_array",
            "include_metadata": false,
            "include_headers": true,
            "max_attempts": 5,
            "retry_delay": "2s",
            "path_style": true
        }"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix.as_deref(), Some("data/raw"));
        assert_eq!(config.endpoint.as_deref(), Some("http://localhost:9000"));
        assert_eq!(config.file_rotation, FileRotation::Messages);
        assert_eq!(config.max_messages_per_file, Some(5000));
        assert!(!config.include_metadata);
        assert!(config.include_headers);
        assert_eq!(config.max_attempts, Some(5));
        assert_eq!(config.path_style, Some(true));
    }

    #[test]
    fn partial_credentials_detected() {
        let json = r#"{"bucket":"b","region":"us-east-1","access_key_id":"key"}"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(client::create_bucket(&config));
        assert!(result.is_err());
    }

    #[test]
    fn debug_does_not_leak_secrets() {
        let json =
            r#"{"bucket":"b","region":"r","access_key_id":"AKIA","secret_access_key":"s3cr3t"}"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("AKIA"));
        assert!(!debug_output.contains("s3cr3t"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn s3sink_debug_does_not_leak_bucket_credentials() {
        let json = r#"{"bucket":"b","region":"us-east-1","access_key_id":"AKIAIOSFODNN7EXAMPLE","secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        let sink = S3Sink::new(1, config);
        let debug_output = format!("{:?}", sink);
        assert!(
            !debug_output.contains("AKIAIOSFODNN7EXAMPLE"),
            "Debug output must not contain the access key"
        );
        assert!(
            !debug_output.contains("wJalrXUtnFEMI"),
            "Debug output must not contain the secret key"
        );
    }

    #[test]
    fn max_retries_alias_accepted() {
        let json = r#"{"bucket":"b","region":"us-east-1","max_retries":5}"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_attempts, Some(5));
    }
}
