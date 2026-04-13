/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use iggy_connector_sdk::{Error, sink_connector};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub mod buffer;
pub mod client;
pub mod formatter;
pub mod path;
pub mod sink;

sink_connector!(S3Sink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_FILE_SIZE: &str = "8MiB";
const DEFAULT_PATH_TEMPLATE: &str = "{stream}/{topic}/{date}/{hour}";
const DEFAULT_OUTPUT_FORMAT: &str = "json_lines";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3SinkConfig {
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
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
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub retry_delay: Option<String>,
    #[serde(default)]
    pub path_style: Option<bool>,
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
pub enum OutputFormat {
    JsonLines,
    JsonArray,
    Raw,
}

impl OutputFormat {
    pub fn from_str_config(s: &str) -> Result<Self, Error> {
        match s.to_lowercase().as_str() {
            "json_lines" | "jsonl" | "jsonlines" => Ok(OutputFormat::JsonLines),
            "json_array" | "json" => Ok(OutputFormat::JsonArray),
            "raw" => Ok(OutputFormat::Raw),
            other => Err(Error::InvalidConfigValue(format!(
                "Unknown output format: '{other}'. Expected: json_lines, json_array, or raw"
            ))),
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            OutputFormat::JsonLines => "jsonl",
            OutputFormat::JsonArray => "json",
            OutputFormat::Raw => "bin",
        }
    }
}

#[derive(Debug)]
pub struct S3Sink {
    id: u32,
    config: S3SinkConfig,
    bucket: Option<Box<s3::Bucket>>,
    buffers: tokio::sync::Mutex<HashMap<BufferKey, buffer::FileBuffer>>,
    max_file_size_bytes: u64,
    output_format: OutputFormat,
    state: tokio::sync::Mutex<SinkState>,
    retry_delay: std::time::Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BufferKey {
    pub stream: String,
    pub topic: String,
    pub partition_id: u32,
}

#[derive(Debug)]
struct SinkState {
    messages_processed: u64,
    uploads_completed: u64,
    upload_errors: u64,
}

impl S3Sink {
    pub fn new(id: u32, config: S3SinkConfig) -> Self {
        let output_format = match OutputFormat::from_str_config(&config.output_format) {
            Ok(fmt) => fmt,
            Err(e) => {
                tracing::warn!(
                    "S3 sink ID: {id} invalid output_format '{}': {e}, defaulting to json_lines",
                    config.output_format,
                );
                OutputFormat::JsonLines
            }
        };

        let max_file_size_bytes = match parse_file_size(&config.max_file_size) {
            Ok(size) => size,
            Err(e) => {
                tracing::warn!(
                    "S3 sink ID: {id} invalid max_file_size '{}': {e}, defaulting to 8 MiB",
                    config.max_file_size,
                );
                8 * 1024 * 1024
            }
        };

        let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = match humantime::Duration::from_str(delay_str) {
            Ok(d) => d.into(),
            Err(e) => {
                tracing::warn!(
                    "S3 sink ID: {id} invalid retry_delay '{delay_str}': {e}, defaulting to 1s",
                );
                std::time::Duration::from_secs(1)
            }
        };

        S3Sink {
            id,
            config,
            bucket: None,
            buffers: tokio::sync::Mutex::new(HashMap::new()),
            max_file_size_bytes,
            output_format,
            state: tokio::sync::Mutex::new(SinkState {
                messages_processed: 0,
                uploads_completed: 0,
                upload_errors: 0,
            }),
            retry_delay,
        }
    }

    fn max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }
}

use std::str::FromStr;

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
            OutputFormat::from_str_config("json_lines").unwrap(),
            OutputFormat::JsonLines
        );
        assert_eq!(
            OutputFormat::from_str_config("jsonl").unwrap(),
            OutputFormat::JsonLines
        );
        assert_eq!(
            OutputFormat::from_str_config("JSONLINES").unwrap(),
            OutputFormat::JsonLines
        );
    }

    #[test]
    fn output_format_json_array() {
        assert_eq!(
            OutputFormat::from_str_config("json_array").unwrap(),
            OutputFormat::JsonArray
        );
        assert_eq!(
            OutputFormat::from_str_config("json").unwrap(),
            OutputFormat::JsonArray
        );
    }

    #[test]
    fn output_format_raw() {
        assert_eq!(
            OutputFormat::from_str_config("raw").unwrap(),
            OutputFormat::Raw
        );
    }

    #[test]
    fn output_format_invalid() {
        assert!(OutputFormat::from_str_config("xml").is_err());
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
            "max_retries": 5,
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
        assert_eq!(config.max_retries, Some(5));
        assert_eq!(config.path_style, Some(true));
    }

    #[test]
    fn partial_credentials_detected() {
        let config_with_key_only = S3SinkConfig {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            prefix: None,
            endpoint: None,
            access_key_id: Some("key".to_string()),
            secret_access_key: None,
            path_template: default_path_template(),
            file_rotation: FileRotation::Size,
            max_file_size: default_max_file_size(),
            max_messages_per_file: None,
            output_format: default_output_format(),
            include_metadata: true,
            include_headers: false,
            max_retries: None,
            retry_delay: None,
            path_style: None,
        };
        assert!(client::validate_credentials(&config_with_key_only).is_err());
    }
}
