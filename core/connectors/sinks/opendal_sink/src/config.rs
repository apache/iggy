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

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{collections::BTreeMap, fmt, path::Path, time::Duration};

use iggy_connector_sdk::{
    Error,
    retry::{RetryPolicy, parse_duration},
};
use opendal::Operator;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::time::timeout;

use crate::formatter::Formatter;

const DEFAULT_PATH_TEMPLATE: &str = "{stream}/{topic}/{date}/{hour}";
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_OUTPUT_FORMAT: &str = "json_lines";
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const DEFAULT_MAX_FILE_SIZE: &str = "8MiB";
const DEFAULT_WRITE_TIMEOUT: &str = "60s";
const MAX_OBJECT_SIZE: u64 = 5000 * 1024 * 1024;

#[derive(Clone, Deserialize)]
pub struct OpenDalSinkConfig {
    pub service: String,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default = "default_path_template")]
    pub path_template: String,
    #[serde(default)]
    pub options: BTreeMap<String, SecretString>,
    #[serde(default = "default_output_format")]
    pub output_format: String,
    #[serde(default = "default_true")]
    pub include_metadata: bool,
    #[serde(default)]
    pub include_headers: bool,
    #[serde(default, alias = "max_retries")]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub retry_delay: Option<String>,
    #[serde(default)]
    pub max_file_size: Option<String>,
    #[serde(default)]
    pub write_timeout: Option<String>,
    #[serde(default)]
    pub if_not_exists: Option<bool>,
    #[serde(default)]
    pub verbose_logging: Option<bool>,
}

impl fmt::Debug for OpenDalSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenDalSinkConfig")
            .field("service", &self.service)
            .field("path_prefix", &self.path_prefix)
            .field("path_template", &self.path_template)
            .field("option_keys", &self.options.keys().collect::<Vec<_>>())
            .field("output_format", &self.output_format)
            .field("include_metadata", &self.include_metadata)
            .field("include_headers", &self.include_headers)
            .field("max_attempts", &self.max_attempts)
            .field("retry_delay", &self.retry_delay)
            .field("max_file_size", &self.max_file_size)
            .field("write_timeout", &self.write_timeout)
            .field("if_not_exists", &self.if_not_exists)
            .field("verbose_logging", &self.verbose_logging)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct ValidatedConfig {
    pub(crate) service: String,
    pub(crate) path_prefix: String,
    pub(crate) path_template: String,
    pub(crate) options: BTreeMap<String, SecretString>,
    pub(crate) max_file_size: usize,
    pub(crate) output_format: OutputFormat,
    pub(crate) include_metadata: bool,
    pub(crate) include_headers: bool,
    pub(crate) retry_policy: RetryPolicy,
    pub(crate) write_timeout: Duration,
    pub(crate) if_not_exists: bool,
    pub(crate) verbose_logging: bool,
}

#[derive(Debug)]
pub(crate) enum Validation {
    Valid(ValidatedConfig),
    Invalid {
        config: OpenDalSinkConfig,
        error: Error,
    },
}

impl OpenDalSinkConfig {
    pub(crate) fn validate(self) -> Validation {
        let checked = (|| {
            let mut service = self.service.trim().to_ascii_lowercase();
            if service == "file" {
                service.replace_range(.., "fs");
            }
            if service.is_empty() {
                return Err(Error::InvalidConfigValue(
                    "OpenDAL service cannot be empty".to_string(),
                ));
            }
            if service == "memory" {
                return Err(Error::InvalidConfigValue(
                    "OpenDAL memory storage does not preserve archived messages".to_string(),
                ));
            }
            if !self.path_template.contains("{stream}") || !self.path_template.contains("{topic}") {
                return Err(Error::InvalidConfigValue(
                    "OpenDAL path_template must contain {stream} and {topic}".to_string(),
                ));
            }
            let max_file_size = self
                .max_file_size
                .as_deref()
                .unwrap_or(DEFAULT_MAX_FILE_SIZE);
            let max_file_size = max_file_size
                .parse::<byte_unit::Byte>()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!("Invalid max_file_size: {error}"))
                })?
                .as_u64();
            if max_file_size == 0 || max_file_size > MAX_OBJECT_SIZE {
                return Err(Error::InvalidConfigValue(
                    "OpenDAL max_file_size must be between 1 byte and 5000 MiB".to_string(),
                ));
            }
            let max_file_size = usize::try_from(max_file_size).map_err(|_| {
                Error::InvalidConfigValue(
                    "OpenDAL max_file_size exceeds addressable memory".to_string(),
                )
            })?;
            let write_timeout =
                parse_duration(self.write_timeout.as_deref(), DEFAULT_WRITE_TIMEOUT);
            if write_timeout.is_zero() {
                return Err(Error::InvalidConfigValue(
                    "OpenDAL write_timeout must be greater than zero".to_string(),
                ));
            }
            let output_format = OutputFormat::try_from(self.output_format.as_str())?;
            let retry_policy = RetryPolicy {
                max_attempts: self
                    .max_attempts
                    .unwrap_or(DEFAULT_MAX_ATTEMPTS)
                    .clamp(1, DEFAULT_MAX_ATTEMPTS),
                base_delay: parse_duration(self.retry_delay.as_deref(), DEFAULT_RETRY_DELAY),
                max_delay: MAX_BACKOFF,
            };
            let mut option_keys = self.options.keys().collect::<Vec<_>>();
            option_keys.sort_unstable_by(|left, right| {
                left.bytes()
                    .map(|byte| byte.to_ascii_lowercase())
                    .cmp(right.bytes().map(|byte| byte.to_ascii_lowercase()))
            });
            if let Some(keys) = option_keys
                .windows(2)
                .find(|keys| keys[0].eq_ignore_ascii_case(keys[1]))
            {
                return Err(Error::InvalidConfigValue(format!(
                    "OpenDAL option '{}' is specified more than once",
                    keys[0].to_ascii_lowercase()
                )));
            }
            let atomic_write_dir = if service == "fs" {
                let root = self
                    .options
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("root"))
                    .map(|(_, value)| value.expose_secret())
                    .filter(|root| !root.is_empty())
                    .ok_or_else(|| {
                        Error::InvalidConfigValue("OpenDAL fs requires root".to_string())
                    })?;
                if self
                    .options
                    .keys()
                    .any(|key| key.eq_ignore_ascii_case("atomic_write_dir"))
                {
                    None
                } else {
                    Some(
                        Path::new(root)
                            .join(".iggy-opendal-tmp")
                            .to_string_lossy()
                            .into_owned(),
                    )
                }
            } else {
                None
            };
            Ok((
                service,
                max_file_size,
                output_format,
                retry_policy,
                write_timeout,
                atomic_write_dir,
            ))
        })();
        let (service, max_file_size, output_format, retry_policy, write_timeout, atomic_write_dir) =
            match checked {
                Ok(checked) => checked,
                Err(error) => {
                    return Validation::Invalid {
                        config: self,
                        error,
                    };
                }
            };
        let mut options = self
            .options
            .into_iter()
            .map(|(mut key, value)| {
                key.make_ascii_lowercase();
                (key, value)
            })
            .collect::<BTreeMap<_, _>>();
        if let Some(atomic_write_dir) = atomic_write_dir {
            options.insert(
                "atomic_write_dir".to_string(),
                SecretString::from(atomic_write_dir),
            );
        }
        let mut path_prefix = self.path_prefix.unwrap_or_default();
        path_prefix.truncate(path_prefix.trim_end_matches('/').len());
        let leading_slashes = path_prefix.len() - path_prefix.trim_start_matches('/').len();
        path_prefix.drain(..leading_slashes);
        Validation::Valid(ValidatedConfig {
            service,
            path_prefix,
            path_template: self.path_template,
            options,
            max_file_size,
            output_format,
            include_metadata: self.include_metadata,
            include_headers: self.include_headers,
            retry_policy,
            write_timeout,
            if_not_exists: self.if_not_exists.unwrap_or(false),
            verbose_logging: self.verbose_logging.unwrap_or(false),
        })
    }
}

impl ValidatedConfig {
    pub(crate) fn build_formatter(&self) -> Formatter {
        Formatter::new(
            self.output_format,
            self.include_metadata,
            self.include_headers,
        )
    }

    pub(crate) async fn build_operator(&self) -> Result<Operator, Error> {
        opendal::install_default();
        self.validate_atomic_directory().await?;
        let service = self.service.as_str();
        let operator = Operator::via_iter(
            service,
            self.options
                .iter()
                .map(|(key, value)| (key.clone(), value.expose_secret().to_string())),
        )
        .map_err(|error| {
            Error::InitError(format!(
                "Failed to create OpenDAL service '{service}': {error}"
            ))
        })?;
        let info = operator.info();
        let capability = info.capability();
        if !capability.write {
            return Err(Error::InvalidConfigValue(format!(
                "OpenDAL service '{service}' does not support writes"
            )));
        }
        if !capability.delete {
            return Err(Error::InvalidConfigValue(format!(
                "OpenDAL service '{service}' does not support write probe deletion"
            )));
        }
        if self.if_not_exists && !capability.write_with_if_not_exists {
            return Err(Error::InvalidConfigValue(format!(
                "OpenDAL service '{service}' does not support if_not_exists"
            )));
        }
        Ok(operator)
    }

    async fn validate_atomic_directory(&self) -> Result<(), Error> {
        if self.service != "fs" {
            return Ok(());
        }
        timeout(self.write_timeout, async {
            let root = self
                .options
                .get("root")
                .map(|value| value.expose_secret())
                .filter(|root| !root.is_empty())
                .ok_or_else(|| Error::InvalidConfigValue("OpenDAL fs requires root".to_string()))?;
            let Some(atomic_directory) = self
                .options
                .get("atomic_write_dir")
                .map(|value| value.expose_secret())
                .filter(|directory| !directory.is_empty())
            else {
                return Ok(());
            };
            tokio::fs::create_dir_all(root).await.map_err(|error| {
                Error::InitError(format!("Cannot create OpenDAL root: {error}"))
            })?;
            tokio::fs::create_dir_all(atomic_directory)
                .await
                .map_err(|error| {
                    Error::InitError(format!("Cannot create OpenDAL atomic directory: {error}"))
                })?;
            #[cfg(unix)]
            {
                let root_metadata = tokio::fs::metadata(root).await.map_err(|error| {
                    Error::InitError(format!("Cannot read OpenDAL root: {error}"))
                })?;
                let atomic_metadata =
                    tokio::fs::metadata(atomic_directory)
                        .await
                        .map_err(|error| {
                            Error::InitError(format!(
                                "Cannot read OpenDAL atomic directory: {error}"
                            ))
                        })?;
                if root_metadata.dev() != atomic_metadata.dev() {
                    return Err(Error::InvalidConfigValue(
                        "OpenDAL atomic_write_dir and root must use the same filesystem"
                            .to_string(),
                    ));
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::InitError("OpenDAL filesystem check timed out".to_string()))?
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

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "json_lines" | "jsonl" | "jsonlines" => Ok(Self::JsonLines),
            "json_array" => Ok(Self::JsonArray),
            "raw" => Ok(Self::Raw),
            other => Err(Error::InvalidConfigValue(format!(
                "Unknown output format: '{other}'. Expected: json_lines, json_array, or raw"
            ))),
        }
    }
}

impl OutputFormat {
    pub(crate) fn file_extension(self) -> &'static str {
        match self {
            Self::JsonLines => "jsonl",
            Self::JsonArray => "json",
            Self::Raw => "bin",
        }
    }
}

fn default_path_template() -> String {
    DEFAULT_PATH_TEMPLATE.to_string()
}

fn default_output_format() -> String {
    DEFAULT_OUTPUT_FORMAT.to_string()
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs;

    use iggy_connector_sdk::Sink;
    use tempfile::TempDir;

    use super::*;
    use crate::OpenDalSink;

    #[test]
    fn given_memory_service_when_opening_should_reject_volatile_storage() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let mut config = test_config("/tmp");
            config.service = "memory".to_string();
            config.options.clear();
            let mut sink = OpenDalSink::new(1, config);

            let error = sink
                .open()
                .await
                .expect_err("volatile storage must not accept archive messages");

            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }

    #[test]
    fn given_config_when_debugged_should_redact_option_values() {
        let config = test_config("/secret/storage/path");

        let raw_output = format!("{config:?}");
        let Validation::Valid(validated) = config.validate() else {
            panic!("configuration should be valid");
        };
        for output in [raw_output, format!("{validated:?}")] {
            assert!(output.contains("root"));
            assert!(!output.contains("/secret/storage/path"));
        }
    }

    #[test]
    fn given_whitespace_service_when_opening_should_reject_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let mut config = test_config("/tmp");
            config.service = " \t".to_string();
            let mut sink = OpenDalSink::new(1, config);

            let error = sink
                .open()
                .await
                .expect_err("whitespace service should fail");

            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }
    #[test]
    fn given_empty_path_template_when_opening_should_reject_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let mut config = test_config("/tmp");
            config.path_template.clear();
            let mut sink = OpenDalSink::new(1, config);

            let error = sink
                .open()
                .await
                .expect_err("empty path template should fail");

            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }

    #[test]
    fn given_unknown_output_format_when_opening_should_reject_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let mut config = test_config("/tmp");
            config.output_format = "csv".to_string();
            let mut sink = OpenDalSink::new(1, config);

            let error = sink
                .open()
                .await
                .expect_err("unknown output format should fail");

            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }

    #[test]
    fn given_missing_topic_or_stream_when_opening_should_reject_template() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            for template in ["{date}/{hour}", "{stream}/{date}", "{topic}/{hour}"] {
                let mut config = test_config("/tmp");
                config.path_template = template.to_string();
                let error = OpenDalSink::new(1, config)
                    .open()
                    .await
                    .expect_err("template must separate streams and topics");
                assert!(matches!(error, Error::InvalidConfigValue(_)));
            }
        });
    }

    #[test]
    fn given_missing_or_empty_root_when_opening_should_not_create_atomic_directory() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            for root in [None, Some("")] {
                for explicit_atomic_directory in [false, true] {
                    let temp_dir = TempDir::new().expect("temp directory should be created");
                    let atomic_directory = temp_dir.path().join("not-created");
                    let mut config = test_config("");
                    config.options.clear();
                    if let Some(root) = root {
                        config
                            .options
                            .insert("root".to_string(), SecretString::from(root));
                    }
                    if explicit_atomic_directory {
                        config.options.insert(
                            "atomic_write_dir".to_string(),
                            SecretString::from(atomic_directory.to_string_lossy().into_owned()),
                        );
                    }

                    let error = OpenDalSink::new(1, config)
                        .open()
                        .await
                        .expect_err("filesystem storage requires a nonempty root");

                    assert!(matches!(error, Error::InvalidConfigValue(_)));
                    assert!(!atomic_directory.exists());
                    assert_eq!(
                        fs::read_dir(temp_dir.path())
                            .expect("temp directory should be readable")
                            .count(),
                        0
                    );
                }
            }
        });
    }

    #[test]
    fn given_file_alias_and_uppercase_options_when_opening_should_use_atomic_writes() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            for custom_directory in [false, true] {
                let root = TempDir::new().expect("root should be created");
                let mut config = test_config(&root.path().to_string_lossy());
                config.service = "FiLe".to_string();
                config.options = BTreeMap::from([(
                    "ROOT".to_string(),
                    SecretString::from(root.path().to_string_lossy().into_owned()),
                )]);
                let atomic_directory = root.path().join(if custom_directory {
                    "custom-atomic"
                } else {
                    ".iggy-opendal-tmp"
                });
                if custom_directory {
                    config.options.insert(
                        "ATOMIC_WRITE_DIR".to_string(),
                        SecretString::from(atomic_directory.to_string_lossy().into_owned()),
                    );
                }
                let mut sink = OpenDalSink::new(1, config);
                sink.open().await.expect("filesystem alias should open");
                assert!(atomic_directory.is_dir());
                assert_eq!(
                    fs::read_dir(&atomic_directory)
                        .expect("atomic directory should exist")
                        .count(),
                    0
                );
                if custom_directory {
                    assert!(!root.path().join(".iggy-opendal-tmp").exists());
                }
            }
        });
    }

    pub(crate) fn test_config(root: &str) -> OpenDalSinkConfig {
        OpenDalSinkConfig {
            service: "fs".to_string(),
            path_prefix: Some("iggy/messages".to_string()),
            path_template: default_path_template(),
            options: BTreeMap::from([("root".to_string(), SecretString::from(root.to_string()))]),
            output_format: default_output_format(),
            include_metadata: false,
            include_headers: false,
            max_attempts: Some(1),
            retry_delay: None,
            max_file_size: None,
            write_timeout: None,
            if_not_exists: None,
            verbose_logging: None,
        }
    }
}
