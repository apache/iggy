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

use crate::config::FlinkSourceConfig;
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Failed to parse JSON: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Failed to parse YAML: {0}")]
    YamlError(String),

    #[error("Failed to parse TOML: {0}")]
    TomlError(#[from] toml::de::Error),

    #[error("Unsupported config format: {0}")]
    UnsupportedFormat(String),
}

#[allow(dead_code)]
pub(crate) fn load_config<P: AsRef<Path>>(path: P) -> Result<FlinkSourceConfig, ConfigError> {
    let path = path.as_ref();
    let content = fs::read_to_string(path)?;

    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

    match extension.to_lowercase().as_str() {
        "json" => load_from_json(&content),
        "yaml" | "yml" => load_from_yaml(&content),
        "toml" => load_from_toml(&content),
        _ => {
            // Try to detect format from content
            if let Ok(config) = load_from_json(&content) {
                return Ok(config);
            }
            if let Ok(config) = load_from_yaml(&content) {
                return Ok(config);
            }
            if let Ok(config) = load_from_toml(&content) {
                return Ok(config);
            }
            Err(ConfigError::UnsupportedFormat(extension.to_string()))
        }
    }
}

#[allow(dead_code)]
pub(crate) fn load_from_json(content: &str) -> Result<FlinkSourceConfig, ConfigError> {
    Ok(serde_json::from_str(content)?)
}

#[allow(dead_code)]
pub(crate) fn load_from_yaml(_content: &str) -> Result<FlinkSourceConfig, ConfigError> {
    // Note: FlinkSource doesn't have serde_yml dependency yet
    // For now, return an error or parse manually
    Err(ConfigError::YamlError(
        "YAML support not yet implemented".to_string(),
    ))
}

#[allow(dead_code)]
pub(crate) fn load_from_toml(content: &str) -> Result<FlinkSourceConfig, ConfigError> {
    Ok(toml::from_str(content)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceType;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_json_config() {
        let json_config = r#"
        {
            "flink_cluster_url": "http://localhost:8081",
            "source_type": "kafka",
            "source_identifier": "input-events",
            "batch_size": 100
        }
        "#;

        let config = load_from_json(json_config).unwrap();
        assert_eq!(config.flink_cluster_url, "http://localhost:8081");
        assert!(matches!(config.source_type, SourceType::Kafka));
        assert_eq!(config.source_identifier, "input-events");
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_load_toml_config() {
        let toml_config = r#"
        flink_cluster_url = "http://localhost:8081"
        source_type = "kafka"
        source_identifier = "input-events"
        batch_size = 100
        poll_interval_ms = 1000
        "#;

        let config = load_from_toml(toml_config).unwrap();
        assert_eq!(config.flink_cluster_url, "http://localhost:8081");
        assert!(matches!(config.source_type, SourceType::Kafka));
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.poll_interval_ms, 1000);
    }

    #[test]
    fn test_load_from_file() {
        let mut file = NamedTempFile::new().unwrap();
        let json_content = r#"
        {
            "flink_cluster_url": "http://localhost:8081",
            "source_type": "kafka",
            "source_identifier": "test-topic",
            "batch_size": 50
        }
        "#;

        file.write_all(json_content.as_bytes()).unwrap();

        let config = load_config(file.path()).unwrap();
        assert_eq!(config.flink_cluster_url, "http://localhost:8081");
        assert_eq!(config.source_identifier, "test-topic");
    }

    #[test]
    fn test_auto_detect_format() {
        let mut file = NamedTempFile::new().unwrap();
        let toml_content = r#"
        flink_cluster_url = "http://localhost:8081"
        source_type = "kinesis"
        source_identifier = "my-stream"
        batch_size = 200
        "#;

        file.write_all(toml_content.as_bytes()).unwrap();

        // Even without extension, it should detect TOML
        let config = load_config(file.path()).unwrap();
        assert_eq!(config.flink_cluster_url, "http://localhost:8081");
        assert!(matches!(config.source_type, SourceType::Kinesis));
    }
}

