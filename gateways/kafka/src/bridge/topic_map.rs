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
use std::path::Path;

use serde::Deserialize;

use crate::bridge::error::BridgeError;

/// Explicit Kafka-topic → Iggy stream/topic override. Absent entries fall back to
/// [`TopicMapping::default_stream`] plus the Kafka topic name unchanged - see
/// [`TopicMapping::resolve`].
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TopicOverride {
    pub stream: String,
    pub topic: String,
}

/// Kafka topic name → Iggy stream/topic mapping, loaded from TOML.
///
/// Default rule (no override): the Iggy stream is [`default_stream`](Self::default_stream) and
/// the Iggy topic name is the Kafka topic name unchanged. A gateway that fronts a single Kafka
/// "cluster" for one Iggy stream never needs an override entry at all.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct TopicMapping {
    pub default_stream: String,
    #[serde(default)]
    pub topics: HashMap<String, TopicOverride>,
}

impl TopicMapping {
    /// Resolves a Kafka topic name to `(iggy_stream, iggy_topic)`.
    #[must_use]
    pub fn resolve(&self, kafka_topic: &str) -> (String, String) {
        self.topics.get(kafka_topic).map_or_else(
            || (self.default_stream.clone(), kafka_topic.to_string()),
            |over| (over.stream.clone(), over.topic.clone()),
        )
    }

    /// Parses a `TopicMapping` from a TOML document.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `raw` is not valid TOML for this shape, or if
    /// `default_stream` is empty (every unmapped Kafka topic would otherwise resolve to an empty
    /// stream name, which the Iggy SDK rejects only once a request is actually made).
    pub fn from_toml_str(raw: &str) -> Result<Self, BridgeError> {
        let mapping: Self = toml::from_str(raw)
            .map_err(|e| BridgeError::InvalidConfig(format!("invalid topic mapping TOML: {e}")))?;
        if mapping.default_stream.trim().is_empty() {
            return Err(BridgeError::InvalidConfig(
                "topic mapping's default_stream must not be empty".to_string(),
            ));
        }
        Ok(mapping)
    }

    /// Reads and parses a `TopicMapping` TOML file.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if the file cannot be read, or on the same
    /// conditions as [`from_toml_str`](Self::from_toml_str).
    pub fn from_file(path: &Path) -> Result<Self, BridgeError> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            BridgeError::InvalidConfig(format!(
                "failed to read topic mapping file '{}': {e}",
                path.display()
            ))
        })?;
        Self::from_toml_str(&raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_no_override_should_resolve_to_default_stream_and_same_topic_name() {
        let mapping = TopicMapping {
            default_stream: "kafka".to_string(),
            topics: HashMap::new(),
        };
        assert_eq!(
            mapping.resolve("orders"),
            ("kafka".to_string(), "orders".to_string())
        );
    }

    #[test]
    fn given_override_should_resolve_to_mapped_stream_and_topic() {
        let mut topics = HashMap::new();
        topics.insert(
            "orders".to_string(),
            TopicOverride {
                stream: "billing".to_string(),
                topic: "orders_v2".to_string(),
            },
        );
        let mapping = TopicMapping {
            default_stream: "kafka".to_string(),
            topics,
        };
        assert_eq!(
            mapping.resolve("orders"),
            ("billing".to_string(), "orders_v2".to_string())
        );
        assert_eq!(
            mapping.resolve("payments"),
            ("kafka".to_string(), "payments".to_string())
        );
    }

    #[test]
    fn from_toml_str_parses_default_stream_and_overrides() {
        let toml = r#"
            default_stream = "kafka"

            [topics.orders]
            stream = "billing"
            topic = "orders_v2"
        "#;
        let mapping = TopicMapping::from_toml_str(toml).unwrap();
        assert_eq!(mapping.default_stream, "kafka");
        assert_eq!(
            mapping.resolve("orders"),
            ("billing".to_string(), "orders_v2".to_string())
        );
    }

    #[test]
    fn from_toml_str_rejects_empty_default_stream() {
        let toml = r#"default_stream = """#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_malformed_toml() {
        let err = TopicMapping::from_toml_str("not valid toml {{{").unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_file_rejects_missing_file() {
        let err = TopicMapping::from_file(Path::new("/nonexistent/topic_map.toml")).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }
}
