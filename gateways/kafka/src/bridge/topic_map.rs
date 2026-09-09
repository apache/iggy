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

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

use crate::bridge::error::BridgeError;

/// Upper bound `Identifier::named` itself enforces (`identifier/mod.rs`). Checked here too so a
/// too-long name in the mapping file fails at config load with a message that names the file and
/// field at fault, instead of surfacing much later as an opaque `InvalidIdentifier` deep inside
/// `IggyBridge::ensure_stream`/`ensure_topic`.
const MAX_IDENTIFIER_LEN: usize = 255;

/// Rejects an empty name, one with leading/trailing whitespace, or one over
/// [`MAX_IDENTIFIER_LEN`] bytes.
///
/// Whitespace is rejected outright rather than trimmed and stored: silently trimming here (or
/// adding that later) would change the resolved Iggy stream/topic name under an already-running
/// deployment without the operator changing anything they can see in their own config file.
///
/// `pub(crate)`: also used by `bridge::config` for `IGGY_KAFKA_IGGY_STREAM`, which feeds
/// `default_stream` through the same no-file path this module's own validation covers on the
/// TOML-file path.
pub(crate) fn validate_identifier_name(field: &str, value: &str) -> Result<(), BridgeError> {
    if value.is_empty() {
        return Err(BridgeError::InvalidConfig(format!(
            "{field} must not be empty"
        )));
    }
    if value.trim() != value {
        return Err(BridgeError::InvalidConfig(format!(
            "{field} must not have leading or trailing whitespace: {value:?}"
        )));
    }
    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(BridgeError::InvalidConfig(format!(
            "{field} is {} bytes, over the {MAX_IDENTIFIER_LEN}-byte limit Identifier::named \
             enforces: {value:?}",
            value.len()
        )));
    }
    Ok(())
}

/// Explicit Kafka-topic → Iggy stream/topic override. Absent entries fall back to
/// [`TopicMapping::default_stream`] plus the Kafka topic name unchanged - see
/// [`TopicMapping::resolve`].
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TopicOverride {
    pub stream: String,
    pub topic: String,
}

/// Kafka topic name → Iggy stream/topic mapping, loaded from TOML.
///
/// Default rule (no override): the Iggy stream is [`default_stream`](Self::default_stream) and
/// the Iggy topic name is the Kafka topic name unchanged. A gateway that fronts a single Kafka
/// "cluster" for one Iggy stream never needs an override entry at all.
///
/// No `Default` impl: an empty `default_stream` is not a valid `TopicMapping` -
/// [`from_toml_str`](Self::from_toml_str) rejects it, and a derived `Default` would silently
/// produce exactly that.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TopicMapping {
    pub default_stream: String,
    #[serde(default)]
    pub topics: HashMap<String, TopicOverride>,
}

impl TopicMapping {
    /// Resolves a Kafka topic name to `(iggy_stream, iggy_topic)`.
    ///
    /// Not injective: two distinct Kafka topics can resolve to the same Iggy stream/topic pair,
    /// merging their messages. `from_toml_str` rejects the checkable cases (two overrides sharing
    /// a target, or an override's target aliasing the default-path resolution of its own topic
    /// name) at config load, but the space of *unlisted* Kafka topic names is unbounded, so a
    /// collision against one that never gets an override entry can't be ruled out ahead of time.
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
    /// Returns [`BridgeError::InvalidConfig`] if `raw` is not valid TOML for this shape (an
    /// unrecognized field - e.g. a `[topic.x]` typo for `[topics.x]` - is rejected here rather
    /// than silently parsing to an empty override map), if `default_stream` or any override's
    /// `stream`/`topic` is empty, has leading/trailing whitespace, or exceeds
    /// [`MAX_IDENTIFIER_LEN`], or if two override entries are not injective (see
    /// [`resolve`](Self::resolve)) - in every validation case, letting it through here would
    /// otherwise fail much later, deep in `IggyBridge::ensure_stream`/`ensure_topic`, with no link
    /// back to the config entry at fault.
    pub fn from_toml_str(raw: &str) -> Result<Self, BridgeError> {
        let mapping: Self = toml::from_str(raw)
            .map_err(|e| BridgeError::InvalidConfig(format!("invalid topic mapping TOML: {e}")))?;
        validate_identifier_name("topic mapping's default_stream", &mapping.default_stream)?;

        let mut targets = HashSet::with_capacity(mapping.topics.len());
        for (kafka_topic, over) in &mapping.topics {
            validate_identifier_name(
                &format!("topic mapping override for '{kafka_topic}''s stream"),
                &over.stream,
            )?;
            validate_identifier_name(
                &format!("topic mapping override for '{kafka_topic}''s topic"),
                &over.topic,
            )?;

            let target = (over.stream.clone(), over.topic.clone());
            if !targets.insert(target) {
                return Err(BridgeError::InvalidConfig(format!(
                    "topic mapping override for '{kafka_topic}' targets Iggy stream '{}' topic \
                     '{}', which another override already targets - two Kafka topics would \
                     silently merge into one Iggy topic",
                    over.stream, over.topic
                )));
            }
            // A Kafka topic named exactly `over.topic`, if it never gets its own override entry,
            // resolves via the default path to (default_stream, over.topic) - the same pair this
            // override targets, if the target stream is also the default one. Only the aliasing
            // direction that maps to the *default* stream is checkable at load time; an override
            // targeting some other, non-default stream can't collide with the default path.
            if over.stream == mapping.default_stream && !mapping.topics.contains_key(&over.topic) {
                return Err(BridgeError::InvalidConfig(format!(
                    "topic mapping override for '{kafka_topic}' targets Iggy stream '{}' topic \
                     '{}', which is also where an unmapped Kafka topic literally named '{}' \
                     would resolve to by default - the two would silently merge into one Iggy \
                     topic; target a different Iggy topic name for '{kafka_topic}', or add an \
                     explicit override that redirects a Kafka topic named '{}' elsewhere",
                    over.stream, over.topic, over.topic, over.topic
                )));
            }
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
    fn from_toml_str_rejects_empty_override_stream() {
        let toml = r#"
            default_stream = "kafka"

            [topics.orders]
            stream = ""
            topic = "orders_v2"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_empty_override_topic() {
        let toml = r#"
            default_stream = "kafka"

            [topics.orders]
            stream = "billing"
            topic = ""
        "#;
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

    #[test]
    fn from_toml_str_rejects_a_typo_d_top_level_table_instead_of_silently_ignoring_it() {
        // "topic" (singular) for "topics" - without deny_unknown_fields this parses cleanly to
        // an empty override map, and every topic silently falls back to the default stream.
        let toml = r#"
            default_stream = "kafka"

            [topic.orders]
            stream = "billing"
            topic = "orders_v2"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_default_stream_with_leading_or_trailing_whitespace() {
        let toml = r#"default_stream = " kafka ""#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_default_stream_over_the_identifier_length_limit() {
        let toml = format!(
            "default_stream = \"{}\"",
            "a".repeat(MAX_IDENTIFIER_LEN + 1)
        );
        let err = TopicMapping::from_toml_str(&toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_two_overrides_targeting_the_same_stream_and_topic() {
        let toml = r#"
            default_stream = "kafka"

            [topics.orders]
            stream = "billing"
            topic = "orders_v2"

            [topics.legacy_orders]
            stream = "billing"
            topic = "orders_v2"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_an_override_that_aliases_an_unmapped_topics_default_resolution() {
        // orders_v2 has no override of its own, so it resolves to (kafka, orders_v2) by default -
        // the same pair this override targets.
        let toml = r#"
            default_stream = "kafka"

            [topics.orders]
            stream = "kafka"
            topic = "orders_v2"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_accepts_an_override_targeting_a_non_default_stream_with_the_same_topic_name() {
        // Targets "billing", not the default "kafka" stream, so there is no default-path
        // aliasing to catch regardless of what orders_v2 itself would resolve to.
        let toml = r#"
            default_stream = "kafka"

            [topics.orders]
            stream = "billing"
            topic = "orders_v2"
        "#;
        TopicMapping::from_toml_str(toml).expect("non-default-stream target is not an alias risk");
    }
}
