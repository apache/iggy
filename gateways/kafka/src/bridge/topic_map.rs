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

/// Kafka's own topic-name cap (`Topic.MAX_NAME_LENGTH` in Kafka's own source) - smaller than
/// [`MAX_IDENTIFIER_LEN`], so a Kafka-side name needs its own limit, not Iggy's.
const MAX_KAFKA_TOPIC_NAME_LEN: usize = 249;

/// Rejects an empty name, one with leading/trailing whitespace, or one over
/// [`MAX_IDENTIFIER_LEN`] bytes.
///
/// Whitespace is rejected outright rather than trimmed and stored: silently trimming here (or
/// adding that later) would change the resolved Iggy stream/topic name under an already-running
/// deployment without the operator changing anything they can see in their own config file.
///
/// `pub(crate)`: also used by `bridge::config` for `IGGY_KAFKA_IGGY_STREAM`, which feeds
/// `default_stream` through the same no-file path this module's own validation covers on the
/// TOML-file path. For a Kafka-side topic name (a TOML key, or a `kafka_topic` argument), use
/// [`validate_kafka_topic_name`] instead - Kafka's own limits and legal characters differ from
/// Iggy's.
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

/// Rejects a Kafka topic name that fails Kafka's own naming rules: empty, leading/trailing
/// whitespace, over [`MAX_KAFKA_TOPIC_NAME_LEN`] bytes, or containing a byte outside
/// `[A-Za-z0-9._-]` (Kafka's own `Topic.legalChars`).
///
/// Used both for TOML mapping-file keys (`from_toml_str`) and for the `kafka_topic` argument
/// `IggyBridge::ensure_stream_and_topic`/`high_watermark` take directly - the two paths must agree,
/// or a name the config file would reject can still reach Iggy unvalidated through the second
/// path. A whitespace-padded key is the sharpest failure mode this catches: `resolve`'s lookup is
/// an exact `HashMap::get`, so a padded key parses out of the TOML file cleanly and then matches
/// nothing at runtime, silently falling through to the default stream instead of erroring - the
/// one place in this module that would otherwise fail silently instead of loudly.
///
/// `field` is `"topic mapping key"` or `"kafka_topic"` in current callers, not a generic template -
/// kept as a parameter (like [`validate_identifier_name`]) so the message names the actual site.
pub(crate) fn validate_kafka_topic_name(field: &str, value: &str) -> Result<(), BridgeError> {
    if value.is_empty() {
        return Err(BridgeError::InvalidKafkaTopicName {
            kafka_topic: value.to_string(),
            reason: format!("{field} must not be empty"),
        });
    }
    if value.trim() != value {
        return Err(BridgeError::InvalidKafkaTopicName {
            kafka_topic: value.to_string(),
            reason: format!("{field} must not have leading or trailing whitespace"),
        });
    }
    if value.len() > MAX_KAFKA_TOPIC_NAME_LEN {
        return Err(BridgeError::InvalidKafkaTopicName {
            kafka_topic: value.to_string(),
            reason: format!(
                "{field} is {} bytes, over Kafka's {MAX_KAFKA_TOPIC_NAME_LEN}-byte topic name limit",
                value.len()
            ),
        });
    }
    // Kafka's own `Topic.validate` special-cases these two names as invalid even though every
    // byte in them is otherwise legal (`.` is itself an allowed character) - not a traversal risk
    // here, since Iggy builds on-disk paths from numeric ids rather than the topic name, but the
    // doc above claims this function applies Kafka's own rules, and these are two of them.
    if value == "." || value == ".." {
        return Err(BridgeError::InvalidKafkaTopicName {
            kafka_topic: value.to_string(),
            reason: format!("{field} cannot be \".\" or \"..\""),
        });
    }
    if let Some(illegal) = value
        .bytes()
        .find(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')))
    {
        return Err(BridgeError::InvalidKafkaTopicName {
            kafka_topic: value.to_string(),
            reason: format!(
                "{field} contains {:?}, outside Kafka's legal topic-name characters \
                 (letters, digits, '.', '_', '-')",
                illegal as char
            ),
        });
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

/// Kafka topic name → Iggy stream/topic mapping.
///
/// Default rule (no override): the Iggy stream is [`default_stream`](Self::default_stream) and
/// the Iggy topic name is the Kafka topic name unchanged. A gateway that fronts a single Kafka
/// "cluster" for one Iggy stream never needs an override entry at all.
///
/// Fields are private and every value is validated (empty/whitespace/length/injectivity - see
/// [`new`](Self::new)) on the only two ways to build one: [`new`](Self::new) and
/// [`from_toml_str`](Self::from_toml_str) (which deserializes into a private, unchecked
/// `RawTopicMapping` first, then calls `new`). Public fields plus `#[derive(Deserialize)]`
/// directly on this type would let any caller construct or mutate one straight from TOML or a
/// literal, skipping every check below - which is exactly what happened before this type had a
/// checked constructor: `bridge::config`'s no-file path built one by hand and needed its own
/// separate validation call to make up for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMapping {
    default_stream: String,
    topics: HashMap<String, TopicOverride>,
}

/// Unchecked shadow of [`TopicMapping`], `Deserialize`'s only target - never constructed by hand,
/// never exposed. `TopicMapping::from_toml_str` is the sole path from TOML to a validated
/// `TopicMapping`, by deserializing into this first and then calling
/// [`TopicMapping::new`](TopicMapping::new).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTopicMapping {
    default_stream: String,
    #[serde(default)]
    topics: HashMap<String, TopicOverride>,
}

impl TopicMapping {
    /// Builds a validated `TopicMapping` from already-parsed parts - the checked constructor the
    /// no-file `IGGY_KAFKA_IGGY_STREAM` path in `bridge::config` uses, and the sole validation
    /// gate `from_toml_str` also funnels through.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `default_stream` or any override's `stream`/
    /// `topic` is empty, has leading/trailing whitespace, or exceeds `MAX_IDENTIFIER_LEN`; if any
    /// override key fails Kafka's own topic-naming rules (see
    /// `validate_kafka_topic_name`); or if two override entries are not injective (see
    /// [`resolve`](Self::resolve)).
    pub fn new(
        default_stream: String,
        topics: HashMap<String, TopicOverride>,
    ) -> Result<Self, BridgeError> {
        validate_identifier_name("topic mapping's default_stream", &default_stream)?;

        let mut targets = HashSet::with_capacity(topics.len());
        for (kafka_topic, over) in &topics {
            // `validate_kafka_topic_name` returns `InvalidKafkaTopicName` - the variant a live
            // Kafka request's own `kafka_topic` argument fails with, wire-mapped to
            // `INVALID_TOPIC_EXCEPTION` (17), "the client asked for a bad name." A mapping-file
            // key is not that: it is an operator's config, validated once at load time, long
            // before any Kafka client is involved. Rewrapped as `InvalidConfig` (wire-mapped to
            // -1) so a TOML typo reads as the bridge's own fault, not as if some Kafka client had
            // sent this exact malformed name - matching this function's own `# Errors` doc, which
            // promises `InvalidConfig` for a bad key.
            if let Err(BridgeError::InvalidKafkaTopicName { reason, .. }) =
                validate_kafka_topic_name("topic mapping key", kafka_topic)
            {
                return Err(BridgeError::InvalidConfig(format!(
                    "topic mapping key {kafka_topic:?}: {reason}"
                )));
            }
            validate_identifier_name(
                &format!("topic mapping override for '{kafka_topic}': its stream"),
                &over.stream,
            )?;
            validate_identifier_name(
                &format!("topic mapping override for '{kafka_topic}': its topic"),
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
            if over.stream == default_stream && !topics.contains_key(&over.topic) {
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
        Ok(Self {
            default_stream,
            topics,
        })
    }

    /// The Iggy stream a Kafka topic with no explicit override resolves to.
    #[must_use]
    pub fn default_stream(&self) -> &str {
        &self.default_stream
    }

    /// Resolves a Kafka topic name to `(iggy_stream, iggy_topic)`.
    ///
    /// Not injective: two distinct Kafka topics can resolve to the same Iggy stream/topic pair,
    /// merging their messages. `new` rejects the checkable cases (two overrides sharing a target,
    /// or an override's target aliasing the default-path resolution of its own topic name) at
    /// config load, but the space of *unlisted* Kafka topic names is unbounded, so a collision
    /// against one that never gets an override entry can't be ruled out ahead of time.
    #[must_use]
    pub fn resolve<'a>(&'a self, kafka_topic: &'a str) -> (&'a str, &'a str) {
        self.topics.get(kafka_topic).map_or_else(
            || (self.default_stream.as_str(), kafka_topic),
            |over| (over.stream.as_str(), over.topic.as_str()),
        )
    }

    /// Parses a `TopicMapping` from a TOML document.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `raw` is not valid TOML for this shape (an
    /// unrecognized field - e.g. a `[topic.x]` typo for `[topics.x]` - is rejected here rather
    /// than silently parsing to an empty override map), or on the same conditions as
    /// [`new`](Self::new) - in every validation case, letting it through here would otherwise
    /// fail much later, deep in `IggyBridge::ensure_stream`/`ensure_topic`, with no link back to
    /// the config entry at fault.
    pub fn from_toml_str(raw: &str) -> Result<Self, BridgeError> {
        let raw_mapping: RawTopicMapping = toml::from_str(raw)
            .map_err(|e| BridgeError::InvalidConfig(format!("invalid topic mapping TOML: {e}")))?;
        Self::new(raw_mapping.default_stream, raw_mapping.topics)
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

    fn mapping_with(default_stream: &str, topics: HashMap<String, TopicOverride>) -> TopicMapping {
        TopicMapping::new(default_stream.to_string(), topics)
            .expect("valid mapping for this test's fixture data")
    }

    #[test]
    fn given_no_override_should_resolve_to_default_stream_and_same_topic_name() {
        let mapping = mapping_with("kafka", HashMap::new());
        assert_eq!(mapping.resolve("orders"), ("kafka", "orders"));
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
        let mapping = mapping_with("kafka", topics);
        assert_eq!(mapping.resolve("orders"), ("billing", "orders_v2"));
        assert_eq!(mapping.resolve("payments"), ("kafka", "payments"));
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
        assert_eq!(mapping.default_stream(), "kafka");
        assert_eq!(mapping.resolve("orders"), ("billing", "orders_v2"));
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

    #[test]
    fn new_rejects_empty_default_stream() {
        // The bypass this closes: bridge::config's no-file path (and anyone else) used to be
        // able to hand-build a TopicMapping struct literal, skipping every check from_toml_str
        // ran. new() is now the only way in, TOML or not.
        let err = TopicMapping::new(String::new(), HashMap::new()).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn new_rejects_a_padded_override_target_stream() {
        let mut topics = HashMap::new();
        topics.insert(
            "orders".to_string(),
            TopicOverride {
                stream: " billing".to_string(),
                topic: "orders_v2".to_string(),
            },
        );
        let err = TopicMapping::new("kafka".to_string(), topics).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_a_whitespace_padded_topic_mapping_key() {
        // The one silent-failure case this whole module exists to close: without key
        // validation, " orders " parses out of the TOML cleanly, then never matches
        // resolve()'s exact HashMap::get for the real Kafka topic "orders" - it just silently
        // falls through to the default stream instead of erroring.
        //
        // InvalidConfig, not InvalidKafkaTopicName: this is an operator's TOML typo, caught at
        // config load, not a live Kafka client sending a malformed topic name - InvalidConfig
        // wire-maps to -1 (the bridge's own fault), InvalidKafkaTopicName to 17 (the client's).
        let toml = r#"
            default_stream = "kafka"

            [topics." orders "]
            stream = "billing"
            topic = "orders_v2"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_an_empty_topic_mapping_key() {
        let toml = r#"
            default_stream = "kafka"

            [topics.""]
            stream = "billing"
            topic = "orders_v2"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_a_topic_mapping_key_over_kafkas_length_limit() {
        let toml = format!(
            "default_stream = \"kafka\"\n\n[topics.{:?}]\nstream = \"billing\"\ntopic = \"orders_v2\"\n",
            "a".repeat(MAX_KAFKA_TOPIC_NAME_LEN + 1)
        );
        let err = TopicMapping::from_toml_str(&toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn from_toml_str_rejects_a_topic_mapping_key_with_an_illegal_character() {
        // Kafka's own legal-character set has no '/' in it - a topic named this could never be
        // sent by a real Kafka client, so an override entry for it can never fire either.
        let toml = r#"
            default_stream = "kafka"

            [topics."orders/2024"]
            stream = "billing"
            topic = "orders_v2"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)));
    }

    #[test]
    fn validate_kafka_topic_name_rejects_dot_and_dotdot() {
        // Kafka's own Topic.validate special-cases exactly these two names as invalid, even
        // though '.' alone is a legal character in every other position.
        let err = validate_kafka_topic_name("kafka_topic", ".").unwrap_err();
        assert!(matches!(err, BridgeError::InvalidKafkaTopicName { .. }));
        let err = validate_kafka_topic_name("kafka_topic", "..").unwrap_err();
        assert!(matches!(err, BridgeError::InvalidKafkaTopicName { .. }));
        // A real Kafka application name containing dots (not equal to "." or "..") stays legal.
        validate_kafka_topic_name("kafka_topic", "org.apache.kafka.events").unwrap();
    }

    #[test]
    fn validate_kafka_topic_name_accepts_every_legal_character_class() {
        validate_kafka_topic_name("kafka_topic", "Order.Events_2024-v2").unwrap();
    }
}
