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

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

use crate::bridge::error::BridgeError;
use crate::bridge::iggy_bridge::cluster_id_stream;

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
    let Some(violation) = kafka_topic_name_violation(value) else {
        return Ok(());
    };
    let reason = match violation {
        KafkaTopicNameViolation::Empty => format!("{field} must not be empty"),
        KafkaTopicNameViolation::Padded => {
            format!("{field} must not have leading or trailing whitespace")
        }
        KafkaTopicNameViolation::TooLong => format!(
            "{field} is {} bytes, over Kafka's {MAX_KAFKA_TOPIC_NAME_LEN}-byte topic name limit",
            value.len()
        ),
        KafkaTopicNameViolation::DotName => format!("{field} cannot be \".\" or \"..\""),
        KafkaTopicNameViolation::IllegalByte(illegal) => format!(
            "{field} contains {:?}, outside Kafka's legal topic-name characters \
             (letters, digits, '.', '_', '-')",
            illegal as char
        ),
    };
    Err(BridgeError::InvalidKafkaTopicName {
        kafka_topic: value.to_string(),
        reason,
    })
}

/// Whether `value` passes [`validate_kafka_topic_name`], without building its error.
pub(crate) fn is_valid_kafka_topic_name(value: &str) -> bool {
    kafka_topic_name_violation(value).is_none()
}

/// The first of Kafka's topic-name rules `value` breaks.
enum KafkaTopicNameViolation {
    Empty,
    Padded,
    TooLong,
    DotName,
    IllegalByte(u8),
}

fn kafka_topic_name_violation(value: &str) -> Option<KafkaTopicNameViolation> {
    if value.is_empty() {
        return Some(KafkaTopicNameViolation::Empty);
    }
    if value.trim() != value {
        return Some(KafkaTopicNameViolation::Padded);
    }
    if value.len() > MAX_KAFKA_TOPIC_NAME_LEN {
        return Some(KafkaTopicNameViolation::TooLong);
    }
    // Kafka's own `Topic.validate` special-cases these two names as invalid even though every
    // byte in them is otherwise legal (`.` is itself an allowed character).
    if value == "." || value == ".." {
        return Some(KafkaTopicNameViolation::DotName);
    }
    value
        .bytes()
        .find(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')))
        .map(KafkaTopicNameViolation::IllegalByte)
}

/// The prefix of a shadow stream's name. Iggy names the stream that shadows stream `<stream>` of
/// datacenter `<origin>` `__shadow_<origin>_<stream>` (`core/metadata/src/stm/shadow.rs`).
const SHADOW_STREAM_PREFIX: &str = "__shadow_";

/// The longest datacenter name that Iggy takes as `[geo] origin`.
const MAX_ORIGIN_LEN: usize = 32;

/// Whether `origin` is a datacenter name that Iggy takes as `[geo] origin`: a lowercase letter,
/// then lowercase letters, digits and `-`, at most 32 bytes, not ending with `-`. Such a name holds
/// no `.` and no `_`, so a shadow's Kafka name and its stream's name both split at the first one.
fn is_origin_name(origin: &str) -> bool {
    origin.len() <= MAX_ORIGIN_LEN
        && origin.starts_with(|c: char| c.is_ascii_lowercase())
        && !origin.ends_with('-')
        && origin
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// The name of the stream that shadows stream `stream` of datacenter `origin`.
#[must_use]
pub fn shadow_stream_name(origin: &str, stream: &str) -> String {
    format!("{SHADOW_STREAM_PREFIX}{origin}_{stream}")
}

/// The datacenter and the stream that the stream named `name` shadows, or `None` for a stream
/// that shadows nothing.
#[must_use]
pub fn split_shadow_stream(name: &str) -> Option<(&str, &str)> {
    let (origin, stream) = name.strip_prefix(SHADOW_STREAM_PREFIX)?.split_once('_')?;
    (is_origin_name(origin) && !stream.is_empty()).then_some((origin, stream))
}

/// The datacenter and the Kafka name in `<origin>.<kafka topic>`, or `None` for a name of
/// another form.
fn split_shadow_kafka_topic(kafka_topic: &str) -> Option<(&str, &str)> {
    let (origin, topic) = kafka_topic.split_once('.')?;
    (is_origin_name(origin) && is_valid_kafka_topic_name(topic)).then_some((origin, topic))
}

/// The Iggy stream and topic of one Kafka topic name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IggyNames<'a> {
    pub stream: Cow<'a, str>,
    pub topic: &'a str,
    /// The datacenter whose geo topic the name shadows. A shadow is the read-only copy of
    /// another datacenter's geo topic.
    pub origin: Option<&'a str>,
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
    /// `validate_kafka_topic_name`); if two override entries are not injective (see
    /// [`resolve`](Self::resolve)); if `default_stream` with `.meta` appended, the stream that
    /// holds the cluster id, exceeds `MAX_IDENTIFIER_LEN`; or if an override targets that
    /// stream.
    pub fn new(
        default_stream: String,
        topics: HashMap<String, TopicOverride>,
    ) -> Result<Self, BridgeError> {
        validate_identifier_name("topic mapping's default_stream", &default_stream)?;
        // The gateway keeps its cluster id in this stream. It is checked here so a default
        // stream too long to name it fails at config load, not at the first start against Iggy.
        let meta_stream = cluster_id_stream(&default_stream);
        validate_identifier_name(
            "topic mapping's default_stream with the `.meta` suffix of its cluster id stream",
            &meta_stream,
        )?;

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

            // A Kafka client could then read or delete the topic that holds the cluster id.
            if over.stream == meta_stream {
                return Err(BridgeError::InvalidConfig(format!(
                    "topic mapping override for '{kafka_topic}' targets Iggy stream '{}', which \
                     holds this gateway's cluster id; target another stream",
                    over.stream
                )));
            }

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

    /// Every configured `(kafka_topic, override)` pair, for callers that need to enumerate the
    /// mapping rather than resolve one name through it (`Metadata`'s "list all topics" case).
    pub fn overrides(&self) -> impl Iterator<Item = (&str, &TopicOverride)> {
        self.topics.iter().map(|(k, v)| (k.as_str(), v))
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

    /// The Kafka name that lists Iggy topic `topic` of stream `stream`, by the rules of
    /// `IggyBridge::list_kafka_topics`: an override's key for its target, else the topic's own name
    /// in the default stream when no override claims that name and Kafka accepts it. `None` for a
    /// topic that no Kafka name lists.
    #[must_use]
    pub fn kafka_topic_of<'a>(&'a self, stream: &str, topic: &'a str) -> Option<&'a str> {
        if let Some((kafka_topic, _)) = self
            .topics
            .iter()
            .find(|(_, over)| over.stream == stream && over.topic == topic)
        {
            return Some(kafka_topic);
        }
        (stream == self.default_stream
            && !self.topics.contains_key(topic)
            && is_valid_kafka_topic_name(topic))
        .then_some(topic)
    }

    /// Whether `kafka_topic` is no override's key yet resolves to the Iggy stream and topic an
    /// override targets, so writing through it would land in that override's data.
    #[must_use]
    pub fn aliases_an_override(&self, kafka_topic: &str) -> bool {
        if self.topics.contains_key(kafka_topic) {
            return false;
        }
        let target = self.resolve(kafka_topic);
        self.topics
            .values()
            .any(|over| (over.stream.as_str(), over.topic.as_str()) == target)
    }

    /// Resolves `kafka_topic` as [`Self::resolve`] does, except for the name of a shadow. A name
    /// `<origin>.<kafka topic>` that no override claims resolves to the shadow of that Kafka
    /// topic in datacenter `origin`, once `is_shadow_stream` holds the shadow's stream.
    /// `MirrorMaker 2` names the topics that it copies from another cluster the same way.
    #[must_use]
    pub fn resolve_name<'a>(
        &'a self,
        kafka_topic: &'a str,
        is_shadow_stream: impl Fn(&str) -> bool,
    ) -> IggyNames<'a> {
        if !self.topics.contains_key(kafka_topic)
            && let Some((origin, origin_topic)) = split_shadow_kafka_topic(kafka_topic)
        {
            let (stream, topic) = self.resolve(origin_topic);
            let shadow = shadow_stream_name(origin, stream);
            if is_shadow_stream(&shadow) {
                return IggyNames {
                    stream: Cow::Owned(shadow),
                    topic,
                    origin: Some(origin),
                };
            }
        }
        let (stream, topic) = self.resolve(kafka_topic);
        IggyNames {
            stream: Cow::Borrowed(stream),
            topic,
            origin: None,
        }
    }

    /// Whether `kafka_topic` has the form of a shadow's name that no override claims. It names a
    /// shadow once the shadow's stream exists.
    #[must_use]
    pub fn may_name_a_shadow(&self, kafka_topic: &str) -> bool {
        !self.topics.contains_key(kafka_topic) && split_shadow_kafka_topic(kafka_topic).is_some()
    }

    /// The Kafka name of topic `topic` of the shadow stream `stream`: the datacenter, a dot, and
    /// the Kafka name of the geo topic that it shadows. `None` when `stream` shadows nothing, when
    /// no Kafka name lists the geo topic, when an override claims the name, or when the name breaks
    /// Kafka's rules.
    #[must_use]
    pub fn shadow_kafka_topic(&self, stream: &str, topic: &str) -> Option<String> {
        let (origin, origin_stream) = split_shadow_stream(stream)?;
        let kafka_topic = self.kafka_topic_of(origin_stream, topic)?;
        let name = format!("{origin}.{kafka_topic}");
        (is_valid_kafka_topic_name(&name) && !self.topics.contains_key(&name)).then_some(name)
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
    fn given_a_default_stream_whose_meta_stream_is_too_long_should_reject_it() {
        // A 250-byte name leaves its `.meta` stream at the 255-byte limit; one byte more crosses
        // it.
        let longest = "s".repeat(MAX_IDENTIFIER_LEN - ".meta".len());
        assert!(TopicMapping::new(longest.clone(), HashMap::new()).is_ok());
        let err = TopicMapping::new(format!("{longest}s"), HashMap::new()).unwrap_err();
        assert!(matches!(err, BridgeError::InvalidConfig(_)), "got {err:?}");
    }

    #[test]
    fn given_an_override_into_the_cluster_id_stream_should_reject_it() {
        let toml = r#"
            default_stream = "kafka"

            [topics.ids]
            stream = "kafka.meta"
            topic = "cluster_id"
        "#;
        let err = TopicMapping::from_toml_str(toml).unwrap_err();
        assert!(
            matches!(&err, BridgeError::InvalidConfig(message) if message.contains("cluster id")),
            "got {err:?}"
        );
    }

    #[test]
    fn given_an_override_into_another_meta_stream_should_accept_it() {
        let toml = r#"
            default_stream = "kafka"

            [topics.ids]
            stream = "other.meta"
            topic = "ids"
        "#;
        assert!(TopicMapping::from_toml_str(toml).is_ok());
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

    #[test]
    fn is_valid_kafka_topic_name_agrees_with_validate_kafka_topic_name() {
        let too_long = "a".repeat(MAX_KAFKA_TOPIC_NAME_LEN + 1);
        for name in [
            "orders",
            "Order.Events_2024-v2",
            "",
            " orders",
            ".",
            "..",
            "has a space",
            "orders/2024",
            too_long.as_str(),
        ] {
            assert_eq!(
                is_valid_kafka_topic_name(name),
                validate_kafka_topic_name("kafka_topic", name).is_ok(),
                "{name:?}"
            );
        }
    }

    #[test]
    fn given_an_override_key_or_an_unrelated_name_should_not_report_an_alias() {
        let mut topics = HashMap::new();
        topics.insert(
            "orders".to_string(),
            TopicOverride {
                stream: "billing".to_string(),
                topic: "orders_v2".to_string(),
            },
        );
        let mapping = mapping_with("kafka", topics);
        assert!(!mapping.aliases_an_override("orders"));
        assert!(!mapping.aliases_an_override("orders_v2"));
        assert!(!mapping.aliases_an_override("payments"));
    }

    /// `new` refuses this shape, so the mapping is built by hand: the predicate must not rely on
    /// that check to keep a name from writing into an override's topic.
    #[test]
    fn given_a_name_resolving_onto_an_override_target_should_report_an_alias() {
        let mut topics = HashMap::new();
        topics.insert(
            "orders".to_string(),
            TopicOverride {
                stream: "kafka".to_string(),
                topic: "orders_v2".to_string(),
            },
        );
        let mapping = TopicMapping {
            default_stream: "kafka".to_string(),
            topics,
        };
        assert!(mapping.aliases_an_override("orders_v2"));
        assert!(!mapping.aliases_an_override("orders"));
    }

    #[test]
    fn given_iggy_topics_should_name_the_kafka_topic_that_lists_each() {
        let mut topics = HashMap::new();
        topics.insert(
            "orders".to_string(),
            TopicOverride {
                stream: "billing".to_string(),
                topic: "orders_v2".to_string(),
            },
        );
        topics.insert(
            "audit".to_string(),
            TopicOverride {
                stream: "kafka".to_string(),
                topic: "audit_log".to_string(),
            },
        );
        topics.insert(
            "audit_log".to_string(),
            TopicOverride {
                stream: "logs".to_string(),
                topic: "audit_log".to_string(),
            },
        );
        let mapping = mapping_with("kafka", topics);

        assert_eq!(
            mapping.kafka_topic_of("kafka", "payments"),
            Some("payments")
        );
        assert_eq!(
            mapping.kafka_topic_of("billing", "orders_v2"),
            Some("orders")
        );
        assert_eq!(mapping.kafka_topic_of("kafka", "audit_log"), Some("audit"));
        assert_eq!(
            mapping.kafka_topic_of("logs", "audit_log"),
            Some("audit_log")
        );
        assert_eq!(
            mapping.kafka_topic_of("kafka", "orders"),
            None,
            "an override's key in the default stream is not its topic"
        );
        assert_eq!(mapping.kafka_topic_of("billing", "payments"), None);
        assert_eq!(mapping.kafka_topic_of("kafka", "not kafka"), None);
    }

    /// `orders` of the default stream `kafka`, and `payments` mapped to `billing/payments_v2`.
    fn geo_mapping() -> TopicMapping {
        let mut topics = HashMap::new();
        topics.insert(
            "payments".to_string(),
            TopicOverride {
                stream: "billing".to_string(),
                topic: "payments_v2".to_string(),
            },
        );
        mapping_with("kafka", topics)
    }

    #[test]
    fn given_shadow_stream_names_should_split_them_into_the_origin_and_its_stream() {
        assert_eq!(
            split_shadow_stream("__shadow_dc-a_kafka"),
            Some(("dc-a", "kafka"))
        );
        assert_eq!(
            split_shadow_stream("__shadow_dc-a_my_stream"),
            Some(("dc-a", "my_stream")),
            "an origin holds no `_`, so the stream keeps every later one"
        );
        assert_eq!(
            split_shadow_stream(&shadow_stream_name("eu-west-2", "kafka")),
            Some(("eu-west-2", "kafka"))
        );
        for name in [
            "kafka",
            "__shadow_",
            "__shadow_dc-a",
            "__shadow_dc-a_",
            "__shadow_DC_kafka",
            "__shadow_1dc_kafka",
            "__shadow_dc-_kafka",
        ] {
            assert_eq!(split_shadow_stream(name), None, "{name}");
        }
    }

    #[test]
    fn given_the_name_of_a_shadow_when_its_stream_exists_should_resolve_to_the_shadow() {
        let mapping = geo_mapping();
        let shadows = ["__shadow_dc-a_kafka", "__shadow_dc-a_billing"];
        let exists = |stream: &str| shadows.contains(&stream);

        assert_eq!(
            mapping.resolve_name("dc-a.orders", exists),
            IggyNames {
                stream: Cow::Borrowed("__shadow_dc-a_kafka"),
                topic: "orders",
                origin: Some("dc-a"),
            }
        );
        assert_eq!(
            mapping.resolve_name("dc-a.payments", exists),
            IggyNames {
                stream: Cow::Borrowed("__shadow_dc-a_billing"),
                topic: "payments_v2",
                origin: Some("dc-a"),
            },
            "the shadow of an override's topic resolves through the override"
        );
        assert_eq!(
            mapping.resolve_name("orders", exists),
            IggyNames {
                stream: Cow::Borrowed("kafka"),
                topic: "orders",
                origin: None,
            }
        );
    }

    #[test]
    fn given_the_name_of_a_shadow_when_its_stream_is_missing_should_resolve_to_a_local_topic() {
        let mapping = geo_mapping();
        let names = mapping.resolve_name("dc-b.orders", |stream| stream == "__shadow_dc-a_kafka");

        assert_eq!(names.stream, "kafka");
        assert_eq!(names.topic, "dc-b.orders");
        assert_eq!(names.origin, None);
        assert!(mapping.may_name_a_shadow("dc-b.orders"));
        assert!(!mapping.may_name_a_shadow("orders"));
        assert!(
            !mapping.may_name_a_shadow("Dc.orders"),
            "no datacenter has an upper case name"
        );
    }

    #[test]
    fn given_an_override_whose_key_has_the_form_of_a_shadow_should_resolve_through_it() {
        let mut topics = HashMap::new();
        topics.insert(
            "dc-a.orders".to_string(),
            TopicOverride {
                stream: "imports".to_string(),
                topic: "orders".to_string(),
            },
        );
        let mapping = mapping_with("kafka", topics);
        let names = mapping.resolve_name("dc-a.orders", |_| true);

        assert_eq!(names.stream, "imports");
        assert_eq!(names.origin, None);
        assert!(!mapping.may_name_a_shadow("dc-a.orders"));
        assert_eq!(
            mapping.shadow_kafka_topic("__shadow_dc-a_kafka", "orders"),
            None,
            "the override claims the name, so the shadow is not listed under it"
        );
    }

    #[test]
    fn given_a_shadow_topic_should_list_it_as_its_origin_and_the_kafka_name_of_its_geo_topic() {
        let mapping = geo_mapping();

        assert_eq!(
            mapping
                .shadow_kafka_topic("__shadow_dc-a_kafka", "orders")
                .as_deref(),
            Some("dc-a.orders")
        );
        assert_eq!(
            mapping
                .shadow_kafka_topic("__shadow_dc-a_billing", "payments_v2")
                .as_deref(),
            Some("dc-a.payments")
        );
        assert_eq!(
            mapping.shadow_kafka_topic("kafka", "orders"),
            None,
            "a stream that shadows nothing"
        );
        assert_eq!(
            mapping.shadow_kafka_topic("__shadow_dc-a_other", "orders"),
            None,
            "no Kafka name lists `other/orders`"
        );
        let long = "t".repeat(MAX_KAFKA_TOPIC_NAME_LEN);
        assert_eq!(
            mapping.shadow_kafka_topic("__shadow_dc-a_kafka", &long),
            None,
            "a name past Kafka's limit"
        );
    }

    #[test]
    fn given_a_listed_shadow_name_should_resolve_back_to_its_stream_and_topic() {
        let mapping = geo_mapping();
        for (stream, topic) in [
            ("__shadow_dc-a_kafka", "orders"),
            ("__shadow_dc-a_kafka", "dc-b.orders"),
            ("__shadow_eu-1_billing", "payments_v2"),
        ] {
            let kafka_topic = mapping
                .shadow_kafka_topic(stream, topic)
                .expect("a listed shadow");
            let names = mapping.resolve_name(&kafka_topic, |shadow| shadow == stream);

            assert_eq!((names.stream.as_ref(), names.topic), (stream, topic));
        }
    }
}
