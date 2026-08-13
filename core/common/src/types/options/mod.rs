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

//! Key-value options attached to streams, topics, and users at creation.
//!
//! Options reuse the typed [`HeaderKey`] / [`HeaderValue`] machinery from
//! message user headers. The map MUST stay a `BTreeMap`: metadata snapshots
//! require deterministic ordering across replicas, and a hash map would make
//! snapshot bytes diverge per replica.

use std::collections::BTreeMap;
use std::str::FromStr;

use iggy_binary_protocol::{WireOptions, WireUserHeaderEntry};
use serde::{Deserialize, Serialize};

use crate::types::compression::compression_algorithm::CompressionAlgorithm;
use crate::types::message::{HeaderKey, HeaderKind, HeaderValue};
use crate::{IggyByteSize, IggyError, IggyExpiry, MaxTopicSize};

/// A single resolved option entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionValue {
    /// The effective value, resolved by the admitting primary at creation.
    pub value: HeaderValue,
    /// Whether the client explicitly sent this key. Derived entries
    /// (`explicit == false`) were filled from server defaults at admission
    /// and would have resolved differently under other server configs.
    pub explicit: bool,
}

impl OptionValue {
    #[must_use]
    pub fn explicit(value: HeaderValue) -> Self {
        Self {
            value,
            explicit: true,
        }
    }

    #[must_use]
    pub fn derived(value: HeaderValue) -> Self {
        Self {
            value,
            explicit: false,
        }
    }
}

/// Options attached to a stream, topic, or user, keyed by option name.
pub type ResourceOptions = BTreeMap<HeaderKey, OptionValue>;

/// JSON form for [`ResourceOptions`].
///
/// [`HeaderKey`] serializes as a struct (`kind` + base64 `value`), and JSON
/// object keys must be strings, so the derived impl fails outright with
/// "key must be a string" the moment a map is non-empty. Every HTTP response
/// carrying options goes through here instead, rendering the key as its plain
/// text. Binary transports are unaffected: they never touch serde.
pub mod resource_options_json {
    use super::{HeaderKey, OptionValue, ResourceOptions};
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;
    use std::str::FromStr;

    /// # Errors
    ///
    /// Propagates the serializer's own errors.
    pub fn serialize<S: Serializer>(
        options: &ResourceOptions,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let readable: BTreeMap<String, &OptionValue> = options
            .iter()
            .map(|(key, value)| (String::from_utf8_lossy(key.as_bytes()).into_owned(), value))
            .collect();
        readable.serialize(serializer)
    }

    /// # Errors
    ///
    /// Returns a deserializer error when a key is not a valid option name.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ResourceOptions, D::Error> {
        let readable = BTreeMap::<String, OptionValue>::deserialize(deserializer)?;
        readable
            .into_iter()
            .map(|(key, value)| {
                HeaderKey::from_str(&key)
                    .map(|key| (key, value))
                    .map_err(D::Error::custom)
            })
            .collect()
    }
}

/// Topic option catalog: the keys `CreateTopic` accepts.
pub mod topic_option_keys {
    /// Compression algorithm name (`none`, `gzip`).
    pub const COMPRESSION_ALGORITHM: &str = "compression_algorithm";
    /// Message expiry: `Uint64` micros or a humantime string (`7 days`).
    pub const MESSAGE_EXPIRY: &str = "message_expiry";
    /// Topic size cap: `Uint64` bytes or a byte-size string (`1 GiB`).
    pub const MAX_TOPIC_SIZE: &str = "max_topic_size";
    /// Segment size: `Uint64` bytes or a byte-size string (`128 MiB`).
    /// Bounded: a 512-byte multiple within
    /// [`super::MIN_TOPIC_SEGMENT_SIZE`]..=the server's segment ceiling.
    pub const SEGMENT_SIZE: &str = "segment_size";
    /// Whether writes to this topic's partitions fsync: `Bool`, or the
    /// strings `true` / `false`.
    pub const ENFORCE_FSYNC: &str = "enforce_fsync";
    /// Flush the journal once it holds this many messages: `Uint32`.
    /// Must be non-zero.
    pub const MESSAGES_REQUIRED_TO_SAVE: &str = "messages_required_to_save";
    /// Flush the journal once it holds this many bytes: `Uint64` or a
    /// byte-size string. Paired with [`Self::MESSAGES_REQUIRED_TO_SAVE`];
    /// whichever threshold trips first flushes.
    pub const SIZE_OF_MESSAGES_REQUIRED_TO_SAVE: &str = "size_of_messages_required_to_save";
    /// Reserve the segment's bytes up front on a filesystem that supports it:
    /// `Bool`, or the strings `true` / `false`. Pairs with
    /// [`Self::SEGMENT_SIZE`] -- preallocation reserves exactly that much, so
    /// the two only make sense decided together.
    pub const PREALLOCATE_SEGMENTS: &str = "preallocate_segments";
    /// Copies of the topic's data the cluster should keep: `Uint8` or a
    /// decimal string. Stored and echoed back, but nothing acts on it yet --
    /// replica placement is a cluster-level concern the server does not derive
    /// from topics. It lives here rather than as a command field precisely
    /// because of that: an option costs nothing on the wire when unset,
    /// whereas the fixed byte it replaced rode every `UpdateTopic`.
    pub const REPLICATION_FACTOR: &str = "replication_factor";
}

/// Values an absent topic option resolves to at admission.
///
/// These are the knobs' single source of truth: they used to live in
/// `config.toml` (`[system.topic]`, `[system.partition]`, `[system.segment]`),
/// which meant every one of them had two homes and an operator could not tell
/// which won. A topic carries whatever it was created with; anything the
/// client did not send resolves to the constant here and is persisted as a
/// derived entry, so the effective value is always visible on `GetTopic`.
///
/// Each value matches what the shipped `config.toml` carried, so removing the
/// keys changed no behavior for a topic created without options.
pub const DEFAULT_PARTITIONS_COUNT: u32 = 1;
/// `MaxTopicSize::Unlimited` (was `[system.topic] max_size = "unlimited"`).
pub const DEFAULT_MAX_TOPIC_SIZE: u64 = u64::MAX;
/// `IggyExpiry::NeverExpire` (was `[system.topic] message_expiry = "none"`).
pub const DEFAULT_MESSAGE_EXPIRY: u64 = u64::MAX;
/// 1 GiB (was `[system.segment] size = "1 GiB"`).
pub const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;
/// Was `[system.partition] enforce_fsync = false`.
pub const DEFAULT_ENFORCE_FSYNC: bool = false;
/// Was `[system.partition] messages_required_to_save = 1024`.
pub const DEFAULT_MESSAGES_REQUIRED_TO_SAVE: u32 = 1024;
/// Opt-in, unlike the `[system.segment] preallocate = true` this replaced.
///
/// That default was never actually in force: the reservation ran through
/// `compio::spawn_blocking`, which panics the shard because shard executors
/// disable the blocking pool, so any deployment that worked at all had
/// preallocation off. With the call fixed to run inline it reserves real
/// extents, and `FALLOC_FL_KEEP_SIZE` against the 1 GiB default segment size
/// means 1 GiB of disk per partition the moment it is created -- a full test
/// sweep reserved 393 GB before this was flipped. A topic that wants the
/// latency benefit asks for it with `preallocate_segments`.
pub const DEFAULT_PREALLOCATE_SEGMENTS: bool = false;
/// 1 MiB (was `[system.partition] size_of_messages_required_to_save`).
pub const DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE: u64 = 1024 * 1024;
/// One copy. Was the `replication_factor` byte on the create/update commands,
/// which every topic reported as `1` because nothing ever stored it.
pub const DEFAULT_REPLICATION_FACTOR: u8 = 1;

/// Every runtime knob at its default, for a partition built with no resolved
/// topic options (simulator, unit tests).
impl Default for TopicRuntimeDefaults {
    fn default() -> Self {
        Self {
            segment_size: IggyByteSize::from(DEFAULT_SEGMENT_SIZE),
            enforce_fsync: DEFAULT_ENFORCE_FSYNC,
            messages_required_to_save: DEFAULT_MESSAGES_REQUIRED_TO_SAVE,
            size_of_messages_required_to_save: IggyByteSize::from(
                DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
            ),
            preallocate_segments: DEFAULT_PREALLOCATE_SEGMENTS,
        }
    }
}

/// Smallest per-topic segment size. Segments far below the shipped default
/// explode the per-partition segment count, which state transfer bounds via
/// its manifest entry cap; a partition that crosses it becomes unservable
/// for transfer, so the floor is enforced at admission.
pub const MIN_TOPIC_SEGMENT_SIZE: u64 = 1024 * 1024;

/// Validate an explicit per-topic `segment_size` against its bounds.
///
/// `ceiling` is node-derived: the smaller of the global segment maximum and
/// the state-transfer artifact budget minus one bus frame (a segment may
/// close one whole batch past its cap; an artifact ceiling below that
/// refuses a legal segment and livelocks the partition's rejoin).
///
/// # Errors
///
/// Returns `IggyError::InvalidOptionValue("segment_size")` when the value is
/// below [`MIN_TOPIC_SEGMENT_SIZE`], above `ceiling`, or not a 512-byte
/// multiple.
pub fn validate_topic_segment_size(size_bytes: u64, ceiling: u64) -> Result<(), IggyError> {
    if size_bytes < MIN_TOPIC_SEGMENT_SIZE
        || size_bytes > ceiling
        || !size_bytes.is_multiple_of(512)
    {
        return Err(IggyError::InvalidOptionValue(
            topic_option_keys::SEGMENT_SIZE.to_string(),
        ));
    }
    Ok(())
}

/// Resource whose option catalog `DescribeOptions` serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsScope {
    Topic,
    Stream,
    User,
}

impl OptionsScope {
    #[must_use]
    pub fn as_code(&self) -> u8 {
        match self {
            Self::Topic => 1,
            Self::Stream => 2,
            Self::User => 3,
        }
    }

    /// # Errors
    ///
    /// Returns `IggyError::InvalidCommand` for an unknown scope code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(Self::Topic),
            2 => Ok(Self::Stream),
            3 => Ok(Self::User),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl std::fmt::Display for OptionsScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Topic => write!(f, "topic"),
            Self::Stream => write!(f, "stream"),
            Self::User => write!(f, "user"),
        }
    }
}

impl FromStr for OptionsScope {
    type Err = IggyError;

    fn from_str(scope: &str) -> Result<Self, Self::Err> {
        match scope {
            "topic" => Ok(Self::Topic),
            "stream" => Ok(Self::Stream),
            "user" => Ok(Self::User),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

/// One entry of a resource's option catalog, as served by `DescribeOptions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionSpec {
    /// Option key accepted by the create command.
    pub key: String,
    /// Canonical kind the server persists the value under. A `String` value
    /// parsed via the server-config rules is always accepted too.
    pub kind: HeaderKind,
    /// This node's current resolved default in the canonical kind's
    /// encoding; empty when the key has no server default.
    #[serde(default)]
    pub default_value: Vec<u8>,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}

/// Every key `CreateTopic` currently accepts. Unknown keys are rejected at
/// the edge, never skipped: a silently ignored knob would hand the client
/// server defaults without it ever learning.
pub const TOPIC_OPTION_KEYS: &[&str] = &[
    topic_option_keys::COMPRESSION_ALGORITHM,
    topic_option_keys::MESSAGE_EXPIRY,
    topic_option_keys::MAX_TOPIC_SIZE,
    topic_option_keys::SEGMENT_SIZE,
    topic_option_keys::ENFORCE_FSYNC,
    topic_option_keys::MESSAGES_REQUIRED_TO_SAVE,
    topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
    topic_option_keys::PREALLOCATE_SEGMENTS,
    topic_option_keys::REPLICATION_FACTOR,
];

/// The subset of [`TOPIC_OPTION_KEYS`] an `UpdateTopic` options block may
/// carry.
///
/// Deliberately narrow. Two separate reasons keep a key out:
///
/// * `compression_algorithm`, `message_expiry` and `max_topic_size` have
///   dedicated fixed fields on the update layout. Accepting them in the block
///   too would mean two sources for one setting and a precedence rule nobody
///   can guess, so sending them here is an error rather than a silent
///   last-writer-wins.
/// * The partition runtime knobs (`segment_size`, `enforce_fsync`, both flush
///   thresholds, `preallocate_segments`) are pushed to partitions when the
///   topic is built. Nothing re-pushes them on update, so accepting one would
///   store a value the partitions never see -- a knob that reads as applied
///   and is not. They stay create-only until that propagation exists.
///
/// `replication_factor` is safe precisely because it is inert: it is stored
/// and echoed, and no partition derives behaviour from it.
pub const UPDATABLE_TOPIC_OPTION_KEYS: &[&str] = &[topic_option_keys::REPLICATION_FACTOR];

/// Keys an `UpdateStream` options block may carry. Empty because streams have
/// no catalog keys yet: the block exists so the first one costs a catalog
/// entry instead of another wire change, and until then every key is rejected
/// by name rather than stored and ignored.
pub const UPDATABLE_STREAM_OPTION_KEYS: &[&str] = &[];

/// Keys an `UpdateUser` options block may carry. Empty for the same reason as
/// [`UPDATABLE_STREAM_OPTION_KEYS`].
pub const UPDATABLE_USER_OPTION_KEYS: &[&str] = &[];

/// Build an options map from string key-values, all marked explicit.
///
/// Shared by the update-options types: their keys are all client-sent, so none
/// of the derived-provenance handling that create needs applies. Callers that
/// also have typed fields insert those afterwards, so a typed value wins on
/// collision and keeps its canonical kind.
///
/// # Panics
///
/// Panics when a key or value violates the header-field bounds (1..=255
/// bytes); the wire validator rejects the same inputs immediately after.
#[must_use]
fn raw_options_map(raw: &BTreeMap<String, String>) -> ResourceOptions {
    let mut options = ResourceOptions::new();
    for (key, value) in raw {
        options.insert(
            HeaderKey::from_str(key).expect("raw option key fits a header key"),
            OptionValue::explicit(
                HeaderValue::try_from(value.as_str())
                    .expect("raw option value fits a header value"),
            ),
        );
    }
    options
}

/// Encode string key-values into an options block, all marked explicit.
///
/// # Panics
///
/// See [`raw_options_map`].
#[must_use]
fn raw_options_to_wire(raw: &BTreeMap<String, String>) -> WireOptions {
    crate::wire_conversions::resource_options_to_wire(&raw_options_map(raw), false)
}

/// The options an `UpdateStream` may carry.
///
/// Absent keys leave the stream's existing values alone -- an update patches
/// the option map, it does not replace it. See [`TopicUpdateOptions`] for why.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamUpdateOptions {
    /// Keys sent as `String` values. Checked against
    /// [`UPDATABLE_STREAM_OPTION_KEYS`] server-side.
    pub raw: BTreeMap<String, String>,
}

impl StreamUpdateOptions {
    /// Encode the present keys into an options block.
    ///
    /// # Panics
    ///
    /// See [`raw_options_to_wire`].
    #[must_use]
    pub fn to_wire(&self) -> WireOptions {
        raw_options_to_wire(&self.raw)
    }
}

/// The options an `UpdateUser` may carry. Same patch semantics as
/// [`StreamUpdateOptions`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserUpdateOptions {
    /// Keys sent as `String` values. Checked against
    /// [`UPDATABLE_USER_OPTION_KEYS`] server-side.
    pub raw: BTreeMap<String, String>,
}

impl UserUpdateOptions {
    /// Encode the present keys into an options block.
    ///
    /// # Panics
    ///
    /// See [`raw_options_to_wire`].
    #[must_use]
    pub fn to_wire(&self) -> WireOptions {
        raw_options_to_wire(&self.raw)
    }
}

/// This node's configured fallbacks for the runtime knobs, used to fill the
/// derived-options block for keys a client did not send.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicRuntimeDefaults {
    pub segment_size: IggyByteSize,
    pub enforce_fsync: bool,
    pub messages_required_to_save: u32,
    pub size_of_messages_required_to_save: IggyByteSize,
    pub preallocate_segments: bool,
}

/// A topic's resolved runtime knobs, as carried from the metadata plane to
/// each of its partitions. `None` means "keep the shard-wide configured
/// value": topics created without an options block (simulator, unit tests)
/// have no resolved values to carry.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TopicRuntimeOptions {
    pub segment_size: Option<IggyByteSize>,
    pub enforce_fsync: Option<bool>,
    pub messages_required_to_save: Option<u32>,
    pub size_of_messages_required_to_save: Option<IggyByteSize>,
    pub preallocate_segments: Option<bool>,
}

impl TopicRuntimeOptions {
    /// Derive the runtime knobs from a topic's persisted options map.
    /// A map written by a build that knew an unknown key yields `None` for
    /// every knob rather than failing the partition build: the partition then
    /// runs on shard-wide config, which is the same degradation an older
    /// build already accepts for a key it cannot interpret.
    #[must_use]
    pub fn from_resource_options(options: &ResourceOptions) -> Self {
        let Ok(parsed) = TopicCreateOptions::from_resource_options(options) else {
            return Self::default();
        };
        Self {
            segment_size: parsed.segment_size,
            enforce_fsync: parsed.enforce_fsync,
            messages_required_to_save: parsed.messages_required_to_save,
            size_of_messages_required_to_save: parsed.size_of_messages_required_to_save,
            preallocate_segments: parsed.preallocate_segments,
        }
    }
}

/// The options an `UpdateTopic` may carry, as a type that cannot express a
/// key the update path rejects (see [`UPDATABLE_TOPIC_OPTION_KEYS`]).
///
/// Separate from [`TopicCreateOptions`] on purpose: reusing that struct would
/// let a caller set `segment_size` on an update, encode it, and only learn at
/// the server that it was refused. Absent keys leave the topic's existing
/// value alone -- an update patches the option map, it does not replace it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicUpdateOptions {
    /// Copies the cluster should keep. `0` is rejected.
    pub replication_factor: Option<u8>,
    /// Keys sent as `String` values, for setting a key this build does not
    /// know yet. Still checked against the updatable set server-side.
    pub raw: BTreeMap<String, String>,
}

impl TopicUpdateOptions {
    /// Encode the present keys into an options block, in canonical kinds.
    ///
    /// # Panics
    ///
    /// Panics when a `raw` key or value violates the header-field bounds
    /// (1..=255 bytes), matching [`TopicCreateOptions::to_wire`].
    #[must_use]
    pub fn to_wire(&self) -> WireOptions {
        let mut options = raw_options_map(&self.raw);
        if let Some(replication_factor) = self.replication_factor {
            // Inserted after the raw entries so a typed field wins on
            // collision, and in its canonical kind rather than as a string.
            options.insert(
                HeaderKey::from_str(topic_option_keys::REPLICATION_FACTOR)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(replication_factor)),
            );
        }
        crate::wire_conversions::resource_options_to_wire(&options, false)
    }
}

/// Typed view of the known topic option keys, parsed from a wire block.
///
/// `None` means the key was absent, which always means "resolve from server
/// defaults at admission". Values that parse to their type's `ServerDefault`
/// sentinel are normalized to `None` for the same reason.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicCreateOptions {
    /// Partitions to allocate. NOT an option key: it fills the `CreateTopic`
    /// command's own fixed field, because it is an argument to the operation
    /// rather than a property of the topic (admission consumes it to compute
    /// assignments, and a stored count would go stale on the first
    /// `CreatePartitions`). Carried here so callers pass one bundle.
    /// `None` means [`DEFAULT_PARTITIONS_COUNT`].
    pub partitions_count: Option<u32>,
    pub compression_algorithm: Option<CompressionAlgorithm>,
    pub message_expiry: Option<IggyExpiry>,
    pub max_topic_size: Option<MaxTopicSize>,
    /// Per-topic segment size; `None` resolves against `[system.segment]
    /// size` at admission. `0` is normalized to `None`.
    pub segment_size: Option<IggyByteSize>,
    /// Per-topic fsync enforcement; `None` resolves against
    /// `[system.partition] enforce_fsync`.
    pub enforce_fsync: Option<bool>,
    /// Per-topic message-count flush threshold; `None` resolves against
    /// `[system.partition] messages_required_to_save`. `0` is rejected.
    pub messages_required_to_save: Option<u32>,
    /// Per-topic byte flush threshold; `None` resolves against
    /// [`DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE`].
    pub size_of_messages_required_to_save: Option<IggyByteSize>,
    /// Whether this topic's segments reserve their bytes on open; `None`
    /// resolves against [`DEFAULT_PREALLOCATE_SEGMENTS`].
    pub preallocate_segments: Option<bool>,
    /// Copies the cluster should keep; `None` resolves against
    /// [`DEFAULT_REPLICATION_FACTOR`]. Stored and echoed, not yet acted on.
    /// `0` is rejected.
    pub replication_factor: Option<u8>,
    /// Additional keys sent as `String` values, parsed server-side via the
    /// same rules as config-file values. Lets a client set a key this
    /// build's typed fields do not know yet (e.g. CLI `--set key=value`
    /// against a newer server). A typed field wins over a raw entry for the
    /// same key. Client-side only: [`Self::parse`] never populates it.
    pub raw: BTreeMap<String, String>,
}

impl TopicCreateOptions {
    /// Parse a wire options block against the topic catalog.
    ///
    /// # Errors
    ///
    /// `UnsupportedOptionKey` for a key outside [`TOPIC_OPTION_KEYS`];
    /// `InvalidOptionValue` when a value has the wrong kind or fails its
    /// type's `FromStr`.
    pub fn parse(options: &WireOptions) -> Result<Self, IggyError> {
        let mut parsed = Self::default();
        for entry in options {
            // Wire validation already enforced UTF-8 string keys.
            let key = String::from_utf8_lossy(entry.key);
            parsed.absorb(&entry, &key)?;
        }
        Ok(parsed)
    }

    /// Fold one catalog entry into `self`. Shared by [`Self::parse`] (wire
    /// block) and [`Self::from_resource_options`] (persisted map) so both
    /// enforce the identical key set, kinds, and value bounds.
    fn absorb(&mut self, entry: &WireUserHeaderEntry<'_>, key: &str) -> Result<(), IggyError> {
        let parsed = self;
        {
            match key {
                topic_option_keys::COMPRESSION_ALGORITHM => {
                    let value = parse_compression(entry, key)?;
                    parsed.compression_algorithm = Some(value);
                }
                topic_option_keys::MESSAGE_EXPIRY => {
                    let expiry = IggyExpiry::from(parse_u64_or(entry, key, IggyExpiry::from_str)?);
                    parsed.message_expiry = (expiry != IggyExpiry::ServerDefault).then_some(expiry);
                }
                topic_option_keys::MAX_TOPIC_SIZE => {
                    let size =
                        MaxTopicSize::from(parse_u64_or(entry, key, MaxTopicSize::from_str)?);
                    parsed.max_topic_size = (size != MaxTopicSize::ServerDefault).then_some(size);
                }
                topic_option_keys::SEGMENT_SIZE => {
                    let size = parse_byte_size(entry, key)?;
                    parsed.segment_size = (size != 0).then_some(IggyByteSize::from(size));
                }
                topic_option_keys::ENFORCE_FSYNC => {
                    parsed.enforce_fsync = Some(parse_bool(entry, key)?);
                }
                topic_option_keys::MESSAGES_REQUIRED_TO_SAVE => {
                    let messages = parse_u32(entry, key)?;
                    if messages == 0 {
                        return Err(IggyError::InvalidOptionValue(key.to_string()));
                    }
                    parsed.messages_required_to_save = Some(messages);
                }
                topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE => {
                    let size = parse_byte_size(entry, key)?;
                    parsed.size_of_messages_required_to_save =
                        (size != 0).then_some(IggyByteSize::from(size));
                }
                topic_option_keys::PREALLOCATE_SEGMENTS => {
                    parsed.preallocate_segments = Some(parse_bool(entry, key)?);
                }
                topic_option_keys::REPLICATION_FACTOR => {
                    let factor = parse_u8(entry, key)?;
                    if factor == 0 {
                        return Err(IggyError::InvalidOptionValue(key.to_string()));
                    }
                    parsed.replication_factor = Some(factor);
                }
                _ => return Err(IggyError::UnsupportedOptionKey(key.to_string())),
            }
        }
        Ok(())
    }

    /// Encode the present keys into a client options block, in canonical
    /// kinds. The client-side counterpart of [`Self::parse`].
    ///
    /// # Panics
    ///
    /// Panics when a `raw` key or value violates the header-field bounds
    /// (1..=255 bytes); CLI-grade inputs are expected to be validated by the
    /// wire layer right after encoding anyway.
    #[must_use]
    pub fn to_wire(&self) -> WireOptions {
        let mut options = ResourceOptions::new();
        for (key, value) in &self.raw {
            options.insert(
                HeaderKey::from_str(key).expect("raw option key fits a header key"),
                OptionValue::explicit(
                    HeaderValue::try_from(value.as_str())
                        .expect("raw option value fits a header value"),
                ),
            );
        }
        if let Some(compression_algorithm) = self.compression_algorithm {
            options.insert(
                HeaderKey::from_str(topic_option_keys::COMPRESSION_ALGORITHM)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(
                    HeaderValue::try_from(compression_algorithm.to_string().as_str())
                        .expect("compression name fits a header value"),
                ),
            );
        }
        if let Some(message_expiry) = self.message_expiry {
            options.insert(
                HeaderKey::from_str(topic_option_keys::MESSAGE_EXPIRY)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(u64::from(message_expiry))),
            );
        }
        if let Some(max_topic_size) = self.max_topic_size {
            options.insert(
                HeaderKey::from_str(topic_option_keys::MAX_TOPIC_SIZE)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(u64::from(max_topic_size))),
            );
        }
        if let Some(segment_size) = self.segment_size {
            options.insert(
                HeaderKey::from_str(topic_option_keys::SEGMENT_SIZE)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(segment_size.as_bytes_u64())),
            );
        }
        if let Some(enforce_fsync) = self.enforce_fsync {
            options.insert(
                HeaderKey::from_str(topic_option_keys::ENFORCE_FSYNC)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(enforce_fsync)),
            );
        }
        if let Some(messages_required_to_save) = self.messages_required_to_save {
            options.insert(
                HeaderKey::from_str(topic_option_keys::MESSAGES_REQUIRED_TO_SAVE)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(messages_required_to_save)),
            );
        }
        if let Some(size_of_messages) = self.size_of_messages_required_to_save {
            options.insert(
                HeaderKey::from_str(topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(size_of_messages.as_bytes_u64())),
            );
        }
        if let Some(preallocate_segments) = self.preallocate_segments {
            options.insert(
                HeaderKey::from_str(topic_option_keys::PREALLOCATE_SEGMENTS)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(preallocate_segments)),
            );
        }
        if let Some(replication_factor) = self.replication_factor {
            options.insert(
                HeaderKey::from_str(topic_option_keys::REPLICATION_FACTOR)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from(replication_factor)),
            );
        }
        crate::wire_conversions::resource_options_to_wire(&options, false)
    }

    /// Encode the resolved values for every key the client did NOT send into
    /// a derived-options wire block, in canonical kinds. `partitions_count`
    /// is never included: it is consumed at admission.
    #[must_use]
    pub fn derived_block(
        &self,
        compression_algorithm: CompressionAlgorithm,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
        runtime_defaults: TopicRuntimeDefaults,
    ) -> iggy_binary_protocol::WireOptions {
        let mut derived = ResourceOptions::new();
        if self.compression_algorithm.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::COMPRESSION_ALGORITHM)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(
                    HeaderValue::try_from(compression_algorithm.to_string().as_str())
                        .expect("compression name fits a header value"),
                ),
            );
        }
        if self.message_expiry.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::MESSAGE_EXPIRY)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(u64::from(message_expiry))),
            );
        }
        if self.max_topic_size.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::MAX_TOPIC_SIZE)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(u64::from(max_topic_size))),
            );
        }
        if self.segment_size.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::SEGMENT_SIZE)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(
                    runtime_defaults.segment_size.as_bytes_u64(),
                )),
            );
        }
        if self.enforce_fsync.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::ENFORCE_FSYNC)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(runtime_defaults.enforce_fsync)),
            );
        }
        if self.messages_required_to_save.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::MESSAGES_REQUIRED_TO_SAVE)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(
                    runtime_defaults.messages_required_to_save,
                )),
            );
        }
        if self.size_of_messages_required_to_save.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(
                    runtime_defaults
                        .size_of_messages_required_to_save
                        .as_bytes_u64(),
                )),
            );
        }
        if self.preallocate_segments.is_none() {
            derived.insert(
                HeaderKey::from_str(topic_option_keys::PREALLOCATE_SEGMENTS)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(runtime_defaults.preallocate_segments)),
            );
        }
        if self.replication_factor.is_none() {
            // Not a `TopicRuntimeDefaults` member: no node-local config backs
            // it, so every replica derives the same constant.
            derived.insert(
                HeaderKey::from_str(topic_option_keys::REPLICATION_FACTOR)
                    .expect("catalog key is a valid header key"),
                OptionValue::derived(HeaderValue::from(DEFAULT_REPLICATION_FACTOR)),
            );
        }
        crate::wire_conversions::resource_options_to_wire(&derived, false)
    }

    /// Parse a topic's PERSISTED options map (explicit plus admission-derived
    /// entries) back into typed values.
    ///
    /// The persisted map is the single source of truth for a topic's knobs:
    /// nothing on the STM `Topic` duplicates it per key, so a new key costs
    /// one catalog entry rather than one field on every stored type.
    ///
    /// # Errors
    ///
    /// Same contract as [`Self::parse`]. A persisted map is only ever written
    /// by admission, so an error here means state written by a build that
    /// knew a key this one does not.
    pub fn from_resource_options(options: &ResourceOptions) -> Result<Self, IggyError> {
        let mut parsed = Self::default();
        for (key, option) in options {
            let key = String::from_utf8_lossy(key.as_bytes());
            let entry = WireUserHeaderEntry {
                key_kind: iggy_binary_protocol::WireHeaderKind(HeaderKind::String.as_code()),
                key: key.as_bytes(),
                value_kind: iggy_binary_protocol::WireHeaderKind(option.value.kind().as_code()),
                value: option.value.as_bytes(),
            };
            parsed.absorb(&entry, &key)?;
        }
        Ok(parsed)
    }
}

fn parse_u32(entry: &WireUserHeaderEntry<'_>, key: &str) -> Result<u32, IggyError> {
    if entry.value_kind.0 == HeaderKind::Uint32.as_code() {
        let bytes: [u8; 4] = entry
            .value
            .try_into()
            .map_err(|_| IggyError::InvalidOptionValue(key.to_string()))?;
        return Ok(u32::from_le_bytes(bytes));
    }
    if entry.value_kind.0 == HeaderKind::String.as_code() {
        return std::str::from_utf8(entry.value)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| IggyError::InvalidOptionValue(key.to_string()));
    }
    Err(IggyError::InvalidOptionValue(key.to_string()))
}

/// `Uint64` verbatim, or a `String` routed through `from_str` and converted
/// back to the type's `u64` sentinel encoding.
fn parse_u64_or<T, E>(
    entry: &WireUserHeaderEntry<'_>,
    key: &str,
    from_str: impl Fn(&str) -> Result<T, E>,
) -> Result<u64, IggyError>
where
    u64: From<T>,
{
    if entry.value_kind.0 == HeaderKind::Uint64.as_code() {
        let bytes: [u8; 8] = entry
            .value
            .try_into()
            .map_err(|_| IggyError::InvalidOptionValue(key.to_string()))?;
        return Ok(u64::from_le_bytes(bytes));
    }
    if entry.value_kind.0 == HeaderKind::String.as_code() {
        return std::str::from_utf8(entry.value)
            .ok()
            .and_then(|text| from_str(text).ok())
            .map(u64::from)
            .ok_or_else(|| IggyError::InvalidOptionValue(key.to_string()));
    }
    Err(IggyError::InvalidOptionValue(key.to_string()))
}

/// `Uint64` verbatim, or a byte-size `String` (`128MiB`).
fn parse_byte_size(entry: &WireUserHeaderEntry<'_>, key: &str) -> Result<u64, IggyError> {
    parse_u64_or(entry, key, |text| {
        IggyByteSize::from_str(text).map(|size| size.as_bytes_u64())
    })
}

/// `Bool` verbatim, or the strings `true` / `false`.
fn parse_u8(entry: &WireUserHeaderEntry<'_>, key: &str) -> Result<u8, IggyError> {
    if entry.value_kind.0 == HeaderKind::Uint8.as_code() {
        return entry
            .value
            .first()
            .copied()
            .ok_or_else(|| IggyError::InvalidOptionValue(key.to_string()));
    }
    if entry.value_kind.0 == HeaderKind::String.as_code() {
        return std::str::from_utf8(entry.value)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| IggyError::InvalidOptionValue(key.to_string()));
    }
    Err(IggyError::InvalidOptionValue(key.to_string()))
}

fn parse_bool(entry: &WireUserHeaderEntry<'_>, key: &str) -> Result<bool, IggyError> {
    if entry.value_kind.0 == HeaderKind::Bool.as_code() {
        return match entry.value.first() {
            Some(0) => Ok(false),
            Some(_) => Ok(true),
            None => Err(IggyError::InvalidOptionValue(key.to_string())),
        };
    }
    if entry.value_kind.0 == HeaderKind::String.as_code() {
        return std::str::from_utf8(entry.value)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| IggyError::InvalidOptionValue(key.to_string()));
    }
    Err(IggyError::InvalidOptionValue(key.to_string()))
}

fn parse_compression(
    entry: &WireUserHeaderEntry<'_>,
    key: &str,
) -> Result<CompressionAlgorithm, IggyError> {
    if entry.value_kind.0 == HeaderKind::Uint8.as_code() {
        let code = entry
            .value
            .first()
            .copied()
            .ok_or_else(|| IggyError::InvalidOptionValue(key.to_string()))?;
        return CompressionAlgorithm::from_code(code)
            .map_err(|_| IggyError::InvalidOptionValue(key.to_string()));
    }
    if entry.value_kind.0 == HeaderKind::String.as_code() {
        return std::str::from_utf8(entry.value)
            .ok()
            .and_then(|text| CompressionAlgorithm::from_str(text).ok())
            .ok_or_else(|| IggyError::InvalidOptionValue(key.to_string()));
    }
    Err(IggyError::InvalidOptionValue(key.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_wire_parse_roundtrip_preserves_typed_fields() {
        let options = TopicCreateOptions {
            compression_algorithm: Some(CompressionAlgorithm::Gzip),
            message_expiry: Some(IggyExpiry::from(5_000_000u64)),
            max_topic_size: Some(MaxTopicSize::from(2_000_000_000u64)),
            segment_size: Some(IggyByteSize::from(134_217_728u64)),
            enforce_fsync: Some(true),
            messages_required_to_save: Some(500),
            size_of_messages_required_to_save: Some(IggyByteSize::from(2_097_152u64)),
            preallocate_segments: Some(false),
            replication_factor: Some(3),
            partitions_count: None,
            raw: BTreeMap::new(),
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire()).unwrap();
        assert_eq!(parsed, options);
    }

    #[test]
    fn partitions_count_never_enters_the_options_block() {
        // It is a fixed field of the CreateTopic command, so encoding it as a
        // TLV entry would both duplicate it and make it look like a stored
        // topic setting.
        let options = TopicCreateOptions {
            partitions_count: Some(7),
            ..TopicCreateOptions::default()
        };
        assert!(options.to_wire().is_empty());
        // ...and the key is rejected if a client hand-rolls it into the block.
        let raw = TopicCreateOptions {
            raw: BTreeMap::from([("partitions_count".to_string(), "7".to_string())]),
            ..TopicCreateOptions::default()
        };
        assert_eq!(
            TopicCreateOptions::parse(&raw.to_wire()),
            Err(IggyError::UnsupportedOptionKey(
                "partitions_count".to_string()
            ))
        );
    }

    #[test]
    fn resource_options_round_trip_through_json_with_string_keys() {
        // `HeaderKey` serializes as a struct, and JSON object keys must be
        // strings, so the derived impl fails with "key must be a string" for
        // any non-empty map -- which 500'd every HTTP response carrying
        // options. Keys must render as their plain text.
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Holder {
            #[serde(default, with = "super::resource_options_json")]
            options: ResourceOptions,
        }

        let holder = Holder {
            options: ResourceOptions::from([(
                HeaderKey::from_str(topic_option_keys::SEGMENT_SIZE).unwrap(),
                OptionValue::derived(HeaderValue::from(1024u64 * 1024)),
            )]),
        };
        let json = serde_json::to_string(&holder).expect("options must serialize as JSON");
        assert!(
            json.contains("\"segment_size\""),
            "the key must render as plain text, got: {json}"
        );
        let decoded: Holder = serde_json::from_str(&json).expect("options must round-trip");
        assert_eq!(decoded, holder);
    }

    #[test]
    fn zero_valued_sentinels_normalize_to_unspecified() {
        // 0 is the ServerDefault sentinel for both expiry and size on the
        // wire, so a client sending it means "resolve from the default"
        // rather than "expire immediately" / "no space". This used to be a
        // boot-time config rejection; with the keys per-topic it is a
        // normalization at parse.
        let options = TopicCreateOptions {
            message_expiry: Some(IggyExpiry::from(0u64)),
            max_topic_size: Some(MaxTopicSize::from(0u64)),
            segment_size: Some(IggyByteSize::from(0u64)),
            ..TopicCreateOptions::default()
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire()).unwrap();
        assert_eq!(parsed.message_expiry, None);
        assert_eq!(parsed.max_topic_size, None);
        assert_eq!(parsed.segment_size, None);
    }

    #[test]
    fn runtime_options_derive_from_the_persisted_map() {
        let options = TopicCreateOptions {
            segment_size: Some(IggyByteSize::from(2_097_152u64)),
            enforce_fsync: Some(true),
            messages_required_to_save: Some(9),
            ..TopicCreateOptions::default()
        };
        // What admission persists: the client block merged with derived
        // defaults, keyed by HeaderKey. The channel reads exactly this.
        let persisted =
            crate::wire_conversions::resource_options_from_wire(&options.to_wire(), true).unwrap();
        let runtime = TopicRuntimeOptions::from_resource_options(&persisted);
        assert_eq!(runtime.segment_size, Some(IggyByteSize::from(2_097_152u64)));
        assert_eq!(runtime.enforce_fsync, Some(true));
        assert_eq!(runtime.messages_required_to_save, Some(9));
        assert_eq!(runtime.size_of_messages_required_to_save, None);
    }

    #[test]
    fn typed_field_wins_over_raw_entry_for_the_same_key() {
        let options = TopicCreateOptions {
            enforce_fsync: Some(true),
            raw: BTreeMap::from([("enforce_fsync".to_string(), "false".to_string())]),
            ..TopicCreateOptions::default()
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire()).unwrap();
        assert_eq!(parsed.enforce_fsync, Some(true));
    }

    #[test]
    fn raw_entries_ride_as_strings_and_unknown_keys_reject() {
        // `prepare_queue_depth` stays server-side on purpose: it is a
        // node-resource bound (the view-change wire caps it), so a
        // client-chosen value is an amplifier rather than a topic property.
        let options = TopicCreateOptions {
            raw: BTreeMap::from([("prepare_queue_depth".to_string(), "64".to_string())]),
            ..TopicCreateOptions::default()
        };
        assert_eq!(
            TopicCreateOptions::parse(&options.to_wire()),
            Err(IggyError::UnsupportedOptionKey(
                "prepare_queue_depth".to_string()
            ))
        );

        // Raw entries for catalog keys parse via the config-file rules.
        let options = TopicCreateOptions {
            raw: BTreeMap::from([
                ("segment_size".to_string(), "128MiB".to_string()),
                ("enforce_fsync".to_string(), "true".to_string()),
                (
                    "size_of_messages_required_to_save".to_string(),
                    "4KiB".to_string(),
                ),
            ]),
            ..TopicCreateOptions::default()
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire()).unwrap();
        assert_eq!(
            parsed.segment_size,
            Some(IggyByteSize::from(134_217_728u64))
        );
        assert_eq!(parsed.enforce_fsync, Some(true));
        assert_eq!(
            parsed.size_of_messages_required_to_save,
            Some(IggyByteSize::from(4096u64))
        );
    }

    #[test]
    fn string_values_parse_via_config_rules() {
        let options = TopicCreateOptions {
            raw: BTreeMap::from([
                ("message_expiry".to_string(), "5s".to_string()),
                ("max_topic_size".to_string(), "2GB".to_string()),
                ("compression_algorithm".to_string(), "gzip".to_string()),
            ]),
            ..TopicCreateOptions::default()
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire()).unwrap();
        assert_eq!(parsed.message_expiry, Some(IggyExpiry::from(5_000_000u64)));
        assert_eq!(
            parsed.compression_algorithm,
            Some(CompressionAlgorithm::Gzip)
        );
        assert!(parsed.max_topic_size.is_some());
    }
}
