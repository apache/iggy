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
//!
//! # SDK parity
//!
//! One feature, and the SDKs are at different points on it. What each can do
//! today, so a gap is a known gap rather than a surprise:
//!
//! | SDK    | Set typed keys | Set arbitrary keys | Read the blocks | `describe_options` |
//! |--------|----------------|--------------------|-----------------|--------------------|
//! | Rust   | all            | yes (`raw`)        | yes             | yes                |
//! | Node   | all            | yes                | yes             | yes                |
//! | Python | all            | yes (string dict)  | no              | no                 |
//! | Go     | 3 legacy       | no                 | yes             | no                 |
//! | C#     | 3 legacy       | no                 | yes (TCP)       | yes (TCP)          |
//! | Java   | 3 legacy       | no                 | yes             | no                 |
//! | C++    | 3 legacy       | no                 | no              | no                 |
//!
//! "3 legacy" is `compression_algorithm`, `message_expiry` and `max_topic_size`,
//! the three that used to be fixed fields of `CreateTopic`. Every SDK can
//! therefore express what it could before this feature; what a lagging one
//! cannot yet do is reach the partition runtime knobs or a key added to the
//! server catalog after it shipped.
//!
//! The write side is what to close next, and it is one change per SDK: accept a
//! string map alongside the typed arguments and encode it into the block the
//! same way. The block's byte layout is pinned by a golden vector that Rust,
//! Node, Go and Java each assert independently, so a new encoder has a fixture
//! to match rather than a description to interpret.

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

/// Which provenance classes an encoded options block carries.
///
/// Responses split a resource's options into two blocks so `GetTopic` can say
/// which values the client chose and which admission filled in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionsProvenance {
    /// Every entry, whatever its provenance.
    All,
    /// Only the keys the client sent.
    Explicit,
    /// Only the keys admission resolved from server defaults.
    Derived,
}

impl OptionValue {
    /// Whether this entry belongs in a block encoded for `provenance`.
    #[must_use]
    pub const fn matches(&self, provenance: OptionsProvenance) -> bool {
        match provenance {
            OptionsProvenance::All => true,
            OptionsProvenance::Explicit => self.explicit,
            OptionsProvenance::Derived => !self.explicit,
        }
    }

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
    use super::{HeaderKey, HeaderValue, OptionValue, ResourceOptions};
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;
    use std::str::FromStr;

    /// One entry's JSON form: the value in the same string form a config file
    /// or a `POST` body would carry it in, plus its provenance.
    ///
    /// The stored value is a typed [`HeaderValue`], and serializing that gives
    /// `{"kind":"uint64","value":"<base64>"}` - which no operator can read and
    /// no client can feed back, since the create body takes options as a plain
    /// string map. Rendering the string form makes the "re-send only the
    /// explicit options" round trip actually expressible over HTTP.
    #[derive(Serialize, Deserialize)]
    struct OptionValueJson {
        value: String,
        explicit: bool,
    }

    /// # Errors
    ///
    /// Propagates the serializer's own errors.
    pub fn serialize<S: Serializer>(
        options: &ResourceOptions,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let readable: BTreeMap<String, OptionValueJson> = options
            .iter()
            .map(|(key, option)| {
                (
                    String::from_utf8_lossy(key.as_bytes()).into_owned(),
                    OptionValueJson {
                        value: option.value.to_string_value(),
                        explicit: option.explicit,
                    },
                )
            })
            .collect();
        readable.serialize(serializer)
    }

    /// # Errors
    ///
    /// Returns a deserializer error when a key is not a valid option name or a
    /// value does not fit a header value.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ResourceOptions, D::Error> {
        let readable = BTreeMap::<String, OptionValueJson>::deserialize(deserializer)?;
        readable
            .into_iter()
            .map(|(key, option)| {
                let key = HeaderKey::from_str(&key).map_err(D::Error::custom)?;
                let value = HeaderValue::from_str(&option.value).map_err(D::Error::custom)?;
                Ok((
                    key,
                    OptionValue {
                        value,
                        explicit: option.explicit,
                    },
                ))
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

/// Ceiling for `size_of_messages_required_to_save`.
///
/// A threshold above the largest a segment may be can never trip, so every
/// committed message would sit in the journal until its segment rotates, and a
/// crash does not preserve the journal. These two thresholds used to be
/// operator-only config; anyone holding `create_topic` can set them now, so the
/// ceilings are enforced at admission. Mirrors
/// `configs::validators::SEGMENT_MAX_SIZE_BYTES`, which lives in the crate that
/// depends on this one.
pub const MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE: u64 = 1024 * 1024 * 1024;

/// Ceiling for `messages_required_to_save`, derived from the byte ceiling: a
/// segment that large cannot hold more messages than this, because a message
/// costs at least its header on disk.
pub const MAX_MESSAGES_REQUIRED_TO_SAVE: u32 = 16 * 1024 * 1024;

/// Smallest per-topic segment size. Segments far below the shipped default
/// explode the per-partition segment count, which state transfer bounds via
/// its manifest entry cap; a partition that crosses it becomes unservable
/// for transfer, so the floor is enforced at admission.
pub const MIN_TOPIC_SEGMENT_SIZE: u64 = 1024 * 1024;

/// Largest per-topic `segment_size` a node admits.
///
/// The ceiling used to be computed per node as the smaller of the global
/// segment maximum and `transfer_artifact_bytes_max - max_message_size` (a
/// received segment artifact can be one whole batch past the cap, and an
/// artifact ceiling below that livelocks a partition's rejoin). Config
/// validation now refuses boot unless that subtraction is at least the segment
/// maximum, so the minimum is always the maximum and the per-node expression was
/// dead. Mirrors `configs::validators::SEGMENT_MAX_SIZE_BYTES`, which lives in
/// the crate that depends on this one and pins the two together in a test.
pub const MAX_TOPIC_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

/// Ceiling on what one `preallocate_segments` topic may reserve when created:
/// `segment_size * partitions_count`.
///
/// The reservation runs inline on the shard thread - once per partition as
/// segments rotate, and again for every owned partition at boot - and
/// `FALLOC_FL_KEEP_SIZE` makes it real disk rather than a sparse hole. Without a
/// cap, one create at the 1 GiB default segment size and the maximum partition
/// count reserves a terabyte before a single message arrives.
pub const MAX_PREALLOCATED_TOPIC_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Validate what a preallocating topic would reserve at admission.
///
/// `segment_size_bytes` is the topic's resolved segment size. Callers skip this
/// for a topic whose effective `preallocate_segments` is false: nothing is
/// reserved up front there, so the product does not bound anything.
///
/// # Errors
///
/// Returns `IggyError::InvalidOptionValue("preallocate_segments")` when the
/// reservation would exceed [`MAX_PREALLOCATED_TOPIC_BYTES`].
pub fn validate_preallocated_topic_bytes(
    segment_size_bytes: u64,
    partitions_count: u32,
) -> Result<(), IggyError> {
    let reserved = segment_size_bytes.saturating_mul(u64::from(partitions_count));
    if reserved > MAX_PREALLOCATED_TOPIC_BYTES {
        return Err(IggyError::InvalidOptionValue(
            topic_option_keys::PREALLOCATE_SEGMENTS.to_string(),
        ));
    }
    Ok(())
}

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
    /// Kind the server encodes this key's own default under, and the kind a
    /// client should prefer. Not a promise about what ends up stored: an
    /// explicit entry is persisted and echoed back with the kind the client
    /// sent it as, and a `String` value parsed through the server-config rules
    /// stays a `String` in the map - which is what every CLI `--set` produces.
    pub kind: HeaderKind,
    /// The key's default in `kind`'s encoding; empty when the key has no
    /// default. A build constant rather than a per-node value: these defaults
    /// stopped being config-derived when the `[system.*]` keys became options.
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
];

/// The subset of [`TOPIC_OPTION_KEYS`] an `UpdateTopic` options block may
/// carry.
///
/// These three used to be fixed fields of the update command, which meant one
/// setting had two homes and an update always rewrote all three whether the
/// caller meant to or not. As options they are patched: a key the client did
/// not send keeps its current value.
///
/// The partition runtime knobs (`segment_size`, `enforce_fsync`, both flush
/// thresholds, `preallocate_segments`) stay out, and not only because nothing
/// re-pushes them to a live partition. They describe how a partition's storage
/// was laid down: changing `segment_size` mid-segment leaves one segment sized
/// by the old cap and the next by the new one, and `preallocate_segments` can
/// only act on a file not yet opened. A topic gets them at creation and keeps
/// them, so its segments stay uniform.
pub const UPDATABLE_TOPIC_OPTION_KEYS: &[&str] = &[
    topic_option_keys::COMPRESSION_ALGORITHM,
    topic_option_keys::MESSAGE_EXPIRY,
    topic_option_keys::MAX_TOPIC_SIZE,
];

/// Keys an `UpdateStream` options block may carry. Empty because streams have
/// no catalog keys yet: the block exists so the first one costs a catalog
/// entry instead of another wire change, and until then every key is rejected
/// by name rather than stored and ignored.
pub const UPDATABLE_STREAM_OPTION_KEYS: &[&str] = &[];

/// Keys an `UpdateUser` options block may carry. Empty for the same reason as
/// [`UPDATABLE_STREAM_OPTION_KEYS`].
pub const UPDATABLE_USER_OPTION_KEYS: &[&str] = &[];

/// Longest key echoed back in an option error. A rejected key is attacker-sized
/// by definition, and over HTTP the error text rides the response body back to
/// the client.
///
/// Binary transports carry only the error code, so a TCP, QUIC or WebSocket
/// client rebuilds [`IggyError::UnsupportedOptionKey`] with an empty string and
/// never sees which key was refused. `DescribeOptions` is the discovery path
/// there - see the note on that variant.
const ERROR_KEY_PREVIEW_LEN: usize = 64;

fn key_preview(key: &str) -> String {
    key.chars().take(ERROR_KEY_PREVIEW_LEN).collect()
}

/// Build an options map from string key-values, all marked explicit.
///
/// Shared by the update-options types: their keys are all client-sent, so none
/// of the derived-provenance handling that create needs applies. Callers that
/// also have typed fields insert those afterwards, so a typed value wins on
/// collision and keeps its canonical kind.
///
/// # Errors
///
/// `UnsupportedOptionKey` when a key is empty or over 255 bytes,
/// `InvalidOptionValue` when a value is. Both bounds come from the
/// header-field codec these entries ride.
fn raw_options_map(raw: &BTreeMap<String, String>) -> Result<ResourceOptions, IggyError> {
    let mut options = ResourceOptions::new();
    for (key, value) in raw {
        let header_key = HeaderKey::from_str(key)
            .map_err(|_| IggyError::UnsupportedOptionKey(key_preview(key)))?;
        let header_value = HeaderValue::try_from(value.as_str())
            .map_err(|_| IggyError::InvalidOptionValue(key_preview(key)))?;
        options.insert(header_key, OptionValue::explicit(header_value));
    }
    Ok(options)
}

/// Encode string key-values into an options block, all marked explicit.
///
/// # Errors
///
/// See [`raw_options_map`].
fn raw_options_to_wire(raw: &BTreeMap<String, String>) -> Result<WireOptions, IggyError> {
    crate::wire_conversions::resource_options_to_wire(
        &raw_options_map(raw)?,
        OptionsProvenance::All,
    )
}

/// The options an `UpdateStream` may carry.
///
/// Absent keys leave the stream's existing values alone -- an update patches
/// the option map, it does not replace it. See [`TopicUpdateOptions`] for why.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamUpdateOptions {
    /// Keys sent as `String` values, checked against
    /// [`UPDATABLE_STREAM_OPTION_KEYS`] server-side. That list is empty, so
    /// every key is currently refused by name; the field exists so the first
    /// updatable stream key costs a catalog entry, not a wire change.
    pub raw: BTreeMap<String, String>,
}

impl StreamUpdateOptions {
    /// Encode the present keys into an options block.
    ///
    /// # Errors
    ///
    /// See [`raw_options_to_wire`].
    pub fn to_wire(&self) -> Result<WireOptions, IggyError> {
        raw_options_to_wire(&self.raw)
    }
}

/// The options an `UpdateUser` may carry. Same patch semantics as
/// [`StreamUpdateOptions`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserUpdateOptions {
    /// Keys sent as `String` values, checked against
    /// [`UPDATABLE_USER_OPTION_KEYS`] server-side. That list is empty, so every
    /// key is currently refused by name; the field exists so the first
    /// updatable user key costs a catalog entry, not a wire change.
    pub raw: BTreeMap<String, String>,
}

impl UserUpdateOptions {
    /// Encode the present keys into an options block.
    ///
    /// # Errors
    ///
    /// See [`raw_options_to_wire`].
    pub fn to_wire(&self) -> Result<WireOptions, IggyError> {
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
    ///
    /// Degrades per key, not per map. An entry this build cannot interpret
    /// leaves its own knob unset and every other knob intact, so one key a
    /// newer node wrote cannot silently drop a topic's `enforce_fsync` or
    /// reset its segment size along with it.
    #[must_use]
    pub fn from_resource_options(options: &ResourceOptions) -> Self {
        let parsed = TopicCreateOptions::from_resource_options(options);
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
    /// `None` leaves the topic's current algorithm alone.
    pub compression_algorithm: Option<CompressionAlgorithm>,
    /// `None` leaves the topic's current expiry alone.
    pub message_expiry: Option<IggyExpiry>,
    /// `None` leaves the topic's current cap alone.
    pub max_topic_size: Option<MaxTopicSize>,
    /// Keys sent as `String` values, checked against
    /// [`UPDATABLE_TOPIC_OPTION_KEYS`] server-side. Lets a client reach an
    /// updatable key added to the catalog after this build shipped.
    pub raw: BTreeMap<String, String>,
}

impl TopicUpdateOptions {
    /// Encode the present keys into an options block, in canonical kinds.
    ///
    /// A typed field is inserted after the raw entries, so it wins on collision
    /// and keeps its canonical kind.
    ///
    /// # Errors
    ///
    /// See [`raw_options_map`].
    pub fn to_wire(&self) -> Result<WireOptions, IggyError> {
        let mut options = raw_options_map(&self.raw)?;
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
        crate::wire_conversions::resource_options_to_wire(&options, OptionsProvenance::All)
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
    /// String-valued keys with no typed field above, parsed server-side
    /// through each key's `FromStr`. Lets a client reach a key added to the
    /// server catalog after this build shipped. Outbound only: a typed field
    /// wins on collision in [`Self::to_wire`], and [`Self::parse`] never
    /// populates it.
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
            parsed.absorb_strict(&entry, &key)?;
        }
        Ok(parsed)
    }

    /// Parse a block that is already COMMITTED, skipping what this build
    /// cannot interpret.
    ///
    /// Admission gated the block against the catalog on the primary, once.
    /// Re-running that gate at apply would make the verdict depend on the
    /// build running it, so a replica that predates a key would reject an
    /// operation its peers accepted and diverge from the group permanently.
    /// The catalog belongs at the edge; apply only reads what it knows.
    #[must_use]
    pub fn parse_committed(options: &WireOptions) -> Self {
        let mut parsed = Self::default();
        for entry in options {
            let key = String::from_utf8_lossy(entry.key);
            // Discarding the error is the skip: see `absorb_strict`.
            let _ = parsed.absorb_strict(&entry, &key);
        }
        parsed
    }

    /// Fold one catalog entry into `self`, refusing an entry this build cannot
    /// interpret: a key outside [`TOPIC_OPTION_KEYS`], or a catalog key whose
    /// value kind or payload does not parse. Shared by [`Self::parse`] (wire
    /// block) and [`Self::from_resource_options`] (persisted map) so both read
    /// the identical key set, kinds and value bounds.
    ///
    /// Every arm assigns only after its own parse succeeds, which is what makes
    /// discarding the error a safe skip: a refused entry leaves `self` exactly
    /// as it found it.
    fn absorb_strict(
        &mut self,
        entry: &WireUserHeaderEntry<'_>,
        key: &str,
    ) -> Result<(), IggyError> {
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
                    if messages == 0 || messages > MAX_MESSAGES_REQUIRED_TO_SAVE {
                        return Err(IggyError::InvalidOptionValue(key.to_string()));
                    }
                    parsed.messages_required_to_save = Some(messages);
                }
                topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE => {
                    let size = parse_byte_size(entry, key)?;
                    if size > MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE {
                        return Err(IggyError::InvalidOptionValue(key.to_string()));
                    }
                    parsed.size_of_messages_required_to_save =
                        (size != 0).then_some(IggyByteSize::from(size));
                }
                topic_option_keys::PREALLOCATE_SEGMENTS => {
                    parsed.preallocate_segments = Some(parse_bool(entry, key)?);
                }
                _ => return Err(IggyError::UnsupportedOptionKey(key.to_string())),
            }
        }
        Ok(())
    }

    /// Field-wise overlay: every key this block sets wins, the rest fall back
    /// to `defaults`.
    ///
    /// Apply resolves the client block over the derived one this way. `raw` is
    /// dropped: by the time a block reaches apply, every key it named has
    /// already been parsed into a typed field or refused by admission.
    #[must_use]
    pub fn resolved_over(&self, defaults: &Self) -> Self {
        Self {
            partitions_count: self.partitions_count.or(defaults.partitions_count),
            compression_algorithm: self
                .compression_algorithm
                .or(defaults.compression_algorithm),
            message_expiry: self.message_expiry.or(defaults.message_expiry),
            max_topic_size: self.max_topic_size.or(defaults.max_topic_size),
            segment_size: self.segment_size.or(defaults.segment_size),
            enforce_fsync: self.enforce_fsync.or(defaults.enforce_fsync),
            messages_required_to_save: self
                .messages_required_to_save
                .or(defaults.messages_required_to_save),
            size_of_messages_required_to_save: self
                .size_of_messages_required_to_save
                .or(defaults.size_of_messages_required_to_save),
            preallocate_segments: self.preallocate_segments.or(defaults.preallocate_segments),
            raw: BTreeMap::new(),
        }
    }

    /// Encode the present keys into a client options block, in canonical
    /// kinds. The client-side counterpart of [`Self::parse`].
    ///
    /// # Errors
    ///
    /// See [`Self::to_option_map`].
    pub fn to_wire(&self) -> Result<WireOptions, IggyError> {
        crate::wire_conversions::resource_options_to_wire(
            &self.to_option_map()?,
            OptionsProvenance::All,
        )
    }

    /// The present keys as a canonically-kinded option map.
    ///
    /// A typed field is inserted after the raw entries, so it wins on
    /// collision and keeps its canonical kind.
    ///
    /// # Errors
    ///
    /// See [`raw_options_map`].
    pub fn to_option_map(&self) -> Result<ResourceOptions, IggyError> {
        let mut options = raw_options_map(&self.raw)?;
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
        Ok(options)
    }

    /// Render the present keys as string key-values, for a transport that
    /// carries options as a JSON object instead of a TLV block.
    ///
    /// `compression_algorithm`, `message_expiry` and `max_topic_size` are left
    /// out: the JSON body has dedicated fields for those three, and sending
    /// both would be the two-sources-for-one-setting the update path refuses.
    /// The rest ride as strings and are parsed server-side by the same
    /// `FromStr` rules a config file value goes through, so a typed field
    /// survives the round trip rather than being silently dropped.
    #[must_use]
    pub fn to_string_options(&self) -> BTreeMap<String, String> {
        // Typed fields are inserted over the raw entries, matching the
        // collision rule `to_wire` applies.
        let mut options = self.raw.clone();
        if let Some(segment_size) = self.segment_size {
            options.insert(
                topic_option_keys::SEGMENT_SIZE.to_owned(),
                segment_size.as_bytes_u64().to_string(),
            );
        }
        if let Some(enforce_fsync) = self.enforce_fsync {
            options.insert(
                topic_option_keys::ENFORCE_FSYNC.to_owned(),
                enforce_fsync.to_string(),
            );
        }
        if let Some(messages_required_to_save) = self.messages_required_to_save {
            options.insert(
                topic_option_keys::MESSAGES_REQUIRED_TO_SAVE.to_owned(),
                messages_required_to_save.to_string(),
            );
        }
        if let Some(size_of_messages) = self.size_of_messages_required_to_save {
            options.insert(
                topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE.to_owned(),
                size_of_messages.as_bytes_u64().to_string(),
            );
        }
        if let Some(preallocate_segments) = self.preallocate_segments {
            options.insert(
                topic_option_keys::PREALLOCATE_SEGMENTS.to_owned(),
                preallocate_segments.to_string(),
            );
        }
        options
    }

    /// Encode the resolved values for every key the client did NOT send into
    /// a derived-options wire block, in canonical kinds. `partitions_count`
    /// is never included: it is consumed at admission.
    ///
    /// # Errors
    ///
    /// See [`crate::wire_conversions::resource_options_to_wire`].
    pub fn derived_block(
        &self,
        compression_algorithm: CompressionAlgorithm,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
        runtime_defaults: TopicRuntimeDefaults,
    ) -> Result<WireOptions, IggyError> {
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
        crate::wire_conversions::resource_options_to_wire(&derived, OptionsProvenance::All)
    }

    /// Parse a topic's PERSISTED options map (explicit plus admission-derived
    /// entries) back into typed values.
    ///
    /// The persisted map is the single source of truth for a topic's knobs:
    /// nothing on the STM `Topic` duplicates it per key, so a new key costs
    /// one catalog entry rather than one field on every stored type.
    ///
    /// Infallible by design. A persisted map can hold keys a newer build
    /// wrote, and refusing the whole map for one of them is what would make
    /// this replica read a topic differently from the node that stored it.
    /// Unreadable entries are skipped; their knobs stay unset and resolve
    /// against shard-wide config exactly as an absent key does.
    #[must_use]
    pub fn from_resource_options(options: &ResourceOptions) -> Self {
        let mut parsed = Self::default();
        for (key, option) in options {
            let key = String::from_utf8_lossy(key.as_bytes());
            let entry = WireUserHeaderEntry {
                key_kind: iggy_binary_protocol::WireHeaderKind(HeaderKind::String.as_code()),
                key: key.as_bytes(),
                value_kind: iggy_binary_protocol::WireHeaderKind(option.value.kind().as_code()),
                value: option.value.as_bytes(),
            };
            // Discarding the error is the skip: see `absorb_strict`.
            let _ = parsed.absorb_strict(&entry, &key);
        }
        parsed
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

fn parse_bool(entry: &WireUserHeaderEntry<'_>, key: &str) -> Result<bool, IggyError> {
    if entry.value_kind.0 == HeaderKind::Bool.as_code() {
        // Exactly one byte of 0 or 1. Anything looser admits a value that
        // `HeaderValue::as_bool` later refuses to read, so the stored map would
        // hold an entry its own public accessor rejects; a multi-byte payload
        // would pass this gate and only fail at apply, turning a client
        // mistake into a committed state-machine rejection.
        return match entry.value {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(IggyError::InvalidOptionValue(key.to_string())),
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
        // Exactly one byte, for the same reason as `parse_bool`.
        let [code] = entry.value else {
            return Err(IggyError::InvalidOptionValue(key.to_string()));
        };
        return CompressionAlgorithm::from_code(*code)
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
            partitions_count: None,
            raw: BTreeMap::new(),
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire().unwrap()).unwrap();
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
        assert!(options.to_wire().unwrap().is_empty());
        // ...and the key is rejected if a client hand-rolls it into the block.
        let raw = TopicCreateOptions {
            raw: BTreeMap::from([("partitions_count".to_string(), "7".to_string())]),
            ..TopicCreateOptions::default()
        };
        assert_eq!(
            TopicCreateOptions::parse(&raw.to_wire().unwrap()),
            Err(IggyError::UnsupportedOptionKey(
                "partitions_count".to_string()
            ))
        );
    }

    #[test]
    fn resource_options_render_as_readable_json_and_round_trip() {
        // `HeaderKey` serializes as a struct, and JSON object keys must be
        // strings, so the derived impl fails with "key must be a string" for
        // any non-empty map -- which 500'd every HTTP response carrying
        // options. Keys must render as their plain text, and values in the
        // string form the create body takes them in: a base64 payload behind a
        // kind tag is neither readable nor re-sendable.
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
        assert!(
            json.contains("\"value\":\"1048576\""),
            "the value must render as its string form, got: {json}"
        );

        let decoded: Holder = serde_json::from_str(&json).expect("options must round-trip");
        let (key, option) = decoded.options.iter().next().unwrap();
        assert_eq!(key.as_bytes(), topic_option_keys::SEGMENT_SIZE.as_bytes());
        assert!(!option.explicit, "provenance survives the round trip");
        // Re-read as a string-kinded value, which is exactly what the create
        // body's string map produces, so the server parses it the same way.
        assert_eq!(option.value.kind(), HeaderKind::String);
        assert_eq!(option.value.as_bytes(), b"1048576");
        assert!(
            TopicCreateOptions::parse(
                &crate::wire_conversions::resource_options_to_wire(
                    &decoded.options,
                    OptionsProvenance::All
                )
                .unwrap()
            )
            .is_ok(),
            "the rendered value must parse back through the catalog"
        );
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
        let parsed = TopicCreateOptions::parse(&options.to_wire().unwrap()).unwrap();
        assert_eq!(parsed.message_expiry, None);
        assert_eq!(parsed.max_topic_size, None);
        assert_eq!(parsed.segment_size, None);
    }

    #[test]
    fn sentinel_zeros_do_not_survive_re_encoding() {
        // A client may send 0 to mean "resolve the default". Parsing normalizes
        // it to absent, so admission puts the resolved value in the derived
        // block -- but apply merges with explicit winning. If the literal 0
        // were still in the explicit block it would land back on top as the
        // stored effective value, and a restart would re-parse it to absent and
        // fall back to the node default rather than the value resolved at
        // creation. Re-encoding from the parse is what drops it.
        let sent = TopicCreateOptions {
            message_expiry: Some(IggyExpiry::from(0u64)),
            max_topic_size: Some(MaxTopicSize::from(0u64)),
            segment_size: Some(IggyByteSize::from(0u64)),
            size_of_messages_required_to_save: Some(IggyByteSize::from(0u64)),
            enforce_fsync: Some(true),
            ..TopicCreateOptions::default()
        };
        let parsed = TopicCreateOptions::parse(&sent.to_wire().unwrap()).unwrap();
        let re_encoded = parsed.to_wire().unwrap();
        let stored =
            crate::wire_conversions::resource_options_from_wire(&re_encoded, true).unwrap();

        for key in [
            topic_option_keys::MESSAGE_EXPIRY,
            topic_option_keys::MAX_TOPIC_SIZE,
            topic_option_keys::SEGMENT_SIZE,
            topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
        ] {
            assert!(
                !stored.contains_key(&HeaderKey::from_str(key).unwrap()),
                "{key} sentinel must not be persisted as an explicit value"
            );
        }
        // A non-sentinel key alongside them still rides through untouched.
        assert!(
            stored.contains_key(&HeaderKey::from_str(topic_option_keys::ENFORCE_FSYNC).unwrap())
        );
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
            crate::wire_conversions::resource_options_from_wire(&options.to_wire().unwrap(), true)
                .unwrap();
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
        let parsed = TopicCreateOptions::parse(&options.to_wire().unwrap()).unwrap();
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
            TopicCreateOptions::parse(&options.to_wire().unwrap()),
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
        let parsed = TopicCreateOptions::parse(&options.to_wire().unwrap()).unwrap();
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
        let parsed = TopicCreateOptions::parse(&options.to_wire().unwrap()).unwrap();
        assert_eq!(parsed.message_expiry, Some(IggyExpiry::from(5_000_000u64)));
        assert_eq!(
            parsed.compression_algorithm,
            Some(CompressionAlgorithm::Gzip)
        );
        assert!(parsed.max_topic_size.is_some());
    }

    #[test]
    fn options_block_budget_matches_user_headers() {
        // `MAX_OPTIONS_BYTES` duplicates `MAX_USER_HEADERS_SIZE` because
        // `iggy_binary_protocol` cannot import this crate. This is the only
        // place both are visible, so it is where they get tied together.
        assert_eq!(
            iggy_binary_protocol::MAX_OPTIONS_BYTES,
            crate::MAX_USER_HEADERS_SIZE as usize,
            "options inherit the user-headers byte budget; update both"
        );
    }

    #[test]
    fn max_options_is_reachable_within_the_byte_budget() {
        // The cheapest entry is 12 bytes: kind + u32 length + one byte, for
        // key and value each. If the budget ever drops below this product the
        // entry cap becomes unreachable and `MAX_OPTIONS` stops being the
        // limit that actually binds.
        const MIN_ENTRY_BYTES: usize = 2 * (1 + 4 + 1);
        assert!(
            iggy_binary_protocol::MAX_OPTIONS as usize * MIN_ENTRY_BYTES
                <= iggy_binary_protocol::MAX_OPTIONS_BYTES,
            "MAX_OPTIONS entries must fit in MAX_OPTIONS_BYTES"
        );
    }

    #[test]
    fn flush_threshold_ceilings_are_derived_from_the_segment_maximum() {
        // A threshold no segment can reach never trips, so committed messages
        // sit in the journal - which a crash does not preserve - until the
        // segment rotates. The message ceiling follows from the byte one: a
        // message costs at least its header on disk.
        assert_eq!(
            MAX_MESSAGES_REQUIRED_TO_SAVE as u64,
            MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE
                / crate::IGGY_MESSAGE_HEADER_SIZE
                    .try_into()
                    .unwrap_or(u64::MAX),
            "message ceiling must stay derived from the byte ceiling"
        );
    }

    #[test]
    fn parse_rejects_flush_thresholds_above_their_ceilings() {
        let over_messages = TopicCreateOptions {
            raw: BTreeMap::from([(
                topic_option_keys::MESSAGES_REQUIRED_TO_SAVE.to_string(),
                (u64::from(MAX_MESSAGES_REQUIRED_TO_SAVE) + 1).to_string(),
            )]),
            ..TopicCreateOptions::default()
        };
        assert!(
            TopicCreateOptions::parse(&over_messages.to_wire().unwrap()).is_err(),
            "a message threshold above the ceiling must be refused"
        );

        let over_bytes = TopicCreateOptions {
            raw: BTreeMap::from([(
                topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE.to_string(),
                (MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE + 1).to_string(),
            )]),
            ..TopicCreateOptions::default()
        };
        assert!(
            TopicCreateOptions::parse(&over_bytes.to_wire().unwrap()).is_err(),
            "a byte threshold above the ceiling must be refused"
        );

        let at_ceiling = TopicCreateOptions {
            messages_required_to_save: Some(MAX_MESSAGES_REQUIRED_TO_SAVE),
            size_of_messages_required_to_save: Some(IggyByteSize::from(
                MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
            )),
            ..TopicCreateOptions::default()
        };
        assert!(
            TopicCreateOptions::parse(&at_ceiling.to_wire().unwrap()).is_ok(),
            "the ceiling itself must remain settable"
        );
    }

    #[test]
    fn preallocation_cap_bounds_segment_size_times_partitions() {
        assert!(validate_preallocated_topic_bytes(DEFAULT_SEGMENT_SIZE, 64).is_ok());
        assert!(validate_preallocated_topic_bytes(DEFAULT_SEGMENT_SIZE, 65).is_err());
        // The product saturates rather than wrapping into a passing value.
        assert!(validate_preallocated_topic_bytes(u64::MAX, u32::MAX).is_err());
    }
}
