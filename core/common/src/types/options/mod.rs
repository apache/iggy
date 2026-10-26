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

//! Key-value options of streams, topics, and users: set at creation, changed
//! later with an `UpdateResource` patch.
//!
//! Options reuse the typed [`HeaderKey`] / [`HeaderValue`] machinery from
//! message user headers. The map MUST stay a `BTreeMap`: metadata snapshots
//! require deterministic ordering across replicas, and a hash map would make
//! snapshot bytes diverge per replica.
//!
//! # SDK parity
//!
//! Every SDK sends catalog keys through typed create and update options, names
//! resets with a typed key enum, reads both option blocks back and asks the
//! server which keys it accepts with `describe_options`. A key newer than the
//! build travels through the raw escape hatch, string-keyed entries and reset
//! names that only server admission judges:
//!
//! | SDK    | Typed options                                           | Raw entries and resets          |
//! |--------|---------------------------------------------------------|---------------------------------|
//! | Rust   | `StreamCreateOptions`, `TopicUpdateOptions` and kin     | `raw`, `raw_reset`              |
//! | Node   | `StreamOptions`, `UpdateTopic` and kin                  | `options`, `rawReset`           |
//! | Python | `StreamOptions`, `TopicOptions`, `update_*` keywords    | `raw` or `options`, `raw_reset` |
//! | Go     | `StreamOptions`, `TopicUpdate` and kin                  | `Raw` or `Options`, `RawReset`  |
//! | Java   | `StreamOptions`, `TopicUpdate` and kin                  | `raw`, `rawReset`               |
//! | C#     | `StreamOptions`, `TopicUpdate` and kin                  | `Options`, `RawReset`           |
//! | C++    | `StreamCreateOptions`, `TopicUpdateOptions` and kin     | `SetRawEntries()`, `RawReset()` |
//! | Swift  | `StreamCreateOptions`, `TopicUpdateOptions` and kin     | `raw`, `rawReset`               |
//! | PHP    | named arguments of `createTopic`, `updateTopic` and kin | `$raw`, `$raw_reset`            |
//!
//! REST carries each value as reversible text with its kind, so an HTTP reader
//! rebuilds the value a binary reader gets. It reports both provenances in one
//! map with a per-entry flag, which each SDK splits into the two maps its
//! binary path produces.
//!
//! The byte layout of every option-carrying request and response is pinned by
//! the `resource_options.json` fixture, which Rust and all eight foreign SDKs
//! assert independently, so a new encoder has a fixture to match rather than
//! a description to interpret.

mod catalog;
mod codec;
mod durability;
mod key;
mod partition_resize_policy;

pub use catalog::{
    AdmittedOptions, CATALOG, OPTION_FLAG_CRITICAL, OPTION_FLAG_READ_ONLY, OPTION_FLAG_RESETTABLE,
    OPTION_FLAG_SECRET, OPTION_FLAG_UPDATABLE, OptionAdmissionContext, OptionColumns,
    OptionDefault, OptionDescriptor, OptionMutability, OptionOperation, OptionStorage,
    OptionVisibility, StreamEncryptionSource, admit, canonical_value, check_critical_options,
    critical_keys_marker, derived_defaults, descriptors_for, public_options, stream_key_from_entry,
    stream_wrap_set,
};
pub use codec::OptionValueCodec;
pub use durability::Durability;
pub use key::OptionKey;
pub use partition_resize_policy::PartitionResizePolicy;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

use bytes::Bytes;
use iggy_binary_protocol::requests::system::{MAX_RESET_KEYS, WireResetKeys};
use iggy_binary_protocol::{WireName, WireOptions, WireUserHeaderEntry};
use secrecy::zeroize::Zeroize;
use serde::{Deserialize, Serialize};

use self::catalog::critical_wrap_set;
use crate::http::options::{OptionInputValue, OptionsUpdate, RESET_FIELD};
use crate::types::compression::compression_algorithm::CompressionAlgorithm;
use crate::types::message::{HeaderKey, HeaderKind, HeaderValue};
use crate::utils::stream_key::{StreamEncryptionKey, WrapSet};
use crate::{IggyByteSize, IggyError, IggyExpiry, MaxTopicSize, UserStatus};

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

    /// Zero the value's bytes when this is their only handle, for a value that
    /// may be a secret. Shared bytes are left to their other owners.
    pub fn erase(self) {
        let bytes = self.value.value();
        drop(self);
        erase_buffer(bytes);
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
    use super::{HeaderKey, HeaderKind, HeaderValue, OptionValue, ResourceOptions};
    use crate::IggyError;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;
    use std::str::FromStr;

    /// One entry's JSON form: the value as text, its kind, and its provenance.
    ///
    /// The text is reversible for every kind, so an HTTP reader rebuilds the
    /// exact value a binary reader gets: UTF-8 for strings, `true`/`false`,
    /// base-10 integers, standard padded base64 for raw bytes, and `0x`
    /// followed by the IEEE-754 bits in fixed-width lowercase hex for floats.
    #[derive(Serialize, Deserialize)]
    struct OptionValueJson {
        value: String,
        kind: HeaderKind,
        explicit: bool,
    }

    /// # Errors
    ///
    /// Propagates the serializer's own errors, and a stored value whose bytes
    /// do not match its kind.
    pub fn serialize<S: Serializer>(
        options: &ResourceOptions,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let readable = options
            .iter()
            .map(|(key, option)| {
                Ok((
                    String::from_utf8_lossy(key.as_bytes()).into_owned(),
                    OptionValueJson {
                        value: encode_value(&option.value).map_err(serde::ser::Error::custom)?,
                        kind: option.value.kind(),
                        explicit: option.explicit,
                    },
                ))
            })
            .collect::<Result<BTreeMap<String, OptionValueJson>, S::Error>>()?;
        readable.serialize(serializer)
    }

    /// # Errors
    ///
    /// Returns a deserializer error when a key is not a valid option name or a
    /// value's text does not decode as its kind.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ResourceOptions, D::Error> {
        let readable = BTreeMap::<String, OptionValueJson>::deserialize(deserializer)?;
        readable
            .into_iter()
            .map(|(key, option)| {
                let key = HeaderKey::from_str(&key).map_err(D::Error::custom)?;
                let value = decode_value(option.kind, &option.value).map_err(D::Error::custom)?;
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

    /// The reversible text of `value` for its kind.
    ///
    /// # Errors
    ///
    /// `InvalidHeaderValue` when the bytes do not match the kind's size or
    /// encoding.
    pub fn encode_value(value: &HeaderValue) -> Result<String, IggyError> {
        let bytes = value.as_bytes();
        Ok(match value.kind() {
            HeaderKind::Raw => STANDARD.encode(bytes),
            HeaderKind::String => value.as_str()?.to_owned(),
            HeaderKind::Bool => value.as_bool()?.to_string(),
            HeaderKind::Int8 => value.as_int8()?.to_string(),
            HeaderKind::Int16 => value.as_int16()?.to_string(),
            HeaderKind::Int32 => value.as_int32()?.to_string(),
            HeaderKind::Int64 => value.as_int64()?.to_string(),
            HeaderKind::Int128 => value.as_int128()?.to_string(),
            HeaderKind::Uint8 => value.as_uint8()?.to_string(),
            HeaderKind::Uint16 => value.as_uint16()?.to_string(),
            HeaderKind::Uint32 => value.as_uint32()?.to_string(),
            HeaderKind::Uint64 => value.as_uint64()?.to_string(),
            HeaderKind::Uint128 => value.as_uint128()?.to_string(),
            HeaderKind::Float32 => format!("0x{:08x}", value.as_float32()?.to_bits()),
            HeaderKind::Float64 => format!("0x{:016x}", value.as_float64()?.to_bits()),
        })
    }

    /// The value `text` encodes for `kind`, the inverse of [`encode_value`].
    ///
    /// # Errors
    ///
    /// `InvalidHeaderValue` when the text is not the canonical form of a value
    /// of that kind.
    pub fn decode_value(kind: HeaderKind, text: &str) -> Result<HeaderValue, IggyError> {
        let value = match kind {
            HeaderKind::Raw => {
                HeaderValue::from_raw(kind, &STANDARD.decode(text).map_err(invalid)?)?
            }
            HeaderKind::String => HeaderValue::try_from(text)?,
            HeaderKind::Bool => match text {
                "true" => HeaderValue::from(true),
                "false" => HeaderValue::from(false),
                _ => return Err(IggyError::InvalidHeaderValue),
            },
            HeaderKind::Int8 => HeaderValue::from(text.parse::<i8>().map_err(invalid)?),
            HeaderKind::Int16 => HeaderValue::from(text.parse::<i16>().map_err(invalid)?),
            HeaderKind::Int32 => HeaderValue::from(text.parse::<i32>().map_err(invalid)?),
            HeaderKind::Int64 => HeaderValue::from(text.parse::<i64>().map_err(invalid)?),
            HeaderKind::Int128 => HeaderValue::from(text.parse::<i128>().map_err(invalid)?),
            HeaderKind::Uint8 => HeaderValue::from(text.parse::<u8>().map_err(invalid)?),
            HeaderKind::Uint16 => HeaderValue::from(text.parse::<u16>().map_err(invalid)?),
            HeaderKind::Uint32 => HeaderValue::from(text.parse::<u32>().map_err(invalid)?),
            HeaderKind::Uint64 => HeaderValue::from(text.parse::<u64>().map_err(invalid)?),
            HeaderKind::Uint128 => HeaderValue::from(text.parse::<u128>().map_err(invalid)?),
            HeaderKind::Float32 => HeaderValue::from(f32::from_bits(
                u32::try_from(float_bits(text, 8)?).map_err(invalid)?,
            )),
            HeaderKind::Float64 => HeaderValue::from(f64::from_bits(float_bits(text, 16)?)),
        };
        // Reject non-canonical text (a leading `+`, padding variants, leading
        // zeros) so one value has one JSON form.
        if encode_value(&value)? != text {
            return Err(IggyError::InvalidHeaderValue);
        }
        Ok(value)
    }

    fn invalid<E>(_: E) -> IggyError {
        IggyError::InvalidHeaderValue
    }

    fn float_bits(text: &str, digits: usize) -> Result<u64, IggyError> {
        let hex = text
            .strip_prefix("0x")
            .filter(|hex| hex.len() == digits)
            .ok_or(IggyError::InvalidHeaderValue)?;
        u64::from_str_radix(hex, 16).map_err(|_| IggyError::InvalidHeaderValue)
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
    /// Message completion policy: `String`, either `replicated` or `persisted`.
    pub const DURABILITY: &str = "durability";
    pub const CONSUMER_OFFSET_DURABILITY: &str = "consumer_offset_durability";
    /// Immutable partition-set policy: `String`, either `mutable` or `fixed`.
    pub const PARTITION_RESIZE_POLICY: &str = "partition_resize_policy";
    /// Flush the journal once it holds this many messages: `Uint32`.
    /// Must be non-zero.
    pub const MESSAGES_REQUIRED_TO_SAVE: &str = "messages_required_to_save";
    /// Flush the journal once it holds this many bytes: `Uint64` or a
    /// byte-size string. Paired with [`MESSAGES_REQUIRED_TO_SAVE`];
    /// whichever threshold trips first flushes.
    pub const SIZE_OF_MESSAGES_REQUIRED_TO_SAVE: &str = "size_of_messages_required_to_save";
    /// Reserve the segment's bytes up front on a filesystem that supports it:
    /// `Bool`, or the strings `true` / `false`. Pairs with
    /// [`SEGMENT_SIZE`] -- preallocation reserves exactly that much, so
    /// the two only make sense decided together.
    pub const PREALLOCATE_SEGMENTS: &str = "preallocate_segments";
}

/// Values an absent topic option resolves to at admission.
///
/// These defaults are fixed by the option catalog, not server configuration.
/// Values the client omits are resolved at admission and persisted as derived
/// entries, so `GetTopic` reports the effective values and their provenance.
pub const DEFAULT_PARTITIONS_COUNT: u32 = 1;
/// `MaxTopicSize::Unlimited` (was `[topic] max_size = "unlimited"`).
pub const DEFAULT_MAX_TOPIC_SIZE: u64 = u64::MAX;
/// `IggyExpiry::NeverExpire` (was `[topic] message_expiry = "none"`).
pub const DEFAULT_MESSAGE_EXPIRY: u64 = u64::MAX;
/// 1 GiB.
pub const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;
/// Was `[partition] messages_required_to_save = 1024`.
pub const DEFAULT_MESSAGES_REQUIRED_TO_SAVE: u32 = 1024;
/// Preallocation is opt-in.
///
/// Each owned partition requests `segment_size` bytes when opening a segment.
/// Linux uses `FALLOC_FL_KEEP_SIZE` to reserve extents without extending the
/// logical file. Unsupported or failed reservations fall back to ordinary
/// allocation with a warning.
pub const DEFAULT_PREALLOCATE_SEGMENTS: bool = false;
/// 1 MiB (was `[partition] size_of_messages_required_to_save`).
pub const DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE: u64 = 1024 * 1024;

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
/// Admission uses [`MAX_TOPIC_SEGMENT_SIZE`] as `ceiling`. Server startup
/// separately validates that state-transfer budgets can hold a segment at
/// that ceiling plus one whole batch of overshoot.
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

/// Zero every value of `options` this map holds the only handle to, for a map
/// that may hold a secret value. A value another handle shares is left to it.
pub fn erase_options(options: ResourceOptions) {
    options.into_values().for_each(OptionValue::erase);
}

/// Zero `buffer` when this is its only handle, for a buffer that may hold a
/// secret. A shared buffer is left to its other owners.
pub fn erase_buffer(buffer: Bytes) {
    if let Ok(mut owned) = buffer.try_into_mut() {
        owned.as_mut().zeroize();
    }
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
    /// Canonical kind for this key: what the server encodes its default under,
    /// and what a value set at CREATE is stored as whatever kind the client sent
    /// it as. Create admission re-encodes the block from its own parse, so a
    /// `--set segment_size=128MiB` string lands as a `Uint64`.
    ///
    /// Updates canonicalize the same way.
    pub kind: HeaderKind,
    /// An update may set the key.
    #[serde(default)]
    pub updatable: bool,
    /// An update may reset the key to its default.
    #[serde(default)]
    pub resettable: bool,
    /// Write-only: accepted, never returned.
    #[serde(default)]
    pub secret: bool,
    /// Computed by the server and returned on reads, never accepted.
    #[serde(default)]
    pub read_only: bool,
    /// A node that does not understand the key must not serve the resource.
    #[serde(default)]
    pub critical: bool,
    /// The key's default in `kind`'s encoding; empty when the key has no
    /// default. A build constant rather than a per-node value: these defaults
    /// stopped being config-derived when the `[system.*]` keys became options.
    #[serde(default)]
    pub default_value: Vec<u8>,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}

/// Longest key echoed back in an option error. A rejected key is attacker-sized
/// by definition, and over HTTP the error text rides the response body back to
/// the client.
///
/// Binary transports carry only the error code, so a TCP, QUIC or WebSocket
/// client rebuilds [`IggyError::UnsupportedOptionKey`] with an empty string and
/// never sees which key was refused. `DescribeOptions` is the discovery path
/// there - see the note on that variant.
const ERROR_KEY_PREVIEW_LEN: usize = 64;

pub(crate) fn key_preview(key: &str) -> String {
    key.chars().take(ERROR_KEY_PREVIEW_LEN).collect()
}

/// Reset names in wire form.
///
/// # Errors
///
/// `UnsupportedOptionKey` for a name no option can have, and for a list longer
/// than the wire carries, naming one of its names outside the catalog.
pub(crate) fn wire_reset_names<'a>(
    names: impl Iterator<Item = &'a str>,
) -> Result<WireResetKeys, IggyError> {
    let reset = names
        .map(|name| {
            WireName::new(name).map_err(|_| IggyError::UnsupportedOptionKey(key_preview(name)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if reset.len() > MAX_RESET_KEYS {
        // The catalog is far smaller than the limit, so such a list always
        // names a key admission would refuse first.
        return Err(reset
            .iter()
            .find(|name| OptionKey::from_str(name.as_str()).is_err())
            .map_or(IggyError::InvalidCommand, |name| {
                IggyError::UnsupportedOptionKey(key_preview(name.as_str()))
            }));
    }
    WireResetKeys::try_from(reset).map_err(|_| IggyError::InvalidCommand)
}

/// Build an options map from string key-values, all marked explicit.
///
/// Callers that also have typed fields insert those afterwards, so a typed
/// value wins on collision and keeps its canonical kind.
///
/// # Errors
///
/// `UnsupportedOptionKey` when a key is empty or over 255 bytes,
/// `InvalidOptionValue` when a value is. Both bounds come from the
/// header-field codec these entries ride.
fn raw_options_map(raw: &BTreeMap<String, String>) -> Result<ResourceOptions, IggyError> {
    let mut options = ResourceOptions::new();
    for (key, value) in raw {
        let entry = HeaderKey::from_str(key)
            .map_err(|_| IggyError::UnsupportedOptionKey(key_preview(key)))
            .and_then(|header_key| {
                HeaderValue::try_from(value.as_str())
                    .map(|header_value| (header_key, header_value))
                    .map_err(|_| IggyError::InvalidOptionValue(key_preview(key)))
            });
        match entry {
            Ok((header_key, header_value)) => {
                options.insert(header_key, OptionValue::explicit(header_value));
            }
            Err(error) => {
                // A raw value may be a secret, the stream key among them.
                erase_options(options);
                return Err(error);
            }
        }
    }
    Ok(options)
}

/// An update's options in wire form: the keys to set and the keys to reset
/// to their defaults. Every other key keeps its stored value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionsPatch {
    pub set: WireOptions,
    pub reset: WireResetKeys,
}

/// The options a `CreateStream` may carry.
#[derive(Clone, Default, PartialEq)]
pub struct StreamCreateOptions {
    /// The stream's own data encryption key. `None` follows the server
    /// encryption setting. The server stores the key wrapped and never
    /// returns it, reads report only its source and fingerprint.
    pub encryption_key: Option<StreamEncryptionKey>,
    /// Keys sent as `String` values, for keys newer than this build. A typed
    /// field wins on collision.
    pub raw: BTreeMap<String, String>,
}

/// The options an update may change on a stream. Absent keys keep their
/// stored values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamUpdateOptions {
    /// `None` keeps the stream's name.
    pub name: Option<String>,
    /// Keys to reset to their defaults. No stream key is resettable yet, so a
    /// non-empty set is refused before anything is sent.
    pub reset: BTreeSet<OptionKey>,
    /// Keys to reset by name, for keys newer than this build. The server
    /// decides whether each one may be reset.
    pub raw_reset: BTreeSet<String>,
    /// Keys sent as `String` values, for updatable keys newer than this build.
    pub raw: BTreeMap<String, String>,
}

/// The options an update may change on a topic. Absent keys keep their stored
/// values. Create-only settings (compression, `segment_size`, durability,
/// flush thresholds, preallocation, resize policy) have no field here, so a
/// caller cannot send one only to learn at the server that it was refused.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopicUpdateOptions {
    /// `None` keeps the topic's name.
    pub name: Option<String>,
    /// `None` keeps the topic's current expiry. `ServerDefault` changes
    /// nothing, use `reset` to return to the default.
    pub message_expiry: Option<IggyExpiry>,
    /// `None` keeps the topic's current cap. `ServerDefault` changes nothing,
    /// use `reset` to return to the default.
    pub max_topic_size: Option<MaxTopicSize>,
    /// Keys to reset to their defaults: `message_expiry` or `max_topic_size`.
    pub reset: BTreeSet<OptionKey>,
    /// Keys to reset by name, for keys newer than this build. The server
    /// decides whether each one may be reset.
    pub raw_reset: BTreeSet<String>,
    /// Keys sent as `String` values, for updatable keys newer than this build.
    pub raw: BTreeMap<String, String>,
}

/// The options a `CreateUser` may carry. No user key exists yet, the type
/// exists so the first one costs a field, not a signature change.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserCreateOptions {
    /// Keys sent as `String` values, for keys newer than this build.
    pub raw: BTreeMap<String, String>,
}

/// The options an update may change on a user. Absent keys keep their stored
/// values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserUpdateOptions {
    /// `None` keeps the username.
    pub username: Option<String>,
    /// `None` keeps the status. Deactivating a user ends its sessions.
    pub status: Option<UserStatus>,
    /// Keys to reset to their defaults. No user key is resettable yet, so a
    /// non-empty set is refused before anything is sent.
    pub reset: BTreeSet<OptionKey>,
    /// Keys to reset by name, for keys newer than this build. The server
    /// decides whether each one may be reset.
    pub raw_reset: BTreeSet<String>,
    /// Keys sent as `String` values, for updatable keys newer than this build.
    pub raw: BTreeMap<String, String>,
}

/// How a stream's messages are stored, read from its committed options.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEncryption {
    /// No wrap set: the stream is plaintext for life.
    Plaintext,
    /// Encrypted under the key the wrap set holds.
    Wrapped(WrapSet),
    /// Critical state this build cannot read. The stream is never served as
    /// plaintext then.
    Unavailable(IggyError),
}

/// A topic's effective settings, every one resolved: the stored value, or the
/// catalog default for a key the map does not hold.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicResolvedOptions {
    pub compression_algorithm: CompressionAlgorithm,
    pub message_expiry: IggyExpiry,
    pub max_topic_size: MaxTopicSize,
    pub partition_resize_policy: PartitionResizePolicy,
    /// The partition-facing knobs, with the shard-default semantics of
    /// [`TopicRuntimeOptions`].
    pub runtime: TopicRuntimeOptions,
}

impl StreamCreateOptions {
    /// Encode the present keys into a create options block.
    ///
    /// # Errors
    ///
    /// See `raw_options_map`.
    pub fn to_wire(&self) -> Result<WireOptions, IggyError> {
        let options = self.to_option_map()?;
        let wire =
            crate::wire_conversions::resource_options_to_wire(&options, OptionsProvenance::All);
        erase_options(options);
        wire
    }

    /// The present keys as string values for an HTTP body. The key travels as
    /// canonical standard base64.
    ///
    /// # Errors
    ///
    /// See `raw_options_map`.
    pub fn to_input_map(&self) -> Result<BTreeMap<String, OptionInputValue>, IggyError> {
        let options = self.to_option_map()?;
        let input = option_input_map(&options);
        erase_options(options);
        input
    }

    /// The present keys as a map that holds the key itself, so callers erase it.
    fn to_option_map(&self) -> Result<ResourceOptions, IggyError> {
        let mut options = raw_options_map(&self.raw)?;
        if let Some(key) = &self.encryption_key {
            match HeaderValue::try_from(key.as_bytes().as_slice()) {
                Ok(value) => {
                    options.insert(
                        HeaderKey::from(OptionKey::EncryptionKey),
                        OptionValue::explicit(value),
                    );
                }
                Err(error) => {
                    erase_options(options);
                    return Err(error);
                }
            }
        }
        Ok(options)
    }
}

impl StreamUpdateOptions {
    /// Encode the patch for `UpdateResource`.
    ///
    /// # Errors
    ///
    /// `InvalidCommand` when the patch changes nothing or names a key both
    /// set and reset, the server's admission error for a key that cannot be
    /// reset, `InvalidStreamName` for a name that does not fit.
    pub fn to_patch(&self) -> Result<OptionsPatch, IggyError> {
        options_patch(
            OptionsScope::Stream,
            self.to_option_map()?,
            &self.reset,
            &self.raw_reset,
            !self.raw.is_empty(),
        )
    }

    /// The patch as an HTTP update body: set keys with their values, reset keys
    /// in the `reset` array.
    ///
    /// # Errors
    ///
    /// See [`Self::to_patch`].
    pub fn to_update_body(&self) -> Result<OptionsUpdate, IggyError> {
        update_body(
            OptionsScope::Stream,
            self.to_option_map()?,
            &self.reset,
            &self.raw_reset,
            !self.raw.is_empty(),
        )
    }

    fn to_option_map(&self) -> Result<ResourceOptions, IggyError> {
        let mut options = raw_options_map(&self.raw)?;
        if let Some(name) = &self.name {
            insert_text(&mut options, OptionKey::Name, name);
        }
        Ok(options)
    }
}

impl TopicUpdateOptions {
    /// Encode the patch for `UpdateResource`, in canonical kinds.
    ///
    /// # Errors
    ///
    /// `InvalidCommand` when the patch changes nothing or names a key both
    /// set and reset, `OptionNotUpdatable` for a create-only key,
    /// `OptionNotResettable` for a key without a default, `InvalidTopicName`
    /// for a name that does not fit.
    pub fn to_patch(&self) -> Result<OptionsPatch, IggyError> {
        options_patch(
            OptionsScope::Topic,
            self.to_option_map()?,
            &self.reset,
            &self.raw_reset,
            !self.raw.is_empty(),
        )
    }

    /// The patch as an HTTP update body.
    ///
    /// # Errors
    ///
    /// See [`Self::to_patch`].
    pub fn to_update_body(&self) -> Result<OptionsUpdate, IggyError> {
        update_body(
            OptionsScope::Topic,
            self.to_option_map()?,
            &self.reset,
            &self.raw_reset,
            !self.raw.is_empty(),
        )
    }

    fn to_option_map(&self) -> Result<ResourceOptions, IggyError> {
        let mut options = raw_options_map(&self.raw)?;
        if let Some(name) = &self.name {
            insert_text(&mut options, OptionKey::Name, name);
        }
        if let Some(message_expiry) = self.message_expiry
            && message_expiry != IggyExpiry::ServerDefault
        {
            options.insert(
                HeaderKey::from(OptionKey::MessageExpiry),
                OptionValue::explicit(message_expiry.encode()?),
            );
        }
        if let Some(max_topic_size) = self.max_topic_size
            && max_topic_size != MaxTopicSize::ServerDefault
        {
            options.insert(
                HeaderKey::from(OptionKey::MaxTopicSize),
                OptionValue::explicit(max_topic_size.encode()?),
            );
        }
        Ok(options)
    }
}

impl UserCreateOptions {
    /// Encode the present keys into a create options block.
    ///
    /// # Errors
    ///
    /// See `raw_options_map`.
    pub fn to_wire(&self) -> Result<WireOptions, IggyError> {
        crate::wire_conversions::resource_options_to_wire(
            &raw_options_map(&self.raw)?,
            OptionsProvenance::All,
        )
    }

    /// The present keys as values for an HTTP body.
    ///
    /// # Errors
    ///
    /// See `raw_options_map`.
    pub fn to_input_map(&self) -> Result<BTreeMap<String, OptionInputValue>, IggyError> {
        option_input_map(&raw_options_map(&self.raw)?)
    }
}

impl UserUpdateOptions {
    /// Encode the patch for `UpdateResource`.
    ///
    /// # Errors
    ///
    /// `InvalidCommand` when the patch changes nothing or names a key both
    /// set and reset, the server's admission error for a key that cannot be
    /// reset, `InvalidUsername` for a username that does not fit.
    pub fn to_patch(&self) -> Result<OptionsPatch, IggyError> {
        options_patch(
            OptionsScope::User,
            self.to_option_map()?,
            &self.reset,
            &self.raw_reset,
            !self.raw.is_empty(),
        )
    }

    /// The patch as an HTTP update body.
    ///
    /// # Errors
    ///
    /// See [`Self::to_patch`].
    pub fn to_update_body(&self) -> Result<OptionsUpdate, IggyError> {
        update_body(
            OptionsScope::User,
            self.to_option_map()?,
            &self.reset,
            &self.raw_reset,
            !self.raw.is_empty(),
        )
    }

    fn to_option_map(&self) -> Result<ResourceOptions, IggyError> {
        let mut options = raw_options_map(&self.raw)?;
        if let Some(username) = &self.username {
            insert_text(&mut options, OptionKey::Username, username);
        }
        if let Some(status) = self.status {
            options.insert(
                HeaderKey::from(OptionKey::Status),
                OptionValue::explicit(status.encode()?),
            );
        }
        Ok(options)
    }
}

impl From<&ResourceOptions> for StreamEncryption {
    fn from(options: &ResourceOptions) -> Self {
        match critical_wrap_set(options) {
            Ok(Some(set)) => Self::Wrapped(set),
            Ok(None) => Self::Plaintext,
            Err(error) => Self::Unavailable(error),
        }
    }
}

impl From<&ResourceOptions> for TopicResolvedOptions {
    /// Degrades per key: an entry this build cannot read resolves to its
    /// catalog default and leaves every other setting intact.
    fn from(options: &ResourceOptions) -> Self {
        let parsed = TopicCreateOptions::from_resource_options(options);
        Self {
            compression_algorithm: parsed.compression_algorithm.unwrap_or_default(),
            message_expiry: parsed
                .message_expiry
                .unwrap_or_else(|| IggyExpiry::from(DEFAULT_MESSAGE_EXPIRY)),
            max_topic_size: parsed
                .max_topic_size
                .unwrap_or_else(|| MaxTopicSize::from(DEFAULT_MAX_TOPIC_SIZE)),
            partition_resize_policy: parsed.partition_resize_policy.unwrap_or_default(),
            runtime: TopicRuntimeOptions::from(&parsed),
        }
    }
}

impl Debug for StreamCreateOptions {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let raw: BTreeMap<&str, &str> = self
            .raw
            .iter()
            .map(|(key, value)| {
                let shown = if key.as_str() == <&str>::from(OptionKey::EncryptionKey) {
                    "<redacted>"
                } else {
                    value.as_str()
                };
                (key.as_str(), shown)
            })
            .collect();
        formatter
            .debug_struct("StreamCreateOptions")
            .field("encryption_key", &self.encryption_key)
            .field("raw", &raw)
            .finish()
    }
}

/// Insert a text column value. Text that no header value holds travels as a
/// value of a kind the column never accepts, so admission reports the
/// column's own error after every check that comes before it.
fn insert_text(options: &mut ResourceOptions, key: OptionKey, text: &str) {
    let value = HeaderValue::try_from(text).unwrap_or_else(|_| HeaderValue::from(false));
    options.insert(HeaderKey::from(key), OptionValue::explicit(value));
}

/// Validate a patch for `scope` and encode its set and reset parts.
fn options_patch(
    scope: OptionsScope,
    set: ResourceOptions,
    reset: &BTreeSet<OptionKey>,
    raw_reset: &BTreeSet<String>,
    has_raw_set: bool,
) -> Result<OptionsPatch, IggyError> {
    let reset = validate_patch(scope, &set, reset, raw_reset, has_raw_set)?;
    Ok(OptionsPatch {
        set: crate::wire_conversions::resource_options_to_wire(&set, OptionsProvenance::All)?,
        reset: wire_reset_names(reset.iter().map(String::as_str))?,
    })
}

fn update_body(
    scope: OptionsScope,
    set: ResourceOptions,
    reset: &BTreeSet<OptionKey>,
    raw_reset: &BTreeSet<String>,
    has_raw_set: bool,
) -> Result<OptionsUpdate, IggyError> {
    let reset = validate_patch(scope, &set, reset, raw_reset, has_raw_set)?;
    let set = option_input_map(&set)?;
    if set.contains_key(RESET_FIELD) {
        return Err(IggyError::UnsupportedOptionKey(RESET_FIELD.to_owned()));
    }
    Ok(OptionsUpdate {
        set,
        reset: reset.into_iter().collect(),
    })
}

/// Check a patch the way server admission does and return its reset names.
///
/// A patch with raw entries gets only the structural checks, because the
/// server's catalog may know keys or flags this build does not. The server
/// decides the rest, so a refusal never reads differently on either side.
fn validate_patch(
    scope: OptionsScope,
    set: &ResourceOptions,
    reset: &BTreeSet<OptionKey>,
    raw_reset: &BTreeSet<String>,
    has_raw_set: bool,
) -> Result<Vec<String>, IggyError> {
    let names: Vec<String> = reset
        .iter()
        .map(ToString::to_string)
        .chain(raw_reset.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if has_raw_set || !raw_reset.is_empty() {
        let overlaps = names
            .iter()
            .any(|name| set.keys().any(|key| key.as_bytes() == name.as_bytes()));
        if overlaps || (set.is_empty() && names.is_empty()) {
            return Err(IggyError::InvalidCommand);
        }
        return Ok(names);
    }
    let block = crate::wire_conversions::resource_options_to_wire(set, OptionsProvenance::All)?;
    admit(
        scope,
        OptionOperation::Update,
        &block,
        &names,
        &OptionAdmissionContext::default(),
    )?;
    Ok(names)
}

fn option_input_map(
    options: &ResourceOptions,
) -> Result<BTreeMap<String, OptionInputValue>, IggyError> {
    options
        .iter()
        .map(|(key, option)| {
            Ok((
                String::from_utf8_lossy(key.as_bytes()).into_owned(),
                OptionInputValue::try_from(&option.value)?,
            ))
        })
        .collect()
}

/// A topic's resolved runtime knobs, as carried from the metadata plane to
/// each of its partitions. An unset segment size uses `DEFAULT_SEGMENT_SIZE`;
/// other unset fields keep their shard defaults. Topics without an options
/// block (simulator, unit tests) have no resolved values to carry.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TopicRuntimeOptions {
    pub segment_size: Option<IggyByteSize>,
    pub durability: Durability,
    pub consumer_offset_durability: Durability,
    pub messages_required_to_save: Option<u32>,
    pub size_of_messages_required_to_save: Option<IggyByteSize>,
    pub preallocate_segments: Option<bool>,
}

impl TopicRuntimeOptions {
    #[must_use]
    pub fn effective_segment_size(self) -> IggyByteSize {
        self.segment_size
            .unwrap_or_else(|| IggyByteSize::from(DEFAULT_SEGMENT_SIZE))
    }

    /// Derive the runtime knobs from a topic's persisted options map.
    ///
    /// Degrades per key, not per map. An entry this build cannot interpret
    /// leaves its own knob unset and every other knob intact, so one key a
    /// newer node wrote cannot silently drop a topic's durability or
    /// reset its segment size along with it.
    #[must_use]
    pub fn from_resource_options(options: &ResourceOptions) -> Self {
        Self::from(&TopicCreateOptions::from_resource_options(options))
    }
}

impl From<&TopicCreateOptions> for TopicRuntimeOptions {
    fn from(parsed: &TopicCreateOptions) -> Self {
        Self {
            segment_size: parsed.segment_size,
            durability: parsed.durability,
            consumer_offset_durability: parsed.consumer_offset_durability,
            messages_required_to_save: parsed.messages_required_to_save,
            size_of_messages_required_to_save: parsed.size_of_messages_required_to_save,
            preallocate_segments: parsed.preallocate_segments,
        }
    }
}

/// Typed view of the known topic option keys, parsed from a wire block.
///
/// Optional fields left as `None` resolve from server defaults at admission.
/// The two durability fields instead default independently to `Replicated`
/// and are sent explicitly. Provenance describes the wire request, not whether
/// application code assigned a field after constructing its default value.
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
    /// Per-topic segment size. `None` resolves to 1 GiB at admission.
    /// `0` is normalized to `None`.
    pub segment_size: Option<IggyByteSize>,
    /// Message completion policy, independent of consumer-offset durability.
    pub durability: Durability,
    pub consumer_offset_durability: Durability,
    /// Immutable after creation. `None` resolves to [`PartitionResizePolicy::Mutable`].
    pub partition_resize_policy: Option<PartitionResizePolicy>,
    /// Per-topic message-count flush threshold. `None` resolves to 1024.
    /// `0` is rejected.
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
    pub fn to_explicit_wire(
        &self,
        is_supplied: impl Fn(&str) -> bool,
    ) -> Result<WireOptions, IggyError> {
        let mut options = self.to_option_map()?;
        for key in [
            topic_option_keys::DURABILITY,
            topic_option_keys::CONSUMER_OFFSET_DURABILITY,
        ] {
            if !is_supplied(key) {
                options.remove(&HeaderKey::from_str(key).expect("catalog key is valid"));
            }
        }
        crate::wire_conversions::resource_options_to_wire(&options, OptionsProvenance::All)
    }

    /// Parse a wire options block against the topic catalog.
    ///
    /// # Errors
    ///
    /// `UnsupportedOptionKey` for a key outside the topic catalog,
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
    /// interpret: a key outside the topic catalog, or a catalog key whose
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
        let option = key
            .parse::<OptionKey>()
            .ok()
            .filter(|option| {
                let descriptor = option.descriptor();
                descriptor.applies_to(OptionsScope::Topic)
                    && descriptor.storage == OptionStorage::Map
            })
            .ok_or_else(|| IggyError::UnsupportedOptionKey(key_preview(key)))?;
        match option {
            OptionKey::CompressionAlgorithm => {
                self.compression_algorithm = Some(CompressionAlgorithm::decode(option, entry)?);
            }
            OptionKey::MessageExpiry => {
                let expiry = IggyExpiry::decode(option, entry)?;
                self.message_expiry = (expiry != IggyExpiry::ServerDefault).then_some(expiry);
            }
            OptionKey::MaxTopicSize => {
                let size = MaxTopicSize::decode(option, entry)?;
                self.max_topic_size = (size != MaxTopicSize::ServerDefault).then_some(size);
            }
            OptionKey::SegmentSize => {
                let size = IggyByteSize::decode(option, entry)?;
                self.segment_size = (size.as_bytes_u64() != 0).then_some(size);
            }
            OptionKey::Durability => self.durability = Durability::decode(option, entry)?,
            OptionKey::ConsumerOffsetDurability => {
                self.consumer_offset_durability = Durability::decode(option, entry)?;
            }
            OptionKey::PartitionResizePolicy => {
                self.partition_resize_policy = Some(PartitionResizePolicy::decode(option, entry)?);
            }
            OptionKey::MessagesRequiredToSave => {
                self.messages_required_to_save = Some(u32::decode(option, entry)?);
            }
            OptionKey::SizeOfMessagesRequiredToSave => {
                let size = IggyByteSize::decode(option, entry)?;
                self.size_of_messages_required_to_save = (size.as_bytes_u64() != 0).then_some(size);
            }
            OptionKey::PreallocateSegments => {
                self.preallocate_segments = Some(bool::decode(option, entry)?);
            }
            OptionKey::Name
            | OptionKey::Username
            | OptionKey::Status
            | OptionKey::EncryptionKey
            | OptionKey::EncryptionKeyWrapped
            | OptionKey::EncryptionKeySource
            | OptionKey::EncryptionKeyFingerprint
            | OptionKey::EncryptionKekFingerprint
            | OptionKey::CriticalKeys => {
                return Err(IggyError::UnsupportedOptionKey(key_preview(key)));
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
            durability: self.durability,
            consumer_offset_durability: self.consumer_offset_durability,
            partition_resize_policy: self
                .partition_resize_policy
                .or(defaults.partition_resize_policy),
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
    /// See `raw_options_map`.
    pub fn to_option_map(&self) -> Result<ResourceOptions, IggyError> {
        let mut options = raw_options_map(&self.raw)?;
        if let Some(policy) = self.partition_resize_policy {
            options.insert(
                HeaderKey::from_str(topic_option_keys::PARTITION_RESIZE_POLICY)
                    .expect("catalog key is a valid header key"),
                OptionValue::explicit(HeaderValue::from_str(policy.as_ref())?),
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
        for (key, policy) in [
            (topic_option_keys::DURABILITY, self.durability),
            (
                topic_option_keys::CONSUMER_OFFSET_DURABILITY,
                self.consumer_offset_durability,
            ),
        ] {
            let policy = self.raw.get(key).map_or(Ok(policy), |raw| {
                let raw = raw
                    .parse::<Durability>()
                    .map_err(|_| IggyError::InvalidOptionValue(key.to_owned()))?;
                if policy.is_persisted() && raw != policy {
                    return Err(IggyError::InvalidOptionValue(key.to_owned()));
                }
                Ok(raw)
            })?;
            options.insert(
                HeaderKey::from_str(key).expect("catalog key is valid"),
                OptionValue::explicit(HeaderValue::from_str(policy.as_ref())?),
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

    /// The present keys as HTTP body values, the same set and canonical
    /// values [`Self::to_wire`] encodes.
    ///
    /// # Errors
    ///
    /// See [`Self::to_option_map`].
    pub fn to_input_map(&self) -> Result<BTreeMap<String, OptionInputValue>, IggyError> {
        option_input_map(&self.to_option_map()?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn given_any_single_reset_when_patching_should_match_server_admission() {
        let context = OptionAdmissionContext::default();
        for scope in [
            OptionsScope::Topic,
            OptionsScope::Stream,
            OptionsScope::User,
        ] {
            for key in OptionKey::iter() {
                let typed = options_patch(
                    scope,
                    ResourceOptions::new(),
                    &BTreeSet::from([key]),
                    &BTreeSet::new(),
                    false,
                );
                let server = admit(
                    scope,
                    OptionOperation::Update,
                    &WireOptions::empty(),
                    &[key.to_string()],
                    &context,
                );
                assert_eq!(
                    typed.as_ref().err().map(IggyError::as_code),
                    server.as_ref().err().map(IggyError::as_code),
                    "{scope:?} reset of {key}"
                );
            }
        }
    }

    #[test]
    fn given_raw_key_named_reset_when_building_update_body_should_refuse_unsupported_key() {
        let update = TopicUpdateOptions {
            raw: BTreeMap::from([(RESET_FIELD.to_owned(), "x".to_owned())]),
            ..TopicUpdateOptions::default()
        };
        assert_eq!(
            update.to_update_body(),
            Err(IggyError::UnsupportedOptionKey(RESET_FIELD.to_owned()))
        );
    }

    #[test]
    fn given_raw_reset_when_patching_should_send_it_for_the_server_to_decide() {
        let update = TopicUpdateOptions {
            reset: BTreeSet::from([OptionKey::MessageExpiry]),
            raw_reset: BTreeSet::from(["future_key".to_owned(), "segment_size".to_owned()]),
            ..TopicUpdateOptions::default()
        };
        let names: Vec<String> = update
            .to_patch()
            .expect("the server decides raw resets")
            .reset
            .iter()
            .map(|name| name.as_str().to_owned())
            .collect();
        assert_eq!(names, ["future_key", "message_expiry", "segment_size"]);
        assert_eq!(
            update.to_update_body().expect("body builds").reset,
            BTreeSet::from([
                "future_key".to_owned(),
                "message_expiry".to_owned(),
                "segment_size".to_owned()
            ])
        );
        let overlapping = TopicUpdateOptions {
            raw: BTreeMap::from([("future_key".to_owned(), "1".to_owned())]),
            raw_reset: BTreeSet::from(["future_key".to_owned()]),
            ..TopicUpdateOptions::default()
        };
        assert_eq!(overlapping.to_patch(), Err(IggyError::InvalidCommand));
    }

    #[test]
    fn given_patch_failing_several_checks_when_patching_should_report_the_admission_category() {
        let read_only_raw = StreamUpdateOptions {
            reset: BTreeSet::from([OptionKey::Name]),
            raw: BTreeMap::from([(
                OptionKey::EncryptionKeySource.to_string(),
                "none".to_owned(),
            )]),
            ..StreamUpdateOptions::default()
        };
        let patch = read_only_raw
            .to_patch()
            .expect("the server decides raw keys");
        let reset: Vec<String> = patch
            .reset
            .iter()
            .map(|name| name.as_str().to_owned())
            .collect();
        assert_eq!(
            admit(
                OptionsScope::Stream,
                OptionOperation::Update,
                &patch.set,
                &reset,
                &OptionAdmissionContext::default(),
            )
            .err(),
            Some(IggyError::ReadOnlyOption(
                OptionKey::EncryptionKeySource.to_string()
            ))
        );
        let bad_name_and_reset = TopicUpdateOptions {
            name: Some(String::new()),
            reset: BTreeSet::from([OptionKey::SegmentSize]),
            ..TopicUpdateOptions::default()
        };
        assert_eq!(
            bad_name_and_reset.to_patch(),
            Err(IggyError::OptionNotUpdatable(
                OptionKey::SegmentSize.to_string()
            ))
        );
        let bad_name = TopicUpdateOptions {
            name: Some("n".repeat(256)),
            ..TopicUpdateOptions::default()
        };
        assert_eq!(bad_name.to_patch(), Err(IggyError::InvalidTopicName));
        let short_username = UserUpdateOptions {
            username: Some("a".to_owned()),
            ..UserUpdateOptions::default()
        };
        assert_eq!(short_username.to_patch(), Err(IggyError::InvalidUsername));
    }

    #[test]
    fn given_every_topic_catalog_row_when_creating_should_parse_store_and_default_it() {
        let derived = derived_defaults(OptionsScope::Topic, &ResourceOptions::new())
            .expect("derived defaults build");
        let unset = TopicResolvedOptions::from(&ResourceOptions::new());
        for descriptor in descriptors_for(OptionsScope::Topic).filter(|descriptor| {
            descriptor.storage == OptionStorage::Map
                && descriptor.visibility == OptionVisibility::Public
        }) {
            let key = HeaderKey::from(descriptor.key);
            let default = descriptor
                .default_value()
                .expect("default encodes")
                .expect("every topic map key has a default");
            let stored =
                ResourceOptions::from([(key.clone(), OptionValue::explicit(default.clone()))]);
            let block =
                crate::wire_conversions::resource_options_to_wire(&stored, OptionsProvenance::All)
                    .expect("block encodes");
            let parsed = TopicCreateOptions::parse(&block).expect("create parses every topic key");
            let explicit = crate::wire_conversions::resource_options_from_wire(
                &parsed
                    .to_explicit_wire(|_| true)
                    .expect("explicit block encodes"),
                true,
            )
            .expect("explicit block decodes");
            assert_eq!(
                explicit.get(&key).map(|option| &option.value),
                Some(&default),
                "create stores {}",
                descriptor.key
            );
            assert_eq!(
                derived.get(&key).map(|option| &option.value),
                Some(&default),
                "create derives the catalog default of {}",
                descriptor.key
            );
            let resolved = TopicResolvedOptions::from(&stored);
            assert_eq!(
                (
                    resolved.compression_algorithm,
                    resolved.message_expiry,
                    resolved.max_topic_size,
                    resolved.partition_resize_policy,
                    resolved.runtime.durability,
                    resolved.runtime.consumer_offset_durability,
                    resolved.runtime.effective_segment_size(),
                ),
                (
                    unset.compression_algorithm,
                    unset.message_expiry,
                    unset.max_topic_size,
                    unset.partition_resize_policy,
                    unset.runtime.durability,
                    unset.runtime.consumer_offset_durability,
                    unset.runtime.effective_segment_size(),
                ),
                "reads resolve an absent {} to its catalog default",
                descriptor.key
            );
        }
    }

    #[test]
    fn given_stream_create_options_with_keys_when_debugging_should_redact_them() {
        let options = StreamCreateOptions {
            encryption_key: Some(
                StreamEncryptionKey::try_from([7u8; 32].as_slice()).expect("32-byte key"),
            ),
            raw: BTreeMap::from([(
                OptionKey::EncryptionKey.to_string(),
                "c2VjcmV0LXNlY3JldA==".to_owned(),
            )]),
        };
        let rendered = format!("{options:?}");
        assert!(
            !rendered.contains("c2VjcmV0") && !rendered.contains("7, 7"),
            "{rendered}"
        );
    }

    #[test]
    fn to_wire_parse_roundtrip_preserves_typed_fields() {
        let options = TopicCreateOptions {
            compression_algorithm: Some(CompressionAlgorithm::Gzip),
            message_expiry: Some(IggyExpiry::from(5_000_000u64)),
            max_topic_size: Some(MaxTopicSize::from(2_000_000_000u64)),
            segment_size: Some(IggyByteSize::from(134_217_728u64)),
            durability: Durability::Persisted,
            messages_required_to_save: Some(500),
            size_of_messages_required_to_save: Some(IggyByteSize::from(2_097_152u64)),
            preallocate_segments: Some(false),
            partitions_count: None,
            consumer_offset_durability: Durability::Replicated,
            partition_resize_policy: Some(PartitionResizePolicy::Fixed),
            raw: BTreeMap::new(),
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire().unwrap()).unwrap();
        assert_eq!(parsed, options);
    }

    #[test]
    fn given_omitted_resize_policy_when_resolving_defaults_should_persist_mutable_as_derived() {
        let options = TopicCreateOptions::default();
        let supplied = options.to_wire().unwrap();
        let stored = derived_defaults(
            OptionsScope::Topic,
            &crate::wire_conversions::resource_options_from_wire(&supplied, true).unwrap(),
        )
        .unwrap();
        let policy =
            &stored[&HeaderKey::from_str(topic_option_keys::PARTITION_RESIZE_POLICY).unwrap()];
        assert_eq!(policy.value.as_bytes(), b"mutable");
        assert!(!policy.explicit);
        assert_eq!(
            TopicCreateOptions::from_resource_options(&stored).partition_resize_policy,
            Some(PartitionResizePolicy::Mutable),
        );
        assert!(
            !supplied
                .into_iter()
                .any(|entry| entry.key == topic_option_keys::PARTITION_RESIZE_POLICY.as_bytes())
        );
    }

    #[test]
    fn given_raw_resize_policy_when_encoding_should_match_binary_and_http_values() {
        let mut options = TopicCreateOptions {
            raw: BTreeMap::from([(
                topic_option_keys::PARTITION_RESIZE_POLICY.to_owned(),
                "fixed".to_owned(),
            )]),
            ..TopicCreateOptions::default()
        };
        assert_eq!(
            TopicCreateOptions::parse(&options.to_wire().unwrap())
                .unwrap()
                .partition_resize_policy,
            Some(PartitionResizePolicy::Fixed),
        );
        assert_eq!(
            options.to_input_map().unwrap()[topic_option_keys::PARTITION_RESIZE_POLICY],
            OptionInputValue::String("fixed".to_owned().into())
        );

        options.partition_resize_policy = Some(PartitionResizePolicy::Mutable);
        assert_eq!(
            TopicCreateOptions::parse(&options.to_wire().unwrap())
                .unwrap()
                .partition_resize_policy,
            Some(PartitionResizePolicy::Mutable),
        );
        assert_eq!(
            options.to_input_map().unwrap()[topic_option_keys::PARTITION_RESIZE_POLICY],
            OptionInputValue::String("mutable".to_owned().into())
        );
    }

    #[test]
    fn given_invalid_resize_policy_when_parsing_should_reject_value_and_kind() {
        for value in [
            HeaderValue::from_str("unknown").unwrap(),
            HeaderValue::from(1_u32),
            HeaderValue::from(true),
        ] {
            let options = ResourceOptions::from([(
                HeaderKey::from_str(topic_option_keys::PARTITION_RESIZE_POLICY).unwrap(),
                OptionValue::explicit(value),
            )]);
            let wire =
                crate::wire_conversions::resource_options_to_wire(&options, OptionsProvenance::All)
                    .unwrap();
            assert_eq!(
                TopicCreateOptions::parse(&wire),
                Err(IggyError::InvalidOptionValue(
                    topic_option_keys::PARTITION_RESIZE_POLICY.to_owned()
                )),
            );
        }
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
        assert_eq!(options.to_wire().unwrap().into_iter().count(), 2);
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
        // any non-empty map. Keys render as their plain text, values as
        // readable text plus their kind, so an HTTP reader rebuilds the exact
        // value a binary reader gets.
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
            json.contains("\"value\":\"1048576\"") && json.contains("\"kind\":\"uint64\""),
            "the value must render as text with its kind, got: {json}"
        );

        let decoded: Holder = serde_json::from_str(&json).expect("options must round-trip");
        assert_eq!(
            decoded, holder,
            "kind and provenance survive the round trip"
        );
    }

    #[test]
    fn given_all_header_kinds_when_roundtripping_http_should_preserve_bytes_and_provenance() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Holder {
            #[serde(default, with = "super::resource_options_json")]
            options: ResourceOptions,
        }
        let values = [
            HeaderValue::try_from([0u8, 255, 7].as_slice()).unwrap(),
            HeaderValue::try_from("text with spaces").unwrap(),
            HeaderValue::from(true),
            HeaderValue::from(-5i8),
            HeaderValue::from(-300i16),
            HeaderValue::from(-70_000i32),
            HeaderValue::from(i64::MIN),
            HeaderValue::from(i128::MAX),
            HeaderValue::from(250u8),
            HeaderValue::from(65_000u16),
            HeaderValue::from(u32::MAX),
            HeaderValue::from(u64::MAX - 1),
            HeaderValue::from(u128::MAX),
            HeaderValue::from(f32::from_bits(0x7fc0_0001)),
            HeaderValue::from(-0.0f64),
            HeaderValue::from(f64::from_bits(0x7ff8_0000_0000_0abc)),
        ];
        let options: ResourceOptions = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                (
                    HeaderKey::try_from(format!("future_key_{index:02}").as_str()).unwrap(),
                    OptionValue {
                        value: value.clone(),
                        explicit: index % 2 == 0,
                    },
                )
            })
            .collect();
        let holder = Holder { options };
        let json = serde_json::to_string(&holder).expect("serializes");
        let decoded: Holder = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(decoded, holder, "{json}");
        for (kind, text) in [
            (HeaderKind::Uint64, "+5"),
            (HeaderKind::Uint64, "05"),
            (HeaderKind::Bool, "True"),
            (HeaderKind::Raw, "AAF="),
            (HeaderKind::Float32, "0x7fc00001a"),
            (HeaderKind::Raw, "not base64"),
        ] {
            assert!(
                resource_options_json::decode_value(kind, text).is_err(),
                "{kind:?} {text} must be refused"
            );
        }
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
            durability: Durability::Persisted,
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
        assert!(stored.contains_key(&HeaderKey::from_str(topic_option_keys::DURABILITY).unwrap()));
    }

    #[test]
    fn runtime_options_derive_from_the_persisted_map() {
        let options = TopicCreateOptions {
            segment_size: Some(IggyByteSize::from(2_097_152u64)),
            durability: Durability::Persisted,
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
        assert_eq!(runtime.durability, Durability::Persisted);
        assert_eq!(runtime.messages_required_to_save, Some(9));
        assert_eq!(runtime.size_of_messages_required_to_save, None);
    }

    #[test]
    fn raw_durability_can_strengthen_the_replicated_default() {
        let options = TopicCreateOptions {
            raw: BTreeMap::from([("durability".to_owned(), "persisted".to_owned())]),
            ..TopicCreateOptions::default()
        };
        let parsed = TopicCreateOptions::parse(&options.to_wire().unwrap()).unwrap();
        assert_eq!(parsed.durability, Durability::Persisted);
        assert_eq!(parsed.consumer_offset_durability, Durability::Replicated);
        assert_eq!(
            options.to_input_map().unwrap()["durability"],
            OptionInputValue::String("persisted".to_owned().into())
        );
    }

    #[test]
    fn conflicting_durability_is_rejected_by_both_encoders() {
        let options = TopicCreateOptions {
            durability: Durability::Persisted,
            raw: BTreeMap::from([("durability".to_string(), "replicated".to_string())]),
            ..TopicCreateOptions::default()
        };
        assert!(options.to_wire().is_err());
        assert!(options.to_input_map().is_err());
    }

    #[test]
    fn omitted_durability_is_independent_and_remains_derived() {
        for (key, other) in [
            (
                topic_option_keys::DURABILITY,
                topic_option_keys::CONSUMER_OFFSET_DURABILITY,
            ),
            (
                topic_option_keys::CONSUMER_OFFSET_DURABILITY,
                topic_option_keys::DURABILITY,
            ),
        ] {
            let supplied = crate::wire_conversions::resource_options_to_wire(
                &ResourceOptions::from([(
                    HeaderKey::from_str(key).unwrap(),
                    OptionValue::explicit(HeaderValue::from_str("persisted").unwrap()),
                )]),
                OptionsProvenance::All,
            )
            .unwrap();
            let parsed = TopicCreateOptions::parse(&supplied).unwrap();
            let explicit = parsed
                .to_explicit_wire(|key| {
                    supplied
                        .into_iter()
                        .any(|entry| entry.key == key.as_bytes())
                })
                .unwrap();
            assert_eq!(explicit.into_iter().count(), 1);
            let derived = derived_defaults(
                OptionsScope::Topic,
                &crate::wire_conversions::resource_options_from_wire(&explicit, true).unwrap(),
            )
            .unwrap();
            assert!(!derived.contains_key(&HeaderKey::from_str(key).unwrap()));
            assert_eq!(
                derived[&HeaderKey::from_str(other).unwrap()]
                    .value
                    .as_bytes(),
                b"replicated"
            );
        }
    }

    #[test]
    fn both_durability_defaults_are_emitted_and_legacy_key_is_refused() {
        let options = TopicCreateOptions::default().to_option_map().unwrap();
        for key in [
            topic_option_keys::DURABILITY,
            topic_option_keys::CONSUMER_OFFSET_DURABILITY,
        ] {
            let value = &options[&HeaderKey::from_str(key).unwrap()];
            assert_eq!(value.value.kind(), HeaderKind::String);
            assert_eq!(value.value.as_bytes(), b"replicated");
            assert!(value.explicit);
        }
        let legacy = TopicCreateOptions {
            raw: BTreeMap::from([("enforce_fsync".to_string(), "true".to_string())]),
            ..TopicCreateOptions::default()
        };
        assert!(matches!(
            TopicCreateOptions::parse(&legacy.to_wire().unwrap()),
            Err(IggyError::UnsupportedOptionKey(_))
        ));
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
                ("durability".to_string(), "replicated".to_string()),
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
        assert_eq!(parsed.durability, Durability::Replicated);
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
