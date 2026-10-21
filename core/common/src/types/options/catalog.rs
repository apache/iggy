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

use super::codec::{OptionValueCodec, invalid};
use super::key::OptionKey;
use super::{
    DEFAULT_MAX_TOPIC_SIZE, DEFAULT_MESSAGE_EXPIRY, DEFAULT_MESSAGES_REQUIRED_TO_SAVE,
    DEFAULT_PREALLOCATE_SEGMENTS, DEFAULT_SEGMENT_SIZE, DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
    Durability, MAX_MESSAGES_REQUIRED_TO_SAVE, MAX_PREALLOCATED_TOPIC_BYTES,
    MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE, MAX_TOPIC_SEGMENT_SIZE, MIN_TOPIC_SEGMENT_SIZE,
    OptionValue, OptionsScope, PartitionResizePolicy, ResourceOptions, erase_options,
};
use crate::http::users::defaults::{MAX_USERNAME_LENGTH, MIN_USERNAME_LENGTH};
use crate::types::compression::compression_algorithm::CompressionAlgorithm;
use crate::types::message::{HeaderKey, HeaderKind, HeaderValue};
use crate::utils::stream_key::{StreamEncryptionKey, StreamKeyOrigin, WrapSet};
use crate::wire_conversions::options_block_size;
use crate::{IggyByteSize, IggyError, IggyExpiry, MaxTopicSize, UserStatus};
use iggy_binary_protocol::{WireHeaderKind, WireOptions, WireUserHeaderEntry};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use strum::{Display, EnumString, IntoStaticStr};

/// Bit set in a `DescribeOptions` entry's flags when an update may set the key.
pub const OPTION_FLAG_UPDATABLE: u8 = 1;
/// Bit set when an update may reset the key to its default.
pub const OPTION_FLAG_RESETTABLE: u8 = 1 << 1;
/// Bit set when the key is write-only: accepted, never returned.
pub const OPTION_FLAG_SECRET: u8 = 1 << 2;
/// Bit set when the server computes the key and refuses it from clients.
pub const OPTION_FLAG_READ_ONLY: u8 = 1 << 3;
/// Bit set when a node that does not understand the key must not serve the
/// resource.
pub const OPTION_FLAG_CRITICAL: u8 = 1 << 4;

/// Longest marker a resource may carry. Every critical key name fits many
/// times over, and the bound keeps the marker a single header field.
const CRITICAL_KEYS_MAX_LENGTH: usize = 255;
/// Longest key echoed back in an option error. A rejected key is
/// attacker-sized, and over HTTP the error text rides the response body.
const ERROR_KEY_PREVIEW_LENGTH: usize = 64;
const MAX_RESOURCE_NAME_LENGTH: usize = 255;

const ALL_SCOPES: &[OptionsScope] = &[
    OptionsScope::Stream,
    OptionsScope::Topic,
    OptionsScope::User,
];
const STREAM_AND_TOPIC: &[OptionsScope] = &[OptionsScope::Stream, OptionsScope::Topic];
const STREAM_ONLY: &[OptionsScope] = &[OptionsScope::Stream];
const TOPIC_ONLY: &[OptionsScope] = &[OptionsScope::Topic];
const USER_ONLY: &[OptionsScope] = &[OptionsScope::User];

/// Every option key, indexed by its `OptionKey` discriminant.
pub static CATALOG: [OptionDescriptor; 19] = [
    OptionDescriptor {
        key: OptionKey::Name,
        scopes: STREAM_AND_TOPIC,
        kind: HeaderKind::String,
        storage: OptionStorage::Column,
        mutability: OptionMutability::Updatable,
        default: OptionDefault::None,
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "Resource name, 1 to 255 bytes, unique within its parent. Set by the create \
             command's name field and changed through an update."
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::Username,
        scopes: USER_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Column,
        mutability: OptionMutability::Updatable,
        default: OptionDefault::None,
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            format!(
                "User name, {MIN_USERNAME_LENGTH} to {MAX_USERNAME_LENGTH} bytes, unique. Set by \
                 the create command's username field and changed through an update."
            )
        },
    },
    OptionDescriptor {
        key: OptionKey::Status,
        scopes: USER_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Column,
        mutability: OptionMutability::Updatable,
        default: OptionDefault::None,
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "User status, active or inactive. Set by the create command's status field and \
             changed through an update. Deactivating a user ends its sessions."
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::CompressionAlgorithm,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| CompressionAlgorithm::default().encode()),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "Compression algorithm (none, gzip). Stored and reported, not applied to messages"
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::MessageExpiry,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::Uint64,
        storage: OptionStorage::Map,
        mutability: OptionMutability::Updatable,
        default: OptionDefault::Const(|| Ok(HeaderValue::from(DEFAULT_MESSAGE_EXPIRY))),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "Message expiry in microseconds, or a humantime string (e.g. 7 days)".to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::MaxTopicSize,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::Uint64,
        storage: OptionStorage::Map,
        mutability: OptionMutability::Updatable,
        default: OptionDefault::Const(|| Ok(HeaderValue::from(DEFAULT_MAX_TOPIC_SIZE))),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "Topic-wide sealed-segment size cap, split across all partitions, in bytes or a \
             byte-size string (e.g. 1 GiB); finite values must be at least the segment size"
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::SegmentSize,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::Uint64,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| Ok(HeaderValue::from(DEFAULT_SEGMENT_SIZE))),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            format!(
                "Segment size in bytes, or a byte-size string (e.g. 128 MiB); a 512-byte \
                 multiple within {MIN_TOPIC_SEGMENT_SIZE}..={MAX_TOPIC_SEGMENT_SIZE}"
            )
        },
    },
    OptionDescriptor {
        key: OptionKey::Durability,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| Durability::default().encode()),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "Message completion: replicated or persisted. Independently defaults to \
             replicated. A singleton quorum has one copy. In replicated groups, persisted \
             messages use WAL references to segment bodies, retaining their inodes by hard \
             link until WAL reclamation. The full body size still counts against \
             partition.wal_bytes_max."
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::ConsumerOffsetDurability,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| Durability::default().encode()),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "Explicit offset completion: replicated or persisted. Independently defaults to \
             replicated. Poll auto-commit remains asynchronous. In replicated groups, persisted \
             offsets also enable WAL references to segment bodies, retaining their inodes by \
             hard link until reclamation, even with replicated message durability. Full body \
             sizes count against partition.wal_bytes_max."
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::PartitionResizePolicy,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| PartitionResizePolicy::default().encode()),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            "Immutable after creation: mutable permits partition addition/deletion without a \
             bounded cutover or per-key ordering across layouts; fixed rejects both. Retention \
             and topic deletion remain destructive."
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::MessagesRequiredToSave,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::Uint32,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| Ok(HeaderValue::from(DEFAULT_MESSAGES_REQUIRED_TO_SAVE))),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            format!(
                "Ordinary message-count flush trigger; 1..={MAX_MESSAGES_REQUIRED_TO_SAVE}. \
                 Required persistence, capacity pressure, and lifecycle operations can flush \
                 earlier"
            )
        },
    },
    OptionDescriptor {
        key: OptionKey::SizeOfMessagesRequiredToSave,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::Uint64,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| {
            Ok(HeaderValue::from(DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE))
        }),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            format!(
                "Flush the journal once it holds this many bytes, or a byte-size string; \
                 whichever threshold trips first flushes. At most \
                 {MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE}"
            )
        },
    },
    OptionDescriptor {
        key: OptionKey::PreallocateSegments,
        scopes: TOPIC_ONLY,
        kind: HeaderKind::Bool,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::Const(|| Ok(HeaderValue::from(DEFAULT_PREALLOCATE_SEGMENTS))),
        visibility: OptionVisibility::Public,
        critical: false,
        description: || {
            format!(
                "Request segment_size bytes of disk reservation when each segment opens. \
                 Unsupported or failed reservations fall back to ordinary allocation with a \
                 warning. Reservation runs inline on the owning shard. segment_size * \
                 partitions_count is capped at {MAX_PREALLOCATED_TOPIC_BYTES} bytes per create"
            )
        },
    },
    OptionDescriptor {
        key: OptionKey::EncryptionKey,
        scopes: STREAM_ONLY,
        kind: HeaderKind::Raw,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::None,
        visibility: OptionVisibility::Secret,
        critical: true,
        description: || {
            "The stream's own data encryption key: 32 random bytes, raw or standard padded \
             base64. Create-only and write-only: the server stores it wrapped under a key \
             derived from the server encryption key and never returns it. Requires the server \
             encryption key to be configured. Without it, a server with encryption enabled \
             generates a key for the stream. The key travels in the create request over the \
             transport the client uses."
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::EncryptionKeyWrapped,
        scopes: STREAM_ONLY,
        kind: HeaderKind::Raw,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::None,
        visibility: OptionVisibility::Internal,
        critical: true,
        description: || {
            "The stream key wrapped by the server under one or more key-encryption keys, \
             never returned"
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::EncryptionKeySource,
        scopes: STREAM_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Computed,
        mutability: OptionMutability::ReadOnly,
        default: OptionDefault::None,
        visibility: OptionVisibility::ReadOnly,
        critical: false,
        description: || {
            "Who chose the stream's at-rest key: stream when the client sent it, server when \
             the server generated it, none when the stream's messages are stored unencrypted"
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::EncryptionKeyFingerprint,
        scopes: STREAM_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Computed,
        mutability: OptionMutability::ReadOnly,
        default: OptionDefault::None,
        visibility: OptionVisibility::ReadOnly,
        critical: false,
        description: || {
            "The first 8 bytes of SHA-256 over the label iggy/stream-key-fingerprint/v1 and \
             the stream's key, as 16 lowercase hex characters. Absent when the source is none"
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::EncryptionKekFingerprint,
        scopes: STREAM_ONLY,
        kind: HeaderKind::String,
        storage: OptionStorage::Computed,
        mutability: OptionMutability::ReadOnly,
        default: OptionDefault::None,
        visibility: OptionVisibility::ReadOnly,
        critical: false,
        description: || {
            "Fingerprints of the key-encryption keys that wrap the stream's key, \
             comma-separated: one outside a key rotation, one per configured server key \
             during it. Absent when the source is none"
                .to_owned()
        },
    },
    OptionDescriptor {
        key: OptionKey::CriticalKeys,
        scopes: ALL_SCOPES,
        kind: HeaderKind::String,
        storage: OptionStorage::Map,
        mutability: OptionMutability::CreateOnly,
        default: OptionDefault::None,
        visibility: OptionVisibility::Internal,
        critical: false,
        description: || {
            "Sorted comma-separated names of stored keys a node must understand before \
             serving the resource"
                .to_owned()
        },
    },
];

/// One option key's catalog entry.
pub struct OptionDescriptor {
    pub key: OptionKey,
    pub scopes: &'static [OptionsScope],
    /// Canonical kind: what admission stores whatever kind the client sent.
    pub kind: HeaderKind,
    pub storage: OptionStorage,
    pub mutability: OptionMutability,
    pub default: OptionDefault,
    pub visibility: OptionVisibility,
    /// A node that does not understand the key must not serve the resource.
    pub critical: bool,
    pub description: fn() -> String,
}

/// Where a key's value lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionStorage {
    /// In the resource's options map.
    Map,
    /// In a typed, indexed field of the resource, such as its name.
    Column,
    /// Nowhere: computed when the resource is read.
    Computed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionMutability {
    CreateOnly,
    Updatable,
    ReadOnly,
}

#[derive(Clone, Copy)]
pub enum OptionDefault {
    None,
    Const(fn() -> Result<HeaderValue, IggyError>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionVisibility {
    /// Accepted from clients and returned on reads.
    Public,
    /// Accepted from clients, never returned.
    Secret,
    /// Written by the server, never accepted from clients, never returned.
    Internal,
    /// Computed by the server on reads, never accepted from clients.
    ReadOnly,
}

/// Which command an options block arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionOperation {
    Create,
    Update,
}

/// Who chose a stream's at-rest key, as reported on reads.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString, IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum StreamEncryptionSource {
    /// The client sent the key.
    Stream,
    /// The server generated the key.
    Server,
    /// Stored unencrypted.
    None,
}

/// The fixed columns a create carries or an update patches.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OptionColumns {
    pub name: Option<String>,
    pub username: Option<String>,
    pub status: Option<UserStatus>,
}

/// What admission may know about the resource beyond the request itself.
#[derive(Debug, Clone, Default)]
pub struct OptionAdmissionContext {
    /// The resource's stored options, for an update's capacity check.
    pub current: Option<ResourceOptions>,
    /// The create command's own fixed fields.
    pub create_columns: OptionColumns,
}

/// An options block that passed admission, in canonical kinds.
#[derive(Clone, Default, PartialEq)]
pub struct AdmittedOptions {
    /// Map keys to set, explicit and canonical. Zero sentinels that mean "use
    /// the default" are dropped, so they leave the stored value alone. A secret
    /// key holds the secret itself, so a caller done with the map erases it
    /// with [`super::erase_options`].
    pub explicit: ResourceOptions,
    /// Map keys to reset to their defaults.
    pub reset: Vec<OptionKey>,
    /// Column values the update sets, or the create's validated columns.
    pub columns: OptionColumns,
}

impl Debug for AdmittedOptions {
    /// Prints secret values as `<redacted>`.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let explicit: BTreeMap<String, &dyn Debug> = self
            .explicit
            .iter()
            .map(|(key, option)| {
                let secret = OptionKey::try_from(key)
                    .is_ok_and(|known| known.descriptor().visibility != OptionVisibility::Public);
                let shown: &dyn Debug = if secret { &"<redacted>" } else { &option.value };
                (String::from_utf8_lossy(key.as_bytes()).into_owned(), shown)
            })
            .collect();
        formatter
            .debug_struct("AdmittedOptions")
            .field("explicit", &explicit)
            .field("reset", &self.reset)
            .field("columns", &self.columns)
            .finish()
    }
}

impl From<StreamKeyOrigin> for StreamEncryptionSource {
    fn from(origin: StreamKeyOrigin) -> Self {
        match origin {
            StreamKeyOrigin::Client => Self::Stream,
            StreamKeyOrigin::Server => Self::Server,
        }
    }
}

impl OptionKey {
    /// This key's catalog entry.
    #[must_use]
    pub fn descriptor(self) -> &'static OptionDescriptor {
        &CATALOG[self as usize]
    }
}

impl OptionDescriptor {
    #[must_use]
    pub fn applies_to(&self, scope: OptionsScope) -> bool {
        self.scopes.contains(&scope)
    }

    #[must_use]
    pub fn is_updatable(&self) -> bool {
        self.mutability == OptionMutability::Updatable
    }

    /// An update may reset only a mutable map key with a default. A default
    /// alone does not make a create-only setting resettable.
    #[must_use]
    pub fn is_resettable(&self) -> bool {
        self.is_updatable()
            && self.storage == OptionStorage::Map
            && matches!(self.default, OptionDefault::Const(_))
    }

    /// Whether `DescribeOptions` lists this key. Internal keys are the
    /// server's bookkeeping and stay out of the catalog clients see.
    #[must_use]
    pub fn is_described(&self) -> bool {
        self.visibility != OptionVisibility::Internal
    }

    /// The `DescribeOptions` flag byte.
    #[must_use]
    pub fn flags(&self) -> u8 {
        let mut flags = 0;
        if self.is_updatable() {
            flags |= OPTION_FLAG_UPDATABLE;
        }
        if self.is_resettable() {
            flags |= OPTION_FLAG_RESETTABLE;
        }
        if self.visibility == OptionVisibility::Secret {
            flags |= OPTION_FLAG_SECRET;
        }
        if self.visibility == OptionVisibility::ReadOnly {
            flags |= OPTION_FLAG_READ_ONLY;
        }
        if self.critical {
            flags |= OPTION_FLAG_CRITICAL;
        }
        flags
    }

    /// The key's default in its canonical kind, or `None` without one.
    ///
    /// # Errors
    ///
    /// Propagates a default that does not fit a header value, which the
    /// catalog tests rule out.
    pub fn default_value(&self) -> Result<Option<HeaderValue>, IggyError> {
        match self.default {
            OptionDefault::None => Ok(None),
            OptionDefault::Const(default) => default().map(Some),
        }
    }
}

/// Catalog entries valid in `scope`, in catalog order.
pub fn descriptors_for(scope: OptionsScope) -> impl Iterator<Item = &'static OptionDescriptor> {
    CATALOG
        .iter()
        .filter(move |descriptor| descriptor.applies_to(scope))
}

/// The catalog default of every map key in `scope` that `explicit` does not
/// set, as derived entries, so a create stores a concrete value for each.
///
/// # Errors
///
/// Propagates a default that does not fit a header value, which the catalog
/// tests rule out.
pub fn derived_defaults(
    scope: OptionsScope,
    explicit: &ResourceOptions,
) -> Result<ResourceOptions, IggyError> {
    let mut derived = ResourceOptions::new();
    for descriptor in
        descriptors_for(scope).filter(|descriptor| descriptor.storage == OptionStorage::Map)
    {
        let key = HeaderKey::from(descriptor.key);
        if explicit.contains_key(&key) {
            continue;
        }
        if let Some(default) = descriptor.default_value()? {
            derived.insert(key, OptionValue::derived(default));
        }
    }
    Ok(derived)
}

/// Validate and canonicalize a create or update options block.
///
/// Checks run in a fixed order so every transport and SDK observes the same
/// error category for the same request:
///
/// 1. `InvalidCommand`: a name repeated across set and reset, a repeated reset
///    name, an update with nothing to change, a reset on a create.
/// 2. `UnsupportedOptionKey`: a key outside the catalog or outside `scope`.
/// 3. `ReadOnlyOption`: an internal or read-only key.
/// 4. `OptionNotUpdatable`: a create-only key in an update's set or reset.
/// 5. `OptionNotResettable`: a reset of a column or of a key without default.
/// 6. Value errors: a column inside a create block is `InvalidOptionValue`,
///    column values keep their own errors, other keys `InvalidOptionValue`.
/// 7. `InvalidCommand`: an update that changes nothing once its zero
///    sentinels are dropped.
/// 8. `OptionsBlockTooLarge`: the resulting map exceeds the block limits.
///
/// # Errors
///
/// As listed above.
pub fn admit(
    scope: OptionsScope,
    operation: OptionOperation,
    block: &WireOptions,
    reset: &[String],
    context: &OptionAdmissionContext,
) -> Result<AdmittedOptions, IggyError> {
    let mut reset_names = BTreeSet::new();
    for name in reset {
        if !reset_names.insert(name.as_str()) {
            return Err(IggyError::InvalidCommand);
        }
    }
    let entries: Vec<WireUserHeaderEntry<'_>> = block.into_iter().collect();
    let mut set_names = Vec::with_capacity(entries.len());
    for entry in &entries {
        // Wire validation already enforced UTF-8 string keys.
        let name = std::str::from_utf8(entry.key).map_err(|_| IggyError::InvalidCommand)?;
        if reset_names.contains(name) {
            return Err(IggyError::InvalidCommand);
        }
        set_names.push(name);
    }
    match operation {
        OptionOperation::Create if !reset.is_empty() => return Err(IggyError::InvalidCommand),
        OptionOperation::Update if entries.is_empty() && reset.is_empty() => {
            return Err(IggyError::InvalidCommand);
        }
        _ => {}
    }

    let set_keys = set_names
        .iter()
        .map(|name| scoped_key(scope, name))
        .collect::<Result<Vec<_>, _>>()?;
    let reset_keys = reset
        .iter()
        .map(|name| scoped_key(scope, name))
        .collect::<Result<Vec<_>, _>>()?;
    let all_keys = || set_keys.iter().chain(reset_keys.iter()).copied();

    if let Some(key) = all_keys().find(|key| {
        matches!(
            key.descriptor().visibility,
            OptionVisibility::Internal | OptionVisibility::ReadOnly
        )
    }) {
        return Err(IggyError::ReadOnlyOption(key.to_string()));
    }
    if operation == OptionOperation::Update
        && let Some(key) = all_keys().find(|key| !key.descriptor().is_updatable())
    {
        return Err(IggyError::OptionNotUpdatable(key.to_string()));
    }
    if let Some(key) = reset_keys
        .iter()
        .find(|key| !key.descriptor().is_resettable())
    {
        return Err(IggyError::OptionNotResettable(key.to_string()));
    }
    if operation == OptionOperation::Create
        && let Some(key) = set_keys
            .iter()
            .find(|key| key.descriptor().storage == OptionStorage::Column)
    {
        return Err(invalid(*key));
    }

    let mut columns = match operation {
        OptionOperation::Create => validated_create_columns(scope, &context.create_columns)?,
        OptionOperation::Update => OptionColumns::default(),
    };
    let mut explicit = ResourceOptions::new();
    for (key, entry) in set_keys.iter().zip(&entries) {
        let decoded = match key.descriptor().storage {
            OptionStorage::Column => decode_column(scope, *key, entry, &mut columns),
            OptionStorage::Map => canonical_value(*key, entry).map(|value| {
                if let Some(value) = value {
                    explicit.insert(HeaderKey::from(*key), OptionValue::explicit(value));
                }
            }),
            OptionStorage::Computed => Err(IggyError::ReadOnlyOption(key.to_string())),
        };
        if let Err(error) = decoded {
            erase_options(explicit);
            return Err(error);
        }
    }
    if operation == OptionOperation::Update
        && explicit.is_empty()
        && reset_keys.is_empty()
        && columns == OptionColumns::default()
    {
        return Err(IggyError::InvalidCommand);
    }
    if let Err(error) = check_block_size(context.current.as_ref(), &reset_keys, &explicit) {
        erase_options(explicit);
        return Err(error);
    }

    Ok(AdmittedOptions {
        explicit,
        reset: reset_keys,
        columns,
    })
}

/// The canonical value of one map key, or `None` for a zero sentinel that
/// means "use the default" and therefore sets nothing. A secret key's value
/// is the secret itself, for the caller to erase.
///
/// # Errors
///
/// `InvalidOptionValue` naming `key` when the value does not decode or
/// breaks a per-key bound. `InvalidEncryptionKey` for a malformed stream key.
pub fn canonical_value(
    key: OptionKey,
    entry: &WireUserHeaderEntry<'_>,
) -> Result<Option<HeaderValue>, IggyError> {
    let value = match key {
        OptionKey::CompressionAlgorithm => CompressionAlgorithm::decode(key, entry)?.encode()?,
        OptionKey::MessageExpiry => {
            let expiry = IggyExpiry::decode(key, entry)?;
            if expiry == IggyExpiry::ServerDefault {
                return Ok(None);
            }
            expiry.encode()?
        }
        OptionKey::MaxTopicSize => {
            let size = MaxTopicSize::decode(key, entry)?;
            if size == MaxTopicSize::ServerDefault {
                return Ok(None);
            }
            size.encode()?
        }
        OptionKey::SegmentSize | OptionKey::SizeOfMessagesRequiredToSave => {
            let size = IggyByteSize::decode(key, entry)?;
            if size.as_bytes_u64() == 0 {
                return Ok(None);
            }
            size.encode()?
        }
        OptionKey::Durability | OptionKey::ConsumerOffsetDurability => {
            Durability::decode(key, entry)?.encode()?
        }
        OptionKey::PartitionResizePolicy => PartitionResizePolicy::decode(key, entry)?.encode()?,
        OptionKey::MessagesRequiredToSave => u32::decode(key, entry)?.encode()?,
        OptionKey::PreallocateSegments => bool::decode(key, entry)?.encode()?,
        OptionKey::EncryptionKey => {
            HeaderValue::try_from(stream_key_from_entry(entry)?.as_bytes().as_slice())?
        }
        OptionKey::Name
        | OptionKey::Username
        | OptionKey::Status
        | OptionKey::EncryptionKeyWrapped
        | OptionKey::EncryptionKeySource
        | OptionKey::EncryptionKeyFingerprint
        | OptionKey::EncryptionKekFingerprint
        | OptionKey::CriticalKeys => return Err(invalid(key)),
    };
    Ok(Some(value))
}

/// The stream key a create block carries: 32 raw bytes, or their canonical
/// standard padded base64 as a string.
///
/// # Errors
///
/// `InvalidEncryptionKey` for any other kind, length or encoding.
pub fn stream_key_from_entry(
    entry: &WireUserHeaderEntry<'_>,
) -> Result<StreamEncryptionKey, IggyError> {
    match HeaderKind::from_code(entry.value_kind.0) {
        Ok(HeaderKind::Raw) => StreamEncryptionKey::try_from(entry.value),
        Ok(HeaderKind::String) => std::str::from_utf8(entry.value)
            .map_err(|_| IggyError::InvalidEncryptionKey)?
            .parse(),
        _ => Err(IggyError::InvalidEncryptionKey),
    }
}

/// The options a read returns: public map keys by provenance, unknown keys
/// this build stores without understanding, and the stream's encryption
/// projections as derived entries.
///
/// The projections come from the stored wrap set alone, so every node answers
/// the same for the same stream state. A cached create reply keeps the
/// create-time projections, so its KEK fingerprints predate any later rewrap.
///
/// # Errors
///
/// `UnsupportedCriticalOption` when the resource holds critical state this
/// build cannot interpret. Its values are never echoed then.
pub fn public_options(
    scope: OptionsScope,
    stored: &ResourceOptions,
) -> Result<ResourceOptions, IggyError> {
    let wrap_set = critical_wrap_set(stored)?;
    let mut public = ResourceOptions::new();
    for (key, value) in stored {
        let returned = OptionKey::try_from(key).map_or(true, |known| {
            let descriptor = known.descriptor();
            descriptor.visibility == OptionVisibility::Public
                && descriptor.storage == OptionStorage::Map
        });
        if returned {
            public.insert(key.clone(), value.clone());
        }
    }
    if scope == OptionsScope::Stream {
        let mut project = |key: OptionKey, text: &str| -> Result<(), IggyError> {
            public.insert(
                HeaderKey::from(key),
                OptionValue::derived(HeaderValue::try_from(text)?),
            );
            Ok(())
        };
        match wrap_set {
            Some(set) => {
                let source: &'static str = StreamEncryptionSource::from(set.origin()).into();
                project(OptionKey::EncryptionKeySource, source)?;
                project(OptionKey::EncryptionKeyFingerprint, &set.key_fingerprint())?;
                project(
                    OptionKey::EncryptionKekFingerprint,
                    &set.kek_fingerprints().join(","),
                )?;
            }
            None => {
                let source: &'static str = StreamEncryptionSource::None.into();
                project(OptionKey::EncryptionKeySource, source)?;
            }
        }
    }
    Ok(public)
}

/// The stream's stored wrap set, if the stream is encrypted.
///
/// # Errors
///
/// `UnsupportedCriticalOption` for a set this build cannot read.
pub fn stream_wrap_set(stored: &ResourceOptions) -> Result<Option<WrapSet>, IggyError> {
    stored
        .get(&HeaderKey::from(OptionKey::EncryptionKeyWrapped))
        .map(|option| WrapSet::try_from(option.value.as_bytes()))
        .transpose()
}

/// The canonical `critical_keys` marker naming `keys`.
///
/// # Errors
///
/// `InvalidOptionValue` when the list is empty or does not fit the marker.
pub fn critical_keys_marker(keys: &[OptionKey]) -> Result<HeaderValue, IggyError> {
    let names: BTreeSet<&'static str> = keys.iter().map(|key| (*key).into()).collect();
    let marker = names.into_iter().collect::<Vec<_>>().join(",");
    if marker.is_empty() || marker.len() > CRITICAL_KEYS_MAX_LENGTH {
        return Err(invalid(OptionKey::CriticalKeys));
    }
    HeaderValue::try_from(marker.as_str())
}

/// Refuse stored state this build must understand but cannot.
///
/// A malformed marker, a listed key this build does not know as critical, a
/// listed key missing from the map, and a wrapped key in an unknown envelope
/// version all fail. A wrapped key is critical even when no marker names it.
///
/// # Errors
///
/// `UnsupportedCriticalOption` naming the offending key.
pub fn check_critical_options(stored: &ResourceOptions) -> Result<(), IggyError> {
    critical_wrap_set(stored).map(|_| ())
}

/// [`check_critical_options`], returning the stream's wrap set it parsed so a
/// caller that needs the set reads it once.
///
/// # Errors
///
/// As [`check_critical_options`].
pub(super) fn critical_wrap_set(stored: &ResourceOptions) -> Result<Option<WrapSet>, IggyError> {
    if let Some(marker) = stored.get(&HeaderKey::from(OptionKey::CriticalKeys)) {
        let names = parse_critical_marker(&marker.value)?;
        for name in names {
            let known = name
                .parse::<OptionKey>()
                .ok()
                .filter(|key| key.descriptor().critical)
                .ok_or_else(|| IggyError::UnsupportedCriticalOption(key_preview(name)))?;
            if !stored.contains_key(&HeaderKey::from(known)) {
                return Err(IggyError::UnsupportedCriticalOption(name.to_owned()));
            }
        }
    }
    stream_wrap_set(stored)
}

/// A string key in `scope`, or `UnsupportedOptionKey`.
fn scoped_key(scope: OptionsScope, name: &str) -> Result<OptionKey, IggyError> {
    name.parse::<OptionKey>()
        .ok()
        .filter(|key| key.descriptor().applies_to(scope))
        .ok_or_else(|| IggyError::UnsupportedOptionKey(key_preview(name)))
}

/// Refuse a resulting map past the block limits: `current` with `reset_keys`
/// at their defaults and `explicit` set. The size is summed over the three
/// maps, so no value is cloned or encoded to measure it.
fn check_block_size(
    current: Option<&ResourceOptions>,
    reset_keys: &[OptionKey],
    explicit: &ResourceOptions,
) -> Result<(), IggyError> {
    let mut defaults = ResourceOptions::new();
    for key in reset_keys {
        if let Some(default) = key.descriptor().default_value()? {
            defaults.insert(HeaderKey::from(*key), OptionValue::derived(default));
        }
    }
    let kept = current
        .into_iter()
        .flatten()
        .filter(|(key, _)| !defaults.contains_key(*key) && !explicit.contains_key(*key));
    let reset = defaults
        .iter()
        .filter(|(key, _)| !explicit.contains_key(*key));
    options_block_size(kept.chain(reset).chain(explicit)).map(|_| ())
}

fn validated_create_columns(
    scope: OptionsScope,
    columns: &OptionColumns,
) -> Result<OptionColumns, IggyError> {
    if let Some(name) = &columns.name {
        validate_name(scope, name)?;
    }
    if let Some(username) = &columns.username {
        validate_username(username)?;
    }
    Ok(columns.clone())
}

fn decode_column(
    scope: OptionsScope,
    key: OptionKey,
    entry: &WireUserHeaderEntry<'_>,
    columns: &mut OptionColumns,
) -> Result<(), IggyError> {
    let text = (entry.value_kind == WireHeaderKind(HeaderKind::String.as_code()))
        .then(|| std::str::from_utf8(entry.value).ok())
        .flatten();
    match key {
        OptionKey::Name => {
            let name = text.ok_or_else(|| name_error(scope))?;
            validate_name(scope, name)?;
            columns.name = Some(name.to_owned());
        }
        OptionKey::Username => {
            let username = text.ok_or(IggyError::InvalidUsername)?;
            validate_username(username)?;
            columns.username = Some(username.to_owned());
        }
        OptionKey::Status => {
            columns.status = Some(
                text.ok_or(IggyError::InvalidUserStatus)?
                    .parse()
                    .map_err(|_| IggyError::InvalidUserStatus)?,
            );
        }
        _ => return Err(invalid(key)),
    }
    Ok(())
}

fn validate_name(scope: OptionsScope, name: &str) -> Result<(), IggyError> {
    if name.is_empty() || name.len() > MAX_RESOURCE_NAME_LENGTH {
        return Err(name_error(scope));
    }
    Ok(())
}

fn validate_username(username: &str) -> Result<(), IggyError> {
    if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&username.len()) {
        return Err(IggyError::InvalidUsername);
    }
    Ok(())
}

const fn name_error(scope: OptionsScope) -> IggyError {
    match scope {
        OptionsScope::Topic => IggyError::InvalidTopicName,
        OptionsScope::Stream | OptionsScope::User => IggyError::InvalidStreamName,
    }
}

fn parse_critical_marker(value: &HeaderValue) -> Result<Vec<&str>, IggyError> {
    let malformed = || IggyError::UnsupportedCriticalOption(OptionKey::CriticalKeys.to_string());
    if value.kind() != HeaderKind::String || value.as_bytes().len() > CRITICAL_KEYS_MAX_LENGTH {
        return Err(malformed());
    }
    let text = std::str::from_utf8(value.as_bytes()).map_err(|_| malformed())?;
    let names: Vec<&str> = text.split(',').collect();
    let well_formed = names.iter().all(|name| {
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    });
    if !well_formed || names.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(malformed());
    }
    Ok(names)
}

fn key_preview(key: &str) -> String {
    key.chars().take(ERROR_KEY_PREVIEW_LENGTH).collect()
}

#[cfg(test)]
mod tests {
    use super::super::{OptionsProvenance, topic_option_keys};
    use super::*;
    use crate::wire_conversions::resource_options_to_wire;
    use std::str::FromStr;
    use strum::IntoEnumIterator;

    #[test]
    fn given_catalog_when_listing_keys_should_never_name_the_reset_field() {
        assert!(
            OptionKey::iter().all(|key| key.to_string() != crate::RESET_FIELD),
            "an option named `reset` would collide with the HTTP update body"
        );
    }

    #[test]
    fn given_secret_catalog_key_when_describing_should_be_create_only() {
        // No update path wraps a secret, so an updatable one would be stored in
        // the clear.
        for descriptor in CATALOG
            .iter()
            .filter(|descriptor| descriptor.visibility == OptionVisibility::Secret)
        {
            assert_eq!(
                descriptor.mutability,
                OptionMutability::CreateOnly,
                "{} is a secret",
                descriptor.key
            );
        }
    }

    #[test]
    fn given_catalog_when_iterating_should_have_unique_scoped_descriptors() {
        assert_eq!(CATALOG.len(), OptionKey::iter().count());
        for (index, key) in OptionKey::iter().enumerate() {
            let descriptor = key.descriptor();
            assert_eq!(descriptor.key, key, "catalog row {index} out of order");
            assert!(!descriptor.scopes.is_empty(), "{key} has no scope");
            assert!(
                !(descriptor.description)().is_empty(),
                "{key} has no description"
            );
            if let Some(default) = descriptor.default_value().expect("default encodes") {
                assert_eq!(default.kind(), descriptor.kind, "{key} default kind");
            }
            match descriptor.visibility {
                OptionVisibility::ReadOnly => {
                    assert_eq!(descriptor.storage, OptionStorage::Computed, "{key}");
                }
                OptionVisibility::Public
                | OptionVisibility::Secret
                | OptionVisibility::Internal => {
                    assert_ne!(descriptor.storage, OptionStorage::Computed, "{key}");
                }
            }
            let header = HeaderKey::from(key);
            assert_eq!(OptionKey::try_from(&header), Ok(key));
            assert_eq!(OptionKey::from_str(&key.to_string()), Ok(key));
        }
        for (constant, key) in [
            (
                topic_option_keys::COMPRESSION_ALGORITHM,
                OptionKey::CompressionAlgorithm,
            ),
            (topic_option_keys::MESSAGE_EXPIRY, OptionKey::MessageExpiry),
            (topic_option_keys::MAX_TOPIC_SIZE, OptionKey::MaxTopicSize),
            (topic_option_keys::SEGMENT_SIZE, OptionKey::SegmentSize),
            (topic_option_keys::DURABILITY, OptionKey::Durability),
            (
                topic_option_keys::CONSUMER_OFFSET_DURABILITY,
                OptionKey::ConsumerOffsetDurability,
            ),
            (
                topic_option_keys::PARTITION_RESIZE_POLICY,
                OptionKey::PartitionResizePolicy,
            ),
            (
                topic_option_keys::MESSAGES_REQUIRED_TO_SAVE,
                OptionKey::MessagesRequiredToSave,
            ),
            (
                topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
                OptionKey::SizeOfMessagesRequiredToSave,
            ),
            (
                topic_option_keys::PREALLOCATE_SEGMENTS,
                OptionKey::PreallocateSegments,
            ),
        ] {
            assert_eq!(constant, key.to_string());
        }
        let resettable: Vec<OptionKey> = OptionKey::iter()
            .filter(|key| key.descriptor().is_resettable())
            .collect();
        assert_eq!(
            resettable,
            [OptionKey::MessageExpiry, OptionKey::MaxTopicSize]
        );
    }

    #[test]
    fn given_canonical_and_text_values_when_decoding_should_match_typed_values() {
        let cases: Vec<(OptionKey, HeaderValue, HeaderValue)> = vec![
            (
                OptionKey::CompressionAlgorithm,
                HeaderValue::from(2u8),
                HeaderValue::try_from("gzip").unwrap(),
            ),
            (
                OptionKey::CompressionAlgorithm,
                HeaderValue::try_from("gzip").unwrap(),
                HeaderValue::try_from("gzip").unwrap(),
            ),
            (
                OptionKey::MessageExpiry,
                HeaderValue::try_from("5s").unwrap(),
                HeaderValue::from(5_000_000u64),
            ),
            (
                OptionKey::MessageExpiry,
                HeaderValue::try_from("18446744073709551614").unwrap(),
                HeaderValue::from(u64::MAX - 1),
            ),
            (
                OptionKey::MaxTopicSize,
                HeaderValue::try_from("2 GiB").unwrap(),
                HeaderValue::from(2u64 * 1024 * 1024 * 1024),
            ),
            (
                OptionKey::SegmentSize,
                HeaderValue::try_from("128MiB").unwrap(),
                HeaderValue::from(134_217_728u64),
            ),
            (
                OptionKey::Durability,
                HeaderValue::try_from("persisted").unwrap(),
                HeaderValue::try_from("persisted").unwrap(),
            ),
            (
                OptionKey::ConsumerOffsetDurability,
                HeaderValue::try_from("replicated").unwrap(),
                HeaderValue::try_from("replicated").unwrap(),
            ),
            (
                OptionKey::PartitionResizePolicy,
                HeaderValue::try_from("fixed").unwrap(),
                HeaderValue::try_from("fixed").unwrap(),
            ),
            (
                OptionKey::MessagesRequiredToSave,
                HeaderValue::try_from("64").unwrap(),
                HeaderValue::from(64u32),
            ),
            (
                OptionKey::MessagesRequiredToSave,
                HeaderValue::from(64u32),
                HeaderValue::from(64u32),
            ),
            (
                OptionKey::SizeOfMessagesRequiredToSave,
                HeaderValue::try_from("4KiB").unwrap(),
                HeaderValue::from(4096u64),
            ),
            (
                OptionKey::PreallocateSegments,
                HeaderValue::try_from("true").unwrap(),
                HeaderValue::from(true),
            ),
            (
                OptionKey::PreallocateSegments,
                HeaderValue::from(false),
                HeaderValue::from(false),
            ),
        ];
        for (key, input, expected) in cases {
            let canonical = canonical_value(key, &entry(key, &input))
                .unwrap_or_else(|error| panic!("{key} {input:?}: {error}"))
                .expect("not a sentinel");
            assert_eq!(canonical, expected, "{key}");
        }
        let invalid_cases: Vec<(OptionKey, HeaderValue)> = vec![
            (OptionKey::CompressionAlgorithm, HeaderValue::from(9u8)),
            (OptionKey::MessageExpiry, HeaderValue::from(7u32)),
            (
                OptionKey::MessageExpiry,
                HeaderValue::try_from("99999999999999999999").unwrap(),
            ),
            (OptionKey::MessagesRequiredToSave, HeaderValue::from(0u32)),
            (OptionKey::MessagesRequiredToSave, HeaderValue::from(7u64)),
            (
                OptionKey::MessagesRequiredToSave,
                HeaderValue::from(MAX_MESSAGES_REQUIRED_TO_SAVE + 1),
            ),
            (
                OptionKey::SizeOfMessagesRequiredToSave,
                HeaderValue::from(MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE + 1),
            ),
            (OptionKey::PreallocateSegments, HeaderValue::from(2u8)),
            (
                OptionKey::Durability,
                HeaderValue::try_from("eventually").unwrap(),
            ),
        ];
        for (key, input) in invalid_cases {
            assert_eq!(
                canonical_value(key, &entry(key, &input)),
                Err(IggyError::InvalidOptionValue(key.to_string())),
                "{key} {input:?}"
            );
        }
    }

    #[test]
    fn given_create_only_option_when_resetting_should_reject_not_updatable() {
        for key in [OptionKey::SegmentSize, OptionKey::Durability] {
            assert_eq!(
                admit(
                    OptionsScope::Topic,
                    OptionOperation::Update,
                    &WireOptions::empty(),
                    &[key.to_string()],
                    &OptionAdmissionContext::default(),
                ),
                Err(IggyError::OptionNotUpdatable(key.to_string()))
            );
        }
        assert_eq!(
            admit(
                OptionsScope::Topic,
                OptionOperation::Update,
                &WireOptions::empty(),
                &[OptionKey::Name.to_string()],
                &OptionAdmissionContext::default(),
            ),
            Err(IggyError::OptionNotResettable(OptionKey::Name.to_string()))
        );
        let admitted = admit(
            OptionsScope::Topic,
            OptionOperation::Update,
            &WireOptions::empty(),
            &[OptionKey::MaxTopicSize.to_string()],
            &OptionAdmissionContext::default(),
        )
        .expect("a mutable key with a default resets");
        assert_eq!(admitted.reset, [OptionKey::MaxTopicSize]);
        for scope in [OptionsScope::Stream, OptionsScope::User] {
            for key in descriptors_for(scope).map(|descriptor| descriptor.key) {
                assert!(
                    admit(
                        scope,
                        OptionOperation::Update,
                        &WireOptions::empty(),
                        &[key.to_string()],
                        &OptionAdmissionContext::default(),
                    )
                    .is_err(),
                    "{key} must not reset in {scope}"
                );
            }
        }
    }

    #[test]
    fn given_fixed_create_columns_when_repeated_in_options_should_reject() {
        assert_eq!(
            admit(
                OptionsScope::Stream,
                OptionOperation::Create,
                &block(&[(OptionKey::Name, HeaderValue::try_from("shadow").unwrap())]),
                &[],
                &OptionAdmissionContext::default(),
            ),
            Err(IggyError::InvalidOptionValue(OptionKey::Name.to_string()))
        );
        assert_eq!(
            admit(
                OptionsScope::User,
                OptionOperation::Create,
                &block(&[(OptionKey::Status, HeaderValue::try_from("active").unwrap())]),
                &[],
                &OptionAdmissionContext::default(),
            ),
            Err(IggyError::InvalidOptionValue(OptionKey::Status.to_string()))
        );
        let admitted = admit(
            OptionsScope::User,
            OptionOperation::Update,
            &block(&[
                (
                    OptionKey::Status,
                    HeaderValue::try_from("inactive").unwrap(),
                ),
                (
                    OptionKey::Username,
                    HeaderValue::try_from("renamed").unwrap(),
                ),
            ]),
            &[],
            &OptionAdmissionContext::default(),
        )
        .expect("columns patch through an update");
        assert_eq!(admitted.columns.status, Some(UserStatus::Inactive));
        assert_eq!(admitted.columns.username.as_deref(), Some("renamed"));
        assert!(admitted.explicit.is_empty());
        assert_eq!(
            admit(
                OptionsScope::User,
                OptionOperation::Update,
                &block(&[(OptionKey::Username, HeaderValue::try_from("ab").unwrap())]),
                &[],
                &OptionAdmissionContext::default(),
            ),
            Err(IggyError::InvalidUsername)
        );
        assert_eq!(
            admit(
                OptionsScope::Topic,
                OptionOperation::Update,
                &block(&[(OptionKey::Name, HeaderValue::from(7u32))]),
                &[],
                &OptionAdmissionContext::default(),
            ),
            Err(IggyError::InvalidTopicName)
        );
    }

    #[test]
    fn given_update_of_only_zero_sentinels_when_admitting_should_refuse_it_as_empty() {
        for entries in [
            vec![(OptionKey::MessageExpiry, HeaderValue::from(0u64))],
            vec![
                (OptionKey::MaxTopicSize, HeaderValue::from(0u64)),
                (OptionKey::MessageExpiry, HeaderValue::from(0u64)),
            ],
        ] {
            assert_eq!(
                admit(
                    OptionsScope::Topic,
                    OptionOperation::Update,
                    &block(&entries),
                    &[],
                    &OptionAdmissionContext::default(),
                )
                .err(),
                Some(IggyError::InvalidCommand),
                "{entries:?}"
            );
        }
    }

    #[test]
    fn given_topic_sentinels_when_canonicalizing_should_preserve_existing_provenance_rules() {
        let admitted = admit(
            OptionsScope::Topic,
            OptionOperation::Update,
            &block(&[
                (OptionKey::MessageExpiry, HeaderValue::from(0u64)),
                (OptionKey::MaxTopicSize, HeaderValue::from(0u64)),
                (OptionKey::Name, HeaderValue::try_from("orders").unwrap()),
            ]),
            &[],
            &OptionAdmissionContext::default(),
        )
        .expect("zero sentinels next to a rename are accepted");
        assert!(
            admitted.explicit.is_empty(),
            "a zero sentinel leaves the stored value and its provenance alone"
        );
        let admitted = admit(
            OptionsScope::Topic,
            OptionOperation::Update,
            &block(&[(
                OptionKey::MessageExpiry,
                HeaderValue::try_from("1 day").unwrap(),
            )]),
            &[OptionKey::MaxTopicSize.to_string()],
            &OptionAdmissionContext::default(),
        )
        .expect("set and reset");
        assert_eq!(
            admitted.explicit[&HeaderKey::from(OptionKey::MessageExpiry)],
            OptionValue::explicit(HeaderValue::from(86_400_000_000u64))
        );
        assert_eq!(admitted.reset, [OptionKey::MaxTopicSize]);
    }

    #[test]
    fn given_oversized_patch_result_when_admitting_should_reject_without_mutation() {
        let mut current = ResourceOptions::new();
        for index in 0..iggy_binary_protocol::MAX_OPTIONS {
            current.insert(
                HeaderKey::try_from(format!("future_key_{index:05}").as_str()).unwrap(),
                OptionValue::explicit(HeaderValue::from(1u8)),
            );
        }
        let context = OptionAdmissionContext {
            current: Some(current.clone()),
            ..OptionAdmissionContext::default()
        };
        assert!(matches!(
            admit(
                OptionsScope::Topic,
                OptionOperation::Update,
                &block(&[(OptionKey::MessageExpiry, HeaderValue::from(9u64))]),
                &[],
                &context,
            ),
            Err(IggyError::OptionsBlockTooLarge(_))
        ));
        assert_eq!(context.current, Some(current));
    }

    #[test]
    fn given_patch_result_at_the_byte_cap_when_admitting_should_admit_it_but_not_one_byte_more() {
        let filler_key = |index: usize| {
            HeaderKey::try_from(format!("future_key_{index:05}").as_str()).expect("valid key")
        };
        let filler_value =
            |length: usize| HeaderValue::try_from(vec![1u8; length].as_slice()).expect("fits");
        let mut current = ResourceOptions::new();
        for key in [OptionKey::MessageExpiry, OptionKey::MaxTopicSize] {
            current.insert(
                HeaderKey::from(key),
                OptionValue::explicit(HeaderValue::from(1u8)),
            );
        }
        for index in 0..355 {
            current.insert(filler_key(index), OptionValue::explicit(filler_value(255)));
        }
        let patch = block(&[(OptionKey::MessageExpiry, HeaderValue::from(9u64))]);
        let reset = [OptionKey::MaxTopicSize.to_string()];
        let mut resulting = current.clone();
        resulting.insert(
            HeaderKey::from(OptionKey::MessageExpiry),
            OptionValue::explicit(HeaderValue::from(9u64)),
        );
        resulting.insert(
            HeaderKey::from(OptionKey::MaxTopicSize),
            OptionValue::derived(
                OptionKey::MaxTopicSize
                    .descriptor()
                    .default_value()
                    .expect("default encodes")
                    .expect("has a default"),
            ),
        );
        let resulting_size = resource_options_to_wire(&resulting, OptionsProvenance::All)
            .expect("under the cap")
            .as_bytes()
            .len();
        // The last entry's kinds, lengths and 16-byte key.
        let free = iggy_binary_protocol::MAX_OPTIONS_BYTES - resulting_size - 26;
        assert!(
            (1..255).contains(&free),
            "the last value fits a header value"
        );
        let admit_with_last_value = |length: usize| {
            let mut current = current.clone();
            current.insert(filler_key(355), OptionValue::explicit(filler_value(length)));
            let context = OptionAdmissionContext {
                current: Some(current),
                ..OptionAdmissionContext::default()
            };
            admit(
                OptionsScope::Topic,
                OptionOperation::Update,
                &patch,
                &reset,
                &context,
            )
        };

        assert!(admit_with_last_value(free).is_ok(), "exactly at the cap");
        assert!(matches!(
            admit_with_last_value(free + 1),
            Err(IggyError::OptionsBlockTooLarge(_))
        ));
    }

    #[test]
    fn given_unknown_critical_option_when_projecting_should_refuse_without_echoing_value() {
        let mut stored = ResourceOptions::new();
        stored.insert(
            HeaderKey::try_from("future_secret").unwrap(),
            OptionValue::explicit(HeaderValue::try_from("do-not-echo").unwrap()),
        );
        stored.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::derived(HeaderValue::try_from("future_secret").unwrap()),
        );
        assert_eq!(
            public_options(OptionsScope::Stream, &stored),
            Err(IggyError::UnsupportedCriticalOption(
                "future_secret".to_owned()
            ))
        );

        let mut stored = ResourceOptions::new();
        stored.insert(
            HeaderKey::try_from("future_tuning").unwrap(),
            OptionValue::explicit(HeaderValue::from(5u64)),
        );
        stored.insert(
            HeaderKey::from(OptionKey::MessageExpiry),
            OptionValue::derived(HeaderValue::from(9u64)),
        );
        let public = public_options(OptionsScope::Stream, &stored)
            .expect("non-critical unknown keys round-trip");
        assert_eq!(
            public[&HeaderKey::try_from("future_tuning").unwrap()],
            OptionValue::explicit(HeaderValue::from(5u64))
        );
        assert_eq!(
            public[&HeaderKey::from(OptionKey::EncryptionKeySource)],
            OptionValue::derived(HeaderValue::try_from("none").unwrap())
        );
        assert!(!public.contains_key(&HeaderKey::from(OptionKey::EncryptionKeyFingerprint)));
        assert!(!public.contains_key(&HeaderKey::from(OptionKey::EncryptionKekFingerprint)));
    }

    #[test]
    fn given_wrap_set_when_projecting_stream_should_report_origin_and_both_fingerprints() {
        let context = crate::ServerKeyContext::from_config(
            "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=",
            &[],
            true,
        )
        .unwrap();
        let key = StreamEncryptionKey::try_from([5u8; 32].as_slice()).unwrap();
        for (origin, source) in [
            (StreamKeyOrigin::Client, "stream"),
            (StreamKeyOrigin::Server, "server"),
        ] {
            let set = WrapSet::from(context.wrap(&key, origin).unwrap());
            let mut stored = ResourceOptions::new();
            stored.insert(
                HeaderKey::from(OptionKey::EncryptionKeyWrapped),
                OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
            );
            stored.insert(
                HeaderKey::from(OptionKey::CriticalKeys),
                OptionValue::explicit(
                    critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).unwrap(),
                ),
            );
            let public = public_options(OptionsScope::Stream, &stored).unwrap();
            let text = |key: OptionKey| {
                public[&HeaderKey::from(key)]
                    .value
                    .as_str()
                    .unwrap()
                    .to_owned()
            };
            assert_eq!(text(OptionKey::EncryptionKeySource), source);
            assert_eq!(text(OptionKey::EncryptionKeyFingerprint), key.fingerprint());
            assert_eq!(
                text(OptionKey::EncryptionKekFingerprint),
                context.kek_fingerprint().unwrap()
            );
            assert!(!public.contains_key(&HeaderKey::from(OptionKey::EncryptionKeyWrapped)));
            assert!(!public.contains_key(&HeaderKey::from(OptionKey::CriticalKeys)));
        }
    }

    #[test]
    fn given_malformed_critical_marker_when_resolving_should_mark_unavailable() {
        let wrapped = crate::ServerKeyContext::from_config(
            "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=",
            &[],
            false,
        )
        .unwrap()
        .wrap(
            &StreamEncryptionKey::try_from([5u8; 32].as_slice()).unwrap(),
            StreamKeyOrigin::Client,
        )
        .unwrap();
        let with_wrapped = |marker: Option<&str>| {
            let mut stored = ResourceOptions::new();
            stored.insert(
                HeaderKey::from(OptionKey::EncryptionKeyWrapped),
                OptionValue::derived(HeaderValue::try_from(wrapped.as_ref()).unwrap()),
            );
            if let Some(marker) = marker {
                stored.insert(
                    HeaderKey::from(OptionKey::CriticalKeys),
                    OptionValue::derived(HeaderValue::try_from(marker).unwrap()),
                );
            }
            stored
        };
        assert_eq!(
            check_critical_options(&with_wrapped(Some("encryption_key_wrapped"))),
            Ok(())
        );
        assert_eq!(check_critical_options(&with_wrapped(None)), Ok(()));
        for marker in [
            "encryption_key_wrapped,encryption_key_wrapped",
            "encryption_key_wrapped,",
            "Encryption_key_wrapped",
            "zz_unknown,encryption_key_wrapped",
            "encryption_key_wrapped,message_expiry",
        ] {
            assert!(
                check_critical_options(&with_wrapped(Some(marker))).is_err(),
                "{marker} must be refused"
            );
        }
        let mut stored = with_wrapped(Some("encryption_key_wrapped"));
        stored.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::derived(HeaderValue::try_from([2u8; 78].as_slice()).unwrap()),
        );
        assert!(check_critical_options(&stored).is_err());
        let mut topic = ResourceOptions::new();
        topic.insert(
            HeaderKey::from(OptionKey::SegmentSize),
            OptionValue::derived(HeaderValue::from(DEFAULT_SEGMENT_SIZE)),
        );
        assert_eq!(check_critical_options(&topic), Ok(()));
        assert_eq!(
            critical_keys_marker(&[OptionKey::EncryptionKeyWrapped])
                .unwrap()
                .as_bytes(),
            b"encryption_key_wrapped"
        );
    }

    #[test]
    fn given_admitted_stream_key_when_debugging_should_redact_it() {
        let key = HeaderValue::try_from([7u8; 32].as_slice()).unwrap();
        let admitted = admit(
            OptionsScope::Stream,
            OptionOperation::Create,
            &block(&[(OptionKey::EncryptionKey, key)]),
            &[],
            &OptionAdmissionContext::default(),
        )
        .expect("a 32-byte key is admitted");
        let rendered = format!("{admitted:?}");
        assert!(!rendered.contains("7, 7"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }

    #[test]
    fn given_invalid_requests_when_admitting_should_report_categories_in_order() {
        let context = OptionAdmissionContext::default();
        let set_and_reset = admit(
            OptionsScope::Topic,
            OptionOperation::Update,
            &block(&[(OptionKey::MessageExpiry, HeaderValue::from(9u64))]),
            &[OptionKey::MessageExpiry.to_string()],
            &context,
        );
        assert_eq!(set_and_reset, Err(IggyError::InvalidCommand));
        assert_eq!(
            admit(
                OptionsScope::Topic,
                OptionOperation::Update,
                &WireOptions::empty(),
                &[],
                &context
            ),
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(
            admit(
                OptionsScope::Stream,
                OptionOperation::Update,
                &block(&[(OptionKey::MessageExpiry, HeaderValue::from(9u64))]),
                &[],
                &context
            ),
            Err(IggyError::UnsupportedOptionKey(
                OptionKey::MessageExpiry.to_string()
            ))
        );
        assert_eq!(
            admit(
                OptionsScope::Stream,
                OptionOperation::Create,
                &block(&[(
                    OptionKey::EncryptionKeyWrapped,
                    HeaderValue::try_from([1u8; 77].as_slice()).unwrap()
                )]),
                &[],
                &context
            ),
            Err(IggyError::ReadOnlyOption(
                OptionKey::EncryptionKeyWrapped.to_string()
            ))
        );
        assert_eq!(
            admit(
                OptionsScope::Stream,
                OptionOperation::Update,
                &block(&[(
                    OptionKey::EncryptionKey,
                    HeaderValue::try_from([1u8; 32].as_slice()).unwrap()
                )]),
                &[],
                &context
            ),
            Err(IggyError::OptionNotUpdatable(
                OptionKey::EncryptionKey.to_string()
            ))
        );
        let created = admit(
            OptionsScope::Stream,
            OptionOperation::Create,
            &block(&[(
                OptionKey::EncryptionKey,
                HeaderValue::try_from("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap(),
            )]),
            &[],
            &context,
        )
        .expect("a canonical base64 stream key is admitted");
        let stored = &created.explicit[&HeaderKey::from(OptionKey::EncryptionKey)];
        assert_eq!(stored.value.kind(), HeaderKind::Raw);
        assert_eq!(stored.value.as_bytes(), (0u8..32).collect::<Vec<_>>());
    }

    fn entry<'a>(key: OptionKey, value: &'a HeaderValue) -> WireUserHeaderEntry<'a> {
        let name: &'static str = key.into();
        WireUserHeaderEntry {
            key_kind: WireHeaderKind(HeaderKind::String.as_code()),
            key: name.as_bytes(),
            value_kind: WireHeaderKind(value.kind().as_code()),
            value: value.as_bytes(),
        }
    }

    fn block(entries: &[(OptionKey, HeaderValue)]) -> WireOptions {
        let options: ResourceOptions = entries
            .iter()
            .map(|(key, value)| (HeaderKey::from(*key), OptionValue::explicit(value.clone())))
            .collect();
        resource_options_to_wire(&options, OptionsProvenance::All).expect("encodes")
    }
}
