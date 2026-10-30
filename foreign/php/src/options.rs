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

use std::collections::BTreeSet;

use ext_php_rs::{
    binary::Binary,
    boxed::ZBox,
    convert::{IntoZval, StubLiteral},
    exception::PhpResult,
    php_class, php_enum, php_impl,
    types::{ZendHashTable, Zval},
};
use iggy::prelude::{
    HeaderKind, HeaderValue, IggyError, OptionKey, OptionSpec as RustOptionSpec,
    OptionsScope as RustOptionsScope, ResourceOptions,
};

use crate::error::to_php_exception;

/// Stream option keys, as the cases an update's reset list takes.
///
/// No stream option is resettable in this release, so `Client::updateStream()` refuses every
/// case with the error the server would return.
#[php_enum]
#[php(name = "Iggy\\StreamOptionKey")]
#[derive(Clone, Copy)]
pub enum StreamOptionKey {
    #[php(value = "name")]
    Name,
    #[php(value = "encryption_key")]
    EncryptionKey,
    #[php(value = "encryption_key_source")]
    EncryptionKeySource,
    #[php(value = "encryption_key_fingerprint")]
    EncryptionKeyFingerprint,
    #[php(value = "encryption_kek_fingerprint")]
    EncryptionKekFingerprint,
}

impl From<StreamOptionKey> for OptionKey {
    fn from(key: StreamOptionKey) -> Self {
        match key {
            StreamOptionKey::Name => Self::Name,
            StreamOptionKey::EncryptionKey => Self::EncryptionKey,
            StreamOptionKey::EncryptionKeySource => Self::EncryptionKeySource,
            StreamOptionKey::EncryptionKeyFingerprint => Self::EncryptionKeyFingerprint,
            StreamOptionKey::EncryptionKekFingerprint => Self::EncryptionKekFingerprint,
        }
    }
}

// Lets a reset list default to `[]` in the generated stub.
impl StubLiteral for StreamOptionKey {
    fn stub_literal(&self) -> String {
        let case = match self {
            Self::Name => "Name",
            Self::EncryptionKey => "EncryptionKey",
            Self::EncryptionKeySource => "EncryptionKeySource",
            Self::EncryptionKeyFingerprint => "EncryptionKeyFingerprint",
            Self::EncryptionKekFingerprint => "EncryptionKekFingerprint",
        };
        format!("\\Iggy\\StreamOptionKey::{case}")
    }
}

/// Topic option keys, as the cases an update's reset list takes.
///
/// Only `MessageExpiry` and `MaxTopicSize` are resettable in this release. The server refuses
/// every other case with its own category.
#[php_enum]
#[php(name = "Iggy\\TopicOptionKey")]
#[derive(Clone, Copy)]
pub enum TopicOptionKey {
    #[php(value = "name")]
    Name,
    #[php(value = "compression_algorithm")]
    CompressionAlgorithm,
    #[php(value = "message_expiry")]
    MessageExpiry,
    #[php(value = "max_topic_size")]
    MaxTopicSize,
    #[php(value = "segment_size")]
    SegmentSize,
    #[php(value = "durability")]
    Durability,
    #[php(value = "consumer_offset_durability")]
    ConsumerOffsetDurability,
    #[php(value = "partition_resize_policy")]
    PartitionResizePolicy,
    #[php(value = "messages_required_to_save")]
    MessagesRequiredToSave,
    #[php(value = "size_of_messages_required_to_save")]
    SizeOfMessagesRequiredToSave,
    #[php(value = "preallocate_segments")]
    PreallocateSegments,
}

impl From<TopicOptionKey> for OptionKey {
    fn from(key: TopicOptionKey) -> Self {
        match key {
            TopicOptionKey::Name => Self::Name,
            TopicOptionKey::CompressionAlgorithm => Self::CompressionAlgorithm,
            TopicOptionKey::MessageExpiry => Self::MessageExpiry,
            TopicOptionKey::MaxTopicSize => Self::MaxTopicSize,
            TopicOptionKey::SegmentSize => Self::SegmentSize,
            TopicOptionKey::Durability => Self::Durability,
            TopicOptionKey::ConsumerOffsetDurability => Self::ConsumerOffsetDurability,
            TopicOptionKey::PartitionResizePolicy => Self::PartitionResizePolicy,
            TopicOptionKey::MessagesRequiredToSave => Self::MessagesRequiredToSave,
            TopicOptionKey::SizeOfMessagesRequiredToSave => Self::SizeOfMessagesRequiredToSave,
            TopicOptionKey::PreallocateSegments => Self::PreallocateSegments,
        }
    }
}

// Lets a reset list default to `[]` in the generated stub.
impl StubLiteral for TopicOptionKey {
    fn stub_literal(&self) -> String {
        let case = match self {
            Self::Name => "Name",
            Self::CompressionAlgorithm => "CompressionAlgorithm",
            Self::MessageExpiry => "MessageExpiry",
            Self::MaxTopicSize => "MaxTopicSize",
            Self::SegmentSize => "SegmentSize",
            Self::Durability => "Durability",
            Self::ConsumerOffsetDurability => "ConsumerOffsetDurability",
            Self::PartitionResizePolicy => "PartitionResizePolicy",
            Self::MessagesRequiredToSave => "MessagesRequiredToSave",
            Self::SizeOfMessagesRequiredToSave => "SizeOfMessagesRequiredToSave",
            Self::PreallocateSegments => "PreallocateSegments",
        };
        format!("\\Iggy\\TopicOptionKey::{case}")
    }
}

/// The resource whose option catalog `Client::describeOptions()` reads.
#[php_enum]
#[php(name = "Iggy\\OptionsScope")]
#[derive(Clone, Copy)]
pub enum OptionsScope {
    #[php(value = "topic")]
    Topic,
    #[php(value = "stream")]
    Stream,
    #[php(value = "user")]
    User,
}

impl From<OptionsScope> for RustOptionsScope {
    fn from(scope: OptionsScope) -> Self {
        match scope {
            OptionsScope::Topic => Self::Topic,
            OptionsScope::Stream => Self::Stream,
            OptionsScope::User => Self::User,
        }
    }
}

/// One option key as the server's catalog describes it.
#[php_class]
#[php(name = "Iggy\\OptionSpec")]
pub struct OptionSpec {
    inner: RustOptionSpec,
}

impl From<RustOptionSpec> for OptionSpec {
    fn from(inner: RustOptionSpec) -> Self {
        Self { inner }
    }
}

#[php_impl]
impl OptionSpec {
    #[php(getter)]
    pub fn key(&self) -> String {
        self.inner.key.clone()
    }

    /// The value kind, for example `string`, `uint64` or `raw`.
    #[php(getter)]
    pub fn kind(&self) -> String {
        self.inner.kind.to_string()
    }

    /// An update may set the key.
    #[php(getter)]
    pub fn updatable(&self) -> bool {
        self.inner.updatable
    }

    /// An update may reset the key to its default.
    #[php(getter)]
    pub fn resettable(&self) -> bool {
        self.inner.resettable
    }

    /// Accepted on create, never returned by a read.
    #[php(getter)]
    pub fn secret(&self) -> bool {
        self.inner.secret
    }

    /// Computed by the server and returned on reads, never accepted.
    #[php(getter, change_case = "snake_case")]
    pub fn read_only(&self) -> bool {
        self.inner.read_only
    }

    /// A node that does not understand the key must not serve the resource.
    #[php(getter)]
    pub fn critical(&self) -> bool {
        self.inner.critical
    }

    /// The default in the kind's binary encoding, an empty string when the key has none.
    #[php(getter, change_case = "snake_case")]
    pub fn default_value(&self) -> Binary<u8> {
        Binary::new(self.inner.default_value.clone())
    }

    #[php(getter)]
    pub fn description(&self) -> String {
        self.inner.description.clone()
    }
}

/// The typed and raw reset lists of an update.
///
/// A name listed twice, typed or raw, is refused as `InvalidCommand`, as server admission
/// refuses a repeated reset name.
pub(crate) fn reset_names<K: Into<OptionKey>>(
    typed: Vec<K>,
    raw: Option<Vec<String>>,
) -> PhpResult<(BTreeSet<OptionKey>, BTreeSet<String>)> {
    let mut reset = BTreeSet::new();
    let mut names = BTreeSet::new();
    for key in typed {
        let key = key.into();
        let name: &'static str = key.into();
        if !reset.insert(key) || !names.insert(name.to_owned()) {
            return Err(to_php_exception(IggyError::InvalidCommand));
        }
    }
    let mut raw_reset = BTreeSet::new();
    for name in raw.unwrap_or_default() {
        if !names.insert(name.clone()) {
            return Err(to_php_exception(IggyError::InvalidCommand));
        }
        raw_reset.insert(name);
    }
    Ok((reset, raw_reset))
}

/// The options of one provenance as a PHP array of option name to scalar value.
///
/// Integers that do not fit a PHP int, and 128-bit integers, are decimal strings. Raw values
/// are binary strings.
pub(crate) fn options_array(
    options: &ResourceOptions,
    explicit: bool,
) -> PhpResult<ZBox<ZendHashTable>> {
    let mut array = ZendHashTable::new();
    for (key, option) in options
        .iter()
        .filter(|(_, option)| option.explicit == explicit)
    {
        let name = key.as_str().map_err(to_php_exception)?;
        array
            .insert(name, option_scalar(&option.value)?)
            .map_err(to_php_exception)?;
    }
    Ok(array)
}

fn option_scalar(value: &HeaderValue) -> PhpResult<Zval> {
    let zval = match value.kind() {
        HeaderKind::Raw => Binary::new(value.as_bytes().to_vec()).into_zval(false),
        HeaderKind::String => value.as_str().map_err(to_php_exception)?.into_zval(false),
        HeaderKind::Bool => value.as_bool().map_err(to_php_exception)?.into_zval(false),
        HeaderKind::Int8 => i64::from(value.as_int8().map_err(to_php_exception)?).into_zval(false),
        HeaderKind::Int16 => {
            i64::from(value.as_int16().map_err(to_php_exception)?).into_zval(false)
        }
        HeaderKind::Int32 => {
            i64::from(value.as_int32().map_err(to_php_exception)?).into_zval(false)
        }
        HeaderKind::Int64 => value.as_int64().map_err(to_php_exception)?.into_zval(false),
        HeaderKind::Int128 => value
            .as_int128()
            .map_err(to_php_exception)?
            .to_string()
            .into_zval(false),
        HeaderKind::Uint8 => {
            i64::from(value.as_uint8().map_err(to_php_exception)?).into_zval(false)
        }
        HeaderKind::Uint16 => {
            i64::from(value.as_uint16().map_err(to_php_exception)?).into_zval(false)
        }
        HeaderKind::Uint32 => {
            i64::from(value.as_uint32().map_err(to_php_exception)?).into_zval(false)
        }
        HeaderKind::Uint64 => {
            let number = value.as_uint64().map_err(to_php_exception)?;
            match i64::try_from(number) {
                Ok(number) => number.into_zval(false),
                Err(_) => number.to_string().into_zval(false),
            }
        }
        HeaderKind::Uint128 => value
            .as_uint128()
            .map_err(to_php_exception)?
            .to_string()
            .into_zval(false),
        HeaderKind::Float32 => {
            f64::from(value.as_float32().map_err(to_php_exception)?).into_zval(false)
        }
        HeaderKind::Float64 => value
            .as_float64()
            .map_err(to_php_exception)?
            .into_zval(false),
    };
    zval.map_err(to_php_exception)
}
