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

use super::key::OptionKey;
use super::{
    Durability, MAX_MESSAGES_REQUIRED_TO_SAVE, MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
    PartitionResizePolicy,
};
use crate::types::compression::compression_algorithm::CompressionAlgorithm;
use crate::types::message::{HeaderKind, HeaderValue};
use crate::{IggyByteSize, IggyError, IggyExpiry, MaxTopicSize, UserStatus};
use iggy_binary_protocol::WireUserHeaderEntry;
use std::str::FromStr;

/// One value type an option key carries: how it reads from a wire entry,
/// its canonical encoding and its string form.
///
/// `decode` accepts the canonical kind and a `String` in the same form a
/// config file or an HTTP body carries, and applies the bounds that belong to
/// `key` itself. Bounds that depend on another key or on stored state are
/// checked by the caller.
pub trait OptionValueCodec: Sized {
    const KIND: HeaderKind;

    /// # Errors
    ///
    /// `InvalidOptionValue` naming `key` when the kind, the payload or a
    /// per-key bound does not hold.
    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError>;

    /// # Errors
    ///
    /// `InvalidHeaderValue` when the canonical form does not fit a header value.
    fn encode(&self) -> Result<HeaderValue, IggyError>;

    fn to_option_string(&self) -> String;
}

impl OptionValueCodec for CompressionAlgorithm {
    const KIND: HeaderKind = HeaderKind::String;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        if entry.value_kind.0 == HeaderKind::Uint8.as_code() {
            // Exactly one byte, for the same reason as the bool decoder.
            let [code] = entry.value else {
                return Err(invalid(key));
            };
            return Self::from_code(*code).map_err(|_| invalid(key));
        }
        parse_text(key, entry, |text| Self::from_str(text).ok())
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        HeaderValue::try_from(self.to_string().as_str())
    }

    fn to_option_string(&self) -> String {
        self.to_string()
    }
}

impl OptionValueCodec for IggyExpiry {
    const KIND: HeaderKind = HeaderKind::Uint64;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        decode_u64_or(key, entry, |text| Self::from_str(text).ok().map(u64::from)).map(Self::from)
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        Ok(HeaderValue::from(u64::from(*self)))
    }

    fn to_option_string(&self) -> String {
        u64::from(*self).to_string()
    }
}

impl OptionValueCodec for MaxTopicSize {
    const KIND: HeaderKind = HeaderKind::Uint64;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        decode_u64_or(key, entry, |text| Self::from_str(text).ok().map(u64::from)).map(Self::from)
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        Ok(HeaderValue::from(u64::from(*self)))
    }

    fn to_option_string(&self) -> String {
        u64::from(*self).to_string()
    }
}

impl OptionValueCodec for IggyByteSize {
    const KIND: HeaderKind = HeaderKind::Uint64;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        let bytes = decode_u64_or(key, entry, |text| {
            Self::from_str(text).ok().map(|size| size.as_bytes_u64())
        })?;
        if key == OptionKey::SizeOfMessagesRequiredToSave
            && bytes > MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE
        {
            return Err(invalid(key));
        }
        Ok(Self::from(bytes))
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        Ok(HeaderValue::from(self.as_bytes_u64()))
    }

    fn to_option_string(&self) -> String {
        self.as_bytes_u64().to_string()
    }
}

impl OptionValueCodec for Durability {
    const KIND: HeaderKind = HeaderKind::String;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        parse_text(key, entry, |text| text.parse().ok())
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        HeaderValue::from_str(self.as_ref())
    }

    fn to_option_string(&self) -> String {
        self.to_string()
    }
}

impl OptionValueCodec for PartitionResizePolicy {
    const KIND: HeaderKind = HeaderKind::String;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        parse_text(key, entry, |text| text.parse().ok())
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        HeaderValue::from_str(self.as_ref())
    }

    fn to_option_string(&self) -> String {
        self.to_string()
    }
}

impl OptionValueCodec for UserStatus {
    const KIND: HeaderKind = HeaderKind::String;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        parse_text(key, entry, |text| text.parse().ok())
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        HeaderValue::try_from(self.to_string().as_str())
    }

    fn to_option_string(&self) -> String {
        self.to_string()
    }
}

impl OptionValueCodec for bool {
    const KIND: HeaderKind = HeaderKind::Bool;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        if entry.value_kind.0 == HeaderKind::Bool.as_code() {
            // Exactly one byte of 0 or 1. Anything looser admits a value that
            // `HeaderValue::as_bool` later refuses to read, so the stored map
            // would hold an entry its own public accessor rejects.
            return match entry.value {
                [0] => Ok(false),
                [1] => Ok(true),
                _ => Err(invalid(key)),
            };
        }
        parse_text(key, entry, |text| text.parse().ok())
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        Ok(HeaderValue::from(*self))
    }

    fn to_option_string(&self) -> String {
        self.to_string()
    }
}

impl OptionValueCodec for u32 {
    const KIND: HeaderKind = HeaderKind::Uint32;

    fn decode(key: OptionKey, entry: &WireUserHeaderEntry<'_>) -> Result<Self, IggyError> {
        let value = if entry.value_kind.0 == HeaderKind::Uint32.as_code() {
            let bytes: [u8; 4] = entry.value.try_into().map_err(|_| invalid(key))?;
            Self::from_le_bytes(bytes)
        } else {
            parse_text(key, entry, |text| text.parse().ok())?
        };
        if key == OptionKey::MessagesRequiredToSave
            && (value == 0 || value > MAX_MESSAGES_REQUIRED_TO_SAVE)
        {
            return Err(invalid(key));
        }
        Ok(value)
    }

    fn encode(&self) -> Result<HeaderValue, IggyError> {
        Ok(HeaderValue::from(*self))
    }

    fn to_option_string(&self) -> String {
        self.to_string()
    }
}

/// `InvalidOptionValue` naming `key`.
pub(super) fn invalid(key: OptionKey) -> IggyError {
    IggyError::InvalidOptionValue(key.to_string())
}

/// `Uint64` verbatim, or a `String` holding a checked decimal `u64` or the
/// type's own text form.
///
/// The decimal form comes first so a client that cannot carry a 64-bit integer
/// natively (a JSON number above 2^53) can still send the exact value.
fn decode_u64_or(
    key: OptionKey,
    entry: &WireUserHeaderEntry<'_>,
    from_text: impl Fn(&str) -> Option<u64>,
) -> Result<u64, IggyError> {
    if entry.value_kind.0 == HeaderKind::Uint64.as_code() {
        let bytes: [u8; 8] = entry.value.try_into().map_err(|_| invalid(key))?;
        return Ok(u64::from_le_bytes(bytes));
    }
    parse_text(key, entry, |text| {
        if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
            return text.parse().ok();
        }
        from_text(text)
    })
}

/// The value of a `String` entry through `from_text`, refusing every other kind.
fn parse_text<T>(
    key: OptionKey,
    entry: &WireUserHeaderEntry<'_>,
    from_text: impl Fn(&str) -> Option<T>,
) -> Result<T, IggyError> {
    if entry.value_kind.0 != HeaderKind::String.as_code() {
        return Err(invalid(key));
    }
    std::str::from_utf8(entry.value)
        .ok()
        .and_then(from_text)
        .ok_or_else(|| invalid(key))
}
