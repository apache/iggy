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

use super::describe_options::{OPTIONS_SCOPE_STREAM, OPTIONS_SCOPE_TOPIC, OPTIONS_SCOPE_USER};
use crate::WireError;
use crate::WireIdentifier;
use crate::codec::{WireDecode, WireEncode, read_u8};
use crate::primitives::identifier::WireName;
use crate::primitives::options::{
    WireOptions, decode_options_prefixed, encode_options_prefixed, options_prefixed_size,
};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::slice::Iter;

/// Most reset names one patch carries, the reach of its count byte.
pub const MAX_RESET_KEYS: usize = u8::MAX as usize;

/// The resource an `UpdateResource` patches, tagged by its options scope code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceTarget {
    Topic {
        stream_id: WireIdentifier,
        topic_id: WireIdentifier,
    },
    Stream {
        stream_id: WireIdentifier,
    },
    User {
        user_id: WireIdentifier,
    },
}

/// Patch one resource's options: set the keys in `options`, reset the keys in
/// `reset` to their defaults, leave every other key alone.
///
/// Wire format:
/// ```text
/// [scope:u8][identifiers][reset_count:u8][reset_key:WireName]*[options to end of body]
/// ```
/// Identifiers are `[stream_id][topic_id]` for a topic, `[stream_id]` for a
/// stream and `[user_id]` for a user. A patch with nothing to set or reset, a
/// key named twice, or a key both set and reset is refused at decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateResourceRequest {
    pub target: ResourceTarget,
    pub reset: WireResetKeys,
    pub options: WireOptions,
}

/// The names a patch resets, at most [`MAX_RESET_KEYS`] so the count byte
/// always matches the names after it.
///
/// Wire format: `[count:u8][name:WireName]*`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WireResetKeys(Vec<WireName>);

impl ResourceTarget {
    /// The options scope code the target is tagged with on the wire.
    #[must_use]
    pub const fn scope_code(&self) -> u8 {
        match self {
            Self::Topic { .. } => OPTIONS_SCOPE_TOPIC,
            Self::Stream { .. } => OPTIONS_SCOPE_STREAM,
            Self::User { .. } => OPTIONS_SCOPE_USER,
        }
    }
}

impl TryFrom<Vec<WireName>> for WireResetKeys {
    type Error = WireError;

    fn try_from(names: Vec<WireName>) -> Result<Self, Self::Error> {
        if names.len() > MAX_RESET_KEYS {
            return Err(validation("more reset keys than one patch carries"));
        }
        Ok(Self(names))
    }
}

impl Deref for WireResetKeys {
    type Target = [WireName];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for &'a WireResetKeys {
    type Item = &'a WireName;
    type IntoIter = Iter<'a, WireName>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl WireEncode for WireResetKeys {
    fn encoded_size(&self) -> usize {
        1 + self.0.iter().map(WireEncode::encoded_size).sum::<usize>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        // The constructor bounds the length to what the count byte holds.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(self.0.len() as u8);
        for name in &self.0 {
            name.encode(buf);
        }
    }
}

impl WireDecode for WireResetKeys {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let count = read_u8(buf, 0)?;
        let mut position = 1;
        let mut names = Vec::with_capacity(usize::from(count));
        let mut seen = BTreeSet::new();
        for _ in 0..count {
            let (name, consumed) = WireName::decode(&buf[position..])?;
            position += consumed;
            if !seen.insert(name.as_str().to_owned()) {
                return Err(validation("a reset key is named twice"));
            }
            names.push(name);
        }
        Ok((Self(names), position))
    }
}

impl WireEncode for ResourceTarget {
    fn encoded_size(&self) -> usize {
        1 + match self {
            Self::Topic {
                stream_id,
                topic_id,
            } => stream_id.encoded_size() + topic_id.encoded_size(),
            Self::Stream { stream_id } => stream_id.encoded_size(),
            Self::User { user_id } => user_id.encoded_size(),
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.scope_code());
        match self {
            Self::Topic {
                stream_id,
                topic_id,
            } => {
                stream_id.encode(buf);
                topic_id.encode(buf);
            }
            Self::Stream { stream_id } => stream_id.encode(buf),
            Self::User { user_id } => user_id.encode(buf),
        }
    }
}

impl WireDecode for ResourceTarget {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let scope = read_u8(buf, 0)?;
        let mut position = 1;
        let target = match scope {
            OPTIONS_SCOPE_TOPIC => {
                let (stream_id, consumed) = WireIdentifier::decode(&buf[position..])?;
                position += consumed;
                let (topic_id, consumed) = WireIdentifier::decode(&buf[position..])?;
                position += consumed;
                Self::Topic {
                    stream_id,
                    topic_id,
                }
            }
            OPTIONS_SCOPE_STREAM => {
                let (stream_id, consumed) = WireIdentifier::decode(&buf[position..])?;
                position += consumed;
                Self::Stream { stream_id }
            }
            OPTIONS_SCOPE_USER => {
                let (user_id, consumed) = WireIdentifier::decode(&buf[position..])?;
                position += consumed;
                Self::User { user_id }
            }
            unknown => {
                return Err(WireError::Validation(Cow::Owned(format!(
                    "unknown resource scope: {unknown}"
                ))));
            }
        };
        Ok((target, position))
    }
}

impl WireEncode for UpdateResourceRequest {
    fn encoded_size(&self) -> usize {
        self.target.encoded_size() + self.reset.encoded_size() + self.options.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.target.encode(buf);
        self.reset.encode(buf);
        self.options.encode(buf);
    }
}

impl WireDecode for UpdateResourceRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (target, mut position) = ResourceTarget::decode(buf)?;
        let (reset, consumed) = WireResetKeys::decode(&buf[position..])?;
        position += consumed;
        let options = WireOptions::from_slice(&buf[position..])?;
        for entry in &options {
            if reset
                .iter()
                .any(|name| name.as_str().as_bytes() == entry.key)
            {
                return Err(validation("a key is both set and reset"));
            }
        }
        if reset.is_empty() && options.is_empty() {
            return Err(validation("the patch sets and resets nothing"));
        }
        Ok((
            Self {
                target,
                reset,
                options,
            },
            buf.len(),
        ))
    }
}

const fn validation(reason: &'static str) -> WireError {
    WireError::Validation(Cow::Borrowed(reason))
}

/// Size of an optional name behind a presence byte.
pub(crate) fn optional_name_size(name: Option<&WireName>) -> usize {
    1 + name.map_or(0, WireEncode::encoded_size)
}

/// Write an optional name behind a strict 0/1 presence byte.
pub(crate) fn encode_optional_name(name: Option<&WireName>, buf: &mut BytesMut) {
    match name {
        Some(name) => {
            buf.put_u8(1);
            name.encode(buf);
        }
        None => buf.put_u8(0),
    }
}

/// Read an optional name behind a strict 0/1 presence byte at `position`.
pub(crate) fn decode_optional_name(
    buf: &[u8],
    position: usize,
) -> Result<(Option<WireName>, usize), WireError> {
    match read_u8(buf, position)? {
        0 => Ok((None, 1)),
        1 => {
            let (name, consumed) = WireName::decode(&buf[position + 1..])?;
            Ok((Some(name), 1 + consumed))
        }
        _ => Err(validation("presence byte must be 0 or 1")),
    }
}

/// Size of the explicit and derived length-prefixed option blocks.
pub(crate) fn resolved_blocks_size(explicit: &WireOptions, derived: &WireOptions) -> usize {
    options_prefixed_size(explicit) + options_prefixed_size(derived)
}

/// Write the explicit then derived length-prefixed option blocks.
pub(crate) fn encode_resolved_blocks(
    explicit: &WireOptions,
    derived: &WireOptions,
    buf: &mut BytesMut,
) {
    encode_options_prefixed(explicit, buf);
    encode_options_prefixed(derived, buf);
}

/// Read the explicit then derived blocks at `position`, which must end the
/// body exactly.
pub(crate) fn decode_resolved_blocks(
    buf: &[u8],
    position: usize,
) -> Result<(WireOptions, WireOptions), WireError> {
    let (explicit, consumed) = decode_options_prefixed(buf, position)?;
    let (derived, derived_consumed) = decode_options_prefixed(buf, position + consumed)?;
    if position + consumed + derived_consumed != buf.len() {
        return Err(validation(
            "trailing bytes after the resolved option blocks",
        ));
    }
    Ok((explicit, derived))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::user_headers::encode_user_headers;

    const STRING: u8 = 2;
    const UINT64: u8 = 12;

    #[test]
    fn given_resource_patch_when_roundtripping_should_match_golden_bytes() {
        let topic = UpdateResourceRequest {
            target: ResourceTarget::Topic {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::named("orders").unwrap(),
            },
            reset: reset_keys(&["max_topic_size"]),
            options: options(&[
                (
                    STRING,
                    b"message_expiry",
                    UINT64,
                    &86_400_000_000u64.to_le_bytes(),
                ),
                (STRING, b"name", STRING, b"orders-v2"),
            ]),
        };
        let bytes = topic.to_bytes();
        assert_eq!(&bytes[..1], &[OPTIONS_SCOPE_TOPIC]);
        let (decoded, consumed) = UpdateResourceRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, topic);
        assert_eq!(decoded.target.scope_code(), OPTIONS_SCOPE_TOPIC);

        let stream = UpdateResourceRequest {
            target: ResourceTarget::Stream {
                stream_id: WireIdentifier::numeric(3),
            },
            reset: WireResetKeys::default(),
            options: options(&[(STRING, b"name", STRING, b"renamed")]),
        };
        let bytes = stream.to_bytes();
        let mut expected = vec![OPTIONS_SCOPE_STREAM];
        expected.extend_from_slice(&WireIdentifier::numeric(3).to_bytes());
        expected.push(0);
        expected.extend_from_slice(stream.options.as_bytes());
        assert_eq!(&bytes[..], expected.as_slice());
        assert_eq!(UpdateResourceRequest::decode_from(&bytes).unwrap(), stream);

        let user = UpdateResourceRequest {
            target: ResourceTarget::User {
                user_id: WireIdentifier::named("alice").unwrap(),
            },
            reset: WireResetKeys::default(),
            options: options(&[
                (STRING, b"status", STRING, b"inactive"),
                (STRING, b"username", STRING, b"alice2"),
            ]),
        };
        let bytes = user.to_bytes();
        assert_eq!(UpdateResourceRequest::decode_from(&bytes).unwrap(), user);
    }

    #[test]
    fn given_malformed_patch_when_decoding_should_reject_without_panicking() {
        let valid = UpdateResourceRequest {
            target: ResourceTarget::Topic {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(2),
            },
            reset: reset_keys(&["max_topic_size"]),
            options: options(&[(STRING, b"message_expiry", UINT64, &7u64.to_le_bytes())]),
        };
        let bytes = valid.to_bytes();
        let options_start = bytes.len() - valid.options.encoded_size();
        // Every prefix that cuts the fixed part or splits a TLV entry fails.
        // Cutting exactly at the start of the options block leaves a valid
        // reset-only patch, so that one boundary decodes.
        for length in 0..bytes.len() {
            let outcome = UpdateResourceRequest::decode(&bytes[..length]);
            if length == options_start {
                assert!(outcome.is_ok(), "reset-only prefix must decode");
            } else {
                assert!(outcome.is_err(), "prefix of {length} bytes must fail");
            }
        }

        let mut unknown_scope = bytes.to_vec();
        unknown_scope[0] = 9;
        assert!(UpdateResourceRequest::decode(&unknown_scope).is_err());

        let duplicate_reset = UpdateResourceRequest {
            reset: reset_keys(&["max_topic_size", "max_topic_size"]),
            options: WireOptions::empty(),
            ..valid.clone()
        };
        assert!(UpdateResourceRequest::decode(&duplicate_reset.to_bytes()).is_err());

        let cross_duplicate = UpdateResourceRequest {
            reset: reset_keys(&["message_expiry"]),
            ..valid.clone()
        };
        assert!(UpdateResourceRequest::decode(&cross_duplicate.to_bytes()).is_err());

        let empty = UpdateResourceRequest {
            reset: WireResetKeys::default(),
            options: WireOptions::empty(),
            ..valid.clone()
        };
        assert!(UpdateResourceRequest::decode(&empty.to_bytes()).is_err());

        let mut duplicate_set = valid.target.to_bytes().to_vec();
        duplicate_set.push(0);
        let mut entries = BytesMut::new();
        encode_user_headers(
            &[
                (STRING, b"message_expiry", UINT64, &7u64.to_le_bytes()),
                (STRING, b"message_expiry", UINT64, &8u64.to_le_bytes()),
            ],
            &mut entries,
        );
        duplicate_set.extend_from_slice(&entries);
        assert!(UpdateResourceRequest::decode(&duplicate_set).is_err());
    }

    #[test]
    fn given_more_reset_names_than_the_count_byte_holds_when_building_should_refuse() {
        let names = |count: usize| -> Vec<WireName> {
            (0..count)
                .map(|index| WireName::new(format!("key_{index}")).unwrap())
                .collect()
        };
        assert!(WireResetKeys::try_from(names(MAX_RESET_KEYS + 1)).is_err());
        let request = UpdateResourceRequest {
            target: ResourceTarget::Stream {
                stream_id: WireIdentifier::numeric(1),
            },
            reset: WireResetKeys::try_from(names(MAX_RESET_KEYS)).unwrap(),
            options: WireOptions::empty(),
        };
        assert_eq!(
            UpdateResourceRequest::decode_from(&request.to_bytes()).unwrap(),
            request
        );
    }

    fn reset_keys(names: &[&str]) -> WireResetKeys {
        WireResetKeys::try_from(
            names
                .iter()
                .map(|name| WireName::new(*name).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn options(entries: &[(u8, &[u8], u8, &[u8])]) -> WireOptions {
        let mut buf = BytesMut::new();
        encode_user_headers(entries, &mut buf);
        WireOptions::from_bytes(buf.freeze()).unwrap()
    }
}
