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

use crate::IggyError;
use crate::types::message::{HeaderKey, HeaderKind, HeaderValue};
use crate::types::options::{
    OptionKey, OptionValue, OptionsPatch, OptionsProvenance, ResourceOptions, erase_options,
    key_preview, wire_reset_names,
};
use crate::wire_conversions::resource_options_to_wire;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use iggy_binary_protocol::WireOptions;
use secrecy::zeroize::Zeroizing;
use serde::de::{Error as _, MapAccess, Visitor};
use serde::ser::{Error as _, SerializeMap};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

/// The update body member that lists the keys to reset. No option is ever
/// named `reset`.
pub const RESET_FIELD: &str = "reset";

/// The largest integer a JSON number carries exactly in every client, 2^53 - 1.
const MAX_EXACT_JSON_INTEGER: u64 = (1 << 53) - 1;

/// One option value in an HTTP body: a JSON boolean, a JSON integer, or a
/// string in the same form a config file uses (`"7 days"`, `"1 GiB"`). A
/// 64-bit value above 2^53 travels as a decimal string so JavaScript clients
/// keep its precision. Text is erased on drop, since it may be the base64
/// stream key.
#[derive(Clone, PartialEq, Eq)]
pub enum OptionInputValue {
    Bool(bool),
    U64(u64),
    String(Zeroizing<String>),
}

/// Option values for a create body, keyed by option name. A name repeated in
/// the JSON object is refused rather than silently collapsed.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct OptionInputMap(pub BTreeMap<String, OptionInputValue>);

/// An HTTP update body: one flat object of option keys plus an optional
/// `reset` array. A value sets the key, an absent key or `null` changes
/// nothing, a key named in `reset` returns to its default. `null` never resets,
/// because many JSON serializers write unset optional fields as `null`. A body
/// that changes nothing, with no keys, only nulls or only zero sentinels such
/// as `"message_expiry": 0`, is refused with `InvalidCommand` on every
/// transport.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct OptionsUpdate {
    pub set: BTreeMap<String, OptionInputValue>,
    pub reset: BTreeSet<String>,
}

struct InputValueVisitor;

struct UniqueKeysVisitor;

struct OptionsUpdateVisitor;

impl OptionInputMap {
    /// Encode the values into a create options block, each in the kind its
    /// catalog entry expects where the JSON value fits it. Admission validates
    /// the keys and values, so a mismatch fails in the same category as on
    /// the binary transports. The map built on the way may hold the stream
    /// key, so it is erased once encoded.
    ///
    /// # Errors
    ///
    /// `UnsupportedOptionKey` for a key that is not a valid option name.
    pub fn to_wire(&self) -> Result<WireOptions, IggyError> {
        let options = input_options(&self.0)?;
        let wire = resource_options_to_wire(&options, OptionsProvenance::All);
        erase_options(options);
        wire
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl OptionsUpdate {
    /// Split the body into the keys to set and the keys to reset, in wire
    /// form. Admission validates the keys and values.
    ///
    /// # Errors
    ///
    /// As [`OptionInputMap::to_wire`], and `UnsupportedOptionKey` for a reset
    /// name that is not a valid option name or a reset list longer than the
    /// wire carries.
    pub fn to_patch(&self) -> Result<OptionsPatch, IggyError> {
        let set = input_options(&self.set)?;
        let patch = wire_reset_names(self.reset.iter().map(String::as_str)).and_then(|reset| {
            Ok(OptionsPatch {
                set: resource_options_to_wire(&set, OptionsProvenance::All)?,
                reset,
            })
        });
        erase_options(set);
        patch
    }
}

impl TryFrom<&HeaderValue> for OptionInputValue {
    type Error = IggyError;

    /// The HTTP form of a canonical option value. Raw bytes, the stream
    /// encryption key, travel as standard base64, and a kind without a JSON
    /// form travels as the text a config file uses.
    fn try_from(value: &HeaderValue) -> Result<Self, Self::Error> {
        match value.kind() {
            HeaderKind::String => Ok(Self::String(Zeroizing::new(value.as_str()?.to_owned()))),
            HeaderKind::Bool => Ok(Self::Bool(value.as_bool()?)),
            HeaderKind::Uint8 => Ok(Self::U64(u64::from(value.as_uint8()?))),
            HeaderKind::Uint16 => Ok(Self::U64(u64::from(value.as_uint16()?))),
            HeaderKind::Uint32 => Ok(Self::U64(u64::from(value.as_uint32()?))),
            HeaderKind::Uint64 => {
                let number = value.as_uint64()?;
                Ok(if number > MAX_EXACT_JSON_INTEGER {
                    Self::String(Zeroizing::new(number.to_string()))
                } else {
                    Self::U64(number)
                })
            }
            HeaderKind::Raw => Ok(Self::String(Zeroizing::new(
                STANDARD.encode(value.as_bytes()),
            ))),
            _ => Ok(Self::String(Zeroizing::new(value.to_string_value()))),
        }
    }
}

impl Debug for OptionInputValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool(value) => write!(formatter, "{value}"),
            Self::U64(value) => write!(formatter, "{value}"),
            Self::String(value) => write!(formatter, "{:?}", value.as_str()),
        }
    }
}

impl Debug for OptionInputMap {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_map()
            .entries(
                self.0
                    .iter()
                    .map(|(key, value)| (key, redacted(key, Some(value)))),
            )
            .finish()
    }
}

impl Debug for OptionsUpdate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OptionsUpdate")
            .field(
                "set",
                &self
                    .set
                    .iter()
                    .map(|(key, value)| (key, redacted(key, Some(value))))
                    .collect::<BTreeMap<_, _>>(),
            )
            .field("reset", &self.reset)
            .finish()
    }
}

impl Serialize for OptionInputValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::U64(value) => serializer.serialize_u64(*value),
            Self::String(value) => serializer.serialize_str(value),
        }
    }
}

impl<'de> Deserialize<'de> for OptionInputValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(InputValueVisitor)
    }
}

impl Serialize for OptionInputMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OptionInputMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(UniqueKeysVisitor)
    }
}

impl Serialize for OptionsUpdate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let reset_entries = usize::from(!self.reset.is_empty());
        if self.set.contains_key(RESET_FIELD) {
            return Err(S::Error::custom("no option is named reset"));
        }
        let mut map = serializer.serialize_map(Some(self.set.len() + reset_entries))?;
        for (key, value) in &self.set {
            map.serialize_entry(key, value)?;
        }
        if !self.reset.is_empty() {
            map.serialize_entry(RESET_FIELD, &self.reset)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for OptionsUpdate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(OptionsUpdateVisitor)
    }
}

impl Visitor<'_> for InputValueVisitor {
    type Value = OptionInputValue;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a boolean, a non-negative integer or a string")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(OptionInputValue::Bool(value))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(OptionInputValue::U64(value))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        u64::try_from(value)
            .map(OptionInputValue::U64)
            .map_err(|_| E::custom("a negative integer is not an option value"))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(OptionInputValue::String(Zeroizing::new(value.to_owned())))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(OptionInputValue::String(Zeroizing::new(value)))
    }
}

impl<'de> Visitor<'de> for UniqueKeysVisitor {
    type Value = OptionInputMap;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an object of option names to scalar values")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut values = OptionInputMap::default();
        while let Some((key, value)) = access.next_entry::<String, OptionInputValue>()? {
            if values.0.contains_key(&key) {
                return Err(A::Error::custom(format!("duplicate option key: {key}")));
            }
            values.0.insert(key, value);
        }
        Ok(values)
    }
}

impl<'de> Visitor<'de> for OptionsUpdateVisitor {
    type Value = OptionsUpdate;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .write_str("an object of option names to scalar values and an optional reset array")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut update = OptionsUpdate::default();
        let mut seen = BTreeSet::new();
        while let Some(key) = access.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(A::Error::custom(format!("duplicate option key: {key}")));
            }
            if key == RESET_FIELD {
                for name in access.next_value::<Vec<String>>()? {
                    if !update.reset.insert(name.clone()) {
                        return Err(A::Error::custom(format!("duplicate reset key: {name}")));
                    }
                }
                continue;
            }
            if let Some(value) = access.next_value::<Option<OptionInputValue>>()? {
                update.set.insert(key, value);
            }
        }
        Ok(update)
    }
}

fn input_options(
    values: &BTreeMap<String, OptionInputValue>,
) -> Result<ResourceOptions, IggyError> {
    let mut options = ResourceOptions::new();
    for (key, value) in values {
        let Ok(header_key) = HeaderKey::from_str(key) else {
            // An entry taken before this one may hold the stream key.
            erase_options(options);
            return Err(IggyError::UnsupportedOptionKey(key_preview(key)));
        };
        options.insert(
            header_key,
            OptionValue::explicit(input_header_value(key, value)),
        );
    }
    Ok(options)
}

/// The header value an HTTP option value encodes to: the kind its catalog
/// entry expects where the JSON value fits it, otherwise the JSON value's own
/// kind. Admission then refuses a mismatch in its own category order.
///
/// Text that no header value holds (empty, or over 255 bytes) travels as a
/// value of a kind its key never accepts, so admission still reports the
/// key's own error, a name error for a name and `InvalidOptionValue` for a
/// map key, after every check that comes before it.
fn input_header_value(key: &str, value: &OptionInputValue) -> HeaderValue {
    let kind = OptionKey::from_str(key)
        .ok()
        .map(|option| option.descriptor().kind);
    match value {
        OptionInputValue::String(text) => {
            HeaderValue::try_from(text.as_str()).unwrap_or_else(|_| match kind {
                Some(HeaderKind::Bool) => HeaderValue::from(0u64),
                _ => HeaderValue::from(false),
            })
        }
        OptionInputValue::Bool(flag) => HeaderValue::from(*flag),
        OptionInputValue::U64(number) => {
            number_in_kind(kind, *number).unwrap_or_else(|| HeaderValue::from(*number))
        }
    }
}

fn number_in_kind(kind: Option<HeaderKind>, number: u64) -> Option<HeaderValue> {
    match kind? {
        HeaderKind::Uint8 => u8::try_from(number).ok().map(HeaderValue::from),
        HeaderKind::Uint16 => u16::try_from(number).ok().map(HeaderValue::from),
        HeaderKind::Uint32 => u32::try_from(number).ok().map(HeaderValue::from),
        HeaderKind::Uint64 => Some(HeaderValue::from(number)),
        HeaderKind::Uint128 => Some(HeaderValue::from(u128::from(number))),
        HeaderKind::Int8 => i8::try_from(number).ok().map(HeaderValue::from),
        HeaderKind::Int16 => i16::try_from(number).ok().map(HeaderValue::from),
        HeaderKind::Int32 => i32::try_from(number).ok().map(HeaderValue::from),
        HeaderKind::Int64 => i64::try_from(number).ok().map(HeaderValue::from),
        HeaderKind::Int128 => Some(HeaderValue::from(i128::from(number))),
        _ => None,
    }
}

fn redacted<'a>(key: &str, value: Option<&'a OptionInputValue>) -> Option<&'a dyn Debug> {
    if key == <&'static str>::from(OptionKey::EncryptionKey) {
        return Some(&"<redacted>");
    }
    value.map(|value| value as &dyn Debug)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::options::{
        AdmittedOptions, OptionAdmissionContext, OptionOperation, OptionsScope, admit,
        descriptors_for,
    };
    use iggy_binary_protocol::WireName;
    use secrecy::zeroize::ZeroizeOnDrop;

    #[test]
    fn given_update_body_when_parsing_json_should_skip_nulls_and_separate_resets() {
        let update: OptionsUpdate = serde_json::from_str(
            r#"{"message_expiry":"1 day","max_topic_size":null,"messages_required_to_save":64,"preallocate_segments":true,"reset":["segment_size"]}"#,
        )
        .expect("valid update body");
        assert!(!update.set.contains_key("max_topic_size"));
        let parts = update.to_patch().expect("encodes");
        assert_eq!(*parts.reset, [WireName::new("segment_size").unwrap()]);
        let kinds: BTreeMap<String, u8> = (&parts.set)
            .into_iter()
            .map(|entry| {
                (
                    String::from_utf8_lossy(entry.key).into_owned(),
                    entry.value_kind.0,
                )
            })
            .collect();
        assert_eq!(
            kinds,
            BTreeMap::from([
                ("message_expiry".to_owned(), HeaderKind::String.as_code()),
                (
                    "messages_required_to_save".to_owned(),
                    HeaderKind::Uint32.as_code()
                ),
                (
                    "preallocate_segments".to_owned(),
                    HeaderKind::Bool.as_code()
                ),
            ])
        );
    }

    #[test]
    fn given_update_body_when_serializing_should_omit_unset_keys_and_empty_reset() {
        let update = OptionsUpdate {
            set: BTreeMap::from([(
                "message_expiry".to_owned(),
                OptionInputValue::String(Zeroizing::new("7 days".to_owned())),
            )]),
            reset: BTreeSet::new(),
        };
        assert_eq!(
            serde_json::to_string(&update).unwrap(),
            r#"{"message_expiry":"7 days"}"#
        );
        let with_reset = OptionsUpdate {
            reset: BTreeSet::from(["max_topic_size".to_owned()]),
            ..update
        };
        let json = serde_json::to_string(&with_reset).unwrap();
        assert_eq!(
            json,
            r#"{"message_expiry":"7 days","reset":["max_topic_size"]}"#
        );
        assert_eq!(
            serde_json::from_str::<OptionsUpdate>(&json).unwrap(),
            with_reset
        );
    }

    #[test]
    fn given_invalid_json_shapes_when_parsing_should_refuse() {
        for body in [
            r#"{"message_expiry":1,"message_expiry":2}"#,
            r#"{"message_expiry":{"nested":1}}"#,
            r#"{"message_expiry":[1]}"#,
            r#"[1]"#,
            r#""text""#,
            r#"{"message_expiry":-1}"#,
            r#"{"message_expiry":1.5}"#,
            r#"{"reset":"message_expiry"}"#,
            r#"{"reset":[1]}"#,
            r#"{"reset":["message_expiry","message_expiry"]}"#,
            r#"{"reset":null}"#,
        ] {
            assert!(
                serde_json::from_str::<OptionsUpdate>(body).is_err(),
                "{body} must be refused"
            );
        }
        assert!(serde_json::from_str::<OptionInputMap>(r#"{"a":"x","a":"y"}"#).is_err());
    }

    #[test]
    fn given_json_value_that_does_not_fit_its_key_when_admitting_should_report_the_binary_category()
    {
        let long_name = format!(r#"{{"name":"{}"}}"#, "n".repeat(256));
        for (scope, body, expected) in [
            (
                OptionsScope::Topic,
                r#"{"name":5}"#,
                IggyError::InvalidTopicName,
            ),
            (
                OptionsScope::Stream,
                r#"{"name":""}"#,
                IggyError::InvalidStreamName,
            ),
            (
                OptionsScope::Topic,
                long_name.as_str(),
                IggyError::InvalidTopicName,
            ),
            (
                OptionsScope::User,
                r#"{"status":true}"#,
                IggyError::InvalidUserStatus,
            ),
            (
                OptionsScope::User,
                r#"{"username":""}"#,
                IggyError::InvalidUsername,
            ),
            (
                OptionsScope::User,
                r#"{"name":""}"#,
                IggyError::UnsupportedOptionKey("name".to_owned()),
            ),
            (
                OptionsScope::Topic,
                r#"{"segment_size":true}"#,
                IggyError::OptionNotUpdatable("segment_size".to_owned()),
            ),
            (
                OptionsScope::Stream,
                r#"{"encryption_key_source":1}"#,
                IggyError::ReadOnlyOption("encryption_key_source".to_owned()),
            ),
            (
                OptionsScope::Topic,
                r#"{"message_expiry":true}"#,
                IggyError::InvalidOptionValue("message_expiry".to_owned()),
            ),
        ] {
            assert_eq!(admit_update(scope, body).err(), Some(expected), "{body}");
        }
        for (scope, body, expected) in [
            (
                OptionsScope::Topic,
                r#"{"messages_required_to_save":4294967296}"#,
                IggyError::InvalidOptionValue("messages_required_to_save".to_owned()),
            ),
            (
                OptionsScope::Topic,
                r#"{"preallocate_segments":""}"#,
                IggyError::InvalidOptionValue("preallocate_segments".to_owned()),
            ),
            (
                OptionsScope::Stream,
                r#"{"encryption_key":""}"#,
                IggyError::InvalidEncryptionKey,
            ),
        ] {
            assert_eq!(admit_create(scope, body).err(), Some(expected), "{body}");
        }
    }

    #[test]
    fn given_text_no_header_value_holds_for_any_key_when_admitting_should_refuse() {
        let long_text = "v".repeat(256);
        for scope in [
            OptionsScope::Topic,
            OptionsScope::Stream,
            OptionsScope::User,
        ] {
            for descriptor in descriptors_for(scope) {
                for text in ["", long_text.as_str()] {
                    let body = format!(r#"{{"{}":"{text}"}}"#, descriptor.key);
                    assert!(
                        admit_update(scope, &body).is_err(),
                        "{scope:?} update {}",
                        descriptor.key
                    );
                    assert!(
                        admit_create(scope, &body).is_err(),
                        "{scope:?} create {}",
                        descriptor.key
                    );
                }
            }
        }
    }

    #[test]
    fn given_oversized_key_when_converting_should_echo_a_bounded_preview() {
        let key = "k".repeat(300);
        let echoed = |error: Option<IggyError>| match error {
            Some(IggyError::UnsupportedOptionKey(shown)) => shown,
            other => panic!("expected an unsupported key, got {other:?}"),
        };
        let set = OptionsUpdate {
            set: BTreeMap::from([(key.clone(), OptionInputValue::U64(1))]),
            reset: BTreeSet::new(),
        };
        assert_eq!(echoed(set.to_patch().err()), "k".repeat(64));
        let reset = OptionsUpdate {
            set: BTreeMap::new(),
            reset: BTreeSet::from([key.clone()]),
        };
        assert_eq!(echoed(reset.to_patch().err()), "k".repeat(64));
        let create = OptionInputMap(BTreeMap::from([(key, OptionInputValue::U64(1))]));
        assert_eq!(echoed(create.to_wire().err()), "k".repeat(64));
    }

    #[test]
    fn given_more_resets_than_the_wire_carries_when_converting_should_refuse_unsupported_key() {
        let update = OptionsUpdate {
            set: BTreeMap::new(),
            reset: (0..256).map(|index| format!("key_{index:03}")).collect(),
        };
        assert_eq!(
            update.to_patch(),
            Err(IggyError::UnsupportedOptionKey("key_000".to_owned()))
        );
    }

    #[test]
    fn given_u64_above_2_pow_53_when_encoding_for_http_should_send_a_decimal_string() {
        let exact = (1u64 << 53) - 1;
        assert_eq!(
            OptionInputValue::try_from(&HeaderValue::from(exact)),
            Ok(OptionInputValue::U64(exact))
        );
        assert_eq!(
            OptionInputValue::try_from(&HeaderValue::from(exact + 2)),
            Ok(OptionInputValue::String(Zeroizing::new(
                (exact + 2).to_string()
            )))
        );
    }

    #[test]
    fn given_set_key_named_reset_when_serializing_should_refuse() {
        let update = OptionsUpdate {
            set: BTreeMap::from([(
                RESET_FIELD.to_owned(),
                OptionInputValue::String(Zeroizing::new("x".to_owned())),
            )]),
            reset: BTreeSet::new(),
        };
        assert!(serde_json::to_string(&update).is_err());
    }

    #[test]
    fn given_secret_input_when_debugging_should_redact() {
        let input: OptionInputMap =
            serde_json::from_str(r#"{"encryption_key":"c2VjcmV0LXNlY3JldA=="}"#).unwrap();
        let rendered = format!("{input:?}");
        assert!(!rendered.contains("c2VjcmV0"), "{rendered}");
        let update: OptionsUpdate =
            serde_json::from_str(r#"{"encryption_key":"c2VjcmV0LXNlY3JldA=="}"#).unwrap();
        assert!(!format!("{update:?}").contains("c2VjcmV0"));
    }

    #[test]
    fn given_parsed_bodies_with_stream_key_when_moving_values_out_should_keep_them_erased_on_drop()
    {
        const BODY: &str = r#"{"encryption_key":"c2VjcmV0LXNlY3JldA==","message_expiry":7}"#;
        let input: OptionInputMap = serde_json::from_str(BODY).expect("valid create options");
        let OptionsUpdate { set, reset } = serde_json::from_str(BODY).expect("valid update body");
        assert!(reset.is_empty());
        for mut values in [input.0, set] {
            let key = values
                .remove(<&str>::from(OptionKey::EncryptionKey))
                .expect("key parsed");
            let OptionInputValue::String(text) = key else {
                panic!("the key parses as text");
            };
            assert_erased_on_drop(&text);
            assert_eq!(text.as_str(), "c2VjcmV0LXNlY3JldA==");
            assert_eq!(values["message_expiry"], OptionInputValue::U64(7));
        }
    }

    fn admit_update(scope: OptionsScope, body: &str) -> Result<AdmittedOptions, IggyError> {
        let update: OptionsUpdate = serde_json::from_str(body).expect("valid update body");
        let patch = update.to_patch()?;
        let reset: Vec<String> = patch
            .reset
            .iter()
            .map(|name| name.as_str().to_owned())
            .collect();
        admit(
            scope,
            OptionOperation::Update,
            &patch.set,
            &reset,
            &OptionAdmissionContext::default(),
        )
    }

    fn admit_create(scope: OptionsScope, body: &str) -> Result<AdmittedOptions, IggyError> {
        let input: OptionInputMap = serde_json::from_str(body).expect("valid create options");
        admit(
            scope,
            OptionOperation::Create,
            &input.to_wire()?,
            &[],
            &OptionAdmissionContext::default(),
        )
    }

    fn assert_erased_on_drop<T: ZeroizeOnDrop>(_: &T) {}
}
