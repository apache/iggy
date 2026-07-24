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

use std::collections::BTreeMap;

use crate::{Error, Payload, Schema};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StructuredValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<StructuredValue>),
    Object(BTreeMap<String, StructuredValue>),
}

pub(crate) trait FormatAdapter: Send + Sync {
    fn decode_payload(&self, payload: Payload) -> Result<StructuredValue, Error>;
    fn encode_payload(&self, value: StructuredValue) -> Result<Payload, Error>;
}

struct JsonAdapter;
struct TextAdapter;
struct RawAdapter;

static JSON_ADAPTER: JsonAdapter = JsonAdapter;
static TEXT_ADAPTER: TextAdapter = TextAdapter;
static RAW_ADAPTER: RawAdapter = RawAdapter;

pub(crate) fn decode_payload(payload: Payload) -> Result<StructuredValue, Error> {
    let adapter = match &payload {
        Payload::Json(_) => adapter_for_schema(Schema::Json)?,
        Payload::Text(_) => adapter_for_schema(Schema::Text)?,
        Payload::Raw(_) => adapter_for_schema(Schema::Raw)?,
        Payload::Proto(_) | Payload::FlatBuffer(_) | Payload::Avro(_) => {
            return Err(Error::InvalidPayloadType);
        }
    };

    adapter.decode_payload(payload)
}

pub(crate) fn encode_payload(
    value: StructuredValue,
    target_schema: Schema,
) -> Result<Payload, Error> {
    adapter_for_schema(target_schema)?.encode_payload(value)
}

fn adapter_for_schema(schema: Schema) -> Result<&'static dyn FormatAdapter, Error> {
    match schema {
        Schema::Json => Ok(&JSON_ADAPTER),
        Schema::Text => Ok(&TEXT_ADAPTER),
        Schema::Raw => Ok(&RAW_ADAPTER),
        Schema::Proto | Schema::FlatBuffer | Schema::Avro => Err(Error::InvalidPayloadType),
    }
}

impl FormatAdapter for JsonAdapter {
    fn decode_payload(&self, payload: Payload) -> Result<StructuredValue, Error> {
        match payload {
            Payload::Json(value) => Ok(value.into()),
            _ => Err(Error::InvalidPayloadType),
        }
    }

    fn encode_payload(&self, value: StructuredValue) -> Result<Payload, Error> {
        Ok(Payload::Json(value.try_into()?))
    }
}

impl FormatAdapter for TextAdapter {
    fn decode_payload(&self, payload: Payload) -> Result<StructuredValue, Error> {
        let Payload::Text(text) = payload else {
            return Err(Error::InvalidPayloadType);
        };

        let mut bytes = text.clone().into_bytes();
        match simd_json::to_owned_value(&mut bytes) {
            Ok(value) => Ok(value.into()),
            Err(_) => Ok(StructuredValue::String(text)),
        }
    }

    fn encode_payload(&self, value: StructuredValue) -> Result<Payload, Error> {
        match value {
            StructuredValue::String(text) => Ok(Payload::Text(text)),
            StructuredValue::Bytes(bytes) => String::from_utf8(bytes)
                .map(Payload::Text)
                .map_err(|_| Error::InvalidTextPayload),
            value => structured_json_string(value).map(Payload::Text),
        }
    }
}

impl FormatAdapter for RawAdapter {
    fn decode_payload(&self, payload: Payload) -> Result<StructuredValue, Error> {
        match payload {
            Payload::Raw(bytes) => Ok(StructuredValue::Bytes(bytes)),
            _ => Err(Error::InvalidPayloadType),
        }
    }

    fn encode_payload(&self, value: StructuredValue) -> Result<Payload, Error> {
        match value {
            StructuredValue::Bytes(bytes) => Ok(Payload::Raw(bytes)),
            StructuredValue::String(text) => Ok(Payload::Raw(text.into_bytes())),
            value => structured_json_bytes(value).map(Payload::Raw),
        }
    }
}

impl From<simd_json::OwnedValue> for StructuredValue {
    fn from(value: simd_json::OwnedValue) -> Self {
        match value {
            simd_json::OwnedValue::Static(node) => match node {
                simd_json::StaticNode::Null => Self::Null,
                simd_json::StaticNode::Bool(value) => Self::Bool(value),
                simd_json::StaticNode::I64(value) => Self::I64(value),
                simd_json::StaticNode::U64(value) => Self::U64(value),
                simd_json::StaticNode::F64(value) => Self::F64(value),
            },
            simd_json::OwnedValue::String(value) => Self::String(value),
            simd_json::OwnedValue::Array(values) => {
                Self::Array(values.into_iter().map(Self::from).collect())
            }
            simd_json::OwnedValue::Object(values) => Self::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect(),
            ),
        }
    }
}

impl TryFrom<StructuredValue> for simd_json::OwnedValue {
    type Error = Error;

    fn try_from(value: StructuredValue) -> Result<Self, Self::Error> {
        match value {
            StructuredValue::Null => Ok(Self::Static(simd_json::StaticNode::Null)),
            StructuredValue::Bool(value) => Ok(Self::Static(simd_json::StaticNode::Bool(value))),
            StructuredValue::I64(value) => Ok(Self::Static(simd_json::StaticNode::I64(value))),
            StructuredValue::U64(value) => Ok(Self::Static(simd_json::StaticNode::U64(value))),
            StructuredValue::F64(value) => Ok(Self::Static(simd_json::StaticNode::F64(value))),
            StructuredValue::String(value) => Ok(Self::String(value)),
            StructuredValue::Bytes(_) => Err(Error::InvalidRecordValue(
                "binary structured value cannot be represented as JSON".to_string(),
            )),
            StructuredValue::Array(values) => values
                .into_iter()
                .map(Self::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(Box::new)
                .map(Self::Array),
            StructuredValue::Object(values) => {
                let object: Result<simd_json::owned::Object, Error> = values
                    .into_iter()
                    .map(|(key, value)| Self::try_from(value).map(|value| (key, value)))
                    .collect();
                Ok(Self::Object(Box::new(object?)))
            }
        }
    }
}

fn structured_json_string(value: StructuredValue) -> Result<String, Error> {
    let json: simd_json::OwnedValue = value.try_into()?;
    simd_json::to_string(&json).map_err(|_| Error::InvalidJsonPayload)
}

fn structured_json_bytes(value: StructuredValue) -> Result<Vec<u8>, Error> {
    let json: simd_json::OwnedValue = value.try_into()?;
    simd_json::to_vec(&json).map_err(|_| Error::InvalidJsonPayload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_value() -> StructuredValue {
        StructuredValue::Object(BTreeMap::from([
            (
                "array".to_string(),
                StructuredValue::Array(vec![
                    StructuredValue::Null,
                    StructuredValue::Bool(true),
                    StructuredValue::I64(-42),
                    StructuredValue::U64(42),
                    StructuredValue::F64(1.5),
                    StructuredValue::String("value".to_string()),
                ]),
            ),
            (
                "object".to_string(),
                StructuredValue::Object(BTreeMap::from([(
                    "empty".to_string(),
                    StructuredValue::Array(vec![]),
                )])),
            ),
        ]))
    }

    #[test]
    fn json_adapter_should_round_trip_all_json_value_kinds() {
        let original = simd_json::json!({
            "null": null,
            "bool": true,
            "i64": -1,
            "u64": u64::MAX,
            "f64": 1.5,
            "string": "value",
            "array": [1, 2, 3]
        });

        let value = decode_payload(Payload::Json(original.clone())).unwrap();
        let encoded = encode_payload(value, Schema::Json).unwrap();

        assert!(matches!(encoded, Payload::Json(value) if value == original));
    }

    #[test]
    fn json_conversion_should_preserve_numeric_variants() {
        let cases = [
            (
                simd_json::OwnedValue::Static(simd_json::StaticNode::I64(-1)),
                StructuredValue::I64(-1),
            ),
            (
                simd_json::OwnedValue::Static(simd_json::StaticNode::U64(u64::MAX)),
                StructuredValue::U64(u64::MAX),
            ),
            (
                simd_json::OwnedValue::Static(simd_json::StaticNode::F64(1.25)),
                StructuredValue::F64(1.25),
            ),
        ];

        for (json, expected) in cases {
            assert_eq!(StructuredValue::from(json), expected);
        }
    }

    #[test]
    fn json_adapter_should_round_trip_nested_structures() {
        let original = nested_value();

        let encoded = encode_payload(original.clone(), Schema::Json).unwrap();
        let decoded = decode_payload(encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn text_adapter_should_parse_json_objects() {
        let value = decode_payload(Payload::Text(r#"{"name":"Iggy"}"#.to_string())).unwrap();

        assert!(matches!(value, StructuredValue::Object(_)));
    }

    #[test]
    fn text_adapter_should_parse_each_json_root_kind() {
        let cases = [
            ("null", StructuredValue::Null),
            ("true", StructuredValue::Bool(true)),
            ("-42", StructuredValue::I64(-42)),
            ("42", StructuredValue::U64(42)),
            ("1.5", StructuredValue::F64(1.5)),
            (r#""value""#, StructuredValue::String("value".to_string())),
            (
                "[1,2]",
                StructuredValue::Array(vec![StructuredValue::U64(1), StructuredValue::U64(2)]),
            ),
        ];

        for (text, expected) in cases {
            assert_eq!(
                decode_payload(Payload::Text(text.to_string())).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn text_adapter_should_preserve_plain_text() {
        let value = decode_payload(Payload::Text("plain text".to_string())).unwrap();
        let encoded = encode_payload(value, Schema::Text).unwrap();

        assert!(matches!(encoded, Payload::Text(text) if text == "plain text"));
    }

    #[test]
    fn text_adapter_should_preserve_empty_and_whitespace_only_text() {
        for text in ["", " ", "\n\t"] {
            assert_eq!(
                decode_payload(Payload::Text(text.to_string())).unwrap(),
                StructuredValue::String(text.to_string())
            );
        }
    }

    #[test]
    fn text_adapter_should_encode_nested_structures_as_json() {
        let encoded = encode_payload(nested_value(), Schema::Text).unwrap();
        let Payload::Text(text) = encoded else {
            panic!("expected text payload");
        };
        let mut bytes = text.into_bytes();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        assert_eq!(
            StructuredValue::from(json),
            nested_value(),
            "encoded text must preserve the complete structured value"
        );
    }

    #[test]
    fn text_adapter_should_decode_valid_utf8_bytes() {
        let encoded = encode_payload(
            StructuredValue::Bytes("你好, Iggy".as_bytes().to_vec()),
            Schema::Text,
        )
        .unwrap();

        assert!(matches!(encoded, Payload::Text(text) if text == "你好, Iggy"));
    }

    #[test]
    fn text_adapter_should_reject_non_utf8_bytes() {
        let result = encode_payload(StructuredValue::Bytes(vec![0xff, 0xfe]), Schema::Text);

        assert_eq!(result.unwrap_err(), Error::InvalidTextPayload);
    }

    #[test]
    fn raw_adapter_should_preserve_non_utf8_bytes() {
        let bytes = vec![0, 159, 146, 150];
        let value = decode_payload(Payload::Raw(bytes.clone())).unwrap();
        let encoded = encode_payload(value, Schema::Raw).unwrap();

        assert!(matches!(encoded, Payload::Raw(encoded) if encoded == bytes));
    }

    #[test]
    fn raw_adapter_should_encode_strings_without_json_quotes() {
        let encoded = encode_payload(
            StructuredValue::String("plain text".to_string()),
            Schema::Raw,
        )
        .unwrap();

        assert!(matches!(encoded, Payload::Raw(bytes) if bytes == b"plain text"));
    }

    #[test]
    fn raw_adapter_should_encode_nested_structures_as_json_bytes() {
        let encoded = encode_payload(nested_value(), Schema::Raw).unwrap();
        let Payload::Raw(mut bytes) = encoded else {
            panic!("expected raw payload");
        };
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        assert_eq!(StructuredValue::from(json), nested_value());
    }

    #[test]
    fn json_adapter_should_reject_binary_values() {
        let result = encode_payload(StructuredValue::Bytes(vec![1, 2, 3]), Schema::Json);

        assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
    }

    #[test]
    fn structured_encoders_should_reject_nested_binary_values() {
        let value = StructuredValue::Object(BTreeMap::from([(
            "binary".to_string(),
            StructuredValue::Bytes(vec![1, 2, 3]),
        )]));

        for schema in [Schema::Json, Schema::Text, Schema::Raw] {
            assert!(
                matches!(
                    encode_payload(value.clone(), schema),
                    Err(Error::InvalidRecordValue(_))
                ),
                "{schema} must not silently discard nested binary data"
            );
        }
    }

    #[test]
    fn decode_payload_should_reject_schema_dependent_formats() {
        let payloads = [
            Payload::Proto("value".to_string()),
            Payload::FlatBuffer(vec![1, 2, 3]),
            Payload::Avro(vec![1, 2, 3]),
        ];

        for payload in payloads {
            assert_eq!(
                decode_payload(payload).unwrap_err(),
                Error::InvalidPayloadType
            );
        }
    }

    #[test]
    fn adapter_factory_should_reject_schema_dependent_formats() {
        for schema in [Schema::Proto, Schema::FlatBuffer, Schema::Avro] {
            assert!(matches!(
                adapter_for_schema(schema),
                Err(Error::InvalidPayloadType)
            ));
            assert_eq!(
                encode_payload(StructuredValue::Null, schema).unwrap_err(),
                Error::InvalidPayloadType
            );
        }
    }

    #[test]
    fn concrete_adapters_should_reject_the_wrong_payload_variant() {
        assert_eq!(
            JSON_ADAPTER
                .decode_payload(Payload::Text("value".to_string()))
                .unwrap_err(),
            Error::InvalidPayloadType
        );
        assert_eq!(
            TEXT_ADAPTER
                .decode_payload(Payload::Raw(vec![1]))
                .unwrap_err(),
            Error::InvalidPayloadType
        );
        assert_eq!(
            RAW_ADAPTER
                .decode_payload(Payload::Json(simd_json::json!(null)))
                .unwrap_err(),
            Error::InvalidPayloadType
        );
    }
}
