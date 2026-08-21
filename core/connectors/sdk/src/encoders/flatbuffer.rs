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

use std::collections::HashMap;
use std::path::PathBuf;

use base64::Engine;
use flatbuffers::FlatBufferBuilder;
use serde::{Deserialize, Serialize};

use crate::structured::{self, StructuredValue};
use crate::{Error, Payload, Schema, StreamEncoder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatBufferEncoderConfig {
    pub schema_path: Option<PathBuf>,
    pub root_table_name: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
    pub preserve_unknown_fields: bool,
    pub include_paths: Vec<PathBuf>,
    pub table_size_hint: usize,
}

impl Default for FlatBufferEncoderConfig {
    fn default() -> Self {
        Self {
            schema_path: None,
            root_table_name: None,
            field_mappings: None,
            preserve_unknown_fields: false,
            include_paths: vec![PathBuf::from(".")],
            table_size_hint: 1024,
        }
    }
}

pub struct FlatBufferStreamEncoder {
    config: FlatBufferEncoderConfig,
}

impl FlatBufferStreamEncoder {
    pub fn new(config: FlatBufferEncoderConfig) -> Self {
        Self { config }
    }

    pub fn new_default() -> Self {
        Self::new(FlatBufferEncoderConfig::default())
    }

    pub fn update_config(&mut self, config: FlatBufferEncoderConfig) -> Result<(), Error> {
        self.config = config;
        Ok(())
    }

    fn apply_field_transformations(&self, value: StructuredValue) -> StructuredValue {
        if let Some(mappings) = &self.config.field_mappings {
            match value {
                StructuredValue::Object(values) => StructuredValue::Object(
                    values
                        .into_iter()
                        .map(|(key, value)| {
                            let key = mappings.get(&key).cloned().unwrap_or(key);
                            (key, value)
                        })
                        .collect(),
                ),
                value => value,
            }
        } else {
            value
        }
    }

    fn encode_structured_to_flatbuffer(&self, value: StructuredValue) -> Result<Vec<u8>, Error> {
        let mut builder = FlatBufferBuilder::with_capacity(self.config.table_size_hint);

        match value {
            StructuredValue::Object(values) => {
                let mut entries = Vec::with_capacity(values.len() * 2);

                for (key, value) in values {
                    let key_offset = builder.create_string(&key);
                    entries.push(key_offset);

                    let value_string = match value {
                        StructuredValue::String(value) => value,
                        value => self.structured_json_string(value)?,
                    };
                    let value_offset = builder.create_string(&value_string);
                    entries.push(value_offset);
                }

                let vector = builder.create_vector(&entries);
                builder.finish_minimal(vector);
            }
            StructuredValue::Bytes(data) => {
                let vector = builder.create_vector(&data);
                builder.finish_minimal(vector);
            }
            StructuredValue::String(text) => {
                let string_offset = builder.create_string(&text);
                builder.finish_minimal(string_offset);
            }
            value => {
                let json_string = self.structured_json_string(value)?;
                let string_offset = builder.create_string(&json_string);
                builder.finish_minimal(string_offset);
            }
        }

        Ok(builder.finished_data().to_vec())
    }

    fn structured_json_string(&self, value: StructuredValue) -> Result<String, Error> {
        let Payload::Json(json) = structured::encode_payload(value, Schema::Json)? else {
            return Err(Error::InvalidPayloadType);
        };
        simd_json::to_string(&json).map_err(|_| Error::InvalidJsonPayload)
    }

    pub fn convert_format(
        &self,
        payload: Payload,
        target_format: Schema,
    ) -> Result<Payload, Error> {
        match (payload, target_format) {
            (Payload::FlatBuffer(data), Schema::Json) => {
                let json_value = simd_json::json!({
                    "flatbuffer_size": data.len(),
                    "raw_data_base64": base64::engine::general_purpose::STANDARD.encode(&data)
                });
                Ok(Payload::Json(json_value))
            }
            (Payload::FlatBuffer(data), Schema::Text) => {
                let base64_data = base64::engine::general_purpose::STANDARD.encode(&data);
                Ok(Payload::Text(base64_data))
            }
            (Payload::FlatBuffer(data), Schema::Raw) => Ok(Payload::Raw(data)),

            (Payload::Json(json), Schema::Text) => {
                let text =
                    simd_json::to_string_pretty(&json).map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Text(text))
            }
            (Payload::Json(json), Schema::Raw) => {
                let bytes = simd_json::to_vec(&json).map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Raw(bytes))
            }

            (Payload::Text(text), Schema::Json) => {
                let mut text_bytes = text.into_bytes();
                let json_value = simd_json::to_owned_value(&mut text_bytes)
                    .map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Json(json_value))
            }
            (Payload::Text(text), Schema::Raw) => Ok(Payload::Raw(text.into_bytes())),

            (Payload::Raw(data), Schema::Text) => {
                let text = String::from_utf8(data).map_err(|_| Error::InvalidTextPayload)?;
                Ok(Payload::Text(text))
            }
            (Payload::Raw(mut data), Schema::Json) => {
                let json_value =
                    simd_json::to_owned_value(&mut data).map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Json(json_value))
            }

            (payload, _) => Ok(payload),
        }
    }
}

impl StreamEncoder for FlatBufferStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::FlatBuffer
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        match payload {
            Payload::FlatBuffer(data) => Ok(data),
            payload => {
                let value = structured::decode_payload(payload)?;
                let transformed_value = self.apply_field_transformations(value);
                self.encode_structured_to_flatbuffer(transformed_value)
            }
        }
    }
}

impl Default for FlatBufferStreamEncoder {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_root_string(encoded: &[u8]) -> &str {
        flatbuffers::root::<&str>(encoded).unwrap()
    }

    fn decode_root_bytes(encoded: &[u8]) -> Vec<u8> {
        flatbuffers::root::<flatbuffers::Vector<'_, u8>>(encoded)
            .unwrap()
            .iter()
            .collect()
    }

    fn decode_root_strings(encoded: &[u8]) -> Vec<&str> {
        flatbuffers::root::<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<&str>>>(encoded)
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn encode_should_handle_json_payload() {
        let encoder = FlatBufferStreamEncoder::default();

        let json_value = simd_json::json!({
            "name": "John",
            "age": 30
        });

        let encoded = encoder.encode(Payload::Json(json_value)).unwrap();

        assert_eq!(
            decode_root_strings(&encoded),
            vec!["age", "30", "name", "John"]
        );
    }

    #[test]
    fn encode_should_preserve_plain_text_as_root_string() {
        let encoder = FlatBufferStreamEncoder::default();

        let encoded = encoder
            .encode(Payload::Text("Hello, World!".to_string()))
            .unwrap();

        assert_eq!(decode_root_string(&encoded), "Hello, World!");
    }

    #[test]
    fn encode_should_preserve_raw_payload_as_root_byte_vector() {
        let encoder = FlatBufferStreamEncoder::default();
        let raw_data = vec![0, 1, 2, 0xff];

        let encoded = encoder.encode(Payload::Raw(raw_data.clone())).unwrap();

        assert_eq!(decode_root_bytes(&encoded), raw_data);
    }

    #[test]
    fn encode_should_preserve_empty_raw_payload() {
        let encoder = FlatBufferStreamEncoder::default();

        let encoded = encoder.encode(Payload::Raw(vec![])).unwrap();

        assert!(decode_root_bytes(&encoded).is_empty());
    }

    #[test]
    fn encode_should_serialize_structured_scalars_and_arrays_as_root_strings() {
        let encoder = FlatBufferStreamEncoder::default();
        let cases = [
            (Payload::Json(simd_json::json!(null)), "null"),
            (Payload::Json(simd_json::json!(true)), "true"),
            (Payload::Json(simd_json::json!(-42)), "-42"),
            (Payload::Json(simd_json::json!([1, 2, 3])), "[1,2,3]"),
        ];

        for (payload, expected) in cases {
            let encoded = encoder.encode(payload).unwrap();
            assert_eq!(decode_root_string(&encoded), expected);
        }
    }

    #[test]
    fn encode_should_preserve_json_string_without_quotes() {
        let encoder = FlatBufferStreamEncoder::default();

        let encoded = encoder
            .encode(Payload::Json(simd_json::json!("Iggy")))
            .unwrap();

        assert_eq!(decode_root_string(&encoded), "Iggy");
    }

    #[test]
    fn encode_should_parse_json_text_before_encoding() {
        let encoder = FlatBufferStreamEncoder::default();

        let encoded = encoder
            .encode(Payload::Text("[1,true,null]".to_string()))
            .unwrap();

        assert_eq!(decode_root_string(&encoded), "[1,true,null]");
    }

    #[test]
    fn encode_should_pass_through_flatbuffer_payload() {
        let encoder = FlatBufferStreamEncoder::default();

        let flatbuffer_data = vec![1, 2, 3, 4, 5];
        let flatbuffer_payload = Payload::FlatBuffer(flatbuffer_data.clone());
        let result = encoder.encode(flatbuffer_payload);

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert_eq!(encoded_data, flatbuffer_data);
    }

    #[test]
    fn encode_should_reject_schema_dependent_payloads_without_schema() {
        let encoder = FlatBufferStreamEncoder::default();

        assert!(matches!(
            encoder.encode(Payload::Proto("value".to_string())),
            Err(Error::InvalidPayloadType)
        ));
        assert!(matches!(
            encoder.encode(Payload::Avro(vec![1, 2, 3])),
            Err(Error::InvalidPayloadType)
        ));
    }

    #[test]
    fn encode_should_apply_field_mappings_when_configured() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("old_field".to_string(), "new_field".to_string());

        let config = FlatBufferEncoderConfig {
            field_mappings: Some(field_mappings),
            ..FlatBufferEncoderConfig::default()
        };
        let encoder = FlatBufferStreamEncoder::new(config);

        let json_value = simd_json::json!({
            "old_field": "should_be_renamed",
            "unchanged_field": "stays_same"
        });

        let value = structured::decode_payload(Payload::Json(json_value)).unwrap();
        let transformed = encoder.apply_field_transformations(value);

        assert_eq!(
            transformed,
            StructuredValue::Object(std::collections::BTreeMap::from([
                (
                    "new_field".to_string(),
                    StructuredValue::String("should_be_renamed".to_string())
                ),
                (
                    "unchanged_field".to_string(),
                    StructuredValue::String("stays_same".to_string())
                ),
            ]))
        );
    }

    #[test]
    fn field_mappings_should_only_rename_top_level_fields() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("old_field".to_string(), "new_field".to_string());
        let encoder = FlatBufferStreamEncoder::new(FlatBufferEncoderConfig {
            field_mappings: Some(field_mappings),
            ..FlatBufferEncoderConfig::default()
        });
        let value = StructuredValue::Object(std::collections::BTreeMap::from([(
            "nested".to_string(),
            StructuredValue::Object(std::collections::BTreeMap::from([(
                "old_field".to_string(),
                StructuredValue::String("value".to_string()),
            )])),
        )]));

        let transformed = encoder.apply_field_transformations(value.clone());

        assert_eq!(transformed, value);
    }

    #[test]
    fn field_mappings_should_leave_non_object_values_unchanged() {
        let encoder = FlatBufferStreamEncoder::new(FlatBufferEncoderConfig {
            field_mappings: Some(HashMap::from([(
                "old_field".to_string(),
                "new_field".to_string(),
            )])),
            ..FlatBufferEncoderConfig::default()
        });
        let values = [
            StructuredValue::Null,
            StructuredValue::String("value".to_string()),
            StructuredValue::Array(vec![StructuredValue::U64(1)]),
            StructuredValue::Bytes(vec![1, 2, 3]),
        ];

        for value in values {
            assert_eq!(encoder.apply_field_transformations(value.clone()), value);
        }
    }

    #[test]
    fn convert_format_should_transform_flatbuffer_to_json() {
        let encoder = FlatBufferStreamEncoder::default();

        let flatbuffer_data = vec![1, 2, 3, 4, 5];
        let result =
            encoder.convert_format(Payload::FlatBuffer(flatbuffer_data.clone()), Schema::Json);

        assert!(
            matches!(result, Ok(Payload::Json(value)) if value == simd_json::json!({
                "flatbuffer_size": 5,
                "raw_data_base64": "AQIDBAU="
            }))
        );
    }

    #[test]
    fn convert_format_should_transform_flatbuffer_to_text() {
        let encoder = FlatBufferStreamEncoder::default();

        let flatbuffer_data = vec![1, 2, 3, 4, 5];
        let result = encoder.convert_format(Payload::FlatBuffer(flatbuffer_data), Schema::Text);

        assert!(matches!(result, Ok(Payload::Text(text)) if text == "AQIDBAU="));
    }

    #[test]
    fn convert_format_should_transform_flatbuffer_to_raw_without_modification() {
        let encoder = FlatBufferStreamEncoder::default();
        let data = vec![0, 1, 2, 0xff];

        let result = encoder.convert_format(Payload::FlatBuffer(data.clone()), Schema::Raw);

        assert!(matches!(result, Ok(Payload::Raw(bytes)) if bytes == data));
    }

    #[test]
    fn convert_format_should_transform_json_to_text_and_raw() {
        let encoder = FlatBufferStreamEncoder::default();
        let value = simd_json::json!({"name": "Iggy", "active": true});

        let text = encoder
            .convert_format(Payload::Json(value.clone()), Schema::Text)
            .unwrap();
        let raw = encoder
            .convert_format(Payload::Json(value.clone()), Schema::Raw)
            .unwrap();

        let Payload::Text(text) = text else {
            panic!("expected text payload");
        };
        let Payload::Raw(mut raw) = raw else {
            panic!("expected raw payload");
        };
        let mut text = text.into_bytes();
        assert_eq!(simd_json::to_owned_value(&mut text).unwrap(), value);
        assert_eq!(simd_json::to_owned_value(&mut raw).unwrap(), value);
    }

    #[test]
    fn convert_format_should_transform_valid_text_and_raw_json_to_json() {
        let encoder = FlatBufferStreamEncoder::default();
        let expected = simd_json::json!({"name": "Iggy"});

        for payload in [
            Payload::Text(r#"{"name":"Iggy"}"#.to_string()),
            Payload::Raw(br#"{"name":"Iggy"}"#.to_vec()),
        ] {
            assert!(matches!(
                encoder.convert_format(payload, Schema::Json),
                Ok(Payload::Json(value)) if value == expected
            ));
        }
    }

    #[test]
    fn convert_format_should_reject_invalid_json_text_and_raw() {
        let encoder = FlatBufferStreamEncoder::default();

        for payload in [
            Payload::Text("not json".to_string()),
            Payload::Raw(b"not json".to_vec()),
        ] {
            assert_eq!(
                encoder.convert_format(payload, Schema::Json).unwrap_err(),
                Error::InvalidJsonPayload
            );
        }
    }

    #[test]
    fn convert_format_should_transform_text_and_raw_utf8_bidirectionally() {
        let encoder = FlatBufferStreamEncoder::default();
        let text = "你好, Iggy";

        assert!(matches!(
            encoder.convert_format(Payload::Text(text.to_string()), Schema::Raw),
            Ok(Payload::Raw(bytes)) if bytes == text.as_bytes()
        ));
        assert!(matches!(
            encoder.convert_format(Payload::Raw(text.as_bytes().to_vec()), Schema::Text),
            Ok(Payload::Text(value)) if value == text
        ));
    }

    #[test]
    fn convert_format_should_reject_non_utf8_raw_as_text() {
        let encoder = FlatBufferStreamEncoder::default();

        let result = encoder.convert_format(Payload::Raw(vec![0xff, 0xfe]), Schema::Text);

        assert_eq!(result.unwrap_err(), Error::InvalidTextPayload);
    }

    #[test]
    fn convert_format_should_pass_through_same_format() {
        let encoder = FlatBufferStreamEncoder::default();

        assert!(matches!(
            encoder.convert_format(Payload::Text("value".to_string()), Schema::Text),
            Ok(Payload::Text(value)) if value == "value"
        ));
        assert!(matches!(
            encoder.convert_format(Payload::Raw(vec![1, 2, 3]), Schema::Raw),
            Ok(Payload::Raw(value)) if value == vec![1, 2, 3]
        ));
    }

    #[test]
    fn config_should_have_sensible_defaults() {
        let config = FlatBufferEncoderConfig::default();

        assert!(config.schema_path.is_none());
        assert!(config.root_table_name.is_none());
        assert!(config.field_mappings.is_none());
        assert!(!config.preserve_unknown_fields);
        assert_eq!(config.include_paths.len(), 1);
        assert_eq!(config.table_size_hint, 1024);
    }

    #[test]
    fn encoder_should_be_creatable_with_custom_config() {
        let config = FlatBufferEncoderConfig {
            schema_path: Some(PathBuf::from("/path/to/schema.fbs")),
            root_table_name: Some("MyTable".to_string()),
            table_size_hint: 2048,
            ..FlatBufferEncoderConfig::default()
        };

        let encoder = FlatBufferStreamEncoder::new(config.clone());

        assert_eq!(encoder.config.schema_path, config.schema_path);
        assert_eq!(encoder.config.root_table_name, config.root_table_name);
        assert_eq!(encoder.config.table_size_hint, config.table_size_hint);
    }

    #[test]
    fn update_config_should_replace_the_complete_configuration() {
        let mut encoder = FlatBufferStreamEncoder::default();
        let config = FlatBufferEncoderConfig {
            schema_path: Some(PathBuf::from("schema.bfbs")),
            root_table_name: Some("Root".to_string()),
            field_mappings: Some(HashMap::from([("old".to_string(), "new".to_string())])),
            preserve_unknown_fields: true,
            include_paths: vec![PathBuf::from("schemas"), PathBuf::from("includes")],
            table_size_hint: 4096,
        };

        encoder.update_config(config.clone()).unwrap();

        assert_eq!(encoder.config.schema_path, config.schema_path);
        assert_eq!(encoder.config.root_table_name, config.root_table_name);
        assert_eq!(encoder.config.field_mappings, config.field_mappings);
        assert_eq!(
            encoder.config.preserve_unknown_fields,
            config.preserve_unknown_fields
        );
        assert_eq!(encoder.config.include_paths, config.include_paths);
        assert_eq!(encoder.config.table_size_hint, config.table_size_hint);
    }

    #[test]
    fn encoder_should_report_flatbuffer_schema() {
        assert_eq!(
            FlatBufferStreamEncoder::default().schema(),
            Schema::FlatBuffer
        );
    }
}
