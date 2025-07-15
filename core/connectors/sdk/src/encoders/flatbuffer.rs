/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::{Error, Payload, Schema, StreamEncoder};
use base64::Engine;
use flatbuffers::FlatBufferBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

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

    fn apply_field_transformations(&self, payload: Payload) -> Result<Payload, Error> {
        if let Some(mappings) = &self.config.field_mappings {
            match payload {
                Payload::Json(json_value) => {
                    if let simd_json::OwnedValue::Object(mut map) = json_value {
                        let mut new_entries = Vec::new();

                        for (key, value) in map.iter() {
                            if let Some(new_key) = mappings.get(key) {
                                new_entries.push((new_key.clone(), value.clone()));
                            } else {
                                new_entries.push((key.clone(), value.clone()));
                            }
                        }

                        map.clear();
                        for (key, value) in new_entries {
                            map.insert(key, value);
                        }

                        Ok(Payload::Json(simd_json::OwnedValue::Object(map)))
                    } else {
                        Ok(Payload::Json(json_value))
                    }
                }
                other => Ok(other),
            }
        } else {
            Ok(payload)
        }
    }

    fn encode_json_to_flatbuffer(
        &self,
        json_value: simd_json::OwnedValue,
    ) -> Result<Vec<u8>, Error> {
        let mut builder = FlatBufferBuilder::with_capacity(self.config.table_size_hint);

        match json_value {
            simd_json::OwnedValue::Object(obj) => {
                let mut string_offsets = Vec::new();
                let mut string_keys = Vec::new();

                for (key, value) in obj.iter() {
                    let key_offset = builder.create_string(key);
                    string_keys.push(key_offset);

                    let value_string = match value {
                        simd_json::OwnedValue::String(s) => s.clone(),
                        other => {
                            simd_json::to_string(other).map_err(|_| Error::InvalidJsonPayload)?
                        }
                    };
                    let value_offset = builder.create_string(&value_string);
                    string_offsets.push(value_offset);
                }

                let pairs: Vec<_> = string_keys.into_iter().zip(string_offsets).collect();

                let mut vector_data = Vec::new();
                for (key_offset, value_offset) in pairs {
                    vector_data.push(key_offset.value());
                    vector_data.push(value_offset.value());
                }

                let vector = builder.create_vector(&vector_data);
                builder.finish_minimal(vector);
            }
            simd_json::OwnedValue::Array(arr) => {
                let json_string = simd_json::to_string(&simd_json::OwnedValue::Array(arr))
                    .map_err(|_| Error::InvalidJsonPayload)?;
                let string_offset = builder.create_string(&json_string);
                builder.finish_minimal(string_offset);
            }
            other => {
                let json_string =
                    simd_json::to_string(&other).map_err(|_| Error::InvalidJsonPayload)?;
                let string_offset = builder.create_string(&json_string);
                builder.finish_minimal(string_offset);
            }
        }

        Ok(builder.finished_data().to_vec())
    }

    fn encode_text_to_flatbuffer(&self, text: String) -> Result<Vec<u8>, Error> {
        let mut builder = FlatBufferBuilder::with_capacity(self.config.table_size_hint);

        let mut json_bytes = text.clone().into_bytes();
        if let Ok(json_value) = simd_json::to_owned_value(&mut json_bytes) {
            return self.encode_json_to_flatbuffer(json_value);
        }

        let string_offset = builder.create_string(&text);
        builder.finish_minimal(string_offset);

        Ok(builder.finished_data().to_vec())
    }

    fn encode_raw_to_flatbuffer(&self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        let mut builder = FlatBufferBuilder::with_capacity(self.config.table_size_hint);

        let vector = builder.create_vector(&data);
        builder.finish_minimal(vector);

        Ok(builder.finished_data().to_vec())
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
        let transformed_payload = self.apply_field_transformations(payload)?;

        match transformed_payload {
            Payload::Json(json_value) => self.encode_json_to_flatbuffer(json_value),
            Payload::Text(text) => self.encode_text_to_flatbuffer(text),
            Payload::Raw(data) => self.encode_raw_to_flatbuffer(data),
            Payload::FlatBuffer(data) => Ok(data),
            Payload::Proto(text) => self.encode_text_to_flatbuffer(text),
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

    #[test]
    fn encode_should_handle_json_payload() {
        let encoder = FlatBufferStreamEncoder::default();

        let json_value = simd_json::json!({
            "name": "John",
            "age": 30
        });

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
    }

    #[test]
    fn encode_should_handle_text_payload() {
        let encoder = FlatBufferStreamEncoder::default();

        let text_payload = Payload::Text("Hello, World!".to_string());
        let result = encoder.encode(text_payload);

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
    }

    #[test]
    fn encode_should_handle_raw_payload() {
        let encoder = FlatBufferStreamEncoder::default();

        let raw_data = vec![1, 2, 3, 4, 5];
        let raw_payload = Payload::Raw(raw_data);
        let result = encoder.encode(raw_payload);

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
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

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
    }

    #[test]
    fn convert_format_should_transform_flatbuffer_to_json() {
        let encoder = FlatBufferStreamEncoder::default();

        let flatbuffer_data = vec![1, 2, 3, 4, 5];
        let result =
            encoder.convert_format(Payload::FlatBuffer(flatbuffer_data.clone()), Schema::Json);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("flatbuffer_size"));
                assert!(map.contains_key("raw_data_base64"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn convert_format_should_transform_flatbuffer_to_text() {
        let encoder = FlatBufferStreamEncoder::default();

        let flatbuffer_data = vec![1, 2, 3, 4, 5];
        let result = encoder.convert_format(Payload::FlatBuffer(flatbuffer_data), Schema::Text);

        assert!(result.is_ok());
        if let Ok(Payload::Text(text)) = result {
            assert!(!text.is_empty());
        } else {
            panic!("Expected Text payload");
        }
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
}
