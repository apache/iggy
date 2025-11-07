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

use crate::{Error, Payload, Schema, StreamDecoder};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatBufferConfig {
    pub schema_path: Option<PathBuf>,
    pub root_table_name: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
    pub extract_as_json: bool,
    pub preserve_unknown_fields: bool,
    pub verify_buffers: bool,
    pub include_paths: Vec<PathBuf>,
}

impl Default for FlatBufferConfig {
    fn default() -> Self {
        Self {
            schema_path: None,
            root_table_name: None,
            field_mappings: None,
            extract_as_json: true,
            preserve_unknown_fields: false,
            verify_buffers: true,
            include_paths: vec![PathBuf::from(".")],
        }
    }
}

pub struct FlatBufferStreamDecoder {
    config: FlatBufferConfig,
}

impl FlatBufferStreamDecoder {
    pub fn new(config: FlatBufferConfig) -> Self {
        Self { config }
    }

    pub fn new_default() -> Self {
        Self::new(FlatBufferConfig::default())
    }

    pub fn update_config(&mut self, config: FlatBufferConfig) -> Result<(), Error> {
        self.config = config;
        Ok(())
    }

    fn verify_buffer(&self, data: &[u8]) -> Result<(), Error> {
        if !self.config.verify_buffers {
            return Ok(());
        }

        if data.len() < 4 {
            error!("FlatBuffer too small: {} bytes", data.len());
            return Err(Error::CannotDecode(Schema::FlatBuffer));
        }

        Ok(())
    }

    fn decode_as_json(&self, data: &[u8]) -> Result<Payload, Error> {
        self.verify_buffer(data)?;

        let mut json_map = Box::new(simd_json::owned::Object::new());

        json_map.insert(
            "buffer_size".to_string(),
            simd_json::OwnedValue::from(data.len()),
        );
        json_map.insert(
            "has_root_table".to_string(),
            simd_json::OwnedValue::from(true),
        );

        if let Some(root_table_name) = &self.config.root_table_name {
            json_map.insert(
                "root_table_type".to_string(),
                simd_json::OwnedValue::from(root_table_name.clone()),
            );
        }

        let base64_data = base64::engine::general_purpose::STANDARD.encode(data);
        json_map.insert(
            "raw_data_base64".to_string(),
            simd_json::OwnedValue::from(base64_data),
        );

        let json_payload = simd_json::OwnedValue::Object(json_map);
        let transformed = self.apply_field_transformations(Payload::Json(json_payload))?;
        Ok(transformed)
    }

    fn decode_as_raw(&self, data: Vec<u8>) -> Result<Payload, Error> {
        if self.config.verify_buffers {
            self.verify_buffer(&data)?;
        }
        Ok(Payload::FlatBuffer(data))
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
}

impl StreamDecoder for FlatBufferStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::FlatBuffer
    }

    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        if payload.is_empty() {
            return Err(Error::InvalidPayloadType);
        }

        if self.config.extract_as_json {
            self.decode_as_json(&payload)
        } else {
            self.decode_as_raw(payload)
        }
    }
}

impl Default for FlatBufferStreamDecoder {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flatbuffers::FlatBufferBuilder;

    #[test]
    fn decode_should_fail_given_empty_payload() {
        let decoder = FlatBufferStreamDecoder::default();
        let result = decoder.decode(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_should_return_flatbuffer_payload_when_extract_as_json_is_disabled() {
        let config = FlatBufferConfig {
            extract_as_json: false,
            verify_buffers: false,
            ..FlatBufferConfig::default()
        };
        let decoder = FlatBufferStreamDecoder::new(config);

        let test_data = b"fake_flatbuffer_data".to_vec();
        let result = decoder.decode(test_data.clone());

        assert!(result.is_ok());
        if let Ok(Payload::FlatBuffer(data)) = result {
            assert_eq!(data, test_data);
        } else {
            panic!("Expected FlatBuffer payload");
        }
    }

    #[test]
    fn decode_should_apply_field_mappings_when_configured() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("buffer_size".to_string(), "size".to_string());
        field_mappings.insert("root_table_type".to_string(), "table_type".to_string());

        let config = FlatBufferConfig {
            field_mappings: Some(field_mappings),
            extract_as_json: true,
            verify_buffers: false,
            ..FlatBufferConfig::default()
        };
        let decoder = FlatBufferStreamDecoder::new(config);

        let mut builder = FlatBufferBuilder::new();
        let root = builder.create_vector(&[0u8; 4]);
        builder.finish_minimal(root);
        let test_data = builder.finished_data().to_vec();

        let result = decoder.decode(test_data);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = &json_value {
                assert!(map.contains_key("size"));
                assert!(!map.contains_key("buffer_size"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn config_should_have_sensible_defaults() {
        let config = FlatBufferConfig::default();

        assert!(config.schema_path.is_none());
        assert!(config.root_table_name.is_none());
        assert!(config.field_mappings.is_none());
        assert!(config.extract_as_json);
        assert!(!config.preserve_unknown_fields);
        assert!(config.verify_buffers);
        assert_eq!(config.include_paths.len(), 1);
    }

    #[test]
    fn decoder_should_be_creatable_with_custom_config() {
        let config = FlatBufferConfig {
            schema_path: Some(PathBuf::from("/path/to/schema.fbs")),
            root_table_name: Some("MyTable".to_string()),
            extract_as_json: false,
            verify_buffers: false,
            ..FlatBufferConfig::default()
        };

        let decoder = FlatBufferStreamDecoder::new(config.clone());

        assert_eq!(decoder.config.schema_path, config.schema_path);
        assert_eq!(decoder.config.root_table_name, config.root_table_name);
        assert_eq!(decoder.config.extract_as_json, config.extract_as_json);
        assert_eq!(decoder.config.verify_buffers, config.verify_buffers);
    }
}
