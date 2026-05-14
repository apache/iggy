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
use apache_avro::Schema as AvroSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvroConfig {
    pub schema_path: Option<PathBuf>,
    pub schema_json: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
    pub extract_as_json: bool,
}

impl Default for AvroConfig {
    fn default() -> Self {
        Self {
            schema_path: None,
            schema_json: None,
            field_mappings: None,
            extract_as_json: true,
        }
    }
}

pub struct AvroStreamDecoder {
    config: AvroConfig,
    schema: Option<AvroSchema>,
}

impl AvroStreamDecoder {
    /// Use [`Self::try_new`] for fail-fast behaviour.
    pub fn new(config: AvroConfig) -> Self {
        let mut decoder = Self {
            config,
            schema: None,
        };
        if let Err(e) = decoder.load_schema() {
            error!("Failed to load Avro schema during decoder creation: {}", e);
        }
        decoder
    }

    pub fn try_new(config: AvroConfig) -> Result<Self, Error> {
        let mut decoder = Self {
            config,
            schema: None,
        };
        decoder.load_schema()?;
        Ok(decoder)
    }

    pub fn new_default() -> Self {
        Self::new(AvroConfig::default())
    }

    pub fn update_config(&mut self, config: AvroConfig) -> Result<(), Error> {
        let old_config = std::mem::replace(&mut self.config, config);
        if let Err(e) = self.load_schema() {
            // Rollback on failure to keep config/schema consistent
            self.config = old_config;
            return Err(e);
        }
        Ok(())
    }

    fn load_schema(&mut self) -> Result<(), Error> {
        if let Some(schema_json) = &self.config.schema_json {
            match AvroSchema::parse_str(schema_json) {
                Ok(schema) => {
                    info!("Loaded Avro schema from inline JSON");
                    self.schema = Some(schema);
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to parse inline Avro schema: {}", e);
                    return Err(Error::InvalidConfigValue(format!(
                        "Invalid Avro schema JSON: {e}"
                    )));
                }
            }
        }

        if let Some(schema_path) = &self.config.schema_path {
            match std::fs::read_to_string(schema_path) {
                Ok(schema_content) => match AvroSchema::parse_str(&schema_content) {
                    Ok(schema) => {
                        info!("Loaded Avro schema from file: {:?}", schema_path);
                        self.schema = Some(schema);
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Failed to parse Avro schema file: {}", e);
                        return Err(Error::InvalidConfigValue(format!(
                            "Invalid Avro schema file: {e}"
                        )));
                    }
                },
                Err(e) => {
                    error!("Failed to read Avro schema file: {}", e);
                    return Err(Error::InvalidConfigValue(format!(
                        "Cannot read schema file: {e}"
                    )));
                }
            }
        }

        self.schema = None;
        Ok(())
    }

    fn decode_as_json(&self, payload: &[u8]) -> Result<Payload, Error> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            error!("Cannot decode Avro to JSON without a schema");
            Error::InvalidConfigValue("Avro schema is required for JSON extraction".to_string())
        })?;

        let mut reader = payload;
        let avro_value = apache_avro::from_avro_datum(schema, &mut reader, None).map_err(|e| {
            error!("Failed to decode Avro datum: {}", e);
            Error::CannotDecode(Schema::Avro)
        })?;

        if !reader.is_empty() {
            error!(
                "Avro payload contains trailing bytes after datum: {} bytes remaining",
                reader.len()
            );
            return Err(Error::CannotDecode(Schema::Avro));
        }

        let serde_value = serde_json::Value::try_from(avro_value).map_err(|e| {
            error!("Failed to convert Avro value to JSON: {}", e);
            Error::CannotDecode(Schema::Avro)
        })?;

        let mut json_bytes = serde_json::to_vec(&serde_value).map_err(|e| {
            error!("Failed to serialize JSON value: {}", e);
            Error::CannotDecode(Schema::Avro)
        })?;

        let json_value = simd_json::to_owned_value(&mut json_bytes).map_err(|e| {
            error!("Failed to parse JSON into simd_json: {}", e);
            Error::CannotDecode(Schema::Avro)
        })?;

        let transformed = self.apply_field_transformations(Payload::Json(json_value))?;
        Ok(transformed)
    }

    fn decode_as_raw(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        Ok(Payload::Avro(payload))
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

impl StreamDecoder for AvroStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Avro
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

impl Default for AvroStreamDecoder {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema_json() -> String {
        r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }"#
        .to_string()
    }

    fn encode_test_avro_data(schema_json: &str) -> Vec<u8> {
        use apache_avro::types::Value as AvroValue;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let record = AvroValue::Record(vec![
            ("name".to_string(), AvroValue::String("Alice".to_string())),
            ("age".to_string(), AvroValue::Int(30)),
        ]);
        apache_avro::to_avro_datum(&schema, record).unwrap()
    }

    #[test]
    fn decode_should_fail_given_empty_payload() {
        let decoder = AvroStreamDecoder::default();
        let result = decoder.decode(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_should_return_avro_payload_when_extract_as_json_is_disabled() {
        let config = AvroConfig {
            extract_as_json: false,
            ..AvroConfig::default()
        };
        let decoder = AvroStreamDecoder::new(config);

        let test_data = b"fake_avro_data".to_vec();
        let result = decoder.decode(test_data.clone());

        assert!(result.is_ok());
        if let Ok(Payload::Avro(data)) = result {
            assert_eq!(data, test_data);
        } else {
            panic!("Expected Avro payload");
        }
    }

    #[test]
    fn decode_should_decode_avro_to_json_with_schema() {
        let schema_json = create_test_schema_json();
        let avro_data = encode_test_avro_data(&schema_json);

        let config = AvroConfig {
            schema_json: Some(schema_json),
            extract_as_json: true,
            ..AvroConfig::default()
        };
        let decoder = AvroStreamDecoder::new(config);

        let result = decoder.decode(avro_data);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = &json_value {
                assert!(map.contains_key("name"));
                assert!(map.contains_key("age"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn decode_should_apply_field_mappings_when_configured() {
        let schema_json = create_test_schema_json();
        let avro_data = encode_test_avro_data(&schema_json);

        let mut field_mappings = HashMap::new();
        field_mappings.insert("name".to_string(), "full_name".to_string());

        let config = AvroConfig {
            schema_json: Some(schema_json),
            extract_as_json: true,
            field_mappings: Some(field_mappings),
            ..AvroConfig::default()
        };
        let decoder = AvroStreamDecoder::new(config);

        let result = decoder.decode(avro_data);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = &json_value {
                assert!(map.contains_key("full_name"));
                assert!(!map.contains_key("name"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn config_should_have_sensible_defaults() {
        let config = AvroConfig::default();

        assert!(config.schema_path.is_none());
        assert!(config.schema_json.is_none());
        assert!(config.field_mappings.is_none());
        assert!(config.extract_as_json);
    }

    #[test]
    fn decoder_should_be_creatable_with_custom_config() {
        let config = AvroConfig {
            schema_path: Some(PathBuf::from("/path/to/schema.avsc")),
            schema_json: Some(r#"{"type": "string"}"#.to_string()),
            extract_as_json: false,
            ..AvroConfig::default()
        };

        let decoder = AvroStreamDecoder::new(config.clone());

        assert_eq!(decoder.config.schema_path, config.schema_path);
        assert_eq!(decoder.config.schema_json, config.schema_json);
        assert_eq!(decoder.config.extract_as_json, config.extract_as_json);
    }

    #[test]
    fn decode_should_fail_without_schema_when_extract_as_json_is_true() {
        let config = AvroConfig {
            schema_json: None,
            schema_path: None,
            extract_as_json: true,
            ..AvroConfig::default()
        };
        let decoder = AvroStreamDecoder::new(config);

        let result = decoder.decode(b"{\"name\": \"test\"}".to_vec());
        assert!(
            result.is_err(),
            "Expected error when schema is missing and extract_as_json=true"
        );
    }

    #[test]
    fn try_new_should_fail_on_invalid_schema() {
        let config = AvroConfig {
            schema_json: Some("not a valid schema".to_string()),
            ..AvroConfig::default()
        };
        let result = AvroStreamDecoder::try_new(config);
        assert!(
            result.is_err(),
            "Expected try_new to fail with invalid schema"
        );
    }

    #[test]
    fn update_config_should_rollback_on_invalid_schema() {
        let valid_schema = create_test_schema_json();
        let mut decoder = AvroStreamDecoder::new(AvroConfig {
            schema_json: Some(valid_schema.clone()),
            extract_as_json: true,
            ..AvroConfig::default()
        });

        assert!(decoder.schema.is_some());

        let old_config = decoder.config.clone();
        let invalid_config = AvroConfig {
            schema_json: Some("invalid schema".to_string()),
            extract_as_json: true,
            ..AvroConfig::default()
        };

        let result = decoder.update_config(invalid_config);
        assert!(result.is_err(), "Expected update_config to fail");

        // After rollback, config and schema should remain unchanged
        assert_eq!(decoder.config.schema_json, old_config.schema_json);
        assert!(decoder.schema.is_some());
    }

    #[test]
    fn decode_should_reject_trailing_bytes() {
        let schema_json = create_test_schema_json();
        let mut avro_data = encode_test_avro_data(&schema_json);
        avro_data.extend_from_slice(b"trailing garbage");

        let config = AvroConfig {
            schema_json: Some(schema_json),
            extract_as_json: true,
            ..AvroConfig::default()
        };
        let decoder = AvroStreamDecoder::new(config);

        let result = decoder.decode(avro_data);
        assert!(
            result.is_err(),
            "Expected decode to fail when payload has trailing bytes"
        );
    }
}
