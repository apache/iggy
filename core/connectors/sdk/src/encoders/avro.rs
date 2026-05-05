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

use crate::{Error, Payload, Schema, StreamEncoder, convert::owned_value_to_serde_json};
use apache_avro::Schema as AvroSchema;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AvroEncoderConfig {
    pub schema_path: Option<PathBuf>,
    pub schema_json: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
}

pub struct AvroStreamEncoder {
    config: AvroEncoderConfig,
    schema: Option<AvroSchema>,
}

impl AvroStreamEncoder {
    /// If you need fail-fast behaviour,
    /// use [`try_new`](Self::try_new) instead.
    pub fn new(config: AvroEncoderConfig) -> Self {
        let mut encoder = Self {
            config,
            schema: None,
        };
        if let Err(e) = encoder.load_schema() {
            error!("Failed to load Avro schema during encoder creation: {}", e);
        }
        encoder
    }

    pub fn try_new(config: AvroEncoderConfig) -> Result<Self, Error> {
        let mut encoder = Self {
            config,
            schema: None,
        };
        encoder.load_schema()?;
        Ok(encoder)
    }

    pub fn new_default() -> Self {
        Self::new(AvroEncoderConfig::default())
    }

    pub fn update_config(&mut self, config: AvroEncoderConfig) -> Result<(), Error> {
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

    fn encode_json_to_avro(&self, json_value: simd_json::OwnedValue) -> Result<Vec<u8>, Error> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            error!("Cannot encode JSON to Avro without a schema");
            Error::InvalidConfigValue("Avro schema is required for encoding".to_string())
        })?;

        let serde_value = owned_value_to_serde_json(&json_value);
        let avro_value = Self::serde_json_to_avro_value(serde_value, schema)?;

        apache_avro::to_avro_datum(schema, avro_value).map_err(|e| {
            error!("Failed to encode Avro datum: {}", e);
            Error::Serialization(format!("Avro encoding failed: {e}"))
        })
    }

    fn serde_json_to_avro_value(
        value: serde_json::Value,
        schema: &apache_avro::Schema,
    ) -> Result<apache_avro::types::Value, Error> {
        use apache_avro::Schema as AvroSchema;
        use apache_avro::types::Value as AvroValue;

        match (value, schema) {
            (serde_json::Value::Object(map), AvroSchema::Record(record_schema)) => {
                let mut record = Vec::new();
                for field in &record_schema.fields {
                    let field_value = map
                        .get(&field.name)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    record.push((
                        field.name.clone(),
                        Self::serde_json_to_avro_value(field_value, &field.schema)?,
                    ));
                }
                Ok(AvroValue::Record(record))
            }
            (serde_json::Value::Array(arr), AvroSchema::Array(array_schema)) => {
                Ok(AvroValue::Array(
                    arr.into_iter()
                        .map(|v| Self::serde_json_to_avro_value(v, &array_schema.items))
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            (serde_json::Value::Object(map), AvroSchema::Map(map_schema)) => Ok(AvroValue::Map(
                map.into_iter()
                    .map(|(k, v)| {
                        Self::serde_json_to_avro_value(v, &map_schema.types).map(|av| (k, av))
                    })
                    .collect::<Result<_, _>>()?,
            )),
            (value, AvroSchema::Union(union_schema)) => {
                let schemas = union_schema.variants();
                for (idx, variant_schema) in schemas.iter().enumerate() {
                    match Self::serde_json_to_avro_value(value.clone(), variant_schema) {
                        Ok(avro_val) if avro_val.validate(variant_schema) => {
                            return Ok(AvroValue::Union(idx as u32, Box::new(avro_val)));
                        }
                        _ => continue,
                    }
                }
                Err(Error::InvalidPayloadType)
            }
            (serde_json::Value::Null, AvroSchema::Null) => Ok(AvroValue::Null),
            (serde_json::Value::Bool(b), AvroSchema::Boolean) => Ok(AvroValue::Boolean(b)),
            (serde_json::Value::Number(n), AvroSchema::Int) => {
                let v = n.as_i64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Int: {n}"))
                })?;
                let v_i32 = i32::try_from(v).map_err(|_| {
                    Error::Serialization(format!("JSON number {v} out of range for Avro Int (i32)"))
                })?;
                Ok(AvroValue::Int(v_i32))
            }
            (serde_json::Value::Number(n), AvroSchema::Long) => {
                let v = n.as_i64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Long: {n}"))
                })?;
                Ok(AvroValue::Long(v))
            }
            (serde_json::Value::Number(n), AvroSchema::Float) => {
                let v = n.as_f64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Float: {n}"))
                })?;
                Ok(AvroValue::Float(v as f32))
            }
            (serde_json::Value::Number(n), AvroSchema::Double) => {
                let v = n.as_f64().ok_or_else(|| {
                    Error::Serialization(format!("Cannot convert JSON number to Avro Double: {n}"))
                })?;
                Ok(AvroValue::Double(v))
            }
            (serde_json::Value::String(s), AvroSchema::String) => Ok(AvroValue::String(s)),
            (serde_json::Value::String(s), AvroSchema::Bytes) => {
                Ok(AvroValue::Bytes(s.into_bytes()))
            }
            (serde_json::Value::Array(arr), AvroSchema::Bytes) => {
                let mut bytes = Vec::with_capacity(arr.len());
                for (i, v) in arr.into_iter().enumerate() {
                    let n = v.as_u64().ok_or_else(|| {
                        Error::Serialization(format!(
                            "Bytes array element at index {i} is not a valid u8: {v}"
                        ))
                    })?;
                    let b = u8::try_from(n).map_err(|_| {
                        Error::Serialization(format!(
                            "Bytes array element at index {i} out of u8 range: {n}"
                        ))
                    })?;
                    bytes.push(b);
                }
                Ok(AvroValue::Bytes(bytes))
            }

            (value, schema) => {
                let avro_val: AvroValue = value.into();
                if avro_val.validate(schema) {
                    Ok(avro_val)
                } else {
                    Err(Error::Serialization(format!(
                        "JSON value does not match Avro schema: {schema:?}"
                    )))
                }
            }
        }
    }

    fn encode_text_to_avro(&self, text: String) -> Result<Vec<u8>, Error> {
        let mut text_bytes = text.into_bytes();
        if let Ok(json_value) = simd_json::to_owned_value(&mut text_bytes) {
            return self.encode_json_to_avro(json_value);
        }

        Err(Error::InvalidJsonPayload)
    }

    fn encode_raw_to_avro(&self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        Ok(data)
    }

    pub fn convert_format(
        &self,
        payload: Payload,
        target_format: Schema,
    ) -> Result<Payload, Error> {
        match (payload, target_format) {
            (Payload::Avro(data), Schema::Json) => {
                let mut data_copy = data.clone();
                let json_value = simd_json::to_owned_value(&mut data_copy)
                    .unwrap_or_else(|_| simd_json::json!({ "avro_data_base64": base64::engine::general_purpose::STANDARD.encode(&data) }));
                Ok(Payload::Json(json_value))
            }
            (Payload::Avro(data), Schema::Text) => {
                let base64_data = base64::engine::general_purpose::STANDARD.encode(&data);
                Ok(Payload::Text(base64_data))
            }
            (Payload::Avro(data), Schema::Raw) => Ok(Payload::Raw(data)),

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

impl StreamEncoder for AvroStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Avro
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        let transformed_payload = self.apply_field_transformations(payload)?;

        match transformed_payload {
            Payload::Json(json_value) => self.encode_json_to_avro(json_value),
            Payload::Text(text) => self.encode_text_to_avro(text),
            Payload::Raw(data) => self.encode_raw_to_avro(data),
            Payload::Avro(data) => Ok(data),
            Payload::Proto(text) => self.encode_text_to_avro(text),
            Payload::FlatBuffer(data) => self.encode_raw_to_avro(data),
        }
    }
}

impl Default for AvroStreamEncoder {
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

    #[test]
    fn encode_should_handle_json_payload_with_schema() {
        let schema_json = create_test_schema_json();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let json_value = simd_json::json!({
            "name": "Alice",
            "age": 30
        });

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
    }

    #[test]
    fn encode_should_pass_through_avro_payload() {
        let encoder = AvroStreamEncoder::default();

        let avro_data = vec![1, 2, 3, 4, 5];
        let avro_payload = Payload::Avro(avro_data.clone());
        let result = encoder.encode(avro_payload);

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert_eq!(encoded_data, avro_data);
    }

    #[test]
    fn encode_should_apply_field_mappings_when_configured() {
        let schema_json = create_test_schema_json();
        let mut field_mappings = HashMap::new();
        field_mappings.insert("full_name".to_string(), "name".to_string());

        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            field_mappings: Some(field_mappings),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let json_value = simd_json::json!({
            "full_name": "Alice",
            "age": 30
        });

        let result = encoder.encode(Payload::Json(json_value));

        assert!(result.is_ok());
        let encoded_data = result.unwrap();
        assert!(!encoded_data.is_empty());
    }

    #[test]
    fn encode_should_fail_without_schema_for_json_payload() {
        let encoder = AvroStreamEncoder::default();

        let json_value = simd_json::json!({
            "name": "Alice",
            "age": 30
        });

        let result = encoder.encode(Payload::Json(json_value));
        assert!(result.is_err());
    }

    #[test]
    fn encode_should_handle_union_null_string_schema() {
        let schema_json = r#"["null", "string"]"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!("hello")));
        assert!(result.is_ok(), "Expected string union variant to encode");
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn encode_should_handle_union_null_int_schema() {
        let schema_json = r#"["null", "int"]"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!(42)));
        assert!(result.is_ok(), "Expected int union variant to encode");
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn encode_should_fail_on_int_overflow() {
        let schema_json = r#"{"type": "int"}"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!(i64::MAX)));
        assert!(result.is_err(), "Expected overflow to fail");
    }

    #[test]
    fn encode_should_fail_on_invalid_bytes_array_element() {
        let schema_json = r#"{"type": "bytes"}"#.to_string();
        let config = AvroEncoderConfig {
            schema_json: Some(schema_json),
            ..AvroEncoderConfig::default()
        };
        let encoder = AvroStreamEncoder::new(config);

        let result = encoder.encode(Payload::Json(simd_json::json!([1, 2, 300])));
        assert!(result.is_err(), "Expected out-of-range byte to fail");
    }

    #[test]
    fn convert_format_should_transform_avro_to_json() {
        let encoder = AvroStreamEncoder::default();

        let avro_data = vec![1, 2, 3, 4, 5];
        let result = encoder.convert_format(Payload::Avro(avro_data.clone()), Schema::Json);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("avro_data_base64"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn convert_format_should_transform_avro_to_text() {
        let encoder = AvroStreamEncoder::default();

        let avro_data = vec![1, 2, 3, 4, 5];
        let result = encoder.convert_format(Payload::Avro(avro_data), Schema::Text);

        assert!(result.is_ok());
        if let Ok(Payload::Text(text)) = result {
            assert!(!text.is_empty());
        } else {
            panic!("Expected Text payload");
        }
    }

    #[test]
    fn config_should_have_sensible_defaults() {
        let config = AvroEncoderConfig::default();

        assert!(config.schema_path.is_none());
        assert!(config.schema_json.is_none());
        assert!(config.field_mappings.is_none());
    }

    #[test]
    fn encoder_should_be_creatable_with_custom_config() {
        let config = AvroEncoderConfig {
            schema_path: Some(PathBuf::from("/path/to/schema.avsc")),
            schema_json: Some(r#"{"type": "string"}"#.to_string()),
            ..AvroEncoderConfig::default()
        };

        let encoder = AvroStreamEncoder::new(config.clone());

        assert_eq!(encoder.config.schema_path, config.schema_path);
        assert_eq!(encoder.config.schema_json, config.schema_json);
    }

    #[test]
    fn try_new_should_fail_on_invalid_schema() {
        let config = AvroEncoderConfig {
            schema_json: Some("not a valid schema".to_string()),
            ..AvroEncoderConfig::default()
        };
        let result = AvroStreamEncoder::try_new(config);
        assert!(
            result.is_err(),
            "Expected try_new to fail with invalid schema"
        );
    }

    #[test]
    fn update_config_should_rollback_on_invalid_schema() {
        let valid_schema = r#"{"type": "string"}"#.to_string();
        let mut encoder = AvroStreamEncoder::new(AvroEncoderConfig {
            schema_json: Some(valid_schema.clone()),
            ..AvroEncoderConfig::default()
        });

        assert!(encoder.schema.is_some());

        let old_config = encoder.config.clone();
        let invalid_config = AvroEncoderConfig {
            schema_json: Some("invalid schema".to_string()),
            ..AvroEncoderConfig::default()
        };

        let result = encoder.update_config(invalid_config);
        assert!(result.is_err(), "Expected update_config to fail");

        // After rollback, config and schema should remain unchanged
        assert_eq!(encoder.config.schema_json, old_config.schema_json);
        assert!(encoder.schema.is_some());
    }
}
