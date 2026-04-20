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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

use super::{Transform, TransformType};
use crate::decoders::avro::{AvroConfig, AvroStreamDecoder};
use crate::encoders::avro::{AvroEncoderConfig, AvroStreamEncoder};
use crate::{DecodedMessage, Error, Payload, Schema, TopicMetadata};
use crate::{StreamDecoder, StreamEncoder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvroConvertConfig {
    pub source_format: Schema,
    pub target_format: Schema,
    pub schema_path: Option<PathBuf>,
    pub schema_json: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
    pub conversion_options: AvroConversionOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvroConversionOptions {
    pub pretty_json: bool,
    pub include_metadata: bool,
    pub strict_mode: bool,
}

impl Default for AvroConvertConfig {
    fn default() -> Self {
        Self {
            source_format: Schema::Avro,
            target_format: Schema::Json,
            schema_path: None,
            schema_json: None,
            field_mappings: None,
            conversion_options: AvroConversionOptions::default(),
        }
    }
}

impl Default for AvroConversionOptions {
    fn default() -> Self {
        Self {
            pretty_json: false,
            include_metadata: false,
            strict_mode: false,
        }
    }
}

pub struct AvroConvert {
    config: AvroConvertConfig,
}

impl AvroConvert {
    pub fn new(config: AvroConvertConfig) -> Self {
        Self { config }
    }

    pub fn new_default() -> Self {
        Self::new(AvroConvertConfig::default())
    }

    fn apply_field_mappings(
        &self,
        payload: Payload,
        field_mappings: &HashMap<String, String>,
    ) -> Result<Payload, Error> {
        match payload {
            Payload::Json(json_value) => {
                if let simd_json::OwnedValue::Object(mut map) = json_value {
                    let mut new_entries = Vec::new();

                    for (key, value) in map.iter() {
                        if let Some(new_key) = field_mappings.get(key) {
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
    }
}

impl Transform for AvroConvert {
    fn r#type(&self) -> TransformType {
        TransformType::AvroConvert
    }

    fn transform(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        // Apply field mappings if configured
        if let Some(field_mappings) = &self.config.field_mappings {
            message.payload = self.apply_field_mappings(message.payload, field_mappings)?;
        }

        // Perform format conversion based on configuration
        message.payload = match (&self.config.source_format, &self.config.target_format) {
            (Schema::Json, Schema::Avro) => {
                let encoder_config = AvroEncoderConfig {
                    schema_path: self.config.schema_path.clone(),
                    schema_json: self.config.schema_json.clone(),
                    field_mappings: self.config.field_mappings.clone(),
                };
                let encoder = AvroStreamEncoder::new(encoder_config);
                let encoded_bytes = encoder.encode(message.payload)?;
                Payload::Avro(encoded_bytes)
            }
            (Schema::Avro, Schema::Json) => {
                let decoder_config = AvroConfig {
                    schema_path: self.config.schema_path.clone(),
                    schema_json: self.config.schema_json.clone(),
                    field_mappings: self.config.field_mappings.clone(),
                    extract_as_json: true,
                };
                let decoder = AvroStreamDecoder::new(decoder_config);
                if let Payload::Avro(bytes) = message.payload {
                    decoder.decode(bytes)?
                } else {
                    return Err(Error::InvalidPayloadType);
                }
            }
            (Schema::Avro, Schema::Text) => {
                let encoder = AvroStreamEncoder::default();
                encoder.convert_format(message.payload, Schema::Text)?
            }
            (Schema::Avro, Schema::Raw) => {
                let encoder = AvroStreamEncoder::default();
                encoder.convert_format(message.payload, Schema::Raw)?
            }
            (Schema::Text, Schema::Avro) => {
                let encoder_config = AvroEncoderConfig {
                    schema_path: self.config.schema_path.clone(),
                    schema_json: self.config.schema_json.clone(),
                    field_mappings: self.config.field_mappings.clone(),
                };
                let encoder = AvroStreamEncoder::new(encoder_config);
                let encoded_bytes = encoder.encode(message.payload)?;
                Payload::Avro(encoded_bytes)
            }
            (Schema::Raw, Schema::Avro) => {
                if let Payload::Raw(data) = message.payload {
                    Payload::Avro(data)
                } else {
                    return Err(Error::InvalidPayloadType);
                }
            }
            (source, target) if source == target => message.payload,
            _ => {
                // For unsupported conversions, pass through unchanged
                message.payload
            }
        };

        Ok(Some(message))
    }
}

impl Default for AvroConvert {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TopicMetadata;

    fn create_test_message(payload: Payload) -> DecodedMessage {
        DecodedMessage {
            id: Some(123),
            offset: Some(456),
            checksum: Some(789),
            timestamp: Some(1234567890),
            origin_timestamp: Some(1234567890),
            headers: None,
            payload,
        }
    }

    fn create_test_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "test_topic".to_string(),
        }
    }

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
        use apache_avro::Schema as AvroSchema;
        use apache_avro::types::Value as AvroValue;
        let schema = AvroSchema::parse_str(schema_json).unwrap();
        let record = AvroValue::Record(vec![
            ("name".to_string(), AvroValue::String("Alice".to_string())),
            ("age".to_string(), AvroValue::Int(30)),
        ]);
        apache_avro::to_avro_datum(&schema, record).unwrap()
    }

    #[test]
    fn transform_should_convert_avro_to_json_successfully() {
        let schema_json = create_test_schema_json();
        let avro_data = encode_test_avro_data(&schema_json);

        let config = AvroConvertConfig {
            source_format: Schema::Avro,
            target_format: Schema::Json,
            schema_json: Some(schema_json),
            ..AvroConvertConfig::default()
        };
        let converter = AvroConvert::new(config);
        let metadata = create_test_metadata();

        let avro_payload = Payload::Avro(avro_data);
        let message = create_test_message(avro_payload);

        let result = converter.transform(&metadata, message);

        assert!(result.is_ok());
        if let Ok(Some(transformed_message)) = result {
            if let Payload::Json(json_value) = transformed_message.payload {
                if let simd_json::OwnedValue::Object(map) = json_value {
                    assert!(map.contains_key("name"));
                    assert!(map.contains_key("age"));
                } else {
                    panic!("Expected JSON object");
                }
            } else {
                panic!("Expected JSON payload");
            }
        } else {
            panic!("Expected transformed message");
        }
    }

    #[test]
    fn transform_should_convert_json_to_avro_successfully() {
        let schema_json = create_test_schema_json();

        let config = AvroConvertConfig {
            source_format: Schema::Json,
            target_format: Schema::Avro,
            schema_json: Some(schema_json),
            ..AvroConvertConfig::default()
        };
        let converter = AvroConvert::new(config);
        let metadata = create_test_metadata();

        let json_payload = Payload::Json(simd_json::json!({
            "name": "Bob",
            "age": 25
        }));
        let message = create_test_message(json_payload);

        let result = converter.transform(&metadata, message);

        assert!(result.is_ok());
        if let Ok(Some(transformed_message)) = result {
            if let Payload::Avro(data) = transformed_message.payload {
                assert!(!data.is_empty());
            } else {
                panic!("Expected Avro payload");
            }
        } else {
            panic!("Expected transformed message");
        }
    }

    #[test]
    fn transform_should_convert_avro_to_text_successfully() {
        let avro_data = vec![1, 2, 3, 4, 5];

        let config = AvroConvertConfig {
            source_format: Schema::Avro,
            target_format: Schema::Text,
            ..AvroConvertConfig::default()
        };
        let converter = AvroConvert::new(config);
        let metadata = create_test_metadata();

        let avro_payload = Payload::Avro(avro_data);
        let message = create_test_message(avro_payload);

        let result = converter.transform(&metadata, message);

        assert!(result.is_ok());
        if let Ok(Some(transformed_message)) = result {
            if let Payload::Text(text) = transformed_message.payload {
                assert!(!text.is_empty());
            } else {
                panic!("Expected Text payload");
            }
        } else {
            panic!("Expected transformed message");
        }
    }

    #[test]
    fn transform_should_apply_field_mappings_during_conversion() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("name".to_string(), "full_name".to_string());

        let schema_json = create_test_schema_json();
        let avro_data = encode_test_avro_data(&schema_json);

        let config = AvroConvertConfig {
            source_format: Schema::Avro,
            target_format: Schema::Json,
            schema_json: Some(schema_json),
            field_mappings: Some(field_mappings),
            ..AvroConvertConfig::default()
        };
        let converter = AvroConvert::new(config);
        let metadata = create_test_metadata();

        let avro_payload = Payload::Avro(avro_data);
        let message = create_test_message(avro_payload);

        let result = converter.transform(&metadata, message);

        assert!(result.is_ok());
        if let Ok(Some(transformed_message)) = result {
            if let Payload::Json(json_value) = transformed_message.payload {
                if let simd_json::OwnedValue::Object(map) = json_value {
                    assert!(map.contains_key("full_name"));
                    assert!(!map.contains_key("name"));
                } else {
                    panic!("Expected JSON object");
                }
            } else {
                panic!("Expected JSON payload");
            }
        } else {
            panic!("Expected transformed message");
        }
    }

    #[test]
    fn transform_should_pass_through_same_format() {
        let config = AvroConvertConfig {
            source_format: Schema::Avro,
            target_format: Schema::Avro,
            ..AvroConvertConfig::default()
        };
        let converter = AvroConvert::new(config);
        let metadata = create_test_metadata();

        let avro_data = vec![1, 2, 3, 4, 5];
        let avro_payload = Payload::Avro(avro_data.clone());
        let message = create_test_message(avro_payload);

        let result = converter.transform(&metadata, message);

        assert!(result.is_ok());
        if let Ok(Some(transformed_message)) = result {
            if let Payload::Avro(data) = transformed_message.payload {
                assert_eq!(data, avro_data);
            } else {
                panic!("Expected Avro payload");
            }
        } else {
            panic!("Expected transformed message");
        }
    }
}
