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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

use super::{Transform, TransformType};
use crate::decoders::flatbuffer::{FlatBufferConfig, FlatBufferStreamDecoder};
use crate::encoders::flatbuffer::{FlatBufferEncoderConfig, FlatBufferStreamEncoder};
use crate::{DecodedMessage, Error, Payload, Schema, TopicMetadata};
use crate::{StreamDecoder, StreamEncoder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatBufferConvertConfig {
    pub source_format: Schema,
    pub target_format: Schema,
    pub schema_path: Option<PathBuf>,
    pub root_table_name: Option<String>,
    pub field_mappings: Option<HashMap<String, String>>,
    pub include_paths: Vec<PathBuf>,
    pub preserve_unknown_fields: bool,
    pub conversion_options: FlatBufferConversionOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatBufferConversionOptions {
    pub verify_buffers: bool,
    pub pretty_json: bool,
    pub include_metadata: bool,
    pub buffer_size_hint: usize,
    pub strict_mode: bool,
}

impl Default for FlatBufferConvertConfig {
    fn default() -> Self {
        Self {
            source_format: Schema::FlatBuffer,
            target_format: Schema::Json,
            schema_path: None,
            root_table_name: None,
            field_mappings: None,
            include_paths: vec![PathBuf::from(".")],
            preserve_unknown_fields: false,
            conversion_options: FlatBufferConversionOptions::default(),
        }
    }
}

impl Default for FlatBufferConversionOptions {
    fn default() -> Self {
        Self {
            verify_buffers: true,
            pretty_json: false,
            include_metadata: false,
            buffer_size_hint: 1024,
            strict_mode: false,
        }
    }
}

pub struct FlatBufferConvert {
    config: FlatBufferConvertConfig,
}

impl FlatBufferConvert {
    pub fn new(config: FlatBufferConvertConfig) -> Result<Self, Error> {
        Self::validate_conversion(config.source_format, config.target_format)?;
        Ok(Self { config })
    }

    fn validate_conversion(source: Schema, target: Schema) -> Result<(), Error> {
        if matches!(
            (source, target),
            (Schema::Json, Schema::FlatBuffer)
                | (Schema::FlatBuffer, Schema::Json)
                | (Schema::FlatBuffer, Schema::Text)
                | (Schema::FlatBuffer, Schema::Raw)
        ) || source == target
        {
            return Ok(());
        }

        Err(Error::InvalidConfigValue(format!(
            "unsupported FlatBuffer conversion: {source} -> {target}"
        )))
    }

    fn payload_matches_source_format(source_format: Schema, payload: &Payload) -> bool {
        matches!(
            (source_format, payload),
            (Schema::Json, Payload::Json(_))
                | (Schema::Raw, Payload::Raw(_))
                | (Schema::Text, Payload::Text(_))
                | (Schema::Proto, Payload::Proto(_))
                | (Schema::FlatBuffer, Payload::FlatBuffer(_))
                | (Schema::Avro, Payload::Avro(_))
        )
    }
}

impl Transform for FlatBufferConvert {
    fn r#type(&self) -> TransformType {
        TransformType::FlatBufferConvert
    }

    fn transform(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if !Self::payload_matches_source_format(self.config.source_format, &message.payload) {
            return Err(Error::InvalidConfigValue(format!(
                "expected {} payload",
                self.config.source_format
            )));
        }

        message.payload = match (&self.config.source_format, &self.config.target_format) {
            (Schema::Json, Schema::FlatBuffer) => {
                let encoder_config = FlatBufferEncoderConfig {
                    schema_path: self.config.schema_path.clone(),
                    root_table_name: self.config.root_table_name.clone(),
                    field_mappings: self.config.field_mappings.clone(),
                    preserve_unknown_fields: self.config.preserve_unknown_fields,
                    include_paths: self.config.include_paths.clone(),
                    table_size_hint: self.config.conversion_options.buffer_size_hint,
                };
                let encoder = FlatBufferStreamEncoder::new(encoder_config);
                let encoded_bytes = encoder.encode(message.payload)?;
                Payload::FlatBuffer(encoded_bytes)
            }
            (Schema::FlatBuffer, Schema::Json) => {
                let decoder_config = FlatBufferConfig {
                    schema_path: self.config.schema_path.clone(),
                    root_table_name: self.config.root_table_name.clone(),
                    field_mappings: self.config.field_mappings.clone(),
                    extract_as_json: true,
                    preserve_unknown_fields: self.config.preserve_unknown_fields,
                    verify_buffers: self.config.conversion_options.verify_buffers,
                    include_paths: self.config.include_paths.clone(),
                };
                let decoder = FlatBufferStreamDecoder::new(decoder_config);
                let Payload::FlatBuffer(bytes) = message.payload else {
                    return Err(Error::InvalidPayloadType);
                };
                decoder.decode(bytes)?
            }
            (Schema::FlatBuffer, Schema::Text) => {
                let encoder = FlatBufferStreamEncoder::default();
                encoder.convert_format(message.payload, Schema::Text)?
            }
            (Schema::FlatBuffer, Schema::Raw) => {
                let encoder = FlatBufferStreamEncoder::default();
                encoder.convert_format(message.payload, Schema::Raw)?
            }
            (source, target) if source == target => message.payload,
            _ => unreachable!("conversion pair was validated during construction"),
        };

        Ok(Some(message))
    }
}

impl Default for FlatBufferConvert {
    fn default() -> Self {
        Self::new(FlatBufferConvertConfig::default())
            .expect("default FlatBuffer conversion configuration must be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn message(payload: Payload) -> DecodedMessage {
        DecodedMessage {
            id: None,
            offset: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload,
        }
    }

    fn metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "test_topic".to_string(),
        }
    }

    #[test]
    fn given_unsupported_conversion_should_fail_during_construction() {
        let result = FlatBufferConvert::new(FlatBufferConvertConfig {
            source_format: Schema::Text,
            target_format: Schema::FlatBuffer,
            ..FlatBufferConvertConfig::default()
        });

        assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_payload_not_matching_each_source_format_should_return_error() {
        let cases = [
            (Schema::Json, Payload::Text(String::new())),
            (Schema::Raw, Payload::Json(simd_json::json!({}))),
            (Schema::Text, Payload::Raw(vec![])),
            (Schema::Proto, Payload::Text(String::new())),
            (Schema::FlatBuffer, Payload::Avro(vec![])),
            (Schema::Avro, Payload::FlatBuffer(vec![])),
        ];

        for (source_format, payload) in cases {
            let converter = FlatBufferConvert::new(FlatBufferConvertConfig {
                source_format,
                target_format: source_format,
                ..FlatBufferConvertConfig::default()
            })
            .unwrap();

            let result = converter.transform(&metadata(), message(payload));

            assert!(
                matches!(result, Err(Error::InvalidConfigValue(_))),
                "{source_format} should reject a mismatched payload"
            );
        }
    }

    #[test]
    fn given_same_source_and_target_format_should_pass_through() {
        let converter = FlatBufferConvert::new(FlatBufferConvertConfig {
            source_format: Schema::FlatBuffer,
            target_format: Schema::FlatBuffer,
            ..FlatBufferConvertConfig::default()
        })
        .unwrap();
        let bytes = vec![1, 2, 3, 4];

        let result = converter
            .transform(&metadata(), message(Payload::FlatBuffer(bytes.clone())))
            .unwrap()
            .unwrap();

        assert!(matches!(result.payload, Payload::FlatBuffer(data) if data == bytes));
    }

    #[test]
    fn given_json_to_flatbuffer_should_apply_field_mappings_once() {
        let field_mappings = HashMap::from([
            ("first".to_string(), "second".to_string()),
            ("second".to_string(), "third".to_string()),
        ]);
        let config = FlatBufferConvertConfig {
            source_format: Schema::Json,
            target_format: Schema::FlatBuffer,
            field_mappings: Some(field_mappings.clone()),
            ..FlatBufferConvertConfig::default()
        };
        let encoder = FlatBufferStreamEncoder::new(FlatBufferEncoderConfig {
            field_mappings: Some(field_mappings),
            ..FlatBufferEncoderConfig::default()
        });
        let payload = Payload::Json(simd_json::json!({"first": "value"}));
        let expected = encoder.encode(payload.clone()).unwrap();

        let result = FlatBufferConvert::new(config)
            .unwrap()
            .transform(&metadata(), message(payload))
            .unwrap()
            .unwrap();

        assert!(matches!(result.payload, Payload::FlatBuffer(data) if data == expected));
    }

    #[test]
    fn given_flatbuffer_when_converting_to_text_should_base64_encode_payload() {
        let converter = FlatBufferConvert::new(FlatBufferConvertConfig {
            source_format: Schema::FlatBuffer,
            target_format: Schema::Text,
            ..FlatBufferConvertConfig::default()
        })
        .unwrap();
        let bytes = vec![1, 2, 3, 4];
        let expected = base64::engine::general_purpose::STANDARD.encode(&bytes);

        let result = converter
            .transform(&metadata(), message(Payload::FlatBuffer(bytes)))
            .unwrap()
            .unwrap();

        assert!(matches!(result.payload, Payload::Text(text) if text == expected));
    }

    #[test]
    fn given_flatbuffer_when_converting_to_raw_should_preserve_bytes() {
        let converter = FlatBufferConvert::new(FlatBufferConvertConfig {
            source_format: Schema::FlatBuffer,
            target_format: Schema::Raw,
            ..FlatBufferConvertConfig::default()
        })
        .unwrap();
        let bytes = vec![1, 2, 3, 4];

        let result = converter
            .transform(&metadata(), message(Payload::FlatBuffer(bytes.clone())))
            .unwrap()
            .unwrap();

        assert!(matches!(result.payload, Payload::Raw(data) if data == bytes));
    }

    #[test]
    fn given_flatbuffer_when_converting_to_json_should_include_buffer_metadata() {
        let converter = FlatBufferConvert::new(FlatBufferConvertConfig {
            source_format: Schema::FlatBuffer,
            target_format: Schema::Json,
            ..FlatBufferConvertConfig::default()
        })
        .unwrap();
        let bytes = vec![1, 2, 3, 4];
        let expected_base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);

        let result = converter
            .transform(&metadata(), message(Payload::FlatBuffer(bytes.clone())))
            .unwrap()
            .unwrap();

        let Payload::Json(simd_json::OwnedValue::Object(map)) = result.payload else {
            panic!("expected JSON object payload");
        };
        assert_eq!(
            map.get("buffer_size"),
            Some(&simd_json::OwnedValue::from(bytes.len()))
        );
        assert_eq!(
            map.get("raw_data_base64"),
            Some(&simd_json::OwnedValue::from(expected_base64))
        );
    }
}
