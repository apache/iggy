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
    pub fn new(config: FlatBufferConvertConfig) -> Self {
        Self { config }
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

impl Transform for FlatBufferConvert {
    fn r#type(&self) -> TransformType {
        TransformType::FlatBufferConvert
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
                if let Payload::FlatBuffer(bytes) = message.payload {
                    decoder.decode(bytes)?
                } else {
                    return Err(Error::InvalidPayloadType);
                }
            }
            (Schema::FlatBuffer, Schema::Text) => {
                let encoder = FlatBufferStreamEncoder::default();
                encoder.convert_format(message.payload, Schema::Text)?
            }
            (Schema::FlatBuffer, Schema::Raw) => {
                let encoder = FlatBufferStreamEncoder::default();
                encoder.convert_format(message.payload, Schema::Raw)?
            }
            _ => {
                // For unsupported conversions, pass through unchanged
                message.payload
            }
        };

        Ok(Some(message))
    }
}

impl Default for FlatBufferConvert {
    fn default() -> Self {
        Self::new(FlatBufferConvertConfig::default())
    }
}
