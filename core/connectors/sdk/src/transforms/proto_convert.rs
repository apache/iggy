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

use base64::Engine;
use prost::Message;
use prost_types::Any;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use tracing::error;

use crate::{DecodedMessage, Error, Payload, TopicMetadata};

use super::{Transform, TransformType};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtoConvertConfig {
    pub target_format: ConvertFormat,
    pub preserve_structure: bool,
    pub field_mappings: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConvertFormat {
    Json,
    Proto,
    Text,
}

pub struct ProtoConvert {
    config: ProtoConvertConfig,
}

impl ProtoConvert {
    pub fn new(config: ProtoConvertConfig) -> Self {
        Self { config }
    }

    fn convert_proto_to_json(&self, payload: &[u8]) -> Result<Payload, Error> {
        match Any::decode(payload) {
            Ok(any) => {
                let json_value = simd_json::json!({
                    "type_url": any.type_url,
                    "value": base64::engine::general_purpose::STANDARD.encode(&any.value),
                    "converted_at": chrono::Utc::now().to_rfc3339(),
                });
                Ok(Payload::Json(json_value))
            }
            Err(e) => {
                error!("Failed to decode protobuf message: {e}");
                Err(Error::InvalidProtobufPayload)
            }
        }
    }

    fn convert_json_to_proto(&self, json_value: &OwnedValue) -> Result<Payload, Error> {
        let json_string =
            simd_json::to_string(json_value).map_err(|_| Error::InvalidJsonPayload)?;

        let any = Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
            value: json_string.into_bytes(),
        };

        let proto_bytes = any.encode_to_vec();
        Ok(Payload::Raw(proto_bytes))
    }

    fn convert_to_text(&self, payload: &Payload) -> Result<Payload, Error> {
        match payload {
            Payload::Json(value) => {
                let text =
                    simd_json::to_string_pretty(value).map_err(|_| Error::InvalidJsonPayload)?;
                Ok(Payload::Text(text))
            }
            Payload::Proto(text) => Ok(Payload::Text(text.clone())),
            Payload::Raw(data) => match self.convert_proto_to_json(data) {
                Ok(Payload::Json(value)) => {
                    let text = simd_json::to_string_pretty(&value)
                        .map_err(|_| Error::InvalidJsonPayload)?;
                    Ok(Payload::Text(text))
                }
                _ => Ok(Payload::Text(
                    "Raw bytes: ".to_string() + &data.len().to_string() + " bytes",
                )),
            },
            Payload::Text(text) => Ok(Payload::Text(text.clone())),
        }
    }
}

impl Transform for ProtoConvert {
    fn r#type(&self) -> TransformType {
        TransformType::ProtoConvert
    }

    fn transform(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let new_payload = match self.config.target_format {
            ConvertFormat::Json => match &message.payload {
                Payload::Proto(_) | Payload::Raw(_) => match &message.payload {
                    Payload::Raw(data) => self.convert_proto_to_json(data)?,
                    Payload::Proto(text) => {
                        let data = text.as_bytes();
                        self.convert_proto_to_json(data)?
                    }
                    _ => return Ok(Some(message)),
                },
                _ => return Ok(Some(message)),
            },
            ConvertFormat::Proto => match &message.payload {
                Payload::Json(value) => self.convert_json_to_proto(value)?,
                _ => return Ok(Some(message)),
            },
            ConvertFormat::Text => self.convert_to_text(&message.payload)?,
        };

        message.payload = new_payload;
        Ok(Some(message))
    }
}
