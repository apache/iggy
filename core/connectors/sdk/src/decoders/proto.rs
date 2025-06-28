/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
use prost::Message;
use prost_types::Any;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoConfig {
    pub schema_path: Option<String>,
    pub message_type: Option<String>,
    pub use_any_wrapper: bool,
    pub field_mappings: Option<HashMap<String, String>>,
}

impl Default for ProtoConfig {
    fn default() -> Self {
        Self {
            schema_path: None,
            message_type: None,
            use_any_wrapper: true,
            field_mappings: None,
        }
    }
}

pub struct ProtoStreamDecoder {
    config: ProtoConfig,
}

impl ProtoStreamDecoder {
    pub fn new(config: ProtoConfig) -> Self {
        Self { config }
    }

    pub fn new_default() -> Self {
        Self::new(ProtoConfig::default())
    }

    fn decode_as_any(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        match Any::decode(payload.as_slice()) {
            Ok(any) => {
                let json_value = simd_json::json!({
                    "type_url": any.type_url,
                    "value": base64::engine::general_purpose::STANDARD.encode(&any.value),
                });
                Ok(Payload::Json(json_value))
            }
            Err(e) => {
                error!("Failed to decode protobuf Any message: {e}");
                Err(Error::CannotDecode(Schema::Proto))
            }
        }
    }

    fn decode_as_raw(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        Ok(Payload::Raw(payload))
    }
}

impl StreamDecoder for ProtoStreamDecoder {
    fn schema(&self) -> Schema {
        Schema::Proto
    }

    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        if payload.is_empty() {
            return Err(Error::InvalidPayloadType);
        }
        if self.config.use_any_wrapper {
            self.decode_as_any(payload)
        } else {
            self.decode_as_raw(payload)
        }
    }
}

impl Default for ProtoStreamDecoder {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::Any;

    #[test]
    fn decode_should_succeed_given_valid_protobuf_any_message() {
        let decoder = ProtoStreamDecoder::default();

        let any = Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
            value: b"Hello, World!".to_vec(),
        };

        let encoded = any.encode_to_vec();
        let result = decoder.decode(encoded);

        assert!(result.is_ok());
        if let Ok(Payload::Json(json_value)) = result {
            if let simd_json::OwnedValue::Object(map) = &json_value {
                assert!(map.contains_key("type_url"));
                assert!(map.contains_key("value"));
            } else {
                panic!("Expected JSON object");
            }
        } else {
            panic!("Expected JSON payload");
        }
    }

    #[test]
    fn decode_should_fail_given_empty_payload() {
        let decoder = ProtoStreamDecoder::default();
        let result = decoder.decode(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_should_return_raw_payload_when_any_wrapper_is_disabled() {
        let config = ProtoConfig {
            use_any_wrapper: false,
            ..ProtoConfig::default()
        };
        let decoder = ProtoStreamDecoder::new(config);

        let test_data = b"Hello, World!".to_vec();
        let result = decoder.decode(test_data.clone());

        assert!(result.is_ok());
        if let Ok(Payload::Raw(data)) = result {
            assert_eq!(data, test_data);
        } else {
            panic!("Expected Raw payload");
        }
    }
}
