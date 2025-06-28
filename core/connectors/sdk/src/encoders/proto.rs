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
use prost::Message;
use prost_types::Any;

pub struct ProtoStreamEncoder;

impl ProtoStreamEncoder {
    pub fn new() -> Self {
        Self
    }

    fn encode_json_to_proto(&self, json_value: &simd_json::OwnedValue) -> Result<Vec<u8>, Error> {
        let json_string =
            simd_json::to_string(json_value).map_err(|_| Error::InvalidJsonPayload)?;

        let any = Any {
            type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
            value: json_string.into_bytes(),
        };

        Ok(any.encode_to_vec())
    }

    fn encode_text_to_proto(&self, text: &str) -> Result<Vec<u8>, Error> {
        let json_value = simd_json::json!({
            "text": text,
            "timestamp": chrono::Utc::now().timestamp_millis(),
        });
        self.encode_json_to_proto(&json_value)
    }

    fn encode_raw_to_proto(&self, raw_data: &[u8]) -> Result<Vec<u8>, Error> {
        let any = Any {
            type_url: "type.googleapis.com/google.protobuf.BytesValue".to_string(),
            value: raw_data.to_vec(),
        };

        Ok(any.encode_to_vec())
    }
}

impl StreamEncoder for ProtoStreamEncoder {
    fn schema(&self) -> Schema {
        Schema::Proto
    }

    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error> {
        match payload {
            Payload::Json(value) => self.encode_json_to_proto(&value),
            Payload::Text(text) => self.encode_text_to_proto(&text),
            Payload::Raw(data) => self.encode_raw_to_proto(&data),
            Payload::Proto(_) => Err(Error::InvalidPayloadType),
        }
    }
}

impl Default for ProtoStreamEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::Any;

    #[test]
    fn encode_should_succeed_given_json_payload() {
        let encoder = ProtoStreamEncoder::new();

        let json_value = simd_json::json!({
            "message": "Hello, World!",
            "number": 42
        });

        let payload = Payload::Json(json_value);
        let result = encoder.encode(payload);

        assert!(result.is_ok());
        let encoded = result.unwrap();

        let decoded = Any::decode(encoded.as_slice());
        assert!(decoded.is_ok());
    }

    #[test]
    fn encode_should_succeed_given_text_payload() {
        let encoder = ProtoStreamEncoder::new();

        let payload = Payload::Text("Hello, World!".to_string());
        let result = encoder.encode(payload);

        assert!(result.is_ok());
        let encoded = result.unwrap();

        let decoded = Any::decode(encoded.as_slice());
        assert!(decoded.is_ok());
    }

    #[test]
    fn encode_should_succeed_given_raw_payload() {
        let encoder = ProtoStreamEncoder::new();

        let payload = Payload::Raw(b"Hello, World!".to_vec());
        let result = encoder.encode(payload);

        assert!(result.is_ok());
        let encoded = result.unwrap();

        let decoded = Any::decode(encoded.as_slice());
        assert!(decoded.is_ok());
    }

    #[test]
    fn encode_should_fail_given_proto_payload() {
        let encoder = ProtoStreamEncoder::new();

        let payload = Payload::Proto("Hello, World!".to_string());
        let result = encoder.encode(payload);

        assert!(result.is_err());
    }
}
