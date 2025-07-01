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

use iggy_connector_sdk::encoders::proto::{ProtoEncoderConfig, ProtoStreamEncoder};
use iggy_connector_sdk::{Payload, Schema, StreamEncoder};
use prost::Message;
use prost_types::Any;
use std::path::PathBuf;

#[test]
fn encode_should_succeed_given_json_payload_with_any_wrapper() {
    let encoder = ProtoStreamEncoder::default();

    let json_value = simd_json::json!({
        "user_id": 123,
        "name": "John Doe",
        "email": "john@example.com"
    });

    let result = encoder.encode(Payload::Json(json_value));

    assert!(result.is_ok());
    let encoded_bytes = result.unwrap();
    assert!(!encoded_bytes.is_empty());

    let decoded_any = Any::decode(encoded_bytes.as_slice());
    assert!(decoded_any.is_ok());

    let any = decoded_any.unwrap();
    assert!(any.type_url.contains("google.protobuf.StringValue"));
    assert!(!any.value.is_empty());
}

#[test]
fn encode_should_succeed_given_text_payload_with_metadata() {
    let encoder = ProtoStreamEncoder::default();

    let text_payload = Payload::Text("Hello, World!".to_string());
    let result = encoder.encode(text_payload);

    assert!(result.is_ok());
    let encoded_bytes = result.unwrap();

    let decoded_any = Any::decode(encoded_bytes.as_slice()).unwrap();
    let json_string = String::from_utf8(decoded_any.value).unwrap();
    let mut json_bytes = json_string.into_bytes();
    let json_value = simd_json::to_owned_value(&mut json_bytes).unwrap();

    if let simd_json::OwnedValue::Object(map) = json_value {
        assert!(map.contains_key("text"));
        assert!(map.contains_key("timestamp"));
        assert!(map.contains_key("encoding"));
    } else {
        panic!("Expected JSON object with metadata");
    }
}

#[test]
fn encode_should_succeed_given_raw_payload_as_bytes() {
    let encoder = ProtoStreamEncoder::default();

    let raw_data = b"Binary data here".to_vec();
    let result = encoder.encode(Payload::Raw(raw_data.clone()));

    assert!(result.is_ok());
    let encoded_bytes = result.unwrap();

    let decoded_any = Any::decode(encoded_bytes.as_slice()).unwrap();
    assert!(decoded_any.type_url.contains("google.protobuf.BytesValue"));
    assert_eq!(decoded_any.value, raw_data);
}

#[test]
fn encoder_should_return_proto_schema() {
    let encoder = ProtoStreamEncoder::default();
    assert_eq!(encoder.schema(), Schema::Proto);
}

// Integration Tests
#[test]
fn integration_should_encode_real_protobuf_message_with_schema() {
    let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        schema_path: Some(PathBuf::from("examples/user.proto")),
        message_type: Some("com.example.User".to_string()),
        use_any_wrapper: false,
        ..ProtoEncoderConfig::default()
    });

    let json_payload = Payload::Json(simd_json::json!({
        "id": 123,
        "name": "John Doe",
        "email": "john@example.com",
        "active": true
    }));

    let result = encoder.encode(json_payload);

    assert!(
        result.is_ok(),
        "Encoding should succeed with schema or fallback"
    );

    let encoded_bytes = result.unwrap();
    assert!(
        !encoded_bytes.is_empty(),
        "Encoded data should not be empty"
    );

    println!(
        "Successfully encoded {} bytes of protobuf data",
        encoded_bytes.len()
    );

    let data_str = String::from_utf8_lossy(&encoded_bytes);
    assert!(data_str.contains("John Doe"), "Should contain user name");
    assert!(
        data_str.contains("john@example.com"),
        "Should contain user email"
    );
}

#[test]
fn integration_should_handle_schema_loading_gracefully() {
    let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        schema_path: Some(PathBuf::from("nonexistent/schema.proto")),
        message_type: Some("com.example.NonExistent".to_string()),
        use_any_wrapper: true,
        ..ProtoEncoderConfig::default()
    });

    let json_payload = Payload::Json(simd_json::json!({
        "test": "data"
    }));

    let result = encoder.encode(json_payload);
    assert!(
        result.is_ok(),
        "Should fallback gracefully when schema loading fails"
    );
}
