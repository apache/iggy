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

use iggy_connector_sdk::decoders::proto::{ProtoConfig, ProtoStreamDecoder};
use iggy_connector_sdk::encoders::proto::{ProtoEncoderConfig, ProtoStreamEncoder};
use iggy_connector_sdk::transforms::{ProtoConvert, ProtoConvertConfig, Transform};
use iggy_connector_sdk::{Payload, Schema, StreamDecoder, StreamEncoder};
use prost::Message;
use prost_types::Any;
use std::collections::HashMap;
use std::path::PathBuf;

#[tokio::test]
async fn should_transform_with_real_schema_and_field_mapping() {
    let mut field_mappings = HashMap::new();
    field_mappings.insert("user_id".to_string(), "id".to_string());
    field_mappings.insert("full_name".to_string(), "name".to_string());

    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        schema_path: Some(PathBuf::from("examples/user.proto")),
        message_type: Some("com.example.User".to_string()),
        field_mappings: Some(field_mappings),
        ..ProtoConvertConfig::default()
    };

    let converter = ProtoConvert::new(config);
    let metadata = iggy_connector_sdk::TopicMetadata {
        stream: "test_stream".to_string(),
        topic: "test_topic".to_string(),
    };

    let input_message = iggy_connector_sdk::DecodedMessage {
        id: Some(1),
        offset: Some(0),
        checksum: Some(0),
        timestamp: Some(1642771200),
        origin_timestamp: Some(1642771200),
        headers: None,
        payload: Payload::Json(simd_json::json!({
            "user_id": 456,
            "full_name": "Jane Smith",
            "email": "jane@example.com",
            "active": true,
            "created_at": 1642771200,
            "tags": ["admin", "user"],
            "address": {
                "street": "456 Admin Ave",
                "city": "Admin City",
                "country": "USA",
                "postal_code": "12345"
            }
        })),
    };

    let result = converter.transform(&metadata, input_message);
    assert!(result.is_ok(), "Schema-based transform should succeed");

    if let Ok(Some(transformed)) = result {
        match transformed.payload {
            Payload::Proto(proto_text) => {
                assert!(proto_text.contains("id"), "Should contain mapped id field");
                assert!(
                    proto_text.contains("name"),
                    "Should contain mapped name field"
                );
                assert!(
                    proto_text.contains("Jane Smith"),
                    "Should contain user data"
                );
                println!("Schema-transformed proto: {proto_text}");
            }
            Payload::Raw(bytes) => {
                println!(
                    "Schema transform produced {} raw protobuf bytes",
                    bytes.len()
                );
                assert!(!bytes.is_empty(), "Raw bytes should not be empty");
            }
            other => panic!("Expected Proto or Raw payload, got: {other:?}"),
        }
    }
}

#[tokio::test]
async fn should_use_any_wrapper_as_fallback_when_no_schema() {
    let encoder_config = ProtoEncoderConfig {
        use_any_wrapper: true,
        ..ProtoEncoderConfig::default()
    };
    let encoder = ProtoStreamEncoder::new_with_config(encoder_config);

    let decoder_config = ProtoConfig {
        use_any_wrapper: true,
        ..ProtoConfig::default()
    };
    let decoder = ProtoStreamDecoder::new(decoder_config);

    let user_json = simd_json::json!({
        "id": 123,
        "name": "John Doe",
        "email": "john@example.com",
        "active": true,
        "created_at": 1642771200,
        "tags": ["developer", "rust"],
        "address": {
            "street": "123 Main St",
            "city": "San Francisco",
            "country": "USA",
            "postal_code": "94105"
        }
    });

    let encode_result = encoder.encode(Payload::Json(user_json.clone()));
    match &encode_result {
        Ok(bytes) => println!("Encoding succeeded: {} bytes", bytes.len()),
        Err(e) => println!("Encoding failed: {e:?}"),
    }
    assert!(encode_result.is_ok(), "Encoding should succeed");
    let encoded_bytes = encode_result.unwrap();
    assert!(
        !encoded_bytes.is_empty(),
        "Encoded data should not be empty"
    );

    let decode_result = decoder.decode(encoded_bytes);
    assert!(decode_result.is_ok(), "Decoding should succeed");

    match decode_result.unwrap() {
        Payload::Json(decoded_json) => {
            if let simd_json::OwnedValue::Object(map) = &decoded_json {
                println!(
                    "Decoded JSON: {}",
                    simd_json::to_string_pretty(&decoded_json).unwrap()
                );

                assert!(map.contains_key("type_url"));
                assert!(map.contains_key("value"));
            }
        }
        other => panic!("Expected JSON payload, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_fallback_to_any_wrapper_when_schema_file_missing() {
    let encoder_config = ProtoEncoderConfig {
        schema_path: Some(PathBuf::from("nonexistent/schema.proto")),
        message_type: Some("com.example.User".to_string()),
        use_any_wrapper: true,
        ..ProtoEncoderConfig::default()
    };
    let encoder = ProtoStreamEncoder::new_with_config(encoder_config);

    let test_data = simd_json::json!({
        "id": 123,
        "name": "Test User",
        "email": "test@example.com"
    });

    let encode_result = encoder.encode(Payload::Json(test_data));
    match &encode_result {
        Ok(bytes) => println!("Fallback encoding succeeded: {} bytes", bytes.len()),
        Err(e) => println!("Fallback encoding failed: {e:?}"),
    }
    assert!(encode_result.is_ok(), "Fallback encoding should succeed");
}

#[tokio::test]
async fn should_transform_json_to_proto_with_field_mappings() {
    let mut field_mappings = HashMap::new();
    field_mappings.insert("user_id".to_string(), "id".to_string());
    field_mappings.insert("full_name".to_string(), "name".to_string());

    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        field_mappings: Some(field_mappings),
        ..ProtoConvertConfig::default()
    };

    let converter = ProtoConvert::new(config);
    let metadata = iggy_connector_sdk::TopicMetadata {
        stream: "test_stream".to_string(),
        topic: "test_topic".to_string(),
    };

    let input_message = iggy_connector_sdk::DecodedMessage {
        id: Some(1),
        offset: Some(0),
        checksum: Some(0),
        timestamp: Some(1642771200),
        origin_timestamp: Some(1642771200),
        headers: None,
        payload: Payload::Json(simd_json::json!({
            "user_id": 456,
            "full_name": "Jane Smith",
            "email": "jane@example.com",
            "active": true
        })),
    };

    let result = converter.transform(&metadata, input_message);
    assert!(result.is_ok(), "Transform should succeed");

    if let Ok(Some(transformed)) = result {
        match transformed.payload {
            Payload::Proto(proto_text) => {
                assert!(proto_text.contains("id"), "Should contain mapped id field");
                assert!(
                    proto_text.contains("name"),
                    "Should contain mapped name field"
                );
                assert!(
                    proto_text.contains("Jane Smith"),
                    "Should contain user data"
                );
                println!("Transformed proto: {proto_text}");
            }
            Payload::Raw(_) => {
                println!("Transform produced raw protobuf bytes");
            }
            other => panic!("Expected Proto or Raw payload, got: {other:?}"),
        }
    }
}

#[tokio::test]
async fn should_encode_decode_any_wrapper_with_type_validation() {
    let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        use_any_wrapper: true,
        ..ProtoEncoderConfig::default()
    });

    let decoder = ProtoStreamDecoder::new(ProtoConfig {
        use_any_wrapper: true,
        ..ProtoConfig::default()
    });

    let test_data = simd_json::json!({
        "message": "Hello, protobuf world!",
        "timestamp": 1642771200,
        "metadata": {
            "source": "integration_test",
            "version": "1.0"
        }
    });

    let encoded = encoder.encode(Payload::Json(test_data.clone())).unwrap();
    assert!(!encoded.is_empty());

    let any_message = Any::decode(encoded.as_slice());
    assert!(any_message.is_ok(), "Should decode as valid Any message");

    let decoded = decoder.decode(encoded).unwrap();
    match decoded {
        Payload::Json(json_value) => {
            if let simd_json::OwnedValue::Object(map) = &json_value {
                assert!(map.contains_key("type_url"));
                assert!(map.contains_key("value"));
                println!(
                    "Any wrapper result: {}",
                    simd_json::to_string_pretty(&json_value).unwrap()
                );
            }
        }
        other => panic!("Expected JSON with Any wrapper, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_perform_json_to_proto_to_json_roundtrip() {
    let json_to_proto_config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        ..ProtoConvertConfig::default()
    };

    let proto_to_json_config = ProtoConvertConfig {
        source_format: Schema::Proto,
        target_format: Schema::Json,
        ..ProtoConvertConfig::default()
    };

    let json_to_proto = ProtoConvert::new(json_to_proto_config);
    let proto_to_json = ProtoConvert::new(proto_to_json_config);

    let metadata = iggy_connector_sdk::TopicMetadata {
        stream: "test_stream".to_string(),
        topic: "test_topic".to_string(),
    };

    let original_data = simd_json::json!({
        "id": 999,
        "name": "End-to-End Test User",
        "email": "e2e@test.com",
        "active": true,
        "created_at": 1642771200
    });

    let original_message = iggy_connector_sdk::DecodedMessage {
        id: Some(1),
        offset: Some(0),
        checksum: Some(0),
        timestamp: Some(1642771200),
        origin_timestamp: Some(1642771200),
        headers: None,
        payload: Payload::Json(original_data.clone()),
    };

    let proto_result = json_to_proto.transform(&metadata, original_message);
    assert!(
        proto_result.is_ok(),
        "JSON to Proto conversion should succeed"
    );

    let proto_message = proto_result.unwrap().unwrap();

    let json_result = proto_to_json.transform(&metadata, proto_message);
    assert!(
        json_result.is_ok(),
        "Proto to JSON conversion should succeed"
    );

    let final_message = json_result.unwrap().unwrap();

    match final_message.payload {
        Payload::Json(final_json) => {
            println!(
                "Original: {}",
                simd_json::to_string_pretty(&original_data).unwrap()
            );
            println!(
                "Final: {}",
                simd_json::to_string_pretty(&final_json).unwrap()
            );

            if let simd_json::OwnedValue::Object(final_map) = &final_json {
                if let simd_json::OwnedValue::Object(original_map) = &original_data {
                    for key in ["name", "email"] {
                        if let (Some(original_val), Some(final_val)) =
                            (original_map.get(key), final_map.get(key))
                        {
                            assert_eq!(original_val, final_val, "Field {key} should be preserved");
                        }
                    }
                }
            }
        }
        other => panic!("Expected final JSON payload, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_encode_complex_nested_data_with_any_wrapper() {
    let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        use_any_wrapper: true,
        ..ProtoEncoderConfig::default()
    });

    let complex_data = simd_json::json!({
        "users": [
            {
                "id": 1,
                "name": "User One",
                "email": "user1@example.com",
                "active": true,
                "created_at": 1642771200,
                "tags": ["admin", "developer"],
                "address": {
                    "street": "123 Admin St",
                    "city": "Admin City",
                    "country": "USA",
                    "postal_code": "12345"
                }
            },
            {
                "id": 2,
                "name": "User Two",
                "email": "user2@example.com",
                "active": false,
                "created_at": 1642857600,
                "tags": ["user"],
                "address": {
                    "street": "456 User Ave",
                    "city": "User Town",
                    "country": "USA",
                    "postal_code": "67890"
                }
            }
        ],
        "total_count": 2
    });

    let result = encoder.encode(Payload::Json(complex_data));
    match &result {
        Ok(bytes) => println!("Complex encoding succeeded: {} bytes", bytes.len()),
        Err(e) => println!("Complex encoding failed: {e:?}"),
    }
    assert!(
        result.is_ok(),
        "Complex nested message encoding should succeed"
    );

    let encoded_bytes = result.unwrap();
    assert!(!encoded_bytes.is_empty());

    println!(
        "Successfully encoded complex nested message: {} bytes",
        encoded_bytes.len()
    );
}
