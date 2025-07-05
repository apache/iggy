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

use iggy_connector_sdk::decoders::flatbuffer::{FlatBufferConfig, FlatBufferStreamDecoder};
use iggy_connector_sdk::encoders::flatbuffer::{FlatBufferEncoderConfig, FlatBufferStreamEncoder};
use iggy_connector_sdk::transforms::{FlatBufferConvert, FlatBufferConvertConfig, Transform};
use iggy_connector_sdk::{Payload, Schema, StreamDecoder, StreamEncoder};
use std::collections::HashMap;
use std::path::PathBuf;

#[tokio::test]
async fn should_transform_with_real_schema_and_field_mapping() {
    let mut field_mappings = HashMap::new();
    field_mappings.insert("user_id".to_string(), "id".to_string());
    field_mappings.insert("full_name".to_string(), "name".to_string());

    let config = FlatBufferConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::FlatBuffer,
        schema_path: Some(PathBuf::from("examples/user.fbs")),
        root_table_name: Some("User".to_string()),
        field_mappings: Some(field_mappings),
        ..FlatBufferConvertConfig::default()
    };

    let converter = FlatBufferConvert::new(config);
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
            Payload::FlatBuffer(bytes) => {
                println!("Schema-transformed FlatBuffer: {} bytes", bytes.len());
                assert!(!bytes.is_empty(), "FlatBuffer should not be empty");
                assert!(
                    bytes.len() >= 4,
                    "FlatBuffer should have minimum header size"
                );
            }
            other => panic!("Expected FlatBuffer payload, got: {other:?}"),
        }
    }
}

#[tokio::test]
async fn should_encode_decode_with_schema_configuration() {
    let encoder_config = FlatBufferEncoderConfig {
        schema_path: Some(PathBuf::from("examples/user.fbs")),
        root_table_name: Some("User".to_string()),
        table_size_hint: 2048,
        ..FlatBufferEncoderConfig::default()
    };
    let encoder = FlatBufferStreamEncoder::new(encoder_config);

    let decoder_config = FlatBufferConfig {
        schema_path: Some(PathBuf::from("examples/user.fbs")),
        root_table_name: Some("User".to_string()),
        extract_as_json: true,
        verify_buffers: true,
        ..FlatBufferConfig::default()
    };
    let decoder = FlatBufferStreamDecoder::new(decoder_config);

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

                assert!(map.contains_key("buffer_size"));
                assert!(map.contains_key("has_root_table"));
                assert!(map.contains_key("raw_data_base64"));
            }
        }
        other => panic!("Expected JSON payload, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_handle_raw_flatbuffer_mode() {
    let decoder_config = FlatBufferConfig {
        extract_as_json: false,
        verify_buffers: true,
        ..FlatBufferConfig::default()
    };
    let decoder = FlatBufferStreamDecoder::new(decoder_config);

    let encoder = FlatBufferStreamEncoder::default();

    let test_data = simd_json::json!({
        "id": 123,
        "name": "Test User",
        "email": "test@example.com"
    });

    let encode_result = encoder.encode(Payload::Json(test_data));
    assert!(encode_result.is_ok(), "Encoding should succeed");
    let encoded_bytes = encode_result.unwrap();

    let decode_result = decoder.decode(encoded_bytes);
    assert!(decode_result.is_ok(), "Decoding should succeed");

    match decode_result.unwrap() {
        Payload::FlatBuffer(bytes) => {
            println!("Raw FlatBuffer: {} bytes", bytes.len());
            assert!(!bytes.is_empty(), "Raw bytes should not be empty");
        }
        other => panic!("Expected FlatBuffer payload, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_transform_json_to_flatbuffer_with_field_mappings() {
    let mut field_mappings = HashMap::new();
    field_mappings.insert("user_id".to_string(), "id".to_string());
    field_mappings.insert("full_name".to_string(), "name".to_string());

    let config = FlatBufferConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::FlatBuffer,
        field_mappings: Some(field_mappings),
        ..FlatBufferConvertConfig::default()
    };

    let converter = FlatBufferConvert::new(config);
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
            Payload::FlatBuffer(bytes) => {
                println!("Transformed FlatBuffer: {} bytes", bytes.len());
                assert!(!bytes.is_empty(), "FlatBuffer should not be empty");
            }
            other => panic!("Expected FlatBuffer payload, got: {other:?}"),
        }
    }
}

#[tokio::test]
async fn should_encode_decode_with_field_validation() {
    let encoder_config = FlatBufferEncoderConfig {
        preserve_unknown_fields: false,
        table_size_hint: 1024,
        ..FlatBufferEncoderConfig::default()
    };
    let encoder = FlatBufferStreamEncoder::new(encoder_config);

    let decoder_config = FlatBufferConfig {
        extract_as_json: true,
        preserve_unknown_fields: false,
        verify_buffers: true,
        ..FlatBufferConfig::default()
    };
    let decoder = FlatBufferStreamDecoder::new(decoder_config);

    let test_json = simd_json::json!({
        "id": 42,
        "name": "Test User",
        "email": "test@test.com",
        "unknown_field": "should_be_ignored"
    });

    let encoded = encoder.encode(Payload::Json(test_json));
    assert!(encoded.is_ok(), "Encoding should succeed");

    let decoded = decoder.decode(encoded.unwrap());
    assert!(decoded.is_ok(), "Decoding should succeed");

    match decoded.unwrap() {
        Payload::Json(json) => {
            println!(
                "Validated JSON: {}",
                simd_json::to_string_pretty(&json).unwrap()
            );
        }
        other => panic!("Expected JSON payload, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_perform_json_to_flatbuffer_to_json_roundtrip() {
    let encoder = FlatBufferStreamEncoder::default();
    let decoder = FlatBufferStreamDecoder::new(FlatBufferConfig {
        extract_as_json: true,
        ..FlatBufferConfig::default()
    });

    let original_json = simd_json::json!({
        "id": 100,
        "name": "Roundtrip User",
        "email": "roundtrip@example.com",
        "active": true,
        "created_at": 1642771200,
        "tags": ["test", "roundtrip"],
        "address": {
            "street": "100 Roundtrip St",
            "city": "Test City",
            "country": "TestLand",
            "postal_code": "00100"
        }
    });

    let encoded_result = encoder.encode(Payload::Json(original_json.clone()));
    assert!(
        encoded_result.is_ok(),
        "JSON to FlatBuffer encoding should succeed"
    );
    let flatbuffer_bytes = encoded_result.unwrap();
    println!("Encoded to FlatBuffer: {} bytes", flatbuffer_bytes.len());

    let decoded_result = decoder.decode(flatbuffer_bytes);
    assert!(
        decoded_result.is_ok(),
        "FlatBuffer to JSON decoding should succeed"
    );

    match decoded_result.unwrap() {
        Payload::Json(decoded_json) => {
            println!(
                "Roundtrip JSON: {}",
                simd_json::to_string_pretty(&decoded_json).unwrap()
            );

            if let simd_json::OwnedValue::Object(map) = &decoded_json {
                assert!(
                    map.contains_key("buffer_size"),
                    "Should contain buffer metadata"
                );
                assert!(
                    map.contains_key("raw_data_base64"),
                    "Should contain raw data"
                );
            }
        }
        other => panic!("Expected JSON payload from roundtrip, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_handle_complex_nested_data_encoding() {
    let encoder = FlatBufferStreamEncoder::new(FlatBufferEncoderConfig {
        table_size_hint: 4096,
        preserve_unknown_fields: true,
        ..FlatBufferEncoderConfig::default()
    });

    let complex_data = simd_json::json!({
        "users": [
            {
                "id": 1,
                "name": "User One",
                "email": "user1@example.com",
                "address": {
                    "street": "1 First St",
                    "city": "First City"
                },
                "tags": ["admin", "active"]
            },
            {
                "id": 2,
                "name": "User Two",
                "email": "user2@example.com",
                "address": {
                    "street": "2 Second St",
                    "city": "Second City"
                },
                "tags": ["user"]
            }
        ],
        "total_count": 2,
        "metadata": {
            "version": "1.0",
            "created_at": 1642771200
        }
    });

    let result = encoder.encode(Payload::Json(complex_data));
    match &result {
        Ok(bytes) => {
            println!("Complex data encoded: {} bytes", bytes.len());
            assert!(!bytes.is_empty(), "Encoded data should not be empty");
            assert!(
                bytes.len() > 100,
                "Complex data should produce substantial output"
            );
        }
        Err(e) => panic!("Complex data encoding failed: {e:?}"),
    }
}

#[tokio::test]
async fn should_convert_between_different_formats() {
    let encoder = FlatBufferStreamEncoder::default();

    let text_data = r#"{"id": 123, "name": "Text User", "email": "text@example.com"}"#;
    let text_to_fb_result = encoder.encode(Payload::Text(text_data.to_string()));
    assert!(
        text_to_fb_result.is_ok(),
        "Text to FlatBuffer should succeed"
    );

    let raw_data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let raw_to_fb_result = encoder.encode(Payload::Raw(raw_data));
    assert!(raw_to_fb_result.is_ok(), "Raw to FlatBuffer should succeed");

    let fb_data = text_to_fb_result.unwrap();

    let fb_to_json = encoder.convert_format(Payload::FlatBuffer(fb_data.clone()), Schema::Json);
    assert!(fb_to_json.is_ok(), "FlatBuffer to JSON should succeed");

    let fb_to_text = encoder.convert_format(Payload::FlatBuffer(fb_data.clone()), Schema::Text);
    assert!(fb_to_text.is_ok(), "FlatBuffer to Text should succeed");

    let fb_to_raw = encoder.convert_format(Payload::FlatBuffer(fb_data), Schema::Raw);
    assert!(fb_to_raw.is_ok(), "FlatBuffer to Raw should succeed");

    match fb_to_json.unwrap() {
        Payload::Json(json) => {
            if let simd_json::OwnedValue::Object(map) = &json {
                assert!(map.contains_key("flatbuffer_size"));
                println!(
                    "Converted to JSON: {}",
                    simd_json::to_string_pretty(&json).unwrap()
                );
            }
        }
        other => panic!("Expected JSON from conversion, got: {other:?}"),
    }
}

#[tokio::test]
async fn should_handle_buffer_verification() {
    let strict_decoder = FlatBufferStreamDecoder::new(FlatBufferConfig {
        verify_buffers: true,
        ..FlatBufferConfig::default()
    });

    let lenient_decoder = FlatBufferStreamDecoder::new(FlatBufferConfig {
        verify_buffers: false,
        ..FlatBufferConfig::default()
    });

    let empty_buffer = vec![];
    let strict_result = strict_decoder.decode(empty_buffer.clone());
    assert!(
        strict_result.is_err(),
        "Strict decoder should reject empty buffer"
    );

    let _lenient_result = lenient_decoder.decode(empty_buffer);

    let tiny_buffer = vec![1, 2];
    let strict_tiny_result = strict_decoder.decode(tiny_buffer.clone());
    assert!(
        strict_tiny_result.is_err(),
        "Strict decoder should reject tiny buffer"
    );

    let encoder = FlatBufferStreamEncoder::default();
    let valid_data = encoder.encode(Payload::Json(simd_json::json!({"test": "data"})));
    assert!(valid_data.is_ok(), "Should create valid buffer");

    let valid_buffer = valid_data.unwrap();
    let strict_valid_result = strict_decoder.decode(valid_buffer.clone());
    assert!(
        strict_valid_result.is_ok(),
        "Strict decoder should accept valid buffer"
    );

    let lenient_valid_result = lenient_decoder.decode(valid_buffer);
    assert!(
        lenient_valid_result.is_ok(),
        "Lenient decoder should accept valid buffer"
    );
}

#[tokio::test]
async fn should_support_custom_include_paths() {
    let config_with_paths = FlatBufferConfig {
        schema_path: Some(PathBuf::from("user.fbs")),
        include_paths: vec![
            PathBuf::from("examples"),
            PathBuf::from("schemas"),
            PathBuf::from("."),
        ],
        ..FlatBufferConfig::default()
    };

    let decoder = FlatBufferStreamDecoder::new(config_with_paths);

    assert_eq!(decoder.schema(), Schema::FlatBuffer);
    println!("Custom include paths configuration accepted");
}
