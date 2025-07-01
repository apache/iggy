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

use iggy_connector_sdk::transforms::proto_convert::{
    ConversionOptions, ProtoConvert, ProtoConvertConfig,
};
use iggy_connector_sdk::transforms::{Transform, TransformType};
use iggy_connector_sdk::{DecodedMessage, Payload, Schema, TopicMetadata};
use std::collections::HashMap;
use std::path::PathBuf;

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

#[test]
fn transform_should_convert_protobuf_to_json_successfully() {
    let config = ProtoConvertConfig {
        source_format: Schema::Proto,
        target_format: Schema::Json,
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let proto_payload = Payload::Proto(r#"{"name": "John", "age": 30}"#.to_string());
    let message = create_test_message(proto_payload);

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
fn transform_should_convert_json_to_protobuf_successfully() {
    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let json_payload = Payload::Json(simd_json::json!({
        "user_id": 123,
        "name": "John Doe",
        "email": "john@example.com"
    }));
    let message = create_test_message(json_payload);

    let result = converter.transform(&metadata, message);

    assert!(result.is_ok());
    if let Ok(Some(transformed_message)) = result {
        if let Payload::Proto(proto_text) = transformed_message.payload {
            assert!(proto_text.contains("user_id"));
            assert!(proto_text.contains("John Doe"));
            assert!(proto_text.contains("john@example.com"));
        } else {
            panic!("Expected Proto payload");
        }
    } else {
        panic!("Expected transformed message");
    }
}

#[test]
fn transform_should_apply_field_mappings_during_conversion() {
    let mut field_mappings = HashMap::new();
    field_mappings.insert("user_id".to_string(), "id".to_string());
    field_mappings.insert("full_name".to_string(), "name".to_string());

    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Json,
        field_mappings: Some(field_mappings),
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let json_payload = Payload::Json(simd_json::json!({
        "user_id": 123,
        "full_name": "John Doe",
        "email": "john@example.com"
    }));
    let message = create_test_message(json_payload);

    let result = converter.transform(&metadata, message);

    assert!(result.is_ok());
    if let Ok(Some(transformed_message)) = result {
        if let Payload::Json(json_value) = transformed_message.payload {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("id"));
                assert!(map.contains_key("name"));
                assert!(map.contains_key("email"));
                assert!(!map.contains_key("user_id"));
                assert!(!map.contains_key("full_name"));
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
fn transform_should_include_metadata_when_configured() {
    let config = ProtoConvertConfig {
        source_format: Schema::Proto,
        target_format: Schema::Json,
        conversion_options: ConversionOptions {
            include_metadata: true,
            ..ConversionOptions::default()
        },
        message_type: Some("com.example.User".to_string()),
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let proto_payload = Payload::Proto(r#"{"name": "John"}"#.to_string());
    let message = create_test_message(proto_payload);

    let result = converter.transform(&metadata, message);

    assert!(result.is_ok());
    if let Ok(Some(transformed_message)) = result {
        if let Payload::Json(json_value) = transformed_message.payload {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("data"));
                assert!(map.contains_key("metadata"));

                if let Some(simd_json::OwnedValue::Object(metadata)) = map.get("metadata") {
                    assert!(metadata.contains_key("converted_from"));
                    assert!(metadata.contains_key("timestamp"));
                    assert!(metadata.contains_key("message_type"));
                }
            } else {
                panic!("Expected JSON object with metadata");
            }
        } else {
            panic!("Expected JSON payload");
        }
    } else {
        panic!("Expected transformed message");
    }
}

#[test]
fn transform_should_use_pretty_json_when_configured() {
    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Text,
        conversion_options: ConversionOptions {
            pretty_json: true,
            ..ConversionOptions::default()
        },
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let json_payload = Payload::Json(simd_json::json!({
        "name": "John",
        "age": 30
    }));
    let message = create_test_message(json_payload);

    let result = converter.transform(&metadata, message);

    assert!(result.is_ok());
    if let Ok(Some(transformed_message)) = result {
        if let Payload::Text(text) = transformed_message.payload {
            assert!(text.contains('\n'));
            assert!(text.contains("  "));
        } else {
            panic!("Expected Text payload");
        }
    } else {
        panic!("Expected transformed message");
    }
}

#[test]
fn transform_should_handle_raw_protobuf_any_message() {
    let config = ProtoConvertConfig {
        source_format: Schema::Proto,
        target_format: Schema::Json,
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let any = prost_types::Any {
        type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
        value: b"Hello, World!".to_vec(),
    };
    let encoded_any = prost::Message::encode_to_vec(&any);

    let proto_payload = Payload::Raw(encoded_any);
    let message = create_test_message(proto_payload);

    let result = converter.transform(&metadata, message);

    assert!(result.is_ok());
    if let Ok(Some(transformed_message)) = result {
        if let Payload::Json(json_value) = transformed_message.payload {
            if let simd_json::OwnedValue::Object(map) = json_value {
                assert!(map.contains_key("type_url"));
                assert!(map.contains_key("value"));
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
fn transform_should_convert_between_json_and_text_directly() {
    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Text,
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let json_payload = Payload::Json(simd_json::json!({
        "message": "test",
        "number": 42
    }));
    let message = create_test_message(json_payload);

    let result = converter.transform(&metadata, message);

    assert!(result.is_ok());
    if let Ok(Some(transformed_message)) = result {
        if let Payload::Text(text) = transformed_message.payload {
            assert!(text.contains("message"));
            assert!(text.contains("test"));
            assert!(text.contains("42"));
        } else {
            panic!("Expected Text payload");
        }
    } else {
        panic!("Expected transformed message");
    }
}

#[test]
fn transform_should_convert_between_raw_and_json_directly() {
    let config = ProtoConvertConfig {
        source_format: Schema::Raw,
        target_format: Schema::Json,
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let json_string = r#"{"name": "John", "age": 30}"#;
    let raw_payload = Payload::Raw(json_string.as_bytes().to_vec());
    let message = create_test_message(raw_payload);

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
fn transform_should_handle_same_format_conversion_gracefully() {
    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Json,
        ..ProtoConvertConfig::default()
    };
    let converter = ProtoConvert::new(config);
    let metadata = create_test_metadata();

    let json_payload = Payload::Json(simd_json::json!({"test": "data"}));
    let message = create_test_message(json_payload.clone());

    let result = converter.transform(&metadata, message);

    assert!(result.is_ok());
    if let Ok(Some(transformed_message)) = result {
        if let Payload::Json(result_json) = transformed_message.payload {
            assert_eq!(result_json, simd_json::json!({"test": "data"}));
        } else {
            panic!("Expected JSON payload");
        }
    } else {
        panic!("Expected transformed message");
    }
}

#[test]
fn load_schema_should_handle_missing_schema_path_gracefully() {
    let mut converter = ProtoConvert::new(ProtoConvertConfig {
        schema_path: None,
        message_type: None,
        ..ProtoConvertConfig::default()
    });

    let result = converter.load_schema();
    assert!(
        result.is_ok(),
        "Should handle missing schema path gracefully"
    );
}

#[test]
fn load_schema_should_log_warning_for_unimplemented_schema_compilation() {
    let mut converter = ProtoConvert::new(ProtoConvertConfig {
        schema_path: Some(PathBuf::from("test.proto")),
        message_type: Some("com.example.Test".to_string()),
        ..ProtoConvertConfig::default()
    });

    let result = converter.load_schema();

    assert!(
        result.is_ok(),
        "Should handle unimplemented schema compilation gracefully"
    );
}

#[test]
fn config_should_have_sensible_defaults() {
    let config = ProtoConvertConfig::default();

    assert_eq!(config.source_format, Schema::Proto);
    assert_eq!(config.target_format, Schema::Json);
    assert!(config.schema_path.is_none());
    assert!(config.message_type.is_none());
    assert!(config.field_mappings.is_none());
    assert!(config.schema_registry_url.is_none());
    assert!(config.descriptor_set.is_none());
    assert_eq!(config.include_paths, vec![PathBuf::from(".")]);
    assert!(!config.preserve_unknown_fields);

    let conv_opts = &config.conversion_options;
    assert!(conv_opts.validate_messages);
    assert!(!conv_opts.pretty_json);
    assert!(!conv_opts.include_metadata);
    assert_eq!(conv_opts.type_url_prefix, "type.googleapis.com");
    assert!(!conv_opts.strict_mode);
}

#[test]
fn converter_should_be_creatable_with_custom_config() {
    let config = ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        schema_path: Some(PathBuf::from("schemas/user.proto")),
        message_type: Some("com.example.User".to_string()),
        field_mappings: Some(HashMap::from([
            ("user_id".to_string(), "id".to_string()),
            ("full_name".to_string(), "name".to_string()),
        ])),
        schema_registry_url: Some("http://schema-registry:8081".to_string()),
        descriptor_set: None,
        include_paths: vec![PathBuf::from("."), PathBuf::from("schemas/common")],
        preserve_unknown_fields: true,
        conversion_options: ConversionOptions {
            validate_messages: false,
            pretty_json: true,
            include_metadata: true,
            type_url_prefix: "custom.example.com".to_string(),
            strict_mode: true,
        },
    };

    // Just test that the converter can be created with custom config
    let converter = ProtoConvert::new(config);

    // Test that it returns the correct transform type
    assert_eq!(converter.r#type(), TransformType::ProtoConvert);
}

#[test]
fn converter_should_return_correct_transform_type() {
    let converter = ProtoConvert::default();
    assert_eq!(converter.r#type(), TransformType::ProtoConvert);
}
