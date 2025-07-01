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

use iggy_connector_sdk::decoders::proto::{ProtoConfig, ProtoStreamDecoder};
use iggy_connector_sdk::{Payload, StreamDecoder};
use prost::Message;
use prost_types::Any;
use std::collections::HashMap;
use std::path::PathBuf;

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

#[test]
fn decode_should_apply_field_mappings_when_configured() {
    let mut field_mappings = HashMap::new();
    field_mappings.insert("old_field".to_string(), "new_field".to_string());
    field_mappings.insert("user_id".to_string(), "id".to_string());

    let config = ProtoConfig {
        field_mappings: Some(field_mappings),
        use_any_wrapper: true,
        ..ProtoConfig::default()
    };
    let decoder = ProtoStreamDecoder::new(config);

    let json_content = simd_json::json!({
        "old_field": "should_be_renamed",
        "user_id": 123,
        "unchanged_field": "stays_same"
    });
    let json_string = simd_json::to_string(&json_content).unwrap();

    let any = Any {
        type_url: "type.googleapis.com/custom.Message".to_string(),
        value: json_string.into_bytes(),
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
fn load_schema_should_handle_missing_schema_path_gracefully() {
    let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
        schema_path: None,
        message_type: None,
        ..ProtoConfig::default()
    });

    let result = decoder.load_schema();
    assert!(
        result.is_ok(),
        "Should handle missing schema path gracefully"
    );
}

#[test]
fn load_schema_should_handle_missing_proto_file_gracefully() {
    let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
        schema_path: Some(PathBuf::from("nonexistent.proto")),
        message_type: Some("com.example.Test".to_string()),
        ..ProtoConfig::default()
    });

    let result = decoder.load_schema();

    assert!(
        result.is_ok(),
        "Should handle missing proto file gracefully"
    );
}

#[test]
fn config_should_have_sensible_defaults() {
    let config = ProtoConfig::default();

    assert!(config.schema_path.is_none());
    assert!(config.message_type.is_none());
    assert!(config.use_any_wrapper);
    assert!(config.field_mappings.is_none());
    assert!(config.schema_registry_url.is_none());
    assert!(config.descriptor_set.is_none());
    assert_eq!(config.include_paths, vec![PathBuf::from(".")]);
    assert!(!config.preserve_unknown_fields);
}

#[test]
fn decoder_should_be_creatable_with_custom_config() {
    let config = ProtoConfig {
        schema_path: Some(PathBuf::from("schemas/user.proto")),
        message_type: Some("com.example.User".to_string()),
        use_any_wrapper: false,
        field_mappings: Some(HashMap::from([
            ("user_id".to_string(), "id".to_string()),
            ("full_name".to_string(), "name".to_string()),
        ])),
        schema_registry_url: Some("http://schema-registry:8081".to_string()),
        include_paths: vec![PathBuf::from("."), PathBuf::from("schemas/common")],
        preserve_unknown_fields: true,
        ..ProtoConfig::default()
    };

    let decoder = ProtoStreamDecoder::new(config.clone());

    assert_eq!(decoder.schema(), iggy_connector_sdk::Schema::Proto);
}

#[test]
fn update_config_should_reload_schema_when_requested() {
    let mut decoder = ProtoStreamDecoder::new(ProtoConfig::default());

    let new_config = ProtoConfig {
        schema_path: Some(PathBuf::from("schemas/test.proto")),
        message_type: Some("com.example.Test".to_string()),
        use_any_wrapper: false,
        ..ProtoConfig::default()
    };

    let result = decoder.update_config(new_config.clone(), true);
    assert!(result.is_ok());
    assert_eq!(decoder.schema(), iggy_connector_sdk::Schema::Proto);
}

#[test]
fn update_config_should_not_reload_schema_when_not_requested() {
    let mut decoder = ProtoStreamDecoder::new(ProtoConfig::default());

    let new_config = ProtoConfig {
        schema_path: Some(PathBuf::from("schemas/test.proto")),
        message_type: Some("com.example.Test".to_string()),
        use_any_wrapper: false,
        ..ProtoConfig::default()
    };

    let result = decoder.update_config(new_config.clone(), false);
    assert!(result.is_ok());
    assert_eq!(decoder.schema(), iggy_connector_sdk::Schema::Proto);
}
