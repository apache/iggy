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

use base64::Engine;
use iggy_connector_sdk::decoders::proto::{ProtoConfig, ProtoStreamDecoder};
use iggy_connector_sdk::encoders::proto::{ProtoEncoderConfig, ProtoStreamEncoder};
use iggy_connector_sdk::transforms::{ProtoConvert, ProtoConvertConfig, Transform};
use iggy_connector_sdk::{Payload, Schema, StreamDecoder, StreamEncoder};
use prost::Message;
use prost_types::Any;
use std::collections::HashMap;
use std::path::PathBuf;

const INTEGER_SCHEMA: &str = r#"
        syntax = "proto3";
        message IntegerRecord {
            int32 int32_value = 1;
            int64 int64_value = 2;
            uint32 uint32_value = 3;
            uint64 uint64_value = 4;
            sint32 sint32_value = 5;
            sint64 sint64_value = 6;
            fixed32 fixed32_value = 7;
            fixed64 fixed64_value = 8;
            sfixed32 sfixed32_value = 9;
            sfixed64 sfixed64_value = 10;
        }
    "#;

#[derive(Clone, PartialEq, Message)]
struct ValueRecord {
    #[prost(float, tag = "1")]
    single: f32,
    #[prost(double, tag = "2")]
    double: f64,
    #[prost(bytes, tag = "3")]
    bytes: Vec<u8>,
    #[prost(message, optional, tag = "4")]
    nested: Option<String>,
}

#[test]
fn given_float_and_binary_fields_when_converting_should_match_prost_values() {
    let mut schema = protox_parse::parse(
        "values.proto",
        r#"
        syntax = "proto3";
        message Nested { string value = 1; }
        message ValueRecord {
            float single = 1;
            double double = 2;
            bytes bytes = 3;
            Nested nested = 4;
        }
    "#,
    )
    .expect("parse scalar and binary schema");
    let record_schema = schema
        .message_type
        .iter_mut()
        .find(|message| message.name() == "ValueRecord")
        .expect("find record schema");
    let nested_field = record_schema
        .field
        .iter_mut()
        .find(|field| field.name() == "nested")
        .expect("find nested message field");
    nested_field.r#type = Some(prost_types::field_descriptor_proto::Type::Message as i32);
    nested_field.type_name = Some(".Nested".to_string());
    let converter = ProtoConvert::new(ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        descriptor_set: Some(prost_types::FileDescriptorSet { file: vec![schema] }.encode_to_vec()),
        message_type: Some("ValueRecord".to_string()),
        ..ProtoConvertConfig::default()
    });
    for (single, double) in [(1.25, -2.5), (f32::MIN, f64::MIN), (f32::MAX, f64::MAX)] {
        let expected = ValueRecord {
            single,
            double,
            bytes: vec![0, 1, 255],
            nested: Some("nested".to_string()),
        };
        let converted = converter.transform(
            &iggy_connector_sdk::TopicMetadata { stream: "values".to_string(), topic: "values".to_string() },
            iggy_connector_sdk::DecodedMessage {
                id: None, offset: None, checksum: None, timestamp: None,
                origin_timestamp: None, headers: None,
                payload: Payload::Json(simd_json::json!({
                    "single": single, "double": double,
                    "bytes": base64::engine::general_purpose::STANDARD.encode(&expected.bytes),
                    "nested": base64::engine::general_purpose::STANDARD.encode("nested".to_string().encode_to_vec()),
                })),
            },
        ).expect("convert scalar and binary record").expect("preserve record");
        let Payload::Raw(encoded) = converted.payload else {
            panic!("expected schema-encoded protobuf bytes");
        };
        let decoded =
            ValueRecord::decode(encoded.as_slice()).expect("decode converted values with prost");
        assert_eq!(decoded, expected);
    }
}

#[derive(Clone, PartialEq, Message)]
struct IntegerRecord {
    #[prost(int32, tag = "1")]
    int32_value: i32,
    #[prost(int64, tag = "2")]
    int64_value: i64,
    #[prost(uint32, tag = "3")]
    uint32_value: u32,
    #[prost(uint64, tag = "4")]
    uint64_value: u64,
    #[prost(sint32, tag = "5")]
    sint32_value: i32,
    #[prost(sint64, tag = "6")]
    sint64_value: i64,
    #[prost(fixed32, tag = "7")]
    fixed32_value: u32,
    #[prost(fixed64, tag = "8")]
    fixed64_value: u64,
    #[prost(sfixed32, tag = "9")]
    sfixed32_value: i32,
    #[prost(sfixed64, tag = "10")]
    sfixed64_value: i64,
}

#[test]
fn given_nested_message_when_encoding_should_resolve_qualified_name() {
    let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        descriptor_set: Some(nested_descriptor_set()),
        message_type: Some("example.Outer.Middle.Inner".to_string()),
        ..ProtoEncoderConfig::default()
    });
    let encoded = encoder
        .encode(Payload::Json(simd_json::json!({"label": "nested"})))
        .expect("encode nested message type");
    let record =
        FixedFieldRecord::decode(encoded.as_slice()).expect("decode nested record with prost");
    assert_eq!(record.label, "nested");
}

#[test]
fn given_nested_message_when_decoding_should_resolve_qualified_name() {
    let decoder = ProtoStreamDecoder::new(ProtoConfig {
        descriptor_set: Some(nested_descriptor_set()),
        message_type: Some("example.Outer.Middle.Inner".to_string()),
        ..ProtoConfig::default()
    });
    let record = FixedFieldRecord {
        label: "nested".to_string(),
        ..Default::default()
    };
    let Payload::Json(decoded) = decoder
        .decode(record.encode_to_vec())
        .expect("decode nested message type")
    else {
        panic!("expected nested message fields");
    };
    assert_eq!(decoded, simd_json::json!({"label": "nested"}));
}

#[test]
fn given_nested_message_when_converting_should_resolve_qualified_name() {
    let converter = ProtoConvert::new(ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        descriptor_set: Some(nested_descriptor_set()),
        message_type: Some("example.Outer.Middle.Inner".to_string()),
        ..ProtoConvertConfig::default()
    });
    let converted = converter
        .transform(
            &iggy_connector_sdk::TopicMetadata {
                stream: "nested".to_string(),
                topic: "nested".to_string(),
            },
            iggy_connector_sdk::DecodedMessage {
                id: None,
                offset: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                headers: None,
                payload: Payload::Json(simd_json::json!({"label": "nested"})),
            },
        )
        .expect("convert nested message type")
        .expect("preserve message");
    let Payload::Raw(encoded) = converted.payload else {
        panic!("expected schema-encoded nested message");
    };
    let record =
        FixedFieldRecord::decode(encoded.as_slice()).expect("decode nested record with prost");
    assert_eq!(record.label, "nested");
}

#[test]
fn given_loaded_encoder_when_config_changes_should_match_reload_setting() {
    for reload_schema in [false, true] {
        for schema_path in [
            None,
            Some(std::env::temp_dir().join(format!("{}.proto", uuid::Uuid::new_v4()))),
        ] {
            let mut encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
                descriptor_set: Some(integer_descriptor_set()),
                message_type: Some("IntegerRecord".to_string()),
                ..ProtoEncoderConfig::default()
            });
            encoder
                .update_config(
                    ProtoEncoderConfig {
                        schema_path,
                        ..ProtoEncoderConfig::default()
                    },
                    reload_schema,
                )
                .expect("update encoder configuration");
            let (record, json) = integer_cases()[2].clone();
            let encoded = encoder
                .encode(Payload::Json(json.clone()))
                .expect("encode record");
            if reload_schema {
                let wrapped = Any::decode(encoded.as_slice()).expect("decode fallback Any");
                let text = wrapped
                    .to_msg::<String>()
                    .expect("unpack fallback StringValue");
                let decoded: simd_json::OwnedValue =
                    simd_json::from_slice(&mut text.into_bytes()).expect("parse fallback JSON");
                assert_eq!(decoded, json);
            } else {
                assert_eq!(IntegerRecord::decode(encoded.as_slice()).unwrap(), record);
            }
        }
    }
}

#[test]
fn given_loaded_decoder_when_config_changes_should_match_reload_setting() {
    for reload_schema in [false, true] {
        for schema_path in [
            None,
            Some(std::env::temp_dir().join(format!("{}.proto", uuid::Uuid::new_v4()))),
        ] {
            let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
                descriptor_set: Some(integer_descriptor_set()),
                message_type: Some("IntegerRecord".to_string()),
                ..ProtoConfig::default()
            });
            decoder
                .update_config(
                    ProtoConfig {
                        schema_path,
                        use_any_wrapper: false,
                        ..ProtoConfig::default()
                    },
                    reload_schema,
                )
                .expect("update decoder configuration");
            let (record, json) = integer_cases()[2].clone();
            let encoded = record.encode_to_vec();
            let decoded = decoder.decode(encoded.clone()).expect("decode record");
            if reload_schema {
                let Payload::Raw(bytes) = decoded else {
                    panic!("expected configured raw fallback");
                };
                assert_eq!(bytes, encoded);
            } else {
                let Payload::Json(fields) = decoded else {
                    panic!("expected retained schema");
                };
                assert_eq!(fields, json);
            }
        }
    }
}

#[test]
fn given_loaded_encoder_when_update_fails_should_preserve_configuration() {
    let mut encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        descriptor_set: Some(integer_descriptor_set()),
        message_type: Some("IntegerRecord".to_string()),
        ..ProtoEncoderConfig::default()
    });
    assert!(
        encoder
            .update_config(
                ProtoEncoderConfig {
                    descriptor_set: Some(vec![0xff]),
                    field_mappings: Some(HashMap::from([(
                        "int32_value".to_string(),
                        "renamed".to_string()
                    )])),
                    ..ProtoEncoderConfig::default()
                },
                true
            )
            .is_err()
    );
    let (record, json) = integer_cases()[2].clone();
    let encoded = encoder
        .encode(Payload::Json(json))
        .expect("encode after failed update");
    assert_eq!(IntegerRecord::decode(encoded.as_slice()).unwrap(), record);
    encoder
        .load_schema()
        .expect("reload restored configuration");
}

#[test]
fn given_loaded_decoder_when_update_fails_should_preserve_configuration() {
    let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
        descriptor_set: Some(integer_descriptor_set()),
        message_type: Some("IntegerRecord".to_string()),
        ..ProtoConfig::default()
    });
    assert!(
        decoder
            .update_config(
                ProtoConfig {
                    descriptor_set: Some(vec![0xff]),
                    field_mappings: Some(HashMap::from([(
                        "int32_value".to_string(),
                        "renamed".to_string()
                    )])),
                    ..ProtoConfig::default()
                },
                true
            )
            .is_err()
    );
    let (record, json) = integer_cases()[2].clone();
    let Payload::Json(decoded) = decoder
        .decode(record.encode_to_vec())
        .expect("decode after failed update")
    else {
        panic!("expected retained schema");
    };
    assert_eq!(decoded, json);
    decoder
        .load_schema()
        .expect("reload restored configuration");
}

#[test]
fn given_loaded_schemas_when_file_changes_should_preserve_errors_and_clear_fallback() {
    let directory = std::env::temp_dir();
    let path = directory.join(format!("{}.proto", uuid::Uuid::new_v4()));
    std::fs::write(&path, INTEGER_SCHEMA).expect("write owned schema fixture");
    let mut encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        schema_path: Some(path.clone()),
        message_type: Some("IntegerRecord".to_string()),
        include_paths: vec![directory.clone()],
        ..ProtoEncoderConfig::default()
    });
    let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
        schema_path: Some(path.clone()),
        message_type: Some("IntegerRecord".to_string()),
        include_paths: vec![directory.clone()],
        use_any_wrapper: false,
        ..ProtoConfig::default()
    });
    let mut converter = ProtoConvert::new(ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        schema_path: Some(path.clone()),
        message_type: Some("IntegerRecord".to_string()),
        include_paths: vec![directory],
        ..ProtoConvertConfig::default()
    });
    std::fs::write(&path, "syntax = broken").expect("replace owned schema fixture");
    let encoder_error = encoder.load_schema();
    let decoder_error = decoder.load_schema();
    let converter_error = converter.load_schema();
    std::fs::remove_file(&path).expect("remove owned schema fixture");
    assert!(encoder_error.is_err());
    assert!(decoder_error.is_err());
    assert!(converter_error.is_err());

    let (record, json) = integer_cases()[2].clone();
    let convert = |converter: &ProtoConvert| {
        converter
            .transform(
                &iggy_connector_sdk::TopicMetadata {
                    stream: "integers".to_string(),
                    topic: "integers".to_string(),
                },
                iggy_connector_sdk::DecodedMessage {
                    id: None,
                    offset: None,
                    checksum: None,
                    timestamp: None,
                    origin_timestamp: None,
                    headers: None,
                    payload: Payload::Json(json.clone()),
                },
            )
            .expect("convert record")
            .expect("preserve record")
            .payload
    };
    let encoded = encoder
        .encode(Payload::Json(json.clone()))
        .expect("encode retained schema");
    assert_eq!(IntegerRecord::decode(encoded.as_slice()).unwrap(), record);
    let Payload::Json(decoded) = decoder
        .decode(record.encode_to_vec())
        .expect("decode retained schema")
    else {
        panic!("expected retained decoder schema");
    };
    assert_eq!(decoded, json);
    let Payload::Raw(converted) = convert(&converter) else {
        panic!("expected retained converter schema");
    };
    assert_eq!(IntegerRecord::decode(converted.as_slice()).unwrap(), record);

    encoder
        .load_schema()
        .expect("missing file selects fallback");
    decoder
        .load_schema()
        .expect("missing file selects fallback");
    converter
        .load_schema()
        .expect("missing file selects fallback");
    let encoded = encoder
        .encode(Payload::Json(json.clone()))
        .expect("encode fallback");
    let any = Any::decode(encoded.as_slice()).expect("decode fallback Any");
    let text = any.to_msg::<String>().expect("unpack fallback StringValue");
    let decoded: simd_json::OwnedValue = simd_json::from_slice(&mut text.into_bytes()).unwrap();
    assert_eq!(decoded, json);
    let Payload::Raw(decoded) = decoder
        .decode(record.encode_to_vec())
        .expect("decode fallback")
    else {
        panic!("expected decoder raw fallback");
    };
    assert_eq!(decoded, record.encode_to_vec());
    let Payload::Proto(text) = convert(&converter) else {
        panic!("expected converter text fallback");
    };
    let decoded: simd_json::OwnedValue = simd_json::from_slice(&mut text.into_bytes()).unwrap();
    assert_eq!(decoded, json);
}

#[test]
fn given_integer_schema_when_encoding_should_match_prost_values() {
    let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        descriptor_set: Some(integer_descriptor_set()),
        message_type: Some("IntegerRecord".to_string()),
        ..ProtoEncoderConfig::default()
    });
    for (expected, json) in integer_cases() {
        let encoded = encoder
            .encode(Payload::Json(json))
            .expect("encode integer record");
        let decoded =
            IntegerRecord::decode(encoded.as_slice()).expect("decode integer record with prost");
        assert_eq!(decoded, expected);
    }
}

#[test]
fn given_integer_schema_when_converting_should_match_prost_values() {
    let converter = ProtoConvert::new(ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        descriptor_set: Some(integer_descriptor_set()),
        message_type: Some("IntegerRecord".to_string()),
        ..ProtoConvertConfig::default()
    });
    let metadata = iggy_connector_sdk::TopicMetadata {
        stream: "integers".to_string(),
        topic: "integers".to_string(),
    };
    for (expected, json) in integer_cases() {
        let message = iggy_connector_sdk::DecodedMessage {
            id: None,
            offset: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload: Payload::Json(json),
        };
        let converted = converter
            .transform(&metadata, message)
            .expect("convert integer record")
            .expect("preserve message");
        let Payload::Raw(encoded) = converted.payload else {
            panic!("expected schema-encoded protobuf bytes");
        };
        let decoded = IntegerRecord::decode(encoded.as_slice())
            .expect("decode converted integer record with prost");
        assert_eq!(decoded, expected);
    }
}

#[test]
fn given_prost_integers_when_decoding_should_preserve_values() {
    let decoder = ProtoStreamDecoder::new(ProtoConfig {
        descriptor_set: Some(integer_descriptor_set()),
        message_type: Some("IntegerRecord".to_string()),
        ..ProtoConfig::default()
    });
    for (record, expected) in integer_cases() {
        let encoded = record.encode_to_vec();
        if encoded.is_empty() {
            assert!(matches!(
                decoder.decode(encoded),
                Err(iggy_connector_sdk::Error::InvalidPayloadType)
            ));
            continue;
        }
        let Payload::Json(decoded) = decoder
            .decode(encoded)
            .expect("decode prost integer record")
        else {
            panic!("expected JSON integer fields");
        };
        assert_eq!(decoded, expected);
    }
}

#[derive(Clone, PartialEq, Message)]
struct FixedFieldRecord {
    #[prost(fixed32, tag = "1")]
    narrow: u32,
    #[prost(fixed64, tag = "2")]
    wide: u64,
    #[prost(string, tag = "3")]
    label: String,
}

#[test]
fn given_unknown_fixed_fields_when_decoding_should_preserve_following_fields() {
    let record = FixedFieldRecord {
        narrow: 1,
        wide: u64::MAX,
        label: "after fixed fields".to_string(),
    };
    for preserve_unknown_fields in [false, true] {
        let decoder = fixed_field_decoder(preserve_unknown_fields);
        let Payload::Json(simd_json::OwnedValue::Object(decoded)) = decoder
            .decode(record.encode_to_vec())
            .expect("decode after unknown fixed fields")
        else {
            panic!("expected JSON object");
        };
        assert_eq!(decoded["label"], record.label);
        assert_eq!(decoded.len(), if preserve_unknown_fields { 3 } else { 1 });
        assert_eq!(
            decoded.contains_key("unknown_field_1"),
            preserve_unknown_fields
        );
        assert_eq!(
            decoded.contains_key("unknown_field_2"),
            preserve_unknown_fields
        );
    }
}

#[test]
fn given_truncated_fixed_fields_when_decoding_should_reject_payload() {
    for preserve_unknown_fields in [false, true] {
        let decoder = fixed_field_decoder(preserve_unknown_fields);
        for (tag, width) in [
            (1 << 3 | 5, size_of::<u32>()),
            (2 << 3 | 1, size_of::<u64>()),
        ] {
            for length in 0..width {
                let mut truncated = vec![tag];
                truncated.resize(1 + length, 0);
                assert!(
                    decoder.decode(truncated).is_err(),
                    "tag={tag}, length={length}, preserve={preserve_unknown_fields}"
                );
            }
        }
    }
}

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

            if let simd_json::OwnedValue::Object(final_map) = &final_json
                && let simd_json::OwnedValue::Object(original_map) = &original_data
            {
                for key in ["name", "email"] {
                    if let (Some(original_val), Some(final_val)) =
                        (original_map.get(key), final_map.get(key))
                    {
                        assert_eq!(original_val, final_val, "Field {key} should be preserved");
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

fn integer_descriptor_set() -> Vec<u8> {
    let schema =
        protox_parse::parse("integers.proto", INTEGER_SCHEMA).expect("parse integer schema");
    prost_types::FileDescriptorSet { file: vec![schema] }.encode_to_vec()
}

fn integer_cases() -> [(IntegerRecord, simd_json::OwnedValue); 5] {
    [
        (0, 0, 0, 0),
        (-1, -1, 1, 1),
        (150, 9_000_000_000, 150, 9_000_000_000),
        (i32::MIN, i64::MIN, u32::MAX, u64::MAX),
        (i32::MAX, i64::MAX, u32::MAX, u64::MAX),
    ]
    .map(|(signed32, signed64, unsigned32, unsigned64)| {
        let expected = IntegerRecord {
            int32_value: signed32,
            int64_value: signed64,
            uint32_value: unsigned32,
            uint64_value: unsigned64,
            sint32_value: signed32,
            sint64_value: signed64,
            fixed32_value: unsigned32,
            fixed64_value: unsigned64,
            sfixed32_value: signed32,
            sfixed64_value: signed64,
        };
        let json = simd_json::json!({
            "int32_value": signed32,
            "int64_value": signed64,
            "uint32_value": unsigned32,
            "uint64_value": unsigned64,
            "sint32_value": signed32,
            "sint64_value": signed64,
            "fixed32_value": unsigned32,
            "fixed64_value": unsigned64,
            "sfixed32_value": signed32,
            "sfixed64_value": signed64,
        });
        (expected, json)
    })
}

fn fixed_field_decoder(preserve_unknown_fields: bool) -> ProtoStreamDecoder {
    let schema = protox_parse::parse(
        "fixed.proto",
        r#"
        syntax = "proto3";
        message FixedFieldRecord { string label = 3; }
    "#,
    )
    .expect("parse schema without fixed fields");
    ProtoStreamDecoder::new(ProtoConfig {
        descriptor_set: Some(prost_types::FileDescriptorSet { file: vec![schema] }.encode_to_vec()),
        message_type: Some("FixedFieldRecord".to_string()),
        preserve_unknown_fields,
        ..ProtoConfig::default()
    })
}

fn nested_descriptor_set() -> Vec<u8> {
    let schema = protox_parse::parse(
        "nested.proto",
        r#"
        syntax = "proto3";
        package example;
        message Outer {
            message Middle {
                message Inner { string label = 3; }
            }
        }
        "#,
    )
    .expect("parse nested message schema");
    prost_types::FileDescriptorSet { file: vec![schema] }.encode_to_vec()
}
