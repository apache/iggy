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

#[path = "resource_options_fixtures/schema.rs"]
mod schema;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use iggy_binary_protocol::requests::system::DescribeOptionsRequest;
use iggy_binary_protocol::responses::streams::{GetStreamResponse, StreamResponse, TopicHeader};
use iggy_binary_protocol::responses::system::DescribeOptionsResponse;
use iggy_binary_protocol::responses::topics::GetTopicResponse;
use iggy_binary_protocol::responses::users::{UserDetailsResponse, UserResponse};
use iggy_binary_protocol::{WireDecode, WireEncode, WireName};
use iggy_common::wire_conversions::{resource_options_from_wire, resource_options_to_wire_split};
use iggy_common::{
    CATALOG, Durability, HeaderKey, HeaderValue, OptionColumns, OptionKey, OptionStorage,
    OptionValue, OptionVisibility, OptionsScope, ResourceOptions, StreamDetails,
    StreamEncryptionKey, StreamEncryptionSource, TopicDetails, UserInfoDetails, UserStatus,
    descriptors_for, public_options, stream_key_fingerprint,
};
use schema::{
    CANONICAL_PATH, CatalogRow, CreateStreamInput, CreateStreamVector, CreateTopicInput,
    CreateTopicVector, CreateUserInput, CreateUserVector, DescribeOptionsVector, DescribedOption,
    ErrorExpectation, GENERATOR, HttpUpdateVector, IdentifierInput, InvalidKeyVector, KeyInput,
    LANGUAGE_COPIES, PatchInput, ResourceOptionsFixture, SCHEMA_VERSION, SCOPES, StreamKeyState,
    StreamKeyVectors, StreamProjection, StreamResponseVector, TargetInput, TopicProjection,
    TopicResponseVector, UpdateResourceErrorVector, UpdateResourceVector, UserProjection,
    UserResponseVector, WireConstants, admit_create, admit_update, contains_bytes,
    describe_options_response, evaluate_http_update, update_resource_request,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::iter;
use std::path::Path;

const FINGERPRINT_LABEL: &str = "iggy/stream-key-fingerprint/v1";
const FIRST_KEK_CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const SECOND_KEK_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
const CREATED_AT_MICROS: u64 = 1_710_000_000_000_000;
const SEVEN_DAYS_MICROS: &str = "604800000000";
const ONE_DAY_MICROS: &str = "86400000000";
const ABOVE_2_POW_53: u64 = (1 << 53) + 1;

type GeneratorResult<T> = Result<T, Box<dyn Error>>;

fn main() -> GeneratorResult<()> {
    let fixture = ResourceOptionsFixture {
        schema_version: SCHEMA_VERSION,
        generated_by: GENERATOR.to_owned(),
        conventions: BTreeMap::from(
            [
                (
                    "byte_strings",
                    "Every *_hex field is lowercase hex without separators.",
                ),
                (
                    "u64_values",
                    "u64 option values and typed u64 inputs are decimal strings, so values above 2^53 keep their precision.",
                ),
                (
                    "message_expiry",
                    "Microseconds. 18446744073709551615 means never expire.",
                ),
                (
                    "max_topic_size",
                    "Bytes. 18446744073709551615 means unlimited.",
                ),
                (
                    "option_maps",
                    "Option maps use the HTTP JSON form: value as reversible text, kind, and explicit provenance. Request blocks mark every entry explicit.",
                ),
                (
                    "options_block_order",
                    "Options block entries are encoded in ascending key byte order.",
                ),
                (
                    "reset_order",
                    "The typed encoders and the HTTP handler write reset keys in ascending key byte order.",
                ),
                (
                    "request_bodies",
                    "body_hex and update_resource_hex are command payloads without framing. constants.command_codes maps each command to its code and VSR operation.",
                ),
                (
                    "errors",
                    "name is the snake_case IggyError name and code its numeric code.",
                ),
                (
                    "stream_state",
                    "state is the stored server state a stream response was projected from. SDKs need only response_hex and expected.",
                ),
                (
                    "never_returned_keys",
                    "No response, decoded projection or accessor may contain these keys or their values.",
                ),
            ]
            .map(|(name, text)| (name.to_owned(), text.to_owned())),
        ),
        constants: WireConstants::live()?,
        catalog: CATALOG
            .iter()
            .map(CatalogRow::try_from)
            .collect::<Result<_, _>>()?,
        describe_options: describe_options_vectors()?,
        stream_key: stream_key_vectors()?,
        update_resource: update_resource_vectors()?,
        update_resource_errors: update_resource_error_vectors()?,
        create_stream: create_stream_vectors()?,
        create_topic: create_topic_vectors()?,
        create_user: create_user_vectors()?,
        http_update: http_update_vectors()?,
        stream_responses: stream_response_vectors()?,
        topic_responses: topic_response_vectors()?,
        user_responses: user_response_vectors()?,
    };
    let mut json = serde_json::to_string_pretty(&fixture)?;
    json.push('\n');
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in iter::once(CANONICAL_PATH).chain(LANGUAGE_COPIES) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, &json)?;
        println!("Wrote {}", path.display());
    }
    Ok(())
}

fn describe_options_vectors() -> GeneratorResult<Vec<DescribeOptionsVector>> {
    SCOPES
        .into_iter()
        .map(|scope| {
            let bytes = describe_options_response(scope)?.to_bytes();
            let entries = DescribeOptionsResponse::decode_from(&bytes)?
                .entries
                .iter()
                .map(DescribedOption::try_from)
                .collect::<Result<_, _>>()?;
            Ok(DescribeOptionsVector {
                scope,
                request_hex: hex::encode(
                    DescribeOptionsRequest {
                        scope: scope.as_code(),
                    }
                    .to_bytes(),
                ),
                response_hex: hex::encode(bytes),
                entries,
            })
        })
        .collect()
}

fn stream_key_vectors() -> GeneratorResult<StreamKeyVectors> {
    let key_bytes: Vec<u8> = (0u8..32).collect();
    let key = StreamEncryptionKey::try_from(key_bytes.as_slice())?;
    let key_base64 = STANDARD.encode(&key_bytes);
    let fingerprint = stream_key_fingerprint(&key_bytes)?;
    if KeyInput::Base64(key_base64.clone()).parse()? != key || key.fingerprint() != fingerprint {
        return Err("the raw and base64 forms of the fixed key disagree".into());
    }
    let without_padding = key_base64.trim_end_matches('=').to_owned();
    let noncanonical_trailing_bits = format!("{}9=", &key_base64[..key_base64.len() - 2]);
    let invalid_inputs = [
        (
            "raw_31_bytes",
            KeyInput::RawHex(hex::encode(&key_bytes[..31])),
        ),
        (
            "raw_33_bytes",
            KeyInput::RawHex(hex::encode([key_bytes.as_slice(), &[32]].concat())),
        ),
        (
            "base64_with_whitespace",
            KeyInput::Base64(format!("{key_base64}\n")),
        ),
        ("base64_missing_padding", KeyInput::Base64(without_padding)),
        (
            "base64_noncanonical_trailing_bits",
            KeyInput::Base64(noncanonical_trailing_bits),
        ),
    ];
    let invalid_keys = invalid_inputs
        .into_iter()
        .map(|(case, input)| {
            let Err(error) = input.parse() else {
                return Err(format!("invalid key {case} was accepted").into());
            };
            Ok(InvalidKeyVector {
                case: case.to_owned(),
                error: ErrorExpectation::from(&error),
                input,
            })
        })
        .collect::<GeneratorResult<_>>()?;
    Ok(StreamKeyVectors {
        key_hex: hex::encode(&key_bytes),
        key_base64,
        fingerprint_label: FINGERPRINT_LABEL.to_owned(),
        fingerprint,
        invalid_keys,
    })
}

fn update_resource_vectors() -> GeneratorResult<Vec<UpdateResourceVector>> {
    let numeric_topic = TargetInput {
        stream_id: Some(IdentifierInput::Numeric(1)),
        topic_id: Some(IdentifierInput::Numeric(2)),
        user_id: None,
    };
    let cases = [
        (
            "topic_set_expiry_without_name_reset_max_topic_size",
            OptionsScope::Topic,
            numeric_topic.clone(),
            PatchInput {
                message_expiry: Some(SEVEN_DAYS_MICROS.to_owned()),
                reset: vec![OptionKey::MaxTopicSize],
                ..PatchInput::default()
            },
        ),
        (
            "topic_rename_only_named_identifiers",
            OptionsScope::Topic,
            TargetInput {
                stream_id: Some(IdentifierInput::Named("orders".to_owned())),
                topic_id: Some(IdentifierInput::Named("events".to_owned())),
                user_id: None,
            },
            PatchInput {
                name: Some("events-v2".to_owned()),
                ..PatchInput::default()
            },
        ),
        (
            "topic_rename_set_expiry_reset_max_topic_size",
            OptionsScope::Topic,
            TargetInput {
                stream_id: Some(IdentifierInput::Numeric(7)),
                topic_id: Some(IdentifierInput::Named("events".to_owned())),
                user_id: None,
            },
            PatchInput {
                name: Some("events-v3".to_owned()),
                message_expiry: Some(ONE_DAY_MICROS.to_owned()),
                reset: vec![OptionKey::MaxTopicSize],
                ..PatchInput::default()
            },
        ),
        (
            "topic_reset_both_defaults",
            OptionsScope::Topic,
            numeric_topic.clone(),
            PatchInput {
                reset: vec![OptionKey::MaxTopicSize, OptionKey::MessageExpiry],
                ..PatchInput::default()
            },
        ),
        (
            "topic_max_topic_size_above_2_pow_53",
            OptionsScope::Topic,
            numeric_topic,
            PatchInput {
                max_topic_size: Some(ABOVE_2_POW_53.to_string()),
                ..PatchInput::default()
            },
        ),
        (
            "stream_rename",
            OptionsScope::Stream,
            TargetInput {
                stream_id: Some(IdentifierInput::Numeric(3)),
                ..TargetInput::default()
            },
            PatchInput {
                name: Some("payments-v2".to_owned()),
                ..PatchInput::default()
            },
        ),
        (
            "user_username_and_status",
            OptionsScope::User,
            TargetInput {
                user_id: Some(IdentifierInput::Numeric(5)),
                ..TargetInput::default()
            },
            PatchInput {
                username: Some("alice2".to_owned()),
                status: Some(UserStatus::Inactive),
                ..PatchInput::default()
            },
        ),
    ];
    cases
        .into_iter()
        .map(|(case, scope, target, input)| {
            let request = update_resource_request(scope, &target, &input)?;
            admit_update(&request)?;
            Ok(UpdateResourceVector {
                case: case.to_owned(),
                scope,
                wire_set: resource_options_from_wire(&request.options, true)?,
                wire_reset: request
                    .reset
                    .iter()
                    .map(|name| name.as_str().to_owned())
                    .collect(),
                body_hex: hex::encode(request.to_bytes()),
                target,
                input,
            })
        })
        .collect()
}

fn update_resource_error_vectors() -> GeneratorResult<Vec<UpdateResourceErrorVector>> {
    let numeric_topic = TargetInput {
        stream_id: Some(IdentifierInput::Numeric(1)),
        topic_id: Some(IdentifierInput::Numeric(2)),
        user_id: None,
    };
    let stream = TargetInput {
        stream_id: Some(IdentifierInput::Numeric(3)),
        ..TargetInput::default()
    };
    let user = TargetInput {
        user_id: Some(IdentifierInput::Numeric(5)),
        ..TargetInput::default()
    };
    let cases = [
        (
            "topic_empty_update",
            OptionsScope::Topic,
            numeric_topic.clone(),
            PatchInput::default(),
        ),
        (
            "topic_set_and_reset_same_key",
            OptionsScope::Topic,
            numeric_topic.clone(),
            PatchInput {
                message_expiry: Some(SEVEN_DAYS_MICROS.to_owned()),
                reset: vec![OptionKey::MessageExpiry],
                ..PatchInput::default()
            },
        ),
        (
            "topic_reset_create_only_key",
            OptionsScope::Topic,
            numeric_topic.clone(),
            PatchInput {
                reset: vec![OptionKey::CompressionAlgorithm],
                ..PatchInput::default()
            },
        ),
        (
            "stream_reset_name",
            OptionsScope::Stream,
            stream.clone(),
            PatchInput {
                reset: vec![OptionKey::Name],
                ..PatchInput::default()
            },
        ),
        (
            "user_reset_status",
            OptionsScope::User,
            user.clone(),
            PatchInput {
                reset: vec![OptionKey::Status],
                ..PatchInput::default()
            },
        ),
        (
            "stream_reset_read_only_key",
            OptionsScope::Stream,
            stream.clone(),
            PatchInput {
                reset: vec![OptionKey::EncryptionKeySource],
                ..PatchInput::default()
            },
        ),
        (
            "user_reset_internal_key",
            OptionsScope::User,
            user.clone(),
            PatchInput {
                reset: vec![OptionKey::CriticalKeys],
                ..PatchInput::default()
            },
        ),
        (
            "topic_empty_name",
            OptionsScope::Topic,
            numeric_topic.clone(),
            PatchInput {
                name: Some(String::new()),
                ..PatchInput::default()
            },
        ),
        (
            "topic_empty_name_and_create_only_reset",
            OptionsScope::Topic,
            numeric_topic,
            PatchInput {
                name: Some(String::new()),
                reset: vec![OptionKey::SegmentSize],
                ..PatchInput::default()
            },
        ),
        (
            "stream_empty_name",
            OptionsScope::Stream,
            stream,
            PatchInput {
                name: Some(String::new()),
                ..PatchInput::default()
            },
        ),
        (
            "user_short_username",
            OptionsScope::User,
            user.clone(),
            PatchInput {
                username: Some("a".to_owned()),
                ..PatchInput::default()
            },
        ),
        (
            "user_short_username_and_status_reset",
            OptionsScope::User,
            user,
            PatchInput {
                username: Some("a".to_owned()),
                reset: vec![OptionKey::Status],
                ..PatchInput::default()
            },
        ),
    ];
    cases
        .into_iter()
        .map(|(case, scope, target, input)| {
            let Err(error) = update_resource_request(scope, &target, &input) else {
                return Err(format!("invalid update {case} was accepted").into());
            };
            Ok(UpdateResourceErrorVector {
                case: case.to_owned(),
                scope,
                target,
                input,
                error: ErrorExpectation::from(&error),
            })
        })
        .collect()
}

fn create_stream_vectors() -> GeneratorResult<Vec<CreateStreamVector>> {
    let cases = [
        (
            "stream_without_key",
            CreateStreamInput {
                name: "orders".to_owned(),
                encryption_key: None,
                raw: BTreeMap::new(),
            },
        ),
        (
            "stream_with_key",
            CreateStreamInput {
                name: "orders".to_owned(),
                encryption_key: Some(KeyInput::RawHex(hex::encode((0u8..32).collect::<Vec<_>>()))),
                raw: BTreeMap::new(),
            },
        ),
        (
            "stream_with_raw_extra_key",
            CreateStreamInput {
                name: "orders".to_owned(),
                encryption_key: None,
                raw: BTreeMap::from([("future_option".to_owned(), "enabled".to_owned())]),
            },
        ),
    ];
    cases
        .into_iter()
        .map(|(case, input)| {
            let request = input.request()?;
            let columns = OptionColumns {
                name: Some(input.name.clone()),
                ..OptionColumns::default()
            };
            Ok(CreateStreamVector {
                case: case.to_owned(),
                wire_options: resource_options_from_wire(&request.options, true)?,
                body_hex: hex::encode(request.to_bytes()),
                http_body: input.http_body()?,
                admission_error: admit_create(OptionsScope::Stream, columns, &request.options)
                    .err()
                    .map(|error| ErrorExpectation::from(&error)),
                input,
            })
        })
        .collect()
}

fn create_topic_vectors() -> GeneratorResult<Vec<CreateTopicVector>> {
    let input = CreateTopicInput {
        stream_id: IdentifierInput::Numeric(1),
        name: "events".to_owned(),
        partitions_count: 3,
        compression_algorithm: Some("gzip".to_owned()),
        message_expiry: Some(SEVEN_DAYS_MICROS.to_owned()),
        max_topic_size: Some(ABOVE_2_POW_53.to_string()),
        segment_size: Some((128u64 * 1024 * 1024).to_string()),
        durability: Durability::Persisted,
        consumer_offset_durability: Durability::Replicated,
    };
    let request = input.request()?;
    let columns = OptionColumns {
        name: Some(input.name.clone()),
        ..OptionColumns::default()
    };
    Ok(vec![CreateTopicVector {
        case: "topic_with_options".to_owned(),
        wire_options: resource_options_from_wire(&request.options, true)?,
        body_hex: hex::encode(request.to_bytes()),
        http_body: input.http_body()?,
        admission_error: admit_create(OptionsScope::Topic, columns, &request.options)
            .err()
            .map(|error| ErrorExpectation::from(&error)),
        input,
    }])
}

fn create_user_vectors() -> GeneratorResult<Vec<CreateUserVector>> {
    let input = CreateUserInput {
        username: "alice".to_owned(),
        password: "s3cret-pass".to_owned(),
        status: UserStatus::Active,
    };
    let request = input.request()?;
    let columns = OptionColumns {
        username: Some(input.username.clone()),
        status: Some(input.status),
        ..OptionColumns::default()
    };
    Ok(vec![CreateUserVector {
        case: "user_with_empty_options".to_owned(),
        wire_options: resource_options_from_wire(&request.options, true)?,
        body_hex: hex::encode(request.to_bytes()),
        http_body: input.http_body()?,
        admission_error: admit_create(OptionsScope::User, columns, &request.options)
            .err()
            .map(|error| ErrorExpectation::from(&error)),
        input,
    }])
}

fn http_update_vectors() -> GeneratorResult<Vec<HttpUpdateVector>> {
    let topic = TargetInput {
        stream_id: Some(IdentifierInput::Numeric(1)),
        topic_id: Some(IdentifierInput::Numeric(2)),
        user_id: None,
    };
    let stream = TargetInput {
        stream_id: Some(IdentifierInput::Numeric(3)),
        ..TargetInput::default()
    };
    let user = TargetInput {
        user_id: Some(IdentifierInput::Numeric(5)),
        ..TargetInput::default()
    };
    let cases = [
        (
            "topic_flat_keys",
            OptionsScope::Topic,
            &topic,
            r#"{"name":"events-v2","message_expiry":"7 days"}"#,
        ),
        (
            "topic_reset_array",
            OptionsScope::Topic,
            &topic,
            r#"{"message_expiry":604800000000,"reset":["max_topic_size"]}"#,
        ),
        (
            "topic_null_ignored",
            OptionsScope::Topic,
            &topic,
            r#"{"name":null,"max_topic_size":null,"message_expiry":"1 day"}"#,
        ),
        (
            "topic_u64_above_2_pow_53_as_string",
            OptionsScope::Topic,
            &topic,
            r#"{"max_topic_size":"9007199254740993"}"#,
        ),
        (
            "stream_rename",
            OptionsScope::Stream,
            &stream,
            r#"{"name":"payments-v2"}"#,
        ),
        (
            "user_username_and_status",
            OptionsScope::User,
            &user,
            r#"{"username":"alice2","status":"inactive"}"#,
        ),
        (
            "topic_duplicate_key",
            OptionsScope::Topic,
            &topic,
            r#"{"message_expiry":"1 day","message_expiry":"2 days"}"#,
        ),
        (
            "topic_nested_object",
            OptionsScope::Topic,
            &topic,
            r#"{"message_expiry":{"value":"1 day"}}"#,
        ),
        (
            "topic_null_only",
            OptionsScope::Topic,
            &topic,
            r#"{"max_topic_size":null}"#,
        ),
        (
            "topic_zero_sentinel_only",
            OptionsScope::Topic,
            &topic,
            r#"{"message_expiry":0}"#,
        ),
        (
            "topic_reset_names_set_key",
            OptionsScope::Topic,
            &topic,
            r#"{"message_expiry":"1 day","reset":["message_expiry"]}"#,
        ),
        (
            "topic_unknown_key",
            OptionsScope::Topic,
            &topic,
            r#"{"retention":"1 day"}"#,
        ),
        (
            "topic_set_create_only_key",
            OptionsScope::Topic,
            &topic,
            r#"{"compression_algorithm":"gzip"}"#,
        ),
        (
            "topic_reset_create_only_key",
            OptionsScope::Topic,
            &topic,
            r#"{"reset":["compression_algorithm"]}"#,
        ),
        (
            "stream_reset_name",
            OptionsScope::Stream,
            &stream,
            r#"{"reset":["name"]}"#,
        ),
        (
            "topic_name_as_number",
            OptionsScope::Topic,
            &topic,
            r#"{"name":5}"#,
        ),
        (
            "stream_empty_name",
            OptionsScope::Stream,
            &stream,
            r#"{"name":""}"#,
        ),
        (
            "user_status_as_bool",
            OptionsScope::User,
            &user,
            r#"{"status":true}"#,
        ),
        (
            "topic_create_only_key_as_bool",
            OptionsScope::Topic,
            &topic,
            r#"{"segment_size":true}"#,
        ),
        (
            "stream_read_only_key_as_number",
            OptionsScope::Stream,
            &stream,
            r#"{"encryption_key_source":1}"#,
        ),
        (
            "topic_invalid_value",
            OptionsScope::Topic,
            &topic,
            r#"{"message_expiry":"soon"}"#,
        ),
    ];
    cases
        .into_iter()
        .map(|(case, scope, target, body)| {
            let evaluation = evaluate_http_update(scope, target, body)?;
            Ok(HttpUpdateVector {
                case: case.to_owned(),
                scope,
                target: target.clone(),
                body: body.to_owned(),
                update_resource_hex: evaluation.update_resource.map(hex::encode),
                outcome: evaluation.outcome,
                error: evaluation.error,
            })
        })
        .collect()
}

fn stream_response_vectors() -> GeneratorResult<Vec<StreamResponseVector>> {
    let cases = [
        (
            "stream_source_none",
            1,
            "plain-stream",
            StreamKeyState {
                encryption_key_source: StreamEncryptionSource::None,
                data_key: None,
                kek_config_keys: Vec::new(),
                stored_extra: ResourceOptions::new(),
            },
        ),
        (
            "stream_source_stream_with_unknown_explicit_key",
            2,
            "client-key-stream",
            StreamKeyState {
                encryption_key_source: StreamEncryptionSource::Stream,
                data_key: Some(KeyInput::RawHex(hex::encode((0u8..32).collect::<Vec<_>>()))),
                kek_config_keys: vec![FIRST_KEK_CONFIG_KEY.to_owned()],
                stored_extra: ResourceOptions::from([(
                    HeaderKey::try_from("future_option")?,
                    OptionValue::explicit(HeaderValue::try_from("enabled")?),
                )]),
            },
        ),
        (
            "stream_source_server_two_keks",
            3,
            "server-key-stream",
            StreamKeyState {
                encryption_key_source: StreamEncryptionSource::Server,
                data_key: Some(KeyInput::RawHex(hex::encode(
                    (32u8..64).collect::<Vec<_>>(),
                ))),
                kek_config_keys: vec![
                    FIRST_KEK_CONFIG_KEY.to_owned(),
                    SECOND_KEK_CONFIG_KEY.to_owned(),
                ],
                stored_extra: ResourceOptions::new(),
            },
        ),
    ];
    cases
        .into_iter()
        .map(|(case, id, name, state)| {
            let stored = state.stored_options()?;
            let (options, derived_options) =
                resource_options_to_wire_split(&public_options(OptionsScope::Stream, &stored)?)?;
            let bytes = GetStreamResponse {
                stream: StreamResponse {
                    id,
                    created_at: CREATED_AT_MICROS,
                    topics_count: 0,
                    size_bytes: 4096,
                    messages_count: 12,
                    name: WireName::new(name)?,
                    options,
                    derived_options,
                },
                topics: Vec::new(),
            }
            .to_bytes();
            let expected = StreamProjection::from(&StreamDetails::try_from(
                GetStreamResponse::decode_from(&bytes)?,
            )?);
            ensure_no_secrets(case, &bytes, &stored, &expected.options)?;
            if let Some(data_key) = &state.data_key {
                let key = data_key.parse()?;
                if contains_bytes(&bytes, key.as_bytes())
                    || contains_bytes(&bytes, STANDARD.encode(key.as_bytes()).as_bytes())
                {
                    return Err(format!("{case} response carries the stream key").into());
                }
            }
            Ok(StreamResponseVector {
                case: case.to_owned(),
                state,
                response_hex: hex::encode(&bytes),
                expected,
            })
        })
        .collect()
}

fn topic_response_vectors() -> GeneratorResult<Vec<TopicResponseVector>> {
    let mut stored = ResourceOptions::from([
        (
            HeaderKey::from(OptionKey::CompressionAlgorithm),
            OptionValue::explicit(HeaderValue::try_from("gzip")?),
        ),
        (
            HeaderKey::from(OptionKey::MessageExpiry),
            OptionValue::explicit(HeaderValue::from(SEVEN_DAYS_MICROS.parse::<u64>()?)),
        ),
    ]);
    for descriptor in descriptors_for(OptionsScope::Topic)
        .filter(|descriptor| descriptor.storage == OptionStorage::Map)
    {
        if let Some(default) = descriptor.default_value()? {
            stored
                .entry(HeaderKey::from(descriptor.key))
                .or_insert_with(|| OptionValue::derived(default));
        }
    }
    let (options, derived_options) =
        resource_options_to_wire_split(&public_options(OptionsScope::Topic, &stored)?)?;
    let bytes = GetTopicResponse {
        topic: TopicHeader {
            id: 4,
            created_at: CREATED_AT_MICROS,
            partitions_count: 0,
            size_bytes: 2048,
            messages_count: 7,
            name: WireName::new("events")?,
            options,
            derived_options,
        },
        partitions: Vec::new(),
    }
    .to_bytes();
    let expected = TopicProjection::from(&TopicDetails::try_from(GetTopicResponse::decode_from(
        &bytes,
    )?)?);
    ensure_no_secrets(
        "topic_explicit_and_derived",
        &bytes,
        &stored,
        &expected.options,
    )?;
    Ok(vec![TopicResponseVector {
        case: "topic_explicit_and_derived".to_owned(),
        response_hex: hex::encode(&bytes),
        expected,
    }])
}

fn user_response_vectors() -> GeneratorResult<Vec<UserResponseVector>> {
    let cases = [
        (
            "user_empty_blocks",
            5,
            "alice",
            UserStatus::Active,
            ResourceOptions::new(),
        ),
        (
            "user_populated_blocks",
            6,
            "bob",
            UserStatus::Inactive,
            ResourceOptions::from([
                (
                    HeaderKey::try_from("future_option")?,
                    OptionValue::explicit(HeaderValue::try_from("enabled")?),
                ),
                (
                    HeaderKey::try_from("future_quota")?,
                    OptionValue::derived(HeaderValue::from(ABOVE_2_POW_53)),
                ),
            ]),
        ),
    ];
    cases
        .into_iter()
        .map(|(case, id, username, status, stored)| {
            let (options, derived_options) =
                resource_options_to_wire_split(&public_options(OptionsScope::User, &stored)?)?;
            let bytes = UserDetailsResponse {
                user: UserResponse {
                    id,
                    created_at: CREATED_AT_MICROS,
                    status: status.as_code(),
                    username: WireName::new(username)?,
                    options,
                    derived_options,
                },
                permissions: None,
            }
            .to_bytes();
            let expected = UserProjection::from(&UserInfoDetails::try_from(
                UserDetailsResponse::decode_from(&bytes)?,
            )?);
            ensure_no_secrets(case, &bytes, &stored, &expected.options)?;
            Ok(UserResponseVector {
                case: case.to_owned(),
                response_hex: hex::encode(&bytes),
                expected,
            })
        })
        .collect()
}

/// Refuse a response that echoes a secret or internal key or stored secret bytes.
fn ensure_no_secrets(
    case: &str,
    response: &[u8],
    stored: &ResourceOptions,
    projected: &ResourceOptions,
) -> GeneratorResult<()> {
    for descriptor in CATALOG.iter().filter(|descriptor| {
        matches!(
            descriptor.visibility,
            OptionVisibility::Secret | OptionVisibility::Internal
        )
    }) {
        let key = HeaderKey::from(descriptor.key);
        if projected.contains_key(&key) {
            return Err(format!("{case} projects {}", descriptor.key).into());
        }
        if let Some(value) = stored.get(&key)
            && contains_bytes(response, value.value.as_bytes())
        {
            return Err(format!("{case} response carries {}", descriptor.key).into());
        }
    }
    Ok(())
}
