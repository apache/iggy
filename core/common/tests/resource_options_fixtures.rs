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

#[path = "../examples/resource_options_fixtures/schema.rs"]
mod schema;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::system::{DescribeOptionsRequest, UpdateResourceRequest};
use iggy_binary_protocol::requests::topics::CreateTopicRequest;
use iggy_binary_protocol::requests::users::CreateUserRequest;
use iggy_binary_protocol::responses::streams::{GetStreamResponse, StreamResponse, TopicHeader};
use iggy_binary_protocol::responses::system::DescribeOptionsResponse;
use iggy_binary_protocol::responses::topics::GetTopicResponse;
use iggy_binary_protocol::responses::users::{UserDetailsResponse, UserResponse};
use iggy_binary_protocol::version::pack_protocol_version;
use iggy_binary_protocol::{Operation, WireDecode, WireEncode, WireName, lookup_command};
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::wire_conversions::{resource_options_from_wire, resource_options_to_wire_split};
use iggy_common::{
    CATALOG, HeaderKey, OPTION_FLAG_CRITICAL, OPTION_FLAG_READ_ONLY, OPTION_FLAG_RESETTABLE,
    OPTION_FLAG_SECRET, OPTION_FLAG_UPDATABLE, OptionColumns, OptionKey, OptionsScope,
    ServerKeyContext, StreamDetails, StreamEncryptionSource, TopicDetails, UserInfoDetails,
    public_options, stream_key_fingerprint,
};
use ring::digest::{SHA256, digest};
use schema::{
    CANONICAL_PATH, CatalogRow, CommandCode, DescribedOption, ErrorExpectation, GENERATOR,
    HttpOutcome, KeyInput, LANGUAGE_COPIES, ResourceOptionsFixture, SCHEMA_VERSION, SCOPES,
    StreamProjection, TopicProjection, UserProjection, WireConstants, admit_create, admit_update,
    contains_bytes, describe_options_response, evaluate_http_update, update_resource_request,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

const KNOWN_FINGERPRINT: &str = "8441e38538cbfa57";
const ABOVE_2_POW_53: u64 = (1 << 53) + 1;

#[test]
fn given_live_wire_constants_when_comparing_fixture_should_match_codes_and_versions() {
    let fixture = fixture();
    assert_eq!(fixture.schema_version, SCHEMA_VERSION);
    assert_eq!(fixture.generated_by, GENERATOR);
    let constants = &fixture.constants;
    assert_eq!(
        *constants,
        WireConstants::live().expect("live constants build")
    );
    assert_eq!(
        constants.protocol_version.packed,
        pack_protocol_version(0, 12, 0)
    );
    assert_eq!(constants.protocol_version.text, "0.12.0");
    assert_eq!(
        constants.command_codes["update_resource"],
        CommandCode {
            code: 16,
            operation: Some(150),
        }
    );
    for (name, code) in &constants.retired_command_codes {
        assert!(
            lookup_command(*code).is_none(),
            "retired command {name} ({code}) is still dispatched"
        );
    }
    for (name, value) in &constants.retired_operations {
        assert!(
            !Operation::is_known_code(*value),
            "retired operation {name} ({value}) is still known"
        );
    }
    let update_resource = &constants.operations["update_resource"];
    assert!(!update_resource.internal);
    for name in [
        "update_stream_resolved",
        "update_topic_resolved",
        "update_user_resolved",
    ] {
        let resolved = &constants.operations[name];
        assert!(resolved.internal, "{name} must be internal");
        assert_eq!(resolved.request_operation, update_resource.value, "{name}");
    }
    assert!(constants.operations["rewrap_stream_keys"].internal);
    let scope_codes: Vec<(&str, u8)> = constants
        .scope_codes
        .iter()
        .map(|(name, code)| (name.as_str(), *code))
        .collect();
    assert_eq!(scope_codes, [("stream", 2), ("topic", 1), ("user", 3)]);
    assert_eq!(
        constants.never_returned_keys,
        [
            OptionKey::EncryptionKey,
            OptionKey::EncryptionKeyWrapped,
            OptionKey::CriticalKeys,
        ]
    );
}

#[test]
fn given_live_catalog_when_comparing_fixture_should_match_keys_kinds_defaults_and_flags() {
    let fixture = fixture();
    let live = CATALOG
        .iter()
        .map(CatalogRow::try_from)
        .collect::<Result<Vec<_>, _>>()
        .expect("live catalog rows build");
    assert_eq!(fixture.catalog, live);

    let row = |key: OptionKey| {
        fixture
            .catalog
            .iter()
            .find(|row| row.key == key)
            .expect("catalog row exists")
    };
    assert_eq!(row(OptionKey::Name).flags, OPTION_FLAG_UPDATABLE);
    assert_eq!(
        row(OptionKey::CompressionAlgorithm).flags,
        0,
        "compression_algorithm is create-only"
    );
    assert_eq!(
        row(OptionKey::CompressionAlgorithm).default.as_deref(),
        Some("none")
    );
    assert_eq!(
        row(OptionKey::MessageExpiry).flags,
        OPTION_FLAG_UPDATABLE | OPTION_FLAG_RESETTABLE
    );
    assert_eq!(
        row(OptionKey::MaxTopicSize).default.as_deref(),
        Some("18446744073709551615")
    );
    assert_eq!(
        row(OptionKey::EncryptionKey).flags,
        OPTION_FLAG_SECRET | OPTION_FLAG_CRITICAL
    );
    assert_eq!(
        row(OptionKey::EncryptionKekFingerprint).flags,
        OPTION_FLAG_READ_ONLY
    );
    assert!(!row(OptionKey::EncryptionKeyWrapped).described);
    assert!(!row(OptionKey::CriticalKeys).described);
}

#[test]
fn given_describe_options_vectors_when_decoding_should_match_catalog_flags() {
    let fixture = fixture();
    let scopes: Vec<OptionsScope> = fixture
        .describe_options
        .iter()
        .map(|vector| vector.scope)
        .collect();
    assert_eq!(scopes, SCOPES);
    for vector in &fixture.describe_options {
        let request = DescribeOptionsRequest {
            scope: vector.scope.as_code(),
        };
        assert_eq!(hex::encode(request.to_bytes()), vector.request_hex);

        let bytes = hex::decode(&vector.response_hex).expect("response is hex");
        let (response, consumed) =
            DescribeOptionsResponse::decode(&bytes).expect("describe response decodes");
        assert_eq!(consumed, bytes.len(), "{}", vector.scope);
        let entries = response
            .entries
            .iter()
            .map(DescribedOption::try_from)
            .collect::<Result<Vec<_>, _>>()
            .expect("entries decode");
        assert_eq!(entries, vector.entries, "{}", vector.scope);

        let rebuilt = describe_options_response(vector.scope).expect("server response builds");
        assert_eq!(
            rebuilt.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.scope
        );

        let from_catalog: Vec<DescribedOption> = fixture
            .catalog
            .iter()
            .filter(|row| row.described && row.scopes.contains(&vector.scope))
            .map(|row| DescribedOption {
                key: row.key.to_string(),
                kind: row.kind,
                flags: row.flags,
                default: row.default.clone(),
            })
            .collect();
        assert_eq!(entries, from_catalog, "{}", vector.scope);
    }
}

#[test]
fn given_fixed_key_when_comparing_fixture_should_match_fingerprint() {
    let vectors = fixture().stream_key;
    let key = hex::decode(&vectors.key_hex).expect("key is hex");
    assert_eq!(key, (0u8..32).collect::<Vec<_>>());
    assert_eq!(STANDARD.encode(&key), vectors.key_base64);
    assert_eq!(vectors.fingerprint, KNOWN_FINGERPRINT);
    assert_eq!(
        stream_key_fingerprint(&key).expect("32-byte key"),
        vectors.fingerprint
    );
    assert_eq!(
        KeyInput::Base64(vectors.key_base64.clone())
            .parse()
            .expect("canonical base64 key")
            .fingerprint(),
        vectors.fingerprint
    );
    let mut labelled = vectors.fingerprint_label.as_bytes().to_vec();
    labelled.extend_from_slice(&key);
    assert_eq!(
        hex::encode(&digest(&SHA256, &labelled).as_ref()[..8]),
        vectors.fingerprint
    );

    let cases: BTreeSet<&str> = vectors
        .invalid_keys
        .iter()
        .map(|vector| vector.case.as_str())
        .collect();
    assert_eq!(
        cases,
        BTreeSet::from([
            "base64_missing_padding",
            "base64_noncanonical_trailing_bits",
            "base64_with_whitespace",
            "raw_31_bytes",
            "raw_33_bytes",
        ])
    );
    for vector in &vectors.invalid_keys {
        let refused = vector
            .input
            .parse()
            .err()
            .map(|error| ErrorExpectation::from(&error));
        assert_eq!(refused.as_ref(), Some(&vector.error), "{}", vector.case);
        assert_eq!(
            vector.error.name, "invalid_encryption_key",
            "{}",
            vector.case
        );
    }
}

#[test]
fn given_update_resource_vectors_when_decoding_should_match_all_scopes() {
    let fixture = fixture();
    let mut scopes = Vec::new();
    for vector in &fixture.update_resource {
        let bytes = hex::decode(&vector.body_hex).expect("body is hex");
        let decoded = UpdateResourceRequest::decode_from(&bytes).expect("vector decodes");
        let typed = update_resource_request(vector.scope, &vector.target, &vector.input)
            .expect("typed input encodes");
        assert_eq!(decoded, typed, "{}", vector.case);
        assert_eq!(
            typed.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.case
        );
        assert_eq!(
            resource_options_from_wire(&decoded.options, true).expect("set block decodes"),
            vector.wire_set,
            "{}",
            vector.case
        );
        let reset: Vec<&str> = decoded.reset.iter().map(WireName::as_str).collect();
        assert_eq!(reset, vector.wire_reset, "{}", vector.case);
        assert_eq!(admit_update(&decoded), Ok(()), "{}", vector.case);
        scopes.push(vector.scope);
    }
    for scope in SCOPES {
        assert!(scopes.contains(&scope), "no update vector for {scope}");
    }
    let keeps_u64_precision = fixture
        .update_resource
        .iter()
        .filter_map(|vector| {
            vector
                .wire_set
                .get(&HeaderKey::from(OptionKey::MaxTopicSize))
        })
        .any(|option| option.value.as_uint64() == Ok(ABOVE_2_POW_53));
    assert!(keeps_u64_precision, "no u64 value above 2^53");
}

#[test]
fn given_invalid_update_inputs_when_encoding_should_refuse_before_sending() {
    let fixture = fixture();
    for vector in &fixture.update_resource_errors {
        let refused = update_resource_request(vector.scope, &vector.target, &vector.input)
            .err()
            .map(|error| ErrorExpectation::from(&error));
        assert_eq!(refused.as_ref(), Some(&vector.error), "{}", vector.case);
    }
    let names: BTreeSet<&str> = fixture
        .update_resource_errors
        .iter()
        .map(|vector| vector.error.name.as_str())
        .collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "invalid_command",
            "invalid_stream_name",
            "invalid_topic_name",
            "invalid_username",
            "option_not_resettable",
            "option_not_updatable",
            "read_only_option"
        ])
    );
}

#[test]
fn given_create_vectors_when_decoding_should_match_typed_requests() {
    let fixture = fixture();
    for vector in &fixture.create_stream {
        let bytes = hex::decode(&vector.body_hex).expect("body is hex");
        let decoded = CreateStreamRequest::decode_from(&bytes).expect("vector decodes");
        let typed = vector.input.request().expect("typed input encodes");
        assert_eq!(decoded, typed, "{}", vector.case);
        assert_eq!(
            typed.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.case
        );
        assert_eq!(
            resource_options_from_wire(&decoded.options, true).expect("options decode"),
            vector.wire_options,
            "{}",
            vector.case
        );
        let columns = OptionColumns {
            name: Some(vector.input.name.clone()),
            ..OptionColumns::default()
        };
        let admission = admit_create(OptionsScope::Stream, columns.clone(), &decoded.options)
            .err()
            .map(|error| ErrorExpectation::from(&error));
        assert_eq!(admission, vector.admission_error, "{}", vector.case);
        assert_eq!(
            vector.input.http_body().expect("HTTP body encodes"),
            vector.http_body,
            "{}",
            vector.case
        );
        let posted: CreateStream =
            serde_json::from_str(&vector.http_body).expect("HTTP body parses");
        let http_admission = admit_create(
            OptionsScope::Stream,
            columns,
            &posted.options.to_wire().expect("HTTP options encode"),
        )
        .err()
        .map(|error| ErrorExpectation::from(&error));
        assert_eq!(http_admission, vector.admission_error, "{}", vector.case);
    }
    let keyed = fixture
        .create_stream
        .iter()
        .filter_map(|vector| {
            vector
                .wire_options
                .get(&HeaderKey::from(OptionKey::EncryptionKey))
        })
        .count();
    assert_eq!(keyed, 1, "exactly one create carries a stream key");

    for vector in &fixture.create_topic {
        let bytes = hex::decode(&vector.body_hex).expect("body is hex");
        let decoded = CreateTopicRequest::decode_from(&bytes).expect("vector decodes");
        let typed = vector.input.request().expect("typed input encodes");
        assert_eq!(decoded, typed, "{}", vector.case);
        assert_eq!(
            typed.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.case
        );
        assert_eq!(
            resource_options_from_wire(&decoded.options, true).expect("options decode"),
            vector.wire_options,
            "{}",
            vector.case
        );
        let columns = OptionColumns {
            name: Some(vector.input.name.clone()),
            ..OptionColumns::default()
        };
        let admission = admit_create(OptionsScope::Topic, columns.clone(), &decoded.options)
            .err()
            .map(|error| ErrorExpectation::from(&error));
        assert_eq!(admission, vector.admission_error, "{}", vector.case);
        assert_eq!(
            vector.input.http_body().expect("HTTP body encodes"),
            vector.http_body,
            "{}",
            vector.case
        );
        let posted: CreateTopic =
            serde_json::from_str(&vector.http_body).expect("HTTP body parses");
        let http_admission = admit_create(
            OptionsScope::Topic,
            columns,
            &posted.options.to_wire().expect("HTTP options encode"),
        )
        .err()
        .map(|error| ErrorExpectation::from(&error));
        assert_eq!(http_admission, vector.admission_error, "{}", vector.case);
    }

    for vector in &fixture.create_user {
        let bytes = hex::decode(&vector.body_hex).expect("body is hex");
        let decoded = CreateUserRequest::decode_from(&bytes).expect("vector decodes");
        let typed = vector.input.request().expect("typed input encodes");
        assert_eq!(decoded, typed, "{}", vector.case);
        assert_eq!(
            typed.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.case
        );
        assert!(vector.wire_options.is_empty(), "{}", vector.case);
        let columns = OptionColumns {
            username: Some(vector.input.username.clone()),
            status: Some(vector.input.status),
            ..OptionColumns::default()
        };
        let admission = admit_create(OptionsScope::User, columns, &decoded.options)
            .err()
            .map(|error| ErrorExpectation::from(&error));
        assert_eq!(admission, vector.admission_error, "{}", vector.case);
        assert_eq!(
            vector.input.http_body().expect("HTTP body encodes"),
            vector.http_body,
            "{}",
            vector.case
        );
    }
}

#[test]
fn given_http_update_vectors_when_parsing_should_match_update_resource() {
    let fixture = fixture();
    let mut outcomes = Vec::new();
    for vector in &fixture.http_update {
        let evaluation = evaluate_http_update(vector.scope, &vector.target, &vector.body)
            .expect("body converts");
        assert_eq!(
            evaluation.update_resource.as_deref().map(hex::encode),
            vector.update_resource_hex,
            "{}",
            vector.case
        );
        assert_eq!(evaluation.outcome, vector.outcome, "{}", vector.case);
        assert_eq!(evaluation.error, vector.error, "{}", vector.case);
        outcomes.push(vector.outcome);
    }
    for outcome in [
        HttpOutcome::Accepted,
        HttpOutcome::JsonRefused,
        HttpOutcome::DecodeRefused,
        HttpOutcome::AdmissionRefused,
    ] {
        assert!(outcomes.contains(&outcome), "no vector for {outcome:?}");
    }

    let null_ignored = fixture
        .http_update
        .iter()
        .find(|vector| vector.case == "topic_null_ignored")
        .and_then(|vector| vector.update_resource_hex.as_deref())
        .expect("null vector has a request");
    let request =
        UpdateResourceRequest::decode_from(&hex::decode(null_ignored).expect("request is hex"))
            .expect("null vector decodes");
    let set_keys: Vec<&[u8]> = (&request.options)
        .into_iter()
        .map(|entry| entry.key)
        .collect();
    assert_eq!(set_keys, [b"message_expiry".as_slice()]);
    assert!(request.reset.is_empty());
}

#[test]
fn given_response_vectors_when_decoding_should_match_typed_projection() {
    let fixture = fixture();
    let never_returned: Vec<HeaderKey> = fixture
        .constants
        .never_returned_keys
        .iter()
        .map(|key| HeaderKey::from(*key))
        .collect();

    let mut sources = Vec::new();
    for vector in &fixture.stream_responses {
        let bytes = hex::decode(&vector.response_hex).expect("response is hex");
        let (response, consumed) =
            GetStreamResponse::decode(&bytes).expect("stream response decodes");
        assert_eq!(consumed, bytes.len(), "{}", vector.case);
        let details = StreamDetails::try_from(response).expect("stream converts");
        assert_eq!(
            StreamProjection::from(&details),
            vector.expected,
            "{}",
            vector.case
        );

        let stored = vector.state.stored_options().expect("stored state builds");
        let public = public_options(OptionsScope::Stream, &stored).expect("stream projects");
        let (options, derived_options) =
            resource_options_to_wire_split(&public).expect("blocks encode");
        let rebuilt = GetStreamResponse {
            stream: StreamResponse {
                id: vector.expected.id,
                created_at: vector.expected.created_at,
                topics_count: vector.expected.topics_count,
                size_bytes: vector.expected.size_bytes,
                messages_count: vector.expected.messages_count,
                name: WireName::new(vector.expected.name.as_str()).expect("stream name"),
                options,
                derived_options,
            },
            topics: Vec::new(),
        };
        assert_eq!(
            rebuilt.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.case
        );

        for key in &never_returned {
            assert!(
                !vector.expected.options.contains_key(key),
                "{} returns a secret or internal key",
                vector.case
            );
        }
        if let Some(wrapped) = stored.get(&HeaderKey::from(OptionKey::EncryptionKeyWrapped)) {
            assert!(
                !contains_bytes(&bytes, wrapped.value.as_bytes()),
                "{} carries the wrap set",
                vector.case
            );
        }
        match &vector.state.data_key {
            Some(data_key) => {
                let key = data_key.parse().expect("state key parses");
                assert!(!contains_bytes(&bytes, key.as_bytes()), "{}", vector.case);
                assert!(
                    !contains_bytes(&bytes, STANDARD.encode(key.as_bytes()).as_bytes()),
                    "{}",
                    vector.case
                );
                assert_eq!(
                    vector.expected.encryption_key_fingerprint,
                    Some(key.fingerprint()),
                    "{}",
                    vector.case
                );
            }
            None => assert_eq!(
                vector.expected.encryption_key_fingerprint, None,
                "{}",
                vector.case
            ),
        }
        let mut kek_fingerprints: Vec<String> = vector
            .state
            .kek_config_keys
            .iter()
            .map(|config_key| {
                ServerKeyContext::from_config(config_key, &[], true)
                    .expect("config key is valid")
                    .kek_fingerprint()
                    .expect("config key derives a KEK")
            })
            .collect();
        kek_fingerprints.sort();
        assert_eq!(
            vector.expected.encryption_kek_fingerprints, kek_fingerprints,
            "{}",
            vector.case
        );
        assert_eq!(
            vector.expected.encryption_key_source,
            Some(vector.state.encryption_key_source),
            "{}",
            vector.case
        );
        sources.push(vector.state.encryption_key_source);
    }
    for source in [
        StreamEncryptionSource::Stream,
        StreamEncryptionSource::Server,
        StreamEncryptionSource::None,
    ] {
        assert!(sources.contains(&source), "no stream vector for {source}");
    }
    assert!(
        fixture.stream_responses.iter().any(|vector| vector
            .expected
            .encryption_kek_fingerprints
            .len()
            == 2),
        "no stream wrapped under two KEKs"
    );

    for vector in &fixture.topic_responses {
        let bytes = hex::decode(&vector.response_hex).expect("response is hex");
        let (response, consumed) =
            GetTopicResponse::decode(&bytes).expect("topic response decodes");
        assert_eq!(consumed, bytes.len(), "{}", vector.case);
        let details = TopicDetails::try_from(response).expect("topic converts");
        assert_eq!(
            TopicProjection::from(&details),
            vector.expected,
            "{}",
            vector.case
        );
        assert_eq!(
            public_options(OptionsScope::Topic, &vector.expected.options).expect("topic projects"),
            vector.expected.options,
            "{}",
            vector.case
        );
        let (options, derived_options) =
            resource_options_to_wire_split(&vector.expected.options).expect("blocks encode");
        assert!(
            !options.is_empty() && !derived_options.is_empty(),
            "{}",
            vector.case
        );
        let rebuilt = GetTopicResponse {
            topic: TopicHeader {
                id: vector.expected.id,
                created_at: vector.expected.created_at,
                partitions_count: vector.expected.partitions_count,
                size_bytes: vector.expected.size_bytes,
                messages_count: vector.expected.messages_count,
                name: WireName::new(vector.expected.name.as_str()).expect("topic name"),
                options,
                derived_options,
            },
            partitions: Vec::new(),
        };
        assert_eq!(
            rebuilt.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.case
        );
        for key in [
            OptionKey::CompressionAlgorithm,
            OptionKey::MessageExpiry,
            OptionKey::MaxTopicSize,
        ] {
            assert!(
                vector.expected.options.contains_key(&HeaderKey::from(key)),
                "{} lacks {key}",
                vector.case
            );
        }
    }

    for vector in &fixture.user_responses {
        let bytes = hex::decode(&vector.response_hex).expect("response is hex");
        let (response, consumed) =
            UserDetailsResponse::decode(&bytes).expect("user response decodes");
        assert_eq!(consumed, bytes.len(), "{}", vector.case);
        let details = UserInfoDetails::try_from(response).expect("user converts");
        assert_eq!(
            UserProjection::from(&details),
            vector.expected,
            "{}",
            vector.case
        );
        assert_eq!(
            public_options(OptionsScope::User, &vector.expected.options).expect("user projects"),
            vector.expected.options,
            "{}",
            vector.case
        );
        for key in &never_returned {
            assert!(
                !vector.expected.options.contains_key(key),
                "{}",
                vector.case
            );
        }
        let (options, derived_options) =
            resource_options_to_wire_split(&vector.expected.options).expect("blocks encode");
        let rebuilt = UserDetailsResponse {
            user: UserResponse {
                id: vector.expected.id,
                created_at: vector.expected.created_at,
                status: vector.expected.status.as_code(),
                username: WireName::new(vector.expected.username.as_str()).expect("username"),
                options,
                derived_options,
            },
            permissions: None,
        };
        assert_eq!(
            rebuilt.to_bytes().as_ref(),
            bytes.as_slice(),
            "{}",
            vector.case
        );
    }
    let user_blocks: BTreeSet<(bool, bool)> = fixture
        .user_responses
        .iter()
        .map(|vector| {
            let options = &vector.expected.options;
            (
                options.values().any(|option| option.explicit),
                options.values().any(|option| !option.explicit),
            )
        })
        .collect();
    assert_eq!(user_blocks, BTreeSet::from([(false, false), (true, true)]));
}

#[test]
fn given_language_copies_when_comparing_should_match_canonical_bytes() {
    let canonical = read_manifest_file(CANONICAL_PATH);
    for copy in LANGUAGE_COPIES {
        assert!(
            read_manifest_file(copy) == canonical,
            "{copy} differs from {CANONICAL_PATH}, rerun the resource_options_fixtures example"
        );
    }
}

fn fixture() -> ResourceOptionsFixture {
    serde_json::from_slice(&read_manifest_file(CANONICAL_PATH)).expect("fixture matches the schema")
}

fn read_manifest_file(relative: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "cannot read {}: {error}. Generate it with: cargo run -p iggy_common --example resource_options_fixtures",
            path.display()
        )
    })
}
