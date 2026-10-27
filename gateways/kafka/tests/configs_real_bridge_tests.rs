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

//! `DescribeConfigs`, `AlterConfigs` and `IncrementalAlterConfigs` against a real `iggy-server`.

use std::sync::Arc;

use kafka_protocol::messages::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::alter_configs_response::AlterConfigsResponse;
use kafka_protocol::messages::describe_configs_request::{
    DescribeConfigsRequest, DescribeConfigsResource,
};
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsSynonym,
};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalAlterConfigsResource,
    AlterableConfig as IncrementalAlterableConfig, IncrementalAlterConfigsRequest,
};
use kafka_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use kafka_protocol::protocol::StrBytes;
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST, ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::{
    alter_configs, describe_configs, incremental_alter_configs,
};

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/wire.rs"]
mod wire;

use iggy_server::TestServer;
use wire::{decode, encode};

const DESCRIBE_V4: i16 = 4;
const DESCRIBE_V1: i16 = 1;
const ALTER_V2: i16 = 2;
const ALTER_V0: i16 = 0;
const INCREMENTAL_V1: i16 = 1;
const INCREMENTAL_V0: i16 = 0;
const TOPIC: i8 = 2;
const SOURCE_TOPIC: i8 = 1;
const SOURCE_DEFAULT: i8 = 5;
/// Kafka `AlterConfigOp.OpType` bytes.
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;
const OP_SUBTRACT: i8 = 3;

/// The entries of one incremental resource: name, operation byte, value.
type Configs = &'static [(&'static str, i8, Option<&'static str>)];

const fn text(value: &'static str) -> StrBytes {
    StrBytes::from_static_str(value)
}

async fn connected_state(server: &TestServer) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge connects");
    GatewayState::new(
        Some(Arc::new(bridge)),
        8 * 1024 * 1024,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
}

async fn create_topic(state: &GatewayState, name: &str) {
    let bridge = state.bridge.as_ref().expect("bridge");
    bridge
        .ensure_stream_and_topic(name, 1)
        .await
        .expect("create topic");
}

fn describe_resource(name: &str, keys: Option<Vec<&'static str>>) -> DescribeConfigsResource {
    DescribeConfigsResource::default()
        .with_resource_type(TOPIC)
        .with_resource_name(StrBytes::from(name.to_string()))
        .with_configuration_keys(keys.map(|keys| keys.into_iter().map(text).collect()))
}

async fn describe(
    state: &GatewayState,
    version: i16,
    resources: Vec<DescribeConfigsResource>,
    include_synonyms: bool,
    include_documentation: bool,
) -> DescribeConfigsResponse {
    let request = DescribeConfigsRequest::default()
        .with_resources(resources)
        .with_include_synonyms(include_synonyms)
        .with_include_documentation(include_documentation);
    let outcome = describe_configs::handle(state, version, encode(&request, version)).await;
    decode(outcome.expect_response("DescribeConfigs answers"), version)
}

fn alter_resource(
    name: &str,
    resource_type: i8,
    configs: &[(&'static str, &'static str)],
) -> AlterConfigsResource {
    AlterConfigsResource::default()
        .with_resource_type(resource_type)
        .with_resource_name(StrBytes::from(name.to_string()))
        .with_configs(
            configs
                .iter()
                .map(|(key, value)| {
                    AlterableConfig::default()
                        .with_name(text(key))
                        .with_value(Some(text(value)))
                })
                .collect(),
        )
}

async fn alter(
    state: &GatewayState,
    version: i16,
    resources: Vec<AlterConfigsResource>,
    validate_only: bool,
) -> AlterConfigsResponse {
    let request = AlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(validate_only);
    let outcome = alter_configs::handle(state, None, version, encode(&request, version)).await;
    decode(outcome.expect_response("AlterConfigs answers"), version)
}

fn incremental_resource(
    name: &str,
    resource_type: i8,
    configs: Configs,
) -> IncrementalAlterConfigsResource {
    IncrementalAlterConfigsResource::default()
        .with_resource_type(resource_type)
        .with_resource_name(StrBytes::from(name.to_string()))
        .with_configs(
            configs
                .iter()
                .map(|(key, operation, value)| {
                    IncrementalAlterableConfig::default()
                        .with_name(text(key))
                        .with_config_operation(*operation)
                        .with_value(value.map(text))
                })
                .collect(),
        )
}

async fn incremental_alter(
    state: &GatewayState,
    version: i16,
    resources: Vec<IncrementalAlterConfigsResource>,
    validate_only: bool,
) -> IncrementalAlterConfigsResponse {
    let request = IncrementalAlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(validate_only);
    let outcome =
        incremental_alter_configs::handle(state, None, version, encode(&request, version)).await;
    decode(
        outcome.expect_response("IncrementalAlterConfigs answers"),
        version,
    )
}

/// One resource, one topic: its error code and message.
async fn incremental_one(
    state: &GatewayState,
    version: i16,
    name: &str,
    configs: Configs,
) -> (i16, String) {
    let response = incremental_alter(
        state,
        version,
        vec![incremental_resource(name, TOPIC, configs)],
        false,
    )
    .await;
    let resource = &response.responses[0];
    (
        resource.error_code,
        resource
            .error_message
            .as_ref()
            .map_or(String::new(), |message| message.as_str().to_string()),
    )
}

/// `retention.ms` as a describe reports it: the value and its config source.
async fn retention(state: &GatewayState, name: &str) -> (String, i8) {
    let described = describe(
        state,
        DESCRIBE_V4,
        vec![describe_resource(name, Some(vec!["retention.ms"]))],
        false,
        false,
    )
    .await;
    let config = &described.results[0].configs[0];
    (
        config
            .value
            .as_ref()
            .map_or(String::new(), |value| value.as_str().to_string()),
        config.config_source,
    )
}

fn entry<'a>(
    configs: &'a [DescribeConfigsResourceResult],
    name: &str,
) -> &'a DescribeConfigsResourceResult {
    configs
        .iter()
        .find(|config| config.name.as_str() == name)
        .unwrap_or_else(|| panic!("missing config {name}"))
}

#[tokio::test]
#[serial]
async fn describe_configs_returns_retention_without_cleanup_and_alter_persists_retention() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let described = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", None)],
        true,
        true,
    )
    .await;
    let result = &described.results[0];
    assert_eq!(result.error_code, ERROR_NONE);
    assert_eq!(result.configs.len(), 1);
    let retention = entry(&result.configs, "retention.ms");
    assert_eq!(retention.value.as_ref().map(StrBytes::as_str), Some("-1"));
    assert!(!retention.read_only);
    assert!(!retention.is_sensitive);
    assert_eq!(retention.config_source, SOURCE_DEFAULT);
    assert_eq!(retention.synonyms, [] as [DescribeConfigsSynonym; 0]);
    assert!(retention.documentation.is_some());

    let without_docs = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", None)],
        false,
        false,
    )
    .await;
    let bare = &without_docs.results[0].configs;
    assert!(entry(bare, "retention.ms").documentation.is_none());
    assert_eq!(
        entry(bare, "retention.ms").synonyms,
        [] as [DescribeConfigsSynonym; 0]
    );

    let filtered = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["cleanup.policy"]))],
        false,
        true,
    )
    .await;
    // Kafka Connect starts when the entry is missing and refuses a topic that reports `delete`.
    assert_eq!(filtered.results[0].error_code, ERROR_NONE);
    assert_eq!(
        filtered.results[0].configs,
        [] as [DescribeConfigsResourceResult; 0]
    );

    let v1 = describe(
        &state,
        DESCRIBE_V1,
        vec![describe_resource("orders", None)],
        false,
        false,
    )
    .await;
    assert_eq!(v1.results[0].error_code, ERROR_NONE);
    assert_eq!(v1.results[0].configs.len(), 1);

    let validated = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &[("retention.ms", "8000")])],
        true,
    )
    .await;
    assert_eq!(validated.responses[0].error_code, ERROR_NONE);
    let still_default = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["retention.ms"]))],
        false,
        false,
    )
    .await;
    let unchanged = &still_default.results[0].configs[0];
    assert_eq!(unchanged.value.as_ref().map(StrBytes::as_str), Some("-1"));
    assert_eq!(unchanged.config_source, SOURCE_DEFAULT);

    let written = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &[("retention.ms", "8000")])],
        false,
    )
    .await;
    assert_eq!(written.responses[0].error_code, ERROR_NONE);
    let after = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["retention.ms"]))],
        false,
        false,
    )
    .await;
    let changed = &after.results[0].configs[0];
    assert_eq!(changed.value.as_ref().map(StrBytes::as_str), Some("8000"));
    assert_eq!(changed.config_source, SOURCE_TOPIC);

    let cleared = alter(
        &state,
        ALTER_V0,
        vec![alter_resource("orders", TOPIC, &[("retention.ms", "-1")])],
        false,
    )
    .await;
    assert_eq!(cleared.responses[0].error_code, ERROR_NONE);
    let never = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["retention.ms"]))],
        false,
        false,
    )
    .await;
    let cleared_entry = &never.results[0].configs[0];
    assert_eq!(
        cleared_entry.value.as_ref().map(StrBytes::as_str),
        Some("-1")
    );
    assert_eq!(cleared_entry.config_source, SOURCE_TOPIC);
}

#[tokio::test]
#[serial]
async fn describe_and_alter_configs_reject_unknown_nontopic_missing_and_invalid_names() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let unknown = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource(
            "orders",
            Some(vec!["retention.ms", "no.such"]),
        )],
        false,
        false,
    )
    .await;
    assert_eq!(unknown.results[0].error_code, ERROR_NONE);
    assert!(unknown.results[0].error_message.is_none());
    assert_eq!(unknown.results[0].configs.len(), 1);
    assert_eq!(unknown.results[0].configs[0].name.as_str(), "retention.ms");

    let broker = describe(
        &state,
        DESCRIBE_V4,
        vec![
            DescribeConfigsResource::default()
                .with_resource_type(4)
                .with_resource_name(StrBytes::from_static_str("1"))
                .with_configuration_keys(None),
            describe_resource("orders", Some(vec!["cleanup.policy"])),
        ],
        false,
        false,
    )
    .await;
    assert_eq!(broker.results[0].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(
        broker.results[0].configs,
        [] as [DescribeConfigsResourceResult; 0]
    );
    assert_eq!(
        broker.results[0]
            .error_message
            .as_ref()
            .map(StrBytes::as_str),
        Some("only topic resources are supported")
    );
    assert_eq!(broker.results[1].error_code, ERROR_NONE);

    let missing = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("missing-topic", None)],
        false,
        false,
    )
    .await;
    assert_eq!(
        missing.results[0].error_code,
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        missing.results[0].configs,
        [] as [DescribeConfigsResourceResult; 0]
    );

    let invalid = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("has a space", None)],
        false,
        false,
    )
    .await;
    assert_eq!(invalid.results[0].error_code, ERROR_INVALID_TOPIC_EXCEPTION);
    let reason = invalid.results[0]
        .error_message
        .as_ref()
        .map_or("", StrBytes::as_str);
    assert!(!reason.contains("has a space"));

    let rejected = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &[("no.such", "1")])],
        false,
    )
    .await;
    assert_eq!(rejected.responses[0].error_code, ERROR_INVALID_CONFIG);
    let still = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["retention.ms"]))],
        false,
        false,
    )
    .await;
    assert_eq!(
        still.results[0].configs[0]
            .value
            .as_ref()
            .map(StrBytes::as_str),
        Some("-1")
    );
    assert_eq!(still.results[0].configs[0].config_source, SOURCE_DEFAULT);

    let readonly = alter(
        &state,
        ALTER_V2,
        vec![alter_resource(
            "orders",
            TOPIC,
            &[("cleanup.policy", "compact")],
        )],
        false,
    )
    .await;
    assert_eq!(readonly.responses[0].error_code, ERROR_INVALID_CONFIG);

    let missing_alter = alter(
        &state,
        ALTER_V2,
        vec![alter_resource(
            "missing-topic",
            TOPIC,
            &[("retention.ms", "1000")],
        )],
        false,
    )
    .await;
    assert_eq!(
        missing_alter.responses[0].error_code,
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    );

    let bad_name = alter(
        &state,
        ALTER_V2,
        vec![alter_resource(
            "has a space",
            TOPIC,
            &[("retention.ms", "1000")],
        )],
        false,
    )
    .await;
    assert_eq!(
        bad_name.responses[0].error_code,
        ERROR_INVALID_TOPIC_EXCEPTION
    );
    let alter_reason = bad_name.responses[0]
        .error_message
        .as_ref()
        .map_or("", StrBytes::as_str);
    assert!(!alter_reason.contains("has a space"));

    let nontopic = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("1", 4, &[("retention.ms", "1000")])],
        false,
    )
    .await;
    assert_eq!(nontopic.responses[0].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(
        nontopic.responses[0]
            .error_message
            .as_ref()
            .map(StrBytes::as_str),
        Some("only topic resources are supported")
    );
}

#[tokio::test]
#[serial]
async fn alter_configs_one_bad_key_skips_only_that_resource() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "kept").await;
    create_topic(&state, "changed").await;

    let altered = alter(
        &state,
        ALTER_V2,
        vec![
            alter_resource("kept", TOPIC, &[("retention.ms", "4000"), ("no.such", "1")]),
            alter_resource("changed", TOPIC, &[("retention.ms", "5000")]),
        ],
        false,
    )
    .await;
    assert_eq!(altered.responses[0].error_code, ERROR_INVALID_CONFIG);
    assert_eq!(altered.responses[1].error_code, ERROR_NONE);

    let described = describe(
        &state,
        DESCRIBE_V4,
        vec![
            describe_resource("kept", Some(vec!["retention.ms"])),
            describe_resource("changed", Some(vec!["retention.ms"])),
        ],
        false,
        false,
    )
    .await;
    assert_eq!(
        described.results[0].configs[0]
            .value
            .as_ref()
            .map(StrBytes::as_str),
        Some("-1")
    );
    assert_eq!(
        described.results[0].configs[0].config_source,
        SOURCE_DEFAULT
    );
    assert_eq!(
        described.results[1].configs[0]
            .value
            .as_ref()
            .map(StrBytes::as_str),
        Some("5000")
    );
    assert_eq!(described.results[1].configs[0].config_source, SOURCE_TOPIC);
}

/// Regression test for the duplicate-resource memoization bug: a second `AlterConfigs`
/// resource entry for an already-seen topic name must not be silently answered from a cached
/// result - its own, different `retention.ms` must never be reported as applied when the write
/// for it never ran.
#[tokio::test]
#[serial]
async fn alter_configs_rejects_every_occurrence_of_a_duplicate_resource_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let altered = alter(
        &state,
        ALTER_V2,
        vec![
            alter_resource("orders", TOPIC, &[("retention.ms", "4000")]),
            alter_resource("orders", TOPIC, &[("retention.ms", "5000")]),
        ],
        false,
    )
    .await;
    assert_eq!(altered.responses.len(), 2);
    assert_eq!(altered.responses[0].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(altered.responses[1].error_code, ERROR_INVALID_REQUEST);

    // Neither occurrence's value was applied - the stored retention is still the default.
    let described = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["retention.ms"]))],
        false,
        false,
    )
    .await;
    assert_eq!(
        described.results[0].configs[0]
            .value
            .as_ref()
            .map(StrBytes::as_str),
        Some("-1")
    );
}

/// The `DescribeConfigs` sibling of the same memoization pattern: a duplicate resource name
/// with a *different* `configuration_keys` filter must not be answered from the first entry's
/// cached, differently-filtered result.
#[tokio::test]
#[serial]
async fn describe_configs_rejects_every_occurrence_of_a_duplicate_resource_name() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let described = describe(
        &state,
        DESCRIBE_V4,
        vec![
            describe_resource("orders", Some(vec!["retention.ms"])),
            describe_resource("orders", Some(vec!["cleanup.policy"])),
        ],
        false,
        false,
    )
    .await;
    assert_eq!(described.results.len(), 2);
    assert_eq!(described.results[0].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(described.results[1].error_code, ERROR_INVALID_REQUEST);
}

/// An empty alter list is not a reset. Kafka would replace the set; this gateway rejects it.
#[tokio::test]
#[serial]
async fn alter_configs_rejects_an_empty_list_and_leaves_retention_unchanged() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let first = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &[("retention.ms", "4000")])],
        false,
    )
    .await;
    assert_eq!(first.responses[0].error_code, ERROR_NONE);

    let reset = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &[])],
        false,
    )
    .await;
    assert_eq!(reset.responses[0].error_code, ERROR_INVALID_CONFIG);
    assert!(
        reset.responses[0]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("empty"))
    );

    let described = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["retention.ms"]))],
        false,
        false,
    )
    .await;
    assert_eq!(
        described.results[0].configs[0]
            .value
            .as_ref()
            .map(StrBytes::as_str),
        Some("4000"),
        "an empty configs list must not clear a previously set retention.ms"
    );
    assert_eq!(described.results[0].configs[0].config_source, SOURCE_TOPIC);
}

#[tokio::test]
#[serial]
async fn incremental_alter_configs_sets_deletes_and_validates_retention() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let validated = incremental_alter(
        &state,
        INCREMENTAL_V1,
        vec![incremental_resource(
            "orders",
            TOPIC,
            &[("retention.ms", OP_SET, Some("8000"))],
        )],
        true,
    )
    .await;
    assert_eq!(validated.responses[0].error_code, ERROR_NONE);
    assert_eq!(
        retention(&state, "orders").await,
        ("-1".to_string(), SOURCE_DEFAULT),
        "validate_only must not write"
    );

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.ms", OP_SET, Some("8000"))]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        retention(&state, "orders").await,
        ("8000".to_string(), SOURCE_TOPIC)
    );

    assert_eq!(
        incremental_one(&state, INCREMENTAL_V1, "orders", &[]).await,
        (ERROR_NONE, String::new()),
        "an empty operation list is a no-op, not a reset"
    );
    assert_eq!(
        retention(&state, "orders").await,
        ("8000".to_string(), SOURCE_TOPIC)
    );

    // `kafka-configs.sh --delete-config` sends an empty value with its delete.
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.ms", OP_DELETE, Some(""))]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        retention(&state, "orders").await,
        ("-1".to_string(), SOURCE_TOPIC),
        "a delete stores never-expire, and the key stays explicitly set"
    );

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V0,
            "orders",
            &[("retention.ms", OP_SET, Some("7000"))]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        retention(&state, "orders").await,
        ("7000".to_string(), SOURCE_TOPIC)
    );
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V0,
            "orders",
            &[("retention.ms", OP_DELETE, None)]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        retention(&state, "orders").await,
        ("-1".to_string(), SOURCE_TOPIC)
    );

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.ms", OP_SET, Some("6000"))]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.ms", OP_DELETE, Some("5000"))]
        )
        .await,
        (ERROR_NONE, String::new()),
        "the value sent with a delete is ignored"
    );
    assert_eq!(
        retention(&state, "orders").await,
        ("-1".to_string(), SOURCE_TOPIC)
    );
}

/// A delete writes never-expire even where nothing was ever set, so the describe source flips
/// to the topic although the value does not change. Kafka writes nothing there.
#[tokio::test]
#[serial]
async fn incremental_alter_configs_delete_on_a_never_altered_topic_marks_retention_explicit() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;
    assert_eq!(
        retention(&state, "orders").await,
        ("-1".to_string(), SOURCE_DEFAULT)
    );

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.ms", OP_DELETE, Some(""))]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        retention(&state, "orders").await,
        ("-1".to_string(), SOURCE_TOPIC)
    );
}

/// Request faults are answered without an Iggy call, so a missing topic still gets
/// `INVALID_REQUEST` for them. Config faults come after the lookup, so a missing topic gets
/// `UNKNOWN_TOPIC_OR_PARTITION` for those. Nothing in this test writes.
#[tokio::test]
#[serial]
async fn incremental_alter_configs_answers_request_faults_then_config_faults() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.ms", OP_SET, Some("4000"))]
        )
        .await,
        (ERROR_NONE, String::new())
    );

    let request_faults: [(&str, Configs, &str); 4] = [
        (
            "orders",
            &[
                ("retention.ms", OP_SET, Some("1")),
                ("retention.ms", OP_SET, Some("2")),
            ],
            "Error due to duplicate config keys",
        ),
        (
            "orders",
            &[("retention.ms", OP_SET, None), ("no.such", OP_SET, None)],
            "Null value not supported for : retention.ms, no.such",
        ),
        (
            "orders",
            &[("retention.ms", 7, Some("1"))],
            "Unknown operations type 7",
        ),
        (
            "missing-topic",
            &[("retention.ms", 7, Some("1"))],
            "Unknown operations type 7",
        ),
    ];
    for (name, configs, message) in request_faults {
        assert_eq!(
            incremental_one(&state, INCREMENTAL_V1, name, configs).await,
            (ERROR_INVALID_REQUEST, message.to_string()),
            "{name}: {configs:?}"
        );
    }

    let config_faults: [(Configs, &str); 6] = [
        (
            &[("retention.ms", OP_APPEND, Some("1"))],
            "Can't APPEND to key retention.ms because its type is not LIST.",
        ),
        (
            &[("no.such", OP_SUBTRACT, Some("1"))],
            "Can't SUBTRACT to key no.such because its type is not LIST.",
        ),
        (
            &[("cleanup.policy", OP_APPEND, Some("compact"))],
            "cleanup.policy is read-only",
        ),
        (
            &[("no.such", OP_SET, Some("1"))],
            "unknown config key 'no.such'",
        ),
        (
            &[("cleanup.policy", OP_SET, Some("compact"))],
            "cleanup.policy is read-only",
        ),
        (
            &[("retention.ms", OP_SET, Some("abc"))],
            "retention.ms must be -1 or a positive count",
        ),
    ];
    for (configs, message) in config_faults {
        let (code, text) = incremental_one(&state, INCREMENTAL_V1, "orders", configs).await;
        assert_eq!(code, ERROR_INVALID_CONFIG, "{configs:?}");
        assert!(text.starts_with(message), "{configs:?}: {text}");
    }

    // `validate_only` runs the same checks, so a dry run reports a fault instead of hiding it.
    let validated = incremental_alter(
        &state,
        INCREMENTAL_V1,
        vec![incremental_resource(
            "orders",
            TOPIC,
            &[
                ("retention.ms", OP_SET, Some("1")),
                ("retention.ms", OP_SET, Some("2")),
            ],
        )],
        true,
    )
    .await;
    assert_eq!(validated.responses[0].error_code, ERROR_INVALID_REQUEST);
    let validated = incremental_alter(
        &state,
        INCREMENTAL_V1,
        vec![incremental_resource(
            "orders",
            TOPIC,
            &[("retention.ms", OP_APPEND, Some("1"))],
        )],
        true,
    )
    .await;
    assert_eq!(validated.responses[0].error_code, ERROR_INVALID_CONFIG);
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "missing-topic",
            &[("no.such", OP_SET, Some("1"))]
        )
        .await
        .0,
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    );

    // A delete of a key this gateway never stored removes nothing, as Kafka answers an
    // override that was never set.
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[
                ("no.such", OP_DELETE, None),
                ("cleanup.policy", OP_DELETE, None)
            ]
        )
        .await,
        (ERROR_NONE, String::new())
    );

    let (code, text) = incremental_one(
        &state,
        INCREMENTAL_V1,
        "has a space",
        &[("retention.ms", OP_SET, Some("1000"))],
    )
    .await;
    assert_eq!(code, ERROR_INVALID_TOPIC_EXCEPTION);
    assert!(!text.contains("has a space"));

    let nontopic = incremental_alter(
        &state,
        INCREMENTAL_V1,
        vec![incremental_resource(
            "1",
            4,
            &[("retention.ms", OP_SET, Some("1000"))],
        )],
        false,
    )
    .await;
    assert_eq!(nontopic.responses[0].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(
        nontopic.responses[0]
            .error_message
            .as_ref()
            .map(StrBytes::as_str),
        Some("only topic resources are supported")
    );

    assert_eq!(
        retention(&state, "orders").await,
        ("4000".to_string(), SOURCE_TOPIC),
        "no refused request may write"
    );
}

#[tokio::test]
#[serial]
async fn incremental_alter_configs_rejects_every_occurrence_of_a_duplicate_resource() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let altered = incremental_alter(
        &state,
        INCREMENTAL_V1,
        vec![
            incremental_resource("orders", TOPIC, &[("retention.ms", OP_SET, Some("4000"))]),
            incremental_resource("orders", TOPIC, &[("retention.ms", OP_SET, Some("5000"))]),
        ],
        false,
    )
    .await;
    assert_eq!(altered.responses.len(), 2);
    for resource in &altered.responses {
        assert_eq!(resource.error_code, ERROR_INVALID_REQUEST);
        assert_eq!(
            resource.error_message.as_ref().map(StrBytes::as_str),
            Some("Each resource must appear at most once.")
        );
    }
    assert_eq!(
        retention(&state, "orders").await,
        ("-1".to_string(), SOURCE_DEFAULT)
    );
}

#[tokio::test]
#[serial]
async fn incremental_alter_configs_one_bad_resource_skips_only_that_resource() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "kept").await;
    create_topic(&state, "changed").await;

    let altered = incremental_alter(
        &state,
        INCREMENTAL_V1,
        vec![
            incremental_resource(
                "kept",
                TOPIC,
                &[
                    ("retention.ms", OP_SET, Some("4000")),
                    ("no.such", OP_APPEND, Some("1")),
                ],
            ),
            incremental_resource("changed", TOPIC, &[("retention.ms", OP_SET, Some("5000"))]),
        ],
        false,
    )
    .await;
    assert_eq!(altered.responses[0].error_code, ERROR_INVALID_CONFIG);
    assert_eq!(altered.responses[1].error_code, ERROR_NONE);
    assert_eq!(
        retention(&state, "kept").await,
        ("-1".to_string(), SOURCE_DEFAULT)
    );
    assert_eq!(
        retention(&state, "changed").await,
        ("5000".to_string(), SOURCE_TOPIC)
    );
}
