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

//! `DescribeConfigs` and `AlterConfigs` against a real `iggy-server`.

use std::sync::Arc;

use kafka_protocol::messages::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::alter_configs_response::AlterConfigsResponse;
use kafka_protocol::messages::describe_configs_request::{
    DescribeConfigsRequest, DescribeConfigsResource,
};
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse,
};
use kafka_protocol::protocol::StrBytes;
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    BrokerAdvertise, ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST, ERROR_INVALID_TOPIC_EXCEPTION,
    ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::{alter_configs, describe_configs};

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
const TOPIC: i8 = 2;
const SOURCE_TOPIC: i8 = 1;
const SOURCE_DEFAULT: i8 = 5;

const fn text(value: &'static str) -> StrBytes {
    StrBytes::from_static_str(value)
}

async fn connected_state(server: &TestServer) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge connects");
    GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        8 * 1024 * 1024,
        false,
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
async fn describe_configs_returns_retention_and_cleanup_and_alter_persists_retention() {
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
    assert_eq!(result.configs.len(), 2);
    let retention = entry(&result.configs, "retention.ms");
    assert_eq!(retention.value.as_ref().map(StrBytes::as_str), Some("-1"));
    assert!(!retention.read_only);
    assert!(!retention.is_sensitive);
    assert_eq!(retention.config_source, SOURCE_DEFAULT);
    assert!(retention.synonyms.is_empty());
    assert!(retention.documentation.is_some());
    let cleanup = entry(&result.configs, "cleanup.policy");
    assert_eq!(cleanup.value.as_ref().map(StrBytes::as_str), Some("delete"));
    assert!(cleanup.read_only);
    assert_eq!(cleanup.config_source, SOURCE_DEFAULT);
    assert!(cleanup.synonyms.is_empty());
    assert!(cleanup.documentation.is_some());

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
    assert!(entry(bare, "retention.ms").synonyms.is_empty());

    let filtered = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource("orders", Some(vec!["cleanup.policy"]))],
        false,
        true,
    )
    .await;
    assert_eq!(filtered.results[0].error_code, ERROR_NONE);
    assert_eq!(filtered.results[0].configs.len(), 1);
    assert_eq!(
        filtered.results[0].configs[0].name.as_str(),
        "cleanup.policy"
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
    assert_eq!(v1.results[0].configs.len(), 2);

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
    assert!(broker.results[0].configs.is_empty());
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
    assert!(missing.results[0].configs.is_empty());

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
