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

use iggy::prelude::{Durability, Identifier, TopicClient, TopicCreateOptions};
use iggy_gateway_kafka::bridge::{IggyBridge, IggyBridgeConfig, KafkaTopicSettings, TopicDefaults};
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

use iggy_server::{TestServer, raw_client};
use wire::{decode, encode};

const DESCRIBE_V4: i16 = 4;
const DESCRIBE_V1: i16 = 1;
const ALTER_V2: i16 = 2;
const ALTER_V0: i16 = 0;
const INCREMENTAL_V1: i16 = 1;
const INCREMENTAL_V0: i16 = 0;
const TOPIC: i8 = 2;
const BROKER: i8 = 4;
const BROKER_LOGGER: i8 = 8;
/// Every topic key of Kafka 4.4 but `cleanup.policy`.
const TOPIC_KEYS_REPORTED: usize = 36;
const SOURCE_TOPIC: i8 = 1;
const SOURCE_STATIC_BROKER: i8 = 4;
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
    state_with(server.test_config()).await
}

async fn state_with(config: IggyBridgeConfig) -> GatewayState {
    let bridge = IggyBridge::connect(config).await.expect("bridge connects");
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

/// `retention.ms` and `retention.bytes` as a describe reports them: each value and its source.
async fn stored(state: &GatewayState, name: &str) -> [(String, i8); 2] {
    let described = describe(
        state,
        DESCRIBE_V4,
        vec![describe_resource(
            name,
            Some(vec!["retention.ms", "retention.bytes"]),
        )],
        false,
        false,
    )
    .await;
    let configs = &described.results[0].configs;
    ["retention.ms", "retention.bytes"].map(|key| {
        let config = entry(configs, key);
        (
            config
                .value
                .as_ref()
                .map_or(String::new(), |value| value.as_str().to_string()),
            config.config_source,
        )
    })
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

/// Iggy does not compact, so a policy without `delete` keeps every record: the alter drops the
/// expiry and the size cap, whatever the retention keys say. The policy itself is not stored, so
/// a later alter of the retention must name it.
#[tokio::test]
#[serial]
async fn alters_to_compact_keep_every_record_and_accept_keys_iggy_ignores() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;
    let expiring = [("retention.ms", "60000"), ("retention.bytes", "2147483648")];

    let set = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &expiring)],
        false,
    )
    .await;
    assert_eq!(set.responses[0].error_code, ERROR_NONE);
    let compacted = alter(
        &state,
        ALTER_V2,
        vec![alter_resource(
            "orders",
            TOPIC,
            &[
                ("cleanup.policy", "compact"),
                ("retention.ms", "60000"),
                ("min.insync.replicas", "1"),
                ("compression.type", "lz4"),
            ],
        )],
        false,
    )
    .await;
    assert_eq!(compacted.responses[0].error_code, ERROR_NONE);
    assert_eq!(
        stored(&state, "orders").await,
        [
            ("-1".to_string(), SOURCE_TOPIC),
            ("-1".to_string(), SOURCE_TOPIC)
        ]
    );
    let described = describe(
        &state,
        DESCRIBE_V4,
        vec![describe_resource(
            "orders",
            Some(vec![
                "cleanup.policy",
                "min.insync.replicas",
                "compression.type",
            ]),
        )],
        false,
        false,
    )
    .await;
    let reported: Vec<(&str, Option<&str>)> = described.results[0]
        .configs
        .iter()
        .map(|config| (config.name.as_str(), config.value.as_deref()))
        .collect();
    assert_eq!(
        reported,
        [
            ("min.insync.replicas", Some("1")),
            ("compression.type", Some("producer"))
        ],
        "the policy is not reported, min.insync.replicas reports the majority of one node, and \
         ignored keys report the Kafka default"
    );

    let refused = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &expiring)],
        false,
    )
    .await;
    assert_eq!(
        refused.responses[0].error_code, ERROR_INVALID_CONFIG,
        "the topic keeps every record, and the alter does not name the policy"
    );
    assert!(
        refused.responses[0]
            .error_message
            .as_ref()
            .is_some_and(|text| text.as_str().contains("names cleanup.policy")),
        "{:?}",
        refused.responses[0].error_message
    );
    assert_eq!(
        stored(&state, "orders").await,
        [
            ("-1".to_string(), SOURCE_TOPIC),
            ("-1".to_string(), SOURCE_TOPIC)
        ]
    );
    let named = [("cleanup.policy", "delete"), expiring[0], expiring[1]];
    let set = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &named)],
        false,
    )
    .await;
    assert_eq!(set.responses[0].error_code, ERROR_NONE);
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("cleanup.policy", OP_SUBTRACT, Some("delete"))],
        )
        .await,
        (ERROR_NONE, String::new()),
        "an empty policy keeps every record too"
    );
    assert_eq!(
        stored(&state, "orders").await,
        [
            ("-1".to_string(), SOURCE_TOPIC),
            ("-1".to_string(), SOURCE_TOPIC)
        ]
    );
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[
                ("cleanup.policy", OP_SET, Some("compact,delete")),
                ("retention.ms", OP_SET, Some("60000"))
            ],
        )
        .await,
        (ERROR_NONE, String::new()),
    );
    assert_eq!(
        stored(&state, "orders").await[0],
        ("60000".to_string(), SOURCE_TOPIC),
        "with delete, retention applies"
    );
}

#[tokio::test]
#[serial]
async fn describe_configs_returns_every_key_but_cleanup_and_alter_persists_retention() {
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
    assert_eq!(result.configs.len(), TOPIC_KEYS_REPORTED);
    let retention = entry(&result.configs, "retention.ms");
    assert_eq!(retention.value.as_ref().map(StrBytes::as_str), Some("-1"));
    assert!(!retention.read_only);
    assert!(!retention.is_sensitive);
    assert_eq!(retention.config_source, SOURCE_DEFAULT);
    assert_eq!(
        retention.synonyms,
        [DescribeConfigsSynonym::default()
            .with_name(StrBytes::from_static_str("retention.ms"))
            .with_value(Some(StrBytes::from_static_str("-1")))
            .with_source(SOURCE_DEFAULT)],
        "include_synonyms lists the entry itself"
    );
    assert!(retention.documentation.is_some());
    let retention_bytes = entry(&result.configs, "retention.bytes");
    assert_eq!(
        retention_bytes.value.as_ref().map(StrBytes::as_str),
        Some("-1")
    );
    assert!(!retention_bytes.read_only);
    assert_eq!(retention_bytes.config_source, SOURCE_DEFAULT);
    let segment = entry(&result.configs, "segment.bytes");
    assert_eq!(
        segment.value.as_ref().map(StrBytes::as_str),
        Some("1073741824")
    );
    assert!(segment.read_only);
    let min_isr = entry(&result.configs, "min.insync.replicas");
    assert_eq!(
        min_isr.value.as_ref().map(StrBytes::as_str),
        Some("1"),
        "the majority of the one node that stores the topic"
    );
    assert!(!min_isr.read_only, "accepted up to that majority");
    assert_eq!(min_isr.config_source, SOURCE_DEFAULT);
    let largest = entry(&result.configs, "max.message.bytes");
    assert_eq!(
        largest.value.as_ref().map(StrBytes::as_str),
        Some("8388608"),
        "the largest batch that Produce takes, the 8 MiB frame"
    );
    assert!(!largest.read_only);
    let timestamps = entry(&result.configs, "message.timestamp.type");
    assert!(timestamps.read_only, "accepted only at the default");
    assert!(
        result
            .configs
            .iter()
            .all(|config| config.name.as_str() != "cleanup.policy")
    );

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
    assert_eq!(v1.results[0].configs.len(), TOPIC_KEYS_REPORTED);

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
async fn describe_and_alter_configs_reject_unknown_unsupported_missing_and_invalid_names() {
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

    let other_resource = |resource_type: i8, name: &'static str| {
        DescribeConfigsResource::default()
            .with_resource_type(resource_type)
            .with_resource_name(StrBytes::from_static_str(name))
            .with_configuration_keys(None)
    };
    let broker = describe(
        &state,
        DESCRIBE_V4,
        vec![
            other_resource(BROKER, "1"),
            other_resource(BROKER, "7"),
            other_resource(BROKER_LOGGER, "1"),
            describe_resource("orders", Some(vec!["cleanup.policy"])),
        ],
        false,
        false,
    )
    .await;
    assert_eq!(broker.results[0].error_code, ERROR_NONE);
    let auto_create = entry(&broker.results[0].configs, "auto.create.topics.enable");
    assert_eq!(
        auto_create.value.as_ref().map(StrBytes::as_str),
        Some("false")
    );
    let min_isr = entry(&broker.results[0].configs, "min.insync.replicas");
    assert_eq!(min_isr.value.as_ref().map(StrBytes::as_str), Some("1"));
    // Iggy keeps a topic on every node by default, and this cluster has one.
    let factor = entry(&broker.results[0].configs, "default.replication.factor");
    assert_eq!(factor.value.as_ref().map(StrBytes::as_str), Some("1"));
    assert_eq!(factor.config_source, SOURCE_STATIC_BROKER);
    assert_eq!(broker.results[1].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(
        broker.results[1]
            .error_message
            .as_ref()
            .map(StrBytes::as_str),
        Some("Unexpected broker id, expected 1 or empty string, but received 7")
    );
    assert_eq!(broker.results[2].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(
        broker.results[2].configs,
        [] as [DescribeConfigsResourceResult; 0]
    );
    assert_eq!(
        broker.results[2]
            .error_message
            .as_ref()
            .map(StrBytes::as_str),
        Some("only topic and broker resources are supported")
    );
    assert_eq!(broker.results[3].error_code, ERROR_NONE);

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
            &[("message.timestamp.type", "LogAppendTime")],
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

/// `AlterConfigs` replaces the whole configuration, as in Kafka: a stored key the resource
/// leaves out goes back to its default, and an empty list resets every stored key.
#[tokio::test]
#[serial]
async fn alter_configs_replaces_the_whole_set_so_an_empty_list_resets_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;

    let first = alter(
        &state,
        ALTER_V2,
        vec![alter_resource(
            "orders",
            TOPIC,
            &[("retention.ms", "4000"), ("retention.bytes", "2147483648")],
        )],
        false,
    )
    .await;
    assert_eq!(first.responses[0].error_code, ERROR_NONE);
    assert_eq!(
        stored(&state, "orders").await,
        [
            ("4000".to_string(), SOURCE_TOPIC),
            ("2147483648".to_string(), SOURCE_TOPIC)
        ]
    );

    let bytes_only = alter(
        &state,
        ALTER_V2,
        vec![alter_resource(
            "orders",
            TOPIC,
            &[("retention.bytes", "2147483648")],
        )],
        false,
    )
    .await;
    assert_eq!(bytes_only.responses[0].error_code, ERROR_NONE);
    assert_eq!(
        stored(&state, "orders").await,
        [
            ("-1".to_string(), SOURCE_DEFAULT),
            ("2147483648".to_string(), SOURCE_TOPIC)
        ],
        "a stored key the resource leaves out goes back to its default"
    );

    let reset = alter(
        &state,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &[])],
        false,
    )
    .await;
    assert_eq!(reset.responses[0].error_code, ERROR_NONE);
    assert_eq!(
        stored(&state, "orders").await,
        [
            ("-1".to_string(), SOURCE_DEFAULT),
            ("-1".to_string(), SOURCE_DEFAULT)
        ],
        "an empty list resets every stored key"
    );

    let null_value = alter(
        &state,
        ALTER_V2,
        vec![
            AlterConfigsResource::default()
                .with_resource_type(TOPIC)
                .with_resource_name(text("orders"))
                .with_configs(vec![
                    AlterableConfig::default()
                        .with_name(text("retention.ms"))
                        .with_value(None),
                ]),
        ],
        false,
    )
    .await;
    assert_eq!(null_value.responses[0].error_code, ERROR_INVALID_REQUEST);
    assert_eq!(
        null_value.responses[0]
            .error_message
            .as_ref()
            .map(StrBytes::as_str),
        Some("Null value not supported for : retention.ms")
    );
}

/// `retention.bytes` caps each partition, so Iggy stores it times the partition count.
/// `segment.bytes` is fixed when Iggy creates the topic, so an alter accepts only its value.
#[tokio::test]
#[serial]
async fn alter_configs_store_retention_bytes_per_partition_and_keep_the_segment_size() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    state
        .bridge
        .as_ref()
        .expect("bridge")
        .ensure_stream_and_topic("orders", 3)
        .await
        .expect("create topic");

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.bytes", OP_SET, Some("1073741824"))]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        stored(&state, "orders").await[1],
        ("1073741824".to_string(), SOURCE_TOPIC),
        "the describe reports the cap of one partition, not of the topic"
    );

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.bytes", OP_SET, Some("1048576"))],
        )
        .await,
        (ERROR_NONE, String::new()),
        "Kafka accepts a cap below one segment"
    );
    assert_eq!(
        stored(&state, "orders").await[1],
        ("357913941".to_string(), SOURCE_TOPIC),
        "Iggy refuses a topic cap below one segment, so the three partitions share one segment"
    );
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.bytes", OP_SET, Some("0"))],
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        stored(&state, "orders").await[1],
        ("357913941".to_string(), SOURCE_TOPIC),
        "0 keeps one segment, and never lifts the cap"
    );

    let (code, text) = incremental_one(
        &state,
        INCREMENTAL_V1,
        "orders",
        &[("segment.bytes", OP_SET, Some("67108864"))],
    )
    .await;
    assert_eq!(code, ERROR_INVALID_CONFIG);
    assert!(text.contains("only when it creates a topic"), "{text}");
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[
                ("segment.bytes", OP_SET, Some("1073741824")),
                ("min.insync.replicas", OP_SET, Some("1")),
                ("cleanup.policy", OP_SET, Some("delete")),
            ]
        )
        .await,
        (ERROR_NONE, String::new()),
        "a key set to the value the gateway reports changes nothing"
    );

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.bytes", OP_DELETE, None)]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        stored(&state, "orders").await[1],
        ("-1".to_string(), SOURCE_DEFAULT)
    );
}

/// The gateway defaults apply to a topic created without the keys. A describe reports them as
/// broker settings, as Kafka reports a topic that inherits `log.retention.ms`, and a delete of an
/// override brings them back.
#[tokio::test]
#[serial]
async fn gateway_defaults_apply_to_new_topics_and_come_back_on_a_delete() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = state_with(IggyBridgeConfig {
        topic_defaults: TopicDefaults {
            retention_ms: Some(604_800_000),
            retention_bytes: Some(2_147_483_648),
            durability: Durability::Persisted,
        },
        ..server.test_config()
    })
    .await;
    state
        .bridge
        .as_ref()
        .expect("bridge")
        .create_kafka_topic("orders", 2)
        .await
        .expect("create topic");

    let defaults = [
        ("604800000".to_string(), SOURCE_STATIC_BROKER),
        ("2147483648".to_string(), SOURCE_STATIC_BROKER),
    ];
    assert_eq!(stored(&state, "orders").await, defaults);

    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[("retention.ms", OP_SET, Some("1000"))]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        stored(&state, "orders").await[0],
        ("1000".to_string(), SOURCE_TOPIC)
    );
    assert_eq!(
        incremental_one(
            &state,
            INCREMENTAL_V1,
            "orders",
            &[
                ("retention.ms", OP_DELETE, None),
                ("retention.bytes", OP_DELETE, None)
            ]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    assert_eq!(
        stored(&state, "orders").await,
        defaults,
        "a delete takes each key back to the gateway default"
    );

    let details = raw_client(&server)
        .await
        .get_topic(
            &Identifier::named("kafka").expect("stream"),
            &Identifier::named("orders").expect("topic"),
        )
        .await
        .expect("get_topic call")
        .expect("the topic exists");
    let created = TopicCreateOptions::from_resource_options(&details.options);
    assert_eq!(created.durability, Durability::Persisted);
    assert_eq!(created.consumer_offset_durability, Durability::Persisted);
    assert_eq!(
        u64::from(details.resolved_options().max_topic_size),
        2 * 2_147_483_648,
        "Iggy holds the cap of both partitions"
    );
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
        ("-1".to_string(), SOURCE_DEFAULT),
        "a delete takes retention.ms back to its default"
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
        ("-1".to_string(), SOURCE_DEFAULT)
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
        ("-1".to_string(), SOURCE_DEFAULT)
    );
}

/// A delete of a `retention.ms` that was never set writes nothing, as in Kafka, so the describe
/// source stays the default.
#[tokio::test]
#[serial]
async fn incremental_alter_configs_delete_on_a_never_altered_topic_writes_nothing() {
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
        ("-1".to_string(), SOURCE_DEFAULT)
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
            &[("cleanup.policy", OP_APPEND, Some("Compact"))],
            "Invalid value Compact for configuration cleanup.policy: String must be one of: \
             compact, delete",
        ),
        (
            &[("no.such", OP_SET, Some("1"))],
            "Unknown topic config name: no.such",
        ),
        (
            &[("message.timestamp.type", OP_SET, Some("LogAppendTime"))],
            "Invalid value LogAppendTime for configuration message.timestamp.type",
        ),
        (
            &[("retention.ms", OP_SET, Some("abc"))],
            "Invalid value abc for configuration retention.ms: Not a number of type LONG",
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

/// A delete of a key that no client set changes nothing, as in Kafka, also once an operator sets a
/// gateway default: the topic keeps the default it was created with. A legacy `AlterConfigs` that
/// leaves out a segment size set at create keeps that size, so it can still change the retention.
#[tokio::test]
#[serial]
async fn alters_leave_unset_keys_and_a_replace_keeps_a_segment_size_set_at_create() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    create_topic(&state, "orders").await;
    let with_defaults = state_with(IggyBridgeConfig {
        topic_defaults: TopicDefaults {
            retention_ms: Some(604_800_000),
            retention_bytes: Some(2_147_483_648),
            durability: Durability::Replicated,
        },
        ..server.test_config()
    })
    .await;

    assert_eq!(
        incremental_one(
            &with_defaults,
            INCREMENTAL_V1,
            "orders",
            &[
                ("retention.ms", OP_DELETE, None),
                ("retention.bytes", OP_DELETE, None)
            ]
        )
        .await,
        (ERROR_NONE, String::new())
    );
    let replaced = alter(
        &with_defaults,
        ALTER_V2,
        vec![alter_resource("orders", TOPIC, &[])],
        false,
    )
    .await;
    assert_eq!(replaced.responses[0].error_code, ERROR_NONE);
    assert_eq!(
        stored(&with_defaults, "orders").await,
        [
            ("-1".to_string(), SOURCE_DEFAULT),
            ("-1".to_string(), SOURCE_DEFAULT)
        ],
        "no client set them, so neither takes the new gateway default"
    );

    let segmented = KafkaTopicSettings {
        segment_size: Some(64 * 1024 * 1024),
        ..KafkaTopicSettings::default()
    };
    state
        .bridge
        .as_ref()
        .expect("bridge")
        .create_kafka_topic_with("segmented", 1, &segmented)
        .await
        .expect("create topic");
    let replaced = alter(
        &state,
        ALTER_V2,
        vec![alter_resource(
            "segmented",
            TOPIC,
            &[("retention.ms", "60000")],
        )],
        false,
    )
    .await;
    assert_eq!(
        replaced.responses[0].error_code, ERROR_NONE,
        "a replace keeps the segment size that Iggy cannot reset"
    );
    assert_eq!(
        stored(&state, "segmented").await[0],
        ("60000".to_string(), SOURCE_TOPIC)
    );
    let (code, _) = incremental_one(
        &state,
        INCREMENTAL_V1,
        "segmented",
        &[("segment.bytes", OP_DELETE, None)],
    )
    .await;
    assert_eq!(
        code, ERROR_INVALID_CONFIG,
        "an explicit delete is still refused"
    );
}
