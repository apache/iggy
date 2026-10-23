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

//! `DescribeConfigs`, `AlterConfigs` and `IncrementalAlterConfigs` with the bridge off.

use kafka_protocol::messages::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::alter_configs_response::AlterConfigsResponse;
use kafka_protocol::messages::describe_configs_request::{
    DescribeConfigsRequest, DescribeConfigsResource,
};
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResponse;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalAlterConfigsResource,
    AlterableConfig as IncrementalAlterableConfig, IncrementalAlterConfigsRequest,
};
use kafka_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use kafka_protocol::protocol::StrBytes;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::auth::AuthenticatedPrincipal;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::acl::PrincipalPermissions;
use iggy_gateway_kafka::protocol::api::{
    ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION, ERROR_TOPIC_AUTHORIZATION_FAILED, GatewayState,
    is_supported_version, supported_api_ranges,
};
use iggy_gateway_kafka::protocol::handlers::{
    alter_configs, describe_configs, incremental_alter_configs,
};

#[path = "common/codec.rs"]
mod codec;
#[path = "common/wire.rs"]
mod wire;

use wire::{decode, encode};

const DESCRIBE_VERSION: i16 = 4;
const ALTER_VERSION: i16 = 2;
const INCREMENTAL_VERSION: i16 = 1;
/// Kafka `AlterConfigOp.OpType.SET`.
const OP_SET: i8 = 0;

fn state_without_bridge() -> GatewayState {
    GatewayState::new(
        None,
        8 * 1024 * 1024,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    )
}

#[tokio::test]
async fn describe_configs_without_a_bridge_returns_not_controller() {
    let request = DescribeConfigsRequest::default()
        .with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_static_str("orders"))
                .with_configuration_keys(None),
        ])
        .with_include_synonyms(false)
        .with_include_documentation(false);
    let outcome = describe_configs::handle(
        &state_without_bridge(),
        DESCRIBE_VERSION,
        encode(&request, DESCRIBE_VERSION),
    )
    .await;
    let response: DescribeConfigsResponse = decode(
        outcome.expect_response("DescribeConfigs answers"),
        DESCRIBE_VERSION,
    );
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].error_code, ERROR_NOT_CONTROLLER);
    assert_eq!(
        response.results[0].configs,
        [] as [kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
            0]
    );
}

#[tokio::test]
async fn alter_configs_without_a_bridge_returns_not_controller() {
    let request = AlterConfigsRequest::default()
        .with_resources(vec![
            AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_static_str("orders"))
                .with_configs(vec![
                    AlterableConfig::default()
                        .with_name(StrBytes::from_static_str("retention.ms"))
                        .with_value(Some(StrBytes::from_static_str("1000"))),
                ]),
        ])
        .with_validate_only(false);
    let outcome = alter_configs::handle(
        &state_without_bridge(),
        None,
        ALTER_VERSION,
        encode(&request, ALTER_VERSION),
    )
    .await;
    let response: AlterConfigsResponse = decode(
        outcome.expect_response("AlterConfigs answers"),
        ALTER_VERSION,
    );
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, ERROR_NOT_CONTROLLER);
}

fn incremental_resource(name: &str) -> IncrementalAlterConfigsResource {
    IncrementalAlterConfigsResource::default()
        .with_resource_type(2)
        .with_resource_name(StrBytes::from(name.to_string()))
        .with_configs(vec![
            IncrementalAlterableConfig::default()
                .with_name(StrBytes::from_static_str("retention.ms"))
                .with_config_operation(OP_SET)
                .with_value(Some(StrBytes::from_static_str("1000"))),
        ])
}

async fn incremental_alter(
    principal: Option<&AuthenticatedPrincipal>,
    resources: Vec<IncrementalAlterConfigsResource>,
) -> IncrementalAlterConfigsResponse {
    let request = IncrementalAlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(false);
    let outcome = incremental_alter_configs::handle(
        &state_without_bridge(),
        principal,
        INCREMENTAL_VERSION,
        encode(&request, INCREMENTAL_VERSION),
    )
    .await;
    decode(
        outcome.expect_response("IncrementalAlterConfigs answers"),
        INCREMENTAL_VERSION,
    )
}

#[test]
fn incremental_alter_configs_is_advertised_at_v0_and_v1() {
    let range = supported_api_ranges()
        .iter()
        .find(|range| range.api_key == 44)
        .expect("IncrementalAlterConfigs is advertised");
    assert_eq!((range.min_version, range.max_version), (0, 1));
    assert!(is_supported_version(44, 0));
    assert!(is_supported_version(44, 1));
    assert!(!is_supported_version(44, 2));
}

#[tokio::test]
async fn incremental_alter_configs_without_a_bridge_returns_not_controller() {
    let response = incremental_alter(None, vec![incremental_resource("orders")]).await;
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, ERROR_NOT_CONTROLLER);
    assert_eq!(response.responses[0].resource_name.as_str(), "orders");
}

#[tokio::test]
async fn incremental_alter_configs_over_the_cap_is_rejected_even_with_the_bridge_off() {
    const OVER_CAP: usize = 101;
    let resources = (0..OVER_CAP)
        .map(|i| incremental_resource(&format!("topic-{i}")))
        .collect();
    let response = incremental_alter(None, resources).await;
    assert_eq!(response.responses.len(), OVER_CAP);
    assert!(
        response
            .responses
            .iter()
            .all(|r| r.error_code == ERROR_POLICY_VIOLATION)
    );
}

/// The `manage_topics` gate sits before the bridge-availability stub, so a denied principal
/// never reaches `NOT_CONTROLLER`, and an allowed one does.
#[tokio::test]
async fn incremental_alter_configs_requires_manage_topics_before_the_stub() {
    let principal = |manage_topics| AuthenticatedPrincipal {
        username: "alice".to_string(),
        permissions: PrincipalPermissions {
            manage_topics,
            ..PrincipalPermissions::default()
        },
        permissions_known: true,
    };

    let denied = incremental_alter(
        Some(&principal(false)),
        vec![
            incremental_resource("orders"),
            incremental_resource("payments"),
        ],
    )
    .await;
    assert_eq!(denied.responses.len(), 2);
    assert!(
        denied
            .responses
            .iter()
            .all(|r| r.error_code == ERROR_TOPIC_AUTHORIZATION_FAILED)
    );
    assert!(
        denied.responses[0]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("not authorized"))
    );

    let allowed =
        incremental_alter(Some(&principal(true)), vec![incremental_resource("orders")]).await;
    assert_eq!(allowed.responses[0].error_code, ERROR_NOT_CONTROLLER);
}

/// The over-100-distinct-topics cap is checked before the bridge-availability stub: with the
/// bridge off (as every test in this file is), a request that would previously fall straight
/// through to `NOT_CONTROLLER` must still be rejected `POLICY_VIOLATION` first. Before this, the
/// cap lived inside `describe_all`/`alter_all`, past the bridge-off early return, so no
/// bridge-off test could ever reach it.
#[tokio::test]
async fn describe_configs_over_the_cap_is_rejected_even_with_the_bridge_off() {
    const OVER_CAP: usize = 101;
    let resources = (0..OVER_CAP)
        .map(|i| {
            DescribeConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from(format!("topic-{i}")))
                .with_configuration_keys(None)
        })
        .collect();
    let request = DescribeConfigsRequest::default()
        .with_resources(resources)
        .with_include_synonyms(false)
        .with_include_documentation(false);
    let outcome = describe_configs::handle(
        &state_without_bridge(),
        DESCRIBE_VERSION,
        encode(&request, DESCRIBE_VERSION),
    )
    .await;
    let response: DescribeConfigsResponse = decode(
        outcome.expect_response("DescribeConfigs answers"),
        DESCRIBE_VERSION,
    );
    assert_eq!(response.results.len(), OVER_CAP);
    assert!(
        response
            .results
            .iter()
            .all(|result| result.error_code == ERROR_POLICY_VIOLATION)
    );
}

#[tokio::test]
async fn alter_configs_over_the_cap_is_rejected_even_with_the_bridge_off() {
    const OVER_CAP: usize = 101;
    let resources = (0..OVER_CAP)
        .map(|i| {
            AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from(format!("topic-{i}")))
                .with_configs(vec![
                    AlterableConfig::default()
                        .with_name(StrBytes::from_static_str("retention.ms"))
                        .with_value(Some(StrBytes::from_static_str("1000"))),
                ])
        })
        .collect();
    let request = AlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(false);
    let outcome = alter_configs::handle(
        &state_without_bridge(),
        None,
        ALTER_VERSION,
        encode(&request, ALTER_VERSION),
    )
    .await;
    let response: AlterConfigsResponse = decode(
        outcome.expect_response("AlterConfigs answers"),
        ALTER_VERSION,
    );
    assert_eq!(response.responses.len(), OVER_CAP);
    assert!(
        response
            .responses
            .iter()
            .all(|r| r.error_code == ERROR_POLICY_VIOLATION)
    );
}
