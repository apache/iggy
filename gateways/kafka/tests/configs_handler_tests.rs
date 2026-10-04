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

//! `DescribeConfigs` and `AlterConfigs` with the bridge off.

use kafka_protocol::messages::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::alter_configs_response::AlterConfigsResponse;
use kafka_protocol::messages::describe_configs_request::{
    DescribeConfigsRequest, DescribeConfigsResource,
};
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResponse;
use kafka_protocol::protocol::StrBytes;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{BrokerAdvertise, ERROR_NOT_CONTROLLER, GatewayState};
use iggy_gateway_kafka::protocol::handlers::{alter_configs, describe_configs};

#[path = "common/codec.rs"]
mod codec;
#[path = "common/wire.rs"]
mod wire;

use wire::{decode, encode};

const DESCRIBE_VERSION: i16 = 4;
const ALTER_VERSION: i16 = 2;

fn state_without_bridge() -> GatewayState {
    GatewayState::new(
        BrokerAdvertise::default(),
        None,
        8 * 1024 * 1024,
        false,
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
    assert!(response.results[0].configs.is_empty());
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
