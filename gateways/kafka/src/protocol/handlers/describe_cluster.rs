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

//! `DescribeCluster` (API key 60).
//!
//! Answers the cluster id and the one broker Metadata advertises on the same listener. Java
//! `Admin.describeCluster` and Confluent REST Proxy ask this before Metadata.
//!
//! `cluster_authorized_operations` stays at the schema default, `i32::MIN`, even when the
//! client asks for it. Kafka fills it from the cluster ACLs, and this gateway does not compute
//! them, so a Java client reads `authorizedOperations()` as null: unknown, rather than a
//! made-up set.

use bytes::Bytes;
use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
use kafka_protocol::messages::{BrokerId, DescribeClusterRequest, DescribeClusterResponse};
use kafka_protocol::protocol::StrBytes;

use crate::cluster_id::ClusterId;
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_DESCRIBE_CLUSTER, ApiVersionRange, BrokerAdvertise, ConnectionState,
    ERROR_INVALID_REQUEST, ERROR_MISMATCHED_ENDPOINT_TYPE, ERROR_UNSUPPORTED_ENDPOINT_TYPE,
    ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_describe_cluster_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DESCRIBE_CLUSTER,
    min_version: 0,
    max_version: 2,
};

/// Kafka's `EndpointType.BROKER`, the default, and the only kind of endpoint a gateway is.
const ENDPOINT_TYPE_BROKER: i8 = 1;
/// Kafka's `EndpointType.CONTROLLER`.
const ENDPOINT_TYPE_CONTROLLER: i8 = 2;

/// Kafka's text for a controller request sent to a broker, word for word: the endpoint it was
/// sent to, then the one the request wanted.
const MISMATCHED_CONTROLLER_MESSAGE: &str = concat!(
    "The request was sent to an endpoint of type BROKER, ",
    "but we wanted an endpoint of type CONTROLLER"
);

/// The node id this gateway advertises for itself, in Metadata and here alike.
const SELF_NODE_ID: i32 = 1;

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_DESCRIBE_CLUSTER, api_version) {
        return unsupported_version_response(API_KEY_DESCRIBE_CLUSTER, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request = match decode_guarded::<DescribeClusterRequest>(
        api_version,
        body,
        validate_describe_cluster_shape,
    ) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode DescribeCluster request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "DescribeCluster",
            );
        }
    };
    respond_or_close(
        encode_response(api_version, &request, &state.cluster_id, &connection.broker),
        "DescribeCluster",
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(
    version: i16,
    request: &DescribeClusterRequest,
    cluster_id: &ClusterId,
    broker: &BrokerAdvertise,
) -> Result<Bytes> {
    // Kafka's `AuthHelper.computeDescribeClusterResponse`, whose refusals leave the endpoint
    // type at its default. v0 has no endpoint type field, so it decodes as the broker default
    // and never reaches the two refusals.
    let refusal = match request.endpoint_type {
        ENDPOINT_TYPE_BROKER => None,
        ENDPOINT_TYPE_CONTROLLER => Some((
            ERROR_MISMATCHED_ENDPOINT_TYPE,
            StrBytes::from_static_str(MISMATCHED_CONTROLLER_MESSAGE),
        )),
        other => Some((
            ERROR_UNSUPPORTED_ENDPOINT_TYPE,
            StrBytes::from_string(format!("Unsupported endpoint type {other}")),
        )),
    };
    if let Some((error_code, message)) = refusal {
        let response = DescribeClusterResponse::default()
            .with_error_code(error_code)
            .with_error_message(Some(message));
        return encode_message(&response, version, 128);
    }
    let broker_entry = DescribeClusterBroker::default()
        .with_broker_id(BrokerId(SELF_NODE_ID))
        .with_host(StrBytes::from_string(broker.host.clone()))
        .with_port(broker.port);
    let response = DescribeClusterResponse::default()
        .with_endpoint_type(ENDPOINT_TYPE_BROKER)
        .with_cluster_id(StrBytes::from_string(cluster_id.to_string()))
        .with_controller_id(BrokerId(SELF_NODE_ID))
        .with_brokers(vec![broker_entry]);
    encode_message(&response, version, 128)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let response = DescribeClusterResponse::default().with_error_code(error_code);
    encode_message(&response, version, 64)
}

#[cfg(test)]
mod tests {
    use kafka_protocol::protocol::Decodable;

    use super::*;

    fn decode(version: i16, body: Bytes) -> DescribeClusterResponse {
        let mut body = body;
        DescribeClusterResponse::decode(&mut body, version).expect("a DescribeCluster response")
    }

    fn broker() -> BrokerAdvertise {
        BrokerAdvertise {
            host: "gateway.example".to_string(),
            port: 19092,
        }
    }

    #[test]
    fn given_every_version_when_described_should_report_the_id_and_this_broker() {
        let cluster_id = ClusterId::random();
        for version in RANGE.min_version..=RANGE.max_version {
            let body = encode_response(
                version,
                &DescribeClusterRequest::default(),
                &cluster_id,
                &broker(),
            )
            .expect("encode");
            let response = decode(version, body);
            assert_eq!(response.error_code, 0, "v{version}");
            assert_eq!(response.cluster_id.as_str(), cluster_id.as_str());
            assert_eq!(response.controller_id, BrokerId(SELF_NODE_ID));
            assert_eq!(response.cluster_authorized_operations, i32::MIN);
            assert_eq!(response.brokers.len(), 1);
            let entry = &response.brokers[0];
            assert_eq!(entry.broker_id, BrokerId(SELF_NODE_ID));
            assert_eq!(entry.host.as_str(), "gateway.example");
            assert_eq!(entry.port, 19092);
            assert_eq!(entry.rack, None);
            assert!(!entry.is_fenced);
        }
    }

    #[test]
    fn given_the_controller_endpoint_type_when_described_should_answer_mismatched() {
        let request =
            DescribeClusterRequest::default().with_endpoint_type(ENDPOINT_TYPE_CONTROLLER);
        for version in 1..=RANGE.max_version {
            let body = encode_response(version, &request, &ClusterId::random(), &broker())
                .expect("encode");
            let response = decode(version, body);
            assert_eq!(response.error_code, ERROR_MISMATCHED_ENDPOINT_TYPE);
            assert_eq!(
                response.error_message.as_deref(),
                Some(
                    "The request was sent to an endpoint of type BROKER, but we wanted an \
                     endpoint of type CONTROLLER"
                )
            );
            assert_eq!(response.endpoint_type, ENDPOINT_TYPE_BROKER);
            assert_eq!(response.brokers, vec![]);
            assert_eq!(response.cluster_id.as_str(), "");
        }
    }

    #[test]
    fn given_an_unknown_endpoint_type_when_described_should_answer_unsupported() {
        for endpoint_type in [0, 3, -1] {
            let request = DescribeClusterRequest::default().with_endpoint_type(endpoint_type);
            let body =
                encode_response(1, &request, &ClusterId::random(), &broker()).expect("encode");
            let response = decode(1, body);
            assert_eq!(response.error_code, ERROR_UNSUPPORTED_ENDPOINT_TYPE);
            assert_eq!(
                response.error_message.as_deref(),
                Some(format!("Unsupported endpoint type {endpoint_type}").as_str())
            );
            assert_eq!(response.endpoint_type, ENDPOINT_TYPE_BROKER);
        }
    }
}
