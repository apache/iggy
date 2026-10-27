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

//! `IncrementalAlterConfigs` (API key 44).
//!
//! `kafka-configs --alter` on a topic has sent this key since Kafka 2.5. `AlterConfigs` (33)
//! comes from Java `Admin.alterConfigs` before 4.0, which removed it, and from other clients'
//! non-incremental alter calls such as librdkafka's `rd_kafka_AlterConfigs`. Each entry carries
//! one operation on one key, where 33 carries a whole new set. Both end in the same write, `retention.ms` onto the topic's
//! `message_expiry`, behind the same checks. See `docs/CONFIGS.md`.

use std::collections::HashSet;

use bytes::Bytes;
use iggy::prelude::{IggyError, IggyExpiry};
use kafka_protocol::messages::IncrementalAlterConfigsRequest;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{BridgeError, IggyBridge, StreamTopicCache, TopicLoad};
use crate::protocol::api::{
    API_KEY_INCREMENTAL_ALTER_CONFIGS, ApiVersionRange, ERROR_INVALID_CONFIG,
    ERROR_INVALID_REQUEST, ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_CONTROLLER,
    ERROR_POLICY_VIOLATION, ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_AUTHORIZATION_FAILED,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome, REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_incremental_alter_configs_shape;
use crate::protocol::handlers::topic_config::{
    ONLY_TOPIC_RESOURCES, RESOURCE_TYPE_TOPIC, alter_denial, bridge_failure, incremental_ops,
    name_reason, plan_incremental_update, topic_cap_and_duplicates, topic_cap_message,
};
use crate::protocol::handlers::{
    decode_guarded, encode_message, is_supported_version, respond_or_close,
    unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_INCREMENTAL_ALTER_CONFIGS,
    min_version: 0,
    max_version: 1,
};

const API_NAME: &str = "IncrementalAlterConfigs";

/// Kafka's own text for a topic that more than one resource of the request names.
const DUPLICATE_RESOURCE: &str = "Each resource must appear at most once.";

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_INCREMENTAL_ALTER_CONFIGS, api_version) {
        return unsupported_version_response(
            API_KEY_INCREMENTAL_ALTER_CONFIGS,
            api_version,
            |version| encode_error_response(version, ERROR_INVALID_REQUEST),
        );
    }

    let req = match decode_guarded::<IncrementalAlterConfigsRequest>(
        api_version,
        body,
        |version, body| {
            validate_incremental_alter_configs_shape(version, body, state.max_frame_size)
        },
    ) {
        Ok(req) => req,
        Err(error) => {
            tracing::debug!(%error, "Failed to decode IncrementalAlterConfigs request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                API_NAME,
            );
        }
    };

    // Authorization and the cap come before the bridge-availability stub: neither depends on
    // the controller being reachable.
    if let Some(responses) = authorize(principal, &req.resources) {
        return respond_or_close(
            encode_message(&response(responses), api_version, 256),
            API_NAME,
        );
    }
    let (exceeds_cap, duplicate_names) = topic_cap_and_duplicates(&req.resources);
    if exceeds_cap {
        let message = StrBytes::from(topic_cap_message(API_NAME));
        tracing::warn!(
            "IncrementalAlterConfigs request addresses too many distinct topics; rejecting"
        );
        let responses = req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_POLICY_VIOLATION, Some(message.clone())))
            .collect();
        return respond_or_close(
            encode_message(&response(responses), api_version, 256),
            API_NAME,
        );
    }

    let Some(bridge) = &state.bridge else {
        let responses = req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_NOT_CONTROLLER, None))
            .collect();
        return respond_or_close(
            encode_message(&response(responses), api_version, 256),
            API_NAME,
        );
    };

    let deadline = Instant::now() + REQUEST_DEADLINE;
    let responses = alter_all(bridge, &req, &duplicate_names, deadline).await;
    respond_or_close(
        encode_message(&response(responses), api_version, 256),
        API_NAME,
    )
}

/// Denies every resource when [`alter_denial`] says `principal` may not alter topic
/// configuration.
fn authorize(
    principal: Option<&AuthenticatedPrincipal>,
    resources: &[AlterConfigsResource],
) -> Option<Vec<AlterConfigsResourceResponse>> {
    let message = alter_denial(principal, API_NAME)?;
    Some(
        resources
            .iter()
            .map(|resource| {
                resource_error(
                    resource,
                    ERROR_TOPIC_AUTHORIZATION_FAILED,
                    Some(message.clone()),
                )
            })
            .collect(),
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> crate::error::Result<Bytes> {
    let responses = vec![
        AlterConfigsResourceResponse::default()
            .with_error_code(error_code)
            .with_error_message(None)
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_static_str("")),
    ];
    encode_message(&response(responses), version, 128)
}

fn response(responses: Vec<AlterConfigsResourceResponse>) -> IncrementalAlterConfigsResponse {
    IncrementalAlterConfigsResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(responses)
}

async fn alter_all(
    bridge: &IggyBridge,
    req: &IncrementalAlterConfigsRequest,
    duplicate_names: &HashSet<&str>,
    deadline: Instant,
) -> Vec<AlterConfigsResourceResponse> {
    let mut cache = StreamTopicCache::default();
    let mut responses = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    for resource in &req.resources {
        if resource.resource_type == RESOURCE_TYPE_TOPIC
            && duplicate_names.contains(resource.resource_name.as_str())
        {
            responses.push(resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                Some(StrBytes::from_static_str(DUPLICATE_RESOURCE)),
            ));
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "IncrementalAlterConfigs deadline passed; answering remaining resources \
                     retriable instead of starting new Iggy calls"
                );
            }
            responses.push(resource_error(resource, ERROR_REQUEST_TIMED_OUT, None));
            continue;
        }

        let result = alter_one(bridge, &mut cache, resource, req.validate_only, deadline).await;
        if Instant::now() >= deadline {
            deadline_exceeded = true;
        }
        responses.push(result);
    }
    responses
}

async fn alter_one(
    bridge: &IggyBridge,
    cache: &mut StreamTopicCache,
    resource: &AlterConfigsResource,
    validate_only: bool,
    deadline: Instant,
) -> AlterConfigsResourceResponse {
    if resource.resource_type != RESOURCE_TYPE_TOPIC {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from_static_str(ONLY_TOPIC_RESOURCES)),
        );
    }
    let name = resource.resource_name.as_str();
    if let Err(error) = validate_kafka_topic_name("kafka_topic", name) {
        return resource_error(
            resource,
            ERROR_INVALID_TOPIC_EXCEPTION,
            Some(StrBytes::from(name_reason(&error))),
        );
    }

    // Request faults need no Iggy call, so they are answered before the topic lookup. Config
    // faults come after it, as in `AlterConfigs`: a missing topic is never `INVALID_CONFIG`.
    let ops = match incremental_ops(resource.configs.iter().map(|config| {
        (
            config.name.as_str(),
            config.config_operation,
            config.value.as_ref().map(StrBytes::as_str),
        )
    })) {
        Ok(ops) => ops,
        Err(fault) => {
            return resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                Some(StrBytes::from(fault.message())),
            );
        }
    };

    match cache.lookup(bridge, name, deadline, API_NAME).await {
        TopicLoad::Found(_) => {}
        TopicLoad::Missing => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None);
        }
        TopicLoad::Failed(error) => {
            let (code, message) = bridge_failure(error, API_NAME, "altering");
            return resource_error(resource, code, message);
        }
        TopicLoad::NotStarted | TopicLoad::TimedOut => {
            return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None);
        }
    }

    let expiry = match plan_incremental_update(&ops) {
        Ok(expiry) => expiry,
        Err(fault) => {
            return resource_error(
                resource,
                ERROR_INVALID_CONFIG,
                Some(StrBytes::from(fault.message())),
            );
        }
    };
    if validate_only {
        return resource_error(resource, ERROR_NONE, None);
    }
    // Nothing named `retention.ms`, so there is nothing to store. Kafka answers an empty
    // operation list, and a delete of an override that was never set, the same way.
    let Some(expiry) = expiry else {
        return resource_error(resource, ERROR_NONE, None);
    };

    write_expiry(bridge, resource, expiry, deadline).await
}

async fn write_expiry(
    bridge: &IggyBridge,
    resource: &AlterConfigsResource,
    expiry: IggyExpiry,
    deadline: Instant,
) -> AlterConfigsResourceResponse {
    let name = resource.resource_name.as_str();
    // Not started past the deadline and not wrapped in `timeout_at`: dropping the future does not
    // abort the SDK call. The bridge bounds its own round trip with `with_request_timeout`, so a
    // write can still commit after this handler answered REQUEST_TIMED_OUT.
    if Instant::now() >= deadline {
        tracing::warn!(
            resource = name,
            "IncrementalAlterConfigs deadline passed before update_topic; not starting the write"
        );
        return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None);
    }
    match bridge.update_kafka_topic_message_expiry(name, expiry).await {
        Ok(()) | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => {
            resource_error(resource, ERROR_NONE, None)
        }
        Err(error) => {
            let (code, message) = bridge_failure(&error, API_NAME, "altering");
            resource_error(resource, code, message)
        }
    }
}

fn resource_error(
    resource: &AlterConfigsResource,
    error_code: i16,
    error_message: Option<StrBytes>,
) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse::default()
        .with_error_code(error_code)
        .with_error_message(error_message)
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::acl::PrincipalPermissions;

    fn principal(manage_topics: bool, permissions_known: bool) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            username: "alice".to_string(),
            permissions: PrincipalPermissions {
                manage_topics,
                ..PrincipalPermissions::default()
            },
            permissions_known,
        }
    }

    fn resource(name: &str) -> AlterConfigsResource {
        AlterConfigsResource::default()
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from(name.to_string()))
    }

    #[test]
    fn no_principal_is_not_gated() {
        assert!(authorize(None, &[resource("orders")]).is_none());
    }

    #[test]
    fn a_principal_with_manage_topics_is_allowed() {
        let principal = principal(true, true);
        assert!(authorize(Some(&principal), &[resource("orders")]).is_none());
    }

    #[test]
    fn a_principal_without_manage_topics_is_denied_every_resource() {
        let principal = principal(false, true);
        let responses = authorize(
            Some(&principal),
            &[resource("orders"), resource("payments")],
        )
        .expect("denied");
        assert_eq!(responses.len(), 2);
        assert!(
            responses
                .iter()
                .all(|r| r.error_code == ERROR_TOPIC_AUTHORIZATION_FAILED)
        );
    }

    #[test]
    fn unread_permissions_fail_closed() {
        let principal = principal(true, false);
        let responses = authorize(Some(&principal), &[resource("orders")]).expect("denied");
        assert_eq!(responses[0].error_code, ERROR_TOPIC_AUTHORIZATION_FAILED);
    }

    #[tokio::test]
    async fn given_a_passed_deadline_when_writing_should_answer_timed_out_without_calling_iggy() {
        let bridge = IggyBridge::offline();
        let response = write_expiry(
            &bridge,
            &resource("orders"),
            IggyExpiry::NeverExpire,
            Instant::now(),
        )
        .await;
        // A write started on the offline bridge fails fast as NOT_LEADER_OR_FOLLOWER instead.
        assert_eq!(response.error_code, ERROR_REQUEST_TIMED_OUT);
        assert!(response.error_message.is_none());
    }
}
