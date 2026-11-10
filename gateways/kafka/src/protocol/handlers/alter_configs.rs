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

//! `AlterConfigs` (API key 33).

use std::collections::HashSet;

use bytes::Bytes;
use kafka_protocol::messages::AlterConfigsRequest;
use kafka_protocol::messages::alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::alter_configs_response::{
    AlterConfigsResourceResponse, AlterConfigsResponse,
};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{IggyBridge, StreamTopicCache, TopicLoad};
use crate::protocol::api::{
    API_KEY_ALTER_CONFIGS, ApiVersionRange, ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION,
    ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_AUTHORIZATION_FAILED, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    GatewayState, HandleOutcome, REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_alter_configs_shape;
use crate::protocol::handlers::topic_config::keys::ConfigContext;
use crate::protocol::handlers::topic_config::{
    ONLY_TOPIC_RESOURCES, RESOURCE_TYPE_TOPIC, alter_denial, bridge_failure, name_reason,
    null_values, plan_replace, topic_cap_and_duplicates, topic_cap_message, write_alter,
};
use crate::protocol::handlers::{
    decode_guarded, encode_message, is_supported_version, respond_or_close,
    unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_ALTER_CONFIGS,
    min_version: 0,
    max_version: 2,
};

pub async fn handle(
    state: &GatewayState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_ALTER_CONFIGS, api_version) {
        return unsupported_version_response(API_KEY_ALTER_CONFIGS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<AlterConfigsRequest>(api_version, body, |version, body| {
        validate_alter_configs_shape(version, body, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            tracing::debug!(%error, "Failed to decode AlterConfigs request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "AlterConfigs",
            );
        }
    };

    // Checked before the bridge-availability stub, not inside `alter_all`: a cap or an
    // authorization decision does not depend on the controller being reachable, and a check
    // that only runs once the bridge is up is never exercised by a serverless/stub test.
    if let Some(responses) = authorize(principal, &req.resources) {
        return respond_or_close(
            encode_message(&response(responses), api_version, 256),
            "AlterConfigs",
        );
    }
    let (exceeds_cap, duplicate_names) = topic_cap_and_duplicates(&req.resources);
    if exceeds_cap {
        let message = StrBytes::from(topic_cap_message("AlterConfigs"));
        tracing::warn!("AlterConfigs request addresses too many distinct topics; rejecting");
        let responses = req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_POLICY_VIOLATION, Some(message.clone())))
            .collect();
        return respond_or_close(
            encode_message(&response(responses), api_version, 256),
            "AlterConfigs",
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
            "AlterConfigs",
        );
    };

    let deadline = Instant::now() + REQUEST_DEADLINE;
    let alter = Alter {
        bridge,
        partition_growth: &state.partition_growth,
        context: ConfigContext::of(bridge, state.max_frame_size),
        deadline,
    };
    let responses = alter_all(&alter, &req, &duplicate_names).await;
    respond_or_close(
        encode_message(&response(responses), api_version, 256),
        "AlterConfigs",
    )
}

/// Denies every resource when [`alter_denial`] says `principal` may not alter topic
/// configuration.
fn authorize(
    principal: Option<&AuthenticatedPrincipal>,
    resources: &[AlterConfigsResource],
) -> Option<Vec<AlterConfigsResourceResponse>> {
    let message = alter_denial(principal, "AlterConfigs")?;
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

fn response(responses: Vec<AlterConfigsResourceResponse>) -> AlterConfigsResponse {
    AlterConfigsResponse::default()
        .with_throttle_time_ms(0)
        .with_responses(responses)
}

/// What every resource of one request alters through.
struct Alter<'a> {
    bridge: &'a IggyBridge,
    partition_growth: &'a Mutex<()>,
    context: ConfigContext<'a>,
    deadline: Instant,
}

async fn alter_all(
    alter: &Alter<'_>,
    req: &AlterConfigsRequest,
    duplicate_names: &HashSet<&str>,
) -> Vec<AlterConfigsResourceResponse> {
    let deadline = alter.deadline;
    let mut cache = StreamTopicCache::default();
    let mut responses = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    for resource in &req.resources {
        if resource.resource_type == RESOURCE_TYPE_TOPIC
            && duplicate_names.contains(resource.resource_name.as_str())
        {
            responses.push(resource_error(resource, ERROR_INVALID_REQUEST, None));
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "AlterConfigs deadline passed; answering remaining resources retriable \
                     instead of starting new Iggy calls"
                );
            }
            responses.push(resource_error(resource, ERROR_REQUEST_TIMED_OUT, None));
            continue;
        }

        let result = alter_one(alter, &mut cache, resource, req.validate_only).await;
        if Instant::now() >= deadline {
            deadline_exceeded = true;
        }
        responses.push(result);
    }
    responses
}

async fn alter_one(
    alter: &Alter<'_>,
    cache: &mut StreamTopicCache,
    resource: &AlterConfigsResource,
    validate_only: bool,
) -> AlterConfigsResourceResponse {
    let (bridge, deadline) = (alter.bridge, alter.deadline);
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

    let configs = || {
        resource.configs.iter().map(|config| {
            (
                config.name.as_str(),
                config.value.as_ref().map(StrBytes::as_str),
            )
        })
    };
    // A request fault, as Kafka's broker answers it before it forwards the alter.
    if let Some(fault) = null_values(configs()) {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from(fault.message())),
        );
    }

    // Existence is checked before config keys. A missing topic is
    // UNKNOWN_TOPIC_OR_PARTITION even when a key would also be invalid.
    // A deadline that has already passed must not start the lockstep Iggy call.
    let topic = match cache.lookup(bridge, name, deadline, "AlterConfigs").await {
        TopicLoad::Found(topic) => *topic,
        TopicLoad::Missing => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None);
        }
        TopicLoad::Failed(error) => {
            let (code, message) = bridge_failure(error, "AlterConfigs", "altering");
            return resource_error(resource, code, message);
        }
        TopicLoad::NotStarted | TopicLoad::TimedOut => {
            return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None);
        }
    };

    // Kafka's AlterConfigs replaces the whole configuration, so a stored key the resource
    // leaves out, or an empty list, goes back to its default.
    let update = match plan_replace(configs(), &topic, &alter.context) {
        Ok(update) => update,
        Err(fault) => {
            return resource_error(
                resource,
                ERROR_INVALID_CONFIG,
                Some(StrBytes::from(fault.message())),
            );
        }
    };

    // `validate_only` already performed the existence read and the same key checks.
    // It does not call `update_topic`.
    if validate_only {
        return resource_error(resource, ERROR_NONE, None);
    }

    let (code, message) = write_alter(
        bridge,
        alter.partition_growth,
        name,
        update,
        |topic| plan_replace(configs(), topic, &alter.context),
        deadline,
        "AlterConfigs",
    )
    .await;
    resource_error(resource, code, message)
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
            user: None,
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
}
