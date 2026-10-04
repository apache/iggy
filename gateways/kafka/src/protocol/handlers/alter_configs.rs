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

use std::collections::HashMap;

use bytes::Bytes;
use iggy::prelude::IggyError;
use kafka_protocol::messages::AlterConfigsRequest;
use kafka_protocol::messages::alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::alter_configs_response::{
    AlterConfigsResourceResponse, AlterConfigsResponse,
};
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{BridgeError, IggyBridge};
use crate::protocol::api::{
    API_KEY_ALTER_CONFIGS, ApiVersionRange, ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION,
    ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_alter_configs_shape;
use crate::protocol::handlers::topic_config::{
    ONLY_TOPIC_RESOURCES, RESOURCE_TYPE_TOPIC, UNKNOWN_TAGGED_FIELDS, exceeds_topic_cap,
    plan_retention_update, topic_cap_message,
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

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
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

    if !req.unknown_tagged_fields.is_empty() {
        return respond_or_close(
            encode_message(
                &response(reject_tagged_resources(&req.resources)),
                api_version,
                256,
            ),
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
    let responses = alter_all(bridge, &req, deadline).await;
    respond_or_close(
        encode_message(&response(responses), api_version, 256),
        "AlterConfigs",
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

async fn alter_all(
    bridge: &IggyBridge,
    req: &AlterConfigsRequest,
    deadline: Instant,
) -> Vec<AlterConfigsResourceResponse> {
    let topic_names = req
        .resources
        .iter()
        .filter(|resource| resource.resource_type == RESOURCE_TYPE_TOPIC)
        .map(|resource| resource.resource_name.as_str());
    if exceeds_topic_cap(topic_names) {
        let message = StrBytes::from(topic_cap_message("AlterConfigs"));
        tracing::warn!("AlterConfigs request addresses too many distinct topics; rejecting");
        return req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_POLICY_VIOLATION, Some(message.clone())))
            .collect();
    }

    let mut seen: HashMap<String, AlterConfigsResourceResponse> = HashMap::new();
    let mut responses = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    for resource in &req.resources {
        if resource.resource_type == RESOURCE_TYPE_TOPIC
            && let Some(prior) = seen.get(resource.resource_name.as_str())
        {
            responses.push(prior.clone());
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
            let timed_out = resource_error(resource, ERROR_REQUEST_TIMED_OUT, None);
            remember_topic(&mut seen, resource, &timed_out);
            responses.push(timed_out);
            continue;
        }

        let result = alter_one(bridge, resource, req.validate_only, deadline).await;
        if Instant::now() >= deadline {
            deadline_exceeded = true;
        }
        remember_topic(&mut seen, resource, &result);
        responses.push(result);
    }
    responses
}

fn remember_topic(
    seen: &mut HashMap<String, AlterConfigsResourceResponse>,
    resource: &AlterConfigsResource,
    result: &AlterConfigsResourceResponse,
) {
    if resource.resource_type == RESOURCE_TYPE_TOPIC {
        seen.insert(resource.resource_name.as_str().to_string(), result.clone());
    }
}

async fn alter_one(
    bridge: &IggyBridge,
    resource: &AlterConfigsResource,
    validate_only: bool,
    deadline: Instant,
) -> AlterConfigsResourceResponse {
    if !resource.unknown_tagged_fields.is_empty()
        || resource
            .configs
            .iter()
            .any(|config| !config.unknown_tagged_fields.is_empty())
    {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(static_text(UNKNOWN_TAGGED_FIELDS)),
        );
    }
    if resource.resource_type != RESOURCE_TYPE_TOPIC {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(static_text(ONLY_TOPIC_RESOURCES)),
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

    // Existence is checked before config keys. A missing topic is
    // UNKNOWN_TOPIC_OR_PARTITION even when a key would also be invalid.
    // A deadline that has already passed must not start the lockstep Iggy call.
    if Instant::now() >= deadline {
        tracing::warn!(
            resource = name,
            "AlterConfigs deadline passed before the topic lookup"
        );
        return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None);
    }
    match bridge.get_kafka_topic(name).await {
        Ok(Some(_topic)) => {}
        Ok(None) => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None);
        }
        Err(error) => {
            let (code, message) = bridge_failure(&error);
            return resource_error(resource, code, message);
        }
    }

    let expiry = match plan_retention_update(resource.configs.iter().map(|config| {
        (
            config.name.as_str(),
            config.value.as_ref().map(StrBytes::as_str),
        )
    })) {
        Ok(expiry) => expiry,
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
    let Some(expiry) = expiry else {
        return resource_error(resource, ERROR_NONE, None);
    };
    if validate_only {
        return resource_error(resource, ERROR_NONE, None);
    }

    // Do not start the write once the deadline has passed, and do not wrap it in
    // `timeout_at`. Dropping that future does not abort the SDK task, so
    // `UpdateTopic` can commit after this handler has already answered
    // REQUEST_TIMED_OUT. A short retention then deletes sealed segments.
    if Instant::now() >= deadline {
        tracing::warn!(
            resource = name,
            "AlterConfigs deadline passed before update_topic; not starting the write"
        );
        return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None);
    }
    match bridge.update_kafka_topic_message_expiry(name, expiry).await {
        Ok(()) | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => {
            resource_error(resource, ERROR_NONE, None)
        }
        Err(error) => {
            let (code, message) = bridge_failure(&error);
            resource_error(resource, code, message)
        }
    }
}

fn reject_tagged_resources(
    resources: &[AlterConfigsResource],
) -> Vec<AlterConfigsResourceResponse> {
    if resources.is_empty() {
        return vec![
            AlterConfigsResourceResponse::default()
                .with_error_code(ERROR_INVALID_REQUEST)
                .with_error_message(Some(static_text(UNKNOWN_TAGGED_FIELDS)))
                .with_resource_type(0)
                .with_resource_name(StrBytes::from_static_str("")),
        ];
    }
    resources
        .iter()
        .map(|resource| {
            resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                Some(static_text(UNKNOWN_TAGGED_FIELDS)),
            )
        })
        .collect()
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

fn name_reason(error: &BridgeError) -> String {
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => reason.clone(),
        _ => "invalid topic name".to_string(),
    }
}

fn bridge_failure(error: &BridgeError) -> (i16, Option<StrBytes>) {
    let code = error.to_kafka_error_code();
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => {
            tracing::debug!(reason, "AlterConfigs rejected an invalid topic name");
            (code, Some(StrBytes::from(reason.clone())))
        }
        BridgeError::Timeout | BridgeError::SendLost(_) => {
            tracing::warn!(%error, "AlterConfigs exceeded the bridge deadline");
            (code, None)
        }
        other => {
            tracing::error!(%other, "AlterConfigs failed while updating topic configuration");
            (
                code,
                Some(static_text("internal error updating topic configuration")),
            )
        }
    }
}

const fn static_text(message: &'static str) -> StrBytes {
    StrBytes::from_static_str(message)
}
