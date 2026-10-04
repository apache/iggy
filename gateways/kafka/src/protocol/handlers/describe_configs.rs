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

//! `DescribeConfigs` (API key 32).

use std::collections::HashMap;

use bytes::Bytes;
use iggy::prelude::TopicDetails;
use kafka_protocol::messages::DescribeConfigsRequest;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
    DescribeConfigsSynonym,
};
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{BridgeError, IggyBridge};
use crate::protocol::api::{
    API_KEY_DESCRIBE_CONFIGS, ApiVersionRange, ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION,
    ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_describe_configs_shape;
use crate::protocol::handlers::topic_config::{
    CLEANUP_DOC, CLEANUP_POLICY, CLEANUP_POLICY_VALUE, CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_TOPIC,
    CONFIG_TYPE_LONG, CONFIG_TYPE_STRING, ListedKey, ONLY_TOPIC_RESOURCES, RESOURCE_TYPE_TOPIC,
    RETENTION_DOC, RETENTION_MS, UNKNOWN_TAGGED_FIELDS, exceeds_topic_cap, listed_keys,
    message_expiry_is_explicit, retention_ms, topic_cap_message,
};
use crate::protocol::handlers::{
    decode_guarded, encode_message, is_supported_version, respond_or_close,
    unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DESCRIBE_CONFIGS,
    min_version: 1,
    max_version: 4,
};

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_DESCRIBE_CONFIGS, api_version) {
        return unsupported_version_response(API_KEY_DESCRIBE_CONFIGS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<DescribeConfigsRequest>(api_version, body, |version, body| {
        validate_describe_configs_shape(version, body, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            tracing::debug!(%error, "Failed to decode DescribeConfigs request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "DescribeConfigs",
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
            "DescribeConfigs",
        );
    }

    let Some(bridge) = &state.bridge else {
        let results = req
            .resources
            .iter()
            .map(|resource| resource_error(resource, ERROR_NOT_CONTROLLER, None, Vec::new()))
            .collect();
        return respond_or_close(
            encode_message(&response(results), api_version, 256),
            "DescribeConfigs",
        );
    };

    let deadline = Instant::now() + REQUEST_DEADLINE;
    let results = describe_all(bridge, &req, deadline).await;
    respond_or_close(
        encode_message(&response(results), api_version, 256),
        "DescribeConfigs",
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> crate::error::Result<Bytes> {
    let results = vec![
        DescribeConfigsResult::default()
            .with_error_code(error_code)
            .with_error_message(None)
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_static_str(""))
            .with_configs(Vec::new()),
    ];
    encode_message(&response(results), version, 128)
}

fn response(results: Vec<DescribeConfigsResult>) -> DescribeConfigsResponse {
    DescribeConfigsResponse::default()
        .with_throttle_time_ms(0)
        .with_results(results)
}

async fn describe_all(
    bridge: &IggyBridge,
    req: &DescribeConfigsRequest,
    deadline: Instant,
) -> Vec<DescribeConfigsResult> {
    let topic_names = req
        .resources
        .iter()
        .filter(|resource| resource.resource_type == RESOURCE_TYPE_TOPIC)
        .map(|resource| resource.resource_name.as_str());
    if exceeds_topic_cap(topic_names) {
        let message = StrBytes::from(topic_cap_message("DescribeConfigs"));
        tracing::warn!("DescribeConfigs request addresses too many distinct topics; rejecting");
        return req
            .resources
            .iter()
            .map(|resource| {
                resource_error(
                    resource,
                    ERROR_POLICY_VIOLATION,
                    Some(message.clone()),
                    Vec::new(),
                )
            })
            .collect();
    }

    // `get_kafka_topics` lists partition counts, not `message_expiry`, so each distinct
    // topic is one `get_kafka_topic`. Duplicates reuse the first result and do not call again.
    let mut seen: HashMap<String, DescribeConfigsResult> = HashMap::new();
    let mut results = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    for resource in &req.resources {
        if resource.resource_type == RESOURCE_TYPE_TOPIC
            && let Some(prior) = seen.get(resource.resource_name.as_str())
        {
            results.push(prior.clone());
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "DescribeConfigs deadline passed; answering remaining resources retriable \
                     instead of starting new Iggy calls"
                );
            }
            let timed_out = resource_error(resource, ERROR_REQUEST_TIMED_OUT, None, Vec::new());
            remember_topic(&mut seen, resource, &timed_out);
            results.push(timed_out);
            continue;
        }

        let result = describe_one(
            bridge,
            resource,
            req.include_synonyms,
            req.include_documentation,
            deadline,
        )
        .await;
        if Instant::now() >= deadline {
            deadline_exceeded = true;
        }
        remember_topic(&mut seen, resource, &result);
        results.push(result);
    }
    results
}

fn remember_topic(
    seen: &mut HashMap<String, DescribeConfigsResult>,
    resource: &DescribeConfigsResource,
    result: &DescribeConfigsResult,
) {
    if resource.resource_type == RESOURCE_TYPE_TOPIC {
        seen.insert(resource.resource_name.as_str().to_string(), result.clone());
    }
}

async fn describe_one(
    bridge: &IggyBridge,
    resource: &DescribeConfigsResource,
    include_synonyms: bool,
    include_documentation: bool,
    deadline: Instant,
) -> DescribeConfigsResult {
    if !resource.unknown_tagged_fields.is_empty() {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(static_text(UNKNOWN_TAGGED_FIELDS)),
            Vec::new(),
        );
    }
    if resource.resource_type != RESOURCE_TYPE_TOPIC {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(static_text(ONLY_TOPIC_RESOURCES)),
            Vec::new(),
        );
    }
    let name = resource.resource_name.as_str();
    if let Err(error) = validate_kafka_topic_name("kafka_topic", name) {
        return resource_error(
            resource,
            ERROR_INVALID_TOPIC_EXCEPTION,
            Some(StrBytes::from(name_reason(&error))),
            Vec::new(),
        );
    }

    if Instant::now() >= deadline {
        tracing::warn!(
            resource = name,
            "DescribeConfigs deadline passed before the topic lookup"
        );
        return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None, Vec::new());
    }
    let topic = match bridge.get_kafka_topic(name).await {
        Ok(Some(topic)) => topic,
        Ok(None) => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None, Vec::new());
        }
        Err(error) => {
            let (code, message) = bridge_failure(&error, "reading");
            return resource_error(resource, code, message, Vec::new());
        }
    };

    let (configs, unknown, unrepresentable) = described_entries(
        &topic,
        resource.configuration_keys.as_deref(),
        include_synonyms,
        include_documentation,
    );

    if unknown || unrepresentable {
        let message = if unrepresentable {
            "stored message expiry is not a whole number of milliseconds"
        } else {
            "unknown config key"
        };
        return resource_error(
            resource,
            ERROR_INVALID_CONFIG,
            Some(static_text(message)),
            configs,
        );
    }
    resource_error(resource, ERROR_NONE, None, configs)
}

#[must_use]
fn described_entries(
    topic: &TopicDetails,
    configuration_keys: Option<&[StrBytes]>,
    include_synonyms: bool,
    include_documentation: bool,
) -> (Vec<DescribeConfigsResourceResult>, bool, bool) {
    let explicit = message_expiry_is_explicit(&topic.options);
    let retention = retention_ms(topic.message_expiry, explicit);
    let keys = listed_keys(configuration_keys);
    let mut unknown = false;
    let mut unrepresentable = false;
    let mut configs = Vec::with_capacity(keys.len());
    for key in keys {
        match key {
            ListedKey::Retention => {
                let (value, source) = retention.as_ref().map_or_else(
                    |()| {
                        // A stored duration that cannot be shown as retention.ms is still the
                        // topic's expiry, not the never-expire default.
                        unrepresentable = true;
                        (None, CONFIG_SOURCE_TOPIC)
                    },
                    |retention| (Some(retention.value.as_str()), retention.source),
                );
                configs.push(config_entry(
                    RETENTION_MS,
                    value,
                    false,
                    source,
                    CONFIG_TYPE_LONG,
                    include_synonyms,
                    include_documentation.then_some(RETENTION_DOC),
                ));
            }
            ListedKey::Cleanup => configs.push(config_entry(
                CLEANUP_POLICY,
                Some(CLEANUP_POLICY_VALUE),
                true,
                CONFIG_SOURCE_DEFAULT,
                CONFIG_TYPE_STRING,
                include_synonyms,
                include_documentation.then_some(CLEANUP_DOC),
            )),
            ListedKey::Unknown(name) => {
                unknown = true;
                configs.push(config_entry(
                    &name,
                    None,
                    false,
                    CONFIG_SOURCE_DEFAULT,
                    0,
                    include_synonyms,
                    None,
                ));
            }
        }
    }
    (configs, unknown, unrepresentable)
}

fn config_entry(
    name: &str,
    value: Option<&str>,
    read_only: bool,
    source: i8,
    config_type: i8,
    include_synonyms: bool,
    documentation: Option<&str>,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult::default()
        .with_name(StrBytes::from(name.to_string()))
        .with_value(value.map(|value| StrBytes::from(value.to_string())))
        .with_read_only(read_only)
        .with_config_source(source)
        .with_is_sensitive(false)
        .with_synonyms(synonyms(include_synonyms))
        .with_config_type(config_type)
        .with_documentation(documentation.map(|text| StrBytes::from(text.to_string())))
}

/// Neither known key has a synonym. `include_synonyms` asks for the list and receives none.
const fn synonyms(_include_synonyms: bool) -> Vec<DescribeConfigsSynonym> {
    Vec::new()
}

fn reject_tagged_resources(resources: &[DescribeConfigsResource]) -> Vec<DescribeConfigsResult> {
    if resources.is_empty() {
        return vec![
            DescribeConfigsResult::default()
                .with_error_code(ERROR_INVALID_REQUEST)
                .with_error_message(Some(static_text(UNKNOWN_TAGGED_FIELDS)))
                .with_resource_type(0)
                .with_resource_name(StrBytes::from_static_str(""))
                .with_configs(Vec::new()),
        ];
    }
    resources
        .iter()
        .map(|resource| {
            resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                Some(static_text(UNKNOWN_TAGGED_FIELDS)),
                Vec::new(),
            )
        })
        .collect()
}

fn resource_error(
    resource: &DescribeConfigsResource,
    error_code: i16,
    error_message: Option<StrBytes>,
    configs: Vec<DescribeConfigsResourceResult>,
) -> DescribeConfigsResult {
    DescribeConfigsResult::default()
        .with_error_code(error_code)
        .with_error_message(error_message)
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
        .with_configs(configs)
}

fn name_reason(error: &BridgeError) -> String {
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => reason.clone(),
        _ => "invalid topic name".to_string(),
    }
}

fn bridge_failure(error: &BridgeError, action: &str) -> (i16, Option<StrBytes>) {
    let code = error.to_kafka_error_code();
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => {
            tracing::debug!(reason, "DescribeConfigs rejected an invalid topic name");
            (code, Some(StrBytes::from(reason.clone())))
        }
        BridgeError::Timeout | BridgeError::SendLost(_) => {
            tracing::warn!(%error, "DescribeConfigs {action} exceeded the bridge deadline");
            (code, None)
        }
        other => {
            tracing::error!(%other, "DescribeConfigs failed while {action} topic configuration");
            (
                code,
                Some(static_text("internal error reading topic configuration")),
            )
        }
    }
}

const fn static_text(message: &'static str) -> StrBytes {
    StrBytes::from_static_str(message)
}
