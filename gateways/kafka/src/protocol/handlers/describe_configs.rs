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

use crate::bridge::IggyBridge;
use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::protocol::api::{
    API_KEY_DESCRIBE_CONFIGS, ApiVersionRange, ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION,
    ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    REQUEST_DEADLINE,
};
use crate::protocol::bounds_guard::validate_describe_configs_shape;
use crate::protocol::handlers::topic_config::{
    CLEANUP_DOC, CLEANUP_POLICY, CLEANUP_POLICY_VALUE, CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_TOPIC,
    CONFIG_TYPE_LONG, CONFIG_TYPE_STRING, CONFIG_TYPE_UNKNOWN, ListedKey, ONLY_TOPIC_RESOURCES,
    RESOURCE_TYPE_TOPIC, RETENTION_DOC, RETENTION_MS, bridge_failure, exceeds_topic_cap,
    find_duplicate_names, listed_keys, message_expiry_is_explicit, name_reason, retention_ms,
    static_text, topic_cap_message,
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
    if !req.unknown_tagged_fields.is_empty() {
        let message = static_text("unknown tagged fields are not supported");
        return if req.resources.is_empty() {
            vec![
                DescribeConfigsResult::default()
                    .with_error_code(ERROR_INVALID_REQUEST)
                    .with_error_message(Some(message))
                    .with_resource_type(RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str(""))
                    .with_configs(Vec::new()),
            ]
        } else {
            req.resources
                .iter()
                .map(|resource| {
                    resource_error(
                        resource,
                        ERROR_INVALID_REQUEST,
                        Some(message.clone()),
                        Vec::new(),
                    )
                })
                .collect()
        };
    }

    let topic_names: Vec<&str> = req
        .resources
        .iter()
        .filter(|resource| resource.resource_type == RESOURCE_TYPE_TOPIC)
        .map(|resource| resource.resource_name.as_str())
        .collect();
    if exceeds_topic_cap(topic_names.iter().copied()) {
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

    // Real Kafka refuses every occurrence of a duplicate resource name outright rather than
    // answering it from a cached result that may have been filtered by a different
    // `configuration_keys` list - the same choice CreateTopics makes for a repeated topic name.
    let duplicate_names = find_duplicate_names(topic_names.iter().copied());

    let mut results = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    for resource in &req.resources {
        if resource.resource_type == RESOURCE_TYPE_TOPIC
            && duplicate_names.contains(resource.resource_name.as_str())
        {
            results.push(resource_error(
                resource,
                ERROR_INVALID_REQUEST,
                None,
                Vec::new(),
            ));
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
            results.push(resource_error(
                resource,
                ERROR_REQUEST_TIMED_OUT,
                None,
                Vec::new(),
            ));
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
        results.push(result);
    }
    results
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
            Some(static_text("unknown tagged fields are not supported")),
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
    let topic = match tokio::time::timeout_at(deadline, bridge.get_kafka_topic(name)).await {
        Ok(Ok(Some(topic))) => topic,
        Ok(Ok(None)) => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None, Vec::new());
        }
        Ok(Err(error)) => {
            let (code, message) = bridge_failure(&error, "DescribeConfigs", "reading");
            return resource_error(resource, code, message, Vec::new());
        }
        Err(_elapsed) => {
            tracing::warn!(
                resource = name,
                "DescribeConfigs: this resource's bridge work exceeded the request deadline; \
                 answering retriable instead of blocking further"
            );
            return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None, Vec::new());
        }
    };

    let (configs, unknown_key, unrepresentable) = described_entries(
        &topic,
        resource.configuration_keys.as_deref(),
        include_synonyms,
        include_documentation,
    );

    if let Some(unknown_key) = unknown_key {
        return resource_error(
            resource,
            ERROR_INVALID_CONFIG,
            Some(StrBytes::from(format!(
                "unknown config key '{unknown_key}'"
            ))),
            configs,
        );
    }
    if unrepresentable {
        return resource_error(
            resource,
            ERROR_INVALID_CONFIG,
            Some(static_text(
                "stored message expiry is not a whole number of milliseconds",
            )),
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
) -> (Vec<DescribeConfigsResourceResult>, Option<String>, bool) {
    let explicit = message_expiry_is_explicit(&topic.options);
    let retention = retention_ms(topic.message_expiry, explicit);
    let keys = listed_keys(configuration_keys);
    let mut unknown_key = None;
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
                if unknown_key.is_none() {
                    unknown_key = Some(name.clone());
                }
                configs.push(config_entry(
                    &name,
                    None,
                    false,
                    CONFIG_SOURCE_DEFAULT,
                    CONFIG_TYPE_UNKNOWN,
                    include_synonyms,
                    None,
                ));
            }
        }
    }
    (configs, unknown_key, unrepresentable)
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

/// Neither known key has a synonym. `true` asks for the list and receives none.
#[allow(
    clippy::if_same_then_else,
    reason = "true asks for synonyms and there are none"
)]
fn synonyms(include_synonyms: bool) -> Vec<DescribeConfigsSynonym> {
    if include_synonyms {
        Vec::new()
    } else {
        Vec::new()
    }
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
