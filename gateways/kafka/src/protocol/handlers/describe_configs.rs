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

use std::collections::HashSet;

use bytes::Bytes;
use iggy::prelude::IggyExpiry;
use kafka_protocol::messages::DescribeConfigsRequest;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
};
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{IggyBridge, StreamTopicCache, TopicLoad};
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
    RETENTION_DOC, RETENTION_MS, bridge_failure, listed_keys, name_reason, retention_ms,
    topic_cap_and_duplicates, topic_cap_message,
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

    // Checked before the bridge-availability stub, not inside `describe_all`: the cap is a
    // request-shape policy, not a controller-availability one, and a check that only runs once
    // the bridge is up is never exercised by a serverless/stub test.
    let (exceeds_cap, duplicate_names) = topic_cap_and_duplicates(&req.resources);
    if exceeds_cap {
        let message = StrBytes::from(topic_cap_message("DescribeConfigs"));
        tracing::warn!("DescribeConfigs request addresses too many distinct topics; rejecting");
        let results = req
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
        return respond_or_close(
            encode_message(&response(results), api_version, 256),
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
    let results = describe_all(bridge, &req, &duplicate_names, deadline).await;
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
    duplicate_names: &HashSet<&str>,
    deadline: Instant,
) -> Vec<DescribeConfigsResult> {
    let mut cache = StreamTopicCache::default();
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
            &mut cache,
            resource,
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
    cache: &mut StreamTopicCache,
    resource: &DescribeConfigsResource,
    include_documentation: bool,
    deadline: Instant,
) -> DescribeConfigsResult {
    if resource.resource_type != RESOURCE_TYPE_TOPIC {
        return resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from_static_str(ONLY_TOPIC_RESOURCES)),
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

    let snapshot = match cache
        .lookup(bridge, name, deadline, "DescribeConfigs")
        .await
    {
        TopicLoad::Found(snapshot) => snapshot,
        TopicLoad::Missing => {
            return resource_error(resource, ERROR_UNKNOWN_TOPIC_OR_PARTITION, None, Vec::new());
        }
        TopicLoad::Failed(error) => {
            let (code, message) = bridge_failure(error, "DescribeConfigs", "reading");
            return resource_error(resource, code, message, Vec::new());
        }
        TopicLoad::NotStarted | TopicLoad::TimedOut => {
            return resource_error(resource, ERROR_REQUEST_TIMED_OUT, None, Vec::new());
        }
    };

    let (configs, unrepresentable) = described_entries(
        snapshot.message_expiry,
        snapshot.message_expiry_explicit,
        resource.configuration_keys.as_deref(),
        include_documentation,
    );

    if unrepresentable {
        return resource_error(
            resource,
            ERROR_INVALID_CONFIG,
            Some(StrBytes::from_static_str(
                "stored message expiry is not a whole number of milliseconds",
            )),
            configs,
        );
    }
    resource_error(resource, ERROR_NONE, None, configs)
}

#[must_use]
fn described_entries(
    message_expiry: IggyExpiry,
    message_expiry_explicit: bool,
    configuration_keys: Option<&[StrBytes]>,
    include_documentation: bool,
) -> (Vec<DescribeConfigsResourceResult>, bool) {
    let retention = retention_ms(message_expiry, message_expiry_explicit);
    let keys = listed_keys(configuration_keys);
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
                    include_documentation.then_some(RETENTION_DOC),
                ));
            }
            ListedKey::Cleanup => configs.push(config_entry(
                CLEANUP_POLICY,
                Some(CLEANUP_POLICY_VALUE),
                true,
                CONFIG_SOURCE_DEFAULT,
                CONFIG_TYPE_STRING,
                include_documentation.then_some(CLEANUP_DOC),
            )),
            // A resource error makes clients drop every returned config, including
            // retention.ms. An unmodeled name is omitted and the resource stays successful.
            ListedKey::Unknown => {}
        }
    }
    (configs, unrepresentable)
}

fn config_entry(
    name: &str,
    value: Option<&str>,
    read_only: bool,
    source: i8,
    config_type: i8,
    documentation: Option<&str>,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult::default()
        .with_name(StrBytes::from(name.to_string()))
        .with_value(value.map(|value| StrBytes::from(value.to_string())))
        .with_read_only(read_only)
        .with_config_source(source)
        .with_is_sensitive(false)
        .with_synonyms(Vec::new())
        .with_config_type(config_type)
        .with_documentation(documentation.map(|text| StrBytes::from(text.to_string())))
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use iggy::prelude::{
        CompressionAlgorithm, HeaderKey, HeaderValue, IggyByteSize, IggyExpiry, IggyTimestamp,
        MaxTopicSize, OptionValue, ResourceOptions, TopicDetails, topic_option_keys,
    };
    use kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
    use kafka_protocol::protocol::StrBytes;

    use super::described_entries;
    use crate::protocol::handlers::topic_config::{
        CLEANUP_POLICY, RETENTION_MS, message_expiry_is_explicit, parse_retention_ms,
    };

    fn topic_details(expiry: IggyExpiry, explicit: bool) -> TopicDetails {
        let mut options = ResourceOptions::new();
        if explicit {
            let key = HeaderKey::from_str(topic_option_keys::MESSAGE_EXPIRY).expect("expiry key");
            options.insert(
                key,
                OptionValue::explicit(HeaderValue::from(u64::from(expiry))),
            );
        }
        TopicDetails {
            id: 1,
            created_at: IggyTimestamp::default(),
            name: "orders".to_string(),
            size: IggyByteSize::default(),
            message_expiry: expiry,
            compression_algorithm: CompressionAlgorithm::None,
            max_topic_size: MaxTopicSize::default(),
            messages_count: 0,
            partitions_count: 1,
            partitions: Vec::new(),
            options,
        }
    }

    fn entries(
        details: &TopicDetails,
        configuration_keys: Option<&[StrBytes]>,
        include_documentation: bool,
    ) -> (Vec<DescribeConfigsResourceResult>, bool) {
        described_entries(
            details.message_expiry,
            message_expiry_is_explicit(&details.options),
            configuration_keys,
            include_documentation,
        )
    }

    fn value_of<'a>(configs: &'a [DescribeConfigsResourceResult], name: &str) -> Option<&'a str> {
        configs
            .iter()
            .find(|config| config.name.as_str() == name)
            .and_then(|config| config.value.as_ref().map(StrBytes::as_str))
    }

    #[test]
    fn default_describe_returns_retention_and_cleanup() {
        let expiry = parse_retention_ms("120000").expect("2 minutes in ms");
        let details = topic_details(expiry, true);
        let (configs, bad) = entries(&details, None, false);
        assert!(!bad);
        assert_eq!(
            configs
                .iter()
                .map(|config| config.name.as_str())
                .collect::<Vec<_>>(),
            vec![RETENTION_MS, CLEANUP_POLICY]
        );
        assert_eq!(value_of(&configs, RETENTION_MS), Some("120000"));
        let cleanup = configs
            .iter()
            .find(|config| config.name.as_str() == CLEANUP_POLICY)
            .expect("cleanup");
        assert!(cleanup.synonyms.is_empty());
    }

    #[test]
    fn synonyms_are_always_empty() {
        let expiry = parse_retention_ms("120000").expect("ms");
        let details = topic_details(expiry, true);
        let (configs, _) = entries(&details, None, false);
        assert!(configs.iter().all(|config| config.synonyms.is_empty()));
    }

    #[test]
    fn requested_unknown_names_are_omitted_and_known_keys_remain() {
        let expiry = parse_retention_ms("1500").expect("ms");
        let details = topic_details(expiry, true);
        let asked = [
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str("no.such"),
            StrBytes::from_static_str("retention.minutes"),
            StrBytes::from_static_str("retention.hours"),
        ];
        let (configs, bad) = entries(&details, Some(&asked), false);
        assert!(!bad);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name.as_str(), RETENTION_MS);
        assert_eq!(
            configs[0].value.as_ref().map(StrBytes::as_str),
            Some("1500")
        );
    }

    #[test]
    fn a_name_repeated_in_the_request_is_returned_once() {
        let expiry = parse_retention_ms("1500").expect("ms");
        let details = topic_details(expiry, true);
        let asked = [
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str(RETENTION_MS),
        ];
        let (configs, _) = entries(&details, Some(&asked), false);
        assert_eq!(configs.len(), 1);
    }

    #[test]
    fn never_expire_reports_negative_one() {
        let (configs, bad) = entries(&topic_details(IggyExpiry::NeverExpire, true), None, false);
        assert!(!bad);
        assert_eq!(value_of(&configs, RETENTION_MS), Some("-1"));
    }

    #[test]
    fn documentation_is_attached_only_when_asked() {
        let expiry = parse_retention_ms("1500").expect("ms");
        let details = topic_details(expiry, true);
        let (configs, _) = entries(&details, None, true);
        assert!(configs.iter().all(|config| config.documentation.is_some()));
        let (configs, _) = entries(&details, None, false);
        assert!(configs.iter().all(|config| config.documentation.is_none()));
    }
}
