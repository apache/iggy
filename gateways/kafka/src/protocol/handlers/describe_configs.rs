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
use iggy::prelude::{IggyExpiry, ResourceOptions};
use kafka_protocol::messages::DescribeConfigsRequest;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
    DescribeConfigsSynonym,
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
    CONFIG_TYPE_LONG, CONFIG_TYPE_STRING, IggyTopicKey, ListedKey, MILLIS_PER_HOUR,
    MILLIS_PER_MINUTE, ONLY_TOPIC_RESOURCES, RESOURCE_TYPE_TOPIC, RETENTION_DOC, RETENTION_HOURS,
    RETENTION_HOURS_DOC, RETENTION_MINUTES, RETENTION_MINUTES_DOC, RETENTION_MS, RetentionMs,
    RetentionSynonyms, bridge_failure, exceeds_topic_cap, find_duplicate_names, listed_keys,
    message_expiry_is_explicit, name_reason, retention_in_unit, retention_ms, static_text,
    topic_cap_message,
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
    let results = describe_all(state, bridge, &req, deadline).await;
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
    state: &GatewayState,
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
            state,
            bridge,
            &mut cache,
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
    state: &GatewayState,
    bridge: &IggyBridge,
    cache: &mut StreamTopicCache,
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

    let (stream, iggy_topic) = bridge.topic_identity(name);
    let remembered = state.remembered_retention_synonyms(&IggyTopicKey::new(stream, iggy_topic));
    let (configs, unrepresentable) = described_entries(
        snapshot.message_expiry,
        &snapshot.options,
        resource.configuration_keys.as_deref(),
        include_synonyms,
        include_documentation,
        remembered,
    );

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
    message_expiry: IggyExpiry,
    options: &ResourceOptions,
    configuration_keys: Option<&[StrBytes]>,
    include_synonyms: bool,
    include_documentation: bool,
    remembered: RetentionSynonyms,
) -> (Vec<DescribeConfigsResourceResult>, bool) {
    let explicit = message_expiry_is_explicit(options);
    let retention = retention_ms(message_expiry, explicit);
    let keys = listed_keys(configuration_keys);
    let mut unrepresentable = false;
    let mut configs = Vec::with_capacity(keys.len());
    for key in keys {
        match key {
            ListedKey::Retention => {
                let (value, source) = retention_parts(&retention, &mut unrepresentable);
                configs.push(config_entry(
                    RETENTION_MS,
                    value,
                    false,
                    source,
                    CONFIG_TYPE_LONG,
                    synonyms(include_synonyms, remembered, value, source),
                    include_documentation.then_some(RETENTION_DOC),
                ));
            }
            ListedKey::Minutes => configs.push(derived_retention_entry(
                RETENTION_MINUTES,
                MILLIS_PER_MINUTE,
                &retention,
                include_documentation.then_some(RETENTION_MINUTES_DOC),
                &mut unrepresentable,
            )),
            ListedKey::Hours => configs.push(derived_retention_entry(
                RETENTION_HOURS,
                MILLIS_PER_HOUR,
                &retention,
                include_documentation.then_some(RETENTION_HOURS_DOC),
                &mut unrepresentable,
            )),
            ListedKey::Cleanup => configs.push(config_entry(
                CLEANUP_POLICY,
                Some(CLEANUP_POLICY_VALUE),
                true,
                CONFIG_SOURCE_DEFAULT,
                CONFIG_TYPE_STRING,
                Vec::new(),
                include_documentation.then_some(CLEANUP_DOC),
            )),
            // A resource error makes clients drop every returned config, including
            // retention.ms. An unmodeled name is omitted and the resource stays successful.
            ListedKey::Unknown(_) => {}
        }
    }
    (configs, unrepresentable)
}

const fn retention_parts<'a>(
    retention: &'a Result<RetentionMs, ()>,
    unrepresentable: &mut bool,
) -> (Option<&'a str>, i8) {
    if let Ok(retention) = retention {
        (Some(retention.value.as_str()), retention.source)
    } else {
        // A stored duration that cannot be shown as retention.ms is still the
        // topic's expiry, not the never-expire default.
        *unrepresentable = true;
        (None, CONFIG_SOURCE_TOPIC)
    }
}

fn derived_retention_entry(
    name: &str,
    unit_ms: u64,
    retention: &Result<RetentionMs, ()>,
    documentation: Option<&str>,
    unrepresentable: &mut bool,
) -> DescribeConfigsResourceResult {
    let (millis, source) = retention_parts(retention, unrepresentable);
    let value = millis.and_then(|millis| retention_in_unit(millis, unit_ms));
    config_entry(
        name,
        value.as_deref(),
        false,
        source,
        CONFIG_TYPE_LONG,
        Vec::new(),
        documentation,
    )
}

fn config_entry(
    name: &str,
    value: Option<&str>,
    read_only: bool,
    source: i8,
    config_type: i8,
    synonyms: Vec<DescribeConfigsSynonym>,
    documentation: Option<&str>,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult::default()
        .with_name(StrBytes::from(name.to_string()))
        .with_value(value.map(|value| StrBytes::from(value.to_string())))
        .with_read_only(read_only)
        .with_config_source(source)
        .with_is_sensitive(false)
        .with_synonyms(synonyms)
        .with_config_type(config_type)
        .with_documentation(documentation.map(|text| StrBytes::from(text.to_string())))
}

fn synonyms(
    include_synonyms: bool,
    remembered: RetentionSynonyms,
    retention_ms_value: Option<&str>,
    source: i8,
) -> Vec<DescribeConfigsSynonym> {
    if !include_synonyms || remembered.is_empty() {
        return Vec::new();
    }
    let mut listed = Vec::new();
    if remembered.minutes {
        listed.push(synonym(
            RETENTION_MINUTES,
            retention_ms_value.and_then(|value| retention_in_unit(value, MILLIS_PER_MINUTE)),
            source,
        ));
    }
    if remembered.hours {
        listed.push(synonym(
            RETENTION_HOURS,
            retention_ms_value.and_then(|value| retention_in_unit(value, MILLIS_PER_HOUR)),
            source,
        ));
    }
    listed
}

fn synonym(name: &'static str, value: Option<String>, source: i8) -> DescribeConfigsSynonym {
    DescribeConfigsSynonym::default()
        .with_name(StrBytes::from_static_str(name))
        .with_value(value.map(StrBytes::from))
        .with_source(source)
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
        CLEANUP_POLICY, CONFIG_SOURCE_TOPIC, RETENTION_HOURS, RETENTION_MINUTES, RETENTION_MS,
        RetentionSynonyms, parse_retention_ms,
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
        include_synonyms: bool,
        include_documentation: bool,
        remembered: RetentionSynonyms,
    ) -> (Vec<DescribeConfigsResourceResult>, bool) {
        described_entries(
            details.message_expiry,
            &details.options,
            configuration_keys,
            include_synonyms,
            include_documentation,
            remembered,
        )
    }

    fn value_of<'a>(configs: &'a [DescribeConfigsResourceResult], name: &str) -> Option<&'a str> {
        configs
            .iter()
            .find(|config| config.name.as_str() == name)
            .and_then(|config| config.value.as_ref().map(StrBytes::as_str))
    }

    #[test]
    fn minutes_and_hours_describe_as_retention_ms_with_synonyms_when_asked() {
        let minutes = parse_retention_ms("120000").expect("2 minutes in ms");
        let remembered = RetentionSynonyms {
            minutes: true,
            hours: false,
        };
        let details = topic_details(minutes, true);
        let (configs, bad) = entries(&details, None, true, false, remembered);
        assert!(!bad);
        assert_eq!(
            configs
                .iter()
                .map(|config| config.name.as_str())
                .collect::<Vec<_>>(),
            vec![RETENTION_MS, CLEANUP_POLICY]
        );
        assert_eq!(value_of(&configs, RETENTION_MS), Some("120000"));
        let retention = configs
            .iter()
            .find(|config| config.name.as_str() == RETENTION_MS)
            .expect("retention.ms");
        assert_eq!(retention.synonyms.len(), 1);
        assert_eq!(retention.synonyms[0].name.as_str(), RETENTION_MINUTES);
        assert_eq!(
            retention.synonyms[0].value.as_ref().map(StrBytes::as_str),
            Some("2")
        );
        assert_eq!(retention.synonyms[0].source, CONFIG_SOURCE_TOPIC);
        let cleanup = configs
            .iter()
            .find(|config| config.name.as_str() == CLEANUP_POLICY)
            .expect("cleanup");
        assert!(cleanup.synonyms.is_empty());

        let hours = parse_retention_ms("3600000").expect("1 hour in ms");
        let (configs, bad) = entries(
            &topic_details(hours, true),
            None,
            true,
            false,
            RetentionSynonyms {
                minutes: false,
                hours: true,
            },
        );
        assert!(!bad);
        let retention = configs
            .iter()
            .find(|config| config.name.as_str() == RETENTION_MS)
            .expect("retention.ms");
        assert_eq!(
            retention.value.as_ref().map(StrBytes::as_str),
            Some("3600000")
        );
        assert_eq!(retention.synonyms.len(), 1);
        assert_eq!(retention.synonyms[0].name.as_str(), RETENTION_HOURS);
        assert_eq!(
            retention.synonyms[0].value.as_ref().map(StrBytes::as_str),
            Some("1")
        );
    }

    #[test]
    fn synonyms_are_empty_when_not_requested_or_only_milliseconds_were_set() {
        let expiry = parse_retention_ms("120000").expect("ms");
        let details = topic_details(expiry, true);
        let remembered = RetentionSynonyms {
            minutes: true,
            hours: true,
        };
        let (configs, _) = entries(&details, None, false, false, remembered);
        assert!(configs.iter().all(|config| config.synonyms.is_empty()));

        let (configs, _) = entries(&details, None, true, false, RetentionSynonyms::default());
        assert!(configs.iter().all(|config| config.synonyms.is_empty()));
    }

    #[test]
    fn requested_minutes_or_hours_return_that_name_from_stored_milliseconds() {
        let expiry = parse_retention_ms("90000").expect("ms");
        let details = topic_details(expiry, true);
        let asked = [StrBytes::from_static_str(RETENTION_MINUTES)];
        let (configs, bad) = entries(
            &details,
            Some(&asked),
            true,
            false,
            RetentionSynonyms {
                minutes: true,
                hours: false,
            },
        );
        assert!(!bad);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name.as_str(), RETENTION_MINUTES);
        assert_eq!(configs[0].value.as_ref().map(StrBytes::as_str), Some("1"));
        assert!(configs[0].synonyms.is_empty());

        let asked = [StrBytes::from_static_str(RETENTION_HOURS)];
        let never = topic_details(IggyExpiry::NeverExpire, true);
        let (configs, bad) = entries(
            &never,
            Some(&asked),
            false,
            false,
            RetentionSynonyms::default(),
        );
        assert!(!bad);
        assert_eq!(configs[0].name.as_str(), RETENTION_HOURS);
        assert_eq!(configs[0].value.as_ref().map(StrBytes::as_str), Some("-1"));
    }

    #[test]
    fn stored_never_expire_reports_negative_one_for_a_remembered_hour_synonym() {
        let (configs, bad) = entries(
            &topic_details(IggyExpiry::NeverExpire, true),
            None,
            true,
            false,
            RetentionSynonyms {
                minutes: false,
                hours: true,
            },
        );
        assert!(!bad);
        let retention = configs
            .iter()
            .find(|config| config.name.as_str() == RETENTION_MS)
            .expect("retention.ms");
        assert_eq!(retention.value.as_ref().map(StrBytes::as_str), Some("-1"));
        assert_eq!(
            retention.synonyms[0].value.as_ref().map(StrBytes::as_str),
            Some("-1")
        );
    }

    #[test]
    fn an_unmodeled_key_is_omitted_and_known_keys_remain() {
        let expiry = parse_retention_ms("1500").expect("ms");
        let details = topic_details(expiry, true);
        let asked = [
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str("no.such"),
        ];
        let (configs, bad) = entries(
            &details,
            Some(&asked),
            false,
            false,
            RetentionSynonyms::default(),
        );
        assert!(!bad);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].name.as_str(), RETENTION_MS);
        assert_eq!(
            configs[0].value.as_ref().map(StrBytes::as_str),
            Some("1500")
        );
    }
}
