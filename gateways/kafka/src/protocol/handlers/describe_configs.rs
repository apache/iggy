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
//!
//! A topic reports every key of Kafka 4.4 except `cleanup.policy`: the three keys Iggy stores,
//! what the gateway applies for some others, and Kafka's default for the rest. A broker reports a
//! fixed set for this gateway. See `topic_config::keys` and `docs/CONFIGS.md`.

use std::collections::HashSet;

use bytes::Bytes;
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
use crate::protocol::handlers::topic_config::keys::{
    BrokerContext, ConfigContext, ConfigEntry, broker_entries, topic_entries,
};
use crate::protocol::handlers::topic_config::{
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC, bridge_failure, describe_cap_message,
    exceeds_describe_cap, name_reason, topic_cap_and_duplicates,
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

const ONLY_TOPIC_AND_BROKER_RESOURCES: &str = "only topic and broker resources are supported";

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
    let (_, duplicate_names) = topic_cap_and_duplicates(&req.resources);
    if exceeds_describe_cap(&req.resources) {
        let message = StrBytes::from(describe_cap_message());
        tracing::warn!("DescribeConfigs request asks for too many resources; rejecting");
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

    let broker = BrokerContext {
        node_id: state.node_id,
        auto_create_topics: state.auto_topic_creation.is_some(),
        config: ConfigContext::of(bridge, state.max_frame_size),
    };
    let deadline = Instant::now() + REQUEST_DEADLINE;
    let results = describe_all(bridge, &broker, &req, &duplicate_names, deadline).await;
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
    broker: &BrokerContext<'_>,
    req: &DescribeConfigsRequest,
    duplicate_names: &HashSet<&str>,
    deadline: Instant,
) -> Vec<DescribeConfigsResult> {
    let mut cache = StreamTopicCache::default();
    let mut results = Vec::with_capacity(req.resources.len());
    let mut deadline_exceeded = false;
    let shown = Shown {
        documentation: req.include_documentation,
        synonyms: req.include_synonyms,
    };
    for resource in &req.resources {
        match resource.resource_type {
            RESOURCE_TYPE_TOPIC => {}
            // Answered from this gateway alone, so the deadline does not apply.
            RESOURCE_TYPE_BROKER => {
                results.push(describe_broker(broker, resource, shown));
                continue;
            }
            _ => {
                results.push(resource_error(
                    resource,
                    ERROR_INVALID_REQUEST,
                    Some(StrBytes::from_static_str(ONLY_TOPIC_AND_BROKER_RESOURCES)),
                    Vec::new(),
                ));
                continue;
            }
        }
        if duplicate_names.contains(resource.resource_name.as_str()) {
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

        let result = describe_topic(
            bridge,
            &broker.config,
            &mut cache,
            resource,
            shown,
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

/// What each entry carries beside its value, as the request asks.
#[derive(Debug, Clone, Copy)]
struct Shown {
    documentation: bool,
    synonyms: bool,
}

async fn describe_topic(
    bridge: &IggyBridge,
    context: &ConfigContext<'_>,
    cache: &mut StreamTopicCache,
    resource: &DescribeConfigsResource,
    shown: Shown,
    deadline: Instant,
) -> DescribeConfigsResult {
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

    let entries = topic_entries(snapshot, context, resource.configuration_keys.as_deref());
    // Only a stored expiry that is not a whole number of milliseconds has no Kafka form. It is
    // still the expiry of the topic, so it is not reported as the never-expire default.
    let unrepresentable = entries.iter().any(|entry| entry.value.is_none());
    let configs = described(entries, shown);
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

/// A BROKER resource, as Kafka answers it: the empty name holds the cluster-wide dynamic
/// defaults, of which this gateway has none, and a broker id must be this gateway's own. An
/// admin client sends the describe of a broker to that broker.
fn describe_broker(
    broker: &BrokerContext<'_>,
    resource: &DescribeConfigsResource,
    shown: Shown,
) -> DescribeConfigsResult {
    let name = resource.resource_name.as_str();
    if name.is_empty() {
        return resource_error(resource, ERROR_NONE, None, Vec::new());
    }
    match name.parse::<i32>() {
        Ok(node_id) if node_id == broker.node_id => {
            let entries = broker_entries(broker, resource.configuration_keys.as_deref());
            resource_error(resource, ERROR_NONE, None, described(entries, shown))
        }
        Ok(_) => resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from(format!(
                "Unexpected broker id, expected {} or empty string, but received {name}",
                broker.node_id
            ))),
            Vec::new(),
        ),
        Err(_) => resource_error(
            resource,
            ERROR_INVALID_REQUEST,
            Some(StrBytes::from(format!(
                "Broker id must be an integer, but it is: {name}"
            ))),
            Vec::new(),
        ),
    }
}

/// The wire form of `entries`. With `include_synonyms`, each entry lists itself as its one
/// synonym, with its value and source, as Kafka lists at least the entry itself. Kafka also lists
/// the broker keys the value falls back to, which the gateway does not.
fn described(entries: Vec<ConfigEntry>, shown: Shown) -> Vec<DescribeConfigsResourceResult> {
    entries
        .into_iter()
        .map(|entry| {
            let name = StrBytes::from_static_str(entry.name);
            let value = entry.value.map(StrBytes::from);
            let synonyms = if shown.synonyms {
                vec![
                    DescribeConfigsSynonym::default()
                        .with_name(name.clone())
                        .with_value(value.clone())
                        .with_source(entry.source),
                ]
            } else {
                Vec::new()
            };
            DescribeConfigsResourceResult::default()
                .with_name(name)
                .with_value(value)
                .with_read_only(entry.read_only)
                .with_config_source(entry.source)
                .with_is_sensitive(false)
                .with_synonyms(synonyms)
                .with_config_type(entry.config_type.code())
                .with_documentation(
                    shown
                        .documentation
                        .then(|| StrBytes::from_static_str(entry.documentation)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::TopicDefaults;

    fn broker_resource(name: &str, keys: Option<Vec<&'static str>>) -> DescribeConfigsResource {
        DescribeConfigsResource::default()
            .with_resource_type(RESOURCE_TYPE_BROKER)
            .with_resource_name(StrBytes::from(name.to_string()))
            .with_configuration_keys(keys.map(|keys| {
                keys.into_iter()
                    .map(StrBytes::from_static_str)
                    .collect::<Vec<_>>()
            }))
    }

    fn context(defaults: &TopicDefaults) -> BrokerContext<'_> {
        BrokerContext {
            node_id: 2,
            auto_create_topics: true,
            config: ConfigContext {
                defaults,
                max_message_bytes: 8 * 1024 * 1024,
                default_replication_factor: None,
            },
        }
    }

    const BARE: Shown = Shown {
        documentation: false,
        synonyms: false,
    };

    #[test]
    fn given_this_gateway_id_should_describe_the_broker() {
        let defaults = TopicDefaults::default();
        let result = describe_broker(&context(&defaults), &broker_resource("2", None), BARE);
        assert_eq!(result.error_code, ERROR_NONE);
        let auto_create = result
            .configs
            .iter()
            .find(|config| config.name.as_str() == "auto.create.topics.enable")
            .expect("reported");
        assert_eq!(
            auto_create.value.as_ref().map(StrBytes::as_str),
            Some("true")
        );
        assert!(
            result
                .configs
                .iter()
                .all(|config| config.documentation.is_none())
        );
        assert!(result.configs.iter().all(|config| config.read_only));

        assert!(
            result
                .configs
                .iter()
                .all(|config| config.synonyms.is_empty())
        );

        let asked = broker_resource("2", Some(vec!["min.insync.replicas"]));
        let documented = Shown {
            documentation: true,
            ..BARE
        };
        let result = describe_broker(&context(&defaults), &asked, documented);
        assert_eq!(result.configs.len(), 1);
        assert!(result.configs[0].documentation.is_some());
    }

    #[test]
    fn given_include_synonyms_should_list_each_entry_as_its_own_synonym() {
        let defaults = TopicDefaults::default();
        let asked = broker_resource("2", Some(vec!["auto.create.topics.enable"]));
        let with_synonyms = Shown {
            synonyms: true,
            ..BARE
        };
        let result = describe_broker(&context(&defaults), &asked, with_synonyms);
        let entry = &result.configs[0];
        assert_eq!(
            entry.synonyms,
            [DescribeConfigsSynonym::default()
                .with_name(entry.name.clone())
                .with_value(entry.value.clone())
                .with_source(entry.config_source)]
        );
    }

    #[test]
    fn given_the_empty_broker_name_should_report_no_cluster_defaults() {
        let defaults = TopicDefaults::default();
        let result = describe_broker(&context(&defaults), &broker_resource("", None), BARE);
        assert_eq!(result.error_code, ERROR_NONE);
        assert_eq!(result.configs, [] as [DescribeConfigsResourceResult; 0]);
    }

    #[test]
    fn given_another_broker_id_should_answer_kafkas_invalid_request() {
        let defaults = TopicDefaults::default();
        let other = describe_broker(&context(&defaults), &broker_resource("7", None), BARE);
        assert_eq!(other.error_code, ERROR_INVALID_REQUEST);
        assert_eq!(
            other.error_message.as_ref().map(StrBytes::as_str),
            Some("Unexpected broker id, expected 2 or empty string, but received 7")
        );
        let named = describe_broker(&context(&defaults), &broker_resource("b1", None), BARE);
        assert_eq!(named.error_code, ERROR_INVALID_REQUEST);
        assert_eq!(
            named.error_message.as_ref().map(StrBytes::as_str),
            Some("Broker id must be an integer, but it is: b1")
        );
    }
}
