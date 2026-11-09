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

//! The metrics that the request path writes: requests, latency, error codes and connections.

use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::ApiKey;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::{Registry, Unit};

use crate::listener::SecurityProtocol;
use crate::observability::census::{Census, CensusCollector};
use crate::observability::disconnect::CloseCause;
use crate::observability::health::ReadyState;
use crate::observability::response_errors::error_codes;
use crate::protocol::api::{
    HandleOutcome, acl_advertised_ranges, is_supported_version, sasl_advertised_ranges,
};

/// The prefix of every metric name.
const PREFIX: &str = "iggy_kafka_gateway";

/// Upper bounds of the request latency buckets, in seconds. From a Metadata answered in memory
/// to a `JoinGroup` that waits out a rebalance.
const DURATION_BUCKETS: [f64; 16] = [
    0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0,
];

/// The metrics of one gateway, in a registry of their own.
pub struct GatewayMetrics {
    registry: Registry,
    requests: Family<RequestLabels, Counter>,
    request_duration: Family<ApiLabels, Histogram, fn() -> Histogram>,
    response_errors: Family<ErrorLabels, Counter>,
    connections_accepted: Family<ListenerLabels, Counter>,
    connections_refused: Family<ListenerLabels, Counter>,
    connections_closed: Family<CloseLabels, Counter>,
    census: Arc<CensusCollector>,
}

impl Default for GatewayMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl GatewayMetrics {
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix(PREFIX);
        let requests = Family::<RequestLabels, Counter>::default();
        let request_duration =
            Family::<ApiLabels, Histogram, fn() -> Histogram>::new_with_constructor(|| {
                Histogram::new(DURATION_BUCKETS)
            });
        let response_errors = Family::<ErrorLabels, Counter>::default();
        let connections_accepted = Family::<ListenerLabels, Counter>::default();
        let connections_refused = Family::<ListenerLabels, Counter>::default();
        let connections_closed = Family::<CloseLabels, Counter>::default();
        let census = Arc::new(CensusCollector::default());

        registry.register(
            "build",
            "The version of the gateway",
            Info::new([("version", env!("CARGO_PKG_VERSION"))]),
        );
        registry.register(
            "requests",
            "Kafka requests answered, per API and version. A version outside the ones the gateway \
             serves is counted as unsupported",
            requests.clone(),
        );
        registry.register_with_unit(
            "request_duration",
            "Time from a request read in full to its response written, per API",
            Unit::Seconds,
            request_duration.clone(),
        );
        registry.register(
            "response_errors",
            "Error codes in the responses sent, per API. A response counts each partition, topic \
             or group that carries a code other than NONE",
            response_errors.clone(),
        );
        registry.register(
            "connections_accepted",
            "Kafka connections accepted, per listener",
            connections_accepted.clone(),
        );
        registry.register(
            "connections_refused",
            "Kafka connections refused because the listener held IGGY_KAFKA_MAX_CONNECTIONS",
            connections_refused.clone(),
        );
        registry.register(
            "connections_closed",
            "Kafka connections closed, per listener and cause",
            connections_closed.clone(),
        );
        registry.register_collector(Box::new(Arc::clone(&census)));

        Self {
            registry,
            requests,
            request_duration,
            response_errors,
            connections_accepted,
            connections_refused,
            connections_closed,
            census,
        }
    }

    /// Counts one answered request, and each error code its response carries.
    pub fn record_response(&self, api_key: i16, api_version: i16, outcome: &HandleOutcome) {
        let api = Api::of(api_key);
        self.requests
            .get_or_create(&RequestLabels {
                api,
                version: Version::of(api_key, api_version),
            })
            .inc();
        let (HandleOutcome::Respond(body) | HandleOutcome::RespondThenClose(body)) = outcome else {
            return;
        };
        // Counted per code before the family is touched, so a response with many failed
        // partitions takes the family's lock once per code, not once per partition.
        let mut counts: Vec<(i16, u64)> = Vec::new();
        error_codes(api_key, api_version, body, &mut |code| match counts
            .iter_mut()
            .find(|(seen, _)| *seen == code)
        {
            Some((_, count)) => *count += 1,
            None => counts.push((code, 1)),
        });
        for (code, count) in counts {
            self.response_errors
                .get_or_create(&ErrorLabels {
                    api,
                    code,
                    error: ErrorName(code),
                })
                .inc_by(count);
        }
    }

    /// Records how long one request of `api_key` took.
    pub fn observe_duration(&self, api_key: i16, elapsed: Duration) {
        self.request_duration
            .get_or_create(&ApiLabels {
                api: Api::of(api_key),
            })
            .observe(elapsed.as_secs_f64());
    }

    pub(super) fn connection_accepted(&self, listener: SecurityProtocol) {
        self.connections_accepted
            .get_or_create(&ListenerLabels {
                listener: Listener(listener),
            })
            .inc();
    }

    pub(super) fn connection_refused(&self, listener: SecurityProtocol) {
        self.connections_refused
            .get_or_create(&ListenerLabels {
                listener: Listener(listener),
            })
            .inc();
    }

    pub(super) fn connection_closed(&self, listener: SecurityProtocol, cause: CloseCause) {
        self.connections_closed
            .get_or_create(&CloseLabels {
                listener: Listener(listener),
                cause,
            })
            .inc();
    }

    /// Every metric in the `OpenMetrics` text format, with `census` and `ready` as the state read
    /// at this scrape.
    pub(super) fn render(&self, census: Census, ready: &ReadyState) -> String {
        self.census.publish(census, ready.clone());
        let mut text = String::new();
        if let Err(error) = encode(&mut text, &self.registry) {
            tracing::warn!(%error, "failed to encode the metrics");
        }
        text
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RequestLabels {
    api: Api,
    version: Version,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ApiLabels {
    api: Api,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ErrorLabels {
    api: Api,
    code: i16,
    error: ErrorName,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ListenerLabels {
    listener: Listener,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CloseLabels {
    listener: Listener,
    cause: CloseCause,
}

/// A Kafka API by its name, as Kafka's own request metrics name it. A key that Kafka does not
/// define is `unknown`, so a client cannot add a label value per key it makes up.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Api(Option<ApiKey>);

impl Api {
    fn of(api_key: i16) -> Self {
        Self(ApiKey::try_from(api_key).ok())
    }
}

impl EncodeLabelValue for Api {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        match self.0 {
            Some(key) => write!(encoder, "{key:?}"),
            None => encoder.write_str("unknown"),
        }
    }
}

/// A request version the gateway serves, or `None`, which is labeled `unsupported`. Bounded for
/// the same reason as [`Api`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Version(Option<i16>);

impl Version {
    fn of(api_key: i16, api_version: i16) -> Self {
        let advertised_by_listener = sasl_advertised_ranges()
            .iter()
            .chain(acl_advertised_ranges())
            .any(|range| {
                range.api_key == api_key
                    && (range.min_version..=range.max_version).contains(&api_version)
            });
        let served = advertised_by_listener || is_supported_version(api_key, api_version);
        Self(served.then_some(api_version))
    }
}

impl EncodeLabelValue for Version {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        match self.0 {
            Some(version) => write!(encoder, "{version}"),
            None => encoder.write_str("unsupported"),
        }
    }
}

/// A Kafka error code by its name in Kafka's `Errors` enum, such as `NOT_LEADER_OR_FOLLOWER`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct ErrorName(i16);

impl EncodeLabelValue for ErrorName {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        encoder.write_str(&kafka_error_name(self.0))
    }
}

/// `NotLeaderOrFollower` from `kafka_protocol` becomes `NOT_LEADER_OR_FOLLOWER`.
fn kafka_error_name(code: i16) -> String {
    let error = match ResponseError::try_from_code(code) {
        None => return "NONE".to_string(),
        Some(ResponseError::Unknown(_)) => return "UNKNOWN".to_string(),
        Some(error) => error.to_string(),
    };
    let mut name = String::with_capacity(error.len() + 8);
    for (index, character) in error.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            name.push('_');
        }
        name.push(character.to_ascii_uppercase());
    }
    name
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Listener(SecurityProtocol);

impl EncodeLabelValue for Listener {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        write!(encoder, "{}", self.0)
    }
}

impl EncodeLabelValue for CloseCause {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        encoder.write_str(self.label())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
    };
    use kafka_protocol::protocol::StrBytes;

    use super::*;
    use crate::observability::census::{
        CoordinatorCensus, GroupCensus, ListenerCensus, Pool, PoolCensus, ProduceSessionCensus,
    };
    use crate::protocol::api::{
        API_KEY_FETCH, API_KEY_PRODUCE, API_KEY_SASL_HANDSHAKE, ERROR_NOT_LEADER_OR_FOLLOWER,
        ERROR_REQUEST_TIMED_OUT,
    };
    use crate::protocol::handlers::encode_message;

    fn rendered(metrics: &GatewayMetrics) -> String {
        metrics.render(Census::default(), &ReadyState::Starting)
    }

    /// The value of the sample `series` in `text`.
    fn sample(text: &str, series: &str) -> Option<f64> {
        text.lines().find_map(|line| {
            let value = line.strip_prefix(series)?.strip_prefix(' ')?;
            value.split_whitespace().next()?.parse().ok()
        })
    }

    #[test]
    fn given_a_census_should_render_each_of_its_series() {
        let census = Census {
            listeners: vec![ListenerCensus {
                protocol: SecurityProtocol::SaslSsl,
                open: 3,
                limit: 100,
            }],
            pools: vec![
                PoolCensus {
                    pool: Pool::Fetch,
                    used: 2,
                    capacity: 16,
                },
                PoolCensus {
                    pool: Pool::ProduceLarge,
                    used: 1,
                    capacity: 4,
                },
                PoolCensus {
                    pool: Pool::Authentication,
                    used: 5,
                    capacity: 6,
                },
                PoolCensus {
                    pool: Pool::CertificateLookup,
                    used: 0,
                    capacity: 6,
                },
            ],
            produce_memory: Some((1_024, 4_096)),
            produce_sessions: Some(ProduceSessionCensus {
                own: 7,
                limit: 8,
                shared: 9,
            }),
            groups: GroupCensus {
                states: vec![("Stable", 2), ("Empty", 1)],
                members: 5,
            },
            coordinator: Some(CoordinatorCensus {
                served: vec![1, 3],
                partitions: 4,
                lease_age: Duration::from_millis(1_500),
            }),
            sends_in_doubt: Some(11),
            iggy_reachable: Some(true),
        };
        let text = GatewayMetrics::new().render(census, &ReadyState::Ready);

        let expected = [
            ("iggy_kafka_gateway_ready", 1.0),
            ("iggy_kafka_gateway_iggy_reachable", 1.0),
            (
                "iggy_kafka_gateway_connections_open{listener=\"SASL_SSL\"}",
                3.0,
            ),
            (
                "iggy_kafka_gateway_connections_limit{listener=\"SASL_SSL\"}",
                100.0,
            ),
            ("iggy_kafka_gateway_slots_used{pool=\"fetch\"}", 2.0),
            ("iggy_kafka_gateway_slots{pool=\"fetch\"}", 16.0),
            ("iggy_kafka_gateway_slots_used{pool=\"produce_large\"}", 1.0),
            ("iggy_kafka_gateway_slots{pool=\"produce_large\"}", 4.0),
            (
                "iggy_kafka_gateway_slots_used{pool=\"authentication\"}",
                5.0,
            ),
            ("iggy_kafka_gateway_slots{pool=\"authentication\"}", 6.0),
            (
                "iggy_kafka_gateway_slots_used{pool=\"certificate_lookup\"}",
                0.0,
            ),
            ("iggy_kafka_gateway_slots{pool=\"certificate_lookup\"}", 6.0),
            ("iggy_kafka_gateway_produce_memory_used_bytes", 1_024.0),
            ("iggy_kafka_gateway_produce_memory_capacity_bytes", 4_096.0),
            ("iggy_kafka_gateway_produce_sessions", 7.0),
            ("iggy_kafka_gateway_produce_sessions_limit", 8.0),
            ("iggy_kafka_gateway_produce_shared_producers", 9.0),
            ("iggy_kafka_gateway_produce_sends_in_doubt_total", 11.0),
            ("iggy_kafka_gateway_groups{state=\"Stable\"}", 2.0),
            ("iggy_kafka_gateway_groups{state=\"Empty\"}", 1.0),
            ("iggy_kafka_gateway_group_members", 5.0),
            ("iggy_kafka_gateway_coordinator_partitions_served", 2.0),
            (
                "iggy_kafka_gateway_coordinator_partition_served{partition=\"0\"}",
                0.0,
            ),
            (
                "iggy_kafka_gateway_coordinator_partition_served{partition=\"1\"}",
                1.0,
            ),
            (
                "iggy_kafka_gateway_coordinator_partition_served{partition=\"3\"}",
                1.0,
            ),
            ("iggy_kafka_gateway_coordinator_lease_age_seconds", 1.5),
        ];
        for (series, value) in expected {
            assert_eq!(sample(&text, series), Some(value), "{series} in:\n{text}");
        }
    }

    fn produce_response(codes: &[i16]) -> Bytes {
        let partitions = codes
            .iter()
            .enumerate()
            .map(|(index, code)| {
                PartitionProduceResponse::default()
                    .with_index(i32::try_from(index).unwrap())
                    .with_error_code(*code)
            })
            .collect();
        let response = ProduceResponse::default().with_responses(vec![
            TopicProduceResponse::default()
                .with_name(StrBytes::from_static_str("orders").into())
                .with_partition_responses(partitions),
        ]);
        encode_message(&response, 9, 64).unwrap()
    }

    #[test]
    fn given_kafka_error_codes_should_name_them_as_kafka_does() {
        assert_eq!(kafka_error_name(6), "NOT_LEADER_OR_FOLLOWER");
        assert_eq!(kafka_error_name(-1), "UNKNOWN_SERVER_ERROR");
        assert_eq!(kafka_error_name(58), "SASL_AUTHENTICATION_FAILED");
        assert_eq!(kafka_error_name(0), "NONE");
        assert_eq!(kafka_error_name(31_000), "UNKNOWN");
    }

    #[test]
    fn given_a_response_with_failed_partitions_should_count_each_code_per_api() {
        let metrics = GatewayMetrics::new();
        let body = produce_response(&[
            ERROR_NOT_LEADER_OR_FOLLOWER,
            0,
            ERROR_NOT_LEADER_OR_FOLLOWER,
            ERROR_REQUEST_TIMED_OUT,
        ]);

        metrics.record_response(API_KEY_PRODUCE, 9, &HandleOutcome::Respond(body));

        let text = rendered(&metrics);
        assert!(
            text.contains(
                "iggy_kafka_gateway_response_errors_total{api=\"Produce\",code=\"6\",\
                 error=\"NOT_LEADER_OR_FOLLOWER\"} 2"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "iggy_kafka_gateway_response_errors_total{api=\"Produce\",code=\"7\",\
                 error=\"REQUEST_TIMED_OUT\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains("iggy_kafka_gateway_requests_total{api=\"Produce\",version=\"9\"} 1"),
            "{text}"
        );
        assert!(!text.contains("error=\"NONE\""), "{text}");
    }

    #[test]
    fn given_keys_and_versions_a_client_makes_up_should_keep_the_labels_bounded() {
        let metrics = GatewayMetrics::new();
        for version in [99, 100, 1_000] {
            metrics.record_response(API_KEY_FETCH, version, &HandleOutcome::Close);
        }
        metrics.record_response(4_242, 0, &HandleOutcome::Close);
        metrics.record_response(API_KEY_SASL_HANDSHAKE, 1, &HandleOutcome::Close);

        let text = rendered(&metrics);
        assert!(
            text.contains(
                "iggy_kafka_gateway_requests_total{api=\"Fetch\",version=\"unsupported\"} 3"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "iggy_kafka_gateway_requests_total{api=\"unknown\",version=\"unsupported\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "iggy_kafka_gateway_requests_total{api=\"SaslHandshake\",version=\"1\"} 1"
            ),
            "a SASL version that a listener advertises is served: {text}"
        );
    }

    #[test]
    fn given_a_request_duration_should_observe_it_in_seconds_per_api() {
        let metrics = GatewayMetrics::new();
        metrics.observe_duration(API_KEY_PRODUCE, Duration::from_millis(3));

        let text = rendered(&metrics);
        assert!(
            text.contains(
                "iggy_kafka_gateway_request_duration_seconds_bucket{le=\"0.005\",api=\"Produce\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "iggy_kafka_gateway_request_duration_seconds_bucket{le=\"0.0025\",api=\"Produce\"} 0"
            ),
            "{text}"
        );
    }

    #[test]
    fn given_closed_connections_should_count_them_per_listener_and_cause() {
        let metrics = GatewayMetrics::new();
        metrics.connection_accepted(SecurityProtocol::SaslSsl);
        metrics.connection_closed(SecurityProtocol::SaslSsl, CloseCause::FrameTooLarge);
        metrics.connection_refused(SecurityProtocol::Plaintext);

        let text = rendered(&metrics);
        assert!(
            text.contains(
                "iggy_kafka_gateway_connections_closed_total{listener=\"SASL_SSL\",\
                 cause=\"frame_too_large\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains("iggy_kafka_gateway_connections_accepted_total{listener=\"SASL_SSL\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("iggy_kafka_gateway_connections_refused_total{listener=\"PLAINTEXT\"} 1"),
            "{text}"
        );
        assert!(
            text.contains(&format!(
                "iggy_kafka_gateway_build_info{{version=\"{}\"}} 1",
                env!("CARGO_PKG_VERSION")
            )),
            "{text}"
        );
    }
}
