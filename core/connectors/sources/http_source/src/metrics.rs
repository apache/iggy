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

//! Gateway-side Prometheus metrics, served on the admin listener.
//!
//! The runtime's own stage histograms start at `poll()`, so they cannot see
//! the number that matters most for a webhook gateway: how long a sender
//! waited between TCP accept and its 200. This connector is an HTTP server,
//! so it measures that itself.
//!
//! Names carry an `http_source_` prefix to keep them clear of the runtime's
//! `iggy_connector_` family. One registry per shared listener, with every
//! series labelled by instance.

use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

use crate::SharedState;
use crate::routes::EndpointOrigin;
use crate::types::unix_now_seconds;

/// Instance label for requests that never resolved to one, so a scan for
/// live endpoint ids still shows up rather than going uncounted.
pub const UNROUTED: &str = "unrouted";

/// Sub-millisecond at the low end: accepting a webhook is a route lookup, a
/// signature check, and a channel send, so the interesting range is tight.
const REQUEST_BUCKETS_SECONDS: [f64; 12] = [
    50e-6, 100e-6, 250e-6, 500e-6, 1e-3, 2.5e-3, 5e-3, 10e-3, 25e-3, 50e-3, 100e-3, 500e-3,
];

/// Which surface a request arrived on.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum PathKind {
    Named,
    Secret,
}

/// Response class, coarse on purpose: a per-status-code label would let a
/// caller inflate cardinality by probing.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StatusClass {
    Success,
    ClientError,
    ServerError,
}

impl From<u16> for StatusClass {
    fn from(status: u16) -> Self {
        match status {
            200..=399 => Self::Success,
            400..=499 => Self::ClientError,
            _ => Self::ServerError,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct InstanceLabel {
    pub instance: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestLabels {
    pub instance: String,
    pub kind: PathKind,
    pub status: StatusClass,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DurationLabels {
    pub instance: String,
    pub status: StatusClass,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct EndpointLabels {
    pub instance: String,
    pub kind: EndpointOrigin,
}

#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    requests: Family<RequestLabels, Counter>,
    request_duration_seconds: Family<DurationLabels, Histogram, fn() -> Histogram>,
    rejected_full: Family<InstanceLabel, Counter>,
    dropped_on_close: Family<InstanceLabel, Counter>,
    headers_clamped: Family<InstanceLabel, Counter>,
    headers_dropped: Family<InstanceLabel, Counter>,
    buffer_used: Family<InstanceLabel, Gauge>,
    buffer_capacity: Family<InstanceLabel, Gauge>,
    endpoints_active: Family<EndpointLabels, Gauge>,
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let requests = Family::<RequestLabels, Counter>::default();
        let request_duration_seconds: Family<DurationLabels, Histogram, fn() -> Histogram> =
            Family::new_with_constructor(request_histogram);
        let rejected_full = Family::<InstanceLabel, Counter>::default();
        let dropped_on_close = Family::<InstanceLabel, Counter>::default();
        let headers_clamped = Family::<InstanceLabel, Counter>::default();
        let headers_dropped = Family::<InstanceLabel, Counter>::default();
        let buffer_used = Family::<InstanceLabel, Gauge>::default();
        let buffer_capacity = Family::<InstanceLabel, Gauge>::default();
        let endpoints_active = Family::<EndpointLabels, Gauge>::default();

        registry.register(
            "http_source_requests",
            "Webhook requests by path kind and response class",
            requests.clone(),
        );
        registry.register(
            "http_source_request_duration_seconds",
            "Time from request accepted to response, in seconds",
            request_duration_seconds.clone(),
        );
        registry.register(
            "http_source_rejected_full",
            "Requests answered 429 because the instance bridge was full",
            rejected_full.clone(),
        );
        registry.register(
            "http_source_dropped_on_close",
            "Accepted messages still queued when the instance closed",
            dropped_on_close.clone(),
        );
        registry.register(
            "http_source_headers_clamped",
            "Forwarded header values truncated to the Iggy 255-byte limit",
            headers_clamped.clone(),
        );
        registry.register(
            "http_source_headers_dropped",
            "Forwarded header values Iggy would have rejected outright",
            headers_dropped.clone(),
        );
        registry.register(
            "http_source_buffer_used",
            "Messages queued in the instance bridge",
            buffer_used.clone(),
        );
        registry.register(
            "http_source_buffer_capacity",
            "Configured capacity of the instance bridge",
            buffer_capacity.clone(),
        );
        registry.register(
            "http_source_endpoints_active",
            "Secret-path endpoints currently accepting requests, by origin",
            endpoints_active.clone(),
        );

        Metrics {
            registry,
            requests,
            request_duration_seconds,
            rejected_full,
            dropped_on_close,
            headers_clamped,
            headers_dropped,
            buffer_used,
            buffer_capacity,
            endpoints_active,
        }
    }

    /// Labels are built per call rather than cached per instance: one small
    /// `String` clone is noise next to copying the request body, and the
    /// cache would have to cover every kind and status combination.
    pub fn record_request(&self, instance: &str, kind: PathKind, status: u16, elapsed: Duration) {
        let status = StatusClass::from(status);
        self.requests
            .get_or_create(&RequestLabels {
                instance: instance.to_owned(),
                kind,
                status,
            })
            .inc();
        self.request_duration_seconds
            .get_or_create(&DurationLabels {
                instance: instance.to_owned(),
                status,
            })
            .observe(elapsed.as_secs_f64());
    }

    pub fn record_rejected_full(&self, instance: &str) {
        self.rejected_full.get_or_create(&label(instance)).inc();
    }

    pub fn record_dropped_on_close(&self, instance: &str, dropped: u64) {
        self.dropped_on_close
            .get_or_create(&label(instance))
            .inc_by(dropped);
    }

    pub fn record_headers(&self, instance: &str, clamped: u64, dropped: u64) {
        if clamped > 0 {
            self.headers_clamped
                .get_or_create(&label(instance))
                .inc_by(clamped);
        }
        if dropped > 0 {
            self.headers_dropped
                .get_or_create(&label(instance))
                .inc_by(dropped);
        }
    }

    /// Reads without creating. `get_or_create` would materialise a zero
    /// series as a side effect, so an admin health check would make counters
    /// appear in every later scrape - contradicting the documented rule that
    /// an untouched family is absent rather than zero.
    pub fn headers_clamped(&self, instance: &str) -> u64 {
        self.headers_clamped
            .get(&label(instance))
            .map_or(0, |counter| counter.get())
    }

    pub fn headers_dropped(&self, instance: &str) -> u64 {
        self.headers_dropped
            .get(&label(instance))
            .map_or(0, |counter| counter.get())
    }

    /// Drops a departed instance's sampled gauges.
    ///
    /// Counters stay: they are cumulative and their last value remains true.
    /// Gauges are instantaneous, so leaving them behind would report a queue
    /// depth for an instance that no longer exists.
    pub fn forget_instance(&self, instance: &str) {
        let label = label(instance);
        self.buffer_used.remove(&label);
        self.buffer_capacity.remove(&label);
        for origin in [EndpointOrigin::Static, EndpointOrigin::Dynamic] {
            self.endpoints_active.remove(&EndpointLabels {
                instance: instance.to_owned(),
                kind: origin,
            });
        }
    }

    /// Refreshes the sampled gauges and renders the Prometheus text format.
    ///
    /// The gauges are read from the instances at scrape time rather than
    /// maintained on the hot path: queue depth and endpoint counts are only
    /// meaningful as instantaneous values, and nothing observes them between
    /// scrapes.
    pub fn encode(&self, instances: &[Arc<SharedState>]) -> String {
        for instance in instances {
            let label = label(&instance.instance_name);
            self.buffer_used
                .get_or_create(&label)
                .set(instance.sender.len() as i64);
            self.buffer_capacity
                .get_or_create(&label)
                .set(instance.config.buffer_capacity as i64);

            let registry = instance.registry();
            let now = unix_now_seconds();
            for origin in [EndpointOrigin::Static, EndpointOrigin::Dynamic] {
                self.endpoints_active
                    .get_or_create(&EndpointLabels {
                        instance: instance.instance_name.clone(),
                        kind: origin,
                    })
                    .set(registry.serving_count_by_origin(origin, now) as i64);
            }
        }

        let mut buffer = String::new();
        if let Err(error) = encode(&mut buffer, &self.registry) {
            error!(
                "Failed to encode {} metrics. {error}",
                crate::CONNECTOR_NAME
            );
        }
        buffer
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new()
    }
}

fn label(instance: &str) -> InstanceLabel {
    InstanceLabel {
        instance: instance.to_owned(),
    }
}

fn request_histogram() -> Histogram {
    Histogram::new(REQUEST_BUCKETS_SECONDS.iter().copied())
}

impl EncodeLabelValue for PathKind {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        match self {
            Self::Named => "named",
            Self::Secret => "secret",
        }
        .encode(encoder)
    }
}

impl EncodeLabelValue for StatusClass {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        match self {
            Self::Success => "2xx",
            Self::ClientError => "4xx",
            Self::ServerError => "5xx",
        }
        .encode(encoder)
    }
}

impl EncodeLabelValue for EndpointOrigin {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        match self {
            Self::Static => "static",
            Self::Dynamic => "dynamic",
        }
        .encode(encoder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_status_codes_when_classified_should_collapse_to_three_classes() {
        assert_eq!(StatusClass::from(200), StatusClass::Success);
        assert_eq!(StatusClass::from(204), StatusClass::Success);
        assert_eq!(StatusClass::from(404), StatusClass::ClientError);
        assert_eq!(StatusClass::from(429), StatusClass::ClientError);
        assert_eq!(StatusClass::from(500), StatusClass::ServerError);
    }

    #[test]
    fn given_recorded_requests_when_encoded_should_carry_prefixed_labelled_series() {
        let metrics = Metrics::new();

        metrics.record_request(
            "http_github",
            PathKind::Secret,
            200,
            Duration::from_micros(80),
        );
        metrics.record_request(
            "http_github",
            PathKind::Named,
            401,
            Duration::from_micros(40),
        );
        metrics.record_rejected_full("http_github");

        let encoded = metrics.encode(&[]);
        assert!(encoded.contains(
            "http_source_requests_total{instance=\"http_github\",kind=\"secret\",status=\"2xx\"} 1"
        ));
        assert!(encoded.contains(
            "http_source_requests_total{instance=\"http_github\",kind=\"named\",status=\"4xx\"} 1"
        ));
        assert!(
            encoded.contains("http_source_rejected_full_total{instance=\"http_github\"} 1"),
            "a 429 needs its own series: it is backpressure, not a caller error"
        );
        assert!(encoded.contains("http_source_request_duration_seconds_bucket"));
    }

    #[test]
    fn given_header_losses_when_recorded_should_count_clamps_and_drops_apart() {
        let metrics = Metrics::new();

        metrics.record_headers("http_github", 2, 5);
        metrics.record_headers("http_github", 1, 0);

        // Distinct totals on purpose: equal ones would survive the two
        // counters being swapped, which is the bug this test exists to catch.
        assert_eq!(metrics.headers_clamped("http_github"), 3);
        assert_eq!(metrics.headers_dropped("http_github"), 5);
    }

    #[test]
    fn given_untouched_families_when_encoded_should_omit_them_until_first_use() {
        let metrics = Metrics::new();

        let empty = metrics.encode(&[]);
        assert!(
            empty.trim() == "# EOF",
            "a labelled family has no series to emit before it is touched, so \
             dashboards must treat these as absent rather than zero"
        );

        metrics.record_rejected_full("http_github");

        assert!(
            metrics
                .encode(&[])
                .contains("# TYPE http_source_rejected_full counter"),
            "and it must appear the moment there is something to report"
        );
    }
}
