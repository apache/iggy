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

//! What an operator reads from a running gateway.
//!
//! [`HttpEndpoint`] serves Prometheus metrics, readiness and liveness at `IGGY_KAFKA_HTTP_ADDR`.
//! Each closed Kafka connection is counted by its [`CloseCause`], and logged at the level that
//! matches who can act on that cause. Warnings are rate limited per cause. See
//! `docs/OBSERVABILITY.md`.

mod census;
mod disconnect;
mod health;
mod http;
mod metrics;
mod response_errors;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;

pub use census::{
    Census, CensusSource, CoordinatorCensus, GroupCensus, ListenerCensus, Pool, PoolCensus,
    ProduceSessionCensus,
};
pub use disconnect::{CloseCause, Disconnect, FrameTimeout};
pub use health::{Readiness, ReadyState};
pub use http::{HealthCheck, HttpEndpoint, check};
pub use metrics::GatewayMetrics;

use crate::bridge::Reach;
use crate::listener::SecurityProtocol;
use crate::protocol::api::HandleOutcome;
use disconnect::DisconnectLog;

/// The metrics, the readiness and the disconnect log of one gateway. The connections write to it,
/// and the HTTP endpoint reads it.
pub struct Observability {
    metrics: GatewayMetrics,
    readiness: Readiness,
    disconnects: DisconnectLog,
    /// What a scrape reads of the serving gateway. Set while `KafkaGateway::run` serves.
    census: Mutex<Option<Arc<dyn CensusSource>>>,
    /// Whether an [`HttpEndpoint`] serves this. Without one nothing reads the readiness, so the
    /// gateway opens no Iggy session to follow Iggy for it.
    published: AtomicBool,
}

impl Default for Observability {
    fn default() -> Self {
        Self::new()
    }
}

impl Observability {
    #[must_use]
    pub fn new() -> Self {
        Self {
            metrics: GatewayMetrics::new(),
            readiness: Readiness::default(),
            disconnects: DisconnectLog::default(),
            census: Mutex::default(),
            published: AtomicBool::new(false),
        }
    }

    /// Whether an [`HttpEndpoint`] serves this, so the readiness has a reader.
    #[must_use]
    pub fn is_published(&self) -> bool {
        self.published.load(Ordering::Acquire)
    }

    fn publish(&self) {
        self.published.store(true, Ordering::Release);
    }

    #[must_use]
    pub const fn metrics(&self) -> &GatewayMetrics {
        &self.metrics
    }

    #[must_use]
    pub const fn readiness(&self) -> &Readiness {
        &self.readiness
    }

    /// Counts one answered request of `api_key` at `api_version`, with the error codes its
    /// response carries.
    pub fn record_response(&self, api_key: i16, api_version: i16, outcome: &HandleOutcome) {
        self.metrics.record_response(api_key, api_version, outcome);
    }

    /// Records how long one request of `api_key` took, from its frame read in full to its response
    /// written.
    pub fn observe_duration(&self, api_key: i16, elapsed: std::time::Duration) {
        self.metrics.observe_duration(api_key, elapsed);
    }

    /// Counts a connection that a listener accepted.
    pub fn connection_accepted(&self, listener: SecurityProtocol) {
        self.metrics.connection_accepted(listener);
    }

    /// Counts and logs a connection that `listener` refused because it holds its maximum.
    pub fn connection_refused(&self, peer: SocketAddr, listener: SecurityProtocol, limit: usize) {
        self.metrics.connection_refused(listener);
        self.disconnects.log(
            peer,
            listener,
            &Disconnect::with_detail(
                CloseCause::ConnectionLimit,
                format!("IGGY_KAFKA_MAX_CONNECTIONS is {limit}"),
            ),
        );
    }

    /// Counts and logs the end of a connection that `listener` accepted.
    pub fn connection_closed(
        &self,
        peer: SocketAddr,
        listener: SecurityProtocol,
        disconnect: &Disconnect,
    ) {
        self.metrics.connection_closed(listener, disconnect.cause());
        self.disconnects.log(peer, listener, disconnect);
    }

    /// Marks the gateway as serving. `source` answers the census of each scrape, and `iggy`
    /// says whether the bridge reaches Iggy, when there is a bridge.
    pub fn serving(&self, source: Arc<dyn CensusSource>, iggy: Option<watch::Receiver<Reach>>) {
        *self.census.lock().unwrap_or_else(PoisonError::into_inner) = Some(source);
        self.readiness.serving(iggy);
    }

    /// Marks the gateway as draining: `/ready` answers 503 from here on.
    pub fn draining(&self) {
        self.readiness.draining();
    }

    /// Ends the census of the serving gateway, once its connections are gone.
    pub fn stopped(&self) {
        self.census
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    /// Every metric in the `OpenMetrics` text format, with the census read now.
    pub async fn render_metrics(&self) -> String {
        let source = self
            .census
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut census = match source {
            Some(source) => source.census().await,
            None => Census::default(),
        };
        census.iggy_reachable = self.readiness.iggy_reachable();
        self.metrics.render(census, &self.readiness.state())
    }
}
