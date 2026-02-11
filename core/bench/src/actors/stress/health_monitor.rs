/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::error_classifier;
use super::stress_context::StressContext;
use crate::utils::{ClientFactory, login_root};
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{debug, info, warn};

const PING_INTERVAL_SECS: u64 = 5;
const STATS_INTERVAL_SECS: u64 = 10;
const METADATA_INTERVAL_SECS: u64 = 30;

/// Periodically probes server health and metadata convergence.
///
/// Runs `ping`, `get_stats`, `get_me`, `get_clients`, and metadata queries
/// at different frequencies. Reports latency degradation.
pub struct HealthMonitor {
    client_factory: Arc<dyn ClientFactory>,
    ctx: Arc<StressContext>,
}

impl HealthMonitor {
    pub fn new(client_factory: Arc<dyn ClientFactory>, ctx: Arc<StressContext>) -> Self {
        Self {
            client_factory,
            ctx,
        }
    }

    pub async fn run(self) {
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let mut tick = 0u64;
        while !self.ctx.is_cancelled() {
            // Ping every cycle (5s)
            self.probe_ping(&client).await;

            // Stats every 2 cycles (10s)
            if tick.is_multiple_of(STATS_INTERVAL_SECS / PING_INTERVAL_SECS) {
                self.probe_stats(&client).await;
            }

            if tick.is_multiple_of(METADATA_INTERVAL_SECS / PING_INTERVAL_SECS) {
                self.probe_metadata(&client).await;
            }

            tick += 1;
            tokio::time::sleep(std::time::Duration::from_secs(PING_INTERVAL_SECS)).await;
        }
    }

    async fn probe_ping(&self, client: &IggyClient) {
        let start = Instant::now();
        match client.ping().await {
            Ok(()) => {
                let latency = start.elapsed();
                self.ctx.stats.ping_ok.fetch_add(1, Ordering::Relaxed);
                if latency.as_millis() > 500 {
                    warn!("Health: ping latency {latency:?} exceeds 500ms");
                }
            }
            Err(e) => {
                self.ctx.stats.ping_err.fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                warn!("Health: ping failed: {e}");
            }
        }
    }

    async fn probe_stats(&self, client: &IggyClient) {
        let start = Instant::now();
        match client.get_stats().await {
            Ok(stats) => {
                let latency = start.elapsed();
                self.ctx.stats.get_stats_ok.fetch_add(1, Ordering::Relaxed);
                info!(
                    "Health: server stats in {latency:?} - messages: {}, streams: {}, topics: {}",
                    stats.messages_count, stats.streams_count, stats.topics_count
                );
            }
            Err(e) => {
                self.ctx.stats.get_stats_err.fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                warn!("Health: get_stats failed: {e}");
            }
        }
    }

    async fn probe_metadata(&self, client: &IggyClient) {
        // get_me
        match client.get_me().await {
            Ok(_) => {
                self.ctx.stats.get_me_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.ctx.stats.get_me_err.fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Health: get_me failed: {e}");
            }
        }

        // get_clients
        match client.get_clients().await {
            Ok(clients) => {
                self.ctx
                    .stats
                    .get_clients_ok
                    .fetch_add(1, Ordering::Relaxed);
                debug!("Health: {} connected clients", clients.len());
            }
            Err(e) => {
                self.ctx
                    .stats
                    .get_clients_err
                    .fetch_add(1, Ordering::Relaxed);
                error_classifier::record_error(&self.ctx.stats, &e);
                debug!("Health: get_clients failed: {e}");
            }
        }
    }
}
