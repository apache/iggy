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

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use tracing::error;

#[derive(Debug)]
pub(crate) struct Metrics {
    registry: Registry,
    http_requests: Counter,
    streams: Gauge,
    topics: Gauge,
    partitions: Gauge,
    segments: Gauge,
    messages: Gauge,
    users: Gauge,
    clients: Gauge,
}

impl Metrics {
    pub fn init() -> Self {
        let mut metrics = Metrics {
            registry: <Registry>::default(),
            http_requests: Counter::default(),
            streams: Gauge::default(),
            topics: Gauge::default(),
            partitions: Gauge::default(),
            segments: Gauge::default(),
            messages: Gauge::default(),
            users: Gauge::default(),
            clients: Gauge::default(),
        };

        metrics.register_counter("http_requests", metrics.http_requests.clone());
        metrics.register_gauge("streams", metrics.streams.clone());
        metrics.register_gauge("topics", metrics.topics.clone());
        metrics.register_gauge("partitions", metrics.partitions.clone());
        metrics.register_gauge("segments", metrics.segments.clone());
        metrics.register_gauge("messages", metrics.messages.clone());
        metrics.register_gauge("users", metrics.users.clone());
        metrics.register_gauge("clients", metrics.clients.clone());

        metrics
    }

    fn register_counter(&mut self, name: &str, counter: Counter) {
        self.registry
            .register(name, format!("total count of {name}"), counter)
    }

    fn register_gauge(&mut self, name: &str, gauge: Gauge) {
        self.registry
            .register(name, format!("total count of {name}"), gauge)
    }

    pub fn get_formatted_output(&self) -> String {
        let mut buffer = String::new();
        if let Err(err) = encode(&mut buffer, &self.registry) {
            error!("Failed to encode metrics: {}", err);
        }
        buffer
    }

    pub fn increment_http_requests(&self) {
        self.http_requests.inc();
    }

    pub fn increment_streams(&self, count: u32) {
        self.streams.inc_by(count as i64);
    }

    pub fn decrement_streams(&self, count: u32) {
        self.streams.dec_by(count as i64);
    }

    pub fn increment_topics(&self, count: u32) {
        self.topics.inc_by(count as i64);
    }

    pub fn decrement_topics(&self, count: u32) {
        self.topics.dec_by(count as i64);
    }

    pub fn increment_partitions(&self, count: u32) {
        self.partitions.inc_by(count as i64);
    }

    pub fn decrement_partitions(&self, count: u32) {
        self.partitions.dec_by(count as i64);
    }

    pub fn increment_segments(&self, count: u32) {
        self.segments.inc_by(count as i64);
    }

    pub fn decrement_segments(&self, count: u32) {
        self.segments.dec_by(count as i64);
    }

    pub fn increment_messages(&self, count: u64) {
        self.messages.inc_by(count as i64);
    }

    pub fn decrement_messages(&self, count: u64) {
        self.messages.dec_by(count as i64);
    }

    pub fn increment_users(&self, count: u32) {
        self.users.inc_by(count as i64);
    }

    pub fn decrement_users(&self, count: u32) {
        self.users.dec_by(count as i64);
    }

    pub fn increment_clients(&self, count: u32) {
        self.clients.inc_by(count as i64);
    }

    pub fn decrement_clients(&self, count: u32) {
        self.clients.dec_by(count as i64);
    }
}
