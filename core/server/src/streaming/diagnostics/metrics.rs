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
use std::sync::Arc;
use tracing::error;

#[derive(Debug, Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
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
        let mut registry = Registry::default();

        let http_requests = Counter::default();
        let streams = Gauge::default();
        let topics = Gauge::default();
        let partitions = Gauge::default();
        let segments = Gauge::default();
        let messages = Gauge::default();
        let users = Gauge::default();
        let clients = Gauge::default();

        registry.register(
            "http_requests",
            "total count of http_requests",
            http_requests.clone(),
        );
        registry.register("streams", "total count of streams", streams.clone());
        registry.register("topics", "total count of topics", topics.clone());
        registry.register(
            "partitions",
            "total count of partitions",
            partitions.clone(),
        );
        registry.register("segments", "total count of segments", segments.clone());
        registry.register("messages", "total count of messages", messages.clone());
        registry.register("users", "total count of users", users.clone());
        registry.register("clients", "total count of clients", clients.clone());

        let registry = registry.into();
        Self {
            registry,
            http_requests,
            streams,
            topics,
            partitions,
            segments,
            messages,
            users,
            clients,
        }
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
