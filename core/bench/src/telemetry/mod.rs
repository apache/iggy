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

pub mod buffer;
pub mod setup;

pub use setup::{TelemetryConfig, TelemetryContext};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use std::sync::Arc;
use std::time::Instant;

/// Container for all OpenTelemetry metrics instruments
pub struct BenchMetrics {
    // Counters
    pub messages_sent: Counter<u64>,
    pub messages_received: Counter<u64>,
    pub bytes_sent: Counter<u64>,
    pub bytes_received: Counter<u64>,
    pub batches_sent: Counter<u64>,
    pub batches_received: Counter<u64>,

    // Histograms for latency distribution
    pub send_latency: Histogram<f64>,
    pub receive_latency: Histogram<f64>,
    pub batch_latency: Histogram<f64>,
    pub end_to_end_latency: Histogram<f64>,

    // Gauges for current state
    pub active_producers: Gauge<u64>,
    pub active_consumers: Gauge<u64>,
    pub throughput_messages_per_sec: Gauge<f64>,
    pub throughput_mb_per_sec: Gauge<f64>,
    pub current_rate_limit_mb_per_sec: Gauge<f64>,

    // Message size histograms
    pub message_size: Histogram<u64>,
    pub batch_size: Histogram<u64>,
}

impl BenchMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            // Counters
            messages_sent: meter
                .u64_counter("iggy.bench.messages.sent")
                .with_description("Total number of messages sent")
                .with_unit("messages")
                .build(),

            messages_received: meter
                .u64_counter("iggy.bench.messages.received")
                .with_description("Total number of messages received")
                .with_unit("messages")
                .build(),

            bytes_sent: meter
                .u64_counter("iggy.bench.bytes.sent")
                .with_description("Total bytes sent")
                .with_unit("bytes")
                .build(),

            bytes_received: meter
                .u64_counter("iggy.bench.bytes.received")
                .with_description("Total bytes received")
                .with_unit("bytes")
                .build(),

            batches_sent: meter
                .u64_counter("iggy.bench.batches.sent")
                .with_description("Total number of batches sent")
                .with_unit("batches")
                .build(),

            batches_received: meter
                .u64_counter("iggy.bench.batches.received")
                .with_description("Total number of batches received")
                .with_unit("batches")
                .build(),

            // Histograms
            send_latency: meter
                .f64_histogram("iggy.bench.latency.send")
                .with_description("Send operation latency")
                .with_unit("microseconds")
                .build(),

            receive_latency: meter
                .f64_histogram("iggy.bench.latency.receive")
                .with_description("Receive operation latency")
                .with_unit("microseconds")
                .build(),

            batch_latency: meter
                .f64_histogram("iggy.bench.latency.batch")
                .with_description("Batch processing latency")
                .with_unit("microseconds")
                .build(),

            end_to_end_latency: meter
                .f64_histogram("iggy.bench.latency.e2e")
                .with_description("End-to-end message latency")
                .with_unit("microseconds")
                .build(),

            // Gauges
            active_producers: meter
                .u64_gauge("iggy.bench.actors.producers")
                .with_description("Number of active producers")
                .with_unit("actors")
                .build(),

            active_consumers: meter
                .u64_gauge("iggy.bench.actors.consumers")
                .with_description("Number of active consumers")
                .with_unit("actors")
                .build(),

            throughput_messages_per_sec: meter
                .f64_gauge("iggy.bench.throughput.messages")
                .with_description("Current throughput in messages per second")
                .with_unit("messages/sec")
                .build(),

            throughput_mb_per_sec: meter
                .f64_gauge("iggy.bench.throughput.bytes")
                .with_description("Current throughput in MB per second")
                .with_unit("MB/sec")
                .build(),

            current_rate_limit_mb_per_sec: meter
                .f64_gauge("iggy.bench.rate_limit")
                .with_description("Current rate limit in MB per second")
                .with_unit("MB/sec")
                .build(),

            // Size histograms
            message_size: meter
                .u64_histogram("iggy.bench.size.message")
                .with_description("Distribution of message sizes")
                .with_unit("bytes")
                .build(),

            batch_size: meter
                .u64_histogram("iggy.bench.size.batch")
                .with_description("Distribution of batch sizes")
                .with_unit("messages")
                .build(),
        }
    }
}

/// Lightweight metrics handle for actors to record metrics
#[derive(Clone)]
pub struct MetricsHandle {
    // TODO: Will be used for direct metric recording in future optimizations
    #[allow(dead_code)]
    metrics: Arc<BenchMetrics>,
    buffer: Arc<buffer::MetricsBuffer>,
    actor_labels: Vec<KeyValue>,
}

impl MetricsHandle {
    pub fn new(
        metrics: Arc<BenchMetrics>,
        buffer: Arc<buffer::MetricsBuffer>,
        actor_type: &str,
        actor_id: u32,
        stream_id: u32,
    ) -> Self {
        let actor_labels = vec![
            KeyValue::new("actor_type", actor_type.to_string()),
            KeyValue::new("actor_id", i64::from(actor_id)),
            KeyValue::new("stream_id", i64::from(stream_id)),
        ];

        Self {
            metrics,
            buffer,
            actor_labels,
        }
    }

    /// Record a batch send operation
    pub fn record_batch_sent(&self, messages: u64, bytes: u64, latency_us: f64) {
        // Buffer the raw data for async processing
        self.buffer.push(buffer::MetricEvent {
            timestamp: Instant::now(),
            event_type: buffer::EventType::BatchSent {
                messages,
                bytes,
                latency_us,
            },
            labels: self.actor_labels.clone(),
        });
    }

    /// Record a batch receive operation
    pub fn record_batch_received(&self, messages: u64, bytes: u64, latency_us: f64) {
        self.buffer.push(buffer::MetricEvent {
            timestamp: Instant::now(),
            event_type: buffer::EventType::BatchReceived {
                messages,
                bytes,
                latency_us,
            },
            labels: self.actor_labels.clone(),
        });
    }

    /// Record throughput measurement
    pub fn record_throughput(&self, messages_per_sec: f64, mb_per_sec: f64) {
        self.buffer.push(buffer::MetricEvent {
            timestamp: Instant::now(),
            event_type: buffer::EventType::Throughput {
                messages_per_sec,
                mb_per_sec,
            },
            labels: self.actor_labels.clone(),
        });
    }
}
