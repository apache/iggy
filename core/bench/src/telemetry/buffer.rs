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

use opentelemetry::KeyValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::warn;

/// Event types for metrics
#[derive(Clone, Debug)]
pub enum EventType {
    BatchSent {
        messages: u64,
        bytes: u64,
        latency_us: f64,
    },
    BatchReceived {
        messages: u64,
        bytes: u64,
        latency_us: f64,
    },
    Throughput {
        messages_per_sec: f64,
        mb_per_sec: f64,
    },
}

/// Single metric event to be processed
#[derive(Clone, Debug)]
pub struct MetricEvent {
    pub timestamp: Instant,
    pub event_type: EventType,
    pub labels: Vec<KeyValue>,
}

/// Lock-free metrics buffer using channels
/// This avoids blocking the benchmark threads
pub struct MetricsBuffer {
    sender: mpsc::UnboundedSender<MetricEvent>,
    dropped_count: Arc<AtomicU64>,
    buffer_size: Arc<AtomicUsize>,
}

impl MetricsBuffer {
    pub fn new(batch_size: usize, flush_interval: Duration) -> (Self, MetricsProcessor) {
        let (sender, receiver) = mpsc::unbounded_channel();

        let buffer = Self {
            sender,
            dropped_count: Arc::new(AtomicU64::new(0)),
            buffer_size: Arc::new(AtomicUsize::new(0)),
        };

        let processor = MetricsProcessor {
            receiver,
            batch_size,
            flush_interval,
            batch: Vec::with_capacity(batch_size),
            last_flush: Instant::now(),
            buffer_size: buffer.buffer_size.clone(),
        };

        (buffer, processor)
    }

    /// Push a metric event to the buffer (non-blocking)
    pub fn push(&self, event: MetricEvent) {
        // Try to send, but don't block if buffer is full
        match self.sender.send(event) {
            Ok(()) => {
                self.buffer_size.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // Channel is full, increment dropped counter
                self.dropped_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size.load(Ordering::Relaxed)
    }
}

/// Processes buffered metrics asynchronously
pub struct MetricsProcessor {
    receiver: mpsc::UnboundedReceiver<MetricEvent>,
    batch_size: usize,
    flush_interval: Duration,
    batch: Vec<MetricEvent>,
    last_flush: Instant,
    buffer_size: Arc<AtomicUsize>,
}

impl MetricsProcessor {
    /// Process metrics in a background task
    pub async fn run(mut self, metrics: Arc<super::BenchMetrics>) {
        loop {
            // Check if we should flush based on time
            let should_flush_time = self.last_flush.elapsed() >= self.flush_interval;

            // Try to receive events with a timeout
            let timeout = if should_flush_time {
                Duration::from_millis(1)
            } else {
                self.flush_interval - self.last_flush.elapsed()
            };

            match tokio::time::timeout(timeout, self.receiver.recv()).await {
                Ok(Some(event)) => {
                    self.batch.push(event);
                    // Decrement buffer size since we've received the event
                    self.buffer_size.fetch_sub(1, Ordering::Relaxed);

                    // Flush if batch is full
                    if self.batch.len() >= self.batch_size {
                        self.flush_batch(&metrics);
                    }
                }
                Ok(None) => {
                    // Channel closed, flush remaining and exit
                    if !self.batch.is_empty() {
                        self.flush_batch(&metrics);
                    }
                    break;
                }
                Err(_) => {
                    // Timeout - flush if we have data
                    if !self.batch.is_empty() && should_flush_time {
                        self.flush_batch(&metrics);
                    }
                }
            }
        }
    }

    fn flush_batch(&mut self, metrics: &super::BenchMetrics) {
        // Process all events in the batch
        for event in self.batch.drain(..) {
            match event.event_type {
                EventType::BatchSent {
                    messages,
                    bytes,
                    latency_us,
                } => {
                    metrics.messages_sent.add(messages, &event.labels);
                    metrics.bytes_sent.add(bytes, &event.labels);
                    metrics.batches_sent.add(1, &event.labels);
                    metrics.send_latency.record(latency_us, &event.labels);
                    metrics.batch_latency.record(latency_us, &event.labels);
                }
                EventType::BatchReceived {
                    messages,
                    bytes,
                    latency_us,
                } => {
                    metrics.messages_received.add(messages, &event.labels);
                    metrics.bytes_received.add(bytes, &event.labels);
                    metrics.batches_received.add(1, &event.labels);
                    metrics.receive_latency.record(latency_us, &event.labels);
                }
                EventType::Throughput {
                    messages_per_sec,
                    mb_per_sec,
                } => {
                    metrics
                        .throughput_messages_per_sec
                        .record(messages_per_sec, &event.labels);
                    metrics
                        .throughput_mb_per_sec
                        .record(mb_per_sec, &event.labels);
                }
            }
        }

        self.last_flush = Instant::now();
    }
}

/// Periodic reporter for buffer stats
pub struct BufferStatsReporter {
    buffer: Arc<MetricsBuffer>,
    interval: Duration,
}

impl BufferStatsReporter {
    pub const fn new(buffer: Arc<MetricsBuffer>, interval: Duration) -> Self {
        Self { buffer, interval }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.interval);
        loop {
            interval.tick().await;

            let dropped = self.buffer.dropped_events();
            if dropped > 0 {
                warn!(
                    "Metrics buffer dropped {} events (buffer size: {})",
                    dropped,
                    self.buffer.buffer_size()
                );
            }
        }
    }
}
