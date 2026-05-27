/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use std::time::Duration;
use tracing::info;

/// Tracing target for per-batch benchmark events. Filter via
/// `RUST_LOG=iggy_connectors::benchmark=info`.
pub const TARGET: &str = "iggy_connectors::benchmark";

#[inline]
pub fn as_micros(elapsed: Duration) -> u64 {
    elapsed.as_micros() as u64
}

#[allow(clippy::too_many_arguments)]
pub fn emit_sink_event(
    connector_key: &str,
    stream: &str,
    topic: &str,
    partition_id: u32,
    current_offset: u64,
    batch_size: usize,
    processed_count: usize,
    decode_us: u64,
    prepare_us: u64,
    ffi_us: u64,
    total_us: u64,
) {
    info!(
        target: TARGET,
        connector_type = "sink",
        connector_key = connector_key,
        stream = stream,
        topic = topic,
        partition_id = partition_id,
        current_offset = current_offset,
        batch_size = batch_size,
        processed_count = processed_count,
        decode_us = decode_us,
        prepare_us = prepare_us,
        ffi_us = ffi_us,
        total_us = total_us,
        "benchmark"
    );
}

#[allow(clippy::too_many_arguments)]
pub fn emit_source_event(
    connector_key: &str,
    stream: &str,
    topic: &str,
    batch_size: usize,
    sent_count: usize,
    decode_us: u64,
    prepare_us: u64,
    iggy_send_us: u64,
    state_save_us: Option<u64>,
    total_us: u64,
) {
    // Flat bool + u64, not an Option: keeps the field JSON-numeric (a Debug Option serializes as a quoted string).
    info!(
        target: TARGET,
        connector_type = "source",
        connector_key = connector_key,
        stream = stream,
        topic = topic,
        batch_size = batch_size,
        sent_count = sent_count,
        decode_us = decode_us,
        prepare_us = prepare_us,
        iggy_send_us = iggy_send_us,
        state_saved = state_save_us.is_some(),
        state_save_us = state_save_us.unwrap_or(0),
        total_us = total_us,
        "benchmark"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::registry::Registry;

    #[derive(Debug, Default, Clone)]
    struct CapturedEvent {
        target: String,
        message: String,
        fields: std::collections::BTreeMap<String, String>,
    }

    struct CaptureLayer {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    struct FieldVisitor<'a> {
        out: &'a mut std::collections::BTreeMap<String, String>,
        message: &'a mut String,
    }

    impl Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.message = format!("{value:?}");
            } else {
                self.out
                    .insert(field.name().to_string(), format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "message" {
                *self.message = value.to_string();
            } else {
                self.out.insert(field.name().to_string(), value.to_string());
            }
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.out.insert(field.name().to_string(), value.to_string());
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.out.insert(field.name().to_string(), value.to_string());
        }
    }

    impl<S: Subscriber> Layer<S> for CaptureLayer {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut captured = CapturedEvent {
                target: event.metadata().target().to_string(),
                ..Default::default()
            };
            let mut visitor = FieldVisitor {
                out: &mut captured.fields,
                message: &mut captured.message,
            };
            event.record(&mut visitor);
            self.events.lock().unwrap().push(captured);
        }
    }

    fn capture<F: FnOnce()>(emit: F) -> Vec<CapturedEvent> {
        let events: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let layer = CaptureLayer {
            events: events.clone(),
        };
        let subscriber = Registry::default().with(layer);
        tracing::subscriber::with_default(subscriber, emit);
        let guard = events.lock().unwrap();
        guard.clone()
    }

    #[test]
    fn given_zero_duration_when_formatted_should_be_zero_micros() {
        assert_eq!(as_micros(Duration::from_secs(0)), 0);
    }

    #[test]
    fn given_some_duration_when_formatted_should_match_micros() {
        assert_eq!(as_micros(Duration::from_micros(1234)), 1234);
    }

    #[test]
    fn given_sink_event_emitted_when_captured_should_contain_all_fields() {
        let events = capture(|| {
            emit_sink_event(
                "postgres", "qw", "records", 0, 100, 50, 50, 120, 234, 1456, 1700,
            );
        });
        let event = events
            .iter()
            .find(|event| event.target == TARGET && event.message == "benchmark")
            .expect("benchmark event should be captured");
        assert_eq!(event.fields.get("connector_type").unwrap(), "sink");
        assert_eq!(event.fields.get("connector_key").unwrap(), "postgres");
        assert_eq!(event.fields.get("stream").unwrap(), "qw");
        assert_eq!(event.fields.get("topic").unwrap(), "records");
        assert_eq!(event.fields.get("batch_size").unwrap(), "50");
        assert_eq!(event.fields.get("processed_count").unwrap(), "50");
        assert_eq!(event.fields.get("decode_us").unwrap(), "120");
        assert_eq!(event.fields.get("prepare_us").unwrap(), "234");
        assert_eq!(event.fields.get("ffi_us").unwrap(), "1456");
        assert_eq!(event.fields.get("total_us").unwrap(), "1700");
    }

    #[test]
    fn given_source_event_with_state_save_when_captured_should_contain_some_value() {
        let events = capture(|| {
            emit_source_event(
                "random",
                "example_stream",
                "example_topic",
                25,
                23,
                12,
                34,
                567,
                Some(89),
                750,
            );
        });
        let event = events
            .iter()
            .find(|event| event.target == TARGET && event.message == "benchmark")
            .expect("benchmark event should be captured");
        assert_eq!(event.fields.get("connector_type").unwrap(), "source");
        assert_eq!(event.fields.get("connector_key").unwrap(), "random");
        assert_eq!(event.fields.get("batch_size").unwrap(), "25");
        assert_eq!(event.fields.get("sent_count").unwrap(), "23");
        assert_eq!(event.fields.get("decode_us").unwrap(), "12");
        assert_eq!(event.fields.get("prepare_us").unwrap(), "34");
        assert_eq!(event.fields.get("iggy_send_us").unwrap(), "567");
        assert_eq!(event.fields.get("state_saved").unwrap(), "true");
        assert_eq!(event.fields.get("state_save_us").unwrap(), "89");
        assert_eq!(event.fields.get("total_us").unwrap(), "750");
    }

    #[test]
    fn given_source_event_without_state_save_when_captured_should_be_zero_and_unsaved() {
        let events = capture(|| {
            emit_source_event(
                "random",
                "example_stream",
                "example_topic",
                25,
                23,
                12,
                34,
                567,
                None,
                750,
            );
        });
        let event = events
            .iter()
            .find(|event| event.target == TARGET && event.message == "benchmark")
            .expect("benchmark event should be captured");
        assert_eq!(event.fields.get("state_saved").unwrap(), "false");
        assert_eq!(event.fields.get("state_save_us").unwrap(), "0");
    }
}
