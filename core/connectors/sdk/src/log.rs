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

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::{Event, Level, Subscriber};
use tracing_log::{AsLog, LogTracer, NormalizeEvent};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

/// Callback for plugins to send log records to the runtime.
///
/// # Arguments
/// * `level` - Log level (0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR)
/// * `target_ptr` - Pointer to the target string (module path)
/// * `target_len` - Length of the target string
/// * `message_ptr` - Pointer to the message string
/// * `message_len` - Length of the message string
pub type LogCallback = extern "C" fn(
    level: u8,
    target_ptr: *const u8,
    target_len: usize,
    message_ptr: *const u8,
    message_len: usize,
);

/// A tracing Layer that forwards events to the runtime via callback.
/// This allows plugin logs to be routed through the runtime's OTEL subscriber.
/// Installs the subscriber of a plugin, which hands every event to the runtime through
/// `callback`, and routes the `log` records of the plugin's dependencies into it. Only the first
/// call of a plugin library takes effect.
pub(crate) fn init_plugin_logging(callback: LogCallback) {
    _ = Registry::default()
        .with(CallbackLayer::new(callback))
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .try_init();
    // Each plugin library has its own `log` logger, and records sent to an unset one vanish.
    _ = LogTracer::builder()
        .with_max_level(LevelFilter::current().as_log())
        .init();
}

pub struct CallbackLayer {
    callback: LogCallback,
}

impl CallbackLayer {
    pub fn new(callback: LogCallback) -> Self {
        Self { callback }
    }
}

impl<S: Subscriber> Layer<S> for CallbackLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // A record bridged from `log` keeps its own target and level here.
        let normalized = event.normalized_metadata();
        let metadata = normalized.as_ref().unwrap_or_else(|| event.metadata());
        let level = match *metadata.level() {
            Level::TRACE => 0u8,
            Level::DEBUG => 1,
            Level::INFO => 2,
            Level::WARN => 3,
            Level::ERROR => 4,
        };

        let target = metadata.target();

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let message = visitor.message;

        (self.callback)(
            level,
            target.as_ptr(),
            target.len(),
            message.as_ptr(),
            message.len(),
        );
    }
}

/// Visitor to extract the message field from a tracing Event
#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" || self.message.is_empty() {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" || self.message.is_empty() {
            self.message = format!("{:?}", value);
        }
    }
}
