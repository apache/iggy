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

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

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
        let level = match *event.metadata().level() {
            Level::TRACE => 0u8,
            Level::DEBUG => 1,
            Level::INFO => 2,
            Level::WARN => 3,
            Level::ERROR => 4,
        };

        let target = event.metadata().target();

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
