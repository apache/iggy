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

use iggy_common::{ConnectionStringOptions, WebSocketConnectionStringOptions};
use std::fmt;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

impl Subscriber for Warnings {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() == Level::WARN
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        if self.enabled(event.metadata()) {
            event.record(&mut self.clone());
        }
    }
}

impl Visit for Warnings {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.0.lock().unwrap().push(format!("{value:?}"));
        }
    }
}

#[test]
fn given_deprecated_retry_options_when_parsing_should_warn_even_if_overridden() {
    let warnings = Warnings::default();
    tracing::subscriber::with_default(warnings.clone(), || {
        WebSocketConnectionStringOptions::parse_options("reconnection_max_retries=5").unwrap();
        assert!(warnings.0.lock().unwrap().is_empty());

        WebSocketConnectionStringOptions::parse_options(
            "reconnection_retries=1&reconnection_retries=2&reconnection_max_retries=3",
        )
        .unwrap();
        assert!(
            WebSocketConnectionStringOptions::parse_options("reconnection_retries=invalid")
                .is_err()
        );
    });
    let messages = warnings.0.lock().unwrap();
    assert_eq!(messages.len(), 3);
    for message in messages.iter() {
        assert!(
            message.contains("`reconnection_retries` is deprecated"),
            "{message}"
        );
        assert!(
            message.contains("use `reconnection_max_retries` instead"),
            "{message}"
        );
    }
}
