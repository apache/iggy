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

// Warning capture has its own test binary so concurrent parser tests cannot
// race tracing's process-wide callsite registration.

use iggy_common::{ConnectionStringOptions, IggyError, TcpConnectionStringOptions};
use std::sync::{Arc, Mutex};

fn parse(options: &str) -> Result<TcpConnectionStringOptions, IggyError> {
    TcpConnectionStringOptions::parse_options(options)
}

struct WarningMessages(Arc<Mutex<Vec<String>>>);

#[derive(Default)]
struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl tracing::Subscriber for WarningMessages {
    fn register_callsite(
        &self,
        _metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN
            && event.metadata().target()
                == "iggy_common::types::configuration::tcp_config::tcp_connection_string_options"
        {
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.0);
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[test]
fn should_warn_for_each_deprecated_retry_option_even_if_overridden() {
    let warnings = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::with_default(WarningMessages(Arc::clone(&warnings)), || {
        parse("reconnection_max_retries=5").unwrap();
        assert!(warnings.lock().unwrap().is_empty());

        parse("reconnection_retries=3").unwrap();
        parse("reconnection_retries=3&reconnection_max_retries=7").unwrap();
        parse("reconnection_max_retries=7&reconnection_retries=3").unwrap();
        parse("reconnection_retries=1&reconnection_retries=2").unwrap();
        assert!(matches!(
            parse("reconnection_retries=invalid"),
            Err(IggyError::InvalidConnectionString)
        ));
    });
    let warnings = warnings.lock().unwrap();
    assert_eq!(warnings.len(), 6);
    for warning in warnings.iter() {
        assert!(
            warning.contains("TCP connection string option `reconnection_retries` is deprecated"),
            "{warning}"
        );
        assert!(
            warning.contains("use `reconnection_max_retries` instead"),
            "{warning}"
        );
    }
}
