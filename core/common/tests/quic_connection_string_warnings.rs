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

use iggy_common::{ConnectionStringOptions, IggyError, QuicConnectionStringOptions};
use std::sync::{Arc, Mutex};

fn parse(options: &str) -> Result<QuicConnectionStringOptions, IggyError> {
    QuicConnectionStringOptions::parse_options(options)
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
                == "iggy_common::types::configuration::quic_config::quic_connection_string_options"
        {
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.0);
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

fn capture_warnings(f: impl FnOnce()) -> String {
    let messages = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::with_default(WarningMessages(Arc::clone(&messages)), f);
    messages.lock().unwrap().join("\n")
}

#[test]
fn deprecated_aliases_warn_on_every_occurrence_and_identify_replacements() {
    let warnings = capture_warnings(|| {
        parse(
            "reconnection_reestablish_after=100ms&reestablish_after=500ms&reconnection_reestablish_after=1s",
        )
        .unwrap();
        parse("validate_certificate=false&tls_validate_certificate=true&validate_certificate=true")
            .unwrap();
    });
    assert_eq!(
        warnings
            .matches("option `reconnection_reestablish_after` is deprecated")
            .count(),
        2
    );
    assert_eq!(
        warnings
            .matches("option `validate_certificate` is deprecated")
            .count(),
        2
    );
    assert!(warnings.contains("use `reestablish_after` instead"));
    assert!(warnings.contains("use `tls_validate_certificate` instead"));

    let warnings = capture_warnings(|| {
        parse("reestablish_after=500ms&tls_validate_certificate=true").unwrap();
    });
    assert!(
        warnings.is_empty(),
        "canonical options must not warn: {warnings}"
    );

    let warnings = capture_warnings(|| {
        assert!(matches!(
            parse("validate_certificate=invalid"),
            Err(IggyError::InvalidConnectionString)
        ));
    });
    assert!(warnings.contains("use `tls_validate_certificate` instead"));

    let warnings = capture_warnings(|| {
        assert!(matches!(
            parse("reconnection_reestablish_after=invalid"),
            Err(IggyError::InvalidConnectionString)
        ));
    });
    assert!(warnings.contains("use `reestablish_after` instead"));
}
