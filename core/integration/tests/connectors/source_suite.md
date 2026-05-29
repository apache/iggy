<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Source Connector Pareto Suite

## Purpose

The source suite provides reusable source-side integration test helpers for
connector behavior that repeatedly appears in review. It is a Pareto baseline,
not a replacement for backend-specific source tests.

## Covered In The First Suite

- Source eventually produces messages into Iggy.
- Source continues producing after connector runtime restart.
- Polling uses bounded retries and a timeout.

## Not Covered By The Generic Helper

- Backend mark/delete side effects.
- Exact duplicate or replay assertions.
- Forced Iggy send failures.
- Source-specific cursor validation.

## How To Use

Import the helper from a connector integration test module and keep the
connector-specific harness setup in that test file.

```rust
use crate::connectors::source_suite;
use integration::harness::seeds;
use integration::iggy_harness;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/<name>/source.toml")),
    seed = seeds::connector_stream
)]
async fn source_produces_messages(harness: &TestHarness) {
    let messages = source_suite::assert_source_produces_messages(harness).await;
    assert!(!messages.is_empty());
}
```

For restart coverage, use distinct consumers before and after the runtime
restart so the helper validates fresh production on both sides of the restart.

```rust
use crate::connectors::source_suite;
use integration::harness::seeds;
use integration::iggy_harness;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/<name>/source.toml")),
    seed = seeds::connector_stream
)]
async fn source_produces_after_restart(harness: &mut TestHarness) {
    let before_config = source_suite::config_for_consumer("source_before_restart");
    let before = source_suite::poll_until_min_messages(harness, &before_config).await;
    assert!(!before.is_empty());

    harness.server_mut().stop_dependents().expect("stop connectors");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("restart connectors");

    let after_config = source_suite::config_for_consumer("source_after_restart");
    let after = source_suite::poll_until_min_messages(harness, &after_config).await;
    assert!(!after.is_empty());
}
```

## When To Add Connector-Specific Tests

Add connector-specific tests when the source has an external cursor,
delete-after-read behavior, processed or mark columns, CDC offsets, or
backend-specific replay guarantees.

## Future Follow-Ups

- Deterministic send-failure tests after the runtime has an approved injection
  point.
- Source adapters for Postgres, Elasticsearch, and InfluxDB once Docker-backed
  tests are available locally.
- Explicit duplicate or replay-window assertions for connectors with stable
  payload identity.
