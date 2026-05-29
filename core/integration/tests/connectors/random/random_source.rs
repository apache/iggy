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

use crate::connectors::source_suite;
use integration::harness::seeds;
use integration::iggy_harness;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/random/source.toml")),
    seed = seeds::connector_stream
)]
async fn random_source_produces_messages(harness: &TestHarness) {
    let messages = source_suite::assert_source_produces_messages(harness).await;
    assert!(
        !messages.is_empty(),
        "No messages received from random source"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/random/source.toml")),
    seed = seeds::connector_stream
)]
async fn state_persists_across_connector_restart(harness: &mut TestHarness) {
    let before_config = source_suite::config_for_consumer("source_suite_before_restart");
    let before = source_suite::poll_until_min_messages(harness, &before_config).await;
    assert!(
        !before.is_empty(),
        "source suite expected messages before connector restart"
    );

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors");

    let after_config = source_suite::config_for_consumer("source_suite_after_restart");
    let after = source_suite::poll_until_min_messages(harness, &after_config).await;
    assert!(
        !after.is_empty(),
        "source suite expected messages after connector restart"
    );
}
