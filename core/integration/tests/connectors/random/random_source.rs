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

use iggy_binary_protocol::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use integration::harness::seeds;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::sleep;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/random/source.toml")),
    seed = seeds::connector_stream
)]
async fn random_source_produces_messages(harness: &TestHarness) {
    sleep(Duration::from_secs(1)).await;

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "test_consumer".try_into().unwrap();

    let messages = client
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &Consumer::new(consumer_id),
            &PollingStrategy::next(),
            10,
            true,
        )
        .await
        .expect("Failed to poll messages");

    assert!(
        !messages.messages.is_empty(),
        "No messages received from random source"
    );
    assert!(
        messages.current_offset > 0,
        "Current offset should be greater than 0"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/random/source.toml")),
    seed = seeds::connector_stream
)]
async fn state_persists_across_connector_restart(harness: &mut TestHarness) {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "state_test_consumer".try_into().unwrap();

    sleep(Duration::from_secs(1)).await;

    let client = harness.root_client().await.unwrap();
    let offset_before = {
        let messages = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                100,
                true,
            )
            .await
            .expect("Failed to poll messages before restart");
        assert!(
            messages.current_offset > 0,
            "Should have messages before restart"
        );
        messages.current_offset
    };

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors");
    sleep(Duration::from_secs(1)).await;

    let offset_after = client
        .poll_messages(
            &stream_id,
            &topic_id,
            None,
            &Consumer::new(consumer_id),
            &PollingStrategy::next(),
            100,
            true,
        )
        .await
        .expect("Failed to poll messages after restart")
        .current_offset;

    assert!(
        offset_after > offset_before,
        "After restart, offset {offset_after} should be greater than before {offset_before}"
    );
}
