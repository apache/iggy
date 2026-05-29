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

use iggy_common::{Consumer, Identifier, IggyMessage, MessageClient, PollingStrategy};
use integration::harness::{TestHarness, seeds};
use tokio::time::{sleep, timeout};

pub(crate) struct SourceSuiteConfig {
    pub(crate) consumer_name: &'static str,
    pub(crate) min_messages: usize,
    pub(crate) poll_batch: u32,
    pub(crate) warmup: Duration,
    pub(crate) retry_interval: Duration,
    pub(crate) timeout: Duration,
}

impl Default for SourceSuiteConfig {
    fn default() -> Self {
        Self {
            consumer_name: "source_suite_consumer",
            min_messages: 1,
            poll_batch: 100,
            warmup: Duration::from_secs(1),
            retry_interval: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        }
    }
}

pub(crate) async fn poll_until_min_messages(
    harness: &TestHarness,
    config: &SourceSuiteConfig,
) -> Vec<IggyMessage> {
    sleep(config.warmup).await;

    let client = harness.root_client().await.expect("root client");
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = config.consumer_name.try_into().unwrap();

    let poll = async {
        loop {
            let polled = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    None,
                    &Consumer::new(consumer_id.clone()),
                    &PollingStrategy::next(),
                    config.poll_batch,
                    true,
                )
                .await
                .expect("poll source messages");

            if polled.messages.len() >= config.min_messages {
                return polled.messages;
            }

            sleep(config.retry_interval).await;
        }
    };

    timeout(config.timeout, poll)
        .await
        .expect("source suite timed out waiting for messages")
}

pub(crate) async fn assert_source_produces_messages(harness: &TestHarness) -> Vec<IggyMessage> {
    let messages = poll_until_min_messages(harness, &SourceSuiteConfig::default()).await;
    assert!(
        !messages.is_empty(),
        "source suite expected at least one message"
    );
    messages
}

pub(crate) fn config_for_consumer(consumer_name: &'static str) -> SourceSuiteConfig {
    SourceSuiteConfig {
        consumer_name,
        ..SourceSuiteConfig::default()
    }
}
