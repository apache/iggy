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

use std::time::Duration;

use iggy_common::{Consumer, Identifier, MessageClient, PollingStrategy};
use integration::harness::{TestHarness, seeds};
use tokio::time::{sleep, timeout};

const CONSUMER_NAME: &str = "random_source_liveness_consumer";
const POLL_BATCH: u32 = 100;
const RETRY_INTERVAL: Duration = Duration::from_millis(100);
const POLL_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn assert_produces_messages(harness: &TestHarness) {
    let client = harness.root_client().await.expect("root client");
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = CONSUMER_NAME.try_into().unwrap();

    let poll = async {
        loop {
            if let Ok(polled) = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    None,
                    &Consumer::new(consumer_id.clone()),
                    &PollingStrategy::next(),
                    POLL_BATCH,
                    true,
                )
                .await
                && !polled.messages.is_empty()
            {
                return;
            }

            sleep(RETRY_INTERVAL).await;
        }
    };

    timeout(POLL_TIMEOUT, poll).await.unwrap_or_else(|_| {
        panic!(
            "random source liveness timed out after {:?} waiting for messages",
            POLL_TIMEOUT
        )
    })
}
