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

//! Sink plugin with configurable failure behaviour, for integration tests that
//! need to observe what the runtime does when a sink rejects a batch.
//!
//! Not shipped: this crate exists only to back tests under
//! `core/integration/tests/connectors/`.

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{error, info};

sink_connector!(TestSink);

#[derive(Debug)]
struct State {
    batches_consumed: usize,
}

#[derive(Debug)]
pub struct TestSink {
    id: u32,
    fail_after_batches: Option<usize>,
    reject_topics: Vec<String>,
    state: Mutex<State>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestSinkConfig {
    /// Accept this many batches, then reject every batch after. `None` accepts
    /// everything; `Some(0)` rejects the first batch.
    fail_after_batches: Option<usize>,
    /// Reject only batches from these topics. Lets a multi-topic sink fail on
    /// one topic while still accepting the others, so a test can tell a stopped
    /// task apart from one that ran and was rejected.
    reject_topics: Option<Vec<String>>,
}

impl TestSink {
    pub fn new(id: u32, config: TestSinkConfig) -> Self {
        TestSink {
            id,
            fail_after_batches: config.fail_after_batches,
            reject_topics: config.reject_topics.unwrap_or_default(),
            state: Mutex::new(State {
                batches_consumed: 0,
            }),
        }
    }
}

#[async_trait]
impl Sink for TestSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened test sink connector with ID: {}, fail after batches: {:?}, reject topics: {:?}",
            self.id, self.fail_after_batches, self.reject_topics
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let batch_index = state.batches_consumed;
        let topic_selected =
            self.reject_topics.is_empty() || self.reject_topics.contains(&topic_metadata.topic);
        let should_fail = topic_selected
            && self
                .fail_after_batches
                .is_some_and(|threshold| batch_index >= threshold);
        if !should_fail {
            state.batches_consumed += 1;
        }
        drop(state);

        if should_fail {
            error!(
                "Test sink with ID: {} rejecting batch: {batch_index} of {} messages, stream: {}, topic: {}, partition: {}",
                self.id,
                messages.len(),
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
            );
            return Err(Error::CannotStoreData(format!(
                "test sink configured to reject batches from index {batch_index}"
            )));
        }

        info!(
            "Test sink with ID: {} accepted batch: {batch_index} of {} messages, stream: {}, topic: {}, partition: {}, last offset: {}",
            self.id,
            messages.len(),
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages.last().map(|message| message.offset).unwrap_or(0),
        );
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Test sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
