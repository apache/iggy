/* Licensed to the Apache Software Foundation (ASF) under one
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

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

sink_connector!(StdoutSink);

#[derive(Debug)]
struct State {
    invocations_count: usize,
}

#[derive(Debug)]
pub struct StdoutSink {
    id: u32,
    print_payload: bool,
    state: Mutex<State>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StdoutSinkConfig {
    print_payload: Option<bool>,
}

impl StdoutSink {
    pub fn new(id: u32, config: StdoutSinkConfig) -> Self {
        StdoutSink {
            id,
            print_payload: config.print_payload.unwrap_or(false),
            state: Mutex::new(State {
                invocations_count: 0,
            }),
        }
    }
}

#[async_trait]
impl Sink for StdoutSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened stdout sink connector with ID: {}, print payload: {}",
            self.id, self.print_payload
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
        state.invocations_count += 1;
        let invocation = state.invocations_count;
        drop(state);

        info!(
            "Stdout sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );
        if self.print_payload {
            for message in messages {
                info!(
                    "Message offset: {}, payload: {:#?}",
                    message.offset, message.payload
                );
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Stdout sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
