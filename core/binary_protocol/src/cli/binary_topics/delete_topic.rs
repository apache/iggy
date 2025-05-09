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

use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use crate::Client;
use anyhow::Context;
use async_trait::async_trait;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::Identifier;
use tracing::{event, Level};

pub struct DeleteTopicCmd {
    delete_topic: DeleteTopic,
}

impl DeleteTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            delete_topic: DeleteTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteTopicCmd {
    fn explain(&self) -> String {
        format!(
            "delete topic with ID: {} in stream with ID: {}",
            self.delete_topic.topic_id, self.delete_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_topic(&self.delete_topic.stream_id, &self.delete_topic.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting topic with ID: {} in stream {}",
                    self.delete_topic.topic_id, self.delete_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} in stream with ID: {} deleted",
            self.delete_topic.topic_id, self.delete_topic.stream_id
        );

        Ok(())
    }
}
