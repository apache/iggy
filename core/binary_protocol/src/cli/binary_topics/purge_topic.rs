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

use crate::Client;
use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use iggy_common::Identifier;
use iggy_common::purge_topic::PurgeTopic;
use tracing::{Level, event};

pub struct PurgeTopicCmd {
    purge_topic: PurgeTopic,
}

impl PurgeTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            purge_topic: PurgeTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for PurgeTopicCmd {
    fn explain(&self) -> String {
        format!(
            "purge topic with ID: {} in stream with ID: {}",
            self.purge_topic.topic_id, self.purge_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .purge_topic(&self.purge_topic.stream_id, &self.purge_topic.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem purging topic with ID: {} in stream {}",
                    self.purge_topic.topic_id, self.purge_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} in stream with ID: {} purged",
            self.purge_topic.topic_id, self.purge_topic.stream_id);

        Ok(())
    }
}
