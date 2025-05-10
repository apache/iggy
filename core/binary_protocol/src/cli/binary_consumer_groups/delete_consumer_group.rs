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
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use tracing::{Level, event};

pub struct DeleteConsumerGroupCmd {
    delete_consumer_group: DeleteConsumerGroup,
}

impl DeleteConsumerGroupCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, group_id: Identifier) -> Self {
        Self {
            delete_consumer_group: DeleteConsumerGroup {
                stream_id,
                topic_id,
                group_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "delete consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
            self.delete_consumer_group.group_id,
            self.delete_consumer_group.topic_id,
            self.delete_consumer_group.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .delete_consumer_group(&self.delete_consumer_group.stream_id, &self.delete_consumer_group.topic_id, &self.delete_consumer_group.group_id)
            .await
            .with_context(|| {
                format!(
                    "Problem deleting consumer group with ID: {} for topic with ID: {} and stream with ID: {}",
                    self.delete_consumer_group.group_id, self.delete_consumer_group.topic_id, self.delete_consumer_group.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Consumer group with ID: {} deleted for topic with ID: {} and stream with ID: {}",
            self.delete_consumer_group.group_id,
            self.delete_consumer_group.topic_id,
            self.delete_consumer_group.stream_id,
        );

        Ok(())
    }
}
