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
use iggy_common::create_consumer_group::CreateConsumerGroup;
use tracing::{Level, event};

pub struct CreateConsumerGroupCmd {
    create_consumer_group: CreateConsumerGroup,
}

impl CreateConsumerGroupCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, name: String) -> Self {
        Self {
            create_consumer_group: CreateConsumerGroup {
                stream_id,
                topic_id,
                name,
            },
        }
    }

    fn get_group_id_info(&self) -> String {
        "ID auto incremented".to_string()
    }
}

#[async_trait]
impl CliCommand for CreateConsumerGroupCmd {
    fn explain(&self) -> String {
        format!(
            "create consumer group: {}, name: {} for topic with ID: {} and stream with ID: {}",
            self.get_group_id_info(),
            self.create_consumer_group.name,
            self.create_consumer_group.topic_id,
            self.create_consumer_group.stream_id,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_consumer_group(&self.create_consumer_group.stream_id, &self.create_consumer_group.topic_id, &self.create_consumer_group.name)
            .await
            .with_context(|| {
                format!(
                    "Problem creating consumer group ({}, name: {}) for topic with ID: {} and stream with ID: {}",
                    self.get_group_id_info(), self.create_consumer_group.name, self.create_consumer_group.topic_id, self.create_consumer_group.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Consumer group: {}, name: {} created for topic with ID: {} and stream with ID: {}",
            self.get_group_id_info(),
            self.create_consumer_group.name,
            self.create_consumer_group.topic_id,
            self.create_consumer_group.stream_id,
        );

        Ok(())
    }
}
