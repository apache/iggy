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
use comfy_table::Table;
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::{Consumer, ConsumerKind, Identifier};
use tracing::{Level, event};

pub struct GetConsumerOffsetCmd {
    get_consumer_offset: GetConsumerOffset,
}

impl GetConsumerOffsetCmd {
    pub fn new(
        consumer_id: Identifier,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        kind: ConsumerKind,
    ) -> Self {
        Self {
            get_consumer_offset: GetConsumerOffset {
                consumer: Consumer {
                    kind,
                    id: consumer_id,
                },
                stream_id,
                topic_id,
                partition_id: Some(partition_id),
            },
        }
    }

    pub fn get_consumer_info(&self) -> String {
        match self.get_consumer_offset.consumer.kind {
            ConsumerKind::Consumer => {
                format!("consumer with ID: {}", self.get_consumer_offset.consumer.id)
            }
            ConsumerKind::ConsumerGroup => format!(
                "consumer group with ID: {}",
                self.get_consumer_offset.consumer.id
            ),
        }
    }
}

#[async_trait]
impl CliCommand for GetConsumerOffsetCmd {
    fn explain(&self) -> String {
        format!(
            "get consumer offset for {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
            self.get_consumer_info(),
            self.get_consumer_offset.stream_id,
            self.get_consumer_offset.topic_id,
            self.get_consumer_offset.partition_id.unwrap(),
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let consumer_offset = client.get_consumer_offset(&self.get_consumer_offset.consumer, &self.get_consumer_offset.stream_id, &self.get_consumer_offset.topic_id, self.get_consumer_offset.partition_id).await.with_context(|| {
            format!(
                "Problem getting consumer offset for {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
                self.get_consumer_info(), self.get_consumer_offset.stream_id, self.get_consumer_offset.topic_id, self.get_consumer_offset.partition_id.unwrap()
            )
        })?;

        if consumer_offset.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Consumer offset for {} for stream with ID: {} and topic with ID: {} and partition with ID: {} was not found", self.get_consumer_info(), self.get_consumer_offset.stream_id, self.get_consumer_offset.topic_id, self.get_consumer_offset.partition_id.unwrap());
            return Ok(());
        }

        let consumer_offset = consumer_offset.unwrap();
        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec![
            "Consumer ID",
            format!("{}", self.get_consumer_offset.consumer.id).as_str(),
        ]);
        table.add_row(vec![
            "Stream ID",
            format!("{}", self.get_consumer_offset.stream_id).as_str(),
        ]);
        table.add_row(vec![
            "Topic ID",
            format!("{}", self.get_consumer_offset.topic_id).as_str(),
        ]);
        table.add_row(vec![
            "Partition ID",
            format!("{}", consumer_offset.partition_id).as_str(),
        ]);
        table.add_row(vec![
            "Current offset",
            format!("{}", consumer_offset.current_offset).as_str(),
        ]);
        table.add_row(vec![
            "Stored offset",
            format!("{}", consumer_offset.stored_offset).as_str(),
        ]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
