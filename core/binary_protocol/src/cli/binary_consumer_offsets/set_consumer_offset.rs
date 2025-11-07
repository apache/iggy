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
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::{Consumer, ConsumerKind};
use tracing::{Level, event};

pub struct SetConsumerOffsetCmd {
    set_consumer_offset: StoreConsumerOffset,
}

impl SetConsumerOffsetCmd {
    pub fn new(
        consumer_id: Identifier,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
        offset: u64,
        kind: ConsumerKind,
    ) -> Self {
        Self {
            set_consumer_offset: StoreConsumerOffset {
                consumer: Consumer {
                    kind,
                    id: consumer_id,
                },
                stream_id,
                topic_id,
                partition_id: Some(partition_id),
                offset,
            },
        }
    }
}

#[async_trait]
impl CliCommand for SetConsumerOffsetCmd {
    fn explain(&self) -> String {
        format!(
            "set consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {} to {}",
            self.set_consumer_offset.consumer.id,
            self.set_consumer_offset.stream_id,
            self.set_consumer_offset.topic_id,
            self.set_consumer_offset.partition_id.unwrap(),
            self.set_consumer_offset.offset,
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .store_consumer_offset(&self.set_consumer_offset.consumer, &self.set_consumer_offset.stream_id, &self.set_consumer_offset.topic_id, self.set_consumer_offset.partition_id, self.set_consumer_offset.offset)
            .await
            .with_context(|| {
                format!(
                    "Problem setting consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
                    self.set_consumer_offset.consumer.id, self.set_consumer_offset.stream_id, self.set_consumer_offset.topic_id, self.set_consumer_offset.partition_id.unwrap()
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {} set to {}",
            self.set_consumer_offset.consumer.id,
            self.set_consumer_offset.stream_id,
            self.set_consumer_offset.topic_id,
            self.set_consumer_offset.partition_id.unwrap(),
            self.set_consumer_offset.offset,
        );

        Ok(())
    }
}
