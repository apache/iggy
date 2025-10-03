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
use anyhow::{Context, Error};
use async_trait::async_trait;
use iggy_common::Identifier;
use tracing::{Level, event};

pub struct FlushMessagesCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: usize,
    fsync: bool,
}

impl FlushMessagesCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        fsync: bool,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
            fsync,
        }
    }
}

#[async_trait]
impl CliCommand for FlushMessagesCmd {
    fn explain(&self) -> String {
        format!(
            "flush messages from topic with ID: {} and stream with ID: {} (partition with ID: {}) {}",
            self.topic_id,
            self.stream_id,
            self.partition_id,
            if self.fsync {
                "with fsync"
            } else {
                "without fsync"
            },
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), Error> {
        client
            .flush_unsaved_buffer(
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
                self.fsync,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem flushing messages from topic with ID: {} and stream with ID: {} (partition with ID: {}) {}",
                    self.topic_id, self.stream_id, self.partition_id, if self.fsync { "with fsync" } else { "without fsync" },
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Flushed messages from topic with ID: {} and stream with ID: {} (partition with ID: {}) {}",
            self.topic_id,
            self.stream_id,
            self.partition_id,
            if self.fsync { "with fsync" } else { "without fsync" },
        );

        Ok(())
    }
}
