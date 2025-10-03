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
use iggy_common::delete_segments::DeleteSegments;
use tracing::{Level, event};

pub struct DeleteSegmentsCmd {
    delete_segments: DeleteSegments,
}

impl DeleteSegmentsCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        segments_count: u32,
    ) -> Self {
        Self {
            delete_segments: DeleteSegments {
                stream_id,
                topic_id,
                partition_id,
                segments_count,
            },
        }
    }
}

#[async_trait]
impl CliCommand for DeleteSegmentsCmd {
    fn explain(&self) -> String {
        let mut segments = String::from("segment");
        if self.delete_segments.segments_count > 1 {
            segments.push('s');
        };

        format!(
            "delete {} {segments} for topic with ID: {}, stream with ID: {} and partition with ID: {}",
            self.delete_segments.segments_count,
            self.delete_segments.topic_id,
            self.delete_segments.stream_id,
            self.delete_segments.partition_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut segments = String::from("segment");
        if self.delete_segments.segments_count > 1 {
            segments.push('s');
        };

        client
            .delete_segments(
                &self.delete_segments.stream_id,
                &self.delete_segments.topic_id,
                self.delete_segments.partition_id,
                self.delete_segments.segments_count,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem deleting {} {segments} for topic with ID: {}, stream with ID: {} and partition with ID: {}",
                    self.delete_segments.segments_count,
                    self.delete_segments.topic_id,
                    self.delete_segments.stream_id,
                    self.delete_segments.partition_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Deleted {} {segments} for topic with ID: {}, stream with ID: {} and partition with ID: {}",
            self.delete_segments.segments_count,
            self.delete_segments.topic_id,
            self.delete_segments.stream_id,
            self.delete_segments.partition_id
        );

        Ok(())
    }
}
