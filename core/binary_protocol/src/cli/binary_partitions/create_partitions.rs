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
use iggy_common::create_partitions::CreatePartitions;
use tracing::{Level, event};

pub struct CreatePartitionsCmd {
    create_partition: CreatePartitions,
}

impl CreatePartitionsCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, partitions_count: usize) -> Self {
        Self {
            create_partition: CreatePartitions {
                stream_id,
                topic_id,
                partitions_count,
            },
        }
    }
}

#[async_trait]
impl CliCommand for CreatePartitionsCmd {
    fn explain(&self) -> String {
        let mut partitions = String::from("partition");
        if self.create_partition.partitions_count > 1 {
            partitions.push('s');
        };

        format!(
            "create {} {partitions} for topic with ID: {} and stream with ID: {}",
            self.create_partition.partitions_count,
            self.create_partition.topic_id,
            self.create_partition.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut partitions = String::from("partition");
        if self.create_partition.partitions_count > 1 {
            partitions.push('s');
        };

        client
            .create_partitions(
                &self.create_partition.stream_id,
                &self.create_partition.topic_id,
                self.create_partition.partitions_count,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem creating {} {partitions} for topic with ID: {} and stream with ID: {}",
                    self.create_partition.partitions_count,
                    self.create_partition.topic_id,
                    self.create_partition.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Created {} {partitions} for topic with ID: {} and stream with ID: {}",
            self.create_partition.partitions_count,
            self.create_partition.topic_id,
            self.create_partition.stream_id,
        );

        Ok(())
    }
}
