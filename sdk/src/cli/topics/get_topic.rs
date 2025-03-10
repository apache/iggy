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

use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::topics::get_topic::GetTopic;
use crate::utils::expiry::IggyExpiry;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub struct GetTopicCmd {
    get_topic: GetTopic,
}

impl GetTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            get_topic: GetTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for GetTopicCmd {
    fn explain(&self) -> String {
        format!(
            "get topic with ID: {} from stream with ID: {}",
            self.get_topic.topic_id, self.get_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let topic = client
            .get_topic(&self.get_topic.stream_id, &self.get_topic.topic_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting topic with ID: {} in stream {}",
                    self.get_topic.topic_id, self.get_topic.stream_id
                )
            })?;

        if topic.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Topic with ID: {} in stream {} was not found", self.get_topic.topic_id, self.get_topic.stream_id);
            return Ok(());
        }

        let topic = topic.unwrap();
        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Topic id", format!("{}", topic.id).as_str()]);
        table.add_row(vec![
            "Created",
            topic.created_at.to_utc_string("%Y-%m-%d %H:%M:%S").as_str(),
        ]);
        table.add_row(vec!["Topic name", topic.name.as_str()]);
        table.add_row(vec!["Topic size", format!("{}", topic.size).as_str()]);
        table.add_row(vec![
            "Compression",
            topic.compression_algorithm.to_string().as_str(),
        ]);
        table.add_row(vec![
            "Message expiry",
            match topic.message_expiry {
                IggyExpiry::NeverExpire => String::from("unlimited"),
                IggyExpiry::ServerDefault => String::from("server_default"),
                IggyExpiry::ExpireDuration(value) => format!("{}", value),
            }
            .as_str(),
        ]);
        table.add_row(vec![
            "Max topic size",
            format!("{}", topic.max_topic_size).as_str(),
        ]);
        table.add_row(vec![
            "Topic message count",
            format!("{}", topic.messages_count).as_str(),
        ]);
        table.add_row(vec![
            "Partitions count",
            format!("{}", topic.partitions_count).as_str(),
        ]);

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}
