// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use core::fmt;
use iggy_common::{Client, Identifier, TopicUpdateOptions};
use tracing::{Level, event};

pub struct UpdateTopicCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    options: TopicUpdateOptions,
}

impl UpdateTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier, options: TopicUpdateOptions) -> Self {
        Self {
            stream_id,
            topic_id,
            options,
        }
    }

    /// The changes this command asks for, in a fixed order.
    fn changes(&self) -> String {
        let mut changes = Vec::new();
        if let Some(name) = &self.options.name {
            changes.push(format!("name: {name}"));
        }
        if let Some(message_expiry) = &self.options.message_expiry {
            changes.push(format!("message expiry: {message_expiry}"));
        }
        if let Some(max_topic_size) = &self.options.max_topic_size {
            changes.push(format!("max topic size: {max_topic_size}"));
        }
        for (key, value) in &self.options.raw {
            changes.push(format!("{key}: {value}"));
        }
        for key in &self.options.reset {
            changes.push(format!("reset: {key}"));
        }
        for key in &self.options.raw_reset {
            changes.push(format!("reset: {key}"));
        }
        changes.join(", ")
    }
}

#[async_trait]
impl CliCommand for UpdateTopicCmd {
    fn explain(&self) -> String {
        format!("{self}")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_topic(&self.stream_id, &self.topic_id, &self.options)
            .await
            .with_context(|| {
                format!(
                    "Problem updating topic with ID: {} ({}) in stream with ID: {}",
                    self.topic_id,
                    self.changes(),
                    self.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} updated ({}) in stream with ID: {}",
            self.topic_id,
            self.changes(),
            self.stream_id,
        );

        Ok(())
    }
}

impl fmt::Display for UpdateTopicCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "update topic with ID: {} ({}) in stream with ID: {}",
            self.topic_id,
            self.changes(),
            self.stream_id
        )
    }
}
