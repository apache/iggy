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
use iggy_common::{Client, Identifier, StreamUpdateOptions};
use tracing::{Level, event};

pub struct UpdateStreamCmd {
    stream_id: Identifier,
    options: StreamUpdateOptions,
}

impl UpdateStreamCmd {
    pub fn new(stream_id: Identifier, options: StreamUpdateOptions) -> Self {
        Self { stream_id, options }
    }

    /// The changes this command asks for, in a fixed order.
    fn changes(&self) -> String {
        let mut changes = Vec::new();
        if let Some(name) = &self.options.name {
            changes.push(format!("name: {name}"));
        }
        // A stream option value may be key material, so only the key is shown.
        for key in self.options.raw.keys() {
            changes.push(format!("set: {key}"));
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
impl CliCommand for UpdateStreamCmd {
    fn explain(&self) -> String {
        format!("{self}")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_stream(&self.stream_id, &self.options)
            .await
            .with_context(|| {
                format!(
                    "Problem updating stream with ID: {} ({})",
                    self.stream_id,
                    self.changes()
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with ID: {} updated ({})",
            self.stream_id,
            self.changes(),
        );

        Ok(())
    }
}

impl fmt::Display for UpdateStreamCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "update stream with ID: {} ({})",
            self.stream_id,
            self.changes()
        )
    }
}
