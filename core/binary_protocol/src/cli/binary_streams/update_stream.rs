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
use iggy_common::update_stream::UpdateStream;
use tracing::{Level, event};

pub struct UpdateStreamCmd {
    update_stream: UpdateStream,
}

impl UpdateStreamCmd {
    pub fn new(stream_id: Identifier, name: String) -> Self {
        UpdateStreamCmd {
            update_stream: UpdateStream { stream_id, name },
        }
    }
}

#[async_trait]
impl CliCommand for UpdateStreamCmd {
    fn explain(&self) -> String {
        format!(
            "update stream with ID: {} and name: {}",
            self.update_stream.stream_id, self.update_stream.name
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_stream(&self.update_stream.stream_id, &self.update_stream.name)
            .await
            .with_context(|| {
                format!(
                    "Problem updating stream with ID: {} and name: {}",
                    self.update_stream.stream_id, self.update_stream.name
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with ID: {} updated name: {}",
            self.update_stream.stream_id, self.update_stream.name
        );

        Ok(())
    }
}
