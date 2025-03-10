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
use crate::streams::purge_stream::PurgeStream;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct PurgeStreamCmd {
    purge_stream: PurgeStream,
}

impl PurgeStreamCmd {
    pub fn new(stream_id: Identifier) -> Self {
        Self {
            purge_stream: PurgeStream { stream_id },
        }
    }
}

#[async_trait]
impl CliCommand for PurgeStreamCmd {
    fn explain(&self) -> String {
        format!("purge stream with ID: {}", self.purge_stream.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .purge_stream(&self.purge_stream.stream_id)
            .await
            .with_context(|| {
                format!(
                    "Problem purging stream with ID: {}",
                    self.purge_stream.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO, "Stream with ID: {} purged", self.purge_stream.stream_id);

        Ok(())
    }
}
