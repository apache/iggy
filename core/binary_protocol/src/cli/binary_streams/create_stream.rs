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
use iggy_common::create_stream::CreateStream;
use tracing::{Level, event};

pub struct CreateStreamCmd {
    create_stream: CreateStream,
}

impl CreateStreamCmd {
    pub fn new(name: String) -> Self {
        Self {
            create_stream: CreateStream { name },
        }
    }

    fn get_stream_id_info(&self) -> String {
        "ID auto incremented".to_string()
    }
}

#[async_trait]
impl CliCommand for CreateStreamCmd {
    fn explain(&self) -> String {
        format!(
            "create stream with name: {} and {}",
            self.create_stream.name,
            self.get_stream_id_info(),
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .create_stream(&self.create_stream.name)
            .await
            .with_context(|| {
                format!(
                    "Problem creating stream (name: {} and {})",
                    self.create_stream.name,
                    self.get_stream_id_info(),
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Stream with name: {} and {} created",
            self.create_stream.name,
            self.get_stream_id_info(),
        );

        Ok(())
    }
}
