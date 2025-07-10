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

use std::sync::Arc;

use iggy::prelude::{Identifier, IggyClient, IggyError};
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::Parameters},
    model::{CallToolResult, Content, ErrorData, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use serde::Serialize;
use streams::GetStream;
use tracing::error;

mod consumer_groups;
mod consumer_offsets;
mod messages;
mod partitions;
mod personal_access_tokens;
mod segments;
mod streams;
mod system;
mod topics;
mod users;

#[derive(Debug, Clone)]
pub struct IggyService {
    tool_router: ToolRouter<Self>,
    service: InternalService,
}

#[tool_router]
impl IggyService {
    pub fn new(consumer: Arc<IggyClient>, producer: Arc<IggyClient>) -> Self {
        Self {
            tool_router: Self::tool_router(),
            service: InternalService {
                consumer,
                _producer: producer,
            },
        }
    }

    #[tool(description = "Get streams")]
    pub async fn get_streams(&self) -> Result<CallToolResult, ErrorData> {
        self.service.get_streams().await
    }

    #[tool(description = "Get stream")]
    pub async fn get_stream(
        &self,
        Parameters(GetStream { stream_id }): Parameters<GetStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.service.get_stream(id(&stream_id)?).await
    }
}

#[tool_handler]
impl ServerHandler for IggyService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some("Iggy service".into()),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

fn id(id: &str) -> Result<Identifier, ErrorData> {
    Identifier::from_str_value(id).map_err(|e| {
        let message = format!("Failed to parse identifier. {e}");
        error!(message);
        ErrorData::invalid_request(message, None)
    })
}

#[derive(Debug, Clone)]
struct InternalService {
    consumer: Arc<IggyClient>,
    _producer: Arc<IggyClient>,
}

impl InternalService {
    fn request(
        &self,
        result: Result<impl Sized + Serialize, IggyError>,
    ) -> Result<CallToolResult, ErrorData> {
        let result = result.map_err(|e| {
            let message = format!("There was an error when invoking the method. {e}");
            error!(message);
            ErrorData::invalid_request(message, None)
        })?;

        let content = Content::json(result).map_err(|error| {
            let message = format!("Failed to serialize result. {error}");
            error!(message);
            ErrorData::internal_error(message, None)
        })?;

        Ok(CallToolResult::success(vec![content]))
    }
}
