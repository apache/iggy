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

use iggy::prelude::{Identifier, IggyClient, IggyError, StreamClient};
use requests::*;
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::Parameters},
    model::{CallToolResult, Content, ErrorData, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use serde::Serialize;
use tracing::error;

use crate::Permissions;
mod requests;

#[derive(Debug, Clone)]
pub struct IggyService {
    tool_router: ToolRouter<Self>,
    consumer: Arc<IggyClient>,
    _producer: Arc<IggyClient>,
    permissions: Permissions,
}

#[tool_router]
impl IggyService {
    pub fn new(
        consumer: Arc<IggyClient>,
        producer: Arc<IggyClient>,
        permissions: Permissions,
    ) -> Self {
        Self {
            tool_router: Self::tool_router(),
            consumer,
            _producer: producer,
            permissions,
        }
    }

    #[tool(description = "Get stream")]
    pub async fn get_stream(
        &self,
        Parameters(GetStream { stream_id }): Parameters<GetStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.consumer.get_stream(&id(&stream_id)?).await)
    }

    #[tool(description = "Get streams")]
    pub async fn get_streams(&self) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_read()?;
        request(self.consumer.get_streams().await)
    }

    #[tool(description = "Create stream")]
    pub async fn create_stream(
        &self,
        Parameters(CreateStream { name, stream_id }): Parameters<CreateStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_create()?;
        request(self.consumer.create_stream(&name, stream_id).await)
    }

    #[tool(description = "Update stream")]
    pub async fn update_stream(
        &self,
        Parameters(UpdateStream { stream_id, name }): Parameters<UpdateStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_update()?;
        request(self.consumer.update_stream(&id(&stream_id)?, &name).await)
    }

    #[tool(description = "Delete stream")]
    pub async fn delete_stream(
        &self,
        Parameters(DeleteStream { stream_id }): Parameters<DeleteStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(self.consumer.delete_stream(&id(&stream_id)?).await)
    }

    #[tool(description = "Purge stream")]
    pub async fn purge_stream(
        &self,
        Parameters(PurgeStream { stream_id }): Parameters<PurgeStream>,
    ) -> Result<CallToolResult, ErrorData> {
        self.permissions.ensure_delete()?;
        request(self.consumer.purge_stream(&id(&stream_id)?).await)
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

fn request(result: Result<impl Sized + Serialize, IggyError>) -> Result<CallToolResult, ErrorData> {
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
