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

use iggy_common::Stream;
use integration::{
    test_mcp_server::{McpClient, TestMcpServer},
    test_server::TestServer,
};
use rmcp::{
    ServiceError,
    model::{CallToolRequestParam, CallToolResult, ListToolsResult},
    serde::de::DeserializeOwned,
    serde_json,
};

#[tokio::test]
async fn mcp_server_should_list_tools() {
    let infra = setup().await;
    let client = infra.client;
    let tools = client.list_tools().await.expect("Failed to list tools");

    assert!(!tools.tools.is_empty());
    let tools_count = tools.tools.len();
    assert_eq!(tools_count, 40);
}

#[tokio::test]
async fn mcp_server_should_handle_ping() {
    assert_empty_response("ping", None).await;
}

#[tokio::test]
async fn mcp_server_should_return_list_of_streams() {
    assert_response::<Vec<Stream>>("get_streams", None, |streams| assert!(streams.is_empty()))
        .await;
}

async fn assert_empty_response(method: &str, data: Option<serde_json::Value>) {
    assert_response::<()>(method, data, |()| {}).await
}

async fn assert_response<T: DeserializeOwned>(
    method: &str,
    data: Option<serde_json::Value>,
    assert_response: impl FnOnce(T),
) {
    let infra = setup().await;
    let client = infra.client;
    let error_message = format!("Failed to invoke method: {method}",);
    let mut result = client.invoke(method, data).await.expect(&error_message);

    if result.content.is_empty() {
        panic!("No content returned");
    }

    let result = result.content.remove(0);
    let Some(text) = result.as_text() else {
        panic!("Expected text response");
    };

    let json = serde_json::from_str::<T>(&text.text).expect("Failed to parse JSON");
    assert_response(json)
}

async fn setup() -> McpInfra {
    let mut test_server = TestServer::default();
    test_server.start();
    let iggy_server_address = test_server
        .get_raw_tcp_addr()
        .expect("Failed to get Iggy TCP address");
    println!("Iggy server address: {iggy_server_address}");
    let mut test_mcp_server = TestMcpServer::with_iggy_address(&iggy_server_address);
    test_mcp_server.start();
    test_mcp_server.ensure_started().await;
    let client = test_mcp_server.get_client().await;
    let server_info = client.peer_info();
    println!("Connected to MCP server: {server_info:#?}");
    McpInfra {
        _iggy_server: test_server,
        _mcp_server: test_mcp_server,
        client: TestMcpClient { client },
    }
}

#[derive(Debug)]
struct McpInfra {
    _iggy_server: TestServer,
    _mcp_server: TestMcpServer,
    client: TestMcpClient,
}

#[derive(Debug)]
struct TestMcpClient {
    client: McpClient,
}

impl TestMcpClient {
    pub async fn list_tools(&self) -> Result<ListToolsResult, ServiceError> {
        self.client.list_tools(Default::default()).await
    }

    pub async fn invoke(
        &self,
        method: &str,
        data: Option<serde_json::Value>,
    ) -> Result<CallToolResult, ServiceError> {
        self.client
            .call_tool(CallToolRequestParam {
                name: method.to_owned().into(),
                arguments: data.and_then(|value| value.as_object().cloned()),
            })
            .await
    }
}
