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

use integration::{test_mcp_server::TestMcpServer, test_server::TestServer};

#[tokio::test]
async fn mcp_server_should_list_tools() {
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
    println!("Invoking MCP client");

    let server_info = client.peer_info();
    println!("Connected to MCP server: {server_info:#?}");

    let tools = client
        .list_tools(Default::default())
        .await
        .expect("Failed to list tools");
    println!("Available tools: {tools:#?}");

    assert!(!tools.tools.is_empty());
    let tools_count = tools.tools.len();
    assert_eq!(tools_count, 40);
}
