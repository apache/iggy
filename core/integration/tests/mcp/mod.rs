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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::time::Duration;

use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, ConsumerGroup, ConsumerGroupDetails,
    ConsumerOffsetInfo, IggyMessage, MessageClient, Partitioning, Permissions,
    PersonalAccessTokenClient, PersonalAccessTokenExpiry, PersonalAccessTokenInfo, PolledMessages,
    RawPersonalAccessToken, Snapshot, Stats, Stream, StreamDetails, Topic, TopicDetails,
    UserClient, UserCreateOptions, UserInfo, UserInfoDetails, UserStatus,
};
use integration::{
    harness::{McpClient, McpConfig, TestHarness, cargo_bin_path, seeds},
    iggy_harness,
};
use reqwest::{
    StatusCode,
    header::{self, HeaderName},
};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientCapabilities, ClientConfig, Implementation},
    serde::de::DeserializeOwned,
    serde_json::{self, json},
    service::ServiceError,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::ChildStdout;
use tokio::time::timeout;

const MCP_STDIO_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_PERMISSION_ENV: &str = "IGGY_MCP_PERMISSIONS_ADMIN";
const AUTH_TOKEN_ENV: &str = "IGGY_MCP_HTTP_AUTH_TOKEN";
const AUTH_TOKEN: &str = "mcp-test-token-for-bearer-checks";

async fn invoke<T: DeserializeOwned>(
    client: &McpClient,
    method: &str,
    data: Option<serde_json::Value>,
) -> T {
    let params = CallToolRequestParams::new(method.to_owned());
    let params = match data.and_then(|v| v.as_object().cloned()) {
        Some(args) => params.with_arguments(args),
        None => params,
    };
    let mut result = client
        .call_tool(params)
        .await
        .unwrap_or_else(|e| panic!("Failed to invoke {method}: {e}"));

    let content = result.content.remove(0);
    let text = content
        .as_text()
        .unwrap_or_else(|| panic!("Expected text response for {method}"));
    serde_json::from_str(&text.text)
        .unwrap_or_else(|e| panic!("Failed to parse {method} response: {e}"))
}

async fn invoke_empty(client: &McpClient, method: &str, data: Option<serde_json::Value>) {
    let params = CallToolRequestParams::new(method.to_owned());
    let params = match data.and_then(|v| v.as_object().cloned()) {
        Some(args) => params.with_arguments(args),
        None => params,
    };
    let result = client
        .call_tool(params)
        .await
        .unwrap_or_else(|e| panic!("Failed to invoke {method}: {e}"));

    assert!(!result.is_error.unwrap_or(false), "{method} returned error");
}

async fn start_mcp(envs: &[(&str, &str)]) -> TestHarness {
    let mut harness = TestHarness::builder()
        .default_server()
        .root_tcp_client()
        .mcp(
            McpConfig::builder()
                .consumer_name(seeds::names::CONSUMER)
                .extra_envs(
                    envs.iter()
                        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                        .collect(),
                )
                .build(),
        )
        .build()
        .expect("Failed to build MCP harness");
    harness
        .start_with_seed(|client| async move { seeds::mcp_standard(&client).await })
        .await
        .expect("Failed to start MCP harness");
    harness
}

async fn start_administration_mcp() -> TestHarness {
    start_mcp(&[(ADMIN_PERMISSION_ENV, "true")]).await
}

async fn post_initialize(url: &str, headers: &[(HeaderName, &str)]) -> StatusCode {
    let mut request = reqwest::Client::new()
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .body(initialize_request().to_string());
    for (name, value) in headers {
        request = request.header(name, *value);
    }
    request.send().await.expect("MCP request failed").status()
}

fn initialize_request() -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "mcp-integration-test", "version": "1"}
        }
    })
}

fn stdio_mcp(address: SocketAddr, username: &str, password: &str) -> tokio::process::Command {
    let mut command = Command::new(cargo_bin_path("iggy-mcp").expect("MCP binary required"));
    command
        .env("IGGY_MCP_TRANSPORT", "stdio")
        .env("IGGY_MCP_IGGY_ADDRESS", address.to_string())
        .env("IGGY_MCP_IGGY_USERNAME", username)
        .env("IGGY_MCP_IGGY_PASSWORD", password)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    let mut command = tokio::process::Command::from(command);
    command.kill_on_drop(true);
    command
}

async fn read_reply(lines: &mut Lines<BufReader<ChildStdout>>, id: u64) -> serde_json::Value {
    loop {
        let line = timeout(MCP_STDIO_TIMEOUT, lines.next_line())
            .await
            .expect("MCP reply timed out")
            .expect("Failed to read MCP reply")
            .expect("MCP closed stdout before replying");
        let reply: serde_json::Value = serde_json::from_str(&line).expect("Invalid MCP reply");
        if reply["id"] == id {
            return reply;
        }
    }
}

#[iggy_harness(server(mcp))]
async fn should_list_tools(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let tools = mcp_client
        .list_tools(Default::default())
        .await
        .expect("Failed to list tools");

    assert!(!tools.tools.is_empty());
    assert_eq!(tools.tools.len(), 41);
}

#[iggy_harness(server(mcp))]
async fn should_handle_ping(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(&mcp_client, "ping", None).await;
}

#[iggy_harness(server(mcp))]
async fn should_report_unknown_recovery_operations_without_losing_the_operation_id(
    harness: &TestHarness,
) {
    let client = harness.mcp_client().await.expect("MCP client required");
    for method in ["resolve_operation", "abandon_operation"] {
        let operation_id = u64::MAX.to_string();
        let arguments = json!({"operation_id": operation_id});
        let params = CallToolRequestParams::new(method.to_owned())
            .with_arguments(arguments.as_object().unwrap().clone());
        let result = client.call_tool(params).await;
        let Err(ServiceError::McpError(error)) = result else {
            panic!("Expected an operation-not-retained error from {method}, got {result:?}");
        };
        assert_eq!(error.data.unwrap()["operation_id"], operation_id);
    }
}

// Pins two nodes: the default vsr harness spawns three, but this test asserts a
// two-node roster. Exercises the binary GetClusterMetadata roster end-to-end
// over MCP (MCP server -> TCP -> iggy).
#[iggy_harness(cluster_nodes = 2, server(mcp))]
async fn should_return_cluster_metadata(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let cluster: ClusterMetadata = invoke(&mcp_client, "get_cluster_metadata", None).await;

    assert!(!cluster.name.is_empty());
    assert_eq!(cluster.nodes.len(), 2);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_list_of_streams(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let streams: Vec<Stream> = invoke(&mcp_client, "get_streams", None).await;

    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name, seeds::names::STREAM);
    assert_eq!(streams[0].topics_count, 1);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_stream_details(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let stream: StreamDetails = invoke(
        &mcp_client,
        "get_stream",
        Some(json!({"stream_id": seeds::names::STREAM})),
    )
    .await;

    assert_eq!(stream.name, seeds::names::STREAM);
    assert_eq!(stream.topics_count, 1);
    assert_eq!(stream.messages_count, 1);
}

#[iggy_harness(server(mcp))]
async fn should_create_stream(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let name = "new_stream";
    let stream: StreamDetails =
        invoke(&mcp_client, "create_stream", Some(json!({"name": name}))).await;

    assert_eq!(stream.name, name);
    assert_eq!(stream.topics_count, 0);
    assert_eq!(stream.messages_count, 0);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_update_stream(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "update_stream",
        Some(json!({"stream_id": seeds::names::STREAM, "name": "updated_stream"})),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_stream(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_stream",
        Some(json!({"stream_id": seeds::names::STREAM})),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_list_of_topics(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let topics: Vec<Topic> = invoke(
        &mcp_client,
        "get_topics",
        Some(json!({"stream_id": seeds::names::STREAM})),
    )
    .await;

    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, seeds::names::TOPIC);
    assert_eq!(topics[0].partitions_count, 1);
    assert_eq!(topics[0].messages_count, 1);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_topic_details(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let topic: TopicDetails = invoke(
        &mcp_client,
        "get_topic",
        Some(json!({"stream_id": seeds::names::STREAM, "topic_id": seeds::names::TOPIC})),
    )
    .await;

    assert_eq!(topic.id, 0);
    assert_eq!(topic.name, seeds::names::TOPIC);
    assert_eq!(topic.messages_count, 1);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn given_zero_partitions_when_creating_a_topic_should_allow_adding_partitions_later(
    harness: &TestHarness,
) {
    let client = harness.mcp_client().await.expect("MCP client required");
    let name = "empty-topic";
    let topic: TopicDetails = invoke(
        &client,
        "create_topic",
        Some(json!({"stream_id": seeds::names::STREAM, "name": name, "partitions_count": 0})),
    )
    .await;

    assert_eq!(topic.partitions_count, 0);
    assert!(topic.partitions.is_empty());
    invoke_empty(
        &client,
        "create_partitions",
        Some(json!({"stream_id": seeds::names::STREAM, "topic_id": name, "partitions_count": 1})),
    )
    .await;
    let topic: TopicDetails = invoke(
        &client,
        "get_topic",
        Some(json!({"stream_id": seeds::names::STREAM, "topic_id": name})),
    )
    .await;
    assert_eq!(topic.partitions_count, 1);
    let tools = client
        .list_tools(Default::default())
        .await
        .expect("tools list");
    let create = tools
        .tools
        .iter()
        .find(|tool| tool.name == "create_topic")
        .expect("create_topic is listed");
    let count = &create.input_schema["properties"]["partitions_count"];
    assert_eq!(count["minimum"], json!(0));
    assert!(
        count["description"]
            .as_str()
            .expect("partition guidance")
            .contains("zero creates a topic with no partitions")
    );
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_create_topic(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let name = "new_topic";
    let topic: TopicDetails = invoke(
        &mcp_client,
        "create_topic",
        Some(json!({"stream_id": seeds::names::STREAM, "name": name, "partitions_count": 1})),
    )
    .await;

    assert_eq!(topic.id, 1);
    assert_eq!(topic.name, name);
    assert_eq!(topic.partitions_count, 1);
    assert_eq!(topic.messages_count, 0);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_update_topic(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "update_topic",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "name": "updated_topic"
        })),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_topic(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_topic",
        Some(json!({"stream_id": seeds::names::STREAM, "topic_id": seeds::names::TOPIC})),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_create_partitions(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "create_partitions",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partitions_count": 3
        })),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_partitions(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_partitions",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partitions_count": 1
        })),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_segments(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_segments",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partition_id": 0,
            "segments_count": 1
        })),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_poll_messages(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let messages: PolledMessages = invoke(
        &mcp_client,
        "poll_messages",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partition_id": 0,
            "offset": 0
        })),
    )
    .await;

    assert_eq!(messages.messages.len(), 1);
    assert_eq!(messages.messages[0].header.offset, 0);
    let payload = messages.messages[0]
        .payload_as_string()
        .expect("Failed to parse payload");
    assert_eq!(payload, seeds::names::MESSAGE_PAYLOAD);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_send_messages(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    for partitioning in [None, Some("partition")] {
        invoke_empty(
            &mcp_client,
            "send_messages",
            Some(json!({
                "stream_id": seeds::names::STREAM,
                "topic_id": seeds::names::TOPIC,
                "partitioning": partitioning,
                "messages": [{"payload": "test"}]
            })),
        )
        .await;
    }
    let messages: PolledMessages = invoke(
        &mcp_client,
        "poll_messages",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partition_id": 0,
            "offset": 1
        })),
    )
    .await;
    assert_eq!(messages.messages.len(), 2);
    for message in messages.messages {
        assert_eq!(message.payload_as_string().expect("UTF-8 payload"), "test");
    }
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_stats(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let stats: Stats = invoke(&mcp_client, "get_stats", None).await;

    assert!(!stats.hostname.is_empty());
    assert_eq!(stats.messages_count, 1);
}

#[iggy_harness(server(mcp))]
async fn should_return_me(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let client: ClientInfoDetails = invoke(&mcp_client, "get_me", None).await;

    assert!(client.client_id > 0);
}

#[iggy_harness(server(mcp))]
async fn should_return_clients(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let clients: Vec<ClientInfo> = invoke(&mcp_client, "get_clients", None).await;

    assert!(!clients.is_empty());
}

#[tokio::test]
#[serial_test::parallel]
async fn should_handle_snapshot() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let snapshot: Snapshot = invoke(&mcp_client, "snapshot", None).await;

    assert!(!snapshot.0.is_empty());
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_consumer_groups(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let groups: Vec<ConsumerGroup> = invoke(
        &mcp_client,
        "get_consumer_groups",
        Some(json!({"stream_id": seeds::names::STREAM, "topic_id": seeds::names::TOPIC})),
    )
    .await;

    assert!(!groups.is_empty());
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_consumer_group_details(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let group: ConsumerGroupDetails = invoke(
        &mcp_client,
        "get_consumer_group",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "group_id": seeds::names::CONSUMER_GROUP
        })),
    )
    .await;

    assert_eq!(group.name, seeds::names::CONSUMER_GROUP);
    assert_eq!(group.partitions_count, 1);
    assert_eq!(group.members_count, 0);
    assert!(group.members.is_empty());
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_create_consumer_group(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let name = "new_group";
    let group: ConsumerGroupDetails = invoke(
        &mcp_client,
        "create_consumer_group",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "name": name
        })),
    )
    .await;

    assert_eq!(group.name, name);
    assert_eq!(group.partitions_count, 1);
    assert_eq!(group.members_count, 0);
    assert!(group.members.is_empty());
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_consumer_group(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_consumer_group",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "group_id": seeds::names::CONSUMER_GROUP
        })),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_consumer_offset(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let offset: Option<ConsumerOffsetInfo> = invoke(
        &mcp_client,
        "get_consumer_offset",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partition_id": 0
        })),
    )
    .await;

    let offset = offset.expect("Expected consumer offset");
    assert_eq!(offset.partition_id, 0);
    assert_eq!(offset.stored_offset, 0);
    assert_eq!(offset.current_offset, 0);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_store_consumer_offset(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "store_consumer_offset",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partition_id": 0,
            "offset": 0
        })),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_consumer_offset(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_consumer_offset",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partition_id": 0,
            "offset": 0
        })),
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn should_return_personal_access_tokens() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let tokens: Vec<PersonalAccessTokenInfo> =
        invoke(&mcp_client, "get_personal_access_tokens", None).await;

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].name, seeds::names::PERSONAL_ACCESS_TOKEN);
}

#[tokio::test]
#[serial_test::parallel]
async fn should_create_personal_access_token() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let name = "test_token";
    let expiry = PersonalAccessTokenExpiry::NeverExpire.to_string();
    let token: RawPersonalAccessToken = invoke(
        &mcp_client,
        "create_personal_access_token",
        Some(json!({"name": name, "expiry": expiry})),
    )
    .await;

    assert!(!token.token.is_empty());
}

#[tokio::test]
#[serial_test::parallel]
async fn should_refuse_personal_access_token_with_unparsable_expiry() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let name = "test_token";
    let arguments = json!({"name": name, "expiry": "tomorrow"});
    let params = CallToolRequestParams::new("create_personal_access_token".to_owned())
        .with_arguments(arguments.as_object().expect("Tool arguments").clone());
    let result = mcp_client.call_tool(params).await;

    assert!(
        matches!(&result, Err(ServiceError::McpError(error))
            if error.message.starts_with("Failed to parse personal access token expiry")),
        "Expected an expiry parse error, got {result:?}"
    );
    let tokens: Vec<PersonalAccessTokenInfo> =
        invoke(&mcp_client, "get_personal_access_tokens", None).await;
    assert!(
        tokens.iter().all(|token| token.name != name),
        "A refused expiry must not create a token: {tokens:?}"
    );
}

#[tokio::test]
#[serial_test::parallel]
async fn should_delete_personal_access_token() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_personal_access_token",
        Some(json!({"name": seeds::names::PERSONAL_ACCESS_TOKEN})),
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn should_return_users() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let users: Vec<UserInfo> = invoke(&mcp_client, "get_users", None).await;

    assert_eq!(users.len(), 2);
}

#[tokio::test]
#[serial_test::parallel]
async fn should_return_user_details() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let user: UserInfoDetails = invoke(
        &mcp_client,
        "get_user",
        Some(json!({"user_id": seeds::names::USER})),
    )
    .await;

    assert_eq!(user.username, seeds::names::USER);
}

#[tokio::test]
#[serial_test::parallel]
async fn should_create_user() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let username = "test-mcp-user";
    let user: UserInfoDetails = invoke(
        &mcp_client,
        "create_user",
        Some(json!({"username": username, "password": "secret"})),
    )
    .await;

    assert_eq!(user.username, username);
    assert_eq!(
        user.status,
        UserStatus::Active,
        "A user created without `active` must be able to sign in"
    );
}

#[tokio::test]
#[serial_test::parallel]
async fn should_update_user() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "update_user",
        Some(json!({
            "user_id": seeds::names::USER,
            "username": "updated-user",
            "active": false
        })),
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn should_delete_user() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_user",
        Some(json!({"user_id": seeds::names::USER})),
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn should_update_permissions() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let permissions = json!({
        "global": {
            "manage_servers": true,
            "read_users": true,
        },
        // `mcp_standard` seeds one stream with one topic, so both take id 0.
        "streams": {
            "0": {
                "manage_stream": true,
                "manage_topics": true,
                "topics": {
                    "0": {
                        "manage_topic": true,
                        "read_topic": true,
                        "poll_messages": true,
                        "send_messages": true,
                    }
                }
            }
        }
    });

    invoke_empty(
        &mcp_client,
        "update_permissions",
        Some(json!({"user_id": seeds::names::USER, "permissions": permissions})),
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn should_change_password() {
    let harness = start_administration_mcp().await;
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "change_password",
        Some(json!({
            "user_id": seeds::names::USER,
            "current_password": seeds::names::USER_PASSWORD,
            "new_password": "new_secret"
        })),
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn should_reject_mutating_tools_with_read_only_permissions() {
    let mut harness = TestHarness::builder()
        .default_server()
        .root_tcp_client()
        .mcp(
            McpConfig::builder()
                .consumer_name(seeds::names::CONSUMER)
                .extra_envs(HashMap::from([
                    (
                        "IGGY_MCP_PERMISSIONS_CREATE".to_string(),
                        "false".to_string(),
                    ),
                    ("IGGY_MCP_PERMISSIONS_READ".to_string(), "true".to_string()),
                    (
                        "IGGY_MCP_PERMISSIONS_UPDATE".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "IGGY_MCP_PERMISSIONS_DELETE".to_string(),
                        "false".to_string(),
                    ),
                    (ADMIN_PERMISSION_ENV.to_string(), "true".to_string()),
                ]))
                .build(),
        )
        .build()
        .expect("Failed to build read-only MCP harness");
    harness
        .start_with_seed(|client| async move {
            seeds::mcp_standard(&client).await?;
            let mut messages = [IggyMessage::builder()
                .payload(seeds::names::MESSAGE_PAYLOAD.into())
                .build()?];
            client
                .send_messages(
                    &seeds::names::STREAM.try_into()?,
                    &seeds::names::TOPIC.try_into()?,
                    &Partitioning::partition_id(0),
                    &mut messages,
                )
                .await?;
            Ok(())
        })
        .await
        .expect("Failed to start read-only MCP harness");
    let client = harness.mcp_client().await.expect("MCP client required");
    let partition = json!({
        "stream_id": seeds::names::STREAM,
        "topic_id": seeds::names::TOPIC,
        "partition_id": 0,
    });
    let messages: PolledMessages = invoke(&client, "poll_messages", Some(partition.clone())).await;
    assert_eq!(messages.messages.len(), 2, "Read-only polling must work");

    let mut store = partition.clone();
    store["offset"] = json!(1);
    let mut auto_commit = partition.clone();
    auto_commit["auto_commit"] = json!(true);
    let mut next = partition.clone();
    next["strategy"] = json!("next");
    let mut unexpected = Vec::new();
    for (method, mut arguments, permission) in [
        (
            "create_personal_access_token",
            json!({"name": "forbidden-token"}),
            "create",
        ),
        (
            "delete_personal_access_token",
            json!({"name": seeds::names::PERSONAL_ACCESS_TOKEN}),
            "delete",
        ),
        ("store_consumer_offset", store, "update"),
        ("delete_consumer_offset", partition.clone(), "delete"),
        ("poll_messages", auto_commit, "update"),
        ("poll_messages", next, "update"),
        (
            "resolve_operation",
            json!({"operation_id": "1"}),
            "write recovery",
        ),
        (
            "abandon_operation",
            json!({"operation_id": "1"}),
            "write recovery",
        ),
    ] {
        let params = CallToolRequestParams::new(method.to_owned())
            .with_arguments(arguments.as_object_mut().expect("Tool arguments").clone());
        let result = client.call_tool(params).await;
        if !matches!(&result, Err(ServiceError::McpError(error))
            if error.message == format!("Insufficient '{permission}' permissions"))
        {
            unexpected.push(format!(
                "{method} ({arguments}), required {permission}: {result:?}"
            ));
        }
    }
    assert!(
        unexpected.is_empty(),
        "Mutating tools bypassed read-only permissions: {unexpected:?}"
    );

    let offset: Option<ConsumerOffsetInfo> =
        invoke(&client, "get_consumer_offset", Some(partition)).await;
    assert_eq!(offset.expect("Seeded offset must remain").stored_offset, 0);
    let tokens: Vec<PersonalAccessTokenInfo> =
        invoke(&client, "get_personal_access_tokens", None).await;
    assert_eq!(
        tokens.len(),
        1,
        "Rejected calls must leave tokens unchanged"
    );
    assert_eq!(tokens[0].name, seeds::names::PERSONAL_ACCESS_TOKEN);
}

#[iggy_harness]
async fn should_exit_stdio_on_disconnect_or_signal(harness: &TestHarness) {
    for close_stdin in [true, false] {
        let address = harness.server().tcp_addr().expect("TCP address required");
        let mut child = stdio_mcp(address, "iggy", "iggy")
            .spawn()
            .expect("Failed to start stdio MCP server");
        let mut stdin = child.stdin.take().expect("MCP stdin required");
        let mut stdout = BufReader::new(child.stdout.take().expect("MCP stdout required"));
        let initialize = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "stdio-shutdown-test", "version": "1"}
            }
        });
        stdin
            .write_all(format!("{initialize}\n").as_bytes())
            .await
            .expect("Failed to initialize MCP session");
        let mut response = String::new();
        timeout(MCP_STDIO_TIMEOUT, stdout.read_line(&mut response))
            .await
            .expect("MCP initialization timed out")
            .expect("Failed to read MCP initialization response");
        let response: serde_json::Value =
            serde_json::from_str(&response).expect("Invalid MCP initialization response");
        assert!(
            response.get("result").is_some(),
            "Initialization failed: {response}"
        );
        let initialized = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
        stdin
            .write_all(format!("{initialized}\n").as_bytes())
            .await
            .expect("Failed to confirm MCP initialization");
        if close_stdin {
            drop(stdin);
        } else {
            let status = Command::new("kill")
                .arg("-TERM")
                .arg(child.id().expect("Running MCP process").to_string())
                .status()
                .expect("Failed to signal MCP server");
            assert!(status.success(), "Failed to send SIGTERM to MCP server");
        }

        let status = timeout(MCP_STDIO_TIMEOUT, child.wait())
            .await
            .unwrap_or_else(|_| panic!("MCP server did not exit (close_stdin={close_stdin})"))
            .expect("Failed to wait for MCP server");
        assert!(
            status.success(),
            "MCP server failed during shutdown: {status}"
        );
    }
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_refuse_administration_tools_by_default(harness: &TestHarness) {
    let client = harness.mcp_client().await.expect("MCP client required");
    let mut unexpected = Vec::new();
    for (method, arguments) in [
        ("get_personal_access_tokens", json!({})),
        (
            "create_personal_access_token",
            json!({"name": "forbidden-token"}),
        ),
        (
            "delete_personal_access_token",
            json!({"name": seeds::names::PERSONAL_ACCESS_TOKEN}),
        ),
        ("get_users", json!({})),
        ("get_user", json!({"user_id": seeds::names::USER})),
        (
            "create_user",
            json!({"username": "forbidden-user", "password": "secret"}),
        ),
        (
            "update_user",
            json!({"user_id": seeds::names::USER, "active": false}),
        ),
        ("delete_user", json!({"user_id": seeds::names::USER})),
        ("update_permissions", json!({"user_id": seeds::names::USER})),
        (
            "change_password",
            json!({
                "user_id": seeds::names::USER,
                "current_password": seeds::names::USER_PASSWORD,
                "new_password": "changed"
            }),
        ),
        ("snapshot", json!({})),
    ] {
        let params = CallToolRequestParams::new(method.to_owned())
            .with_arguments(arguments.as_object().expect("Tool arguments").clone());
        let result = client.call_tool(params).await;
        if !matches!(&result, Err(ServiceError::McpError(error))
            if error.message == "Insufficient 'admin' permissions")
        {
            unexpected.push(format!("{method}: {result:?}"));
        }
    }
    assert!(
        unexpected.is_empty(),
        "Administration tools ran without the admin permission: {unexpected:?}"
    );

    let root = harness.root_client().await.expect("Root client required");
    let users = root.get_users().await.expect("Failed to read users");
    assert!(users.iter().all(|user| user.username != "forbidden-user"));
    let user = users
        .iter()
        .find(|user| user.username == seeds::names::USER)
        .expect("The seeded user must remain");
    assert_eq!(user.status, UserStatus::Active);
    let tokens = root
        .get_personal_access_tokens()
        .await
        .expect("Failed to read personal access tokens");
    assert_eq!(tokens.len(), 1, "Refused calls must leave tokens unchanged");
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_refuse_send_messages_when_any_message_is_invalid(harness: &TestHarness) {
    let client = harness.mcp_client().await.expect("MCP client required");
    let partition = json!({
        "stream_id": seeds::names::STREAM,
        "topic_id": seeds::names::TOPIC,
        "partition_id": 0,
    });
    let mut send = partition.clone();
    send["partitioning"] = json!("partition");
    send["messages"] = json!([{"payload": "valid"}, {"payload": ""}]);
    let params = CallToolRequestParams::new("send_messages".to_owned())
        .with_arguments(send.as_object().expect("Tool arguments").clone());
    let result = client.call_tool(params).await;

    assert!(
        matches!(&result, Err(ServiceError::McpError(_))),
        "A batch with an empty payload must be refused, got {result:?}"
    );
    let mut poll = partition;
    poll["offset"] = json!(1);
    let messages: PolledMessages = invoke(&client, "poll_messages", Some(poll)).await;
    assert!(
        messages.messages.is_empty(),
        "A refused batch must send no message, got {:?}",
        messages.messages
    );
}

#[tokio::test]
#[serial_test::parallel]
async fn should_require_the_configured_bearer_token() {
    let harness = start_mcp(&[(AUTH_TOKEN_ENV, AUTH_TOKEN)]).await;
    let mcp = harness.mcp().expect("MCP server required");
    let url = mcp.mcp_url();
    let bearer = format!("Bearer {AUTH_TOKEN}");
    for (headers, expected) in [
        (vec![], StatusCode::UNAUTHORIZED),
        (
            vec![(header::AUTHORIZATION, "Bearer wrong-token")],
            StatusCode::UNAUTHORIZED,
        ),
        (
            vec![(header::AUTHORIZATION, AUTH_TOKEN)],
            StatusCode::UNAUTHORIZED,
        ),
        (
            vec![
                (header::AUTHORIZATION, bearer.as_str()),
                (header::AUTHORIZATION, "Bearer wrong-token"),
            ],
            StatusCode::UNAUTHORIZED,
        ),
        (
            vec![(header::AUTHORIZATION, bearer.as_str())],
            StatusCode::OK,
        ),
    ] {
        assert_eq!(
            post_initialize(&url, &headers).await,
            expected,
            "Authorization headers {headers:?}"
        );
    }

    let health = reqwest::get(format!("http://{}/health", mcp.http_address()))
        .await
        .expect("Health request failed");
    assert_eq!(health.status(), StatusCode::OK);

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url).auth_header(AUTH_TOKEN),
    );
    let client = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new("mcp-integration-test", "1"),
    )
    .serve(transport)
    .await
    .expect("Failed to connect with the bearer token");
    let streams: Vec<Stream> = invoke(&client, "get_streams", None).await;
    assert_eq!(streams.len(), 1);
}

#[iggy_harness(server(mcp))]
async fn should_refuse_browser_origins_when_cors_is_disabled(harness: &TestHarness) {
    let url = harness.mcp().expect("MCP server required").mcp_url();

    assert_eq!(post_initialize(&url, &[]).await, StatusCode::OK);
    assert_eq!(
        post_initialize(&url, &[(header::ORIGIN, "http://untrusted.example")]).await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
#[serial_test::parallel]
async fn should_admit_only_listed_origins_when_cors_is_enabled() {
    let harness = start_mcp(&[
        ("IGGY_MCP_HTTP_CORS_ENABLED", "true"),
        (
            "IGGY_MCP_HTTP_CORS_ALLOWED_ORIGINS",
            "[http://console.example]",
        ),
    ])
    .await;
    let url = harness.mcp().expect("MCP server required").mcp_url();

    assert_eq!(
        post_initialize(&url, &[(header::ORIGIN, "http://console.example")]).await,
        StatusCode::OK
    );
    assert_eq!(
        post_initialize(&url, &[(header::ORIGIN, "http://untrusted.example")]).await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
#[serial_test::parallel]
async fn should_accept_only_configured_host_names() {
    let harness = start_mcp(&[("IGGY_MCP_HTTP_ALLOWED_HOSTS", "[127.0.0.1, mcp.example]")]).await;
    let url = harness.mcp().expect("MCP server required").mcp_url();

    assert_eq!(
        post_initialize(&url, &[(header::HOST, "mcp.example")]).await,
        StatusCode::OK
    );
    assert_eq!(
        post_initialize(&url, &[(header::HOST, "untrusted.example")]).await,
        StatusCode::FORBIDDEN
    );
}

#[iggy_harness]
async fn should_refuse_to_start_with_an_unsafe_or_invalid_http_configuration(
    harness: &TestHarness,
) {
    let address = harness.server().tcp_addr().expect("TCP address required");
    let mut accepted = Vec::new();
    for overrides in [
        vec![("IGGY_MCP_HTTP_ADDRESS", "0.0.0.0:0")],
        vec![
            ("IGGY_MCP_HTTP_CORS_ENABLED", "true"),
            ("IGGY_MCP_HTTP_CORS_ALLOWED_ORIGINS", "[*]"),
        ],
        vec![
            ("IGGY_MCP_HTTP_CORS_ENABLED", "true"),
            ("IGGY_MCP_HTTP_CORS_ALLOWED_METHODS", "[FETCH]"),
        ],
        vec![
            ("IGGY_MCP_HTTP_TLS_ENABLED", "true"),
            ("IGGY_MCP_HTTP_TLS_CERT_FILE", "missing-cert.pem"),
            ("IGGY_MCP_HTTP_TLS_KEY_FILE", "missing-key.pem"),
        ],
        vec![("IGGY_MCP_PERMISSIONS_READ", "sometimes")],
    ] {
        let mut command = Command::new(cargo_bin_path("iggy-mcp").expect("MCP binary required"));
        command
            .env("IGGY_MCP_TRANSPORT", "http")
            .env("IGGY_MCP_HTTP_ADDRESS", "127.0.0.1:0")
            .env("IGGY_MCP_IGGY_ADDRESS", address.to_string())
            .env("IGGY_MCP_IGGY_USERNAME", "iggy")
            .env("IGGY_MCP_IGGY_PASSWORD", "iggy")
            .envs(overrides.iter().copied())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = tokio::process::Command::from(command)
            .kill_on_drop(true)
            .spawn()
            .expect("Failed to start MCP server");
        match timeout(MCP_STDIO_TIMEOUT, child.wait()).await {
            Ok(status) => {
                let status = status.expect("Failed to wait for MCP server");
                if status.code() != Some(1) {
                    accepted.push(format!("{overrides:?} ended with {status}"));
                }
            }
            Err(_) => accepted.push(format!("{overrides:?} kept running")),
        }
    }
    assert!(
        accepted.is_empty(),
        "MCP server must refuse each configuration with an error, not a panic: {accepted:?}"
    );
}

#[iggy_harness]
async fn should_keep_stdio_logs_free_of_call_payloads_by_default(harness: &TestHarness) {
    const USERNAME: &str = "mcp-log-user";
    const PASSWORD: &str = "Zq9-mcp-connection-value";
    const AGENT_PASSWORD: &str = "Xk7-agent-created-value";
    let root = harness.root_client().await.expect("Root client required");
    root.create_user(
        USERNAME,
        PASSWORD,
        UserStatus::Active,
        Some(Permissions::root()),
        &UserCreateOptions::default(),
    )
    .await
    .expect("Failed to create the MCP user");

    let address = harness.server().tcp_addr().expect("TCP address required");
    let mut command = stdio_mcp(address, USERNAME, PASSWORD);
    command
        .env(ADMIN_PERMISSION_ENV, "true")
        .env_remove("RUST_LOG")
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("Failed to start stdio MCP server");
    let mut stdin = child.stdin.take().expect("MCP stdin required");
    let mut lines = BufReader::new(child.stdout.take().expect("MCP stdout required")).lines();
    let mut stderr = child.stderr.take().expect("MCP stderr required");
    let logs = tokio::spawn(async move {
        let mut logs = String::new();
        stderr.read_to_string(&mut logs).await.map(|_| logs)
    });

    let calls = [
        (1, initialize_request()),
        (
            2,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "create_user",
                    "arguments": {"username": "agent-made-user", "password": AGENT_PASSWORD}
                }
            }),
        ),
        (
            3,
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "create_personal_access_token",
                    "arguments": {"name": "log-test-token"}
                }
            }),
        ),
    ];
    let mut replies = Vec::new();
    for (id, request) in calls {
        stdin
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("Failed to write MCP request");
        replies.push(read_reply(&mut lines, id).await);
        if id == 1 {
            let initialized = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
            stdin
                .write_all(format!("{initialized}\n").as_bytes())
                .await
                .expect("Failed to confirm MCP initialization");
        }
    }
    for reply in &replies {
        assert!(
            reply.get("result").is_some() && reply["result"]["isError"] != true,
            "MCP call failed: {reply}"
        );
    }
    let token: RawPersonalAccessToken = serde_json::from_str(
        replies[2]["result"]["content"][0]["text"]
            .as_str()
            .expect("Token reply text"),
    )
    .expect("Token reply JSON");

    drop(stdin);
    timeout(MCP_STDIO_TIMEOUT, child.wait())
        .await
        .expect("MCP server did not exit")
        .expect("Failed to wait for MCP server");
    let logs = logs
        .await
        .expect("Log reader failed")
        .expect("Failed to read MCP logs");
    let found: Vec<&str> = [
        (&PASSWORD[..3], "connection setting"),
        (AGENT_PASSWORD, "tool argument"),
        (token.token.as_str(), "tool result"),
    ]
    .into_iter()
    .filter(|(value, _)| logs.contains(value))
    .map(|(_, what)| what)
    .collect();
    assert!(
        found.is_empty(),
        "MCP stdio logs at the default level hold {found:?}"
    );
}
