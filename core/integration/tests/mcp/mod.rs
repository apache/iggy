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

use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, ConsumerGroup, ConsumerGroupDetails,
    ConsumerOffsetInfo, PersonalAccessTokenExpiry, PersonalAccessTokenInfo, PolledMessages,
    RawPersonalAccessToken, Snapshot, Stats, Stream, StreamDetails, Topic, TopicDetails, UserInfo,
    UserInfoDetails,
};
use integration::{
    harness::{McpClient, seeds},
    iggy_harness,
};
use rmcp::{
    model::CallToolRequestParams,
    serde::de::DeserializeOwned,
    serde_json::{self, json},
};

async fn invoke<T: DeserializeOwned>(
    client: &McpClient,
    method: &str,
    data: Option<serde_json::Value>,
) -> T {
    let mut result = client
        .call_tool(CallToolRequestParams {
            name: method.to_owned().into(),
            arguments: data.and_then(|v| v.as_object().cloned()),
            task: None,
            meta: None,
        })
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
    let result = client
        .call_tool(CallToolRequestParams {
            name: method.to_owned().into(),
            arguments: data.and_then(|v| v.as_object().cloned()),
            task: None,
            meta: None,
        })
        .await
        .unwrap_or_else(|e| panic!("Failed to invoke {method}: {e}"));

    assert!(!result.is_error.unwrap_or(false), "{method} returned error");
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

#[iggy_harness(server(cluster.enabled = true, mcp))]
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
async fn should_purge_stream(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "purge_stream",
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
async fn should_purge_topic(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "purge_topic",
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
    invoke_empty(
        &mcp_client,
        "send_messages",
        Some(json!({
            "stream_id": seeds::names::STREAM,
            "topic_id": seeds::names::TOPIC,
            "partition_id": 0,
            "messages": [{"payload": "test"}]
        })),
    )
    .await;
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

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_handle_snapshot(harness: &TestHarness) {
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

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_personal_access_tokens(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let tokens: Vec<PersonalAccessTokenInfo> =
        invoke(&mcp_client, "get_personal_access_tokens", None).await;

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].name, seeds::names::PERSONAL_ACCESS_TOKEN);
}

#[iggy_harness(server(mcp))]
async fn should_create_personal_access_token(harness: &TestHarness) {
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

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_personal_access_token(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_personal_access_token",
        Some(json!({"name": seeds::names::PERSONAL_ACCESS_TOKEN})),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_users(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let users: Vec<UserInfo> = invoke(&mcp_client, "get_users", None).await;

    assert_eq!(users.len(), 2);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_return_user_details(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let user: UserInfoDetails = invoke(
        &mcp_client,
        "get_user",
        Some(json!({"user_id": seeds::names::USER})),
    )
    .await;

    assert_eq!(user.username, seeds::names::USER);
}

#[iggy_harness(server(mcp))]
async fn should_create_user(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let username = "test-mcp-user";
    let user: UserInfoDetails = invoke(
        &mcp_client,
        "create_user",
        Some(json!({"username": username, "password": "secret"})),
    )
    .await;

    assert_eq!(user.username, username);
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_update_user(harness: &TestHarness) {
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

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_delete_user(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    invoke_empty(
        &mcp_client,
        "delete_user",
        Some(json!({"user_id": seeds::names::USER})),
    )
    .await;
}

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_update_permissions(harness: &TestHarness) {
    let mcp_client = harness.mcp_client().await.expect("MCP client required");
    let permissions = json!({
        "global": {
            "manage_servers": true,
            "read_users": true,
        },
        "streams": {
            "1": {
                "manage_stream": true,
                "manage_topics": true,
                "topics": {
                    "1": {
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

#[iggy_harness(server(mcp), seed = seeds::mcp_standard)]
async fn should_change_password(harness: &TestHarness) {
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
