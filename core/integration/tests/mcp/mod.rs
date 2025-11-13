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

use iggy::prelude::{Client, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, IggyClient};
use iggy_binary_protocol::{
    ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PersonalAccessTokenClient,
    StreamClient, TopicClient, UserClient,
};
use iggy_common::{
    ClientInfo, ClientInfoDetails, Consumer, ConsumerGroup, ConsumerGroupDetails,
    ConsumerOffsetInfo, Identifier, IggyExpiry, IggyMessage, MaxTopicSize, Partitioning,
    PersonalAccessTokenExpiry, PersonalAccessTokenInfo, PolledMessages, RawPersonalAccessToken,
    Snapshot, Stats, Stream, StreamDetails, Topic, TopicDetails, UserInfo, UserInfoDetails,
    UserStatus,
};
use integration::{
    test_mcp_server::{CONSUMER_NAME, McpClient, TestMcpServer},
    test_server::{IpAddrKind, TestServer},
};
use lazy_static::lazy_static;
use rmcp::{
    ServiceError,
    model::{CallToolRequestParam, CallToolResult, ListToolsResult},
    serde::de::DeserializeOwned,
    serde_json::{self, json},
};
use serial_test::parallel;
use std::collections::HashMap;

const STREAM_NAME: &str = "test_stream";
const TOPIC_NAME: &str = "test_topic";
const MESSAGE_PAYLOAD: &str = "test_message";
const CONSUMER_GROUP_NAME: &str = "test_consumer_group";
const PERSONAL_ACCESS_TOKEN_NAME: &str = "test_personal_access_token";
const USER_NAME: &str = "test_user";
const USER_PASSWORD: &str = "secret";

lazy_static! {
    static ref STREAM_ID: Identifier =
        Identifier::from_str_value(STREAM_NAME).expect("Failed to create stream ID");
    static ref TOPIC_ID: Identifier =
        Identifier::from_str_value(TOPIC_NAME).expect("Failed to create topic ID");
    static ref CONSUMER_GROUP_ID: Identifier =
        Identifier::from_str_value(CONSUMER_GROUP_NAME).expect("Failed to create group ID");
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_list_tools() {
    let infra = setup().await;
    let client = infra.mcp_client;
    let tools = client.list_tools().await.expect("Failed to list tools");

    assert!(!tools.tools.is_empty());
    let tools_count = tools.tools.len();
    assert_eq!(tools_count, 40);
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_handle_ping() {
    assert_empty_response("ping", None).await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_list_of_streams() {
    assert_response::<Vec<Stream>>("get_streams", None, |streams| {
        assert_eq!(streams.len(), 1);
        let stream = &streams[0];
        assert_eq!(&stream.name, STREAM_NAME);
        assert_eq!(&stream.topics_count, &1);
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_stream_details() {
    assert_response::<StreamDetails>(
        "get_stream",
        Some(json!({"stream_id": STREAM_NAME})),
        |stream| {
            assert_eq!(stream.name, STREAM_NAME);
            assert_eq!(stream.topics_count, 1);
            assert_eq!(stream.messages_count, 1);
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_create_stream() {
    let name = "new_stream";
    assert_response::<StreamDetails>("create_stream", Some(json!({ "name": name})), |stream| {
        assert_eq!(stream.name, name);
        assert_eq!(stream.topics_count, 0);
        assert_eq!(stream.messages_count, 0);
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_update_stream() {
    let name = "updated_stream";
    assert_empty_response(
        "update_stream",
        Some(json!({"stream_id": STREAM_NAME, "name": name})),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_stream() {
    assert_empty_response("delete_stream", Some(json!({"stream_id": STREAM_NAME}))).await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_purge_stream() {
    assert_empty_response("purge_stream", Some(json!({"stream_id": STREAM_NAME}))).await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_list_of_topics() {
    assert_response::<Vec<Topic>>(
        "get_topics",
        Some(json!({"stream_id": STREAM_NAME})),
        |topics| {
            assert_eq!(topics.len(), 1);
            let topic = &topics[0];
            assert_eq!(topic.name, TOPIC_NAME);
            assert_eq!(topic.partitions_count, 1);
            assert_eq!(topic.messages_count, 1);
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_topic_details() {
    assert_response::<TopicDetails>(
        "get_topic",
        Some(json!({"stream_id": STREAM_NAME, "topic_id": TOPIC_NAME})),
        |topic| {
            assert_eq!(topic.id, 0);
            assert_eq!(topic.name, TOPIC_NAME);
            assert_eq!(topic.messages_count, 1);
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_create_topic() {
    let name = "new_topic";
    assert_response::<TopicDetails>(
        "create_topic",
        Some(json!({ "stream_id": STREAM_NAME, "name": name, "partitions_count": 1})),
        |topic| {
            assert_eq!(topic.id, 1);
            assert_eq!(topic.name, name);
            assert_eq!(topic.partitions_count, 1);
            assert_eq!(topic.messages_count, 0);
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_update_topic() {
    let name = "updated_topic";
    assert_empty_response(
        "update_topic",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "name": name})),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_topic() {
    assert_empty_response(
        "delete_topic",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_purge_topic() {
    assert_empty_response(
        "purge_topic",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_create_partitions() {
    assert_empty_response(
        "create_partitions",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partitions_count": 3 })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_partitions() {
    assert_empty_response(
        "delete_partitions",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partitions_count": 1 })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_segments() {
    assert_empty_response(
        "delete_segments",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partition_id": 0, "segments_count": 1 })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_poll_messages() {
    assert_response::<PolledMessages>(
        "poll_messages",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partition_id": 0, "offset": 0 })),
        |messages| {
            assert_eq!(messages.messages.len(), 1);
            let message = &messages.messages[0];
            assert_eq!(message.header.offset, 0);
            let payload = message.payload_as_string().expect("Failed to parse message payload");
            assert_eq!(payload, MESSAGE_PAYLOAD);
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_send_messages() {
    assert_empty_response(
        "send_messages",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partition_id": 0, "messages": [
            {
                "payload": "test"
            }
        ] })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_stats() {
    assert_response::<Stats>("get_stats", None, |stats| {
        assert!(!stats.hostname.is_empty());
        assert_eq!(stats.messages_count, 1);
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_me() {
    assert_response::<ClientInfoDetails>("get_me", None, |client| {
        assert!(client.client_id > 0);
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_clients() {
    assert_response::<Vec<ClientInfo>>("get_clients", None, |clients| {
        assert!(!clients.is_empty());
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_handle_snapshot() {
    assert_response::<Snapshot>("snapshot", None, |snapshot| {
        assert!(!snapshot.0.is_empty());
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_consumer_groups() {
    assert_response::<Vec<ConsumerGroup>>(
        "get_consumer_groups",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME})),
        |groups| {
            assert!(!groups.is_empty());
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_consumer_group_details() {
    assert_response::<ConsumerGroupDetails>("get_consumer_group", Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "group_id": CONSUMER_GROUP_NAME })), |group| {
        assert_eq!(group.name, CONSUMER_GROUP_NAME);
        assert_eq!(group.partitions_count, 1);
        assert_eq!(group.members_count, 0);
        assert!(group.members.is_empty());

    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_create_consumer_group() {
    let name = "test";
    assert_response::<ConsumerGroupDetails>(
        "create_consumer_group",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "name": name })),
        |group| {
            assert_eq!(group.name, name);
            assert_eq!(group.partitions_count, 1);
            assert_eq!(group.members_count, 0);
            assert!(group.members.is_empty());
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_consumer_group() {
    assert_empty_response(
        "delete_consumer_group",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "group_id": CONSUMER_GROUP_NAME })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_consumer_offset() {
    assert_response::<Option<ConsumerOffsetInfo>>(
        "get_consumer_offset",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partition_id": 0 })),
        |offset| {
            assert!(offset.is_some());
            let offset = offset.unwrap();
            assert_eq!(offset.partition_id, 0);
            assert_eq!(offset.stored_offset, 0);
            assert_eq!(offset.current_offset, 0);
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_store_consumer_offset() {
    assert_empty_response(
        "store_consumer_offset",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partition_id": 0, "offset": 0 })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_consumer_offset() {
    assert_empty_response(
        "delete_consumer_offset",
        Some(json!({ "stream_id": STREAM_NAME, "topic_id": TOPIC_NAME, "partition_id": 0, "offset": 0 })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_personal_access_tokens() {
    assert_response::<Vec<PersonalAccessTokenInfo>>("get_personal_access_tokens", None, |tokens| {
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].name, PERSONAL_ACCESS_TOKEN_NAME);
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_create_personal_access_token() {
    let name = "test_token";
    let expiry = PersonalAccessTokenExpiry::NeverExpire.to_string();
    assert_response::<RawPersonalAccessToken>(
        "create_personal_access_token",
        Some(json!({ "name": name, "expiry": expiry  })),
        |token| {
            assert!(!token.token.is_empty());
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_personal_access_token() {
    assert_empty_response(
        "delete_personal_access_token",
        Some(json!({ "name": PERSONAL_ACCESS_TOKEN_NAME})),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_users() {
    assert_response::<Vec<UserInfo>>("get_users", None, |users| {
        assert_eq!(users.len(), 2);
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_return_user_details() {
    assert_response::<UserInfoDetails>("get_user", Some(json!({ "user_id": USER_NAME})), |user| {
        assert_eq!(user.username, USER_NAME);
    })
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_create_user() {
    let username = "test-mcp-user";
    assert_response::<UserInfoDetails>(
        "create_user",
        Some(json!({ "username": username, "password": "secret"})),
        |user| {
            assert_eq!(user.username, username);
        },
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_update_user() {
    assert_empty_response(
        "update_user",
        Some(json!({ "user_id": USER_NAME, "username": "test-mcp-user", "active": false})),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_delete_user() {
    assert_empty_response("delete_user", Some(json!({ "user_id": USER_NAME}))).await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_update_permissions() {
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

    assert_empty_response(
        "update_permissions",
        Some(json!({ "user_id": USER_NAME, "permissions": permissions })),
    )
    .await;
}

#[tokio::test]
#[parallel]
async fn mcp_server_should_change_password() {
    assert_empty_response(
        "change_password",
        Some(json!({ "user_id": USER_NAME, "current_password": USER_PASSWORD, "new_password": "secret2"})),
    )
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
    let client = infra.mcp_client;
    let result = invoke_request(&client, method, data).await;
    assert_response(result)
}

async fn invoke_request<T: DeserializeOwned>(
    client: &TestMcpClient,
    method: &str,
    data: Option<serde_json::Value>,
) -> T {
    let error_message = format!("Failed to invoke MCP method: {method}",);
    let mut result = client.invoke(method, data).await.expect(&error_message);
    let result = result.content.remove(0);
    let Some(text) = result.as_text() else {
        panic!("Expected text response for MCP method: {method}");
    };

    serde_json::from_str::<T>(&text.text).expect("Failed to parse JSON")
}

async fn setup() -> McpInfra {
    let mut iggy_envs = HashMap::new();
    iggy_envs.insert("IGGY_QUIC_ENABLED".to_owned(), "false".to_owned());
    let mut test_server = TestServer::new(Some(iggy_envs), true, None, IpAddrKind::V4);
    test_server.start();
    let iggy_server_address = test_server
        .get_raw_tcp_addr()
        .expect("Failed to get Iggy TCP address");
    seed_data(&iggy_server_address).await;

    let mut test_mcp_server = TestMcpServer::with_iggy_address(&iggy_server_address);
    test_mcp_server.start();
    test_mcp_server.ensure_started().await;
    let mcp_client = test_mcp_server.get_client().await;

    McpInfra {
        _iggy_server: test_server,
        _mcp_server: test_mcp_server,
        mcp_client: TestMcpClient { mcp_client },
    }
}

async fn seed_data(iggy_server_address: &str) {
    let iggy_port = iggy_server_address
        .split(':')
        .next_back()
        .unwrap()
        .parse::<u16>()
        .unwrap();

    let iggy_client = IggyClient::from_connection_string(&format!(
        "iggy://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@localhost:{iggy_port}"
    ))
    .expect("Failed to create Iggy client");

    iggy_client
        .connect()
        .await
        .expect("Failed to initialize Iggy client");

    iggy_client
        .create_stream(STREAM_NAME)
        .await
        .expect("Failed to create stream");

    iggy_client
        .create_topic(
            &STREAM_ID,
            TOPIC_NAME,
            1,
            iggy_common::CompressionAlgorithm::None,
            None,
            IggyExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("Failed to create topic");

    let mut messages = vec![
        IggyMessage::builder()
            .payload(MESSAGE_PAYLOAD.into())
            .build()
            .expect("Failed to build message"),
    ];

    iggy_client
        .send_messages(
            &STREAM_ID,
            &TOPIC_ID,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    let consumer =
        Consumer::new(Identifier::named(CONSUMER_NAME).expect("Failed to create consumer"));

    iggy_client
        .store_consumer_offset(&consumer, &STREAM_ID, &TOPIC_ID, Some(0), 0)
        .await
        .expect("Failed to store consumer offset");

    iggy_client
        .create_consumer_group(&STREAM_ID, &TOPIC_ID, CONSUMER_GROUP_NAME)
        .await
        .expect("Failed to create consumer group");

    iggy_client
        .create_user(USER_NAME, USER_PASSWORD, UserStatus::Active, None)
        .await
        .expect("Failed to create user");

    iggy_client
        .create_personal_access_token(
            PERSONAL_ACCESS_TOKEN_NAME,
            PersonalAccessTokenExpiry::NeverExpire,
        )
        .await
        .expect("Failed to create personal access token");
}

#[derive(Debug)]
struct McpInfra {
    _iggy_server: TestServer,
    _mcp_server: TestMcpServer,
    mcp_client: TestMcpClient,
}

#[derive(Debug)]
struct TestMcpClient {
    mcp_client: McpClient,
}

impl TestMcpClient {
    pub async fn list_tools(&self) -> Result<ListToolsResult, ServiceError> {
        self.mcp_client.list_tools(Default::default()).await
    }

    pub async fn invoke(
        &self,
        method: &str,
        data: Option<serde_json::Value>,
    ) -> Result<CallToolResult, ServiceError> {
        self.mcp_client
            .call_tool(CallToolRequestParam {
                name: method.to_owned().into(),
                arguments: data.and_then(|value| value.as_object().cloned()),
            })
            .await
    }
}
