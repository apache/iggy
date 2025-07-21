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
use iggy_binary_protocol::{MessageClient, StreamClient, TopicClient};
use iggy_common::{
    Identifier, IggyExpiry, IggyMessage, MaxTopicSize, Partitioning, Stream, StreamDetails, Topic,
    TopicDetails,
};
use integration::{
    test_mcp_server::{McpClient, TestMcpServer},
    test_server::TestServer,
};
use lazy_static::lazy_static;
use rmcp::{
    ServiceError,
    model::{CallToolRequestParam, CallToolResult, ListToolsResult},
    serde::de::DeserializeOwned,
    serde_json::{self, json},
};

const STREAM_NAME: &str = "test_stream";
const TOPIC_NAME: &str = "test_topic";
const MESSAGE_PAYLOAD: &str = "test_message";

lazy_static! {
    static ref STREAM_ID: Identifier =
        Identifier::from_str_value(STREAM_NAME).expect("Failed to create stream ID");
    static ref TOPIC_ID: Identifier =
        Identifier::from_str_value(TOPIC_NAME).expect("Failed to create topic ID");
}

#[tokio::test]
async fn mcp_server_should_list_tools() {
    let infra = setup().await;
    let client = infra.mcp_client;
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
    assert_response::<Vec<Stream>>("get_streams", None, |streams| {
        assert_eq!(streams.len(), 1);
        let stream = &streams[0];
        assert_eq!(&stream.name, STREAM_NAME);
        assert_eq!(&stream.topics_count, &1);
    })
    .await;
}

#[tokio::test]
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
async fn mcp_server_should_return_list_of_topcics() {
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
async fn mcp_server_should_return_topic_details() {
    assert_response::<TopicDetails>(
        "get_topic",
        Some(json!({"stream_id": STREAM_NAME, "topic_id": TOPIC_NAME})),
        |topic| {
            assert_eq!(topic.id, 1);
            assert_eq!(topic.name, TOPIC_NAME);
            assert_eq!(topic.messages_count, 1);
        },
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
        .create_stream(STREAM_NAME, None)
        .await
        .expect("Failed to create stream");

    iggy_client
        .create_topic(
            &STREAM_ID,
            TOPIC_NAME,
            1,
            iggy_common::CompressionAlgorithm::None,
            None,
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
            &Partitioning::partition_id(1),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");
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
