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

use futures::future::join_all;
use iggy::prelude::*;
use iggy_common::{ConsumerGroup, TransportProtocol, UserInfo};
use integration::harness::TestHarness;
use std::sync::Arc;
use tokio::sync::Barrier;

const CONCURRENT_CLIENTS: usize = 20;
const USER_PASSWORD: &str = "secret";
const TEST_STREAM_NAME: &str = "race-test-stream";
const TEST_TOPIC_NAME: &str = "race-test-topic";
const PARTITIONS_COUNT: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    User,
    Stream,
    Topic,
    Partition,
    ConsumerGroup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioType {
    Hot,  // Different names - all should succeed
    Cold, // Same name - only first should succeed
}

pub fn barrier_on() -> bool {
    true
}

pub fn barrier_off() -> bool {
    false
}

type OperationResult = Result<(), IggyError>;

async fn create_client_for_transport(
    harness: &TestHarness,
    transport: TransportProtocol,
) -> IggyClient {
    harness
        .root_client_for(transport)
        .await
        .expect("Failed to create client")
}

pub async fn run(
    harness: &TestHarness,
    transport: TransportProtocol,
    resource_type: ResourceType,
    scenario_type: ScenarioType,
    use_barrier: bool,
) {
    let root_client = create_client_for_transport(harness, transport).await;

    // For topic tests, create parent stream first
    if resource_type == ResourceType::Topic {
        root_client.create_stream(TEST_STREAM_NAME).await.unwrap();
    }

    // For partition and consumer group tests, create parent stream and topic first
    if resource_type == ResourceType::Partition || resource_type == ResourceType::ConsumerGroup {
        root_client.create_stream(TEST_STREAM_NAME).await.unwrap();
        let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
        root_client
            .create_topic(
                &stream_id,
                TEST_TOPIC_NAME,
                PARTITIONS_COUNT,
                CompressionAlgorithm::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await
            .unwrap();
    }

    let results = match (resource_type, scenario_type) {
        (ResourceType::User, ScenarioType::Hot) => {
            execute_users_hot(harness, transport, use_barrier).await
        }
        (ResourceType::User, ScenarioType::Cold) => {
            execute_users_cold(harness, transport, use_barrier).await
        }
        (ResourceType::Stream, ScenarioType::Hot) => {
            execute_streams_hot(harness, transport, use_barrier).await
        }
        (ResourceType::Stream, ScenarioType::Cold) => {
            execute_streams_cold(harness, transport, use_barrier).await
        }
        (ResourceType::Topic, ScenarioType::Hot) => {
            execute_topics_hot(harness, transport, use_barrier).await
        }
        (ResourceType::Topic, ScenarioType::Cold) => {
            execute_topics_cold(harness, transport, use_barrier).await
        }
        (ResourceType::Partition, ScenarioType::Hot) => {
            execute_partitions_hot(harness, transport, use_barrier).await
        }
        // Partitions don't have names, so Cold scenario doesn't apply
        (ResourceType::Partition, ScenarioType::Cold) => vec![],
        (ResourceType::ConsumerGroup, ScenarioType::Hot) => {
            execute_consumer_groups_hot(harness, transport, use_barrier).await
        }
        (ResourceType::ConsumerGroup, ScenarioType::Cold) => {
            execute_consumer_groups_cold(harness, transport, use_barrier).await
        }
    };

    if !results.is_empty() {
        validate_results(&results, scenario_type);
        validate_server_state(harness, transport, resource_type, scenario_type).await;
        cleanup_resources(&root_client, resource_type).await;
    }
}

async fn execute_users_hot(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for client_id in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            let username = format!("race-user-{}", client_id);
            client
                .create_user(&username, USER_PASSWORD, UserStatus::Active, None)
                .await
                .map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_users_cold(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    const DUPLICATE_USER: &str = "race-user-duplicate";
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for _ in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            client
                .create_user(DUPLICATE_USER, USER_PASSWORD, UserStatus::Active, None)
                .await
                .map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_streams_hot(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for client_id in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            let stream_name = format!("race-stream-{}", client_id);
            client.create_stream(&stream_name).await.map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_streams_cold(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    const DUPLICATE_STREAM: &str = "race-stream-duplicate";
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for _ in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            client.create_stream(DUPLICATE_STREAM).await.map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_topics_hot(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for client_id in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
            let topic_name = format!("race-topic-{}", client_id);
            client
                .create_topic(
                    &stream_id,
                    &topic_name,
                    PARTITIONS_COUNT,
                    CompressionAlgorithm::default(),
                    None,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await
                .map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_topics_cold(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    const DUPLICATE_TOPIC: &str = "race-topic-duplicate";
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for _ in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
            client
                .create_topic(
                    &stream_id,
                    DUPLICATE_TOPIC,
                    PARTITIONS_COUNT,
                    CompressionAlgorithm::default(),
                    None,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await
                .map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_partitions_hot(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for _ in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
            let topic_id = Identifier::named(TEST_TOPIC_NAME).unwrap();
            client
                .create_partitions(&stream_id, &topic_id, 1)
                .await
                .map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_consumer_groups_hot(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for client_id in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
            let topic_id = Identifier::named(TEST_TOPIC_NAME).unwrap();
            let group_name = format!("race-consumer-group-{}", client_id);
            client
                .create_consumer_group(&stream_id, &topic_id, &group_name)
                .await
                .map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

async fn execute_consumer_groups_cold(
    harness: &TestHarness,
    transport: TransportProtocol,
    use_barrier: bool,
) -> Vec<OperationResult> {
    const DUPLICATE_CONSUMER_GROUP: &str = "race-consumer-group-duplicate";
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(CONCURRENT_CLIENTS)));

    let mut handles = Vec::with_capacity(CONCURRENT_CLIENTS);
    for _ in 0..CONCURRENT_CLIENTS {
        let client = create_client_for_transport(harness, transport).await;
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            if let Some(b) = barrier {
                b.wait().await;
            }
            let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
            let topic_id = Identifier::named(TEST_TOPIC_NAME).unwrap();
            client
                .create_consumer_group(&stream_id, &topic_id, DUPLICATE_CONSUMER_GROUP)
                .await
                .map(|_| ())
        }));
    }

    join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect()
}

fn validate_results(results: &[OperationResult], scenario_type: ScenarioType) {
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let error_count = results.iter().filter(|r| r.is_err()).count();

    match scenario_type {
        ScenarioType::Hot => {
            // Hot path: all operations should succeed
            assert_eq!(
                success_count,
                CONCURRENT_CLIENTS,
                "Hot path: Expected all {} operations to succeed, but only {} succeeded. Errors: {:?}",
                CONCURRENT_CLIENTS,
                success_count,
                results.iter().filter(|r| r.is_err()).collect::<Vec<_>>()
            );
            assert_eq!(
                error_count, 0,
                "Hot path: Expected 0 errors, but got {}",
                error_count
            );
        }
        ScenarioType::Cold => {
            // Cold path: exactly 1 success, rest failures
            assert_eq!(
                success_count, 1,
                "Cold path: Expected exactly 1 success, but got {}. All results: {:?}",
                success_count, results
            );
            assert_eq!(
                error_count,
                CONCURRENT_CLIENTS - 1,
                "Cold path: Expected {} errors, but got {}",
                CONCURRENT_CLIENTS - 1,
                error_count
            );

            // Verify errors are appropriate "already exists" errors
            for result in results.iter().filter(|r| r.is_err()) {
                let err = result.as_ref().unwrap_err();
                assert!(
                    matches!(
                        err,
                        IggyError::UserAlreadyExists
                            | IggyError::StreamNameAlreadyExists(_)
                            | IggyError::TopicNameAlreadyExists(_, _)
                            | IggyError::ConsumerGroupNameAlreadyExists(_, _)
                            | IggyError::HttpResponseError(400, _)
                    ),
                    "Expected 'already exists' error, got: {:?}",
                    err
                );
            }
        }
    }
}

async fn validate_server_state(
    harness: &TestHarness,
    transport: TransportProtocol,
    resource_type: ResourceType,
    scenario_type: ScenarioType,
) {
    const VALIDATION_CLIENT_COUNT: usize = 5;
    let mut clients = Vec::with_capacity(VALIDATION_CLIENT_COUNT);

    // Create multiple clients for validation
    for _ in 0..VALIDATION_CLIENT_COUNT {
        let client = create_client_for_transport(harness, transport).await;
        clients.push(client);
    }

    // Collect state from all clients
    match resource_type {
        ResourceType::User => {
            let mut user_states = Vec::new();
            for client in &clients {
                let state = validate_users_state(client, scenario_type).await;
                user_states.push(state);
            }

            // Verify all clients see the same state
            let first_state = &user_states[0];
            for (i, state) in user_states.iter().enumerate().skip(1) {
                assert_eq!(
                    state.len(),
                    first_state.len(),
                    "Client {} sees different number of users ({}) than client 0 ({})",
                    i,
                    state.len(),
                    first_state.len()
                );

                // Sort and compare usernames to ensure same set
                let mut first_usernames: Vec<_> = first_state.iter().map(|u| &u.username).collect();
                let mut current_usernames: Vec<_> = state.iter().map(|u| &u.username).collect();
                first_usernames.sort();
                current_usernames.sort();

                assert_eq!(
                    current_usernames, first_usernames,
                    "Client {} sees different users than client 0",
                    i
                );
            }
        }
        ResourceType::Stream => {
            let mut stream_states = Vec::new();
            for client in &clients {
                let state = validate_streams_state(client, scenario_type).await;
                stream_states.push(state);
            }

            // Verify all clients see the same state
            let first_state = &stream_states[0];
            for (i, state) in stream_states.iter().enumerate().skip(1) {
                assert_eq!(
                    state.len(),
                    first_state.len(),
                    "Client {} sees different number of streams ({}) than client 0 ({})",
                    i,
                    state.len(),
                    first_state.len()
                );

                // Sort and compare stream names
                let mut first_names: Vec<_> = first_state.iter().map(|s| &s.name).collect();
                let mut current_names: Vec<_> = state.iter().map(|s| &s.name).collect();
                first_names.sort();
                current_names.sort();

                assert_eq!(
                    current_names, first_names,
                    "Client {} sees different streams than client 0",
                    i
                );
            }
        }
        ResourceType::Topic => {
            let mut topic_states = Vec::new();
            for client in &clients {
                let state = validate_topics_state(client, scenario_type).await;
                topic_states.push(state);
            }

            // Verify all clients see the same state
            let first_state = &topic_states[0];
            for (i, state) in topic_states.iter().enumerate().skip(1) {
                assert_eq!(
                    state.len(),
                    first_state.len(),
                    "Client {} sees different number of topics ({}) than client 0 ({})",
                    i,
                    state.len(),
                    first_state.len()
                );

                // Sort and compare topic names
                let mut first_names: Vec<_> = first_state.iter().map(|t| &t.name).collect();
                let mut current_names: Vec<_> = state.iter().map(|t| &t.name).collect();
                first_names.sort();
                current_names.sort();

                assert_eq!(
                    current_names, first_names,
                    "Client {} sees different topics than client 0",
                    i
                );
            }
        }
        ResourceType::Partition => {
            let mut partition_counts = Vec::new();
            for client in &clients {
                let count = validate_partitions_state(client).await;
                partition_counts.push(count);
            }

            let first_count = partition_counts[0];
            for (i, count) in partition_counts.iter().enumerate().skip(1) {
                assert_eq!(
                    *count, first_count,
                    "Client {} sees different partition count ({}) than client 0 ({})",
                    i, count, first_count
                );
            }
        }
        ResourceType::ConsumerGroup => {
            let mut group_states = Vec::new();
            for client in &clients {
                let state = validate_consumer_groups_state(client, scenario_type).await;
                group_states.push(state);
            }

            let first_state = &group_states[0];
            for (i, state) in group_states.iter().enumerate().skip(1) {
                assert_eq!(
                    state.len(),
                    first_state.len(),
                    "Client {} sees different number of consumer groups ({}) than client 0 ({})",
                    i,
                    state.len(),
                    first_state.len()
                );

                let mut first_names: Vec<_> = first_state.iter().map(|g| &g.name).collect();
                let mut current_names: Vec<_> = state.iter().map(|g| &g.name).collect();
                first_names.sort();
                current_names.sort();

                assert_eq!(
                    current_names, first_names,
                    "Client {} sees different consumer groups than client 0",
                    i
                );
            }
        }
    }
}

async fn validate_users_state(client: &IggyClient, scenario_type: ScenarioType) -> Vec<UserInfo> {
    let users = client.get_users().await.expect("Failed to get users");
    let test_users: Vec<_> = users
        .into_iter()
        .filter(|u| u.username.starts_with("race-user-"))
        .collect();

    match scenario_type {
        ScenarioType::Hot => {
            // Hot path: should have CONCURRENT_CLIENTS unique users
            assert_eq!(
                test_users.len(),
                CONCURRENT_CLIENTS,
                "Hot path: Expected {} users, but found {}. Users: {:?}",
                CONCURRENT_CLIENTS,
                test_users.len(),
                test_users.iter().map(|u| &u.username).collect::<Vec<_>>()
            );

            // All users should have unique IDs
            let mut ids: Vec<u32> = test_users.iter().map(|u| u.id).collect();
            ids.sort_unstable();
            let unique_ids: std::collections::HashSet<u32> = ids.iter().copied().collect();
            assert_eq!(
                unique_ids.len(),
                test_users.len(),
                "Hot path: Found duplicate user IDs: {:?}",
                ids
            );

            // All users should have unique names
            let names: std::collections::HashSet<&str> =
                test_users.iter().map(|u| u.username.as_str()).collect();
            assert_eq!(
                names.len(),
                test_users.len(),
                "Hot path: Found duplicate usernames"
            );
        }
        ScenarioType::Cold => {
            // Cold path: should have exactly 1 user with duplicate name
            assert_eq!(
                test_users.len(),
                1,
                "Cold path: Expected exactly 1 user, but found {}. Users: {:?}",
                test_users.len(),
                test_users.iter().map(|u| &u.username).collect::<Vec<_>>()
            );

            let user = &test_users[0];
            assert_eq!(
                user.username, "race-user-duplicate",
                "Cold path: Expected user named 'race-user-duplicate', found '{}'",
                user.username
            );
        }
    }

    test_users
}

async fn validate_streams_state(client: &IggyClient, scenario_type: ScenarioType) -> Vec<Stream> {
    let streams = client.get_streams().await.expect("Failed to get streams");
    let test_streams: Vec<_> = streams
        .into_iter()
        .filter(|s| s.name.starts_with("race-stream-"))
        .collect();

    match scenario_type {
        ScenarioType::Hot => {
            // Hot path: should have CONCURRENT_CLIENTS unique streams
            assert_eq!(
                test_streams.len(),
                CONCURRENT_CLIENTS,
                "Hot path: Expected {} streams, but found {}. Streams: {:?}",
                CONCURRENT_CLIENTS,
                test_streams.len(),
                test_streams.iter().map(|s| &s.name).collect::<Vec<_>>()
            );

            // All streams should have unique IDs
            let mut ids: Vec<u32> = test_streams.iter().map(|s| s.id).collect();
            ids.sort_unstable();
            let unique_ids: std::collections::HashSet<u32> = ids.iter().copied().collect();
            assert_eq!(
                unique_ids.len(),
                test_streams.len(),
                "Hot path: Found duplicate stream IDs: {:?}",
                ids
            );

            // All streams should have unique names
            let names: std::collections::HashSet<&str> =
                test_streams.iter().map(|s| s.name.as_str()).collect();
            assert_eq!(
                names.len(),
                test_streams.len(),
                "Hot path: Found duplicate stream names"
            );
        }
        ScenarioType::Cold => {
            // Cold path: should have exactly 1 stream with duplicate name
            assert_eq!(
                test_streams.len(),
                1,
                "Cold path: Expected exactly 1 stream, but found {}. Streams: {:?}",
                test_streams.len(),
                test_streams.iter().map(|s| &s.name).collect::<Vec<_>>()
            );

            let stream = &test_streams[0];
            assert_eq!(
                stream.name, "race-stream-duplicate",
                "Cold path: Expected stream named 'race-stream-duplicate', found '{}'",
                stream.name
            );
        }
    }

    test_streams
}

async fn validate_topics_state(client: &IggyClient, scenario_type: ScenarioType) -> Vec<Topic> {
    let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
    let stream = client
        .get_stream(&stream_id)
        .await
        .expect("Failed to get test stream")
        .expect("Test stream not found");

    let test_topics: Vec<_> = stream
        .topics
        .into_iter()
        .filter(|t| t.name.starts_with("race-topic-"))
        .collect();

    match scenario_type {
        ScenarioType::Hot => {
            // Hot path: should have CONCURRENT_CLIENTS unique topics
            assert_eq!(
                test_topics.len(),
                CONCURRENT_CLIENTS,
                "Hot path: Expected {} topics, but found {}. Topics: {:?}",
                CONCURRENT_CLIENTS,
                test_topics.len(),
                test_topics.iter().map(|t| &t.name).collect::<Vec<_>>()
            );

            // All topics should have unique IDs
            let mut ids: Vec<u32> = test_topics.iter().map(|t| t.id).collect();
            ids.sort_unstable();
            let unique_ids: std::collections::HashSet<u32> = ids.iter().copied().collect();
            assert_eq!(
                unique_ids.len(),
                test_topics.len(),
                "Hot path: Found duplicate topic IDs: {:?}",
                ids
            );

            // All topics should have unique names
            let names: std::collections::HashSet<&str> =
                test_topics.iter().map(|t| t.name.as_str()).collect();
            assert_eq!(
                names.len(),
                test_topics.len(),
                "Hot path: Found duplicate topic names"
            );
        }
        ScenarioType::Cold => {
            // Cold path: should have exactly 1 topic with duplicate name
            assert_eq!(
                test_topics.len(),
                1,
                "Cold path: Expected exactly 1 topic, but found {}. Topics: {:?}",
                test_topics.len(),
                test_topics.iter().map(|t| &t.name).collect::<Vec<_>>()
            );

            let topic = &test_topics[0];
            assert_eq!(
                topic.name, "race-topic-duplicate",
                "Cold path: Expected topic named 'race-topic-duplicate', found '{}'",
                topic.name
            );
        }
    }

    test_topics
}

async fn validate_partitions_state(client: &IggyClient) -> u32 {
    let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TEST_TOPIC_NAME).unwrap();
    let topic = client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("Failed to get test topic")
        .expect("Test topic not found");

    // Expected: initial partition + CONCURRENT_CLIENTS added partitions (1 each)
    let expected_partitions = PARTITIONS_COUNT + CONCURRENT_CLIENTS as u32;
    assert_eq!(
        topic.partitions_count, expected_partitions,
        "Expected {} partitions (1 initial + {} added), but found {}",
        expected_partitions, CONCURRENT_CLIENTS, topic.partitions_count
    );

    topic.partitions_count
}

async fn validate_consumer_groups_state(
    client: &IggyClient,
    scenario_type: ScenarioType,
) -> Vec<ConsumerGroup> {
    let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TEST_TOPIC_NAME).unwrap();
    let groups = client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("Failed to get consumer groups");
    let test_groups: Vec<_> = groups
        .into_iter()
        .filter(|g| g.name.starts_with("race-consumer-group-"))
        .collect();

    match scenario_type {
        ScenarioType::Hot => {
            assert_eq!(
                test_groups.len(),
                CONCURRENT_CLIENTS,
                "Hot path: Expected {} consumer groups, but found {}. Groups: {:?}",
                CONCURRENT_CLIENTS,
                test_groups.len(),
                test_groups.iter().map(|g| &g.name).collect::<Vec<_>>()
            );

            let mut ids: Vec<u32> = test_groups.iter().map(|g| g.id).collect();
            ids.sort_unstable();
            let unique_ids: std::collections::HashSet<u32> = ids.iter().copied().collect();
            assert_eq!(
                unique_ids.len(),
                test_groups.len(),
                "Hot path: Found duplicate consumer group IDs: {:?}",
                ids
            );

            let names: std::collections::HashSet<&str> =
                test_groups.iter().map(|g| g.name.as_str()).collect();
            assert_eq!(
                names.len(),
                test_groups.len(),
                "Hot path: Found duplicate consumer group names"
            );
        }
        ScenarioType::Cold => {
            assert_eq!(
                test_groups.len(),
                1,
                "Cold path: Expected exactly 1 consumer group, but found {}. Groups: {:?}",
                test_groups.len(),
                test_groups.iter().map(|g| &g.name).collect::<Vec<_>>()
            );

            let group = &test_groups[0];
            assert_eq!(
                group.name, "race-consumer-group-duplicate",
                "Cold path: Expected consumer group named 'race-consumer-group-duplicate', found '{}'",
                group.name
            );
        }
    }

    test_groups
}

async fn cleanup_resources(client: &IggyClient, resource_type: ResourceType) {
    match resource_type {
        ResourceType::User => {
            let users = client.get_users().await.unwrap();
            for user in users {
                if user.username.starts_with("race-user-") {
                    let _ = client
                        .delete_user(&Identifier::numeric(user.id).unwrap())
                        .await;
                }
            }
        }
        ResourceType::Stream => {
            let streams = client.get_streams().await.unwrap();
            for stream in streams {
                if stream.name.starts_with("race-stream-") {
                    let _ = client
                        .delete_stream(&Identifier::numeric(stream.id).unwrap())
                        .await;
                }
            }
        }
        // Just delete test stream (also cleans up topics, partitions, and consumer groups)
        ResourceType::Topic | ResourceType::Partition | ResourceType::ConsumerGroup => {
            let _ = client
                .delete_stream(&Identifier::named(TEST_STREAM_NAME).unwrap())
                .await;
        }
    }
}
