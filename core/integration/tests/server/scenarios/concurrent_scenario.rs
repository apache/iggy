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

use crate::server::scenarios::create_client;
use futures::future::join_all;
use iggy::prelude::*;
use iggy_common::UserInfo;
use integration::test_server::{ClientFactory, login_root};
use std::sync::Arc;
use tokio::sync::Barrier;

const OPERATIONS_COUNT: usize = 40;
const MULTIPLE_CLIENT_COUNT: usize = 10;
const OPERATIONS_PER_CLIENT: usize = OPERATIONS_COUNT / MULTIPLE_CLIENT_COUNT;
const USER_PASSWORD: &str = "secret";
const TEST_STREAM_NAME: &str = "race-test-stream";
const PARTITIONS_COUNT: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    User,
    Stream,
    Topic,
    // TODO(hubcio): add ConsumerGroup
    // TODO(hubcio): add Partition
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

pub async fn run(
    client_factory: &dyn ClientFactory,
    resource_type: ResourceType,
    scenario_type: ScenarioType,
    use_barrier: bool,
) {
    let root_client = create_client(client_factory).await;
    login_root(&root_client).await;

    // For topic tests, create parent stream first
    if resource_type == ResourceType::Topic {
        root_client.create_stream(TEST_STREAM_NAME).await.unwrap();
    }

    let results = match (resource_type, scenario_type) {
        (ResourceType::User, ScenarioType::Cold) => {
            execute_multiple_clients_users_cold(client_factory, use_barrier).await
        }
        (ResourceType::Stream, ScenarioType::Hot) => {
            execute_multiple_clients_streams_hot(client_factory, use_barrier).await
        }
        (ResourceType::Stream, ScenarioType::Cold) => {
            execute_multiple_clients_streams_cold(client_factory, use_barrier).await
        }
        (ResourceType::Topic, ScenarioType::Hot) => {
            execute_multiple_clients_topics_hot(client_factory, use_barrier).await
        }
        (ResourceType::Topic, ScenarioType::Cold) => {
            execute_multiple_clients_topics_cold(client_factory, use_barrier).await
        }
        _ => vec![],
        // TODO: Figure out why those tests timeout in CI.
        /*
        (ResourceType::User, ScenarioType::Hot) => {
            execute_multiple_clients_users_hot(client_factory, use_barrier).await
        }
        */
    };

    if !results.is_empty() {
        validate_results(&results, scenario_type);
        validate_server_state(client_factory, resource_type, scenario_type).await;
        cleanup_resources(&root_client, resource_type).await;
    }
}

async fn _execute_multiple_clients_users_hot(
    client_factory: &dyn ClientFactory,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let mut handles = Vec::with_capacity(MULTIPLE_CLIENT_COUNT);
    let barrier = if use_barrier {
        Some(Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)))
    } else {
        None
    };

    for client_id in 0..MULTIPLE_CLIENT_COUNT {
        let client = create_client(client_factory).await;
        login_root(&client).await;

        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            if let Some(b) = barrier_clone {
                b.wait().await;
            }

            let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
            for i in 0..OPERATIONS_PER_CLIENT {
                let username = format!("race-user-{}-{}", client_id, i);
                let result = client
                    .create_user(&username, USER_PASSWORD, UserStatus::Active, None)
                    .await
                    .map(|_| ());
                results.push(result);
            }
            results
        });

        handles.push(handle);
    }

    let all_results = join_all(handles).await;
    all_results
        .into_iter()
        .flat_map(|r| r.expect("Tokio task panicked"))
        .collect()
}

async fn execute_multiple_clients_users_cold(
    client_factory: &dyn ClientFactory,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let mut handles = Vec::with_capacity(MULTIPLE_CLIENT_COUNT);
    const DUPLICATE_USER: &str = "race-user-duplicate";
    let barrier = if use_barrier {
        Some(Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)))
    } else {
        None
    };

    for _ in 0..MULTIPLE_CLIENT_COUNT {
        let client = create_client(client_factory).await;
        login_root(&client).await;

        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            if let Some(b) = barrier_clone {
                b.wait().await;
            }

            let mut results = Vec::with_capacity(MULTIPLE_CLIENT_COUNT);
            for _ in 0..OPERATIONS_PER_CLIENT {
                let result = client
                    .create_user(DUPLICATE_USER, USER_PASSWORD, UserStatus::Active, None)
                    .await
                    .map(|_| ());
                results.push(result);
            }
            results
        });

        handles.push(handle);
    }

    let all_results = join_all(handles).await;
    all_results
        .into_iter()
        .flat_map(|r| r.expect("Tokio task panicked"))
        .collect()
}

async fn execute_multiple_clients_streams_hot(
    client_factory: &dyn ClientFactory,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let mut handles = Vec::with_capacity(MULTIPLE_CLIENT_COUNT);
    let barrier = if use_barrier {
        Some(Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)))
    } else {
        None
    };

    for client_id in 0..MULTIPLE_CLIENT_COUNT {
        let client = create_client(client_factory).await;
        login_root(&client).await;

        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            if let Some(b) = barrier_clone {
                b.wait().await;
            }

            let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
            for i in 0..OPERATIONS_PER_CLIENT {
                let stream_name = format!("race-stream-{}-{}", client_id, i);
                let result = client.create_stream(&stream_name).await.map(|_| ());
                results.push(result);
            }
            results
        });

        handles.push(handle);
    }

    let all_results = join_all(handles).await;
    all_results
        .into_iter()
        .flat_map(|r| r.expect("Tokio task panicked"))
        .collect()
}

async fn execute_multiple_clients_streams_cold(
    client_factory: &dyn ClientFactory,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let mut handles = Vec::with_capacity(MULTIPLE_CLIENT_COUNT);
    const DUPLICATE_STREAM: &str = "race-stream-duplicate";
    let barrier = if use_barrier {
        Some(Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)))
    } else {
        None
    };

    for _ in 0..MULTIPLE_CLIENT_COUNT {
        let client = create_client(client_factory).await;
        login_root(&client).await;

        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            if let Some(b) = barrier_clone {
                b.wait().await;
            }

            let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
            for _ in 0..OPERATIONS_PER_CLIENT {
                let result = client.create_stream(DUPLICATE_STREAM).await.map(|_| ());
                results.push(result);
            }
            results
        });

        handles.push(handle);
    }

    let all_results = join_all(handles).await;
    all_results
        .into_iter()
        .flat_map(|r| r.expect("Tokio task panicked"))
        .collect()
}

async fn execute_multiple_clients_topics_hot(
    client_factory: &dyn ClientFactory,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let mut handles = Vec::with_capacity(MULTIPLE_CLIENT_COUNT);
    let barrier = if use_barrier {
        Some(Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)))
    } else {
        None
    };

    for client_id in 0..MULTIPLE_CLIENT_COUNT {
        let client = create_client(client_factory).await;
        login_root(&client).await;

        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            if let Some(b) = barrier_clone {
                b.wait().await;
            }

            let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
            let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();

            for i in 0..OPERATIONS_PER_CLIENT {
                let topic_name = format!("race-topic-{}-{}", client_id, i);
                let result = client
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
                    .map(|_| ());
                results.push(result);
            }
            results
        });

        handles.push(handle);
    }

    let all_results = join_all(handles).await;
    all_results
        .into_iter()
        .flat_map(|r| r.expect("Tokio task panicked"))
        .collect()
}

async fn execute_multiple_clients_topics_cold(
    client_factory: &dyn ClientFactory,
    use_barrier: bool,
) -> Vec<OperationResult> {
    let mut handles = Vec::with_capacity(MULTIPLE_CLIENT_COUNT);
    const DUPLICATE_TOPIC: &str = "race-topic-duplicate";
    let barrier = if use_barrier {
        Some(Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)))
    } else {
        None
    };

    for _ in 0..MULTIPLE_CLIENT_COUNT {
        let client = create_client(client_factory).await;
        login_root(&client).await;

        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            if let Some(b) = barrier_clone {
                b.wait().await;
            }

            let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
            let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();

            for _ in 0..OPERATIONS_PER_CLIENT {
                let result = client
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
                    .map(|_| ());
                results.push(result);
            }
            results
        });

        handles.push(handle);
    }

    let all_results = join_all(handles).await;
    all_results
        .into_iter()
        .flat_map(|r| r.expect("Tokio task panicked"))
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
                OPERATIONS_COUNT,
                "Hot path: Expected all {} operations to succeed, but only {} succeeded. Errors: {:?}",
                OPERATIONS_COUNT,
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
                OPERATIONS_COUNT - 1,
                "Cold path: Expected {} errors, but got {}",
                OPERATIONS_COUNT - 1,
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
    client_factory: &dyn ClientFactory,
    resource_type: ResourceType,
    scenario_type: ScenarioType,
) {
    const VALIDATION_CLIENT_COUNT: usize = 5;
    let mut clients = Vec::with_capacity(VALIDATION_CLIENT_COUNT);

    // Create multiple clients for validation
    for _ in 0..VALIDATION_CLIENT_COUNT {
        let client = create_client(client_factory).await;
        login_root(&client).await;
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
            // Hot path: should have OPERATIONS_COUNT unique users
            assert_eq!(
                test_users.len(),
                OPERATIONS_COUNT,
                "Hot path: Expected {} users, but found {}. Users: {:?}",
                OPERATIONS_COUNT,
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
            // Hot path: should have OPERATIONS_COUNT unique streams
            assert_eq!(
                test_streams.len(),
                OPERATIONS_COUNT,
                "Hot path: Expected {} streams, but found {}. Streams: {:?}",
                OPERATIONS_COUNT,
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
            // Hot path: should have OPERATIONS_COUNT unique topics
            assert_eq!(
                test_topics.len(),
                OPERATIONS_COUNT,
                "Hot path: Expected {} topics, but found {}. Topics: {:?}",
                OPERATIONS_COUNT,
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
                let _ = client
                    .delete_stream(&Identifier::numeric(stream.id).unwrap())
                    .await;
            }
        }
        // Just delete test stream
        ResourceType::Topic => {
            let _ = client
                .delete_stream(&Identifier::named(TEST_STREAM_NAME).unwrap())
                .await;
        }
    }
}
