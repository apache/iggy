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
use integration::iggy_harness;
use std::sync::Arc;
use test_case::test_matrix;
use tokio::sync::Barrier;

const OPERATIONS_COUNT: usize = 40;
const MULTIPLE_CLIENT_COUNT: usize = 10;
const OPERATIONS_PER_CLIENT: usize = OPERATIONS_COUNT / MULTIPLE_CLIENT_COUNT;
const USER_PASSWORD: &str = "secret";
const TEST_STREAM_NAME: &str = "race-test-stream";
const PARTITIONS_COUNT: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceType {
    User,
    Stream,
    Topic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioType {
    Hot,
    Cold,
}

fn user() -> ResourceType {
    ResourceType::User
}

fn stream() -> ResourceType {
    ResourceType::Stream
}

fn topic() -> ResourceType {
    ResourceType::Topic
}

fn hot() -> ScenarioType {
    ScenarioType::Hot
}

fn cold() -> ScenarioType {
    ScenarioType::Cold
}

fn barrier_on() -> bool {
    true
}

fn barrier_off() -> bool {
    false
}

type OperationResult = Result<(), IggyError>;

#[iggy_harness(transport = [Tcp, Http, Quic, WebSocket], server(tcp.socket.nodelay = true))]
#[test_matrix(
    [user(), stream(), topic()],
    [hot(), cold()],
    [barrier_on(), barrier_off()]
)]
async fn matrix(
    harness: &TestHarness,
    resource_type: ResourceType,
    scenario_type: ScenarioType,
    use_barrier: bool,
) {
    let root_client = harness.root_client().await.unwrap();

    if resource_type == ResourceType::Topic {
        root_client.create_stream(TEST_STREAM_NAME).await.unwrap();
    }

    let clients = harness.root_clients(MULTIPLE_CLIENT_COUNT).await.unwrap();

    let results = match (resource_type, scenario_type) {
        (ResourceType::User, ScenarioType::Cold) => execute_users_cold(clients, use_barrier).await,
        (ResourceType::Stream, ScenarioType::Hot) => {
            execute_streams_hot(clients, use_barrier).await
        }
        (ResourceType::Stream, ScenarioType::Cold) => {
            execute_streams_cold(clients, use_barrier).await
        }
        (ResourceType::Topic, ScenarioType::Hot) => execute_topics_hot(clients, use_barrier).await,
        (ResourceType::Topic, ScenarioType::Cold) => {
            execute_topics_cold(clients, use_barrier).await
        }
        // TODO: Figure out why User/Hot tests timeout in CI
        (ResourceType::User, ScenarioType::Hot) => vec![],
    };

    if !results.is_empty() {
        validate_results(&results, scenario_type);
        let validation_client = harness.root_client().await.unwrap();
        validate_server_state(&validation_client, resource_type, scenario_type).await;
        cleanup_resources(&root_client, resource_type).await;
    }
}

async fn execute_users_cold(clients: Vec<IggyClient>, use_barrier: bool) -> Vec<OperationResult> {
    const DUPLICATE_USER: &str = "race-user-duplicate";
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)));

    let handles: Vec<_> = clients
        .into_iter()
        .map(|client| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                if let Some(b) = barrier {
                    b.wait().await;
                }
                let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
                for _ in 0..OPERATIONS_PER_CLIENT {
                    let result = client
                        .create_user(DUPLICATE_USER, USER_PASSWORD, UserStatus::Active, None)
                        .await
                        .map(|_| ());
                    results.push(result);
                }
                results
            })
        })
        .collect();

    join_all(handles)
        .await
        .into_iter()
        .flat_map(|r| r.expect("Task panicked"))
        .collect()
}

async fn execute_streams_hot(clients: Vec<IggyClient>, use_barrier: bool) -> Vec<OperationResult> {
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)));

    let handles: Vec<_> = clients
        .into_iter()
        .enumerate()
        .map(|(client_id, client)| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                if let Some(b) = barrier {
                    b.wait().await;
                }
                let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
                for i in 0..OPERATIONS_PER_CLIENT {
                    let stream_name = format!("race-stream-{client_id}-{i}");
                    let result = client.create_stream(&stream_name).await.map(|_| ());
                    results.push(result);
                }
                results
            })
        })
        .collect();

    join_all(handles)
        .await
        .into_iter()
        .flat_map(|r| r.expect("Task panicked"))
        .collect()
}

async fn execute_streams_cold(clients: Vec<IggyClient>, use_barrier: bool) -> Vec<OperationResult> {
    const DUPLICATE_STREAM: &str = "race-stream-duplicate";
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)));

    let handles: Vec<_> = clients
        .into_iter()
        .map(|client| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                if let Some(b) = barrier {
                    b.wait().await;
                }
                let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
                for _ in 0..OPERATIONS_PER_CLIENT {
                    let result = client.create_stream(DUPLICATE_STREAM).await.map(|_| ());
                    results.push(result);
                }
                results
            })
        })
        .collect();

    join_all(handles)
        .await
        .into_iter()
        .flat_map(|r| r.expect("Task panicked"))
        .collect()
}

async fn execute_topics_hot(clients: Vec<IggyClient>, use_barrier: bool) -> Vec<OperationResult> {
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)));

    let handles: Vec<_> = clients
        .into_iter()
        .enumerate()
        .map(|(client_id, client)| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                if let Some(b) = barrier {
                    b.wait().await;
                }
                let mut results = Vec::with_capacity(OPERATIONS_PER_CLIENT);
                let stream_id = Identifier::named(TEST_STREAM_NAME).unwrap();

                for i in 0..OPERATIONS_PER_CLIENT {
                    let topic_name = format!("race-topic-{client_id}-{i}");
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
            })
        })
        .collect();

    join_all(handles)
        .await
        .into_iter()
        .flat_map(|r| r.expect("Task panicked"))
        .collect()
}

async fn execute_topics_cold(clients: Vec<IggyClient>, use_barrier: bool) -> Vec<OperationResult> {
    const DUPLICATE_TOPIC: &str = "race-topic-duplicate";
    let barrier = use_barrier.then(|| Arc::new(Barrier::new(MULTIPLE_CLIENT_COUNT)));

    let handles: Vec<_> = clients
        .into_iter()
        .map(|client| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                if let Some(b) = barrier {
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
            })
        })
        .collect();

    join_all(handles)
        .await
        .into_iter()
        .flat_map(|r| r.expect("Task panicked"))
        .collect()
}

fn validate_results(results: &[OperationResult], scenario_type: ScenarioType) {
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let error_count = results.iter().filter(|r| r.is_err()).count();

    match scenario_type {
        ScenarioType::Hot => {
            assert_eq!(
                success_count,
                OPERATIONS_COUNT,
                "Hot path: Expected all {OPERATIONS_COUNT} operations to succeed, but only {success_count} succeeded. Errors: {:?}",
                results.iter().filter(|r| r.is_err()).collect::<Vec<_>>()
            );
            assert_eq!(
                error_count, 0,
                "Hot path: Expected 0 errors, but got {error_count}"
            );
        }
        ScenarioType::Cold => {
            assert_eq!(
                success_count, 1,
                "Cold path: Expected exactly 1 success, but got {success_count}. All results: {results:?}"
            );
            assert_eq!(
                error_count,
                OPERATIONS_COUNT - 1,
                "Cold path: Expected {} errors, but got {error_count}",
                OPERATIONS_COUNT - 1
            );

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
                    "Expected 'already exists' error, got: {err:?}"
                );
            }
        }
    }
}

async fn validate_server_state(
    client: &IggyClient,
    resource_type: ResourceType,
    scenario_type: ScenarioType,
) {
    match resource_type {
        ResourceType::User => validate_users_state(client, scenario_type).await,
        ResourceType::Stream => validate_streams_state(client, scenario_type).await,
        ResourceType::Topic => validate_topics_state(client, scenario_type).await,
    }
}

async fn validate_users_state(client: &IggyClient, scenario_type: ScenarioType) {
    let users = client.get_users().await.expect("Failed to get users");
    let test_users: Vec<_> = users
        .into_iter()
        .filter(|u| u.username.starts_with("race-user-"))
        .collect();

    match scenario_type {
        ScenarioType::Hot => {
            assert_eq!(
                test_users.len(),
                OPERATIONS_COUNT,
                "Hot path: Expected {OPERATIONS_COUNT} users, found {}",
                test_users.len()
            );
            let ids: std::collections::HashSet<u32> = test_users.iter().map(|u| u.id).collect();
            assert_eq!(
                ids.len(),
                test_users.len(),
                "Hot path: Found duplicate user IDs"
            );
        }
        ScenarioType::Cold => {
            assert_eq!(
                test_users.len(),
                1,
                "Cold path: Expected 1 user, found {}",
                test_users.len()
            );
            assert_eq!(test_users[0].username, "race-user-duplicate");
        }
    }
}

async fn validate_streams_state(client: &IggyClient, scenario_type: ScenarioType) {
    let streams = client.get_streams().await.expect("Failed to get streams");
    let test_streams: Vec<_> = streams
        .into_iter()
        .filter(|s| s.name.starts_with("race-stream-"))
        .collect();

    match scenario_type {
        ScenarioType::Hot => {
            assert_eq!(
                test_streams.len(),
                OPERATIONS_COUNT,
                "Hot path: Expected {OPERATIONS_COUNT} streams, found {}",
                test_streams.len()
            );
            let ids: std::collections::HashSet<u32> = test_streams.iter().map(|s| s.id).collect();
            assert_eq!(
                ids.len(),
                test_streams.len(),
                "Hot path: Found duplicate stream IDs"
            );
        }
        ScenarioType::Cold => {
            assert_eq!(
                test_streams.len(),
                1,
                "Cold path: Expected 1 stream, found {}",
                test_streams.len()
            );
            assert_eq!(test_streams[0].name, "race-stream-duplicate");
        }
    }
}

async fn validate_topics_state(client: &IggyClient, scenario_type: ScenarioType) {
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
            assert_eq!(
                test_topics.len(),
                OPERATIONS_COUNT,
                "Hot path: Expected {OPERATIONS_COUNT} topics, found {}",
                test_topics.len()
            );
            let ids: std::collections::HashSet<u32> = test_topics.iter().map(|t| t.id).collect();
            assert_eq!(
                ids.len(),
                test_topics.len(),
                "Hot path: Found duplicate topic IDs"
            );
        }
        ScenarioType::Cold => {
            assert_eq!(
                test_topics.len(),
                1,
                "Cold path: Expected 1 topic, found {}",
                test_topics.len()
            );
            assert_eq!(test_topics[0].name, "race-topic-duplicate");
        }
    }
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
        ResourceType::Topic => {
            let _ = client
                .delete_stream(&Identifier::named(TEST_STREAM_NAME).unwrap())
                .await;
        }
    }
}
