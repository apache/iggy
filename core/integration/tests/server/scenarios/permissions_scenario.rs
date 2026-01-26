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

//! Comprehensive permissions scenario test.
//!
//! Tests the full permission model including:
//! - Global permissions (servers, users, streams, topics, messages)
//! - Stream-specific permissions
//! - Topic-specific permissions
//! - Permission inheritance hierarchy
//! - Positive cases (permission grants access)
//! - Negative cases (missing permission denies access)

use crate::server::scenarios::create_client;
use ahash::AHashMap;
use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};

const STREAM_1: &str = "perm-stream-1";
const STREAM_2: &str = "perm-stream-2";
const TOPIC_1: &str = "perm-topic-1";
const TOPIC_2: &str = "perm-topic-2";
const PARTITIONS: u32 = 1;

pub async fn run(client_factory: &dyn ClientFactory) {
    let root_client = create_client(client_factory).await;
    login_root(&root_client).await;

    setup_test_resources(&root_client).await;

    // Test categories
    test_no_permissions(client_factory, &root_client).await;
    test_system_permissions(client_factory, &root_client).await;
    test_user_permissions(client_factory, &root_client).await;
    test_stream_permissions(client_factory, &root_client).await;
    test_topic_permissions(client_factory, &root_client).await;
    test_message_permissions(client_factory, &root_client).await;
    test_stream_specific_permissions(client_factory, &root_client).await;
    test_topic_specific_permissions(client_factory, &root_client).await;

    // Permission inheritance/implication tests
    test_global_permission_inheritance(client_factory, &root_client).await;
    test_stream_permission_inheritance(client_factory, &root_client).await;
    test_topic_permission_inheritance(client_factory, &root_client).await;

    // Consumer group operations matrix
    test_consumer_group_operations(client_factory, &root_client).await;

    // Union semantics tests
    test_union_semantics(client_factory, &root_client).await;

    // Missing resource behavior tests
    test_missing_resource_behavior(client_factory, &root_client).await;

    cleanup(&root_client).await;
}

async fn setup_test_resources(root_client: &IggyClient) {
    root_client
        .create_stream(STREAM_1)
        .await
        .expect("create stream 1");

    root_client
        .create_stream(STREAM_2)
        .await
        .expect("create stream 2");

    for stream_name in [STREAM_1, STREAM_2] {
        let stream_id = Identifier::named(stream_name).unwrap();
        for topic_name in [TOPIC_1, TOPIC_2] {
            root_client
                .create_topic(
                    &stream_id,
                    topic_name,
                    PARTITIONS,
                    CompressionAlgorithm::None,
                    None,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await
                .expect("create topic");
        }
    }
}

async fn cleanup(root_client: &IggyClient) {
    for stream_name in [STREAM_1, STREAM_2] {
        let _ = root_client
            .delete_stream(&Identifier::named(stream_name).unwrap())
            .await;
    }
}

// =============================================================================
// User Creation Helpers
// =============================================================================

async fn create_test_user(
    root_client: &IggyClient,
    username: &str,
    permissions: Option<Permissions>,
) {
    let _ = root_client
        .delete_user(&Identifier::named(username).unwrap())
        .await;

    root_client
        .create_user(username, "password123", UserStatus::Active, permissions)
        .await
        .expect("create test user");
}

async fn delete_test_user(root_client: &IggyClient, username: &str) {
    let _ = root_client
        .delete_user(&Identifier::named(username).unwrap())
        .await;
}

async fn login_user(client_factory: &dyn ClientFactory, username: &str) -> IggyClient {
    let client = create_client(client_factory).await;
    client
        .login_user(username, "password123")
        .await
        .expect("login user");
    client
}

fn no_permissions() -> Permissions {
    Permissions {
        global: GlobalPermissions::default(),
        streams: None,
    }
}

// =============================================================================
// Test: User with no permissions
// =============================================================================

async fn test_no_permissions(client_factory: &dyn ClientFactory, root_client: &IggyClient) {
    const USER: &str = "no-perms-user";
    create_test_user(root_client, USER, Some(no_permissions())).await;
    let client = login_user(client_factory, USER).await;

    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // All operations should fail
    assert_unauthorized(client.get_stats().await, "get_stats");
    assert_unauthorized(client.get_clients().await, "get_clients");
    assert_unauthorized(client.get_users().await, "get_users");
    assert_unauthorized(client.get_streams().await, "get_streams");
    assert_unauthorized(client.get_stream(&stream_id).await, "get_stream");
    assert_unauthorized(client.get_topics(&stream_id).await, "get_topics");
    assert_unauthorized(client.get_topic(&stream_id, &topic_id).await, "get_topic");
    assert_unauthorized(client.create_stream("x").await, "create_stream");
    assert_unauthorized(
        client
            .create_topic(
                &stream_id,
                "x",
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await,
        "create_topic",
    );

    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "send_messages",
    );
    assert_unauthorized(
        client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await,
        "poll_messages",
    );

    delete_test_user(root_client, USER).await;
}

// =============================================================================
// Test: System permissions (read_servers, manage_servers)
// =============================================================================

async fn test_system_permissions(client_factory: &dyn ClientFactory, root_client: &IggyClient) {
    // User with read_servers only
    const READ_USER: &str = "read-servers-user";
    create_test_user(
        root_client,
        READ_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_servers: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, READ_USER).await;
    client
        .get_stats()
        .await
        .expect("read_servers: get_stats should work");
    client
        .get_clients()
        .await
        .expect("read_servers: get_clients should work");

    // But cannot read users or streams
    assert_unauthorized(client.get_users().await, "read_servers: get_users");
    assert_unauthorized(client.get_streams().await, "read_servers: get_streams");

    delete_test_user(root_client, READ_USER).await;

    // User with manage_servers (implies read_servers capabilities)
    const MANAGE_USER: &str = "manage-servers-user";
    create_test_user(
        root_client,
        MANAGE_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_servers: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_USER).await;
    client
        .get_stats()
        .await
        .expect("manage_servers: get_stats should work");
    client
        .get_clients()
        .await
        .expect("manage_servers: get_clients should work");

    delete_test_user(root_client, MANAGE_USER).await;
}

// =============================================================================
// Test: User permissions (read_users, manage_users)
// =============================================================================

async fn test_user_permissions(client_factory: &dyn ClientFactory, root_client: &IggyClient) {
    // User with read_users only
    const READ_USER: &str = "read-users-user";
    create_test_user(
        root_client,
        READ_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_users: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, READ_USER).await;
    client
        .get_users()
        .await
        .expect("read_users: get_users should work");
    client
        .get_user(&Identifier::numeric(0).unwrap())
        .await
        .expect("read_users: get_user should work");

    // Cannot create/delete users
    assert_unauthorized(
        client
            .create_user(
                "test-user-to-create",
                "validpassword123",
                UserStatus::Active,
                None,
            )
            .await,
        "read_users: create_user",
    );
    assert_unauthorized(
        client
            .delete_user(&Identifier::named(READ_USER).unwrap())
            .await,
        "read_users: delete_user",
    );

    delete_test_user(root_client, READ_USER).await;

    // User with manage_users
    const MANAGE_USER: &str = "manage-users-user";
    create_test_user(
        root_client,
        MANAGE_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_users: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_USER).await;
    client
        .get_users()
        .await
        .expect("manage_users: get_users should work");

    // Create a temporary user
    client
        .create_user("temp-user", "temp", UserStatus::Active, None)
        .await
        .expect("manage_users: create_user should work");

    // Update and delete it
    client
        .update_user(
            &Identifier::named("temp-user").unwrap(),
            Some("temp-user-updated"),
            None,
        )
        .await
        .expect("manage_users: update_user should work");

    client
        .delete_user(&Identifier::named("temp-user-updated").unwrap())
        .await
        .expect("manage_users: delete_user should work");

    delete_test_user(root_client, MANAGE_USER).await;
}

// =============================================================================
// Test: Stream permissions (read_streams, manage_streams)
// =============================================================================

async fn test_stream_permissions(client_factory: &dyn ClientFactory, root_client: &IggyClient) {
    let stream_id = Identifier::named(STREAM_1).unwrap();

    // User with read_streams only
    const READ_USER: &str = "read-streams-user";
    create_test_user(
        root_client,
        READ_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_streams: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, READ_USER).await;
    client
        .get_streams()
        .await
        .expect("read_streams: get_streams should work");
    client
        .get_stream(&stream_id)
        .await
        .expect("read_streams: get_stream should work");

    // Cannot create/update/delete streams
    assert_unauthorized(
        client.create_stream("x").await,
        "read_streams: create_stream",
    );
    assert_unauthorized(
        client.update_stream(&stream_id, "new-name").await,
        "read_streams: update_stream",
    );
    assert_unauthorized(
        client.delete_stream(&stream_id).await,
        "read_streams: delete_stream",
    );

    delete_test_user(root_client, READ_USER).await;

    // User with manage_streams
    const MANAGE_USER: &str = "manage-streams-user";
    create_test_user(
        root_client,
        MANAGE_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_streams: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_USER).await;
    client
        .get_streams()
        .await
        .expect("manage_streams: get_streams should work");

    // Create, update, delete stream
    client
        .create_stream("temp-stream")
        .await
        .expect("manage_streams: create_stream should work");

    client
        .update_stream(&Identifier::named("temp-stream").unwrap(), "temp-stream-v2")
        .await
        .expect("manage_streams: update_stream should work");

    client
        .delete_stream(&Identifier::named("temp-stream-v2").unwrap())
        .await
        .expect("manage_streams: delete_stream should work");

    // manage_streams also implies manage_topics
    let topic_id = Identifier::named(TOPIC_1).unwrap();
    client
        .get_topics(&stream_id)
        .await
        .expect("manage_streams: get_topics should work");
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("manage_streams: get_topic should work");

    delete_test_user(root_client, MANAGE_USER).await;
}

// =============================================================================
// Test: Topic permissions (read_topics, manage_topics)
// =============================================================================

async fn test_topic_permissions(client_factory: &dyn ClientFactory, root_client: &IggyClient) {
    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // User with read_topics only
    const READ_USER: &str = "read-topics-user";
    create_test_user(
        root_client,
        READ_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_topics: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, READ_USER).await;
    client
        .get_topics(&stream_id)
        .await
        .expect("read_topics: get_topics should work");
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("read_topics: get_topic should work");

    // read_topics allows consumer group operations
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("read_topics: get_consumer_groups should work");

    // Cannot create/update/delete topics
    assert_unauthorized(
        client
            .create_topic(
                &stream_id,
                "x",
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await,
        "read_topics: create_topic",
    );
    assert_unauthorized(
        client
            .update_topic(
                &stream_id,
                &topic_id,
                "new-name",
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await,
        "read_topics: update_topic",
    );

    delete_test_user(root_client, READ_USER).await;

    // User with manage_topics
    const MANAGE_USER: &str = "manage-topics-user";
    create_test_user(
        root_client,
        MANAGE_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_topics: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_USER).await;

    // Create topic
    client
        .create_topic(
            &stream_id,
            "temp-topic",
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("manage_topics: create_topic should work");

    // Update topic
    client
        .update_topic(
            &stream_id,
            &Identifier::named("temp-topic").unwrap(),
            "temp-topic-v2",
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("manage_topics: update_topic should work");

    // Delete topic
    client
        .delete_topic(&stream_id, &Identifier::named("temp-topic-v2").unwrap())
        .await
        .expect("manage_topics: delete_topic should work");

    // Cannot create streams
    assert_unauthorized(
        client.create_stream("x").await,
        "manage_topics: create_stream",
    );

    delete_test_user(root_client, MANAGE_USER).await;
}

// =============================================================================
// Test: Message permissions (poll_messages, send_messages)
// =============================================================================

async fn test_message_permissions(client_factory: &dyn ClientFactory, root_client: &IggyClient) {
    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // User with poll_messages only
    const POLL_USER: &str = "poll-messages-user";
    create_test_user(
        root_client,
        POLL_USER,
        Some(Permissions {
            global: GlobalPermissions {
                poll_messages: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, POLL_USER).await;

    // Can poll messages
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("poll_messages: poll_messages should work");

    // Cannot send messages
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "poll_messages: send_messages",
    );

    // Cannot read streams/topics without those permissions
    assert_unauthorized(client.get_streams().await, "poll_messages: get_streams");

    delete_test_user(root_client, POLL_USER).await;

    // User with send_messages only
    const SEND_USER: &str = "send-messages-user";
    create_test_user(
        root_client,
        SEND_USER,
        Some(Permissions {
            global: GlobalPermissions {
                send_messages: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, SEND_USER).await;

    // Can send messages
    let mut msgs = test_messages();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("send_messages: send_messages should work");

    // Cannot poll messages
    assert_unauthorized(
        client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await,
        "send_messages: poll_messages",
    );

    delete_test_user(root_client, SEND_USER).await;

    // User with both poll and send
    const BOTH_USER: &str = "poll-send-user";
    create_test_user(
        root_client,
        BOTH_USER,
        Some(Permissions {
            global: GlobalPermissions {
                poll_messages: true,
                send_messages: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, BOTH_USER).await;

    let mut msgs = test_messages();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("both: send_messages should work");

    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("both: poll_messages should work");

    delete_test_user(root_client, BOTH_USER).await;
}

// =============================================================================
// Test: Stream-specific permissions
// =============================================================================

async fn test_stream_specific_permissions(
    client_factory: &dyn ClientFactory,
    root_client: &IggyClient,
) {
    let stream1_id = Identifier::named(STREAM_1).unwrap();
    let stream2_id = Identifier::named(STREAM_2).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // Get the actual stream IDs from the server
    let streams = root_client.get_streams().await.expect("get streams");
    let stream1_numeric_id = streams
        .iter()
        .find(|s| s.name == STREAM_1)
        .map(|s| s.id as usize)
        .expect("stream 1 should exist");

    // User with permissions only for stream 1
    const USER: &str = "stream-specific-user";
    create_test_user(
        root_client,
        USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    read_stream: true,
                    manage_topics: true,
                    poll_messages: true,
                    send_messages: true,
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, USER).await;

    // Can access stream 1
    client
        .get_stream(&stream1_id)
        .await
        .expect("stream-specific: get_stream 1 should work");
    client
        .get_topics(&stream1_id)
        .await
        .expect("stream-specific: get_topics 1 should work");
    client
        .get_topic(&stream1_id, &topic_id)
        .await
        .expect("stream-specific: get_topic 1 should work");

    // Can send/poll messages on stream 1
    let mut msgs = test_messages();
    client
        .send_messages(
            &stream1_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("stream-specific: send_messages 1 should work");

    client
        .poll_messages(
            &stream1_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("stream-specific: poll_messages 1 should work");

    // Cannot access stream 2
    assert_unauthorized(
        client.get_stream(&stream2_id).await,
        "stream-specific: get_stream 2",
    );
    assert_unauthorized(
        client.get_topics(&stream2_id).await,
        "stream-specific: get_topics 2",
    );
    assert_unauthorized(
        client.get_topic(&stream2_id, &topic_id).await,
        "stream-specific: get_topic 2",
    );

    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream2_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "stream-specific: send_messages 2",
    );
    assert_unauthorized(
        client
            .poll_messages(
                &stream2_id,
                &topic_id,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await,
        "stream-specific: poll_messages 2",
    );

    // Cannot get all streams (no global read_streams)
    assert_unauthorized(client.get_streams().await, "stream-specific: get_streams");

    delete_test_user(root_client, USER).await;
}

// =============================================================================
// Test: Topic-specific permissions
// =============================================================================

async fn test_topic_specific_permissions(
    client_factory: &dyn ClientFactory,
    root_client: &IggyClient,
) {
    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic1_id = Identifier::named(TOPIC_1).unwrap();
    let topic2_id = Identifier::named(TOPIC_2).unwrap();

    // Get the actual IDs from the server
    let streams = root_client.get_streams().await.expect("get streams");
    let stream1_numeric_id = streams
        .iter()
        .find(|s| s.name == STREAM_1)
        .map(|s| s.id as usize)
        .expect("stream 1 should exist");

    let topics = root_client
        .get_topics(&stream_id)
        .await
        .expect("get topics");
    let topic1_numeric_id = topics
        .iter()
        .find(|t| t.name == TOPIC_1)
        .map(|t| t.id as usize)
        .expect("topic 1 should exist");

    // User with permissions only for topic 1 in stream 1
    const USER: &str = "topic-specific-user";
    create_test_user(
        root_client,
        USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    topics: Some(AHashMap::from([(
                        topic1_numeric_id,
                        TopicPermissions {
                            read_topic: true,
                            poll_messages: true,
                            send_messages: true,
                            ..Default::default()
                        },
                    )])),
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, USER).await;

    // Can access topic 1
    client
        .get_topic(&stream_id, &topic1_id)
        .await
        .expect("topic-specific: get_topic 1 should work");

    // Can send/poll messages on topic 1
    let mut msgs = test_messages();
    client
        .send_messages(
            &stream_id,
            &topic1_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("topic-specific: send_messages 1 should work");

    client
        .poll_messages(
            &stream_id,
            &topic1_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("topic-specific: poll_messages 1 should work");

    // Cannot access topic 2
    assert_unauthorized(
        client.get_topic(&stream_id, &topic2_id).await,
        "topic-specific: get_topic 2",
    );

    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic2_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "topic-specific: send_messages 2",
    );
    assert_unauthorized(
        client
            .poll_messages(
                &stream_id,
                &topic2_id,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await,
        "topic-specific: poll_messages 2",
    );

    // Cannot list all topics (no stream-level read_topics)
    assert_unauthorized(
        client.get_topics(&stream_id).await,
        "topic-specific: get_topics",
    );

    delete_test_user(root_client, USER).await;
}

// =============================================================================
// Test: Global permission inheritance/implication
// Tests that higher-level permissions imply lower-level ones WITHOUT explicitly
// setting the implied flags. This locks in the inheritance rules.
// =============================================================================

async fn test_global_permission_inheritance(
    client_factory: &dyn ClientFactory,
    root_client: &IggyClient,
) {
    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // Test: global read_streams implies read_topics, poll_messages, and CG ops
    // Doc: "read_streams permission allows to read the streams and includes all the permissions of read_topics"
    // Doc: "read_topics ... includes all the permissions of poll_messages" + CG ops
    const READ_STREAMS_USER: &str = "inherit-read-streams-user";
    create_test_user(
        root_client,
        READ_STREAMS_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_streams: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, READ_STREAMS_USER).await;

    // read_streams → read_topics
    client
        .get_topics(&stream_id)
        .await
        .expect("read_streams → read_topics: get_topics");
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("read_streams → read_topics: get_topic");

    // read_streams → read_topics → poll_messages
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("read_streams → poll_messages");

    // read_streams → read_topics → CG ops
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("read_streams → CG ops");

    // send_messages NOT implied
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "read_streams does NOT imply send_messages",
    );

    // manage operations NOT implied
    assert_unauthorized(
        client.create_stream("x").await,
        "read_streams does NOT imply manage_streams",
    );
    assert_unauthorized(
        client
            .create_topic(
                &stream_id,
                "x",
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await,
        "read_streams does NOT imply manage_topics",
    );

    delete_test_user(root_client, READ_STREAMS_USER).await;

    // Test: global manage_streams implies read_streams, manage_topics, read_topics, poll_messages, CG ops
    // Doc: "manage_streams permission ... includes all the permissions of read_streams"
    // Doc: "Also, it allows to manage all the topics of a stream, thus it has all the permissions of manage_topics"
    const MANAGE_STREAMS_USER: &str = "inherit-manage-streams-user";
    create_test_user(
        root_client,
        MANAGE_STREAMS_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_streams: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_STREAMS_USER).await;

    // manage_streams → read_streams
    client
        .get_streams()
        .await
        .expect("manage_streams → read_streams: get_streams");
    client
        .get_stream(&stream_id)
        .await
        .expect("manage_streams → read_streams: get_stream");

    // manage_streams → manage_topics (topic CRUD)
    client
        .create_topic(
            &stream_id,
            "temp-inherit-topic",
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("manage_streams → manage_topics: create_topic");
    client
        .delete_topic(
            &stream_id,
            &Identifier::named("temp-inherit-topic").unwrap(),
        )
        .await
        .expect("manage_streams → manage_topics: delete_topic");

    // manage_streams → read_topics
    client
        .get_topics(&stream_id)
        .await
        .expect("manage_streams → read_topics: get_topics");

    // manage_streams → read_streams → read_topics → poll_messages
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("manage_streams → poll_messages");

    // manage_streams → read_topics → CG ops
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("manage_streams → CG ops");

    // send_messages NOT implied
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "manage_streams does NOT imply send_messages",
    );

    delete_test_user(root_client, MANAGE_STREAMS_USER).await;

    // Test: global manage_topics implies read_topics, poll_messages, CG ops
    // Doc: "manage_topics permission allows to manage the topics and includes all the permissions of read_topics"
    const MANAGE_TOPICS_USER: &str = "inherit-manage-topics-user";
    create_test_user(
        root_client,
        MANAGE_TOPICS_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_topics: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_TOPICS_USER).await;

    // manage_topics → read_topics
    client
        .get_topics(&stream_id)
        .await
        .expect("manage_topics → read_topics: get_topics");
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("manage_topics → read_topics: get_topic");

    // manage_topics → read_topics → poll_messages
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("manage_topics → poll_messages");

    // manage_topics → read_topics → CG ops
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("manage_topics → CG ops");

    // read_streams NOT implied
    assert_unauthorized(
        client.get_streams().await,
        "manage_topics does NOT imply read_streams",
    );

    delete_test_user(root_client, MANAGE_TOPICS_USER).await;

    // Test: global read_topics implies poll_messages and CG ops
    // Doc: "read_topics permission allows to read the topics, manage consumer groups, and includes all the permissions of poll_messages"
    const READ_TOPICS_USER: &str = "inherit-read-topics-user";
    create_test_user(
        root_client,
        READ_TOPICS_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_topics: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, READ_TOPICS_USER).await;

    // read_topics allows reading topics
    client
        .get_topics(&stream_id)
        .await
        .expect("read_topics: get_topics");
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("read_topics: get_topic");

    // read_topics → poll_messages
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("read_topics → poll_messages");

    // read_topics → CG ops
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("read_topics → CG ops");

    delete_test_user(root_client, READ_TOPICS_USER).await;

    // Test: manage_servers implies read_servers
    const MANAGE_SERVERS_ONLY_USER: &str = "inherit-manage-servers-user";
    create_test_user(
        root_client,
        MANAGE_SERVERS_ONLY_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_servers: true,
                // NOT setting read_servers
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_SERVERS_ONLY_USER).await;

    // read_servers implied
    client
        .get_stats()
        .await
        .expect("manage_servers implies read_servers: get_stats should work");
    client
        .get_clients()
        .await
        .expect("manage_servers implies read_servers: get_clients should work");

    delete_test_user(root_client, MANAGE_SERVERS_ONLY_USER).await;

    // Test: manage_users implies read_users
    const MANAGE_USERS_ONLY_USER: &str = "inherit-manage-users-user";
    create_test_user(
        root_client,
        MANAGE_USERS_ONLY_USER,
        Some(Permissions {
            global: GlobalPermissions {
                manage_users: true,
                // NOT setting read_users
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_USERS_ONLY_USER).await;

    // read_users implied
    client
        .get_users()
        .await
        .expect("manage_users implies read_users: get_users should work");
    client
        .get_user(&Identifier::numeric(0).unwrap())
        .await
        .expect("manage_users implies read_users: get_user should work");

    delete_test_user(root_client, MANAGE_USERS_ONLY_USER).await;
}

// =============================================================================
// Test: Stream-scoped permission inheritance
// Tests inheritance rules at the stream level without setting implied flags.
// =============================================================================

async fn test_stream_permission_inheritance(
    client_factory: &dyn ClientFactory,
    root_client: &IggyClient,
) {
    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // Get stream numeric ID
    let streams = root_client.get_streams().await.expect("get streams");
    let stream1_numeric_id = streams
        .iter()
        .find(|s| s.name == STREAM_1)
        .map(|s| s.id as usize)
        .expect("stream 1 should exist");

    // Test: stream.read_stream implies read_topics, poll_messages, CG ops
    // Doc: "read_stream permission allows to read the stream and includes all the permissions of read_topics"
    // Doc: "Also, it allows to read all the messages of a topic, thus it has all the permissions of poll_messages"
    const READ_STREAM_USER: &str = "inherit-read-stream-user";
    create_test_user(
        root_client,
        READ_STREAM_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    read_stream: true,
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, READ_STREAM_USER).await;

    // read_stream allows get_stream
    client
        .get_stream(&stream_id)
        .await
        .expect("stream.read_stream: get_stream");

    // read_stream → read_topics
    client
        .get_topics(&stream_id)
        .await
        .expect("stream.read_stream → read_topics: get_topics");
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("stream.read_stream → read_topics: get_topic");

    // read_stream → poll_messages
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("stream.read_stream → poll_messages");

    // read_stream → read_topics → CG ops
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("stream.read_stream → CG ops");

    // send_messages NOT implied
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "stream.read_stream does NOT imply send_messages",
    );

    // manage operations NOT implied
    assert_unauthorized(
        client
            .create_topic(
                &stream_id,
                "x",
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await,
        "stream.read_stream does NOT imply manage_topics",
    );

    delete_test_user(root_client, READ_STREAM_USER).await;

    // Test: stream.manage_stream implies read_stream, manage_topics, read_topics, poll_messages, CG ops
    // Doc: "manage_stream permission allows to manage the stream and includes all the permissions of read_stream"
    // Doc: "Also, it allows to manage all the topics of a stream, thus it has all the permissions of manage_topics"
    const MANAGE_STREAM_USER: &str = "inherit-manage-stream-user";
    create_test_user(
        root_client,
        MANAGE_STREAM_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    manage_stream: true,
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_STREAM_USER).await;

    // manage_stream → read_stream
    client
        .get_stream(&stream_id)
        .await
        .expect("stream.manage_stream → read_stream: get_stream");

    // manage_stream → manage_topics (topic CRUD)
    client
        .create_topic(
            &stream_id,
            "temp-manage-stream-topic",
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("stream.manage_stream → manage_topics: create_topic");
    client
        .update_topic(
            &stream_id,
            &Identifier::named("temp-manage-stream-topic").unwrap(),
            "temp-manage-stream-topic-v2",
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("stream.manage_stream → manage_topics: update_topic");
    client
        .delete_topic(
            &stream_id,
            &Identifier::named("temp-manage-stream-topic-v2").unwrap(),
        )
        .await
        .expect("stream.manage_stream → manage_topics: delete_topic");

    // manage_stream → read_topics
    client
        .get_topics(&stream_id)
        .await
        .expect("stream.manage_stream → read_topics: get_topics");
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("stream.manage_stream → read_topics: get_topic");

    // manage_stream → read_stream → poll_messages
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("stream.manage_stream → poll_messages");

    // manage_stream → read_topics → CG ops
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("stream.manage_stream → CG ops");

    // send_messages NOT implied
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "stream.manage_stream does NOT imply send_messages",
    );

    delete_test_user(root_client, MANAGE_STREAM_USER).await;

    // Test: stream.manage_topics implies read_topics and poll_messages
    const MANAGE_TOPICS_STREAM_USER: &str = "inherit-manage-topics-stream-user";
    create_test_user(
        root_client,
        MANAGE_TOPICS_STREAM_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    manage_topics: true,
                    // NOT setting read_topics, poll_messages
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_TOPICS_STREAM_USER).await;

    // manage_topics allows topic CRUD
    client
        .create_topic(
            &stream_id,
            "temp-manage-topic",
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("stream.manage_topics: create_topic should work");
    client
        .delete_topic(&stream_id, &Identifier::named("temp-manage-topic").unwrap())
        .await
        .expect("stream.manage_topics: delete_topic should work");

    // read_topics implied
    client
        .get_topics(&stream_id)
        .await
        .expect("stream.manage_topics implies read_topics: get_topics should work");

    // poll_messages implied
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("stream.manage_topics implies poll_messages: poll_messages should work");

    // read_stream NOT implied
    assert_unauthorized(
        client.get_stream(&stream_id).await,
        "stream.manage_topics does NOT imply read_stream",
    );

    delete_test_user(root_client, MANAGE_TOPICS_STREAM_USER).await;
}

// =============================================================================
// Test: Topic-scoped permission inheritance
// Tests inheritance rules at the topic level without setting implied flags.
// =============================================================================

async fn test_topic_permission_inheritance(
    client_factory: &dyn ClientFactory,
    root_client: &IggyClient,
) {
    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // Get IDs
    let streams = root_client.get_streams().await.expect("get streams");
    let stream1_numeric_id = streams
        .iter()
        .find(|s| s.name == STREAM_1)
        .map(|s| s.id as usize)
        .expect("stream 1 should exist");

    let topics = root_client
        .get_topics(&stream_id)
        .await
        .expect("get topics");
    let topic1_numeric_id = topics
        .iter()
        .find(|t| t.name == TOPIC_1)
        .map(|t| t.id as usize)
        .expect("topic 1 should exist");

    // Test: topic.read_topic implies poll_messages and CG operations
    const READ_TOPIC_USER: &str = "inherit-read-topic-user";
    create_test_user(
        root_client,
        READ_TOPIC_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    topics: Some(AHashMap::from([(
                        topic1_numeric_id,
                        TopicPermissions {
                            read_topic: true,
                            // NOT setting poll_messages
                            ..Default::default()
                        },
                    )])),
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, READ_TOPIC_USER).await;

    // get_topic should work
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("topic.read_topic: get_topic should work");

    // poll_messages implied
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("topic.read_topic implies poll_messages: poll_messages should work");

    // CG operations implied
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("topic.read_topic implies CG ops: get_consumer_groups should work");

    // send_messages NOT implied
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "topic.read_topic does NOT imply send_messages",
    );

    delete_test_user(root_client, READ_TOPIC_USER).await;

    // Test: topic.manage_topic implies read_topic and poll_messages
    const MANAGE_TOPIC_USER: &str = "inherit-manage-topic-user";
    create_test_user(
        root_client,
        MANAGE_TOPIC_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    topics: Some(AHashMap::from([(
                        topic1_numeric_id,
                        TopicPermissions {
                            manage_topic: true,
                            // NOT setting read_topic, poll_messages
                            ..Default::default()
                        },
                    )])),
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, MANAGE_TOPIC_USER).await;

    // read_topic implied
    client
        .get_topic(&stream_id, &topic_id)
        .await
        .expect("topic.manage_topic implies read_topic: get_topic should work");

    // poll_messages implied
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("topic.manage_topic implies poll_messages: poll_messages should work");

    // CG operations implied
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("topic.manage_topic implies CG ops: get_consumer_groups should work");

    delete_test_user(root_client, MANAGE_TOPIC_USER).await;
}

// =============================================================================
// Test: Consumer group operations matrix
// Tests all CG operations under various permission levels and verifies they
// are denied under poll_messages-only.
// =============================================================================

async fn test_consumer_group_operations(
    client_factory: &dyn ClientFactory,
    root_client: &IggyClient,
) {
    let stream_id = Identifier::named(STREAM_1).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    // Test: poll_messages only does NOT allow CG operations
    const POLL_ONLY_USER: &str = "cg-poll-only-user";
    create_test_user(
        root_client,
        POLL_ONLY_USER,
        Some(Permissions {
            global: GlobalPermissions {
                poll_messages: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, POLL_ONLY_USER).await;

    // poll_messages should work
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("poll_messages: poll should work");

    // CG operations should be denied
    assert_unauthorized(
        client.get_consumer_groups(&stream_id, &topic_id).await,
        "poll_messages only: get_consumer_groups should be denied",
    );
    assert_unauthorized(
        client
            .get_consumer_group(&stream_id, &topic_id, &Identifier::numeric(1).unwrap())
            .await,
        "poll_messages only: get_consumer_group should be denied",
    );
    assert_unauthorized(
        client
            .create_consumer_group(&stream_id, &topic_id, "test-cg")
            .await,
        "poll_messages only: create_consumer_group should be denied",
    );

    delete_test_user(root_client, POLL_ONLY_USER).await;

    // Test: read_topics allows all CG operations
    const READ_TOPICS_CG_USER: &str = "cg-read-topics-user";
    create_test_user(
        root_client,
        READ_TOPICS_CG_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_topics: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, READ_TOPICS_CG_USER).await;

    // All CG operations should work
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("read_topics: get_consumer_groups should work");

    let cg = client
        .create_consumer_group(&stream_id, &topic_id, "test-cg-read-topics")
        .await
        .expect("read_topics: create_consumer_group should work");

    let cg_id = Identifier::numeric(cg.id).unwrap();

    client
        .get_consumer_group(&stream_id, &topic_id, &cg_id)
        .await
        .expect("read_topics: get_consumer_group should work");

    assert_ok_or_feature_unavailable(
        client
            .join_consumer_group(&stream_id, &topic_id, &cg_id)
            .await,
        "read_topics: join_consumer_group should work",
    );

    assert_ok_or_feature_unavailable(
        client
            .leave_consumer_group(&stream_id, &topic_id, &cg_id)
            .await,
        "read_topics: leave_consumer_group should work",
    );

    client
        .delete_consumer_group(&stream_id, &topic_id, &cg_id)
        .await
        .expect("read_topics: delete_consumer_group should work");

    delete_test_user(root_client, READ_TOPICS_CG_USER).await;

    // Get IDs for scoped test
    let streams = root_client.get_streams().await.expect("get streams");
    let stream1_numeric_id = streams
        .iter()
        .find(|s| s.name == STREAM_1)
        .map(|s| s.id as usize)
        .expect("stream 1 should exist");
    let topics = root_client
        .get_topics(&stream_id)
        .await
        .expect("get topics");
    let topic1_numeric_id = topics
        .iter()
        .find(|t| t.name == TOPIC_1)
        .map(|t| t.id as usize)
        .expect("topic 1 should exist");

    // Test: topic.read_topic allows CG operations on that topic
    const READ_TOPIC_CG_USER: &str = "cg-read-topic-user";
    create_test_user(
        root_client,
        READ_TOPIC_CG_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    topics: Some(AHashMap::from([(
                        topic1_numeric_id,
                        TopicPermissions {
                            read_topic: true,
                            ..Default::default()
                        },
                    )])),
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, READ_TOPIC_CG_USER).await;

    // CG operations should work on topic 1
    client
        .get_consumer_groups(&stream_id, &topic_id)
        .await
        .expect("topic.read_topic: get_consumer_groups should work");

    let cg = client
        .create_consumer_group(&stream_id, &topic_id, "test-cg-read-topic")
        .await
        .expect("topic.read_topic: create_consumer_group should work");

    let cg_id = Identifier::numeric(cg.id).unwrap();

    assert_ok_or_feature_unavailable(
        client
            .join_consumer_group(&stream_id, &topic_id, &cg_id)
            .await,
        "topic.read_topic: join_consumer_group should work",
    );

    assert_ok_or_feature_unavailable(
        client
            .leave_consumer_group(&stream_id, &topic_id, &cg_id)
            .await,
        "topic.read_topic: leave_consumer_group should work",
    );

    client
        .delete_consumer_group(&stream_id, &topic_id, &cg_id)
        .await
        .expect("topic.read_topic: delete_consumer_group should work");

    // CG operations should be denied on topic 2
    let topic2_id = Identifier::named(TOPIC_2).unwrap();
    assert_unauthorized(
        client.get_consumer_groups(&stream_id, &topic2_id).await,
        "topic.read_topic: CG ops on other topic should be denied",
    );

    delete_test_user(root_client, READ_TOPIC_CG_USER).await;
}

// =============================================================================
// Test: Union semantics (global + scoped should be OR, not AND)
// Tests that stream permissions extend rather than restrict global permissions.
// =============================================================================

async fn test_union_semantics(client_factory: &dyn ClientFactory, root_client: &IggyClient) {
    let stream1_id = Identifier::named(STREAM_1).unwrap();
    let stream2_id = Identifier::named(STREAM_2).unwrap();
    let topic_id = Identifier::named(TOPIC_1).unwrap();

    let streams = root_client.get_streams().await.expect("get streams");
    let stream1_numeric_id = streams
        .iter()
        .find(|s| s.name == STREAM_1)
        .map(|s| s.id as usize)
        .expect("stream 1 should exist");

    let topics = root_client
        .get_topics(&stream1_id)
        .await
        .expect("get topics");
    let topic1_numeric_id = topics
        .iter()
        .find(|t| t.name == TOPIC_1)
        .map(|t| t.id as usize)
        .expect("topic 1 should exist");

    // Test: global send_messages + scoped send_messages=false should still allow send
    // (global permissions should not be restricted by scoped permissions)
    const UNION_USER: &str = "union-semantics-user";
    create_test_user(
        root_client,
        UNION_USER,
        Some(Permissions {
            global: GlobalPermissions {
                send_messages: true,
                ..Default::default()
            },
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    send_messages: false, // Explicitly false at stream level
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, UNION_USER).await;

    // Should STILL be able to send (global overrides scoped restriction)
    let mut msgs = test_messages();
    client
        .send_messages(
            &stream1_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("union: global send_messages should work even with scoped=false");

    // Should also work on stream 2 (not in scoped map)
    let mut msgs = test_messages();
    client
        .send_messages(
            &stream2_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("union: global send_messages should work on all streams");

    delete_test_user(root_client, UNION_USER).await;

    // Test: global read_streams + scoped map with one stream should still allow reading other streams
    const GLOBAL_READ_SCOPED_MAP_USER: &str = "union-global-read-scoped-user";
    create_test_user(
        root_client,
        GLOBAL_READ_SCOPED_MAP_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_streams: true, // Global read for all streams
                ..Default::default()
            },
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    send_messages: true, // Extra permission for stream 1 only
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, GLOBAL_READ_SCOPED_MAP_USER).await;

    // Should be able to read all streams (global)
    client
        .get_streams()
        .await
        .expect("union: global read_streams should list all streams");
    client
        .get_stream(&stream1_id)
        .await
        .expect("union: should read stream 1");
    client
        .get_stream(&stream2_id)
        .await
        .expect("union: should read stream 2 (global, not in scoped map)");

    // Should be able to send to stream 1 (scoped)
    let mut msgs = test_messages();
    client
        .send_messages(
            &stream1_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("union: scoped send_messages on stream 1 should work");

    // Should NOT be able to send to stream 2 (no global send, no scoped send)
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream2_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "union: no send permission for stream 2",
    );

    delete_test_user(root_client, GLOBAL_READ_SCOPED_MAP_USER).await;

    // Test: topic-level permission should be additive with stream-level
    const TOPIC_ADDITIVE_USER: &str = "union-topic-additive-user";
    create_test_user(
        root_client,
        TOPIC_ADDITIVE_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    read_stream: true, // Can read stream and topics
                    topics: Some(AHashMap::from([(
                        topic1_numeric_id,
                        TopicPermissions {
                            send_messages: true, // Extra: can send to topic 1
                            ..Default::default()
                        },
                    )])),
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, TOPIC_ADDITIVE_USER).await;

    // Stream read operations should work
    client
        .get_stream(&stream1_id)
        .await
        .expect("additive: stream.read_stream should work");
    client
        .get_topics(&stream1_id)
        .await
        .expect("additive: should get all topics");

    // Poll should work on both topics (implied by read_stream)
    client
        .poll_messages(
            &stream1_id,
            &topic_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("additive: poll topic 1 should work");

    let topic2_id = Identifier::named(TOPIC_2).unwrap();
    client
        .poll_messages(
            &stream1_id,
            &topic2_id,
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("additive: poll topic 2 should work (via stream.read_stream)");

    // Send should ONLY work on topic 1 (explicit topic permission)
    let mut msgs = test_messages();
    client
        .send_messages(
            &stream1_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("additive: send to topic 1 should work (explicit topic permission)");

    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &stream1_id,
                &topic2_id,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "additive: send to topic 2 should be denied (no permission)",
    );

    delete_test_user(root_client, TOPIC_ADDITIVE_USER).await;
}

// =============================================================================
// Test: Missing resource behavior
// Tests behavior when resources don't exist, for both authorized and
// unauthorized users.
// =============================================================================

async fn test_missing_resource_behavior(
    client_factory: &dyn ClientFactory,
    root_client: &IggyClient,
) {
    let nonexistent_stream = Identifier::named("nonexistent-stream").unwrap();
    let nonexistent_topic = Identifier::named("nonexistent-topic").unwrap();
    let existing_stream = Identifier::named(STREAM_1).unwrap();

    // Test: Authorized user accessing non-existent resources
    // Should get ResourceNotFound, not Unauthorized
    const AUTHORIZED_USER: &str = "missing-resource-authorized-user";
    create_test_user(
        root_client,
        AUTHORIZED_USER,
        Some(Permissions {
            global: GlobalPermissions {
                read_streams: true,
                read_topics: true,
                poll_messages: true,
                send_messages: true,
                ..Default::default()
            },
            streams: None,
        }),
    )
    .await;

    let client = login_user(client_factory, AUTHORIZED_USER).await;

    // Non-existent stream
    assert_not_found_or_related(
        client.get_stream(&nonexistent_stream).await,
        "authorized: get non-existent stream",
    );

    // Non-existent topic in existing stream
    assert_not_found_or_related(
        client.get_topic(&existing_stream, &nonexistent_topic).await,
        "authorized: get non-existent topic",
    );

    // Poll from non-existent stream/topic
    assert_not_found_or_related(
        client
            .poll_messages(
                &nonexistent_stream,
                &nonexistent_topic,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await,
        "authorized: poll from non-existent stream/topic",
    );

    // Send to non-existent stream/topic
    let mut msgs = test_messages();
    assert_not_found_or_related(
        client
            .send_messages(
                &nonexistent_stream,
                &nonexistent_topic,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "authorized: send to non-existent stream/topic",
    );

    delete_test_user(root_client, AUTHORIZED_USER).await;

    // Test: Unauthorized user accessing non-existent resources
    // Policy: return empty OK (None) for both not-found and unauthorized to prevent
    // resource enumeration attacks - attacker can't distinguish between the two.
    const UNAUTHORIZED_USER: &str = "missing-resource-unauthorized-user";
    create_test_user(root_client, UNAUTHORIZED_USER, Some(no_permissions())).await;

    let client = login_user(client_factory, UNAUTHORIZED_USER).await;

    // Non-existent stream - returns empty (no leak of existence)
    assert_unauthorized(
        client.get_stream(&nonexistent_stream).await,
        "unauthorized: get non-existent stream",
    );

    // Non-existent topic - returns empty (no leak of existence)
    assert_unauthorized(
        client.get_topic(&existing_stream, &nonexistent_topic).await,
        "unauthorized: get non-existent topic",
    );

    // Poll from non-existent - returns error (write operations don't hide existence)
    assert_unauthorized(
        client
            .poll_messages(
                &nonexistent_stream,
                &nonexistent_topic,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await,
        "unauthorized: poll from non-existent",
    );

    // Send to non-existent - returns error (write operations don't hide existence)
    let mut msgs = test_messages();
    assert_unauthorized(
        client
            .send_messages(
                &nonexistent_stream,
                &nonexistent_topic,
                &Partitioning::partition_id(0),
                &mut msgs,
            )
            .await,
        "unauthorized: send to non-existent",
    );

    delete_test_user(root_client, UNAUTHORIZED_USER).await;

    // Test: Authorized for some streams, accessing non-existent stream
    // This tests the edge case where user has scoped permissions
    let streams = root_client.get_streams().await.expect("get streams");
    let stream1_numeric_id = streams
        .iter()
        .find(|s| s.name == STREAM_1)
        .map(|s| s.id as usize)
        .expect("stream 1 should exist");

    const SCOPED_USER: &str = "missing-resource-scoped-user";
    create_test_user(
        root_client,
        SCOPED_USER,
        Some(Permissions {
            global: GlobalPermissions::default(),
            streams: Some(AHashMap::from([(
                stream1_numeric_id,
                StreamPermissions {
                    read_stream: true,
                    poll_messages: true,
                    send_messages: true,
                    ..Default::default()
                },
            )])),
        }),
    )
    .await;

    let client = login_user(client_factory, SCOPED_USER).await;

    // Accessing non-existent stream - returns empty (no leak of existence)
    assert_unauthorized(
        client.get_stream(&nonexistent_stream).await,
        "scoped: get non-existent stream",
    );

    // Accessing non-existent topic in authorized stream - returns empty
    assert_not_found_or_related(
        client.get_topic(&existing_stream, &nonexistent_topic).await,
        "scoped: get non-existent topic in authorized stream",
    );

    delete_test_user(root_client, SCOPED_USER).await;
}

// =============================================================================
// Helpers
// =============================================================================

fn test_messages() -> Vec<IggyMessage> {
    vec![
        IggyMessage::builder()
            .payload(Bytes::from("test-payload"))
            .build()
            .unwrap(),
    ]
}

fn assert_unauthorized<T: std::fmt::Debug>(result: Result<T, IggyError>, context: &str) {
    // Accept Unauthorized, NotFound errors, or Ok(None) - all are valid responses
    // that don't leak resource existence information.
    let dummy_id = Identifier::numeric(0).unwrap();
    let acceptable_codes = [
        IggyError::Unauthorized.as_code(),
        IggyError::ResourceNotFound(String::new()).as_code(),
        IggyError::StreamIdNotFound(dummy_id.clone()).as_code(),
        IggyError::StreamNameNotFound(String::new()).as_code(),
        IggyError::TopicIdNotFound(dummy_id.clone(), dummy_id.clone()).as_code(),
        IggyError::TopicNameNotFound(String::new(), String::new()).as_code(),
    ];

    match result {
        Err(e) if acceptable_codes.contains(&e.as_code()) => {}
        Err(e) => panic!(
            "{}: expected Unauthorized or NotFound, got {:?} ({})",
            context,
            e,
            e.as_code()
        ),
        Ok(_) => {
            // All protocols return Ok(None) for unauthorized/not-found to prevent resource enumeration
        }
    }
}

fn assert_not_found_or_related<T: std::fmt::Debug>(result: Result<T, IggyError>, context: &str) {
    // Accept various "not found" error codes that indicate resource absence.
    // This is used when an authorized user tries to access a non-existent resource.
    let dummy_id = Identifier::numeric(0).unwrap();
    let not_found_codes = [
        IggyError::ResourceNotFound(String::new()).as_code(),
        IggyError::StreamIdNotFound(dummy_id.clone()).as_code(),
        IggyError::StreamNameNotFound(String::new()).as_code(),
        IggyError::TopicIdNotFound(dummy_id.clone(), dummy_id.clone()).as_code(),
        IggyError::TopicNameNotFound(String::new(), String::new()).as_code(),
    ];

    match result {
        Err(e) if not_found_codes.contains(&e.as_code()) => {}
        Err(e) => panic!(
            "{}: expected NotFound-related error, got {:?} (code: {})",
            context,
            e,
            e.as_code()
        ),
        Ok(_) => {
            // All protocols return Ok(None) for not-found to prevent resource enumeration
        }
    }
}

/// Assert that result is Ok or FeatureUnavailable.
/// Used for stateful operations (join/leave consumer group) that are not supported on HTTP.
fn assert_ok_or_feature_unavailable(result: Result<(), IggyError>, context: &str) {
    match result {
        Ok(()) => {}
        Err(e) if e.as_code() == IggyError::FeatureUnavailable.as_code() => {}
        Err(e) => panic!(
            "{}: expected Ok or FeatureUnavailable, got {:?} (code: {})",
            context,
            e,
            e.as_code()
        ),
    }
}
