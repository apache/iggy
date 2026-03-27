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

//! Authentication scenario test.
//!
//! Validates that all commands require authentication except:
//! - Ping (health check)
//! - LoginUser (authenticate with username/password)
//! - LoginWithPersonalAccessToken (authenticate with PAT)
//!
//! Covers all command codes from the dispatch table to ensure no command
//! is missed when new commands are added.

use crate::server::scenarios::create_client;
use bytes::Bytes;
use iggy::prelude::*;
use iggy_binary_protocol::dispatch::COMMAND_TABLE;
use integration::harness::{TestHarness, login_root};

const STREAM_NAME: &str = "auth-test-stream";
const TOPIC_NAME: &str = "auth-test-topic";

/// Shared test context with identifiers used across command tests.
struct TestContext {
    stream_id: Identifier,
    topic_id: Identifier,
    user_id: Identifier,
    group_id: Identifier,
    consumer: Consumer,
}

impl TestContext {
    fn new() -> Self {
        Self {
            stream_id: Identifier::named(STREAM_NAME).unwrap(),
            topic_id: Identifier::named(TOPIC_NAME).unwrap(),
            user_id: Identifier::numeric(1).unwrap(),
            group_id: Identifier::named("test-group").unwrap(),
            consumer: Consumer::default(),
        }
    }
}

pub async fn run(harness: &TestHarness) {
    let client = create_client(harness).await;

    // Phase 1: Verify ping works without auth
    client.ping().await.expect("ping should work without auth");

    // Phase 2: Verify all protected commands fail without auth
    test_all_commands_require_auth(&client).await;

    // Phase 3: Login and verify commands work
    let identity = login_root(&client).await.expect("login failed");
    assert_eq!(identity.user_id, 0, "root user should have id 0");
    setup_test_resources(&client).await;
    verify_auth_works(&client).await;

    // Phase 4: Logout and verify commands fail again
    client.logout_user().await.expect("logout should succeed");
    test_all_commands_require_auth(&client).await;

    // Phase 5: Test PAT authentication
    login_root(&client).await.expect("login failed");
    let raw_pat = client
        .create_personal_access_token("auth-test-pat", PersonalAccessTokenExpiry::NeverExpire)
        .await
        .expect("should create PAT");

    client.logout_user().await.expect("logout should succeed");
    assert!(
        client.get_streams().await.is_err(),
        "should fail after logout"
    );

    let identity = client
        .login_with_personal_access_token(&raw_pat.token)
        .await
        .expect("PAT login should work");
    assert_eq!(identity.user_id, 0, "PAT should authenticate as root");

    client
        .get_streams()
        .await
        .expect("get_streams should work after PAT login");

    cleanup_test_resources(&client).await;
}

/// Tests all commands require authentication by iterating the dispatch table.
/// New entries in `COMMAND_TABLE` will hit the wildcard arm and panic,
/// forcing an explicit decision about each new command.
async fn test_all_commands_require_auth(client: &IggyClient) {
    use iggy_binary_protocol::codes::*;

    let ctx = TestContext::new();

    for entry in COMMAND_TABLE {
        let code = entry.code;
        let name = entry.name;

        // ================================================================
        // SKIPPED COMMANDS (8 total)
        // ================================================================
        // No auth required
        if matches!(
            code,
            PING_CODE | LOGIN_USER_CODE | LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
        ) {
            continue;
        }
        // Stateful - not supported on HTTP
        if matches!(code, JOIN_CONSUMER_GROUP_CODE | LEAVE_CONSUMER_GROUP_CODE) {
            continue;
        }
        // Special setup required
        if matches!(
            code,
            GET_SNAPSHOT_FILE_CODE | DELETE_SEGMENTS_CODE | LOGOUT_USER_CODE
        ) {
            continue;
        }

        // ================================================================
        // REQUIRES AUTH
        // ================================================================
        let result: Result<(), IggyError> = match code {
            // System
            GET_STATS_CODE => client.get_stats().await.map(|_| ()),
            GET_ME_CODE => client.get_me().await.map(|_| ()),
            GET_CLIENT_CODE => client.get_client(1).await.map(|_| ()),
            GET_CLIENTS_CODE => client.get_clients().await.map(|_| ()),
            GET_CLUSTER_METADATA_CODE => client.get_cluster_metadata().await.map(|_| ()),

            // Users
            GET_USER_CODE => client.get_user(&ctx.user_id).await.map(|_| ()),
            GET_USERS_CODE => client.get_users().await.map(|_| ()),
            CREATE_USER_CODE => client
                .create_user("test", "test", UserStatus::Active, None)
                .await
                .map(|_| ()),
            DELETE_USER_CODE => client.delete_user(&ctx.user_id).await,
            UPDATE_USER_CODE => client.update_user(&ctx.user_id, Some("x"), None).await,
            UPDATE_PERMISSIONS_CODE => client.update_permissions(&ctx.user_id, None).await,
            CHANGE_PASSWORD_CODE => client.change_password(&ctx.user_id, "old", "new").await,

            // PAT
            GET_PERSONAL_ACCESS_TOKENS_CODE => {
                client.get_personal_access_tokens().await.map(|_| ())
            }
            CREATE_PERSONAL_ACCESS_TOKEN_CODE => client
                .create_personal_access_token("x", PersonalAccessTokenExpiry::NeverExpire)
                .await
                .map(|_| ()),
            DELETE_PERSONAL_ACCESS_TOKEN_CODE => client.delete_personal_access_token("x").await,

            // Streams
            GET_STREAM_CODE => client.get_stream(&ctx.stream_id).await.map(|_| ()),
            GET_STREAMS_CODE => client.get_streams().await.map(|_| ()),
            CREATE_STREAM_CODE => client.create_stream("x").await.map(|_| ()),
            DELETE_STREAM_CODE => client.delete_stream(&ctx.stream_id).await,
            UPDATE_STREAM_CODE => client.update_stream(&ctx.stream_id, "x").await,
            PURGE_STREAM_CODE => client.purge_stream(&ctx.stream_id).await,

            // Topics
            GET_TOPIC_CODE => client
                .get_topic(&ctx.stream_id, &ctx.topic_id)
                .await
                .map(|_| ()),
            GET_TOPICS_CODE => client.get_topics(&ctx.stream_id).await.map(|_| ()),
            CREATE_TOPIC_CODE => client
                .create_topic(
                    &ctx.stream_id,
                    "x",
                    1,
                    CompressionAlgorithm::None,
                    None,
                    IggyExpiry::NeverExpire,
                    MaxTopicSize::ServerDefault,
                )
                .await
                .map(|_| ()),
            DELETE_TOPIC_CODE => client.delete_topic(&ctx.stream_id, &ctx.topic_id).await,
            UPDATE_TOPIC_CODE => {
                client
                    .update_topic(
                        &ctx.stream_id,
                        &ctx.topic_id,
                        "x",
                        CompressionAlgorithm::None,
                        None,
                        IggyExpiry::NeverExpire,
                        MaxTopicSize::ServerDefault,
                    )
                    .await
            }
            PURGE_TOPIC_CODE => client.purge_topic(&ctx.stream_id, &ctx.topic_id).await,

            // Partitions
            CREATE_PARTITIONS_CODE => {
                client
                    .create_partitions(&ctx.stream_id, &ctx.topic_id, 1)
                    .await
            }
            DELETE_PARTITIONS_CODE => {
                client
                    .delete_partitions(&ctx.stream_id, &ctx.topic_id, 1)
                    .await
            }

            // Messages
            SEND_MESSAGES_CODE => {
                let mut msgs = vec![
                    IggyMessage::builder()
                        .payload(Bytes::from("x"))
                        .build()
                        .unwrap(),
                ];
                client
                    .send_messages(
                        &ctx.stream_id,
                        &ctx.topic_id,
                        &Partitioning::partition_id(0),
                        &mut msgs,
                    )
                    .await
            }
            POLL_MESSAGES_CODE => client
                .poll_messages(
                    &ctx.stream_id,
                    &ctx.topic_id,
                    Some(0),
                    &ctx.consumer,
                    &PollingStrategy::offset(0),
                    1,
                    false,
                )
                .await
                .map(|_| ()),
            FLUSH_UNSAVED_BUFFER_CODE => {
                client
                    .flush_unsaved_buffer(&ctx.stream_id, &ctx.topic_id, 0, false)
                    .await
            }

            // Consumer Offsets
            GET_CONSUMER_OFFSET_CODE => client
                .get_consumer_offset(&ctx.consumer, &ctx.stream_id, &ctx.topic_id, Some(0))
                .await
                .map(|_| ()),
            STORE_CONSUMER_OFFSET_CODE => {
                client
                    .store_consumer_offset(&ctx.consumer, &ctx.stream_id, &ctx.topic_id, Some(0), 0)
                    .await
            }
            DELETE_CONSUMER_OFFSET_CODE => {
                client
                    .delete_consumer_offset(&ctx.consumer, &ctx.stream_id, &ctx.topic_id, Some(0))
                    .await
            }

            // Consumer Groups
            GET_CONSUMER_GROUP_CODE => client
                .get_consumer_group(&ctx.stream_id, &ctx.topic_id, &ctx.group_id)
                .await
                .map(|_| ()),
            GET_CONSUMER_GROUPS_CODE => client
                .get_consumer_groups(&ctx.stream_id, &ctx.topic_id)
                .await
                .map(|_| ()),
            CREATE_CONSUMER_GROUP_CODE => client
                .create_consumer_group(&ctx.stream_id, &ctx.topic_id, "x")
                .await
                .map(|_| ()),
            DELETE_CONSUMER_GROUP_CODE => {
                client
                    .delete_consumer_group(&ctx.stream_id, &ctx.topic_id, &ctx.group_id)
                    .await
            }

            _ => panic!("Unhandled command code {code} ({name}) in auth test"),
        };

        assert_unauthenticated(result, name);
    }
}

fn assert_unauthenticated(result: Result<(), IggyError>, name: &str) {
    match result {
        Err(e) if e.as_code() == IggyError::FeatureUnavailable.as_code() => {}
        Err(e) if e.as_code() == IggyError::Unauthenticated.as_code() => {}
        Err(e) => panic!(
            "{name}: expected Unauthenticated ({}), got {:?} ({})",
            IggyError::Unauthenticated.as_code(),
            e,
            e.as_code()
        ),
        Ok(()) => panic!("{name}: expected Unauthenticated, got Ok"),
    }
}

/// Verifies a subset of commands work when authenticated.
async fn verify_auth_works(client: &IggyClient) {
    let ctx = TestContext::new();

    // System
    client.get_stats().await.expect("get_stats");
    client.get_clients().await.expect("get_clients");
    client
        .get_cluster_metadata()
        .await
        .expect("get_cluster_metadata");

    // Streams & Topics
    client.get_streams().await.expect("get_streams");
    client.get_stream(&ctx.stream_id).await.expect("get_stream");
    client.get_topics(&ctx.stream_id).await.expect("get_topics");
    client
        .get_topic(&ctx.stream_id, &ctx.topic_id)
        .await
        .expect("get_topic");

    // Messages
    let mut msgs = vec![
        IggyMessage::builder()
            .payload(Bytes::from("test"))
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &ctx.stream_id,
            &ctx.topic_id,
            &Partitioning::partition_id(0),
            &mut msgs,
        )
        .await
        .expect("send_messages");

    client
        .poll_messages(
            &ctx.stream_id,
            &ctx.topic_id,
            Some(0),
            &ctx.consumer,
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .expect("poll_messages");

    // Users & PATs
    client.get_users().await.expect("get_users");
    client.get_personal_access_tokens().await.expect("get_pats");
}

async fn setup_test_resources(client: &IggyClient) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("create topic");
}

async fn cleanup_test_resources(client: &IggyClient) {
    let _ = client.delete_personal_access_token("auth-test-pat").await;
    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .expect("delete stream");
}
