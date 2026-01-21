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
//! Uses exhaustive matching on `ServerCommand` to ensure no command is missed
//! when new commands are added.

use crate::server::scenarios::create_client;
use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use server::binary::command::ServerCommand;
use strum::IntoEnumIterator;

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

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;

    // Phase 1: Verify ping works without auth
    client.ping().await.expect("ping should work without auth");

    // Phase 2: Verify all protected commands fail without auth
    test_all_commands_require_auth(&client).await;

    // Phase 3: Login and verify commands work
    let identity = login_root(&client).await;
    assert_eq!(identity.user_id, 0, "root user should have id 0");
    setup_test_resources(&client).await;
    verify_auth_works(&client).await;

    // Phase 4: Logout and verify commands fail again
    client.logout_user().await.expect("logout should succeed");
    test_all_commands_require_auth(&client).await;

    // Phase 5: Test PAT authentication
    login_root(&client).await;
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

/// Tests all commands require authentication using exhaustive matching.
/// If a new command is added to ServerCommand, this match becomes non-exhaustive.
async fn test_all_commands_require_auth(client: &IggyClient) {
    let ctx = TestContext::new();

    for cmd in ServerCommand::iter() {
        let (name, result): (&str, Result<(), IggyError>) = match cmd {
            // ================================================================
            // NO AUTH REQUIRED (3 commands)
            // ================================================================
            ServerCommand::Ping(_)
            | ServerCommand::LoginUser(_)
            | ServerCommand::LoginWithPersonalAccessToken(_) => continue,

            // ================================================================
            // STATEFUL - NOT SUPPORTED ON HTTP (2 commands)
            // ================================================================
            ServerCommand::JoinConsumerGroup(_) | ServerCommand::LeaveConsumerGroup(_) => continue,

            // ================================================================
            // SPECIAL SETUP REQUIRED (3 commands)
            // ================================================================
            ServerCommand::GetSnapshot(_)
            | ServerCommand::DeleteSegments(_)
            | ServerCommand::LogoutUser(_) => continue,

            // ================================================================
            // REQUIRES AUTH (39 commands)
            // ================================================================

            // System
            ServerCommand::GetStats(_) => ("GetStats", client.get_stats().await.map(|_| ())),
            ServerCommand::GetMe(_) => ("GetMe", client.get_me().await.map(|_| ())),
            ServerCommand::GetClient(_) => ("GetClient", client.get_client(1).await.map(|_| ())),
            ServerCommand::GetClients(_) => ("GetClients", client.get_clients().await.map(|_| ())),
            ServerCommand::GetClusterMetadata(_) => (
                "GetClusterMetadata",
                client.get_cluster_metadata().await.map(|_| ()),
            ),

            // Users
            ServerCommand::GetUser(_) => {
                ("GetUser", client.get_user(&ctx.user_id).await.map(|_| ()))
            }
            ServerCommand::GetUsers(_) => ("GetUsers", client.get_users().await.map(|_| ())),
            ServerCommand::CreateUser(_) => (
                "CreateUser",
                client
                    .create_user("test", "test", UserStatus::Active, None)
                    .await
                    .map(|_| ()),
            ),
            ServerCommand::DeleteUser(_) => ("DeleteUser", client.delete_user(&ctx.user_id).await),
            ServerCommand::UpdateUser(_) => (
                "UpdateUser",
                client.update_user(&ctx.user_id, Some("x"), None).await,
            ),
            ServerCommand::UpdatePermissions(_) => (
                "UpdatePermissions",
                client.update_permissions(&ctx.user_id, None).await,
            ),
            ServerCommand::ChangePassword(_) => (
                "ChangePassword",
                client.change_password(&ctx.user_id, "old", "new").await,
            ),

            // PAT
            ServerCommand::GetPersonalAccessTokens(_) => (
                "GetPersonalAccessTokens",
                client.get_personal_access_tokens().await.map(|_| ()),
            ),
            ServerCommand::CreatePersonalAccessToken(_) => (
                "CreatePersonalAccessToken",
                client
                    .create_personal_access_token("x", PersonalAccessTokenExpiry::NeverExpire)
                    .await
                    .map(|_| ()),
            ),
            ServerCommand::DeletePersonalAccessToken(_) => (
                "DeletePersonalAccessToken",
                client.delete_personal_access_token("x").await,
            ),

            // Streams
            ServerCommand::GetStream(_) => (
                "GetStream",
                client.get_stream(&ctx.stream_id).await.map(|_| ()),
            ),
            ServerCommand::GetStreams(_) => ("GetStreams", client.get_streams().await.map(|_| ())),
            ServerCommand::CreateStream(_) => {
                ("CreateStream", client.create_stream("x").await.map(|_| ()))
            }
            ServerCommand::DeleteStream(_) => {
                ("DeleteStream", client.delete_stream(&ctx.stream_id).await)
            }
            ServerCommand::UpdateStream(_) => (
                "UpdateStream",
                client.update_stream(&ctx.stream_id, "x").await,
            ),
            ServerCommand::PurgeStream(_) => {
                ("PurgeStream", client.purge_stream(&ctx.stream_id).await)
            }

            // Topics
            ServerCommand::GetTopic(_) => (
                "GetTopic",
                client
                    .get_topic(&ctx.stream_id, &ctx.topic_id)
                    .await
                    .map(|_| ()),
            ),
            ServerCommand::GetTopics(_) => (
                "GetTopics",
                client.get_topics(&ctx.stream_id).await.map(|_| ()),
            ),
            ServerCommand::CreateTopic(_) => (
                "CreateTopic",
                client
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
            ),
            ServerCommand::DeleteTopic(_) => (
                "DeleteTopic",
                client.delete_topic(&ctx.stream_id, &ctx.topic_id).await,
            ),
            ServerCommand::UpdateTopic(_) => (
                "UpdateTopic",
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
                    .await,
            ),
            ServerCommand::PurgeTopic(_) => (
                "PurgeTopic",
                client.purge_topic(&ctx.stream_id, &ctx.topic_id).await,
            ),

            // Partitions
            ServerCommand::CreatePartitions(_) => (
                "CreatePartitions",
                client
                    .create_partitions(&ctx.stream_id, &ctx.topic_id, 1)
                    .await,
            ),
            ServerCommand::DeletePartitions(_) => (
                "DeletePartitions",
                client
                    .delete_partitions(&ctx.stream_id, &ctx.topic_id, 1)
                    .await,
            ),

            // Messages
            ServerCommand::SendMessages(_) => {
                let mut msgs = vec![
                    IggyMessage::builder()
                        .payload(Bytes::from("x"))
                        .build()
                        .unwrap(),
                ];
                (
                    "SendMessages",
                    client
                        .send_messages(
                            &ctx.stream_id,
                            &ctx.topic_id,
                            &Partitioning::partition_id(0),
                            &mut msgs,
                        )
                        .await,
                )
            }
            ServerCommand::PollMessages(_) => (
                "PollMessages",
                client
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
            ),
            ServerCommand::FlushUnsavedBuffer(_) => (
                "FlushUnsavedBuffer",
                client
                    .flush_unsaved_buffer(&ctx.stream_id, &ctx.topic_id, 0, false)
                    .await,
            ),

            // Consumer Offsets
            ServerCommand::GetConsumerOffset(_) => (
                "GetConsumerOffset",
                client
                    .get_consumer_offset(&ctx.consumer, &ctx.stream_id, &ctx.topic_id, Some(0))
                    .await
                    .map(|_| ()),
            ),
            ServerCommand::StoreConsumerOffset(_) => (
                "StoreConsumerOffset",
                client
                    .store_consumer_offset(&ctx.consumer, &ctx.stream_id, &ctx.topic_id, Some(0), 0)
                    .await,
            ),
            ServerCommand::DeleteConsumerOffset(_) => (
                "DeleteConsumerOffset",
                client
                    .delete_consumer_offset(&ctx.consumer, &ctx.stream_id, &ctx.topic_id, Some(0))
                    .await,
            ),

            // Consumer Groups
            ServerCommand::GetConsumerGroup(_) => (
                "GetConsumerGroup",
                client
                    .get_consumer_group(&ctx.stream_id, &ctx.topic_id, &ctx.group_id)
                    .await
                    .map(|_| ()),
            ),
            ServerCommand::GetConsumerGroups(_) => (
                "GetConsumerGroups",
                client
                    .get_consumer_groups(&ctx.stream_id, &ctx.topic_id)
                    .await
                    .map(|_| ()),
            ),
            ServerCommand::CreateConsumerGroup(_) => (
                "CreateConsumerGroup",
                client
                    .create_consumer_group(&ctx.stream_id, &ctx.topic_id, "x")
                    .await
                    .map(|_| ()),
            ),
            ServerCommand::DeleteConsumerGroup(_) => (
                "DeleteConsumerGroup",
                client
                    .delete_consumer_group(&ctx.stream_id, &ctx.topic_id, &ctx.group_id)
                    .await,
            ),
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
