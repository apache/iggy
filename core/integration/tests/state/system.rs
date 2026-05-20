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

use crate::state::StateSetup;
use iggy_binary_protocol::requests::{
    consumer_groups::CreateConsumerGroupRequest, partitions::CreatePartitionsRequest,
    personal_access_tokens::CreatePersonalAccessTokenRequest, streams::CreateStreamRequest,
    streams::DeleteStreamRequest, topics::CreateTopicRequest, users::CreateUserRequest,
};
use iggy_binary_protocol::{WireIdentifier, WireName};
use server::state::command::EntryCommand;
use server::state::models::{
    CreateConsumerGroupWithId, CreatePersonalAccessTokenWithHash, CreateStreamWithId,
    CreateTopicWithId, CreateUserWithId,
};
use server::state::system::SystemState;

#[compio::test]
async fn should_be_initialized_based_on_state_entries() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    state.init().await.unwrap();

    let user_id = 0;
    let create_user = CreateUserRequest {
        username: WireName::new("user").unwrap(),
        password: "secret".to_string(),
        status: 1, // Active
        permissions: None,
    };

    let stream1_id = 1u32;
    let create_stream1 = CreateStreamRequest {
        name: WireName::new("stream1").unwrap(),
    };

    let topic1_id = 1u32;
    let create_topic1 = CreateTopicRequest {
        stream_id: WireIdentifier::numeric(stream1_id),
        partitions_count: 1,
        compression_algorithm: 1, // None compression
        message_expiry: 0,        // NeverExpire
        max_topic_size: 0,        // ServerDefault
        replication_factor: 0,    // None
        name: WireName::new("topic1").unwrap(),
    };

    let stream2_id = 2u32;
    let create_stream2 = CreateStreamRequest {
        name: WireName::new("stream2").unwrap(),
    };

    let topic2_id = 2u32;
    let create_topic2 = CreateTopicRequest {
        stream_id: WireIdentifier::numeric(stream2_id),
        partitions_count: 1,
        compression_algorithm: 1,
        message_expiry: 0,
        max_topic_size: 0,
        replication_factor: 0,
        name: WireName::new("topic2").unwrap(),
    };

    let create_partitions = CreatePartitionsRequest {
        stream_id: WireIdentifier::numeric(stream1_id),
        topic_id: WireIdentifier::numeric(topic1_id),
        partitions_count: 2,
    };

    let delete_stream = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(stream2_id),
    };

    let create_personal_access_token = CreatePersonalAccessTokenWithHash {
        command: CreatePersonalAccessTokenRequest {
            name: WireName::new("test").unwrap(),
            expiry: 0, // NeverExpire
        },
        hash: "hash".to_string(),
    };

    let create_consumer_group = CreateConsumerGroupRequest {
        stream_id: WireIdentifier::numeric(stream1_id),
        topic_id: WireIdentifier::numeric(topic1_id),
        name: WireName::new("test").unwrap(),
    };

    let group_id = 1u32;

    state
        .apply(
            user_id,
            &EntryCommand::CreateUser(CreateUserWithId {
                user_id,
                command: create_user,
            }),
        )
        .await
        .unwrap();
    state
        .apply(
            user_id,
            &EntryCommand::CreateStream(CreateStreamWithId {
                stream_id: stream1_id,
                command: create_stream1,
            }),
        )
        .await
        .unwrap();
    state
        .apply(
            user_id,
            &EntryCommand::CreateTopic(CreateTopicWithId {
                topic_id: topic1_id,
                command: create_topic1,
            }),
        )
        .await
        .unwrap();
    state
        .apply(
            user_id,
            &EntryCommand::CreateStream(CreateStreamWithId {
                stream_id: stream2_id,
                command: create_stream2,
            }),
        )
        .await
        .unwrap();
    state
        .apply(
            user_id,
            &EntryCommand::CreateTopic(CreateTopicWithId {
                topic_id: topic2_id,
                command: create_topic2,
            }),
        )
        .await
        .unwrap();
    state
        .apply(user_id, &EntryCommand::CreatePartitions(create_partitions))
        .await
        .unwrap();
    state
        .apply(user_id, &EntryCommand::DeleteStream(delete_stream))
        .await
        .unwrap();
    state
        .apply(
            user_id,
            &EntryCommand::CreatePersonalAccessToken(create_personal_access_token),
        )
        .await
        .unwrap();
    state
        .apply(
            user_id,
            &EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId {
                group_id,
                command: create_consumer_group,
            }),
        )
        .await
        .unwrap();

    let entries = state.load_entries().await.unwrap();
    assert_eq!(entries.len(), 9);

    let mut system = SystemState::init(entries).await.unwrap();

    assert_eq!(system.users.len(), 1);
    let mut user = system.users.remove(&user_id).unwrap();
    assert_eq!(user.id, user_id);
    assert_eq!(user.username, "user");
    assert_eq!(user.password_hash, "secret");
    assert_eq!(user.personal_access_tokens.len(), 1);

    let personal_access_token = user.personal_access_tokens.remove("test").unwrap();
    assert_eq!(personal_access_token.token_hash, "hash");
    assert_eq!(personal_access_token.name, "test");

    assert_eq!(system.streams.len(), 1);
    let mut stream = system.streams.remove(&stream1_id).unwrap();
    assert_eq!(stream.id, stream1_id);
    assert_eq!(stream.name, "stream1");
    assert_eq!(stream.topics.len(), 1);

    let mut topic = stream.topics.remove(&topic1_id).unwrap();
    assert_eq!(topic.id, topic1_id);
    assert_eq!(topic.name, "topic1");
    assert_eq!(topic.partitions.len(), 3);

    assert_eq!(topic.consumer_groups.len(), 1);
    let consumer_group = topic.consumer_groups.remove(&group_id).unwrap();

    assert_eq!(consumer_group.id, group_id);
    assert_eq!(consumer_group.name, "test");
}
