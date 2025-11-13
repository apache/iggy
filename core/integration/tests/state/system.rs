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
use iggy::prelude::IggyExpiry;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_stream::DeleteStream;
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
    let create_user = CreateUser {
        username: "user".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    };
    let create_user_clone = CreateUser {
        username: "user".to_string(),
        password: "secret".to_string(),
        status: Default::default(),
        permissions: None,
    };

    let stream1_id = 1;
    let create_stream1 = CreateStream {
        name: "stream1".to_string(),
    };

    let create_stream1_clone = CreateStream {
        name: "stream1".to_string(),
    };

    let topic1_id = 1;
    let create_topic1 = CreateTopic {
        stream_id: stream1_id.try_into().unwrap(),
        partitions_count: 1,
        compression_algorithm: Default::default(),
        message_expiry: Default::default(),
        max_topic_size: Default::default(),
        name: "topic1".to_string(),
        replication_factor: None,
    };

    let create_topic1_clone = CreateTopic {
        stream_id: stream1_id.try_into().unwrap(),
        partitions_count: 1,
        compression_algorithm: Default::default(),
        message_expiry: Default::default(),
        max_topic_size: Default::default(),
        name: "topic1".to_string(),
        replication_factor: None,
    };

    let stream2_id = 2;
    let create_stream2 = CreateStream {
        name: "stream2".to_string(),
    };

    let topic2_id = 2;
    let create_topic2 = CreateTopic {
        stream_id: stream2_id.try_into().unwrap(),
        partitions_count: 1,
        compression_algorithm: Default::default(),
        message_expiry: Default::default(),
        max_topic_size: Default::default(),
        name: "topic2".to_string(),
        replication_factor: None,
    };

    let create_partitions = CreatePartitions {
        stream_id: stream1_id.try_into().unwrap(),
        topic_id: topic1_id.try_into().unwrap(),
        partitions_count: 2,
    };

    let delete_stream = DeleteStream {
        stream_id: stream2_id.try_into().unwrap(),
    };

    let create_personal_access_token = CreatePersonalAccessTokenWithHash {
        command: CreatePersonalAccessToken {
            name: "test".to_string(),
            expiry: IggyExpiry::NeverExpire,
        },
        hash: "hash".to_string(),
    };

    let create_personal_access_token_clone = CreatePersonalAccessTokenWithHash {
        command: CreatePersonalAccessToken {
            name: "test".to_string(),
            expiry: IggyExpiry::NeverExpire,
        },
        hash: "hash".to_string(),
    };

    let group_id = 1;
    let create_consumer_group = CreateConsumerGroup {
        stream_id: stream1_id.try_into().unwrap(),
        topic_id: topic1_id.try_into().unwrap(),
        name: "test".to_string(),
    };

    let create_consumer_group_clone = CreateConsumerGroup {
        stream_id: stream1_id.try_into().unwrap(),
        topic_id: topic1_id.try_into().unwrap(),
        name: "test".to_string(),
    };

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
    assert_eq!(user.username, create_user_clone.username);
    assert_eq!(user.password_hash, create_user_clone.password);
    assert_eq!(user.personal_access_tokens.len(), 1);

    let personal_access_token = user
        .personal_access_tokens
        .remove(&create_personal_access_token_clone.command.name)
        .unwrap();
    assert_eq!(
        personal_access_token.token_hash,
        create_personal_access_token_clone.hash
    );
    assert_eq!(
        personal_access_token.name,
        create_personal_access_token_clone.command.name
    );

    assert_eq!(system.streams.len(), 1);
    let mut stream = system.streams.remove(&stream1_id).unwrap();
    assert_eq!(stream.id, stream1_id);
    assert_eq!(stream.name, create_stream1_clone.name);
    assert_eq!(stream.topics.len(), 1);

    let mut topic = stream.topics.remove(&topic1_id).unwrap();
    assert_eq!(topic.id, topic1_id);
    assert_eq!(topic.name, create_topic1_clone.name);
    assert_eq!(topic.partitions.len(), 3);

    assert_eq!(topic.consumer_groups.len(), 1);
    let consumer_group = topic.consumer_groups.remove(&group_id).unwrap();

    assert_eq!(consumer_group.id, group_id);
    assert_eq!(consumer_group.name, create_consumer_group_clone.name);
}
