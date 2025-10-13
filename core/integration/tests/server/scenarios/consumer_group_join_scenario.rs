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

use crate::server::scenarios::{
    CONSUMER_GROUP_NAME, PARTITIONS_COUNT, STREAM_NAME, TOPIC_NAME, USERNAME_1, USERNAME_2,
    USERNAME_3, cleanup, create_client, join_consumer_group,
};
use iggy::clients::client::IggyClient;
use iggy::prelude::ClientInfoDetails;
use iggy::prelude::CompressionAlgorithm;
use iggy::prelude::ConsumerGroupDetails;
use iggy::prelude::Identifier;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use iggy::prelude::{ConsumerGroupClient, StreamClient, SystemClient, TopicClient};
use integration::test_server::{
    ClientFactory, assert_clean_system, create_user, login_root, login_user,
};

pub async fn run(client_factory: &dyn ClientFactory) {
    let system_client = create_client(client_factory).await;

    let client1 = create_client(client_factory).await;
    let client2 = create_client(client_factory).await;
    let client3 = create_client(client_factory).await;

    login_root(&system_client).await;

    // 1. Create the stream
    let stream = system_client.create_stream(STREAM_NAME).await.unwrap();

    let stream_id = stream.id;

    // 2. Create the topic
    let topic = system_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let topic_id = topic.id;

    // 3. Create the consumer group
    let consumer_group = system_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    let consumer_group_id = consumer_group.id;

    // 4. Create the users for all clients
    create_user(&system_client, USERNAME_1).await;
    create_user(&system_client, USERNAME_2).await;
    create_user(&system_client, USERNAME_3).await;

    // 5. Login all the clients
    login_user(&client1, USERNAME_1).await;
    login_user(&client2, USERNAME_2).await;
    login_user(&client3, USERNAME_3).await;

    // 5. Join the consumer group by client 1
    join_consumer_group(&client1).await;

    // 5. Get client1 info and validate that it contains the single consumer group
    let _ =
        get_me_and_validate_consumer_groups(&client1, stream_id, topic_id, consumer_group_id).await;

    // 6. Validate that the consumer group has 1 member and this member has all partitions assigned
    let consumer_group =
        get_consumer_group_and_validate_members(&system_client, 1, consumer_group_id).await;
    let member = &consumer_group.members[0];
    assert_eq!(member.partitions_count, PARTITIONS_COUNT);
    assert_eq!(member.partitions.len() as u32, PARTITIONS_COUNT);

    // 7. Join the consumer group by client 2
    join_consumer_group(&client2).await;

    // 8. Validate that client 2 contains the single consumer group
    get_me_and_validate_consumer_groups(&client2, stream_id, topic_id, consumer_group_id).await;

    // 9. Validate that the consumer group has 2 members and partitions are distributed between them
    let consumer_group =
        get_consumer_group_and_validate_members(&system_client, 2, consumer_group_id).await;
    let member1 = &consumer_group.members[0];
    let member2 = &consumer_group.members[1];
    assert!(member1.partitions_count >= 1 && member1.partitions_count < PARTITIONS_COUNT);
    assert!(member2.partitions_count >= 1 && member2.partitions_count < PARTITIONS_COUNT);
    assert_eq!(
        member1.partitions_count + member2.partitions_count,
        PARTITIONS_COUNT
    );

    // 10. Join the consumer group by client 3
    join_consumer_group(&client3).await;

    // 11. Validate that client 3 contains the single consumer group
    get_me_and_validate_consumer_groups(&client3, stream_id, topic_id, consumer_group_id).await;

    // 12. Validate that the consumer group has 3 members and partitions are equally distributed between them
    let consumer_group =
        get_consumer_group_and_validate_members(&system_client, 3, consumer_group_id).await;
    let member1 = &consumer_group.members[0];
    let member2 = &consumer_group.members[1];
    let member3 = &consumer_group.members[2];
    assert_eq!(member1.partitions_count, 1);
    assert_eq!(member2.partitions_count, 1);
    assert_eq!(member3.partitions_count, 1);
    assert_ne!(member1.partitions[0], member2.partitions[0]);
    assert_ne!(member1.partitions[0], member3.partitions[0]);
    assert_ne!(member2.partitions[0], member3.partitions[0]);

    cleanup(&system_client, true).await;
    assert_clean_system(&system_client).await;
}

async fn get_me_and_validate_consumer_groups(
    client: &IggyClient,
    stream_id: u32,
    topic_id: u32,
    consumer_group_id: u32,
) -> ClientInfoDetails {
    let client_info = client.get_me().await.unwrap();

    assert!(client_info.client_id > 0);
    assert_eq!(client_info.consumer_groups_count, 1);
    assert_eq!(client_info.consumer_groups.len(), 1);

    let consumer_group = &client_info.consumer_groups[0];
    assert_eq!(consumer_group.stream_id, stream_id);
    assert_eq!(consumer_group.topic_id, topic_id);
    assert_eq!(consumer_group.group_id, consumer_group_id);

    client_info
}

async fn get_consumer_group_and_validate_members(
    client: &IggyClient,
    members_count: u32,
    consumer_group_id: u32,
) -> ConsumerGroupDetails {
    let consumer_group = client
        .get_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get consumer group");

    assert_eq!(consumer_group.id, consumer_group_id);
    assert_eq!(consumer_group.name, CONSUMER_GROUP_NAME);
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, members_count);
    assert_eq!(consumer_group.members.len() as u32, members_count);

    consumer_group
}
