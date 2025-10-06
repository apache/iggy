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
    CONSUMER_GROUP_NAME, CONSUMER_KIND, MESSAGES_COUNT, PARTITION_ID, PARTITIONS_COUNT,
    STREAM_NAME, TOPIC_NAME, get_consumer_group, leave_consumer_group,
};
use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::{ClientFactory, assert_clean_system};
use std::str::FromStr;

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);

    let consumer = Consumer {
        kind: CONSUMER_KIND,
        id: Identifier::named("test-consumer").unwrap(),
    };

    // 0. Ping server
    client.ping().await.unwrap();

    // 1. Login as root user
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    // 2. Ensure that streams do not exist
    let streams = client.get_streams().await.unwrap();
    assert!(streams.is_empty());

    // 3. Create the stream
    let stream = client.create_stream(STREAM_NAME).await.unwrap();

    let stream_id = stream.id;
    assert_eq!(stream.name, STREAM_NAME);

    // 4. Get streams and validate that created stream exists
    let streams = client.get_streams().await.unwrap();
    assert_eq!(streams.len(), 1);
    let stream = streams.first().unwrap();
    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, STREAM_NAME);
    assert_eq!(stream.topics_count, 0);
    assert_eq!(stream.size, 0);
    assert_eq!(stream.messages_count, 0);

    // 5. Get stream details by ID
    let stream = client
        .get_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");

    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, STREAM_NAME);
    assert_eq!(stream.topics_count, 0);
    assert!(stream.topics.is_empty());
    assert_eq!(stream.size, 0);
    assert_eq!(stream.messages_count, 0);

    // 6. Get stream details by name
    let stream = client
        .get_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");

    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, STREAM_NAME);

    // 7. Try to create the stream with the same name and validate that it fails
    let create_stream_result = client.create_stream(STREAM_NAME).await;
    assert!(create_stream_result.is_err());

    // 9. Create the topic
    let topic = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            Default::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let topic_id = topic.id;
    assert_eq!(topic.name, TOPIC_NAME);

    // 10. Get topics and validate that created topic exists
    let topics = client
        .get_topics(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
    assert_eq!(topics.len(), 1);
    let topic = topics.first().unwrap();
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, TOPIC_NAME);
    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);
    assert_eq!(topic.compression_algorithm, CompressionAlgorithm::default());
    assert_eq!(topic.size, 0);
    assert_eq!(topic.messages_count, 0);
    assert_eq!(topic.message_expiry, IggyExpiry::NeverExpire);
    assert_eq!(topic.max_topic_size, MaxTopicSize::Unlimited);
    assert_eq!(topic.replication_factor, 1);

    // 11. Get topic details by ID
    let topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, TOPIC_NAME);
    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);
    assert_eq!(topic.partitions.len(), PARTITIONS_COUNT as usize);
    assert_eq!(topic.size, 0);
    assert_eq!(topic.messages_count, 0);
    for (id, topic_partition) in topic.partitions.iter().enumerate() {
        assert_eq!(topic_partition.id, id as u32);
        assert_eq!(topic_partition.segments_count, 1);
        assert_eq!(topic_partition.size, 0);
        assert_eq!(topic_partition.current_offset, 0);
        assert_eq!(topic_partition.messages_count, 0);
    }

    // 12. Get topic details by name
    let topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, TOPIC_NAME);

    // 13. Get stream details and validate that created topic exists
    let stream = client
        .get_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");
    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, STREAM_NAME);
    assert_eq!(stream.topics_count, 1);
    assert_eq!(stream.topics.len(), 1);
    assert_eq!(stream.messages_count, 0);
    let stream_topic = stream.topics.first().unwrap();
    assert_eq!(stream_topic.id, topic.id);
    assert_eq!(stream_topic.name, topic.name);
    assert_eq!(stream_topic.partitions_count, topic.partitions_count);
    assert_eq!(stream_topic.size, 0);
    assert_eq!(stream_topic.messages_count, 0);

    // 15. Try to create the topic with the same name and validate that it fails
    let create_topic_result = client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            Default::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await;
    assert!(create_topic_result.is_err());

    // 17. Send messages to the specific topic and partition
    let mut messages = create_messages();
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    // 18. Poll messages from the specific partition in topic
    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_COUNT,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled_messages.messages.len() as u32, MESSAGES_COUNT);
    for i in 0..MESSAGES_COUNT {
        let offset = i as u64;
        let message = polled_messages.messages.get(i as usize).unwrap();
        assert_message(message, offset);
    }

    // 19. Messages should be also polled in the smaller batches
    let batches_count = 10;
    let batch_length = MESSAGES_COUNT / batches_count;
    for i in 0..batches_count {
        let start_offset = (i * batch_length) as u64;
        let polled_messages = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                Some(PARTITION_ID),
                &consumer,
                &PollingStrategy::offset(start_offset),
                batch_length,
                false,
            )
            .await
            .unwrap();
        assert_eq!(polled_messages.messages.len() as u32, batch_length);
        for i in 0..batch_length as u64 {
            let offset = start_offset + i;
            let message = polled_messages.messages.get(i as usize).unwrap();
            assert_message(message, offset);
        }
    }

    // 20. Get topic details and validate the partition details
    let topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, TOPIC_NAME);
    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);
    assert_eq!(topic.partitions.len(), PARTITIONS_COUNT as usize);
    assert_eq!(topic.size, 89806);
    assert_eq!(topic.messages_count, MESSAGES_COUNT as u64);
    let topic_partition = topic.partitions.get((PARTITION_ID) as usize).unwrap();
    assert_eq!(topic_partition.id, PARTITION_ID);
    assert_eq!(topic_partition.segments_count, 1);
    assert!(topic_partition.size > 0);
    assert_eq!(topic_partition.current_offset, (MESSAGES_COUNT - 1) as u64);
    assert_eq!(topic_partition.messages_count, MESSAGES_COUNT as u64);

    // 21. Ensure that messages do not exist in the second partition in the same topic
    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID + 1),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_COUNT,
            false,
        )
        .await
        .unwrap();
    assert!(polled_messages.messages.is_empty());

    // 22. Get the customer offset and ensure it's none
    let offset = client
        .get_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .expect("Failed to get consumer offset");
    assert!(offset.is_none());

    // 23. Store the consumer offset
    let stored_offset = 10;
    client
        .store_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            stored_offset,
        )
        .await
        .unwrap();

    // 24. Get the existing customer offset and ensure it's the previously stored value
    let offset = client
        .get_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .unwrap()
        .expect("Failed to get consumer offset");
    assert_eq!(offset.partition_id, PARTITION_ID);
    assert_eq!(offset.current_offset, (MESSAGES_COUNT - 1) as u64);
    assert_eq!(offset.stored_offset, stored_offset);

    // 25. Delete the consumer offset
    client
        .delete_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .unwrap();

    // 26. Get the customer offset and ensure it's none
    let offset = client
        .get_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .expect("Failed to get consumer offset");

    assert!(offset.is_none());

    client
        .store_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            stored_offset,
        )
        .await
        .unwrap();

    // 27. Poll messages from the specific partition in topic using next with auto commit
    let messages_count = 10;
    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::next(),
            messages_count,
            true,
        )
        .await
        .unwrap();
    assert_eq!(polled_messages.messages.len() as u32, messages_count);
    let first_offset = polled_messages.messages.first().unwrap().header.offset;
    let last_offset = polled_messages.messages.last().unwrap().header.offset;
    let expected_last_offset = stored_offset + messages_count as u64;
    assert_eq!(first_offset, stored_offset + 1);
    assert_eq!(last_offset, expected_last_offset);

    // 28. Get the existing customer offset and ensure that auto commit during poll has worked
    let offset = client
        .get_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
        )
        .await
        .unwrap()
        .expect("Failed to get consumer offset");
    assert_eq!(offset.partition_id, PARTITION_ID);
    assert_eq!(offset.current_offset, (MESSAGES_COUNT - 1) as u64);
    assert_eq!(offset.stored_offset, expected_last_offset);

    // 29. Get the consumer groups and validate that there are no groups
    let consumer_groups = client
        .get_consumer_groups(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap();

    assert!(consumer_groups.is_empty());

    // 30. Create the consumer group
    let consumer_group = client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    let consumer_group_id = consumer_group.id;
    assert_eq!(consumer_group.name, CONSUMER_GROUP_NAME);

    // 31. Get the consumer groups and validate that there is one group
    let consumer_groups = client
        .get_consumer_groups(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(consumer_groups.len(), 1);
    let consumer_group = consumer_groups.first().unwrap();
    assert_eq!(consumer_group.id, consumer_group_id);
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, 0);

    // 32. Get the consumer group details
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
    assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
    assert_eq!(consumer_group.members_count, 0);
    assert!(consumer_group.members.is_empty());

    // 33. Join the consumer group and then leave it if the feature is available
    let result = client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await;

    match result {
        Ok(_) => {
            let consumer_group = get_consumer_group(&client).await;
            assert_eq!(consumer_group.id, consumer_group_id);
            assert_eq!(consumer_group.partitions_count, PARTITIONS_COUNT);
            assert_eq!(consumer_group.name, CONSUMER_GROUP_NAME);
            assert_eq!(consumer_group.members_count, 1);
            assert_eq!(consumer_group.members.len(), 1);
            let member = &consumer_group.members[0];
            assert_eq!(member.partitions_count, PARTITIONS_COUNT);

            let me = client.get_me().await.unwrap();
            assert!(me.client_id > 0);
            assert_eq!(me.consumer_groups_count, 1);
            assert_eq!(me.consumer_groups.len(), 1);
            let consumer_group = &me.consumer_groups[0];
            assert_eq!(consumer_group.stream_id, stream_id);
            assert_eq!(consumer_group.topic_id, topic_id);
            assert_eq!(consumer_group.group_id, consumer_group_id);

            leave_consumer_group(&client).await;

            let consumer_group = get_consumer_group(&client).await;
            assert_eq!(consumer_group.members_count, 0);
            assert!(consumer_group.members.is_empty());

            let me = client.get_me().await.unwrap();
            assert_eq!(me.consumer_groups_count, 0);
            assert!(me.consumer_groups.is_empty());
        }
        Err(e) => assert_eq!(e.as_code(), IggyError::FeatureUnavailable.as_code()),
    }

    // 34. Get the stats and validate that there is one stream
    let stats = client.get_stats().await.unwrap();
    assert!(!stats.hostname.is_empty());
    assert!(!stats.os_name.is_empty());
    assert!(!stats.os_version.is_empty());
    assert!(!stats.kernel_version.is_empty());
    assert_eq!(stats.streams_count, 1);
    assert_eq!(stats.topics_count, 1);
    assert_eq!(stats.partitions_count, PARTITIONS_COUNT);
    assert_eq!(stats.segments_count, PARTITIONS_COUNT);
    assert_eq!(stats.messages_count, MESSAGES_COUNT as u64);
    assert!(!stats.iggy_server_version.is_empty());
    assert!(stats.iggy_server_semver.is_some());
    let iggy_server_semver = stats.iggy_server_semver.unwrap();
    assert!(iggy_server_semver > 0);

    // 35. Delete the consumer group
    client
        .delete_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();

    // 36. Create new partitions and validate that the number of partitions is increased
    client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            PARTITIONS_COUNT,
        )
        .await
        .unwrap();

    let topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");

    assert_eq!(topic.partitions_count, 2 * PARTITIONS_COUNT);

    // 37. Delete the partitions and validate that the number of partitions is decreased
    client
        .delete_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            PARTITIONS_COUNT,
        )
        .await
        .unwrap();

    let topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");

    assert_eq!(topic.partitions_count, PARTITIONS_COUNT);

    // 38. Update the existing topic and ensure it's updated
    let updated_topic_name = format!("{TOPIC_NAME}-updated");
    let updated_message_expiry = 1000;
    let message_expiry_duration = updated_message_expiry.into();
    let updated_max_topic_size = MaxTopicSize::Custom(IggyByteSize::from_str("2 GB").unwrap());
    let updated_replication_factor = 5;

    client
        .update_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &updated_topic_name,
            CompressionAlgorithm::Gzip,
            Some(updated_replication_factor),
            IggyExpiry::ExpireDuration(message_expiry_duration),
            updated_max_topic_size,
        )
        .await
        .unwrap();

    let updated_topic = client
        .get_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(&updated_topic_name).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");

    assert_eq!(updated_topic.name, updated_topic_name);
    assert_eq!(
        updated_topic.message_expiry,
        IggyExpiry::ExpireDuration(message_expiry_duration)
    );
    assert_eq!(
        updated_topic.compression_algorithm,
        CompressionAlgorithm::Gzip
    );
    assert_eq!(updated_topic.max_topic_size, updated_max_topic_size);
    assert_eq!(updated_topic.replication_factor, updated_replication_factor);

    // 39. Purge the existing topic and ensure it has no messages
    client
        .purge_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(&updated_topic_name).unwrap(),
        )
        .await
        .unwrap();

    let polled_messages = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(&updated_topic_name).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_COUNT,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled_messages.current_offset, 0);
    assert!(polled_messages.messages.is_empty());

    // 40. Update the existing stream and ensure it's updated
    let updated_stream_name = format!("{STREAM_NAME}-updated");

    client
        .update_stream(
            &Identifier::named(STREAM_NAME).unwrap(),
            &updated_stream_name,
        )
        .await
        .unwrap();

    let updated_stream = client
        .get_stream(&Identifier::named(&updated_stream_name).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");

    assert_eq!(updated_stream.name, updated_stream_name);

    // 41. Purge the existing stream and ensure it has no messages
    let mut messages = create_messages();
    client
        .send_messages(
            &Identifier::named(&updated_stream_name).unwrap(),
            &Identifier::named(&updated_topic_name).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    client
        .purge_stream(&Identifier::named(&updated_stream_name).unwrap())
        .await
        .unwrap();

    let polled_messages = client
        .poll_messages(
            &Identifier::named(&updated_stream_name).unwrap(),
            &Identifier::named(&updated_topic_name).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGES_COUNT,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled_messages.current_offset, 0);
    assert!(polled_messages.messages.is_empty());

    // 42. Delete the existing topic and ensure it doesn't exist anymore
    client
        .delete_topic(
            &Identifier::named(&updated_stream_name).unwrap(),
            &Identifier::named(&updated_topic_name).unwrap(),
        )
        .await
        .unwrap();
    let topics = client
        .get_topics(&Identifier::named(&updated_stream_name).unwrap())
        .await
        .unwrap();
    assert!(topics.is_empty());

    // 43. Create the stream
    let stream_name = format!("{STREAM_NAME}-auto");
    let _ = client.create_stream(&stream_name).await.unwrap();

    let stream = client
        .get_stream(&Identifier::named(&stream_name).unwrap())
        .await
        .unwrap()
        .expect("Failed to get stream");

    assert_eq!(stream.name, stream_name);

    // 44. Create the topic
    let topic_name = format!("{TOPIC_NAME}-auto");
    let _ = client
        .create_topic(
            &Identifier::named(&stream_name).unwrap(),
            &topic_name,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let topic = client
        .get_topic(
            &Identifier::named(&stream_name).unwrap(),
            &Identifier::named(&topic_name).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get topic");

    assert_eq!(topic.name, topic_name);

    // 45. Delete the existing streams and ensure there's no streams left
    let streams = client.get_streams().await.unwrap();
    assert_eq!(streams.len(), 2);

    for stream in streams {
        client
            .delete_stream(&Identifier::named(&stream.name).unwrap())
            .await
            .unwrap();
    }

    let streams = client.get_streams().await.unwrap();
    assert!(streams.is_empty());

    // 46. Get clients and ensure that there's 0 (HTTP) or 1 (TCP, QUIC) client
    let clients = client.get_clients().await.unwrap();

    assert!(clients.len() <= 1);

    assert_clean_system(&client).await;
}

fn assert_message(message: &IggyMessage, offset: u64) {
    let expected_payload = create_message_payload(offset);
    assert!(message.header.timestamp > 0);
    assert_eq!(message.header.offset, offset);
    assert_eq!(message.payload, expected_payload);
}

fn create_messages() -> Vec<IggyMessage> {
    let mut messages = Vec::new();
    for offset in 0..MESSAGES_COUNT {
        let id = (offset + 1) as u128;
        let payload = create_message_payload(offset as u64);
        messages.push(
            IggyMessage::builder()
                .id(id)
                .payload(payload)
                .build()
                .expect("Failed to create message"),
        );
    }
    messages
}

fn create_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {offset}"))
}
