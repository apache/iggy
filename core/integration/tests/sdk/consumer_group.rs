// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::str::FromStr;
use std::time::Duration;

use futures::StreamExt;
use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::timeout;

const STREAM_NAME: &str = "consumer-group-rejoin-stream";
const TOPIC_NAME: &str = "consumer-group-rejoin-topic";
const CONSUMER_GROUP_NAME: &str = "consumer-group-rejoin-group";
const CONSUMER_USERNAME: &str = "consumer-group-rejoin-user";
const CONSUMER_PASSWORD: &str = "password123";
const CONSUMER_REJOIN_TIMEOUT: Duration = Duration::from_secs(10);

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn consumer_group_retries_rejoin_after_failure(harness: &TestHarness) {
    let root_client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();
    let user_id = Identifier::named(CONSUMER_USERNAME).unwrap();
    let consumer_permissions = Permissions {
        global: GlobalPermissions {
            read_streams: true,
            ..Default::default()
        },
        streams: None,
    };

    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    root_client
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();
    root_client
        .create_user(
            CONSUMER_USERNAME,
            CONSUMER_PASSWORD,
            UserStatus::Active,
            Some(consumer_permissions.clone()),
        )
        .await
        .unwrap();

    let consumer_client = harness.new_client().await.expect("Failed to create client");
    consumer_client
        .login_user(CONSUMER_USERNAME, CONSUMER_PASSWORD)
        .await
        .unwrap();

    let mut consumer = consumer_client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .auto_join_consumer_group()
        .build();
    consumer.init().await.unwrap();

    let group = root_client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 1);
    assert_eq!(group.members.len(), 1);

    let mut messages = vec![IggyMessage::from_str("message").unwrap()];
    root_client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    consumer_client
        .leave_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    let group = root_client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 0);
    assert!(group.members.is_empty());

    root_client
        .update_permissions(
            &user_id,
            Some(Permissions {
                global: GlobalPermissions {
                    poll_messages: true,
                    ..Default::default()
                },
                streams: None,
            }),
        )
        .await
        .unwrap();

    let rejoin_result = timeout(CONSUMER_REJOIN_TIMEOUT, consumer.next())
        .await
        .expect("Consumer rejoin should fail before timeout")
        .expect("Consumer stream should remain open");
    assert!(matches!(rejoin_result, Err(IggyError::Unauthorized)));

    root_client
        .update_permissions(&user_id, Some(consumer_permissions))
        .await
        .unwrap();

    let received = timeout(CONSUMER_REJOIN_TIMEOUT, consumer.next())
        .await
        .expect("Consumer should recover before timeout")
        .expect("Consumer stream should remain open")
        .expect("Consumer should rejoin after its membership is revoked");
    assert_eq!(received.message.payload, "message");

    let group = root_client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 1);
    assert_eq!(group.members.len(), 1);
}
