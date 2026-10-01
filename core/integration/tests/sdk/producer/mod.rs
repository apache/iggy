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

mod background;

use bytes::Bytes;
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use integration::iggy_harness;

const PARTITION_ID: u32 = 0;
const STREAM_NAME: &str = "test-stream-producer";
const TOPIC_NAME: &str = "test-topic-producer";
const PARTITIONS_COUNT: u32 = 3;

fn create_message_payload(offset: u64) -> Bytes {
    Bytes::from(format!("message {offset}"))
}

#[iggy_harness]
async fn given_persisted_auto_creation_when_producer_sends_should_confirm_and_poll(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.unwrap();
    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .topic_durability(Durability::Persisted)
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .build();
    producer.init().await.unwrap();
    let payload = create_message_payload(0);
    let message = IggyMessage::builder()
        .id(1)
        .payload(payload.clone())
        .build()
        .unwrap();
    let response = producer.send_one(message).await.unwrap();
    assert_eq!(response.confirmations.len(), 1);
    assert_eq!(response.confirmations[0].base_offset, 0);
    let polled = client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .unwrap();
    assert_eq!(polled.messages.len(), 1);
    assert_eq!(polled.messages[0].payload, payload);
    cleanup(&client).await;
}

async fn init_system(client: &IggyClient) {
    // 1. Create the stream
    client.create_stream(STREAM_NAME).await.unwrap();

    // 2. Create the topic
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS_COUNT),
                message_expiry: Some(IggyExpiry::NeverExpire),
                durability: iggy::prelude::Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
}

async fn cleanup(system_client: &IggyClient) {
    system_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
