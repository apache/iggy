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

use futures::StreamExt;
use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::{Duration, timeout};

const STREAM_NAME: &str = "delete-offset-stream";
const TOPIC_NAME: &str = "delete-offset-topic";
const CONSUMER_NAME: &str = "delete-offset-consumer";
const ASSIGNED_PARTITION_ID: u32 = 3;
const POLL_TIMEOUT: Duration = Duration::from_secs(10);

#[iggy_harness]
async fn standalone_consumer_deletes_partition_zero_on_none_in_delete_offset(
    harness: &TestHarness,
) {
    let client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();

    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(4),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    // send messages to all partitions
    for partition_id in [0, ASSIGNED_PARTITION_ID] {
        let mut messages = vec![
            IggyMessage::from_str("message_1").unwrap(),
            IggyMessage::from_str("message_2").unwrap(),
            IggyMessage::from_str("message_3").unwrap(),
        ];
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // get consumer assigned to last partition
    let mut consumer = client
        .consumer(
            CONSUMER_NAME,
            STREAM_NAME,
            TOPIC_NAME,
            ASSIGNED_PARTITION_ID,
        )
        .unwrap()
        .auto_commit(AutoCommit::Disabled)
        .batch_length(1)
        .build();
    consumer.init().await.unwrap();

    timeout(POLL_TIMEOUT, consumer.next())
        .await
        .expect("Consumer should receive a message before timeout")
        .expect("Consumer stream should remain open")
        .expect("Consumer should poll from the assigned partition");
    assert_eq!(consumer.partition_id(), ASSIGNED_PARTITION_ID);

    // set offsets for assigned partition and partition 0 manually
    consumer
        .store_offset(2, Some(ASSIGNED_PARTITION_ID))
        .await
        .unwrap();
    consumer.store_offset(1, Some(0)).await.unwrap();

    // Check that the offset was stored on the server as intended.
    let mut stored_offset =
        fetch_stored_offset(&client, &stream_id, &topic_id, ASSIGNED_PARTITION_ID).await;
    assert_eq!(stored_offset, Some(2));

    let mut stored_offset_p0 = fetch_stored_offset(&client, &stream_id, &topic_id, 0).await;
    assert_eq!(stored_offset_p0, Some(1));

    // assigned partition was untouched
    stored_offset =
        fetch_stored_offset(&client, &stream_id, &topic_id, ASSIGNED_PARTITION_ID).await;
    assert_eq!(stored_offset, Some(2));

    // delete with none and check both again
    consumer.delete_offset(None).await.unwrap();
    // this fails, since partition 0 got deleted
    stored_offset_p0 = fetch_stored_offset(&client, &stream_id, &topic_id, 0).await;
    assert_eq!(stored_offset_p0, Some(1));
}

/// Read the offset the server has stored for the consumer.
async fn fetch_stored_offset(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: u32,
) -> Option<u64> {
    client
        .get_consumer_offset(
            &Consumer::new(Identifier::named(CONSUMER_NAME).unwrap()),
            stream_id,
            topic_id,
            Some(partition_id),
        )
        .await
        .unwrap()
        .map(|offset| offset.stored_offset)
}
