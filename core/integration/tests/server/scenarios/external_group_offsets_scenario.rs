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

//! Offsets of a group managed outside Iggy, kept under an Iggy consumer group nobody joins.

use crate::server::scenarios::{PARTITION_ID, STREAM_NAME, TOPIC_NAME, cleanup};
use iggy::prelude::*;
use integration::harness::{TestHarness, assert_clean_system, disk};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const GROUP: &str = "kafka.cg.orders";
const CLEANUP_WAIT: Duration = Duration::from_secs(30);

pub async fn run(harness: &TestHarness) {
    let client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let stream_id = client.create_stream(STREAM_NAME).await.unwrap().id;
    let topic_id = client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap()
        .id;
    client
        .create_consumer_group(&stream, &topic, GROUP)
        .await
        .unwrap();
    let external = Consumer::external_group(Identifier::named(GROUP).unwrap());
    let offset = |consumer: Consumer| {
        let client = &client;
        let (stream, topic) = (&stream, &topic);
        async move {
            client
                .get_consumer_offset(&consumer, stream, topic, Some(PARTITION_ID))
                .await
                .unwrap()
                .map(|info| info.stored_offset)
        }
    };

    // The partition is empty and nobody joined: both still store, as sent.
    for value in [0, 42] {
        client
            .store_consumer_offset(&external, &stream, &topic, Some(PARTITION_ID), value)
            .await
            .unwrap();
        assert_eq!(offset(external.clone()).await, Some(value));
    }
    assert_eq!(
        offset(Consumer::group(Identifier::named(GROUP).unwrap())).await,
        None,
        "external offsets must not share the group's own key"
    );

    let polled = client
        .poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &external,
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .unwrap_err();
    assert_eq!(polled.as_code(), IggyError::FeatureUnavailable.as_code());

    let missing = Consumer::external_group(Identifier::named("kafka.cg.missing").unwrap());
    let refused = client
        .store_consumer_offset(&missing, &stream, &topic, Some(PARTITION_ID), 0)
        .await
        .unwrap_err();
    assert_eq!(
        refused.as_code(),
        IggyError::ConsumerGroupNameNotFound(String::new(), Identifier::default()).as_code()
    );

    client
        .delete_consumer_offset(&external, &stream, &topic, Some(PARTITION_ID))
        .await
        .unwrap();
    assert_eq!(offset(external.clone()).await, None);

    // Deleting the group deletes its external offsets, files included.
    client
        .store_consumer_offset(&external, &stream, &topic, Some(PARTITION_ID), 7)
        .await
        .unwrap();
    client
        .delete_consumer_group(&stream, &topic, &Identifier::named(GROUP).unwrap())
        .await
        .unwrap();
    let data_path = harness.server().data_path();
    let deadline = Instant::now() + CLEANUP_WAIT;
    loop {
        let ids = disk::consumer_offset_file_ids(
            &data_path,
            stream_id,
            topic_id,
            PARTITION_ID,
            ConsumerKind::ExternalGroup,
        )
        .expect("external group offset directory");
        if ids.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "deleting the group left external offsets {ids:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }

    cleanup(&client, false).await;
    assert_clean_system(&client).await;
}
