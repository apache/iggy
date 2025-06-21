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

use bytes::Bytes;
use iggy::prelude::*;
use integration::test_server::{assert_clean_system, login_root};

pub async fn run(client: &IggyClient) {
    login_root(client).await;

    let stream_name = "test-tls-stream";
    let stream_id = 1;
    client
        .create_stream(stream_name, Some(stream_id))
        .await
        .unwrap();

    let stream = client
        .get_stream(&Identifier::numeric(stream_id).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stream.name, stream_name);

    let topic_name = "test-tls-topic";
    let topic_id = 1;
    client
        .create_topic(
            &Identifier::numeric(stream_id).unwrap(),
            topic_name,
            1,
            CompressionAlgorithm::default(),
            None,
            Some(topic_id),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from("Hello TLS!"))
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            &Partitioning::partition_id(1),
            &mut messages,
        )
        .await
        .unwrap();

    let polled_messages = client
        .poll_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            Some(1),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            true,
        )
        .await
        .unwrap();

    assert_eq!(polled_messages.messages.len(), 1);
    assert_eq!(polled_messages.messages[0].payload.as_ref(), b"Hello TLS!");

    client
        .delete_stream(&Identifier::numeric(stream_id).unwrap())
        .await
        .unwrap();
    assert_clean_system(client).await;
}
