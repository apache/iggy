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

use crate::streaming::common::test_setup::TestSetup;
use crate::streaming::create_messages;
use ahash::AHashMap;
use iggy::prelude::*;
use server::state::system::StreamState;
use server::streaming::polling_consumer::PollingConsumer;
use server::streaming::segments::IggyMessagesBatchMut;
use server::streaming::streams::stream::Stream;
use tokio::fs;

#[tokio::test]
async fn should_persist_stream_with_topics_directory_and_info_file() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{stream_id}");
        let stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );

        stream.persist().await.unwrap();

        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;
    }
}

#[tokio::test]
async fn should_load_existing_stream_from_disk() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{stream_id}");
        let stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );
        stream.persist().await.unwrap();
        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;

        let mut loaded_stream = Stream::empty(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );
        let state = StreamState {
            id: stream_id,
            name: name.clone(),
            created_at: IggyTimestamp::now(),
            topics: AHashMap::new(),
        };
        loaded_stream.load(state).await.unwrap();

        assert_eq!(loaded_stream.stream_id, stream.stream_id);
        assert_eq!(loaded_stream.name, stream.name);
        assert_eq!(loaded_stream.path, stream.path);
        assert_eq!(loaded_stream.topics_path, stream.topics_path);
    }
}

#[tokio::test]
async fn should_delete_existing_stream_from_disk() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{stream_id}");
        let stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );
        stream.persist().await.unwrap();
        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;

        stream.delete().await.unwrap();

        assert!(fs::metadata(&stream.path).await.is_err());
    }
}

#[tokio::test]
async fn should_purge_existing_stream_on_disk() {
    let setup = TestSetup::init().await;
    setup.create_streams_directory().await;
    let stream_ids = get_stream_ids();
    for stream_id in stream_ids {
        let name = format!("test-{stream_id}");
        let mut stream = Stream::create(
            stream_id,
            &name,
            setup.config.clone(),
            setup.storage.clone(),
        );
        stream.persist().await.unwrap();
        assert_persisted_stream(&stream.path, &setup.config.topic.path).await;

        let topic_id = 1;
        stream
            .create_topic(
                Some(topic_id),
                "test",
                1,
                IggyExpiry::NeverExpire,
                Default::default(),
                MaxTopicSize::ServerDefault,
                1,
            )
            .unwrap();

        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let messages_size = messages
            .iter()
            .map(|msg| msg.get_size_bytes().as_bytes_u32())
            .sum::<u32>();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
        let topic = stream
            .get_topic(&Identifier::numeric(topic_id).unwrap())
            .unwrap();
        let partition_id = topic
            .calculate_partition_id(&Partitioning::partition_id(1))
            .unwrap();
        topic.append_messages(partition_id, batch).await.unwrap();
        let (_, loaded_messages) = topic
            .get_messages(
                PollingConsumer::Consumer(1, 1),
                1,
                PollingStrategy::offset(0),
                100,
            )
            .await
            .unwrap();

        assert_eq!(loaded_messages.count(), messages_count);

        // TODO: Fixme
        /*
        stream.purge().await.unwrap();
        let (metadata, loaded_messages) = topic
            .get_messages(
                PollingConsumer::Consumer(1, 1),
                1,
                PollingStrategy::offset(0),
                100,
            )
            .await
            .unwrap();
        assert_eq!(metadata.current_offset, 0);
        assert!(loaded_messages.is_empty());
        */
    }
}

async fn assert_persisted_stream(stream_path: &str, topics_directory: &str) {
    let stream_metadata = fs::metadata(stream_path).await.unwrap();
    assert!(stream_metadata.is_dir());
    let topics_path = format!("{stream_path}/{topics_directory}");
    let topics_metadata = fs::metadata(&topics_path).await.unwrap();
    assert!(topics_metadata.is_dir());
}

fn get_stream_ids() -> Vec<u32> {
    vec![1, 2, 3, 5, 10, 100, 1000, 99999]
}
