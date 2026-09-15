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

use iggy::prelude::*;
use iggy::stream_builder::{IggyConsumerConfig, IggyStreamConsumer};
use integration::iggy_harness;

const STREAM_NAME: &str = "stream-builder-stream";
const TOPIC_NAME: &str = "stream-builder-topic";
const CONSUMER_NAME: &str = "stream-builder-consumer";

#[iggy_harness]
async fn given_consumer_config_when_building_twice_should_reuse_the_named_stream_and_topic(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.expect("Root client");
    let config = consumer_config();
    let stream_id = Identifier::named(STREAM_NAME).unwrap();

    for build in 1..=2 {
        let mut consumer = IggyStreamConsumer::build(&client, &config)
            .await
            .unwrap_or_else(|err| {
                panic!("Stream consumer should initialize on build {build}: {err}")
            });

        let streams = client.get_streams().await.expect("Read streams");
        let stream_names: Vec<&str> = streams.iter().map(|stream| stream.name.as_str()).collect();
        assert_eq!(stream_names, vec![STREAM_NAME]);

        let topics = client.get_topics(&stream_id).await.expect("Read topics");
        let topic_names: Vec<&str> = topics.iter().map(|topic| topic.name.as_str()).collect();
        assert_eq!(topic_names, vec![TOPIC_NAME]);

        consumer.shutdown().await.expect("Shut down consumer");
    }
}

fn consumer_config() -> IggyConsumerConfig {
    IggyConsumerConfig::builder()
        .stream_name(STREAM_NAME)
        .topic_name(TOPIC_NAME)
        .consumer_name(CONSUMER_NAME)
        .consumer_kind(ConsumerKind::Consumer)
        .auto_commit(AutoCommit::Disabled)
        .batch_length(1)
        .create_stream_if_not_exists(true)
        .create_topic_if_not_exists(true)
        .partitions_count(1)
        .polling_interval("1ms".parse().unwrap())
        .polling_strategy(PollingStrategy::first())
        .polling_retry_interval(NonZeroIggyDuration::ONE_SECOND)
        .init_retries(0)
        .init_interval(NonZeroIggyDuration::ONE_SECOND)
        .build()
}
