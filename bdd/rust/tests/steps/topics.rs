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

use crate::common::global_context::GlobalContext;
use cucumber::{then, when};
use iggy::prelude::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, TopicClient};

#[when(
    regex = r"^I create a topic with ID (\d+) and name (.+) in stream (\d+) with (\d+) partitions$"
)]
pub async fn when_create_topic(
    world: &mut GlobalContext,
    topic_id: u32,
    topic_name: String,
    stream_id: u32,
    partitions_count: u32,
) {
    let client = world.client.as_ref().expect("Client should be available");
    let topic = client
        .create_topic(
            &Identifier::numeric(stream_id).unwrap(),
            &topic_name,
            partitions_count,
            CompressionAlgorithm::default(),
            None,
            Some(topic_id),
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("Should be able to create topic");

    world.last_topic_id = Some(topic.id);
    world.last_topic_name = Some(topic.name.clone());
    world.last_topic_partitions = Some(topic.partitions_count);
}

#[then("the topic should be created successfully")]
pub async fn then_topic_created_successfully(world: &mut GlobalContext) {
    assert!(
        world.last_topic_id.is_some(),
        "Topic should have been created"
    );
}

#[then(regex = r"^the topic should have ID (\d+) and name (.+)$")]
pub async fn then_topic_has_id_and_name(
    world: &mut GlobalContext,
    expected_id: u32,
    expected_name: String,
) {
    let topic_id = world.last_topic_id.expect("Topic should exist");
    let topic_name = world.last_topic_name.as_ref().expect("Topic should exist");
    assert_eq!(topic_id, expected_id, "Topic should have expected ID");
    assert_eq!(
        topic_name, &expected_name,
        "Topic should have expected name"
    );
}

#[then(regex = r"^the topic should have (\d+) partitions$")]
pub async fn then_topic_has_partitions(world: &mut GlobalContext, expected_partitions: u32) {
    let topic_partitions = world.last_topic_partitions.expect("Topic should exist");
    assert_eq!(
        topic_partitions, expected_partitions,
        "Topic should have expected number of partitions"
    );
}
