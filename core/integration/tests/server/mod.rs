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

mod authentication_scenario;
mod bench_scenario;
mod concurrent_scenario;
mod consumer_group_auto_commit_reconnection_scenario;
mod consumer_group_join_scenario;
mod consumer_group_multiple_clients_polling_messages_scenario;
mod consumer_group_offset_cleanup_scenario;
mod consumer_group_single_client_polling_messages_scenario;
mod consumer_timestamp_polling_scenario;
mod create_message_payload;
mod cross_protocol_pat_scenario;
mod delete_segments_scenario;
pub mod encryption_scenario;
mod message_headers_scenario;
mod message_size_scenario;
mod offset_retrieval_scenario;
mod permissions_scenario;
pub mod read_during_persistence_scenario;
mod segment_rotation_race_scenario;
mod single_message_per_batch_scenario;
pub mod stale_client_consumer_group_scenario;
mod stream_size_validation_scenario;
mod system_scenario;
mod timestamp_retrieval_scenario;
mod tls_scenario;
mod user_scenario;

use iggy::prelude::*;
use integration::harness::delete_user;

pub(crate) const PARTITION_ID: u32 = 0;
pub(crate) const STREAM_NAME: &str = "test-stream";
pub(crate) const TOPIC_NAME: &str = "test-topic";
pub(crate) const PARTITIONS_COUNT: u32 = 3;
pub(crate) const CONSUMER_GROUP_NAME: &str = "test-consumer-group";
pub(crate) const USERNAME_1: &str = "user1";
pub(crate) const USERNAME_2: &str = "user2";
pub(crate) const USERNAME_3: &str = "user3";
#[allow(dead_code)]
pub(crate) const CONSUMER_KIND: ConsumerKind = ConsumerKind::Consumer;
pub(crate) const MESSAGES_COUNT: u32 = 1337;

pub(crate) async fn get_consumer_group(client: &IggyClient) -> ConsumerGroupDetails {
    client
        .get_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get consumer group")
}

pub(crate) async fn join_consumer_group(client: &IggyClient) {
    client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();
}

pub(crate) async fn cleanup(system_client: &IggyClient, delete_users: bool) {
    if delete_users {
        delete_user(system_client, USERNAME_1).await;
        delete_user(system_client, USERNAME_2).await;
        delete_user(system_client, USERNAME_3).await;
    }
    system_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
