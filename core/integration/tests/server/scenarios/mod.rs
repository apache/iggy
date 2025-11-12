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

pub mod bench_scenario;
pub mod concurrent_scenario;
pub mod consumer_group_join_scenario;
pub mod consumer_group_with_multiple_clients_polling_messages_scenario;
pub mod consumer_group_with_single_client_polling_messages_scenario;
pub mod create_message_payload;
pub mod delete_segments_scenario;
pub mod encryption_scenario;
pub mod message_headers_scenario;
pub mod message_size_scenario;
pub mod stream_size_validation_scenario;
pub mod system_scenario;
pub mod tcp_tls_scenario;
pub mod user_scenario;
pub mod websocket_tls_scenario;

use iggy::prelude::*;
use integration::test_server::{ClientFactory, delete_user};

const PARTITION_ID: u32 = 0;
const STREAM_NAME: &str = "test-stream";
const TOPIC_NAME: &str = "test-topic";
const PARTITIONS_COUNT: u32 = 3;
const CONSUMER_GROUP_NAME: &str = "test-consumer-group";
const USERNAME_1: &str = "user1";
const USERNAME_2: &str = "user2";
const USERNAME_3: &str = "user3";
const CONSUMER_KIND: ConsumerKind = ConsumerKind::Consumer;
const MESSAGES_COUNT: u32 = 1337;

async fn create_client(client_factory: &dyn ClientFactory) -> IggyClient {
    let client = client_factory.create_client().await;
    IggyClient::create(client, None, None)
}

async fn get_consumer_group(client: &IggyClient) -> ConsumerGroupDetails {
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

async fn join_consumer_group(client: &IggyClient) {
    client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();
}

async fn leave_consumer_group(client: &IggyClient) {
    client
        .leave_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();
}

async fn cleanup(system_client: &IggyClient, delete_users: bool) {
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
