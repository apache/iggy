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

use crate::server::scenarios::{
    consumer_group_join_scenario, consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario, create_message_payload,
    message_headers_scenario, stream_size_validation_scenario, system_scenario, user_scenario,
};
use integration::{quic_client::QuicClientFactory, test_server::TestServer};
use serial_test::parallel;

#[tokio::test]
#[parallel]
async fn system_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    system_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn user_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    user_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn message_headers_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    message_headers_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn create_message_payload_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    create_message_payload::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_join_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    consumer_group_join_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_single_client_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    consumer_group_with_single_client_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_multiple_clients_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    consumer_group_with_multiple_clients_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn stream_size_validation_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_quic_udp_addr().unwrap();
    let client_factory = QuicClientFactory { server_addr };
    stream_size_validation_scenario::run(&client_factory).await;
}
