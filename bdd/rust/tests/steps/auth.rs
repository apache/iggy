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
use cucumber::given;
use iggy::prelude::*;
use std::sync::Arc;

#[given("I am authenticated as the root user")]
pub async fn given_authenticated_as_root(world: &mut GlobalContext) {
    let server_addr = world
        .server_addr
        .as_ref()
        .expect("Server should be running")
        .clone();

    let config = TcpClientConfig {
        server_address: server_addr,
        ..TcpClientConfig::default()
    };

    let tcp_client = TcpClient::create(Arc::new(config)).expect("Failed to create TCP client");
    Client::connect(&tcp_client)
        .await
        .expect("Client should connect");

    let client = IggyClient::create(ClientWrapper::Tcp(tcp_client), None, None);

    client.ping().await.expect("Server should respond to ping");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("Failed to login as root");

    world.client = Some(client);
}
