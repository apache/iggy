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

use crate::common::global_context::GlobalContext;
use crate::helpers::cluster::create_and_connect_client;
use crate::helpers::env::{no_quorum_address, root_password, root_username};
use cucumber::{given, then, when};
use iggy::prelude::{Identifier, StreamClient, UserClient};

#[given("I have a cluster node that cannot reach quorum")]
pub async fn given_node_without_quorum(world: &mut GlobalContext) {
    world.server_addr = Some(no_quorum_address());
}

#[when("I sign out")]
pub async fn when_sign_out(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("Client should be available");
    client.logout_user().await.expect("Sign-out should succeed");
}

#[when(regex = r"^I request the stream with ID (\d+)$")]
pub async fn when_request_stream(world: &mut GlobalContext, stream_id: u32) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = Identifier::numeric(stream_id).expect("Stream ID should be valid");
    world.last_call_result = Some(client.get_stream(&stream_id).await.map(|_| ()));
}

#[when("I sign in to that node as the root user")]
pub async fn when_sign_in_to_node(world: &mut GlobalContext) {
    let server_addr = world
        .server_addr
        .as_ref()
        .expect("Server address should be set");
    let client = create_and_connect_client(server_addr).await;
    let result = client
        .login_user(&root_username(), &root_password())
        .await
        .map(|_| ());
    world.client = Some(client);
    world.last_call_result = Some(result);
}

#[then(regex = r"^the call should fail with error code (\d+)$")]
pub async fn then_call_fails_with_code(world: &mut GlobalContext, code: u32) {
    let error = world
        .last_call_result
        .take()
        .expect("Should have made a call")
        .expect_err("Call should have failed");
    assert_eq!(error.as_code(), code, "Call failed with {error}");
}
