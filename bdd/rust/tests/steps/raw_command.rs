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
use bytes::Bytes;
use cucumber::{then, when};
use iggy::prelude::IggyError;

#[when(regex = r"^I send a raw command with code (\d+) and an empty payload$")]
pub async fn when_send_raw_command(world: &mut GlobalContext, code: u32) {
    let client = world.client.as_ref().expect("Client should be available");
    world.last_raw_result = Some(client.send_binary_request(code, Bytes::new()).await);
}

#[then("the raw command should succeed with an empty response")]
pub async fn then_raw_command_succeeds_with_empty_response(world: &mut GlobalContext) {
    let response = world
        .last_raw_result
        .as_ref()
        .expect("Should have sent a raw command")
        .as_ref()
        .expect("Raw command should have succeeded");
    assert!(response.is_empty(), "Response should be empty");
}

#[then("the raw command should succeed with a non-empty response")]
pub async fn then_raw_command_succeeds_with_non_empty_response(world: &mut GlobalContext) {
    let response = world
        .last_raw_result
        .as_ref()
        .expect("Should have sent a raw command")
        .as_ref()
        .expect("Raw command should have succeeded");
    assert!(!response.is_empty(), "Response should not be empty");
}

#[then("the raw command should fail with an invalid command error")]
pub async fn then_raw_command_fails_with_invalid_command_error(world: &mut GlobalContext) {
    let error = world
        .last_raw_result
        .as_ref()
        .expect("Should have sent a raw command")
        .as_ref()
        .expect_err("Raw command should have failed");
    assert_eq!(*error, IggyError::InvalidCommand);
}
