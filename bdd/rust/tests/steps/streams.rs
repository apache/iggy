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
use cucumber::{given, then, when};
use iggy::prelude::{Identifier, StreamClient};

#[given("I have no streams in the system")]
pub async fn given_no_streams(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("Client should be available");
    let streams = client
        .get_streams()
        .await
        .expect("Should be able to get streams");
    assert!(
        streams.is_empty(),
        "System should have no streams initially"
    );
}

#[when(regex = r#"^I create a stream with name "(.+)"$"#)]
pub async fn when_create_stream(world: &mut GlobalContext, stream_name: String) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream = client
        .create_stream(&stream_name)
        .await
        .expect("Should be able to create stream");

    world.last_stream_id = Some(stream.id);
    world.last_stream_name = Some(stream.name.clone());
}

#[then("the stream should be created successfully")]
pub async fn then_stream_created_successfully(world: &mut GlobalContext) {
    assert!(
        world.last_stream_id.is_some(),
        "Stream should have been created"
    );
}

#[then(regex = r#"^the stream should have name "(.+)"$"#)]
pub async fn then_stream_has_name(world: &mut GlobalContext, expected_name: String) {
    let stream_name = world
        .last_stream_name
        .as_ref()
        .expect("Stream should exist");
    assert_eq!(
        stream_name, &expected_name,
        "Stream should have expected name"
    );
}

#[given(regex = r#"^a stream with name "(.+)" exists$"#)]
pub async fn given_stream_exists(world: &mut GlobalContext, stream_name: String) {
    when_create_stream(world, stream_name).await;
}

#[when("I get the stream by its numeric ID")]
pub async fn when_get_stream_by_numeric_id(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .last_stream_id
        .expect("Stream should have been created");
    let stream = client
        .get_stream(&Identifier::numeric(stream_id).expect("Stream ID should be valid"))
        .await
        .expect("Should be able to get stream");

    world.last_stream_name = stream.map(|stream| stream.name);
}

#[then(regex = r#"^the returned stream should have name "(.+)"$"#)]
pub async fn then_returned_stream_has_name(world: &mut GlobalContext, expected_name: String) {
    then_stream_has_name(world, expected_name).await;
}

#[when("I list all streams")]
pub async fn when_list_all_streams(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .last_stream_id
        .expect("Stream should have been created");
    let streams = client
        .get_streams()
        .await
        .expect("Should be able to get streams");

    world.last_stream_was_found = streams.iter().any(|stream| stream.id == stream_id);
}

#[then("the stream list should contain the created stream")]
pub async fn then_stream_list_contains_created_stream(world: &mut GlobalContext) {
    assert!(
        world.last_stream_was_found,
        "Stream list should contain the created stream"
    );
}

#[when(regex = r#"^I update the stream name to "(.+)"$"#)]
pub async fn when_update_stream_name(world: &mut GlobalContext, stream_name: String) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .last_stream_id
        .expect("Stream should have been created");
    client
        .update_stream(
            &Identifier::numeric(stream_id).expect("Stream ID should be valid"),
            &stream_name,
        )
        .await
        .expect("Should be able to update stream");
}

#[then(regex = r#"^getting the stream by its numeric ID should return name "(.+)"$"#)]
pub async fn then_get_stream_returns_name(world: &mut GlobalContext, expected_name: String) {
    when_get_stream_by_numeric_id(world).await;
    then_returned_stream_has_name(world, expected_name).await;
}

#[when("I delete the stream by its numeric ID")]
pub async fn when_delete_stream_by_numeric_id(world: &mut GlobalContext) {
    let client = world.client.as_ref().expect("Client should be available");
    let stream_id = world
        .last_stream_id
        .expect("Stream should have been created");
    client
        .delete_stream(&Identifier::numeric(stream_id).expect("Stream ID should be valid"))
        .await
        .expect("Should be able to delete stream");
}

#[then("getting the stream by its numeric ID should return no stream")]
pub async fn then_get_stream_returns_no_stream(world: &mut GlobalContext) {
    when_get_stream_by_numeric_id(world).await;
    assert!(
        world.last_stream_name.is_none(),
        "Deleted stream should not be returned"
    );
}
