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

//! Each sink start connects an Iggy client of its own, so a parked poll of one
//! sink cannot hold back the polls and offset stores of another. A member id
//! is the lowest free slot of its group, so it repeats across groups and after
//! a member leaves. These tests therefore tell the members apart by the
//! clients that the server lists, each with the number of groups it is in.

use iggy::prelude::{Identifier, IggyClient, StreamClient, SystemClient};
use iggy_common::ClientInfo;
use iggy_connector_sdk::api::{ConnectorStatus, SinkInfoResponse};
use integration::harness::{seeds, wait_for_consumer_group_assignment};
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const FIRST_SINK_KEY: &str = "stdout_first";
const SECOND_SINK_KEY: &str = "stdout_second";
const RESTART_SINK_KEY: &str = "stdout_restart";
const PARTIAL_SETUP_SINK_KEY: &str = "stdout_partial_setup";
const DEFAULT_SINK_GROUP_PREFIX: &str = "iggy-connect-sink-";
/// A stopped consumer can finish its shutdown after the 5 s that a sink stop
/// waits for it.
const MEMBERSHIP_BUDGET: Duration = Duration::from_secs(30);
/// Well under the 36 s session timeout of the test server, 1.2 x its 30 s
/// heartbeat interval. A closed connection alone would pass after its session
/// expires, so only a logout passes within this budget.
const LOGOUT_BUDGET: Duration = Duration::from_secs(10);
const CLIENTS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const INSTANCE_CLOSE_BUDGET: Duration = Duration::from_secs(10);
const STDOUT_SINK_OPENED: &str = "Opened stdout sink connector with ID: ";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/sink_clients.toml")),
    seed = seeds::connector_stream
)]
async fn given_two_started_sinks_when_clients_listed_should_show_one_client_per_sink_group(
    harness: &TestHarness,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    for key in [FIRST_SINK_KEY, SECOND_SINK_KEY] {
        wait_for_sink_group(&client, key, 1, MEMBERSHIP_BUDGET).await;
    }

    let clients = client
        .get_clients()
        .await
        .expect("clients should be listed");
    assert_eq!(
        single_group_clients(&clients).len(),
        2,
        "each sink group should have a client of its own as its member, got: {clients:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/sink_restart.toml")),
    seed = seeds::connector_stream
)]
async fn given_running_sink_when_restarted_through_api_should_rejoin_group_with_new_client(
    harness: &TestHarness,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    wait_for_sink_group(&client, RESTART_SINK_KEY, 1, MEMBERSHIP_BUDGET).await;
    let before = client
        .get_clients()
        .await
        .expect("clients should be listed");
    let [old_member] = single_group_clients(&before)[..] else {
        panic!("the sink client should be the only group member, got: {before:?}");
    };

    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let response = Client::new()
        .post(format!("{api_url}/sinks/{RESTART_SINK_KEY}/restart"))
        .send()
        .await
        .expect("restart request should complete");
    assert_eq!(response.status().as_u16(), 204, "restart should succeed");

    let old_client_gone =
        |clients: &[ClientInfo]| clients.iter().all(|info| info.client_id != old_member);
    let after = wait_for_clients(&client, old_client_gone).await;
    assert!(
        old_client_gone(&after),
        "the restart should shut down the old sink client {old_member}, got: {after:?}"
    );
    assert_eq!(
        after.len(),
        before.len(),
        "the restart should replace the sink client, not add one: {before:?} -> {after:?}"
    );
    assert_eq!(
        single_group_clients(&after).len(),
        1,
        "the new sink client should be the only group member, got: {after:?}"
    );
    wait_for_sink_group(&client, RESTART_SINK_KEY, 1, MEMBERSHIP_BUDGET).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/sink_partial_setup.toml")),
    seed = seeds::connector_stream
)]
async fn given_sink_with_failing_second_stream_when_runtime_starts_should_leave_no_group_member(
    harness: &TestHarness,
) {
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let sink: SinkInfoResponse = Client::new()
        .get(format!("{api_url}/sinks/{PARTIAL_SETUP_SINK_KEY}"))
        .send()
        .await
        .expect("sink request should complete")
        .error_for_status()
        .expect("sink endpoint should succeed")
        .json()
        .await
        .expect("sink response should decode");
    assert_eq!(sink.status, ConnectorStatus::Error);
    let last_error = sink
        .last_error
        .expect("the failed sink should expose a last_error");
    assert!(
        last_error.message.contains("no_such_stream")
            && last_error.message.contains("was not found"),
        "last_error should name the missing second stream, got: {}",
        last_error.message
    );

    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    wait_for_sink_group(&client, PARTIAL_SETUP_SINK_KEY, 0, LOGOUT_BUDGET).await;
    let clients = client
        .get_clients()
        .await
        .expect("clients should be listed");
    assert!(
        clients.iter().all(|info| info.consumer_groups_count == 0),
        "no client should stay a group member after the failed setup, got: {clients:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/sink_restart.toml")),
    seed = seeds::connector_stream
)]
async fn given_running_sink_when_restart_setup_fails_should_close_the_new_plugin_instance(
    harness: &TestHarness,
) {
    let client = harness
        .root_client()
        .await
        .expect("root client should connect");
    wait_for_sink_group(&client, RESTART_SINK_KEY, 1, MEMBERSHIP_BUDGET).await;
    // A start opens the plugin before it builds the consumers, and a missing
    // stream fails that second step.
    client
        .delete_stream(&Identifier::named(seeds::names::STREAM).expect("valid stream name"))
        .await
        .expect("stream should be deleted");

    let runtime = harness
        .connectors_runtime()
        .expect("connector runtime should be available");
    let response = Client::new()
        .post(format!(
            "{}/sinks/{RESTART_SINK_KEY}/restart",
            runtime.http_url()
        ))
        .send()
        .await
        .expect("restart request should complete");
    assert!(
        !response.status().is_success(),
        "the restart should fail without its stream, got: {}",
        response.status()
    );

    let opened = last_opened_stdout_sink_id(&runtime.collect_logs().0)
        .expect("the restart should open a new plugin instance");
    let closed = format!("Stdout sink connector with ID: {opened} is closed.");
    let deadline = Instant::now() + INSTANCE_CLOSE_BUDGET;
    while !runtime.collect_logs().0.contains(&closed) && Instant::now() < deadline {
        sleep(CLIENTS_POLL_INTERVAL).await;
    }
    assert!(
        runtime.collect_logs().0.contains(&closed),
        "the failed restart should close the plugin instance {opened} that it opened"
    );
}

/// The id of the stdout sink instance that the runtime opened last.
fn last_opened_stdout_sink_id(logs: &str) -> Option<u32> {
    let start = logs.rfind(STDOUT_SINK_OPENED)? + STDOUT_SINK_OPENED.len();
    logs[start..].split(',').next()?.parse().ok()
}

async fn wait_for_sink_group(
    client: &IggyClient,
    sink_key: &str,
    members_count: u32,
    budget: Duration,
) {
    let stream_id = Identifier::named(seeds::names::STREAM).expect("valid stream name");
    let topic_id = Identifier::named(seeds::names::TOPIC).expect("valid topic name");
    let group_id = Identifier::named(&format!("{DEFAULT_SINK_GROUP_PREFIX}{sink_key}"))
        .expect("valid group name");
    wait_for_consumer_group_assignment(
        client,
        &stream_id,
        &topic_id,
        &group_id,
        members_count,
        budget,
    )
    .await;
}

/// Ids of the clients that belong to exactly one consumer group.
fn single_group_clients(clients: &[ClientInfo]) -> Vec<u32> {
    clients
        .iter()
        .filter(|info| info.consumer_groups_count == 1)
        .map(|info| info.client_id)
        .collect()
}

/// Lists the clients until `ready` holds or the budget runs out, and returns
/// the last list.
async fn wait_for_clients(
    client: &IggyClient,
    ready: impl Fn(&[ClientInfo]) -> bool,
) -> Vec<ClientInfo> {
    let deadline = Instant::now() + MEMBERSHIP_BUDGET;
    loop {
        let clients = client
            .get_clients()
            .await
            .expect("clients should be listed");
        if ready(&clients) || Instant::now() >= deadline {
            return clients;
        }
        sleep(CLIENTS_POLL_INTERVAL).await;
    }
}
