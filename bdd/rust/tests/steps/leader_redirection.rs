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

use crate::common::leader_context::LeaderContext;
use crate::helpers::cluster;
use cucumber::{given, then, when};
use iggy::prelude::*;
use integration::test_server::login_root;
use std::time::Duration;
use tokio::time::sleep;

// ============================================================================
// Background steps for cluster configuration
// ============================================================================

#[given(regex = r"^I have cluster configuration enabled with (\d+) nodes$")]
async fn given_cluster_config(world: &mut LeaderContext, node_count: usize) {
    world.cluster.enabled = true;
    world.cluster.nodes = Vec::with_capacity(node_count);
}

#[given(regex = r"^node (\d+) is configured on port (\d+)$")]
async fn given_node_configured(world: &mut LeaderContext, node_id: u32, port: u16) {
    let node = ClusterNode {
        id: node_id,
        name: format!("node-{}", node_id),
        address: format!("iggy-server:{}", port),
        role: ClusterNodeRole::Follower,
        status: ClusterNodeStatus::Healthy,
    };
    world.add_node(node);
}

#[given(regex = r"^I start server (\d+) on port (\d+) as (leader|follower)$")]
async fn given_start_clustered_server(
    world: &mut LeaderContext,
    node_id: u32,
    port: u16,
    role: String,
) {
    // Clustered server (leader or follower)
    let addr = cluster::resolve_server_address(&role, port);
    world.store_server_addr(role.clone(), addr);

    // Update node role in cluster configuration
    let node_role = match role.as_str() {
        "leader" => ClusterNodeRole::Leader,
        "follower" => ClusterNodeRole::Follower,
        _ => unreachable!("Regex ensures only leader or follower"),
    };

    cluster::update_node_role(&mut world.cluster.nodes, node_id, port, node_role);
}

#[given(regex = r"^I start a single server on port (\d+) without clustering enabled$")]
async fn given_start_single_server(world: &mut LeaderContext, port: u16) {
    // Single server without clustering
    let addr = cluster::resolve_server_address("single", port);
    world.store_server_addr("single".to_string(), addr);
    world.cluster.enabled = false;
}

#[when(regex = r"^I create a client connecting to (follower|leader) on port (\d+)$")]
async fn when_create_client_to_role(world: &mut LeaderContext, role: String, _port: u16) {
    let addr = world
        .get_server_addr(&role)
        .unwrap_or_else(|| panic!("{} server should be configured", role))
        .clone();

    let client = cluster::create_and_connect_client(&addr).await;
    world.store_client("main".to_string(), client);

    // Track redirection expectation
    if role == "leader" {
        world.test_state.redirection_occurred = false;
    }
}

#[when(regex = r"^I create a client connecting directly to leader on port (\d+)$")]
async fn when_create_client_direct_to_leader(world: &mut LeaderContext, port: u16) {
    let addr = world
        .get_server_addr("leader")
        .expect("Leader server should be configured")
        .clone();

    // Verify the leader is on the expected port
    assert!(
        addr.contains(&format!(":{}", port)),
        "Leader should be on port {}, but address is {}",
        port,
        addr
    );

    let client = cluster::create_and_connect_client(&addr).await;
    world.store_client("main".to_string(), client);
    world.test_state.redirection_occurred = false;
}

#[when(regex = r"^I create a client connecting to port (\d+)$")]
async fn when_create_client_to_port(world: &mut LeaderContext, port: u16) {
    let role = cluster::server_type_from_port(port);
    let addr = world
        .get_server_addr(role)
        .unwrap_or_else(|| panic!("Server on port {} should be configured", port))
        .clone();

    let client = cluster::create_and_connect_client(&addr).await;
    world.store_client("main".to_string(), client);
}

#[when(regex = r"^I create client ([A-Z]) connecting to port (\d+)$")]
async fn when_create_named_client(world: &mut LeaderContext, client_name: String, port: u16) {
    // Determine which server based on port
    let role = cluster::server_type_from_port(port);
    let addr = world
        .get_server_addr(role)
        .unwrap_or_else(|| panic!("Server on port {} should be configured", port))
        .clone();

    let client = cluster::create_and_connect_client(&addr).await;
    world.store_client(client_name, client);
}

#[when(regex = r"^(?:I|both clients) authenticate as root user$")]
async fn when_authenticate_root(world: &mut LeaderContext) {
    // Determine if we're authenticating all clients or just "main"
    let client_names: Vec<String> = if world.clients.len() > 1 {
        world.clients.keys().cloned().collect()
    } else {
        vec!["main".to_string()]
    };

    for client_name in client_names {
        let client = world
            .get_client(&client_name)
            .unwrap_or_else(|| panic!("Client {} should be created", client_name));

        login_root(client).await;

        // Small delay between multiple authentications to avoid race conditions
        if world.clients.len() > 1 {
            sleep(Duration::from_millis(100)).await;
        }
    }
}

#[when(regex = r#"^I create a stream named "(.+)"$"#)]
async fn when_create_stream(world: &mut LeaderContext, stream_name: String) {
    let client = world
        .get_client("main")
        .expect("Client should be available");

    let stream = client
        .create_stream(&stream_name)
        .await
        .expect("Should be able to create stream");

    world.test_state.last_stream_id = Some(stream.id);
    world.test_state.last_stream_name = Some(stream.name.clone());
}

#[then("the stream should be created successfully on the leader")]
async fn then_stream_created_successfully(world: &mut LeaderContext) {
    assert!(
        world.test_state.last_stream_id.is_some(),
        "Stream should have been created on leader"
    );
}

#[then(
    regex = r"^the client should (?:automatically redirect to leader on|stay connected to|redirect to) port (\d+)$"
)]
async fn then_verify_client_port(world: &mut LeaderContext, expected_port: u16) {
    let client = world.get_client("main").expect("Client should exist");

    // Verify connection to expected port
    cluster::verify_client_connection(client, expected_port)
        .await
        .expect("Connection verification should succeed");

    // Check cluster metadata if available
    if let Ok(Some(leader)) = cluster::verify_leader_in_metadata(client).await {
        // If we found a leader and we're connected to the leader port, mark redirection
        if cluster::extract_port_from_address(&leader.address) == Some(expected_port) {
            world.test_state.redirection_occurred = true;
        }
    }
}

#[then(regex = r"^client ([A-Z]) should (?:stay connected to|redirect to) port (\d+)$")]
async fn then_verify_named_client_port(
    world: &mut LeaderContext,
    client_name: String,
    expected_port: u16,
) {
    let client = world
        .get_client(&client_name)
        .unwrap_or_else(|| panic!("Client {} should exist", client_name));

    cluster::verify_client_connection(client, expected_port)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Client {} connection verification should succeed",
                client_name
            )
        });

    if let Ok(Some(leader)) = cluster::verify_leader_in_metadata(client).await {
        assert!(
            cluster::extract_port_from_address(&leader.address).is_some(),
            "Client {} should find valid leader in cluster metadata",
            client_name
        );
    }
}

#[then("the client should not perform any redirection")]
async fn then_no_redirection(world: &mut LeaderContext) {
    assert!(
        !world.test_state.redirection_occurred,
        "No redirection should occur when connecting directly to leader"
    );
}

#[then(regex = r"^the connection should remain on port (\d+)$")]
async fn then_connection_remains(world: &mut LeaderContext, port: u16) {
    let client = world.get_client("main").expect("Client should exist");

    cluster::verify_client_connection(client, port)
        .await
        .expect("Should remain on original port");

    assert!(
        !world.test_state.redirection_occurred,
        "Connection should not have been redirected"
    );
}

#[then("the client should connect successfully without redirection")]
async fn then_connect_without_redirection(world: &mut LeaderContext) {
    let client = world.get_client("main").expect("Client should exist");

    client
        .ping()
        .await
        .expect("Client should be able to ping server");

    assert!(
        !world.test_state.redirection_occurred,
        "No redirection should occur without clustering"
    );
}

#[then("both clients should be using the same server")]
async fn then_both_use_same_server(world: &mut LeaderContext) {
    let client_a = world.get_client("A").expect("Client A should exist");
    let client_b = world.get_client("B").expect("Client B should exist");

    // Get connection info for both clients
    let conn_info_a = client_a.get_connection_info().await;
    let conn_info_b = client_b.get_connection_info().await;

    // Verify both clients are connected to the same server
    assert_eq!(
        conn_info_a.server_address, conn_info_b.server_address,
        "Both clients should be connected to the same server"
    );

    // Verify both can communicate
    client_a
        .ping()
        .await
        .expect("Client A should be able to ping");
    client_b
        .ping()
        .await
        .expect("Client B should be able to ping");

    // Verify cluster metadata consistency if available
    if let (Ok(Some(leader_a)), Ok(Some(leader_b))) = (
        cluster::verify_leader_in_metadata(client_a).await,
        cluster::verify_leader_in_metadata(client_b).await,
    ) {
        assert_eq!(
            leader_a.address, leader_b.address,
            "Both clients should see the same leader"
        );
    }
}
