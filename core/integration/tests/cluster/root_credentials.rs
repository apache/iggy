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

use iggy::prelude::*;
use iggy_binary_protocol::requests::users::CreateUserRequest;
use iggy_binary_protocol::{HEADER_SIZE, Operation, PrepareHeader, WireDecode};
use integration::harness::TestHarness;
use integration::harness::disk::leader_node_index_via;
use integration::iggy_harness;
use std::path::Path;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout};

#[iggy_harness(cluster_nodes = 3, manual_start)]
async fn given_different_replica_credentials_when_bootstrapping_and_failing_over_should_keep_the_leaders_root(
    harness: &mut TestHarness,
) {
    for node in 1..3 {
        harness
            .node_mut(node)
            .add_env("IGGY_ROOT_USERNAME", format!("unused-root-{node}"));
        harness
            .node_mut(node)
            .add_env("IGGY_ROOT_PASSWORD", format!("unused-root-password-{node}"));
    }
    harness.node_mut(2).remove_env("IGGY_ROOT_USERNAME");
    harness.node_mut(2).remove_env("IGGY_ROOT_PASSWORD");
    harness.start().await.expect("bootstrap the cluster");
    let leader = leader_node_index_via(harness, 0).await;
    assert_eq!(leader, 0, "the first leader supplies the root credentials");
    let root = read_root_creation(&harness.node(leader).data_path());
    assert_eq!(root.username.as_str(), DEFAULT_ROOT_USERNAME);
    assert_ne!(root.password, DEFAULT_ROOT_PASSWORD);
    assert!(root.password.starts_with("$argon2"));
    for node in 0..3 {
        assert_root_login(harness, node).await;
        let replicated = read_root_creation(&harness.node(node).data_path());
        assert_eq!(replicated.username, root.username);
        assert!(
            replicated.password == root.password,
            "every replica must persist the leader-generated password hash"
        );
        assert!(
            !harness
                .node(node)
                .data_path()
                .join("metadata/snapshot.bin")
                .exists(),
            "initial root persistence must not depend on a checkpoint"
        );
        harness.node_mut(node).remove_env("IGGY_ROOT_USERNAME");
        harness.node_mut(node).remove_env("IGGY_ROOT_PASSWORD");
    }
    harness.kill_cluster().expect("crash every replica");
    harness
        .restart_cluster()
        .await
        .expect("restart without root credentials");
    for node in 0..3 {
        assert_root_login(harness, node).await;
    }
    let leader = leader_node_index_via(harness, 0).await;
    harness
        .kill_node(leader)
        .expect("crash the root's original leader");
    let survivor = (leader + 1) % 3;
    assert_root_login(harness, survivor).await;
    assert_ne!(leader_node_index_via(harness, survivor).await, leader);
}

async fn assert_root_login(harness: &TestHarness, node: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(Ok(client)) =
            timeout(Duration::from_secs(1), harness.root_client_for_node(node)).await
        {
            client.disconnect().await.expect("disconnect root client");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "root login on node {node} did not recover"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

fn read_root_creation(data_path: &Path) -> CreateUserRequest {
    let bytes = std::fs::read(data_path.join("metadata/journal.wal"))
        .expect("read the persisted metadata journal");
    let header = bytemuck::checked::pod_read_unaligned::<PrepareHeader>(&bytes[..HEADER_SIZE]);
    assert_eq!(header.operation, Operation::CreateUser);
    assert_eq!(header.client, 0);
    assert_eq!(header.user_id, 0);
    assert_eq!(header.op, 1);
    CreateUserRequest::decode_from(&bytes[HEADER_SIZE..header.size as usize])
        .expect("decode the replicated root creation")
}
