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

//! Spec tests for logging in at a node that is not the metadata primary.
//!
//! A client may dial any node. Credentials verify against replicated user
//! state, so the whole login except the consensus proposal already works on a
//! backup; the backup forwards the verified identity to the primary over the
//! replica interconnect and binds the session itself once the register
//! commits. Nothing about the client's frame or its credentials travels.
//!
//! These tests speak to the backup through the raw transport
//! (`TcpClient::login_user`) rather than through `IggyClient`, whose
//! `login_user` redirects to the leader after a successful sign-in and would
//! hide which node actually served the register.

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// Budget for a login that may have to replay past a transient refusal (a
/// primary still catching up, a view settling) and past a full re-election.
///
/// Must exceed TWO of the SDK's `RESPONSE_READ_TIMEOUT` (30s): on a read
/// timeout `send_raw_with_response` reconnects once and re-enters `send_raw`
/// with a fresh deadline, so a single `login_user` against an unresponsive
/// node can burn 2 x 30s before returning. A budget at or below that ceiling
/// admits exactly ONE attempt and makes the retry loops below unreachable.
const LOGIN_BUDGET: Duration = Duration::from_secs(90);
const LOGIN_RETRY_INTERVAL: Duration = Duration::from_millis(250);

/// The only partition of the single-partition topic the produce test creates.
/// Partition ids are 0-based.
const PARTITION_ID: u32 = 0;

/// Budget for a PAT to replicate from the node that minted it. The backup
/// verifies the token against its own replicated copy and fails closed until
/// it arrives, which is the deliberate parity with the HTTP forward.
const REPLICATION_BUDGET: Duration = Duration::from_secs(15);

/// A connected, NOT signed-in client on `address`.
///
/// `AutoLogin::Disabled` on purpose: `connect()` must not sign in or settle
/// leadership, so the login below is the first and only thing the node is
/// asked to do.
async fn connect_without_login(address: SocketAddr) -> TcpClient {
    let config = TcpClientConfig {
        server_address: address.to_string(),
        nodelay: true,
        ..TcpClientConfig::default()
    };
    let client = TcpClient::create(Arc::new(config)).expect("build a tcp client");
    Client::connect(&client)
        .await
        .expect("connect without signing in");
    client
}

/// The TCP port the roster marks as the metadata primary's.
async fn leader_tcp_port(harness: &TestHarness) -> u16 {
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("a root client (redirecting to the leader if node 0 is not it)");
    let metadata = client
        .get_cluster_metadata()
        .await
        .expect("get cluster metadata");
    metadata
        .nodes
        .iter()
        .find(|node| node.role == ClusterNodeRole::Leader)
        .unwrap_or_else(|| panic!("the cluster must have elected a leader, got {metadata}"))
        .endpoints
        .tcp
}

/// The TCP address of a node that is not the metadata primary.
async fn backup_address(harness: &TestHarness) -> SocketAddr {
    let leader_port = leader_tcp_port(harness).await;
    (0..harness.cluster_size())
        .map(|index| {
            harness
                .node(index)
                .tcp_addr()
                .expect("every node must expose a TCP address")
        })
        .find(|address| address.port() != leader_port)
        .expect("a multi-node cluster has a node that does not lead")
}

/// Sign in, replaying transient refusals until `budget` runs out.
///
/// A login is transient whenever the cluster cannot commit right now (an
/// election in flight, a primary still catching up), and the SDK's contract
/// for that answer is to replay.
async fn login_root_within(
    client: &TcpClient,
    budget: Duration,
) -> Result<IdentityInfo, IggyError> {
    let deadline = Instant::now() + budget;
    loop {
        match client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
        {
            Ok(identity) => return Ok(identity),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => sleep(LOGIN_RETRY_INTERVAL).await,
        }
    }
}

#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_backup_when_a_client_signs_in_should_bind_the_session_there(
    harness: &TestHarness,
) {
    let address = backup_address(harness).await;
    let client = connect_without_login(address).await;

    let identity = login_root_within(&client, LOGIN_BUDGET)
        .await
        .expect("a backup must complete a login by forwarding the register");
    assert_eq!(identity.user_id, 0, "root user should have id 0");

    // The session is bound on THIS node, not merely committed somewhere: an
    // authenticated read is served locally and would be evicted otherwise.
    client
        .get_me()
        .await
        .expect("the backup must serve an authenticated read on the session it bound");
}

#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_backup_bound_session_when_the_client_logs_out_should_remove_it_cluster_wide(
    harness: &TestHarness,
) {
    let address = backup_address(harness).await;
    let client = connect_without_login(address).await;

    login_root_within(&client, LOGIN_BUDGET)
        .await
        .expect("a backup must complete the login before logout");
    client
        .logout_user()
        .await
        .expect("the backup must forward Logout to the metadata primary");

    assert_eq!(
        client
            .get_me()
            .await
            .expect_err("the local session must be unbound after logout")
            .as_code(),
        IggyError::Unauthenticated.as_code(),
    );
}

#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_backup_when_a_pat_login_arrives_should_bind_the_session_there(
    harness: &TestHarness,
) {
    const TOKEN_NAME: &str = "backup-login-pat";

    let leader_client = harness
        .root_client_for_node(0)
        .await
        .expect("a root client at the leader");
    let raw_pat = leader_client
        .create_personal_access_token(TOKEN_NAME, PersonalAccessTokenExpiry::NeverExpire)
        .await
        .expect("mint a PAT at the leader");

    let address = backup_address(harness).await;
    let client = connect_without_login(address).await;

    // The backup verifies the token against its own replicated copy, so it
    // refuses until the mint has replicated. Fail-closed by design; poll
    // through the window rather than asserting on its width.
    let deadline = Instant::now() + REPLICATION_BUDGET;
    let identity = loop {
        match client
            .login_with_personal_access_token(&raw_pat.token)
            .await
        {
            Ok(identity) => break identity,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "a replicated PAT must eventually authenticate at a backup, got {error}"
                );
                sleep(LOGIN_RETRY_INTERVAL).await;
            }
        }
    };
    assert_eq!(identity.user_id, 0, "the PAT authenticates as root");

    client
        .get_me()
        .await
        .expect("the backup must serve an authenticated read on the session it bound");
}

#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_backup_when_credentials_are_wrong_should_refuse_terminally(harness: &TestHarness) {
    let address = backup_address(harness).await;
    let client = connect_without_login(address).await;

    // Terminal, and decided locally: the credential check never leaves the
    // backup, so a wrong password is refused before anything is forwarded.
    let error = client
        .login_user(DEFAULT_ROOT_USERNAME, "definitely-not-the-root-password")
        .await
        .expect_err("wrong credentials must be refused");
    assert_eq!(
        error.as_code(),
        IggyError::InvalidCredentials.as_code(),
        "a backup must answer wrong credentials with the terminal error, got {error}"
    );
}

/// Losing a replica must not cost the forward its answer: the primary is
/// still there, so the surviving backup keeps completing logins.
///
/// Deliberately kills a FOLLOWER rather than the primary: killing the
/// primary tests re-election, not this feature. The sibling test below
/// covers the primary-kill path.
#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_degraded_cluster_when_a_client_signs_in_at_a_backup_should_succeed(
    harness: &mut TestHarness,
) {
    let leader_port = leader_tcp_port(harness).await;
    let backups: Vec<usize> = (0..harness.cluster_size())
        .filter(|index| {
            harness
                .node(*index)
                .tcp_addr()
                .is_some_and(|address| address.port() != leader_port)
        })
        .collect();
    let (killed, survivor) = (backups[0], backups[1]);
    let survivor_address = harness
        .node(survivor)
        .tcp_addr()
        .expect("the surviving backup must expose a TCP address");

    harness.stop_node(killed).expect("stop one backup");

    let client = connect_without_login(survivor_address).await;
    let identity = login_root_within(&client, LOGIN_BUDGET)
        .await
        .expect("a backup must still forward its register with one replica down");
    assert_eq!(identity.user_id, 0, "root user should have id 0");
    client
        .get_me()
        .await
        .expect("the backup must serve an authenticated read on the session it bound");
}

/// Kill the metadata PRIMARY, then sign in at a survivor: the two survivors
/// must elect a new primary and complete the forwarded register. Used to
/// starve on roughly half the runs before the view-start pipeline fix
/// (a register admitted during the superblock persist panicked the pump);
/// the deterministic interleaving is pinned by the simulator gate.
#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_killed_primary_when_a_client_signs_in_at_a_survivor_should_succeed(
    harness: &mut TestHarness,
) {
    let leader_port = leader_tcp_port(harness).await;
    let leader = (0..harness.cluster_size())
        .find(|index| {
            harness
                .node(*index)
                .tcp_addr()
                .is_some_and(|address| address.port() == leader_port)
        })
        .expect("the leader must be one of the roster nodes");
    let survivor = (0..harness.cluster_size())
        .find(|index| *index != leader)
        .expect("a 3-node cluster has a survivor");
    let survivor_address = harness
        .node(survivor)
        .tcp_addr()
        .expect("the survivor must expose a TCP address");

    harness.stop_node(leader).expect("stop the primary");

    let client = connect_without_login(survivor_address).await;
    let identity = login_root_within(&client, LOGIN_BUDGET)
        .await
        .expect("survivors must re-elect and complete the login");
    assert_eq!(identity.user_id, 0, "root user should have id 0");
}

#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_backup_when_auto_login_dials_it_should_settle_on_the_leader(
    harness: &TestHarness,
) {
    const STREAM_NAME: &str = "backup-login-stream";
    const TOPIC_NAME: &str = "backup-login-topic";
    const PAYLOAD: &str = "produced after a backup-dialed login";

    let address = backup_address(harness).await;
    let client = IggyClient::create(
        ClientWrapper::Tcp(connect_without_login(address).await),
        None,
        None,
    );

    // `IggyClient::login_user` redirects to the leader after the backup has
    // served the sign-in, which is what makes replicated writes work below.
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("a backup-dialed login must succeed and settle on the leader");

    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream after a backup-dialed login");
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic after a backup-dialed login");

    let topic_id = Identifier::named(TOPIC_NAME).expect("topic identifier");
    let mut messages = vec![IggyMessage::from(PAYLOAD)];
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("produce after a backup-dialed login");

    let polled = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1,
            false,
        )
        .await
        .expect("poll after a backup-dialed login");
    assert_eq!(
        polled.messages.len(),
        1,
        "the produced message must poll back"
    );
}
