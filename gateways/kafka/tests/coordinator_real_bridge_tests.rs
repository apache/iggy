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

//! Several gateways on one real `iggy-server`, sharing their consumer groups through the
//! coordinator log.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/gateways.rs"]
mod gateways;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use iggy::prelude::{
    Consumer, ConsumerGroupClient, GlobalPermissions, Identifier, IggyMessage, MessageClient,
    Partitioning, Permissions, PollOptions, PollingStrategy, StreamClient, StreamCreateOptions,
    TopicClient, TopicCreateOptions, UserClient, UserCreateOptions, UserStatus,
};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    GroupId, MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use secrecy::SecretString;
use serial_test::serial;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::auth::{AuthenticatedPrincipal, UserKey};
use iggy_gateway_kafka::bridge::{
    COORDINATOR_LEASE_GROUP, COORDINATOR_LOG_TOPIC, cluster_id_stream,
};
use iggy_gateway_kafka::coordinator::record::{Body, Record};
use iggy_gateway_kafka::coordinator::{
    Coordination, CoordinationConfig, PARTITIONS, partition_of_group,
};
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::listener::parse_brokers;
use iggy_gateway_kafka::protocol::acl::PrincipalPermissions;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_DESCRIBE_GROUPS, API_KEY_METADATA, API_KEY_OFFSET_COMMIT,
    API_KEY_OFFSET_FETCH, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_NOT_COORDINATOR,
    GatewayState, handle_connection_request,
};
use iggy_gateway_kafka::server::bind_listener;
use iggy_gateway_kafka::{CoordinatorLog, GatewayConfig};

use gateways::{
    Gateway, SETTLE, TOPIC, agreed_coordinator, call, commit, connect, coordinators, create_topic,
    eventually, fetch, gateway_config, gateway_of, heartbeat, join_and_sync, start_gateway,
    start_gateways,
};
use iggy_server::{TestServer, raw_client};

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_three_gateways_when_finding_coordinators_should_agree_and_spread_the_groups() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let gateways = start_gateways(&vec![server.test_config(); 3]).await;

    let groups: Vec<String> = (0..64).map(|n| format!("group-{n}")).collect();
    let named = eventually(SETTLE, "agreement on every coordinator", || async {
        let first = coordinators(gateways[0].addr, &groups).await?;
        for gateway in &gateways[1..] {
            if coordinators(gateway.addr, &groups).await? != first {
                return None;
            }
        }
        let nodes: BTreeSet<i32> = first.iter().copied().collect();
        (nodes.len() == 3).then_some(first)
    })
    .await;
    assert_eq!(named.len(), groups.len());

    for gateway in gateways {
        gateway.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_stable_group_when_its_coordinator_stops_should_keep_its_member_generation_and_offsets()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let mut gateways = start_gateways(&vec![server.test_config(); 2]).await;
    let group = "payments";

    let all: Vec<&Gateway> = gateways.iter().collect();
    let first = agreed_coordinator(&all, group).await;
    let coordinator = gateway_of(&gateways, first).addr;
    let (member_id, generation) = join_and_sync(coordinator, group).await;
    assert_eq!(
        commit(
            coordinator,
            group,
            &member_id,
            generation,
            42,
            "partition time 17"
        )
        .await,
        ERROR_NONE
    );
    assert_eq!(
        fetch(coordinator, group).await,
        (ERROR_NONE, 42, "partition time 17".to_string())
    );

    let at = gateways
        .iter()
        .position(|gateway| gateway.node_id == first)
        .expect("the coordinator");
    gateways.remove(at).stop().await;
    let successor = &gateways[0];
    // Until the successor claims the partition, it names the stopped gateway it last heard of.
    let groups = [group.to_string()];
    eventually(SETTLE, "the other gateway to take the group", || async {
        let named = coordinators(successor.addr, &groups).await?[0];
        (named == i32::from(successor.node_id)).then_some(())
    })
    .await;

    assert_eq!(
        heartbeat(successor.addr, group, &member_id, generation).await,
        ERROR_NONE,
        "the member keeps its id and generation, so it does not rebalance"
    );
    assert_eq!(
        fetch(successor.addr, group).await,
        (ERROR_NONE, 42, "partition time 17".to_string()),
        "the offset and its metadata move with the group"
    );
    let request = DescribeGroupsRequest::default()
        .with_groups(vec![GroupId(StrBytes::from_static_str(group))]);
    let described: DescribeGroupsResponse =
        call(successor.addr, API_KEY_DESCRIBE_GROUPS, 5, &request).await;
    let member = &described.groups[0].members[0];
    assert_eq!(
        (member.member_id.as_str(), member.client_id.as_str()),
        (member_id.as_str(), "coordinator-test"),
        "the member keeps the client id of its join"
    );

    for gateway in gateways {
        gateway.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_newer_claim_in_the_log_when_the_coordinator_reads_it_should_stop_and_load_again() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let gateways = start_gateways(&vec![server.test_config(); 1]).await;
    let gateway = &gateways[0];
    let group = "audit";

    agreed_coordinator(&[gateway], group).await;
    let (member_id, generation) = join_and_sync(gateway.addr, group).await;

    // Another gateway's claim, numbered above any this one made.
    let client = raw_client(&server).await;
    let claim = Record {
        claim: 1_000_000,
        checkpoint: u64::MAX,
        body: Body::Claim { node_id: 9 },
    }
    .encode()
    .expect("a claim encodes");
    let mut messages = vec![
        IggyMessage::builder()
            .payload(claim)
            .build()
            .expect("a message"),
    ];
    client
        .send_messages(
            &Identifier::named(&cluster_id_stream("kafka")).expect("stream name"),
            &Identifier::named(COORDINATOR_LOG_TOPIC).expect("topic name"),
            &Partitioning::partition_id(partition_of_group(group)),
            &mut messages,
        )
        .await
        .expect("append the claim");

    eventually(SETTLE, "the coordinator to stop", || async {
        let code = heartbeat(gateway.addr, group, &member_id, generation).await;
        (code != ERROR_NONE).then_some(code)
    })
    .await;
    // Iggy still gives it the partition, so it claims again above the foreign claim and loads
    // the group from the log.
    let code = eventually(
        SETTLE,
        "the coordinator to load the group again",
        || async {
            let code = heartbeat(gateway.addr, group, &member_id, generation).await;
            (code == ERROR_NONE || code == ERROR_NOT_COORDINATOR).then_some(code)
        },
    )
    .await;
    let code = if code == ERROR_NONE {
        code
    } else {
        eventually(SETTLE, "the group to come back", || async {
            let code = heartbeat(gateway.addr, group, &member_id, generation).await;
            (code == ERROR_NONE).then_some(code)
        })
        .await
    };
    assert_eq!(code, ERROR_NONE);

    for gateway in gateways {
        gateway.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_two_gateways_when_creating_a_topic_should_refuse_it_off_the_controller() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let gateways = start_gateways(&vec![server.test_config(); 2]).await;

    let controller = eventually(SETTLE, "agreement on the controller", || async {
        let mut named = BTreeSet::new();
        for gateway in &gateways {
            let response: MetadataResponse = call(
                gateway.addr,
                API_KEY_METADATA,
                9,
                &MetadataRequest::default(),
            )
            .await;
            named.insert(response.controller_id.0);
        }
        let node = *named.first()?;
        (named.len() == 1
            && coordinators(gateways[0].addr, &["any".to_string()])
                .await
                .is_some())
        .then(|| u16::try_from(node).expect("a node id"))
    })
    .await;
    let other = gateways
        .iter()
        .find(|gateway| gateway.node_id != controller)
        .expect("a gateway that is not the controller");

    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_static_str("created")))
            .with_num_partitions(1)
            .with_replication_factor(1),
    ]);
    let refused: CreateTopicsResponse = call(other.addr, API_KEY_CREATE_TOPICS, 5, &request).await;
    assert_eq!(refused.topics[0].error_code, ERROR_NOT_CONTROLLER);
    let created: CreateTopicsResponse = call(
        gateway_of(&gateways, controller).addr,
        API_KEY_CREATE_TOPICS,
        5,
        &request,
    )
    .await;
    assert_eq!(created.topics[0].error_code, ERROR_NONE);

    for gateway in gateways {
        gateway.stop().await;
    }
}

/// Each gateway leads every partition, so a client of a gateway that crashed goes on through
/// another at its next Metadata.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_two_gateways_when_asked_for_metadata_should_list_both_and_lead_from_each() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let gateways = start_gateways(&vec![server.test_config(); 2]).await;
    let request = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC)))),
    ]));

    let leaders = eventually(SETTLE, "both gateways listed", || async {
        let mut leaders = Vec::new();
        for gateway in &gateways {
            let response: MetadataResponse =
                call(gateway.addr, API_KEY_METADATA, 9, &request).await;
            let brokers: BTreeSet<i32> = response
                .brokers
                .iter()
                .map(|broker| broker.node_id.0)
                .collect();
            if brokers != BTreeSet::from([1, 2]) {
                return None;
            }
            let named: BTreeSet<i32> = response.topics[0]
                .partitions
                .iter()
                .map(|partition| partition.leader_id.0)
                .collect();
            leaders.push((i32::from(gateway.node_id), named));
        }
        Some(leaders)
    })
    .await;
    for (node_id, named) in leaders {
        assert_eq!(named, BTreeSet::from([node_id]), "gateway {node_id}");
    }

    for gateway in gateways {
        gateway.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_gateway_that_restarts_when_its_member_heartbeats_should_find_the_group_in_the_log()
{
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let group = "billing";

    let socket = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let addr = socket.local_addr().expect("local addr");
    let brokers = parse_brokers(&format!("1@{addr}")).expect("one listed gateway");
    let gateway = start_gateway(
        server.test_config(),
        gateway_config(1, brokers.clone()),
        socket,
        addr,
    )
    .await;
    agreed_coordinator(&[&gateway], group).await;
    let (member_id, generation) = join_and_sync(addr, group).await;
    assert_eq!(
        commit(addr, group, &member_id, generation, 7, "restart").await,
        ERROR_NONE
    );
    gateway.stop().await;

    let socket = bind_listener(&addr.to_string()).expect("bind the same port again");
    let gateway = start_gateway(
        server.test_config(),
        gateway_config(1, brokers),
        socket,
        addr,
    )
    .await;
    agreed_coordinator(&[&gateway], group).await;
    assert_eq!(
        heartbeat(addr, group, &member_id, generation).await,
        ERROR_NONE,
        "the restarted gateway restores the member from the log"
    );
    assert_eq!(
        fetch(addr, group).await,
        (ERROR_NONE, 7, "restart".to_string())
    );

    gateway.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_checkpoints_in_the_log_when_the_gateway_restarts_should_load_the_latest_state() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let group = "ledger";
    let config = |brokers| GatewayConfig {
        coordination: CoordinationConfig {
            checkpoint_records: 4,
            ..CoordinationConfig::default()
        },
        ..gateway_config(1, brokers)
    };

    let socket = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let addr = socket.local_addr().expect("local addr");
    let brokers = parse_brokers(&format!("1@{addr}")).expect("one listed gateway");
    let gateway = start_gateway(server.test_config(), config(brokers.clone()), socket, addr).await;
    agreed_coordinator(&[&gateway], group).await;
    let (member_id, generation) = join_and_sync(addr, group).await;
    for offset in 1..=12 {
        assert_eq!(
            commit(
                addr,
                group,
                &member_id,
                generation,
                offset,
                &format!("commit {offset}")
            )
            .await,
            ERROR_NONE
        );
    }
    gateway.stop().await;

    let client = raw_client(&server).await;
    let polled = client
        .poll_messages(
            &Identifier::named(&cluster_id_stream("kafka")).expect("stream name"),
            &Identifier::named(COORDINATOR_LOG_TOPIC).expect("topic name"),
            partition_of_group(group),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 1_000,
                ..PollOptions::default()
            },
        )
        .await
        .expect("read the log partition");
    let ends = polled
        .messages
        .iter()
        .filter_map(|message| Record::decode(&message.payload).ok())
        .filter(|record| matches!(record.body, Body::CheckpointEnd { .. }))
        .count();
    assert!(
        ends >= 2,
        "the owner writes a checkpoint every 4 records, found {ends}"
    );

    let socket = bind_listener(&addr.to_string()).expect("bind the same port again");
    let gateway = start_gateway(server.test_config(), config(brokers), socket, addr).await;
    agreed_coordinator(&[&gateway], group).await;
    assert_eq!(
        heartbeat(addr, group, &member_id, generation).await,
        ERROR_NONE
    );
    assert_eq!(
        fetch(addr, group).await,
        (ERROR_NONE, 12, "commit 12".to_string())
    );

    gateway.stop().await;
}

/// A log topic that a crash of a quorum can lose a claim from would let two gateways own one
/// partition. Only an operator makes one.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_log_topic_that_is_not_persisted_when_linking_should_refuse_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let client = raw_client(&server).await;
    let stream = cluster_id_stream("kafka");
    client
        .create_stream(&stream, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    client
        .create_topic(
            &Identifier::named(&stream).expect("stream name"),
            COORDINATOR_LOG_TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create the log topic");

    let bridge = connect(server.test_config()).await;
    let error = bridge
        .coordinator_link(PARTITIONS, 1)
        .await
        .expect_err("a replicated log topic is refused");
    assert!(
        error.to_string().contains("needs durability persisted"),
        "{error}"
    );
    bridge.close().await.expect("close the bridge");
}

/// A topic deleted and created again while no gateway runs reaches no gateway as a deletion, so
/// the log still holds the group's offset of the old topic. The group must not resume the new
/// topic at it.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_topic_created_again_while_no_gateway_runs_when_fetching_should_not_answer_its_old_offset()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let group = "audit";

    let socket = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let addr = socket.local_addr().expect("local addr");
    let brokers = parse_brokers(&format!("1@{addr}")).expect("one listed gateway");
    let gateway = start_gateway(
        server.test_config(),
        gateway_config(1, brokers.clone()),
        socket,
        addr,
    )
    .await;
    agreed_coordinator(&[&gateway], group).await;
    let (member_id, generation) = join_and_sync(addr, group).await;
    assert_eq!(
        commit(addr, group, &member_id, generation, 7, "old topic").await,
        ERROR_NONE
    );
    gateway.stop().await;

    let bridge = connect(server.test_config()).await;
    bridge
        .delete_kafka_topic(TOPIC)
        .await
        .expect("delete the topic");
    bridge
        .create_kafka_topic(TOPIC, 2)
        .await
        .expect("create the topic again");
    bridge.close().await.expect("close the bridge");

    let socket = bind_listener(&addr.to_string()).expect("bind the same port again");
    let gateway = start_gateway(
        server.test_config(),
        gateway_config(1, brokers),
        socket,
        addr,
    )
    .await;
    agreed_coordinator(&[&gateway], group).await;
    assert_eq!(
        fetch(addr, group).await,
        (ERROR_NONE, -1, String::new()),
        "the topic created again holds no offset of the group"
    );

    gateway.stop().await;
}

/// A bridge user without `manage_servers` cannot read Iggy's change log. With the log on only by
/// default, the gateway then starts without it, as before the log, and coordinates its groups
/// alone.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_bridge_user_without_manage_servers_when_the_log_is_on_by_default_should_start_without_it()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let username = "no-servers";
    raw_client(&server)
        .await
        .create_user(
            username,
            "no-servers-password",
            UserStatus::Active,
            Some(Permissions {
                global: GlobalPermissions {
                    manage_streams: true,
                    read_streams: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                    ..GlobalPermissions::default()
                },
                ..Permissions::default()
            }),
            &UserCreateOptions::default(),
        )
        .await
        .expect("create the user");
    let mut bridge = server.test_config();
    bridge.username = username.to_string();
    bridge.password = SecretString::from("no-servers-password");
    let group = "fallback";

    let socket = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let addr = socket.local_addr().expect("local addr");
    let brokers = parse_brokers(&format!("1@{addr}")).expect("one listed gateway");
    let config = GatewayConfig {
        coordinator_log: CoordinatorLog::Preferred,
        ..gateway_config(1, brokers)
    };
    let gateway = start_gateway(bridge, config, socket, addr).await;
    assert_eq!(agreed_coordinator(&[&gateway], group).await, 1);
    let (member_id, generation) = join_and_sync(addr, group).await;
    assert_eq!(
        heartbeat(addr, group, &member_id, generation).await,
        ERROR_NONE
    );
    let log_topic = raw_client(&server)
        .await
        .get_topic(
            &Identifier::named(&cluster_id_stream("kafka")).expect("stream name"),
            &Identifier::named(COORDINATOR_LOG_TOPIC).expect("topic name"),
        )
        .await
        .expect("read the log topic");
    assert!(log_topic.is_none(), "the gateway runs without the log");

    gateway.stop().await;
}

/// Iggy can take until the lease group's rebalancing timeout to hand a partition over, and the
/// next gateway claims it only then. A lease member that never claims stands for that wait: the
/// partitions Iggy gives it must keep their coordinator meanwhile.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_partition_that_leaves_the_share_when_no_gateway_claims_it_should_stay_served() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let socket = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let addr = socket.local_addr().expect("local addr");
    let brokers = parse_brokers(&format!("1@{addr}")).expect("one listed gateway");
    let gateway = start_gateway(
        server.test_config(),
        gateway_config(1, brokers),
        socket,
        addr,
    )
    .await;
    let groups: Vec<String> = (0..64).map(|n| format!("group-{n}")).collect();
    eventually(SETTLE, "the gateway to coordinate every group", || async {
        let named = coordinators(addr, &groups).await?;
        named.iter().all(|node| *node == 1).then_some(())
    })
    .await;

    let silent = raw_client(&server).await;
    let stream = Identifier::named(&cluster_id_stream("kafka")).expect("stream name");
    let topic = Identifier::named(COORDINATOR_LOG_TOPIC).expect("topic name");
    let lease = Identifier::named(COORDINATOR_LEASE_GROUP).expect("group name");
    silent
        .join_consumer_group(&stream, &topic, &lease)
        .await
        .expect("join the lease group");
    let share = eventually(SETTLE, "a share for the silent member", || async {
        let share = silent
            .sync_consumer_group(&stream, &topic, &lease)
            .await
            .ok()??;
        (!share.partitions.is_empty()).then_some(share.partitions)
    })
    .await;
    // The gateway reads its share every second.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let named = coordinators(addr, &groups)
        .await
        .expect("every group keeps a coordinator");
    assert!(named.iter().all(|node| *node == 1), "{named:?}");
    let moved = groups
        .iter()
        .find(|group| share.contains(&partition_of_group(group)))
        .expect("a group of a partition the silent member got");
    let (member_id, generation) = join_and_sync(addr, moved).await;
    assert_eq!(
        heartbeat(addr, moved, &member_id, generation).await,
        ERROR_NONE
    );

    gateway.stop().await;
}

/// A group without members that commits nothing for the retention loses its offsets in Iggy too,
/// so its keys stop counting against `partition.consumer_offsets_max`. A group that commits on
/// keeps them, and an expired group commits again from scratch.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_group_without_commits_for_the_retention_should_delete_its_offsets_in_iggy() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let retention = Duration::from_secs(4);
    let socket = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let addr = socket.local_addr().expect("local addr");
    let brokers = parse_brokers(&format!("1@{addr}")).expect("one listed gateway");
    let config = GatewayConfig {
        coordination: CoordinationConfig {
            offsets_retention: retention,
            retention_check: Duration::from_millis(200),
            ..CoordinationConfig::default()
        },
        ..gateway_config(1, brokers)
    };
    let gateway = start_gateway(server.test_config(), config, socket, addr).await;
    let outside = StrBytes::default();
    for group in ["idle", "busy"] {
        agreed_coordinator(&[&gateway], group).await;
        eventually(SETTLE, "a first commit", || async {
            (commit(addr, group, &outside, -1, 1, "").await == ERROR_NONE).then_some(())
        })
        .await;
    }
    let idle_since = Instant::now();
    let raw = raw_client(&server).await;
    assert!(
        offset_group_exists(&raw, "idle").await,
        "the commit creates the Iggy group"
    );

    let mut offset = 1;
    let last = eventually(
        Duration::from_secs(30),
        "the deletion of the idle group",
        || {
            // A commit that moves the offset is logged at once, so `busy` never looks idle.
            offset += 1;
            let (raw, outside, offset) = (&raw, &outside, offset);
            async move {
                assert_eq!(
                    commit(addr, "busy", outside, -1, offset, "").await,
                    ERROR_NONE
                );
                // Iggy goes first, so the log answers until its own deletion lands.
                let gone = !offset_group_exists(raw, "idle").await
                    && fetch(addr, "idle").await == (ERROR_NONE, -1, String::new());
                gone.then_some(offset)
            }
        },
    )
    .await;

    assert!(
        idle_since.elapsed() >= retention.saturating_sub(Duration::from_millis(500)),
        "deleted after {:?}, before the retention",
        idle_since.elapsed()
    );
    assert!(offset_group_exists(&raw, "busy").await);
    assert_eq!(fetch(addr, "busy").await, (ERROR_NONE, last, String::new()));
    let again = eventually(
        SETTLE,
        "a commit once the deletion lets the group go",
        || async {
            let code = commit(addr, "idle", &outside, -1, 5, "").await;
            (code != ERROR_COORDINATOR_LOAD_IN_PROGRESS).then_some(code)
        },
    )
    .await;
    assert_eq!(again, ERROR_NONE, "the group commits again from scratch");
    assert_eq!(fetch(addr, "idle").await, (ERROR_NONE, 5, String::new()));

    gateway.stop().await;
}

/// A principal that may not create groups commits only to groups that exist. Its refused commit
/// never reaches the log, so `OffsetFetch`, which the log answers, does not return it.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_principal_that_may_not_create_groups_when_it_commits_to_a_new_group_should_log_nothing()
 {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let bridge = Arc::new(connect(server.test_config()).await);
    // As a gateway starts: the cluster id creates the stream that the log lives in.
    bridge.load_cluster_id().await.expect("load the cluster id");
    let groups = GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new());
    let coordination = Coordination::start(
        &bridge,
        groups.clone(),
        1,
        CoordinationConfig::default(),
        CancellationToken::new(),
    )
    .await
    .expect("coordinate the groups");
    let state = GatewayState::new(Some(bridge), 8 * 1024 * 1024, 1, groups)
        .with_coordination(Arc::clone(&coordination));
    let poller = principal(false);

    let refused = eventually(SETTLE, "a coordinator for the group", || async {
        let code = commit_by(&state, &poller, "fresh", 7).await;
        (code != ERROR_COORDINATOR_LOAD_IN_PROGRESS && code != ERROR_NOT_COORDINATOR)
            .then_some(code)
    })
    .await;
    assert_eq!(refused, ERROR_GROUP_AUTHORIZATION_FAILED);
    assert_eq!(
        committed(&state, "fresh").await,
        -1,
        "the log holds nothing"
    );

    assert_eq!(
        commit_by(&state, &principal(true), "fresh", 8).await,
        ERROR_NONE
    );
    assert_eq!(
        commit_by(&state, &poller, "fresh", 9).await,
        ERROR_NONE,
        "the group exists now"
    );
    assert_eq!(committed(&state, "fresh").await, 9);

    coordination
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await;
}

/// Whether Iggy holds the group that keeps the offsets of Kafka group `group` on [`TOPIC`].
async fn offset_group_exists(raw: &iggy::prelude::IggyClient, group: &str) -> bool {
    let named = |name: &str| Identifier::named(name).expect("valid Iggy name");
    raw.get_consumer_group(
        &named("kafka"),
        &named(TOPIC),
        &named(&format!("kafka.cg.{group}")),
    )
    .await
    .expect("read the Iggy group")
    .is_some()
}

/// A signed-in Iggy user that may poll, and read topics when `read_topics`.
fn principal(read_topics: bool) -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        username: "reporting".to_string(),
        user: Some(UserKey {
            user_id: 9,
            created_at: 1,
        }),
        permissions: PrincipalPermissions {
            read_topics,
            poll_messages: true,
            ..PrincipalPermissions::default()
        },
        topic_reads: Arc::default(),
        permissions_known: true,
    }
}

/// Commits `offset` for partition 0 of [`TOPIC`] as `principal`, outside the group protocol. The
/// partition's code.
async fn commit_by(
    state: &GatewayState,
    principal: &AuthenticatedPrincipal,
    group: &str,
    offset: i64,
) -> i16 {
    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_generation_id_or_member_epoch(-1)
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(offset),
                ]),
        ]);
    let body = handle_connection_request(
        state,
        &ConnectionState::default(),
        Some(principal),
        API_KEY_OFFSET_COMMIT,
        7,
        wire::encode(&request, 7),
    )
    .await
    .expect_response("OffsetCommit answers");
    let response: OffsetCommitResponse = wire::decode(body, 7);
    response.topics[0].partitions[0].error_code
}

/// The committed offset of `group` for partition 0 of [`TOPIC`].
async fn committed(state: &GatewayState, group: &str) -> i64 {
    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_indexes(vec![0]),
        ]));
    let body = handle_connection_request(
        state,
        &ConnectionState::default(),
        None,
        API_KEY_OFFSET_FETCH,
        6,
        wire::encode(&request, 6),
    )
    .await
    .expect_response("OffsetFetch answers");
    let response: OffsetFetchResponse = wire::decode(body, 6);
    assert_eq!(response.error_code, ERROR_NONE);
    response.topics[0].partitions[0].committed_offset
}
