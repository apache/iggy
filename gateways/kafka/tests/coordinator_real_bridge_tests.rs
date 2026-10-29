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

use iggy::prelude::{
    Consumer, Identifier, IggyMessage, MessageClient, Partitioning, PollingStrategy,
};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    GroupId, MetadataRequest, MetadataResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use serial_test::serial;

use iggy_gateway_kafka::GatewayConfig;
use iggy_gateway_kafka::bridge::{COORDINATOR_LOG_TOPIC, cluster_id_stream};
use iggy_gateway_kafka::coordinator::record::{Body, Record};
use iggy_gateway_kafka::coordinator::{CoordinationConfig, partition_of_group};
use iggy_gateway_kafka::listener::parse_brokers;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_DESCRIBE_GROUPS, API_KEY_METADATA, ERROR_NONE,
    ERROR_NOT_CONTROLLER, ERROR_NOT_COORDINATOR,
};
use iggy_gateway_kafka::server::bind_listener;

use gateways::{
    Gateway, SETTLE, TOPIC, agreed_coordinator, call, commit, coordinators, create_topic,
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

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_two_gateways_when_asked_for_metadata_should_name_the_same_spread_leaders() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    create_topic(server.test_config()).await;
    let gateways = start_gateways(&vec![server.test_config(); 2]).await;
    let request = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC)))),
    ]));

    let leaders = eventually(SETTLE, "agreement on the leaders", || async {
        let mut answers = Vec::new();
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
            let leaders: Vec<i32> = response.topics[0]
                .partitions
                .iter()
                .map(|partition| partition.leader_id.0)
                .collect();
            answers.push(leaders);
        }
        (answers[0] == answers[1]).then(|| answers[0].clone())
    })
    .await;
    assert_eq!(leaders.len(), 2);

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
            Some(partition_of_group(group)),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            1_000,
            false,
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
