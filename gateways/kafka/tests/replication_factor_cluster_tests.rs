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

//! A Kafka replication factor on a cluster of three Iggy nodes. Iggy stores it (#4692) and
//! refuses one above the node count, and Metadata reports as many replicas.

use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    BrokerId, CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse,
    TopicName,
};
use kafka_protocol::protocol::StrBytes;
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::bridge::topic_settings::replication_factor_of;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    BrokerAdvertise, Brokers, ConnectionState, ERROR_INVALID_REPLICATION_FACTOR, ERROR_NONE,
    GatewayState,
};
use iggy_gateway_kafka::protocol::handlers::{create_topics, metadata};

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/wire.rs"]
mod wire;

use iggy_server::TestCluster;
use wire::{decode, encode};

const NODES: usize = 3;
const CREATE_TOPICS_VERSION: i16 = 5;
const METADATA_VERSION: i16 = 9;
/// How long a new cluster can take before it accepts metadata writes.
const SETTLE: Duration = Duration::from_secs(60);

/// A bridge to node 0, once the cluster accepts one.
async fn connect(cluster: &TestCluster) -> IggyBridge {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        match IggyBridge::connect(cluster.bridge_config(0)).await {
            Ok(bridge) => return bridge,
            Err(error) => assert!(
                tokio::time::Instant::now() < deadline,
                "no bridge within {SETTLE:?}: {error}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn creatable(name: &str, replication_factor: i16) -> CreatableTopic {
    CreatableTopic::default()
        .with_name(TopicName(StrBytes::from(name.to_string())))
        .with_num_partitions(2)
        .with_replication_factor(replication_factor)
}

/// Gateway 1 of three, as each listener of a gateway beside node 0 lists them.
fn three_gateways() -> ConnectionState {
    let endpoint = |node: i32| BrokerAdvertise {
        host: format!("gateway-{node}.example"),
        port: 9092,
    };
    ConnectionState::for_listener(endpoint(1), false, false).with_brokers(Arc::new(Brokers {
        node_id: 1,
        peers: vec![(2, endpoint(2)), (3, endpoint(3))],
    }))
}

#[tokio::test]
#[serial]
async fn given_three_nodes_when_creating_topics_should_store_the_factor_and_report_its_replicas() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let cluster = TestCluster::spawn(data_dir.path(), NODES).await;
    let bridge = Arc::new(connect(&cluster).await);
    assert_eq!(bridge.iggy_node_count(), Some(NODES));
    assert_eq!(bridge.default_replication_factor(), Some(3));
    let state = GatewayState::new(
        Some(Arc::clone(&bridge)),
        8 * 1024 * 1024,
        0,
        GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
    );

    let request = CreateTopicsRequest::default()
        .with_topics(vec![
            creatable("two-copies", 2),
            creatable("every-node", -1),
            creatable("four-copies", 4),
        ])
        .with_timeout_ms(30_000);
    let outcome = create_topics::handle(
        &state,
        &ConnectionState::default(),
        CREATE_TOPICS_VERSION,
        encode(&request, CREATE_TOPICS_VERSION),
    )
    .await;
    let response: CreateTopicsResponse = decode(
        outcome.expect_response("CreateTopics answers"),
        CREATE_TOPICS_VERSION,
    );
    let answers: Vec<(&str, i16, i16, Option<&str>)> = response
        .topics
        .iter()
        .map(|topic| {
            (
                topic.name.as_str(),
                topic.error_code,
                topic.replication_factor,
                topic.error_message.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        answers[..2],
        [
            ("two-copies", ERROR_NONE, 2, None),
            ("every-node", ERROR_NONE, 3, None)
        ]
    );
    assert_eq!(answers[2].0, "four-copies");
    assert_eq!(answers[2].1, ERROR_INVALID_REPLICATION_FACTOR);
    assert_eq!(
        answers[2].3,
        Some(
            "Unable to replicate the partition 4 time(s): The target replication factor of 4 \
             cannot be reached because only 3 Iggy node(s) are in the cluster."
        )
    );

    let request = MetadataRequest::default().with_topics(Some(
        ["two-copies", "every-node"]
            .map(|name| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_static_str(name))))
            })
            .to_vec(),
    ));
    let outcome = metadata::handle(
        &state,
        &three_gateways(),
        None,
        METADATA_VERSION,
        encode(&request, METADATA_VERSION),
    )
    .await;
    let response: MetadataResponse = decode(
        outcome.expect_response("Metadata answers"),
        METADATA_VERSION,
    );
    for (topic, expected) in response.topics.iter().zip([
        vec![BrokerId(1), BrokerId(2)],
        vec![BrokerId(1), BrokerId(2), BrokerId(3)],
    ]) {
        assert_eq!(topic.error_code, ERROR_NONE);
        assert_eq!(topic.partitions.len(), 2);
        for partition in &topic.partitions {
            assert_eq!(partition.leader_id, BrokerId(1));
            assert_eq!(partition.replica_nodes, expected);
            assert_eq!(partition.isr_nodes, expected);
        }
    }

    for (name, factor) in [("two-copies", 2_u32), ("every-node", 3)] {
        let details = bridge
            .get_kafka_topic(name)
            .await
            .expect("get_topic call")
            .expect("the topic exists");
        assert_eq!(
            replication_factor_of(&details.options),
            Some(factor),
            "{name}"
        );
        for partition in &details.partitions {
            assert_eq!(
                u32::try_from(partition.members.len()),
                Ok(factor),
                "{name}: the nodes that store the partition"
            );
        }
    }
    let missing = bridge
        .get_kafka_topic("four-copies")
        .await
        .expect("get_topic call");
    assert!(missing.is_none(), "a refused factor creates nothing");
}
