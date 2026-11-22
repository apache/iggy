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

//! Geo topics through the gateway, against real `iggy-server` processes, one for each datacenter.
//! Each datacenter writes its own geo topic and reads the other's from a read-only shadow, which
//! the gateway lists as `<origin>.<topic>`.
//!
//! The last test drives real Kafka clients from containers: a producer in each datacenter, and one
//! consumer group with a pattern subscription that moves from one datacenter to the other. It needs
//! Docker and `apache/kafka:4.4.0`, and it skips without them unless `KAFKA_E2E_REQUIRED=1`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    BrokerId, CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest,
    CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse, DescribeConfigsRequest,
    DescribeConfigsResponse, GroupId, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse,
    TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use serial_test::serial;
use tempfile::TempDir;

use iggy_gateway_kafka::GatewayConfig;
use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_PARTITIONS, API_KEY_CREATE_TOPICS, API_KEY_DELETE_TOPICS,
    API_KEY_DESCRIBE_CONFIGS, API_KEY_INCREMENTAL_ALTER_CONFIGS, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_OFFSET_FETCH, API_KEY_PRODUCE, ERROR_INVALID_CONFIG,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_POLICY_VIOLATION, ERROR_TOPIC_ALREADY_EXISTS,
};
use iggy_gateway_kafka::server::bind_listener;

#[path = "common/codec.rs"]
mod codec;
#[path = "common/docker_clients.rs"]
mod docker_clients;
#[path = "common/gateways.rs"]
mod gateways;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use docker_clients::{missing_prerequisite, run_to_end, skip};
use gateways::{Gateway, call, eventually, listed, start_gateway};
use iggy_server::TestServer;

const TOPIC: &str = "orders";
const TOPIC_TYPE: &str = "iggy.topic.type";
const KAFKA_IMAGE: &str = "apache/kafka:4.4.0";
/// How long a datacenter takes to accept a geo topic, adopt a peer's, or publish a message to it.
const SETTLE: Duration = Duration::from_secs(60);
/// The Kafka name of the shadow, in `dc-b`, of the geo topic `orders` of `dc-a`.
const SHADOW_OF_A: &str = "dc-a.orders";
const RESOURCE_TOPIC: i8 = 2;

/// One datacenter: a single-node Iggy cluster with its own bucket, and a gateway in front of it.
struct Datacenter {
    server: TestServer,
    gateway: Gateway,
    _data: TempDir,
}

impl Datacenter {
    /// Starts datacenter `origin`, which writes `own` and follows `peer`, the bucket of datacenter
    /// `peer_origin`, when there is one.
    async fn start(origin: &str, own: &Path, peer: Option<(&str, &Path)>) -> Self {
        let data = tempfile::tempdir().expect("a data directory");
        let own = own.to_string_lossy().into_owned();
        let mut env = vec![
            ("IGGY_GEO_ORIGIN", origin.to_string()),
            ("IGGY_TIERED_STORAGE_BACKEND", "fs".to_string()),
            ("IGGY_TIERED_STORAGE_PATH", own),
            ("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s".to_string()),
            ("IGGY_GEO_LONG_TERM_REWRITE_MAX_INTERVAL", "1s".to_string()),
            ("IGGY_GEO_POSITIONS_PUBLISH_INTERVAL", "250ms".to_string()),
            ("IGGY_GEO_POSITIONS_POLL_INTERVAL", "250ms".to_string()),
            (
                "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL",
                "100ms".to_string(),
            ),
        ];
        if let Some((peer_origin, peer)) = peer {
            env.push(("IGGY_GEO_PEERS_0_ORIGIN", peer_origin.to_string()));
            env.push(("IGGY_GEO_PEERS_0_BACKEND", "fs".to_string()));
            env.push(("IGGY_GEO_PEERS_0_PATH", peer.to_string_lossy().into_owned()));
        }
        let env: Vec<(&str, &str)> = env
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        let server = TestServer::spawn_with_env(data.path(), &env).await;
        let socket = bind_listener("127.0.0.1:0").expect("bind a listener");
        let addr = socket.local_addr().expect("the listener's address");
        let config = GatewayConfig {
            node_id: 1,
            brokers: listed(&[addr]),
            shutdown_drain_timeout: Duration::from_secs(1),
            ..GatewayConfig::default()
        };
        let gateway = start_gateway(server.test_config(), config, socket, addr).await;
        Self {
            server,
            gateway,
            _data: data,
        }
    }

    const fn addr(&self) -> SocketAddr {
        self.gateway.addr
    }

    async fn stop(self) {
        self.gateway.stop().await;
        drop(self.server);
    }
}

/// `dc-a` and `dc-b`, each following the other's bucket.
struct Datacenters {
    a: Datacenter,
    b: Datacenter,
    _buckets: TempDir,
}

impl Datacenters {
    async fn start() -> Self {
        let buckets = tempfile::tempdir().expect("a directory for the buckets");
        let bucket_a = buckets.path().join("dc-a");
        let bucket_b = buckets.path().join("dc-b");
        std::fs::create_dir_all(&bucket_a).expect("the bucket of dc-a");
        std::fs::create_dir_all(&bucket_b).expect("the bucket of dc-b");
        let a = Datacenter::start("dc-a", &bucket_a, Some(("dc-b", &bucket_b))).await;
        let b = Datacenter::start("dc-b", &bucket_b, Some(("dc-a", &bucket_a))).await;
        Self {
            a,
            b,
            _buckets: buckets,
        }
    }

    async fn stop(self) {
        self.a.stop().await;
        self.b.stop().await;
    }
}

fn name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_string()))
}

/// Creates `topic` with `configs`, the error code and message of the answer.
async fn create_topic(
    gateway: SocketAddr,
    topic: &str,
    partitions: i32,
    configs: &[(&'static str, &'static str)],
) -> (i16, Option<String>) {
    let configs = configs
        .iter()
        .map(|(key, value)| {
            CreatableTopicConfig::default()
                .with_name(StrBytes::from_static_str(key))
                .with_value(Some(StrBytes::from_static_str(value)))
        })
        .collect();
    let request = CreateTopicsRequest::default()
        .with_timeout_ms(30_000)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(name(topic))
                .with_num_partitions(partitions)
                .with_replication_factor(-1)
                .with_configs(configs),
        ]);
    let response: CreateTopicsResponse = call(gateway, API_KEY_CREATE_TOPICS, 5, &request).await;
    let result = &response.topics[0];
    (
        result.error_code,
        result.error_message.as_ref().map(ToString::to_string),
    )
}

/// Creates the geo topic `topic`, retried while the cluster cannot take a geo topic yet: its
/// store identity check has not passed.
async fn create_geo_topic(gateway: SocketAddr, topic: &str, partitions: i32) {
    eventually(SETTLE, "the cluster takes a geo topic", || async {
        match create_topic(gateway, topic, partitions, &[(TOPIC_TYPE, "geo")]).await {
            (ERROR_NONE, _) => Some(()),
            (ERROR_POLICY_VIOLATION, _) => None,
            (code, message) => panic!("CreateTopics of a geo topic answered {code}: {message:?}"),
        }
    })
    .await;
}

/// The Kafka topics that a Metadata request for every topic lists.
async fn listed_topics(gateway: SocketAddr) -> Vec<String> {
    let request = MetadataRequest::default().with_topics(None);
    let response: MetadataResponse = call(gateway, API_KEY_METADATA, 9, &request).await;
    response
        .topics
        .iter()
        .filter_map(|topic| topic.name.as_ref().map(|name| name.0.to_string()))
        .collect()
}

/// The offset after the last message of partition 0 of `topic`, or `None` on an error.
async fn end_offset(gateway: SocketAddr, topic: &str) -> Option<i64> {
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(name(topic))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_timestamp(-1),
                ]),
        ]);
    let response: ListOffsetsResponse = call(gateway, API_KEY_LIST_OFFSETS, 5, &request).await;
    let partition = &response.topics[0].partitions[0];
    (partition.error_code == ERROR_NONE).then_some(partition.offset)
}

fn record(offset: i64, value: &str) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset,
        // A fixed `offset - sequence`, so the encoder writes one batch, as Kafka requires.
        sequence: NO_SEQUENCE + i32::try_from(offset).expect("a small offset"),
        timestamp: 1_700_000_000_000,
        key: None,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: IndexMap::new(),
    }
}

/// Produces `values` to partition 0 of `topic` with `acks=all`: the error code, message and base
/// offset of the answer.
async fn produce(gateway: SocketAddr, topic: &str, values: &[&str]) -> (i16, Option<String>, i64) {
    let records: Vec<Record> = (0..)
        .zip(values)
        .map(|(offset, value)| record(offset, value))
        .collect();
    let mut batch = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut batch, records.iter(), &options).expect("encode the batch");
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(30_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(name(topic))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(batch.freeze())),
                ]),
        ]);
    let response: ProduceResponse = call(gateway, API_KEY_PRODUCE, 9, &request).await;
    let partition = &response.responses[0].partition_responses[0];
    (
        partition.error_code,
        partition.error_message.as_ref().map(ToString::to_string),
        partition.base_offset,
    )
}

async fn delete_topic(gateway: SocketAddr, topic: &str) -> i16 {
    let request = DeleteTopicsRequest::default()
        .with_timeout_ms(30_000)
        .with_topic_names(vec![name(topic)]);
    let response: DeleteTopicsResponse = call(gateway, API_KEY_DELETE_TOPICS, 4, &request).await;
    response.responses[0].error_code
}

async fn create_partitions(gateway: SocketAddr, topic: &str, count: i32) -> i16 {
    let request = CreatePartitionsRequest::default()
        .with_timeout_ms(30_000)
        .with_topics(vec![
            CreatePartitionsTopic::default()
                .with_name(name(topic))
                .with_count(count)
                .with_assignments(None),
        ]);
    let response: CreatePartitionsResponse =
        call(gateway, API_KEY_CREATE_PARTITIONS, 2, &request).await;
    response.results[0].error_code
}

/// The value, read-only flag and source that `DescribeConfigs` reports for `key` of `topic`.
async fn describe(gateway: SocketAddr, topic: &str, key: &'static str) -> (String, bool, i8) {
    let request = DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(RESOURCE_TOPIC)
            .with_resource_name(StrBytes::from_string(topic.to_string()))
            .with_configuration_keys(Some(vec![StrBytes::from_static_str(key)])),
    ]);
    let response: DescribeConfigsResponse =
        call(gateway, API_KEY_DESCRIBE_CONFIGS, 4, &request).await;
    let result = &response.results[0];
    assert_eq!(result.error_code, ERROR_NONE, "{:?}", result.error_message);
    let config = &result.configs[0];
    (
        config.value.as_ref().map_or_default(ToString::to_string),
        config.read_only,
        config.config_source,
    )
}

/// Sets `key` of `topic` to `value`: the error code and message of the answer.
async fn alter(
    gateway: SocketAddr,
    topic: &str,
    key: &'static str,
    value: &'static str,
) -> (i16, Option<String>) {
    let request = IncrementalAlterConfigsRequest::default().with_resources(vec![
        AlterConfigsResource::default()
            .with_resource_type(RESOURCE_TOPIC)
            .with_resource_name(StrBytes::from_string(topic.to_string()))
            .with_configs(vec![
                AlterableConfig::default()
                    .with_name(StrBytes::from_static_str(key))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_static_str(value))),
            ]),
    ]);
    let response: IncrementalAlterConfigsResponse =
        call(gateway, API_KEY_INCREMENTAL_ALTER_CONFIGS, 1, &request).await;
    let result = &response.responses[0];
    (
        result.error_code,
        result.error_message.as_ref().map(ToString::to_string),
    )
}

/// The offset that `group` committed on partition 0 of each of `topics`, `-1` for none.
async fn committed(gateway: SocketAddr, group: &str, topics: &[&str]) -> Vec<i64> {
    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_topics(Some(
            topics
                .iter()
                .map(|topic| {
                    OffsetFetchRequestTopic::default()
                        .with_name(name(topic))
                        .with_partition_indexes(vec![0])
                })
                .collect(),
        ));
    let response: OffsetFetchResponse = call(gateway, API_KEY_OFFSET_FETCH, 6, &request).await;
    topics
        .iter()
        .map(|topic| {
            response
                .topics
                .iter()
                .find(|listed| listed.name.0.as_str() == *topic)
                .and_then(|listed| listed.partitions.first())
                .filter(|partition| partition.error_code == ERROR_NONE)
                .map_or(-1, |partition| partition.committed_offset)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_iggy_topic_type_geo_when_creating_a_topic_should_make_a_geo_topic_that_keeps_its_configuration()
 {
    let bucket = tempfile::tempdir().expect("a bucket");
    let dc = Datacenter::start("dc-a", bucket.path(), None).await;

    create_geo_topic(dc.addr(), TOPIC, 1).await;

    assert_eq!(
        describe(dc.addr(), TOPIC, TOPIC_TYPE).await,
        ("geo".to_string(), true, 1),
        "the type, read-only, set on the topic (DYNAMIC_TOPIC_CONFIG)"
    );
    let (code, _, base) = produce(dc.addr(), TOPIC, &["a", "b", "c"]).await;
    assert_eq!(
        (code, base),
        (ERROR_NONE, 0),
        "acks=all answers once the bucket holds it"
    );
    assert_eq!(end_offset(dc.addr(), TOPIC).await, Some(3));
    let (code, message) = alter(dc.addr(), TOPIC, "retention.ms", "60000").await;
    assert_eq!(code, ERROR_INVALID_CONFIG);
    assert_eq!(
        message.as_deref(),
        Some("Iggy keeps the configuration of a geo topic as it was created")
    );
    assert_eq!(
        create_partitions(dc.addr(), TOPIC, 2).await,
        ERROR_POLICY_VIOLATION,
        "a geo topic keeps its partition count"
    );
    let (code, _) = create_topic(dc.addr(), "normal", 1, &[]).await;
    assert_eq!(code, ERROR_NONE);
    assert_eq!(
        describe(dc.addr(), "normal", TOPIC_TYPE).await,
        ("normal".to_string(), true, 5),
        "the default (DEFAULT_CONFIG)"
    );

    dc.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_geo_topic_of_another_datacenter_should_list_its_shadow_as_the_origin_and_the_topic_and_refuse_writes_to_it()
 {
    let dcs = Datacenters::start().await;
    create_geo_topic(dcs.a.addr(), TOPIC, 1).await;
    let (code, _, _) = produce(dcs.a.addr(), TOPIC, &["a0", "a1", "a2", "a3", "a4"]).await;
    assert_eq!(code, ERROR_NONE);

    eventually(SETTLE, "dc-b lists the shadow of dc-a's topic", || async {
        listed_topics(dcs.b.addr())
            .await
            .iter()
            .any(|topic| topic == SHADOW_OF_A)
            .then_some(())
    })
    .await;
    eventually(
        SETTLE,
        "dc-b serves the messages that dc-a published",
        || async { (end_offset(dcs.b.addr(), SHADOW_OF_A).await == Some(5)).then_some(()) },
    )
    .await;
    assert!(
        !listed_topics(dcs.a.addr())
            .await
            .iter()
            .any(|topic| topic.starts_with("dc-a.")),
        "a datacenter holds no shadow of itself"
    );
    assert_eq!(
        describe(dcs.b.addr(), SHADOW_OF_A, TOPIC_TYPE).await.0,
        "geo",
        "a shadow reads as geo"
    );

    let (code, message, _) = produce(dcs.b.addr(), SHADOW_OF_A, &["b0"]).await;
    assert_eq!(code, ERROR_INVALID_TOPIC_EXCEPTION, "{message:?}");
    assert_eq!(
        create_topic(dcs.b.addr(), SHADOW_OF_A, 1, &[]).await.0,
        ERROR_TOPIC_ALREADY_EXISTS,
        "a tool that makes sure the topic exists takes the shadow"
    );
    let (code, message) = create_topic(dcs.b.addr(), "dc-a.payments", 1, &[]).await;
    assert_eq!(code, ERROR_INVALID_TOPIC_EXCEPTION);
    assert!(
        message
            .as_deref()
            .is_some_and(|text| text.contains("datacenter 'dc-a'")),
        "{message:?}"
    );
    assert_eq!(
        delete_topic(dcs.b.addr(), SHADOW_OF_A).await,
        ERROR_INVALID_TOPIC_EXCEPTION
    );
    assert_eq!(
        alter(dcs.b.addr(), SHADOW_OF_A, "retention.ms", "60000")
            .await
            .0,
        ERROR_INVALID_CONFIG
    );
    assert_eq!(
        create_partitions(dcs.b.addr(), SHADOW_OF_A, 2).await,
        ERROR_INVALID_TOPIC_EXCEPTION,
        "answered at once, not after a wait for partitions that never come"
    );
    assert_eq!(
        end_offset(dcs.b.addr(), SHADOW_OF_A).await,
        Some(5),
        "no refused write reached the shadow"
    );

    dcs.stop().await;
}

/// The pattern a consumer that wants every datacenter's messages subscribes with: the local topic
/// and the shadow of each other datacenter's.
const PATTERN: &str = "(dc-a\\.|dc-b\\.)?orders";
const GROUP: &str = "billing";
/// The messages each datacenter writes to its own geo topic.
const SENT: usize = 20;
/// The messages the group reads in `dc-b` before it moves to `dc-a`.
const READ_FIRST: usize = 25;

/// Writes `values` to `orders` through the console producer, with `acks=all`.
fn produce_with_console(bootstrap: &str, values: &[String]) {
    let lines = format!("{}\\n", values.join("\\n"));
    let script = format!(
        "printf '{lines}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server \
         {bootstrap} --topic {TOPIC} --producer-property acks=all --producer-property \
         max.block.ms=20000"
    );
    let finished = run_to_end(
        KAFKA_IMAGE,
        &["sh".to_string(), "-c".to_string(), script],
        Duration::from_secs(120),
    );
    assert!(
        finished.succeeded && !finished.stderr.contains("ERROR"),
        "the console producer must send, got: {}",
        finished.stderr
    );
}

/// Reads `count` messages as group `GROUP` with the pattern subscription `PATTERN`, one message
/// a poll, so the offsets it commits as it closes are those of the messages it printed.
fn consume_with_console(bootstrap: &str, count: usize) -> Vec<String> {
    let args: Vec<String> = [
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        bootstrap,
        "--include",
        PATTERN,
        "--group",
        GROUP,
        "--from-beginning",
        "--max-messages",
        &count.to_string(),
        "--timeout-ms",
        "60000",
        "--consumer-property",
        "max.poll.records=1",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    let finished = run_to_end(KAFKA_IMAGE, &args, Duration::from_secs(180));
    assert!(
        finished.succeeded,
        "the console consumer must run, got: {}",
        finished.stderr
    );
    finished
        .stdout
        .lines()
        .filter(|line| line.starts_with("dc-"))
        .map(str::to_string)
        .collect()
}

/// The sequence numbers that each datacenter's values carry, in the order read.
fn by_origin(values: &[String]) -> BTreeMap<String, Vec<usize>> {
    let mut origins: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for value in values {
        let (origin, sequence) = value.rsplit_once('-').expect("an origin and a sequence");
        origins
            .entry(origin.to_string())
            .or_default()
            .push(sequence.parse().expect("a sequence number"));
    }
    origins
}

fn values_of(origin: &str) -> Vec<String> {
    (0..SENT)
        .map(|sequence| format!("{origin}-{sequence:03}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn given_a_kafka_group_with_a_pattern_subscription_when_it_moves_to_the_other_datacenter_should_read_both_logs_once_with_no_gaps()
 {
    if let Some(reason) = missing_prerequisite(&[KAFKA_IMAGE]) {
        skip(&reason);
        return;
    }
    let dcs = Datacenters::start().await;
    create_geo_topic(dcs.a.addr(), TOPIC, 1).await;
    create_geo_topic(dcs.b.addr(), TOPIC, 1).await;
    let (bootstrap_a, bootstrap_b) = (dcs.a.addr().to_string(), dcs.b.addr().to_string());
    let (to_a, to_b) = (bootstrap_a.clone(), bootstrap_b.clone());
    tokio::task::spawn_blocking(move || {
        produce_with_console(&to_a, &values_of("dc-a"));
        produce_with_console(&to_b, &values_of("dc-b"));
    })
    .await
    .expect("the producers ran");
    let published = |gateway: SocketAddr, shadow: &'static str| async move {
        let sent = i64::try_from(SENT).expect("a small count");
        (end_offset(gateway, shadow).await == Some(sent)).then_some(())
    };
    eventually(SETTLE, "dc-b serves every message of dc-a", || {
        published(dcs.b.addr(), SHADOW_OF_A)
    })
    .await;
    eventually(SETTLE, "dc-a serves every message of dc-b", || {
        published(dcs.a.addr(), "dc-b.orders")
    })
    .await;

    let consume_b = bootstrap_b.clone();
    let first = tokio::task::spawn_blocking(move || consume_with_console(&consume_b, READ_FIRST))
        .await
        .expect("the consumer ran in dc-b");
    assert_eq!(first.len(), READ_FIRST, "{first:?}");
    let first = by_origin(&first);
    for (origin, sequences) in &first {
        let expected: Vec<usize> = (0..sequences.len()).collect();
        assert_eq!(
            sequences, &expected,
            "dc-b reads the log of {origin} from its start, with no gaps or duplicates"
        );
    }
    let read = |origin: &str| first.get(origin).map_or(0, Vec::len);
    let expected = vec![
        i64::try_from(read("dc-a")).expect("a small count"),
        i64::try_from(read("dc-b")).expect("a small count"),
    ];
    let gateway_a = dcs.a.addr();
    eventually(SETTLE, "dc-a holds the group's positions from dc-b", || {
        let expected = expected.clone();
        async move {
            (committed(gateway_a, GROUP, &[TOPIC, "dc-b.orders"]).await == expected).then_some(())
        }
    })
    .await;

    let rest = 2 * SENT - READ_FIRST;
    let then = tokio::task::spawn_blocking(move || consume_with_console(&bootstrap_a, rest))
        .await
        .expect("the consumer ran in dc-a");
    assert_eq!(then.len(), rest, "{then:?}");
    for (origin, sequences) in by_origin(&then) {
        let expected: Vec<usize> = (read(&origin)..SENT).collect();
        assert_eq!(
            sequences, expected,
            "dc-a resumes the log of {origin} at the committed offset, with no gaps or duplicates"
        );
    }

    dcs.stop().await;
}
