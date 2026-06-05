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

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, Subcommand};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Encodable, StrBytes};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn};

#[derive(Parser)]
#[command(
    name = "kafka-message-gen",
    about = "Generate Kafka wire protocol binary messages for all API keys and versions",
    long_about = "Generates correctly-framed Kafka protocol requests from Kafka 4.1.0 schemas.\n\
Each output .bin file is TCP-ready: [len:i32][api_key:i16][api_version:i16][correlation_id:i32][client_id][payload]"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List all supported API keys with name and version range
    List,
    /// Generate binary .bin files for all API keys and versions
    Generate {
        #[arg(short, long, default_value = "kafka_messages")]
        output: PathBuf,
        /// Filter to a single API key integer
        #[arg(long)]
        api_key: Option<i16>,
        /// Filter to a single version
        #[arg(long)]
        version: Option<i16>,
        /// Print hex dump to stdout
        #[arg(long)]
        hex: bool,
    },
    /// Send messages to a live Kafka-compatible server and show responses
    Send {
        #[arg(long, default_value = "127.0.0.1:9092")]
        host: String,
        #[arg(long)]
        api_key: Option<i16>,
        #[arg(long)]
        version: Option<i16>,
        #[arg(long, default_value = "5000")]
        timeout_ms: u64,
    },
    /// Send all messages and report pass/fail — exit code 1 if any fail
    Verify {
        #[arg(long, default_value = "127.0.0.1:9092")]
        host: String,
        #[arg(long)]
        fail_fast: bool,
    },
}

// ── API Registry ─────────────────────────────────────────────────────────────
// Source: validVersions in apache/kafka trunk JSON schema files, Kafka 4.1.0
// Format: (api_key, name, min_version, max_version)
const API_REGISTRY: &[(i16, &str, i16, i16)] = &[
    (0, "Produce", 3, 13),
    (1, "Fetch", 4, 18),
    (2, "ListOffsets", 1, 11),
    (3, "Metadata", 0, 13),
    (8, "OffsetCommit", 2, 10),
    (9, "OffsetFetch", 1, 10),
    (10, "FindCoordinator", 0, 6),
    (11, "JoinGroup", 0, 9),
    (12, "Heartbeat", 0, 4),
    (13, "LeaveGroup", 0, 5),
    (14, "SyncGroup", 0, 5),
    (15, "DescribeGroups", 0, 6),
    (16, "ListGroups", 0, 5),
    (17, "SaslHandshake", 0, 1),
    (18, "ApiVersions", 0, 5),
    (19, "CreateTopics", 2, 7),
    (20, "DeleteTopics", 1, 6),
    (21, "DeleteRecords", 0, 2),
    (22, "InitProducerId", 0, 6),
    (23, "OffsetForLeaderEpoch", 2, 4),
    (24, "AddPartitionsToTxn", 0, 5),
    (25, "AddOffsetsToTxn", 0, 4),
    (26, "EndTxn", 0, 5),
    (27, "WriteTxnMarkers", 1, 2),
    (28, "TxnOffsetCommit", 0, 5),
    (29, "DescribeAcls", 1, 3),
    (30, "CreateAcls", 1, 3),
    (31, "DeleteAcls", 1, 3),
    (32, "DescribeConfigs", 1, 4),
    (33, "AlterConfigs", 0, 2),
    (34, "AlterReplicaLogDirs", 1, 2),
    (35, "DescribeLogDirs", 1, 5),
    (36, "SaslAuthenticate", 0, 2),
    (37, "CreatePartitions", 0, 3),
    (38, "CreateDelegationToken", 1, 3),
    (39, "RenewDelegationToken", 1, 2),
    (40, "ExpireDelegationToken", 1, 2),
    (41, "DescribeDelegationToken", 1, 3),
    (42, "DeleteGroups", 0, 2),
    (43, "ElectLeaders", 0, 2),
    (44, "IncrementalAlterConfigs", 0, 1),
    (45, "AlterPartitionReassignments", 0, 1),
    (46, "ListPartitionReassignments", 0, 1),
    (47, "OffsetDelete", 0, 0),
    (48, "DescribeClientQuotas", 0, 1),
    (49, "AlterClientQuotas", 0, 1),
    (50, "DescribeUserScramCredentials", 0, 0),
    (51, "AlterUserScramCredentials", 0, 0),
    (55, "DescribeQuorum", 2, 3),
    (56, "AlterPartition", 2, 3),
    (57, "UpdateFeatures", 0, 2),
    (60, "DescribeCluster", 0, 2),
    (61, "DescribeProducers", 0, 0),
    (64, "UnregisterBroker", 0, 0),
    (65, "DescribeTransactions", 0, 0),
    (66, "ListTransactions", 0, 1),
    (67, "AllocateProducerIds", 0, 0),
    (68, "ConsumerGroupHeartbeat", 0, 1),
    (69, "ConsumerGroupDescribe", 0, 1),
    (71, "GetTelemetrySubscriptions", 0, 0),
    (72, "PushTelemetry", 0, 0),
    (74, "AssignReplicasToDirs", 0, 0),
    (75, "DescribeTopicPartitions", 0, 0),
    (76, "ListClientMetricsResources", 0, 0),
];

// ── Flexible version table ────────────────────────────────────────────────────
// Source: flexibleVersions field in each Kafka JSON schema.
// Returns the first version using compact encoding, or None if never flexible.
fn first_flexible_version(api_key: i16) -> Option<i16> {
    match api_key {
        0 => Some(9),
        1 => Some(12),
        2 => Some(6),
        3 => Some(9),
        8 => Some(8),
        9 => Some(6),
        10 => Some(3),
        11 => Some(6),
        12 => Some(4),
        13 => Some(4),
        14 => Some(4),
        15 => Some(5),
        16 => Some(3),
        17 => None,
        18 => Some(3),
        19 => Some(5),
        20 => Some(4),
        21 => Some(2),
        22 => Some(2),
        23 => Some(4),
        24 => Some(3),
        25 => Some(3),
        26 => Some(3),
        27 => Some(1),
        28 => Some(3),
        29 => Some(2),
        30 => Some(2),
        31 => Some(2),
        32 => Some(4),
        33 => Some(2),
        34 => Some(2),
        35 => Some(2),
        36 => Some(2),
        37 => Some(2),
        38 => Some(2),
        39 => Some(2),
        40 => Some(2),
        41 => Some(2),
        42 => Some(2),
        43 => Some(2),
        44 => Some(1),
        45 => Some(1),
        46 => Some(1),
        47 => Some(0),
        48 => Some(1),
        49 => Some(1),
        50 => Some(0),
        51 => Some(0),
        55 => Some(2),
        56 => Some(2),
        57 => Some(1),
        60 => Some(0),
        61 => Some(0),
        64 => Some(0),
        65 => Some(0),
        66 => Some(0),
        67 => Some(0),
        68 => Some(0),
        69 => Some(0),
        71 => Some(0),
        72 => Some(0),
        74 => Some(0),
        75 => Some(0),
        76 => Some(0),
        _ => None,
    }
}

// ── Request framing ───────────────────────────────────────────────────────────
// Wire format (Kafka protocol spec):
//   [total_length: i32]        big-endian, excludes self
//   [api_key: i16]
//   [api_version: i16]
//   [correlation_id: i32]
//   [client_id_len: i16]       -1 = null
//   [client_id: bytes]
//   [tagged_fields: u8(0)]     only present for flexible versions
//   [payload: bytes]
fn frame_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
    payload: &[u8],
    flexible: bool,
) -> Bytes {
    let cid = client_id.as_bytes();
    let hlen = 2 + 2 + 4 + 2 + cid.len() + if flexible { 1 } else { 0 };
    let blen = hlen + payload.len();
    let mut buf = BytesMut::with_capacity(4 + blen);
    buf.put_i32(blen as i32);
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    buf.put_i16(cid.len() as i16);
    buf.put_slice(cid);
    if flexible {
        buf.put_u8(0x00);
    }
    buf.put_slice(payload);
    buf.freeze()
}

// ── Payload builders ──────────────────────────────────────────────────────────
// Build the API-specific encoded body for a given api_key and version.
// All required fields contain realistic non-zero values.
// Returns raw bytes WITHOUT the framing header.
fn build_payload(api_key: i16, version: i16) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    match api_key {
        18 => {
            let mut r = ApiVersionsRequest::default();
            if version >= 3 {
                r.client_software_name = StrBytes::from_static_str("kafka-message-gen");
                r.client_software_version = StrBytes::from_static_str("0.1.0");
            }
            r.encode(&mut buf, version).context("ApiVersions")?;
        }
        3 => {
            let mut r = MetadataRequest::default();
            if version >= 1 {
                r.topics = None;
            }
            if version >= 4 {
                r.allow_auto_topic_creation = true;
            }
            if version >= 8 {
                r.include_cluster_authorized_operations = false;
                r.include_topic_authorized_operations = false;
            }
            r.encode(&mut buf, version).context("Metadata")?;
        }
        0 => {
            use kafka_protocol::messages::produce_request::*;
            use kafka_protocol::records::{
                Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
            };
            let rec = Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: 0,
                timestamp: 1_700_000_000_000,
                key: Some(Bytes::from_static(b"test-key")),
                value: Some(Bytes::from_static(b"test-value")),
                headers: indexmap::IndexMap::new(),
            };
            let mut rb = BytesMut::new();
            RecordBatchEncoder::encode(
                &mut rb,
                [rec].iter(),
                &RecordEncodeOptions {
                    version: 2,
                    compression: Compression::None,
                },
            )
            .context("RecordBatch encode")?;
            let pd = TopicProduceData::default()
                .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(rb.freeze())),
                ]);
            let mut r = ProduceRequest::default()
                .with_acks(-1)
                .with_timeout_ms(5000)
                .with_topic_data(vec![pd]);
            if version >= 3 {
                r.transactional_id = None;
            }
            r.encode(&mut buf, version).context("Produce")?;
        }
        1 => {
            use kafka_protocol::messages::fetch_request::*;
            let fp = FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1_048_576);
            let ft = FetchTopic::default()
                .with_topic(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![fp]);
            let mut r = FetchRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_max_wait_ms(500)
                .with_min_bytes(1)
                .with_topics(vec![ft]);
            if version >= 3 {
                r.max_bytes = 52_428_800;
            }
            if version >= 4 {
                r.isolation_level = 0;
            }
            if version >= 7 {
                r.session_id = 0;
                r.session_epoch = -1;
            }
            r.encode(&mut buf, version).context("Fetch")?;
        }
        2 => {
            use kafka_protocol::messages::list_offsets_request::*;
            let p = ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1);
            let t = ListOffsetsTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![p]);
            ListOffsetsRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_isolation_level(0)
                .with_topics(vec![t])
                .encode(&mut buf, version)
                .context("ListOffsets")?;
        }
        8 => {
            use kafka_protocol::messages::offset_commit_request::*;
            let p = OffsetCommitRequestPartition::default()
                .with_partition_index(0)
                .with_committed_offset(42)
                .with_committed_metadata(Some(StrBytes::from_static_str("")));
            let t = OffsetCommitRequestTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![p]);
            OffsetCommitRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .with_topics(vec![t])
                .encode(&mut buf, version)
                .context("OffsetCommit")?;
        }
        9 => {
            OffsetFetchRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .encode(&mut buf, version)
                .context("OffsetFetch")?;
        }
        10 => {
            FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("test-group"))
                .with_key_type(0)
                .encode(&mut buf, version)
                .context("FindCoordinator")?;
        }
        11 => {
            use kafka_protocol::messages::join_group_request::*;
            let p = JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"\x00\x00\x00\x01\x00\x0atest-topic"));
            JoinGroupRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .with_session_timeout_ms(30_000)
                .with_rebalance_timeout_ms(300_000)
                .with_member_id(StrBytes::from_static_str(""))
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_protocols(vec![p])
                .encode(&mut buf, version)
                .context("JoinGroup")?;
        }
        12 => {
            HeartbeatRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .with_generation_id(1)
                .with_member_id(StrBytes::from_static_str("test-member-1"))
                .encode(&mut buf, version)
                .context("Heartbeat")?;
        }
        13 => {
            LeaveGroupRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .with_member_id(StrBytes::from_static_str("test-member-1"))
                .encode(&mut buf, version)
                .context("LeaveGroup")?;
        }
        14 => {
            SyncGroupRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .with_generation_id(1)
                .with_member_id(StrBytes::from_static_str("test-member-1"))
                .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
                .with_protocol_name(Some(StrBytes::from_static_str("range")))
                .encode(&mut buf, version)
                .context("SyncGroup")?;
        }
        15 => {
            DescribeGroupsRequest::default()
                .with_groups(vec![GroupId::from(StrBytes::from_static_str("test-group"))])
                .with_include_authorized_operations(false)
                .encode(&mut buf, version)
                .context("DescribeGroups")?;
        }
        16 => {
            ListGroupsRequest::default()
                .encode(&mut buf, version)
                .context("ListGroups")?;
        }
        17 => {
            SaslHandshakeRequest::default()
                .with_mechanism(StrBytes::from_static_str("PLAIN"))
                .encode(&mut buf, version)
                .context("SaslHandshake")?;
        }
        19 => {
            use kafka_protocol::messages::create_topics_request::*;
            let t = CreatableTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str(
                    "iggy-test-topic",
                )))
                .with_num_partitions(1)
                .with_replication_factor(1);
            CreateTopicsRequest::default()
                .with_topics(vec![t])
                .with_timeout_ms(30_000)
                .with_validate_only(false)
                .encode(&mut buf, version)
                .context("CreateTopics")?;
        }
        20 => {
            use kafka_protocol::messages::delete_topics_request::*;
            let r = if version >= 6 {
                DeleteTopicsRequest::default()
                    .with_topics(vec![DeleteTopicState::default().with_name(Some(
                        TopicName::from(StrBytes::from_static_str("iggy-test-topic")),
                    ))])
                    .with_timeout_ms(30_000)
            } else {
                DeleteTopicsRequest::default()
                    .with_topic_names(vec![TopicName::from(StrBytes::from_static_str(
                        "iggy-test-topic",
                    ))])
                    .with_timeout_ms(30_000)
            };
            r.encode(&mut buf, version).context("DeleteTopics")?;
        }
        21 => {
            use kafka_protocol::messages::delete_records_request::*;
            let p = DeleteRecordsPartition::default()
                .with_partition_index(0)
                .with_offset(0);
            let t = DeleteRecordsTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![p]);
            DeleteRecordsRequest::default()
                .with_topics(vec![t])
                .with_timeout_ms(30_000)
                .encode(&mut buf, version)
                .context("DeleteRecords")?;
        }
        22 => {
            InitProducerIdRequest::default()
                .with_transactional_id(None)
                .with_transaction_timeout_ms(60_000)
                .encode(&mut buf, version)
                .context("InitProducerId")?;
        }
        24 => {
            use kafka_protocol::messages::add_partitions_to_txn_request::*;
            let t = AddPartitionsToTxnTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![0i32]);
            AddPartitionsToTxnRequest::default()
                .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_static_str(
                    "test-txn",
                )))
                .with_v3_and_below_producer_id(ProducerId(100))
                .with_v3_and_below_producer_epoch(1)
                .with_v3_and_below_topics(vec![t])
                .encode(&mut buf, version)
                .context("AddPartitionsToTxn")?;
        }
        25 => {
            AddOffsetsToTxnRequest::default()
                .with_transactional_id(TransactionalId(StrBytes::from_static_str("test-txn")))
                .with_producer_id(ProducerId(100))
                .with_producer_epoch(1)
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .encode(&mut buf, version)
                .context("AddOffsetsToTxn")?;
        }
        26 => {
            EndTxnRequest::default()
                .with_transactional_id(TransactionalId(StrBytes::from_static_str("test-txn")))
                .with_producer_id(ProducerId(100))
                .with_producer_epoch(1)
                .with_committed(true)
                .encode(&mut buf, version)
                .context("EndTxn")?;
        }
        28 => {
            use kafka_protocol::messages::txn_offset_commit_request::*;
            let p = TxnOffsetCommitRequestPartition::default()
                .with_partition_index(0)
                .with_committed_offset(42)
                .with_committed_metadata(Some(StrBytes::from_static_str("")));
            let t = TxnOffsetCommitRequestTopic::default()
                .with_name(TopicName::from(StrBytes::from_static_str("test-topic")))
                .with_partitions(vec![p]);
            TxnOffsetCommitRequest::default()
                .with_transactional_id(TransactionalId(StrBytes::from_static_str("test-txn")))
                .with_group_id(GroupId::from(StrBytes::from_static_str("test-group")))
                .with_producer_id(ProducerId(100))
                .with_producer_epoch(1)
                .with_topics(vec![t])
                .encode(&mut buf, version)
                .context("TxnOffsetCommit")?;
        }
        32 => {
            use kafka_protocol::messages::describe_configs_request::*;
            let r = DescribeConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_static_str("test-topic"));
            DescribeConfigsRequest::default()
                .with_resources(vec![r])
                .encode(&mut buf, version)
                .context("DescribeConfigs")?;
        }
        36 => {
            SaslAuthenticateRequest::default()
                .with_auth_bytes(Bytes::from_static(b"\x00iggy\x00secret"))
                .encode(&mut buf, version)
                .context("SaslAuthenticate")?;
        }
        other => {
            warn!("api_key={other}: no explicit builder — empty payload (framing test)");
        }
    }
    Ok(buf.freeze())
}

// Build a complete framed Kafka request message ready for TCP transmission.
fn build_framed(api_key: i16, version: i16, corr: i32) -> Result<Bytes> {
    let payload = build_payload(api_key, version)?;
    let flexible = first_flexible_version(api_key)
        .map(|fv| version >= fv)
        .unwrap_or(false);
    Ok(frame_request(
        api_key,
        version,
        corr,
        "kafka-message-gen",
        &payload,
        flexible,
    ))
}

// ── Commands ──────────────────────────────────────────────────────────────────

fn cmd_list() {
    println!(
        "{:<6} {:<42} {:<10} {:<10} {:<8}",
        "Key", "Name", "MinVer", "MaxVer", "Count"
    );
    println!("{}", "─".repeat(78));
    for &(k, n, min, max) in API_REGISTRY {
        println!(
            "{:<6} {:<42} {:<10} {:<10} {:<8}",
            k,
            n,
            min,
            max,
            max - min + 1
        );
    }
    let total: i16 = API_REGISTRY
        .iter()
        .map(|&(_, _, min, max)| max - min + 1)
        .sum();
    println!("{}", "─".repeat(78));
    println!(
        "Total: {} API keys  |  {} versioned messages",
        API_REGISTRY.len(),
        total
    );
}

async fn cmd_generate(
    out: PathBuf,
    fk: Option<i16>,
    fv: Option<i16>,
    hex_dump: bool,
) -> Result<()> {
    tokio::fs::create_dir_all(&out).await?;
    let (mut n, mut corr) = (0usize, 1i32);
    for &(ak, name, min, max) in API_REGISTRY {
        if fk.is_some_and(|k| k != ak) {
            continue;
        }
        for v in min..=max {
            if fv.is_some_and(|fv| fv != v) {
                continue;
            }
            match build_framed(ak, v, corr) {
                Ok(msg) => {
                    let fname = format!("{:03}_{}_v{}.bin", ak, name, v);
                    tokio::fs::write(out.join(&fname), &msg).await?;
                    if hex_dump {
                        println!("── {} v{} ({} bytes) ──", name, v, msg.len());
                        println!("{}", hex::encode(&msg));
                        println!();
                    } else {
                        info!("  {} ({} bytes)", fname, msg.len());
                    }
                    n += 1;
                    corr += 1;
                }
                Err(e) => warn!("SKIP {} v{}: {e}", name, v),
            }
        }
    }
    println!("\n✓ Generated {n} messages → {}/", out.display());
    println!(
        "  Quick test: cat {}/018_ApiVersions_v3.bin | nc 127.0.0.1 9092 | xxd",
        out.display()
    );
    Ok(())
}

async fn run_send(
    host: &str,
    fk: Option<i16>,
    fv: Option<i16>,
    toms: u64,
) -> Result<(usize, usize)> {
    let mut stream = TcpStream::connect(host)
        .await
        .with_context(|| format!("Cannot connect to {host}"))?;
    info!("Connected to {host}");
    let (mut ok, mut fail, mut corr) = (0usize, 0usize, 1i32);
    for &(ak, name, min, max) in API_REGISTRY {
        if fk.is_some_and(|k| k != ak) {
            continue;
        }
        for v in min..=max {
            if fv.is_some_and(|fv| fv != v) {
                continue;
            }
            let msg = match build_framed(ak, v, corr) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Build {} v{}: {e}", name, v);
                    fail += 1;
                    continue;
                }
            };
            stream
                .write_all(&msg)
                .await
                .with_context(|| format!("Write {} v{}", name, v))?;
            let res = tokio::time::timeout(std::time::Duration::from_millis(toms), async {
                let mut lb = [0u8; 4];
                stream.read_exact(&mut lb).await?;
                let mut body = vec![0u8; i32::from_be_bytes(lb) as usize];
                stream.read_exact(&mut body).await?;
                Ok::<Vec<u8>, std::io::Error>(body)
            })
            .await;
            match res {
                Ok(Ok(r)) => {
                    let ec = if r.len() >= 6 {
                        i16::from_be_bytes(r[4..6].try_into().unwrap())
                    } else {
                        -1
                    };
                    let sym = if ec <= 0 { "✓" } else { "⚠" };
                    println!("{sym} {} v{} → {} bytes  ec={ec}", name, v, r.len());
                    ok += 1;
                }
                Ok(Err(e)) => {
                    println!("✗ {} v{} → IO error: {e}", name, v);
                    fail += 1;
                }
                Err(_) => {
                    println!("✗ {} v{} → timeout ({}ms)", name, v, toms);
                    fail += 1;
                }
            }
            corr += 1;
        }
    }
    Ok((ok, fail))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::List => cmd_list(),
        Command::Generate {
            output,
            api_key,
            version,
            hex,
        } => cmd_generate(output, api_key, version, hex).await?,
        Command::Send {
            host,
            api_key,
            version,
            timeout_ms,
        } => {
            let (ok, fail) = run_send(&host, api_key, version, timeout_ms).await?;
            println!("\nResult: {ok} OK  {fail} failed");
        }
        Command::Verify { host, .. } => {
            let (ok, fail) = run_send(&host, None, None, 5000).await?;
            println!("\n=== Verify: {ok} passed  {fail} failed ===");
            if fail > 0 {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
