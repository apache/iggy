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
use iggy_gateway_kafka::protocol::api::supported_api_ranges;
use iggy_gateway_kafka::protocol::header::request_header_version;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Encodable, StrBytes};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn};

mod response;

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
        /// Filter to API key(s), repeatable: --api-key 0 --api-key 1
        #[arg(long, action = clap::ArgAction::Append)]
        api_key: Vec<i16>,
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
        /// Compact one-line output (default is verbose decoded response)
        #[arg(long)]
        quiet: bool,
    },
    Verify {
        #[arg(long, default_value = "127.0.0.1:9092")]
        host: String,
        /// Limit to these API keys (repeatable). Defaults to all gateway-scoped keys.
        #[arg(long, action = clap::ArgAction::Append)]
        api_key: Vec<i16>,
        /// Limit to a single protocol version
        #[arg(long)]
        version: Option<i16>,
        #[arg(long, default_value = "5000")]
        timeout_ms: u64,
        /// Stop on the first failure
        #[arg(long)]
        fail_fast: bool,
        /// Use the full Kafka 4.1 registry (for real brokers), not the Iggy gateway scope
        #[arg(long)]
        all_apis: bool,
        /// Compact one-line output (default is verbose decoded response)
        #[arg(long)]
        quiet: bool,
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

/// Gateway verify scope from `iggy_gateway_kafka::supported_api_ranges()`.
/// Names come from `API_REGISTRY` (Kafka catalog); version bounds come from the gateway.
fn gateway_verify_registry() -> Vec<(i16, &'static str, i16, i16)> {
    supported_api_ranges()
        .iter()
        .map(|range| {
            let name = API_REGISTRY
                .iter()
                .find(|(key, _, _, _)| *key == range.api_key)
                .map_or("Unknown", |(_, name, _, _)| *name);
            (range.api_key, name, range.min_version, range.max_version)
        })
        .collect()
}

// ── Request framing ───────────────────────────────────────────────────────────
// Wire format (Kafka protocol spec):
//   [total_length: i32]        big-endian, excludes self
//   [api_key: i16]
//   [api_version: i16]
//   [correlation_id: i32]
//   header v1: [client_id: NULLABLE_STRING]
//   header v2: [client_id: COMPACT_NULLABLE_STRING] [request_header_tagged_fields]
//   [payload: bytes]

fn write_unsigned_varint(buf: &mut BytesMut, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

fn write_compact_nullable_string(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        None => write_unsigned_varint(buf, 0),
        Some(s) => {
            write_unsigned_varint(buf, (s.len() + 1) as u64);
            buf.put_slice(s.as_bytes());
        }
    }
}

fn frame_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
    payload: &[u8],
    flexible: bool,
) -> Bytes {
    let mut header = BytesMut::new();
    header.put_i16(api_key);
    header.put_i16(api_version);
    header.put_i32(correlation_id);
    if flexible {
        write_compact_nullable_string(&mut header, Some(client_id));
        header.put_u8(0); // empty request-header tagged fields
    } else {
        header.put_i16(i16::try_from(client_id.len()).expect("client_id fits i16"));
        header.put_slice(client_id.as_bytes());
    }

    let blen = header.len() + payload.len();
    let mut buf = BytesMut::with_capacity(4 + blen);
    buf.put_i32(i32::try_from(blen).expect("frame fits i32"));
    buf.put_slice(&header);
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
    // Header v2 == flexible request encoding; threshold lives in iggy-gateway-kafka.
    let flexible = request_header_version(api_key, version) >= 2;
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
    filter_keys: Vec<i16>,
    fv: Option<i16>,
    hex_dump: bool,
) -> Result<()> {
    tokio::fs::create_dir_all(&out).await?;
    let (mut n, mut corr) = (0usize, 1i32);
    for &(ak, name, min, max) in API_REGISTRY {
        if !filter_keys.is_empty() && !filter_keys.contains(&ak) {
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

async fn connect(host: &str) -> Result<TcpStream> {
    TcpStream::connect(host)
        .await
        .with_context(|| format!("Cannot connect to {host}"))
}

async fn read_kafka_response(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut lb = [0u8; 4];
    stream.read_exact(&mut lb).await?;
    let frame_len = i32::from_be_bytes(lb);
    if frame_len <= 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid response frame length: {frame_len}"),
        ));
    }
    let mut body = vec![
        0u8;
        usize::try_from(frame_len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "response frame length does not fit usize",
            )
        })?
    ];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

#[allow(clippy::too_many_arguments)]
async fn run_send(
    host: &str,
    registry: &[(i16, &str, i16, i16)],
    filter_keys: &[i16],
    fv: Option<i16>,
    toms: u64,
    fail_fast: bool,
    quiet: bool,
    strict: bool,
) -> Result<(usize, usize)> {
    let mut stream = connect(host).await?;
    info!("Connected to {host}");
    let (mut ok, mut fail, mut corr) = (0usize, 0usize, 1i32);
    'outer: for &(ak, name, min, max) in registry {
        if !filter_keys.is_empty() && !filter_keys.contains(&ak) {
            continue;
        }
        for v in min..=max {
            if fv.is_some_and(|wanted| wanted != v) {
                continue;
            }
            let msg = match build_framed(ak, v, corr) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Build {} v{}: {e}", name, v);
                    fail += 1;
                    if fail_fast {
                        break 'outer;
                    }
                    continue;
                }
            };
            if let Err(e) = stream.write_all(&msg).await {
                println!("✗ {name} v{v} → write error: {e}");
                fail += 1;
                stream = connect(host).await?;
                if fail_fast {
                    break 'outer;
                }
                corr += 1;
                continue;
            }

            let res = tokio::time::timeout(
                std::time::Duration::from_millis(toms),
                read_kafka_response(&mut stream),
            )
            .await;

            match res {
                Ok(Ok(r)) => {
                    let summary = response::analyze_response(ak, v, corr, &r);
                    summary.print(name, v, quiet);
                    if strict && let Some(reason) = summary.verify_failure_reason(ak) {
                        println!("✗ {name} v{v} → verify fail: {reason}");
                        fail += 1;
                        if fail_fast {
                            break 'outer;
                        }
                        corr += 1;
                        continue;
                    }
                    ok += 1;
                }
                Ok(Err(e)) => {
                    println!("✗ {name} v{v} → IO error: {e}");
                    fail += 1;
                    stream = connect(host).await?;
                    if fail_fast {
                        break 'outer;
                    }
                }
                Err(_) => {
                    println!("✗ {name} v{v} → timeout ({toms}ms)");
                    fail += 1;
                    stream = connect(host).await?;
                    if fail_fast {
                        break 'outer;
                    }
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
            quiet,
        } => {
            let filter_keys: Vec<i16> = api_key.into_iter().collect();
            let (ok, fail) = run_send(
                &host,
                API_REGISTRY,
                &filter_keys,
                version,
                timeout_ms,
                false,
                quiet,
                false,
            )
            .await?;
            println!("\nResult: {ok} OK  {fail} failed");
        }
        Command::Verify {
            host,
            api_key,
            version,
            timeout_ms,
            fail_fast,
            all_apis,
            quiet,
        } => {
            let gateway_registry = gateway_verify_registry();
            let registry: &[(i16, &str, i16, i16)] = if all_apis {
                API_REGISTRY
            } else {
                &gateway_registry
            };
            let (ok, fail) = run_send(
                &host, registry, &api_key, version, timeout_ms, fail_fast, quiet, true,
            )
            .await?;
            println!("\n=== Verify: {ok} passed  {fail} failed ===");
            if fail > 0 {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
