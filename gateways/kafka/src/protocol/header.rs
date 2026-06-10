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

#![allow(
    clippy::pedantic,
    clippy::missing_const_for_fn,
    clippy::match_same_arms
)]

use bytes::{BufMut, Bytes};

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::codec::{Decoder, Encoder};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

/// Returns the request header version to use for a given (api_key, api_version) pair.
///
/// Header v1 is the standard non-flexible format (nullable string client_id).
/// Header v2 is the flexible format (compact nullable string client_id + empty tagged fields).
/// The threshold at which each API key switches from v1 to v2 is defined by the Kafka protocol.
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    let flexible_from: i16 = match api_key {
        0 => 9,         // Produce
        1 => 12,        // Fetch
        2 => 6,         // ListOffsets
        3 => 9,         // Metadata
        4 => 4,         // LeaderAndIsr
        5 => 2,         // StopReplica
        6 => 6,         // UpdateMetadata
        7 => 3,         // ControlledShutdown
        8 => 8,         // OffsetCommit
        9 => 6,         // OffsetFetch
        10 => 3,        // FindCoordinator
        11 => 6,        // JoinGroup
        12 => 4,        // Heartbeat
        13 => 4,        // LeaveGroup
        14 => 4,        // SyncGroup
        15 => 5,        // DescribeGroups
        16 => 3,        // ListGroups
        17 => i16::MAX, // SaslHandshake — never flexible
        18 => 3,        // ApiVersions
        19 => 5,        // CreateTopics
        20 => 4,        // DeleteTopics
        21 => 2,        // DeleteRecords
        22 => 2,        // InitProducerId
        23 => 4,        // OffsetForLeaderEpoch
        24 => 3,        // AddPartitionsToTxn
        25 => 3,        // AddOffsetsToTxn
        26 => 3,        // EndTxn
        27 => 1,        // WriteTxnMarkers
        28 => 3,        // TxnOffsetCommit
        29 => 2,        // DescribeAcls
        30 => 2,        // CreateAcls
        31 => 2,        // DeleteAcls
        32 => 4,        // DescribeConfigs
        33 => 2,        // AlterConfigs
        34 => 2,        // AlterReplicaLogDirs
        35 => 2,        // DescribeLogDirs
        36 => 2,        // SaslAuthenticate
        37 => 2,        // CreatePartitions
        38 => 2,        // CreateDelegationToken
        39 => 2,        // RenewDelegationToken
        40 => 2,        // ExpireDelegationToken
        41 => 2,        // DescribeDelegationToken
        42 => 2,        // DeleteGroups
        43 => 2,        // ElectLeaders
        44 => 1,        // IncrementalAlterConfigs
        45 => 0,        // AlterPartitionReassignments — always flexible
        46 => 0,        // ListPartitionReassignments — always flexible
        47 => i16::MAX, // OffsetDelete — never flexible
        48 => 1,        // DescribeClientQuotas
        49 => 1,        // AlterClientQuotas
        50 => 0,        // DescribeUserScramCredentials — always flexible
        51 => 0,        // AlterUserScramCredentials — always flexible
        55 => 0,        // DescribeQuorum — always flexible
        56 => 0,        // AlterPartition — always flexible
        57 => 1,        // UpdateFeatures
        60 => 0,        // DescribeCluster — always flexible
        61 => 0,        // DescribeProducers — always flexible
        64 => 0,        // UnregisterBroker — always flexible
        65 => 0,        // DescribeTransactions — always flexible
        66 => 0,        // ListTransactions — always flexible
        67 => 0,        // AllocateProducerIds — always flexible
        68 => 0,        // ConsumerGroupHeartbeat — always flexible
        69 => 0,        // ConsumerGroupDescribe — always flexible
        71 => 0,        // GetTelemetrySubscriptions — always flexible
        72 => 0,        // PushTelemetry — always flexible
        74 => 0,        // AssignReplicasToDirs — always flexible
        75 => 0,        // DescribeTopicPartitions — always flexible
        76 => 0,        // ListClientMetricsResources — always flexible
        77 => 0,        // ShareGroupHeartbeat — always flexible (Kafka 4.0)
        78 => 0,        // ShareGroupDescribe — always flexible
        79 => 0,        // ShareFetch — always flexible
        80 => 0,        // ShareAcknowledge — always flexible
        _ => i16::MAX,  // Unknown API — assume non-flexible
    };
    if api_version >= flexible_from { 2 } else { 1 }
}

/// Returns the response header version to use when replying to a given (api_key, api_version).
///
/// ApiVersions (18) is a special case: the server ALWAYS returns response header v0 (no tagged
/// fields) so that clients that don't yet know the server supports flexible encoding can still
/// parse the discovery response.  All other flexible-version APIs use response header v1.
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    if api_key == 18 {
        return 0;
    }
    if request_header_version(api_key, api_version) >= 2 {
        1
    } else {
        0
    }
}

impl RequestHeader {
    pub fn decode(bytes: Bytes, header_version: i16) -> Result<Self> {
        let mut d = Decoder::new(bytes);
        Self::decode_from(&mut d, header_version)
    }

    /// Decode from a shared `Decoder`.
    ///
    /// Header v1 (non-flexible):
    ///   api_key i16 | api_version i16 | correlation_id i32 | client_id NULLABLE_STRING
    ///
    /// Header v2 (flexible):
    ///   api_key i16 | api_version i16 | correlation_id i32
    ///   | client_id COMPACT_NULLABLE_STRING | _tagged_fields UNSIGNED_VARINT
    pub fn decode_from(d: &mut Decoder, header_version: i16) -> Result<Self> {
        match header_version {
            1 => {
                let api_key = d.read_i16()?;
                let api_version = d.read_i16()?;
                let correlation_id = d.read_i32()?;
                let client_id = d.read_nullable_string()?;
                Ok(Self {
                    api_key,
                    api_version,
                    correlation_id,
                    client_id,
                })
            }
            2 => {
                let api_key = d.read_i16()?;
                let api_version = d.read_i16()?;
                let correlation_id = d.read_i32()?;
                let client_id = d.read_compact_nullable_string()?;
                d.read_tagged_fields()?;
                Ok(Self {
                    api_key,
                    api_version,
                    correlation_id,
                    client_id,
                })
            }
            v => Err(KafkaProtocolError::UnsupportedHeaderVersion(v)),
        }
    }
}

impl ResponseHeader {
    /// Encode the response header.
    ///
    /// v0: correlation_id i32  (non-flexible APIs and ApiVersions)
    /// v1: correlation_id i32 + empty tagged fields  (flexible APIs)
    pub fn encode(&self, header_version: i16) -> Bytes {
        let mut e = Encoder::with_capacity(5);
        e.write_i32(self.correlation_id);
        if header_version >= 1 {
            e.write_empty_tagged_fields();
        }
        e.freeze()
    }

    /// Write this header directly into an existing buffer (avoids a separate heap alloc).
    pub fn encode_into(&self, buf: &mut bytes::BytesMut, header_version: i16) {
        buf.put_i32(self.correlation_id);
        if header_version >= 1 {
            buf.put_u8(0); // empty tagged fields
        }
    }

    /// Byte size of the encoded header for a given version.
    #[must_use]
    pub fn encoded_size(header_version: i16) -> usize {
        if header_version >= 1 { 5 } else { 4 }
    }
}
