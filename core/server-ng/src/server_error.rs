/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use metadata::impls::recovery::RecoveryError;
// TODO: decouple logging errors from the `server` crate.
use server::server_error::LogError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerNgError {
    #[error("failed to load server-ng config")]
    Config(#[source] configs::ConfigurationError),
    #[error("failed to prepare server-ng directories")]
    CreateDirectories(#[source] iggy_common::IggyError),
    #[error("failed to serialize current server-ng config")]
    CurrentConfigSerialize(#[source] toml::ser::Error),
    #[error("failed to write current server-ng config at {path}")]
    CurrentConfigWrite {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to initialize server-ng logging")]
    Logging(#[source] LogError),
    #[error("failed to recover metadata snapshot and journal")]
    MetadataRecovery(#[source] RecoveryError),
    #[error("failed to parse {context} socket address '{address}'")]
    SocketAddressParse {
        context: &'static str,
        address: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("cluster enabled but no node is configured for replica {replica_id}")]
    ClusterNodeNotFound { replica_id: u8 },
    #[error("cluster node count {count} exceeds supported u8 replica count")]
    ClusterReplicaCountTooLarge { count: usize },
    #[error("cluster mode requires --replica-id to identify the current node")]
    MissingReplicaId,
    #[error("cluster node for replica {replica_id} is missing tcp_replica port")]
    ClusterReplicaPortMissing { replica_id: u8 },
    #[error(
        "recovered namespace stream {stream_id}, topic {topic_id}, partition {partition_id} exceeds configured limits (max_streams={max_streams}, max_topics={max_topics}, max_partitions={max_partitions})"
    )]
    RecoveredNamespaceOutOfBounds {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        max_streams: usize,
        max_topics: usize,
        max_partitions: usize,
    },
    #[error(
        "failed to load partition log for stream {stream_id}, topic {topic_id}, partition {partition_id}"
    )]
    PartitionLogLoad {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        #[source]
        source: iggy_common::IggyError,
    },
    #[error("failed to start server-ng TCP listeners")]
    StartTcpListeners(#[source] iggy_common::IggyError),
    #[error(
        "failed to initialize messages writer for stream {stream_id}, topic {topic_id}, partition {partition_id}"
    )]
    MessagesWriterInit {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        #[source]
        source: iggy_common::IggyError,
    },
    #[error(
        "failed to initialize index writer for stream {stream_id}, topic {topic_id}, partition {partition_id}"
    )]
    IndexWriterInit {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        #[source]
        source: iggy_common::IggyError,
    },
    #[error(
        "failed to load segment indexes for stream {stream_id}, topic {topic_id}, partition {partition_id}"
    )]
    SegmentIndexesLoad {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        #[source]
        source: iggy_common::IggyError,
    },
    #[error(
        "failed to create initial segment storage for stream {stream_id}, topic {topic_id}, partition {partition_id}"
    )]
    InitialSegmentStorage {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        #[source]
        source: iggy_common::IggyError,
    },
}
