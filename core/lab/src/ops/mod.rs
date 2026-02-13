/* Licensed to the Apache Software Foundation (ASF) under one
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

pub mod classify;
pub mod execute;
pub mod generate;
pub mod precondition;
pub mod shadow_update;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Op {
    CreateStream {
        name: String,
    },
    DeleteStream {
        name: String,
    },
    PurgeStream {
        name: String,
    },
    CreateTopic {
        stream: String,
        name: String,
        partitions: u32,
    },
    DeleteTopic {
        stream: String,
        topic: String,
    },
    PurgeTopic {
        stream: String,
        topic: String,
    },
    SendMessages {
        stream: String,
        topic: String,
        partition: u32,
        count: u32,
    },
    PollMessages {
        stream: String,
        topic: String,
        partition: u32,
        count: u32,
        consumer_id: u32,
    },
    DeleteSegments {
        stream: String,
        topic: String,
        partition: u32,
        count: u32,
    },
    CreatePartitions {
        stream: String,
        topic: String,
        count: u32,
    },
    DeletePartitions {
        stream: String,
        topic: String,
        count: u32,
    },
    CreateConsumerGroup {
        stream: String,
        topic: String,
        name: String,
    },
    DeleteConsumerGroup {
        stream: String,
        topic: String,
        name: String,
    },
    JoinConsumerGroup {
        stream: String,
        topic: String,
        group: String,
    },
    LeaveConsumerGroup {
        stream: String,
        topic: String,
        group: String,
    },
    GetConsumerGroup {
        stream: String,
        topic: String,
        group: String,
    },
    PollGroupMessages {
        stream: String,
        topic: String,
        group: String,
        count: u32,
    },
    StoreConsumerOffset {
        stream: String,
        topic: String,
        partition: u32,
        offset: u64,
        consumer_id: u32,
    },
    GetStreams,
    GetStreamDetails {
        stream: String,
    },
    GetTopicDetails {
        stream: String,
        topic: String,
    },
    GetStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum OpOutcome {
    Success {
        detail: Option<String>,
    },
    Error {
        class: ErrorClass,
        error: String,
        context: String,
    },
}

impl OpOutcome {
    pub fn success_from(detail: Option<String>) -> Self {
        Self::Success { detail }
    }

    pub fn error(class: ErrorClass, error: impl Into<String>, context: impl Into<String>) -> Self {
        Self::Error {
            class,
            error: error.into(),
            context: context.into(),
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    pub fn error_class(&self) -> Option<ErrorClass> {
        match self {
            Self::Error { class, .. } => Some(*class),
            Self::Success { .. } => None,
        }
    }

    pub fn result_tag(&self) -> &'static str {
        match self {
            Self::Success { .. } => "ok",
            Self::Error { class, .. } => class.tag(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// Another worker deleted the resource concurrently — benign
    ExpectedConcurrent,
    /// Server returned something that contradicts shadow state
    ServerBug,
    /// Transient network/timeout error — retry-safe
    Transient,
}

impl ErrorClass {
    pub fn tag(self) -> &'static str {
        match self {
            Self::ExpectedConcurrent => "expected_error",
            Self::ServerBug => "server_bug",
            Self::Transient => "transient",
        }
    }
}

/// Selects which kind of operation to generate (before target resolution).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum OpKind {
    CreateStream,
    DeleteStream,
    PurgeStream,
    CreateTopic,
    DeleteTopic,
    PurgeTopic,
    SendMessages,
    PollMessages,
    DeleteSegments,
    CreatePartitions,
    DeletePartitions,
    CreateConsumerGroup,
    DeleteConsumerGroup,
    JoinConsumerGroup,
    LeaveConsumerGroup,
    GetConsumerGroup,
    PollGroupMessages,
    StoreConsumerOffset,
    GetStreams,
    GetStreamDetails,
    GetTopicDetails,
    GetStats,
}

impl OpKind {
    pub fn is_creative(self) -> bool {
        matches!(
            self,
            Self::CreateStream
                | Self::CreateTopic
                | Self::CreatePartitions
                | Self::CreateConsumerGroup
        )
    }

    pub fn is_destructive(self) -> bool {
        matches!(
            self,
            Self::DeleteStream
                | Self::DeleteTopic
                | Self::PurgeStream
                | Self::PurgeTopic
                | Self::DeleteSegments
                | Self::DeletePartitions
                | Self::DeleteConsumerGroup
        )
    }

    /// Operations that remove or purge entire streams/topics.
    /// Always restricted to churn namespace at generation time.
    pub fn is_resource_destructive(self) -> bool {
        matches!(
            self,
            Self::DeleteStream | Self::DeleteTopic | Self::PurgeStream | Self::PurgeTopic
        )
    }
}

impl Op {
    pub fn kind_tag(&self) -> &'static str {
        match self {
            Self::CreateStream { .. } => "create_stream",
            Self::DeleteStream { .. } => "delete_stream",
            Self::PurgeStream { .. } => "purge_stream",
            Self::CreateTopic { .. } => "create_topic",
            Self::DeleteTopic { .. } => "delete_topic",
            Self::PurgeTopic { .. } => "purge_topic",
            Self::SendMessages { .. } => "send_messages",
            Self::PollMessages { .. } => "poll_messages",
            Self::DeleteSegments { .. } => "delete_segments",
            Self::CreatePartitions { .. } => "create_partitions",
            Self::DeletePartitions { .. } => "delete_partitions",
            Self::CreateConsumerGroup { .. } => "create_consumer_group",
            Self::DeleteConsumerGroup { .. } => "delete_consumer_group",
            Self::JoinConsumerGroup { .. } => "join_consumer_group",
            Self::LeaveConsumerGroup { .. } => "leave_consumer_group",
            Self::GetConsumerGroup { .. } => "get_consumer_group",
            Self::PollGroupMessages { .. } => "poll_group_messages",
            Self::StoreConsumerOffset { .. } => "store_consumer_offset",
            Self::GetStreams => "get_streams",
            Self::GetStreamDetails { .. } => "get_stream_details",
            Self::GetTopicDetails { .. } => "get_topic_details",
            Self::GetStats => "get_stats",
        }
    }

    /// Compact human-readable target context for traces and error messages.
    pub fn context(&self) -> String {
        match self {
            Self::CreateStream { name }
            | Self::DeleteStream { name }
            | Self::PurgeStream { name } => format!("stream={name}"),

            Self::CreateTopic {
                stream,
                name,
                partitions,
            } => format!("stream={stream} topic={name} partitions={partitions}"),

            Self::DeleteTopic { stream, topic } | Self::PurgeTopic { stream, topic } => {
                format!("stream={stream} topic={topic}")
            }

            Self::SendMessages {
                stream,
                topic,
                partition,
                count,
            } => format!("stream={stream} topic={topic} partition={partition} count={count}"),

            Self::PollMessages {
                stream,
                topic,
                partition,
                count,
                consumer_id,
            } => format!(
                "stream={stream} topic={topic} partition={partition} count={count} consumer={consumer_id}"
            ),

            Self::DeleteSegments {
                stream,
                topic,
                partition,
                count,
            } => format!("stream={stream} topic={topic} partition={partition} count={count}"),

            Self::CreatePartitions {
                stream,
                topic,
                count,
            }
            | Self::DeletePartitions {
                stream,
                topic,
                count,
            } => format!("stream={stream} topic={topic} count={count}"),

            Self::CreateConsumerGroup {
                stream,
                topic,
                name,
            }
            | Self::DeleteConsumerGroup {
                stream,
                topic,
                name,
            } => format!("stream={stream} topic={topic} group={name}"),

            Self::JoinConsumerGroup {
                stream,
                topic,
                group,
            }
            | Self::LeaveConsumerGroup {
                stream,
                topic,
                group,
            }
            | Self::GetConsumerGroup {
                stream,
                topic,
                group,
            } => format!("stream={stream} topic={topic} group={group}"),

            Self::PollGroupMessages {
                stream,
                topic,
                group,
                count,
            } => format!("stream={stream} topic={topic} group={group} count={count}"),

            Self::StoreConsumerOffset {
                stream,
                topic,
                partition,
                offset,
                consumer_id,
            } => format!(
                "stream={stream} topic={topic} partition={partition} offset={offset} consumer={consumer_id}"
            ),

            Self::GetStreams => "all".into(),
            Self::GetStreamDetails { stream } => format!("stream={stream}"),
            Self::GetTopicDetails { stream, topic } => {
                format!("stream={stream} topic={topic}")
            }
            Self::GetStats => "server".into(),
        }
    }

    /// Whether this op mutates shadow state on success.
    /// Non-mutating ops (data-plane reads/writes, gets) can skip the write lock.
    pub fn mutates_shadow(&self) -> bool {
        matches!(
            self,
            Self::CreateStream { .. }
                | Self::DeleteStream { .. }
                | Self::PurgeStream { .. }
                | Self::CreateTopic { .. }
                | Self::DeleteTopic { .. }
                | Self::PurgeTopic { .. }
                | Self::CreatePartitions { .. }
                | Self::DeletePartitions { .. }
                | Self::CreateConsumerGroup { .. }
                | Self::DeleteConsumerGroup { .. }
        )
    }
}
