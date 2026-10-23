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

//! Command dispatch table mapping codes and operations to metadata.
//!
//! This is the protocol's identity registry. Every command has an entry
//! with its numeric code, human-readable name, and optional VSR operation.
//!
//! Two lookup paths:
//! - `lookup_command(code)`: current framing reads `[length][code][payload]`, looks up by code
//! - `lookup_by_operation(op)`: future VSR framing reads 256-byte header, looks up by operation

#[allow(clippy::wildcard_imports)]
use crate::codes::*;
use crate::consensus::Operation;

/// Metadata for a single protocol command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandMeta {
    pub code: u32,
    pub name: &'static str,
    /// VSR operation for replicated commands. `None` for non-replicated
    /// commands that bypass consensus.
    pub operation: Option<Operation>,
}

impl CommandMeta {
    /// Returns `true` if this command is replicated through VSR consensus.
    #[must_use]
    pub const fn is_replicated(&self) -> bool {
        self.operation.is_some()
    }

    const fn new(code: u32, name: &'static str, operation: Option<Operation>) -> Self {
        Self {
            code,
            name,
            operation,
        }
    }

    const fn non_replicated(code: u32, name: &'static str) -> Self {
        Self::new(code, name, None)
    }

    const fn replicated(code: u32, name: &'static str, op: Operation) -> Self {
        Self::new(code, name, Some(op))
    }
}

/// All known command metadata entries.
pub const COMMAND_TABLE: &[CommandMeta] = &[
    // System
    CommandMeta::non_replicated(PING_CODE, "ping"),
    CommandMeta::non_replicated(GET_STATS_CODE, "stats"),
    CommandMeta::non_replicated(GET_SNAPSHOT_FILE_CODE, "snapshot"),
    CommandMeta::non_replicated(GET_CLUSTER_METADATA_CODE, "cluster.metadata"),
    CommandMeta::non_replicated(GET_ME_CODE, "me"),
    CommandMeta::non_replicated(GET_CLIENT_CODE, "client.get"),
    CommandMeta::non_replicated(GET_CLIENTS_CODE, "client.list"),
    // Users
    CommandMeta::non_replicated(GET_USER_CODE, "user.get"),
    CommandMeta::non_replicated(GET_USERS_CODE, "user.list"),
    CommandMeta::replicated(CREATE_USER_CODE, "user.create", Operation::CreateUser),
    CommandMeta::replicated(DELETE_USER_CODE, "user.delete", Operation::DeleteUser),
    CommandMeta::replicated(
        UPDATE_PERMISSIONS_CODE,
        "user.permissions",
        Operation::UpdatePermissions,
    ),
    CommandMeta::replicated(
        CHANGE_PASSWORD_CODE,
        "user.password",
        Operation::ChangePassword,
    ),
    CommandMeta::non_replicated(LOGIN_USER_CODE, "user.login"),
    CommandMeta::non_replicated(LOGOUT_USER_CODE, "user.logout"),
    CommandMeta::non_replicated(LOGIN_REGISTER_CODE, "user.login_register"),
    // Personal Access Tokens
    CommandMeta::non_replicated(
        GET_PERSONAL_ACCESS_TOKENS_CODE,
        "personal_access_token.list",
    ),
    CommandMeta::replicated(
        CREATE_PERSONAL_ACCESS_TOKEN_CODE,
        "personal_access_token.create",
        Operation::CreatePersonalAccessToken,
    ),
    CommandMeta::replicated(
        DELETE_PERSONAL_ACCESS_TOKEN_CODE,
        "personal_access_token.delete",
        Operation::DeletePersonalAccessToken,
    ),
    CommandMeta::non_replicated(
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
        "personal_access_token.login",
    ),
    // Messages
    CommandMeta::non_replicated(POLL_MESSAGES_CODE, "message.poll"),
    CommandMeta::replicated(SEND_MESSAGES_CODE, "message.send", Operation::SendMessages),
    // Consumer Offsets
    CommandMeta::non_replicated(GET_CONSUMER_OFFSET_CODE, "consumer_offset.get"),
    CommandMeta::replicated(
        STORE_CONSUMER_OFFSET_CODE,
        "consumer_offset.store",
        Operation::StoreConsumerOffset,
    ),
    CommandMeta::replicated(
        DELETE_CONSUMER_OFFSET_CODE,
        "consumer_offset.delete",
        Operation::DeleteConsumerOffset,
    ),
    // Streams
    CommandMeta::non_replicated(GET_STREAM_CODE, "stream.get"),
    CommandMeta::non_replicated(GET_STREAMS_CODE, "stream.list"),
    CommandMeta::replicated(CREATE_STREAM_CODE, "stream.create", Operation::CreateStream),
    CommandMeta::replicated(DELETE_STREAM_CODE, "stream.delete", Operation::DeleteStream),
    // Topics
    CommandMeta::non_replicated(GET_TOPIC_CODE, "topic.get"),
    CommandMeta::non_replicated(GET_TOPICS_CODE, "topic.list"),
    CommandMeta::replicated(CREATE_TOPIC_CODE, "topic.create", Operation::CreateTopic),
    CommandMeta::replicated(DELETE_TOPIC_CODE, "topic.delete", Operation::DeleteTopic),
    // Partitions
    CommandMeta::replicated(
        CREATE_PARTITIONS_CODE,
        "partition.create",
        Operation::CreatePartitions,
    ),
    CommandMeta::replicated(
        DELETE_PARTITIONS_CODE,
        "partition.delete",
        Operation::DeletePartitions,
    ),
    // Segments
    CommandMeta::replicated(
        DELETE_SEGMENTS_CODE,
        "segment.delete",
        Operation::DeleteSegments,
    ),
    // Consumer Groups
    CommandMeta::non_replicated(GET_CONSUMER_GROUP_CODE, "consumer_group.get"),
    CommandMeta::non_replicated(GET_CONSUMER_GROUPS_CODE, "consumer_group.list"),
    CommandMeta::replicated(
        CREATE_CONSUMER_GROUP_CODE,
        "consumer_group.create",
        Operation::CreateConsumerGroup,
    ),
    CommandMeta::replicated(
        DELETE_CONSUMER_GROUP_CODE,
        "consumer_group.delete",
        Operation::DeleteConsumerGroup,
    ),
    CommandMeta::replicated(
        JOIN_CONSUMER_GROUP_CODE,
        "consumer_group.join",
        Operation::JoinConsumerGroup,
    ),
    CommandMeta::replicated(
        LEAVE_CONSUMER_GROUP_CODE,
        "consumer_group.leave",
        Operation::LeaveConsumerGroup,
    ),
    CommandMeta::non_replicated(SYNC_CONSUMER_GROUP_CODE, "consumer_group.sync"),
    // Login + Register (PAT - Personal Access Token variant)
    CommandMeta::non_replicated(LOGIN_REGISTER_WITH_PAT_CODE, "user.login_register_with_pat"),
    // Options catalog discovery
    CommandMeta::non_replicated(DESCRIBE_OPTIONS_CODE, "options.describe"),
    CommandMeta::non_replicated(BIND_SESSION_CODE, "session.bind"),
    CommandMeta::non_replicated(GET_PARTITION_ROUTE_CODE, "partition.route"),
    // Generic option patch for streams, topics and users
    CommandMeta::replicated(
        UPDATE_RESOURCE_CODE,
        "resource.update",
        Operation::UpdateResource,
    ),
];

/// Lookup command metadata by command code.
///
/// Uses a `match` (compiled to a jump table / binary search) for O(1) lookup
/// instead of linear scan. The match maps code -> table index, keeping
/// `COMMAND_TABLE` as the single source of truth for all metadata.
#[must_use]
pub const fn lookup_command(code: u32) -> Option<&'static CommandMeta> {
    // Indices must match the order of entries in COMMAND_TABLE above.
    let idx = match code {
        PING_CODE => 0,
        GET_STATS_CODE => 1,
        GET_SNAPSHOT_FILE_CODE => 2,
        GET_CLUSTER_METADATA_CODE => 3,
        GET_ME_CODE => 4,
        GET_CLIENT_CODE => 5,
        GET_CLIENTS_CODE => 6,
        GET_USER_CODE => 7,
        GET_USERS_CODE => 8,
        CREATE_USER_CODE => 9,
        DELETE_USER_CODE => 10,
        UPDATE_PERMISSIONS_CODE => 11,
        CHANGE_PASSWORD_CODE => 12,
        LOGIN_USER_CODE => 13,
        LOGOUT_USER_CODE => 14,
        LOGIN_REGISTER_CODE => 15,
        GET_PERSONAL_ACCESS_TOKENS_CODE => 16,
        CREATE_PERSONAL_ACCESS_TOKEN_CODE => 17,
        DELETE_PERSONAL_ACCESS_TOKEN_CODE => 18,
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE => 19,
        POLL_MESSAGES_CODE => 20,
        SEND_MESSAGES_CODE => 21,
        GET_CONSUMER_OFFSET_CODE => 22,
        STORE_CONSUMER_OFFSET_CODE => 23,
        DELETE_CONSUMER_OFFSET_CODE => 24,
        GET_STREAM_CODE => 25,
        GET_STREAMS_CODE => 26,
        CREATE_STREAM_CODE => 27,
        DELETE_STREAM_CODE => 28,
        GET_TOPIC_CODE => 29,
        GET_TOPICS_CODE => 30,
        CREATE_TOPIC_CODE => 31,
        DELETE_TOPIC_CODE => 32,
        CREATE_PARTITIONS_CODE => 33,
        DELETE_PARTITIONS_CODE => 34,
        DELETE_SEGMENTS_CODE => 35,
        GET_CONSUMER_GROUP_CODE => 36,
        GET_CONSUMER_GROUPS_CODE => 37,
        CREATE_CONSUMER_GROUP_CODE => 38,
        DELETE_CONSUMER_GROUP_CODE => 39,
        JOIN_CONSUMER_GROUP_CODE => 40,
        LEAVE_CONSUMER_GROUP_CODE => 41,
        SYNC_CONSUMER_GROUP_CODE => 42,
        LOGIN_REGISTER_WITH_PAT_CODE => 43,
        DESCRIBE_OPTIONS_CODE => 44,
        BIND_SESSION_CODE => 45,
        GET_PARTITION_ROUTE_CODE => 46,
        UPDATE_RESOURCE_CODE => 47,
        _ => return None,
    };
    Some(&COMMAND_TABLE[idx])
}

/// Lookup command metadata by VSR operation.
///
/// Returns `None` for `Operation::Reserved` and for non-replicated commands
/// that have no operation mapping.
#[must_use]
pub const fn lookup_by_operation(op: Operation) -> Option<&'static CommandMeta> {
    // Indices must match the order of entries in COMMAND_TABLE above.
    let idx = match op {
        Operation::CreateUser => 9,
        Operation::DeleteUser => 10,
        Operation::UpdatePermissions => 11,
        Operation::ChangePassword => 12,
        Operation::CreatePersonalAccessToken => 17,
        Operation::DeletePersonalAccessToken => 18,
        Operation::SendMessages => 21,
        Operation::StoreConsumerOffset => 23,
        Operation::DeleteConsumerOffset => 24,
        Operation::CreateStream => 27,
        Operation::DeleteStream => 28,
        Operation::CreateTopic => 31,
        Operation::DeleteTopic => 32,
        Operation::CreatePartitions => 33,
        Operation::DeletePartitions => 34,
        Operation::DeleteSegments => 35,
        Operation::CreateConsumerGroup => 38,
        Operation::DeleteConsumerGroup => 39,
        Operation::JoinConsumerGroup => 40,
        Operation::LeaveConsumerGroup => 41,
        Operation::UpdateResource => 47,
        Operation::CreateTopicWithAssignments
        | Operation::UpdateStreamResolved
        | Operation::UpdateTopicResolved
        | Operation::UpdateUserResolved
        | Operation::RewrapStreamKeys
        | Operation::CreatePartitionsWithAssignments
        | Operation::RemoveConsumerGroupMember
        | Operation::CompleteConsumerGroupRevocation
        | Operation::TruncatePartition
        | Operation::Reserved
        | Operation::Register
        | Operation::Logout
        | Operation::NonReplicated
        | Operation::FinalizeSession
        | Operation::RetireSession
        | Operation::CompleteLifecycle
        | Operation::TransitionPartitionHistory
        | Operation::InstallConsumerGroupOwner
        | Operation::RetireConsumerGroupOwners => return None,
    };
    Some(&COMMAND_TABLE[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_duplicate_codes_in_table() {
        let mut seen = std::collections::HashSet::new();
        for entry in COMMAND_TABLE {
            assert!(
                seen.insert(entry.code),
                "duplicate code {} ({}) in COMMAND_TABLE",
                entry.code,
                entry.name
            );
        }
    }

    #[test]
    fn unknown_code_returns_none() {
        assert!(lookup_command(9999).is_none());
    }

    #[test]
    fn names_are_non_empty() {
        for entry in COMMAND_TABLE {
            assert!(!entry.name.is_empty(), "empty name for code {}", entry.code);
        }
    }

    #[test]
    fn reserved_operation_returns_none() {
        assert!(lookup_by_operation(Operation::Reserved).is_none());
    }

    #[test]
    fn no_duplicate_operations_in_table() {
        let mut seen = std::collections::HashSet::new();
        for entry in COMMAND_TABLE {
            if let Some(op) = entry.operation {
                assert!(
                    seen.insert(op as u8),
                    "duplicate operation {:?} ({}) in COMMAND_TABLE",
                    op,
                    entry.name
                );
            }
        }
    }

    /// Verify that the match indices in `lookup_command` point to entries
    /// whose `.code` field actually matches the looked-up code.
    /// Catches table reordering that would silently break the match.
    #[test]
    fn lookup_command_indices_match_table_codes() {
        for (i, entry) in COMMAND_TABLE.iter().enumerate() {
            let looked_up = lookup_command(entry.code)
                .unwrap_or_else(|| panic!("lookup_command({}) returned None", entry.code));
            assert_eq!(
                looked_up.code, entry.code,
                "lookup_command({}) returned entry at wrong index \
                 (expected table[{i}].code={}, got code={})",
                entry.code, entry.code, looked_up.code
            );
        }
    }

    /// Verify that `lookup_by_operation` indices point to entries whose
    /// `.operation` field matches the looked-up operation.
    #[test]
    fn lookup_by_operation_indices_match_table_ops() {
        for entry in COMMAND_TABLE {
            if let Some(op) = entry.operation {
                let looked_up = lookup_by_operation(op)
                    .unwrap_or_else(|| panic!("lookup_by_operation({op:?}) returned None"));
                assert_eq!(
                    looked_up.operation,
                    Some(op),
                    "lookup_by_operation({op:?}) returned entry with wrong operation: {:?}",
                    looked_up.operation
                );
            }
        }
    }
}
