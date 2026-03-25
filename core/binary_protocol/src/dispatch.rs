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
    CommandMeta::replicated(UPDATE_USER_CODE, "user.update", Operation::UpdateUser),
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
    CommandMeta::non_replicated(FLUSH_UNSAVED_BUFFER_CODE, "message.flush_unsaved_buffer"),
    // Consumer Offsets
    CommandMeta::non_replicated(GET_CONSUMER_OFFSET_CODE, "consumer_offset.get"),
    CommandMeta::replicated(
        STORE_CONSUMER_OFFSET_CODE,
        "consumer_offset.store",
        Operation::StoreConsumerOffset,
    ),
    CommandMeta::non_replicated(DELETE_CONSUMER_OFFSET_CODE, "consumer_offset.delete"),
    // Streams
    CommandMeta::non_replicated(GET_STREAM_CODE, "stream.get"),
    CommandMeta::non_replicated(GET_STREAMS_CODE, "stream.list"),
    CommandMeta::replicated(CREATE_STREAM_CODE, "stream.create", Operation::CreateStream),
    CommandMeta::replicated(DELETE_STREAM_CODE, "stream.delete", Operation::DeleteStream),
    CommandMeta::replicated(UPDATE_STREAM_CODE, "stream.update", Operation::UpdateStream),
    CommandMeta::replicated(PURGE_STREAM_CODE, "stream.purge", Operation::PurgeStream),
    // Topics
    CommandMeta::non_replicated(GET_TOPIC_CODE, "topic.get"),
    CommandMeta::non_replicated(GET_TOPICS_CODE, "topic.list"),
    CommandMeta::replicated(CREATE_TOPIC_CODE, "topic.create", Operation::CreateTopic),
    CommandMeta::replicated(DELETE_TOPIC_CODE, "topic.delete", Operation::DeleteTopic),
    CommandMeta::replicated(UPDATE_TOPIC_CODE, "topic.update", Operation::UpdateTopic),
    CommandMeta::replicated(PURGE_TOPIC_CODE, "topic.purge", Operation::PurgeTopic),
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
    CommandMeta::non_replicated(JOIN_CONSUMER_GROUP_CODE, "consumer_group.join"),
    CommandMeta::non_replicated(LEAVE_CONSUMER_GROUP_CODE, "consumer_group.leave"),
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
        UPDATE_USER_CODE => 11,
        UPDATE_PERMISSIONS_CODE => 12,
        CHANGE_PASSWORD_CODE => 13,
        LOGIN_USER_CODE => 14,
        LOGOUT_USER_CODE => 15,
        GET_PERSONAL_ACCESS_TOKENS_CODE => 16,
        CREATE_PERSONAL_ACCESS_TOKEN_CODE => 17,
        DELETE_PERSONAL_ACCESS_TOKEN_CODE => 18,
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE => 19,
        POLL_MESSAGES_CODE => 20,
        SEND_MESSAGES_CODE => 21,
        FLUSH_UNSAVED_BUFFER_CODE => 22,
        GET_CONSUMER_OFFSET_CODE => 23,
        STORE_CONSUMER_OFFSET_CODE => 24,
        DELETE_CONSUMER_OFFSET_CODE => 25,
        GET_STREAM_CODE => 26,
        GET_STREAMS_CODE => 27,
        CREATE_STREAM_CODE => 28,
        DELETE_STREAM_CODE => 29,
        UPDATE_STREAM_CODE => 30,
        PURGE_STREAM_CODE => 31,
        GET_TOPIC_CODE => 32,
        GET_TOPICS_CODE => 33,
        CREATE_TOPIC_CODE => 34,
        DELETE_TOPIC_CODE => 35,
        UPDATE_TOPIC_CODE => 36,
        PURGE_TOPIC_CODE => 37,
        CREATE_PARTITIONS_CODE => 38,
        DELETE_PARTITIONS_CODE => 39,
        DELETE_SEGMENTS_CODE => 40,
        GET_CONSUMER_GROUP_CODE => 41,
        GET_CONSUMER_GROUPS_CODE => 42,
        CREATE_CONSUMER_GROUP_CODE => 43,
        DELETE_CONSUMER_GROUP_CODE => 44,
        JOIN_CONSUMER_GROUP_CODE => 45,
        LEAVE_CONSUMER_GROUP_CODE => 46,
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
        Operation::CreateStream => 28,
        Operation::UpdateStream => 30,
        Operation::DeleteStream => 29,
        Operation::PurgeStream => 31,
        Operation::CreateTopic => 34,
        Operation::UpdateTopic => 36,
        Operation::DeleteTopic => 35,
        Operation::PurgeTopic => 37,
        Operation::CreatePartitions => 38,
        Operation::DeletePartitions => 39,
        Operation::DeleteSegments => 40,
        Operation::CreateConsumerGroup => 43,
        Operation::DeleteConsumerGroup => 44,
        Operation::CreateUser => 9,
        Operation::UpdateUser => 11,
        Operation::DeleteUser => 10,
        Operation::ChangePassword => 13,
        Operation::UpdatePermissions => 12,
        Operation::CreatePersonalAccessToken => 17,
        Operation::DeletePersonalAccessToken => 18,
        Operation::SendMessages => 21,
        Operation::StoreConsumerOffset => 24,
        Operation::Reserved => return None,
    };
    Some(&COMMAND_TABLE[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_entry() {
        let all_codes = [
            PING_CODE,
            GET_STATS_CODE,
            GET_SNAPSHOT_FILE_CODE,
            GET_CLUSTER_METADATA_CODE,
            GET_ME_CODE,
            GET_CLIENT_CODE,
            GET_CLIENTS_CODE,
            GET_USER_CODE,
            GET_USERS_CODE,
            CREATE_USER_CODE,
            DELETE_USER_CODE,
            UPDATE_USER_CODE,
            UPDATE_PERMISSIONS_CODE,
            CHANGE_PASSWORD_CODE,
            LOGIN_USER_CODE,
            LOGOUT_USER_CODE,
            GET_PERSONAL_ACCESS_TOKENS_CODE,
            CREATE_PERSONAL_ACCESS_TOKEN_CODE,
            DELETE_PERSONAL_ACCESS_TOKEN_CODE,
            LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
            POLL_MESSAGES_CODE,
            SEND_MESSAGES_CODE,
            FLUSH_UNSAVED_BUFFER_CODE,
            GET_CONSUMER_OFFSET_CODE,
            STORE_CONSUMER_OFFSET_CODE,
            DELETE_CONSUMER_OFFSET_CODE,
            GET_STREAM_CODE,
            GET_STREAMS_CODE,
            CREATE_STREAM_CODE,
            DELETE_STREAM_CODE,
            UPDATE_STREAM_CODE,
            PURGE_STREAM_CODE,
            GET_TOPIC_CODE,
            GET_TOPICS_CODE,
            CREATE_TOPIC_CODE,
            DELETE_TOPIC_CODE,
            UPDATE_TOPIC_CODE,
            PURGE_TOPIC_CODE,
            CREATE_PARTITIONS_CODE,
            DELETE_PARTITIONS_CODE,
            DELETE_SEGMENTS_CODE,
            GET_CONSUMER_GROUP_CODE,
            GET_CONSUMER_GROUPS_CODE,
            CREATE_CONSUMER_GROUP_CODE,
            DELETE_CONSUMER_GROUP_CODE,
            JOIN_CONSUMER_GROUP_CODE,
            LEAVE_CONSUMER_GROUP_CODE,
        ];
        for code in all_codes {
            assert!(
                lookup_command(code).is_some(),
                "missing dispatch entry for code {code}"
            );
        }
    }

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
    fn lookup_by_operation_roundtrips_with_lookup_command() {
        let replicated_ops = [
            Operation::CreateStream,
            Operation::UpdateStream,
            Operation::DeleteStream,
            Operation::PurgeStream,
            Operation::CreateTopic,
            Operation::UpdateTopic,
            Operation::DeleteTopic,
            Operation::PurgeTopic,
            Operation::CreatePartitions,
            Operation::DeletePartitions,
            Operation::DeleteSegments,
            Operation::CreateConsumerGroup,
            Operation::DeleteConsumerGroup,
            Operation::CreateUser,
            Operation::UpdateUser,
            Operation::DeleteUser,
            Operation::ChangePassword,
            Operation::UpdatePermissions,
            Operation::CreatePersonalAccessToken,
            Operation::DeletePersonalAccessToken,
            Operation::SendMessages,
            Operation::StoreConsumerOffset,
        ];
        for op in replicated_ops {
            let meta = lookup_by_operation(op)
                .unwrap_or_else(|| panic!("no dispatch entry for operation {op:?}"));

            let by_code = lookup_command(meta.code)
                .unwrap_or_else(|| panic!("no dispatch entry for code {}", meta.code));

            assert_eq!(
                meta.code, by_code.code,
                "lookup_by_operation and lookup disagree for {op:?}"
            );
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
