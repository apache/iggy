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

use crate::WireError;

// TODO: consider converting these constants into a `#[repr(u32)]` enum with `TryFrom<u32>`
// for type safety at conversion boundaries (see PR #2946 discussion).

// -- System --
pub const PING_CODE: u32 = 1;
pub const GET_STATS_CODE: u32 = 10;
pub const GET_SNAPSHOT_FILE_CODE: u32 = 11;
pub const GET_CLUSTER_METADATA_CODE: u32 = 12;
pub const GET_ME_CODE: u32 = 20;
pub const GET_CLIENT_CODE: u32 = 21;
pub const GET_CLIENTS_CODE: u32 = 22;

// -- Users --
pub const GET_USER_CODE: u32 = 31;
pub const GET_USERS_CODE: u32 = 32;
pub const CREATE_USER_CODE: u32 = 33;
pub const DELETE_USER_CODE: u32 = 34;
pub const UPDATE_USER_CODE: u32 = 35;
pub const UPDATE_PERMISSIONS_CODE: u32 = 36;
pub const CHANGE_PASSWORD_CODE: u32 = 37;
pub const LOGIN_USER_CODE: u32 = 38;
pub const LOGOUT_USER_CODE: u32 = 39;

// -- Personal Access Tokens --
pub const GET_PERSONAL_ACCESS_TOKENS_CODE: u32 = 41;
pub const CREATE_PERSONAL_ACCESS_TOKEN_CODE: u32 = 42;
pub const DELETE_PERSONAL_ACCESS_TOKEN_CODE: u32 = 43;
pub const LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE: u32 = 44;

// -- Messages --
pub const POLL_MESSAGES_CODE: u32 = 100;
pub const SEND_MESSAGES_CODE: u32 = 101;
pub const FLUSH_UNSAVED_BUFFER_CODE: u32 = 102;

// -- Consumer Offsets --
pub const GET_CONSUMER_OFFSET_CODE: u32 = 120;
pub const STORE_CONSUMER_OFFSET_CODE: u32 = 121;
pub const DELETE_CONSUMER_OFFSET_CODE: u32 = 122;

// -- Streams --
pub const GET_STREAM_CODE: u32 = 200;
pub const GET_STREAMS_CODE: u32 = 201;
pub const CREATE_STREAM_CODE: u32 = 202;
pub const DELETE_STREAM_CODE: u32 = 203;
pub const UPDATE_STREAM_CODE: u32 = 204;
pub const PURGE_STREAM_CODE: u32 = 205;

// -- Topics --
pub const GET_TOPIC_CODE: u32 = 300;
pub const GET_TOPICS_CODE: u32 = 301;
pub const CREATE_TOPIC_CODE: u32 = 302;
pub const DELETE_TOPIC_CODE: u32 = 303;
pub const UPDATE_TOPIC_CODE: u32 = 304;
pub const PURGE_TOPIC_CODE: u32 = 305;

// -- Partitions --
pub const CREATE_PARTITIONS_CODE: u32 = 402;
pub const DELETE_PARTITIONS_CODE: u32 = 403;

// -- Segments --
pub const DELETE_SEGMENTS_CODE: u32 = 503;

// -- Consumer Groups --
pub const GET_CONSUMER_GROUP_CODE: u32 = 600;
pub const GET_CONSUMER_GROUPS_CODE: u32 = 601;
pub const CREATE_CONSUMER_GROUP_CODE: u32 = 602;
pub const DELETE_CONSUMER_GROUP_CODE: u32 = 603;
pub const JOIN_CONSUMER_GROUP_CODE: u32 = 604;
pub const LEAVE_CONSUMER_GROUP_CODE: u32 = 605;

/// # Errors
/// Returns `WireError::UnknownCommand` if the code is not recognized.
pub const fn command_name(code: u32) -> Result<&'static str, WireError> {
    match code {
        PING_CODE => Ok("ping"),
        GET_STATS_CODE => Ok("stats"),
        GET_SNAPSHOT_FILE_CODE => Ok("snapshot"),
        GET_CLUSTER_METADATA_CODE => Ok("cluster.metadata"),
        GET_ME_CODE => Ok("me"),
        GET_CLIENT_CODE => Ok("client.get"),
        GET_CLIENTS_CODE => Ok("client.list"),
        GET_USER_CODE => Ok("user.get"),
        GET_USERS_CODE => Ok("user.list"),
        CREATE_USER_CODE => Ok("user.create"),
        DELETE_USER_CODE => Ok("user.delete"),
        UPDATE_USER_CODE => Ok("user.update"),
        UPDATE_PERMISSIONS_CODE => Ok("user.permissions"),
        CHANGE_PASSWORD_CODE => Ok("user.password"),
        LOGIN_USER_CODE => Ok("user.login"),
        LOGOUT_USER_CODE => Ok("user.logout"),
        GET_PERSONAL_ACCESS_TOKENS_CODE => Ok("personal_access_token.list"),
        CREATE_PERSONAL_ACCESS_TOKEN_CODE => Ok("personal_access_token.create"),
        DELETE_PERSONAL_ACCESS_TOKEN_CODE => Ok("personal_access_token.delete"),
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE => Ok("personal_access_token.login"),
        POLL_MESSAGES_CODE => Ok("message.poll"),
        SEND_MESSAGES_CODE => Ok("message.send"),
        FLUSH_UNSAVED_BUFFER_CODE => Ok("message.flush_unsaved_buffer"),
        GET_CONSUMER_OFFSET_CODE => Ok("consumer_offset.get"),
        STORE_CONSUMER_OFFSET_CODE => Ok("consumer_offset.store"),
        DELETE_CONSUMER_OFFSET_CODE => Ok("consumer_offset.delete"),
        GET_STREAM_CODE => Ok("stream.get"),
        GET_STREAMS_CODE => Ok("stream.list"),
        CREATE_STREAM_CODE => Ok("stream.create"),
        DELETE_STREAM_CODE => Ok("stream.delete"),
        UPDATE_STREAM_CODE => Ok("stream.update"),
        PURGE_STREAM_CODE => Ok("stream.purge"),
        GET_TOPIC_CODE => Ok("topic.get"),
        GET_TOPICS_CODE => Ok("topic.list"),
        CREATE_TOPIC_CODE => Ok("topic.create"),
        DELETE_TOPIC_CODE => Ok("topic.delete"),
        UPDATE_TOPIC_CODE => Ok("topic.update"),
        PURGE_TOPIC_CODE => Ok("topic.purge"),
        CREATE_PARTITIONS_CODE => Ok("partition.create"),
        DELETE_PARTITIONS_CODE => Ok("partition.delete"),
        DELETE_SEGMENTS_CODE => Ok("segment.delete"),
        GET_CONSUMER_GROUP_CODE => Ok("consumer_group.get"),
        GET_CONSUMER_GROUPS_CODE => Ok("consumer_group.list"),
        CREATE_CONSUMER_GROUP_CODE => Ok("consumer_group.create"),
        DELETE_CONSUMER_GROUP_CODE => Ok("consumer_group.delete"),
        JOIN_CONSUMER_GROUP_CODE => Ok("consumer_group.join"),
        LEAVE_CONSUMER_GROUP_CODE => Ok("consumer_group.leave"),
        _ => Err(WireError::UnknownCommand(code)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_CODES: &[u32] = &[
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

    #[test]
    fn every_code_has_a_name() {
        for &code in ALL_CODES {
            assert!(
                command_name(code).is_ok(),
                "missing name for command code {code}"
            );
        }
    }

    #[test]
    fn no_duplicate_codes() {
        let mut seen = std::collections::HashSet::new();
        for &code in ALL_CODES {
            assert!(seen.insert(code), "duplicate command code: {code}");
        }
    }

    #[test]
    fn unknown_code_returns_error() {
        assert!(command_name(9999).is_err());
    }
}
