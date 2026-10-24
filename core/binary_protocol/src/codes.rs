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
pub const DESCRIBE_OPTIONS_CODE: u32 = 13;
// Code 14 is retired.
pub const BIND_SESSION_CODE: u32 = 15;
pub const UPDATE_RESOURCE_CODE: u32 = 16;
pub const GET_ME_CODE: u32 = 20;
pub const GET_CLIENT_CODE: u32 = 21;
pub const GET_CLIENTS_CODE: u32 = 22;

// -- Users --
pub const GET_USER_CODE: u32 = 31;
pub const GET_USERS_CODE: u32 = 32;
pub const CREATE_USER_CODE: u32 = 33;
pub const DELETE_USER_CODE: u32 = 34;
pub const UPDATE_PERMISSIONS_CODE: u32 = 36;
pub const CHANGE_PASSWORD_CODE: u32 = 37;
pub const LOGIN_USER_CODE: u32 = 38;
pub const LOGOUT_USER_CODE: u32 = 39;
pub const LOGIN_REGISTER_CODE: u32 = 40;
pub const LOGIN_REGISTER_WITH_PAT_CODE: u32 = 45;

// -- Personal Access Tokens --
pub const GET_PERSONAL_ACCESS_TOKENS_CODE: u32 = 41;
pub const CREATE_PERSONAL_ACCESS_TOKEN_CODE: u32 = 42;
pub const DELETE_PERSONAL_ACCESS_TOKEN_CODE: u32 = 43;
pub const LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE: u32 = 44;

// -- Messages --
pub const POLL_MESSAGES_CODE: u32 = 100;
pub const SEND_MESSAGES_CODE: u32 = 101;
// 102 was FLUSH_UNSAVED_BUFFER. Never reuse it: older clients still send it.
// 103 was GET_POLL_ROUTING. Never reuse it: released clients still send it.
// 104 was POLL_MESSAGES_ON_PRIMARY. Never reuse it: released clients still send it.
/// The route of one partition: its incarnation, the group view and the primary
/// of that view. Sends, polls and consumer offsets all route with it.
pub const GET_PARTITION_ROUTE_CODE: u32 = 105;

// -- Consumer Offsets --
pub const GET_CONSUMER_OFFSET_CODE: u32 = 120;
pub const STORE_CONSUMER_OFFSET_CODE: u32 = 121;
pub const DELETE_CONSUMER_OFFSET_CODE: u32 = 122;
// 123 was GET_CONSUMER_OFFSET_ROUTING. Never reuse it: released clients still send it.
// 124 was DELETE_CONSUMER_OFFSET_2. Never reuse it.

// -- Streams --
pub const GET_STREAM_CODE: u32 = 200;
pub const GET_STREAMS_CODE: u32 = 201;
pub const CREATE_STREAM_CODE: u32 = 202;
pub const DELETE_STREAM_CODE: u32 = 203;
// 205 was PURGE_STREAM. Never reuse it.

// -- Topics --
pub const GET_TOPIC_CODE: u32 = 300;
pub const GET_TOPICS_CODE: u32 = 301;
pub const CREATE_TOPIC_CODE: u32 = 302;
pub const DELETE_TOPIC_CODE: u32 = 303;
// 305 was PURGE_TOPIC. Never reuse it.

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
pub const SYNC_CONSUMER_GROUP_CODE: u32 = 606;

/// Lookup the human-readable name for a command code.
///
/// # Errors
/// Returns `WireError::UnknownCommand` if the code is not recognized.
pub const fn command_name(code: u32) -> Result<&'static str, WireError> {
    match crate::dispatch::lookup_command(code) {
        Some(meta) => Ok(meta.name),
        None => Err(WireError::UnknownCommand(code)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RETIRED_CODES: &[u32] = &[14, 35, 102, 103, 104, 123, 124, 204, 205, 304, 305];

    const ALL_CODES: &[u32] = &[
        PING_CODE,
        GET_STATS_CODE,
        GET_SNAPSHOT_FILE_CODE,
        GET_CLUSTER_METADATA_CODE,
        DESCRIBE_OPTIONS_CODE,
        BIND_SESSION_CODE,
        UPDATE_RESOURCE_CODE,
        GET_ME_CODE,
        GET_CLIENT_CODE,
        GET_CLIENTS_CODE,
        GET_USER_CODE,
        GET_USERS_CODE,
        CREATE_USER_CODE,
        DELETE_USER_CODE,
        UPDATE_PERMISSIONS_CODE,
        CHANGE_PASSWORD_CODE,
        LOGIN_USER_CODE,
        LOGOUT_USER_CODE,
        LOGIN_REGISTER_CODE,
        LOGIN_REGISTER_WITH_PAT_CODE,
        GET_PERSONAL_ACCESS_TOKENS_CODE,
        CREATE_PERSONAL_ACCESS_TOKEN_CODE,
        DELETE_PERSONAL_ACCESS_TOKEN_CODE,
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
        POLL_MESSAGES_CODE,
        GET_PARTITION_ROUTE_CODE,
        SEND_MESSAGES_CODE,
        GET_CONSUMER_OFFSET_CODE,
        STORE_CONSUMER_OFFSET_CODE,
        DELETE_CONSUMER_OFFSET_CODE,
        GET_STREAM_CODE,
        GET_STREAMS_CODE,
        CREATE_STREAM_CODE,
        DELETE_STREAM_CODE,
        GET_TOPIC_CODE,
        GET_TOPICS_CODE,
        CREATE_TOPIC_CODE,
        DELETE_TOPIC_CODE,
        CREATE_PARTITIONS_CODE,
        DELETE_PARTITIONS_CODE,
        DELETE_SEGMENTS_CODE,
        GET_CONSUMER_GROUP_CODE,
        GET_CONSUMER_GROUPS_CODE,
        CREATE_CONSUMER_GROUP_CODE,
        DELETE_CONSUMER_GROUP_CODE,
        JOIN_CONSUMER_GROUP_CODE,
        LEAVE_CONSUMER_GROUP_CODE,
        SYNC_CONSUMER_GROUP_CODE,
    ];

    #[test]
    fn every_code_has_a_name() {
        assert_eq!(
            ALL_CODES.len(),
            crate::dispatch::COMMAND_TABLE.len(),
            "command-code tests must cover the complete dispatch registry"
        );
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

    #[test]
    fn retired_codes_remain_unknown() {
        for &code in RETIRED_CODES {
            assert!(
                command_name(code).is_err(),
                "retired command code {code} must not be reused"
            );
        }
    }
}
