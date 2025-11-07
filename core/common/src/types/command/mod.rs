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

use crate::BytesSerializable;
use crate::Validatable;
use crate::error::IggyError;
use std::fmt::Display;

pub trait Command: BytesSerializable + Validatable<IggyError> + Send + Sync + Display {
    fn code(&self) -> u32;
}

pub const PING: &str = "ping";
pub const PING_CODE: u32 = 1;
pub const GET_STATS: &str = "stats";
pub const GET_STATS_CODE: u32 = 10;
pub const GET_SNAPSHOT_FILE: &str = "snapshot";
pub const GET_SNAPSHOT_FILE_CODE: u32 = 11;
pub const GET_CLUSTER_METADATA: &str = "cluster.metadata";
pub const GET_CLUSTER_METADATA_CODE: u32 = 12;
pub const GET_ME: &str = "me";
pub const GET_ME_CODE: u32 = 20;
pub const GET_CLIENT: &str = "client.get";
pub const GET_CLIENT_CODE: u32 = 21;
pub const GET_CLIENTS: &str = "client.list";
pub const GET_CLIENTS_CODE: u32 = 22;
pub const GET_USER: &str = "user.get";
pub const GET_USER_CODE: u32 = 31;
pub const GET_USERS: &str = "user.list";
pub const GET_USERS_CODE: u32 = 32;
pub const CREATE_USER: &str = "user.create";
pub const CREATE_USER_CODE: u32 = 33;
pub const DELETE_USER: &str = "user.delete";
pub const DELETE_USER_CODE: u32 = 34;
pub const UPDATE_USER: &str = "user.update";
pub const UPDATE_USER_CODE: u32 = 35;
pub const UPDATE_PERMISSIONS: &str = "user.permissions";
pub const UPDATE_PERMISSIONS_CODE: u32 = 36;
pub const CHANGE_PASSWORD: &str = "user.password";
pub const CHANGE_PASSWORD_CODE: u32 = 37;
pub const LOGIN_USER: &str = "user.login";
pub const LOGIN_USER_CODE: u32 = 38;
pub const LOGOUT_USER: &str = "user.logout";
pub const LOGOUT_USER_CODE: u32 = 39;
pub const GET_PERSONAL_ACCESS_TOKENS: &str = "personal_access_token.list";
pub const GET_PERSONAL_ACCESS_TOKENS_CODE: u32 = 41;
pub const CREATE_PERSONAL_ACCESS_TOKEN: &str = "personal_access_token.create";
pub const CREATE_PERSONAL_ACCESS_TOKEN_CODE: u32 = 42;
pub const DELETE_PERSONAL_ACCESS_TOKEN: &str = "personal_access_token.delete";
pub const DELETE_PERSONAL_ACCESS_TOKEN_CODE: u32 = 43;
pub const LOGIN_WITH_PERSONAL_ACCESS_TOKEN: &str = "personal_access_token.login";
pub const LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE: u32 = 44;
pub const POLL_MESSAGES: &str = "message.poll";
pub const POLL_MESSAGES_CODE: u32 = 100;
pub const SEND_MESSAGES: &str = "message.send";
pub const SEND_MESSAGES_CODE: u32 = 101;
pub const FLUSH_UNSAVED_BUFFER: &str = "message.flush_unsaved_buffer";
pub const FLUSH_UNSAVED_BUFFER_CODE: u32 = 102;
pub const GET_CONSUMER_OFFSET: &str = "consumer_offset.get";
pub const GET_CONSUMER_OFFSET_CODE: u32 = 120;
pub const STORE_CONSUMER_OFFSET: &str = "consumer_offset.store";
pub const STORE_CONSUMER_OFFSET_CODE: u32 = 121;
pub const DELETE_CONSUMER_OFFSET: &str = "consumer_offset.delete";
pub const DELETE_CONSUMER_OFFSET_CODE: u32 = 122;
pub const GET_STREAM: &str = "stream.get";
pub const GET_STREAM_CODE: u32 = 200;
pub const GET_STREAMS: &str = "stream.list";
pub const GET_STREAMS_CODE: u32 = 201;
pub const CREATE_STREAM: &str = "stream.create";
pub const CREATE_STREAM_CODE: u32 = 202;
pub const DELETE_STREAM: &str = "stream.delete";
pub const DELETE_STREAM_CODE: u32 = 203;
pub const UPDATE_STREAM: &str = "stream.update";
pub const UPDATE_STREAM_CODE: u32 = 204;
pub const PURGE_STREAM: &str = "stream.purge";
pub const PURGE_STREAM_CODE: u32 = 205;
pub const GET_TOPIC: &str = "topic.get";
pub const GET_TOPIC_CODE: u32 = 300;
pub const GET_TOPICS: &str = "topic.list";
pub const GET_TOPICS_CODE: u32 = 301;
pub const CREATE_TOPIC: &str = "topic.create";
pub const CREATE_TOPIC_CODE: u32 = 302;
pub const DELETE_TOPIC: &str = "topic.delete";
pub const DELETE_TOPIC_CODE: u32 = 303;
pub const UPDATE_TOPIC: &str = "topic.update";
pub const UPDATE_TOPIC_CODE: u32 = 304;
pub const PURGE_TOPIC: &str = "topic.purge";
pub const PURGE_TOPIC_CODE: u32 = 305;
pub const CREATE_PARTITIONS: &str = "partition.create";
pub const CREATE_PARTITIONS_CODE: u32 = 402;
pub const DELETE_PARTITIONS: &str = "partition.delete";
pub const DELETE_PARTITIONS_CODE: u32 = 403;
pub const DELETE_SEGMENTS: &str = "segment.delete";
pub const DELETE_SEGMENTS_CODE: u32 = 503;
pub const GET_CONSUMER_GROUP: &str = "consumer_group.get";
pub const GET_CONSUMER_GROUP_CODE: u32 = 600;
pub const GET_CONSUMER_GROUPS: &str = "consumer_group.list";
pub const GET_CONSUMER_GROUPS_CODE: u32 = 601;
pub const CREATE_CONSUMER_GROUP: &str = "consumer_group.create";
pub const CREATE_CONSUMER_GROUP_CODE: u32 = 602;
pub const DELETE_CONSUMER_GROUP: &str = "consumer_group.delete";
pub const DELETE_CONSUMER_GROUP_CODE: u32 = 603;
pub const JOIN_CONSUMER_GROUP: &str = "consumer_group.join";
pub const JOIN_CONSUMER_GROUP_CODE: u32 = 604;
pub const LEAVE_CONSUMER_GROUP: &str = "consumer_group.leave";
pub const LEAVE_CONSUMER_GROUP_CODE: u32 = 605;

pub fn get_name_from_code(code: u32) -> Result<&'static str, IggyError> {
    match code {
        PING_CODE => Ok(PING),
        GET_STATS_CODE => Ok(GET_STATS),
        GET_ME_CODE => Ok(GET_ME),
        GET_CLIENT_CODE => Ok(GET_CLIENT),
        GET_CLIENTS_CODE => Ok(GET_CLIENTS),
        GET_USER_CODE => Ok(GET_USER),
        GET_USERS_CODE => Ok(GET_USERS),
        CREATE_USER_CODE => Ok(CREATE_USER),
        DELETE_USER_CODE => Ok(DELETE_USER),
        UPDATE_USER_CODE => Ok(UPDATE_USER),
        UPDATE_PERMISSIONS_CODE => Ok(UPDATE_PERMISSIONS),
        CHANGE_PASSWORD_CODE => Ok(CHANGE_PASSWORD),
        LOGIN_USER_CODE => Ok(LOGIN_USER),
        LOGOUT_USER_CODE => Ok(LOGOUT_USER),
        GET_PERSONAL_ACCESS_TOKENS_CODE => Ok(GET_PERSONAL_ACCESS_TOKENS),
        CREATE_PERSONAL_ACCESS_TOKEN_CODE => Ok(CREATE_PERSONAL_ACCESS_TOKEN),
        DELETE_PERSONAL_ACCESS_TOKEN_CODE => Ok(DELETE_PERSONAL_ACCESS_TOKEN),
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE => Ok(LOGIN_WITH_PERSONAL_ACCESS_TOKEN),
        SEND_MESSAGES_CODE => Ok(SEND_MESSAGES),
        POLL_MESSAGES_CODE => Ok(POLL_MESSAGES),
        FLUSH_UNSAVED_BUFFER_CODE => Ok(FLUSH_UNSAVED_BUFFER),
        STORE_CONSUMER_OFFSET_CODE => Ok(STORE_CONSUMER_OFFSET),
        GET_CONSUMER_OFFSET_CODE => Ok(GET_CONSUMER_OFFSET),
        GET_STREAM_CODE => Ok(GET_STREAM),
        GET_STREAMS_CODE => Ok(GET_STREAMS),
        CREATE_STREAM_CODE => Ok(CREATE_STREAM),
        DELETE_STREAM_CODE => Ok(DELETE_STREAM),
        UPDATE_STREAM_CODE => Ok(UPDATE_STREAM),
        PURGE_STREAM_CODE => Ok(PURGE_STREAM),
        GET_TOPIC_CODE => Ok(GET_TOPIC),
        GET_TOPICS_CODE => Ok(GET_TOPICS),
        CREATE_TOPIC_CODE => Ok(CREATE_TOPIC),
        DELETE_TOPIC_CODE => Ok(DELETE_TOPIC),
        UPDATE_TOPIC_CODE => Ok(UPDATE_TOPIC),
        PURGE_TOPIC_CODE => Ok(PURGE_TOPIC),
        CREATE_PARTITIONS_CODE => Ok(CREATE_PARTITIONS),
        DELETE_PARTITIONS_CODE => Ok(DELETE_PARTITIONS),
        DELETE_SEGMENTS_CODE => Ok(DELETE_SEGMENTS),
        GET_CONSUMER_GROUP_CODE => Ok(GET_CONSUMER_GROUP),
        GET_CONSUMER_GROUPS_CODE => Ok(GET_CONSUMER_GROUPS),
        CREATE_CONSUMER_GROUP_CODE => Ok(CREATE_CONSUMER_GROUP),
        DELETE_CONSUMER_GROUP_CODE => Ok(DELETE_CONSUMER_GROUP),
        JOIN_CONSUMER_GROUP_CODE => Ok(JOIN_CONSUMER_GROUP),
        LEAVE_CONSUMER_GROUP_CODE => Ok(LEAVE_CONSUMER_GROUP),
        GET_SNAPSHOT_FILE_CODE => Ok(GET_SNAPSHOT_FILE),
        GET_CLUSTER_METADATA_CODE => Ok(GET_CLUSTER_METADATA),
        _ => Err(IggyError::InvalidCommand),
    }
}
