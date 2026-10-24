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

//! Mirrors of the server's HTTP JSON shapes.
//!
//! `iggy_common` owns the real types but pulls tokio with `net`, which does
//! not build for `wasm32-unknown-unknown`, so the UI keeps its own copies of
//! the fields it renders. Unknown fields are ignored on purpose.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdentityInfo {
    pub user_id: u32,
    pub access_token: Option<TokenInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenInfo {
    pub token: String,
    pub expiry: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UserDetails {
    pub id: u32,
    pub username: String,
}

/// One entry of a resource's `options` map, as written by
/// `iggy_common::resource_options_json`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OptionValue {
    pub value: String,
    pub kind: String,
    pub explicit: bool,
}

pub type ResourceOptions = BTreeMap<String, OptionValue>;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Stream {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub topics_count: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StreamDetails {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub topics_count: u32,
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Topic {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub partitions_count: u32,
    #[serde(default)]
    pub options: ResourceOptions,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TopicDetails {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub partitions_count: u32,
    pub partitions: Vec<Partition>,
    #[serde(default)]
    pub options: ResourceOptions,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Partition {
    pub id: u32,
    pub created_at: u64,
    pub segments_count: u32,
    pub current_offset: u64,
    pub size: String,
    pub messages_count: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PolledMessages {
    pub partition_id: u32,
    pub current_offset: u64,
    pub count: u32,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Message {
    pub header: MessageHeader,
    /// Standard padded base64.
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MessageHeader {
    pub checksum: u64,
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub user_headers_length: u32,
    pub payload_length: u32,
}
