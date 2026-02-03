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
mod client;
mod identifier;
mod stream_details;
mod topic_details;

use client::{Client, delete_connection, new_connection};
use identifier::{Identifier, delete_identifier, identifier_from_named, identifier_from_numeric};
use stream_details::{StreamDetails, delete_stream_details};
use topic_details::{TopicDetails, delete_topic_details};

use std::sync::LazyLock;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "iggy::ffi")]
mod ffi {
    struct Message {
        checksum: u64,
        id_high: u64,
        id_low: u64,
        offset: u64,
        timestamp: u64,
        origin_timestamp: u64,
        user_headers_length: u32,
        payload_length: u32,
        payload: Vec<u8>,
        user_headers: Vec<u8>,
    }

    struct PolledMessages {
        partition_id: u32,
        current_offset: u64,
        count: u32,
        messages: Vec<Message>,
    }

    extern "Rust" {
        type Client;
        type Identifier;
        type StreamDetails;
        type TopicDetails;

        // Client functions
        fn new_connection(connection_string: String) -> Result<*mut Client>;
        fn login_user(self: &Client, username: String, password: String) -> Result<()>;
        fn connect(self: &Client) -> Result<()>;
        fn create_stream(self: &Client, stream_name: String) -> Result<()>;
        #[allow(clippy::too_many_arguments)]
        fn create_topic(
            self: &Client,
            stream_id: &Identifier,
            name: String,
            partitions_count: u32,
            compression_algorithm: String,
            replication_factor: u8,
            expiry_type: String,
            expiry_value: u32,
            max_topic_size: String,
        ) -> Result<()>;
        fn send_messages(
            self: &Client,
            messages: Vec<Message>,
            stream_id: &Identifier,
            topic: &Identifier,
            partitioning: u32,
        ) -> Result<()>;
        #[allow(clippy::too_many_arguments)]
        fn poll_messages(
            self: &Client,
            stream_id: &Identifier,
            topic: &Identifier,
            partition_id: u32,
            polling_strategy_kind: String,
            polling_strategy_value: u64,
            count: u32,
            auto_commit: bool,
        ) -> Result<PolledMessages>;
        fn get_stream(self: &Client, stream_id: &Identifier) -> Result<*mut StreamDetails>;
        fn get_topic(
            self: &Client,
            stream_id: &Identifier,
            topic_id: &Identifier,
        ) -> Result<*mut TopicDetails>;

        // Identifier functions
        fn identifier_from_named(identifier_name: String) -> Result<*mut Identifier>;
        fn identifier_from_numeric(identifier_id: u32) -> Result<*mut Identifier>;
        fn get_value(self: &Identifier) -> String;

        // StreamDetails functions
        fn id(self: &StreamDetails) -> u32;
        fn name(self: &StreamDetails) -> String;
        fn messages_count(self: &StreamDetails) -> u64;
        fn topics_count(self: &StreamDetails) -> u32;

        // TopicDetails functions
        fn id(self: &TopicDetails) -> u32;
        fn name(self: &TopicDetails) -> String;
        fn messages_count(self: &TopicDetails) -> u64;
        fn partitions_count(self: &TopicDetails) -> u32;

        // Destroyers
        unsafe fn delete_connection(client: *mut Client);
        unsafe fn delete_identifier(identifier: *mut Identifier);
        unsafe fn delete_stream_details(details: *mut StreamDetails);
        unsafe fn delete_topic_details(details: *mut TopicDetails);

    }
}
