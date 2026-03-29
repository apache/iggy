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
mod consumer_group;
mod identifier;
mod messages;
mod stream;
mod topic;

use client::{Client, delete_connection, new_connection};
use messages::new_message;
use std::sync::LazyLock;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "iggy::ffi")]
mod ffi {
    struct Identifier {
        kind: String,
        length: u8,
        value: Vec<u8>,
    }

    struct Topic {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        message_expiry: u64,
        compression_algorithm: String,
        max_topic_size: u64,
        replication_factor: u8,
        messages_count: u64,
        partitions_count: u32,
    }

    struct Stream {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        messages_count: u64,
        topics_count: u32,
    }

    struct Message {
        checksum: u64,
        id_lo: u64,
        id_hi: u64,
        offset: u64,
        timestamp: u64,
        origin_timestamp: u64,
        user_headers_length: u32,
        payload_length: u32,
        reserved: u64,
        payload: Vec<u8>,
        user_headers: Vec<u8>,
    }

    struct PolledMessages {
        partition_id: u32,
        current_offset: u64,
        count: u32,
        messages: Vec<Message>,
    }

    struct StreamDetails {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        messages_count: u64,
        topics_count: u32,
        topics: Vec<Topic>,
    }

    struct ConsumerGroupMember {
        id: u32,
        partitions_count: u32,
        partitions: Vec<u32>,
    }

    struct ConsumerGroupDetails {
        id: u32,
        name: String,
        partitions_count: u32,
        members_count: u32,
        members: Vec<ConsumerGroupMember>,
    }

    extern "Rust" {
        type Client;

        // Client functions
        fn new_connection(connection_string: String) -> Result<*mut Client>;
        fn login_user(self: &Client, username: String, password: String) -> Result<()>;
        fn connect(self: &Client) -> Result<()>;
        fn create_stream(self: &Client, stream_name: String) -> Result<()>;
        fn get_streams(self: &Client) -> Result<Vec<Stream>>;
        fn get_stream(self: &Client, stream_id: Identifier) -> Result<StreamDetails>;
        fn delete_stream(self: &Client, stream_id: Identifier) -> Result<()>;
        // fn purge_stream(&self, stream_id: Identifier) -> Result<()>;
        #[allow(clippy::too_many_arguments)]
        fn create_topic(
            self: &Client,
            stream_id: Identifier,
            topic_name: String,
            partitions_count: u32,
            compression_algorithm: String,
            replication_factor: u8,
            message_expiry_kind: String,
            message_expiry_value: u64,
            max_topic_size: String,
        ) -> Result<()>;
        // fn purge_topic(&self, stream_id: Identifier, topic_id: Identifier) -> Result<()>;
        fn create_partitions(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partitions_count: u32,
        ) -> Result<()>;
        fn delete_partitions(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partitions_count: u32,
        ) -> Result<()>;
        fn create_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            name: String,
        ) -> Result<ConsumerGroupDetails>;
        fn get_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
        ) -> Result<ConsumerGroupDetails>;
        fn delete_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
        ) -> Result<()>;

        #[allow(clippy::too_many_arguments)]
        fn poll_messages(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
            polling_strategy_kind: String,
            polling_strategy_value: u64,
            count: u32,
            auto_commit: bool,
        ) -> Result<PolledMessages>;

        fn new_message(payload: Vec<u8>) -> Result<Message>;

        #[allow(clippy::too_many_arguments)]
        fn send_messages(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partitioning_kind: String,
            partitioning_value: Vec<u8>,
            messages: Vec<Message>,
        ) -> Result<()>;

        unsafe fn delete_connection(client: *mut Client) -> Result<()>;

        // Identifier functions
        fn from_string(self: &mut Identifier, id: String) -> Result<()>;
        fn from_numeric(self: &mut Identifier, id: u32) -> Result<()>;
    }
}
