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
mod stream;
mod topic;

use client::{Client, delete_connection, new_connection};
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

    struct ConsumerGroupInfo {
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    }

    struct ClientInfo {
        client_id: u32,
        has_user_id: bool,
        user_id: u32,
        address: String,
        transport: String,
        consumer_groups_count: u32,
    }

    struct ClientInfoDetails {
        client_id: u32,
        has_user_id: bool,
        user_id: u32,
        address: String,
        transport: String,
        consumer_groups_count: u32,
        consumer_groups: Vec<ConsumerGroupInfo>,
    }

    struct CacheMetricEntry {
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        hits: u64,
        misses: u64,
        hit_ratio: f32,
    }

    struct Stats {
        process_id: u32,
        cpu_usage: f32,
        total_cpu_usage: f32,
        memory_usage: u64,
        total_memory: u64,
        available_memory: u64,
        run_time: u64,
        start_time: u64,
        read_bytes: u64,
        written_bytes: u64,
        messages_size_bytes: u64,
        streams_count: u32,
        topics_count: u32,
        partitions_count: u32,
        segments_count: u32,
        messages_count: u64,
        clients_count: u32,
        consumer_groups_count: u32,
        hostname: String,
        os_name: String,
        os_version: String,
        kernel_version: String,
        iggy_server_version: String,
        has_server_semver: bool,
        iggy_server_semver: u32,
        cache_metrics: Vec<CacheMetricEntry>,
        threads_count: u32,
        free_disk_space: u64,
        total_disk_space: u64,
    }

    extern "Rust" {
        type Client;

        // Client functions
        fn new_connection(connection_string: String) -> Result<*mut Client>;
        fn login_user(self: &Client, username: String, password: String) -> Result<()>;
        fn connect(self: &Client) -> Result<()>;
        fn create_stream(self: &Client, stream_name: String) -> Result<()>;
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
        fn get_stats(self: &Client) -> Result<Stats>;
        fn get_me(self: &Client) -> Result<ClientInfoDetails>;
        fn get_client(self: &Client, client_id: u32) -> Result<ClientInfoDetails>;
        fn get_clients(self: &Client) -> Result<Vec<ClientInfo>>;
        fn ping(self: &Client) -> Result<()>;
        fn heartbeat_interval(self: &Client) -> u64;
        fn snapshot(
            self: &Client,
            snapshot_compression: String,
            snapshot_types: Vec<String>,
        ) -> Result<Vec<u8>>;

        unsafe fn delete_connection(client: *mut Client) -> Result<()>;

        // Identifier functions
        fn set_string(self: &mut Identifier, id: String) -> Result<()>;
        fn set_numeric(self: &mut Identifier, id: u32) -> Result<()>;
    }
}
