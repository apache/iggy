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
mod consumer;
mod identifier;
mod messages;
mod producer;
mod type_conversion;

use client::{Client, delete_connection as delete_client, new_connection};
use consumer::Consumer;
use messages::make_message;
use producer::Producer;
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

    struct Partition {
        id: u32,
        created_at: u64,
        segments_count: u32,
        current_offset: u64,
        size_bytes: u64,
        messages_count: u64,
    }

    struct TopicDetails {
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
        partitions: Vec<Partition>,
    }

    struct Stream {
        id: u32,
        created_at: u64,
        name: String,
        size_bytes: u64,
        messages_count: u64,
        topics_count: u32,
    }

    struct IggyMessageToSend {
        id_lo: u64,
        id_hi: u64,
        payload: Vec<u8>,
        user_headers: Vec<u8>,
    }

    struct IggyMessagePolled {
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
        messages: Vec<IggyMessagePolled>,
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

    struct ConsumerGroup {
        id: u32,
        name: String,
        partitions_count: u32,
        members_count: u32,
    }

    struct ConsumerGroupInfo {
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    }

    struct ConsumerOffsetInfo {
        partition_id: u32,
        current_offset: u64,
        stored_offset: u64,
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
        run_time_micros: u64,
        start_time_epoch_micros: u64,
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
        // `iggy_server_semver` is only meaningful when this flag is true.
        has_server_semver: bool,
        // Uses `0` when the Rust `Option<u32>` is absent; check `has_server_semver`
        // before reading this field.
        iggy_server_semver: u32,
        cache_metrics: Vec<CacheMetricEntry>,
        threads_count: u32,
        free_disk_space: u64,
        total_disk_space: u64,
    }

    struct TransportEndpoints {
        tcp: u16,
        quic: u16,
        http: u16,
        websocket: u16,
    }

    struct ClusterNode {
        name: String,
        ip: String,
        endpoints: TransportEndpoints,
        role: String,
        status: String,
    }

    struct ClusterMetadata {
        name: String,
        nodes: Vec<ClusterNode>,
    }

    struct GlobalPermissions {
        manage_servers: bool,
        read_servers: bool,
        manage_users: bool,
        read_users: bool,
        manage_streams: bool,
        read_streams: bool,
        manage_topics: bool,
        read_topics: bool,
        poll_messages: bool,
        send_messages: bool,
    }

    struct TopicPermissions {
        manage_topic: bool,
        read_topic: bool,
        poll_messages: bool,
        send_messages: bool,
    }

    struct TopicPermissionEntry {
        topic_id: u32,
        permissions: TopicPermissions,
    }

    struct StreamPermissions {
        manage_stream: bool,
        read_stream: bool,
        manage_topics: bool,
        read_topics: bool,
        poll_messages: bool,
        send_messages: bool,
        topics: Vec<TopicPermissionEntry>,
    }

    struct StreamPermissionEntry {
        stream_id: u32,
        permissions: StreamPermissions,
    }

    struct Permissions {
        global: GlobalPermissions,
        streams: Vec<StreamPermissionEntry>,
    }

    extern "Rust" {
        type Client;
        type Consumer;
        type Producer;

        // Client functions
        fn new_connection(connection_string: String) -> Result<*mut Client>;
        fn login_user(self: &Client, username: String, password: String) -> Result<()>;
        fn logout_user(self: &Client) -> Result<()>;
        fn connect(self: &Client) -> Result<()>;
        fn create_stream(self: &Client, stream_name: String) -> Result<StreamDetails>;
        fn update_stream(self: &Client, stream_id: Identifier, stream_name: String) -> Result<()>;
        fn get_streams(self: &Client) -> Result<Vec<Stream>>;
        fn get_stream(self: &Client, stream_id: Identifier) -> Result<StreamDetails>;
        fn delete_stream(self: &Client, stream_id: Identifier) -> Result<()>;
        fn purge_stream(self: &Client, stream_id: Identifier) -> Result<()>;
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
        ) -> Result<TopicDetails>;
        fn get_topic(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
        ) -> Result<TopicDetails>;
        fn get_topics(self: &Client, stream_id: Identifier) -> Result<Vec<Topic>>;
        #[allow(clippy::too_many_arguments)]
        fn update_topic(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            topic_name: String,
            compression_algorithm: String,
            replication_factor: u8,
            message_expiry_kind: String,
            message_expiry_value: u64,
            max_topic_size: String,
        ) -> Result<()>;
        fn delete_topic(self: &Client, stream_id: Identifier, topic_id: Identifier) -> Result<()>;
        fn purge_topic(self: &Client, stream_id: Identifier, topic_id: Identifier) -> Result<()>;
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
        fn get_consumer_groups(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
        ) -> Result<Vec<ConsumerGroup>>;
        fn delete_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
        ) -> Result<()>;
        fn join_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
        ) -> Result<()>;
        fn leave_consumer_group(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            group_id: Identifier,
        ) -> Result<()>;
        fn store_consumer_offset(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
            offset: u64,
        ) -> Result<()>;
        fn get_consumer_offset(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
        ) -> Result<ConsumerOffsetInfo>;
        fn delete_consumer_offset(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            consumer_kind: String,
            consumer_id: Identifier,
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

        fn make_message(payload: Vec<u8>) -> IggyMessageToSend;

        #[allow(clippy::too_many_arguments)]
        fn send_messages(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partitioning_kind: String,
            partitioning_value: Vec<u8>,
            messages: Vec<IggyMessageToSend>,
        ) -> Result<()>;
        fn flush_unsaved_buffer(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            fsync: bool,
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

        // Future functions
        fn disconnect(self: &Client) -> Result<()>;
        fn shutdown(self: &Client) -> Result<()>;
        // fn subscribe_events(self: &Client) -> Result<()>;
        fn delete_segments(
            self: &Client,
            stream_id: Identifier,
            topic_id: Identifier,
            partition_id: u32,
            segments_count: u32,
        ) -> Result<()>;
        // fn get_user(self: &Client, user_id: Identifier) -> Result<()>;
        // fn get_users(self: &Client) -> Result<()>;
        // fn create_user(self: &Client, username: String, password: String, status: u8) -> Result<()>;
        // fn delete_user(self: &Client, user_id: Identifier) -> Result<()>;
        // fn update_user(self: &Client, user_id: Identifier, username: String, status: u8) -> Result<()>;
        fn update_permissions(
            self: &Client,
            user_id: Identifier,
            permissions: Permissions,
        ) -> Result<()>;
        fn change_password(
            self: &Client,
            user_id: Identifier,
            current_password: String,
            new_password: String,
        ) -> Result<()>;
        fn get_cluster_metadata(self: &Client) -> Result<ClusterMetadata>;
        // fn get_personal_access_tokens(self: &Client) -> Result<Vec<PersonalAccessTokenInfo>>;
        // fn create_personal_access_token(
        //     self: &Client,
        //     name: String,
        //     expiry: u64,
        // ) -> Result<RawPersonalAccessToken>;
        // fn delete_personal_access_token(self: &Client, name: String) -> Result<()>;
        // fn login_with_personal_access_token(self: &Client, token: String) -> Result<IdentityInfo>;

        unsafe fn delete_client(client: *mut Client) -> Result<()>;

        // Identifier functions
        fn set_string(self: &mut Identifier, id: String) -> Result<()>;
        fn set_numeric(self: &mut Identifier, id: u32) -> Result<()>;

        // Consumer methods
        // fn name(self: &Consumer) -> Result<String>;
        // fn topic(self: &Consumer) -> Result<Identifier>;
        // fn stream(self: &Consumer) -> Result<Identifier>;
        // fn partition_id(self: &Consumer) -> u32;
        // fn store_offset(self: &Consumer, offset: u64, partition_id: u32) -> Result<()>;
        // fn delete_offset(self: &Consumer, partition_id: u32) -> Result<()>;
        // fn get_last_consumed_offset(self: &Consumer, partition_id: u32) -> Result<u64>;
        // fn get_last_stored_offset(self: &Consumer, partition_id: u32) -> Result<u64>;
        // fn init(self: &mut Consumer) -> Result<()>;
        // fn shutdown(self: &mut Consumer) -> Result<()>;
        // unsafe fn delete_consumer(consumer: *mut Consumer) -> Result<()>;

        // Producer methods
        // fn stream(self: &Producer) -> Result<Identifier>;
        // fn topic(self: &Producer) -> Result<Identifier>;
        // fn init(self: &Producer) -> Result<()>;
        // fn send(self: &Producer, messages: Vec<IggyMessageToSend>) -> Result<()>;
        // fn send_one(self: &Producer, message: IggyMessageToSend) -> Result<()>;
        // fn send_with_partitioning(self: &Producer, partitioning_kind: String, partitioning_value: Vec<u8>, messages: Vec<IggyMessageToSend>) -> Result<()>;
        // fn send_to(self: &Producer, stream_id: Identifier, topic_id: Identifier, partitioning_kind: String, partitioning_value: Vec<u8>, messages: Vec<IggyMessageToSend>) -> Result<()>;
        // fn shutdown(self: &mut Producer) -> Result<()>;
        // unsafe fn delete_producer(producer: *mut Producer) -> Result<()>;
    }
}
