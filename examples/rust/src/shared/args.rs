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

use clap::Parser;
use iggy::prelude::IggyDuration;
use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "10")]
    pub message_batches_limit: u64,

    #[arg(long, default_value = DEFAULT_ROOT_USERNAME)]
    pub username: String,

    #[arg(long, default_value = DEFAULT_ROOT_PASSWORD)]
    pub password: String,

    #[arg(long, default_value = "1ms")]
    pub interval: String,

    #[arg(long, default_value = "example-stream")]
    pub stream_id: String,

    #[arg(long, default_value = "example-topic")]
    pub topic_id: String,

    #[arg(long, default_value = "0")]
    pub partition_id: u32,

    #[arg(long, default_value = "1")]
    pub partitions_count: u32,

    #[arg(long, default_value = "1")]
    pub compression_algorithm: u8,

    #[arg(long, default_value = "1")]
    pub consumer_kind: u8,

    #[arg(long, default_value = "0")]
    pub consumer_id: u32,

    #[arg(long, default_value = "false")]
    pub balanced_producer: bool,

    #[arg(long, default_value = "1")]
    pub messages_per_batch: u32,

    #[arg(long, default_value = "0")]
    pub offset: u64,

    #[arg(long, default_value = "false")]
    pub auto_commit: bool,

    #[arg(long, default_value = "tcp")]
    pub transport: String,

    #[arg(long, default_value = "")]
    pub encryption_key: String,

    #[arg(long, default_value = "http://localhost:3000")]
    pub http_api_url: String,

    #[arg(long, default_value = "3")]
    pub http_retries: u32,

    #[arg(long, default_value = "true")]
    pub tcp_reconnection_enabled: bool,

    #[arg(long)]
    pub tcp_reconnection_max_retries: Option<u32>,

    #[arg(long, default_value = "1s")]
    pub tcp_reconnection_interval: String,

    #[arg(long, default_value = "5s")]
    pub tcp_reconnection_reestablish_after: String,

    #[arg(long, default_value = "5s")]
    pub tcp_heartbeat_interval: String,

    #[arg(long, default_value = "127.0.0.1:8090")]
    pub tcp_server_address: String,

    #[arg(long, default_value = "false")]
    pub tcp_tls_enabled: bool,

    #[arg(long, default_value = "")]
    pub tcp_tls_domain: String,

    #[arg(long, default_value = "")]
    pub tcp_tls_ca_file: String,

    #[arg(long, default_value = "false")]
    pub tcp_nodelay: bool,

    #[arg(long, default_value = "127.0.0.1:0")]
    pub quic_client_address: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    pub quic_server_address: String,

    #[arg(long, default_value = "localhost")]
    pub quic_server_name: String,

    #[arg(long, default_value = "true")]
    pub quic_reconnection_enabled: bool,

    #[arg(long)]
    pub quic_reconnection_max_retries: Option<u32>,

    #[arg(long, default_value = "1s")]
    pub quic_reconnection_interval: String,

    #[arg(long, default_value = "5s")]
    pub quic_reconnection_reestablish_after: String,

    #[arg(long, default_value = "10000")]
    pub quic_max_concurrent_bidi_streams: u64,

    #[arg(long, default_value = "100000")]
    pub quic_datagram_send_buffer_size: u64,

    #[arg(long, default_value = "1200")]
    pub quic_initial_mtu: u16,

    #[arg(long, default_value = "100000")]
    pub quic_send_window: u64,

    #[arg(long, default_value = "100000")]
    pub quic_receive_window: u64,

    #[arg(long, default_value = "1048576")]
    pub quic_response_buffer_size: u64,

    #[arg(long, default_value = "5000")]
    pub quic_keep_alive_interval: u64,

    #[arg(long, default_value = "10000")]
    pub quic_max_idle_timeout: u64,

    #[arg(long, default_value = "false")]
    pub quic_validate_certificate: bool,

    #[arg(long, default_value = "5s")]
    pub quic_heartbeat_interval: String,

    #[arg(long, default_value = "127.0.0.1:8092")]
    pub websocket_server_address: String,

    #[arg(long, default_value = "true")]
    pub websocket_reconnection_enabled: bool,

    #[arg(long)]
    pub websocket_reconnection_max_retries: Option<u32>,

    #[arg(long, default_value = "1s")]
    pub websocket_reconnection_interval: String,

    #[arg(long, default_value = "5s")]
    pub websocket_reconnection_reestablish_after: String,

    #[arg(long, default_value = "5s")]
    pub websocket_heartbeat_interval: String,

    #[arg(long, default_value = "false")]
    pub websocket_tls_enabled: bool,

    #[arg(long, default_value = "localhost")]
    pub websocket_tls_domain: String,

    #[arg(long, default_value = "false")]
    pub websocket_tls_ca_file: Option<String>,

    #[arg(long, default_value = "false")]
    pub websocket_tls_validate_certificate: bool,
}

impl Args {
    #[inline]
    pub fn new(stream_id: String, topic_id: String) -> Self {
        Self {
            stream_id,
            topic_id,
            ..Default::default()
        }
    }
}

impl Default for Args {
    fn default() -> Self {
        Self {
            message_batches_limit: 10,
            username: DEFAULT_ROOT_PASSWORD.to_string(),
            password: DEFAULT_ROOT_USERNAME.to_string(),
            interval: "1ms".to_string(),
            stream_id: "example-stream".to_string(),
            topic_id: "example-topic".to_string(),
            partition_id: 1,
            partitions_count: 1,
            compression_algorithm: 1,
            consumer_kind: 1,
            consumer_id: 1,
            balanced_producer: false,
            messages_per_batch: 1,
            offset: 0,
            auto_commit: false,
            transport: "tcp".to_string(),
            encryption_key: String::new(),
            http_api_url: "http://localhost:3000".to_string(),
            http_retries: 3,
            tcp_reconnection_enabled: true,
            tcp_reconnection_max_retries: Some(3),
            tcp_reconnection_interval: "1s".to_string(),
            tcp_reconnection_reestablish_after: "5s".to_string(),
            tcp_heartbeat_interval: "5s".to_string(),
            tcp_server_address: "127.0.0.1:8090".to_string(),
            tcp_tls_enabled: false,
            tcp_tls_domain: "localhost".to_string(),
            tcp_tls_ca_file: "".to_string(),
            tcp_nodelay: true,
            quic_client_address: "127.0.0.1:0".to_string(),
            quic_server_address: "127.0.0.1:8080".to_string(),
            quic_server_name: "localhost".to_string(),
            quic_reconnection_enabled: true,
            quic_reconnection_max_retries: None,
            quic_reconnection_interval: "1s".to_string(),
            quic_reconnection_reestablish_after: "5s".to_string(),
            quic_max_concurrent_bidi_streams: 10000,
            quic_datagram_send_buffer_size: 100000,
            quic_initial_mtu: 1200,
            quic_send_window: 100000,
            quic_receive_window: 100000,
            quic_response_buffer_size: 1048576,
            quic_keep_alive_interval: 5000,
            quic_max_idle_timeout: 10000,
            quic_validate_certificate: false,
            quic_heartbeat_interval: "5s".to_string(),
            websocket_server_address: "127.0.0.1:8092".to_string(),
            websocket_reconnection_enabled: true,
            websocket_reconnection_max_retries: None,
            websocket_reconnection_interval: "1s".to_string(),
            websocket_reconnection_reestablish_after: "5s".to_string(),
            websocket_heartbeat_interval: "5s".to_string(),
            websocket_tls_enabled: false,
            websocket_tls_domain: "localhost".to_string(),
            websocket_tls_ca_file: None,
            websocket_tls_validate_certificate: false,
        }
    }
}

impl Args {
    /// Parse command line arguments and apply example-specific defaults if not overridden
    pub fn parse_with_defaults(example_name: &str) -> Self {
        let mut args = Self::parse();

        // If stream_id was not explicitly provided (still has default value)
        if args.stream_id == "example-stream" {
            args.stream_id = Self::get_default_stream_id(example_name);
        }

        // If topic_id was not explicitly provided (still has default value)
        if args.topic_id == "example-topic" {
            args.topic_id = Self::get_default_topic_id(example_name);
        }

        args
    }

    /// Get default stream ID based on example name
    fn get_default_stream_id(example_name: &str) -> String {
        match example_name {
            "basic-producer" | "basic-consumer" => "basic-stream",
            "getting-started-producer" | "getting-started-consumer" => "getting-started",
            "message-envelope-producer" | "message-envelope-consumer" => "envelope-stream",
            "message-headers-producer" | "message-headers-consumer" => "headers-stream",
            "multi-tenant-producer" | "multi-tenant-consumer" => "tenant-stream",
            "new-sdk-producer" | "new-sdk-consumer" => "new-sdk-stream",
            "sink-data-producer" => "sink-stream",
            "stream-basic" => "stream-basic",
            "stream-producer" | "stream-consumer" => "stream-example",
            "stream-producer-config" | "stream-consumer-config" => "stream-config",
            _ => "example-stream",
        }
        .to_string()
    }

    /// Get default topic ID based on example name
    fn get_default_topic_id(example_name: &str) -> String {
        match example_name {
            "basic-producer" | "basic-consumer" => "basic-topic",
            "getting-started-producer" | "getting-started-consumer" => "getting-started-topic",
            "message-envelope-producer" | "message-envelope-consumer" => "orders",
            "message-headers-producer" | "message-headers-consumer" => "orders",
            "multi-tenant-producer" | "multi-tenant-consumer" => "events",
            "new-sdk-producer" | "new-sdk-consumer" => "new-sdk-topic",
            "sink-data-producer" => "users",
            "stream-basic" => "stream-topic",
            "stream-producer" | "stream-consumer" => "stream-topic",
            "stream-producer-config" | "stream-consumer-config" => "config-topic",
            _ => "example-topic",
        }
        .to_string()
    }

    pub fn to_sdk_args(&self) -> iggy::prelude::Args {
        iggy::prelude::Args {
            transport: self.transport.clone(),
            encryption_key: self.encryption_key.clone(),
            http_api_url: self.http_api_url.clone(),
            http_retries: self.http_retries,
            username: self.username.clone(),
            password: self.password.clone(),
            tcp_server_address: self.tcp_server_address.clone(),
            tcp_reconnection_enabled: self.tcp_reconnection_enabled,
            tcp_reconnection_max_retries: self.tcp_reconnection_max_retries,
            tcp_reconnection_interval: self.tcp_reconnection_interval.clone(),
            tcp_reconnection_reestablish_after: self.tcp_reconnection_reestablish_after.clone(),
            tcp_heartbeat_interval: self.tcp_heartbeat_interval.clone(),
            tcp_tls_enabled: self.tcp_tls_enabled,
            tcp_tls_domain: self.tcp_tls_domain.clone(),
            tcp_tls_ca_file: if self.tcp_tls_ca_file.is_empty() {
                None
            } else {
                Some(self.tcp_tls_ca_file.clone())
            },
            tcp_nodelay: self.tcp_nodelay,
            quic_client_address: self.quic_client_address.clone(),
            quic_server_address: self.quic_server_address.clone(),
            quic_server_name: self.quic_server_name.clone(),
            quic_reconnection_enabled: self.quic_reconnection_enabled,
            quic_reconnection_max_retries: self.quic_reconnection_max_retries,
            quic_reconnection_reestablish_after: self.quic_reconnection_reestablish_after.clone(),
            quic_reconnection_interval: self.quic_reconnection_interval.clone(),
            quic_max_concurrent_bidi_streams: self.quic_max_concurrent_bidi_streams,
            quic_datagram_send_buffer_size: self.quic_datagram_send_buffer_size,
            quic_initial_mtu: self.quic_initial_mtu,
            quic_send_window: self.quic_send_window,
            quic_receive_window: self.quic_receive_window,
            quic_response_buffer_size: self.quic_response_buffer_size,
            quic_keep_alive_interval: self.quic_keep_alive_interval,
            quic_max_idle_timeout: self.quic_max_idle_timeout,
            quic_validate_certificate: self.quic_validate_certificate,
            quic_heartbeat_interval: self.quic_heartbeat_interval.clone(),
            websocket_server_address: self.websocket_server_address.clone(),
            websocket_reconnection_enabled: self.websocket_reconnection_enabled,
            websocket_reconnection_max_retries: self.websocket_reconnection_max_retries,
            websocket_reconnection_interval: self.websocket_reconnection_interval.clone(),
            websocket_reconnection_reestablish_after: self
                .websocket_reconnection_reestablish_after
                .clone(),
            websocket_heartbeat_interval: self.websocket_heartbeat_interval.clone(),
            websocket_tls_enabled: self.websocket_tls_enabled,
            websocket_tls_domain: self.websocket_tls_domain.clone(),
            websocket_tls_ca_file: self.websocket_tls_ca_file.clone(),
            websocket_tls_validate_certificate: self.websocket_tls_validate_certificate,
        }
    }

    pub fn get_interval(&self) -> Option<IggyDuration> {
        match self.interval.to_lowercase().as_str() {
            "" | "0" | "none" => None,
            x => Some(IggyDuration::from_str(x).expect("Invalid interval format")),
        }
    }
}
