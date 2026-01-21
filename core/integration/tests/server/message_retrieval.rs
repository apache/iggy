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

use crate::server::scenarios::{offset_scenario, timestamp_scenario};
use integration::{
    tcp_client::TcpClientFactory,
    test_server::{IpAddrKind, TestServer},
};
use serial_test::parallel;
use std::collections::HashMap;
use test_case::test_matrix;

fn segment_size_512b() -> &'static str {
    "512B"
}

fn segment_size_1kb() -> &'static str {
    "1KiB"
}

fn segment_size_10mb() -> &'static str {
    "10MiB"
}

fn cache_none() -> &'static str {
    "none"
}

fn cache_all() -> &'static str {
    "all"
}

fn cache_open_segment() -> &'static str {
    "open_segment"
}

fn msgs_req_32() -> u32 {
    32
}

fn msgs_req_64() -> u32 {
    64
}

fn msgs_req_1024() -> u32 {
    1024
}

fn msgs_req_9984() -> u32 {
    9984
}

#[test_matrix(
    [segment_size_512b(), segment_size_1kb(), segment_size_10mb()],
    [cache_none(), cache_all(), cache_open_segment()],
    [msgs_req_32(), msgs_req_64(), msgs_req_1024(), msgs_req_9984()]
)]
#[tokio::test]
#[parallel]
async fn get_by_offset_scenario(
    segment_size: &str,
    cache_indexes: &str,
    messages_required_to_save: u32,
) {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_SYSTEM_SEGMENT_SIZE".to_string(),
        segment_size.to_string(),
    );
    extra_envs.insert(
        "IGGY_SYSTEM_SEGMENT_CACHE_INDEXES".to_string(),
        cache_indexes.to_string(),
    );
    extra_envs.insert(
        "IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE".to_string(),
        messages_required_to_save.to_string(),
    );
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    offset_scenario::run(&client_factory).await;
}

#[test_matrix(
    [segment_size_512b(), segment_size_1kb(), segment_size_10mb()],
    [cache_none(), cache_all(), cache_open_segment()],
    [msgs_req_32(), msgs_req_64(), msgs_req_1024(), msgs_req_9984()]
)]
#[tokio::test]
#[parallel]
async fn get_by_timestamp_scenario(
    segment_size: &str,
    cache_indexes: &str,
    messages_required_to_save: u32,
) {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_SYSTEM_SEGMENT_SIZE".to_string(),
        segment_size.to_string(),
    );
    extra_envs.insert(
        "IGGY_SYSTEM_SEGMENT_CACHE_INDEXES".to_string(),
        cache_indexes.to_string(),
    );
    extra_envs.insert(
        "IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE".to_string(),
        messages_required_to_save.to_string(),
    );
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    timestamp_scenario::run(&client_factory).await;
}
