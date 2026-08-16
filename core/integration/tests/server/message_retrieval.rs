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

use crate::server::scenarios::{offset_scenario, timestamp_scenario};
use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use serial_test::parallel;
use std::collections::HashMap;
use test_case::test_matrix;

// A topic's segment size must be a 512-byte multiple in
// [`iggy_common::MIN_TOPIC_SEGMENT_SIZE`]..=1 GiB, so the old 512 B and 1 KiB
// cells (which made every batch its own segment) collapse onto the 1 MiB floor.
// The spread that remains still separates "rolls on almost every large batch"
// from "rolls a handful of times over the whole run".
fn segment_size_1mib() -> u64 {
    1024 * 1024
}

fn segment_size_2mib() -> u64 {
    2 * 1024 * 1024
}

fn segment_size_10mib() -> u64 {
    10 * 1024 * 1024
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

/// The two axes that used to be `[system.segment] size` and
/// `[system.partition] messages_required_to_save` are topic creation options
/// now, so they travel with the topic the scenario creates rather than with
/// the server.
fn topic_options(segment_size: u64, messages_required_to_save: u32) -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(1),
        message_expiry: Some(IggyExpiry::NeverExpire),
        segment_size: Some(IggyByteSize::from(segment_size)),
        messages_required_to_save: Some(messages_required_to_save),
        ..TopicCreateOptions::default()
    }
}

fn build_server_config(cache_indexes: &str) -> TestServerConfig {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_SYSTEM_SEGMENT_CACHE_INDEXES".to_string(),
        cache_indexes.to_string(),
    );
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());

    TestServerConfig::builder().extra_envs(extra_envs).build()
}

/// These matrices exercise retrieval, not replication: a single node keeps
/// the wide permutation set cheap under a parallel nextest run, where an
/// oversubscribed multi-node cluster stalls past the liveness window and
/// elects a new primary mid-scenario, killing the client session.
fn build_harness(cache_indexes: &str) -> TestHarness {
    TestHarness::builder()
        .server(build_server_config(cache_indexes))
        .cluster_nodes(1)
        .build()
        .unwrap()
}

#[test_matrix(
    [segment_size_1mib(), segment_size_2mib(), segment_size_10mib()],
    [cache_none(), cache_all(), cache_open_segment()],
    [msgs_req_32(), msgs_req_64(), msgs_req_1024(), msgs_req_9984()]
)]
#[tokio::test]
#[parallel]
async fn get_by_offset_scenario(
    segment_size: u64,
    cache_indexes: &str,
    messages_required_to_save: u32,
) {
    let mut harness = build_harness(cache_indexes);

    harness.start().await.unwrap();

    offset_scenario::run(
        &harness,
        &topic_options(segment_size, messages_required_to_save),
    )
    .await;
}

#[test_matrix(
    [segment_size_1mib(), segment_size_2mib(), segment_size_10mib()],
    [cache_none(), cache_all(), cache_open_segment()],
    [msgs_req_32(), msgs_req_64(), msgs_req_1024(), msgs_req_9984()]
)]
#[tokio::test]
#[parallel]
async fn get_by_timestamp_scenario(
    segment_size: u64,
    cache_indexes: &str,
    messages_required_to_save: u32,
) {
    let mut harness = build_harness(cache_indexes);

    harness.start().await.unwrap();

    timestamp_scenario::run(
        &harness,
        &topic_options(segment_size, messages_required_to_save),
    )
    .await;
}
