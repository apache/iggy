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

use bytes::Bytes;
use iggy::prelude::*;
use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::codes::{CREATE_STREAM_CODE, GET_STATS_CODE, GET_STREAMS_CODE};
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;
use integration::iggy_harness;
use std::collections::BTreeSet;

const STREAM: &str = "replica-raw-stream";

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_cluster_when_reading_stats_from_each_replica_should_answer_from_that_node(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.expect("client");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("login");
    let replicas: Vec<u8> = client
        .get_cluster_metadata()
        .await
        .expect("cluster metadata")
        .nodes
        .iter()
        .map(|node| node.replica_id)
        .collect();
    assert_eq!(replicas.len(), 3);

    let mut processes = BTreeSet::new();
    for replica in &replicas {
        let reply = client
            .send_binary_request_to_replica(GET_STATS_CODE, Bytes::new(), *replica)
            .await
            .unwrap_or_else(|error| panic!("stats of replica {replica}: {error}"));
        let stats = StatsResponse::decode_from(&reply).expect("stats decode");
        processes.insert(stats.process_id);
    }

    assert_eq!(
        processes.len(),
        replicas.len(),
        "each replica answers from its own process"
    );
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_stream_created_by_the_client_when_listing_on_each_replica_should_include_it(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.expect("client");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("login");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let replicas: Vec<u8> = client
        .get_cluster_metadata()
        .await
        .expect("cluster metadata")
        .nodes
        .iter()
        .map(|node| node.replica_id)
        .collect();

    for replica in replicas {
        let reply = client
            .send_binary_request_to_replica(GET_STREAMS_CODE, Bytes::new(), replica)
            .await
            .unwrap_or_else(|error| panic!("streams of replica {replica}: {error}"));
        assert!(
            reply
                .windows(STREAM.len())
                .any(|window| window == STREAM.as_bytes()),
            "replica {replica} answers after applying the stream the session created"
        );
    }
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_an_unknown_replica_or_a_replicated_command_when_sent_should_refuse_it(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.expect("client");
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("login");

    assert_eq!(
        client
            .send_binary_request_to_replica(GET_STATS_CODE, Bytes::new(), 9)
            .await,
        Err(IggyError::InvalidIdentifier)
    );
    assert_eq!(
        client
            .send_binary_request_to_replica(CREATE_STREAM_CODE, Bytes::new(), 1)
            .await,
        Err(IggyError::InvalidCommand)
    );
}
