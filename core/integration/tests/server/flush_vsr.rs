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

//! Flush contract against server-ng (vsr): the server has no on-demand flush
//! primitive, so `FLUSH_UNSAVED_BUFFER` must surface a typed
//! `FeatureUnavailable` over the SDK rather than the non-replicated catch-all's
//! empty-ok, which would fake a durability guarantee.

use iggy::prelude::*;
use integration::iggy_harness;

// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
const PARTITION_ID: u32 = 0;

#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn given_valid_partition_when_flushing_should_reject_feature_unavailable(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_stream("flush-stream")
        .await
        .expect("create stream");
    let stream_id = Identifier::from_str_value("flush-stream").expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            "flush-topic",
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("create topic");
    let topic_id = Identifier::from_str_value("flush-topic").expect("topic identifier");

    // A valid, existing partition proves the deny is feature-unavailable, not a
    // target-not-found in disguise; fsync makes it the strongest durability ask.
    let result = client
        .flush_unsaved_buffer(&stream_id, &topic_id, PARTITION_ID, true)
        .await;

    assert!(
        matches!(&result, Err(error) if error.as_code() == IggyError::FeatureUnavailable.as_code()),
        "flush over TCP-ng must surface Err(FeatureUnavailable), got {result:?}"
    );
}
