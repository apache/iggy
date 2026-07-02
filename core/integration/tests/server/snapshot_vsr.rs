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

//! Snapshot contract against server-ng (vsr): the server has no snapshot
//! primitive, so `GET_SNAPSHOT_FILE` must surface a typed `FeatureUnavailable`
//! over the SDK rather than the non-replicated catch-all's empty-ok, which would
//! attest a zero-byte snapshot artifact the server never produced.

use iggy::prelude::*;
use integration::iggy_harness;

#[iggy_harness(
    test_client_transport = [Tcp],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn given_authenticated_client_when_requesting_snapshot_should_reject_feature_unavailable(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");

    // Snapshot is ungated read access, so an authenticated client reaches the
    // response builder; the deny must be feature-unavailable, not an authz error.
    let result = client
        .snapshot(SnapshotCompression::Stored, vec![SystemSnapshotType::Test])
        .await;

    assert!(
        matches!(&result, Err(error) if error.as_code() == IggyError::FeatureUnavailable.as_code()),
        "snapshot over TCP-ng must surface Err(FeatureUnavailable), got {result:?}"
    );
}
