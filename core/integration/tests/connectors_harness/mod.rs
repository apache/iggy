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

//! The connector tests live in apache/iggy-connectors. They expand
//! `connectors_runtime(...)` and fixture parameters of `#[iggy_harness]`
//! through a git dependency on this crate. This test keeps those expansions
//! compiled here, so that a harness change cannot break them unseen.

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use integration::iggy_harness;
use std::collections::HashMap;

struct NoEnvFixture;

#[async_trait]
impl TestFixture for NoEnvFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self)
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

// `manual_start` keeps the harness from starting `iggy-connectors`, a binary
// that this repository does not build, so nothing reads the config path.
#[iggy_harness(manual_start, server(connectors_runtime(config_path = "unread.toml")))]
async fn given_connectors_runtime_and_fixture_when_harness_is_built_should_hold_runtime_handle(
    harness: &TestHarness,
    _fixture: NoEnvFixture,
) {
    assert!(harness.connectors_runtime().is_some());
}
