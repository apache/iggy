/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::harness::error::TestBinaryError;
use crate::harness::seeds::SeedError;
use async_trait::async_trait;
use iggy::prelude::IggyClient;
use std::collections::HashMap;

/// Trait for test fixtures that manage external dependencies.
///
/// Fixtures are set up before the harness, provide environment variables
/// for connector configuration, and optionally seed data after harness start.
#[async_trait]
pub trait TestFixture: Sized + Send {
    /// Set up the fixture (e.g., start containers, create database connections).
    async fn setup() -> Result<Self, TestBinaryError>;

    /// Environment variables to pass to the connector configuration.
    fn connectors_runtime_envs(&self) -> HashMap<String, String>;

    /// Optional seed function to run after harness start.
    async fn seed(&self, _client: &IggyClient) -> Result<(), SeedError> {
        Ok(())
    }

    /// Whether this fixture has a seed function that should be called.
    fn has_seed(&self) -> bool {
        false
    }
}
