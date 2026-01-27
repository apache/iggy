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

use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use std::net::SocketAddr;
use std::sync::Arc;

/// Common lifecycle for all test binaries.
pub trait TestBinary: Send {
    type Config: Default + Clone;

    fn with_config(config: Self::Config, context: Arc<TestContext>) -> Self;

    fn start(&mut self) -> Result<(), TestBinaryError>;

    fn stop(&mut self) -> Result<(), TestBinaryError>;

    fn is_running(&self) -> bool;

    fn assert_running(&self);

    fn pid(&self) -> Option<u32>;
}

/// Marks a binary that can be restarted while preserving state.
pub trait Restartable: TestBinary {
    fn restart(&mut self) -> Result<(), TestBinaryError>;
}

/// Marks a binary that depends on iggy-server (needs its address to connect).
pub trait IggyServerDependent: TestBinary {
    fn set_iggy_address(&mut self, addr: SocketAddr);

    /// Wait until the dependent service is ready to accept requests.
    /// May release internal resources (e.g., port reservations) once ready.
    fn wait_ready(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), TestBinaryError>> + Send;
}
