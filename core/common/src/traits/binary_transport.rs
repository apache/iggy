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

use crate::{ClientState, DiagnosticEvent, IggyDuration, IggyError};
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait BinaryTransport {
    /// Gets the state of the client.
    async fn get_state(&self) -> ClientState;
    /// Sets the state of the client.
    async fn set_state(&self, state: ClientState);
    async fn publish_event(&self, event: DiagnosticEvent);
    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError>;
    fn get_heartbeat_interval(&self) -> IggyDuration;
}

/// Sealed marker. Downstream crates cannot implement
/// [`VsrSessionControl`] because they cannot name
/// `vsr_session_sealed::Sealed`. The session-mutation methods stay
/// in-crate so only the SDK's login/logout flows can call them.
#[cfg(feature = "vsr")]
mod vsr_session_sealed {
    pub trait Sealed {}
}

/// VSR-internal session control. Distinct from [`BinaryTransport`] so
/// `&dyn BinaryTransport` cannot reach `bind`/`reset` -- mid-session
/// mutation corrupts the dedup counter or silently breaks at-most-once.
#[cfg(feature = "vsr")]
#[async_trait]
pub trait VsrSessionControl: vsr_session_sealed::Sealed + BinaryTransport {
    async fn bind_vsr_session(&self, session: u64) -> Result<(), IggyError>;
    async fn reset_vsr_session(&self) -> Result<(), IggyError>;
}

#[cfg(feature = "vsr")]
pub use vsr_session_sealed::Sealed as VsrSessionSealed;
