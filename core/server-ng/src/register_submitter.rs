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

//! Production [`RegisterSubmitter`] over [`IggyMetadata`]. Wires
//! [`crate::login_register`]'s `submit_register` to
//! [`IggyMetadata::submit_register_in_process`].

use crate::login_register::{LoginRegisterError, RegisterSubmitter};
use consensus::VsrConsensus;
use iggy_binary_protocol::{Message, PrepareHeader};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::IggyMetadata;
use metadata::RegisterSubmitError;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::StateMachine;

/// `RegisterSubmitter` delegating to
/// [`IggyMetadata::submit_register_in_process`]. Stateless wrapper ,
/// session lifecycle lives on metadata.
pub struct IggyRegisterSubmitter<'a, B, J, S, M>
where
    B: MessageBus,
{
    metadata: &'a IggyMetadata<VsrConsensus<B>, J, S, M>,
}

impl<'a, B, J, S, M> IggyRegisterSubmitter<'a, B, J, S, M>
where
    B: MessageBus,
{
    #[must_use]
    pub const fn new(metadata: &'a IggyMetadata<VsrConsensus<B>, J, S, M>) -> Self {
        Self { metadata }
    }
}

impl<B, J, S, M> RegisterSubmitter for IggyRegisterSubmitter<'_, B, J, S, M>
where
    B: MessageBus,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = bytes::Bytes,
            Error = iggy_common::IggyError,
        >,
{
    // !Send: metadata state is RefCell/Cell on single-threaded runtime.
    #[allow(clippy::future_not_send)]
    async fn submit_register(&self, client_id: u128) -> Result<u64, LoginRegisterError> {
        match self.metadata.submit_register_in_process(client_id).await {
            Ok(session) => Ok(session),
            Err(e) => Err(map_submit_error(e)),
        }
    }
}

/// Map [`RegisterSubmitError`] -> [`LoginRegisterError`].
///
/// Transient -> `Transient`. `TryFrom<&LoginRegisterError>` for
/// `EvictionReason` returns `NotEvictable`, so network layer can't ship
/// a wire-terminal `Eviction` and erase a recoverable session. SDK
/// read-timeout replays login+register.
///
/// # Safety
/// `RegisterSubmitError` is `#[non_exhaustive]`; wildcard arm required.
/// Default `Transient`: misclassified-transient is recoverable noise;
/// misclassified-terminal would ship `Eviction` and erase a session
/// that would have recovered. Unknown variants log + `debug_assert` so
/// missing classification surfaces.
//
// TODO(absorb-silently): trait wants transient absorbed via bounded
// retry; today they surface as Transient; arms will collapse once
// retry lands.
fn map_submit_error(e: RegisterSubmitError) -> LoginRegisterError {
    // All paths -> Transient (safe default; see Safety).
    match &e {
        RegisterSubmitError::NotPrimary
        | RegisterSubmitError::NotCaughtUp
        | RegisterSubmitError::PipelineFull
        | RegisterSubmitError::InProgress
        | RegisterSubmitError::Canceled => {}
        other => {
            tracing::warn!(
                error = ?other,
                "map_submit_error: unclassified RegisterSubmitError; default Transient , add explicit arm"
            );
            debug_assert!(
                false,
                "map_submit_error: unclassified RegisterSubmitError {other:?}"
            );
        }
    }
    LoginRegisterError::Transient(e)
}
