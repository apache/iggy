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

//! VSR (Viewstamped Replication) consensus wire types.
//!
//! All headers are exactly 256 bytes with `#[repr(C)]` layout. Size is
//! enforced at compile time. Deserialization is a pointer cast (zero-copy)
//! via `bytemuck::try_from_bytes`.
//!
//! ## Client-facing
//! - [`RequestHeader`] - client -> primary
//! - [`ReplyHeader`] - primary -> client
//! - [`GenericHeader`] - type-erased envelope for dispatch
//!
//! ## Replication (server-to-server)
//! - [`PrepareHeader`] - primary -> replicas (replicate operation)
//! - [`PrepareOkHeader`] - replica -> primary (acknowledge)
//! - [`CommitHeader`] - primary -> replicas (commit, header-only)
//!
//! ## View change (server-to-server)
//! - [`StartViewChangeHeader`] - replica suspects primary failure (header-only)
//! - [`DoViewChangeHeader`] - replica -> primary candidate (header-only)
//! - [`StartViewHeader`] - new primary -> all replicas (header-only)

mod command;
mod error;
mod header;
mod operation;
mod reply_result;

pub use command::Command2;
pub use error::ConsensusError;
pub use header::{
    CHECKSUM_UNSEALED, CommitHeader, ConsensusHeader, DVC_HEADERS_MAX, DoViewChangeHeader,
    EvictionHeader, EvictionReason, GenericHeader, HEADER_SIZE, PrepareHeader, PrepareOkHeader,
    RESERVED_COMMAND_LEN, RepairPrepareHeader, RepairRangeReplyHeader, ReplyHeader, RequestHeader,
    RequestPreparesHeader, RequestStartViewHeader, RequestStateChunkHeader,
    RequestStateTransferHeader, RoutedRequestHeader, SIZE_FIELD_OFFSET, StartViewChangeHeader,
    StartViewHeader, StateChunkHeader, StateTransferTargetHeader, frame_body, frame_checksum_bytes,
    read_size_field,
};
pub use operation::Operation;
pub use reply_result::{RESULT_COUNT_LEN, RESULT_ENTRY_LEN, result_code, result_section_len};
