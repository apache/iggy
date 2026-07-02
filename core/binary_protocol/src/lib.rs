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

//! Wire protocol types and codec for the Iggy binary protocol.
//!
//! This crate is the single source of truth for the binary wire format
//! shared between the Iggy server and SDK. It is a pure codec - no I/O,
//! no async, no runtime dependencies.
//!
//! # Design
//!
//! Protocol types are independent from domain types.
//! Conversion between wire types and domain types happens at the boundary
//! (SDK client impls, server handlers).
//!
//! # Wire frame format
//!
//! **Request** (client -> server):
//! ```text
//! [length:4 bytes, u32 LE][code:4 bytes, u32 LE][payload:N bytes]
//! ```
//!
//! **Response** (server -> client):
//! ```text
//! [status:4 bytes, u32 LE][length:4 bytes, u32 LE][payload:N bytes]
//! ```
//!
//! All multi-byte integers are little-endian. Strings are length-prefixed
//! (u8 length for names, u32 length for longer strings).
//!
//! # VSR consensus framing
//!
//! All consensus headers are 256 bytes with `#[repr(C)]` layout.
//! Deserialization is zero-copy via `bytemuck`. Runtime message wrappers
//! (`Message`, `MessageBag`, backings) live in `server_common`; this crate
//! exposes only the wire-level header types.
//!
//! - Client-facing: [`RequestHeader`], [`ReplyHeader`]
//! - Replication: [`PrepareHeader`], [`PrepareOkHeader`], [`CommitHeader`]
//! - View change: [`StartViewChangeHeader`], [`DoViewChangeHeader`],
//!   [`StartViewHeader`]
//! - Dispatch: [`GenericHeader`] for type-erased initial parsing

pub mod codec;
pub mod codes;
pub mod consensus;
pub mod dispatch;
pub mod error;
pub mod framing;
pub mod message_layout;
pub mod message_view;
pub mod namespace;
pub mod primitives;
pub mod requests;
pub mod responses;
pub mod version;

pub use codec::{WireDecode, WireEncode};
pub use consensus::{
    Command2, CommitHeader, ConsensusError, ConsensusHeader, DoViewChangeHeader, EvictionHeader,
    EvictionReason, GenericHeader, HEADER_SIZE, Operation, PrepareHeader, PrepareOkHeader,
    RESERVED_COMMAND_LEN, ReplyHeader, RequestHeader, SIZE_FIELD_OFFSET, StartViewChangeHeader,
    StartViewHeader, read_size_field, result_code, result_section_len,
};
pub use dispatch::{COMMAND_TABLE, CommandMeta, lookup_by_operation, lookup_command};
pub use error::WireError;
pub use framing::{RequestFrame, RequestFrame2, ResponseFrame, ResponseFrame2, STATUS_OK};
pub use message_view::{
    WireMessageIterator, WireMessageIteratorMut, WireMessageView, WireMessageViewMut,
};
pub use primitives::ack_level::AckLevel;
pub use primitives::consumer::{KIND_CONSUMER_GROUP, WireConsumer};
pub use primitives::identifier::{MAX_WIRE_NAME_LENGTH, WireIdentifier, WireName};
pub use primitives::partition_assignment::CreatedPartitionAssignment;
pub use primitives::partitioning::{MAX_MESSAGES_KEY_LENGTH, WirePartitioning};
pub use primitives::permissions::{
    WireGlobalPermissions, WirePermissions, WireStreamPermissions, WireTopicPermissions,
};
pub use primitives::polling_strategy::WirePollingStrategy;
pub use primitives::user_headers::{
    WireHeaderKind, WireUserHeaderEntry, WireUserHeaderIterator, WireUserHeaders,
    encode_user_headers, user_headers_encoded_size, validate_user_headers,
};
pub use version::{
    ClientVersionInfo, IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN, ProtocolVersion,
    is_protocol_compatible,
};

/// Maximum number of partitions allowed in a single create/delete request.
pub const MAX_PARTITIONS_PER_REQUEST: u32 = 1000;
