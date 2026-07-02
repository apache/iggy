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

pub mod bootstrap;
mod buffer;
mod certificates;
mod consensus_message;
pub mod crypto;
mod deduplication;
pub mod diagnostics;
pub mod executor;
mod in_flight;
mod indexes_mut;
// TODO(hubcio): iobuf was relocated verbatim from `core/binary_protocol/src/consensus/iobuf.rs`
// during the sans-io split. Its implementation is intentionally untouched; the
// lints below predate the move and are tracked as tech debt for a 0.9.x cleanup pass.
#[allow(
    private_interfaces,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::missing_const_for_fn,
    clippy::non_send_fields_in_send_ty,
    clippy::use_self
)]
pub mod iobuf;
mod memory_pool;
mod messages_batch_mut;
mod messages_batch_set;
mod segment_storage;
pub mod send_messages2;
pub mod sharding;

pub use bootstrap::create_directories;
pub use buffer::PooledBuffer;
pub use certificates::generate_self_signed_certificate;
pub use consensus_message::{
    ConsensusMessage, FragmentedBacking, MESSAGE_ALIGN, Message, MessageBacking, MessageBag,
    MutableBacking, RequestBacking, RequestBackingKind, ResponseBacking, ResponseBackingKind,
};
pub use deduplication::MessageDeduplicator;
pub use executor::create_shard_executor;
pub use in_flight::IggyMessagesBatchSetInFlight;
pub use indexes_mut::IggyIndexesMut;
pub use memory_pool::{MEMORY_POOL, MemoryPool, MemoryPoolConfigOther, memory_pool};
pub use messages_batch_mut::IggyMessagesBatchMut;
pub use messages_batch_set::IggyMessagesBatchSet;
pub use segment_storage::{
    IndexReader, IndexWriter, MessagesReader, MessagesWriter, SegmentStorage,
};
