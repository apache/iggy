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

//! The interfaces behind Apache Iggy tiered storage.
//!
//! [`ObjectStore`] is the trait that every provider implements: a streamed
//! put, a ranged get, head, a batch delete and a paged list. [`KeyLayout`]
//! builds the keys of a partition's objects from its recorded
//! [`ObjectPrefix`], and [`SegmentMetadata`] is the user metadata of a
//! segment's `.log` object. [`TierExecutor`] is how uploads, cold reads and
//! deletes hand work to the tier service of a shard.
//!
//! [`MemoryStore`] and [`InlineTierExecutor`] stand in for a provider and for
//! the tier service in tests, and [`contract`] holds the cases that every
//! provider passes.

// Every future here runs on the shard that owns it and never crosses threads.
#![allow(clippy::future_not_send)]

pub mod contract;
mod error;
mod inline;
mod layout;
mod memory;
mod segment_metadata;
mod store;
mod tier;

pub use error::{ErrorClass, ObjectStoreError};
pub use inline::InlineTierExecutor;
pub use layout::{KeyLayout, ObjectPrefix, SegmentKey, SegmentObject};
pub use memory::MemoryStore;
pub use segment_metadata::{SEGMENT_HASH_LEN, SegmentMetadata, SegmentMetadataError};
pub use store::{
    DeleteFailure, DeleteOutcome, ListPage, ListRequest, ListedObject, MAX_KEY_LEN,
    MAX_KEYS_PER_REQUEST, MAX_METADATA_LEN, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    UserMetadata, validate_delete, validate_key, validate_metadata, validate_prefix,
    validate_range,
};
pub use tier::{
    ScopeCancelled, TierExecutor, TierJobCancelled, TierJobClass, TierJobContext, TierJobHandle,
    TierJobRequest, TierScope, TierScopeToken,
};
