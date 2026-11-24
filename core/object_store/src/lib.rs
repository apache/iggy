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
//! segment's `.log` object. [`SealedFreshObject`] writes a fresh object
//! that describes itself, and [`FreshIndex`] reads its index back.
//! [`StoreIdentity`] is the object that claims a root for one cluster, and
//! [`TopicObject`] is the object that describes a geo topic to the clusters
//! that read the store. [`TierExecutor`] is how
//! uploads, cold reads and deletes hand work to the tier service of a shard.
//!
//! [`FsStore`] keeps objects as files in a directory, [`s3::S3Store`] keeps
//! them in an S3 bucket, [`gcs::GcsStore`] keeps them in a Google Cloud
//! Storage bucket, and [`MockS3Store`] keeps them in memory and answers
//! after the latency of a remote store, for benchmarks. [`Backend`] is the
//! one of them that a configuration names. [`ReadOnlyStore`] reads another
//! cluster's store and refuses every write. [`MemoryStore`] and
//! [`InlineTierExecutor`] stand in for a provider and for the tier service in
//! tests, [`RecordingStore`] shows which calls reach a provider, and
//! [`contract`] holds the cases that providers pass.

// Every future here runs on the shard that owns it and never crosses threads.
#![allow(clippy::future_not_send)]

mod backend;
pub mod contract;
pub mod credentials;
mod error;
mod fresh;
mod fresh_index;
mod fs;
pub mod gcs;
mod http;
mod identity;
mod inline;
mod layout;
mod manifest;
mod memory;
mod mock_s3;
mod positions;
mod read_only;
mod recording;
mod recovery;
mod resolver;
pub mod s3;
mod segment_metadata;
mod store;
mod tier;
mod topic;

pub use backend::Backend;
pub use error::{ErrorClass, ObjectStoreError};
pub use fresh::{FreshObject, FreshObjectId, is_fresh_object};
pub use fresh_index::{
    FRESH_BATCH_ROW_SIZE, FRESH_DIRECTORY_ROW_SIZE, FRESH_HEADER_SIZE, FRESH_INDEX_BYTES_MAX,
    FRESH_INDEX_CHECKSUM_SIZE, FRESH_OBJECT_BATCHES_MAX, FRESH_OBJECT_DATA_BYTES_MAX,
    FRESH_OBJECT_PROOFS_MAX, FRESH_PROOF_ROW_SIZE, FreshBatch, FreshBatchRow, FreshDirectoryRow,
    FreshIndex, FreshObjectError, FreshObjectHeader, FreshPartition, FreshProducer, FreshProofRow,
    SealedFreshObject,
};
pub use fs::FsStore;
pub use identity::{IdentityRead, InvalidStoreIdentity, StoreIdentity};
pub use inline::InlineTierExecutor;
pub use layout::{KeyLayout, LongTermKey, ObjectPrefix, SegmentKey, SegmentObject, TopicKey};
pub use manifest::{ManifestError, ManifestSegment, PartitionManifest};
pub use memory::MemoryStore;
pub use mock_s3::{MockS3Latency, MockS3Store};
pub use positions::{
    GeoLog, GeoPosition, MAX_GROUP_NAME_LEN, MAX_POSITIONS_OBJECT_LEN, POSITIONS_PER_KIND_MAX,
    POSITIONS_PER_PARTITION_MAX, PositionHolder, PositionValue, PositionsKey, PositionsObject,
    PositionsObjectError,
};
pub use read_only::ReadOnlyStore;
pub use recording::{RecordingStore, StoreCall, StoreCalls};
pub use recovery::{
    RECOVERY_PARTITIONS_MAX, RecoveredCutover, RecoveryRead, RecoveryRecord, RecoveryRecordError,
};
pub use resolver::ShardResolver;
pub use segment_metadata::{SEGMENT_HASH_LEN, SegmentMetadata, SegmentMetadataError};
pub use store::{
    DeleteFailure, DeleteOutcome, ListPage, ListRequest, ListedObject, MAX_KEY_LEN,
    MAX_KEY_SEGMENT_LEN, MAX_KEY_SEGMENTS, MAX_KEYS_PER_REQUEST, MAX_METADATA_LEN, ObjectMeta,
    ObjectRange, ObjectStore, PutBody, PutMode, PutOptions, UserMetadata, validate_delete,
    validate_key, validate_metadata, validate_prefix, validate_range,
};
pub use tier::{
    ScopeCancelled, TierExecutor, TierJobCancelled, TierJobClass, TierJobHandle, TierJobRequest,
    TierScope, TierScopeToken,
};
pub use topic::{
    MAX_TOPIC_OBJECT_LEN, TopicCompression, TopicGeoMode, TopicObject, TopicObjectError, TopicRead,
    TopicState,
};
