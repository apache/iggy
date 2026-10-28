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

//! Tiered storage proof of concept (RFC #4404): copies every sealed segment of the partitions a
//! node leads to an object store, as cooperative tasks on the owning shard, optionally evicts the
//! local `.log` once it is uploaded, and serves evicted segments to polls from the store through a
//! small chunk cache. It exists to measure all of that against the pump on one node, so it has no
//! replicated floor, no topic options and no restart support (an evicted segment is forgotten at
//! boot), and it is configured from `TIERED_POC_*` environment variables, not the server config.

use crate::shell::ServerShard;
use bytes::{Bytes, BytesMut};
use compio::BufResult;
use compio::fs::{File, OpenOptions};
use compio::io::AsyncWriteAtExt;
use consensus::PartitionsHandle;
use iggy_object_store::fs::FsStore;
use iggy_object_store::s3::{Credentials, S3Config, S3Store};
use iggy_object_store::{
    Backend, ObjectMeta, ObjectStore, ObjectStoreError, PutBody, PutOptions, UploadPacer,
    file_crc32c,
};
use partitions::RemoteSegmentReader;
use shard::{Receiver, TierCursors, TierSegment};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};

const ENV_PREFIX: &str = "TIERED_POC_";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
/// Every pass costs the pump one snapshot frame.
const MIN_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_MAX_IN_FLIGHT: usize = 4;
const DEFAULT_CACHE_CHUNKS: usize = 64;
/// Remote reads fetch and cache whole aligned chunks of this size.
const CHUNK_LEN: u64 = 1024 * 1024;
/// Store calls have no timeout of their own and the HTTP client sets no keepalive, so a call
/// whose peer vanished without a reset would otherwise wait forever.
const STORE_TIMEOUTS: StoreTimeouts = StoreTimeouts {
    get: Duration::from_secs(10),
    head: Duration::from_secs(10),
    put_base: Duration::from_secs(30),
};
/// A PUT may take `put_base` plus its length at this rate.
const PUT_FLOOR_BYTES_PER_SECOND: u64 = 4 * 1024 * 1024;
/// A failed upload waits `interval` before its first retry, doubling up to this.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(300);
/// Holds the minted cluster id in the server's data directory.
const CLUSTER_UID_FILE: &str = "tiered_poc_cluster_uid";
const CLUSTER_UID_LEN: usize = 32;

pub struct TierUploaderConfig {
    backend: BackendConfig,
    /// Key prefix inside the store, ahead of the cluster id.
    prefix: String,
    /// Stands in for the RFC's `cluster_uid`, which the metadata plane does not mint yet. `None`
    /// uses the one minted into `data_path` on first start.
    cluster_uid: Option<String>,
    /// The server's data directory.
    data_path: String,
    interval: Duration,
    max_in_flight: usize,
    /// Unlink the local `.log` once it is uploaded.
    evict: bool,
    /// Capacity of the per-shard chunk cache, in `CHUNK_LEN` chunks.
    cache_chunks: usize,
    /// Per-shard cap on the bytes uploads read and send each second.
    max_upload_bytes_per_second: Option<u64>,
}

enum BackendConfig {
    Fs(String),
    S3(S3Config),
}

#[derive(Debug, Clone, Copy)]
struct StoreTimeouts {
    get: Duration,
    head: Duration,
    put_base: Duration,
}

/// A segment is identified by its partition incarnation and first offset, since slab ids are
/// reused after a delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SegmentId {
    namespace: u64,
    created_revision: u64,
    start_offset: u64,
}

impl SegmentId {
    fn of(segment: &TierSegment) -> Self {
        Self {
            namespace: segment.namespace.inner(),
            created_revision: segment.created_revision,
            start_offset: segment.start_offset,
        }
    }
}

#[derive(Default)]
struct UploadState {
    /// Uploaded segments the pump still lists, with the bytes each upload sent.
    uploaded: HashMap<SegmentId, u64>,
    in_flight: HashSet<SegmentId>,
    failures: HashMap<SegmentId, UploadFailure>,
    /// With eviction off an uploaded segment stays listed, so the pump skips what lies below.
    cursors: TierCursors,
}

struct UploadFailure {
    attempts: u32,
    /// `None` once a failure no retry can fix parked the segment until restart.
    retry_at: Option<Instant>,
}

#[derive(Debug, thiserror::Error)]
enum UploadError {
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
    #[error("{key} already holds other bytes: {detail}")]
    Diverged { key: String, detail: String },
}

impl UploadError {
    fn is_retryable(&self) -> bool {
        match self {
            // The local file, such as a descriptor limit hit while opening it.
            Self::Store(ObjectStoreError::Source(_)) => true,
            Self::Store(error) => error.is_retryable(),
            Self::Diverged { .. } => false,
        }
    }
}

impl TierUploaderConfig {
    /// `None` when no backend is configured, which leaves the uploader off.
    pub fn from_env(data_path: String) -> Result<Option<Self>, String> {
        let fs_dir = env("FS_DIR");
        let s3_endpoint = env("S3_ENDPOINT");
        let backend = match (fs_dir, s3_endpoint) {
            (None, None) => return Ok(None),
            (Some(_), Some(_)) => {
                return Err(format!(
                    "set {ENV_PREFIX}FS_DIR or {ENV_PREFIX}S3_ENDPOINT, not both"
                ));
            }
            (Some(directory), None) => BackendConfig::Fs(directory),
            (None, Some(endpoint)) => BackendConfig::S3(S3Config {
                endpoint,
                region: env("S3_REGION").unwrap_or_else(|| "us-east-1".to_owned()),
                bucket: required("S3_BUCKET")?,
                path_style: env("S3_VIRTUAL_HOST").is_none_or(|value| value != "true"),
                credentials: Credentials {
                    access_key_id: required("S3_ACCESS_KEY_ID")?,
                    secret_access_key: required("S3_SECRET_ACCESS_KEY")?,
                    session_token: env("S3_SESSION_TOKEN"),
                },
            }),
        };
        Ok(Some(Self {
            backend,
            prefix: env("PREFIX").unwrap_or_else(|| "iggy".to_owned()),
            cluster_uid: env("CLUSTER_UID"),
            data_path,
            interval: parsed("INTERVAL_MS")?
                .map_or(DEFAULT_INTERVAL, Duration::from_millis)
                .max(MIN_INTERVAL),
            max_in_flight: parsed("MAX_IN_FLIGHT")?.map_or(DEFAULT_MAX_IN_FLIGHT, |max| {
                usize::try_from(max).unwrap_or(DEFAULT_MAX_IN_FLIGHT).max(1)
            }),
            evict: env("EVICT").is_some_and(|value| value == "true"),
            cache_chunks: parsed("CACHE_CHUNKS")?.map_or(DEFAULT_CACHE_CHUNKS, |chunks| {
                usize::try_from(chunks)
                    .unwrap_or(DEFAULT_CACHE_CHUNKS)
                    .max(1)
            }),
            max_upload_bytes_per_second: parsed("MAX_UPLOAD_MIB_PER_S")?
                .filter(|mebibytes| *mebibytes > 0)
                .map(|mebibytes| mebibytes.saturating_mul(1024 * 1024)),
        }))
    }

    async fn open_backend(&self) -> Result<Backend, ObjectStoreError> {
        match &self.backend {
            BackendConfig::Fs(directory) => Ok(Backend::Fs(FsStore::open(directory).await?)),
            BackendConfig::S3(config) => Ok(Backend::S3(S3Store::new(config.clone())?)),
        }
    }

    /// The slowest rate one PUT may run at before it counts as hung. Concurrent uploads share
    /// the pacer, so each gets its share of the cap.
    fn put_floor_bytes_per_second(&self) -> u64 {
        self.max_upload_bytes_per_second
            .map_or(PUT_FLOOR_BYTES_PER_SECOND, |cap| {
                (cap / self.max_in_flight as u64).clamp(1, PUT_FLOOR_BYTES_PER_SECOND)
            })
    }

    fn retry_delay(&self, attempts: u32) -> Duration {
        self.interval
            .saturating_mul(1 << attempts.saturating_sub(1).min(16))
            .min(MAX_RETRY_DELAY)
    }
}

/// Run until `stop` fires. Every `interval` the pump lists the sealed segments this node leads,
/// and each one not yet in the store is uploaded by its own detached task, at most
/// `max_in_flight` at a time per shard.
pub async fn run_tier_uploader(
    shard: Rc<ServerShard>,
    config: TierUploaderConfig,
    stop: Receiver<()>,
) {
    let cluster_uid = match config.cluster_uid.clone() {
        Some(cluster_uid) => cluster_uid,
        None => match persisted_cluster_uid(Path::new(&config.data_path)).await {
            Ok(cluster_uid) => cluster_uid,
            Err(error) => {
                warn!(shard = shard.id, %error, "tiered PoC: cannot read or mint the cluster id");
                return;
            }
        },
    };
    let store = match config.open_backend().await {
        Ok(store) => Rc::new(store),
        Err(error) => {
            warn!(shard = shard.id, %error, "tiered PoC: cannot open the object store");
            return;
        }
    };
    if shard.id == 0 {
        info!(
            cluster_uid,
            evict = config.evict,
            "tiered PoC uploader started"
        );
        if config.evict {
            warn!(
                "tiered PoC: evicted segments are forgotten at restart, so after one, polls skip \
                 their offsets and serve only what is still local"
            );
        }
    }
    shard
        .plane
        .partitions()
        .set_remote_reader(Rc::new(TierReader::new(
            Rc::clone(&store),
            config.cache_chunks,
            STORE_TIMEOUTS.get,
        )));
    let uploader = Rc::new(Uploader {
        calls: StoreCalls {
            store,
            pacer: config
                .max_upload_bytes_per_second
                .map(|cap| Rc::new(UploadPacer::new(cap))),
            put_floor_bytes_per_second: config.put_floor_bytes_per_second(),
            timeouts: STORE_TIMEOUTS,
        },
        shard,
        config,
        cluster_uid,
        state: RefCell::new(UploadState::default()),
    });
    trace!(shard = uploader.shard.id, "tiered PoC uploader started");
    loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> pass.
        if compio::time::timeout(uploader.config.interval, stop.recv())
            .await
            .is_ok()
        {
            break;
        }
        let cursors = uploader.state.borrow().cursors.clone();
        if let Some(sealed) = uploader.shard.request_tier_snapshot(cursors).await {
            uploader.schedule(sealed);
        }
    }
    trace!(shard = uploader.shard.id, "tiered PoC uploader exited");
}

struct Uploader {
    shard: Rc<ServerShard>,
    calls: StoreCalls,
    config: TierUploaderConfig,
    cluster_uid: String,
    state: RefCell<UploadState>,
}

impl Uploader {
    fn schedule(self: &Rc<Self>, sealed: Vec<TierSegment>) {
        let now = Instant::now();
        let mut state = self.state.borrow_mut();
        let listed: HashSet<SegmentId> = sealed.iter().map(SegmentId::of).collect();
        state.uploaded.retain(|id, _| listed.contains(id));
        state.failures.retain(|id, _| listed.contains(id));
        // A partition's cursor stops at its first segment not uploaded yet.
        let mut stalled = HashSet::new();
        for segment in sealed {
            let id = SegmentId::of(&segment);
            if let Some(&uploaded_size) = state.uploaded.get(&id) {
                if self.config.evict {
                    // The pump refused it until the WAL checkpoint passes the segment.
                    self.request_evict(&segment, uploaded_size);
                } else if !stalled.contains(&id.namespace) {
                    state.cursors.insert(
                        id.namespace,
                        (id.created_revision, segment.end_offset.saturating_add(1)),
                    );
                }
                continue;
            }
            stalled.insert(id.namespace);
            let waiting = state
                .failures
                .get(&id)
                .is_some_and(|failure| failure.retry_at.is_none_or(|retry_at| now < retry_at));
            if waiting
                || state.in_flight.len() >= self.config.max_in_flight
                || !state.in_flight.insert(id)
            {
                continue;
            }
            let uploader = Rc::clone(self);
            compio::runtime::spawn(async move { uploader.upload(segment).await }).detach();
        }
    }

    async fn upload(self: Rc<Self>, segment: TierSegment) {
        let id = SegmentId::of(&segment);
        let key_stem = self.key_stem(&segment);
        let started = Instant::now();
        let result = self.upload_segment(&key_stem, &segment).await;
        let mut state = self.state.borrow_mut();
        state.in_flight.remove(&id);
        match result {
            Ok(()) => {
                state.failures.remove(&id);
                state.uploaded.insert(id, segment.size);
                drop(state);
                info!(
                    shard = self.shard.id,
                    key = %key_stem,
                    bytes = segment.size,
                    elapsed_us = started.elapsed().as_micros(),
                    "tiered PoC uploaded a segment"
                );
                if self.config.evict {
                    self.request_evict(&segment, segment.size);
                }
            }
            Err(error) => {
                let failure = state.failures.entry(id).or_insert(UploadFailure {
                    attempts: 0,
                    retry_at: None,
                });
                failure.attempts += 1;
                if error.is_retryable() {
                    let delay = self.config.retry_delay(failure.attempts);
                    failure.retry_at = Some(Instant::now() + delay);
                    warn!(
                        shard = self.shard.id,
                        key = %key_stem,
                        %error,
                        attempts = failure.attempts,
                        retry_in_ms = delay.as_millis(),
                        "tiered PoC upload failed"
                    );
                } else {
                    failure.retry_at = None;
                    warn!(
                        shard = self.shard.id,
                        key = %key_stem,
                        %error,
                        "tiered PoC upload refused, the segment stays local until restart"
                    );
                }
            }
        }
    }

    async fn upload_segment(
        &self,
        key_stem: &str,
        segment: &TierSegment,
    ) -> Result<(), UploadError> {
        let config = self.shard.plane.partitions().config();
        let (stream_id, topic_id, partition_id) = (
            segment.namespace.stream_id(),
            segment.namespace.topic_id(),
            segment.namespace.partition_id(),
        );
        let identity = identity_metadata(segment);
        self.calls
            .upload_log(
                &format!("{key_stem}.log"),
                &config.get_messages_path(stream_id, topic_id, partition_id, segment.start_offset),
                segment.size,
                &identity,
                self.config.evict,
            )
            .await?;
        self.calls
            .upload_index(
                &format!("{key_stem}.index"),
                &config.get_index_path(stream_id, topic_id, partition_id, segment.start_offset),
                &identity,
            )
            .await
    }

    fn key_stem(&self, segment: &TierSegment) -> String {
        format!(
            "{}/{}/{}/{}/{}/{}/{:020}",
            self.config.prefix,
            self.cluster_uid,
            segment.namespace.stream_id(),
            segment.namespace.topic_id(),
            segment.namespace.partition_id(),
            segment.created_revision,
            segment.start_offset,
        )
    }

    fn request_evict(&self, segment: &TierSegment, uploaded_size: u64) {
        self.shard.request_tier_evict(
            segment.namespace,
            segment.created_revision,
            segment.start_offset,
            uploaded_size,
            Arc::from(format!("{}.log", self.key_stem(segment))),
        );
    }
}

/// One shard's upload calls to the store, each bounded by a timeout.
struct StoreCalls {
    store: Rc<Backend>,
    pacer: Option<Rc<UploadPacer>>,
    put_floor_bytes_per_second: u64,
    timeouts: StoreTimeouts,
}

impl StoreCalls {
    /// Uploads `len` bytes of the `.log` at `path` with its CRC32C, which the store verifies. An
    /// object already under `key` counts only when its size and identity match, and with
    /// `verify_content`, which eviction needs, its CRC32C too.
    async fn upload_log(
        &self,
        key: &str,
        path: &str,
        len: u64,
        identity: &[(String, String)],
        verify_content: bool,
    ) -> Result<(), UploadError> {
        let file = File::open(path).await.map_err(ObjectStoreError::Source)?;
        // HEAD first: after a restart most local segments are already uploaded, and a store
        // that evaluates the PUT condition at completion would take the whole body again.
        match self.head(key).await {
            Ok(existing) => {
                verify_existing(key, &existing, len, identity)?;
                if verify_content {
                    let checksum = file_crc32c(&file, 0, len)
                        .await
                        .map_err(ObjectStoreError::Source)?;
                    verify_existing(key, &existing, len, &[crc32c_entry(checksum)])?;
                }
                return Ok(());
            }
            Err(ObjectStoreError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        let checksum = file_crc32c(&file, 0, len)
            .await
            .map_err(ObjectStoreError::Source)?;
        let mut metadata = identity.to_vec();
        metadata.push(crc32c_entry(checksum));
        let options = PutOptions {
            if_absent: true,
            metadata,
            crc32c: Some(checksum),
        };
        let body = PutBody::File {
            file,
            offset: 0,
            len,
            pacer: self.pacer.clone(),
        };
        self.put_once(key, body, &options).await
    }

    /// Uploads the `.index` at `path`. An object already under `key` counts only when its size
    /// and identity match.
    async fn upload_index(
        &self,
        key: &str,
        path: &str,
        identity: &[(String, String)],
    ) -> Result<(), UploadError> {
        let file = File::open(path).await.map_err(ObjectStoreError::Source)?;
        let len = file
            .metadata()
            .await
            .map_err(ObjectStoreError::Source)?
            .len();
        match self.head(key).await {
            Ok(existing) => return verify_existing(key, &existing, len, identity),
            Err(ObjectStoreError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        let options = PutOptions {
            if_absent: true,
            metadata: identity.to_vec(),
            crc32c: None,
        };
        let body = PutBody::File {
            file,
            offset: 0,
            len,
            pacer: self.pacer.clone(),
        };
        self.put_once(key, body, &options).await
    }

    /// A conditional PUT. Losing the race to another writer counts as uploaded only when that
    /// writer stored the same size and metadata.
    async fn put_once(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), UploadError> {
        let len = body.len();
        let limit = self.timeouts.put_base
            + Duration::from_secs(len / self.put_floor_bytes_per_second.max(1));
        match timed("PUT", limit, self.store.put(key, body, options)).await {
            Ok(()) => Ok(()),
            Err(ObjectStoreError::PreconditionFailed) => {
                let existing = self.head(key).await?;
                verify_existing(key, &existing, len, &options.metadata)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        timed("HEAD", self.timeouts.head, self.store.head(key)).await
    }
}

/// Bounds a store call. A timeout is a transport failure, so the caller retries it.
async fn timed<T>(
    operation: &str,
    limit: Duration,
    call: impl Future<Output = Result<T, ObjectStoreError>>,
) -> Result<T, ObjectStoreError> {
    compio::time::timeout(limit, call)
        .await
        .unwrap_or_else(|_| {
            Err(ObjectStoreError::Transport(format!(
                "{operation} timed out after {limit:?}"
            )))
        })
}

/// What the key alone cannot tell apart: another data directory sharing the store can reuse the
/// same incarnation and offsets.
fn identity_metadata(segment: &TierSegment) -> Vec<(String, String)> {
    vec![
        (
            "created-revision".to_owned(),
            segment.created_revision.to_string(),
        ),
        ("end-offset".to_owned(), segment.end_offset.to_string()),
        (
            "end-timestamp".to_owned(),
            segment.end_timestamp.to_string(),
        ),
        ("start-offset".to_owned(), segment.start_offset.to_string()),
        (
            "start-timestamp".to_owned(),
            segment.start_timestamp.to_string(),
        ),
    ]
}

fn crc32c_entry(checksum: u32) -> (String, String) {
    ("crc32c".to_owned(), format!("{checksum:08x}"))
}

fn verify_existing(
    key: &str,
    existing: &ObjectMeta,
    len: u64,
    expected: &[(String, String)],
) -> Result<(), UploadError> {
    if existing.size != len {
        return Err(UploadError::Diverged {
            key: key.to_owned(),
            detail: format!("{} bytes, the local copy has {len}", existing.size),
        });
    }
    for (name, value) in expected {
        let stored = existing
            .metadata
            .iter()
            .find(|(stored_name, _)| stored_name == name)
            .map(|(_, stored)| stored.as_str());
        if stored != Some(value.as_str()) {
            return Err(UploadError::Diverged {
                key: key.to_owned(),
                detail: format!("{name} {stored:?}, the local copy has {value:?}"),
            });
        }
    }
    Ok(())
}

/// The cluster id minted into `data_path` on first start, so a wiped data directory or a second
/// server sharing the store never writes under keys an earlier run used. Every shard calls this
/// at start: each links its own candidate, a link refuses an existing name, and all read back
/// the one that won.
async fn persisted_cluster_uid(data_path: &Path) -> io::Result<String> {
    let path = data_path.join(CLUSTER_UID_FILE);
    if let Some(cluster_uid) = read_cluster_uid(&path).await? {
        return Ok(cluster_uid);
    }
    let candidate = format!("{:032x}", rand::random::<u128>());
    let temp = data_path.join(format!(
        ".{CLUSTER_UID_FILE}.{:016x}.tmp",
        rand::random::<u64>()
    ));
    let written = write_synced(&temp, candidate.into_bytes()).await;
    let linked = match written {
        Ok(()) => compio::fs::hard_link(&temp, &path).await,
        Err(error) => Err(error),
    };
    let _ = compio::fs::remove_file(&temp).await;
    match linked {
        Ok(()) => File::open(data_path).await?.sync_all().await?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    read_cluster_uid(&path).await?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} vanished after it was created", path.display()),
        )
    })
}

async fn read_cluster_uid(path: &Path) -> io::Result<Option<String>> {
    let bytes = match compio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let valid = bytes.len() == CLUSTER_UID_LEN && bytes.iter().all(u8::is_ascii_hexdigit);
    if !valid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} does not hold a cluster id", path.display()),
        ));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn write_synced(path: &Path, bytes: Vec<u8>) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    let BufResult(written, _) = file.write_all_at(bytes, 0).await;
    written?;
    file.sync_all().await
}

fn env(name: &str) -> Option<String> {
    std::env::var(format!("{ENV_PREFIX}{name}"))
        .ok()
        .filter(|value| !value.is_empty())
}

fn required(name: &str) -> Result<String, String> {
    env(name).ok_or_else(|| format!("{ENV_PREFIX}{name} is required"))
}

fn parsed(name: &str) -> Result<Option<u64>, String> {
    env(name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| format!("{ENV_PREFIX}{name} must be a whole number, got {value:?}"))
        })
        .transpose()
}

/// Serves evicted segments to disk polls. Reads are split into aligned `CHUNK_LEN` chunks, each
/// fetched with one ranged GET and kept in a small per-shard LRU, so a consumer reading a segment
/// sequentially costs one GET per chunk rather than one per poll.
struct TierReader {
    store: Rc<Backend>,
    get_timeout: Duration,
    cache: RefCell<ChunkCache>,
}

struct ChunkCache {
    capacity: usize,
    chunks: HashMap<(String, u64), Bytes>,
    /// Least recently inserted first. Hits do not reorder, which is enough for sequential reads.
    order: VecDeque<(String, u64)>,
}

impl TierReader {
    fn new(store: Rc<Backend>, capacity: usize, get_timeout: Duration) -> Self {
        Self {
            store,
            get_timeout,
            cache: RefCell::new(ChunkCache {
                capacity,
                chunks: HashMap::with_capacity(capacity),
                order: VecDeque::with_capacity(capacity),
            }),
        }
    }

    /// Chunk `index` of `key`, which holds `object_len` bytes. Only a chunk of exactly the
    /// expected length, full or the object's tail, is returned and cached.
    async fn chunk(&self, key: &str, index: u64, object_len: u64) -> io::Result<Bytes> {
        let cache_key = (key.to_owned(), index);
        if let Some(chunk) = self.cache.borrow().chunks.get(&cache_key) {
            return Ok(chunk.clone());
        }
        let start = index * CHUNK_LEN;
        let end = (start + CHUNK_LEN).min(object_len);
        if start >= end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{key} ends before byte {start}"),
            ));
        }
        let started = Instant::now();
        let chunk = compio::time::timeout(self.get_timeout, self.store.get_range(key, start..end))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "{key}: GET of bytes {start}..{end} timed out after {:?}",
                        self.get_timeout
                    ),
                )
            })?
            .map_err(|error| read_error(key, &error))?;
        if chunk.len() as u64 != end - start {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "{key}: GET of bytes {start}..{end} returned {} bytes",
                    chunk.len()
                ),
            ));
        }
        debug!(
            key,
            start,
            bytes = chunk.len(),
            elapsed_us = started.elapsed().as_micros(),
            "tiered PoC fetched a chunk"
        );
        let mut cache = self.cache.borrow_mut();
        if cache.chunks.len() >= cache.capacity
            && let Some(oldest) = cache.order.pop_front()
        {
            cache.chunks.remove(&oldest);
        }
        if cache
            .chunks
            .insert(cache_key.clone(), chunk.clone())
            .is_none()
        {
            cache.order.push_back(cache_key);
        }
        Ok(chunk)
    }
}

/// Keeps the store error's permanence in the I/O error kind the poll walk classifies, see
/// [`RemoteSegmentReader::read`], and names the object.
fn read_error(key: &str, error: &ObjectStoreError) -> io::Error {
    let kind = match error {
        ObjectStoreError::NotFound => io::ErrorKind::NotFound,
        ObjectStoreError::Archived => io::ErrorKind::Unsupported,
        ObjectStoreError::InvalidRange => io::ErrorKind::UnexpectedEof,
        ObjectStoreError::Malformed(_) => io::ErrorKind::InvalidData,
        ObjectStoreError::InvalidKey(_) | ObjectStoreError::InvalidRequest(_) => {
            io::ErrorKind::InvalidInput
        }
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, format!("{key}: {error}"))
}

impl RemoteSegmentReader for TierReader {
    fn read<'a>(
        &'a self,
        key: &'a str,
        position: u64,
        len: usize,
        object_len: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Bytes>> + 'a>> {
        Box::pin(async move {
            let end = position + len as u64;
            let mut bytes = BytesMut::with_capacity(len);
            let mut cursor = position;
            while cursor < end {
                let index = cursor / CHUNK_LEN;
                let chunk = self.chunk(key, index, object_len).await?;
                let skip = usize::try_from(cursor - index * CHUNK_LEN).unwrap_or(usize::MAX);
                if skip >= chunk.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{key} ends before byte {cursor}"),
                    ));
                }
                let wanted = usize::try_from(end - cursor).unwrap_or(usize::MAX);
                let take = (chunk.len() - skip).min(wanted);
                bytes.extend_from_slice(&chunk[skip..skip + take]);
                cursor += take as u64;
            }
            Ok(bytes.freeze())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use message_bus::transports::tls::install_default_crypto_provider;
    use server_common::sharding::IggyNamespace;

    const SEGMENT_BYTES: usize = 3 * 1024 * 1024 + 7;

    fn segment(start_offset: u64) -> TierSegment {
        TierSegment {
            namespace: IggyNamespace::new(1, 1, 0),
            created_revision: 7,
            start_offset,
            end_offset: start_offset + 99,
            start_timestamp: 1_000,
            end_timestamp: 2_000,
            size: SEGMENT_BYTES as u64,
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        store_dir: std::path::PathBuf,
        log_path: String,
        index_path: String,
        calls: StoreCalls,
    }

    async fn fixture() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let store_dir = directory.path().join("store");
        let log_path = directory.path().join("segment.log");
        let index_path = directory.path().join("segment.index");
        let bytes: Vec<u8> = (0..SEGMENT_BYTES)
            .map(|index| u8::try_from(index % 253).unwrap())
            .collect();
        std::fs::write(&log_path, &bytes).unwrap();
        std::fs::write(&index_path, [3; 160]).unwrap();
        let store = Backend::Fs(FsStore::open(&store_dir).await.unwrap());
        Fixture {
            calls: StoreCalls {
                store: Rc::new(store),
                pacer: None,
                put_floor_bytes_per_second: PUT_FLOOR_BYTES_PER_SECOND,
                timeouts: STORE_TIMEOUTS,
            },
            store_dir,
            log_path: log_path.display().to_string(),
            index_path: index_path.display().to_string(),
            _directory: directory,
        }
    }

    #[compio::test]
    async fn given_new_segment_when_uploading_should_store_crc32c_and_identity() {
        let fixture = fixture().await;
        let identity = identity_metadata(&segment(0));

        let uploaded = fixture
            .calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity,
                true,
            )
            .await;

        assert!(uploaded.is_ok(), "{uploaded:?}");
        let stored = fixture.calls.store.head("a/log").await.unwrap();
        let local = File::open(&fixture.log_path).await.unwrap();
        let checksum = file_crc32c(&local, 0, SEGMENT_BYTES as u64).await.unwrap();
        let mut expected = identity.clone();
        expected.push(crc32c_entry(checksum));
        assert!(verify_existing("a/log", &stored, SEGMENT_BYTES as u64, &expected).is_ok());
    }

    #[compio::test]
    async fn given_same_segment_uploaded_when_uploading_again_should_accept_without_rewriting() {
        let fixture = fixture().await;
        let identity = identity_metadata(&segment(0));
        let calls = &fixture.calls;
        calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity,
                true,
            )
            .await
            .unwrap();
        let object = fixture.store_dir.join("a/log");
        let written = std::fs::metadata(&object).unwrap().modified().unwrap();

        let again = calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity,
                true,
            )
            .await;

        assert!(again.is_ok(), "{again:?}");
        assert_eq!(
            std::fs::metadata(&object).unwrap().modified().unwrap(),
            written
        );
    }

    #[compio::test]
    async fn given_key_holding_another_runs_segment_when_uploading_should_refuse() {
        let fixture = fixture().await;
        let calls = &fixture.calls;
        let earlier = identity_metadata(&segment(0));
        calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &earlier,
                false,
            )
            .await
            .unwrap();
        let mut later = segment(0);
        later.end_timestamp += 1;

        let refused = calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity_metadata(&later),
                false,
            )
            .await;

        assert!(
            matches!(refused, Err(UploadError::Diverged { .. })),
            "{refused:?}"
        );
        assert!(!refused.unwrap_err().is_retryable());
    }

    #[compio::test]
    async fn given_same_identity_but_other_bytes_when_uploading_for_eviction_should_refuse() {
        let fixture = fixture().await;
        let calls = &fixture.calls;
        let identity = identity_metadata(&segment(0));
        calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity,
                true,
            )
            .await
            .unwrap();
        let mut bytes = std::fs::read(&fixture.log_path).unwrap();
        bytes[SEGMENT_BYTES / 2] ^= 1;
        std::fs::write(&fixture.log_path, &bytes).unwrap();

        let kept = calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity,
                false,
            )
            .await;
        let refused = calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity,
                true,
            )
            .await;

        assert!(kept.is_ok(), "{kept:?}");
        assert!(
            matches!(refused, Err(UploadError::Diverged { .. })),
            "{refused:?}"
        );
    }

    #[compio::test]
    async fn given_index_of_another_size_when_uploading_should_refuse() {
        let fixture = fixture().await;
        let calls = &fixture.calls;
        let identity = identity_metadata(&segment(0));
        calls
            .upload_index("a/index", &fixture.index_path, &identity)
            .await
            .unwrap();
        std::fs::write(&fixture.index_path, [3; 176]).unwrap();

        let refused = calls
            .upload_index("a/index", &fixture.index_path, &identity)
            .await;

        assert!(
            matches!(refused, Err(UploadError::Diverged { .. })),
            "{refused:?}"
        );
    }

    #[test]
    fn given_existing_object_when_verifying_should_require_size_and_every_entry() {
        let expected = vec![("crc32c".to_owned(), "0000002a".to_owned())];
        let object = |size, metadata: &[(&str, &str)]| ObjectMeta {
            size,
            etag: String::new(),
            metadata: metadata
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
        };

        let matching = verify_existing("k", &object(10, &[("crc32c", "0000002a")]), 10, &expected);
        let resized = verify_existing("k", &object(11, &[("crc32c", "0000002a")]), 10, &expected);
        let other = verify_existing("k", &object(10, &[("crc32c", "0000002b")]), 10, &expected);
        let missing = verify_existing("k", &object(10, &[]), 10, &expected);

        assert!(matching.is_ok());
        assert!(resized.is_err());
        assert!(other.is_err());
        assert!(missing.is_err());
    }

    #[compio::test]
    async fn given_shards_racing_on_first_start_when_minting_cluster_uid_should_agree_and_keep_it()
    {
        let directory = tempfile::tempdir().unwrap();

        let (first, second) = futures::join!(
            persisted_cluster_uid(directory.path()),
            persisted_cluster_uid(directory.path())
        );
        let after_restart = persisted_cluster_uid(directory.path()).await.unwrap();
        let elsewhere = persisted_cluster_uid(tempfile::tempdir().unwrap().path())
            .await
            .unwrap();

        let first = first.unwrap();
        assert_eq!(first.len(), CLUSTER_UID_LEN);
        assert_eq!(second.unwrap(), first);
        assert_eq!(after_restart, first);
        assert_ne!(elsewhere, first);
    }

    #[test]
    fn given_store_errors_when_reading_remotely_should_keep_permanence_and_key() {
        let permanent = [
            ObjectStoreError::NotFound,
            ObjectStoreError::Archived,
            ObjectStoreError::InvalidRange,
            ObjectStoreError::Malformed(String::new()),
        ];
        let transient = [
            ObjectStoreError::Auth(String::new()),
            ObjectStoreError::Throttled(String::new()),
            ObjectStoreError::Transport(String::new()),
        ];

        for error in &permanent {
            let mapped = read_error("a/key.log", error);
            assert_ne!(mapped.kind(), io::ErrorKind::Other, "{error}");
            assert!(mapped.to_string().starts_with("a/key.log: "));
        }
        for error in &transient {
            assert_eq!(read_error("a/key.log", error).kind(), io::ErrorKind::Other);
        }
    }

    #[compio::test]
    async fn given_hung_store_call_when_timed_should_fail_as_retryable_transport() {
        let hung = std::future::pending::<Result<(), ObjectStoreError>>();

        let result = timed("GET", Duration::from_millis(20), hung).await;

        let error = result.unwrap_err();
        assert!(matches!(error, ObjectStoreError::Transport(_)));
        assert!(error.is_retryable());
    }

    /// A store host that accepts connections and never answers, like one that vanished after
    /// the request reached it.
    fn silent_store() -> (std::net::TcpListener, Rc<Backend>) {
        install_default_crypto_provider();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let store = S3Store::new(S3Config {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            region: "us-east-1".to_owned(),
            bucket: "bucket".to_owned(),
            path_style: true,
            credentials: Credentials {
                access_key_id: "access".to_owned(),
                secret_access_key: "secret".to_owned(),
                session_token: None,
            },
        })
        .unwrap();
        (listener, Rc::new(Backend::S3(store)))
    }

    #[compio::test]
    async fn given_silent_store_when_uploading_or_reading_should_fail_retryably_instead_of_hanging()
    {
        const BOUND: Duration = Duration::from_millis(200);
        let fixture = fixture().await;
        let (_listener, store) = silent_store();
        let calls = StoreCalls {
            store: Rc::clone(&store),
            pacer: None,
            put_floor_bytes_per_second: PUT_FLOOR_BYTES_PER_SECOND,
            timeouts: StoreTimeouts {
                get: BOUND,
                head: BOUND,
                put_base: BOUND,
            },
        };
        let reader = TierReader::new(store, 1, BOUND);
        let identity = identity_metadata(&segment(0));

        let uploaded = compio::time::timeout(
            Duration::from_secs(10),
            Box::pin(calls.upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity,
                true,
            )),
        )
        .await
        .expect("the upload must give up on a silent store");
        let read = compio::time::timeout(Duration::from_secs(10), reader.read("a/log", 0, 16, 64))
            .await
            .expect("the read must give up on a silent store");

        assert!(uploaded.unwrap_err().is_retryable());
        assert_eq!(read.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[compio::test]
    async fn given_object_shorter_than_its_segment_when_reading_should_not_cache_the_short_chunk() {
        let fixture = fixture().await;
        let calls = &fixture.calls;
        calls
            .upload_log(
                "a/log",
                &fixture.log_path,
                SEGMENT_BYTES as u64,
                &identity_metadata(&segment(0)),
                false,
            )
            .await
            .unwrap();
        let reader = TierReader::new(Rc::clone(&calls.store), 4, STORE_TIMEOUTS.get);
        let claimed_len = SEGMENT_BYTES as u64 + CHUNK_LEN;

        let tail = reader.read("a/log", 3 * CHUNK_LEN, 7, claimed_len).await;
        let whole = reader.read("a/log", 0, 16, SEGMENT_BYTES as u64).await;

        assert_eq!(tail.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(reader.cache.borrow().chunks.len(), 1);
        assert_eq!(whole.unwrap().len(), 16);
    }

    #[test]
    fn given_repeated_failures_when_backing_off_should_double_up_to_the_cap() {
        let config = TierUploaderConfig {
            backend: BackendConfig::Fs(String::new()),
            prefix: String::new(),
            cluster_uid: None,
            data_path: String::new(),
            interval: Duration::from_secs(1),
            max_in_flight: 4,
            evict: false,
            cache_chunks: 1,
            max_upload_bytes_per_second: Some(8 * 1024 * 1024),
        };

        assert_eq!(config.retry_delay(1), Duration::from_secs(1));
        assert_eq!(config.retry_delay(3), Duration::from_secs(4));
        assert_eq!(config.retry_delay(40), MAX_RETRY_DELAY);
        assert_eq!(config.put_floor_bytes_per_second(), 2 * 1024 * 1024);
    }
}
