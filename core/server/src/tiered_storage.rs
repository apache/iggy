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

//! What the server does with the `[tiered_storage]` section: the checks
//! and the probe before the shards start, and the tier service of each
//! shard with the store identity check of the node.
//!
//! Each shard gets its own store and service, because a provider holds
//! files and connections of the shard that owns it. What the shards share,
//! the credentials of an `s3` store, the access token of a `gcs` store and
//! the objects of a `mock_s3` store, is in [`SharedStore`].

use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::time::Duration;

use configs::tiered_storage::{
    GcsConfig as GcsSection, S3UrlStyle, TieredStorageBackend, TieredStorageConfig,
};
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_common::IggyByteSize;
use iggy_object_store::gcs::{GcsAuth, GcsConfig, GcsStore};
use iggy_object_store::s3::{S3Config, S3Store, SharedCredentials, UrlStyle};
use iggy_object_store::{
    Backend, ErrorClass, FsStore, IdentityRead, KeyLayout, MockS3Latency, MockS3Store, ObjectStore,
    ObjectStoreError, StoreIdentity,
};
use message_bus::IggyMessageBus;
use partitions::{
    ExecutorDeleter, ExecutorFreshSweeper, ExecutorFreshUploader, ExecutorLongTermMerger,
    ExecutorLongTermWriter, ExecutorManifestPublisher, ExecutorObjects, ExecutorTopicObjects,
    ExecutorUploader, RemoteSegments,
};
use shard::tier_service::{ClassLimits, StoreCallPolicy, TierService, TierServiceConfig};
use shard::{Sender, channel};
use tracing::{info, warn};

use crate::origin_recovery::OriginRecovery;
use crate::server_error::ServerError;
use crate::shell::ServerShard;
use crate::store_identity::{self, IdentityChecker, TierHandlers};

/// The longest the boot probe, or one store identity check, waits for the
/// store. A store that does not answer is an outage that the node waits out,
/// so neither waits for the whole request timeout.
const IDENTITY_TIMEOUT: Duration = Duration::from_secs(5);

/// Where an `s3` store is: the keys that `[tiered_storage]` and every
/// `[[geo.peers]]` entry share.
pub struct S3Location<'a> {
    pub endpoint: &'a str,
    pub region: &'a str,
    pub bucket: &'a str,
    pub url_style: S3UrlStyle,
}

/// What every shard of a node shares to reach the store: the credentials
/// that an `s3` store signs with, how a `gcs` store authorizes its
/// requests, and the objects of a `mock_s3` store.
#[derive(Clone, Default)]
pub struct SharedStore {
    credentials: Option<SharedCredentials>,
    gcs: Option<GcsAuth>,
    mock_s3: Option<MockS3Store>,
}

impl SharedStore {
    /// What the shards of a node with `config` share, built once per node.
    ///
    /// # Errors
    /// What [`crate::s3_credentials::credentials`] and
    /// [`crate::gcs_credentials::backend_auth`] return.
    pub fn new(config: &TieredStorageConfig) -> Result<Self, ServerError> {
        Ok(Self {
            credentials: crate::s3_credentials::credentials(config)?,
            gcs: (config.backend == TieredStorageBackend::Gcs)
                .then(|| crate::gcs_credentials::backend_auth(&config.gcs))
                .transpose()?,
            mock_s3: (config.backend == TieredStorageBackend::MockS3)
                .then(|| MockS3Store::new(mock_s3_latency(config))),
        })
    }
}

impl From<SharedCredentials> for SharedStore {
    fn from(credentials: SharedCredentials) -> Self {
        Self {
            credentials: Some(credentials),
            ..Self::default()
        }
    }
}

impl From<GcsAuth> for SharedStore {
    fn from(auth: GcsAuth) -> Self {
        Self {
            gcs: Some(auth),
            ..Self::default()
        }
    }
}

/// Boot-time checks of the section, after `Validatable::validate`. A
/// configuration that can never work refuses the boot here, before any
/// shard starts.
pub fn preflight(config: &TieredStorageConfig) -> Result<(), ServerError> {
    match config.backend {
        TieredStorageBackend::None => return Ok(()),
        // The credentials of the s3 and gcs backends come from
        // `crate::s3_credentials` and `crate::gcs_credentials`.
        TieredStorageBackend::S3 | TieredStorageBackend::Gcs | TieredStorageBackend::MockS3 => {}
        TieredStorageBackend::Fs => {
            let metadata = std::fs::metadata(&config.path).map_err(|source| {
                ServerError::TieredStoragePath {
                    path: config.path.clone(),
                    source,
                }
            })?;
            if !metadata.is_dir() {
                return Err(ServerError::TieredStorage {
                    reason: format!("tiered_storage.path {:?} is not a directory", config.path),
                });
            }
        }
    }
    layout(config)?;
    Ok(())
}

/// Read the store identity object once, before any shard starts, after
/// [`preflight`]. The read also shows whether the root and the credentials
/// work. A ranged read gets an error body where a HEAD gets none, so a store
/// can tell a missing bucket from a missing object.
///
/// When the object is there, the probe also sends a conditional create of
/// the same bytes, which the store must refuse, see
/// [`StoreIdentity::check_conditional_create`]. A store that takes it ignores
/// the condition that uploads rely on. A root that no cluster claimed yet
/// gets the same check from the store identity check, before any upload.
///
/// Returns whether the store answered in time. Only a missing bucket,
/// rejected credentials or a store that ignores conditional creates stop
/// the boot. The store identity check judges any other answer, and a store
/// that does not answer is an outage that the node waits out.
///
/// # Errors
/// [`ServerError::TieredStorageProbe`] when the bucket is missing or the
/// store refuses the credentials.
/// [`ServerError::TieredStorageConditionalCreate`] when the store took the
/// conditional create. [`ServerError::TieredStorageProbeRuntime`] when the
/// runtime for the read cannot start.
pub fn probe(config: &TieredStorageConfig, shared: &SharedStore) -> Result<bool, ServerError> {
    if config.backend == TieredStorageBackend::None {
        return Ok(true);
    }
    let layout = layout(config)?;
    let runtime = server_common::create_shard_executor().map_err(|source| {
        ServerError::TieredStorageProbeRuntime {
            source: server_common::diagnostics::enrich_runtime_create_error(source),
        }
    })?;
    runtime.block_on(async {
        let Some(store) = store(config, shared)? else {
            return Ok(true);
        };
        let timeout = identity_timeout(config);
        let read = compio::time::timeout(timeout, StoreIdentity::read(&store, &layout))
            .await
            .ok();
        let found = match &read {
            Some(Ok(IdentityRead::Found(identity))) => Some(*identity),
            _ => None,
        };
        let answered = probe_verdict(layout.identity_key(), read)?;
        if let Some(identity) = found {
            probe_conditional_create(&store, &layout, identity, timeout, config.backend).await?;
        }
        Ok(answered)
    })
}

/// The conditional create of the boot probe, over the identity object that
/// the probe found. Only a store that took it stops the boot: any other
/// answer leaves the check to the store identity check.
async fn probe_conditional_create<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    identity: StoreIdentity,
    timeout: Duration,
    backend: TieredStorageBackend,
) -> Result<(), ServerError> {
    let check =
        compio::time::timeout(timeout, identity.check_conditional_create(store, layout)).await;
    match check {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source @ ObjectStoreError::Unsupported(_))) => {
            Err(ServerError::TieredStorageConditionalCreate {
                key: layout.identity_key(),
                backend,
                source,
            })
        }
        Ok(Err(error)) => {
            warn!(
                key = layout.identity_key(),
                %error,
                "the object store failed the conditional create of the boot probe, so the store \
                 identity check sends it again before any upload"
            );
            Ok(())
        }
        Err(_) => {
            warn!(
                key = layout.identity_key(),
                "the object store did not answer the conditional create of the boot probe in \
                 time, so the store identity check sends it again before any upload"
            );
            Ok(())
        }
    }
}

/// Give `shard` a tier service over the configured store, and run the store
/// identity check of [`crate::store_identity`] next to it. The shard gets
/// its segment uploader and object deleter only while its node passes the
/// check. Shard 0 runs the check itself, and the other shards follow it.
///
/// The partitions of the shard read their remote segments through the
/// service from here on, which is what lets the node report that it can
/// tier topics.
///
/// Returns the stop signal of the check's task, or `None` without a backend.
pub fn start(
    config: &TieredStorageConfig,
    recovery: Option<OriginRecovery>,
    bus: &Rc<IggyMessageBus>,
    shard: &Rc<ServerShard>,
    replica_id: u8,
    node_store: &SharedStore,
) -> Option<Sender<()>> {
    // `preflight` and `probe` refused the boot already when these fail.
    let backend = store(config, node_store).ok().flatten()?;
    let layout = layout(config).ok()?;
    let service = TierService::new(backend, Rc::clone(bus), &service_config(config));
    shard.set_fresh_upload_pace(
        config.fresh_upload_interval.get_duration(),
        usize::try_from(config.jobs.fresh_uploads).unwrap_or(usize::MAX),
    );
    shard.set_long_term_backlog_limit(config.long_term_backlog_limit.as_bytes_u64());
    shard.set_fresh_pending_bytes(
        config.fresh_pending_bytes.as_bytes_u64(),
        config.fresh_partition_pending_bytes.as_bytes_u64(),
    );
    shard.plane.partitions().set_remote_segments(Rc::new(
        RemoteSegments::new(
            Rc::new(ExecutorObjects::new(service.clone())),
            layout.clone(),
        )
        .with_runtime(Rc::new(Rc::clone(bus))),
    ));
    shard
        .plane
        .metadata()
        .applied_frontier()
        .set_tiered_storage_ready(true);
    let handlers = TierHandlers::new(
        Rc::new(ExecutorUploader::new(service.clone(), layout.clone())),
        Rc::new(ExecutorDeleter::new(service.clone(), layout.clone())),
        Rc::new(ExecutorManifestPublisher::new(
            service.clone(),
            layout.clone(),
        )),
        Rc::new(ExecutorFreshUploader::new(service.clone(), layout.clone())),
        Rc::new(ExecutorLongTermWriter::new(service.clone(), layout.clone())),
        Rc::new(ExecutorLongTermMerger::new(service.clone(), layout.clone())),
        Rc::new(ExecutorFreshSweeper::new(service.clone(), layout.clone())),
        Rc::new(ExecutorTopicObjects::new(service, layout.clone())),
    );
    // The check gets a store of its own, outside the service, so no queued
    // job and no retry delays what it finds.
    let checker = if shard.id == 0 {
        store(config, node_store).ok().flatten().map(|store| {
            IdentityChecker::new(
                store,
                layout,
                replica_id == 0,
                identity_timeout(config),
                recovery,
            )
        })
    } else {
        None
    };
    // Every shard starts a service, and one line per node is enough.
    if shard.id == 0 {
        info!(
            backend = %config.backend,
            store = %store_location(config),
            prefix = %config.prefix,
            "tier service started; uploads, object deletes and the tiering of topics wait for \
             the store identity check"
        );
    }
    let (stop_tx, stop_rx) = channel(1);
    let task = compio::runtime::spawn(store_identity::run(
        Rc::clone(shard),
        handlers,
        checker,
        stop_rx,
    ));
    bus.track_background(task);
    Some(stop_tx)
}

/// The section's caps and timeouts in the form the tier service takes.
#[must_use]
pub fn service_config(config: &TieredStorageConfig) -> TierServiceConfig {
    let jobs = &config.jobs;
    let limits = |count: u32, bytes: Option<IggyByteSize>| ClassLimits {
        jobs: usize::try_from(count).unwrap_or(usize::MAX),
        bytes: bytes.map(|bytes| bytes.as_bytes_u64()),
    };
    TierServiceConfig {
        demand_fetch: limits(jobs.demand_fetches, Some(jobs.demand_fetch_bytes)),
        upload: limits(jobs.uploads, Some(jobs.upload_bytes)),
        fresh_upload: limits(jobs.fresh_uploads, None),
        speculative_fetch: limits(jobs.speculative_fetches, Some(jobs.speculative_fetch_bytes)),
        delete: limits(jobs.deletes, None),
        housekeeping: limits(jobs.housekeeping, None),
        calls: StoreCallPolicy {
            request_timeout: config.request_timeout.get_duration(),
            progress_timeout: config.progress_timeout.get_duration(),
            max_attempts: config.max_attempts,
            retry_delay: config.retry_delay.get_duration(),
            max_retry_delay: config.max_retry_delay.get_duration(),
        },
    }
}

/// The configured store, or `None` without a backend. An `s3` store signs
/// with the credentials of `shared`, and a `mock_s3` store is the one of
/// `shared`.
///
/// # Errors
/// [`ServerError::TieredStorage`] for an `s3` or `gcs` endpoint that cannot
/// address a bucket, an `s3` or `gcs` backend without its credentials, or a
/// `mock_s3` backend without its store.
fn store(
    config: &TieredStorageConfig,
    shared: &SharedStore,
) -> Result<Option<Backend>, ServerError> {
    let refuse = |reason: String| ServerError::TieredStorage { reason };
    Ok(match config.backend {
        TieredStorageBackend::None => None,
        TieredStorageBackend::Fs => Some(Backend::Fs(FsStore::new(&config.path))),
        TieredStorageBackend::MockS3 => {
            Some(Backend::MockS3(shared.mock_s3.clone().ok_or_else(
                || refuse("the mock_s3 backend has no store".to_owned()),
            )?))
        }
        TieredStorageBackend::S3 => {
            let credentials = shared
                .credentials
                .clone()
                .ok_or_else(|| refuse("the s3 backend has no credentials".to_owned()))?;
            Some(Backend::S3(
                S3Store::new(S3Location::from(config).config(credentials))
                    .map_err(|error| refuse(error.to_string()))?,
            ))
        }
        TieredStorageBackend::Gcs => {
            let auth = shared
                .gcs
                .clone()
                .ok_or_else(|| refuse("the gcs backend has no credentials".to_owned()))?;
            Some(Backend::Gcs(
                GcsStore::new(gcs_config(&config.gcs, auth))
                    .map_err(|error| refuse(error.to_string()))?,
            ))
        }
    })
}

/// The bucket of a `[tiered_storage.gcs]` or `[geo.peers.gcs]` table, in the
/// form the provider takes. An empty endpoint is Google Cloud Storage.
#[must_use]
pub fn gcs_config(section: &GcsSection, auth: GcsAuth) -> GcsConfig {
    GcsConfig {
        endpoint: section.endpoint().to_owned(),
        bucket: section.bucket.trim().to_owned(),
        auth,
    }
}

/// Where the store keeps objects, for the log.
fn store_location(config: &TieredStorageConfig) -> String {
    match config.backend {
        TieredStorageBackend::S3 => S3Location::from(config).to_string(),
        TieredStorageBackend::Gcs => format!("{}/{}", config.gcs.endpoint(), config.gcs.bucket),
        TieredStorageBackend::MockS3 => "memory".to_owned(),
        TieredStorageBackend::Fs | TieredStorageBackend::None => config.path.clone(),
    }
}

fn mock_s3_latency(config: &TieredStorageConfig) -> MockS3Latency {
    let mock = &config.mock_s3;
    MockS3Latency {
        put: mock.put_latency.get_duration(),
        get: mock.get_latency.get_duration(),
        other: mock.other_latency.get_duration(),
        bytes_per_second: mock.bytes_per_second.as_bytes_u64(),
    }
}

impl<'a> From<&'a TieredStorageConfig> for S3Location<'a> {
    fn from(config: &'a TieredStorageConfig) -> Self {
        Self {
            endpoint: &config.endpoint,
            region: &config.region,
            bucket: &config.bucket,
            url_style: config.url_style,
        }
    }
}

impl S3Location<'_> {
    /// The location in the form the provider takes. An empty endpoint is AWS
    /// in the region.
    #[must_use]
    pub fn config(&self, credentials: SharedCredentials) -> S3Config {
        let aws = self.endpoint.trim().is_empty();
        S3Config {
            endpoint: self.endpoint(),
            region: self.region.to_owned(),
            bucket: self.bucket.to_owned(),
            url_style: match self.url_style {
                S3UrlStyle::Auto if aws => UrlStyle::VirtualHosted,
                S3UrlStyle::VirtualHosted => UrlStyle::VirtualHosted,
                S3UrlStyle::Auto | S3UrlStyle::Path => UrlStyle::Path,
            },
            credentials,
        }
    }

    /// The endpoint, or AWS in the region when it is empty.
    #[must_use]
    pub fn endpoint(&self) -> String {
        if self.endpoint.trim().is_empty() {
            format!("https://s3.{}.amazonaws.com", self.region)
        } else {
            self.endpoint.to_owned()
        }
    }
}

/// The endpoint and the bucket, for the log.
impl Display for S3Location<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.endpoint(), self.bucket)
    }
}

/// The longest that one read of a store identity object waits.
pub fn identity_timeout(config: &TieredStorageConfig) -> Duration {
    config.request_timeout.get_duration().min(IDENTITY_TIMEOUT)
}

fn layout(config: &TieredStorageConfig) -> Result<KeyLayout, ServerError> {
    KeyLayout::new(&config.prefix).map_err(|source| ServerError::TieredStoragePrefix {
        prefix: config.prefix.clone(),
        source,
    })
}

/// What the answer to the probe of `key` means for the boot, and whether the
/// store answered. `None` when the store did not answer in time.
fn probe_verdict(
    key: String,
    answer: Option<Result<IdentityRead, ObjectStoreError>>,
) -> Result<bool, ServerError> {
    let error = match answer {
        Some(Ok(_)) => return Ok(true),
        Some(Err(error)) => error,
        None => {
            warn!(
                key,
                "the object store did not answer the boot probe in time, so the node boots with \
                 object_store_unreachable raised"
            );
            return Ok(false);
        }
    };
    match error {
        ObjectStoreError::Auth(_) | ObjectStoreError::BucketNotFound(_) => {
            Err(ServerError::TieredStorageProbe { key, source: error })
        }
        error if matches!(error.class(), ErrorClass::Transient) => {
            warn!(
                key,
                %error,
                "the object store failed the boot probe, so the node boots with \
                 object_store_unreachable raised"
            );
            Ok(false)
        }
        // The store answered, so the store identity check judges the answer.
        error => {
            warn!(
                key,
                %error,
                "the object store could not read the store identity object at boot, so this \
                 node runs no uploads or object deletes until a store identity check passes"
            );
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use iggy_object_store::{MemoryStore, PutBody, PutOptions};

    use super::*;
    use crate::store_identity::test_stores::IgnoresCondition;

    fn fs_config(path: &Path) -> TieredStorageConfig {
        TieredStorageConfig {
            backend: TieredStorageBackend::Fs,
            path: path.to_string_lossy().into_owned(),
            ..TieredStorageConfig::default()
        }
    }

    /// The two crates keep their own copy of the provisional defaults, and
    /// they must not drift apart.
    #[test]
    fn given_default_section_when_mapping_should_give_the_tier_service_defaults() {
        assert_eq!(
            service_config(&TieredStorageConfig::default()),
            TierServiceConfig::default()
        );
    }

    #[test]
    fn given_default_section_when_reading_the_fresh_upload_pace_should_give_the_shard_defaults() {
        let config = TieredStorageConfig::default();

        assert_eq!(
            config.fresh_upload_interval.get_duration(),
            shard::FRESH_UPLOAD_INTERVAL
        );
        assert_eq!(
            usize::try_from(config.jobs.fresh_uploads).unwrap(),
            shard::FRESH_UPLOADS
        );
    }

    #[test]
    fn given_default_section_when_reading_the_long_term_backlog_limit_should_give_the_shard_default()
     {
        assert_eq!(
            TieredStorageConfig::default()
                .long_term_backlog_limit
                .as_bytes_u64(),
            shard::rewrite_pacing::LONG_TERM_BACKLOG_LIMIT
        );
        assert_eq!(
            configs::tiered_storage::LONG_TERM_BACKLOG_LIMIT_MIN,
            partitions::LONG_TERM_SEGMENT_BYTES as u64
        );
    }

    #[test]
    fn given_default_section_when_reading_the_fresh_pending_bytes_should_give_the_shard_defaults() {
        let config = TieredStorageConfig::default();

        assert_eq!(
            config.fresh_pending_bytes.as_bytes_u64(),
            shard::FRESH_PENDING_BYTES_MAX
        );
        assert_eq!(
            config.fresh_partition_pending_bytes.as_bytes_u64(),
            shard::FRESH_PARTITION_PENDING_BYTES_MAX
        );
        assert_eq!(
            configs::tiered_storage::FRESH_PENDING_BYTES_MIN,
            shard::FRESH_OBJECT_BYTES
        );
    }

    #[test]
    fn given_no_backend_when_preflighting_should_accept() {
        assert!(preflight(&TieredStorageConfig::default()).is_ok());
    }

    #[test]
    fn given_fs_backend_with_a_directory_when_preflighting_should_accept() {
        let directory = tempfile::tempdir().unwrap();

        assert!(preflight(&fs_config(directory.path())).is_ok());
    }

    #[test]
    fn given_fs_backend_without_its_directory_when_preflighting_should_refuse_the_boot() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing");

        let error = preflight(&fs_config(&missing)).unwrap_err();

        assert!(
            matches!(
                error,
                ServerError::TieredStoragePath { ref source, .. }
                    if source.kind() == std::io::ErrorKind::NotFound
            ),
            "{error}"
        );
    }

    #[test]
    fn given_fs_backend_with_a_file_for_its_path_when_preflighting_should_refuse_the_boot() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file");
        std::fs::write(&file, b"not a directory").unwrap();

        let error = preflight(&fs_config(&file)).unwrap_err();

        assert!(
            matches!(error, ServerError::TieredStorage { .. }),
            "{error}"
        );
    }

    #[test]
    fn given_a_prefix_the_key_layout_refuses_when_preflighting_should_refuse_the_boot() {
        let directory = tempfile::tempdir().unwrap();
        let config = TieredStorageConfig {
            prefix: "iggy/".to_owned(),
            ..fs_config(directory.path())
        };

        let error = preflight(&config).unwrap_err();

        assert!(
            matches!(error, ServerError::TieredStoragePrefix { .. }),
            "{error}"
        );
    }

    #[test]
    fn given_mock_s3_backend_when_building_stores_should_share_one_store_across_them() {
        let config = TieredStorageConfig {
            backend: TieredStorageBackend::MockS3,
            ..TieredStorageConfig::default()
        };
        let shared = SharedStore::new(&config).unwrap();
        let writer = store(&config, &shared).unwrap().unwrap();
        let reader = store(&config, &shared).unwrap().unwrap();

        assert!(probe(&config, &shared).unwrap());
        let read = server_common::create_shard_executor()
            .unwrap()
            .block_on(async {
                writer
                    .put("a/b", PutBody::from_bytes("bytes"), &PutOptions::default())
                    .await
                    .unwrap();
                reader.get_range("a/b", 0..u64::MAX).await.unwrap()
            });

        assert_eq!(read.bytes, "bytes");
        assert!(matches!(
            store(&config, &SharedStore::default()),
            Err(ServerError::TieredStorage { .. })
        ));
    }

    #[test]
    fn given_no_backend_when_probing_should_accept() {
        assert!(probe(&TieredStorageConfig::default(), &SharedStore::default()).unwrap());
    }

    #[test]
    fn given_fs_backend_with_a_directory_when_probing_should_accept() {
        let directory = tempfile::tempdir().unwrap();

        assert!(probe(&fs_config(directory.path()), &SharedStore::default()).unwrap());
    }

    #[test]
    fn given_claimed_fs_root_when_probing_should_pass_the_conditional_create_and_boot() {
        let directory = tempfile::tempdir().unwrap();
        let config = fs_config(directory.path());
        let identity = StoreIdentity { cluster_uid: 7 };
        server_common::create_shard_executor()
            .unwrap()
            .block_on(async {
                let store = FsStore::new(directory.path());
                assert!(
                    identity
                        .claim(&store, &layout(&config).unwrap())
                        .await
                        .unwrap()
                );
            });

        assert!(probe(&config, &SharedStore::default()).unwrap());
    }

    #[test]
    fn given_store_that_ignores_conditional_creates_when_probing_should_refuse_the_boot() {
        let layout = KeyLayout::new("iggy").unwrap();
        let identity = StoreIdentity { cluster_uid: 7 };
        let ignoring = IgnoresCondition(MemoryStore::new());
        let honoring = MemoryStore::new();

        let (refused, passed) = server_common::create_shard_executor()
            .unwrap()
            .block_on(async {
                identity.claim(&ignoring, &layout).await.unwrap();
                identity.claim(&honoring, &layout).await.unwrap();
                let timeout = Duration::from_secs(5);
                (
                    probe_conditional_create(
                        &ignoring,
                        &layout,
                        identity,
                        timeout,
                        TieredStorageBackend::S3,
                    )
                    .await,
                    probe_conditional_create(
                        &honoring,
                        &layout,
                        identity,
                        timeout,
                        TieredStorageBackend::S3,
                    )
                    .await,
                )
            });

        let error = refused.unwrap_err();
        assert!(
            matches!(
                error,
                ServerError::TieredStorageConditionalCreate {
                    source: ObjectStoreError::Unsupported(_),
                    ..
                }
            ),
            "{error}"
        );
        assert!(
            error.to_string().contains("tiered_storage.endpoint"),
            "{error}"
        );
        assert!(passed.is_ok());
    }

    /// A file that no node wrote fails the store identity check, but it
    /// must not stop the boot of every node on the store.
    #[test]
    fn given_hand_written_identity_file_when_probing_should_boot() {
        let directory = tempfile::tempdir().unwrap();
        let identity = directory.path().join("iggy/meta/identity");
        std::fs::create_dir_all(identity.parent().unwrap()).unwrap();
        std::fs::write(
            &identity,
            "iggy-store-identity 1\ncluster_uid 0123456789abcdef0011223344556677\n",
        )
        .unwrap();

        assert!(probe(&fs_config(directory.path()), &SharedStore::default()).unwrap());
    }

    #[test]
    fn given_fs_backend_without_its_directory_when_probing_should_refuse_the_boot() {
        let directory = tempfile::tempdir().unwrap();

        let error = probe(
            &fs_config(&directory.path().join("missing")),
            &SharedStore::default(),
        )
        .unwrap_err();

        assert!(
            matches!(
                error,
                ServerError::TieredStorageProbe {
                    source: ObjectStoreError::BucketNotFound(_),
                    ..
                }
            ),
            "{error}"
        );
    }

    #[test]
    fn given_probe_answers_when_judging_should_refuse_only_a_missing_bucket_or_credentials() {
        let key = || "iggy/meta/identity".to_owned();
        let verdict = |error| probe_verdict(key(), Some(Err(error)));

        assert!(!probe_verdict(key(), None).unwrap(), "no answer in time");
        for found in [IdentityRead::Missing, IdentityRead::Invalid] {
            assert!(probe_verdict(key(), Some(Ok(found))).unwrap());
        }
        for outage in [
            ObjectStoreError::Transport("reset".to_owned()),
            ObjectStoreError::Throttled("slow down".to_owned()),
            ObjectStoreError::Unavailable("503".to_owned()),
        ] {
            assert!(!verdict(outage).unwrap());
        }
        for answer in [
            ObjectStoreError::Malformed("the file has no object header".to_owned()),
            ObjectStoreError::Archived { key: key() },
            ObjectStoreError::InvalidRequest("refused".to_owned()),
        ] {
            assert!(verdict(answer).unwrap());
        }
        for refusal in [
            ObjectStoreError::BucketNotFound("segments".to_owned()),
            ObjectStoreError::Auth("denied".to_owned()),
        ] {
            assert!(matches!(
                verdict(refusal),
                Err(ServerError::TieredStorageProbe { .. })
            ));
        }
    }

    /// Credentials come from `crate::s3_credentials`, after the preflight.
    #[test]
    fn given_s3_backend_without_static_keys_when_preflighting_should_accept() {
        assert!(preflight(&s3_section("")).is_ok());
    }

    #[test]
    fn given_s3_backend_without_credentials_when_probing_should_refuse_the_boot() {
        let error = probe(
            &s3_section("http://localhost:4566"),
            &SharedStore::default(),
        )
        .unwrap_err();

        assert!(
            matches!(error, ServerError::TieredStorage { .. }),
            "{error}"
        );
    }

    #[test]
    fn given_s3_sections_when_mapping_should_pick_the_endpoint_and_url_style() {
        let s3_config = |config: &TieredStorageConfig| S3Location::from(config).config(keys());
        let aws = s3_config(&s3_section(""));
        let custom = s3_config(&s3_section("http://localhost:4566"));
        let forced = s3_config(&TieredStorageConfig {
            url_style: S3UrlStyle::Path,
            ..s3_section("")
        });

        assert_eq!(aws.endpoint, "https://s3.eu-central-1.amazonaws.com");
        assert_eq!(aws.url_style, UrlStyle::VirtualHosted);
        assert_eq!(custom.endpoint, "http://localhost:4566");
        assert_eq!(custom.url_style, UrlStyle::Path);
        assert_eq!(forced.url_style, UrlStyle::Path);
        assert_eq!(
            aws.credentials
                .current()
                .map(|keys| keys.access_key_id.clone())
                .as_deref(),
            Some("AKIDEXAMPLE")
        );
    }

    #[test]
    fn given_s3_endpoint_that_cannot_address_a_bucket_when_probing_should_refuse_the_boot() {
        let error = probe(&s3_section("http://store/with-a-path"), &keys().into()).unwrap_err();

        assert!(
            matches!(error, ServerError::TieredStorage { .. }),
            "{error}"
        );
    }

    /// A store that refuses the connection is an outage, which never stops
    /// the boot.
    #[test]
    fn given_s3_store_that_does_not_answer_when_probing_should_accept() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        assert!(
            !probe(&s3_section(&endpoint), &keys().into()).unwrap(),
            "the store did not answer"
        );
    }

    fn gcs_section(endpoint: &str) -> TieredStorageConfig {
        TieredStorageConfig {
            backend: TieredStorageBackend::Gcs,
            gcs: GcsSection {
                bucket: "segments".to_owned(),
                endpoint: endpoint.to_owned(),
                ..GcsSection::default()
            },
            ..TieredStorageConfig::default()
        }
    }

    #[test]
    fn given_gcs_sections_when_mapping_should_default_to_google_and_keep_the_auth() {
        let google = gcs_config(&gcs_section(" ").gcs, GcsAuth::Anonymous);
        let emulator = gcs_config(
            &gcs_section("http://localhost:4443").gcs,
            GcsAuth::Anonymous,
        );

        assert_eq!(google.endpoint, "https://storage.googleapis.com");
        assert_eq!(google.bucket, "segments");
        assert!(matches!(google.auth, GcsAuth::Anonymous));
        assert_eq!(emulator.endpoint, "http://localhost:4443");
        assert_eq!(
            store_location(&gcs_section("")),
            "https://storage.googleapis.com/segments"
        );
    }

    #[test]
    fn given_gcs_backend_when_building_stores_should_need_its_credentials() {
        let config = gcs_section("http://localhost:4443");

        let built = store(&config, &GcsAuth::Anonymous.into()).unwrap();
        let without = store(&config, &SharedStore::default());

        assert!(matches!(built, Some(Backend::Gcs(_))));
        assert!(
            matches!(&without, Err(ServerError::TieredStorage { reason }) if reason.contains("gcs")),
            "{:?}",
            without.err()
        );
    }

    #[test]
    fn given_gcs_endpoint_that_cannot_address_a_bucket_when_probing_should_refuse_the_boot() {
        let error = probe(
            &gcs_section("http://store/with-a-path"),
            &GcsAuth::Anonymous.into(),
        )
        .unwrap_err();

        assert!(
            matches!(error, ServerError::TieredStorage { .. }),
            "{error}"
        );
    }

    /// A store that refuses the connection is an outage, which never stops
    /// the boot.
    #[test]
    fn given_gcs_store_that_does_not_answer_when_probing_should_accept() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        assert!(
            !probe(&gcs_section(&endpoint), &GcsAuth::Anonymous.into()).unwrap(),
            "the store did not answer"
        );
    }

    fn s3_section(endpoint: &str) -> TieredStorageConfig {
        TieredStorageConfig {
            backend: TieredStorageBackend::S3,
            bucket: "segments".to_owned(),
            region: "eu-central-1".to_owned(),
            endpoint: endpoint.to_owned(),
            ..TieredStorageConfig::default()
        }
    }

    fn keys() -> SharedCredentials {
        iggy_object_store::s3::Credentials {
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: None,
        }
        .into()
    }
}
