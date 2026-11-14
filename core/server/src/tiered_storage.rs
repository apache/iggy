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
//! files and connections of the shard that owns it.

use std::rc::Rc;
use std::time::Duration;

use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use iggy_common::IggyByteSize;
use iggy_object_store::{
    ErrorClass, FsStore, IdentityRead, KeyLayout, ObjectStoreError, StoreIdentity,
};
use message_bus::IggyMessageBus;
use partitions::{ExecutorDeleter, ExecutorUploader};
use shard::tier_service::{ClassLimits, StoreCallPolicy, TierService, TierServiceConfig};
use shard::{Sender, channel};
use tracing::{info, warn};

use crate::server_error::ServerError;
use crate::shell::ServerShard;
use crate::store_identity::{self, IdentityChecker, TierHandlers};

/// The longest the boot probe, or one store identity check, waits for the
/// store. A store that does not answer is an outage that the node waits out,
/// so neither waits for the whole request timeout.
const IDENTITY_TIMEOUT: Duration = Duration::from_secs(5);

/// Boot-time checks of the section, after `Validatable::validate`. A
/// configuration that can never work refuses the boot here, before any
/// shard starts.
pub fn preflight(config: &TieredStorageConfig) -> Result<(), ServerError> {
    match config.backend {
        TieredStorageBackend::None => return Ok(()),
        TieredStorageBackend::S3 => {
            return Err(ServerError::TieredStorage {
                reason: "the s3 backend is not implemented yet".to_owned(),
            });
        }
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
/// Returns whether the store answered in time. Only a missing bucket or
/// rejected credentials stop the boot. The store identity check judges any
/// other answer, and a store that does not answer is an outage that the
/// node waits out.
///
/// # Errors
/// [`ServerError::TieredStorageProbe`] when the bucket is missing or the
/// store refuses the credentials. [`ServerError::TieredStorageProbeRuntime`]
/// when the runtime for the read cannot start.
pub fn probe(config: &TieredStorageConfig) -> Result<bool, ServerError> {
    let Some(store) = fs_store(config) else {
        return Ok(true);
    };
    let layout = layout(config)?;
    let runtime = server_common::create_shard_executor().map_err(|source| {
        ServerError::TieredStorageProbeRuntime {
            source: server_common::diagnostics::enrich_runtime_create_error(source),
        }
    })?;
    let read = runtime.block_on(compio::time::timeout(
        identity_timeout(config),
        StoreIdentity::read(&store, &layout),
    ));
    probe_verdict(layout.identity_key(), read.ok())
}

/// Give `shard` a tier service over the configured store, and run the store
/// identity check of [`crate::store_identity`] next to it. The shard gets
/// its segment uploader and object deleter only while its node passes the
/// check. Shard 0 runs the check itself, and the other shards follow it.
///
/// Returns the stop signal of the check's task, or `None` without a backend.
pub fn start(
    config: &TieredStorageConfig,
    bus: &Rc<IggyMessageBus>,
    shard: &Rc<ServerShard>,
    replica_id: u8,
) -> Option<Sender<()>> {
    let store = fs_store(config)?;
    // `preflight` refused the boot already when this fails.
    let layout = layout(config).ok()?;
    let service = TierService::new(store, Rc::clone(bus), &service_config(config));
    let handlers = TierHandlers::new(
        Rc::new(ExecutorUploader::new(service.clone(), layout.clone())),
        Rc::new(ExecutorDeleter::new(service, layout.clone())),
    );
    // The check gets a store of its own, outside the service, so no queued
    // job and no retry delays what it finds.
    let checker = if shard.id == 0 {
        fs_store(config).map(|store| {
            IdentityChecker::new(store, layout, replica_id == 0, identity_timeout(config))
        })
    } else {
        None
    };
    // Every shard starts a service, and one line per node is enough.
    if shard.id == 0 {
        info!(
            backend = %config.backend,
            path = %config.path,
            prefix = %config.prefix,
            "tier service started; uploads and object deletes wait for the store identity \
             check, and tiering stays off, as this version cannot read evicted segments back"
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

/// The configured store, or `None` without a backend that the server can
/// run.
fn fs_store(config: &TieredStorageConfig) -> Option<FsStore> {
    match config.backend {
        TieredStorageBackend::Fs => Some(FsStore::new(&config.path)),
        TieredStorageBackend::None | TieredStorageBackend::S3 => None,
    }
}

fn identity_timeout(config: &TieredStorageConfig) -> Duration {
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

    use super::*;

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
    fn given_no_backend_when_probing_should_accept() {
        assert!(probe(&TieredStorageConfig::default()).unwrap());
    }

    #[test]
    fn given_fs_backend_with_a_directory_when_probing_should_accept() {
        let directory = tempfile::tempdir().unwrap();

        assert!(probe(&fs_config(directory.path())).unwrap());
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

        assert!(probe(&fs_config(directory.path())).unwrap());
    }

    #[test]
    fn given_fs_backend_without_its_directory_when_probing_should_refuse_the_boot() {
        let directory = tempfile::tempdir().unwrap();

        let error = probe(&fs_config(&directory.path().join("missing"))).unwrap_err();

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

    #[test]
    fn given_s3_backend_when_preflighting_should_refuse_the_boot() {
        let config = TieredStorageConfig {
            backend: TieredStorageBackend::S3,
            bucket: "segments".to_owned(),
            region: "eu-central-1".to_owned(),
            ..TieredStorageConfig::default()
        };

        let error = preflight(&config).unwrap_err();

        assert!(
            matches!(error, ServerError::TieredStorage { .. }),
            "{error}"
        );
    }
}
