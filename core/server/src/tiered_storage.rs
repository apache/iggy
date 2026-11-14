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
//! before the shards start, and the tier service that each shard builds.
//!
//! Each shard gets its own store and service, because a provider holds
//! files and connections of the shard that owns it.

use std::rc::Rc;

use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use iggy_common::IggyByteSize;
use iggy_object_store::{FsStore, KeyLayout};
use message_bus::IggyMessageBus;
use partitions::ExecutorUploader;
use shard::tier_service::{ClassLimits, StoreCallPolicy, TierService, TierServiceConfig};
use tracing::info;

use crate::server_error::ServerError;
use crate::shell::ServerShard;

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
    KeyLayout::new(&config.prefix).map_err(|source| ServerError::TieredStoragePrefix {
        prefix: config.prefix.clone(),
        source,
    })?;
    Ok(())
}

/// Give `shard` a tier service over the configured store, and send its
/// segment uploads through it. Does nothing without a backend.
///
/// An upload starts only for a tiered topic, and no topic turns tiered
/// before every node confirms that the store's prefix belongs to this
/// cluster. Object deletes are not handed over here, because the delete of a
/// removed incarnation runs for any topic, and it must wait for that same
/// confirmation.
pub fn start(config: &TieredStorageConfig, bus: &Rc<IggyMessageBus>, shard: &ServerShard) {
    let store = match config.backend {
        TieredStorageBackend::Fs => FsStore::new(&config.path),
        TieredStorageBackend::None | TieredStorageBackend::S3 => return,
    };
    let Ok(layout) = KeyLayout::new(&config.prefix) else {
        // `preflight` refused the boot already.
        return;
    };
    let service = TierService::new(store, Rc::clone(bus), &service_config(config));
    shard.set_segment_uploader(Some(Rc::new(ExecutorUploader::new(service, layout))));
    // Every shard starts a service, and one line per node is enough.
    if shard.id == 0 {
        info!(
            backend = %config.backend,
            path = %config.path,
            prefix = %config.prefix,
            "tier service started; tiering stays off, as this version cannot check that \
             the store prefix belongs to this cluster"
        );
    }
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
