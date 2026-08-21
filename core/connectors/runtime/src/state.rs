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

mod http;

use crate::configs::runtime::{StateConfig, StateStorageKind};
use crate::error::RuntimeError;
use iggy_connector_sdk::{ConnectorState, Error};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use strum::Display;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

pub use http::{HttpStateFactory, HttpStateProvider};

pub trait StateProvider {
    async fn load(&self) -> Result<Option<ConnectorState>, Error>;
    async fn save(&self, state: ConnectorState) -> Result<(), Error>;
}

#[non_exhaustive]
#[derive(Debug, Display)]
pub enum StateStorage {
    #[strum(to_string = "file")]
    File(FileStateProvider),
    #[strum(to_string = "http")]
    Http(HttpStateProvider),
}

impl StateStorage {
    pub async fn load(&self) -> Result<Option<ConnectorState>, Error> {
        match self {
            StateStorage::File(provider) => provider.load().await,
            StateStorage::Http(provider) => provider.load().await,
        }
    }

    pub async fn save(&self, state: ConnectorState) -> Result<(), Error> {
        match self {
            StateStorage::File(provider) => provider.save(state).await,
            StateStorage::Http(provider) => provider.save(state).await,
        }
    }
}

/// Builds the configured [`StateStorageFactory`]. The only backend-selection
/// point in the runtime: a bad backend configuration fails here, at boot.
pub fn factory_from_config(
    config: &StateConfig,
) -> Result<Arc<dyn StateStorageFactory>, RuntimeError> {
    match config.storage {
        StateStorageKind::File => {
            std::fs::create_dir_all(&config.path).map_err(|create_error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Failed to create state directory '{}': {create_error}",
                    config.path
                ))
            })?;
            info!("State will be stored in: {}", config.path);
            Ok(Arc::new(FileStateFactory::new(config.path.clone())))
        }
        StateStorageKind::Http => {
            let factory = HttpStateFactory::new(&config.http)?;
            info!("State will be stored via HTTP at: {}", config.http.url);
            Ok(Arc::new(factory))
        }
    }
}

/// Builds the per-connector [`StateStorage`]. Resolved once at startup and
/// shared via the runtime context, so API-driven restarts get their storage
/// from the same place as the initial start. Fallible because a storage
/// backend may have per-connector prerequisites, even though the file backend
/// never fails.
pub trait StateStorageFactory: Send + Sync {
    fn storage_for(&self, connector_key: &str) -> Result<StateStorage, RuntimeError>;
}

#[derive(Debug)]
pub struct FileStateFactory {
    path: String,
}

impl FileStateFactory {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

impl StateStorageFactory for FileStateFactory {
    fn storage_for(&self, connector_key: &str) -> Result<StateStorage, RuntimeError> {
        let path = format!("{}/source_{connector_key}.state", self.path);
        Ok(StateStorage::File(FileStateProvider::new(path)))
    }
}

#[derive(Debug)]
pub struct FileStateProvider {
    path: PathBuf,
    tmp_path: PathBuf,
    save_lock: Mutex<()>,
}

impl FileStateProvider {
    pub fn new(path: String) -> Self {
        let path = PathBuf::from(path);
        debug_assert!(
            path.file_name().is_some(),
            "state path must end in a file name, got {}",
            path.display()
        );
        let tmp_path = tmp_path_for(&path);
        FileStateProvider {
            path,
            tmp_path,
            save_lock: Mutex::new(()),
        }
    }

    /// Writes + fdatasyncs the tmp file, cleaning it up on its own failures.
    async fn write_tmp(&self, bytes: &[u8]) -> Result<(), Error> {
        let result = self.write_tmp_inner(bytes).await;
        if result.is_err() {
            cleanup_tmp(&self.tmp_path).await;
        }
        result
    }

    async fn write_tmp_inner(&self, bytes: &[u8]) -> Result<(), Error> {
        let mut tmp = open_options_for_state()
            .open(&self.tmp_path)
            .await
            .map_err(|error| {
                error!(
                    "Cannot create temp state file: {}. {error}.",
                    self.tmp_path.display()
                );
                Error::CannotWriteStateFile
            })?;
        tmp.write_all(bytes).await.map_err(|error| {
            error!(
                "Cannot write temp state file: {}. {error}.",
                self.tmp_path.display()
            );
            Error::CannotWriteStateFile
        })?;
        // fdatasync: skip mtime/atime sync. Parent dir is synced separately.
        tmp.sync_data().await.map_err(|error| {
            error!(
                "Cannot sync temp state file: {}. {error}.",
                self.tmp_path.display()
            );
            Error::CannotWriteStateFile
        })
    }
}

impl StateProvider for FileStateProvider {
    async fn load(&self) -> Result<Option<ConnectorState>, Error> {
        // Orphan tmps are reclaimed by the next save's truncate; deleting
        // here would race a still-draining save across provider instances.
        match fs::read(&self.path).await {
            Ok(buffer) if buffer.is_empty() => {
                info!("State file is empty: {}", self.path.display());
                Ok(None)
            }
            Ok(buffer) => {
                info!("Loaded state file: {}", self.path.display());
                Ok(Some(ConnectorState(buffer)))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                // Missing parent dir = broken config, not a fresh start.
                if let Some(parent) = self.path.parent()
                    && !parent.as_os_str().is_empty()
                    && fs::metadata(parent).await.is_err()
                {
                    error!("State file parent directory missing: {}", parent.display());
                    return Err(Error::CannotOpenStateFile);
                }
                info!(
                    "State file not found, starting fresh: {}",
                    self.path.display()
                );
                Ok(None)
            }
            Err(error) => {
                error!("Cannot read state file: {}. {error}.", self.path.display());
                Err(Error::CannotReadStateFile)
            }
        }
    }

    async fn save(&self, state: ConnectorState) -> Result<(), Error> {
        let _guard = self.save_lock.lock().await;

        self.write_tmp(&state.0).await?;

        if let Err(error) = fs::rename(&self.tmp_path, &self.path).await {
            error!(
                "Cannot rename {} -> {}. {error}.",
                self.tmp_path.display(),
                self.path.display()
            );
            cleanup_tmp(&self.tmp_path).await;
            return Err(Error::CannotWriteStateFile);
        }

        // Make the rename durable. A failure here means the new dentry may not
        // survive a power loss, so surface it rather than silently succeeding.
        sync_parent_dir(&self.path).await?;

        debug!("Saved state file: {}", self.path.display());
        Ok(())
    }
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

fn open_options_for_state() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600); // owner-only: state may carry cursors / tokens
    options
}

async fn cleanup_tmp(tmp_path: &Path) {
    if let Err(error) = fs::remove_file(tmp_path).await
        && error.kind() != ErrorKind::NotFound
    {
        warn!(
            "Failed to remove stale temp state file: {}. {error}.",
            tmp_path.display()
        );
    }
}

#[cfg(unix)]
async fn sync_parent_dir(path: &Path) -> Result<(), Error> {
    let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) else {
        return Ok(());
    };
    let dir = fs::File::open(parent).await.map_err(|error| {
        error!(
            "Failed to open state directory for fsync: {}. {error}.",
            parent.display()
        );
        Error::CannotWriteStateFile
    })?;
    dir.sync_all().await.map_err(|error| {
        error!(
            "Failed to fsync state directory: {}. {error}.",
            parent.display()
        );
        Error::CannotWriteStateFile
    })
}

#[cfg(not(unix))]
async fn sync_parent_dir(_path: &Path) -> Result<(), Error> {
    // Directory fsync is POSIX-only.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn provider_in(dir: &TempDir, name: &str) -> FileStateProvider {
        FileStateProvider::new(dir.path().join(name).to_string_lossy().to_string())
    }

    #[tokio::test]
    async fn given_file_factory_when_storage_built_should_round_trip_state() {
        let dir = TempDir::new().unwrap();
        let factory = FileStateFactory::new(dir.path().to_string_lossy().to_string());
        let storage = factory.storage_for("test").unwrap();
        storage.save(ConnectorState(vec![1, 2, 3])).await.unwrap();
        let loaded = storage.load().await.unwrap().unwrap();
        assert_eq!(loaded.0, vec![1, 2, 3]);
        assert!(dir.path().join("source_test.state").is_file());
    }

    #[tokio::test]
    async fn given_no_existing_file_when_loaded_should_return_none() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        assert!(provider.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_missing_parent_dir_when_loaded_should_error() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "missing_subdir/source_test.state");
        let result = provider.load().await;
        assert!(
            matches!(result, Err(Error::CannotOpenStateFile)),
            "expected CannotOpenStateFile, got {result:?}"
        );
    }

    #[tokio::test]
    async fn given_empty_file_when_loaded_should_return_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("source_test.state");
        fs::write(&path, b"").await.unwrap();
        let provider = FileStateProvider::new(path.to_string_lossy().to_string());
        assert!(provider.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_state_when_saved_then_loaded_should_round_trip() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        provider
            .save(ConnectorState(vec![1, 2, 3, 4, 5]))
            .await
            .unwrap();
        let loaded = provider.load().await.unwrap().unwrap();
        assert_eq!(loaded.0, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn given_multiple_saves_when_loaded_should_return_latest_bytes() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        provider.save(ConnectorState(vec![0; 16])).await.unwrap();
        provider.save(ConnectorState(vec![9; 4])).await.unwrap();
        let loaded = provider.load().await.unwrap().unwrap();
        assert_eq!(loaded.0, vec![9; 4]);
    }

    #[tokio::test]
    async fn given_successful_save_when_inspected_should_leave_no_tmp_artifact() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        provider.save(ConnectorState(vec![42])).await.unwrap();
        assert!(!provider.tmp_path.exists());
        assert!(provider.path.exists());
    }

    #[tokio::test]
    async fn given_orphaned_tmp_when_loaded_should_ignore_it() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        // Simulate a prior-crash leftover (real state + orphan tmp). load()
        // must read the real file and never confuse it with the tmp.
        fs::write(&provider.path, vec![1, 2, 3]).await.unwrap();
        fs::write(&provider.tmp_path, vec![9, 9, 9]).await.unwrap();
        let loaded = provider.load().await.unwrap().unwrap();
        assert_eq!(loaded.0, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn given_stale_tmp_when_save_called_should_overwrite_and_succeed() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        fs::write(&provider.tmp_path, vec![0xFF; 32]).await.unwrap();
        provider.save(ConnectorState(vec![1])).await.unwrap();
        let loaded = provider.load().await.unwrap().unwrap();
        assert_eq!(loaded.0, vec![1]);
        assert!(!provider.tmp_path.exists());
    }

    #[tokio::test]
    async fn given_state_saved_when_provider_recreated_should_resume_from_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("source_test.state");
        let path_str = path.to_string_lossy().to_string();
        FileStateProvider::new(path_str.clone())
            .save(ConnectorState(vec![7, 7, 7]))
            .await
            .unwrap();
        let resumed = FileStateProvider::new(path_str)
            .load()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.0, vec![7, 7, 7]);
    }

    #[tokio::test]
    async fn given_concurrent_saves_when_completed_should_yield_untorn_state() {
        use std::sync::Arc;
        let dir = TempDir::new().unwrap();
        let provider = Arc::new(provider_in(&dir, "source_test.state"));
        let mut handles = Vec::new();
        for byte in 0u8..16 {
            let provider = provider.clone();
            handles.push(tokio::spawn(async move {
                provider.save(ConnectorState(vec![byte; 64])).await
            }));
        }
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        let loaded = provider.load().await.unwrap().unwrap();
        assert_eq!(loaded.0.len(), 64);
        assert!(loaded.0.iter().all(|byte| *byte == loaded.0[0]));
        assert!(!provider.tmp_path.exists());
    }

    #[tokio::test]
    async fn given_empty_state_when_saved_then_loaded_should_return_none() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        provider.save(ConnectorState(Vec::new())).await.unwrap();
        assert!(provider.load().await.unwrap().is_none());
    }

    #[test]
    fn given_path_with_filename_when_tmp_path_built_should_append_tmp_suffix() {
        let p = PathBuf::from("/tmp/state/source_x.state");
        assert_eq!(
            tmp_path_for(&p),
            PathBuf::from("/tmp/state/source_x.state.tmp")
        );
    }

    #[test]
    fn given_path_without_filename_when_tmp_path_built_should_yield_bare_tmp() {
        let p = PathBuf::from("/");
        assert_eq!(tmp_path_for(&p).file_name().unwrap(), ".tmp");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn given_saved_state_when_inspected_on_unix_should_have_mode_0o600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        provider.save(ConnectorState(vec![1, 2, 3])).await.unwrap();
        let mode = std::fs::metadata(&provider.path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "state file should be owner-only");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn given_unreadable_file_when_loaded_should_return_read_error() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        provider.save(ConnectorState(vec![7])).await.unwrap();
        let mut perms = std::fs::metadata(&provider.path).unwrap().permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&provider.path, perms.clone()).unwrap();
        let result = provider.load().await;
        // restore so TempDir can clean up
        perms.set_mode(0o600);
        std::fs::set_permissions(&provider.path, perms).unwrap();
        assert!(
            matches!(result, Err(Error::CannotReadStateFile)),
            "expected CannotReadStateFile, got {result:?}"
        );
    }

    #[test]
    fn given_default_config_when_factory_built_should_use_file_backend() {
        let dir = TempDir::new().unwrap();
        let config = StateConfig {
            path: dir.path().join("state").to_string_lossy().to_string(),
            ..StateConfig::default()
        };
        let factory = factory_from_config(&config).expect("file factory should build");
        assert!(
            matches!(factory.storage_for("test"), Ok(StateStorage::File(_))),
            "default storage must remain the file backend"
        );
        assert!(
            dir.path().join("state").is_dir(),
            "the file arm still creates the state directory at boot"
        );
    }

    #[test]
    fn given_http_storage_without_url_when_factory_built_should_fail() {
        let config = StateConfig {
            storage: StateStorageKind::Http,
            ..StateConfig::default()
        };
        let result = factory_from_config(&config);
        assert!(
            matches!(result, Err(RuntimeError::InvalidConfiguration(ref message)) if message.contains("state.http.url")),
            "http storage without a url must fail boot"
        );
    }

    #[test]
    fn given_http_storage_with_url_when_factory_built_should_use_http_backend() {
        let mut config = StateConfig {
            storage: StateStorageKind::Http,
            ..StateConfig::default()
        };
        config.http.url = "http://localhost:1/state".to_string();
        let factory = factory_from_config(&config).expect("http factory should build");
        assert!(matches!(
            factory.storage_for("test"),
            Ok(StateStorage::Http(_))
        ));
    }

    #[tokio::test]
    async fn given_target_path_is_directory_when_save_called_should_fail() {
        let dir = TempDir::new().unwrap();
        let provider = provider_in(&dir, "source_test.state");
        // Pre-create the target path as a non-empty directory so rename fails.
        std::fs::create_dir(&provider.path).unwrap();
        std::fs::write(provider.path.join("blocker"), b"x").unwrap();
        let result = provider.save(ConnectorState(vec![1])).await;
        assert!(
            matches!(result, Err(Error::CannotWriteStateFile)),
            "expected CannotWriteStateFile, got {result:?}"
        );
        assert!(!provider.tmp_path.exists(), "tmp should be cleaned up");
    }
}
