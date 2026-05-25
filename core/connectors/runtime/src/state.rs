/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use iggy_connector_sdk::{ConnectorState, Error};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use strum::Display;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

pub trait StateProvider {
    async fn load(&self) -> Result<Option<ConnectorState>, Error>;
    async fn save(&self, state: ConnectorState) -> Result<(), Error>;
}

#[non_exhaustive]
#[derive(Debug, Display)]
pub enum StateStorage {
    #[strum(to_string = "file")]
    File(FileStateProvider),
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
        let tmp_path = tmp_path_for(&path);
        FileStateProvider {
            path,
            tmp_path,
            save_lock: Mutex::new(()),
        }
    }
}

impl StateProvider for FileStateProvider {
    async fn load(&self) -> Result<Option<ConnectorState>, Error> {
        // Orphan tmps are reclaimed by the next save's truncate; deleting
        // here would race a still-draining save across provider instances.
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
            && fs::metadata(parent).await.is_err()
        {
            error!("State file parent directory missing: {}", parent.display());
            return Err(Error::CannotOpenStateFile);
        }
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

        if let Err(error) = tmp.write_all(&state.0).await {
            error!(
                "Cannot write temp state file: {}. {error}.",
                self.tmp_path.display()
            );
            drop(tmp);
            cleanup_tmp(&self.tmp_path).await;
            return Err(Error::CannotWriteStateFile);
        }

        // fdatasync: skip mtime/atime sync. Parent dir is synced separately.
        if let Err(error) = tmp.sync_data().await {
            error!(
                "Cannot sync temp state file: {}. {error}.",
                self.tmp_path.display()
            );
            drop(tmp);
            cleanup_tmp(&self.tmp_path).await;
            return Err(Error::CannotWriteStateFile);
        }
        drop(tmp);

        if let Err(error) = fs::rename(&self.tmp_path, &self.path).await {
            error!(
                "Cannot rename {} -> {}. {error}.",
                self.tmp_path.display(),
                self.path.display()
            );
            cleanup_tmp(&self.tmp_path).await;
            return Err(Error::CannotWriteStateFile);
        }

        sync_parent_dir(&self.path).await;

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
async fn sync_parent_dir(path: &Path) {
    let Some(parent) = path.parent() else {
        return;
    };
    match fs::File::open(parent).await {
        Ok(dir) => {
            if let Err(error) = dir.sync_all().await {
                warn!(
                    "Failed to fsync state directory: {}. {error}.",
                    parent.display()
                );
            }
        }
        Err(error) => {
            warn!(
                "Failed to open state directory for fsync: {}. {error}.",
                parent.display()
            );
        }
    }
}

#[cfg(not(unix))]
async fn sync_parent_dir(_path: &Path) {
    // Directory fsync is POSIX-only.
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn provider_in(dir: &TempDir, name: &str) -> FileStateProvider {
        FileStateProvider::new(dir.path().join(name).to_string_lossy().to_string())
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
