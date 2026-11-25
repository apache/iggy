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

//! OpenMessaging Benchmark (OMB) runs from `--omb-results-dir`. They have their own cache, watcher
//! and endpoints, so they never mix with the iggy-bench runs.

pub mod embed;
mod loader;
#[cfg(test)]
pub(crate) mod test_fixtures;

use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::setup::omb_setup_download_name;
use bench_dashboard_shared::omb::summary::{OmbRunList, OmbRunSummary};
#[cfg(unix)]
use nix::fcntl::{AT_FDCWD, OFlag, openat};
#[cfg(unix)]
use nix::sys::stat::Mode;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use thiserror::Error;

/// The loader and the endpoints read no file larger than this. A result of 18 windows has about
/// 100 KB, so only a wrong file reaches the limit.
pub const OMB_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct OmbCache {
    /// The runs of the last load that read the OMB results directory. A load builds the next runs
    /// aside and swaps them in whole, as `BenchmarkCache` does, so a request during a reload, or
    /// after a failed one, reads the previous runs and their list together.
    runs: RwLock<Arc<OmbRuns>>,

    /// One run per direct subdirectory
    results_dir: PathBuf,
}

/// One run as the cache holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct OmbRun {
    pub summary: OmbRunSummary,
    pub files: OmbRunFiles,
}

/// The files of one run that the endpoints serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OmbRunFiles {
    /// The result file that OMB wrote.
    pub result: PathBuf,
    /// The setup sidecar. `None` for a run without one, for a sidecar that doesn't parse, and for
    /// a sidecar with keys that look like secrets, which the server never sends.
    pub setup: Option<PathBuf>,
}

/// A file that the loader or an endpoint can't read whole.
#[derive(Debug, Error)]
pub enum OmbFileError {
    #[error("Can't read '{}': {source}", path.display())]
    Read { path: PathBuf, source: io::Error },
    #[error("'{}' has more than {OMB_MAX_FILE_BYTES} bytes", path.display())]
    TooLarge { path: PathBuf },
    #[error("'{}' is not a regular file", path.display())]
    NotRegularFile { path: PathBuf },
}

#[derive(Debug)]
struct OmbRuns {
    /// Every run, newest first. `enabled` is true, because the cache exists only with an OMB
    /// results directory.
    list: Arc<OmbRunList>,

    /// Each run id to the position of its summary in `list.runs`, and to its files
    files: HashMap<OmbRunId, (usize, OmbRunFiles)>,
}

impl OmbCache {
    pub fn new(results_dir: PathBuf) -> Self {
        Self {
            runs: RwLock::new(Arc::new(OmbRuns::new(Vec::new()))),
            results_dir,
        }
    }

    /// The answer of `GET /api/omb/runs`.
    pub fn run_list(&self) -> Arc<OmbRunList> {
        Arc::clone(&self.runs().list)
    }

    pub fn run(&self, id: OmbRunId) -> Option<OmbRun> {
        let runs = self.runs();
        runs.files.get(&id).map(|(position, files)| OmbRun {
            summary: runs.list.runs[*position].clone(),
            files: files.clone(),
        })
    }

    pub fn has_run(&self, id: OmbRunId) -> bool {
        self.runs().files.contains_key(&id)
    }

    pub fn files(&self, id: OmbRunId) -> Option<OmbRunFiles> {
        self.runs().files.get(&id).map(|(_, files)| files.clone())
    }

    /// Call it after every file read of the load, so requests keep the previous runs until then.
    fn replace_runs(&self, runs: Vec<OmbRun>) {
        *self.runs.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(OmbRuns::new(runs));
    }

    fn runs(&self) -> Arc<OmbRuns> {
        // The writer only swaps an Arc, so a poisoned lock still holds whole runs.
        Arc::clone(&self.runs.read().unwrap_or_else(PoisonError::into_inner))
    }
}

impl OmbRunFiles {
    /// The name of the result file, for the download.
    pub fn result_file_name(&self) -> String {
        self.result
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    /// The name of the setup download, which the run page's download link names too.
    pub fn setup_file_name(&self) -> String {
        omb_setup_download_name(&self.result_file_name())
    }
}

/// Reads the file at `path` whole, unless it has more than [`OMB_MAX_FILE_BYTES`]. Only a regular
/// file is read. The loader skips links, but a link can replace a run file or a run directory
/// after a load, so the file and its directory must not be links here.
pub fn read_file(path: &Path) -> Result<Vec<u8>, OmbFileError> {
    let read_error = |source| OmbFileError::Read {
        path: path.to_path_buf(),
        source,
    };
    let file = open_without_links(path).map_err(read_error)?;
    let metadata = file.metadata().map_err(read_error)?;
    if !metadata.is_file() {
        return Err(OmbFileError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    let size = metadata.len();
    if size > OMB_MAX_FILE_BYTES {
        return Err(OmbFileError::TooLarge {
            path: path.to_path_buf(),
        });
    }
    let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or_default());
    // The limit holds even for a file that grows after the size check.
    file.take(OMB_MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(read_error)?;
    if bytes.len() as u64 > OMB_MAX_FILE_BYTES {
        return Err(OmbFileError::TooLarge {
            path: path.to_path_buf(),
        });
    }
    Ok(bytes)
}

/// Opens `path` without following a link in place of the file or of its directory. With
/// `O_NONBLOCK`, the open of a FIFO doesn't wait for a writer. A regular file ignores the flag.
#[cfg(unix)]
fn open_without_links(path: &Path) -> io::Result<File> {
    let directory = path
        .parent()
        .filter(|directory| !directory.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let flags = OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
    let directory = openat(
        AT_FDCWD,
        directory,
        flags | OFlag::O_DIRECTORY,
        Mode::empty(),
    )?;
    let file = openat(&directory, name, flags | OFlag::O_NONBLOCK, Mode::empty())?;
    Ok(File::from(file))
}

#[cfg(not(unix))]
fn open_without_links(path: &Path) -> io::Result<File> {
    File::open(path)
}

impl OmbRuns {
    /// Sorts `runs` newest first, with the id as the tie break, so the order never depends on the
    /// order of the directory entries.
    fn new(mut runs: Vec<OmbRun>) -> Self {
        runs.sort_by(|left, right| {
            right
                .summary
                .time
                .instant
                .cmp(&left.summary.time.instant)
                .then_with(|| left.summary.id.cmp(&right.summary.id))
        });
        let mut summaries = Vec::with_capacity(runs.len());
        let mut files = HashMap::with_capacity(runs.len());
        for (position, run) in runs.into_iter().enumerate() {
            files.insert(run.summary.id, (position, run.files));
            summaries.push(run.summary);
        }
        Self {
            list: Arc::new(OmbRunList {
                enabled: true,
                runs: summaries,
            }),
            files,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bench_dashboard_shared::omb::setup::OMB_SETUP_FILE_NAME;
    use nix::unistd::mkfifo;
    use std::fs;

    #[test]
    fn given_file_over_size_limit_when_reading_should_fail_without_reading_it() {
        let directory = tempfile::tempdir().expect("a temporary directory is created");
        let path = directory.path().join("large.json");
        // A sparse file, so the test writes no data.
        File::create(&path)
            .and_then(|file| file.set_len(OMB_MAX_FILE_BYTES + 1))
            .expect("the large file is created");

        let result = read_file(&path);

        assert!(
            matches!(result, Err(OmbFileError::TooLarge { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn given_missing_file_when_reading_should_fail_with_not_found() {
        let directory = tempfile::tempdir().expect("a temporary directory is created");

        let result = read_file(&directory.path().join("missing.json"));

        assert!(
            matches!(&result, Err(OmbFileError::Read { source, .. }) if source.kind() == io::ErrorKind::NotFound),
            "got {result:?}"
        );
    }

    #[test]
    fn given_link_in_place_of_file_when_reading_should_fail() {
        let directory = tempfile::tempdir().expect("a temporary directory is created");
        let outside = directory.path().join("outside.toml");
        fs::write(&outside, "suite = \"outside\"\n").expect("the outside file is written");
        let run_dir = directory.path().join("run");
        fs::create_dir(&run_dir).expect("the run directory is created");
        let setup = run_dir.join(OMB_SETUP_FILE_NAME);
        std::os::unix::fs::symlink(&outside, &setup).expect("the link is created");

        let result = read_file(&setup);

        assert!(
            matches!(&result, Err(OmbFileError::Read { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn given_link_in_place_of_run_directory_when_reading_should_fail() {
        let directory = tempfile::tempdir().expect("a temporary directory is created");
        let outside = directory.path().join("outside");
        fs::create_dir(&outside).expect("the outside directory is created");
        fs::write(outside.join(OMB_SETUP_FILE_NAME), "suite = \"outside\"\n")
            .expect("the outside file is written");
        let run_dir = directory.path().join("run");
        std::os::unix::fs::symlink(&outside, &run_dir).expect("the link is created");

        let result = read_file(&run_dir.join(OMB_SETUP_FILE_NAME));

        assert!(
            matches!(&result, Err(OmbFileError::Read { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn given_fifo_when_reading_should_fail_without_waiting_for_writer() {
        let directory = tempfile::tempdir().expect("a temporary directory is created");
        let path = directory.path().join(OMB_SETUP_FILE_NAME);
        mkfifo(&path, Mode::S_IRWXU).expect("the FIFO is created");

        let result = read_file(&path);

        assert!(
            matches!(&result, Err(OmbFileError::NotRegularFile { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn given_result_file_when_naming_setup_download_should_replace_extension_with_setup_toml() {
        for (result, expected) in [
            (
                "run/12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.json",
                "12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.setup.toml",
            ),
            ("run/workload.v2.json", "workload.v2.setup.toml"),
            ("run/result", "result.setup.toml"),
            ("", OMB_SETUP_FILE_NAME),
        ] {
            let files = OmbRunFiles {
                result: PathBuf::from(result),
                setup: None,
            };

            assert_eq!(files.setup_file_name(), expected, "{result}");
        }
    }

    #[test]
    fn given_new_cache_when_listing_should_be_enabled_without_runs() {
        let cache = OmbCache::new(PathBuf::from("omb"));

        let list = cache.run_list();

        assert!(list.enabled);
        assert!(list.runs.is_empty());
    }
}
