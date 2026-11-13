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

//! Cached zip archives of benchmark run directories.

use std::fs::{self, File};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use tempfile::NamedTempFile;
use walkdir::{DirEntry, WalkDir};
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

const FINGERPRINT_PREFIX: &str = "iggy-bench-dashboard run fingerprint ";

/// Makes `zip_path` a zip of every file under `run_dir`. Returns `true` when it built the
/// archive and `false` when the cached one is still current.
///
/// The zip comment holds a fingerprint of `run_dir`, and the cached zip is current while that
/// fingerprint still matches. The fingerprint only uses values read from the results file
/// system, so it never compares them with times from the cache file system, which can run on
/// another clock.
pub fn refresh_zip(run_dir: &Path, zip_path: &Path) -> io::Result<bool> {
    let fingerprint = fingerprint(run_dir)?;
    if has_fingerprint(zip_path, &fingerprint)? {
        return Ok(false);
    }
    build_zip(run_dir, zip_path, &fingerprint)?;
    Ok(true)
}

/// Hashes the relative path, size and times of every entry under `run_dir`. Entries include
/// directories, so an added or removed file changes the fingerprint. On unix it also hashes the
/// ctime and inode, because `rsync -a`, `cp -a` and `tar x` restore old mtimes but can't set
/// those.
fn fingerprint(run_dir: &Path) -> io::Result<String> {
    let mut hasher = DefaultHasher::new();
    for entry in WalkDir::new(run_dir).sort_by_file_name() {
        let Some(entry) = skip_vanished(entry)? else {
            continue;
        };
        let Some(metadata) = skip_vanished(entry.metadata())? else {
            continue;
        };
        relative_path(run_dir, &entry)?.hash(&mut hasher);
        metadata.len().hash(&mut hasher);
        metadata.modified()?.hash(&mut hasher);
        #[cfg(unix)]
        (metadata.ctime(), metadata.ctime_nsec(), metadata.ino()).hash(&mut hasher);
    }
    // DefaultHasher can change between Rust releases. That only makes each zip rebuild once.
    let hash = hasher.finish();
    Ok(format!("{FINGERPRINT_PREFIX}{hash:016x}"))
}

/// A zip that doesn't parse counts as stale, so a damaged cache file gets rebuilt.
fn has_fingerprint(zip_path: &Path, fingerprint: &str) -> io::Result<bool> {
    let zip_file = match File::open(zip_path) {
        Ok(zip_file) => zip_file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    Ok(ZipArchive::new(zip_file).is_ok_and(|archive| archive.comment() == fingerprint.as_bytes()))
}

/// The archive goes to a temp file that is renamed into place, so readers and concurrent
/// builders never see a partial zip. `fingerprint` is taken before the walk, so a file that
/// changes during the build makes the next refresh rebuild.
fn build_zip(run_dir: &Path, zip_path: &Path, fingerprint: &str) -> io::Result<()> {
    let zip_dir = zip_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("'{}' has no parent directory", zip_path.display()),
        )
    })?;
    fs::create_dir_all(zip_dir)?;
    let temp_file = NamedTempFile::new_in(zip_dir)?;

    let mut zip = ZipWriter::new(temp_file);
    zip.set_comment(fingerprint)?;
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .unix_permissions(0o755);
    for entry in WalkDir::new(run_dir) {
        let Some(entry) = skip_vanished(entry)? else {
            continue;
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let mut file = match File::open(entry.path()) {
            Ok(file) => file,
            Err(e) if is_vanished(entry.depth(), &e) => continue,
            Err(e) => return Err(e),
        };
        zip.start_file(relative_path(run_dir, &entry)?.to_string_lossy(), options)?;
        io::copy(&mut file, &mut zip)?;
    }

    let temp_file = zip.finish()?;
    temp_file.as_file().sync_all()?;
    temp_file.persist(zip_path)?;
    Ok(())
}

/// Drops an entry that went away between the directory read and its use. Its parent directory
/// changed with it, so the next fingerprint differs. A missing `run_dir` stays an error, so a
/// deleted run never becomes an empty zip.
fn skip_vanished<T>(result: walkdir::Result<T>) -> io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(e) if e.io_error().is_some_and(|io| is_vanished(e.depth(), io)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn is_vanished(depth: usize, error: &io::Error) -> bool {
    depth > 0 && error.kind() == io::ErrorKind::NotFound
}

fn relative_path<'a>(run_dir: &Path, entry: &'a DirEntry) -> io::Result<&'a Path> {
    entry
        .path()
        .strip_prefix(run_dir)
        .map_err(|e| io::Error::other(format!("Error creating relative path: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    const AGE: Duration = Duration::from_secs(60);

    /// Creates a run directory whose entries all have `mtime`.
    fn dated_run_dir(root: &Path, mtime: SystemTime) -> PathBuf {
        let run_dir = root.join("run");
        fs::create_dir_all(run_dir.join("logs")).expect("create run dir");
        fs::write(run_dir.join("report.json"), "v1").expect("write report");
        fs::write(run_dir.join("logs").join("server.log"), "log").expect("write log");
        for entry in WalkDir::new(&run_dir) {
            set_mtime(entry.expect("walk run dir").path(), mtime);
        }
        run_dir
    }

    fn past() -> SystemTime {
        SystemTime::now() - AGE
    }

    fn set_mtime(path: &Path, mtime: SystemTime) {
        File::open(path)
            .and_then(|file| file.set_modified(mtime))
            .expect("set mtime");
    }

    fn zip_entries(zip_path: &Path) -> Vec<(String, String)> {
        let mut archive =
            ZipArchive::new(File::open(zip_path).expect("open zip")).expect("read zip");
        let mut entries = Vec::with_capacity(archive.len());
        for index in 0..archive.len() {
            let mut file = archive.by_index(index).expect("read zip entry");
            let name = file.name().expect("read zip entry name").into_owned();
            let mut content = String::new();
            file.read_to_string(&mut content)
                .expect("read zip entry content");
            entries.push((name, content));
        }
        entries.sort();
        entries
    }

    fn mtime(path: &Path) -> SystemTime {
        fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .expect("read mtime")
    }

    fn entry(name: &str, content: &str) -> (String, String) {
        (name.to_string(), content.to_string())
    }

    #[test]
    fn given_unchanged_run_dir_when_refreshing_twice_should_build_once() {
        let root = tempfile::tempdir().expect("create temp dir");
        let run_dir = dated_run_dir(root.path(), past());
        let zip_path = root.path().join("cache").join("run.zip");

        assert!(refresh_zip(&run_dir, &zip_path).expect("first refresh"));
        let built = mtime(&zip_path);
        assert!(!refresh_zip(&run_dir, &zip_path).expect("second refresh"));

        assert_eq!(mtime(&zip_path), built);
        assert_eq!(
            zip_entries(&zip_path),
            vec![entry("logs/server.log", "log"), entry("report.json", "v1")]
        );
    }

    #[test]
    fn given_rewritten_report_when_refreshing_should_rebuild_with_new_content() {
        let root = tempfile::tempdir().expect("create temp dir");
        let run_dir = dated_run_dir(root.path(), past());
        let zip_path = root.path().join("cache").join("run.zip");
        assert!(refresh_zip(&run_dir, &zip_path).expect("first refresh"));

        fs::write(run_dir.join("report.json"), "v2").expect("rewrite report");

        assert!(refresh_zip(&run_dir, &zip_path).expect("refresh after rewrite"));
        assert_eq!(
            zip_entries(&zip_path),
            vec![entry("logs/server.log", "log"), entry("report.json", "v2")]
        );
    }

    #[test]
    fn given_removed_file_when_refreshing_should_rebuild_without_it() {
        let root = tempfile::tempdir().expect("create temp dir");
        let run_dir = dated_run_dir(root.path(), past());
        let zip_path = root.path().join("cache").join("run.zip");
        assert!(refresh_zip(&run_dir, &zip_path).expect("first refresh"));

        fs::remove_file(run_dir.join("logs").join("server.log")).expect("remove log");

        assert!(refresh_zip(&run_dir, &zip_path).expect("refresh after removal"));
        assert_eq!(zip_entries(&zip_path), vec![entry("report.json", "v1")]);
    }

    #[cfg(unix)]
    #[test]
    fn given_report_replaced_with_restored_mtimes_when_refreshing_should_rebuild_with_new_content()
    {
        let root = tempfile::tempdir().expect("create temp dir");
        let old_mtime = past();
        let run_dir = dated_run_dir(root.path(), old_mtime);
        let zip_path = root.path().join("cache").join("run.zip");
        assert!(refresh_zip(&run_dir, &zip_path).expect("first refresh"));

        // Same size and mtime as the old report, swapped in by a rename as `rsync -a` does.
        let staged_report = run_dir.join(".report.json.staged");
        fs::write(&staged_report, "v2").expect("stage report");
        set_mtime(&staged_report, old_mtime);
        fs::rename(&staged_report, run_dir.join("report.json")).expect("replace report");
        set_mtime(&run_dir, old_mtime);

        assert!(refresh_zip(&run_dir, &zip_path).expect("refresh after replacement"));
        assert_eq!(
            zip_entries(&zip_path),
            vec![entry("logs/server.log", "log"), entry("report.json", "v2")]
        );
    }

    #[test]
    fn given_file_added_with_restored_mtimes_when_refreshing_should_rebuild_with_it() {
        let root = tempfile::tempdir().expect("create temp dir");
        let old_mtime = past();
        let run_dir = dated_run_dir(root.path(), old_mtime);
        let zip_path = root.path().join("cache").join("run.zip");
        assert!(refresh_zip(&run_dir, &zip_path).expect("first refresh"));

        let client_log = run_dir.join("logs").join("client.log");
        fs::write(&client_log, "client").expect("write client log");
        set_mtime(&client_log, old_mtime);
        set_mtime(&run_dir.join("logs"), old_mtime);

        assert!(refresh_zip(&run_dir, &zip_path).expect("refresh after addition"));
        assert_eq!(
            zip_entries(&zip_path),
            vec![
                entry("logs/client.log", "client"),
                entry("logs/server.log", "log"),
                entry("report.json", "v1")
            ]
        );
    }

    #[test]
    fn given_entries_dated_in_the_future_when_refreshing_twice_should_build_once() {
        let root = tempfile::tempdir().expect("create temp dir");
        let run_dir = dated_run_dir(root.path(), SystemTime::now() + AGE);
        let zip_path = root.path().join("cache").join("run.zip");

        assert!(refresh_zip(&run_dir, &zip_path).expect("first refresh"));
        assert!(!refresh_zip(&run_dir, &zip_path).expect("second refresh"));
    }

    #[test]
    fn given_corrupt_cached_zip_when_refreshing_should_rebuild_it() {
        let root = tempfile::tempdir().expect("create temp dir");
        let run_dir = dated_run_dir(root.path(), past());
        let zip_path = root.path().join("cache").join("run.zip");
        fs::create_dir_all(root.path().join("cache")).expect("create cache dir");
        fs::write(&zip_path, "not a zip").expect("write corrupt zip");

        assert!(refresh_zip(&run_dir, &zip_path).expect("refresh over corrupt zip"));
        assert_eq!(
            zip_entries(&zip_path),
            vec![entry("logs/server.log", "log"), entry("report.json", "v1")]
        );
    }
}
