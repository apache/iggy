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
use std::io;
use std::path::Path;
use tempfile::NamedTempFile;
use walkdir::WalkDir;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

/// Makes `zip_path` a zip of every file under `run_dir`. Returns `true` when it built the
/// archive and `false` when the cached one is still current.
///
/// The cached zip is current while no entry under `run_dir` has an mtime at or after the zip's
/// mtime. Entries include directories, so a removed file also triggers a rebuild.
pub fn refresh_zip(run_dir: &Path, zip_path: &Path) -> io::Result<bool> {
    if is_current(run_dir, zip_path)? {
        return Ok(false);
    }
    build_zip(run_dir, zip_path)?;
    Ok(true)
}

fn is_current(run_dir: &Path, zip_path: &Path) -> io::Result<bool> {
    let zip_mtime = match fs::metadata(zip_path) {
        Ok(metadata) => metadata.modified()?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    for entry in WalkDir::new(run_dir) {
        if entry?.metadata()?.modified()? >= zip_mtime {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The archive goes to a temp file that is renamed into place, so readers and concurrent
/// builders never see a partial zip. The zip gets the mtime of the build start, so a file
/// that changes during the build makes the next refresh rebuild.
fn build_zip(run_dir: &Path, zip_path: &Path) -> io::Result<()> {
    let zip_dir = zip_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("'{}' has no parent directory", zip_path.display()),
        )
    })?;
    fs::create_dir_all(zip_dir)?;
    let temp_file = NamedTempFile::new_in(zip_dir)?;
    // Taken from the file system and not from SystemTime::now(): file mtimes come from a
    // coarser clock, and the freshness check must compare values from the same clock.
    let build_started = temp_file.as_file().metadata()?.modified()?;

    let mut zip = ZipWriter::new(temp_file);
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .unix_permissions(0o755);
    for entry in WalkDir::new(run_dir) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relative_path = entry
            .path()
            .strip_prefix(run_dir)
            .map_err(|e| io::Error::other(format!("Error creating relative path: {e}")))?;
        zip.start_file(relative_path.to_string_lossy(), options)?;
        io::copy(&mut File::open(entry.path())?, &mut zip)?;
    }

    let temp_file = zip.finish()?;
    temp_file.as_file().set_modified(build_started)?;
    temp_file.as_file().sync_all()?;
    temp_file.persist(zip_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};
    use zip::ZipArchive;

    const AGE: Duration = Duration::from_secs(60);

    /// Creates a run directory whose entries are all dated in the past, so the first zip is newer
    /// than every entry, however coarse the file system clock is.
    fn old_run_dir(root: &Path) -> PathBuf {
        let run_dir = root.join("run");
        fs::create_dir_all(run_dir.join("logs")).expect("create run dir");
        fs::write(run_dir.join("report.json"), "v1").expect("write report");
        fs::write(run_dir.join("logs").join("server.log"), "log").expect("write log");
        let past = SystemTime::now() - AGE;
        for entry in WalkDir::new(&run_dir) {
            let path = entry.expect("walk run dir").into_path();
            File::open(&path)
                .and_then(|file| file.set_modified(past))
                .expect("set mtime in the past");
        }
        run_dir
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
        let run_dir = old_run_dir(root.path());
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
        let run_dir = old_run_dir(root.path());
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
        let run_dir = old_run_dir(root.path());
        let zip_path = root.path().join("cache").join("run.zip");
        assert!(refresh_zip(&run_dir, &zip_path).expect("first refresh"));

        fs::remove_file(run_dir.join("logs").join("server.log")).expect("remove log");

        assert!(refresh_zip(&run_dir, &zip_path).expect("refresh after removal"));
        assert_eq!(zip_entries(&zip_path), vec![entry("report.json", "v1")]);
    }
}
