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

use super::{OmbCache, OmbRun, OmbRunFiles, read_file};
use crate::error::IggyBenchDashboardServerError;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::result::{OmbJsonKind, OmbResult, OmbResultError};
use bench_dashboard_shared::omb::setup::{OMB_SETUP_FILE_NAME, OmbSetup, OmbSidecar};
use bench_dashboard_shared::omb::summary::OmbRunSummary;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::{debug, info, warn};

const RESULT_EXTENSION: &str = "json";

/// Why a run directory gives no run.
#[derive(Debug, Error)]
enum SkippedRun {
    #[error("it holds no OMB result")]
    NoResult,
    #[error(
        "it holds {} OMB results ({}), but a run directory holds one, so its sidecar can't say \
         which result it describes",
        .0.len(),
        .0.join(", ")
    )]
    SeveralResults(Vec<String>),
    #[error("can't read the directory: {0}")]
    ReadDir(io::Error),
    #[error("{file_name}: {source}")]
    Unparsable {
        file_name: String,
        source: OmbResultError,
    },
}

/// A JSON file of a run directory with every required field of an OMB result.
struct FoundResult {
    path: PathBuf,
    bytes: Vec<u8>,
    modified: DateTime<Utc>,
}

/// A sidecar that parses.
struct LoadedSidecar {
    setup: OmbSetup,
    /// `None` if the server must not send the file.
    download: Option<PathBuf>,
}

impl OmbCache {
    /// Reads each direct subdirectory of the OMB results directory as one run, and swaps the runs
    /// in. It fails only if the OMB results directory can't be read. A run directory without
    /// exactly one OMB result that parses gives no run, and the log says why.
    pub fn load(&self) -> Result<(), IggyBenchDashboardServerError> {
        info!(
            "Building OMB run cache from directory {}",
            self.results_dir.display()
        );

        let mut run_dirs: Vec<PathBuf> = fs::read_dir(&self.results_dir)
            .map_err(IggyBenchDashboardServerError::Io)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .map(|entry| entry.path())
            .collect();
        // Of two directories with the same result, the first one in this order gives the run on
        // every load.
        run_dirs.sort_unstable();

        let mut runs = Vec::with_capacity(run_dirs.len());
        let mut run_dir_of: HashMap<OmbRunId, &Path> = HashMap::with_capacity(run_dirs.len());
        for run_dir in &run_dirs {
            let run = match load_run(run_dir) {
                Ok(run) => run,
                Err(reason @ SkippedRun::NoResult) => {
                    debug!("Skipping {}: {reason}", run_dir.display());
                    continue;
                }
                Err(reason) => {
                    warn!("Skipping {}: {reason}", run_dir.display());
                    continue;
                }
            };
            match run_dir_of.entry(run.summary.id) {
                Entry::Occupied(first) => info!(
                    "Skipping {}: its result is the same file as the one in {}, so the run is \
                     listed once",
                    run_dir.display(),
                    first.get().display()
                ),
                Entry::Vacant(slot) => {
                    slot.insert(run_dir);
                    runs.push(run);
                }
            }
        }

        info!("Built {} OMB run summaries", runs.len());
        self.replace_runs(runs);
        Ok(())
    }
}

fn load_run(run_dir: &Path) -> Result<OmbRun, SkippedRun> {
    let found = single_result(find_results(run_dir)?)?;
    let file_name = file_name(&found.path);
    let result =
        OmbResult::try_from(found.bytes.as_slice()).map_err(|source| SkippedRun::Unparsable {
            file_name: file_name.clone(),
            source,
        })?;
    let (setup, setup_download) = match load_sidecar(run_dir) {
        Some(sidecar) => (Some(sidecar.setup), sidecar.download),
        None => (None, None),
    };
    let summary = OmbRunSummary::new(
        OmbRunId::of_file(&found.bytes),
        &file_name,
        found.modified,
        &result,
        setup,
    );
    Ok(OmbRun {
        summary,
        files: OmbRunFiles {
            result: found.path,
            setup: setup_download,
        },
    })
}

/// Reads every JSON file of `run_dir` and keeps the OMB results, sorted by path. It skips other
/// JSON quietly, and a file that it can't read or that is not valid JSON with a warning.
fn find_results(run_dir: &Path) -> Result<Vec<FoundResult>, SkippedRun> {
    let entries = fs::read_dir(run_dir)
        .and_then(|entries| entries.collect::<io::Result<Vec<_>>>())
        .map_err(SkippedRun::ReadDir)?;

    let mut results = Vec::new();
    for entry in entries {
        let path = entry.path();
        if path.extension() != Some(OsStr::new(RESULT_EXTENSION)) {
            continue;
        }
        // This doesn't follow a symbolic link, so a link can't make the server send a file from
        // outside the OMB results directory.
        let metadata = match entry.metadata() {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => {
                debug!("Skipping '{}': not a regular file", path.display());
                continue;
            }
            Err(error) => {
                warn!("Skipping '{}': {error}", path.display());
                continue;
            }
        };
        let bytes = match read_file(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!("Skipping a JSON file: {error}");
                continue;
            }
        };
        match OmbJsonKind::of(&bytes) {
            OmbJsonKind::Result => match metadata.modified() {
                Ok(modified) => results.push(FoundResult {
                    path,
                    bytes,
                    modified: DateTime::from(modified),
                }),
                Err(error) => warn!(
                    "Skipping '{}': can't read its modification time: {error}",
                    path.display()
                ),
            },
            OmbJsonKind::Other => debug!("Skipping '{}': not an OMB result", path.display()),
            OmbJsonKind::Broken => warn!("Skipping '{}': not valid JSON", path.display()),
        }
    }
    results.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(results)
}

fn single_result(mut results: Vec<FoundResult>) -> Result<FoundResult, SkippedRun> {
    if results.len() > 1 {
        return Err(SkippedRun::SeveralResults(
            results
                .iter()
                .map(|result| file_name(&result.path))
                .collect(),
        ));
    }
    results.pop().ok_or(SkippedRun::NoResult)
}

/// The sidecar of the run in `run_dir`. `None` for a run without one. A sidecar that can't be read
/// or doesn't parse gives `None` with a warning, so the run loads without it.
fn load_sidecar(run_dir: &Path) -> Option<LoadedSidecar> {
    let path = run_dir.join(OMB_SETUP_FILE_NAME);
    let without_sidecar = |reason: &dyn std::fmt::Display| {
        warn!(
            "Loading the run in {} without its sidecar: {reason}",
            run_dir.display()
        );
    };
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            without_sidecar(&format_args!("'{}' is not a regular file", path.display()));
            return None;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
        Err(error) => {
            without_sidecar(&format_args!("can't read '{}': {error}", path.display()));
            return None;
        }
    }
    let text = match read_file(&path).map(String::from_utf8) {
        Ok(Ok(text)) => text,
        Ok(Err(_)) => {
            without_sidecar(&format_args!("'{}' is not UTF-8 text", path.display()));
            return None;
        }
        Err(error) => {
            without_sidecar(&error);
            return None;
        }
    };
    let sidecar = match text.parse::<OmbSidecar>() {
        Ok(sidecar) => sidecar,
        Err(error) => {
            without_sidecar(&error);
            return None;
        }
    };

    for field in &sidecar.unknown_fields {
        debug!("{}: ignoring the unknown field {field}", path.display());
    }
    for warning in &sidecar.warnings {
        warn!("{}: {warning}", path.display());
    }
    let holds_secrets = sidecar.setup.file_holds_secrets;
    if holds_secrets {
        warn!(
            "{}: the server doesn't send this file, because it holds keys that look like secrets",
            path.display()
        );
    }
    Some(LoadedSidecar {
        setup: sidecar.setup,
        download: (!holds_secrets).then_some(path),
    })
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::OMB_MAX_FILE_BYTES;
    use crate::omb::test_fixtures::{
        EDGE8_DEFAULT, FIXTURE_OMB_RESULTS_DIR, FixtureRun, NEWEST_FIRST, NO_CONSUMERS,
        PUBLISH_ERRORS, RC1_DEFAULT, RC1_PERSISTED,
    };
    use bench_dashboard_shared::omb::summary::OmbTimeSource;
    use serde_json::Value;
    use std::fs::File;

    const OTHER_JSON: &str = r#"{"name": "not an OMB result", "publishRate": [1.0]}"#;

    fn loaded(results_dir: &Path) -> OmbCache {
        let cache = OmbCache::new(results_dir.to_path_buf());
        cache.load().expect("the OMB results directory is readable");
        cache
    }

    fn listed_files(cache: &OmbCache) -> Vec<String> {
        cache
            .run_list()
            .runs
            .iter()
            .map(|summary| summary.file_name.clone())
            .collect()
    }

    fn file_names(runs: &[&FixtureRun]) -> Vec<String> {
        runs.iter().map(|run| run.file_name.to_owned()).collect()
    }

    #[test]
    fn given_fixture_directory_when_loading_should_list_every_run_newest_first() {
        let cache = loaded(Path::new(FIXTURE_OMB_RESULTS_DIR));

        let list = cache.run_list();

        assert!(list.enabled);
        assert_eq!(listed_files(&cache), file_names(&NEWEST_FIRST));
        for (summary, fixture) in list.runs.iter().zip(NEWEST_FIRST) {
            assert_eq!(summary.id, fixture.id(), "{}", fixture.file_name);
            assert_eq!(
                summary.setup.is_some(),
                fixture.has_sidecar,
                "{}",
                fixture.file_name
            );
            let run = cache.run(summary.id).expect("a listed run has files");
            assert_eq!(&run.summary, summary);
            assert_eq!(run.files.result, fixture.result_path());
            assert_eq!(
                run.files.setup,
                fixture.has_sidecar.then(|| fixture.sidecar_path()),
                "{}",
                fixture.file_name
            );
        }
    }

    #[test]
    fn given_fixture_runs_when_loading_should_derive_summaries_from_files_and_sidecars() {
        let cache = loaded(Path::new(FIXTURE_OMB_RESULTS_DIR));

        let rc1 = cache.run(RC1_DEFAULT.id()).expect("the run is listed");
        let no_consumers = cache.run(NO_CONSUMERS.id()).expect("the run is listed");
        let errors = cache.run(PUBLISH_ERRORS.id()).expect("the run is listed");

        assert_eq!(rc1.summary.windows, 18);
        assert_eq!(rc1.summary.publish_latency_ms.p99, 1.523);
        assert_eq!(rc1.summary.time.source, OmbTimeSource::Sidecar);
        assert_eq!(
            rc1.summary.version.map(|version| version.text).as_deref(),
            Some("0.10.0-rc.1")
        );
        assert_eq!(no_consumers.summary.time.source, OmbTimeSource::FileName);
        assert_eq!(no_consumers.summary.end_to_end_latency_ms, None);
        assert_eq!(errors.summary.publish_errors, Some(254_517));
    }

    #[test]
    fn given_runs_in_deeper_directories_when_loading_should_read_direct_subdirectories_only() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        RC1_DEFAULT.copy_into(&results_dir.path().join("group"));
        NO_CONSUMERS.copy_into(results_dir.path());
        fs::copy(
            PUBLISH_ERRORS.result_path(),
            results_dir.path().join(PUBLISH_ERRORS.file_name),
        )
        .expect("the result is copied");

        let cache = loaded(results_dir.path());

        assert_eq!(listed_files(&cache), file_names(&[&NO_CONSUMERS]));
    }

    #[test]
    fn given_directory_without_result_when_loading_should_skip_it() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        fs::remove_file(run_dir.join(RC1_DEFAULT.file_name)).expect("the result is removed");
        fs::write(run_dir.join("benchmark.log"), "log").expect("the log is written");
        fs::create_dir(results_dir.path().join("empty")).expect("the directory is created");
        NO_CONSUMERS.copy_into(results_dir.path());

        let cache = loaded(results_dir.path());

        assert_eq!(listed_files(&cache), file_names(&[&NO_CONSUMERS]));
    }

    #[test]
    fn given_directory_with_two_results_when_loading_should_skip_it() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        fs::copy(
            RC1_PERSISTED.result_path(),
            run_dir.join(RC1_PERSISTED.file_name),
        )
        .expect("the second result is copied");
        NO_CONSUMERS.copy_into(results_dir.path());

        let cache = loaded(results_dir.path());

        assert_eq!(listed_files(&cache), file_names(&[&NO_CONSUMERS]));
        assert!(cache.run(RC1_DEFAULT.id()).is_none());
        assert!(cache.run(RC1_PERSISTED.id()).is_none());
    }

    #[test]
    fn given_files_that_are_not_results_next_to_result_when_loading_should_load_the_result() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        let json = RC1_DEFAULT.json();
        fs::write(run_dir.join("other.json"), OTHER_JSON).expect("the JSON file is written");
        fs::write(run_dir.join("list.json"), "[1, 2, 3]").expect("the JSON file is written");
        fs::write(run_dir.join("cut-short.json"), &json[..json.len() / 2])
            .expect("the broken JSON file is written");
        fs::write(run_dir.join("result.json.tmp"), &json).expect("the copy is written");
        // A sparse file, so the test writes no data.
        File::create(run_dir.join("large.json"))
            .and_then(|file| file.set_len(OMB_MAX_FILE_BYTES + 1))
            .expect("the large file is created");

        let cache = loaded(results_dir.path());

        assert_eq!(listed_files(&cache), file_names(&[&RC1_DEFAULT]));
    }

    #[test]
    fn given_directory_with_json_that_is_not_a_result_when_loading_should_skip_it() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let json = RC1_DEFAULT.json();
        let other_dir = results_dir.path().join("other");
        fs::create_dir(&other_dir).expect("the directory is created");
        fs::write(other_dir.join("other.json"), OTHER_JSON).expect("the JSON file is written");
        let broken_dir = results_dir.path().join("broken");
        fs::create_dir(&broken_dir).expect("the directory is created");
        fs::write(
            broken_dir.join(RC1_DEFAULT.file_name),
            &json[..json.len() / 2],
        )
        .expect("the broken JSON file is written");

        let cache = loaded(results_dir.path());

        assert!(listed_files(&cache).is_empty());
    }

    #[test]
    fn given_result_with_a_field_of_wrong_type_when_loading_should_skip_it() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        let mut result: Value =
            serde_json::from_slice(&RC1_DEFAULT.json()).expect("the fixture is JSON");
        result["messageSize"] = Value::from("1000");
        fs::write(
            run_dir.join(RC1_DEFAULT.file_name),
            serde_json::to_vec(&result).expect("the result serializes"),
        )
        .expect("the result is written");
        NO_CONSUMERS.copy_into(results_dir.path());

        let cache = loaded(results_dir.path());

        assert_eq!(listed_files(&cache), file_names(&[&NO_CONSUMERS]));
    }

    #[test]
    fn given_broken_sidecar_when_loading_should_load_run_without_it() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        fs::write(
            run_dir.join(OMB_SETUP_FILE_NAME),
            "[environment\nlabel = 1\n",
        )
        .expect("the sidecar is overwritten");

        let cache = loaded(results_dir.path());

        let run = cache.run(RC1_DEFAULT.id()).expect("the run is listed");
        assert_eq!(run.summary.setup, None);
        assert_eq!(run.summary.version, None);
        assert_eq!(run.summary.time.source, OmbTimeSource::FileName);
        assert_eq!(run.files.setup, None);
    }

    #[test]
    fn given_sidecar_with_secret_key_when_loading_should_keep_other_values_and_withhold_file() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        fs::write(
            run_dir.join(OMB_SETUP_FILE_NAME),
            "[server]\nversion = \"0.10.0\"\n\n[properties.server]\nhuge_pages = 2048\n\
             root_password = \"do-not-publish\"\n",
        )
        .expect("the sidecar is overwritten");

        let cache = loaded(results_dir.path());

        let run = cache.run(RC1_DEFAULT.id()).expect("the run is listed");
        let setup = run.summary.setup.expect("the sidecar parses");
        assert!(setup.file_holds_secrets);
        let keys: Vec<_> = setup
            .properties()
            .map(|(_, property)| property.key.as_str())
            .collect();
        assert_eq!(keys, ["huge_pages"]);
        assert_eq!(
            run.summary.version.map(|version| version.text).as_deref(),
            Some("0.10.0")
        );
        assert_eq!(run.files.setup, None);
    }

    #[test]
    fn given_sidecar_with_secret_key_in_deeper_table_when_loading_should_withhold_file() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        fs::write(
            run_dir.join(OMB_SETUP_FILE_NAME),
            "[server]\nversion = \"0.10.0\"\n\n[properties.server.auth]\n\
             password = \"do-not-publish\"\n",
        )
        .expect("the sidecar is overwritten");

        let cache = loaded(results_dir.path());

        let run = cache.run(RC1_DEFAULT.id()).expect("the run is listed");
        let setup = run.summary.setup.expect("the sidecar parses");
        assert!(setup.file_holds_secrets);
        assert_eq!(setup.properties, []);
        assert_eq!(run.files.setup, None);
    }

    #[test]
    fn given_identical_results_in_two_directories_when_loading_should_list_one_run() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let first = RC1_DEFAULT.copy_as(results_dir.path(), "a-copy");
        RC1_DEFAULT.copy_as(results_dir.path(), "b-copy");

        let cache = loaded(results_dir.path());

        assert_eq!(listed_files(&cache), file_names(&[&RC1_DEFAULT]));
        let run = cache.run(RC1_DEFAULT.id()).expect("the run is listed");
        assert_eq!(run.files.result, first.join(RC1_DEFAULT.file_name));
    }

    #[test]
    fn given_loaded_runs_when_directory_changes_and_reloads_should_swap_them() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        RC1_DEFAULT.copy_into(results_dir.path());
        let removed = NO_CONSUMERS.copy_into(results_dir.path());
        let cache = loaded(results_dir.path());
        let before = cache.run_list();
        fs::remove_dir_all(removed).expect("the run directory is removed");
        EDGE8_DEFAULT.copy_into(results_dir.path());

        cache.load().expect("the OMB results directory is readable");

        assert_eq!(
            before
                .runs
                .iter()
                .map(|summary| summary.file_name.as_str())
                .collect::<Vec<_>>(),
            [RC1_DEFAULT.file_name, NO_CONSUMERS.file_name]
        );
        assert_eq!(
            listed_files(&cache),
            file_names(&[&RC1_DEFAULT, &EDGE8_DEFAULT])
        );
        assert!(cache.run(NO_CONSUMERS.id()).is_none());
        assert!(cache.files(EDGE8_DEFAULT.id()).is_some());
    }

    #[test]
    fn given_loaded_runs_when_reload_fails_should_keep_serving_them() {
        let root = tempfile::tempdir().expect("a temporary directory is created");
        let results_dir = root.path().join("omb");
        RC1_DEFAULT.copy_into(&results_dir);
        let cache = loaded(&results_dir);
        fs::remove_dir_all(&results_dir).expect("the OMB results directory is removed");

        assert!(cache.load().is_err());

        assert_eq!(listed_files(&cache), file_names(&[&RC1_DEFAULT]));
        assert!(cache.run(RC1_DEFAULT.id()).is_some());
    }

    #[cfg(unix)]
    #[test]
    fn given_symbolic_links_when_loading_should_skip_them() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let linked_run = results_dir.path().join("linked-run");
        fs::create_dir(&linked_run).expect("the run directory is created");
        std::os::unix::fs::symlink(
            RC1_DEFAULT.result_path(),
            linked_run.join(RC1_DEFAULT.file_name),
        )
        .expect("the result link is created");
        let real_run = NO_CONSUMERS.copy_into(results_dir.path());
        std::os::unix::fs::symlink(
            RC1_DEFAULT.sidecar_path(),
            real_run.join(OMB_SETUP_FILE_NAME),
        )
        .expect("the sidecar link is created");
        std::os::unix::fs::symlink(
            EDGE8_DEFAULT
                .result_path()
                .parent()
                .expect("a run directory"),
            results_dir.path().join("linked-directory"),
        )
        .expect("the directory link is created");

        let cache = loaded(results_dir.path());

        assert_eq!(listed_files(&cache), file_names(&[&NO_CONSUMERS]));
        let run = cache.run(NO_CONSUMERS.id()).expect("the run is listed");
        assert_eq!(run.summary.setup, None);
        assert_eq!(run.files.setup, None);
    }
}
