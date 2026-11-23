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

use crate::embed_cache::DEFAULT_MAX_GIB;
use crate::error::IggyBenchDashboardServerError;
use crate::public_url::PublicUrl;
use clap::{CommandFactory, Parser, error::ErrorKind};
use serde::Deserialize;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

const CACHE_DIR_NAME: &str = "iggy-bench-dashboard";

#[derive(Debug, Deserialize, Parser)]
#[command(author, version, about, long_about = None)]
pub struct IggyBenchDashboardServerArgs {
    /// Server host address
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Server port
    #[arg(long, default_value_t = 8061)]
    pub port: u16,

    /// Directory containing performance results
    #[arg(long, default_value = "./performance_results")]
    pub results_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Allowed CORS origins (comma-separated)
    #[arg(long, default_value = "*")]
    pub cors_origins: String,

    /// Directory for cached PNG files, embed pages and artifact zips
    #[arg(long, default_value_os_t = default_cache_dir())]
    pub cache_dir: PathBuf,

    /// Size limit of the cached PNG files and embed pages, in GiB. Past it, the server deletes the
    /// files that readers used least recently
    #[arg(
        long,
        value_name = "GIB",
        default_value_t = DEFAULT_MAX_GIB,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub embed_cache_max_gib: u64,

    /// Dashboard configuration file: official suites, headline workloads, and thresholds
    #[arg(long, default_value = "dashboard.toml")]
    pub config: PathBuf,

    /// Directory containing OpenMessaging Benchmark (OMB) results, one run per subdirectory
    #[arg(long, value_name = "DIR")]
    pub omb_results_dir: Option<PathBuf>,

    /// Address at which readers open the dashboard, for example
    /// https://benchmarks.iggy.apache.org. robots.txt and sitemap.xml use it for absolute links.
    /// Without it, robots.txt names no sitemap, and /sitemap.xml answers 404
    #[arg(long, value_name = "URL")]
    pub public_url: Option<PublicUrl>,
}

/// An `--omb-results-dir` that the server can't use.
#[derive(Debug, Error)]
enum OmbResultsDirError {
    #[error("OMB results directory does not exist: {}", .0.display())]
    Missing(PathBuf),
    #[error("OMB results path is not a directory: {}", .0.display())]
    NotDirectory(PathBuf),
    #[error("Can't resolve '{}': {source}", path.display())]
    Resolve { path: PathBuf, source: io::Error },
    #[error(
        "--omb-results-dir '{}' is the same directory as --results-dir. Put the OMB results in \
         their own directory",
        .0.display()
    )]
    SameAsResults(PathBuf),
    #[error(
        "--omb-results-dir '{}' is inside --results-dir '{}'. Put the OMB results outside the \
         results directory, so that each directory holds the runs of one framework",
        omb_results_dir.display(),
        results_dir.display()
    )]
    InsideResults {
        omb_results_dir: PathBuf,
        results_dir: PathBuf,
    },
    #[error(
        "--results-dir '{}' is inside --omb-results-dir '{}'. Put the results directory outside \
         the OMB results directory, so that each directory holds the runs of one framework",
        results_dir.display(),
        omb_results_dir.display()
    )]
    HoldsResults {
        omb_results_dir: PathBuf,
        results_dir: PathBuf,
    },
    #[error(
        "--cache-dir '{}' is inside --omb-results-dir '{}'. Use a cache directory outside the \
         OMB results directory, because the server must not write into it",
        cache_dir.display(),
        omb_results_dir.display()
    )]
    HoldsCache {
        omb_results_dir: PathBuf,
        cache_dir: PathBuf,
    },
}

impl IggyBenchDashboardServerArgs {
    pub fn parse() -> Self {
        Self::parse_from(std::env::args())
    }

    pub fn validate(&self) {
        let mut cmd = IggyBenchDashboardServerArgs::command();
        if !self.results_dir.exists() {
            cmd.error(
                ErrorKind::InvalidValue,
                format!(
                    "Results directory does not exist: {}",
                    self.results_dir.display()
                ),
            )
            .exit();
        }
        if !self.results_dir.is_dir() {
            cmd.error(
                ErrorKind::InvalidValue,
                format!(
                    "Results path is not a directory: {}",
                    self.results_dir.display()
                ),
            )
            .exit();
        }
        // Before the cache dir exists, so a rejected cache dir never adds a directory to the
        // OMB results.
        if let Some(omb_results_dir) = &self.omb_results_dir
            && let Err(e) =
                check_omb_results_dir(omb_results_dir, &self.results_dir, &self.cache_dir)
        {
            cmd.error(ErrorKind::InvalidValue, e.to_string()).exit();
        }
        if let Err(e) = prepare_cache_dir(&self.results_dir, &self.cache_dir) {
            cmd.error(ErrorKind::InvalidValue, e.to_string()).exit();
        }
    }

    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn cors_origins_list(&self) -> Vec<String> {
        self.cors_origins
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }
}

fn default_cache_dir() -> PathBuf {
    std::env::temp_dir().join(CACHE_DIR_NAME)
}

/// Creates `cache_dir` once it's known not to be `results_dir` or inside it. The check runs before
/// anything is created, so a rejected cache dir never adds a directory to the results.
fn prepare_cache_dir(
    results_dir: &Path,
    cache_dir: &Path,
) -> Result<(), IggyBenchDashboardServerError> {
    let results_dir = results_dir
        .canonicalize()
        .map_err(|e| path_error(e, "resolve", results_dir))?;
    let resolved_cache_dir =
        resolve_path(cache_dir).map_err(|e| path_error(e, "resolve", cache_dir))?;
    if resolved_cache_dir.starts_with(&results_dir) {
        return Err(IggyBenchDashboardServerError::InvalidPath(format!(
            "--cache-dir '{}' is inside --results-dir '{}'. Use a cache directory outside the \
             results directory, because the server must not write into it",
            resolved_cache_dir.display(),
            results_dir.display()
        )));
    }
    fs::create_dir_all(cache_dir).map_err(|e| path_error(e, "create", cache_dir))?;
    Ok(())
}

/// Checks that `omb_results_dir` is a directory of its own: not `results_dir`, not inside it, and
/// not around it. The loaders read one run per direct subdirectory, so nested directories would
/// mix the runs of both. It also checks that `cache_dir` is outside it.
fn check_omb_results_dir(
    omb_results_dir: &Path,
    results_dir: &Path,
    cache_dir: &Path,
) -> Result<(), OmbResultsDirError> {
    if !omb_results_dir.exists() {
        return Err(OmbResultsDirError::Missing(omb_results_dir.to_path_buf()));
    }
    if !omb_results_dir.is_dir() {
        return Err(OmbResultsDirError::NotDirectory(
            omb_results_dir.to_path_buf(),
        ));
    }
    let resolve_error = |path: &Path| {
        let path = path.to_path_buf();
        move |source| OmbResultsDirError::Resolve { path, source }
    };
    let omb_results_dir = omb_results_dir
        .canonicalize()
        .map_err(resolve_error(omb_results_dir))?;
    let results_dir = results_dir
        .canonicalize()
        .map_err(resolve_error(results_dir))?;
    let cache_dir = resolve_path(cache_dir).map_err(resolve_error(cache_dir))?;

    if omb_results_dir == results_dir {
        return Err(OmbResultsDirError::SameAsResults(omb_results_dir));
    }
    if omb_results_dir.starts_with(&results_dir) {
        return Err(OmbResultsDirError::InsideResults {
            omb_results_dir,
            results_dir,
        });
    }
    if results_dir.starts_with(&omb_results_dir) {
        return Err(OmbResultsDirError::HoldsResults {
            omb_results_dir,
            results_dir,
        });
    }
    if cache_dir.starts_with(&omb_results_dir) {
        return Err(OmbResultsDirError::HoldsCache {
            omb_results_dir,
            cache_dir,
        });
    }
    Ok(())
}

/// Resolves `path` like `canonicalize`, even when some of its components don't exist yet. A missing
/// component counts as a plain directory, which is what `create_dir_all` turns it into.
fn resolve_path(path: &Path) -> io::Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in std::path::absolute(path)?.components() {
        if component == Component::ParentDir {
            resolved.pop();
        } else {
            resolved.push(component);
        }
        if let Ok(canonical) = resolved.canonicalize() {
            resolved = canonical;
        }
    }
    Ok(resolved)
}

fn path_error(error: io::Error, action: &str, path: &Path) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("Can't {action} '{}': {error}", path.display()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_results_dir() -> (TempDir, PathBuf) {
        let root = tempfile::tempdir().expect("create temp dir");
        let results_dir = root.path().join("results");
        fs::create_dir(&results_dir).expect("create results dir");
        (root, results_dir)
    }

    fn is_inside_results_error(result: &Result<(), IggyBenchDashboardServerError>) -> bool {
        matches!(result, Err(IggyBenchDashboardServerError::InvalidPath(_)))
    }

    #[test]
    fn given_cache_dir_inside_results_dir_when_preparing_should_fail_without_creating_it() {
        let (_root, results_dir) = temp_results_dir();
        let cache_dir = results_dir.join("cache");

        let result = prepare_cache_dir(&results_dir, &cache_dir);

        assert!(is_inside_results_error(&result), "got {result:?}");
        assert!(!cache_dir.exists());
    }

    #[test]
    fn given_cache_dir_equal_to_results_dir_when_preparing_should_fail() {
        let (_root, results_dir) = temp_results_dir();

        let result = prepare_cache_dir(&results_dir, &results_dir);

        assert!(is_inside_results_error(&result), "got {result:?}");
    }

    #[test]
    fn given_cache_dir_next_to_results_dir_when_preparing_should_create_it() {
        let (root, results_dir) = temp_results_dir();
        // Shares the text prefix "results" with the results dir, but no path component.
        let cache_dir = root.path().join("results-cache").join("nested");

        let result = prepare_cache_dir(&results_dir, &cache_dir);

        assert!(result.is_ok(), "got {result:?}");
        assert!(cache_dir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn given_symlink_into_results_dir_when_preparing_should_fail_without_creating_it() {
        let (root, results_dir) = temp_results_dir();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&results_dir, &link).expect("create symlink");

        let result = prepare_cache_dir(&results_dir, &link.join("cache"));

        assert!(is_inside_results_error(&result), "got {result:?}");
        assert!(!results_dir.join("cache").exists());
    }

    #[test]
    fn given_parent_dir_steps_into_results_dir_when_preparing_should_fail_without_creating_it() {
        let (root, results_dir) = temp_results_dir();
        let cache_dir = root
            .path()
            .join("missing")
            .join("..")
            .join("results")
            .join("cache");

        let result = prepare_cache_dir(&results_dir, &cache_dir);

        assert!(is_inside_results_error(&result), "got {result:?}");
        assert!(!results_dir.join("cache").exists());
        assert!(!root.path().join("missing").exists());
    }

    /// A root with `results`, `omb` and the `cache` path, which doesn't exist yet.
    struct OmbDirs {
        root: TempDir,
        results_dir: PathBuf,
        omb_results_dir: PathBuf,
        cache_dir: PathBuf,
    }

    fn omb_dirs() -> OmbDirs {
        let (root, results_dir) = temp_results_dir();
        let omb_results_dir = root.path().join("omb");
        fs::create_dir(&omb_results_dir).expect("create OMB results dir");
        let cache_dir = root.path().join("cache");
        OmbDirs {
            root,
            results_dir,
            omb_results_dir,
            cache_dir,
        }
    }

    #[test]
    fn given_separate_omb_results_dir_when_checking_should_accept_it() {
        let dirs = omb_dirs();

        let result =
            check_omb_results_dir(&dirs.omb_results_dir, &dirs.results_dir, &dirs.cache_dir);

        assert!(result.is_ok(), "got {result:?}");
    }

    #[test]
    fn given_missing_omb_results_dir_when_checking_should_fail() {
        let dirs = omb_dirs();
        let missing = dirs.root.path().join("missing");

        let result = check_omb_results_dir(&missing, &dirs.results_dir, &dirs.cache_dir);

        assert!(
            matches!(result, Err(OmbResultsDirError::Missing(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn given_omb_results_path_that_is_a_file_when_checking_should_fail() {
        let dirs = omb_dirs();
        let file = dirs.root.path().join("omb.json");
        fs::write(&file, "{}").expect("write file");

        let result = check_omb_results_dir(&file, &dirs.results_dir, &dirs.cache_dir);

        assert!(
            matches!(result, Err(OmbResultsDirError::NotDirectory(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn given_omb_results_dir_equal_to_results_dir_when_checking_should_fail() {
        let dirs = omb_dirs();
        // The same directory, written another way.
        let same = dirs.root.path().join("omb").join("..").join("results");

        let result = check_omb_results_dir(&same, &dirs.results_dir, &dirs.cache_dir);

        assert!(
            matches!(result, Err(OmbResultsDirError::SameAsResults(_))),
            "got {result:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn given_symlink_to_results_dir_as_omb_results_dir_when_checking_should_fail() {
        let dirs = omb_dirs();
        let link = dirs.root.path().join("link");
        std::os::unix::fs::symlink(&dirs.results_dir, &link).expect("create symlink");

        let result = check_omb_results_dir(&link, &dirs.results_dir, &dirs.cache_dir);

        assert!(
            matches!(result, Err(OmbResultsDirError::SameAsResults(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn given_omb_results_dir_inside_results_dir_when_checking_should_fail() {
        let dirs = omb_dirs();
        let inside = dirs.results_dir.join("omb");
        fs::create_dir(&inside).expect("create OMB results dir");

        let result = check_omb_results_dir(&inside, &dirs.results_dir, &dirs.cache_dir);

        assert!(
            matches!(result, Err(OmbResultsDirError::InsideResults { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn given_results_dir_inside_omb_results_dir_when_checking_should_fail() {
        let dirs = omb_dirs();
        let inside = dirs.omb_results_dir.join("results");
        fs::create_dir(&inside).expect("create results dir");

        let result = check_omb_results_dir(&dirs.omb_results_dir, &inside, &dirs.cache_dir);

        assert!(
            matches!(result, Err(OmbResultsDirError::HoldsResults { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn given_cache_dir_inside_omb_results_dir_when_checking_should_fail_without_creating_it() {
        let dirs = omb_dirs();
        let cache_dir = dirs.omb_results_dir.join("cache");

        let result = check_omb_results_dir(&dirs.omb_results_dir, &dirs.results_dir, &cache_dir);

        assert!(
            matches!(result, Err(OmbResultsDirError::HoldsCache { .. })),
            "got {result:?}"
        );
        assert!(!cache_dir.exists());
    }

    #[test]
    fn given_omb_results_dir_option_when_parsing_should_be_optional() {
        let without = IggyBenchDashboardServerArgs::parse_from(["server"]);
        let with = IggyBenchDashboardServerArgs::parse_from(["server", "--omb-results-dir", "omb"]);

        assert_eq!(without.omb_results_dir, None);
        assert_eq!(with.omb_results_dir, Some(PathBuf::from("omb")));
    }
}
