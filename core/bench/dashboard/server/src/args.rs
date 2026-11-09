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

use crate::error::IggyBenchDashboardServerError;
use clap::{CommandFactory, Parser, error::ErrorKind};
use serde::Deserialize;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

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

    /// Directory for cached PNG files and artifact zips
    #[arg(long, default_value_os_t = default_cache_dir())]
    pub cache_dir: PathBuf,

    /// Dashboard configuration file: official suites, headline workloads, and thresholds
    #[arg(long, default_value = "dashboard.toml")]
    pub config: PathBuf,
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
}
