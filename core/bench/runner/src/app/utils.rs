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

use anyhow::{Context, Result};
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::fs;

/// Retrieves the full path to the `performance_results` directory within a `TempDir`.
pub async fn _get_performance_results_directory(tempdir: &TempDir) -> Result<String> {
    let temp_path = tempdir.path();

    let mut entries = fs::read_dir(temp_path)
        .await
        .with_context(|| format!("Failed to read directory: {}", temp_path.display()))?;

    let mut subdirs: Vec<PathBuf> = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            subdirs.push(path);
        }
    }

    if subdirs.len() != 1 {
        anyhow::bail!(
            "Expected exactly one subdirectory in '{}', found {}.",
            temp_path.display(),
            subdirs.len()
        );
    }

    let sole_subdir = &subdirs[0];
    let perf_results_dir = sole_subdir.join("performance_results");

    let metadata = fs::metadata(&perf_results_dir).await.with_context(|| {
        format!(
            "Failed to access '{}'. It may not exist.",
            perf_results_dir.display()
        )
    })?;

    if !metadata.is_dir() {
        anyhow::bail!(
            "'performance_results' exists at '{}', but it is not a directory.",
            perf_results_dir.display()
        );
    }

    let perf_results_str = perf_results_dir
        .to_str()
        .with_context(|| format!("Path '{}' is not valid UTF-8.", perf_results_dir.display()))?
        .to_owned();

    Ok(perf_results_str)
}
