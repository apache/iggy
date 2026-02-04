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

use crate::harness::error::TestBinaryError;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::thread;
use uuid::Uuid;

const TEST_CLEANUP_DISABLED_ENV_VAR: &str = "IGGY_TEST_CLEANUP_DISABLED";

/// Global registry mapping test names to their log directories.
static TEST_DIRECTORIES: Lazy<RwLock<HashMap<String, PathBuf>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get the log directory for a test by name.
pub fn get_test_directory(test_name: &str) -> Option<PathBuf> {
    TEST_DIRECTORIES
        .read()
        .ok()
        .and_then(|map| map.get(test_name).cloned())
}

fn register_test_directory(test_name: &str, path: &Path) {
    if let Ok(mut map) = TEST_DIRECTORIES.write() {
        map.insert(test_name.to_string(), path.to_path_buf());
    }
}

fn is_cleanup_disabled_by_env() -> bool {
    std::env::var(TEST_CLEANUP_DISABLED_ENV_VAR)
        .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1"))
        .unwrap_or(false)
}

static TEST_LOGS_DIR: Lazy<PathBuf> = Lazy::new(|| {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("Failed to find workspace root")
        .join("test_logs")
});

/// Owns the test directory and provides paths for all binaries within a test.
pub struct TestContext {
    test_name: String,
    base_dir: PathBuf,
    cleanup: bool,
    created: bool,
}

impl TestContext {
    pub fn new(test_name: Option<String>, cleanup: bool) -> Result<Self, TestBinaryError> {
        let test_name = test_name.unwrap_or_else(Self::derive_test_name);
        let uuid_suffix = Uuid::new_v4().to_string()[..8].to_string();
        let dir_name = format!("{}_{}", sanitize_path(&test_name), uuid_suffix);

        let base_dir = (*TEST_LOGS_DIR).join(dir_name);

        Ok(Self {
            test_name,
            base_dir,
            cleanup,
            created: false,
        })
    }

    /// Creates the test directory structure. Called lazily on first access.
    pub fn ensure_created(&mut self) -> Result<(), TestBinaryError> {
        if self.created {
            return Ok(());
        }

        fs::create_dir_all(&self.base_dir).map_err(|e| TestBinaryError::FileSystemError {
            path: self.base_dir.clone(),
            source: e,
        })?;

        register_test_directory(&self.test_name, &self.base_dir);
        self.created = true;
        Ok(())
    }

    fn derive_test_name() -> String {
        thread::current()
            .name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string())
    }

    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn mcp_stdout_path(&self, server_id: u32) -> PathBuf {
        self.base_dir.join(format!("mcp_{server_id}_stdout.log"))
    }

    pub fn mcp_stderr_path(&self, server_id: u32) -> PathBuf {
        self.base_dir.join(format!("mcp_{server_id}_stderr.log"))
    }

    pub fn connectors_runtime_state_path(&self, server_id: u32) -> PathBuf {
        self.base_dir
            .join(format!("connectors_runtime_{server_id}_state"))
    }

    pub fn connectors_runtime_stdout_path(&self, server_id: u32) -> PathBuf {
        self.base_dir
            .join(format!("connectors_runtime_{server_id}_stdout.log"))
    }

    pub fn connectors_runtime_stderr_path(&self, server_id: u32) -> PathBuf {
        self.base_dir
            .join(format!("connectors_runtime_{server_id}_stderr.log"))
    }

    pub fn test_stdout_path(&self) -> PathBuf {
        self.base_dir.join("test_stdout.log")
    }

    pub fn cleanup(&self) {
        if !self.cleanup || is_cleanup_disabled_by_env() {
            return;
        }
        if self.base_dir.exists() {
            let _ = fs::remove_dir_all(&self.base_dir);
        }
    }

    pub fn set_cleanup(&mut self, cleanup: bool) {
        self.cleanup = cleanup;
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if !thread::panicking() {
            self.cleanup();
        }
    }
}

fn sanitize_path(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_path() {
        assert_eq!(sanitize_path("test::foo::bar"), "test__foo__bar");
        assert_eq!(sanitize_path("my/test"), "my_test");
        assert_eq!(sanitize_path("test<>name"), "test__name");
    }

    #[test]
    fn test_context_paths() {
        let ctx = TestContext::new(Some("test_context_paths".to_string()), true).unwrap();
        assert!(ctx.mcp_stdout_path(0).ends_with("mcp_0_stdout.log"));
        assert!(
            ctx.connectors_runtime_state_path(0)
                .ends_with("connectors_runtime_0_state")
        );
        assert!(ctx.mcp_stdout_path(1).ends_with("mcp_1_stdout.log"));
    }
}
