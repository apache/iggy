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

use std::{
    collections::HashMap,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use tempfile::TempDir;
use tokio::time::sleep;

const CONFIG_DIR_ENV: &str = "IGGY_CONNECTORS_CONNECTORS_CONFIG_DIR";
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL: Duration = Duration::from_millis(50);

pub struct OpenDalSinkFixture {
    _temp_dir: TempDir,
    object_root: PathBuf,
    config_dir: PathBuf,
}

impl OpenDalSinkFixture {
    pub async fn wait_for_object(&self, offset: u64) -> Result<Vec<u8>, TestBinaryError> {
        let path = self.object_path(offset);

        for _ in 0..POLL_ATTEMPTS {
            match tokio::fs::read(&path).await {
                Ok(payload) => return Ok(payload),
                Err(error) if error.kind() == ErrorKind::NotFound => sleep(POLL_INTERVAL).await,
                Err(source) => return Err(TestBinaryError::FileSystemError { path, source }),
            }
        }

        Err(TestBinaryError::InvalidState {
            message: format!("OpenDAL object was not written: {}", path.display()),
        })
    }

    fn object_path(&self, offset: u64) -> PathBuf {
        self.object_root.join(format!(
            "iggy/messages/test_stream/test_topic/00000-{offset:020}.json"
        ))
    }
}

#[async_trait]
impl TestFixture for OpenDalSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let temp_dir = TempDir::new().map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "OpenDalSinkFixture".to_string(),
            message: format!("Failed to create temporary directory: {error}"),
        })?;
        let config_dir = temp_dir.path().join("config");
        let object_root = temp_dir.path().join("objects");
        create_dir(&config_dir)?;
        create_dir(&object_root)?;

        let root = toml::Value::String(object_root.display().to_string());
        let plugin_path = toml::Value::String(plugin_path()?.display().to_string());
        let config = format!(
            r#"type = "sink"
key = "opendal"
enabled = true
version = 0
name = "OpenDAL sink"
path = {plugin_path}
plugin_config_format = "toml"
verbose = false
benchmark = false

[[streams]]
stream = "test_stream"
topics = ["test_topic"]
schema = "json"
batch_length = 100
poll_interval = "50ms"
consumer_group = "opendal_sink_integration"

[plugin_config]
service = "fs"
path_prefix = "iggy/messages"
path_template = "{{stream}}/{{topic}}"
max_attempts = 1
retry_delay = "1ms"
verbose_logging = false

[plugin_config.options]
root = {root}
"#
        );
        let config_path = config_dir.join("opendal.toml");
        fs::write(&config_path, config).map_err(|source| TestBinaryError::FileSystemError {
            path: config_path,
            source,
        })?;

        Ok(Self {
            _temp_dir: temp_dir,
            object_root,
            config_dir,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([(
            CONFIG_DIR_ENV.to_string(),
            self.config_dir.display().to_string(),
        )])
    }
}

fn create_dir(path: &Path) -> Result<(), TestBinaryError> {
    fs::create_dir(path).map_err(|source| TestBinaryError::FileSystemError {
        path: path.to_path_buf(),
        source,
    })
}

fn plugin_path() -> Result<PathBuf, TestBinaryError> {
    let current_exe = std::env::current_exe().map_err(|error| TestBinaryError::FixtureSetup {
        fixture_type: "OpenDalSinkFixture".to_string(),
        message: format!("Failed to locate integration test executable: {error}"),
    })?;
    let target_dir = current_exe.parent().and_then(Path::parent).ok_or_else(|| {
        TestBinaryError::InvalidState {
            message: format!(
                "Integration test executable has no target directory: {}",
                current_exe.display()
            ),
        }
    })?;

    Ok(target_dir.join("libiggy_connector_opendal_sink"))
}
