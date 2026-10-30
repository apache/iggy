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
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use s3::Bucket;
use tempfile::TempDir;
use tokio::time::sleep;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::method};

use crate::connectors::fixtures::{
    self, S3SinkOps,
    floci::{self, ACCESS_KEY, FlociContainer, REGION, SECRET_KEY},
};

const CONFIG_DIR_ENV: &str = "IGGY_CONNECTORS_CONNECTORS_CONFIG_DIR";
const TEST_BUCKET: &str = "iggy-opendal-test";
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL: Duration = Duration::from_millis(50);

pub struct OpenDalSinkFixture {
    _temp_dir: TempDir,
    object_root: PathBuf,
    config_dir: PathBuf,
}

impl OpenDalSinkFixture {
    pub async fn wait_for_object(
        &self,
        offset_start: u64,
        offset_end: u64,
        expected: &[u8],
    ) -> Result<(), TestBinaryError> {
        let path = self.object_path(offset_start, offset_end);

        for _ in 0..POLL_ATTEMPTS {
            match tokio::fs::read(&path).await {
                Ok(payload) if payload == expected => return Ok(()),
                Ok(_) => sleep(POLL_INTERVAL).await,
                Err(error) if error.kind() == ErrorKind::NotFound => sleep(POLL_INTERVAL).await,
                Err(source) => return Err(TestBinaryError::FileSystemError { path, source }),
            }
        }

        Err(TestBinaryError::InvalidState {
            message: format!(
                "OpenDAL object did not contain the expected payload: {}",
                path.display()
            ),
        })
    }

    fn object_path(&self, offset_start: u64, offset_end: u64) -> PathBuf {
        self.object_root.join(format!(
            "iggy/messages/test_stream/test_topic/\
             00000-{offset_start:020}-{offset_end:020}.jsonl"
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
        write_config(&config_dir, "fs", &format!("root = {root}\n"), 1, "2s")?;

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

pub struct OpenDalS3SinkFixture {
    floci: FlociContainer,
    bucket: Box<Bucket>,
    _temp_dir: TempDir,
    config_dir: PathBuf,
}

impl S3SinkOps for OpenDalS3SinkFixture {
    fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    fn endpoint(&self) -> &str {
        &self.floci.endpoint
    }
}

#[async_trait]
impl TestFixture for OpenDalS3SinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let floci =
            FlociContainer::start(None, &fixtures::unique_container_name("floci-opendal")).await?;
        let bucket = floci::create_bucket(&floci.endpoint, TEST_BUCKET).await?;
        let temp_dir = TempDir::new().map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "OpenDalS3SinkFixture".to_string(),
            message: format!("Failed to create temporary directory: {error}"),
        })?;
        let config_dir = temp_dir.path().join("config");
        create_dir(&config_dir)?;

        let endpoint = toml::Value::String(floci.endpoint.clone());
        let options = format!(
            r#"bucket = "{TEST_BUCKET}"
region = "{REGION}"
endpoint = {endpoint}
access_key_id = "{ACCESS_KEY}"
secret_access_key = "{SECRET_KEY}"
"#
        );
        write_config(&config_dir, "s3", &options, 1, "2s")?;

        Ok(Self {
            floci,
            bucket,
            _temp_dir: temp_dir,
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

pub struct OpenDalControlledS3SinkFixture {
    _server: MockServer,
    state: Arc<Mutex<S3WriteState>>,
    _temp_dir: TempDir,
    config_dir: PathBuf,
}

impl OpenDalControlledS3SinkFixture {
    pub fn fail_next_writes(&self, count: usize) {
        self.state.lock().expect("S3 state lock").response = S3WriteResponse::Temporary(count);
    }

    pub fn deny_writes(&self) {
        self.state.lock().expect("S3 state lock").response = S3WriteResponse::Permanent;
    }

    pub fn delay_writes(&self, delay: Duration) {
        self.state.lock().expect("S3 state lock").response = S3WriteResponse::Delayed(delay);
    }

    pub fn object_requests(&self) -> Vec<Request> {
        self.state.lock().expect("S3 state lock").requests.clone()
    }

    pub fn first_delayed_response_at(&self) -> Option<Instant> {
        self.state
            .lock()
            .expect("S3 state lock")
            .first_delayed_response_at
    }
}

#[async_trait]
impl TestFixture for OpenDalControlledS3SinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let server = MockServer::start().await;
        let state = Arc::new(Mutex::new(S3WriteState::default()));
        Mock::given(method("PUT"))
            .respond_with(S3WriteResponder(state.clone()))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let temp_dir = TempDir::new().map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "OpenDalControlledS3SinkFixture".to_string(),
            message: format!("Failed to create temporary directory: {error}"),
        })?;
        let config_dir = temp_dir.path().join("config");
        create_dir(&config_dir)?;
        let endpoint = toml::Value::String(server.uri());
        let options = format!(
            r#"bucket = "{TEST_BUCKET}"
region = "{REGION}"
endpoint = {endpoint}
access_key_id = "{ACCESS_KEY}"
secret_access_key = "{SECRET_KEY}"
"#
        );
        write_config(&config_dir, "s3", &options, 3, "500ms")?;

        Ok(Self {
            _server: server,
            state,
            _temp_dir: temp_dir,
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

#[derive(Default)]
struct S3WriteState {
    response: S3WriteResponse,
    requests: Vec<Request>,
    first_delayed_response_at: Option<Instant>,
}

#[derive(Default)]
enum S3WriteResponse {
    #[default]
    Success,
    Temporary(usize),
    Permanent,
    Delayed(Duration),
}

struct S3WriteResponder(Arc<Mutex<S3WriteState>>);

impl Respond for S3WriteResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let success = ResponseTemplate::new(200)
            .insert_header("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"");
        // Startup probes must not consume the object write failure plan.
        if !request.url.path().ends_with(".jsonl") {
            return success;
        }

        let mut state = self.0.lock().expect("S3 state lock");
        state.requests.push(request.clone());
        match &mut state.response {
            S3WriteResponse::Temporary(remaining) if *remaining > 0 => {
                *remaining -= 1;
                s3_error(503, "SlowDown", "Please reduce your request rate.")
            }
            S3WriteResponse::Permanent => s3_error(403, "AccessDenied", "Access Denied"),
            S3WriteResponse::Delayed(delay) => {
                let delay = *delay;
                state
                    .first_delayed_response_at
                    .get_or_insert_with(|| Instant::now() + delay);
                success.set_delay(delay)
            }
            _ => success,
        }
    }
}

fn s3_error(status: u16, code: &str, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_raw(
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{message}</Message><RequestId>iggy-opendal-test</RequestId></Error>"#
        ),
        "application/xml",
    )
}

fn write_config(
    config_dir: &Path,
    service: &str,
    options: &str,
    max_attempts: u32,
    write_timeout: &str,
) -> Result<(), TestBinaryError> {
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
consumer_group = "opendal_sink_integration"

[plugin_config]
service = "{service}"
path_prefix = "iggy/messages"
path_template = "{{stream}}/{{topic}}"
output_format = "json_lines"
include_metadata = false
include_headers = false
max_attempts = {max_attempts}
retry_delay = "1ms"
write_timeout = "{write_timeout}"
verbose_logging = false

[plugin_config.options]
{options}"#
    );
    let config_path = config_dir.join("opendal.toml");
    fs::write(&config_path, config).map_err(|source| TestBinaryError::FileSystemError {
        path: config_path,
        source,
    })
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
    let plugin_file = format!(
        "{}iggy_connector_opendal_sink{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let target_dir = current_exe
        .ancestors()
        .find(|directory| directory.join(&plugin_file).is_file())
        .ok_or_else(|| TestBinaryError::InvalidState {
            message: format!(
                "No OpenDAL plugin found above integration test executable: {}",
                current_exe.display()
            ),
        })?;

    Ok(target_dir.join(plugin_file).with_extension(""))
}
