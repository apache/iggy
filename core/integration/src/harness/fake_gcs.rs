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

//! fake-gcs-server, an emulator of Google Cloud Storage in a container, for
//! the tests that need a GCS store. Docker must run.
//!
//! The harness starts it for a test whose server sets
//! `tiered_storage.backend = "gcs"`, see [`crate::harness::TestHarness`].
//! Other tests, such as the GCS provider's contract, call [`new_bucket`].
//!
//! The emulator checks no credentials, so the stores and servers here send
//! none. It decides `ifGenerationMatch=0` on the multipart uploads that the
//! provider sends, but it does not check the CRC32C of an upload and sends
//! no page tokens.
//!
//! Every test of a run shares one container, as with Floci: a fixed name,
//! reuse, and a file lock around the start. Each test gets a bucket of its
//! own.

use std::fs::File;
use std::time::Duration;

use iggy_object_store::gcs::{GcsAuth, GcsConfig, GcsStore};
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt, ReuseDirective};
use tokio::sync::OnceCell;

use crate::harness::error::TestBinaryError;

const IMAGE: &str = "fsouza/fake-gcs-server";
const TAG: &str = "1.52.2";
const PORT: u16 = 4443;
const CONTAINER: &str = "iggy-test-fake-gcs";
const BUCKET_CREATE_ATTEMPTS: u32 = 30;
const BUCKET_CREATE_RETRY_DELAY: Duration = Duration::from_secs(1);

static ENDPOINT: OnceCell<String> = OnceCell::const_new();

/// A bucket in the shared emulator that no other test uses.
#[derive(Debug, Clone)]
pub struct FakeGcsBucket {
    /// The URL of the emulator, with a host name, so that the provider's
    /// resolver runs.
    pub endpoint: String,
    pub bucket: String,
}

impl FakeGcsBucket {
    /// The settings that point a server's `gcs` tiered storage at this
    /// bucket.
    #[must_use]
    pub fn server_envs(&self) -> [(&'static str, String); 3] {
        [
            ("IGGY_TIERED_STORAGE_GCS_ENDPOINT", self.endpoint.clone()),
            ("IGGY_TIERED_STORAGE_GCS_BUCKET", self.bucket.clone()),
            ("IGGY_TIERED_STORAGE_GCS_AUTH", "none".to_owned()),
        ]
    }

    /// The configuration of a GCS store in this bucket.
    #[must_use]
    pub fn gcs_config(&self) -> GcsConfig {
        GcsConfig {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            auth: GcsAuth::Anonymous,
        }
    }
}

/// Creates a bucket that no other test uses, in the emulator that this run
/// shares, which this call starts when no test did.
///
/// # Errors
/// [`TestBinaryError::FixtureSetup`] when the emulator does not start, for
/// example without Docker, or when it refuses the bucket.
pub async fn new_bucket() -> Result<FakeGcsBucket, TestBinaryError> {
    let endpoint = ENDPOINT.get_or_try_init(start).await?.clone();
    let bucket = FakeGcsBucket {
        endpoint,
        bucket: format!("iggy-{:016x}", rand::random::<u64>()),
    };
    create_bucket(bucket.gcs_config()).await?;
    Ok(bucket)
}

/// Starts the emulator, or finds the container that another test process
/// started.
async fn start() -> Result<String, TestBinaryError> {
    let lock_path = std::env::temp_dir().join(format!("{CONTAINER}.lock"));
    let lock = tokio::task::spawn_blocking(move || {
        let file = File::create(&lock_path)?;
        file.lock()?;
        Ok::<_, std::io::Error>(file)
    })
    .await
    .map_err(|error| setup_error(format!("the lock task failed: {error}")))?
    .map_err(|error| setup_error(format!("the container lock: {error}")))?;
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(PORT.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/_internal/healthcheck")
                .with_port(PORT.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_cmd(["-scheme", "http", "-port", "4443", "-backend", "memory"])
        .with_container_name(CONTAINER)
        .with_reuse(ReuseDirective::Always)
        .start()
        .await
        .map_err(|error| setup_error(format!("the container did not start: {error}")))?;
    let port = container
        .get_host_port_ipv4(PORT.tcp())
        .await
        .map_err(|error| setup_error(format!("the container has no port: {error}")))?;
    drop(lock);
    Ok(format!("http://localhost:{port}"))
}

/// The provider runs on compio, not on the harness's runtime, so the bucket
/// is created on a runtime of its own.
async fn create_bucket(config: GcsConfig) -> Result<(), TestBinaryError> {
    let bucket = config.bucket.clone();
    tokio::task::spawn_blocking(move || {
        let runtime = compio::runtime::Runtime::new().map_err(|error| error.to_string())?;
        runtime.block_on(async {
            let store = GcsStore::new(config).map_err(|error| error.to_string())?;
            let mut attempt = 1;
            loop {
                match store.create_bucket().await {
                    Ok(()) => return Ok(()),
                    Err(_) if attempt < BUCKET_CREATE_ATTEMPTS => {
                        attempt += 1;
                        compio::time::sleep(BUCKET_CREATE_RETRY_DELAY).await;
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
        })
    })
    .await
    .map_err(|error| setup_error(format!("the bucket task failed: {error}")))?
    .map_err(|error| setup_error(format!("bucket {bucket:?}: {error}")))
}

fn setup_error(message: String) -> TestBinaryError {
    TestBinaryError::FixtureSetup {
        fixture_type: "fake-gcs-server".to_owned(),
        message,
    }
}
