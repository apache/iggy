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

//! Floci, an emulator of S3 in a container, for the tests that need an S3
//! store. Docker must run.
//!
//! The harness starts it for a test whose server sets
//! `tiered_storage.backend = "s3"`, see [`crate::harness::TestHarness`]. Other
//! tests, such as the S3 provider's contract, call [`new_bucket`].
//!
//! Every test of a run shares one container. nextest runs each test in a
//! process of its own, so the container has a fixed name and is reused, and
//! a file lock keeps two processes from creating it at once. Each test gets a
//! bucket of its own.

use std::fs::File;
use std::time::Duration;

use iggy_object_store::s3::{Credentials, S3Config, S3Store, UrlStyle};
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt, ReuseDirective};
use tokio::sync::OnceCell;

use crate::harness::error::TestBinaryError;

pub const ACCESS_KEY_ID: &str = "test";
pub const SECRET_ACCESS_KEY: &str = "test";
pub const REGION: &str = "us-east-1";

const IMAGE: &str = "floci/floci";
const TAG: &str = "2.4.0";
const PORT: u16 = 4566;
const CONTAINER: &str = "iggy-test-floci";
/// Floci answers its health check before S3 takes requests, so the first
/// bucket creation can fail for a few seconds.
const BUCKET_CREATE_ATTEMPTS: u32 = 30;
const BUCKET_CREATE_RETRY_DELAY: Duration = Duration::from_secs(1);

static ENDPOINT: OnceCell<String> = OnceCell::const_new();

/// A bucket in the shared Floci that no other test uses.
#[derive(Debug, Clone)]
pub struct FlociBucket {
    /// The URL of Floci, with a host name, so that the provider's resolver
    /// runs.
    pub endpoint: String,
    pub bucket: String,
}

impl FlociBucket {
    /// The settings that point a server's `s3` tiered storage at this bucket.
    #[must_use]
    pub fn server_envs(&self) -> [(&'static str, String); 5] {
        [
            ("IGGY_TIERED_STORAGE_ENDPOINT", self.endpoint.clone()),
            ("IGGY_TIERED_STORAGE_BUCKET", self.bucket.clone()),
            ("IGGY_TIERED_STORAGE_REGION", REGION.to_owned()),
            (
                "IGGY_TIERED_STORAGE_ACCESS_KEY_ID",
                ACCESS_KEY_ID.to_owned(),
            ),
            (
                "IGGY_TIERED_STORAGE_SECRET_ACCESS_KEY",
                SECRET_ACCESS_KEY.to_owned(),
            ),
        ]
    }

    /// The configuration of an S3 store in this bucket.
    #[must_use]
    pub fn s3_config(&self) -> S3Config {
        S3Config {
            endpoint: self.endpoint.clone(),
            region: REGION.to_owned(),
            bucket: self.bucket.clone(),
            url_style: UrlStyle::Path,
            credentials: Credentials {
                access_key_id: ACCESS_KEY_ID.to_owned(),
                secret_access_key: SECRET_ACCESS_KEY.to_owned(),
                session_token: None,
            }
            .into(),
        }
    }
}

/// Creates a bucket that no other test uses, in the Floci that this run
/// shares, which this call starts when no test did.
///
/// # Errors
/// [`TestBinaryError::FixtureSetup`] when Floci does not start, for example
/// without Docker, or when it refuses the bucket.
pub async fn new_bucket() -> Result<FlociBucket, TestBinaryError> {
    let endpoint = ENDPOINT.get_or_try_init(start).await?.clone();
    let bucket = FlociBucket {
        endpoint,
        bucket: format!("iggy-{:016x}", rand::random::<u64>()),
    };
    create_bucket(bucket.s3_config()).await?;
    Ok(bucket)
}

/// Starts Floci, or finds the container that another test process started.
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
            HttpWaitStrategy::new("/_localstack/health")
                .with_port(PORT.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("FLOCI_SERVICES_S3_ENFORCE_AUTH", "true")
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

/// The S3 provider runs on compio, not on the harness's runtime, so the
/// bucket is created on a runtime of its own.
async fn create_bucket(config: S3Config) -> Result<(), TestBinaryError> {
    let bucket = config.bucket.clone();
    tokio::task::spawn_blocking(move || {
        let runtime = compio::runtime::Runtime::new().map_err(|error| error.to_string())?;
        runtime.block_on(async {
            let store = S3Store::new(config).map_err(|error| error.to_string())?;
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
        fixture_type: "Floci".to_owned(),
        message,
    }
}
