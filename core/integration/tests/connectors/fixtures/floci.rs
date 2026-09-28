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

use std::time::Duration;

use integration::harness::TestBinaryError;
use s3::{
    Bucket, BucketConfiguration, Region,
    command::Command,
    creds::Credentials,
    request::{Request, tokio_backend::ReqwestRequest},
};
use testcontainers_modules::testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};

pub const ACCESS_KEY: &str = "test";
pub const SECRET_KEY: &str = "test";
pub const REGION: &str = "us-east-1";

const IMAGE: &str = "floci/floci";
const TAG: &str = "2.1.0";
const PORT: u16 = 4566;
const BUCKET_CREATE_ATTEMPTS: usize = 30;
const BUCKET_CREATE_RETRY_DELAY: Duration = Duration::from_secs(1);

pub struct FlociContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub endpoint: String,
    pub internal_endpoint: String,
}

impl FlociContainer {
    pub async fn start(
        network: Option<&str>,
        container_name: &str,
    ) -> Result<Self, TestBinaryError> {
        let request = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(PORT.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/_localstack/health")
                    .with_port(PORT.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_container_name(container_name)
            .with_mapped_port(0, PORT.tcp());
        let request = if let Some(network) = network {
            request.with_network(network)
        } else {
            request
        };
        let container = request
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "FlociContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })?;

        let mapped_port = container.get_host_port_ipv4(PORT).await.map_err(|error| {
            TestBinaryError::FixtureSetup {
                fixture_type: "FlociContainer".to_string(),
                message: format!("Failed to get port: {error}"),
            }
        })?;

        let endpoint = format!("http://localhost:{mapped_port}");
        let internal_endpoint = format!("http://{container_name}:{PORT}");
        tracing::info!("Floci available at {endpoint}");

        Ok(Self {
            container,
            endpoint,
            internal_endpoint,
        })
    }
}

pub async fn create_bucket(
    endpoint: &str,
    bucket_name: &str,
) -> Result<Box<Bucket>, TestBinaryError> {
    let region = Region::Custom {
        region: REGION.to_string(),
        endpoint: endpoint.to_string(),
    };
    let credentials = Credentials::new(Some(ACCESS_KEY), Some(SECRET_KEY), None, None, None)
        .map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "FlociContainer".to_string(),
            message: format!("Failed to create credentials: {error}"),
        })?;

    // rust-s3's bucket creation helper adds a LocationConstraint for custom
    // endpoints, but us-east-1 bucket creation requires an empty body.
    let mut bucket = Bucket::new(bucket_name, region, credentials).map_err(|error| {
        TestBinaryError::FixtureSetup {
            fixture_type: "FlociContainer".to_string(),
            message: format!("Failed to open bucket '{bucket_name}': {error}"),
        }
    })?;
    bucket.set_path_style();
    let mut last_status = 0;
    let mut last_response = String::new();
    for _ in 0..BUCKET_CREATE_ATTEMPTS {
        let request = ReqwestRequest::new(
            &bucket,
            "",
            Command::CreateBucket {
                config: BucketConfiguration::default(),
            },
        )
        .await
        .map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "FlociContainer".to_string(),
            message: format!("Failed to prepare bucket creation for '{bucket_name}': {error}"),
        })?;
        let response =
            request
                .response_data(false)
                .await
                .map_err(|error| TestBinaryError::FixtureSetup {
                    fixture_type: "FlociContainer".to_string(),
                    message: format!("Failed to create bucket '{bucket_name}': {error}"),
                })?;
        last_status = response.status_code();
        if (200..300).contains(&last_status) || last_status == 409 {
            return Ok(bucket);
        }
        last_response = String::from_utf8_lossy(response.as_slice()).into_owned();
        if (400..500).contains(&last_status) {
            break;
        }
        tokio::time::sleep(BUCKET_CREATE_RETRY_DELAY).await;
    }

    Err(TestBinaryError::FixtureSetup {
        fixture_type: "FlociContainer".to_string(),
        message: format!(
            "Bucket '{bucket_name}' not creatable (last status: {last_status}, response: {last_response})"
        ),
    })
}
