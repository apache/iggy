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

use std::collections::HashMap;

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture, seeds};
use s3::Bucket;

use crate::connectors::fixtures::{
    self,
    floci::{self, ACCESS_KEY, FlociContainer, REGION, SECRET_KEY},
};

const BUCKET: &str = "iggy-s3-source-test";
pub const DATA_KEY: &str = "logs/010/nested/data %25+雪";
pub const RESTART_RECORD_COUNT: usize = 100;

pub struct S3SourceFixture {
    _floci: FlociContainer,
    pub bucket: Box<Bucket>,
    pub expected: Vec<Vec<u8>>,
    endpoint: String,
}

impl S3SourceFixture {
    async fn create(expected: Vec<Vec<u8>>) -> Result<Self, TestBinaryError> {
        let floci =
            FlociContainer::start(None, &fixtures::unique_container_name("floci-s3-source"))
                .await?;
        let endpoint = floci.endpoint.clone();
        let bucket = floci::create_bucket(&endpoint, BUCKET).await?;
        let fixture = Self {
            _floci: floci,
            bucket,
            expected,
            endpoint,
        };
        fixture.upload("logs/000-empty", b"").await?;
        fixture
            .upload("outside-prefix", b"must not be emitted")
            .await?;
        let mut body = Vec::new();
        body.extend_from_slice(b"||");
        for (index, record) in fixture.expected.iter().enumerate() {
            body.extend_from_slice(record);
            if index + 1 < fixture.expected.len() {
                body.extend_from_slice(b"||||");
            }
        }
        fixture.upload(DATA_KEY, &body).await?;
        Ok(fixture)
    }

    pub async fn upload(&self, key: &str, body: &[u8]) -> Result<(), TestBinaryError> {
        let response = self.bucket.put_object(key, body).await.map_err(|error| {
            TestBinaryError::FixtureSetup {
                fixture_type: "S3SourceFixture".into(),
                message: format!("Failed to upload {key}: {error}"),
            }
        })?;
        if !(200..300).contains(&response.status_code()) {
            return Err(TestBinaryError::FixtureSetup {
                fixture_type: "S3SourceFixture".into(),
                message: format!("Upload of {key} returned {}", response.status_code()),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl TestFixture for S3SourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::create(vec![
            b"{invalid-json".to_vec(),
            b"\xff\xfe\xef\xbb\xbf".to_vec(),
            b" \r\n ".to_vec(),
            vec![b'x'; 65_533],
            b"unterminated tail".to_vec(),
        ])
        .await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "IGGY_CONNECTORS_SOURCE_S3_PATH".into(),
                "../../target/debug/libiggy_connector_s3_source".into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_STREAMS_0_STREAM".into(),
                seeds::names::STREAM.into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_STREAMS_0_TOPIC".into(),
                seeds::names::TOPIC.into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_STREAMS_0_SCHEMA".into(),
                "raw".into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_BUCKET".into(),
                BUCKET.into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_REGION".into(),
                REGION.into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_ENDPOINT".into(),
                self.endpoint.clone(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_PREFIX".into(),
                "logs/".into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_CUSTOM_DELIMITER".into(),
                "||".into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_POLL_INTERVAL".into(),
                "50ms".into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_MAX_RECORD_BYTES".into(),
                "131072".into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_MAX_BATCH_BYTES".into(),
                "262144".into(),
            ),
            (
                "IGGY_CONNECTORS_SOURCE_S3_PLUGIN_CONFIG_MAX_BATCH_MESSAGES".into(),
                "1".into(),
            ),
            ("AWS_ACCESS_KEY_ID".into(), ACCESS_KEY.into()),
            ("AWS_SECRET_ACCESS_KEY".into(), SECRET_KEY.into()),
            ("AWS_EC2_METADATA_DISABLED".into(), "true".into()),
        ])
    }
}

pub struct S3SourceRestartFixture {
    pub inner: S3SourceFixture,
}

#[async_trait]
impl TestFixture for S3SourceRestartFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let records = (0..RESTART_RECORD_COUNT)
            .map(|index| format!("record-{index:04}").into_bytes())
            .collect();
        Ok(Self {
            inner: S3SourceFixture::create(records).await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}
