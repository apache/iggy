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

use uuid::Uuid;

mod airflow;
mod clickhouse;
mod delta;
mod doris;
mod elasticsearch;
mod http;
mod iceberg;
mod influxdb;
mod mongodb;
mod postgres;
mod quickwit;
mod s3;
mod surrealdb;
mod wiremock;

/// Prefix on every test container name so `just clean-test-containers` reaps
/// them all with one `name=^iggy-test-` filter. A new fixture only has to use
/// `unique_container_name` (or a fixed `iggy-test-<svc>` for reuse containers)
/// to be covered.
pub(crate) const TEST_CONTAINER_PREFIX: &str = "iggy-test-";

/// Unique per-test container name for ephemeral fixtures. Reuse fixtures
/// (elasticsearch, doris) use a fixed `iggy-test-<svc>` literal instead, since
/// the stable name is what lets later test processes attach to the same one.
pub(crate) fn unique_container_name(service: &str) -> String {
    format!(
        "{TEST_CONTAINER_PREFIX}{service}-{}",
        Uuid::new_v4().simple()
    )
}

pub use airflow::AirflowSinkFixture;
pub use clickhouse::{
    ClickHouseSinkFixture, ClickHouseSinkRowBinaryFixture, ClickHouseSinkStringFixture,
};
pub use delta::{DeltaFixture, DeltaS3Fixture};
pub use doris::{
    DorisOps, DorisSinkColumnsMappingFixture, DorisSinkFixture, DorisSinkMaxFilterRatioFixture,
    DorisSinkPreCreatedFixture,
};
pub use elasticsearch::{ElasticsearchSinkFixture, ElasticsearchSourcePreCreatedFixture};
pub use http::{
    HttpSinkIndividualFixture, HttpSinkJsonArrayFixture, HttpSinkMultiTopicFixture,
    HttpSinkNdjsonFixture, HttpSinkNoMetadataFixture, HttpSinkRawFixture,
};
pub use iceberg::{
    DEFAULT_NAMESPACE, DEFAULT_TABLE, IcebergEnvAuthFixture, IcebergOps, IcebergPreCreatedFixture,
};
pub use influxdb::{
    InfluxDb3SinkFixture, InfluxDb3SourceFixture, InfluxDbSinkBase64Fixture, InfluxDbSinkFixture,
    InfluxDbSinkNoMetadataFixture, InfluxDbSinkNsPrecisionFixture, InfluxDbSinkTextFixture,
    InfluxDbSourceFixture, InfluxDbSourceRawFixture, InfluxDbSourceTextFixture,
};
pub use mongodb::{
    MongoDbOps, MongoDbSinkAutoCreateFixture, MongoDbSinkBatchFixture, MongoDbSinkFailpointFixture,
    MongoDbSinkFixture, MongoDbSinkJsonFixture, MongoDbSinkWriteConcernFixture,
};
pub use postgres::{
    PostgresOps, PostgresSinkByteaFixture, PostgresSinkFixture, PostgresSinkJsonFixture,
    PostgresSourceByteaFixture, PostgresSourceCdcFixture, PostgresSourceDeleteFixture,
    PostgresSourceJsonFixture, PostgresSourceJsonbFixture, PostgresSourceMarkFixture,
    PostgresSourceOps,
};
pub use quickwit::{QuickwitFixture, QuickwitOps, QuickwitPreCreatedFixture};
pub use s3::{S3SinkFixture, S3SinkOps, S3SinkRotationFixture};
pub use surrealdb::{
    SurrealDbOps, SurrealDbSinkBatchFixture, SurrealDbSinkFixture, SurrealDbSinkJsonFixture,
    SurrealDbSinkRawFixture,
};
pub use wiremock::{WireMockDirectFixture, WireMockWrappedFixture};
