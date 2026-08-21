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

use super::container::{
	DEFAULT_TEST_STREAM, DEFAULT_TEST_TOPIC, ENV_SOURCE_CONNECTION_STRING,
	ENV_SOURCE_DELETE_AFTER_READ, ENV_SOURCE_INCLUDE_METADATA, ENV_SOURCE_PATH,
	ENV_SOURCE_PAYLOAD_COLUMN, ENV_SOURCE_PAYLOAD_FORMAT, ENV_SOURCE_POLL_INTERVAL,
	ENV_SOURCE_PRIMARY_KEY_COLUMN, ENV_SOURCE_PROCESSED_COLUMN, ENV_SOURCE_STREAMS_0_SCHEMA,
	ENV_SOURCE_STREAMS_0_STREAM, ENV_SOURCE_STREAMS_0_TOPIC, ENV_SOURCE_TABLES,
	ENV_SOURCE_TRACKING_COLUMN, MSSQLContainer, MSSQLOps, MSSQLSourceOps,
};
use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use tiberius::{Client, Config, Query, Row};
use tokio::net::TcpStream;
use std::collections::HashMap;

pub type DBClient = Client<Compat<TcpStream>>;

/// MSSQL source fixture for JSON rows with metadata.
///
/// Creates a table with typed columns that get serialized as JSON with metadata.
pub struct MSSQLSourceJsonFixture {
	container: MSSQLContainer,
}

impl MSSQLOps for MSSQLSourceJsonFixture {
	fn container(&self) -> &MSSQLContainer {
		&self.container
	}
}

impl MSSQLSourceOps for MSSQLSourceJsonFixture {
	fn table_name(&self) -> &str {
		Self::TABLE
	}
}

impl MSSQLSourceJsonFixture {
	const TABLE: &'static str = "test_messages";

	pub async fn create_table(&self, client: &mut DBClient) {
		let query = Query::new(format!(
			"CREATE TABLE {} (
				id INT IDENTITY(1,1) PRIMARY KEY (ID) NOT NULL,
				name VARCHAR(255) NOT NULL,
				count INT NOT NULL,
				amount DECIMAL NOT NULL,
				active BIT NOT NULL,
				timestamp BIGINT NOT NULL,
				tag CHAR(10) NOT NULL
			)",
			Self::TABLE
		));
		query.execute(client)
			.await
			.unwrap_or_else(|e| panic!("Failed to create table: {e}"));
	}

	#[allow(clippy::too_many_arguments)]
	pub async fn insert_row(
		&self,
		client: &mut DBClient,
		id: i32,
		name: &str,
		count: i32,
		amount: f64,
		active: bool,
		timestamp: i64,
	) {
		let tag = format!("{:<10}", format!("tag_{id}"));
		let query = format!(
			"INSERT INTO {} (id, name, count, amount, active, timestamp, tag) VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7)",
			Self::TABLE
		);
		let query = Query::new(query);
		query.bind(id);
		query.bind(name);
		query.bind(count);
		query.bind(amount);
		query.bind(active);
		query.bind(timestamp);
		query.bind(&tag);
		query.execute(client)
			.await
			.unwrap_or_else(|e| panic!("Failed to insert row: {e}"));
	}
}

#[async_trait]
impl TestFixture for MSSQLSourceJsonFixture {
	async fn setup() -> Result<Self, TestBinaryError> {
		let container = MSSQLContainer::start().await?;
		Ok(Self { container })
	}

	fn connectors_runtime_envs(&self) -> HashMap<String, String> {
		let mut envs = HashMap::new();
		envs.insert(
			ENV_SOURCE_CONNECTION_STRING.to_string(),
			self.container.connection_string.clone(),
		);
		envs.insert(ENV_SOURCE_TABLES.to_string(), format!("[{}]", Self::TABLE));
		envs.insert(ENV_SOURCE_ENABLE_CDC.to_string(), "true".to_string());		
		envs.insert(ENV_SOURCE_INCLUDE_METADATA.to_string(), "true".to_string());
		envs.insert(
			ENV_SOURCE_STREAMS_0_STREAM.to_string(),
			DEFAULT_TEST_STREAM.to_string(),
		);
		envs.insert(
			ENV_SOURCE_STREAMS_0_TOPIC.to_string(),
			DEFAULT_TEST_TOPIC.to_string(),
		);
		envs.insert(ENV_SOURCE_STREAMS_0_SCHEMA.to_string(), "json".to_string());
		envs.insert(ENV_SOURCE_POLL_INTERVAL.to_string(), "10ms".to_string());
		envs.insert(
			ENV_SOURCE_PATH.to_string(),
			"../../target/debug/libiggy_connector_mssql_source".to_string(),
		);
		envs
	}
}
