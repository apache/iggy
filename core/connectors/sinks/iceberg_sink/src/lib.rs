/* Licensed to the Apache Software Foundation (ASF) under one
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

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_json::ReaderBuilder;
use async_trait::async_trait;

use iceberg::arrow::schema_to_arrow_schema;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::TableIdent;
use iceberg::{
    writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
    Catalog,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iggy_connector_sdk::{
    sink_connector, ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
};
use parquet::file::properties::WriterProperties;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum IcebergSinkTypes {
    rest,
    hdfs,
}

sink_connector!(IcebergSink);

#[derive(Debug)]
pub struct IcebergSink {
    id: u32,
    config: IcebergSinkConfig,
    client: reqwest::Client,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSinkConfig {
    pub tables: Vec<String>,
    pub catalog_type: IcebergSinkTypes,
    pub bucket_name: String,
    pub uri: String,
    pub credential: String,
    pub auto_create: bool,
    pub part_size: u32,
    pub store_url: String,
    pub store_access_key_id: String,
    pub store_secret_access_key: String,
    pub store_region: String,
    pub store_class: String,
}

impl IcebergSink {
    fn flatten_payload(json: &Value) -> Vec<(String, Option<String>)> {
        let payload = json.get("payload").and_then(|p| p.get("Json"));
        vec![
            (
                "id".to_string(),
                payload
                    .and_then(|p| p.get("id"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            ),
            (
                "title".to_string(),
                payload
                    .and_then(|p| p.get("title"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            ),
            (
                "name".to_string(),
                payload
                    .and_then(|p| p.get("name"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            ),
            (
                "text".to_string(),
                payload
                    .and_then(|p| p.get("text"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            ),
            (
                "test_field".to_string(),
                payload
                    .and_then(|p| p.get("test_field"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            ),
            ("message".to_string(), Some("".to_string())), // fill missing field
        ]
    }
    pub fn new(id: u32, config: IcebergSinkConfig) -> Self {
        IcebergSink {
            id,
            config,
            client: reqwest::Client::new(),
        }
    }
    fn get_s3_client(&self) -> Bucket {
        let credentials = Credentials::new(
            Some(&self.config.store_access_key_id.clone()),
            Some(&self.config.store_secret_access_key.clone()),
            None,
            None,
            None,
        )
        .expect("Invalid credentials");

        let region = Region::Custom {
            region: self.config.store_region.clone(),
            endpoint: self.config.store_url.clone(),
        };

        info!("{} {:?} {:?}", self.config.bucket_name, region, credentials);

        let bucket = Bucket::new(&self.config.bucket_name, region, credentials)
            .expect("Failed to create S3 bucket client");

        *bucket.with_path_style()
    }

    fn get_iceberg_tables(&self) -> Vec<Vec<String>> {
        let tables: Vec<Vec<String>> = self
            .config
            .tables
            .iter()
            .map(|table| {
                table
                    .split('.')
                    .map(|element| element.to_string())
                    .collect::<Vec<String>>()
            })
            .collect();

        info!("{:?}", tables);
        return tables;
    }

    pub fn get_iceberg_url(&self) -> String {
        let table_path = self.get_iceberg_tables()[0].clone();
        if table_path.is_empty() {
            panic!("table_path must contain at least the table name");
        }

        let table_name = table_path.last().unwrap();
        info!("{}", table_name);
        let namespaces = &table_path[..table_path.len() - 1];
        let namespace_path = namespaces.join(".");

        info!(
            "{}",
            format!(
                "{}/v1/namespaces/{}/tables/{}",
                self.config.uri, namespace_path, table_name
            )
        );

        if namespace_path.is_empty() {
            format!("{}/v1/tables/{}", self.config.uri, table_name)
        } else {
            format!(
                "{}/v1/namespaces/{}/tables/{}",
                self.config.uri, namespace_path, table_name
            )
        }
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened Iceberg sink connector with ID: {} for URL: {}",
            self.id, self.config.uri
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        info!(
            "Iceberg sink with ID: {} received: {} messages, format: {}",
            self.id,
            messages.len(),
            messages_metadata.schema
        );

        info!("{:?}", messages[0].payload);
        //
        //writer.close().map_err(|error| {
        //    error!(
        //        "Failed to send HTTP request to ingest messages for index: {}. {error}",
        //        self.id
        //    );
        //    Error::HttpRequestFailed(error.to_string())
        //})?;
        //
        //let bucket = self.get_s3_client();
        //
        //info!("{}", bucket.url());
        //
        //let key = format!("nyc/users/data/{}.parquet", Uuid::new_v4());
        //let response_bucket = bucket.put_object(&key, &buffer).await.map_err(|error| {
        //    error!(
        //        "Failed to commit new parquet file to Iceberg table: {}",
        //        error
        //    );
        //    Error::HttpRequestFailed(error.to_string())
        //})?;
        //
        //let parquet_path = format!("s3://{}/{}", self.config.bucket_name, &key);

        //info!("Parquet file uploaded: {}", parquet_path);

        let iceberg_url = self.get_iceberg_url();

        let mut props: HashMap<String, String> = HashMap::new();

        props.insert("s3.region".to_string(), self.config.store_region.clone());
        props.insert(
            "s3.access-key-id".to_string(),
            self.config.store_access_key_id.clone(),
        );
        props.insert(
            "s3.secret-access-key".to_string(),
            self.config.store_secret_access_key.clone(),
        );
        props.insert("s3.endpoint".to_string(), self.config.store_url.clone());

        let catalog_config = RestCatalogConfig::builder()
            .uri(self.config.uri.clone())
            .props(props)
            .warehouse(self.config.bucket_name.clone())
            .build();

        let catalog = RestCatalog::new(catalog_config);

        let table = catalog
            .load_table(&TableIdent::from_strs(["nyc", "users"]).map_err(|err| {
                error!("Failed to load table from catalog: {}", err);
                Error::HttpRequestFailed(err.to_string())
            })?)
            .await
            .map_err(|err| {
                error!("Failed to get table from catalog: {}", err);
                Error::HttpRequestFailed(err.to_string())
            })?;

        let location = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();

        let file_name_gen = DefaultFileNameGenerator::new(
            "testing".to_string(),
            Some(Uuid::new_v4().to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
            table.file_io().clone(),
            location,
            file_name_gen,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(
            parquet_writer_builder,
            None,
            table.metadata().default_partition_spec_id(),
        );

        let mut writer = data_file_writer_builder.build().await.map_err(|err| {
            error!("Error while constructing data file writer: {}", err);
            Error::HttpRequestFailed(err.to_string())
        })?;

        let json_messages = messages
            .iter()
            .filter_map(|record| match &record.payload {
                Payload::Json(record) => simd_json::to_string(&record).ok(),
                _ => panic!("aaa"),
            })
            .collect::<Vec<_>>()
            .join("\n");

        let cursor = Cursor::new(json_messages);

        let mut reader = ReaderBuilder::new(Arc::new(
            schema_to_arrow_schema(&table.metadata().current_schema().clone()).unwrap(),
        ))
        .build(cursor)
        .unwrap();

        while let Some(batch) = reader.next() {
            let batch_data = batch.unwrap();
            writer.write(batch_data).await.unwrap();
        }

        let data_files = writer.close().await.unwrap();

        let table_commit = Transaction::new(&table);

        let action = table_commit.fast_append().add_data_files(data_files);

        let tx = action.apply(table_commit).map_err(|err| {
            error!("Failed to apply transaction: {}", err);
            Error::HttpRequestFailed(err.to_string())
        })?;

        let table = tx.commit(&catalog).await.map_err(|err| {
            error!("Failed to apply transaction on table: {}", err);
            Error::HttpRequestFailed(err.to_string())
        })?;

        match self.config.catalog_type {
            IcebergSinkTypes::rest => info!("Rest"),
            IcebergSinkTypes::hdfs => info!("HDFS"),
        };

        info!("Finished successfully");

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Iceberg sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert_eq!(true, true);
    }
}
