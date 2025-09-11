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

use core::fmt;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_json::ReaderBuilder;
use async_trait::async_trait;

use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::TableIdent;
use iceberg::{
    writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
    Catalog,
};
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iggy_connector_sdk::{
    sink_connector, ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum IcebergSinkTypes {
    rest,
    hive,
    glue,
}

impl fmt::Display for IcebergSinkTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            IcebergSinkTypes::rest => "rest",
            IcebergSinkTypes::hive => "hive",
            IcebergSinkTypes::glue => "glue",
        };
        write!(f, "{}", s)
    }
}

sink_connector!(IcebergSink);

#[derive(Debug)]
pub struct IcebergSink {
    id: u32,
    config: IcebergSinkConfig,
    tables: Vec<Table>,
    catalog: Option<Box<dyn Catalog>>,
    props: HashMap<String, String>,
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
    pub fn new(id: u32, config: IcebergSinkConfig) -> Self {
        let tables: Vec<Table> = Vec::with_capacity(config.tables.len());
        let mut props: HashMap<String, String> = HashMap::new();

        props.insert("s3.region".to_string(), config.store_region.clone());
        props.insert(
            "s3.access-key-id".to_string(),
            config.store_access_key_id.clone(),
        );
        props.insert(
            "s3.secret-access-key".to_string(),
            config.store_secret_access_key.clone(),
        );
        props.insert("s3.endpoint".to_string(), config.store_url.clone());

        IcebergSink {
            id,
            config,
            tables,
            catalog: None,
            props,
        }
    }

    fn slice_user_table(&self, table: &String) -> Vec<String> {
        table.split('.').map(|s| s.to_string()).collect()
    }

    #[inline(always)]
    fn get_rest_catalog(&self) -> RestCatalog {
        let catalog_config = RestCatalogConfig::builder()
            .uri(self.config.uri.clone())
            .props(self.props.clone())
            .warehouse(self.config.bucket_name.clone())
            .build();

        RestCatalog::new(catalog_config)
    }

    //#[inline(always)]
    //fn get_hms_catalog(&self) -> HmsCatalog {
    //    let config = HmsCatalogConfig::builder()
    //        .props(self.props)
    //        .warehouse(self.config.bucket_name.clone())
    //        .address(self.config.uri.clone())
    //        .thrift_transport(HmsThriftTransport::Buffered)
    //        .build();
    //
    //    HmsCatalog::new(config)
    //}

    #[inline(always)]
    async fn get_glue_catalog(&self) -> Result<GlueCatalog, Error> {
        let config = GlueCatalogConfig::builder()
            .props(self.props.clone())
            .warehouse(self.config.bucket_name.clone())
            .build();

        let catalog = GlueCatalog::new(config).await.map_err(|err| {
            error!("Failed to apply transaction on table: {}", err);
            Error::HttpRequestFailed(err.to_string())
        });
        return catalog;
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened Iceberg sink connector with ID: {} for URL: {}",
            self.id, self.config.uri
        );

        info!(
            "Configuring Iceberg catalog with the following config:\n-region: {}\n-url: {}\n-store class: {}\n-catalog type: {}\n",
            self.config.store_region,
            self.config.store_url,
            self.config.store_class,
            self.config.catalog_type
        );

        let catalog: Box<dyn Catalog> = match self.config.catalog_type {
            IcebergSinkTypes::rest => Box::new(self.get_rest_catalog()),
            IcebergSinkTypes::hive => Box::new(self.get_rest_catalog()),
            IcebergSinkTypes::glue => Box::new(self.get_glue_catalog().await?),
        };

        for declared_table in &self.config.tables {
            let sliced_table = self.slice_user_table(&declared_table);
            let table = catalog
                .load_table(&TableIdent::from_strs(sliced_table).map_err(|err| {
                    error!("Failed to load table from catalog: {}", err);
                    Error::HttpRequestFailed(err.to_string())
                })?)
                .await
                .map_err(|err| {
                    error!("Failed to get table from catalog: {}", err);
                    Error::HttpRequestFailed(err.to_string())
                })?;
            self.tables.push(table);
        }

        self.catalog = Some(catalog);
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

        for table in &self.tables {
            let location = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();

            let file_name_gen = DefaultFileNameGenerator::new(
                Uuid::new_v4().to_string(),
                None,
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

            let _table = tx
                .commit(self.catalog.as_ref().unwrap().as_ref())
                .await
                .map_err(|err| {
                    error!("Failed to apply transaction on table: {}", err);
                    Error::HttpRequestFailed(err.to_string())
                })?;
        }

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
