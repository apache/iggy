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
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum IcebergSinkTypes {
    rest,
    glue,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum IcebergSinkStoreClass {
    s3,
    fs,
    gcs,
    azdls,
    oss,
}

impl fmt::Display for IcebergSinkStoreClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            IcebergSinkStoreClass::s3 => "s3",
            IcebergSinkStoreClass::fs => "fs",
            IcebergSinkStoreClass::gcs => "gcs",
            IcebergSinkStoreClass::oss => "oss",
            IcebergSinkStoreClass::azdls => "azdls",
        };
        write!(f, "{}", s)
    }
}

impl fmt::Display for IcebergSinkTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            IcebergSinkTypes::rest => "rest",
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
    pub warehouse: String,
    pub uri: String,
    pub store_url: String,
    pub store_access_key_id: String,
    pub store_secret_access_key: String,
    pub store_region: String,
    pub store_class: IcebergSinkStoreClass,
}

impl IcebergSink {
    #[inline(always)]
    fn get_props_s3(&self) -> Result<HashMap<String, String>, Error> {
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
        return Ok(props);
    }

    pub fn new(id: u32, config: IcebergSinkConfig) -> Self {
        let tables: Vec<Table> = Vec::with_capacity(config.tables.len());
        let props = HashMap::new();

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
            .warehouse(self.config.warehouse.clone())
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
            .warehouse(self.config.warehouse.clone())
            .build();

        let catalog = GlueCatalog::new(config).await.map_err(|err| {
            error!("Failed to get glue catalog with error: {}. Make sure the catalog is correctly declared on the config file", err);
            Error::InitError(err.to_string())
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

        // Insert adequate props for initializing file IO, else fail to open
        self.props = match self.config.store_class {
            IcebergSinkStoreClass::s3 => self.get_props_s3()?,
            IcebergSinkStoreClass::fs => HashMap::new(),
            _ => {
                error!(
                    "Store class {} is not supported yet",
                    self.config.store_class
                );
                return Err(Error::InvalidConfig);
            }
        };

        let catalog: Box<dyn Catalog> = match self.config.catalog_type {
            IcebergSinkTypes::rest => Box::new(self.get_rest_catalog()),
            IcebergSinkTypes::glue => Box::new(self.get_glue_catalog().await?),
        };

        for declared_table in &self.config.tables {
            let sliced_table = self.slice_user_table(&declared_table);
            let table = catalog
                .load_table(&TableIdent::from_strs(sliced_table).map_err(|err| {
                    error!("Failed to load table from catalog: {}. ", err);
                    Error::InitError(err.to_string())
                })?)
                .await
                .map_err(|err| {
                    error!("Failed to load table from catalog: {}", err);
                    Error::InitError(err.to_string())
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
            let location =
                DefaultLocationGenerator::new(table.metadata().clone()).map_err(|err| {
                    error!(
                        "Failed to get location on table: {}. Error: {}",
                        table.metadata().uuid(),
                        err
                    );
                    Error::InvalidConfig
                })?;

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
                Error::InitError(err.to_string())
            })?;

            let json_messages = messages
                .iter()
                .filter_map(|record| match &record.payload {
                    Payload::Json(record) => simd_json::to_string(&record).ok(),
                    _ => {
                        warn!("Unsupported payload format: {}", messages_metadata.schema);
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");

            if json_messages.is_empty() {
                error!(
                    "Could not serialize payload, expected JSON format, got {} instead",
                    messages_metadata.schema
                );
                return Err(Error::InvalidPayloadType);
            }

            let cursor = Cursor::new(json_messages);

            let mut reader = ReaderBuilder::new(Arc::new(
                schema_to_arrow_schema(&table.metadata().current_schema().clone()).map_err(
                    |err| {
                        error!(
                            "Error while mapping records to Iceberg table with uuid: {}. Error {}",
                            table.metadata().uuid(),
                            err
                        );
                        Error::InvalidRecord
                    },
                )?,
            ))
            .build(cursor)
            .map_err(|err| {
                error!(
                    "Error while building Iceberg reader from message payload: {}",
                    err
                );
                Error::InitError(err.to_string())
            })?;

            while let Some(batch) = reader.next() {
                let batch_data = batch.map_err(|err| {
                    error!("Error while getting record batch: {}", err);
                    Error::InvalidRecord
                })?;
                writer.write(batch_data).await.map_err(|err| {
                    error!("Error while writing record batch: {}", err);
                    Error::InvalidRecord
                })?;
            }

            let data_files = writer.close().await.map_err(|err| {
                error!("Error while writing data records to Parquet file: {}", err);
                Error::InvalidRecord
            })?;

            let table_commit = Transaction::new(&table);

            let action = table_commit.fast_append().add_data_files(data_files);

            let tx = action.apply(table_commit).map_err(|err| {
                error!(
                    "Failed to apply transaction on table with UUID: {}, Error: {}",
                    table.metadata().uuid(),
                    err
                );
                Error::InvalidRecord
            })?;

            let _table = tx
                .commit(self.catalog.as_ref().unwrap().as_ref())
                .await
                .map_err(|err| {
                    error!(
                        "Failed to commit transaction on table with UUID: {}, Error: {}",
                        table.metadata().uuid(),
                        err
                    );
                    Error::InvalidRecord
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
