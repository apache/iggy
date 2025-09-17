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
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload};
use parquet::file::properties::WriterProperties;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{create_table, slice_user_table};

#[derive(Debug)]
pub(crate) struct DynamicRouter {
    catalog: Box<dyn Catalog>,
}

impl DynamicRouter {
    pub fn new(catalog: Box<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl Router for DynamicRouter {
    async fn route_data(
        &self,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct StaticRouter {
    tables: Vec<Table>,
    catalog: Box<dyn Catalog>,
}

impl StaticRouter {
    pub async fn new(
        catalog: Box<dyn Catalog>,
        declared_tables: &Vec<String>,
        auto_create: bool,
    ) -> Result<Self, Error> {
        let mut tables: Vec<Table> = Vec::with_capacity(declared_tables.len());
        for declared_table in declared_tables {
            let sliced_table = slice_user_table(&declared_table);
            let table_ident = &TableIdent::from_strs(sliced_table.clone()).map_err(|err| {
                error!("Failed to load table from catalog: {}. ", err);
                Error::InitError(err.to_string())
            })?;
            let exists = catalog.table_exists(table_ident).await.map_err(|err| {
                error!("Failed to load table from catalog: {}", err);
                Error::InitError(err.to_string())
            })?;

            if !exists {
                if auto_create {
                    // create table and push
                    let table = create_table(sliced_table.last().unwrap().to_string()).await?;
                    tables.push(table);
                    continue;
                } else {
                    continue;
                }
            }
            let table = catalog.load_table(table_ident).await.map_err(|err| {
                error!("Failed to load table from catalog: {}", err);
                Error::InitError(err.to_string())
            })?;
            tables.push(table);
        }
        Ok(StaticRouter { tables, catalog })
    }
}

#[async_trait]
impl Router for StaticRouter {
    async fn route_data(
        &self,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error> {
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

            let _table = tx.commit(self.catalog.as_ref()).await.map_err(|err| {
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
}

#[async_trait]
pub trait Router: std::fmt::Debug + Sync + Send {
    async fn route_data(
        &self,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error>;
}
