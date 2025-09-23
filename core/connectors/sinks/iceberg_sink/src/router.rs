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
use iceberg::TableIdent;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, Struct, StructType};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog,
    writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Schema};
use parquet::file::properties::WriterProperties;
use simd_json::base::ValueAsObject;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::slice_user_table;

pub fn primitive_type_to_literal(pt: &PrimitiveType) -> Result<PrimitiveLiteral, Error> {
    match pt {
        PrimitiveType::Boolean => Ok(PrimitiveLiteral::Boolean(false)),
        PrimitiveType::Int => Ok(PrimitiveLiteral::Int(0)),
        PrimitiveType::Long => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::Decimal { .. } => Ok(PrimitiveLiteral::Int128(0)),
        PrimitiveType::Date => Ok(PrimitiveLiteral::Int(0)), // e.g. days since epoch
        PrimitiveType::Time => Ok(PrimitiveLiteral::Long(0)), // microseconds since midnight
        PrimitiveType::Timestamp => Ok(PrimitiveLiteral::Long(0)), // microseconds since epoch
        PrimitiveType::Timestamptz => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::TimestampNs => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::TimestamptzNs => Ok(PrimitiveLiteral::Long(0)),
        PrimitiveType::String => Ok(PrimitiveLiteral::String(String::new())),
        PrimitiveType::Uuid => Ok(PrimitiveLiteral::Binary(vec![0; 16])),
        PrimitiveType::Fixed(len) => Ok(PrimitiveLiteral::Binary(vec![0; *len as usize])),
        PrimitiveType::Binary => Ok(PrimitiveLiteral::Binary(Vec::new())),
        _ => {
            error!("Partition type not supported");
            Err(Error::InvalidConfig)
        }
    }
}

fn get_partition_type_value(default_partition_type: &StructType) -> Result<Option<Struct>, Error> {
    let mut fields: Vec<Option<Literal>> = Vec::new();

    if default_partition_type.fields().is_empty() {
        return Ok(None);
    };

    for field in default_partition_type.fields() {
        let t = field.field_type.as_primitive_type().unwrap();

        let value = Some(Literal::Primitive(primitive_type_to_literal(t)?));

        fields.push(value);
    }
    Ok(Some(Struct::from_iter(fields)))
}

async fn write_data<I, M>(
    messages: I,
    table: &Table,
    catalog: &dyn Catalog,
    messages_schema: Schema,
) -> Result<(), Error>
where
    I: IntoIterator<Item = M>,
    M: std::ops::Deref<Target = ConsumedMessage>,
{
    let location = DefaultLocationGenerator::new(table.metadata().clone()).map_err(|err| {
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
        get_partition_type_value(table.metadata().default_partition_type())?,
        table.metadata().default_partition_spec_id(),
    );

    let mut writer = data_file_writer_builder.build().await.map_err(|err| {
        error!("Error while constructing data file writer: {}", err);
        Error::InitError(err.to_string())
    })?;

    let json_messages = messages
        .into_iter()
        .filter_map(|record| match &record.payload {
            Payload::Json(record) => simd_json::to_string(&record).ok(),
            _ => {
                warn!("Unsupported payload format: {}", messages_schema);
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    if json_messages.is_empty() {
        error!(
            "Could not serialize payload, expected JSON format, got {} instead",
            messages_schema
        );
        return Err(Error::InvalidPayloadType);
    }

    let cursor = Cursor::new(json_messages);

    let reader = ReaderBuilder::new(Arc::new(
        schema_to_arrow_schema(&table.metadata().current_schema().clone()).map_err(|err| {
            error!(
                "Error while mapping records to Iceberg table with uuid: {}. Error {}",
                table.metadata().uuid(),
                err
            );
            Error::InvalidRecord
        })?,
    ))
    .build(cursor)
    .map_err(|err| {
        error!(
            "Error while building Iceberg reader from message payload: {}",
            err
        );
        Error::InitError(err.to_string())
    })?;

    for batch in reader {
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

    let table_commit = Transaction::new(table);

    let action = table_commit.fast_append().add_data_files(data_files);

    let tx = action.apply(table_commit).map_err(|err| {
        error!(
            "Failed to apply transaction on table with UUID: {}, Error: {}",
            table.metadata().uuid(),
            err
        );
        Error::InvalidRecord
    })?;

    let _table = tx.commit(catalog).await.map_err(|err| {
        error!(
            "Failed to commit transaction on table with UUID: {}, Error: {}",
            table.metadata().uuid(),
            err
        );
        Error::InvalidRecord
    })?;
    Ok(())
}

#[derive(Debug)]
pub(crate) struct DynamicRouter {
    catalog: Box<dyn Catalog>,
    route_field: String,
}

struct DynamicWriter {
    pub tables_to_write: HashMap<String, Table>,
    pub table_to_message: HashMap<String, Vec<Arc<ConsumedMessage>>>,
}

impl DynamicWriter {
    pub fn new() -> Self {
        let tables_to_write = HashMap::new();
        let table_to_message = HashMap::new();
        Self {
            tables_to_write,
            table_to_message,
        }
    }

    fn push_to_existing(&mut self, route_field_val: &str, message: &Arc<ConsumedMessage>) -> bool {
        if let Some(message_vec) = self.table_to_message.get_mut(route_field_val) {
            message_vec.push(Arc::clone(message));
            true
        } else {
            false
        }
    }

    // This will:
    // - Check if the table declared on the route field exists in the iceberg catalog.
    // - If it does, it will try to load it to memory and map the name with the Table object so
    // that we can dynamically send messages to it's correct destination.
    async fn ensure_table_exists(
        &mut self,
        route_field_val: &str,
        catalog: &dyn Catalog,
    ) -> Result<bool, Error> {
        let sliced_table = slice_user_table(route_field_val);
        let table_ident = &TableIdent::from_strs(&sliced_table).map_err(|err| {
            error!("Failed to load table from catalog: {}. ", err);
            Error::InitError(err.to_string())
        })?;

        if !catalog.table_exists(table_ident).await.map_err(|err| {
            error!("Failed to load table from catalog: {}. ", err);
            Error::InitError(err.to_string())
        })? {
            return Ok(false);
        }

        let table_ident = TableIdent::from_strs(&sliced_table).map_err(|err| {
            error!("Failed to load table from catalog: {}.", err);
            Error::InitError(err.to_string())
        })?;

        let table = catalog.load_table(&table_ident).await.map_err(|err| {
            error!("Failed to load table from catalog: {}", err);
            Error::InitError(err.to_string())
        })?;

        self.tables_to_write
            .insert(route_field_val.to_string(), table);

        Ok(true)
    }
}

impl DynamicRouter {
    pub fn new(catalog: Box<dyn Catalog>, route_field: String) -> Self {
        Self {
            catalog,
            route_field,
        }
    }

    fn extract_route_field(&self, message: &ConsumedMessage) -> Option<String> {
        match &message.payload {
            Payload::Json(payload) => payload
                .as_object()
                .and_then(|obj| obj.get(&self.route_field))
                .map(|val| val.to_string()),
            _ => {
                warn!("Unsupported format for iceberg connector");
                None
            }
        }
    }
}

#[async_trait]
impl Router for DynamicRouter {
    async fn route_data(
        &self,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error> {
        let mut writer = DynamicWriter::new();
        for message in messages {
            let message = Arc::new(message);
            let route_field_val = match self.extract_route_field(&message) {
                Some(val) => val,
                None => continue,
            };

            if writer.push_to_existing(&route_field_val, &message) {
                continue;
            }

            let route_field_val_cloned = route_field_val.clone();

            if writer
                .ensure_table_exists(&route_field_val_cloned, self.catalog.as_ref())
                .await?
            {
                if let Some(msgs) = writer.table_to_message.get_mut(&route_field_val_cloned) {
                    msgs.push(message);
                } else {
                    let message_vec: Vec<Arc<ConsumedMessage>> = vec![message];
                    writer
                        .table_to_message
                        .insert(route_field_val_cloned, message_vec);
                }
            }
        }

        for (table_name, table_obj) in &writer.tables_to_write {
            let batch_messages = match writer.table_to_message.get(table_name) {
                Some(m) => m,
                None => continue,
            };
            write_data(
                batch_messages.iter().map(Arc::clone),
                table_obj,
                self.catalog.as_ref(),
                messages_metadata.schema,
            )
            .await?;
            info!(
                "Dynamically routed {} messages to {} iceberg table",
                batch_messages.len(),
                table_name
            );
        }

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
    ) -> Result<Self, Error> {
        let mut tables: Vec<Table> = Vec::with_capacity(declared_tables.len());
        let mut tables_found = 0;
        for declared_table in declared_tables {
            let sliced_table = slice_user_table(declared_table);
            let table_ident = &TableIdent::from_strs(sliced_table.clone()).map_err(|err| {
                error!("Failed to load table from catalog: {}. ", err);
                Error::InitError(err.to_string())
            })?;
            let exists = catalog.table_exists(table_ident).await.map_err(|err| {
                error!("Failed to load table from catalog: {}", err);
                Error::InitError(err.to_string())
            })?;

            if !exists {
                continue;
            };

            tables_found += 1;
            let table = catalog.load_table(table_ident).await.map_err(|err| {
                error!("Failed to load table from catalog: {}", err);
                Error::InitError(err.to_string())
            })?;
            tables.push(table);
        }
        info!(
            "Static router found {} tables on iceberg catalog from {} tables declared",
            tables_found,
            declared_tables.len()
        );
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
            write_data(
                &messages,
                table,
                self.catalog.as_ref(),
                messages_metadata.schema,
            )
            .await?;
            info!(
                "Routed {} messages to iceberg table {} successfully",
                messages.len(),
                table.identifier().name()
            );
        }

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
