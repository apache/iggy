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

use crate::router::{Router, write_data};
use crate::slice_user_table;
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::TableIdent;
use iceberg::table::Table;
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload};
use simd_json::base::ValueAsObject;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct DynamicRouter {
    catalog: Box<dyn Catalog>,
    route_field: String,
}

pub struct DynamicWriter {
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
