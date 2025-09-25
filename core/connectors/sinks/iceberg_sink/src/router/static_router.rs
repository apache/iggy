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
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata};
use tracing::{error, info};

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
