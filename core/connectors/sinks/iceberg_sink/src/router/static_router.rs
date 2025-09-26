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

use crate::router::{Router, is_valid_namespaced_table, table_exists, write_data};
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::table::Table;
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload};
use tracing::{error, info, warn};

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
        for declared_table in declared_tables {
            if !is_valid_namespaced_table(declared_table) {
                error!(
                    "Declared table {} is not valid. It has to include at least one namespace before the table name separated by '.' character",
                    declared_table
                );
                continue;
            }

            let table = match table_exists(declared_table, catalog.as_ref()).await {
                Some(table) => table,
                None => {
                    warn!(
                        "Declared table {} doesn't exist in the configured catalog. Skipping...",
                        declared_table
                    );
                    continue;
                }
            };
            tables.push(table);
        }
        info!(
            "Static router found {} tables on iceberg catalog from {} tables declared",
            tables.len(),
            declared_tables.len()
        );
        if tables.is_empty() {
            error!("No valid tables found. Can't initiate Iceberg connector");
            return Err(Error::InvalidConfig);
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
        let data: Vec<Payload> = messages
            .into_iter()
            .map(|m: ConsumedMessage| m.payload)
            .collect();

        for table in &self.tables {
            write_data(
                &data,
                table,
                self.catalog.as_ref(),
                messages_metadata.schema,
            )
            .await?;
            info!(
                "Routed {} messages to iceberg table {} successfully",
                data.len(),
                table.identifier().name()
            );
        }

        Ok(())
    }
}
