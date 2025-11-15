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

use super::{Error, IcebergSinkConfig, IcebergSinkTypes};
use crate::props::init_props;
use iceberg::Catalog;
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::collections::HashMap;
use tracing::error;

pub async fn init_catalog(config: &IcebergSinkConfig) -> Result<Box<dyn Catalog>, Error> {
    let props = init_props(config)?;
    match config.catalog_type {
        IcebergSinkTypes::REST => Ok(Box::new(get_rest_catalog(config, props))),
        IcebergSinkTypes::GLUE => Ok(Box::new(get_glue_catalog(config, props).await?)),
    }
}

#[inline(always)]
fn get_rest_catalog(config: &IcebergSinkConfig, props: HashMap<String, String>) -> RestCatalog {
    let catalog_config = RestCatalogConfig::builder()
        .uri(config.uri.clone())
        .props(props.clone())
        .warehouse(config.warehouse.clone())
        .build();

    RestCatalog::new(catalog_config)
}

#[inline(always)]
async fn get_glue_catalog(
    config: &IcebergSinkConfig,
    props: HashMap<String, String>,
) -> Result<GlueCatalog, Error> {
    let config = GlueCatalogConfig::builder()
        .props(props.clone())
        .warehouse(config.warehouse.clone())
        .build();

    GlueCatalog::new(config).await.map_err(|err| {
            error!("Failed to get glue catalog with error: {}. Make sure the catalog is correctly declared on the config file", err);
            Error::InitError(err.to_string())
        })
}
