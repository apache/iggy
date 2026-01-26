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
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};

use std::collections::HashMap;

pub async fn init_catalog(config: &IcebergSinkConfig) -> Result<Box<dyn Catalog>, Error> {
    let props = init_props(config)?;
    match config.catalog_type {
        IcebergSinkTypes::REST => get_rest_catalog(config, props).await,
    }
}

#[inline(always)]
async fn get_rest_catalog(
    config: &IcebergSinkConfig,
    props: HashMap<String, String>,
) -> Result<Box<dyn Catalog>, Error> {
    let mut new_props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), config.uri.clone()),
        (
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            config.warehouse.clone(),
        ),
    ]);
    new_props.extend(props);

    let catalog = RestCatalogBuilder::default()
        .load("rest", new_props)
        .await
        .map_err(|err| {
            let error = format!("Failed to initialize REST catalog: {}", err);
            Error::InitError(error)
        })?;

    Ok(Box::new(catalog))
}
