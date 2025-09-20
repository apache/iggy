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

use async_trait::async_trait;

use iceberg::table::Table;
use iceberg::Catalog;
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iggy_connector_sdk::{
    sink_connector, ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::router::{DynamicRouter, Router, StaticRouter};

mod router;

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
    props: HashMap<String, String>,
    router: Option<Box<dyn Router>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSinkConfig {
    pub tables: Vec<String>,
    pub catalog_type: IcebergSinkTypes,
    pub warehouse: String,
    pub uri: String,
    pub auto_create: bool,
    pub evolve_schema: bool,
    pub dynamic_routing: bool,
    pub dynamic_route_field: String,
    pub store_url: String,
    pub store_access_key_id: String,
    pub store_secret_access_key: String,
    pub store_region: String,
    pub store_class: IcebergSinkStoreClass,
}

pub(self) fn slice_user_table(table: &String) -> Vec<String> {
    table.split('.').map(|s| s.to_string()).collect()
}

pub(self) async fn create_table(name: String) -> Result<Table, Error> {
    let table = Table::builder()
        .build()
        .map_err(|err| Error::InvalidState)?;
    return Ok(table);
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
        let props = HashMap::new();
        let router = None;

        IcebergSink {
            id,
            config,
            router,
            props,
        }
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

        if self.config.dynamic_routing {
            self.router = Some(Box::new(DynamicRouter::new(
                catalog,
                self.config.dynamic_route_field.clone(),
            )))
        } else {
            self.router = Some(Box::new(
                StaticRouter::new(catalog, &self.config.tables, self.config.auto_create).await?,
            ));
        }

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

        match &self.router {
            Some(router) => router.route_data(messages_metadata, messages).await?,
            None => {
                error!("Iceberg connector has no router configured");
                return Err(Error::InvalidConfig);
            }
        };

        info!("Finished successfully");

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Iceberg sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
