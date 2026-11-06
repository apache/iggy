// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    IcebergSink,
    catalog::init_catalog,
    router::{BatchOrigin, dynamic_router::DynamicRouter, static_router::StaticRouter},
};
use async_trait::async_trait;
use iceberg::Catalog;
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata};
use tracing::{debug, error, info};

#[async_trait]
impl Sink for IcebergSink {
    async fn open(&mut self) -> Result<(), Error> {
        if self.config.dynamic_routing && self.config.dynamic_route_field.trim().is_empty() {
            return Err(Error::InvalidConfigValue(
                "dynamic_route_field must not be empty in dynamic routing mode".to_string(),
            ));
        }
        if !self.config.dynamic_routing && self.config.tables.is_empty() {
            return Err(Error::InvalidConfigValue(
                "tables must not be empty in static routing mode".to_string(),
            ));
        }
        match (
            &self.config.store_access_key_id,
            &self.config.store_secret_access_key,
        ) {
            (Some(_), Some(_)) => {
                info!(
                    "Opened Iceberg sink connector with ID: {} for URL: {} with the configured store credentials",
                    self.id, self.config.uri
                );
            }
            (None, None) => {
                info!(
                    "Opened Iceberg sink connector with ID: {} for URL: {}. No explicit credentials provided, falling back to default credential provider chain",
                    self.id, self.config.uri
                );
            }
            _ => {
                return Err(Error::InvalidConfigValue(
                    "Partially configured credentials. You must provide both store_access_key_id and store_secret_access_key, or omit both.".to_owned(),
                ));
            }
        }

        info!(
            "Configuring Iceberg catalog with the following config:\n-region: {}\n-url: {}\n-store class: {}\n-catalog type: {}\n",
            self.config.store_region,
            self.config.store_url,
            self.config.store_class,
            self.config.catalog_type
        );

        let catalog: Box<dyn Catalog> = init_catalog(&self.config).await?;
        let invalid_rows = self.config.invalid_rows.unwrap_or_default();

        if self.config.dynamic_routing {
            self.router = Some(Box::new(DynamicRouter::new(
                catalog,
                self.config.dynamic_route_field.clone(),
                invalid_rows,
                self.commit_retry,
            )))
        } else {
            self.router = Some(Box::new(
                StaticRouter::new(
                    catalog,
                    &self.config.tables,
                    invalid_rows,
                    self.commit_retry,
                )
                .await?,
            ));
        }

        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        debug!(
            "Iceberg sink with ID: {} received: {} messages, format: {}",
            self.id,
            messages.len(),
            messages_metadata.schema
        );

        let origin = BatchOrigin {
            connector_id: self.id,
            stream: &topic_metadata.stream,
            topic: &topic_metadata.topic,
            partition_id: messages_metadata.partition_id,
        };
        match &self.router {
            Some(router) => {
                router
                    .route_data(&origin, messages_metadata.schema, messages)
                    .await?
            }
            None => {
                error!("Iceberg connector has no router configured");
                return Err(Error::InvalidConfig);
            }
        };

        debug!("Finished successfully");

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Iceberg sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
