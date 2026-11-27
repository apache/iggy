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

use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::geo_consumer::{
    ClientCatalog, DEFAULT_DISCOVERY_INTERVAL, GeoSources, IggyGeoConsumer,
};
use iggy_common::NonZeroIggyDuration;
use std::sync::Arc;
use std::time::Duration;

/// Builds an [`IggyGeoConsumer`]. Get one from a consumer group's builder with
/// [`IggyConsumerBuilder::with_shadows`]: the options set there apply to the consumer of each
/// topic that the geo consumer reads.
#[derive(Debug)]
pub struct IggyGeoConsumerBuilder {
    consumer: IggyConsumerBuilder,
    sources: GeoSources,
    discovery_interval: Duration,
}

impl IggyGeoConsumerBuilder {
    pub(crate) fn new(consumer: IggyConsumerBuilder) -> Self {
        Self {
            consumer,
            sources: GeoSources::default(),
            discovery_interval: DEFAULT_DISCOVERY_INTERVAL,
        }
    }

    /// Sets the topics that the consumer reads: the topic in this cluster, its shadow topics, or
    /// both. [`GeoSources::Both`] by default.
    pub fn sources(self, sources: GeoSources) -> Self {
        Self { sources, ..self }
    }

    /// Sets how often the consumer looks for shadow topics that appeared or went away. Five
    /// seconds by default. [`IggyGeoConsumer::init`] refuses an interval too long to schedule.
    pub fn discovery_interval(self, interval: NonZeroIggyDuration) -> Self {
        Self {
            discovery_interval: interval.get_duration(),
            ..self
        }
    }

    /// Builds the consumer. Call [`IggyGeoConsumer::init`] before consuming.
    pub fn build(self) -> IggyGeoConsumer {
        IggyGeoConsumer::new(
            self.consumer.consumer_name().to_owned(),
            self.consumer.stream_id().clone(),
            self.consumer.topic_id().clone(),
            self.sources,
            self.discovery_interval,
            Arc::new(ClientCatalog::new(self.consumer, self.discovery_interval)),
        )
    }
}
