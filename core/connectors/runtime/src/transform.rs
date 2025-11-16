/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::RuntimeError;
use crate::configs::connectors::{SharedTransformConfig, TransformsConfig};
use iggy_connector_sdk::transforms::Transform;
use serde::Deserialize;
use std::sync::Arc;

pub fn load(config: &TransformsConfig) -> Result<Vec<Arc<dyn Transform>>, RuntimeError> {
    let mut transforms: Vec<Arc<dyn Transform>> = vec![];
    for (r#type, transform_config) in config.transforms.iter() {
        let shared_config = if transform_config.is_null() {
            SharedTransformConfig::default()
        } else {
            SharedTransformConfig::deserialize(transform_config).map_err(|error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Failed to parse transform config. {error}",
                ))
            })?
        };

        if !shared_config.enabled {
            continue;
        }

        let transform = iggy_connector_sdk::transforms::from_config(*r#type, transform_config)?;
        transforms.push(transform);
    }

    Ok(transforms)
}
