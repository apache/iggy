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

use std::sync::Arc;

use add_fields::AddFields;
use delete_fields::DeleteFields;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use strum_macros::{Display, IntoStaticStr};
use tracing::error;

use crate::{DecodedMessage, Error, TopicMetadata};

pub mod add_fields;
pub mod delete_fields;

/// Fields transformation trait.
pub trait Transform: Send + Sync {
    /// Returns the type of the transform.
    fn r#type(&self) -> TransformType;

    /// Transforms the message, given the format is supported and the transform is applicable.
    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error>;
}

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Display, IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
pub enum TransformType {
    #[strum(to_string = "add_fields")]
    AddFields,
    #[strum(to_string = "delete_fields")]
    DeleteFields,
}

pub fn load(r#type: TransformType, config: serde_json::Value) -> Result<Arc<dyn Transform>, Error> {
    Ok(match r#type {
        TransformType::AddFields => Arc::new(AddFields::new(as_config(r#type, config)?)),
        TransformType::DeleteFields => Arc::new(DeleteFields::new(as_config(r#type, config)?)),
    })
}

fn as_config<T: DeserializeOwned>(
    r#type: TransformType,
    config: serde_json::Value,
) -> Result<T, Error> {
    serde_json::from_value::<T>(config).map_err(|error| {
        error!("Failed to deserialize config for transform: {type}. {error}");
        Error::InvalidConfig
    })
}
