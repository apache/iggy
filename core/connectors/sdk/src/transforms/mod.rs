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

mod add_fields;
mod delete_fields;
mod filter_fields;
pub mod json;
mod update_fields;
use crate::{DecodedMessage, Error, TopicMetadata};
pub use add_fields::{
    AddFields, AddFieldsConfig, ComputedValue as AddComputedValue, Field as AddField,
    FieldValue as AddFieldValue,
};
pub use delete_fields::{DeleteFields, DeleteFieldsConfig};
pub use filter_fields::{
    FilterFields, FilterFieldsConfig, FilterPattern, KeyPattern as FilterKeyPattern,
    ValuePattern as FilterValuePattern,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strum_macros::{Display, IntoStaticStr};
pub use update_fields::{
    ComputedValue as UpdateComputedValue, Field as UpdateField, FieldValue as UpdateFieldValue,
    UpdateCondition, UpdateFields, UpdateFieldsConfig,
};

pub trait Transform: Send + Sync {
    fn r#type(&self) -> TransformType;
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
    AddFields,
    DeleteFields,
    FilterFields,
    UpdateFields,
}

pub fn from_config(t: TransformType, raw: &serde_json::Value) -> Result<Arc<dyn Transform>, Error> {
    match t {
        TransformType::AddFields => {
            let cfg: AddFieldsConfig =
                serde_json::from_value(raw.clone()).map_err(|_| Error::InvalidConfig)?;
            Ok(Arc::new(AddFields::new(cfg)))
        }
        TransformType::DeleteFields => {
            let cfg: DeleteFieldsConfig =
                serde_json::from_value(raw.clone()).map_err(|_| Error::InvalidConfig)?;
            Ok(Arc::new(DeleteFields::new(cfg)))
        }
        TransformType::FilterFields => {
            let cfg: FilterFieldsConfig =
                serde_json::from_value(raw.clone()).map_err(|_| Error::InvalidConfig)?;
            Ok(Arc::new(FilterFields::new(cfg)?))
        }
        TransformType::UpdateFields => {
            let cfg: UpdateFieldsConfig =
                serde_json::from_value(raw.clone()).map_err(|_| Error::InvalidConfig)?;
            Ok(Arc::new(UpdateFields::new(cfg)))
        }
    }
}
