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
pub mod flatbuffer_convert;
pub mod json;
pub mod proto_convert;
mod update_fields;
use crate::{DecodedMessage, Error, TopicMetadata};
pub use add_fields::{AddFields, AddFieldsConfig, Field as AddField};
pub use delete_fields::{DeleteFields, DeleteFieldsConfig};
pub use filter_fields::{
    FilterFields, FilterFieldsConfig, FilterPattern, KeyPattern as FilterKeyPattern,
    ValuePattern as FilterValuePattern,
};
pub use flatbuffer_convert::{FlatBufferConvert, FlatBufferConvertConfig};
pub use proto_convert::{ProtoConvert, ProtoConvertConfig};
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::sync::Arc;
use strum_macros::{Display, IntoStaticStr};
pub use update_fields::{Field as UpdateField, UpdateCondition, UpdateFields, UpdateFieldsConfig};

/// The value of a field, either static or computed at runtime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldValue {
    Static(OwnedValue),
    Computed(ComputedValue),
}

/// Types of computed values that can be generated at runtime
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Display, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
pub enum ComputedValue {
    #[strum(to_string = "date_time")]
    DateTime,
    #[strum(to_string = "timestamp_nanos")]
    TimestampNanos,
    #[strum(to_string = "timestamp_micros")]
    TimestampMicros,
    #[strum(to_string = "timestamp_millis")]
    TimestampMillis,
    #[strum(to_string = "timestamp_seconds")]
    TimestampSeconds,
    #[strum(to_string = "uuid_v4")]
    UuidV4,
    #[strum(to_string = "uuid_v7")]
    UuidV7,
}

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
    ProtoConvert,
    FlatBufferConvert,
}

pub fn from_config(
    transform: TransformType,
    raw: &serde_json::Value,
) -> Result<Arc<dyn Transform>, Error> {
    match transform {
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
        TransformType::ProtoConvert => {
            let cfg: proto_convert::ProtoConvertConfig =
                serde_json::from_value(raw.clone()).map_err(|_| Error::InvalidConfig)?;
            Ok(Arc::new(proto_convert::ProtoConvert::new(cfg)))
        }
        TransformType::FlatBufferConvert => {
            let cfg: FlatBufferConvertConfig =
                serde_json::from_value(raw.clone()).map_err(|_| Error::InvalidConfig)?;
            Ok(Arc::new(FlatBufferConvert::new(cfg)))
        }
    }
}
