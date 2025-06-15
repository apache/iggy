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

use super::{Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use strum_macros::{Display, IntoStaticStr};

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

/// Computes a value based on the specified computed value type
pub fn compute_value(kind: &ComputedValue) -> OwnedValue {
    let now = Utc::now();
    match kind {
        ComputedValue::DateTime => now.to_rfc3339().into(),
        ComputedValue::TimestampNanos => now.timestamp_nanos_opt().unwrap().into(),
        ComputedValue::TimestampMicros => now.timestamp_micros().into(),
        ComputedValue::TimestampMillis => now.timestamp_millis().into(),
        ComputedValue::TimestampSeconds => now.timestamp().into(),
        ComputedValue::UuidV4 => uuid::Uuid::new_v4().to_string().into(),
        ComputedValue::UuidV7 => uuid::Uuid::now_v7().to_string().into(),
    }
}

/// A field to be updated in messages
#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: FieldValue,
    #[serde(default)]
    pub condition: Option<UpdateCondition>,
}

/// Configuration for the UpdateFields transform
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateFieldsConfig {
    #[serde(default)]
    pub fields: Vec<Field>,
}

/// Conditions that determine when a field should be updated
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateCondition {
    Always,
    KeyExists,
    KeyNotExists,
}

/// Transform that updates fields in JSON messages based on conditions
pub struct UpdateFields {
    pub fields: Vec<Field>,
}

impl UpdateFields {
    pub fn new(cfg: UpdateFieldsConfig) -> Self {
        Self { fields: cfg.fields }
    }
}

impl Transform for UpdateFields {
    fn r#type(&self) -> TransformType {
        TransformType::UpdateFields
    }

    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if self.fields.is_empty() {
            return Ok(Some(message));
        }

        match &message.payload {
            Payload::Json(_) => self.transform_json(metadata, message),
            _ => Ok(Some(message)),
        }
    }
}
