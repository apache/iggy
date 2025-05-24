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

use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use strum_macros::{Display, IntoStaticStr};

use crate::{DecodedMessage, Error, Payload, TopicMetadata};

use super::{Transform, TransformType};

#[derive(Debug, Serialize, Deserialize)]
pub struct AddFieldsConfig {
    fields: Vec<Field>,
}

pub struct AddFields {
    fields: Vec<Field>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Field {
    key: String,
    value: FieldValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FieldValue {
    Static(simd_json::OwnedValue),
    Computed(ComputedValue),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Display, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
enum ComputedValue {
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

impl AddFields {
    pub fn new(config: AddFieldsConfig) -> Self {
        Self {
            fields: config.fields,
        }
    }
}

impl Transform for AddFields {
    fn r#type(&self) -> TransformType {
        TransformType::AddFields
    }

    fn transform(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if self.fields.is_empty() {
            return Ok(Some(message));
        }

        let Payload::Json(OwnedValue::Object(ref mut map)) = message.payload else {
            return Ok(Some(message));
        };

        for field in &self.fields {
            match &field.value {
                FieldValue::Static(value) => map.insert(field.key.clone(), value.clone()),
                FieldValue::Computed(value) => match value {
                    ComputedValue::DateTime => {
                        map.insert(field.key.clone(), chrono::Utc::now().to_rfc3339().into())
                    }
                    ComputedValue::TimestampMillis => map.insert(
                        field.key.clone(),
                        chrono::Utc::now().timestamp_millis().into(),
                    ),
                    ComputedValue::TimestampMicros => map.insert(
                        field.key.clone(),
                        chrono::Utc::now().timestamp_micros().into(),
                    ),
                    ComputedValue::TimestampNanos => map.insert(
                        field.key.clone(),
                        chrono::Utc::now().timestamp_nanos_opt().into(),
                    ),
                    ComputedValue::TimestampSeconds => {
                        map.insert(field.key.clone(), chrono::Utc::now().timestamp().into())
                    }
                    ComputedValue::UuidV4 => {
                        map.insert(field.key.clone(), uuid::Uuid::new_v4().to_string().into())
                    }
                    ComputedValue::UuidV7 => {
                        map.insert(field.key.clone(), uuid::Uuid::now_v7().to_string().into())
                    }
                },
            };
        }

        Ok(Some(message))
    }
}
