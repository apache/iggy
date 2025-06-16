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

use super::{FieldValue, Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use serde::{Deserialize, Serialize};

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
