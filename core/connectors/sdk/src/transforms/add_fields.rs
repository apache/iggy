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

/// A field to be added to messages
#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: FieldValue,
}

/// Configuration for the AddFields transform
#[derive(Debug, Serialize, Deserialize)]
pub struct AddFieldsConfig {
    #[serde(default)]
    pub fields: Vec<Field>,
}

/// Transform that adds fields to JSON messages
pub struct AddFields {
    pub fields: Vec<Field>,
}

impl AddFields {
    pub fn new(cfg: AddFieldsConfig) -> Self {
        Self { fields: cfg.fields }
    }
}

impl Transform for AddFields {
    fn r#type(&self) -> TransformType {
        TransformType::AddFields
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
