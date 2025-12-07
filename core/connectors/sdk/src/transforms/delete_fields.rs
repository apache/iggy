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
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use std::collections::HashSet;

/// Configuration for the DeleteFields transform
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteFieldsConfig {
    #[serde(default)]
    pub fields: Vec<String>,
}

/// Transform that removes specified fields from JSON messages
pub struct DeleteFields {
    pub fields: HashSet<String>,
}

impl DeleteFields {
    pub fn new(cfg: DeleteFieldsConfig) -> Self {
        Self {
            fields: cfg.fields.into_iter().collect(),
        }
    }

    pub fn should_remove(&self, k: &str, _v: &OwnedValue) -> bool {
        self.fields.contains(k)
    }
}

impl Transform for DeleteFields {
    fn r#type(&self) -> TransformType {
        TransformType::DeleteFields
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
