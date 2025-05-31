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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;

use crate::{DecodedMessage, Error, Payload, TopicMetadata};

use super::{Transform, TransformType};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteFieldsConfig {
    fields: Vec<String>,
}

pub struct DeleteFields {
    fields: HashSet<String>,
}

impl DeleteFields {
    pub fn new(config: DeleteFieldsConfig) -> Self {
        Self {
            fields: config.fields.into_iter().collect(),
        }
    }
}

impl Transform for DeleteFields {
    fn r#type(&self) -> TransformType {
        TransformType::DeleteFields
    }

    fn transform(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if let Payload::Json(OwnedValue::Object(ref mut map)) = message.payload {
            map.retain(|key, _| !self.fields.contains(key));
        }

        Ok(Some(message))
    }
}
