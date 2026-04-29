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

/// Configuration for the UnwrapEnvelope transform.
///
/// Extracts a nested JSON field from an envelope object and promotes it
/// to the top-level payload. For example, given a Postgres source
/// `DatabaseRecord` with shape `{ table_name, operation_type, timestamp,
/// data: { ... }, old_data }`, setting `field = "data"` replaces the
/// entire payload with the contents of `data`.
#[derive(Debug, Serialize, Deserialize)]
pub struct UnwrapEnvelopeConfig {
    pub field: String,
}

/// Transform that extracts a nested field from a JSON envelope and
/// promotes it as the top-level payload.
pub struct UnwrapEnvelope {
    pub field: String,
}

impl UnwrapEnvelope {
    pub fn new(cfg: UnwrapEnvelopeConfig) -> Self {
        Self { field: cfg.field }
    }
}

impl Transform for UnwrapEnvelope {
    fn r#type(&self) -> TransformType {
        TransformType::UnwrapEnvelope
    }

    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        match &message.payload {
            Payload::Json(_) => self.transform_json(metadata, message),
            _ => Ok(Some(message)),
        }
    }
}
