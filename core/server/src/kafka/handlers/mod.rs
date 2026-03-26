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

mod api_versions;
mod create_topics;
mod delete_topics;
mod fetch;
mod find_coordinator;
mod list_offsets;
mod metadata;
mod produce;

use crate::configs::kafka::KafkaConfig;
use crate::kafka::protocol::codec::write_i16;
use crate::kafka::protocol::{self, RequestHeader};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_common::IggyError;
use std::rc::Rc;
use tracing::warn;

const UNSUPPORTED_API_KEY: i16 = 35;

pub async fn dispatch(
    shard: &Rc<IggyShard>,
    session: &Session,
    config: &KafkaConfig,
    header: &RequestHeader,
    body: &[u8],
) -> Result<Vec<u8>, IggyError> {
    let response = match header.api_key {
        protocol::API_VERSIONS => api_versions::handle(header),
        protocol::METADATA => metadata::handle(shard, config, body).await,
        protocol::FIND_COORDINATOR => find_coordinator::handle(shard, config),
        protocol::PRODUCE => produce::handle(shard, config, body).await,
        protocol::FETCH => fetch::handle(shard, session, config, body).await,
        protocol::LIST_OFFSETS => list_offsets::handle(shard, config, body),
        protocol::CREATE_TOPICS => create_topics::handle(shard, config, body).await,
        protocol::DELETE_TOPICS => delete_topics::handle(shard, config, body).await,
        _ => {
            warn!(
                "Unsupported Kafka API key: {} from client {}",
                header.api_key, session.client_id
            );
            let mut buf = Vec::new();
            write_i16(&mut buf, UNSUPPORTED_API_KEY);
            buf
        }
    };
    Ok(response)
}
