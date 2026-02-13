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

use super::{LaneNamespace, LaneSpec, NamespacePrefixes, NamespaceScope, RateModel, Scenario};
use crate::ops::OpKind;
use crate::safe_name::SafeResourceName;
use async_trait::async_trait;
use iggy::prelude::*;

const SETUP_STREAMS: u32 = 5;
const TOPICS_PER_STREAM: u32 = 2;
const PARTITIONS_PER_TOPIC: u32 = 3;

pub struct ConcurrentCrud;

#[async_trait]
impl Scenario for ConcurrentCrud {
    fn name(&self) -> &'static str {
        "concurrent-crud"
    }

    fn describe(&self) -> &'static str {
        "Concurrent stream and topic CRUD operations.\n\n\
         Phase 1: Setup — creates 5 streams with 2 topics each.\n\
         Phase 2: Chaos — N workers run weighted create/delete/purge ops.\n\
         Phase 3: Verify — post-run invariant checks.\n\n\
         Focuses on detecting race conditions in metadata operations \
         (create-while-deleting, purge-while-creating, etc.)."
    }

    async fn setup(
        &self,
        client: &IggyClient,
        prefixes: &NamespacePrefixes,
    ) -> Result<(), IggyError> {
        tracing::info!(
            "Setup: creating {SETUP_STREAMS} streams with {TOPICS_PER_STREAM} topics each..."
        );
        for i in 0..SETUP_STREAMS {
            let stream_name = SafeResourceName::new(&prefixes.stable, &format!("setup-s{i}"));
            client.create_stream(&stream_name).await?;
            let stream_id = Identifier::from_str_value(&stream_name).unwrap();
            for j in 0..TOPICS_PER_STREAM {
                let topic_name = SafeResourceName::new(&prefixes.base, &format!("setup-t{i}-{j}"));
                client
                    .create_topic(
                        &stream_id,
                        &topic_name,
                        PARTITIONS_PER_TOPIC,
                        CompressionAlgorithm::None,
                        None,
                        IggyExpiry::NeverExpire,
                        MaxTopicSize::Unlimited,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    fn lanes(&self) -> Vec<LaneSpec> {
        vec![
            LaneSpec {
                label: "control-plane",
                op_weights: vec![
                    (OpKind::CreateStream, 20.0),
                    (OpKind::DeleteStream, 15.0),
                    (OpKind::PurgeStream, 10.0),
                    (OpKind::CreateTopic, 20.0),
                    (OpKind::DeleteTopic, 15.0),
                    (OpKind::PurgeTopic, 10.0),
                ],
                worker_count: None,
                scope: NamespaceScope::Any,
                namespace: LaneNamespace::Churn,
                rate: RateModel::Unbounded,
                destructive: true,
            },
            LaneSpec {
                label: "data-plane",
                op_weights: vec![(OpKind::SendMessages, 60.0), (OpKind::PollMessages, 40.0)],
                worker_count: Some(2),
                scope: NamespaceScope::SetupOnly,
                namespace: LaneNamespace::Stable,
                rate: RateModel::Unbounded,
                destructive: false,
            },
        ]
    }
}
