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

use super::{
    LaneSpec, NamespacePrefixes, OriginScope, Scenario, Setup, TargetNamespace, TopicDefaults, lane,
};
use crate::ops::OpKind;
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
         Phase 1: Setup - creates 5 streams with 2 topics each.\n\
         Phase 2: Chaos - N workers run weighted create/delete/purge ops.\n\
         Phase 3: Verify - post-run invariant checks.\n\n\
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
        let setup = Setup::new(client, prefixes);
        let defaults = TopicDefaults {
            partitions: PARTITIONS_PER_TOPIC,
            ..Default::default()
        };
        for i in 0..SETUP_STREAMS {
            let stream = setup.create_stream(&format!("setup-s{i}")).await?;
            for j in 0..TOPICS_PER_STREAM {
                setup
                    .create_topic(&stream, &format!("setup-t{i}-{j}"), &defaults)
                    .await?;
            }
        }
        Ok(())
    }

    fn lanes(&self) -> Vec<LaneSpec> {
        vec![
            lane("control-plane")
                .ops(&[
                    (OpKind::CreateStream, 20.0),
                    (OpKind::DeleteStream, 15.0),
                    (OpKind::PurgeStream, 10.0),
                    (OpKind::CreateTopic, 20.0),
                    (OpKind::DeleteTopic, 15.0),
                    (OpKind::PurgeTopic, 10.0),
                ])
                .namespace(TargetNamespace::Churn)
                .destructive()
                .build(),
            lane("data-plane")
                .ops(&[(OpKind::SendMessages, 60.0), (OpKind::PollMessages, 40.0)])
                .workers(2)
                .scope(OriginScope::SetupOnly)
                .namespace(TargetNamespace::Stable)
                .build(),
        ]
    }
}
