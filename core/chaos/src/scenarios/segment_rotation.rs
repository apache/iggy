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
    LaneSpec, NamespacePrefixes, OriginScope, RateModel, Scenario, Setup, TargetNamespace,
    TopicDefaults, lane,
};
use crate::ops::OpKind;
use async_trait::async_trait;
use iggy::prelude::*;

pub struct SegmentRotation;

#[async_trait]
impl Scenario for SegmentRotation {
    fn name(&self) -> &'static str {
        "segment-rotation"
    }

    fn describe(&self) -> &'static str {
        "Segment rotation stress test.\n\n\
         Phase 1: Setup - creates 1 stream with 1 topic and 1 partition.\n\
         Phase 2: Chaos - all workers send small messages to the same partition \
         while concurrently polling.\n\
         Phase 3: Verify - offset monotonicity, no gaps, watermark violations.\n\n\
         Targets segment rotation logic by producing high message throughput \
         to a single partition."
    }

    async fn setup(
        &self,
        client: &IggyClient,
        prefixes: &NamespacePrefixes,
    ) -> Result<(), IggyError> {
        tracing::info!("Setup: creating 1 stream with 1 topic and 1 partition...");
        let setup = Setup::new(client, prefixes);
        let defaults = TopicDefaults {
            partitions: 1,
            ..Default::default()
        };
        let stream = setup.create_stream("seg-rot-stream").await?;
        setup
            .create_topic(&stream, "seg-rot-topic", &defaults)
            .await?;
        Ok(())
    }

    fn lanes(&self) -> Vec<LaneSpec> {
        vec![
            lane("data-plane")
                .ops(&[(OpKind::SendMessages, 55.0), (OpKind::PollMessages, 45.0)])
                .scope(OriginScope::SetupOnly)
                .namespace(TargetNamespace::Stable)
                .build(),
            lane("segment-cleanup")
                .op(OpKind::DeleteSegments, 100.0)
                .workers(1)
                .scope(OriginScope::SetupOnly)
                .namespace(TargetNamespace::Stable)
                .rate(RateModel::TargetOpsPerSec(2))
                .destructive()
                .build(),
        ]
    }
}
