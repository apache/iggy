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

pub struct SegmentRotation;

#[async_trait]
impl Scenario for SegmentRotation {
    fn name(&self) -> &'static str {
        "segment-rotation"
    }

    fn describe(&self) -> &'static str {
        "Segment rotation stress test.\n\n\
         Phase 1: Setup — creates 1 stream with 1 topic and 1 partition.\n\
         Phase 2: Chaos — all workers send small messages to the same partition \
         while concurrently polling.\n\
         Phase 3: Verify — offset monotonicity, no gaps, watermark violations.\n\n\
         Targets segment rotation logic by producing high message throughput \
         to a single partition."
    }

    async fn setup(
        &self,
        client: &IggyClient,
        prefixes: &NamespacePrefixes,
    ) -> Result<(), IggyError> {
        tracing::info!("Setup: creating 1 stream with 1 topic and 1 partition...");
        let stream_name = SafeResourceName::new(&prefixes.stable, "seg-rot-stream");
        client.create_stream(&stream_name).await?;
        let stream_id = Identifier::from_str_value(&stream_name).unwrap();
        let topic_name = SafeResourceName::new(&prefixes.base, "seg-rot-topic");
        client
            .create_topic(
                &stream_id,
                &topic_name,
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::Unlimited,
            )
            .await?;
        Ok(())
    }

    fn lanes(&self) -> Vec<LaneSpec> {
        vec![
            LaneSpec {
                label: "data-plane",
                op_weights: vec![(OpKind::SendMessages, 55.0), (OpKind::PollMessages, 45.0)],
                worker_count: None,
                scope: NamespaceScope::SetupOnly,
                namespace: LaneNamespace::Stable,
                rate: RateModel::Unbounded,
                destructive: false,
            },
            LaneSpec {
                label: "segment-cleanup",
                op_weights: vec![(OpKind::DeleteSegments, 100.0)],
                worker_count: Some(1),
                scope: NamespaceScope::SetupOnly,
                namespace: LaneNamespace::Stable,
                rate: RateModel::TargetOpsPerSec(2),
                destructive: true,
            },
        ]
    }
}
