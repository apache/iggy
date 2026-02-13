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

use super::{LaneSpec, Scenario, TargetNamespace, lane};
use crate::ops::OpKind;
use async_trait::async_trait;

pub struct MixedWorkload;

#[async_trait]
impl Scenario for MixedWorkload {
    fn name(&self) -> &'static str {
        "mixed-workload"
    }

    fn describe(&self) -> &'static str {
        "Full chaos: all operation types interleaved.\n\n\
         All workers independently generate a weighted mix of every operation type: \
         create/delete streams and topics, send/poll messages, purge, partition \
         management, consumer groups, and consumer offsets.\n\n\
         Default weights: SendMessages 40%, PollMessages 25%, \
         CreateStream/Topic 10%, DeleteStream/Topic 8%, \
         Purge/Segments 7%, ConsumerGroup 5%, Offsets 5%."
    }

    fn lanes(&self) -> Vec<LaneSpec> {
        vec![
            lane("data-plane")
                .ops(&[(OpKind::SendMessages, 60.0), (OpKind::PollMessages, 40.0)])
                .build(),
            lane("control-plane")
                .ops(&[
                    (OpKind::CreateStream, 10.0),
                    (OpKind::DeleteStream, 8.0),
                    (OpKind::PurgeStream, 4.0),
                    (OpKind::CreateTopic, 10.0),
                    (OpKind::DeleteTopic, 8.0),
                    (OpKind::PurgeTopic, 4.0),
                    (OpKind::DeleteSegments, 6.0),
                    (OpKind::CreatePartitions, 4.0),
                    (OpKind::DeletePartitions, 2.0),
                    (OpKind::CreateConsumerGroup, 6.0),
                    (OpKind::DeleteConsumerGroup, 4.0),
                    (OpKind::StoreConsumerOffset, 4.0),
                ])
                .workers(4)
                .namespace(TargetNamespace::Churn)
                .destructive()
                .build(),
            lane("admin")
                .ops(&[
                    (OpKind::GetStreams, 40.0),
                    (OpKind::GetStreamDetails, 30.0),
                    (OpKind::GetTopicDetails, 20.0),
                    (OpKind::GetStats, 10.0),
                ])
                .workers(2)
                .build(),
        ]
    }
}
