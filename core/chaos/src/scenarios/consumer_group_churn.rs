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
    TopicDefaults, VerifyContext, lane,
};
use crate::invariants::{self, ConsumerGroupHealthCheck, InvariantViolation};
use crate::ops::OpKind;
use crate::safe_name::SafeResourceName;
use async_trait::async_trait;
use iggy::prelude::*;
use std::time::Duration;

const STREAMS: u32 = 2;
const TOPICS_PER_STREAM: u32 = 1;
const PARTITIONS_PER_TOPIC: u32 = 6;
const GROUPS_PER_TOPIC: u32 = 1;
const FRESH_VERIFY_MEMBERS: u32 = 3;
const REBALANCE_TIMEOUT: Duration = Duration::from_secs(10);
const SENTINEL_PAYLOAD: &str = "cg-churn-sentinel";

pub struct ConsumerGroupChurn;

struct SetupResource {
    stream_name: SafeResourceName,
    topic_name: SafeResourceName,
    group_name: SafeResourceName,
}

impl ConsumerGroupChurn {
    fn setup_resources(prefixes: &NamespacePrefixes) -> Vec<SetupResource> {
        let mut resources =
            Vec::with_capacity((STREAMS * TOPICS_PER_STREAM * GROUPS_PER_TOPIC) as usize);
        for i in 0..STREAMS {
            let stream_name =
                SafeResourceName::new(&prefixes.stable, &format!("cg-churn-stream-{i}"));
            for j in 0..TOPICS_PER_STREAM {
                let topic_name =
                    SafeResourceName::new(&prefixes.base, &format!("cg-churn-topic-{j}"));
                for k in 0..GROUPS_PER_TOPIC {
                    let group_name =
                        SafeResourceName::new(&prefixes.base, &format!("cg-churn-group-{k}"));
                    resources.push(SetupResource {
                        stream_name: stream_name.clone(),
                        topic_name: topic_name.clone(),
                        group_name,
                    });
                }
            }
        }
        resources
    }
}

#[async_trait]
impl Scenario for ConsumerGroupChurn {
    fn name(&self) -> &'static str {
        "consumer-group-churn"
    }

    fn describe(&self) -> &'static str {
        "Consumer group rebalancing under concurrent membership churn.\n\n\
         Setup: creates 2 streams, each with 1 topic (6 partitions) and 1 consumer group.\n\
         Sends a sentinel message to partition 1 of each topic.\n\n\
         Lanes:\n\
         - stable-members (global workers): poll/offset/join/get - workers that stay joined \
         and experience partition reassignment as churn happens beneath them.\n\
         - producers (2 workers): continuous message production to keep partitions active.\n\
         - membership-churn (2 workers, 5 ops/sec): join/leave at 50/50 - continuous \
         rebalancing pressure on the groups.\n\
         - observers (1 worker): read-only monitoring via get_consumer_group/topic/stream.\n\n\
         Verify: joins fresh members, confirms rebalance completes with unique partition \
         assignments, polls concurrently, asserts exactly one member receives sentinel.\n\n\
         Requires: server with heartbeat.enabled=true and a short heartbeat.interval (e.g. 2s)."
    }

    async fn setup(
        &self,
        client: &IggyClient,
        prefixes: &NamespacePrefixes,
    ) -> Result<(), IggyError> {
        let setup = Setup::new(client, prefixes);
        let defaults = TopicDefaults {
            partitions: PARTITIONS_PER_TOPIC,
            ..Default::default()
        };
        for i in 0..STREAMS {
            let stream = setup.create_stream(&format!("cg-churn-stream-{i}")).await?;
            for j in 0..TOPICS_PER_STREAM {
                let topic = setup
                    .create_topic(&stream, &format!("cg-churn-topic-{j}"), &defaults)
                    .await?;
                for k in 0..GROUPS_PER_TOPIC {
                    setup
                        .create_consumer_group(&stream, &topic, &format!("cg-churn-group-{k}"))
                        .await?;
                }
                setup.send_str(&stream, &topic, 1, SENTINEL_PAYLOAD).await?;
            }
        }
        Ok(())
    }

    fn lanes(&self) -> Vec<LaneSpec> {
        vec![
            lane("stable-members")
                .ops(&[
                    (OpKind::PollGroupMessages, 75.0),
                    (OpKind::StoreConsumerOffset, 15.0),
                    (OpKind::JoinConsumerGroup, 5.0),
                    (OpKind::GetConsumerGroup, 5.0),
                ])
                .scope(OriginScope::SetupOnly)
                .namespace(TargetNamespace::Stable)
                .build(),
            lane("producers")
                .op(OpKind::SendMessages, 100.0)
                .workers(2)
                .scope(OriginScope::SetupOnly)
                .namespace(TargetNamespace::Stable)
                .build(),
            lane("membership-churn")
                .ops(&[
                    (OpKind::JoinConsumerGroup, 50.0),
                    (OpKind::LeaveConsumerGroup, 50.0),
                ])
                .workers(2)
                .scope(OriginScope::SetupOnly)
                .namespace(TargetNamespace::Stable)
                .rate(RateModel::TargetOpsPerSec(5))
                .build(),
            lane("observers")
                .ops(&[
                    (OpKind::GetConsumerGroup, 50.0),
                    (OpKind::GetTopicDetails, 30.0),
                    (OpKind::GetStreamDetails, 20.0),
                ])
                .workers(1)
                .scope(OriginScope::SetupOnly)
                .namespace(TargetNamespace::Stable)
                .build(),
        ]
    }

    async fn verify(&self, ctx: &VerifyContext<'_>) -> Vec<InvariantViolation> {
        let mut violations =
            invariants::post_run_verify(ctx.client, ctx.shadow, ctx.prefixes).await;
        violations.extend(
            invariants::cross_client_consistency(ctx.server_address, ctx.transport, ctx.prefixes)
                .await,
        );

        let resources = Self::setup_resources(ctx.prefixes);
        for res in &resources {
            let stream_id = Identifier::from_str_value(&res.stream_name).unwrap();
            let topic_id = Identifier::from_str_value(&res.topic_name).unwrap();
            let group_id = Identifier::from_str_value(&res.group_name).unwrap();
            let label = format!("{}/{}/{}", res.stream_name, res.topic_name, res.group_name);

            let health_check = ConsumerGroupHealthCheck {
                label: &label,
                server_address: ctx.server_address,
                transport: ctx.transport,
                admin_client: ctx.client,
                stream_id: &stream_id,
                topic_id: &topic_id,
                group_id: &group_id,
                fresh_member_count: FRESH_VERIFY_MEMBERS,
                rebalance_timeout: REBALANCE_TIMEOUT,
                sentinel_payload: SENTINEL_PAYLOAD.as_bytes(),
            };

            violations.extend(health_check.run().await);
        }

        violations
    }
}
