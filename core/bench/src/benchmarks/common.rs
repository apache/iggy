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

use super::{CONSUMER_GROUP_BASE_ID, CONSUMER_GROUP_NAME_PREFIX};
use crate::{
    actors::{
        consumer::typed_benchmark_consumer::TypedBenchmarkConsumer,
        producer::typed_benchmark_producer::TypedBenchmarkProducer,
        producing_consumer::typed_banchmark_producing_consumer::TypedBenchmarkProducingConsumer,
    },
    args::common::IggyBenchArgs,
    utils::finish_condition::{BenchmarkFinishCondition, BenchmarkFinishConditionMode},
};
use bench_report::{benchmark_kind::BenchmarkKind, individual_metrics::BenchmarkIndividualMetrics};
use iggy::prelude::*;
use integration::test_server::{ClientFactory, login_root};
use std::{future::Future, sync::Arc};
use tracing::{error, info};

pub async fn create_consumer(
    client: &IggyClient,
    consumer_group_id: Option<&u32>,
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer_id: u32,
) -> Consumer {
    match consumer_group_id {
        Some(consumer_group_id) => {
            info!(
                "Consumer #{} → joining consumer group #{}",
                consumer_id, consumer_group_id
            );
            let cg_identifier = Identifier::try_from(*consumer_group_id).unwrap();
            client
                .join_consumer_group(stream_id, topic_id, &cg_identifier)
                .await
                .expect("Failed to join consumer group");
            Consumer::group(cg_identifier)
        }
        None => Consumer::new(consumer_id.try_into().unwrap()),
    }
}

pub fn rate_limit_per_actor(total_rate: Option<IggyByteSize>, actors: u32) -> Option<IggyByteSize> {
    total_rate.and_then(|rl| {
        let per_actor = rl.as_bytes_u64() / u64::from(actors);
        if per_actor > 0 {
            Some(per_actor.into())
        } else {
            None
        }
    })
}

#[allow(clippy::cognitive_complexity)]
pub async fn init_consumer_groups(
    client_factory: &Arc<dyn ClientFactory>,
    args: &IggyBenchArgs,
) -> Result<(), IggyError> {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);
    let cg_count = args.number_of_consumer_groups();

    login_root(&client).await;
    for i in 1..=cg_count {
        let consumer_group_id = CONSUMER_GROUP_BASE_ID + i;
        let stream_name = format!("bench-stream-{i}");
        let stream_id: Identifier = stream_name.as_str().try_into()?;
        let topic_id: Identifier = "topic-1".try_into()?;
        let consumer_group_name = format!("{CONSUMER_GROUP_NAME_PREFIX}-{consumer_group_id}");
        info!(
            "Creating test consumer group: name={},  stream={}, topic={}",
            consumer_group_name, stream_name, topic_id
        );
        if let Err(err) = client
            .create_consumer_group(&stream_id, &topic_id, &consumer_group_name)
            .await
        {
            error!("Error when creating consumer group {consumer_group_id}: {err}");
        }
    }
    Ok(())
}

pub fn build_producer_futures(
    client_factory: &Arc<dyn ClientFactory>,
    args: &IggyBenchArgs,
) -> Vec<impl Future<Output = Result<BenchmarkIndividualMetrics, IggyError>> + Send + use<>> {
    let streams = args.streams();
    let partitions = args.number_of_partitions();
    let producers = args.producers();
    let actors = args.producers() + args.consumers();
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let sampling_time = args.sampling_time();
    let moving_average_window = args.moving_average_window();
    let kind = args.kind();
    let shared_finish_condition =
        BenchmarkFinishCondition::new(args, BenchmarkFinishConditionMode::Shared);
    let rate_limit = rate_limit_per_actor(args.rate_limit(), actors);
    let use_high_level_api = args.high_level_api();

    (1..=producers)
        .map(|producer_id| {
            let client_factory = client_factory.clone();

            let finish_condition = if partitions > 1 {
                shared_finish_condition.clone()
            } else {
                BenchmarkFinishCondition::new(args, BenchmarkFinishConditionMode::PerProducer)
            };

            let stream_idx = 1 + ((producer_id - 1) % streams);
            let stream_id = format!("bench-stream-{stream_idx}");

            async move {
                let producer = TypedBenchmarkProducer::new(
                    use_high_level_api,
                    client_factory,
                    kind,
                    producer_id,
                    stream_id,
                    partitions,
                    messages_per_batch,
                    message_size,
                    finish_condition,
                    warmup_time,
                    sampling_time,
                    moving_average_window,
                    rate_limit,
                );
                producer.run().await
            }
        })
        .collect()
}

pub fn build_consumer_futures(
    client_factory: &Arc<dyn ClientFactory>,
    args: &IggyBenchArgs,
) -> Vec<impl Future<Output = Result<BenchmarkIndividualMetrics, IggyError>> + Send + use<>> {
    let cg_count = args.number_of_consumer_groups();
    let consumers = args.consumers();
    let actors = args.producers() + args.consumers();
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let sampling_time = args.sampling_time();
    let moving_average_window = args.moving_average_window();
    let kind = args.kind();
    let polling_kind = if cg_count > 0 {
        PollingKind::Next
    } else {
        PollingKind::Offset
    };
    let origin_timestamp_latency_calculation = match args.kind() {
        BenchmarkKind::PinnedConsumer
        | BenchmarkKind::PinnedProducerAndConsumer
        | BenchmarkKind::BalancedConsumerGroup => false, // TODO(hubcio): in future, PinnedProducerAndConsumer can also be true
        BenchmarkKind::BalancedProducerAndConsumerGroup => true,
        _ => unreachable!(),
    };

    let global_finish_condition =
        BenchmarkFinishCondition::new(args, BenchmarkFinishConditionMode::Shared);
    let rate_limit = rate_limit_per_actor(args.rate_limit(), actors);
    let use_high_level_api = args.high_level_api();

    (1..=consumers)
        .map(|consumer_id| {
            let client_factory = client_factory.clone();
            let finish_condition = if cg_count > 0 {
                global_finish_condition.clone()
            } else {
                BenchmarkFinishCondition::new(args, BenchmarkFinishConditionMode::PerConsumer)
            };
            let stream_idx = if cg_count > 0 {
                1 + ((consumer_id - 1) % cg_count)
            } else {
                consumer_id
            };
            let stream_id = format!("bench-stream-{stream_idx}");
            let consumer_group_id = if cg_count > 0 {
                Some(CONSUMER_GROUP_BASE_ID + 1 + (consumer_id % cg_count))
            } else {
                None
            };

            async move {
                let consumer = TypedBenchmarkConsumer::new(
                    use_high_level_api,
                    client_factory,
                    kind,
                    consumer_id,
                    consumer_group_id,
                    stream_id,
                    messages_per_batch,
                    finish_condition,
                    warmup_time,
                    sampling_time,
                    moving_average_window,
                    polling_kind,
                    rate_limit,
                    origin_timestamp_latency_calculation,
                );
                consumer.run().await
            }
        })
        .collect()
}

#[allow(clippy::needless_pass_by_value)]
pub fn build_producing_consumers_futures(
    client_factory: Arc<dyn ClientFactory>,
    args: Arc<IggyBenchArgs>,
) -> Vec<impl Future<Output = Result<BenchmarkIndividualMetrics, IggyError>> + Send> {
    let producing_consumers = args.producers();
    let streams = args.streams();
    let partitions = args.number_of_partitions();
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let polling_kind = PollingKind::Offset;

    (1..=producing_consumers)
        .map(|actor_id| {
            let client_factory_clone = client_factory.clone();
            let args_clone = args.clone();
            let stream_idx = 1 + ((actor_id - 1) % streams);
            let stream_id = format!("bench-stream-{stream_idx}");

            let send_finish_condition = BenchmarkFinishCondition::new(
                &args,
                BenchmarkFinishConditionMode::PerProducingConsumer,
            );
            let poll_finish_condition = BenchmarkFinishCondition::new(
                &args,
                BenchmarkFinishConditionMode::PerProducingConsumer,
            );
            let origin_timestamp_latency_calculation = match args.kind() {
                BenchmarkKind::PinnedConsumer
                | BenchmarkKind::PinnedProducerAndConsumer
                | BenchmarkKind::BalancedConsumerGroup  // TODO(hubcio): in future, PinnedProducerAndConsumer can also be true
                | BenchmarkKind::EndToEndProducingConsumer => false,
                BenchmarkKind::BalancedProducerAndConsumerGroup => true,
                _ => unreachable!(),
            };
            let use_high_level_api = args.high_level_api();
            let rate_limit = rate_limit_per_actor(args.rate_limit(), producing_consumers);

            async move {
                info!(
                    "Executing producing consumer #{}, stream_id={}",
                    actor_id, stream_id
                );
                let actor = TypedBenchmarkProducingConsumer::new(
                    use_high_level_api,
                    client_factory_clone,
                    args_clone.kind(),
                    actor_id,
                    None,
                    stream_id,
                    partitions,
                    messages_per_batch,
                    message_size,
                    send_finish_condition.clone(),
                    poll_finish_condition.clone(),
                    warmup_time,
                    args_clone.sampling_time(),
                    args_clone.moving_average_window(),
                    rate_limit,
                    polling_kind,
                    origin_timestamp_latency_calculation,
                );
                actor.run().await
            }
        })
        .collect()
}

#[allow(clippy::needless_pass_by_value)]
pub fn build_producing_consumer_groups_futures(
    client_factory: Arc<dyn ClientFactory>,
    args: Arc<IggyBenchArgs>,
) -> Vec<impl Future<Output = Result<BenchmarkIndividualMetrics, IggyError>> + Send> {
    let producers = args.producers();
    let consumers = args.consumers();
    let total_actors = producers.max(consumers);
    let partitions = args.number_of_partitions();
    let cg_count = args.number_of_consumer_groups();
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let start_consumer_group_id = CONSUMER_GROUP_BASE_ID;
    let polling_kind = PollingKind::Next;
    let use_high_level_api = args.high_level_api();
    let shared_send_finish_condition =
        BenchmarkFinishCondition::new(&args, BenchmarkFinishConditionMode::SharedHalf);

    let shared_poll_finish_condition =
        BenchmarkFinishCondition::new(&args, BenchmarkFinishConditionMode::SharedHalf);

    (1..=total_actors)
        .map(|actor_id| {
            let client_factory_clone = client_factory.clone();
            let args_clone = args.clone();
            let stream_idx = 1 + ((actor_id - 1) % cg_count);
            let stream_id = format!("bench-stream-{stream_idx}");

            let should_produce = actor_id <= producers;
            let should_consume = actor_id <= consumers;

            let send_finish_condition = if should_produce {
                shared_send_finish_condition.clone()
            } else {
                BenchmarkFinishCondition::new_empty()
            };

            let poll_finish_condition = if should_consume {
                shared_poll_finish_condition.clone()
            } else {
                BenchmarkFinishCondition::new_empty()
            };

            let rate_limit = if should_produce {
                rate_limit_per_actor(args.rate_limit(), producers)
            } else {
                None
            };

            let consumer_group_id = if should_consume {
                Some(start_consumer_group_id + 1 + (actor_id % cg_count))
            } else {
                None
            };
            let origin_timestamp_latency_calculation = match args.kind() {
                BenchmarkKind::PinnedConsumer
                | BenchmarkKind::PinnedProducerAndConsumer
                | BenchmarkKind::BalancedConsumerGroup  // TODO(hubcio): in future, PinnedProducerAndConsumer can also be true
                | BenchmarkKind::EndToEndProducingConsumer => false,
                BenchmarkKind::BalancedProducerAndConsumerGroup => true,
                _ => unreachable!(),
            };

            async move {
                let actor_type = match (should_produce, should_consume) {
                    (true, true) => "(producer+consumer)",
                    (true, false) => "(producer only)",
                    (false, true) => "(consumer only)",
                    (false, false) => unreachable!(),
                };

                info!(
                    "Executing producing consumer #{}{} stream_id={} {}",
                    actor_id,
                    if should_consume {
                        format!(" in group={}", consumer_group_id.unwrap())
                    } else {
                        String::new()
                    },
                    stream_id,
                    actor_type
                );
                let actor = TypedBenchmarkProducingConsumer::new(
                    use_high_level_api,
                    client_factory_clone,
                    args_clone.kind(),
                    actor_id,
                    consumer_group_id,
                    stream_id,
                    partitions,
                    messages_per_batch,
                    message_size,
                    send_finish_condition,
                    poll_finish_condition,
                    warmup_time,
                    args_clone.sampling_time(),
                    args_clone.moving_average_window(),
                    rate_limit,
                    polling_kind,
                    origin_timestamp_latency_calculation,
                );

                actor.run().await
            }
        })
        .collect()
}
