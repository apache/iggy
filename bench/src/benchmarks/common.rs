use super::benchmark::BenchmarkFutures;
use super::*;
use crate::{
    actors::{consumer::Consumer, producer::Producer, producing_consumer::ProducingConsumer},
    args::common::IggyBenchArgs,
    utils::finish_condition::BenchmarkFinishCondition,
};
use futures::FutureExt;
use iggy::consumer::Consumer as IggyConsumer;
use iggy::{
    client::ConsumerGroupClient, clients::client::IggyClient, error::IggyError,
    identifier::Identifier, messages::poll_messages::PollingKind, utils::byte_size::IggyByteSize,
};
use integration::test_server::{login_root, ClientFactory};
use std::sync::Arc;
use tracing::{error, info};

pub async fn create_consumer(
    client: &IggyClient,
    consumer_group_id: &Option<u32>,
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer_id: u32,
) -> IggyConsumer {
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
            IggyConsumer::group(cg_identifier)
        }
        None => IggyConsumer::new(consumer_id.try_into().unwrap()),
    }
}

pub fn rate_limit_per_actor(
    total_rate: Option<IggyByteSize>,
    actors: u32,
    factor: u64,
) -> Option<IggyByteSize> {
    total_rate.map(|rl| (rl.as_bytes_u64() / factor / (actors as u64)).into())
}

pub async fn init_consumer_groups(
    client_factory: &Arc<dyn ClientFactory>,
    args: &IggyBenchArgs,
) -> Result<(), IggyError> {
    let start_stream_id = args.start_stream_id();
    let topic_id: u32 = 1;
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);
    let cg_count = args.number_of_consumer_groups();

    login_root(&client).await;
    for i in 1..=cg_count {
        let consumer_group_id = CONSUMER_GROUP_BASE_ID + i;
        let stream_id = start_stream_id + i;
        let consumer_group_name = format!("{}-{}", CONSUMER_GROUP_NAME_PREFIX, consumer_group_id);
        info!(
            "Creating test consumer group: name={}, id={}, stream={}, topic={}",
            consumer_group_name, consumer_group_id, stream_id, topic_id
        );
        match client
            .create_consumer_group(
                &stream_id.try_into().unwrap(),
                &topic_id.try_into().unwrap(),
                &consumer_group_name,
                Some(consumer_group_id),
            )
            .await
        {
            Err(IggyError::ConsumerGroupIdAlreadyExists(_, _)) => {
                info!(
                    "Consumer group with id {} already exists",
                    consumer_group_id
                );
            }
            Err(err) => {
                error!("Error when creating consumer group {consumer_group_id}: {err}");
            }
            Ok(_) => {}
        }
    }
    Ok(())
}

pub fn build_producer_futures(
    client_factory: &Arc<dyn ClientFactory>,
    args: &IggyBenchArgs,
) -> BenchmarkFutures {
    let streams = args.streams();
    let partitions = args.number_of_partitions();
    let start_stream_id = args.start_stream_id();
    let producers = args.producers();
    let actors = args.producers() + args.consumers();
    let batches = args.message_batches();
    let total_data = args.total_data();
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let sampling_time = args.sampling_time();
    let moving_average_window = args.moving_average_window();
    let kind = args.kind();
    let global_finish_condition = BenchmarkFinishCondition::new(total_data, 1, batches);
    let rate_limit = rate_limit_per_actor(args.rate_limit(), actors, 1);

    let mut futures = Vec::with_capacity(producers as usize);

    for producer_id in 1..=producers {
        let client_factory = client_factory.clone();
        let finish_condition = if partitions > 1 {
            global_finish_condition.clone()
        } else {
            BenchmarkFinishCondition::new(total_data, producers, batches)
        };
        let stream_id = start_stream_id + 1 + (producer_id % streams);
        let fut = async move {
            let producer = Producer::new(
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
        .boxed();
        futures.push(fut);
    }
    Ok(futures)
}

pub fn build_consumer_futures(
    client_factory: &Arc<dyn ClientFactory>,
    args: &IggyBenchArgs,
) -> BenchmarkFutures {
    let start_stream_id = args.start_stream_id();
    let cg_count = args.number_of_consumer_groups();
    let consumers = args.consumers();
    let actors = args.producers() + args.consumers();
    let batches: Option<std::num::NonZero<u32>> = args.message_batches();
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
    let global_finish_condition = BenchmarkFinishCondition::new(args.total_data(), 1, batches);
    let rate_limit = rate_limit_per_actor(args.rate_limit(), actors, 1);

    let mut futures = Vec::with_capacity(consumers as usize);

    for consumer_id in 1..=consumers {
        let client_factory = client_factory.clone();
        let finish_condition = if cg_count > 0 {
            global_finish_condition.clone()
        } else {
            BenchmarkFinishCondition::new(args.total_data(), consumers, batches)
        };
        let stream_id = if cg_count > 0 {
            start_stream_id + 1 + (consumer_id % cg_count)
        } else {
            start_stream_id + consumer_id
        };
        let consumer_group_id = if cg_count > 0 {
            Some(CONSUMER_GROUP_BASE_ID + 1 + (consumer_id % cg_count))
        } else {
            None
        };
        let fut = async move {
            let consumer = Consumer::new(
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
            );
            consumer.run().await
        }
        .boxed();
        futures.push(fut);
    }
    Ok(futures)
}

#[allow(clippy::too_many_arguments)]
pub fn build_producing_consumers_futures(
    client_factory: Arc<dyn ClientFactory>,
    args: Arc<IggyBenchArgs>,
) -> BenchmarkFutures {
    let producing_consumers = args.producers();
    let streams = args.streams();
    let partitions = args.number_of_partitions();
    let batches = args.message_batches();
    let total_data = args.total_data();
    let cg_count = args.number_of_consumer_groups();
    let warmup_time = args.warmup_time();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let start_stream_id = args.start_stream_id();
    let start_consumer_group_id = CONSUMER_GROUP_BASE_ID;
    let global_finish_condition = BenchmarkFinishCondition::new(total_data, 1, batches);
    let polling_kind = if cg_count > 1 {
        PollingKind::Next
    } else {
        PollingKind::Offset
    };
    let mut futures = Vec::with_capacity(producing_consumers as usize);

    for actor_id in 1..=producing_consumers {
        let client_factory_clone = client_factory.clone();
        let args_clone = args.clone();
        let finish_condition = if cg_count > 1 {
            global_finish_condition.clone()
        } else {
            BenchmarkFinishCondition::new(
                args.total_data(),
                producing_consumers * 2,
                args.message_batches(),
            )
        };
        let rate_limit = rate_limit_per_actor(args.rate_limit(), producing_consumers, 1);
        let use_consumer_groups = cg_count > 1;
        let consumer_group_id = if cg_count > 1 {
            Some(start_consumer_group_id + 1 + (actor_id % cg_count))
        } else {
            None
        };
        let stream_id = if cg_count > 1 {
            start_stream_id + 1 + (actor_id % cg_count)
        } else {
            start_stream_id + 1 + (actor_id % streams)
        };
        let fut = async move {
            info!(
                "Executing producing consumer #{}{} stream_id={}",
                actor_id,
                if use_consumer_groups {
                    format!(" in group={}", consumer_group_id.unwrap())
                } else {
                    "".to_string()
                },
                stream_id
            );
            let actor = ProducingConsumer::new(
                client_factory_clone,
                args_clone.kind(),
                actor_id,
                consumer_group_id,
                stream_id,
                partitions,
                messages_per_batch,
                message_size,
                finish_condition.clone(),
                warmup_time,
                args_clone.sampling_time(),
                args_clone.moving_average_window(),
                rate_limit,
                polling_kind,
            );
            actor.run().await
        }
        .boxed();
        futures.push(fut);
    }
    Ok(futures)
}
