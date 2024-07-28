use crate::shared::args::Args;
use futures_util::StreamExt;
use iggy::client::Client;
use iggy::clients::client::IggyClient;
use iggy::clients::consumer::{AutoCommit, AutoCommitMode};
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer::{Consumer, ConsumerKind};
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::models::messages::PolledMessage;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use tracing::{error, info};

type MessageHandler = dyn Fn(&PolledMessage) -> Result<(), Box<dyn std::error::Error>>;

pub async fn init_by_consumer(args: &Args, client: &dyn Client) {
    let (stream_id, topic_id, partition_id) = (
        args.stream_id.clone(),
        args.topic_id.clone(),
        args.partition_id,
    );
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let stream_id = stream_id.try_into().unwrap();
    let topic_id = topic_id.try_into().unwrap();
    loop {
        interval.tick().await;
        info!("Validating if stream: {stream_id} exists..");
        let stream = client.get_stream(&stream_id).await;
        if stream.is_ok() {
            info!("Stream: {stream_id} was found.");
            break;
        }
    }
    loop {
        interval.tick().await;
        info!("Validating if topic: {} exists..", topic_id);
        let topic = client.get_topic(&stream_id, &topic_id).await;
        if topic.is_err() {
            continue;
        }

        info!("Topic: {} was found.", topic_id);
        let topic = topic.unwrap();
        if topic.partitions_count >= partition_id {
            break;
        }

        panic!(
            "Topic: {} has only {} partition(s), but partition: {} was requested.",
            topic_id, topic.partitions_count, partition_id
        );
    }
}

pub async fn init_by_producer(args: &Args, client: &dyn Client) -> Result<(), IggyError> {
    let stream_id = args.stream_id.clone().try_into()?;
    let topic_name = args.topic_id.clone();
    let stream = client.get_stream(&stream_id).await;
    if stream.is_ok() {
        return Ok(());
    }

    info!("Stream does not exist, creating...");
    client.create_stream(&args.stream_id, None).await?;
    client
        .create_topic(
            &stream_id,
            &topic_name,
            args.partitions_count,
            CompressionAlgorithm::from_code(args.compression_algorithm)?,
            None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await?;
    Ok(())
}

pub async fn consume_messages(
    args: &Args,
    client: &dyn Client,
    handle_message: &MessageHandler,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.consumer_id, args.stream_id, args.topic_id, args.partition_id, args.interval);

    let stream_id = args.stream_id.clone().try_into()?;
    let topic_id = args.topic_id.clone().try_into()?;
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    let mut consumed_batches = 0;
    let consumer = Consumer {
        kind: ConsumerKind::from_code(args.consumer_kind)?,
        id: Identifier::numeric(args.consumer_id).unwrap(),
    };

    loop {
        if args.message_batches_limit > 0 && consumed_batches == args.message_batches_limit {
            info!("Consumed {consumed_batches} batches of messages, exiting.");
            return Ok(());
        }

        interval.tick().await;
        let polled_messages = client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(args.partition_id),
                &consumer,
                &PollingStrategy::next(),
                args.messages_per_batch,
                true,
            )
            .await?;
        if polled_messages.messages.is_empty() {
            info!("No messages found.");
            continue;
        }
        consumed_batches += 1;
        for message in polled_messages.messages {
            handle_message(&message)?;
        }
    }
}

#[allow(dead_code)]
pub async fn consume_messages_new(
    args: &Args,
    client: &IggyClient,
    handle_message: &MessageHandler,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Messages will be polled by consumer: {} from stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.consumer_id, args.stream_id, args.topic_id, args.partition_id, args.interval);

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(args.interval));
    let mut consumed_batches = 0;
    let name = "envelope-example";

    // let mut consumer = match ConsumerKind::from_code(args.consumer_kind)? {
    //     ConsumerKind::Consumer => client
    //         .consumer(name, &args.stream_id, &args.topic_id, args.partition_id)?
    //         .polling_strategy(PollingStrategy::next()),
    //     ConsumerKind::ConsumerGroup => client
    //         .consumer_group(name, &args.stream_id, &args.topic_id)?
    //         .polling_strategy(PollingStrategy::next()),
    // }
    // .auto_commit(AutoCommit::Mode(AutoCommitMode::AfterPollingMessages))
    // .batch_size(args.messages_per_batch)
    // .build();

    let mut consumer = client
        .consumer_group(name, &args.stream_id, &args.topic_id)?
        .polling_strategy(PollingStrategy::next())
        .auto_commit(AutoCommit::Mode(AutoCommitMode::AfterPollingMessages))
        .batch_size(args.messages_per_batch)
        .auto_join_consumer_group()
        .build();

    consumer.init().await?;

    while let Some(message) = consumer.next().await {
        if args.message_batches_limit > 0 && consumed_batches == args.message_batches_limit {
            info!("Consumed {consumed_batches} batches of messages, exiting.");
            return Ok(());
        }

        interval.tick().await;
        if let Ok(message) = message {
            handle_message(&message)?;
            consumed_batches += 1;
        } else if let Err(error) = message {
            error!("Error while handling message: {error}");
            continue;
        }
    }
    Ok(())
}
