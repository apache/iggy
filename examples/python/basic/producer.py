# Basic Producer Example for Iggy Python SDK
# Mirrors the Rust basic-producer example
import asyncio
from loguru import logger
from iggy_py import IggyClient, SendMessage as Message

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 1

async def main():
    client = IggyClient()
    logger.info("Connecting to IggyClient")
    await client.connect()
    logger.info("Connected. Logging in user...")
    await client.login_user("iggy", "iggy")
    logger.info("Logged in.")
    await init_system(client)
    await produce_messages(client)

async def init_system(client: IggyClient):
    try:
        logger.info(f"Creating stream with name {STREAM_NAME}...")
        stream = await client.get_stream(STREAM_NAME)
        if stream is None:
            await client.create_stream(name=STREAM_NAME)
            logger.info("Stream was created successfully.")
        else:
            logger.info(f"Stream {stream.name} already exists with ID {stream.id}")
    except Exception as error:
        logger.error(f"Error creating stream: {error}")
        logger.exception(error)
    try:
        logger.info(f"Creating topic {TOPIC_NAME} in stream {STREAM_NAME}")
        topic = await client.get_topic(STREAM_NAME, TOPIC_NAME)
        if topic is None:
            await client.create_topic(
                stream=STREAM_NAME,
                partitions_count=1,
                name=TOPIC_NAME,
                replication_factor=1
            )
            logger.info(f"Topic was created successfully.")
        else:
            logger.info(f"Topic {topic.name} already exists with ID {topic.id}")
    except Exception as error:
        logger.error(f"Error creating topic {error}")
        logger.exception(error)

async def produce_messages(client: IggyClient):
    interval = 0.5
    logger.info(
        f"Messages will be sent to stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with interval {interval * 1000} ms.")
    current_id = 0
    messages_per_batch = 10
    while True:
        messages = []
        for _ in range(messages_per_batch):
            current_id += 1
            payload = f"message-{current_id}"
            message = Message(payload)
            messages.append(message)
        logger.info(
            f"Attempting to send batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}")
        try:
            await client.send_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partitioning=PARTITION_ID,
                messages=messages,
            )
            logger.info(
                f"Successfully sent batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}")
        except Exception as error:
            logger.error(f"Exception type: {type(error).__name__}, message: {error}")
            logger.exception(error)
        await asyncio.sleep(interval)

if __name__ == "__main__":
    asyncio.run(main())
