# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio

# Assuming we have a Python module for iggy with similar functionality as the Rust one.
from apache_iggy import IggyClient, StreamDetails, TopicDetails
from apache_iggy import SendMessage as Message
from loguru import logger

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 0


async def main():
    client = IggyClient()  # Assuming default constructor has similar functionality.
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
        stream: StreamDetails | None = await client.get_stream(STREAM_NAME)
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
        topic: TopicDetails | None = await client.get_topic(STREAM_NAME, TOPIC_NAME)
        if topic is None:
            await client.create_topic(
                stream=STREAM_NAME,  # Assuming a method exists to create a numeric Identifier.
                partitions_count=1,
                name=TOPIC_NAME,
                replication_factor=1,
            )
            logger.info("Topic was created successfully.")
        else:
            logger.info("Topic {topic.name} already exists with ID {topic.id}")
    except Exception as error:
        logger.error(f"Error creating topic {error}")
        logger.exception(error)


async def produce_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(
        f"Messages will be sent to stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with interval {interval * 1000} ms."
    )
    current_id = 0
    messages_per_batch = 10
    while True:
        messages = []
        for _ in range(messages_per_batch):
            current_id += 1
            payload = f"message-{current_id}"
            message = Message(
                payload
            )  # Assuming a method exists to convert str to Message.
            messages.append(message)
        logger.info(
            f"Attempting to send batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}"
        )
        try:
            await client.send_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partitioning=PARTITION_ID,
                messages=messages,
            )
            logger.info(
                f"Successfully sent batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}"
            )
        except Exception as error:
            logger.error(f"Exception type: {type(error).__name__}, message: {error}")
            logger.exception(error)

        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main())
