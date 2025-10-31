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

# Assuming there's a Python module for iggy with similar functionalities.
from apache_iggy import IggyClient, PollingStrategy, ReceiveMessage
from loguru import logger

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 0


async def main():
    client = IggyClient()  # Assuming default constructor has similar functionality.
    try:
        logger.info("Connecting to IggyClient...")
        await client.connect()
        logger.info("Connected. Logging in user...")
        await client.login_user("iggy", "iggy")
        logger.info("Logged in.")
        await consume_messages(client)
    except Exception as error:
        logger.exception("Exception occurred in main function: {}", error)


async def consume_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(
        f"Messages will be consumed from stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with "
        f"interval {interval * 1000} ms."
    )
    offset = 0
    messages_per_batch = 10
    while True:
        try:
            logger.debug("Polling for messages...")
            polled_messages = await client.poll_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partition_id=PARTITION_ID,
                polling_strategy=PollingStrategy.Next(),
                count=messages_per_batch,
                auto_commit=True,
            )
            if not polled_messages:
                logger.warning("No messages found in current poll")
                await asyncio.sleep(interval)
                continue

            offset += len(polled_messages)
            for message in polled_messages:
                handle_message(message)
            await asyncio.sleep(interval)
        except Exception as error:
            logger.exception("Exception occurred while consuming messages: {}", error)
            break


def handle_message(message: ReceiveMessage):
    payload = message.payload().decode("utf-8")
    logger.info(
        f"Handling message at offset: {message.offset()} with payload: {payload}..."
    )


if __name__ == "__main__":
    asyncio.run(main())
