# Getting Started Consumer Example for Iggy Python SDK
import asyncio
from loguru import logger
from iggy_py import IggyClient, ReceiveMessage, PollingStrategy

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 1

async def main():
    client = IggyClient()
    await client.connect()
    await client.login_user("iggy", "iggy")
    await consume_messages(client)

async def consume_messages(client: IggyClient):
    logger.info("Polling for a single message as a quick start example.")
    polled_messages = await client.poll_messages(
        stream=STREAM_NAME,
        topic=TOPIC_NAME,
        partition_id=PARTITION_ID,
        polling_strategy=PollingStrategy.Next(),
        count=1,
        auto_commit=True
    )
    if not polled_messages:
        logger.info("No messages found.")
    else:
        for message in polled_messages:
            handle_message(message)

def handle_message(message: ReceiveMessage):
    payload = message.payload().decode('utf-8')
    logger.info(f"Received message at offset: {message.offset()} with payload: {payload}")

if __name__ == "__main__":
    asyncio.run(main())
