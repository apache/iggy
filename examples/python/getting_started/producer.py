# Getting Started Producer Example for Iggy Python SDK
import asyncio
from loguru import logger
from iggy_py import IggyClient, SendMessage as Message

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 1

async def main():
    client = IggyClient()
    await client.connect()
    await client.login_user("iggy", "iggy")
    await produce_messages(client)

async def produce_messages(client: IggyClient):
    logger.info("Sending a single message as a quick start example.")
    message = Message("Hello from Python!")
    await client.send_messages(
        stream=STREAM_NAME,
        topic=TOPIC_NAME,
        partitioning=PARTITION_ID,
        messages=[message],
    )
    logger.info("Message sent!")

if __name__ == "__main__":
    asyncio.run(main())
