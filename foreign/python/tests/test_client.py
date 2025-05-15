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

from iggy_py import PollingStrategy
from iggy_py import SendMessage as Message
from iggy_py import IggyClient

STREAM_NAME = "test-stream"
TOPIC_NAME = "test-topic"
PARTITION_ID = 1


async def test_send_and_poll_messages(iggy_client: IggyClient):
    assert iggy_client is not None

    await iggy_client.create_stream(STREAM_NAME)
    stream = await iggy_client.get_stream(STREAM_NAME)
    assert stream is not None
    assert stream.name == STREAM_NAME

    await iggy_client.create_topic(STREAM_NAME, TOPIC_NAME, partitions_count=1)
    topic = await iggy_client.get_topic(STREAM_NAME, TOPIC_NAME)
    assert topic is not None
    assert topic.name == TOPIC_NAME

    messages = [
        Message("Message 1"),
        Message("Message 2"),
    ]
    await iggy_client.send_messages(STREAM_NAME, TOPIC_NAME, PARTITION_ID, messages)

    polled_messages = await iggy_client.poll_messages(
        STREAM_NAME,
        TOPIC_NAME,
        PARTITION_ID,
        PollingStrategy.Next(),
        count=10,
        auto_commit=True,
    )

    assert len(polled_messages) >= 2
    assert polled_messages[0].payload().decode("utf-8") == "Message 1"
    assert polled_messages[1].payload().decode("utf-8") == "Message 2"
