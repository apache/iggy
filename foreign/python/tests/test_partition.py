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

import pytest

from apache_iggy import IggyClient


@pytest.mark.asyncio
async def test_create_and_delete_partitions(iggy_client: IggyClient, unique_name):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name, name=topic_name, partitions_count=2
    )

    await iggy_client.create_partitions(stream_name, topic_name, 2)
    topic = await iggy_client.get_topic(stream_name, topic_name)
    assert topic is not None
    assert topic.partitions_count == 4

    await iggy_client.delete_partitions(stream_name, topic_name, 2)
    topic = await iggy_client.get_topic(stream_name, topic_name)
    assert topic is not None
    assert topic.partitions_count == 2
