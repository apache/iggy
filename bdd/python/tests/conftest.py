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

"""
BDD test configuration and fixtures for Python SDK tests.
"""

import asyncio
import os
from dataclasses import dataclass

import pytest
from apache_iggy import IggyClient, ReceiveMessage


@dataclass
class GlobalContext:
    """Global test context similar to Rust implementation."""

    client: IggyClient | None = None
    server_addr: str | None = None
    last_stream_id: int | None = None
    last_stream_name: str | None = None
    last_topic_id: int | None = None
    last_topic_name: str | None = None
    last_topic_partitions: int | None = None
    last_polled_messages: list[ReceiveMessage] | None = None
    last_sent_message: str | None = None  # Store message payload as string
    last_raw_response: bytes | None = None
    last_raw_error: RuntimeError | None = None


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def context():
    """Create a fresh context for each test scenario."""
    ctx = GlobalContext()

    # Get server address from environment or use default
    ctx.server_addr = os.environ.get("IGGY_TCP_ADDRESS", "127.0.0.1:8090")

    yield ctx
