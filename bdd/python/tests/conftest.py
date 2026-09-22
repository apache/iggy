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


def required_env(name: str) -> str:
    """Read a variable the suite cannot run without.

    A default here would turn a dropped compose variable into a run against
    whatever happens to listen on the fallback address, so a missing value
    aborts the suite instead.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} must be set; run the suite via scripts/run-bdd-tests.sh"
        )
    return value


@pytest.fixture(scope="session")
def root_credentials() -> tuple[str, str]:
    """Root username and password the server was started with."""
    return required_env("IGGY_ROOT_USERNAME"), required_env("IGGY_ROOT_PASSWORD")


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

    ctx.server_addr = required_env("IGGY_TCP_ADDRESS")

    yield ctx
