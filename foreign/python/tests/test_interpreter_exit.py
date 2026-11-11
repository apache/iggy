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
Tests that the interpreter exits cleanly while the SDK is in use.
"""

import asyncio
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from apache_iggy import Consumer, IggyClient
from apache_iggy import SendMessage as Message

from .fixtures import exit_while_handing_over as handing_over
from .fixtures import exit_with_earlier_handler as earlier_handler
from .utils import get_server_config

FIXTURES = Path(__file__).parent / "fixtures"
HANDING_OVER = FIXTURES / "exit_while_handing_over.py"
EARLIER_HANDLER = FIXTURES / "exit_with_earlier_handler.py"
EXIT_TIMEOUT_SECONDS = 20
INTERPRETER_EXITING = b"the Python interpreter is exiting"
PARTITION_ID = 0
ONE_MESSAGE = 1
BACKLOG_MESSAGES = 200


def run_to_exit(script: Path, *arguments: str) -> bytes:
    completed = subprocess.run(  # noqa: S603
        [sys.executable, str(script), *arguments],
        capture_output=True,
        timeout=EXIT_TIMEOUT_SECONDS,
        check=False,
    )

    stderr = completed.stderr.decode(errors="replace")
    assert completed.returncode == 0, stderr
    # pyo3 panics instead of aborting when a thread takes the GIL after the
    # interpreter marks itself as no longer initialized.
    assert "panicked" not in stderr, stderr
    return completed.stdout


async def topic_with_messages(
    iggy_client: IggyClient, unique_name: Callable[..., str], messages: int
) -> tuple[str, str]:
    stream, topic = unique_name("exit-stream-"), unique_name("exit-topic-")
    await iggy_client.create_stream(stream)
    await iggy_client.create_topic(stream, topic)
    await iggy_client.send_messages(
        stream,
        topic,
        PARTITION_ID,
        [Message(f"message-{index}") for index in range(messages)],
    )
    return stream, topic


def server_address() -> str:
    host, port = get_server_config()
    return f"{host}:{port}"


@pytest.mark.parametrize("scenario", sorted(handing_over.SCENARIOS))
def test_exit_while_sdk_hands_result_to_python(scenario: str):
    assert run_to_exit(HANDING_OVER, scenario) == handing_over.FINALIZED


def test_call_after_sdk_exit_handler_raises():
    stdout = run_to_exit(EARLIER_HANDLER, earlier_handler.LATE_CALL)

    assert stdout == INTERPRETER_EXITING


async def test_exit_ends_consume_messages_run(
    iggy_client: IggyClient, unique_name: Callable[..., str]
):
    stream, topic = await topic_with_messages(
        iggy_client, unique_name, BACKLOG_MESSAGES
    )

    stdout = await asyncio.to_thread(
        run_to_exit,
        EARLIER_HANDLER,
        earlier_handler.CONSUMING,
        server_address(),
        stream,
        topic,
    )

    last_seen = int(stdout.split()[-1])
    last_in_backlog = BACKLOG_MESSAGES - 1
    assert last_seen + 1 < last_in_backlog, "the exit left no message to run past"
    stored = await iggy_client.get_consumer_offset(
        stream,
        topic,
        consumer=Consumer.Group(earlier_handler.GROUP),
        partition_id=PARTITION_ID,
    )
    assert stored is not None
    # A message counts before its callback, so the store can lead the callbacks
    # by the one message whose callback the exit refused.
    assert stored.stored_offset <= last_seen + 1


async def test_exit_while_consume_run_waits_for_python(
    iggy_client: IggyClient, unique_name: Callable[..., str]
):
    stream, topic = await topic_with_messages(iggy_client, unique_name, ONE_MESSAGE)

    stdout = await asyncio.to_thread(
        run_to_exit,
        HANDING_OVER,
        handing_over.CONSUMING,
        server_address(),
        stream,
        topic,
    )

    assert stdout == handing_over.FINALIZED
