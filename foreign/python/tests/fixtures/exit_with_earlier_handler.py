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
Exits with an atexit handler that was registered before the import of the SDK.

Python runs atexit handlers in reverse order of registration, and the SDK
registers its handler at import. So the earlier handler runs after the SDK
stopped handing results to Python. A daemon thread runs the event loop, as in
a sync facade over the SDK. Pass a scenario name as the first argument.
"""

import asyncio
import atexit
import importlib
import os
import sys
import threading
import time
import types
from collections.abc import Callable
from datetime import timedelta

STDOUT = 1
# No server listens here. The call must fail before it connects.
UNUSED_SERVER = "iggy+tcp://iggy:iggy@127.0.0.1:1?reconnection_retries=0"
GROUP = "exiting"
POLL_COUNT = 10
POLL_WAIT = timedelta(seconds=1)
CALLBACKS_BEFORE_EXIT = 3
CALLBACK_TIMEOUT_SECONDS = 10
# Paces the callbacks, so that most of the backlog is unread at exit.
CALLBACK_SECONDS = 0.01
# Enough for a run that goes on after the handler of the SDK to pass the backlog.
SECONDS_AFTER_SDK_HANDLER = 1.0
LATE_CALL = "late-call"
# Takes the address of a server, a stream and a topic after its name.
CONSUMING = "consuming"


def import_sdk_after(handler: Callable[[], None]) -> types.ModuleType:
    atexit.register(handler)
    return importlib.import_module("apache_iggy")


def start_loop_thread() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()
    return loop


def exit_with_call_in_handler() -> None:
    loop = start_loop_thread()

    def wait_for_connect() -> None:
        try:
            asyncio.run_coroutine_threadsafe(connect(), loop).result()
        except RuntimeError as error:
            os.write(STDOUT, str(error).encode())

    sdk = import_sdk_after(wait_for_connect)

    async def connect() -> None:
        await sdk.IggyClient.from_connection_string(UNUSED_SERVER).connect()


def exit_while_consuming(address: str, stream: str, topic: str) -> None:
    loop = start_loop_thread()
    sdk = import_sdk_after(lambda: time.sleep(SECONDS_AFTER_SDK_HANDLER))
    callbacks = 0
    enough_callbacks = threading.Event()

    async def record(message) -> None:
        nonlocal callbacks
        os.write(STDOUT, f"{message.offset()}\n".encode())
        callbacks += 1
        if callbacks == CALLBACKS_BEFORE_EXIT:
            enough_callbacks.set()
        await asyncio.sleep(CALLBACK_SECONDS)

    async def consume() -> None:
        client = sdk.IggyClient(address)
        await client.connect()
        await client.login_user("iggy", "iggy")
        # Stores each message that the run hands to the callback.
        commit = sdk.CommitPolicy.Auto(
            sdk.AutoCommit(
                every=sdk.CommitEvery.Message(), after=sdk.CommitAfter.Delivery
            )
        )
        consumer = await client.consumer_group(
            GROUP,
            stream,
            topic,
            poll_options=sdk.PollOptions(
                count=POLL_COUNT, max_wait=POLL_WAIT, commit=commit
            ),
        )
        await consumer.consume_messages(record, None)

    consuming = asyncio.run_coroutine_threadsafe(consume(), loop)
    if not enough_callbacks.wait(CALLBACK_TIMEOUT_SECONDS):
        failure = consuming.exception(timeout=0) if consuming.done() else None
        sys.exit(f"{callbacks} callbacks ran: {failure!r}")


SCENARIOS = {
    LATE_CALL: exit_with_call_in_handler,
    CONSUMING: exit_while_consuming,
}

if __name__ == "__main__":
    scenario, *arguments = sys.argv[1:]
    SCENARIOS[scenario](*arguments)
