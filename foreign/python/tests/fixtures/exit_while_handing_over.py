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
Exits while a thread of the SDK hands the result of connect() to Python.

Python 3.11 to 3.13 end a thread that takes the GIL after finalization starts
with pthread_exit, which aborts the process when it unwinds through Rust
frames. The script keeps the interpreter finalizing for a while, so that a
thread of the SDK that takes the GIL in that time is caught. Pass a scenario
name as the only argument.
"""

import asyncio
import os
import socket
import sys
import threading
import time
import types
from collections.abc import Callable

from apache_iggy import IggyClient

FINALIZATION_SECONDS = 1.0
# Below the time that the SDK waits at exit for a thread inside Python.
HAND_OVER_SECONDS = 0.5
POLL_SECONDS = 0.01
STDOUT = 1
FINALIZED = b"finalized"
MAIN_THREAD = threading.get_ident()


class SlowFinalization:
    def __init__(self, first: Callable[[], None]) -> None:
        self.first = first

    # Python may clear the globals of this module before it frees the object.
    def __del__(
        self, sleep=time.sleep, write=os.write, seconds=FINALIZATION_SECONDS
    ) -> None:
        self.first()
        sleep(seconds)
        write(STDOUT, FINALIZED)


class SlowHandOverLoop(asyncio.SelectorEventLoop):
    """
    Keeps a thread of the SDK inside Python without the GIL, as the write to
    the self-pipe in the real method does for a shorter time.
    """

    handing_over = threading.Event()

    def call_soon_threadsafe(self, callback, *args, context=None):
        if threading.get_ident() != MAIN_THREAD:
            self.handing_over.set()
            time.sleep(HAND_OVER_SECONDS)
        return super().call_soon_threadsafe(callback, *args, context=context)


def keep_finalizing(first: Callable[[], None] = lambda: None) -> None:
    # Python frees the modules in sys.modules after finalization starts. The
    # main module does not work, because a thread inside Python keeps it alive.
    holder = types.ModuleType("slow_finalization")
    vars(holder)["finalizer"] = SlowFinalization(first)
    sys.modules[holder.__name__] = holder


def connect_to(port: int) -> tuple[IggyClient, object]:
    client = IggyClient.from_connection_string(
        f"iggy+tcp://iggy:iggy@127.0.0.1:{port}?reconnection_retries=0"
    )
    return client, client.connect()


def exit_with_result_in_flight() -> object:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]

    async def start_failing_connect() -> object:
        pending = connect_to(closed_port)
        while not SlowHandOverLoop.handing_over.is_set():
            await asyncio.sleep(POLL_SECONDS)
        return pending

    with asyncio.Runner(loop_factory=SlowHandOverLoop) as runner:
        pending = runner.run(start_failing_connect())
    keep_finalizing()
    return pending


def exit_with_result_after_finalization_starts() -> object:
    # The listener never accepts, so connect() waits for an answer to its login
    # until the close resets the connection.
    listener = socket.create_server(("127.0.0.1", 0))

    async def start_connect() -> object:
        return connect_to(listener.getsockname()[1])

    pending = asyncio.run(start_connect())
    keep_finalizing(first=listener.close)
    return pending


SCENARIOS = {
    "in-flight": exit_with_result_in_flight,
    "late": exit_with_result_after_finalization_starts,
}

if __name__ == "__main__":
    pending = SCENARIOS[sys.argv[1]]()
