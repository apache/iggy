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

"""The conformance agent of the Python SDK. It reads JSON requests from stdin, calls the
SDK and writes one JSON answer per request to stdout. It never judges a result. See
conformance/README.md for the protocol."""

import asyncio
import functools
import json
import logging
import os
import sys
from importlib.metadata import version
from typing import Any, TextIO

from apache_iggy import IggyClient, IggyConsumer, PartitionReader, TcpConfig

import calls
from replies import Failure, Unsupported, reply

PROTOCOL_VERSION = 1
CAPABILITIES = [
    "reader",
    "consumer",
    "resolve_operation",
    "abandon_operation",
    "raw_command",
    "raw_command_replica",
    "describe_options",
    "metadata_changes",
]
# A request line carries a whole batch of messages.
LINE_LIMIT = 256 * 1024 * 1024
# How long a reset waits for the calls that it cancels to end.
CANCEL_GRACE_SECONDS = 5

log = logging.getLogger("iggy-agent")


class Agent:
    def __init__(self, output: TextIO):
        self.output = output
        self.clients: dict[str, IggyClient] = {}
        self.readers: dict[str, PartitionReader] = {}
        self.consumers: dict[str, IggyConsumer] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.requests: set[asyncio.Task] = set()

    def receive(self, request: dict[str, Any]) -> None:
        # Each request runs as a task of its own, so a long call never delays a reset or
        # a cancellation behind it. The answer is written when the task ends, also when
        # a reset cancels it before it starts.
        running = asyncio.create_task(self.answer(request))
        self.requests.add(running)
        running.add_done_callback(functools.partial(self.answered, request.get("id")))

    def answered(self, request_id: int, running: asyncio.Task) -> None:
        self.requests.discard(running)
        if running.cancelled():
            fields = {"failure": "the request was cancelled"}
        elif (error := running.exception()) is not None:
            fields = {"failure": f"{type(error).__name__}: {error}"}
        else:
            fields = running.result()
        try:
            line = json.dumps({"id": request_id} | fields)
        except (TypeError, ValueError) as error:
            line = json.dumps(
                {"id": request_id, "failure": f"cannot encode the answer: {error}"}
            )
        self.write(line)

    def write(self, line: str) -> None:
        try:
            self.output.write(line + "\n")
            self.output.flush()
        except OSError as error:
            log.error("cannot write an answer: %s", error)

    async def answer(self, request: dict[str, Any]) -> dict[str, Any]:
        if request["op"] != "await":
            return await reply(self.handle(request))
        started = self.tasks.pop(request["task"], None)
        if started is None:
            return {"failure": f"no task {request['task']!r}"}
        return await started

    async def handle(self, request: dict[str, Any]) -> Any:
        match request["op"]:
            case "create":
                if request["kind"] != "client":
                    raise Unsupported(f"creating a {request['kind']} with create")
                await self.create_client(request["entity"], **request["args"])
            case "call":
                return await calls.call(
                    self,
                    request["entity"],
                    request["method"],
                    request["args"],
                    request.get("save_as"),
                )
            case "start":
                call = calls.call(
                    self, request["entity"], request["method"], request["args"], None
                )
                previous = self.tasks.get(request["task"])
                if previous is not None:
                    previous.cancel()
                self.tasks[request["task"]] = asyncio.create_task(reply(call))
            case "cancel":
                started = self.tasks.pop(request["task"], None)
                if started is None:
                    raise Failure(f"no task {request['task']!r}")
                started.cancel()
            case "close":
                await self.close(request["entity"])
            case "reset":
                await self.reset()
            case op:
                raise Failure(f"unknown op {op!r}")
        return None

    async def create_client(self, entity: str, *, transport, address, login) -> None:
        if transport != "tcp":
            raise Unsupported(f"the {transport} transport")
        client = IggyClient(TcpConfig(server_address=address))
        try:
            await client.connect()
            if login is not None:
                await client.login_user(**login)
        except Exception:
            await self.shut_down(entity, client.shutdown())
            raise
        self.clients[entity] = client

    async def close(self, entity: str) -> None:
        consumer = self.consumers.pop(entity, None)
        if consumer is not None:
            await self.shut_down(entity, consumer.shutdown())
        reader = self.readers.pop(entity, None)
        if reader is not None:
            await self.shut_down(entity, reader.close())
        client = self.clients.pop(entity, None)
        if client is not None:
            await self.shut_down(entity, client.shutdown())

    async def reset(self) -> None:
        current = asyncio.current_task()
        cancelled = [
            task
            for task in (*self.requests, *self.tasks.values())
            if task is not current
        ]
        self.tasks.clear()
        for task in cancelled:
            task.cancel()
        if cancelled:
            await asyncio.wait(cancelled, timeout=CANCEL_GRACE_SECONDS)
        # A consumer and a reader share the connection of their client, so they close
        # first.
        for entity in [*self.consumers, *self.readers, *self.clients]:
            await self.close(entity)

    @staticmethod
    async def shut_down(entity: str, closing) -> None:
        try:
            await closing
        except Exception as error:
            log.warning("cannot close %s: %s", entity, error)


async def serve_stdio(output: TextIO) -> None:
    agent = Agent(output)
    hello = {
        "sdk": "python",
        "sdk_version": version("apache-iggy"),
        "protocol": PROTOCOL_VERSION,
        "transports": ["tcp"],
        "capabilities": CAPABILITIES,
    }
    agent.write(json.dumps({"hello": hello}))
    loop = asyncio.get_running_loop()
    lines = asyncio.StreamReader(limit=LINE_LIMIT)
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(lines), sys.stdin)
    while line := await lines.readline():
        if not line.strip():
            continue
        try:
            request = json.loads(line)
        except ValueError as error:
            log.error("cannot read the request %r: %s", line, error)
            continue
        agent.receive(request)
    await agent.reset()


def main() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    # The answers own the stdout of the process. Anything else that writes to stdout,
    # such as a log line of the SDK, goes to stderr, so it cannot break the protocol.
    output = os.fdopen(os.dup(sys.stdout.fileno()), "w", encoding="utf-8")
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    asyncio.run(serve_stdio(output))


if __name__ == "__main__":
    main()
