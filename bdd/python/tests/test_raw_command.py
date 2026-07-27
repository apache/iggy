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

import asyncio
import socket

from apache_iggy import IggyClient
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("/app/features/raw_command.feature")


@given("I have a running Iggy server")
def running_server(context):
    async def connect():
        host, port = context.server_addr.split(":")
        try:
            address = f"{socket.gethostbyname(host)}:{port}"
        except socket.gaierror:
            address = context.server_addr

        context.client = IggyClient(address)
        await context.client.connect()
        await context.client.ping()

    asyncio.run(connect())


@given("I am authenticated as the root user")
def authenticated_root_user(context):
    async def login():
        await context.client.login_user("iggy", "iggy")

    asyncio.run(login())


@when(parsers.parse("I send a raw command with code {code:d} and an empty payload"))
def send_raw_command(context, code):
    async def send():
        try:
            context.last_raw_response = await context.client.send_binary_request(
                code, b""
            )
            context.last_raw_error = None
        except RuntimeError as error:
            context.last_raw_response = None
            context.last_raw_error = error

    asyncio.run(send())


@then("the raw command should succeed with an empty response")
def raw_command_succeeds_with_empty_response(context):
    assert context.last_raw_error is None
    assert context.last_raw_response == b""


@then("the raw command should succeed with a non-empty response")
def raw_command_succeeds_with_non_empty_response(context):
    assert context.last_raw_error is None
    assert context.last_raw_response


@then("the raw command should fail with an invalid command error")
def raw_command_fails_with_invalid_command(context):
    assert context.last_raw_response is None
    assert context.last_raw_error is not None
    assert "invalid command" in str(context.last_raw_error).lower()
