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

scenarios("/app/features/session_contract.feature")

# The Python SDK raises RuntimeError with the Rust error text and no code.
ERROR_MESSAGES = {
    40: "Unauthenticated",
    58: "Request transiently not accepted",
}


def connect(context):
    async def open_connection():
        host, port = context.server_addr.split(":")
        try:
            address = f"{socket.gethostbyname(host)}:{port}"
        except socket.gaierror:
            address = context.server_addr

        context.client = IggyClient(address)
        await context.client.connect()

    asyncio.run(open_connection())


@given("I have a running Iggy server")
def running_server(context):
    connect(context)


@given("I have a cluster node that cannot reach quorum")
def node_without_quorum(context, no_quorum_address):
    context.server_addr = no_quorum_address
    connect(context)


@given("I am authenticated as the root user")
def authenticated_root_user(context, root_credentials):
    async def login():
        await context.client.login_user(*root_credentials)

    asyncio.run(login())


@when("I sign out")
def sign_out(context):
    async def logout():
        await context.client.logout_user()

    asyncio.run(logout())


@when(parsers.parse("I request the stream with ID {stream_id:d}"))
def request_stream(context, stream_id):
    async def request():
        try:
            await context.client.get_stream(stream_id)
            context.last_call_error = None
        except RuntimeError as error:
            context.last_call_error = error

    asyncio.run(request())


@when("I sign in to that node as the root user")
def sign_in_to_node(context, root_credentials):
    async def sign_in():
        try:
            await context.client.login_user(*root_credentials)
            context.last_call_error = None
        except RuntimeError as error:
            context.last_call_error = error

    asyncio.run(sign_in())


@then(parsers.parse("the call should fail with error code {code:d}"))
def call_fails_with_code(context, code):
    assert context.last_call_error is not None
    assert ERROR_MESSAGES[code] in str(context.last_call_error)
