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
from inspect import unwrap
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from apache_iggy import IggyClient

from . import conftest

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]

STREAM_ID = 7
OPERATION_ID = 2829
CLEANUP_TEST_TIMEOUT_SECONDS = 0.01


class PendingOperationFailure(RuntimeError):
    def __init__(self, operation_id: int, kind: str):
        super().__init__(f"Operation {operation_id}: {kind}")
        self.operation_id = operation_id
        self.kind = kind


@pytest.fixture
def cleanup_request(monkeypatch):
    # PyO3 exposes these exceptions only as results of SDK operations.
    monkeypatch.setattr(conftest, "PendingOperationError", PendingOperationFailure)
    client = Mock(spec=IggyClient)
    client.get_streams = AsyncMock(
        side_effect=[
            [],
            [SimpleNamespace(id=STREAM_ID, created_at=1, name="cleanup-stream")],
        ]
    )
    client.delete_stream = AsyncMock()
    client.resolve_operation = AsyncMock()
    client.get_stream = AsyncMock(return_value=None)
    request = Mock(spec=pytest.FixtureRequest)
    request.fixturenames = ["iggy_client"]
    request.getfixturevalue.return_value = client
    request.session = Mock(shouldstop=False)
    return request


async def test_uncertain_deletion_resolves_the_same_operation(cleanup_request):
    client = cleanup_request.getfixturevalue.return_value
    client.delete_stream.side_effect = PendingOperationFailure(
        OPERATION_ID, "uncertain"
    )
    cleanup = unwrap(conftest.cleanup_streams)(cleanup_request)
    await anext(cleanup)

    with pytest.raises(StopAsyncIteration):
        await anext(cleanup)

    client.delete_stream.assert_awaited_once_with(STREAM_ID)
    client.resolve_operation.assert_awaited_once_with(OPERATION_ID)
    client.get_stream.assert_awaited_once_with(STREAM_ID)
    assert cleanup_request.session.shouldstop is False


@pytest.mark.parametrize("blocker_deleted_stream", [False, True])
async def test_blocked_deletion_resolves_before_retrying(
    cleanup_request, blocker_deleted_stream
):
    client = cleanup_request.getfixturevalue.return_value
    client.delete_stream.side_effect = [
        PendingOperationFailure(OPERATION_ID, "lane_blocked"),
        None,
    ]
    client.get_stream.side_effect = (
        [None, None] if blocker_deleted_stream else [object(), None]
    )
    cleanup = unwrap(conftest.cleanup_streams)(cleanup_request)
    await anext(cleanup)
    client.reset_mock()

    with pytest.raises(StopAsyncIteration):
        await anext(cleanup)

    expected = [
        call.get_streams(),
        call.delete_stream(STREAM_ID),
        call.resolve_operation(OPERATION_ID),
        call.get_stream(STREAM_ID),
    ]
    if not blocker_deleted_stream:
        expected.append(call.delete_stream(STREAM_ID))
    expected.append(call.get_stream(STREAM_ID))
    assert client.mock_calls == expected
    assert cleanup_request.session.shouldstop is False


@pytest.mark.parametrize("failure", ["runtime", "uncertain", "not_retained"])
async def test_cleanup_failure_stops_shared_client_reuse(cleanup_request, failure):
    client = cleanup_request.getfixturevalue.return_value
    error = (
        RuntimeError("deletion rejected")
        if failure == "runtime"
        else PendingOperationFailure(OPERATION_ID, failure)
    )
    client.delete_stream.side_effect = error
    client.resolve_operation.side_effect = error
    cleanup = unwrap(conftest.cleanup_streams)(cleanup_request)
    await anext(cleanup)

    with pytest.raises(RuntimeError) as raised:
        await anext(cleanup)

    assert raised.value is error
    assert str(STREAM_ID) in cleanup_request.session.shouldstop
    assert str(error) in cleanup_request.session.shouldstop
    if failure == "uncertain":
        client.resolve_operation.assert_awaited_once_with(OPERATION_ID)
    else:
        client.resolve_operation.assert_not_awaited()


async def test_cleanup_verifies_the_stream_was_deleted(cleanup_request):
    client = cleanup_request.getfixturevalue.return_value
    client.get_stream.return_value = object()
    cleanup = unwrap(conftest.cleanup_streams)(cleanup_request)
    await anext(cleanup)

    with pytest.raises(RuntimeError, match="still exists"):
        await anext(cleanup)

    assert cleanup_request.session.shouldstop


async def test_cleanup_timeout_stops_shared_client_reuse(cleanup_request, monkeypatch):
    monkeypatch.setattr(
        conftest,
        "STREAM_CLEANUP_TIMEOUT_SECONDS",
        CLEANUP_TEST_TIMEOUT_SECONDS,
    )
    client = cleanup_request.getfixturevalue.return_value
    pending = asyncio.Event()

    async def stalled_delete(_stream_id):
        await pending.wait()

    client.delete_stream.side_effect = stalled_delete
    cleanup = unwrap(conftest.cleanup_streams)(cleanup_request)
    await anext(cleanup)

    with pytest.raises(TimeoutError):
        await anext(cleanup)

    assert str(STREAM_ID) in cleanup_request.session.shouldstop
