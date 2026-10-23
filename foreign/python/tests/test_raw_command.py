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

from datetime import timedelta

import pytest

from apache_iggy import IggyClient, IggyError, PendingOperationError


@pytest.mark.asyncio
async def test_raw_ping_returns_empty_response(iggy_client: IggyClient):
    response = await iggy_client.send_binary_request(1, b"")

    assert response == b""


@pytest.mark.asyncio
async def test_raw_get_stats_returns_non_empty_response(iggy_client: IggyClient):
    response = await iggy_client.send_binary_request(10, b"")

    assert response


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [38, 39, 40, 44, 45])
async def test_raw_session_control_code_is_rejected(iggy_client: IggyClient, code: int):
    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_binary_request(code, b"")


@pytest.mark.asyncio
async def test_raw_unknown_code_is_rejected_by_server(iggy_client: IggyClient):
    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_binary_request(60_000, b"")


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("operation_id", [2**53 + 17, 2**64 - 1])
@pytest.mark.parametrize("method", ["resolve_operation", "abandon_operation"])
async def test_missing_retained_operation_preserves_exact_identity(
    operation_id, method
):
    client = IggyClient("127.0.0.1:1")
    with pytest.raises(PendingOperationError) as raised:
        await getattr(client, method)(operation_id)
    assert raised.value.operation_id == operation_id
    assert raised.value.kind == "not_retained"
    assert raised.value.code == 14008
    assert isinstance(raised.value, IggyError)
    assert isinstance(raised.value, RuntimeError)
    with pytest.raises(AttributeError):
        # pyrefly: ignore  # read-only
        raised.value.operation_id = 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_on_unconnected_client_raises_its_code():
    client = IggyClient("127.0.0.1:1")
    with pytest.raises(IggyError) as raised:
        await client.send_binary_request(1, b"")
    assert raised.value.code == 61
    assert str(raised.value) == "Not connected"
    assert isinstance(raised.value, RuntimeError)
    with pytest.raises(AttributeError):
        # pyrefly: ignore  # read-only
        raised.value.code = 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_after_shutdown_raises_its_code():
    client = IggyClient("127.0.0.1:1")
    await client.shutdown()
    with pytest.raises(IggyError) as raised:
        await client.ping()
    assert raised.value.code == 63


@pytest.mark.unit
@pytest.mark.parametrize("timeout", [timedelta(0), timedelta(microseconds=-1)])
def test_resolve_rejects_nonpositive_timeout(timeout):
    client = IggyClient("127.0.0.1:1")
    with pytest.raises(ValueError):
        client.resolve_operation(1, timeout=timeout)


@pytest.mark.unit
@pytest.mark.parametrize("method", ["resolve_operation", "abandon_operation"])
@pytest.mark.parametrize("operation_id", [-1, 2**64])
def test_recovery_rejects_ids_outside_u64(method, operation_id):
    client = IggyClient("127.0.0.1:1")
    with pytest.raises(OverflowError):
        getattr(client, method)(operation_id)


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs", [{"send_retries": 3}, {"send_retry_interval": timedelta(seconds=1)}]
)
def test_removed_producer_retry_parameters_fail_clearly(kwargs):
    client = IggyClient("127.0.0.1:1")
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        client.producer("stream", "topic", **kwargs)
