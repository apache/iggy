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

from typing import cast

import pytest

from apache_iggy import BinaryRequestKind, IggyClient

KINDS = [BinaryRequestKind.NonReplicated, BinaryRequestKind.Replicated]

# No server registers a handler for this code, and it sits past every range the
# protocol assigns.
VENDOR_CODE = 60_001


@pytest.mark.asyncio
async def test_raw_ping_returns_empty_response(iggy_client: IggyClient):
    response = await iggy_client.send_binary_request(
        BinaryRequestKind.NonReplicated, 1, b""
    )

    assert response == b""


@pytest.mark.asyncio
async def test_raw_get_stats_returns_non_empty_response(iggy_client: IggyClient):
    response = await iggy_client.send_binary_request(
        BinaryRequestKind.NonReplicated, 10, b""
    )

    assert response


@pytest.mark.asyncio
async def test_raw_replicated_declaration_is_ignored_on_classic_framing(
    iggy_client: IggyClient,
):
    # The kind is inert on classic framing, so a replicated declaration on a
    # standard command still succeeds.
    response = await iggy_client.send_binary_request(
        BinaryRequestKind.Replicated, 10, b""
    )

    assert response


@pytest.mark.asyncio
async def test_raw_undefined_kind_is_rejected_before_sending(
    iggy_client: IggyClient,
):
    # The cast defeats the static signature on purpose: the runtime boundary
    # itself must reject a value that is not a BinaryRequestKind.
    with pytest.raises(TypeError):
        await iggy_client.send_binary_request(cast(BinaryRequestKind, "auto"), 1, b"")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("code", [38, 39, 40, 44, 45])
async def test_raw_session_control_code_is_rejected(
    iggy_client: IggyClient, kind: BinaryRequestKind, code: int
):
    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_binary_request(kind, code, b"")


@pytest.mark.asyncio
async def test_raw_vendor_code_is_rejected_by_server(iggy_client: IggyClient):
    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_binary_request(
            BinaryRequestKind.NonReplicated, VENDOR_CODE, b""
        )

    # The rejection is request-level, so the connection stays usable.
    assert (
        await iggy_client.send_binary_request(BinaryRequestKind.NonReplicated, 1, b"")
        == b""
    )


@pytest.mark.asyncio
async def test_raw_replicated_vendor_code_has_no_handler(iggy_client: IggyClient):
    with pytest.raises(RuntimeError):
        await iggy_client.send_binary_request(
            BinaryRequestKind.Replicated, VENDOR_CODE, b""
        )
