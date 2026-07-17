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

import pytest

from apache_iggy import IggyClient


@pytest.mark.asyncio
async def test_raw_ping_returns_empty_response(iggy_client: IggyClient):
    response = await iggy_client.send_raw_with_response(1, b"")

    assert response == b""


@pytest.mark.asyncio
async def test_raw_get_stats_returns_non_empty_response(iggy_client: IggyClient):
    response = await iggy_client.send_raw_with_response(10, b"")

    assert response


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [38, 39, 40, 44, 45])
async def test_raw_session_control_code_is_rejected(iggy_client: IggyClient, code: int):
    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_raw_with_response(code, b"")


@pytest.mark.asyncio
async def test_raw_unknown_code_is_rejected_by_server(iggy_client: IggyClient):
    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_raw_with_response(60_000, b"")
