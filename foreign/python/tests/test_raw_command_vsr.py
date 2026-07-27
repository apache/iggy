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

import os

import pytest

from apache_iggy import BinaryRequestKind, IggyClient

VENDOR_CODE = 60_001

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.vsr,
    pytest.mark.skipif(
        os.getenv("IGGY_TEST_PROTOCOL") != "vsr",
        reason="requires a VSR-built Python extension and VSR server",
    ),
]


async def test_vsr_raw_request_kinds_preserve_session_sequence(
    iggy_client: IggyClient, unique_name
):
    assert (
        await iggy_client.send_binary_request(BinaryRequestKind.NonReplicated, 1, b"")
        == b""
    )

    for _ in range(3):
        with pytest.raises(RuntimeError, match="(?i)invalid command"):
            await iggy_client.send_binary_request(
                BinaryRequestKind.NonReplicated, VENDOR_CODE, b"vendor-body"
            )

    with pytest.raises(RuntimeError, match="(?i)feature.*unavailable"):
        await iggy_client.send_binary_request(
            BinaryRequestKind.Replicated, VENDOR_CODE, b"vendor-body"
        )

    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_binary_request(BinaryRequestKind.Replicated, 10, b"")

    with pytest.raises(RuntimeError, match="(?i)invalid command"):
        await iggy_client.send_binary_request(BinaryRequestKind.NonReplicated, 38, b"")

    # A metadata mutation must still use request ID 1. Advancing the counter
    # for any non-replicated request would create a gap and stall this call.
    await iggy_client.create_stream(unique_name("raw-vsr-"))
