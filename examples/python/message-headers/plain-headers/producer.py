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
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common import (  # noqa: E402
    Order,
    OrderType,
    PlainHeaders,
    connect,
    init_system,
    parse_args,
    produce_messages,
)


def build_plain_headers(order: Order) -> PlainHeaders:
    # The plain `dict[str, str | bytes | bool | int | float]` form is the
    # easiest way to attach headers: the SDK infers a wire type from each
    # Python scalar and translates it into the typed form on the wire.
    return {
        "message-type": order.order_type,
        "content-type": "application/json",
        "schema-version": 1,
        "created-at-ms": int(time.time() * 1000),
        "retryable": order.order_type == OrderType.REJECTED,
        "priority-score": 0.5,
        "trace-bin": order.payload.encode(),
    }


async def main() -> None:
    args = parse_args()
    client = await connect(args.connection_string)
    await init_system(client)
    await produce_messages(client, build_plain_headers)


if __name__ == "__main__":
    asyncio.run(main())
