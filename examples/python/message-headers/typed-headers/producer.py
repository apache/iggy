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

from apache_iggy import HeaderKey, HeaderValue  # noqa: E402
from common import (  # noqa: E402
    Order,
    OrderType,
    TypedHeaders,
    connect,
    init_system,
    parse_args,
    produce_messages,
)


def build_typed_headers(order: Order) -> TypedHeaders:
    # `HeaderKey`/`HeaderValue` preserve an explicit wire type. Use this form
    # when you need control over the exact width/kind of a header (e.g. a
    # 16-bit unsigned int) rather than letting the SDK infer it.
    return {
        HeaderKey.String("message-type"): HeaderValue.String(order.order_type),
        HeaderKey.String("content-type"): HeaderValue.String("application/json"),
        HeaderKey.String("schema-version"): HeaderValue.UnsignedInt16(1),
        HeaderKey.String("created-at-ms"): HeaderValue.UnsignedInt64(
            int(time.time() * 1000)
        ),
        HeaderKey.String("retryable"): HeaderValue.Bool(
            order.order_type == OrderType.REJECTED
        ),
        HeaderKey.String("priority-score"): HeaderValue.Float32(0.5),
        HeaderKey.String("trace-bin"): HeaderValue.Raw(order.payload.encode()),
    }


async def main() -> None:
    args = parse_args()
    client = await connect(args.connection_string)
    await init_system(client)
    await produce_messages(client, build_typed_headers)


if __name__ == "__main__":
    asyncio.run(main())
