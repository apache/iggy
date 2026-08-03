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
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from apache_iggy import ReceiveMessage  # noqa: E402
from common import (  # noqa: E402
    OrderType,
    PlainHeaderValue,
    connect,
    consume_messages,
    format_headers,
    log_order,
    parse_args,
)
from loguru import logger  # noqa: E402


def handle_message(message: ReceiveMessage) -> None:
    payload = json.loads(message.payload().decode("utf-8"))
    headers = message.user_headers()

    logger.info(
        f"Handling message at offset {message.offset()} "
        f"with origin timestamp {message.origin_timestamp()}."
    )

    scalar_headers: dict[PlainHeaderValue, PlainHeaderValue] = {}
    if headers is not None:
        # `to_scalar_dict` converts the typed headers stored on the wire back
        # into the convenient plain `dict[str, str | bytes | bool | int |
        # float]` form.
        scalar_headers = headers.to_scalar_dict()
        logger.info(f"Plain headers: {format_headers(scalar_headers)}")

    log_order(get_message_type(scalar_headers), payload)


def get_message_type(
    headers: dict[PlainHeaderValue, PlainHeaderValue],
) -> OrderType | None:
    message_type = headers.get("message-type")
    if isinstance(message_type, str):
        try:
            return OrderType(message_type)
        except ValueError:
            logger.warning(f"Received unknown message type: {message_type}")
    return None


async def main() -> None:
    args = parse_args()
    client = await connect(args.connection_string)
    await consume_messages(client, handle_message)


if __name__ == "__main__":
    asyncio.run(main())
