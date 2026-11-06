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

"""The answers of the agent protocol: a result, an SDK error, an unsupported call or a
failure of the agent itself."""

from collections.abc import Awaitable
from typing import Any

from apache_iggy import IggyError, PendingOperationError, ReadError


class Unsupported(Exception):
    """The SDK has no such method, argument value, transport or entity kind."""


class Failure(Exception):
    """A fault of the agent itself, such as an argument that it cannot read."""


async def reply(call: Awaitable[Any]) -> dict[str, Any]:
    """Runs a call and turns its outcome into the fields of its answer."""
    try:
        return {"result": await call}
    except IggyError as error:
        return {"error": sdk_error(error)}
    except Unsupported as reason:
        return {"unsupported": str(reason)}
    except Failure as reason:
        return {"failure": str(reason)}
    except Exception as error:
        # An error without an Iggy code, such as a ValueError of the binding, is
        # not an answer of the SDK.
        return {"failure": f"{type(error).__name__}: {error}"}


def sdk_error(error: IggyError) -> dict[str, Any]:
    """An SDK error with its code, and the operation and the partition it names."""
    answer = {"code": error.code, "message": str(error)}
    cause = error.error if isinstance(error, ReadError) else error
    if isinstance(cause, PendingOperationError):
        answer["operation_id"] = cause.operation_id
        if cause.partition_id is not None:
            answer["partition_id"] = cause.partition_id
    if isinstance(error, ReadError) and error.partition_id is not None:
        answer.setdefault("partition_id", error.partition_id)
    return answer
