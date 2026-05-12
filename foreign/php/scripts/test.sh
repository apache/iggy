#!/bin/bash
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

set -euo pipefail

echo "PHP SDK Test Runner"
echo "==================="

IGGY_HOST="${IGGY_HOST:-127.0.0.1}"
IGGY_PORT="${IGGY_PORT:-8090}"

echo "Waiting for Iggy server at ${IGGY_HOST}:${IGGY_PORT}..."
timeout 60 bash -c "
    until timeout 5 bash -c '</dev/tcp/${IGGY_HOST}/${IGGY_PORT}'; do
        echo '   Server not ready, waiting...'
        sleep 2
    done
"
echo "Server is ready."

mkdir -p test-results

PHP_BIN="${PHP:-php}"
PHP_ARGS=()
if [ -n "${PHP_IGGY_EXTENSION:-}" ]; then
    PHP_ARGS+=("-d" "extension=${PHP_IGGY_EXTENSION}")
fi

"${PHP_BIN}" "${PHP_ARGS[@]}" tests/run.php "$@"
