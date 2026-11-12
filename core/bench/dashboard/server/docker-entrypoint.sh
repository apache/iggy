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

set -e

echo "Checking performance_results directory:"
ls -la /data/performance_results
echo "Current user:"
id

ARGS=()
ARGS+=(--host "${HOST}")
ARGS+=(--port "${PORT}")
ARGS+=(--results-dir "${RESULTS_DIR}")
if [ -n "${CONFIG:-}" ]; then
    ARGS+=(--config "${CONFIG}")
fi
if [ -n "${CACHE_DIR:-}" ]; then
    ARGS+=(--cache-dir "${CACHE_DIR}")
fi

if [ -n "${GITHUB_TOKEN:-}" ]; then
    echo "Warning: GITHUB_TOKEN is set, but the server no longer polls GitHub. Put new results into ${RESULTS_DIR}." >&2
fi

exec /app/iggy-bench-dashboard-server "${ARGS[@]}"
