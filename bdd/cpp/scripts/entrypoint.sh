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

# Copy the mounted shared feature files, start the cucumber-cpp wire server, then run the
# Ruby Cucumber driver against it.
set -euo pipefail

if [ -d /app/features ]; then
    cp -f /app/features/*.feature /workspace/bdd/cpp/features/ 2>/dev/null || true
fi

# The image ships no .feature files; the compose mount provides them. Without this guard a
# missing mount would run zero scenarios and exit 0 even with --strict.
if ! ls /workspace/bdd/cpp/features/*.feature >/dev/null 2>&1; then
    echo "no .feature files under /workspace/bdd/cpp/features - is the scenarios mount missing?" >&2
    exit 1
fi

bdd_wire_server &
server_pid=$!

# Wait until the wire server is listening; a fixed sleep flakes on slow machines. The probe
# reads /proc/net/tcp (port 3902 = 0F3E hex, state 0A = LISTEN) instead of connecting,
# because the wire server accepts exactly one connection and a probe connect would consume
# the slot cucumber needs.
wire_ready=false
for _ in {1..50}; do
    if grep -qsE ':0F3E [0-9A-F:]+ 0A ' /proc/net/tcp /proc/net/tcp6; then
        wire_ready=true
        break
    fi
    sleep 0.2
done
if [ "$wire_ready" != true ]; then
    echo "wire server did not open port 3902 within 10 seconds" >&2
    kill "$server_pid" 2>/dev/null || true
    exit 1
fi

cd /workspace/bdd/cpp || exit 1

# --strict fails the run on undefined steps; without it a feature step with no matching C++
# definition would be reported and still exit 0.
set +e
bundle exec cucumber --strict
status=$?
set -e

kill "$server_pid" 2>/dev/null || true
exit "$status"
