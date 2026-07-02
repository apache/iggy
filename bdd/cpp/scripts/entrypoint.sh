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

bdd_wire_server &
server_pid=$!
sleep 2

cd /workspace/bdd/cpp || exit 1

set +e
bundle exec cucumber
status=$?
set -e

kill "$server_pid" 2>/dev/null || true
exit "$status"
