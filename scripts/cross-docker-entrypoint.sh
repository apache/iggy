#!/usr/bin/env bash

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

# Script designed for iggy internal tests for non-native / non-x86_64
# architectures inside Docker container (using cross tool).

set -euo pipefail

echo "Got following docker command \"$*\""

# If this is cargo build command do execute it
if [[ $* == *"cargo test"* ]]; then
    # Strip prefix (sh -c) as it is not evaluated properly under
    # dbus / gnome-keyring session.
    command="$*"
    command=${command#sh -c }

    # Unlock keyring and run command
    dbus-run-session -- sh -c "echo \"iggy\" | gnome-keyring-daemon --unlock && eval \"${command}\""
else
    # Otherwise (non cargo test scenario) just execute what has been requested
    exec "$@"
fi
