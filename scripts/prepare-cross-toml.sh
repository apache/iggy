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

# This script is used to generate Cross.toml file for user which executes
# this script. This is needed since Cross.toml build.dockerfile.build-args
# section requires statically defined Docker build arguments and parameters
# like current UID or GID must be entered (cannot be generated or fetched
# during cross execution time).

readonly CROSS_TOML_FILE="Cross.toml"
USER_UID=$(id -u)
readonly USER_UID
USER_GID=$(id -g)
readonly USER_GID
USER_NAME=$(id -un)
readonly USER_NAME

echo "Preparing ${CROSS_TOML_FILE} file for user ${USER_NAME} with UID ${USER_UID} and GID ${USER_GID}."

cat << EOF > "${CROSS_TOML_FILE}"
[build.env]
passthrough = ["IGGY_SYSTEM_PATH", "IGGY_CI_BUILD", "RUST_BACKTRACE=1"]

[build.dockerfile]
file = "Dockerfile.cross"
build-args = { USER = "${USER_NAME}", CROSS_CONTAINER_UID = "${USER_UID}", CROSS_CONTAINER_GID = "${USER_GID}" }

EOF
