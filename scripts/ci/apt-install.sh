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

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/ci/apt-install.sh [apt-get install flags] <package>...

Install Debian packages behind a bounded `apt-get update`.

GitHub runners intermittently lose egress to azure.archive.ubuntu.com. apt
falls back through /etc/apt/apt-mirrors.txt and can then wedge fetching the
package indices with no output and no timeout of its own, so the job sits
dead until it burns the whole `timeout-minutes` budget. apt's own
Acquire::*::Timeout does not bound this - the observed stalls ran for half an
hour past the 120s default - so both phases are capped externally instead.

The install runs even when every update attempt failed, so a stale but
present index set is not fatal.

Environment:
  APT_UPDATE_TIMEOUT    Seconds allowed per update attempt (default: 120)
  APT_UPDATE_ATTEMPTS   Update attempts before giving up (default: 3)
  APT_INSTALL_TIMEOUT   Seconds allowed for the install (default: 600)
USAGE
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  "")
    usage >&2
    exit 1
    ;;
esac

timeout_seconds="${APT_UPDATE_TIMEOUT:-120}"
attempts="${APT_UPDATE_ATTEMPTS:-3}"
install_timeout="${APT_INSTALL_TIMEOUT:-600}"

for attempt in $(seq 1 "${attempts}"); do
  if sudo timeout --kill-after=10 "${timeout_seconds}" apt-get \
    -o Acquire::Retries=2 \
    -o Acquire::http::Timeout=15 \
    -o Acquire::https::Timeout=15 \
    update; then
    break
  fi
  echo "::warning::apt-get update attempt ${attempt}/${attempts} timed out or failed"
  if [ "${attempt}" -lt "${attempts}" ]; then
    sleep 5
  fi
done

# Generous cap: a healthy install of the heaviest package set here takes well
# under a minute, and killing dpkg mid-configure leaves a broken package DB.
if ! sudo timeout --kill-after=10 "${install_timeout}" apt-get install -y "$@"; then
  echo "::error::apt-get install exceeded ${install_timeout}s or failed"
  exit 1
fi
