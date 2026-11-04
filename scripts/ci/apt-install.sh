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

Install Debian packages behind bounded update and download attempts.

GitHub runners intermittently lose egress to azure.archive.ubuntu.com. apt
falls back through /etc/apt/apt-mirrors.txt and can then wedge fetching the
package indices with no output and no timeout of its own, so the job sits
dead until it burns the whole `timeout-minutes` budget. apt's own
Acquire::*::Timeout does not bound this - the observed stalls ran for half an
hour past the 120s default - so both phases are capped externally instead.

The install runs even when every update attempt failed, so a stale but
present index set is not fatal. Downloads are separate from installation so a
failed mirror cannot interrupt dpkg while it configures packages.

Environment:
  APT_UPDATE_TIMEOUT    Seconds allowed per update attempt (default: 60)
  APT_UPDATE_ATTEMPTS   Update attempts before giving up (default: 2)
  APT_DOWNLOAD_TIMEOUT  Seconds allowed per download attempt (default: 90)
  APT_DOWNLOAD_ATTEMPTS Download attempts before giving up (default: 2)
  APT_INSTALL_TIMEOUT   Seconds allowed for package configuration (default: 180)
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

update_timeout="${APT_UPDATE_TIMEOUT:-60}"
update_attempts="${APT_UPDATE_ATTEMPTS:-2}"
download_timeout="${APT_DOWNLOAD_TIMEOUT:-90}"
download_attempts="${APT_DOWNLOAD_ATTEMPTS:-2}"
install_timeout="${APT_INSTALL_TIMEOUT:-180}"
apt_options=(-o Acquire::Retries=2 -o Acquire::http::Timeout=15 -o Acquire::https::Timeout=15)

switch_from_azure_mirror() {
  local source
  local changed=0
  for source in /etc/apt/apt-mirrors.txt /etc/apt/sources.list /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
    [ -f "$source" ] || continue
    if grep -q 'azure.archive.ubuntu.com' "$source"; then
      sudo sed -i 's@azure\.archive\.ubuntu\.com@archive.ubuntu.com@g' "$source"
      changed=1
    fi
  done
  if [ "$changed" -eq 1 ]; then
    echo "::warning::Switched apt sources from Azure to archive.ubuntu.com"
  fi
}

for attempt in $(seq 1 "${update_attempts}"); do
  if sudo timeout --kill-after=10 "${update_timeout}" apt-get "${apt_options[@]}" update; then
    break
  fi
  echo "::warning::apt-get update attempt ${attempt}/${update_attempts} timed out or failed"
  if [ "$attempt" -eq 1 ]; then
    switch_from_azure_mirror
  fi
done

for attempt in $(seq 1 "${download_attempts}"); do
  if sudo timeout --kill-after=10 "${download_timeout}" apt-get "${apt_options[@]}" --download-only install -y "$@"; then
    break
  fi
  echo "::warning::apt-get download attempt ${attempt}/${download_attempts} timed out or failed"
  if [ "$attempt" -eq 1 ]; then
    switch_from_azure_mirror
    sudo timeout --kill-after=10 "${update_timeout}" apt-get "${apt_options[@]}" update || true
  fi
  if [ "$attempt" -eq "$download_attempts" ]; then
    echo "::error::apt-get could not download the requested packages"
    exit 1
  fi
done

if ! sudo timeout --kill-after=10 "${install_timeout}" apt-get --no-download install -y "$@"; then
  echo "::error::apt-get package configuration exceeded ${install_timeout}s or failed"
  exit 1
fi
