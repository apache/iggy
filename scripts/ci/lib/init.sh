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

# Single entry point for the script-backed pre-commit hooks. Sourcing this runs
# every preflight check, so a new check reaches all hooks by being added here
# rather than to each of them. A check that fails exits the sourcing script.
#
# Keep this parseable and runnable by bash 3.2, or the version check cannot
# report on the shell it is meant to reject.

ensure_bash_version() {
  local min_major=4
  local min_minor=2

  if [ "${BASH_VERSINFO[0]}" -gt "$min_major" ]; then
    return 0
  fi
  if [ "${BASH_VERSINFO[0]}" -eq "$min_major" ] && [ "${BASH_VERSINFO[1]}" -ge "$min_minor" ]; then
    return 0
  fi

  echo "ERROR: this script requires bash >= ${min_major}.${min_minor}, but is running under ${BASH_VERSION}" >&2
  echo "       interpreter: ${BASH}" >&2
  echo "       bash on PATH: $(command -v bash || echo '<none>')" >&2
  if [ "$(uname)" = "Darwin" ]; then
    echo >&2
    echo "macOS ships bash 3.2. Install a current one and make sure it comes first on PATH:" >&2
    echo "  brew install bash" >&2
    echo >&2
    echo "Git GUIs often launch hooks with a minimal PATH where /bin wins. If the command" >&2
    echo "above is already installed, commit from a terminal or fix the GUI's PATH." >&2
  fi
  exit 1
}

ensure_bash_version
