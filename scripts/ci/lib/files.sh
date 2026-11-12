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

# Prints the files that a lint script checks. Mode `ci` prints the files that
# the pull request or the fork push changes, every other mode prints all
# tracked files. A pathspec limits both.
#
# Usage: get_files <mode> [pathspec...]
get_files() {
  local mode="$1"
  shift
  if [ "$mode" != ci ]; then
    git ls-files -- "$@"
  elif [ -n "${CI_BASE_SHA:-}" ]; then
    # A push in a fork has no pull request base. _detect.yml finds it.
    git diff --name-only --diff-filter=ACM "${CI_BASE_SHA}" HEAD -- "$@"
  elif [ -n "${GITHUB_BASE_REF:-}" ]; then
    git fetch --no-tags --depth=1 origin "${GITHUB_BASE_REF}:${GITHUB_BASE_REF}" 2>/dev/null || true
    git diff --name-only --diff-filter=ACM "${GITHUB_BASE_REF}...HEAD" -- "$@"
  elif [ -n "${CI:-}" ]; then
    git diff --name-only --diff-filter=ACM HEAD~1 -- "$@"
  else
    # Outside CI there is no base to compare with.
    git ls-files -- "$@"
  fi
}
