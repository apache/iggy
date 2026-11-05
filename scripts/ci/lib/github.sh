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

# GitHub REST calls for the scripts in scripts/ci. Source it after creating
# TMP, a private temporary directory: the token goes to curl in a header file
# there, never on its command line.
#
# Token: GITHUB_TOKEN, else `gh auth token`, else none, which is enough to
# read a public repo.

GITHUB_API_TOKEN="${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"
if [[ -n $GITHUB_API_TOKEN ]]; then
  printf 'Authorization: Bearer %s\n' "$GITHUB_API_TOKEN" > "$TMP/github-auth"
fi

# github METHOD PATH [JSON]: prints the response body. Exits on a network or
# HTTP error.
github() {
  local method=$1 path=$2 data=${3:-} status
  local args=(-sS -X "$method" -o "$TMP/github.json" -w '%{http_code}'
    -H 'Accept: application/vnd.github+json'
    -H 'X-GitHub-Api-Version: 2022-11-28')
  if [[ -n $GITHUB_API_TOKEN ]]; then
    args+=(-H "@$TMP/github-auth")
  fi
  if [[ -n $data ]]; then
    args+=(-H 'Content-Type: application/json' --data-binary "$data")
  fi
  if ! status=$(curl "${args[@]}" "https://api.github.com$path" 2>"$TMP/github.err"); then
    echo "GitHub $method $path: $(<"$TMP/github.err")" >&2
    exit 1
  fi
  if [[ $status != 2?? ]]; then
    echo "GitHub $method $path: HTTP $status $(<"$TMP/github.json")" >&2
    exit 1
  fi
  cat "$TMP/github.json"
}
