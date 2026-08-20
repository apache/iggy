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

set -Eeuo pipefail
umask 077

readonly github_scope_url="https://github.com/apache"
readonly runner_group="iggy-cloud-run-pilot"
readonly runner_labels="iggy-cloud-run-x64"
readonly runner_home="/home/runner"
readonly metadata_identity_url="http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"

if [[ -n "${RUNNER_REGISTRATION_TOKEN:-}" && -n "${RUNNER_JIT_CONFIG_BROKER_URL:-}" ]]; then
  echo "Configure either RUNNER_REGISTRATION_TOKEN or RUNNER_JIT_CONFIG_BROKER_URL, not both" >&2
  exit 78
fi

runner_name_seed="${HOSTNAME:-cloud-run}-${RANDOM}-${RANDOM}"
runner_name_hash="$(printf '%s' "$runner_name_seed" | sha256sum)"
readonly runner_name="iggy-cloud-run-${runner_name_hash:0:12}"
unset runner_name_seed runner_name_hash

if [[ -n "${RUNNER_JIT_CONFIG_BROKER_URL:-}" ]]; then
  if [[ -z "${RUNNER_JIT_CONFIG_BROKER_AUDIENCE:-}" ]]; then
    echo "RUNNER_JIT_CONFIG_BROKER_AUDIENCE is required with the broker URL" >&2
    exit 78
  fi

  if [[ "$RUNNER_JIT_CONFIG_BROKER_URL" != https://* ]]; then
    echo "RUNNER_JIT_CONFIG_BROKER_URL must use HTTPS" >&2
    exit 78
  fi

  identity_token="$(curl \
    --fail \
    --get \
    --header "Metadata-Flavor: Google" \
    --max-time 10 \
    --noproxy metadata.google.internal \
    --show-error \
    --silent \
    --data-urlencode "audience=${RUNNER_JIT_CONFIG_BROKER_AUDIENCE}" \
    --data-urlencode "format=full" \
    "$metadata_identity_url")"

  request_body="$(jq --compact-output --null-input \
    --arg runner_name "$runner_name" \
    '{runner_name: $runner_name}')"

  broker_response="$(curl \
    --fail \
    --header "Authorization: Bearer ${identity_token}" \
    --header "Content-Type: application/json" \
    --max-time 20 \
    --request POST \
    --show-error \
    --silent \
    --data "$request_body" \
    "$RUNNER_JIT_CONFIG_BROKER_URL")"

  encoded_jit_config="$(jq --exit-status --raw-output \
    '.encoded_jit_config | select(type == "string" and length > 0)' \
    <<< "$broker_response")"

  unset RUNNER_JIT_CONFIG_BROKER_URL RUNNER_JIT_CONFIG_BROKER_AUDIENCE
  identity_token=""
  request_body=""
  broker_response=""
  unset identity_token request_body broker_response

  cd "$runner_home"
  exec ./run.sh --jitconfig "$encoded_jit_config"
fi

if [[ -z "${RUNNER_REGISTRATION_TOKEN:-}" ]]; then
  echo "RUNNER_REGISTRATION_TOKEN or RUNNER_JIT_CONFIG_BROKER_URL is required" >&2
  exit 78
fi

registration_token="$RUNNER_REGISTRATION_TOKEN"
unset RUNNER_REGISTRATION_TOKEN

cd "$runner_home"
./config.sh \
  --disableupdate \
  --ephemeral \
  --labels "$runner_labels" \
  --name "$runner_name" \
  --runnergroup "$runner_group" \
  --token "$registration_token" \
  --unattended \
  --url "$github_scope_url" \
  --work _work

registration_token=""
unset registration_token

shutdown_requested=false
runner_process_id=""

forward_shutdown() {
  local received_signal="$1"

  shutdown_requested=true
  if [[ -n "$runner_process_id" ]]; then
    kill "-${received_signal}" "$runner_process_id" 2>/dev/null || true
  fi
}

trap 'forward_shutdown TERM' TERM
trap 'forward_shutdown INT' INT

set +e
./run.sh &
runner_process_id=$!
wait "$runner_process_id"
runner_exit_code=$?
set -e

if [[ "$shutdown_requested" == true ]]; then
  set +e
  wait "$runner_process_id" 2>/dev/null
  set -e
  exit "$runner_exit_code"
fi

runner_process_id=""
echo "Ephemeral runner exited with code ${runner_exit_code}; waiting for worker-pool scale-down"
exec tail -f /dev/null
