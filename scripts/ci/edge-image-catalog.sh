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

# The edge image catalog decides which Docker images post-merge refreshes
# with an :edge tag. A dockerhub component without a row is never refreshed,
# and a row that no longer lists its image's Dockerfile stops refreshing it
# after the Dockerfile moves. Neither failure is visible in CI, so this check
# keeps the catalog in step with publish.yml.

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/init.sh"

PUBLISH_CONFIG=".github/config/publish.yml"
CATALOG=".github/config/edge-image-variants.json"

usage() {
    echo "Usage: $0 --check"
    echo ""
    echo "Check that ${CATALOG} has exactly one row per dockerhub"
    echo "component in ${PUBLISH_CONFIG}, and that each row lists its Dockerfile."
}

case "${1:-}" in
    --check) ;;
    --help|-h)
        usage
        exit 0
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

COMPONENTS_JSON=$(yq -o=json -I=0 '.components' "$PUBLISH_CONFIG")

# A row lists a Dockerfile by its exact path or by a `dir/**` pattern above
# it. Any other pattern is reported, never assumed to cover the path.
PROBLEMS=$(jq -r --arg publish "$PUBLISH_CONFIG" --argjson components "$COMPONENTS_JSON" '
    def covers($path):
        . as $pattern
        | $pattern == $path
          or (($pattern | endswith("/**")) and ($path | startswith($pattern[:-2])));
    ($components | with_entries(select(.value.registry == "dockerhub"))) as $images
    | (reduce .variants[] as $row ({}; .[$row.id] = $row)) as $rows
    | ($images | keys[] | select($rows[.] == null)
        | "\(.) is a dockerhub component in \($publish) with no catalog row"),
      (.variants | group_by(.id)[] | select(length > 1)
        | "\(.[0].id) has \(length) catalog rows"),
      (.variants[] | select($images[.id] == null)
        | "catalog row \(.id) is not a dockerhub component in \($publish)"),
      (.variants[] | select(.dimensions.component != .id)
        | "catalog row \(.id) has dimensions.component \(.dimensions.component // "<none>")"),
      ($images | to_entries[] | select($rows[.key] != null)
        | .key as $id
        | (.value.dockerfile // "<none>") as $dockerfile
        | select(any($rows[$id].external_paths[]?; covers($dockerfile)) | not)
        | "catalog row \($id) does not list its Dockerfile \($dockerfile)")
' "$CATALOG")

if [[ -n "$PROBLEMS" ]]; then
    while IFS= read -r problem; do
        echo "ERROR: ${problem}" >&2
    done <<< "$PROBLEMS"
    echo "Update ${CATALOG} to match ${PUBLISH_CONFIG}." >&2
    exit 1
fi

echo "Edge image catalog matches ${PUBLISH_CONFIG}"
