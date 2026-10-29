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

# Check that a PR description has a linked issue (or N/A), a description of
# the change, and test notes, the three things PULL_REQUEST_TEMPLATE.md asks
# for. It looks for the content anywhere in the body, so a free-form
# description passes when it covers all three. Plain pattern checks, no
# model. Used by .github/workflows/pr-description.yml and runnable by hand.
# Read-only: it never changes anything on GitHub.
#
# Exits 1 and names each missing part when the description has gaps. In
# Actions it also writes them to the job summary.
#
# Usage:
#   scripts/ci/pr-description-check.sh --pr 1234
#   scripts/ci/pr-description-check.sh --pr 1234 --repo owner/name
#
# Requires curl and jq.
#
# Configuration (environment):
#   GITHUB_TOKEN   token to read the PR. Falls back to `gh auth token`.
#                  Reading a public PR needs none.

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/init.sh"

USAGE="Usage: pr-description-check.sh --pr <number> [--repo owner/name]"

PR=""
REPO="${GITHUB_REPOSITORY:-apache/iggy}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pr) PR="${2:-}"; shift 2 || shift ;;
    --pr=*) PR="${1#*=}"; shift ;;
    --repo) REPO="${2:-}"; shift 2 || shift ;;
    --repo=*) REPO="${1#*=}"; shift ;;
    -h|--help) echo "$USAGE"; exit 0 ;;
    *) echo "$USAGE" >&2; exit 2 ;;
  esac
done
if [[ ! $PR =~ ^[0-9]+$ ]]; then
  echo "$USAGE" >&2
  exit 2
fi

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/github.sh"

github GET "/repos/$REPO/pulls/$PR" > "$TMP/pr.json"

# jq regexes anchor ^ and $ to the whole string, so line-based checks split
# the body into lines first.
FOUND=$(jq -r '
  # Drop template comments and the template lines left unedited.
  ((.body // "")
   | gsub("\r\n"; "\n")
   | gsub("<!--[\\s\\S]*?-->"; "")
   | split("\n")
   | map(select(test("^\\s*(Closes|Relates to)\\s+#\\s*$"; "i") | not))
   | map(select(test("^\\s*-?\\s*(Passed / not passed|Pre-commit hooks ran / not ran)\\s*$"; "i") | not))
   | join("\n")) as $body
  | ($body
     | capture("(?:\\A|\\n)##[ \\t]+Which issue does this PR address\\?[ \\t]*\\n(?<s>[\\s\\S]*?)(?=\\n##[ \\t]|\\z)").s
     // "") as $issue
  | ($body | split("\n") | map(select(test("^\\s*#") | not)) | join(" ")
     | gsub("\\s+"; " ") | sub("^ "; "") | sub(" $"; "")) as $prose
  # An explicit "N/A" or "None" under the issue heading is an answer.
  # "#123" or "owner/repo#123" must stand alone, so a colour like #007bff or
  # an entity like &#123; is not taken for an issue link.
  | (if ($body | test("(^|[^\\w&]|[\\w.-]+/[\\w.-]+)#\\d+\\b|github\\.com/apache/iggy/issues/\\d+") | not)
        and ($issue | split("\n") | any(test("^\\s*(N/A|None)\\b"; "i")) | not)
     then "Link the issue this PR addresses, for example `Closes #123` or `Relates to #123`, or write N/A under \"Which issue does this PR address?\"."
     else empty end),
    (if ($prose | length) < 100
     then "Describe why the change is needed and what it changes."
     else empty end),
    (if ($body | test("\\b(tested|tests? pass|passed|passing|ran|run locally|pre-commit|prek|cargo (test|nextest|clippy)|go test|pytest|npm test|dotnet test|mvn|gradlew?|verified|verification|nothing to run)\\b"; "i") | not)
     then "Say how you tested the change locally, under \"Local Execution\": passed or not passed, and whether pre-commit hooks ran."
     else empty end)
' "$TMP/pr.json")

if [[ -z $FOUND ]]; then
  echo "PR description covers the issue, the change and testing"
  exit 0
fi
LIST="- ${FOUND//$'\n'/$'\n'- }"
echo "PR description is missing parts of the template:"
echo "$LIST"
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
  {
    echo "### PR description is missing parts of the template"
    echo
    echo "$LIST"
    echo
    echo "Edit the PR description to fix these. The check runs again on each edit."
  } >> "$GITHUB_STEP_SUMMARY"
fi
exit 1
