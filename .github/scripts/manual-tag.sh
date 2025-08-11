#!/bin/bash
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

TARGET_COMPONENT="${TARGET_COMPONENT:-all}"
TARGET_VERSION="${TARGET_VERSION:-}"        # now optional; auto-detect if empty
TARGET_COMMIT="${TARGET_COMMIT:-}"
USE_V_PREFIX="${USE_V_PREFIX:-false}"
DRY_RUN="${DRY_RUN:-true}"
GO_VERSION_OVERRIDE="${GO_VERSION_OVERRIDE:-}"

# Counters
TAGS_CREATED=0; TAGS_SKIPPED=0; TAGS_FAILED=0

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

# Component map: "<manifest>|<tag_prefix>|<display>"
declare -A COMPONENTS=(
  # crates.io / Rust
  ["server"]="core/server/Cargo.toml|server-|Apache Iggy Server"
  ["cli"]="core/cli/Cargo.toml|cli-|Apache Iggy CLI"
  ["sdk"]="core/sdk/Cargo.toml|iggy-|Apache Iggy SDK"
  ["iggy-bench"]="core/bench/bench/Cargo.toml|iggy-bench-|Apache Iggy Bench"

  # DockerHub (version taken from each component’s manifest)
  ["mcp"]="core/ai/mcp/Cargo.toml|mcp-|Apache Iggy MCP"
  ["connectors"]="core/connectors/runtime/Cargo.toml|connectors-|Apache Iggy Connectors"
  ["bench-dashboard"]="core/bench/dashboard/server/Cargo.toml|bench-dashboard-|Apache Iggy Bench Dashboard"
  ["web-ui"]="web/package.json|web-ui-|Apache Iggy Web UI"

  # Other SDKs
  ["python-sdk"]="foreign/python/pyproject.toml|python-sdk-|Apache Iggy Python SDK"
  ["node-sdk"]="foreign/node/package.json|node-sdk-|Apache Iggy Node SDK"
  ["java-sdk"]="foreign/java/build.gradle|java-sdk-|Apache Iggy Java SDK"
  ["csharp-sdk"]="foreign/csharp/Iggy_SDK/Iggy_SDK.csproj|csharp-sdk-|Apache Iggy C# SDK"

  # Go (special v-prefix path)
  ["go-sdk"]="foreign/go/go.mod|foreign/go/v|Apache Iggy Go SDK"
)

tag_exists() { git tag -l "$1" | grep -q "^$1$"; }

validate_commit() {
  local commit=$1
  git rev-parse --verify "$commit^{commit}" >/dev/null 2>&1
}

extract_version() {
  # Detect version from manifest contents at current checkout
  local manifest="$1"; local key="$2"; local ver=""

  if [[ "$key" == "go-sdk" ]]; then
    ver="$GO_VERSION_OVERRIDE"
    [[ -z "$ver" ]] && { echo -e "${RED}Go version required (GO_VERSION_OVERRIDE)${NC}" >&2; return 1; }
    echo "$ver"; return 0
  fi

  case "$manifest" in
    *.toml)
      # Cargo.toml or pyproject.toml: version = "x.y.z"
      ver=$(grep -m1 -E '^[[:space:]]*version[[:space:]]*=[[:space:]]*"[0-9a-zA-Z\.\-\+]+\"' "$manifest" \
            | sed -E 's/.*"[ ]*([0-9a-zA-Z\.\-\+]+)".*/\1/') || true
      ;;
    *.json)
      # package.json
      ver=$(grep -m1 -oP '"version"\s*:\s*"\K[^"]+' "$manifest" || true)
      ;;
    *.csproj|*.props|*.targets|*.xml)
      # .csproj: <Version> or <PackageVersion>
      ver=$(grep -m1 -oP '<(PackageVersion|Version)>\K[^<]+' "$manifest" || true)
      ;;
    *build.gradle*|*gradle*)
      # Try inline declaration: version = 'x.y.z'
      if [[ -f "$manifest" ]]; then
        ver=$(grep -m1 -E "version[[:space:]]*=[[:space:]]*['\"][0-9A-Za-z\.\-\+]+['\"]" "$manifest" \
              | sed -E "s/.*['\"]([0-9A-Za-z\.\-\+]+)['\"].*/\1/") || true
      fi
      # Fallback to Gradle properties query via wrapper (if present)
      if [[ -z "$ver" && -x "./foreign/java/gradlew" ]]; then
        ver=$(./foreign/java/gradlew -p foreign/java properties -q | awk -F': ' '/^version:/{print $2}' || true)
      fi
      ;;
  esac

  if [[ -z "$ver" ]]; then
    echo -e "${RED}Failed to detect version from ${manifest}${NC}" >&2
    return 1
  fi
  echo "$ver"
}

create_tag() {
  local tag_name="$1" component_name="$2" version="$3" commit_hash="$4" manifest="$5"

  local commit_date commit_author commit_subject
  commit_date=$(git log -1 --format="%ai" "$commit_hash")
  commit_author=$(git log -1 --format="%an <%ae>" "$commit_hash")
  commit_subject=$(git log -1 --format="%s" "$commit_hash")

  if [[ "$DRY_RUN" == "true" ]]; then
    echo -e "${YELLOW}[DRY RUN]${NC} Would create tag: ${BLUE}${tag_name}${NC}"
    echo -e "  ${CYAN}→ Component:${NC} $component_name"
    echo -e "  ${CYAN}→ Version:${NC}   $version"
    echo -e "  ${CYAN}→ Commit:${NC}    ${commit_hash:0:8} - $commit_subject"
    echo -e "  ${CYAN}→ Manifest:${NC}  $manifest"
    TAGS_CREATED=$((TAGS_CREATED + 1))
    return 0
  fi

  local msg
  msg="Release: $component_name v$version

Component: $component_name
Version: $version
Manifest: $manifest
Original commit: $commit_hash
Original date: $commit_date
Original author: $commit_author

Commit message: $commit_subject

Tagged by: GitHub Actions (manual trigger)
Tagged on: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"

  if git tag -a "$tag_name" "$commit_hash" -m "$msg"; then
    if git push origin "$tag_name"; then
      echo -e "${GREEN}✅ Created and pushed tag:${NC} ${BLUE}$tag_name${NC}"
      TAGS_CREATED=$((TAGS_CREATED + 1)); return 0
    else
      echo -e "${RED}❌ Failed to push tag:${NC} $tag_name"
      git tag -d "$tag_name" >/dev/null 2>&1 || true
      TAGS_FAILED=$((TAGS_FAILED + 1)); return 1
    fi
  else
    echo -e "${RED}❌ Failed to create tag:${NC} $tag_name"
    TAGS_FAILED=$((TAGS_FAILED + 1)); return 1
  fi
}

process_component() {
  local key="$1"
  local info="${COMPONENTS[$key]:-}"
  [[ -z "$info" ]] && { echo -e "${RED}❌ Unknown component: $key${NC}"; return 1; }

  IFS='|' read -r manifest tag_prefix display <<<"$info"

  echo -e "\n${BLUE}Processing $display${NC}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Validate commit exists (workflow checks out that ref, but keep guard)
  if [[ -n "$TARGET_COMMIT" ]] && ! validate_commit "$TARGET_COMMIT"; then
    echo -e "${RED}❌ Invalid commit: $TARGET_COMMIT${NC}"; return 1
  fi

  # Determine version (auto if not provided)
  local ver="$TARGET_VERSION"
  if [[ -z "$ver" ]]; then
    ver="$(extract_version "$manifest" "$key")" || { echo -e "${YELLOW}Skipping $display (no version)${NC}"; return 0; }
  fi

  # Build tag name
  local tag_name=""
  if [[ "$key" == "go-sdk" ]]; then
    # Always v-prefixed path for Go modules
    tag_name="${tag_prefix}${ver}"   # foreign/go/v<ver>
  else
    # No v prefix (you requested 'v' only for Go)
    tag_name="${tag_prefix}${ver}"
  fi

  echo -e "Tag name: ${BLUE}${tag_name}${NC}"

  if tag_exists "$tag_name"; then
    echo -e "${YELLOW}⏭️  Tag already exists, skipping${NC}"
    TAGS_SKIPPED=$((TAGS_SKIPPED + 1)); return 0
  fi

  # Create (and optionally push) the tag
  create_tag "$tag_name" "$display" "$ver" "${TARGET_COMMIT:-HEAD}" "$manifest"
}

main() {
  echo -e "${BLUE}🏷️  Manual Component Tagging${NC}"
  echo "════════════════════════════════════════"
  echo -e "  Component: ${BLUE}${TARGET_COMPONENT}${NC}"
  echo -e "  Commit:    ${BLUE}${TARGET_COMMIT:-HEAD}${NC}"
  echo -e "  Dry run:   ${BLUE}${DRY_RUN}${NC}"

  if [[ "$TARGET_COMPONENT" == "all" ]]; then
    for k in "${!COMPONENTS[@]}"; do process_component "$k"; done
  else
    process_component "$TARGET_COMPONENT"
  fi

  echo -e "\n════════════════════════════════════════"
  echo -e "${BLUE}📊 Summary${NC}"
  echo -e "  Tags created: ${GREEN}$TAGS_CREATED${NC}"
  echo -e "  Tags skipped: ${YELLOW}$TAGS_SKIPPED${NC}"
  echo -e "  Tags failed:  ${RED}$TAGS_FAILED${NC}"
  echo "TAGS_CREATED: $TAGS_CREATED"
}

main "$@"
