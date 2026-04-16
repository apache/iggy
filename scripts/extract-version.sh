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

# Extract version information for Iggy components
#
# This script reads version information from various file formats based on
# the configuration in .github/config/publish.yml. It supports extracting
# versions from Cargo.toml, package.json, pyproject.toml, and other formats.
#
# Usage:
#   ./extract-version.sh <component> [--tag|--should-tag]
#   ./extract-version.sh --all
#   ./extract-version.sh --check
#
# Examples:
#   # Get version for Rust SDK
#   ./extract-version.sh rust-sdk                    # Output: 0.7.0
#
#   # Get git tag for Rust SDK
#   ./extract-version.sh rust-sdk --tag              # Output: iggy-0.7.0
#
#   # Check whether component should be tagged in git
#   ./extract-version.sh sdk-java --should-tag       # Output: false (SNAPSHOT)
#   ./extract-version.sh rust-sdk --should-tag       # Output: true
#
#   # Get version for Python SDK
#   ./extract-version.sh sdk-python                  # Output: 0.5.0
#
#   # Get tag for Node SDK
#   ./extract-version.sh sdk-node --tag              # Output: node-sdk-0.5.0
#
#   # Get version for Go SDK
#   ./extract-version.sh sdk-go                      # Output: 0.7.0
#
#   # List all components with versions
#   ./extract-version.sh --all
#
#   # Validate version consistency across the workspace
#   ./extract-version.sh --check
#
# The script uses the configuration from .github/config/publish.yml to determine:
#   - Where to find the version file (version_file)
#   - What regex pattern to use for extraction (version_regex)
#   - How to format the git tag (tag_pattern)
#   - Package name for Rust crates (package)

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

# Check for required tools
if ! command -v yq &> /dev/null; then
    echo "Error: yq is required but not installed" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_FILE="$REPO_ROOT/.github/config/publish.yml"

# Extract a single config field for a given component
get_config() {
    local component="$1"
    local key="$2"
    yq eval ".components.\"$component\".$key // \"\"" "$CONFIG_FILE"
}

# Generic regex-based extraction
extract_version_with_regex() {
    local file="$1"
    local regex="$2"

    if [[ ! -f "$REPO_ROOT/$file" ]]; then
        echo "Error: File not found: $file" >&2
        return 1
    fi

    # Special handling for XML files (C# .csproj)
    if [[ "$file" == *.csproj ]] || [[ "$file" == *.xml ]]; then
        grep -E '<(PackageVersion|Version)>' "$REPO_ROOT/$file" | head -1 | sed -E 's/.*<[^>]+>([^<]+)<.*/\1/' | tr -d ' '
    elif command -v perl &> /dev/null; then
        perl -0777 -ne "if (m{$regex}) { print \$1; exit; }" "$REPO_ROOT/$file"
    else
        if grep -P "" /dev/null 2>/dev/null; then
            grep -Pzo "$regex" "$REPO_ROOT/$file" | grep -Pao '[0-9]+\.[0-9]+\.[0-9]+[^"]*' | head -1
        else
            grep -E "$regex" "$REPO_ROOT/$file" | head -1 | sed -E "s/.*$regex.*/\1/"
        fi
    fi
}

# Extract version using cargo metadata (for Rust packages)
extract_cargo_version() {
    local package="$1"
    local cargo_file="$2"
    local component="$3"

    cd "$REPO_ROOT"

    # Caller-provided cache: if IGGY_CARGO_METADATA_FILE points at a
    # readable file containing `cargo metadata --no-deps --format-version=1`
    # JSON, use it instead of re-forking cargo. This is the fast path used
    # by .github/workflows/_publish_rust_crates.yml's Extract versions and
    # tags step, which needs 8 version lookups against the same workspace
    # snapshot and would otherwise pay the cargo metadata cost 8 times.
    # File-based (not env-var-based) because cargo metadata for a 36-crate
    # workspace is ~220 KB, which exceeds Linux's per-env-var limit
    # MAX_ARG_STRLEN (128 KB) and would fail with E2BIG on exec().
    if [[ -n "${IGGY_CARGO_METADATA_FILE:-}" ]] && [[ -r "${IGGY_CARGO_METADATA_FILE}" ]] \
       && command -v jq &> /dev/null; then
        local version
        version=$(jq -r --arg pkg "$package" \
                    '.packages[] | select(.name == $pkg) | .version' \
                    "${IGGY_CARGO_METADATA_FILE}" | head -1)
        if [[ -n "$version" ]]; then
            echo "$version"
            return 0
        fi
    fi

    if command -v cargo &> /dev/null && command -v jq &> /dev/null; then
        local version
        version=$(cargo metadata --no-deps --format-version=1 2>/dev/null | \
                  jq -r --arg pkg "$package" '.packages[] | select(.name == $pkg) | .version' | \
                  head -1)

        if [[ -n "$version" ]]; then
            echo "$version"
            return 0
        fi
    fi

    local version_regex
    version_regex=$(get_config "$component" "version_regex")
    if [[ -n "$version_regex" && -f "$REPO_ROOT/$cargo_file" ]]; then
        extract_version_with_regex "$cargo_file" "$version_regex"
    fi
}

# Extract version for a named component. Prints the version string to stdout.
extract_component_version() {
    local component="$1"
    local version=""
    local version_file
    local version_regex
    local package

    version_file=$(get_config "$component" "version_file")
    version_regex=$(get_config "$component" "version_regex")
    package=$(get_config "$component" "package")

    if [[ "$component" == rust-* ]] && [[ -n "$package" ]]; then
        version=$(extract_cargo_version "$package" "$version_file" "$component")

        if [[ -z "$version" ]] && [[ -n "$version_file" ]] && [[ -n "$version_regex" ]]; then
            version=$(extract_version_with_regex "$version_file" "$version_regex")
        fi
    elif [[ -n "$version_file" ]] && [[ -n "$version_regex" ]]; then
        version=$(extract_version_with_regex "$version_file" "$version_regex")
    fi

    echo "$version"
}

# ── --all mode: print a table of all components ──────────────────────────────
handle_all() {
    local components
    components=$(yq eval '.components | keys | .[]' "$CONFIG_FILE")

    printf "%-28s %s\n" "COMPONENT" "VERSION"
    printf "%-28s %s\n" "---------" "-------"

    while IFS= read -r comp; do
        local version
        version=$(extract_component_version "$comp")

        if [[ -z "$version" ]]; then
            version="(error)"
        fi

        printf "%-28s %s\n" "$comp" "$version"
    done <<< "$components"
}

# ── --check mode: validate version consistency ───────────────────────────────
handle_check() {
    local errors=0
    local passes=0

    # --- Check 1: Workspace dep consistency ---
    echo "=== Workspace dep consistency ==="
    local ws_cargo="$REPO_ROOT/Cargo.toml"

    while IFS= read -r line; do
        local pkg_name dep_version
        # Extract package name (left of '=') and version from the dep spec
        pkg_name=$(echo "$line" | sed -E 's/^([a-z_-]+)[[:space:]]*=.*/\1/')
        dep_version=$(echo "$line" | sed -E 's/.*version[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/')

        if [[ -z "$pkg_name" ]] || [[ -z "$dep_version" ]]; then
            continue
        fi

        # Find the matching component in publish.yml by its package field
        local comp
        comp=$(yq eval "[.components | to_entries[] | select(.value.package == \"$pkg_name\") | .key] | .[0] // \"\"" "$CONFIG_FILE")

        if [[ -z "$comp" ]]; then
            # No matching publish.yml component - skip silently
            continue
        fi

        local comp_version
        comp_version=$(extract_component_version "$comp")

        if [[ "$dep_version" == "$comp_version" ]]; then
            echo -e "  ${GREEN}PASS${NC} $pkg_name: workspace=$dep_version, component=$comp_version"
            passes=$((passes + 1))
        else
            echo -e "  ${RED}FAIL${NC} $pkg_name: workspace=$dep_version, component($comp)=$comp_version"
            errors=$((errors + 1))
        fi
    done < <(grep -E '^iggy[a-z_-]* = \{ path = .*, version = ".*" \}' "$ws_cargo")

    echo ""

    # --- Check 2: Python dual-file sync ---
    echo "=== Python dual-file sync ==="
    local python_script="$SCRIPT_DIR/ci/python-sdk-version-sync.sh"
    if [[ -x "$python_script" ]]; then
        if "$python_script" --check; then
            passes=$((passes + 1))
        else
            errors=$((errors + 1))
        fi
    else
        echo -e "  ${RED}FAIL${NC} python-sdk-version-sync.sh not found or not executable"
        errors=$((errors + 1))
    fi

    echo ""

    # --- Summary ---
    local total=$((passes + errors))
    echo "=== Summary ==="
    echo -e "  Passed: ${GREEN}${passes}${NC}/${total}"
    if [[ $errors -gt 0 ]]; then
        echo -e "  Failed: ${RED}${errors}${NC}/${total}"
        exit 1
    else
        echo -e "  ${GREEN}All checks passed.${NC}"
    fi
}

# ── Argument parsing ─────────────────────────────────────────────────────────
COMPONENT=""
RETURN_TAG=false
RETURN_SHOULD_TAG=false
RETURN_IS_PRE_RELEASE=false

# Detect mode flags as first argument only
case "${1:-}" in
    --all)   handle_all; exit 0;;
    --check) handle_check; exit 0;;
esac

# Original single-component flow
COMPONENT="${1:-}"

shift || true
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            RETURN_TAG=true
            shift
            ;;
        --should-tag)
            RETURN_SHOULD_TAG=true
            shift
            ;;
        --is-pre-release)
            RETURN_IS_PRE_RELEASE=true
            shift
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

mutex_count=0
[[ "$RETURN_TAG" == "true" ]] && mutex_count=$((mutex_count + 1))
[[ "$RETURN_SHOULD_TAG" == "true" ]] && mutex_count=$((mutex_count + 1))
[[ "$RETURN_IS_PRE_RELEASE" == "true" ]] && mutex_count=$((mutex_count + 1))
if [[ $mutex_count -gt 1 ]]; then
    echo "Error: --tag, --should-tag, and --is-pre-release are mutually exclusive" >&2
    exit 1
fi

if [[ -z "$COMPONENT" ]]; then
    echo "Usage: $0 <component> [--tag|--should-tag|--is-pre-release]" >&2
    echo "       $0 --all" >&2
    echo "       $0 --check" >&2
    echo "" >&2
    echo "  --tag             Print the git tag this component would use for its current version." >&2
    echo "  --should-tag      Print 'true' if the current version should produce a git tag, 'false'" >&2
    echo "                    otherwise (SNAPSHOT or missing tag_pattern). This is the SINGLE" >&2
    echo "                    source of truth for taggability; publish.yml consults it for every" >&2
    echo "                    SDK matrix row." >&2
    echo "  --is-pre-release  Print 'true' if the current version is a pre-release/pre-stable" >&2
    echo "                    marker across ANY SDK version scheme (-edge, -rc, .devN, bare rcN)," >&2
    echo "                    'false' otherwise. SINGLE source of truth for the auto-publish and" >&2
    echo "                    stable-Docker skip rules in post-merge.yml and publish.yml." >&2
    echo "" >&2
    echo "  --tag, --should-tag, and --is-pre-release are mutually exclusive." >&2
    echo "" >&2
    echo "Available components:" >&2
    yq eval '.components | keys | .[]' "$CONFIG_FILE" | sed 's/^/  - /' >&2
    exit 1
fi

# Check if component exists
if ! yq eval ".components.\"$COMPONENT\"" "$CONFIG_FILE" | grep -q .; then
    echo "Error: Unknown component '$COMPONENT'" >&2
    echo "" >&2
    echo "Available components:" >&2
    yq eval '.components | keys | .[]' "$CONFIG_FILE" | sed 's/^/  - /' >&2
    exit 1
fi

# Main version extraction logic
VERSION=$(extract_component_version "$COMPONENT")

# Validate version was found
if [[ -z "$VERSION" ]]; then
    echo "Error: Could not extract version for component '$COMPONENT'" >&2
    local_vf=$(get_config "$COMPONENT" "version_file")
    local_vr=$(get_config "$COMPONENT" "version_regex")
    if [[ -n "$local_vf" ]]; then
        echo "  Checked file: $local_vf" >&2
    fi
    if [[ -n "$local_vr" ]]; then
        echo "  Using regex: $local_vr" >&2
    fi
    exit 1
fi

# --should-tag: derive whether this version should produce a git tag.
#
# This is THE SINGLE SOURCE OF TRUTH for taggability. Every component
# matrix row in .github/workflows/publish.yml consults this value and
# gates its create-git-tag step on it. Any new SDK whose versioning
# model has mutable pre-release states (another -SNAPSHOT-style language,
# for example) MUST extend the rules here, not add ad-hoc conditions in
# publish.yml - otherwise the SDK matrix and the Docker manifests job
# diverge on what counts as a release.
#
# A component is taggable when (a) it declares a tag_pattern in
# publish.yml and (b) its version is not a -SNAPSHOT placeholder (Java
# mutable releases). The Docker-only "create_edge_docker_tag stable skip"
# rule depends on a workflow input, not the version, and stays in the
# workflow (it layers on top of this result).
if [[ "$RETURN_SHOULD_TAG" == "true" ]]; then
    TAG_PATTERN=$(get_config "$COMPONENT" "tag_pattern")
    if [[ -z "$TAG_PATTERN" ]]; then
        echo "false"
        exit 0
    fi
    if [[ "$VERSION" == *-SNAPSHOT ]]; then
        echo "false"
        exit 0
    fi
    echo "true"
    exit 0
fi

# --is-pre-release: returns "true" for versions that are pre-release/
# pre-stable markers across ALL SDK version schemes we publish. This is
# THE SINGLE SOURCE OF TRUTH for the "is this a pre-release" rule.
# post-merge.yml uses it to decide whether to auto-publish; publish.yml
# uses it for the auto-publish stable-Docker skip rule. Keeping one
# regex here prevents the two call sites from drifting (which they
# previously did - post-merge.yml accepted `.devN` and bare `rcN` while
# publish.yml only accepted `-edge`/`-rc`, so a Python SDK `.devN`
# version would be auto-published to PyPI but never git-tagged).
#
# Matches (any of):
#   -edge[.N]   (rust crates, docker, node SDK)
#   -rc[.N]     (all SDKs)
#   .devN       (Python SDK PEP 440 development markers)
#   rcN$        (legacy bare rcN, retained for compatibility)
if [[ "$RETURN_IS_PRE_RELEASE" == "true" ]]; then
    if [[ "$VERSION" =~ -(edge|rc) ]] \
       || [[ "$VERSION" =~ \.dev[0-9]+$ ]] \
       || [[ "$VERSION" =~ rc[0-9]+$ ]]; then
        echo "true"
    else
        echo "false"
    fi
    exit 0
fi

# Return tag or version based on flag
if [[ "$RETURN_TAG" == "true" ]]; then
    TAG_PATTERN=$(get_config "$COMPONENT" "tag_pattern")
    if [[ -z "$TAG_PATTERN" ]]; then
        echo "Error: No tag pattern defined for component '$COMPONENT'" >&2
        exit 1
    fi

    PREFIX=$(echo "$TAG_PATTERN" | sed -E 's/^(\^?)([^(]*)\(.*/\2/')
    SUFFIX=$(echo "$TAG_PATTERN" | sed -E 's/.*\)[^)]*(\$?)$/\1/')
    TAG="${PREFIX}${VERSION}${SUFFIX}"
    TAG=$(echo "$TAG" | sed 's/^\^//; s/\$$//')

    echo "$TAG"
else
    echo "$VERSION"
fi
