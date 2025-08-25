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
#   ./extract-version.sh <component> [--tag] [--go-sdk-version <version>]
#
# Examples:
#   # Get version for Rust SDK
#   ./extract-version.sh rust-sdk                    # Output: 0.7.0
#
#   # Get git tag for Rust SDK
#   ./extract-version.sh rust-sdk --tag              # Output: iggy-0.7.0
#
#   # Get version for Python SDK
#   ./extract-version.sh sdk-python                  # Output: 0.5.0
#
#   # Get tag for Node SDK
#   ./extract-version.sh sdk-node --tag              # Output: node-sdk-0.5.0
#
#   # Get version for Go SDK (requires explicit version)
#   ./extract-version.sh sdk-go --go-sdk-version 1.2.3  # Output: 1.2.3
#
# The script uses the configuration from .github/config/publish.yml to determine:
#   - Where to find the version file (version_file)
#   - What regex pattern to use for extraction (version_regex)
#   - How to format the git tag (tag_pattern)
#   - Package name for Rust crates (package)

set -euo pipefail

# Check for required tools
if ! command -v yq &> /dev/null; then
    echo "Error: yq is required but not installed" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_FILE="$REPO_ROOT/.github/config/publish.yml"

# Parse arguments
COMPONENT="${1:-}"
RETURN_TAG=false
GO_SDK_VERSION=""

shift || true
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            RETURN_TAG=true
            shift
            ;;
        --go-sdk-version)
            GO_SDK_VERSION="${2:-}"
            shift 2 || shift
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$COMPONENT" ]]; then
    echo "Usage: $0 <component> [--tag] [--go-sdk-version <version>]" >&2
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

# Extract component configuration
get_config() {
    local key="$1"
    yq eval ".components.\"$COMPONENT\".$key // \"\"" "$CONFIG_FILE"
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
        # Extract version from XML tags like <PackageVersion> or <Version>
        grep -E '<(PackageVersion|Version)>' "$REPO_ROOT/$file" | head -1 | sed -E 's/.*<[^>]+>([^<]+)<.*/\1/' | tr -d ' '
    elif command -v perl &> /dev/null; then
        # Use perl for more powerful regex support (supports multiline and lookarounds)
        # Use m{} instead of // to avoid issues with slashes in regex
        perl -0777 -ne "if (m{$regex}) { print \$1; exit; }" "$REPO_ROOT/$file"
    else
        # Fallback to grep -P if available
        if grep -P "" /dev/null 2>/dev/null; then
            grep -Pzo "$regex" "$REPO_ROOT/$file" | grep -Pao '[0-9]+\.[0-9]+\.[0-9]+[^"]*' | head -1
        else
            # Basic fallback - may not work for all patterns
            grep -E "$regex" "$REPO_ROOT/$file" | head -1 | sed -E "s/.*$regex.*/\1/"
        fi
    fi
}

# Extract version using cargo metadata (for Rust packages)
extract_cargo_version() {
    local package="$1"
    local cargo_file="$2"

    cd "$REPO_ROOT"

    # Try cargo metadata first (most reliable)
    if command -v cargo &> /dev/null && command -v jq &> /dev/null; then
        version=$(cargo metadata --no-deps --format-version=1 2>/dev/null | \
                  jq -r --arg pkg "$package" '.packages[] | select(.name == $pkg) | .version' | \
                  head -1)

        if [[ -n "$version" ]]; then
            echo "$version"
            return 0
        fi
    fi

    # Fallback to direct Cargo.toml parsing using the regex from config
    local version_regex
    version_regex=$(get_config "version_regex")
    if [[ -n "$version_regex" && -f "$REPO_ROOT/$cargo_file" ]]; then
        extract_version_with_regex "$cargo_file" "$version_regex"
    fi
}

# Main version extraction logic
VERSION=""
VERSION_FILE=$(get_config "version_file")
VERSION_REGEX=$(get_config "version_regex")
PACKAGE=$(get_config "package")

# Special handling for Go SDK (version must be provided)
if [[ "$COMPONENT" == "sdk-go" ]]; then
    VERSION="$GO_SDK_VERSION"
    if [[ -z "$VERSION" ]]; then
        echo "Error: Go version must be provided with --go-version flag" >&2
        exit 1
    fi
# For Rust components with cargo metadata support
elif [[ "$COMPONENT" == rust-* ]] && [[ -n "$PACKAGE" ]]; then
    # Use package name from config if available
    VERSION=$(extract_cargo_version "$PACKAGE" "$VERSION_FILE")

    # Fallback to regex-based extraction if cargo metadata failed
    if [[ -z "$VERSION" ]] && [[ -n "$VERSION_FILE" ]] && [[ -n "$VERSION_REGEX" ]]; then
        VERSION=$(extract_version_with_regex "$VERSION_FILE" "$VERSION_REGEX")
    fi
# Generic extraction using version_file and version_regex
elif [[ -n "$VERSION_FILE" ]] && [[ -n "$VERSION_REGEX" ]]; then
    VERSION=$(extract_version_with_regex "$VERSION_FILE" "$VERSION_REGEX")
else
    echo "Error: No version extraction method available for component '$COMPONENT'" >&2
    exit 1
fi

# Validate version was found
if [[ -z "$VERSION" ]]; then
    echo "Error: Could not extract version for component '$COMPONENT'" >&2
    if [[ -n "$VERSION_FILE" ]]; then
        echo "  Checked file: $VERSION_FILE" >&2
    fi
    if [[ -n "$VERSION_REGEX" ]]; then
        echo "  Using regex: $VERSION_REGEX" >&2
    fi
    exit 1
fi

# Return tag or version based on flag
if [[ "$RETURN_TAG" == "true" ]]; then
    TAG_PATTERN=$(get_config "tag_pattern")
    if [[ -z "$TAG_PATTERN" ]]; then
        echo "Error: No tag pattern defined for component '$COMPONENT'" >&2
        exit 1
    fi

    # Replace the capture group in the pattern with the actual version
    # The pattern has a capture group like "^iggy-([0-9]+\\.[0-9]+\\.[0-9]+...)$"
    # We need to replace the (...) part with the actual version

    # Extract the prefix (everything before the first capture group)
    PREFIX=$(echo "$TAG_PATTERN" | sed -E 's/^(\^?)([^(]*)\(.*/\2/')

    # Extract the suffix (everything after the capture group)
    SUFFIX=$(echo "$TAG_PATTERN" | sed -E 's/.*\)[^)]*(\$?)$/\1/')

    # Build the tag
    TAG="${PREFIX}${VERSION}${SUFFIX}"

    # Remove regex anchors if present
    TAG=$(echo "$TAG" | sed 's/^\^//; s/\$$//')

    echo "$TAG"
else
    echo "$VERSION"
fi