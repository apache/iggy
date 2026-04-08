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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

usage() {
    cat <<'EOF'
Usage:
  bump-version.sh <component> <flags>
  bump-version.sh --status [<component>]

Components:
  rust-sdk             core/sdk + workspace dep + python iggy dep
  rust-common          core/common + workspace dep
  rust-binary-protocol core/binary_protocol + workspace dep
  rust-server          core/server
  rust-cli             core/cli + workspace dep
  rust-connector-sdk   core/connectors/sdk + workspace dep
  rust-all             All Rust crates at once
  --all                All components (Rust + SDKs)
  sdk-python           foreign/python/Cargo.toml + foreign/python/pyproject.toml
  sdk-node             foreign/node/package.json
  sdk-go               foreign/go/contracts/version.go
  sdk-csharp           foreign/csharp/Iggy_SDK/Iggy_SDK.csproj
  sdk-java             foreign/java/gradle.properties

Version flags (exactly one required):
  --patch       Bump patch version (0.9.3 -> 0.9.4)
  --minor       Bump minor version (0.9.3 -> 0.10.0)
  --major       Bump major version (0.9.3 -> 1.0.0)
  --edge        Increment edge counter (0.9.3-edge.1 -> 0.9.3-edge.2)
                Or add -edge.1 when combined with --patch/--minor/--major
  --strip-edge  Remove edge suffix (0.9.3-edge.1 -> 0.9.3)
  --set V       Set explicit version V (use --force to bypass validation)

Modifiers:
  --dry-run     Preview changes without writing (default: writes immediately)
  --force       Bypass validation (only with --set)

Examples:
  bump-version.sh rust-all --patch --edge       # 0.9.3 -> 0.9.4-edge.1
  bump-version.sh rust-all --edge               # 0.9.3-edge.1 -> 0.9.3-edge.2
  bump-version.sh rust-all --strip-edge         # 0.9.3-edge.1 -> 0.9.3
  bump-version.sh rust-all --minor --edge --dry-run
  bump-version.sh --all --strip-edge
  bump-version.sh rust-sdk --patch
  bump-version.sh sdk-python --set 0.8.0
  bump-version.sh --status
EOF
}

RUST_COMPONENTS="rust-sdk rust-common rust-binary-protocol rust-server rust-cli rust-connector-sdk"
SDK_COMPONENTS="sdk-python sdk-node sdk-go sdk-csharp sdk-java"
ALL_COMPONENTS="${RUST_COMPONENTS} ${SDK_COMPONENTS}"

# Returns "file:format" lines per component.
# Format keys: cargo, cargo-ws-dep:PKG, cargo-dep:PKG, python-cargo, pyproject, json, csproj, gradle, go
get_version_files() {
    local component="$1"
    case "$component" in
        rust-sdk)
            echo "core/sdk/Cargo.toml:cargo"
            echo "Cargo.toml:cargo-ws-dep:iggy"
            echo "foreign/python/Cargo.toml:cargo-dep:iggy"
            ;;
        rust-common)
            echo "core/common/Cargo.toml:cargo"
            echo "Cargo.toml:cargo-ws-dep:iggy_common"
            ;;
        rust-binary-protocol)
            echo "core/binary_protocol/Cargo.toml:cargo"
            echo "Cargo.toml:cargo-ws-dep:iggy_binary_protocol"
            ;;
        rust-server)
            echo "core/server/Cargo.toml:cargo"
            ;;
        rust-cli)
            echo "core/cli/Cargo.toml:cargo"
            echo "Cargo.toml:cargo-ws-dep:iggy-cli"
            ;;
        rust-connector-sdk)
            echo "core/connectors/sdk/Cargo.toml:cargo"
            echo "Cargo.toml:cargo-ws-dep:iggy_connector_sdk"
            ;;
        sdk-python)
            echo "foreign/python/Cargo.toml:python-cargo"
            echo "foreign/python/pyproject.toml:pyproject"
            ;;
        sdk-node)
            echo "foreign/node/package.json:json"
            ;;
        sdk-go)
            echo "foreign/go/contracts/version.go:go"
            ;;
        sdk-csharp)
            echo "foreign/csharp/Iggy_SDK/Iggy_SDK.csproj:csproj"
            ;;
        sdk-java)
            echo "foreign/java/gradle.properties:gradle"
            ;;
        *)
            echo -e "${RED}Unknown component: ${component}${NC}" >&2
            echo "Valid: rust-all ${ALL_COMPONENTS}" >&2
            return 1
            ;;
    esac
}

# Parse canonical semver into "base pre_type pre_num".
# "0.9.3-edge.1" -> "0.9.3 edge 1", "0.9.3" -> "0.9.3 stable 0"
parse_version() {
    local ver="$1"
    if [[ "$ver" =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
        echo "$ver stable 0"
    elif [[ "$ver" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-edge\.([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]} edge ${BASH_REMATCH[2]}"
    else
        echo -e "${RED}Cannot parse version: ${ver}${NC}" >&2
        return 1
    fi
}

# Compute next version from current + keyword + flags.
# Args: current keyword add_edge(0|1) strip_edge(0|1)
compute_next_version() {
    local current="$1" keyword="$2" add_edge="${3:-0}" strip_edge="${4:-0}"
    local parsed base pre_type pre_num
    parsed=$(parse_version "$current") || return 1
    read -r base pre_type pre_num <<< "$parsed"

    local major minor patch
    IFS='.' read -r major minor patch <<< "$base"

    # --strip-edge: remove edge suffix
    if [[ $strip_edge -eq 1 ]]; then
        if [[ "$pre_type" != "edge" ]]; then
            echo -e "${RED}Cannot --strip-edge: ${current} has no edge suffix${NC}" >&2
            return 1
        fi
        echo "$base"
        return 0
    fi

    # edge keyword: increment edge counter
    if [[ "$keyword" == "edge" ]]; then
        if [[ "$pre_type" != "edge" ]]; then
            echo -e "${RED}Cannot 'edge' from stable version ${current}${NC}" >&2
            echo -e "  Use ${GREEN}patch --edge${NC} or ${GREEN}minor --edge${NC} instead" >&2
            return 1
        fi
        echo "${base}-edge.$((pre_num + 1))"
        return 0
    fi

    # patch/minor/major: bump base version
    local new_base
    case "$keyword" in
        patch) new_base="${major}.${minor}.$((patch + 1))" ;;
        minor) new_base="${major}.$((minor + 1)).0" ;;
        major) new_base="$((major + 1)).0.0" ;;
        *)     echo -e "${RED}Invalid keyword: ${keyword}${NC}" >&2; return 1 ;;
    esac

    if [[ $add_edge -eq 1 ]]; then
        echo "${new_base}-edge.1"
    else
        echo "${new_base}"
    fi
}

# Convert canonical semver to ecosystem-specific format.
translate_version() {
    local canonical="$1" format="$2"
    local parsed base pre_type pre_num
    parsed=$(parse_version "$canonical" 2>/dev/null) || { echo "$canonical"; return 0; }
    read -r base pre_type pre_num <<< "$parsed"

    case "$format" in
        cargo|cargo-ws-dep:*|cargo-dep:*|json|csproj|go)
            echo "$canonical" ;;
        python-cargo)
            case "$pre_type" in
                edge)   echo "${base}-dev${pre_num}" ;;
                stable) echo "$base" ;;
            esac ;;
        pyproject)
            case "$pre_type" in
                edge)   echo "${base}.dev${pre_num}" ;;
                stable) echo "$base" ;;
            esac ;;
        gradle)
            case "$pre_type" in
                edge)   echo "${base}-SNAPSHOT" ;;
                stable) echo "$base" ;;
            esac ;;
        *)
            echo -e "${RED}Unknown format: ${format}${NC}" >&2
            return 1 ;;
    esac
}

# Reverse: read ecosystem format, return canonical semver.
canonicalize_version() {
    local raw="$1" format="$2"
    case "$format" in
        cargo|cargo-ws-dep:*|cargo-dep:*|json|csproj|go)
            echo "$raw" ;;
        python-cargo)
            # Handle both old (0.7.2-dev.1) and new (0.7.2-dev1) formats
            if [[ "$raw" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-dev\.?([0-9]+)$ ]]; then
                echo "${BASH_REMATCH[1]}-edge.${BASH_REMATCH[2]}"
            else
                echo "$raw"
            fi ;;
        pyproject)
            # Handle both old (0.7.2.dev.1) and new (0.7.2.dev1) formats
            if [[ "$raw" =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.dev\.?([0-9]+)$ ]]; then
                echo "${BASH_REMATCH[1]}-edge.${BASH_REMATCH[2]}"
            else
                echo "$raw"
            fi ;;
        gradle)
            if [[ "$raw" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-SNAPSHOT$ ]]; then
                echo "${BASH_REMATCH[1]}-edge.0"
            else
                echo "$raw"
            fi ;;
        *)
            echo -e "${RED}Unknown format: ${format}${NC}" >&2
            return 1 ;;
    esac
}

# Extract version string from file based on format.
read_current_version() {
    local file="$1" format="$2"
    local abs_file="${REPO_ROOT}/${file}"
    local fmt_base="${format%%:*}"

    case "$fmt_base" in
        cargo)
            grep '^version = ' "$abs_file" | head -1 | sed 's/version = "\(.*\)"/\1/' ;;
        cargo-ws-dep)
            local pkg="${format#cargo-ws-dep:}"
            grep "^${pkg} = " "$abs_file" | head -1 | sed 's/.*version = "\([^"]*\)".*/\1/' ;;
        cargo-dep)
            local pkg="${format#cargo-dep:}"
            grep "^${pkg} = " "$abs_file" | head -1 | sed 's/.*version = "\([^"]*\)".*/\1/' ;;
        python-cargo)
            grep '^version = ' "$abs_file" | head -1 | sed 's/version = "\(.*\)"/\1/' ;;
        pyproject)
            grep '^version = ' "$abs_file" | head -1 | sed 's/version = "\(.*\)"/\1/' ;;
        json)
            grep '"version"' "$abs_file" | head -1 | sed 's/.*"version": *"\([^"]*\)".*/\1/' ;;
        csproj)
            grep '<Version>' "$abs_file" | head -1 | sed 's/.*<Version>\(.*\)<\/Version>.*/\1/' | tr -d '[:space:]' ;;
        gradle)
            grep '^version=' "$abs_file" | head -1 | sed 's/version=//' ;;
        go)
            grep 'const Version' "$abs_file" | head -1 | sed 's/.*const Version = "\([^"]*\)".*/\1/' ;;
        *)
            echo -e "${RED}Unknown format: ${format}${NC}" >&2
            return 1 ;;
    esac
}

# Portable sed -i wrapper (works on both GNU and BSD/macOS sed).
sedi() {
    if sed --version 2>/dev/null | grep -q 'GNU'; then
        sed -i "$@"
    else
        sed -i '' "$@"
    fi
}

# Write translated version into file using sed.
write_version() {
    local file="$1" format="$2" new_canonical="$3"
    local abs_file="${REPO_ROOT}/${file}"
    local fmt_base="${format%%:*}"
    local translated
    translated=$(translate_version "$new_canonical" "$format")

    case "$fmt_base" in
        cargo)
            sedi "1,/^version = \".*\"/s/^version = \".*\"/version = \"${translated}\"/" "$abs_file" ;;
        cargo-ws-dep)
            local pkg="${format#cargo-ws-dep:}"
            sedi "s/^\(${pkg} = .*version = \"\)[^\"]*/\1${translated}/" "$abs_file" ;;
        cargo-dep)
            local pkg="${format#cargo-dep:}"
            sedi "s/^\(${pkg} = .*version = \"\)[^\"]*/\1${translated}/" "$abs_file" ;;
        python-cargo)
            sedi "1,/^version = \".*\"/s/^version = \".*\"/version = \"${translated}\"/" "$abs_file" ;;
        pyproject)
            sedi '/^\[project\]/,/^\[/{s/^version = ".*"/version = "'"${translated}"'"/;}' "$abs_file" ;;
        json)
            sedi "1,/\"version\": *\"[^\"]*\"/{s/\"version\": *\"[^\"]*\"/\"version\": \"${translated}\"/;}" "$abs_file" ;;
        csproj)
            sedi "1,/<Version>[^<]*<\/Version>/{s/<Version>[^<]*<\/Version>/<Version>${translated}<\/Version>/;}" "$abs_file" ;;
        gradle)
            sedi "s/^version=.*/version=${translated}/" "$abs_file" ;;
        go)
            sedi "1,/const Version = \"[^\"]*\"/{s/const Version = \"[^\"]*\"/const Version = \"${translated}\"/;}" "$abs_file" ;;
        *)
            echo -e "${RED}Unknown format: ${format}${NC}" >&2
            return 1 ;;
    esac
}

# --status: print current versions for all or one component.
cmd_status() {
    local filter="${1:-}"
    local components
    if [[ "$filter" == "rust-all" ]]; then
        components="$RUST_COMPONENTS"
    elif [[ -n "$filter" ]]; then
        components="$filter"
    else
        components="$ALL_COMPONENTS"
    fi

    for comp in $components; do
        echo -e "${CYAN}${comp}${NC}"
        local first_canonical=""
        while IFS= read -r entry; do
            local file="${entry%%:*}"
            local format="${entry#*:}"
            local raw canonical
            raw=$(read_current_version "$file" "$format")
            canonical=$(canonicalize_version "$raw" "$format")
            printf "  %-55s %s" "$file" "$raw"
            if [[ "$raw" != "$canonical" ]]; then
                printf "  (canonical: %s)" "$canonical"
            fi
            printf "\n"
            if [[ -z "$first_canonical" ]]; then
                first_canonical="$canonical"
            fi
        done < <(get_version_files "$comp")
        echo ""
    done
}

# Collect unique files from version entries for dirty check.
collect_files() {
    local component="$1"
    get_version_files "$component" | while IFS= read -r entry; do
        echo "${entry%%:*}"
    done | sort -u
}

# Pre-flight: check for uncommitted changes in version files.
preflight_dirty_check() {
    local component="$1"
    local dirty_files=()
    while IFS= read -r file; do
        if ! git diff --quiet -- "$REPO_ROOT/$file" 2>/dev/null; then
            dirty_files+=("$file")
        fi
        if ! git diff --cached --quiet -- "$REPO_ROOT/$file" 2>/dev/null; then
            dirty_files+=("$file (staged)")
        fi
    done < <(collect_files "$component")

    if [[ ${#dirty_files[@]} -gt 0 ]]; then
        echo -e "${YELLOW}Warning: uncommitted changes in version files:${NC}" >&2
        for f in "${dirty_files[@]}"; do
            echo -e "  ${f}" >&2
        done
    fi
}

# Pre-flight: verify grouped components have consistent canonical versions.
# Returns the canonical version (or exits on inconsistency).
preflight_consistency_check() {
    local component="$1"
    local first_canonical="" first_file=""
    local inconsistent=0

    while IFS= read -r entry; do
        local file="${entry%%:*}"
        local format="${entry#*:}"
        local raw canonical
        raw=$(read_current_version "$file" "$format")
        canonical=$(canonicalize_version "$raw" "$format")

        if [[ -z "$first_canonical" ]]; then
            first_canonical="$canonical"
            first_file="$file"
        elif [[ "$canonical" != "$first_canonical" ]]; then
            if [[ $inconsistent -eq 0 ]]; then
                echo -e "${RED}Inconsistent versions in ${component}:${NC}" >&2
                echo -e "  ${first_file}: ${first_canonical}" >&2
                inconsistent=1
            fi
            echo -e "  ${file}: ${canonical}" >&2
        fi
    done < <(get_version_files "$component")

    if [[ $inconsistent -ne 0 ]]; then
        echo -e "${RED}Fix version inconsistencies before bumping, or use --set --force.${NC}" >&2
        return 1
    fi

    echo "$first_canonical"
}

# Main bump logic.
cmd_bump() {
    local component="$1"
    shift
    local apply=1 set_ver="" force=0 has_edge=0 strip_edge=0 keyword=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --patch)
                [[ -n "$keyword" ]] && { echo -e "${RED}Conflicting flags: --${keyword} and --patch${NC}" >&2; return 1; }
                keyword="patch" ;;
            --minor)
                [[ -n "$keyword" ]] && { echo -e "${RED}Conflicting flags: --${keyword} and --minor${NC}" >&2; return 1; }
                keyword="minor" ;;
            --major)
                [[ -n "$keyword" ]] && { echo -e "${RED}Conflicting flags: --${keyword} and --major${NC}" >&2; return 1; }
                keyword="major" ;;
            --edge) has_edge=1 ;;
            --strip-edge) strip_edge=1 ;;
            --dry-run) apply=0 ;;
            --set)
                [[ $# -lt 2 ]] && { echo -e "${RED}--set requires a version argument${NC}" >&2; return 1; }
                set_ver="$2"; shift ;;
            --force) force=1 ;;
            *) echo -e "${RED}Unknown flag: ${1}${NC}" >&2; return 1 ;;
        esac
        shift
    done

    # Resolve --edge: standalone = increment counter, combined with bump = add suffix
    local add_edge=0
    if [[ $has_edge -eq 1 ]]; then
        if [[ -n "$keyword" ]]; then
            add_edge=1
        else
            keyword="edge"
        fi
    fi

    # Validate: need exactly one action
    if [[ -z "$set_ver" && -z "$keyword" && $strip_edge -eq 0 ]]; then
        echo -e "${RED}Missing version flag. Use --patch, --minor, --major, --edge, --strip-edge, or --set${NC}" >&2
        return 1
    fi

    preflight_dirty_check "$component"
    local current
    current=$(preflight_consistency_check "$component")

    local new_ver
    if [[ -n "$set_ver" ]]; then
        if [[ $force -eq 0 ]]; then
            if ! parse_version "$set_ver" > /dev/null 2>&1; then
                echo -e "${RED}Invalid version format: ${set_ver}${NC}" >&2
                echo "Expected: X.Y.Z or X.Y.Z-edge.N (use --force to bypass)" >&2
                return 1
            fi
        fi
        if [[ $force -eq 0 && "$set_ver" == "$current" ]]; then
            echo -e "${YELLOW}Already at ${set_ver}, nothing to do.${NC}"
            return 0
        fi
        new_ver="$set_ver"
    else
        new_ver=$(compute_next_version "$current" "${keyword:-_none}" "$add_edge" "$strip_edge") || return 1
    fi

    # Java SNAPSHOT idempotency: any edge operation on already-SNAPSHOT is a no-op
    # (SNAPSHOT has no counter, so --edge and --patch --edge both produce SNAPSHOT)
    if [[ "$component" == "sdk-java" ]] && [[ "$keyword" == "edge" || $add_edge -eq 1 ]]; then
        local raw_java
        raw_java=$(read_current_version "foreign/java/gradle.properties" "gradle")
        if [[ "$raw_java" == *-SNAPSHOT ]]; then
            echo -e "${GREEN}sdk-java already at SNAPSHOT (${raw_java}), nothing to do.${NC}"
            return 0
        fi
    fi

    echo -e "${CYAN}Component:${NC} ${component}"
    echo -e "${CYAN}Current:${NC}   ${current}"
    echo -e "${CYAN}New:${NC}       ${new_ver}"
    echo ""

    local changed_files=()
    while IFS= read -r entry; do
        local file="${entry%%:*}"
        local format="${entry#*:}"
        local raw_old translated_new
        raw_old=$(read_current_version "$file" "$format")
        translated_new=$(translate_version "$new_ver" "$format")

        if [[ "$raw_old" == "$translated_new" ]]; then
            printf "  %-55s %s (unchanged)\n" "$file" "$raw_old"
        else
            printf "  %-55s %s -> ${GREEN}%s${NC}\n" "$file" "$raw_old" "$translated_new"
            changed_files+=("$file")
        fi

        if [[ $apply -eq 1 ]]; then
            write_version "$file" "$format" "$new_ver"
            local verify
            verify=$(read_current_version "$file" "$format")
            if [[ "$verify" != "$translated_new" ]]; then
                echo -e "${RED}FAILED to write ${file}: expected '${translated_new}', got '${verify}'${NC}" >&2
                return 1
            fi
        fi
    done < <(get_version_files "$component")

    echo ""
    if [[ $apply -eq 1 ]]; then
        echo -e "${GREEN}Applied.${NC}"
    else
        echo -e "${YELLOW}Dry run. Remove --dry-run to write changes.${NC}"
    fi

    if [[ ${#changed_files[@]} -gt 0 ]]; then
        local unique_files
        IFS=$'\n' read -r -d '' -a unique_files < <(printf '%s\n' "${changed_files[@]}" | sort -u && printf '\0') || true

        if [[ "${BUMP_MULTI:-0}" -eq 1 ]]; then
            # In multi-component mode, accumulate files for a single commit suggestion
            for f in "${unique_files[@]}"; do
                BUMP_MULTI_FILES+=("$f")
            done
        else
            echo ""
            echo "Next steps:"
            echo "  cargo generate-lockfile"
            local git_add="  git add Cargo.lock"
            for f in "${unique_files[@]}"; do
                git_add+=" ${f}"
            done
            echo "$git_add"
            echo "  git commit -m \"chore(release): bump ${component} to ${new_ver}\""
        fi
    fi
}

# --- Argument parsing ---

if [[ $# -eq 0 ]]; then
    usage
    exit 0
fi

case "$1" in
    -h|--help)
        usage
        exit 0 ;;
    --status)
        cmd_status "${2:-}"
        exit 0 ;;
esac

# Bump multiple components with the same flags.
# Collects all changed files and prints a single commit suggestion at the end.
cmd_bump_multi() {
    local components="$1"
    shift
    local failed=0
    BUMP_MULTI=1
    BUMP_MULTI_FILES=()

    for comp in $components; do
        echo -e "${CYAN}--- ${comp} ---${NC}"
        if cmd_bump "$comp" "$@"; then
            :
        else
            failed=$((failed + 1))
            echo -e "${RED}FAILED ${comp}${NC}"
        fi
        echo ""
    done

    if [[ $failed -gt 0 ]]; then
        echo -e "${RED}${failed} component(s) failed.${NC}"
        echo -e "Use ${CYAN}--set${NC} to handle them individually."
        echo -e "Run ${CYAN}git checkout${NC} on affected files to revert partial changes."
        BUMP_MULTI=0
        return 1
    fi

    if [[ ${#BUMP_MULTI_FILES[@]} -gt 0 ]]; then
        local unique_files
        IFS=$'\n' read -r -d '' -a unique_files < <(printf '%s\n' "${BUMP_MULTI_FILES[@]}" | sort -u && printf '\0') || true
        echo "Next steps:"
        echo "  cargo generate-lockfile"
        local git_add="  git add Cargo.lock"
        for f in "${unique_files[@]}"; do
            git_add+=" ${f}"
        done
        echo "$git_add"
        echo "  git commit -m \"chore(release): bump all components\""
    fi

    BUMP_MULTI=0
}

# Resolve component target: --all, rust-all, or named component.
# Remaining args are flags passed to cmd_bump.
case "$1" in
    --all)
        shift
        cmd_bump_multi "$ALL_COMPONENTS" "$@" ;;
    rust-all)
        shift
        cmd_bump_multi "$RUST_COMPONENTS" "$@" ;;
    --*)
        echo -e "${RED}Expected component name, got flag: ${1}${NC}" >&2
        echo "Use: bump-version.sh <component> <flags>" >&2
        exit 1 ;;
    *)
        COMPONENT="$1"
        shift
        cmd_bump "$COMPONENT" "$@" ;;
esac
