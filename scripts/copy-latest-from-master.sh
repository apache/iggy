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

# Script to copy latest files from master branch to current working directory
# Usage: copy-latest-from-master.sh <save|apply> <path1> [path2] [path3] ...
#
# Commands:
#   save    - Clone master and save specified files/directories to /tmp
#   apply   - Apply previously saved files to current working directory
#
# Examples:
#   copy-latest-from-master.sh save .github scripts web/Dockerfile
#   copy-latest-from-master.sh apply

COMMAND="${1:-}"
shift || true

TMP_REPO="/tmp/repo-master"
TMP_SAVED="/tmp/latest-files"

if [ -z "$COMMAND" ]; then
    echo "Usage: $0 <save|apply> [paths...]"
    echo ""
    echo "Commands:"
    echo "  save    Clone master and save specified files/directories"
    echo "  apply   Apply previously saved files to current directory"
    exit 1
fi

case "$COMMAND" in
    save)
        if [ $# -eq 0 ]; then
            echo "Error: No paths specified for save command"
            echo "Usage: $0 save <path1> [path2] ..."
            exit 1
        fi

        echo "📦 Cloning master branch..."
        rm -rf "$TMP_REPO" "$TMP_SAVED"

        # Use GITHUB_REPOSITORY if available, otherwise try to detect from git remote
        if [ -z "${GITHUB_REPOSITORY:-}" ]; then
            # Try to extract from git remote
            REPO_URL=$(git remote get-url origin 2>/dev/null || echo "")
            if [[ "$REPO_URL" =~ github\.com[:/]([^/]+/[^/.]+)(\.git)?$ ]]; then
                GITHUB_REPOSITORY="${BASH_REMATCH[1]}"
            else
                echo "Error: Could not determine repository. Set GITHUB_REPOSITORY environment variable."
                exit 1
            fi
        fi

        git clone --depth 1 --branch master "https://github.com/${GITHUB_REPOSITORY}.git" "$TMP_REPO"

        mkdir -p "$TMP_SAVED"

        echo "💾 Saving specified files from master..."
        for path in "$@"; do
            if [ -e "$TMP_REPO/$path" ]; then
                # Create parent directory structure
                parent_dir=$(dirname "$path")
                if [ "$parent_dir" != "." ]; then
                    mkdir -p "$TMP_SAVED/$parent_dir"
                fi

                # Copy the file or directory
                cp -r "$TMP_REPO/$path" "$TMP_SAVED/$path"
                echo "  ✅ Saved: $path"
            else
                echo "  ⚠️  Not found in master: $path"
            fi
        done

        echo ""
        echo "📋 Summary of saved files:"
        find "$TMP_SAVED" -type f | sed "s|$TMP_SAVED/|  - |"

        # Clean up the cloned repo
        rm -rf "$TMP_REPO"
        ;;

    apply)
        if [ ! -d "$TMP_SAVED" ]; then
            echo "Error: No saved files found. Run 'save' command first."
            exit 1
        fi

        echo "📝 Applying saved files to current directory..."

        # Iterate through all saved files
        find "$TMP_SAVED" -type f | while read -r saved_file; do
            # Get relative path by removing the tmp prefix
            rel_path="${saved_file#"$TMP_SAVED"/}"
            dest_dir=$(dirname "$rel_path")

            # Create destination directory if it doesn't exist
            if [ "$dest_dir" != "." ] && [ ! -d "$dest_dir" ]; then
                echo "  📁 Creating directory: $dest_dir"
                mkdir -p "$dest_dir"
            fi

            # Copy the file
            cp "$saved_file" "$rel_path"
            echo "  ✅ Applied: $rel_path"

            # Make scripts executable if they're in scripts/ directory
            if [[ "$rel_path" == scripts/*.sh ]]; then
                chmod +x "$rel_path"
                echo "     Made executable: $rel_path"
            fi
        done

        echo ""
        echo "✅ All applicable files have been applied"
        ;;

    *)
        echo "Error: Unknown command '$COMMAND'"
        echo "Valid commands: save, apply"
        exit 1
        ;;
esac
