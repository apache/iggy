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

# Validate AI agent skills under .claude/skills/:
#   - YAML frontmatter present, non-empty `name` (kebab-case, <= 64 chars)
#   - non-empty `description` (<= 1024 chars)
#   - relative markdown links inside SKILL.md / TEMPLATE.md resolve

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

SKILLS_DIR=".claude/skills"
fail=0

if [ ! -d "$SKILLS_DIR" ]; then
  echo "ℹ️  No $SKILLS_DIR directory; nothing to validate"
  exit 0
fi

shopt -s nullglob
files=("$SKILLS_DIR"/*/SKILL.md "$SKILLS_DIR"/*/TEMPLATE.md)
if [ ${#files[@]} -eq 0 ]; then
  echo "ℹ️  No skill files found under $SKILLS_DIR"
  exit 0
fi

echo "🔍 Validating ${#files[@]} skill file(s)..."

# Frontmatter check (SKILL.md only).
for f in "$SKILLS_DIR"/*/SKILL.md; do
  awk '
    BEGIN { state=0 }
    NR==1 && $0!="---" { print "  ❌ '"$f"': missing YAML frontmatter on line 1"; exit 1 }
    NR==1 { state=1; next }
    state==1 && $0=="---" { state=2; exit 0 }
    state==1 && /^name:[[:space:]]/ {
      name=$0; sub(/^name:[[:space:]]*/, "", name);
      if (name == "") { print "  ❌ '"$f"': empty name"; exit 1 }
      if (length(name) > 64) { print "  ❌ '"$f"': name > 64 chars"; exit 1 }
      if (name !~ /^[a-z0-9][a-z0-9-]*$/) { print "  ❌ '"$f"': name not kebab-case: " name; exit 1 }
      has_name=1
    }
    state==1 && /^description:[[:space:]]/ {
      desc=$0; sub(/^description:[[:space:]]*/, "", desc);
      if (desc == "") { print "  ❌ '"$f"': empty description"; exit 1 }
      if (length(desc) > 1024) { print "  ❌ '"$f"': description > 1024 chars"; exit 1 }
      has_desc=1
    }
    END {
      if (state != 2) { print "  ❌ '"$f"': frontmatter not closed"; exit 1 }
      if (!has_name) { print "  ❌ '"$f"': missing name field"; exit 1 }
      if (!has_desc) { print "  ❌ '"$f"': missing description field"; exit 1 }
    }
  ' "$f" || fail=1
done

# Relative link resolution.
for f in "${files[@]}"; do
  dir=$(dirname "$f")
  while IFS= read -r link; do
    [ -z "$link" ] && continue
    case "$link" in
      http://*|https://*|mailto:*) continue ;;
    esac
    # Strip fragments (#section) and queries.
    path_part="${link%%#*}"
    path_part="${path_part%%\?*}"
    [ -z "$path_part" ] && continue
    target="$dir/$path_part"
    if ! [ -e "$target" ]; then
      echo "  ❌ $f: broken link -> $link (resolved to $target)"
      fail=1
    fi
  done < <(grep -oE '\]\([^)]+\)' "$f" | sed 's/^](//' | sed 's/)$//')
done

if [ $fail -eq 0 ]; then
  echo "✅ All skill files valid"
else
  echo "❌ Skill validation failed"
  exit 1
fi
