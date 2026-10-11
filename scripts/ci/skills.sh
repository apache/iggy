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

# Validate the AI agent skills that git tracks:
#   - YAML frontmatter present, non-empty `name` (kebab-case, <= 64 chars)
#   - non-empty `description` (<= 1024 chars, block scalars included)
#   - relative markdown links inside skills, templates, and references resolve
#   - every skill has a relative Codex discovery link, and every link has a skill
#   - Claude Code and Codex agree on whether a skill is user-only
# Untracked skills that a user installs locally are not checked.

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/init.sh"

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

SKILLS_DIR=".claude/skills"
CODEX_SKILLS_DIR=".agents/skills"
fail=0

mapfile -t files < <(git ls-files -- ":(glob)$SKILLS_DIR/*/SKILL.md" \
  ":(glob)$SKILLS_DIR/*/TEMPLATE.md" ":(glob)$SKILLS_DIR/*/references/*.md")
if [ ${#files[@]} -eq 0 ]; then
  echo "ℹ️  No skill files found under $SKILLS_DIR"
  exit 0
fi

echo "🔍 Validating ${#files[@]} skill file(s)..."

# Frontmatter check (SKILL.md only).
for f in "${files[@]}"; do
  [[ "$f" == */SKILL.md ]] || continue
  awk '
    # END runs after every exit, so it must not report a second error.
    function die(msg) { print "  ❌ '"$f"': " msg; failed=1; exit 1 }
    function check_desc() {
      if (desc == "") die("empty description")
      if (length(desc) > 1024) die("description > 1024 chars")
    }
    BEGIN { state=0 }
    NR==1 && $0!="---" { die("missing YAML frontmatter on line 1") }
    NR==1 { state=1; next }
    # A block scalar (description: |) continues on the indented and blank lines below it.
    state==1 && in_desc && (/^[[:space:]]/ || $0 == "") {
      line=$0; sub(/^[[:space:]]+/, "", line)
      desc = desc (desc == "" ? "" : " ") line
      next
    }
    state==1 && in_desc { in_desc=0; check_desc() }
    state==1 && $0=="---" { state=2; exit 0 }
    state==1 && /^name:[[:space:]]/ {
      name=$0; sub(/^name:[[:space:]]*/, "", name);
      if (name == "") die("empty name")
      if (length(name) > 64) die("name > 64 chars")
      if (name !~ /^[a-z0-9][a-z0-9-]*$/) die("name not kebab-case: " name)
      has_name=1
    }
    state==1 && /^description:[[:space:]]/ {
      desc=$0; sub(/^description:[[:space:]]*/, "", desc); sub(/[[:space:]]+$/, "", desc)
      has_desc=1
      # Block scalar header: indentation (1-9) and chomping (+/-) in either order, then a comment.
      if (desc ~ /^[|>]([-+]?[1-9]?|[1-9][-+])([[:space:]]+#.*)?$/) { desc=""; in_desc=1; next }
      # Measure a quoted value without its quotes, so "" is empty.
      if (desc ~ /^".*"$/ || desc ~ /^\047.*\047$/) desc=substr(desc, 2, length(desc) - 2)
      check_desc()
    }
    END {
      if (failed) exit 1
      if (state != 2) die("frontmatter not closed")
      if (!has_name) die("missing name field")
      if (!has_desc) die("missing description field")
    }
  ' "$f" || fail=1
done

# Codex finds skills through links under .agents/skills, and both clients must
# agree on whether a skill is user-only.
for f in "${files[@]}"; do
  [[ "$f" == */SKILL.md ]] || continue
  skill_dir=${f%/SKILL.md}
  codex_skill="$CODEX_SKILLS_DIR/${skill_dir##*/}"
  # Only a relative target resolves in every clone.
  if [ "$(readlink "$codex_skill")" != "../../$skill_dir" ]; then
    echo "  ❌ $codex_skill: must be a symlink to ../../$skill_dir (ln -sfn ../../$skill_dir $codex_skill)"
    fail=1
  fi

  claude_flag=$(awk '
    /^---$/ { if (++section == 2) exit; next }
    section == 1 && sub(/^disable-model-invocation:[[:space:]]*/, "") { sub(/[[:space:]]+$/, ""); print; exit }
  ' "$f")
  codex_flag=""
  policy="$skill_dir/agents/openai.yaml"
  if [ -f "$policy" ]; then
    codex_flag=$(awk '
      /^policy:/ { in_policy=1; next }
      /^[^[:space:]#]/ { in_policy=0 }
      in_policy && sub(/^[[:space:]]+allow_implicit_invocation:[[:space:]]*/, "") { sub(/[[:space:]]+$/, ""); print; exit }
    ' "$policy")
  fi
  case "$claude_flag/$codex_flag" in
    true/false | / | /true | false/ | false/true) ;;
    true/ | true/true | /false | false/false)
      echo "  ❌ $f, $policy: set disable-model-invocation: true and policy.allow_implicit_invocation: false together, or neither"
      fail=1
      ;;
    *)
      echo "  ❌ $f, $policy: disable-model-invocation and policy.allow_implicit_invocation take a bare true or false"
      fail=1
      ;;
  esac
done

while IFS= read -r codex_skill; do
  if [ ! -f "$SKILLS_DIR/${codex_skill#"$CODEX_SKILLS_DIR"/}/SKILL.md" ]; then
    echo "  ❌ $codex_skill: no corresponding skill under $SKILLS_DIR"
    fail=1
  fi
done < <(git ls-files -- "$CODEX_SKILLS_DIR")

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
