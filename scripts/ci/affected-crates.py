#!/usr/bin/env python3
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

"""
Dependency-DAG-based affected crate analyzer for CI optimization.

Uses `cargo metadata` to build the workspace dependency graph, then
computes which crates are affected by a set of changed files (from
git diff). Outputs affected crates grouped by test tier:

  Tier 0 (leaf):        No internal deps -> unit tests only
  Tier 1 (mid):         Has internal deps but few dependents -> unit + doc tests
  Tier 2 (core):        Many dependents (>=3) -> full test suite
  Tier 3 (integration): The `integration` and `simulator` crates -> integration tests

Usage:
  # Affected crates for current PR (vs origin/master)
  python3 scripts/ci/affected-crates.py

  # Affected crates for specific base ref
  python3 scripts/ci/affected-crates.py --base-ref origin/master

  # Output as JSON (for CI consumption)
  python3 scripts/ci/affected-crates.py --format json

  # Show full DAG without change filtering
  python3 scripts/ci/affected-crates.py --all

  # Output nextest filter expression
  python3 scripts/ci/affected-crates.py --format nextest-filter
"""

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def run(cmd, **kwargs):
    """Run a command and return stdout, or exit on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        print(f"Error running: {' '.join(cmd)}", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def load_cargo_metadata():
    """Load and parse cargo metadata."""
    raw = run(["cargo", "metadata", "--format-version", "1", "--no-deps"])
    meta_no_deps = json.loads(raw)

    raw_full = run(["cargo", "metadata", "--format-version", "1"])
    meta_full = json.loads(raw_full)

    return meta_no_deps, meta_full


def build_workspace_graph(meta_no_deps, meta_full):
    """
    Build the internal workspace dependency graph.

    Returns:
        workspace: dict of pkg_id -> package info
        deps: dict of pkg_id -> set of internal dependency pkg_ids
        rev_deps: dict of pkg_id -> set of internal reverse dependency pkg_ids
        name_to_id: dict of crate_name -> pkg_id
        id_to_name: dict of pkg_id -> crate_name
        crate_paths: dict of crate_name -> manifest directory (relative to workspace root)
    """
    workspace_root = Path(meta_no_deps["workspace_root"])
    workspace_member_ids = set(meta_no_deps["workspace_members"])

    # Map package IDs to names and paths
    id_to_name = {}
    name_to_id = {}
    crate_paths = {}  # crate_name -> relative path from workspace root

    for pkg in meta_no_deps["packages"]:
        if pkg["id"] in workspace_member_ids:
            id_to_name[pkg["id"]] = pkg["name"]
            name_to_id[pkg["name"]] = pkg["id"]
            manifest_dir = Path(pkg["manifest_path"]).parent
            rel_path = manifest_dir.relative_to(workspace_root)
            crate_paths[pkg["name"]] = str(rel_path)

    # Build dependency graph from resolve section
    deps = defaultdict(set)
    rev_deps = defaultdict(set)

    for node in meta_full["resolve"]["nodes"]:
        crate_id = node["id"]
        if crate_id not in workspace_member_ids:
            continue
        for dep in node["deps"]:
            dep_id = dep["pkg"]
            if dep_id in workspace_member_ids:
                deps[crate_id].add(dep_id)
                rev_deps[dep_id].add(crate_id)

    return workspace_member_ids, deps, rev_deps, name_to_id, id_to_name, crate_paths


def get_changed_files(base_ref):
    """Get list of changed files relative to base_ref."""
    # Try merge-base first for accurate PR diff
    try:
        merge_base = run(["git", "merge-base", base_ref, "HEAD"])
        files = run(["git", "diff", "--name-only", merge_base])
    except SystemExit:
        # Fallback to direct diff
        files = run(["git", "diff", "--name-only", base_ref])

    if not files:
        return []
    return [f for f in files.split("\n") if f]


def map_files_to_crates(changed_files, crate_paths):
    """
    Map changed files to the crates they belong to.

    Returns:
        dict of crate_name -> list of changed files in that crate
    """
    affected = defaultdict(list)

    # Sort crate paths longest-first for most-specific match
    sorted_crates = sorted(crate_paths.items(), key=lambda x: len(x[1]), reverse=True)

    for filepath in changed_files:
        for crate_name, crate_dir in sorted_crates:
            if filepath.startswith(crate_dir + "/") or filepath == crate_dir:
                affected[crate_name].append(filepath)
                break

    return affected


def compute_affected_set(directly_changed, rev_deps, name_to_id, id_to_name):
    """
    Compute the full affected set: directly changed crates + all transitive
    reverse dependencies (crates that depend on changed crates).

    Returns:
        dict of crate_name -> { "reason": "direct"|"transitive", "via": crate_name|None }
    """
    affected = {}

    # Add directly changed crates
    for name in directly_changed:
        affected[name] = {"reason": "direct", "via": None}

    # BFS through reverse dependencies
    queue = list(directly_changed)
    while queue:
        current_name = queue.pop(0)
        current_id = name_to_id.get(current_name)
        if not current_id:
            continue

        for rev_dep_id in rev_deps.get(current_id, set()):
            rev_dep_name = id_to_name[rev_dep_id]
            if rev_dep_name not in affected:
                affected[rev_dep_name] = {"reason": "transitive", "via": current_name}
                queue.append(rev_dep_name)

    return affected


def compute_layers(workspace_ids, deps, id_to_name):
    """Compute topological layers of the dependency DAG."""
    assigned = {}
    remaining = set(workspace_ids)
    layer = 0

    while remaining:
        current_layer = []
        for crate_id in remaining:
            if all(d in assigned for d in deps.get(crate_id, set())):
                current_layer.append(crate_id)
        if not current_layer:
            # Cycle - assign remaining to current layer
            current_layer = list(remaining)
        for crate_id in current_layer:
            assigned[crate_id] = layer
            remaining.discard(crate_id)
        layer += 1

    # Convert to name-based
    layers = defaultdict(list)
    for crate_id, l in assigned.items():
        layers[l].append(id_to_name[crate_id])
    return dict(layers)


def classify_tiers(workspace_ids, deps, rev_deps, id_to_name):
    """
    Classify crates into test tiers based on their position in the DAG.

    Tier 0 (leaf):        No internal deps
    Tier 1 (mid):         Has internal deps, <3 reverse deps
    Tier 2 (core):        >=3 reverse deps (high-impact crates)
    Tier 3 (integration): integration/simulator crates (end-to-end tests)
    """
    INTEGRATION_CRATES = {"integration", "simulator"}
    CORE_THRESHOLD = 3  # number of reverse deps to qualify as "core"

    tiers = {}
    for crate_id in workspace_ids:
        name = id_to_name[crate_id]
        internal_deps = deps.get(crate_id, set())
        reverse_dep_count = len(rev_deps.get(crate_id, set()))

        if name in INTEGRATION_CRATES:
            tiers[name] = 3
        elif not internal_deps:
            tiers[name] = 0
        elif reverse_dep_count >= CORE_THRESHOLD:
            tiers[name] = 2
        else:
            tiers[name] = 1

    return tiers


def format_human(affected, tiers, layers, rev_deps, id_to_name, name_to_id, crate_paths, show_all):
    """Format output for human reading."""
    tier_names = {0: "leaf", 1: "mid", 2: "core", 3: "integration"}
    tier_descriptions = {
        0: "No internal deps - unit tests only",
        1: "Has internal deps - unit + doc tests",
        2: "Many dependents - full test suite",
        3: "End-to-end integration tests",
    }

    lines = []

    # DAG layers
    lines.append("=== Dependency DAG Layers ===")
    for layer_num in sorted(layers):
        crates = sorted(layers[layer_num])
        tier_info = ", ".join(f"{c} (T{tiers.get(c, '?')})" for c in crates)
        lines.append(f"  Layer {layer_num}: {tier_info}")

    lines.append("")
    lines.append("=== Tier Classification ===")
    for tier_num in sorted(tier_descriptions):
        crates = sorted(n for n, t in tiers.items() if t == tier_num)
        lines.append(f"  Tier {tier_num} ({tier_names[tier_num]}): {tier_descriptions[tier_num]}")
        for c in crates:
            rev_count = len(rev_deps.get(name_to_id.get(c, ""), set()))
            lines.append(f"    - {c} ({rev_count} dependents)")

    if not show_all:
        lines.append("")
        lines.append("=== Affected Crates ===")
        if not affected:
            lines.append("  No Rust crates affected by the current changes.")
        else:
            by_tier = defaultdict(list)
            for name, info in affected.items():
                by_tier[tiers.get(name, 1)].append((name, info))

            for tier_num in sorted(by_tier):
                lines.append(f"  Tier {tier_num} ({tier_names[tier_num]}):")
                for name, info in sorted(by_tier[tier_num]):
                    reason = info["reason"]
                    if reason == "direct":
                        lines.append(f"    - {name} (directly changed)")
                    else:
                        lines.append(f"    - {name} (transitive, via {info['via']})")

            lines.append("")
            lines.append("=== Recommended CI Strategy ===")
            affected_tiers = set(tiers.get(n, 1) for n in affected)
            max_tier = max(affected_tiers)

            if max_tier == 0:
                lines.append("  Only leaf crates changed -> run unit tests for affected crates only")
                lines.append("  Skip: integration tests, full workspace build")
            elif max_tier == 1:
                lines.append("  Mid-tier crates changed -> run unit + doc tests for affected crates")
                lines.append("  Skip: full integration suite")
            elif max_tier == 2:
                lines.append("  Core crates changed -> run full test suite for affected crates")
                lines.append("  Include: integration tests for affected dependency chains")
            else:
                lines.append("  Integration crates changed -> run full integration suite")

            # Nextest filter
            affected_names = sorted(affected.keys())
            if affected_names:
                filter_expr = " | ".join(f"package({n})" for n in affected_names)
                lines.append("")
                lines.append(f"  nextest filter: {filter_expr}")

    return "\n".join(lines)


def format_json(affected, tiers, layers, rev_deps, id_to_name, name_to_id, show_all):
    """Format output as JSON for CI consumption."""
    tier_names = {0: "leaf", 1: "mid", 2: "core", 3: "integration"}

    if show_all:
        crate_list = {name: tiers.get(name, 1) for name in sorted(tiers)}
    else:
        crate_list = {name: tiers.get(name, 1) for name in sorted(affected)}

    result = {
        "affected_crates": [],
        "tiers": {},
        "max_tier": 0,
        "nextest_filter": "",
        "nextest_packages": [],
    }

    for name in sorted(crate_list):
        tier = crate_list[name]
        info = affected.get(name, {"reason": "all", "via": None}) if not show_all else {"reason": "all", "via": None}
        rev_count = len(rev_deps.get(name_to_id.get(name, ""), set()))

        result["affected_crates"].append({
            "name": name,
            "tier": tier,
            "tier_name": tier_names.get(tier, "unknown"),
            "reason": info["reason"],
            "via": info["via"],
            "reverse_dep_count": rev_count,
        })

    # Group by tier
    for tier_num in sorted(tier_names):
        crates = [c["name"] for c in result["affected_crates"] if c["tier"] == tier_num]
        result["tiers"][tier_names[tier_num]] = crates

    if result["affected_crates"]:
        result["max_tier"] = max(c["tier"] for c in result["affected_crates"])

    affected_names = [c["name"] for c in result["affected_crates"]]
    result["nextest_packages"] = affected_names
    result["nextest_filter"] = " | ".join(f"package({n})" for n in affected_names)

    return json.dumps(result, indent=2)


def format_nextest_filter(affected, show_all, tiers):
    """Output just the nextest filter expression."""
    if show_all:
        names = sorted(tiers.keys())
    else:
        names = sorted(affected.keys())
    if not names:
        return ""
    return " | ".join(f"package({n})" for n in names)


def format_packages(affected, show_all, tiers):
    """Output -p flags for cargo test / cargo nextest."""
    if show_all:
        names = sorted(tiers.keys())
    else:
        names = sorted(affected.keys())
    if not names:
        return ""
    return " ".join(f"-p {n}" for n in names)


def main():
    parser = argparse.ArgumentParser(
        description="Compute affected crates from the workspace dependency DAG"
    )
    parser.add_argument(
        "--base-ref",
        default=os.environ.get("BASE_REF", "origin/master"),
        help="Git ref to diff against (default: origin/master or $BASE_REF)",
    )
    parser.add_argument(
        "--format",
        choices=["human", "json", "nextest-filter", "packages"],
        default="human",
        help="Output format",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Show full DAG without change filtering",
    )
    parser.add_argument(
        "--changed-files",
        help="Comma-separated list of changed files (overrides git diff)",
    )
    args = parser.parse_args()

    # Load cargo metadata
    meta_no_deps, meta_full = load_cargo_metadata()

    # Build workspace graph
    workspace_ids, deps, rev_deps, name_to_id, id_to_name, crate_paths = \
        build_workspace_graph(meta_no_deps, meta_full)

    # Classify tiers
    tiers = classify_tiers(workspace_ids, deps, rev_deps, id_to_name)

    # Compute layers
    layers = compute_layers(workspace_ids, deps, id_to_name)

    if args.all:
        affected = {id_to_name[cid]: {"reason": "all", "via": None} for cid in workspace_ids}
    else:
        # Get changed files
        if args.changed_files:
            changed_files = [f.strip() for f in args.changed_files.split(",") if f.strip()]
        else:
            changed_files = get_changed_files(args.base_ref)

        if not changed_files:
            if args.format == "json":
                print(json.dumps({"affected_crates": [], "tiers": {}, "max_tier": -1,
                                   "nextest_filter": "", "nextest_packages": []}))
            elif args.format in ("nextest-filter", "packages"):
                pass  # empty output
            else:
                print("No changed files detected.")
            return

        # Map files to crates
        directly_changed = map_files_to_crates(changed_files, crate_paths)

        # Workspace-level files (Cargo.toml, Cargo.lock, etc.) affect everything
        workspace_files = []
        for f in changed_files:
            matched = False
            for crate_dir in crate_paths.values():
                if f.startswith(crate_dir + "/"):
                    matched = True
                    break
            if not matched and (
                f.endswith("Cargo.toml")
                or f.endswith("Cargo.lock")
                or f.endswith("rust-toolchain.toml")
                or f.startswith(".cargo/")
            ):
                workspace_files.append(f)

        if workspace_files:
            # Workspace-level change: all crates affected
            if args.format not in ("nextest-filter", "packages"):
                print(
                    f"Workspace-level files changed ({', '.join(workspace_files)}), "
                    "all crates affected.",
                    file=sys.stderr,
                )
            affected = {id_to_name[cid]: {"reason": "workspace", "via": None}
                        for cid in workspace_ids}
        else:
            # Compute affected set with transitive reverse deps
            affected = compute_affected_set(
                directly_changed.keys(), rev_deps, name_to_id, id_to_name
            )

    # Format output
    if args.format == "human":
        print(format_human(affected, tiers, layers, rev_deps, id_to_name,
                           name_to_id, crate_paths, args.all))
    elif args.format == "json":
        print(format_json(affected, tiers, layers, rev_deps, id_to_name,
                           name_to_id, args.all))
    elif args.format == "nextest-filter":
        result = format_nextest_filter(affected, args.all, tiers)
        if result:
            print(result)
    elif args.format == "packages":
        result = format_packages(affected, args.all, tiers)
        if result:
            print(result)


if __name__ == "__main__":
    main()
