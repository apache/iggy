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

"""Exercise runtime archive transfer and CI selection without compiling the workspace."""

import base64
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
RESTORE = REPO_ROOT / "scripts/ci/restore-runtime.sh"
SERVER_START = "./.github/actions/utils/server-start"
DOWNLOAD_RUNTIME = "./.github/actions/utils/download-runtime"


def run(*args, cwd=REPO_ROOT, env=None):
    return subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True)


def env_without_git(**extra):
    """Drop GIT_* variables: under a hook or `git rebase --exec`, GIT_DIR and
    GIT_INDEX_FILE aim git at the caller's repository, not at its working directory."""
    env = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    return env | extra


class RuntimeArtifacts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.producer = self.root / "producer"
        self.consumer = self.root / "consumer"
        self.artifact = self.root / "artifact"
        for directory in (self.producer / "target/debug", self.consumer, self.artifact):
            directory.mkdir(parents=True)
        self.env = os.environ.copy()
        self.env["GIT_DIR"] = run("git", "rev-parse", "--absolute-git-dir").stdout.strip()
        self.revision = run("git", "rev-parse", "HEAD").stdout.strip()
        self.files = ["target/debug/iggy-server", "target/debug/iggy"]
        for name in self.files:
            binary = self.producer / name
            binary.write_text("#!/bin/sh\nprintf 'shared-runtime\\n'\n")
            binary.chmod(0o755)
        self.pack()

    def pack(self):
        (self.artifact / "revision").write_text(self.revision + "\n")
        result = run("tar", "--zstd", "-cf", str(self.artifact / "runtime.tar.zst"),
                     *self.files, cwd=self.producer)
        self.assertEqual(result.returncode, 0, result.stderr)

    def restore(self):
        return run("bash", str(RESTORE), str(self.artifact), cwd=self.consumer, env=self.env)

    def test_restore_preserves_executables_and_unrelated_files(self):
        unrelated = self.consumer / "keep.txt"
        unrelated.write_text("keep")
        result = self.restore()
        self.assertEqual(result.returncode, 0, result.stderr)
        for name in self.files:
            binary = self.consumer / name
            self.assertTrue(os.access(binary, os.X_OK), name)
            self.assertEqual(binary.read_bytes(), (self.producer / name).read_bytes())
            execution = run(str(binary), cwd=self.consumer)
            self.assertEqual(execution.returncode, 0, execution.stderr)
            self.assertEqual(execution.stdout, "shared-runtime\n")
        self.assertEqual(unrelated.read_text(), "keep")

    def test_wrong_revision_fails_before_overwriting_existing_binary(self):
        binary = self.consumer / self.files[0]
        binary.parent.mkdir(parents=True)
        binary.write_text("existing binary")
        (self.artifact / "revision").write_text("0" * 40 + "\n")
        result = self.restore()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match checkout", result.stdout)
        self.assertEqual(binary.read_text(), "existing binary")

    def test_corrupted_archive_is_rejected(self):
        (self.artifact / "runtime.tar.zst").write_bytes(b"invalid archive")
        result = self.restore()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.consumer / self.files[0]).exists())

    def test_missing_cli_is_not_accepted_as_a_complete_runtime(self):
        self.files.pop()
        self.pack()
        result = self.restore()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Runtime binary missing or not executable: target/debug/iggy", result.stdout)

    def test_nonexecutable_server_is_rejected(self):
        (self.producer / self.files[0]).chmod(0o644)
        self.pack()
        result = self.restore()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Runtime binary missing or not executable: target/debug/iggy-server", result.stdout)

    def test_rerun_restores_the_original_binary(self):
        self.assertEqual(self.restore().returncode, 0)
        (self.consumer / self.files[0]).write_text("overwritten by another build")
        self.assertEqual(self.restore().returncode, 0)
        self.assertEqual((self.consumer / self.files[0]).read_bytes(),
                         (self.producer / self.files[0]).read_bytes())


class RuntimeBuildSelection(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        action = yaml.safe_load((REPO_ROOT / ".github/actions/rust/pre-merge/action.yml").read_text())
        self.script = next(step["run"] for step in action["runs"]["steps"]
                           if step.get("name") == "Build coverage artifacts")
        self.revision = run("git", "rev-parse", "HEAD").stdout.strip()
        self.env = dict(os.environ, RUNNER_TEMP=str(self.root), ARTIFACT_TASK="test-build",
                        TEST_SELECTION="affected", PLAN_STATUS="failure", PLAN_FILE="", STACK_BASE="",
                        GITHUB_WORKSPACE=str(self.root), PATH=f"{self.root}:{os.environ['PATH']}")
        git = self.root / "git"
        git.write_text(f"#!/bin/sh\ncase $1 in\n  rev-parse) echo {self.revision} ;;\n  cat-file) echo 'parent fixture' ;;\n  *) exit 1 ;;\nesac\n")
        git.chmod(0o755)
        self.runtime_files = {
            "iggy-bench": "target/debug/iggy-bench",
            "iggy-mcp": "target/debug/iggy-mcp",
            "iggy-bench-dashboard-server": "target/debug/iggy-bench-dashboard-server",
            "kafka-message-gen": "target/debug/kafka-message-gen",
        }
        packages = []
        artifacts = []
        for package, filename in self.runtime_files.items():
            package_id = f"path+file://{self.root}/{package}#0.1.0"
            packages.append({"name": package, "id": package_id})
            binary = self.root / filename
            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_text(package)
            artifacts.append({
                "reason": "compiler-artifact", "package_id": package_id,
                "target": {"name": binary.stem, "kind": ["bin"]},
                "executable": str(binary),
            })
        (self.root / "metadata.json").write_text(json.dumps({"packages": packages}))
        (self.root / "build.jsonl").write_text("\n".join(json.dumps(artifact) for artifact in artifacts))
        cargo = self.root / "cargo"
        cargo.write_text("""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
with (Path(os.environ['RUNNER_TEMP']) / 'cargo-calls').open('a') as calls:
    calls.write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[1:3] == ['nextest', 'archive']:
    Path(sys.argv[sys.argv.index('--archive-file') + 1]).touch()
elif sys.argv[1] in ('build', 'metadata'):
    output = 'build.jsonl' if sys.argv[1] == 'build' else 'metadata.json'
    print((Path(os.environ['RUNNER_TEMP']) / output).read_text())
""")
        cargo.chmod(0o755)
        generator = self.root / "gateways/kafka/scripts/ci-wire-fixtures.sh"
        generator.parent.mkdir(parents=True)
        generator.write_text("#!/bin/sh\nmkdir -p gateways/kafka/tools/kafka-tool/kafka_messages\n"
                             "printf '%s' \"$KAFKA_MESSAGE_GEN\" > \"$RUNNER_TEMP/kafka-message-gen\"\n")
        generator.chmod(0o755)

    def assert_workspace_fallback(self):
        result = run("bash", "-euo", "pipefail", "-c", self.script, cwd=self.root, env=self.env)
        self.assertEqual(result.returncode, 0, result.stderr)
        artifact = self.root / "rust-test-build"
        self.assertEqual((artifact / "scope").read_text(), "workspace\n")
        self.assertEqual((artifact / "revision").read_text().strip(), self.revision)
        calls = [json.loads(line) for line in (self.root / "cargo-calls").read_text().splitlines()]
        self.assertEqual(calls[0], ["build", "--locked", "--workspace", "--exclude", "server",
                                   "--exclude", "iggy-cli", "--message-format=json-render-diagnostics"])
        self.assertEqual(calls[1], ["nextest", "archive", "--locked", "--profile", "ci",
                                   "--workspace",
                                   "--archive-file", str(artifact / "nextest.tar.zst")])
        self.assert_runtime_archive(list(self.runtime_files.values()))
        self.assertEqual((artifact / "kafka-fixtures").read_text(), "true\n")
        self.assertEqual((self.root / "kafka-message-gen").read_text(),
                         str(self.root / "target/debug/kafka-message-gen"))

    def assert_runtime_archive(self, files):
        result = run("tar", "--zstd", "-tf", str(self.root / "rust-test-build/runtime.tar.zst"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sorted(result.stdout.splitlines()), sorted(files))

    def test_planner_failure_builds_all_test_extras_and_tests(self):
        self.assert_workspace_fallback()

    def test_explicit_workspace_selection_does_not_need_a_planner(self):
        self.env["TEST_SELECTION"] = "workspace"
        self.assert_workspace_fallback()

    def test_stack_top_diffs_fixtures_against_the_stack_base(self):
        # The first parent of the top merge ref is the layer below, and only a
        # lower layer changed gateways/. The fixtures must still be generated.
        (self.root / "git").write_text(f"""#!/bin/sh
case $1 in
  rev-parse) echo {self.revision} ;;
  cat-file) printf 'parent layer-below\\nparent top-head\\n' ;;
  fetch) ;;
  diff) if [ "$3" = stack-base ]; then echo gateways/kafka/src/lib.rs; else echo docs/unrelated.md; fi ;;
  *) exit 1 ;;
esac
""")
        self.env["STACK_BASE"] = "stack-base"
        self.assert_workspace_fallback()

    def select(self, packages, *, tests=(), runtime_packages=()):
        outputs = {
            "cargo-scope:cargo.test": "packages\n" if packages else "skipped\n",
            "cargo-scope:runtime-artifacts": "packages\n" if runtime_packages else "skipped\n",
            "package-names:cargo.test": "".join(package + "\0" for package in packages),
            "package-names:runtime-artifacts": "".join(package + "\0" for package in runtime_packages),
            "target-args:cargo.test": "".join("--test\0" + test + "\0" for test in tests),
        }
        selector = self.root / "cargo-rail-action"
        selector.write_text("#!/usr/bin/env python3\nimport sys\n"
                            f"outputs = {outputs!r}\n"
                            "sys.stdout.write(outputs[f'{sys.argv[2]}:{sys.argv[4]}'])\n")
        selector.chmod(0o755)
        self.env["PLAN_STATUS"] = "success"
        result = run("bash", "-euo", "pipefail", "-c", self.script, cwd=self.root, env=self.env)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.root / "cargo-calls"
        return [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []

    def test_affected_packages_keep_workspace_builds_and_filter_only_the_archive(self):
        calls = self.select(["integration", "iggy"],
                            runtime_packages=["iggy-bench", "iggy-mcp"])
        self.assertEqual(calls[0], ["build", "--locked", "--workspace", "--exclude", "server",
                                   "--exclude", "iggy-cli", "--message-format=json-render-diagnostics"])
        self.assertEqual(calls[-1], ["nextest", "archive", "--locked", "--profile", "ci",
                                   "--workspace", "-E", "package(=integration) | package(=iggy)",
                                   "--archive-file", str(self.root / "rust-test-build/nextest.tar.zst")])
        self.assert_runtime_archive(["target/debug/iggy-bench",
                                     "target/debug/iggy-mcp"])
        self.assertEqual((self.root / "rust-test-build/scope").read_text(), "packages\n")
        self.assertEqual((self.root / "rust-test-build/kafka-fixtures").read_text(), "false\n")

    def test_selected_integration_targets_filter_by_package_kind_and_name(self):
        calls = self.select(["integration", "iggy"], tests=["round_trip", "protocol"])
        self.assertEqual(len(calls), 1)
        self.assertIn("--workspace", calls[0])
        self.assertNotIn("--test", calls[0])
        self.assertEqual(calls[0][calls[0].index("-E") + 1],
                         "(package(=integration) | package(=iggy)) & kind(test) & "
                         "(binary(=round_trip) | binary(=protocol))")

    def test_server_and_cli_prerequisites_use_the_shared_runtime(self):
        calls = self.select(["iggy-gateway-kafka"], runtime_packages=["server", "iggy-cli"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], ["nextest", "archive"])
        self.assert_runtime_archive([])
        self.assertEqual((self.root / "rust-test-build/kafka-fixtures").read_text(), "true\n")
        # No runtime build means no prebuilt generator, so the script builds its own.
        self.assertEqual((self.root / "kafka-message-gen").read_text(), "")

    def test_skipped_scope_does_not_compile_or_archive(self):
        self.assertEqual(self.select([]), [])
        self.assertEqual((self.root / "rust-test-build/scope").read_text(), "skipped\n")
        self.assertFalse((self.root / "rust-test-build/nextest.tar.zst").exists())

    def test_selector_failure_discards_partial_package_selection(self):
        selector = self.root / "cargo-rail-action"
        selector.write_text("""#!/usr/bin/env bash
case "$2:$4" in
  cargo-scope:*) printf 'packages\\n' ;;
  target-args:*) printf '%s\\0' '--test' 'selected-test' ;;
  package-names:cargo.test) printf '%s\\0' 'selected-test-package' ;;
  *) exit 1 ;;
esac
""")
        selector.chmod(0o755)
        self.env["PLAN_STATUS"] = "success"
        self.assert_workspace_fallback()


class RuntimeSelection(unittest.TestCase):
    def detect(self, files, *, pull_request=None, cwd=REPO_ROOT):
        workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/_detect.yml").read_text())
        components = yaml.safe_load((REPO_ROOT / ".github/config/components.yml").read_text())
        step = next(step for step in workflow["jobs"]["detect"]["steps"] if step.get("id") == "mk")
        script = step["with"]["script"]
        script = script.replace("${{ steps.config.outputs.components }}", json.dumps(components))
        script = script.replace("${{ steps.changed.outputs.all_files || '[]' }}", json.dumps(files))
        context = {"eventName": "pull_request", "payload": {"pull_request": pull_request}}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            env = env_without_git(GITHUB_OUTPUT=str(output))
            result = run("node", "-e", f"const context = {json.dumps(context)};\n" + script, cwd=cwd, env=env)
            lines = output.read_text().splitlines() if output.exists() else []
            return result, dict(line.split("=", 1) for line in lines)

    def select(self, files, **kwargs):
        result, outputs = self.detect(files, **kwargs)
        self.assertEqual(result.returncode, 0, result.stderr)
        return outputs

    @staticmethod
    def stack_layer(position, size, base, stack_base="master"):
        return {"base": {"ref": base},
                "stack": {"number": 7, "position": position, "size": size, "base": {"ref": stack_base}}}

    @staticmethod
    def tasks(matrix):
        return {(entry["component"], entry["task"]) for entry in json.loads(matrix)["include"]}

    def stack_checkout(self):
        """Build the checkout of a top layer: master, three layers, and a merge ref per
        layer on top of the merge ref below it, as GitHub builds them. HEAD is the top one.

        Return it with the master commit that the stack forked from."""
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        checkout = Path(temp.name)
        env = env_without_git(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
                              GIT_AUTHOR_NAME="Stack test", GIT_AUTHOR_EMAIL="stack@example.com",
                              GIT_COMMITTER_NAME="Stack test", GIT_COMMITTER_EMAIL="stack@example.com")

        def git(*args):
            result = run("git", *args, cwd=checkout, env=env)
            self.assertEqual(result.returncode, 0, result.stderr)
            return result.stdout.strip()

        def commit(path, content):
            (checkout / path).parent.mkdir(parents=True, exist_ok=True)
            (checkout / path).write_text(content)
            git("add", "--all")
            git("commit", "--quiet", "-m", path)

        git("init", "--quiet", "--initial-branch=master")
        commit("helm/values.yaml", "replicas: 1\n")
        fork = git("rev-parse", "HEAD")
        git("switch", "--quiet", "-c", "layer-1")
        commit("core/server/src/main.rs", "fn main() {}\n")
        git("switch", "--quiet", "-c", "layer-2")
        (checkout / "docs").mkdir()
        git("mv", "helm/values.yaml", "docs/values.yaml")
        git("commit", "--quiet", "-m", "move the values file")
        git("switch", "--quiet", "-c", "layer-3")
        commit("docs/unrelated.md", "notes\n")
        git("switch", "--quiet", "--detach", "master")
        for layer in ("layer-1", "layer-2", "layer-3"):
            git("merge", "--quiet", "--no-ff", "-m", f"Merge {layer}", layer)
        merge = git("rev-parse", "HEAD")
        # The base branch moves after GitHub built the merge commit.
        git("switch", "--quiet", "master")
        commit("web/package.json", "{}\n")
        git("update-ref", "refs/remotes/origin/master", "master")
        git("switch", "--quiet", "--detach", merge)
        return checkout, fork

    def test_component_config_change_runs_selection_validation(self):
        outputs = self.select([".github/config/components.yml"])
        self.assertIn(("ci-workflows", "validate"), self.tasks(outputs["other_matrix"]))

    def test_nextest_config_change_runs_miri(self):
        outputs = self.select([".config/nextest.toml"])
        plan = json.loads(outputs["rust_plan"])
        self.assertIn("miri", [entry["task"] for entry in plan["checks"]])

    def test_runtime_restore_change_exercises_real_consumers(self):
        outputs = self.select(["scripts/ci/restore-runtime.sh"])
        self.assertEqual(outputs["runtime"], "true")
        self.assertTrue(json.loads(outputs["rust_runtime_plan"])["tests"])
        self.assertIn(("ci-workflows", "validate"), self.tasks(outputs["other_matrix"]))

    def test_python_only_change_builds_runtime_without_rust_tests(self):
        outputs = self.select(["foreign/python/src/lib.rs"])
        self.assertEqual(outputs["runtime"], "true")
        self.assertEqual(outputs["rust_plan"], "")
        self.assertEqual(outputs["rust_runtime_plan"], "")
        self.assertEqual(json.loads(outputs["python_matrix"])["include"][0]["component"], "sdk-python")

    def test_server_change_selects_runtime_and_rust_tests(self):
        outputs = self.select(["core/server/src/main.rs"])
        self.assertEqual(outputs["runtime"], "true")
        runtime_plan = json.loads(outputs["rust_runtime_plan"])
        self.assertTrue(runtime_plan["tests"])
        self.assertEqual([entry["task"] for entry in runtime_plan["checks"]], ["test-storage-compat"])
        plan = json.loads(outputs["rust_plan"])
        self.assertFalse({"test", "test-storage-compat"} & {entry["task"] for entry in plan["checks"]})

    def test_swift_only_change_builds_server_for_e2e(self):
        self.assertEqual(self.select(["foreign/swift/Sources/Iggy/IggyClient.swift"])["runtime"], "true")

    def test_unrelated_documentation_does_not_build_server(self):
        self.assertEqual(self.select(["docs/unrelated.md"])["runtime"], "false")

    def test_stack_middle_layer_runs_rust_lint_only(self):
        files = ["core/server/src/main.rs", "foreign/python/src/lib.rs", "helm/values.yaml"]
        result, outputs = self.detect(files, pull_request=self.stack_layer(2, 3, "layer-1"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sorted(entry["task"] for entry in json.loads(outputs["rust_plan"])["checks"]),
                         ["clippy", "fmt", "machete", "sort"])
        self.assertEqual(outputs["rust_runtime_plan"], "")
        self.assertEqual(outputs["stack_base"], "")
        self.assertEqual(outputs["runtime"], "false")
        for name, matrix in outputs.items():
            if name.endswith("_matrix"):
                self.assertEqual(self.tasks(matrix), {("noop", "noop")}, name)
        self.assertIn("::notice title=Stack 7::Pull request 2 of 3 runs lint only.", result.stdout)

    def test_stack_lowest_unmerged_layer_runs_the_full_plan(self):
        files = ["core/server/src/main.rs"]
        unstacked = self.select(files)
        # GitHub retargets a layer to the base branch of the stack after the layers
        # below it merge. Position 3 is then the top too, and it has no layer left to cover.
        for position in (1, 2, 3):
            with self.subTest(position=position):
                self.assertEqual(self.select(files, pull_request=self.stack_layer(position, 3, "master")),
                                 unstacked)

    def test_stack_layer_without_a_size_runs_the_full_plan(self):
        files = ["core/server/src/main.rs"]
        pull_request = self.stack_layer(2, 3, "layer-1")
        del pull_request["stack"]["size"]
        self.assertEqual(self.select(files, pull_request=pull_request), self.select(files))

    def test_stack_of_two_keeps_the_top_layer_on_its_own_diff(self):
        files = ["foreign/python/src/lib.rs"]
        self.assertEqual(self.select(files, pull_request=self.stack_layer(2, 2, "layer-1")),
                         self.select(files))

    def test_stack_top_layer_tests_every_layer(self):
        checkout, fork = self.stack_checkout()
        result, outputs = self.detect(["docs/unrelated.md"], cwd=checkout,
                                      pull_request=self.stack_layer(3, 3, "layer-2"))
        self.assertEqual(result.returncode, 0, result.stderr)
        # Layer 1 changed the server, so the top layer runs its tests and its storage check.
        runtime_plan = json.loads(outputs["rust_runtime_plan"])
        self.assertTrue(runtime_plan["tests"])
        self.assertEqual([entry["task"] for entry in runtime_plan["checks"]], ["test-storage-compat"])
        # Both compare against the commit the stack forked from. The first parent of
        # HEAD would hide the layers below, and the tip of master is not in HEAD.
        self.assertEqual(outputs["stack_base"], fork)
        other = self.tasks(outputs["other_matrix"])
        # Layer 2 moved this file out of helm/, and a rename counts for both paths.
        self.assertIn(("helm", "validate"), other)
        # Only the base branch changed web/, after the stack forked.
        self.assertFalse({task for task in other if task[0] == "web-ui"})
        self.assertIn("::notice title=Stack 7::The top pull request tests", result.stdout)

    def test_stack_top_layer_fails_without_the_stack_diff(self):
        pull_request = self.stack_layer(3, 3, "layer-2", stack_base="branch-that-was-never-fetched")
        result, outputs = self.detect(["docs/unrelated.md"], pull_request=pull_request)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})


class RuntimeConsumers(unittest.TestCase):
    def test_every_server_start_follows_a_runtime_download(self):
        for path in sorted((REPO_ROOT / ".github").rglob("*.yml")):
            document = yaml.safe_load(path.read_text())
            step_lists = [document.get("runs", {}).get("steps", [])]
            step_lists += [job.get("steps", []) for job in document.get("jobs", {}).values()]
            for steps in step_lists:
                uses = [step.get("uses") for step in steps]
                if SERVER_START in uses:
                    self.assertIn(DOWNLOAD_RUNTIME, uses[:uses.index(SERVER_START)], path.relative_to(REPO_ROOT))

    def test_readme_timeout_fails_and_stops_remaining_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text("sleep 30\nprintf 'unexpected execution'\n")
            result = run(
                "bash", "-c",
                'source "$1"; EXAMPLES_EXIT_CODE=0; '
                'run_readme_commands README.md "." 1; exit "$EXAMPLES_EXIT_CODE"',
                "bash", str(REPO_ROOT / "scripts/utils.sh"), cwd=root,
            )
            self.assertEqual(result.returncode, 124, result.stdout + result.stderr)
            self.assertNotIn("unexpected execution", result.stdout)

    def test_python_examples_reuse_synced_environment_for_plain_and_tls(self):
        result, events = self.run_python_examples()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(events, [
            "sync --frozen", "server-start", "run --no-sync example.py", "example",
            "server-start", "run --no-sync example.py --tls", "example --tls",
        ])

    def test_python_sync_failure_stops_before_starting_server_or_examples(self):
        result, events = self.run_python_examples(sync_exit=23)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(events, ["sync --frozen"])

    def test_python_consumer_failures_return_nonzero(self):
        for example in ("basic", "getting-started"):
            failures = {
                "connect": "connection failed",
                "poll": "poll failed",
                "decode": "UnicodeDecodeError",
            }
            if example == "getting-started":
                failures["config"] = "invalid configuration"
            for failure, message in failures.items():
                with self.subTest(example=example, failure=failure):
                    result = self.run_python_consumer(example, failure)
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertIn(message, result.stderr)

    def test_python_consumers_complete_successfully(self):
        for example in ("basic", "getting-started"):
            with self.subTest(example=example):
                result = self.run_python_consumer(example)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def run_python_consumer(self, example, failure=""):
        script = r"""
import asyncio
import runpy
import sys
from unittest.mock import AsyncMock, MagicMock, patch

path, failure = sys.argv[1:]
message = MagicMock()
message.payload.return_value = b"hello" if failure != "decode" else b"\xff"
client = MagicMock()
client.connect = AsyncMock()
client.poll_messages = AsyncMock(return_value=MagicMock(messages=[message]))
sdk = MagicMock()
sdk.IggyClient.return_value = client
sdk.IggyClient.from_connection_string.return_value = client
if failure == "connect":
    client.connect.side_effect = RuntimeError("connection failed")
elif failure == "poll":
    client.poll_messages.side_effect = RuntimeError("poll failed")
elif failure == "config":
    sdk.TcpConfig.side_effect = ValueError("invalid configuration")
sys.argv = [path]
with (
    patch.dict(sys.modules, {"apache_iggy": sdk, "loguru": MagicMock()}),
    patch.object(asyncio, "sleep", new=AsyncMock()),
):
    consumer = runpy.run_path(path, run_name="__main__")
if not failure and client.poll_messages.await_count != consumer["BATCHES_LIMIT"]:
    raise AssertionError("Consumer did not complete all batches")
"""
        return run(sys.executable, "-c", script,
                   str(REPO_ROOT / "examples/python" / example / "consumer.py"), failure)

    def run_python_examples(self, *, sync_exit=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            examples = root / "examples/python"
            binaries = root / "target/debug"
            examples.mkdir(parents=True)
            binaries.mkdir(parents=True)
            (examples / "README.md").write_text(
                "uv run example.py\nuv run example.py --tls\n"
            )
            (examples / "example.py").write_text(
                'import os, sys\n'
                'with open(os.environ["EXAMPLE_EVENTS"], "a") as events:\n'
                '    events.write(" ".join(["example", *sys.argv[1:]]) + "\\n")\n'
            )
            server = binaries / "iggy-server"
            server.write_text(
                '#!/usr/bin/env bash\n'
                'echo server-start >> "$EXAMPLE_EVENTS"\n'
                'echo "client listeners started"\n'
                'exec sleep 60\n'
            )
            server.chmod(0o755)
            uv = binaries / "uv"
            uv.write_text(
                '#!/usr/bin/env bash\n'
                'echo "$*" >> "$EXAMPLE_EVENTS"\n'
                'if [ "$1" = sync ]; then exit "$SYNC_EXIT"; fi\n'
                'if [ "$1" != run ] || [ "$2" != --no-sync ]; then exit 1; fi\n'
                'shift 2\n'
                'exec python3 "$@"\n'
            )
            uv.chmod(0o755)
            events = root / "events"
            env = {**os.environ, "PATH": f'{binaries}:{os.environ["PATH"]}',
                   "EXAMPLE_EVENTS": str(events), "SYNC_EXIT": str(sync_exit)}
            result = run("bash", str(REPO_ROOT / "scripts/run-examples-from-readme.sh"),
                         "--language", "python", cwd=root, env=env)
            return result, events.read_text().splitlines()


class PublishSelection(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote = self.root / "origin.git"
        self.checkout = self.root / "checkout"
        self.checkout.mkdir()
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
                        GIT_AUTHOR_NAME="Publish test", GIT_AUTHOR_EMAIL="publish@example.com",
                        GIT_COMMITTER_NAME="Publish test", GIT_COMMITTER_EMAIL="publish@example.com")
        self.git("init", "--bare", "--quiet", str(self.remote))
        self.git("init", "--quiet")
        self.git("commit", "--allow-empty", "-m", "Release candidate A")
        self.first_commit = self.git("rev-parse", "HEAD").strip()
        self.git("commit", "--allow-empty", "-m", "Release candidate B")
        self.git("remote", "add", "origin", str(self.remote))
        self.git("push", "origin", "HEAD:refs/heads/master")
        self.tags = {
            "rust-common": "common-0.9.0-edge.1",
            "rust-binary-protocol": "binary-protocol-0.9.0-edge.1",
            "rust-sdk": "iggy-0.9.0-edge.1",
            "rust-cli": "cli-0.9.0-edge.1",
            "sdk-python": "python-sdk-0.9.0rc1",
            "sdk-node": "node-sdk-0.9.0-rc.1",
            "sdk-java": "java-sdk-0.9.0-rc.1",
            "sdk-csharp": "csharp-sdk-0.9.0-rc.1",
            "sdk-go": "foreign/go/v0.9.0-rc.1",
        }
        (self.checkout / "scripts").mkdir()

    def git(self, *args):
        result = run("git", *args, cwd=self.checkout, env=self.env)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def release(self, *components):
        for component in components or self.tags:
            tag = self.tags[component]
            if tag:
                self.git("--git-dir", str(self.remote), "tag", "-a", tag,
                         self.first_commit, "-m", tag)

    def plan(self, *, automatic=True, skip_tags=False, docker="rust-server"):
        extractor = []
        for key, tag in self.tags.items():
            extractor.append(f"{key}:--should-tag) echo {'true' if tag else 'false'} ;;")
            if tag:
                extractor.append(f"{key}:--tag) echo '{tag}' ;;")
        (self.checkout / "scripts/extract-version.sh").write_text(
            'case "$1:$2" in\n' + "\n".join(extractor) + "\n*) exit 1 ;;\nesac\n")
        workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/publish.yml").read_text())
        config = yaml.safe_load((REPO_ROOT / ".github/config/publish.yml").read_text())["components"]
        step = next(step for step in workflow["jobs"]["plan"]["steps"] if step.get("id") == "mk")
        script = step["with"]["script"]
        values = {
            "steps.cfg.outputs.components_b64": base64.b64encode(json.dumps(config).encode()).decode(),
            "inputs.publish_crates": ",".join(key for key in self.tags if key.startswith("rust-")),
            "inputs.publish_dockerhub": docker,
            "inputs.publish_other": ",".join(key.removeprefix("sdk-") for key in self.tags if key.startswith("sdk-")),
            "inputs.create_edge_docker_tag": str(automatic).lower(),
            "inputs.skip_tag_creation": str(skip_tags).lower(),
        }
        for expression, value in values.items():
            script = script.replace("${{ " + expression + " }}", value)
        harness = """
const outputs = {};
const core = { setOutput(name, value) { outputs[name] = value; }, info() {}, warning() {} };
const console = { log() {} };
"""
        return run("node", "-e", harness + script + "\nprocess.stdout.write(JSON.stringify(outputs));",
                   cwd=self.checkout, env=self.env)

    def outputs(self, **kwargs):
        result = self.plan(**kwargs)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_queued_publish_drops_completed_releases_but_keeps_edge_images(self):
        before = self.outputs()
        self.assertEqual({target["key"] for target in json.loads(before["targets"])["include"]},
                         set(self.tags) | {"rust-server"})
        self.release()
        self.assertEqual(self.git("tag", "--list"), "")
        after = self.outputs()
        self.assertEqual([target["key"] for target in json.loads(after["targets"])["include"]], ["rust-server"])
        self.assertEqual(after["crates"], "")
        self.assertEqual(after["has_python"], "false")
        self.assertEqual(after["has_docker"], "true")
        self.assertEqual(len(json.loads(after["docker_matrix"])["include"]), 2)

    def test_partial_release_keeps_only_unreleased_crates_and_sdks(self):
        self.release("rust-sdk", "sdk-python")
        outputs = self.outputs()
        self.assertEqual(set(outputs["crates"].split(",")), {"rust-common", "rust-binary-protocol", "rust-cli"})
        self.assertEqual({target["key"] for target in json.loads(outputs["targets"])["include"]},
                         (set(self.tags) - {"rust-sdk", "sdk-python"}) | {"rust-server"})

    def test_completed_release_without_docker_targets_produces_an_empty_plan(self):
        self.release()
        outputs = self.outputs(docker="")
        self.assertEqual(outputs["count"], "0")
        self.assertEqual(outputs["crates"], "")
        self.assertEqual(outputs["has_python"], "false")
        self.assertEqual(outputs["has_docker"], "false")
        self.assertEqual(json.loads(outputs["targets"]), {"include": [{"key": "noop", "type": "noop"}]})

    def test_mutable_snapshot_remains_selected_after_other_releases_complete(self):
        self.tags["sdk-java"] = None
        self.release()
        outputs = self.outputs()
        self.assertEqual([target["key"] for target in json.loads(outputs["non_docker_targets"])["include"]], ["sdk-java"])

    def test_manual_and_explicit_republish_requests_keep_completed_releases(self):
        self.release()
        self.git("remote", "set-url", "origin", str(self.root / "missing.git"))
        for options in ({"automatic": False}, {"skip_tags": True}):
            with self.subTest(**options):
                outputs = self.outputs(**options)
                self.assertEqual({target["key"] for target in json.loads(outputs["targets"])["include"]},
                                 set(self.tags) | {"rust-server"})
                self.assertEqual(outputs["has_python"], "true")

    def test_remote_lookup_failure_does_not_produce_a_publish_plan(self):
        self.git("remote", "set-url", "origin", str(self.root / "missing.git"))
        result = self.plan()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not appear to be a git repository", result.stderr)
        self.assertEqual(result.stdout, "")


class FinalizeStatus(unittest.TestCase):
    def check(self, *results):
        workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/pre-merge.yml").read_text())
        step = next(step for step in workflow["jobs"]["finalize_pr"]["steps"]
                    if step.get("name") == "Check status")
        self.assertEqual(step["env"]["NEEDS_RESULTS"], "${{ join(needs.*.result, ' ') }}")
        # Every expression left in the script only fills the summary table.
        script = re.sub(r"\$\{\{.*?\}\}", "-", step["run"])
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, NEEDS_RESULTS=" ".join(results),
                       GITHUB_STEP_SUMMARY=str(Path(directory) / "summary"))
            return run("bash", "-c", script, env=env).returncode

    def test_success_and_skipped_jobs_pass(self):
        self.assertEqual(self.check("success", "skipped", "success"), 0)

    def test_every_other_result_fails(self):
        # GitHub ends a job that never got a runner as `abandoned`.
        for result in ("failure", "cancelled", "abandoned"):
            with self.subTest(result=result):
                self.assertEqual(self.check("success", result, "skipped"), 1)


if __name__ == "__main__":
    unittest.main()
