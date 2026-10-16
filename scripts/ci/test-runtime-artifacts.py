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

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
RESTORE = REPO_ROOT / "scripts/ci/restore-runtime.sh"
SERVER_START = "./.github/actions/utils/server-start"
DOWNLOAD_RUNTIME = "./.github/actions/utils/download-runtime"


def run(*args, cwd=REPO_ROOT, env=None):
    return subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True)


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
                        TEST_SELECTION="affected", PLAN_STATUS="failure", PLAN_FILE="",
                        GITHUB_WORKSPACE=str(self.root), PATH=f"{self.root}:{os.environ['PATH']}")
        git = self.root / "git"
        git.write_text(f"#!/bin/sh\ncase $1 in\n  rev-parse) echo {self.revision} ;;\n  cat-file) echo 'parent fixture' ;;\n  *) exit 1 ;;\nesac\n")
        git.chmod(0o755)
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
""")
        cargo.chmod(0o755)
        generator = self.root / "gateways/kafka/scripts/ci-wire-fixtures.sh"
        generator.parent.mkdir(parents=True)
        generator.write_text("#!/bin/sh\nmkdir -p gateways/kafka/tools/kafka-tool/kafka_messages\n")
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
                                   "--archive-file", str(artifact / "nextest.tar.zst")])
        self.assertTrue((artifact / "runtime.tar.zst").is_file())
        self.assertEqual((artifact / "kafka-fixtures").read_text(), "true\n")

    def test_planner_failure_builds_all_test_extras_and_tests(self):
        self.assert_workspace_fallback()

    def test_explicit_workspace_selection_does_not_need_a_planner(self):
        self.env["TEST_SELECTION"] = "workspace"
        self.assert_workspace_fallback()

    def test_selector_failure_discards_partial_package_selection(self):
        selector = self.root / "cargo-rail-action"
        selector.write_text("""#!/usr/bin/env bash
case "$2:$4" in
  cargo-scope:*) printf 'packages\\n' ;;
  cargo-args:*) printf '%s\\0' '-p' 'selected-test-package' ;;
  target-args:*) printf '%s\\0' '--lib' ;;
  package-names:cargo.test) printf '%s\\0' 'selected-test-package' ;;
  *) exit 1 ;;
esac
""")
        selector.chmod(0o755)
        self.env["PLAN_STATUS"] = "success"
        self.assert_workspace_fallback()


class RuntimeSelection(unittest.TestCase):
    def select(self, files):
        workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/_detect.yml").read_text())
        components = yaml.safe_load((REPO_ROOT / ".github/config/components.yml").read_text())
        step = next(step for step in workflow["jobs"]["detect"]["steps"] if step.get("id") == "mk")
        script = step["with"]["script"]
        script = script.replace("${{ steps.config.outputs.components }}", json.dumps(components))
        script = script.replace("${{ steps.changed.outputs.all_files || '[]' }}", json.dumps(files))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            env = dict(os.environ, GITHUB_OUTPUT=str(output))
            result = run("node", "-e", "const context = {eventName: 'pull_request'};\n" + script, env=env)
            self.assertEqual(result.returncode, 0, result.stderr)
            return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_component_config_change_runs_selection_validation(self):
        outputs = self.select([".github/config/components.yml"])
        self.assertIn(("ci-workflows", "validate"),
                      [(entry["component"], entry["task"])
                       for entry in json.loads(outputs["other_matrix"])["include"]])

    def test_runtime_restore_change_exercises_real_consumers(self):
        outputs = self.select(["scripts/ci/restore-runtime.sh"])
        self.assertEqual(outputs["runtime"], "true")
        self.assertTrue(json.loads(outputs["rust_runtime_plan"])["tests"])
        self.assertIn(("ci-workflows", "validate"),
                      [(entry["component"], entry["task"])
                       for entry in json.loads(outputs["other_matrix"])["include"]])

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
        self.assertFalse(plan["tests"])
        self.assertFalse({"test", "test-storage-compat"} & {entry["task"] for entry in plan["checks"]})

    def test_swift_only_change_builds_server_for_e2e(self):
        self.assertEqual(self.select(["foreign/swift/Sources/Iggy/IggyClient.swift"])["runtime"], "true")

    def test_unrelated_documentation_does_not_build_server(self):
        self.assertEqual(self.select(["docs/unrelated.md"])["runtime"], "false")


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


if __name__ == "__main__":
    unittest.main()
