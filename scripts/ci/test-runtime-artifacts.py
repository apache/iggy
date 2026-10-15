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

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
RESTORE = REPO_ROOT / "scripts/ci/restore-runtime.sh"


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
        checksums = "".join(
            f"{hashlib.sha256((self.producer / name).read_bytes()).hexdigest()}  {name}\n"
            for name in self.files
        )
        (self.artifact / "SHA256SUMS").write_text(checksums)
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

    def test_corrupted_binary_fails_checksum_validation(self):
        (self.artifact / "SHA256SUMS").write_text("0" * 64 + "  " + self.files[0] + "\n")
        result = self.restore()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FAILED", result.stdout)

    def test_missing_cli_is_not_accepted_as_a_complete_runtime(self):
        self.files.pop()
        self.pack()
        self.assertNotEqual(self.restore().returncode, 0)

    def test_nonexecutable_server_is_rejected(self):
        (self.producer / self.files[0]).chmod(0o644)
        self.pack()
        self.assertNotEqual(self.restore().returncode, 0)

    def test_rerun_restores_the_original_binary(self):
        self.assertEqual(self.restore().returncode, 0)
        (self.consumer / self.files[0]).write_text("overwritten by another build")
        self.assertEqual(self.restore().returncode, 0)
        self.assertEqual((self.consumer / self.files[0]).read_bytes(),
                         (self.producer / self.files[0]).read_bytes())


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

    def test_python_only_change_builds_runtime_without_rust_tests(self):
        outputs = self.select(["foreign/python/src/lib.rs"])
        self.assertEqual(outputs["runtime"], "true")
        self.assertEqual(outputs["rust_plan"], "")
        self.assertEqual(json.loads(outputs["python_matrix"])["include"][0]["component"], "sdk-python")

    def test_server_change_selects_runtime_and_rust_tests(self):
        outputs = self.select(["core/server/src/main.rs"])
        self.assertEqual(outputs["runtime"], "true")
        self.assertTrue(json.loads(outputs["rust_plan"])["tests"])

    def test_swift_only_change_does_not_build_server(self):
        self.assertEqual(self.select(["foreign/swift/Sources/Iggy/IggyClient.swift"])["runtime"], "false")

    def test_unrelated_documentation_does_not_build_server(self):
        self.assertEqual(self.select(["docs/unrelated.md"])["runtime"], "false")


if __name__ == "__main__":
    unittest.main()
