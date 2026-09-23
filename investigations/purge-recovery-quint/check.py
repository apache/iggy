#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run the models; distinguish expected counterexamples from tool failures."""

import argparse
import json
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quint", default="quint")
    parser.add_argument("--suite", choices=["quick", "tlc", "all"], default="all")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    results = root / "results"
    results.mkdir(exist_ok=True)
    checks = []

    def add(name, command, expected):
        checks.append((name, [args.quint, *command], expected))

    if args.suite in ("quick", "all"):
        for model, module in [
            ("recovery", "recovery"),
            ("lifecycle", "lifecycle"),
            ("lifecycle", "lifecycle_transfer"),
        ]:
            add(f"{module}-typecheck", ["typecheck", f"{model}.qnt"], "success")
            add(
                f"{module}-tests",
                [
                    "test",
                    f"{model}.qnt",
                    f"--main={module}",
                    "--backend=typescript",
                    "--seed=4130",
                ],
                "success",
            )
        for name, init, witnesses in [
            ("current", "init", []),
            (
                "certified",
                "initCertified",
                [
                    "freshAfterRestart",
                    "delayedJoin",
                    "rollingRepair",
                    "secondRestart",
                    "recoveredQuorum",
                    "writesInBothEras",
                ],
            ),
        ]:
            add(
                f"recovery-{name}-simulation",
                [
                    "run",
                    "recovery.qnt",
                    f"--init={init}",
                    "--backend=typescript",
                    "--invariant=safe",
                    "--seed=4130",
                    "--max-samples=2000",
                    "--max-steps=40",
                    "--verbosity=3",
                    *(["--witnesses", *witnesses] if witnesses else []),
                ],
                "violation" if name == "current" else "success",
            )
        for module, witnesses in [
            (
                "lifecycle",
                [
                    "partialCleanup",
                    "completedAfterCrash",
                    "freshDuringCleanup",
                    "offsetProgress",
                ],
            ),
            (
                "lifecycle_transfer",
                ["rollbackReached", "installReached", "freshReached"],
            ),
        ]:
            add(
                f"{module}-simulation",
                [
                    "run",
                    "lifecycle.qnt",
                    f"--main={module}",
                    "--backend=typescript",
                    "--invariant=safe",
                    "--seed=4130",
                    "--max-samples=10000",
                    "--max-steps=40",
                    "--witnesses",
                    *witnesses,
                ],
                "success",
            )
        add(
            "lifecycle_transfer-current-simulation",
            [
                "run",
                "lifecycle.qnt",
                "--main=lifecycle_transfer",
                "--step=currentStep",
                "--backend=typescript",
                "--invariant=safe",
                "--seed=4130",
                "--max-samples=1000",
                "--max-steps=20",
            ],
            "violation",
        )

    if args.suite in ("tlc", "all"):
        for name, init in [
            ("current", "init"),
            ("clear", "initClear"),
            ("raise", "initRaise"),
            ("clamp", "initClamp"),
            ("volatile", "initVolatile"),
            ("certified", "initCertified"),
        ]:
            add(
                f"recovery-{name}-tlc",
                [
                    "verify",
                    "recovery.qnt",
                    f"--init={init}",
                    "--backend=tlc",
                    "--invariant=safe",
                    "--tlc-config=tlc.json",
                    "--verbosity=3",
                ],
                "success" if name == "certified" else "violation",
            )
        for module, step, expected in [
            ("lifecycle", "step", "success"),
            ("lifecycle", "earlyCompletionStep", "violation"),
            ("lifecycle", "destructiveRetryStep", "violation"),
            ("lifecycle_transfer", "step", "success"),
            ("lifecycle_transfer", "currentStep", "violation"),
            ("lifecycle_transfer", "omitBackupStep", "violation"),
        ]:
            add(
                f"{module}-{step}-tlc",
                [
                    "verify",
                    "lifecycle.qnt",
                    f"--main={module}",
                    f"--step={step}",
                    "--backend=tlc",
                    "--invariant=safe",
                    "--tlc-config=tlc.json",
                    "--verbosity=3",
                ],
                expected,
            )

    summary = []
    for name, command, expected in checks:
        print(f"Running {name}", flush=True)
        try:
            run = subprocess.run(
                command,
                cwd=root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=240,
            )
            output = run.stdout
            code = run.returncode
        except subprocess.TimeoutExpired as error:
            output = error.stdout or b""
            if isinstance(output, bytes):
                output = output.decode(errors="replace")
            code = "timeout"
        (results / f"{name}.log").write_text(output)
        violation = (
            "Invariant violated" in output or "Invariant q_inv is violated" in output
        )
        success = code == 0
        matched = (
            success
            if expected == "success"
            else isinstance(code, int) and code > 0 and violation
        )
        summary.append(
            dict(
                name=name,
                command=command,
                expected=expected,
                returncode=code,
                matched=matched,
            )
        )
        print(
            f"  {'PASS' if matched else 'UNEXPECTED'}: exit {code}, expected {expected}",
            flush=True,
        )
    (results / f"{args.suite}-summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    return 0 if all(item["matched"] for item in summary) else 1


if __name__ == "__main__":
    sys.exit(main())
