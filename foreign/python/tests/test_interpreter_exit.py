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
Tests that the interpreter exits cleanly while the SDK hands a result to Python.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from .fixtures.exit_while_handing_over import FINALIZED, SCENARIOS

SCRIPT = Path(__file__).parent / "fixtures" / "exit_while_handing_over.py"
EXIT_TIMEOUT_SECONDS = 20


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_exit_while_sdk_hands_result_to_python(scenario: str):
    completed = subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT), scenario],
        capture_output=True,
        timeout=EXIT_TIMEOUT_SECONDS,
        check=False,
    )

    stderr = completed.stderr.decode(errors="replace")
    assert completed.returncode == 0, stderr
    # pyo3 panics instead of aborting when a thread takes the GIL after the
    # interpreter marks itself as no longer initialized.
    assert "panicked" not in stderr, stderr
    assert completed.stdout == FINALIZED
