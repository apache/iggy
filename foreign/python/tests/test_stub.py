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
Tests that the tracked type stub describes the native module.
"""

import ast
import enum
from pathlib import Path

import pytest

import apache_iggy

STUB = ast.parse((Path(__file__).parent.parent / "apache_iggy.pyi").read_text())
STUB_CLASSES = [node for node in STUB.body if isinstance(node, ast.ClassDef)]


class TestStub:
    """The stub against the classes the extension module exports."""

    @pytest.mark.unit
    def test_given_stub_enum_bases_when_importing_should_match_native_enums(self):
        """A class the stub declares as `enum.Enum` is one at runtime."""
        declared = {
            node.name
            for node in STUB_CLASSES
            if "enum.Enum" in map(ast.unparse, node.bases)
        }
        native = {
            node.name
            for node in STUB_CLASSES
            if issubclass(getattr(apache_iggy, node.name), enum.Enum)
        }
        assert declared == native

    @pytest.mark.unit
    def test_given_stub_class_constants_when_importing_should_be_native_members(self):
        """A `typing.ClassVar` of its own class is a member of that class."""
        for node in STUB_CLASSES:
            native_class = getattr(apache_iggy, node.name)
            for statement in node.body:
                if (
                    isinstance(statement, ast.AnnAssign)
                    and isinstance(statement.target, ast.Name)
                    and ast.unparse(statement.annotation)
                    == f"typing.ClassVar[{node.name}]"
                ):
                    member = getattr(native_class, statement.target.id)
                    assert isinstance(member, native_class), statement.target.id
