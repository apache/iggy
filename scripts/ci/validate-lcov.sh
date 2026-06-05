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

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <lcov-file>"
  exit 2
fi

lcov_file="$1"

if [ ! -s "$lcov_file" ]; then
  echo "LCOV report is missing or empty: $lcov_file"
  exit 1
fi

source_file_count="$(grep -c '^SF:' "$lcov_file" || true)"
line_data_count="$(grep -c '^DA:' "$lcov_file" || true)"

if [ "$source_file_count" -eq 0 ]; then
  echo "LCOV report has no source file records: $lcov_file"
  exit 1
fi

if [ "$line_data_count" -eq 0 ]; then
  echo "LCOV report has no line data records: $lcov_file"
  exit 1
fi

echo "LCOV report valid: $lcov_file (${source_file_count} SF, ${line_data_count} DA)"
