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

#!/bin/bash

# Must be executed from the root of the repo.

echo "🚀 Running example scripts..."

cd examples/python || exit 1

EXEC_TIMEOUT_SEC=5

for example in ./*; do
    # Skip readme
    if [ -f "./$example" ]; then
        continue
    fi

    # Run producer
    producer_path="./$example/producer.py"
    if [ ! -f "$producer_path" ]; then
        echo "⚠️ producer.py not found in example '${example}'"
        TEST_EXIT_CODE=1
        continue
    fi

    # Note: Examples might run indefinitely, so we'll just test they start correctly
    # Assuming that the script does not require arguments or has default arguments
    timeout $EXEC_TIMEOUT_SEC python3 "$producer_path"
    EXIT_CODE=$?

    # Code 124: default exit code produced by timeout when the task runs out of time
    if [[ $EXIT_CODE = 0 || $EXIT_CODE = 124 ]]; then
        echo "✅ Producer in example '$example' started successfully"
    else
        echo "❌ Producer in example '$example' failed to start"
        TEST_EXIT_CODE=1
        continue
    fi

    # Run consumer
    consumer_path="./$example/consumer.py"
    if [ ! -f "$consumer_path" ]; then
        echo "⚠️ consumer.py not found in example '$example'"
        TEST_EXIT_CODE=1
        continue
    fi

    # Note: Examples might run indefinitely, so we'll just test they start correctly
    # Assuming that the script does not require arguments or has default arguments.
    timeout $EXEC_TIMEOUT_SEC python3 "$consumer_path"
    EXIT_CODE=$?

    if [[ $EXIT_CODE = 0 || $EXIT_CODE = 124 ]]; then
        echo "✅ Consumer in example '$example' started successfully"
    else
        echo "❌ Consumer in example '$example' failed to start"
        TEST_EXIT_CODE=1
    fi
done

exit "$TEST_EXIT_CODE"
