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
