#!/bin/bash
# Test all Python Iggy examples
set -e

EXAMPLES=(
  "basic/producer.py"
  "basic/consumer.py"
  "getting_started/producer.py"
  "getting_started/consumer.py"
  "message_headers/producer.py"
  "message_headers/consumer.py"
  "message_envelope/producer.py"
  "message_envelope/consumer.py"
  "multi_tenant/producer.py"
  "multi_tenant/consumer.py"
  "stream_builder/producer.py"
  "stream_builder/consumer.py"
)

for example in "${EXAMPLES[@]}"; do
  echo "Running $example ..."
  python "$example" &
  pid=$!
  sleep 3
  kill $pid || true
  echo "$example finished."
done

echo "All Python examples tested."
