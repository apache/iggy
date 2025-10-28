 break down this Iggy HTTP API message payload:

  curl -s -X POST "http://localhost:3000/streams/3/topics/1/messages" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"c3RyZWFtaW5nIGRhdGEgcHJvY2Vzc2luZyB3aXRoIGZsaW5r"}]}'

  URL Breakdown

  http://localhost:3000/streams/3/topics/1/messages
                                 │         │
                                 │         └─ Topic ID: 1 (named "lines")
                                 └─────────── Stream ID: 3 (named "text-input")

  You can use either numeric IDs or names:
  - /streams/3/topics/1/messages ← numeric IDs
  - /streams/text-input/topics/lines/messages ← names

  JSON Payload Structure

  {
    "partitioning": {
      "kind": "balanced",
      "value": ""
    },
    "messages": [
      {
        "payload": "c3RyZWFtaW5nIGRhdGEgcHJvY2Vzc2luZyB3aXRoIGZsaW5r"
      }
    ]
  }

  1. partitioning Object

  Determines which partition the message goes to:

  "partitioning": {
    "kind": "balanced",    // Partitioning strategy
    "value": ""            // Strategy-specific value (empty for balanced)
  }

  Available partitioning strategies:

  "balanced" (Round-robin distribution)

  {"kind": "balanced", "value": ""}
  - Distributes messages evenly across all partitions
  - value must be empty string

  "partition_id" (Direct partition assignment)

  {"kind": "partition_id", "value": "AQ=="}  // Partition 1 (base64 encoded)
  - Send to a specific partition
  - value must be the partition ID encoded in base64
  - Example: Partition 1 → \x01 → base64 → "AQ=="
  - Example: Partition 2 → \x02 → base64 → "Ag=="

  "messages_key" (Hash-based partitioning)

  {"kind": "messages_key", "value": "dXNlcjEyMw=="}  // "user123" in base64
  - Messages with the same key go to the same partition
  - value is the key encoded in base64
  - Ensures ordering for messages with the same key

  2. messages Array

  Array of messages to send (can batch multiple messages):

  "messages": [
    {
      "payload": "c3RyZWFtaW5nIGRhdGEgcHJvY2Vzc2luZyB3aXRoIGZsaW5r",
      "id": 123,              // Optional: message ID (auto-generated if omitted)
      "headers": {            // Optional: custom headers
        "user": "alice",
        "trace_id": "abc123"
      }
    },
    {
      "payload": "YW5vdGhlciBtZXNzYWdl"  // Can send multiple messages
    }
  ]

  3. payload Field (BASE64 ENCODED!)

  This is the actual message content, but it MUST be base64 encoded.

  "payload": "c3RyZWFtaW5nIGRhdGEgcHJvY2Vzc2luZyB3aXRoIGZsaW5r"

  Let me decode this:

  echo "c3RyZWFtaW5nIGRhdGEgcHJvY2Vzc2luZyB3aXRoIGZsaW5r" | base64 -d
  # Output: streaming data processing with flink

  Why Base64?

  1. Binary safety: Iggy messages are arbitrary bytes, not necessarily UTF-8 text
  2. JSON compatibility: Raw bytes can't be represented in JSON
  3. Consistency: Same API works for text, binary data, images, etc.

  How to Create Payloads

  For text messages:

  # Encode text to base64
  echo -n "hello world" | base64
  # Output: aGVsbG8gd29ybGQ=

  # Use in curl
  curl -X POST "http://localhost:3000/streams/3/topics/1/messages" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"aGVsbG8gd29ybGQ="}]}'

  For binary data:

  # Encode a file to base64
  cat image.png | base64 > image.b64

  # Use the base64 string in your JSON payload

  Complete Examples

  Example 1: Simple text message

  # Original text: "hello world"
  # Base64: aGVsbG8gd29ybGQ=

  curl -X POST "http://localhost:3000/streams/3/topics/1/messages" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{
      "partitioning": {"kind": "balanced", "value": ""},
      "messages": [{"payload": "aGVsbG8gd29ybGQ="}]
    }'

  Example 2: Multiple messages

  curl -X POST "http://localhost:3000/streams/3/topics/1/messages" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{
      "partitioning": {"kind": "balanced", "value": ""},
      "messages": [
        {"payload": "aGVsbG8gd29ybGQ="},        // "hello world"
        {"payload": "aGVsbG8gZmxpbms="}          // "hello flink"
      ]
    }'

  Example 3: With message ID and headers

  curl -X POST "http://localhost:3000/streams/3/topics/1/messages" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{
      "partitioning": {"kind": "balanced", "value": ""},
      "messages": [{
        "id": 12345,
        "payload": "aGVsbG8gd29ybGQ=",
        "headers": {
          "user_id": "alice",
          "request_id": "req-001"
        }
      }]
    }'

  Example 4: Partition-specific

  # Send to partition 1
  # Partition ID 1 as bytes: [0x01]
  # Base64 encoded: "AQ=="

  curl -X POST "http://localhost:3000/streams/3/topics/1/messages" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{
      "partitioning": {"kind": "partition_id", "value": "AQ=="},
      "messages": [{"payload": "aGVsbG8gd29ybGQ="}]
    }'

  How Flink Receives It

  When your Flink job reads this message:

  1. IggySource polls messages from Iggy
  2. Receives raw bytes (Iggy automatically decodes base64)
  3. StringDeserializationSchema converts bytes to String:
  public String deserialize(byte[] data, RecordMetadata metadata) {
      return new String(data, charset);  // Converts bytes to "hello world"
  }
  4. Your Flink job processes the text: "hello world"

  Helper Script for Encoding

  Save this as encode-message.sh:
  #!/bin/bash
  TEXT="$1"
  BASE64=$(echo -n "$TEXT" | base64)
  echo "Original: $TEXT"
  echo "Base64:   $BASE64"
  echo ""
  echo "curl command:"
  echo "curl -X POST \"http://localhost:3000/streams/3/topics/1/messages\" \\"
  echo "  -H \"Content-Type: application/json\" \\"
  echo "  -H \"Authorization: Bearer \$TOKEN\" \\"
  echo "  -d '{\"partitioning\":{\"kind\":\"balanced\",\"value\":\"\"},\"messages\":[{\"payload\":\"$BASE64\"}]}'"

  Usage:
  ./encode-message.sh "hello world"
  # Shows you the base64 and complete curl command
