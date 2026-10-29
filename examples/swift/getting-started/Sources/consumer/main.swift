// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Common
import Iggy

// Reads partition 0 of the sample topic from its first message and exits
// after 5 batches. The consumer stores no offset, so every run reads the
// same messages, and it waits for the producer to create the stream.
let options = ExampleOptions.parse()
let client = options.makeClient()
print("Connecting to server at \(options.serverAddress)")
try await client.connect()

let consumer = try client.consumer(
    name: "getting-started", streamID: try Identifier(named: Sample.streamName), topicID: try Identifier(named: Sample.topicName),
    partitionID: Sample.partitionID,
    configuration: ConsumerConfiguration(
        pollingStrategy: .first, batchLength: UInt32(Sample.messagesPerBatch), pollInterval: Sample.interval, autoCommit: .disabled,
        initRetries: 20, initRetryInterval: Sample.interval))
try await consumer.initialize()

print(
    "Messages will be consumed from stream: \(Sample.streamName), topic: \(Sample.topicName), partition: \(Sample.partitionID) with interval \(Sample.interval)."
)
var consumed = 0
for try await received in consumer {
    print("Handling message at offset: \(received.message.offset), payload: \(received.message.payloadString)...")
    consumed += 1
    if consumed == Sample.batchesLimit * Sample.messagesPerBatch {
        break
    }
}
print("Consumed \(Sample.batchesLimit) batches of messages, exiting.")
await consumer.shutdown()
await client.shutdown()
