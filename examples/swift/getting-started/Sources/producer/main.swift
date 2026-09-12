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

// Connects, creates the sample stream and topic when they are missing, and
// sends a few batches of messages before exiting.
let options = ExampleOptions.parse()
let client = options.makeClient()
print("Connecting to server at \(options.serverAddress)")
try await client.connect()

let producer = client.producer(
    stream: try Identifier(named: Defaults.streamName), topic: try Identifier(named: Defaults.topicName),
    configuration: ProducerConfiguration(partitioning: .partition(Defaults.partitionID), topicPartitionsCount: 1))
try await producer.initialize()
print("Stream and topic are ready.")

print(
    "Messages will be sent to stream: \(Defaults.streamName), topic: \(Defaults.topicName), partition: \(Defaults.partitionID) with interval \(Defaults.interval)."
)
var currentID = 0
for batch in 1...Defaults.batchesLimit {
    try await Task.sleep(for: Defaults.interval)
    var messages: [IggyMessage] = []
    for _ in 0..<Defaults.messagesPerBatch {
        currentID += 1
        messages.append(try IggyMessage("message-\(currentID)"))
    }
    let response = try await producer.send(messages)
    let placement = response.confirmations.first.map { "partition \($0.partitionID) at offset \($0.baseOffset)" } ?? "no confirmation"
    print("Sent \(messages.count) message(s) in batch \(batch): \(placement).")
}
print("Sent \(Defaults.batchesLimit) batches of messages, exiting.")
await producer.shutdown()
try await client.shutdown()
