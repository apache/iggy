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
// sends 5 batches of 10 messages to partition 0 before exiting.
let options = ExampleOptions.parse()
let client = options.makeClient()
print("Connecting to server at \(options.serverAddress)")
try await client.connect()

let producer = try client.producer(
    streamID: try Identifier(named: Sample.streamName), topicID: try Identifier(named: Sample.topicName),
    configuration: ProducerConfiguration(partitioning: .partition(Sample.partitionID), topicPartitionsCount: 1, topicMessageExpiry: .never))
try await producer.initialize()
print("Stream and topic are ready.")

print(
    "Messages will be sent to stream: \(Sample.streamName), topic: \(Sample.topicName), partition: \(Sample.partitionID) with interval \(Sample.interval)."
)
var currentID = 0
for _ in 0..<Sample.batchesLimit {
    try await Task.sleep(for: Sample.interval)
    var messages: [IggyMessage] = []
    for _ in 0..<Sample.messagesPerBatch {
        currentID += 1
        messages.append(try IggyMessage("message-\(currentID)"))
    }
    _ = try await producer.send(messages)
    print("Sent \(messages.count) message(s).")
}
print("Sent \(Sample.batchesLimit) batches of messages, exiting.")
await producer.shutdown()
await client.shutdown()
