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

// Connects and reads the sample topic from its first message with an
// `IggyConsumer`, which polls in batches and hands messages over one at a
// time as an `AsyncSequence`. Exits once the producer's batches were read.
let options = ExampleOptions.parse()
let client = options.makeClient()
print("Connecting to server at \(options.serverAddress)")
try await client.connect()

let consumer = try client.consumer(
    name: "getting-started", stream: try Identifier(named: Defaults.streamName), topic: try Identifier(named: Defaults.topicName),
    partition: Defaults.partitionID,
    configuration: ConsumerConfiguration(
        pollingStrategy: .first, batchLength: UInt32(Defaults.messagesPerBatch), pollInterval: Defaults.interval, autoCommit: .disabled,
        initRetries: 20, initRetryInterval: Defaults.interval))
try await consumer.initialize()

print(
    "Messages will be consumed from stream: \(Defaults.streamName), topic: \(Defaults.topicName), partition: \(Defaults.partitionID) with interval \(Defaults.interval)."
)
let limit = Defaults.batchesLimit * Defaults.messagesPerBatch
var consumed = 0
for try await received in consumer {
    print("Handling message at offset: \(received.message.offset), payload: \(received.message.payloadString)")
    consumed += 1
    if consumed == limit {
        break
    }
}
print("Consumed \(Defaults.batchesLimit) batches of messages, exiting.")
await consumer.shutdown()
try await client.shutdown()
