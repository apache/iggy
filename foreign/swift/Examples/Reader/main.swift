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

import Foundation
import Iggy

// Sends messages to a topic of three partitions, reads them back with a
// single poll, a partition reader and a group consumer, and commits with each
// of the three commit policies. Run it against a local server:
// `swift run reader-example` with IGGY_TCP_ADDRESS set, 127.0.0.1:8090 by
// default.

let address = ProcessInfo.processInfo.environment["IGGY_TCP_ADDRESS"] ?? "127.0.0.1:8090"
let client = IggyClient(configuration: ClientConfiguration(address: address, autoLogin: .usernamePassword(username: "iggy", password: "iggy")))
try await client.connect()

let stream: Identifier = "reader-example"
let topic: Identifier = "orders"
if try await client.getStream(stream) == nil {
    _ = try await client.createStream(name: "reader-example")
}
if try await client.getTopic(streamID: stream, topicID: topic) == nil {
    _ = try await client.createTopic(streamID: stream, name: "orders", options: TopicCreateOptions(partitionsCount: 3))
}
for partition: UInt32 in 0..<3 {
    var messages = try (0..<5).map { try IggyMessage("order \(partition)-\($0)") }
    try await client.sendMessages(streamID: stream, topicID: topic, partitioning: .partition(partition), messages: &messages)
}

// One partition, one request. The application stores the checkpoint.
let consumer = Consumer.consumer(try Identifier(named: "single-poll"))
let polled = try await client.pollMessages(
    streamID: stream, topicID: topic, partitionID: 0, consumer: consumer, strategy: .first, options: PollOptions(count: 2, maxWait: .seconds(1)))
print("single poll:", polled.messages.map(\.payloadString))
if let checkpoint = polled.checkpoint {
    try await client.storeConsumerPosition(consumer: consumer, streamID: stream, topicID: topic, checkpoint: checkpoint)
}

// Every partition, one request per primary. The server stores each read.
let reader = try await client.reader(
    streamID: stream, topicID: topic, consumer: .consumer(try Identifier(named: "reader")),
    configuration: ReaderConfiguration(partitions: [0, 1, 2], start: .first, options: PollOptions(maxWait: .seconds(1), commit: .onRead)))
var read = 0
while read < 15, let batch = try await reader.next() {
    print("reader, partition \(batch.partitionID):", batch.messages.map(\.payloadString))
    read += batch.messages.count
}
try await reader.close()

// A group member that counts a message once its handler returned and stores
// the counted positions every second and on shutdown.
let member = try client.consumerGroup(
    name: "reader-example", streamID: stream, topicID: topic,
    configuration: ConsumerConfiguration(
        pollingStrategy: .first, pollOptions: PollOptions(maxWait: .seconds(1), commit: .auto(AutoCommit(interval: .seconds(1), after: .processing)))))
try await member.initialize()
let handled = Task {
    try await member.consumeMessages { received in
        print("group member, partition \(received.partitionID):", received.message.payloadString)
    }
}
try await Task.sleep(for: .seconds(3))
await member.shutdown()
_ = await handled.result
await client.shutdown()
