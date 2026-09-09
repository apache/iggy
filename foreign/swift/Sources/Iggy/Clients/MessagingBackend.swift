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

/// The slice of the client the producer and consumer depend on. Keeping it
/// narrow lets their batching, retry, and commit logic run against an
/// in-process double in tests, with the real transport covered end to end.
protocol MessagingBackend: AnyObject, Sendable {
    func getStream(_ streamID: Identifier) async throws -> StreamDetails?
    func createStream(name: String) async throws -> StreamDetails
    func getTopic(streamID: Identifier, topicID: Identifier) async throws -> TopicDetails?
    func createTopic(streamID: Identifier, name: String, options: TopicCreateOptions) async throws -> TopicDetails
    func sendMessages(streamID: Identifier, topicID: Identifier, partitioning: Partitioning, messages: [IggyMessage]) async throws -> SendMessagesResponse
    func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer, count: UInt32, autoCommit: Bool,
        strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages
    func getConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws -> ConsumerGroupDetails?
    func createConsumerGroup(streamID: Identifier, topicID: Identifier, name: String) async throws -> ConsumerGroupDetails
    func joinConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws
    func leaveConsumerGroup(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws
    func storeConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, offset: UInt64) async throws
    func deleteConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) async throws
    var events: AsyncStream<DiagnosticEvent> { get async }
}

extension IggyClient: MessagingBackend {}
